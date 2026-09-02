# -*- coding: utf-8 -*-
"""
================================================================================
 web-ui/app.py —— Flask Web 控制台（分布式控制系统的人机界面 HMI）
================================================================================
四大功能：
  1. 系统拓扑展示：节点状态卡片（在线/离线/故障）+ 集群健康汇总；
  2. 实时事件流：滚动日志（直接消费共享事件流 bus.jsonl，无需 WebSocket 服务）；
  3. 功能块参数在线配置：选择 节点/功能块/参数 -> 下发 FBConfigUpdated；
  4. 手动触发任务下发：模拟 MES 手工下单（OrderReceived -> factory/web/orders）。

数据通道（与控制平面完全解耦，只读状态槽 + 追加命令）：
  读：runtime/status_*.json（节点状态槽） + runtime/bus.jsonl（事件流尾部）
  写：runtime/bus.jsonl（订单/配置命令，经共享内存通道带锁写入）
================================================================================
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from collections import Counter, deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from flask import Flask, jsonify, render_template, request  # noqa: E402

from communication.message_types import (EventType, Message, Priority,  # noqa: E402
                                         Topics, make_message)
from core.distributed_runtime import (RUNTIME_DIR, SharedMemoryChannel,  # noqa: E402
                                      current_leader, guarded_cycle,
                                      load_config, read_all_status)

logger = logging.getLogger("web_ui")

app = Flask(__name__)                       # 模板目录默认 templates/

# ==============================================================================
# 1. 控制台后端状态（进程内缓存 + 事件流环形缓冲）
# ==============================================================================

# 事件流环形缓冲（最近 500 条，供 /api/events 快速返回）
_EVENT_BUFFER: Deque[Message] = deque(maxlen=500)
_BUFFER_LOCK = threading.Lock()

# 指标环形窗口：[(ts, {source: count})...] 每秒一桶（供 ECharts 折线）
_RATE_WINDOW: Deque[tuple] = deque(maxlen=120)

# 只读的静态目录：功能块目录来自 nodes.yaml（下拉框数据源）
_CONFIG: Dict[str, Any] = {}

# 命令发布通道（共享内存，写入 bus.jsonl；不启动 watcher）
_publisher: Optional[SharedMemoryChannel] = None


def init_backend(config_path: Optional[str] = None) -> None:
    """初始化配置目录与命令发布通道（main 中调用一次）。"""
    global _CONFIG, _publisher
    _CONFIG = load_config(config_path)
    _publisher = SharedMemoryChannel("web-ui", RUNTIME_DIR,
                                     on_message=lambda m: None)

    # 后台线程：tail 共享事件流，填充事件缓冲与速率窗口
    def _tail_events() -> None:
        """事件流 tail 线程（经 guarded_cycle 统一防护，复审报告10 N1）：
        读取异常退避重试，连续异常升级告警并留死亡标记——原实现只捕
        OSError，其余异常会静默杀死线程导致事件流/Web 面板停更。"""
        last_scan = 0.0

        def _tick() -> None:
            nonlocal last_scan
            messages = SharedMemoryChannel.read_journal(RUNTIME_DIR, limit=800)
            new_idx = 0
            with _BUFFER_LOCK:
                if _EVENT_BUFFER:
                    last_id = _EVENT_BUFFER[-1].msg_id
                    for i, m in enumerate(messages):
                        if m.msg_id == last_id:
                            new_idx = i + 1
                            break
                    else:
                        new_idx = 0
                fresh = messages[new_idx:]
                _EVENT_BUFFER.extend(fresh)
            if fresh and time.time() - last_scan >= 1.0:
                last_scan = time.time()
                _push_rate_bucket(fresh)

        guarded_cycle(None, 1.0, _tick, name="event-tail")

    threading.Thread(target=_tail_events, daemon=True, name="event-tail").start()


def _push_rate_bucket(fresh: List[Message]) -> None:
    """把新到事件按来源计数压入速率窗口（每秒一桶）。"""
    now = time.time()
    bucket: Counter = Counter(m.source for m in fresh)
    _RATE_WINDOW.append((now, dict(bucket)))


# ==============================================================================
# 2. 节点状态判定（状态槽 + 心跳时延 -> 在线/离线/故障）
# ==============================================================================

# 拓扑期望节点清单（顺序即卡片顺序）
EXPECTED_NODES = ["node_a", "node_b", "node_c", "node_d", "node_e"]

ROLE_LABELS = {
    "orchestrator": "主控节点", "robot": "机器人节点", "plc": "PLC节点",
    "vision": "视觉节点", "standby": "热备节点", "demo": "演示节点",
}

HEARTBEAT_TIMEOUT_S = 3.0                   # 状态槽超过 3s 未刷新视为离线


def classify_node(status: Dict[str, Any], age_s: float) -> str:
    """
    节点状态三分类：
      fault  —— 状态槽显式 halted（故障注入宕机）；
      offline —— 心跳超时（状态槽过期）；
      online —— 正常。
    """
    if status.get("state") == "halted":
        return "fault"
    if age_s > HEARTBEAT_TIMEOUT_S or status.get("state") == "stopped":
        return "offline"
    return "online"


def build_topology() -> Dict[str, Any]:
    """组装拓扑数据：节点卡片 + 领导者 + 汇总。"""
    now = time.time()
    slots = read_all_status(RUNTIME_DIR)
    nodes: List[Dict[str, Any]] = []
    for node_id in EXPECTED_NODES:
        st = slots.get(node_id)
        if st is None:
            nodes.append({"node": node_id, "state": "offline",
                          "role": (_CONFIG.get("nodes", {})
                                   .get(node_id, {}).get("role", "?")),
                          "label": ROLE_LABELS.get(node_id, node_id),
                          "ip": (_CONFIG.get("nodes", {})
                                 .get(node_id, {}).get("ip", "-")),
                          "hb_age_s": None, "fb_count": 0, "fbs": [],
                          "epoch": 0})
            continue
        age = max(0.0, now - float(st.get("ts", 0)))
        nodes.append({
            "node": node_id,
            "state": classify_node(st, age),
            "role": st.get("role", "?"),
            "label": ROLE_LABELS.get(st.get("role", ""), node_id),
            "ip": st.get("ip", "-"),
            "hb_age_s": round(age, 2),
            "fb_count": st.get("fb_count", 0),
            "fbs": [{"name": fb.get("name"), "type": fb.get("type"),
                     "ecc": fb.get("ecc_state"),
                     "params": fb.get("params", {})}
                    for fb in st.get("fbs", [])],
            "epoch": st.get("epoch", 0),
        })
    summary = Counter(n["state"] for n in nodes)
    return {
        "ts": round(now, 3),
        "leader": current_leader(RUNTIME_DIR),
        "nodes": nodes,
        "summary": {"online": summary.get("online", 0),
                    "offline": summary.get("offline", 0),
                    "fault": summary.get("fault", 0)},
    }


# ==============================================================================
# 3. REST API 路由
# ==============================================================================


@app.get("/")
def index_page():
    """主页面（暗色科技风仪表盘）。"""
    return render_template("index.html")


@app.get("/api/topology")
def api_topology():
    """拓扑与节点状态（2s 轮询）。"""
    return jsonify(build_topology())


@app.get("/api/events")
def api_events():
    """实时事件流（倒序返回最近 N 条）。"""
    limit = min(int(request.args.get("limit", 100)), 500)
    event_type = request.args.get("type")        # 可选事件类型过滤
    with _BUFFER_LOCK:
        snapshot = list(_EVENT_BUFFER)
    if event_type:
        snapshot = [m for m in snapshot if m.event_type == event_type]
    out = []
    for m in reversed(snapshot[-limit:]):        # 最新在前
        out.append({
            "id": m.msg_id[:8],
            "type": m.event_type,
            "source": m.source,
            "topic": m.topic,
            "priority": m.priority_enum.name,
            "payload": m.payload,
            "ts": round(m.timestamp, 3),
        })
    return jsonify({"count": len(out), "events": out})


@app.get("/api/fbs")
def api_fbs():
    """功能块目录：节点 -> 功能块 -> 参数（在线配置面板数据源）。"""
    topo = build_topology()
    catalog = []
    for node in topo["nodes"]:
        if not node["fbs"]:
            # 节点未运行：退回 nodes.yaml 中的静态声明
            static = (_CONFIG.get("nodes", {}).get(node["node"], {})
                      .get("function_blocks", []))
            fbs = [{"name": fb.get("name", fb.get("type")),
                    "type": fb.get("type"),
                    "ecc": "-",
                    "params": fb.get("params", {})} for fb in static]
        else:
            fbs = node["fbs"]
        catalog.append({"node": node["node"], "state": node["state"],
                        "fbs": fbs})
    return jsonify({"catalog": catalog})


@app.post("/api/fb/config")
def api_fb_config():
    """
    功能块参数在线配置。

    请求体 JSON：{"node": "node_c", "fb": "classifier",
                 "params": {"ng_threshold": 0.55}}
    动作：向 factory/config/<node> 发布 FBConfigUpdated，
          目标节点运行时应用后回执（见事件流）。
    """
    body = request.get_json(silent=True) or {}
    node = str(body.get("node", ""))
    fb = str(body.get("fb", ""))
    params = body.get("params") or {}
    if node not in EXPECTED_NODES or not fb or not params:
        return jsonify({"ok": False,
                        "error": "参数不完整（node/fb/params 必填）"}), 400
    if _publisher is None:
        return jsonify({"ok": False, "error": "后端未初始化"}), 500
    msg = make_message(EventType.FB_CONFIG_UPDATED, "web-ui",
                       Topics.config_of(node),
                       {"fb": fb, "params": params},
                       target=node, priority=Priority.HIGH)
    _publisher.send(msg)
    return jsonify({"ok": True, "node": node, "fb": fb, "params": params,
                    "message": "配置已下发，生效回执见事件流"})


@app.post("/api/task/dispatch")
def api_task_dispatch():
    """
    手动触发任务下发（模拟 MES 手工订单）。

    请求体 JSON：{"product": "电机外壳", "quantity": 3, "priority": 1}
    动作：向 factory/web/orders 发布 OrderReceived。
    """
    body = request.get_json(silent=True) or {}
    product = str(body.get("product", "电机外壳"))
    quantity = max(1, min(int(body.get("quantity", 1) or 1), 100))
    priority = max(0, min(int(body.get("priority", 2) or 2), 3))
    if _publisher is None:
        return jsonify({"ok": False, "error": "后端未初始化"}), 500
    order = {
        "order_id": "WEB-%s-%04d" % (time.strftime("%H%M%S"),
                                     int(time.time() * 10) % 10000),
        "product": product, "quantity": quantity,
        "deadline": 300.0, "priority": priority,
        "source": "web-ui", "created_at": round(time.time(), 3),
    }
    msg = make_message(EventType.ORDER_RECEIVED, "web-ui",
                       Topics.WEB_ORDERS, order,
                       priority=Priority.HIGH)
    _publisher.send(msg)
    return jsonify({"ok": True, "order": order,
                    "message": "订单已下发至 factory/web/orders"})


@app.get("/api/metrics")
def api_metrics():
    """
    ECharts 实时性能数据：
      - rates   : 每秒各节点事件数（折线图，最近 120 秒）；
      - latency : 事件端到端时延统计（采集时消费时延，ms）；
      - dist    : 事件类型分布（饼图）；
      - tasks   : 任务完成/失败累计（柱状）；
      - quality : 视觉质检 OK/NG 统计。
    """
    with _BUFFER_LOCK:
        snapshot = list(_EVENT_BUFFER)
    now = time.time()

    # ---- 每秒速率序列（补齐空桶为 0）----
    series_sources: List[str] = sorted({m.source for m in snapshot})
    rates: List[Dict[str, Any]] = []
    for ts, bucket in list(_RATE_WINDOW):
        rates.append({"t": round(ts, 1),
                      **{src: bucket.get(src, 0) for src in series_sources}})

    # ---- 时延（新鲜消息的 age 有限近似：仅统计 60s 内产生的消息）----
    ages = [m.age_ms() for m in snapshot if now - m.timestamp < 60.0]
    latency = {
        "avg_ms": round(sum(ages) / len(ages), 1) if ages else 0.0,
        "p95_ms": round(sorted(ages)[int(len(ages) * 0.95)] - 1, 1) if ages else 0.0,
        "max_ms": round(max(ages), 1) if ages else 0.0,
    }

    # ---- 事件类型分布 / 任务统计 / 质检统计 ----
    dist = Counter(m.event_type for m in snapshot)
    completed = sum(1 for m in snapshot
                    if m.event_type == EventType.TASK_COMPLETED.value)
    failed = sum(1 for m in snapshot
                 if m.event_type == EventType.TASK_FAILED.value)
    ok = sum(1 for m in snapshot
             if m.event_type == EventType.VISION_RESULT.value
             and m.payload.get("verdict") == "OK")
    ng = sum(1 for m in snapshot
             if m.event_type == EventType.VISION_RESULT.value
             and m.payload.get("verdict") == "NG")

    return jsonify({
        "ts": round(now, 3),
        "rates": rates[-120:],
        "sources": series_sources,
        "latency": latency,
        "dist": dict(dist.most_common(10)),
        "tasks": {"completed": completed, "failed": failed},
        "quality": {"ok": ok, "ng": ng},
        "buffered": len(snapshot),
    })


@app.get("/api/overview")
def api_overview():
    """头部汇总条：集群规模 / 事件总数 / 领导者 / 最近告警。"""
    topo = build_topology()
    alert_types = (EventType.ALERT.value, EventType.NODE_OFFLINE.value,
                   EventType.FAILOVER_TRIGGERED.value)
    recent_alert = None
    with _BUFFER_LOCK:
        total = len(_EVENT_BUFFER)
        for m in reversed(_EVENT_BUFFER):        # 最新在前，找最近一条告警
            if m.event_type in alert_types:
                recent_alert = {"type": m.event_type, "payload": m.payload,
                                "ts": round(m.timestamp, 3)}
                break
    return jsonify({
        "summary": topo["summary"],
        "leader": topo["leader"],
        "total_events": total,
        "recent_alert": recent_alert,
        "ts": topo["ts"],
    })


# ==============================================================================
# 4. 主入口
# ==============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(description="分布式控制系统 Web 控制台")
    # 默认只绑定本机回环地址：控制台无鉴权，不应默认暴露到外部网卡
    parser.add_argument("--host", default="127.0.0.1", help="监听地址")
    parser.add_argument("--port", type=int, default=5000, help="监听端口")
    parser.add_argument("--config", default=None, help="nodes.yaml 路径")
    parser.add_argument("--debug", action="store_true", help="Flask调试模式")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
                        datefmt="%H:%M:%S")
    init_backend(args.config)
    logger.info("Web 控制台启动: http://%s:%d （共享内存目录=%s）",
                args.host, args.port, RUNTIME_DIR)
    app.run(host=args.host, port=args.port, debug=args.debug,
            threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
