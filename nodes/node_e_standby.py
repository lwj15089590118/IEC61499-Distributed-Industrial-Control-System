# -*- coding: utf-8 -*-
"""
================================================================================
 nodes/node_e_standby.py —— 节点E：热备节点（Hot Standby）
================================================================================
职责：
  1. 主节点状态监听：订阅主控心跳与状态快照，实时检测其存活；
  2. 状态同步机制：复制主控的任务队列/统计量，保持"热"状态；
  3. 热备切换（50ms 内接管）：
     - 检测：主控心跳超时（timeout_ms，默认 3 个心跳周期）；
     - 切换：强制获取领导者租约（epoch+1，fencing token），
       恢复复制的任务队列，立即以主控身份派发任务；
     - 切换动作本身（锁租约+队列恢复+事件发布）控制在 50ms 预算内。

一致性保证（防脑裂）：
  - 租约（lease）+ 纪元（epoch）：接管后旧主控的派发指令因 epoch 更小
    被所有节点拒绝；旧主控恢复后发现自己不再是租约持有者，自动让位。
================================================================================
"""

from __future__ import annotations

import heapq
import itertools
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from communication.message_types import EventType, Priority, Topics  # noqa: E402
from core.distributed_runtime import (DistributedRuntime, apply_connections,  # noqa: E402
                                      configure_logger, current_leader,
                                      guarded_cycle, read_leader_lease,
                                      try_acquire_leader)
from core.function_block import DataPort, EventPort, FunctionBlock  # noqa: E402

logger = logging.getLogger("node_e")


# ==============================================================================
# 1. 备用队列功能块（主控任务队列的"影子副本"）
# ==============================================================================


class TaskQueueReplicaFB(FunctionBlock):
    """
    任务队列副本：接收主控 STATE_SYNC 快照并重建本地队列。

    接收侧双重门控（防旧主控快照回灌覆盖活队列 —— P0）：
      1) 发送方必须"当前"持有领导者租约（宕机/让位/失租的旧主控被拒）；
      2) 快照纪元不小于本地已见最大纪元（严格过期的快照被拒）。
    接管后本副本直接作为派发数据源，实现"无缝"续跑。

    线程安全：副本堆的全部读写（快照应用/派发泵/接管态直收订单）
    都经 FB 互斥锁（handle_event / enqueue_tasks）串行化。
    """

    EVENT_INPUTS = [
        EventPort("APPLY_SYNC", with_inputs=["queue", "epoch", "completed",
                                             "dispatched", "failed"],
                  comment="应用主控状态快照"),
        EventPort("POP", with_outputs=[], comment="取出一个可派发任务（接管后）"),
    ]
    EVENT_OUTPUTS = [
        EventPort("TASK_DISPATCHED", with_outputs=["task_id", "target_node",
                                                   "action"], comment="任务派发"),
        EventPort("REPLAY_STATUS", with_outputs=["replicated", "lag_ms"],
                  comment="复制状态上报"),
    ]
    DATA_INPUTS = {
        "queue": DataPort("queue", "ANY", [], "复制的任务队列"),
        "epoch": DataPort("epoch", "INT", 0, "主控纪元"),
        "replica_lag_ms": DataPort("replica_lag_ms", "INT", 100, "允许复制延迟"),
    }
    DATA_OUTPUTS = {
        "replicated": DataPort("replicated", "INT", 0, "已复制任务数"),
    }

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        self._heap: List[tuple] = []                    # (priority, seq, task)
        self._seq = itertools.count(1)
        # 运行时引用由装配代码注入（读取领导者租约做快照来源校验）
        self.runtime_ref: Optional[DistributedRuntime] = None
        self.state["master_epoch"] = 0
        self.state["max_seen_epoch"] = 0                # 本地已见最大纪元
        self.state["last_sync_ts"] = 0.0
        self.state["completed"] = 0
        self.state["dispatched"] = 0
        self.state["failed"] = 0

    def execute(self, event_name: str) -> None:
        if event_name == "APPLY_SYNC":
            self._apply_sync()
        elif event_name == "POP":
            self._pop_and_dispatch()

    def _apply_sync(self) -> None:
        """全量覆盖式复制（快照同步简单可靠，避免增量差异合并的复杂性）。"""
        tasks: List[Dict] = self.di["queue"] or []
        epoch = int(self.di["epoch"] or 0)
        sender = str(self.state.get("_ext_source", ""))
        # 门控一：只接受不早于本地已见最大纪元的快照（过期快照直接拒绝）
        if epoch < int(self.state["max_seen_epoch"]):
            logger.warning("忽略过期纪元快照 epoch=%d < %d",
                           epoch, self.state["max_seen_epoch"])
            return
        # 门控二：发送方必须是"当前"租约领导者（宕机/让位的旧主控被拒）
        lease = (read_leader_lease(self.runtime_ref.runtime_dir)
                 if self.runtime_ref else None)
        if lease is None or not sender or lease.get("leader") != sender:
            logger.warning("拒绝非当前领导者的快照 source=%s 租约领导者=%s epoch=%d",
                           sender or "?",
                           (lease or {}).get("leader", "<无有效租约>"), epoch)
            return
        self._heap = [(int(t.get("priority", 2)), next(self._seq), t)
                      for t in tasks]
        heapq.heapify(self._heap)
        self.state["max_seen_epoch"] = max(int(self.state["max_seen_epoch"]),
                                           epoch)
        self.state["master_epoch"] = epoch
        self.state["last_sync_ts"] = time.time()
        self.state["completed"] = int(self.state.get("_ext_completed", 0))
        self.state["dispatched"] = int(self.state.get("_ext_dispatched", 0))
        self.state["failed"] = int(self.state.get("_ext_failed", 0))
        self.do["replicated"] = len(self._heap)
        self.emit("REPLAY_STATUS", {"replicated": len(self._heap), "lag_ms": 0})

    def _pop_and_dispatch(self) -> None:
        """取出队首任务并发出 TASK_DISPATCHED（接管后的派发动作）。"""
        if not self._heap:
            return
        _, _, task = heapq.heappop(self._heap)
        self.do["replicated"] = len(self._heap)
        self.emit("TASK_DISPATCHED", dict(task))

    def enqueue_tasks(self, tasks: List[Dict]) -> None:
        """接管态直收新订单：加锁入堆（与快照复制/派发泵线程互斥）。"""
        with self._lock:
            for task in tasks:
                self._heap.append((int(task.get("priority", 2)),
                                   next(self._seq), task))
            heapq.heapify(self._heap)
            self.do["replicated"] = len(self._heap)

    def pending_count(self) -> int:
        """当前副本中的待派发任务数。"""
        with self._lock:
            return len(self._heap)


# ==============================================================================
# 2. 热备监视功能块
# ==============================================================================


class StandbyMonitorFB(FunctionBlock):
    """
    主控存活监视与热备切换决策功能块。

    事件输入：
      BEAT      主控心跳（刷新 last_seen）；
      SWEEP     周期巡检（定时注入，默认 100ms —— 检测粒度）；
      MASTER_BACK 主控恢复（本备让位回热备态）。
    事件输出：
      FAILOVER_TRIGGERED 切换完成通告（CRITICAL 优先级）；
      STANDBY_HEARTBEAT  热备自身角色心跳载荷。
    """

    EVENT_INPUTS = [
        EventPort("BEAT", with_inputs=["node", "role"], comment="主控心跳"),
        EventPort("SWEEP", comment="周期巡检"),
        EventPort("MASTER_BACK", with_inputs=["node"], comment="主控恢复"),
    ]
    EVENT_OUTPUTS = [
        EventPort("FAILOVER_TRIGGERED", with_outputs=["took_ms", "epoch",
                                                      "recovered_tasks"],
                  comment="热切换完成"),
        EventPort("ALERT_EVT", with_outputs=["message"], comment="告警"),
    ]
    DATA_INPUTS = {
        "node": DataPort("node", "STRING", "", "心跳来源节点"),
        "role": DataPort("role", "STRING", "", "来源角色"),
        "master_node": DataPort("master_node", "STRING", "node_a", "主控节点ID"),
        "timeout_ms": DataPort("timeout_ms", "INT", 1500, "主控失联判定窗口"),
        "takeover_budget_ms": DataPort("takeover_budget_ms", "INT", 50,
                                       "切换动作时间预算"),
    }
    DATA_OUTPUTS = {}

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        self.state["last_beat"] = 0.0
        self.state["master_up"] = True
        self.state["mode"] = "hot-standby"           # hot-standby / active
        self.state["takeover_count"] = 0
        # 运行时与队列副本由装配代码注入
        self.runtime_ref: Optional[DistributedRuntime] = None
        self.replica: Optional[TaskQueueReplicaFB] = None

    def execute(self, event_name: str) -> None:
        if event_name == "BEAT":
            node = str(self.di["node"])              # WITH 刷新到数据输入
            if node == str(self.di["master_node"]):
                was_down = not self.state["master_up"]
                self.state["last_beat"] = time.time()
                self.state["master_up"] = True
                if was_down:
                    logger.info("主控 %s 心跳恢复", node)
        elif event_name == "SWEEP":
            self._sweep()
        elif event_name == "MASTER_BACK":
            # 已接管且原主控复活：本节点持有更新 epoch 租约，保持 ACTIVE，
            # 由原主控自行让位（仲裁依据：leader_lease.json 中的 epoch）
            logger.warning("原主控复活；当前租约持有者：%s（epoch仲裁）",
                           current_leader(self.runtime_ref.runtime_dir)
                           if self.runtime_ref else "?")

    def _sweep(self) -> None:
        """巡检：主控心跳超时 -> 执行热切换（预算内完成核心动作）。"""
        if self.state["mode"] != "hot-standby":
            return                                    # 已是主控身份，不再切换
        last = self.state["last_beat"]
        if last <= 0:
            return                                    # 尚未收到过主控心跳
        silent_ms = (time.time() - last) * 1000.0
        if silent_ms < float(self.di["timeout_ms"]):
            return
        # ---------------- 检测命中：开始热切换 ----------------
        t0 = time.perf_counter()
        self.state["master_up"] = False
        self.state["mode"] = "active"
        self.state["takeover_count"] += 1
        recovered = self.replica.pending_count() if self.replica else 0

        # 1) 强制获取领导者租约（epoch 自增 => fencing token）—— 核心切换动作
        lease = try_acquire_leader("node_e", self.runtime_ref.runtime_dir,
                                   ttl_ms=2000, force=True)
        self.runtime_ref.epoch = int(lease.get("epoch", 1))
        self.runtime_ref.role = "orchestrator"        # 角色提升
        took_ms = (time.perf_counter() - t0) * 1000.0
        self.state["last_takeover_ms"] = round(took_ms, 3)

        # 2) 通告全集群：CRITICAL 优先级，抢占一切排队中的普通事件
        self.emit("FAILOVER_TRIGGERED", {
            "took_ms": round(took_ms, 3),
            "epoch": self.runtime_ref.epoch,
            "recovered_tasks": recovered,
            "old_master": self.di["master_node"],
            "new_master": "node_e",
        })
        budget = float(self.di["takeover_budget_ms"])
        logger.error("【热切换完成】检测静默%.0fms 切换耗时%.2fms（预算%.0fms %s）"
                     " epoch=%d 恢复任务%d",
                     silent_ms, took_ms, budget,
                     "达标" if took_ms <= budget else "超支",
                     self.runtime_ref.epoch, recovered)
        if took_ms > budget:
            self.emit("ALERT_EVT", {"message":
                                    "热切换耗时%.2fms超出预算%.0fms"
                                    % (took_ms, budget)})

    # ------------------------------------------------ 接管后的派发泵
    def pump_dispatch(self) -> None:
        """接管后周期性续约租约并从副本队列取任务派发（模拟主控的调度循环）。"""
        if self.state["mode"] != "active":
            return
        # 接管态必须持续续约领导者租约：否则租约 2s 后过期，旧主控恢复时会
        # 以更高 epoch 抢回租约并恢复派发，epoch fencing 失效形成双主控；
        # 同时 Web 控制台的"当前主控"也会显示为无。
        if self.runtime_ref is not None:
            lease = try_acquire_leader("node_e", self.runtime_ref.runtime_dir,
                                       ttl_ms=2000)
            self.runtime_ref.epoch = max(int(lease.get("epoch", 0)),
                                         self.runtime_ref.epoch)
        if self.replica is not None:
            self.replica.handle_event("POP", {})


# ==============================================================================
# 3. 节点E 装配与主程序
# ==============================================================================

FB_REGISTRY = {"StandbyMonitorFB": StandbyMonitorFB,
               "TaskQueueReplicaFB": TaskQueueReplicaFB}


def build_runtime(config_path: Optional[str] = None) -> DistributedRuntime:
    """装配节点E：监视主控心跳 -> 复制状态 -> 超时热切换。"""
    rt = DistributedRuntime("node_e", config_path=config_path)
    monitor, replica = rt.autoload_fbs(FB_REGISTRY)

    # 组件互相引用（切换动作需要；副本读租约做快照来源校验）
    monitor.runtime_ref = rt
    monitor.replica = replica
    replica.runtime_ref = rt

    # ---- 事件连接：优先 nodes.yaml connections 组态；缺省回退硬编码 ----
    # （快照复制走"注入来源身份"的定制订阅、动态派发路由与订单直收，
    #   这些无法用通用组态表达，见下方"公共装配"）
    fb_index = {"standby_monitor": monitor, "queue_replica": replica}
    conns = rt.node_cfg.get("connections")
    if conns:
        apply_connections(rt, conns, fb_index)
    else:
        _wire_node_e(rt, monitor)

    # ---- 公共装配 ----
    # 状态快照复制：经定制订阅注入"来源节点身份"，
    # 副本据此校验"发送方当前持有租约"（P0 防旧主控快照回灌）。
    def _on_state_sync(msg) -> None:
        replica.handle_event("APPLY_SYNC", dict(msg.payload,
                                                source=msg.source))
    rt.bus.subscribe(Topics.sync_of("node_a"), _on_state_sync,
                     name="standby-state-sync")

    # ---- 接管后：副本队列任务 -> 按目标节点派发（带新 epoch）----
    rt.route_output(replica, "TASK_DISPATCHED",
                    topic=lambda data: Topics.tasks_of(data.get("target_node",
                                                                "unknown")),
                    event_type=EventType.TASK_DISPATCHED)

    # ---- 接管后接收新订单：直接进入副本队列 ----
    # 复用 OrderManager 不引入循环依赖的做法：节点E实现一个轻量直通逻辑
    # （把订单当作高优先任务派给 node_b/c/d 的检测/搬运工序）
    def _order_intake(msg) -> None:
        """接管后处理新订单：简化分解（CARRY+INSPECT 两工序）。"""
        if monitor.state["mode"] != "active":
            return
        payload = msg.payload
        tasks = []
        for i in range(int(payload.get("quantity", 1))):
            for seq, (action, node) in enumerate((("CARRY", "node_b"),
                                                  ("INSPECT", "node_d"))):
                tasks.append({
                    "task_id": "E-%s-%d-%d" % (payload.get("order_id", "ORD"),
                                               i, seq),
                    "order_id": payload.get("order_id", ""),
                    "action": action, "target_node": node,
                    "params": {"product": payload.get("product", "")},
                    "priority": 2, "attempts": 0})
        # 加锁入堆（原实现跨线程裸改 _heap，与派发泵存在并发竞争）
        replica.enqueue_tasks(tasks)
        logger.info("接管态收到订单 %s，副本队列 +%d 任务",
                    payload.get("order_id", "?"),
                    int(payload.get("quantity", 1)) * 2)

    rt.bus.subscribe(Topics.ORDERS, _order_intake, name="standby-order-intake")
    rt.bus.subscribe(Topics.WEB_ORDERS, _order_intake,
                     name="standby-web-order-intake")
    return rt


def _wire_node_e(rt: DistributedRuntime, monitor) -> None:
    """硬编码事件连接（nodes.yaml 无 connections 段时的缺省回退）。"""
    # ---- 主控心跳监听（双通道都会汇入总线，这里只管订阅主题）----
    rt.bind_input(Topics.heartbeat_of("node_a"), monitor, "BEAT")

    # ---- 切换通告：全局 CRITICAL ----
    rt.route_output(monitor, "FAILOVER_TRIGGERED", Topics.FAILOVER,
                    EventType.FAILOVER_TRIGGERED, priority=Priority.CRITICAL)
    rt.route_output(monitor, "ALERT_EVT", Topics.ALERTS, EventType.ALERT,
                    priority=Priority.HIGH)


def spawn_cycle_threads(rt: DistributedRuntime, stop_evt: threading.Event,
                        backoff_s: float = 1.0) -> List[threading.Thread]:
    """
    构造 node_e 的全部 E_CYCLE 周期线程（失联检测巡检/接管派发泵）。

    派发泵（pump）是接管态唯一的续租与派发驱动：原裸循环下 FileLock/IO
    一次异常即杀死线程——租约 2s 过期后 node_a 已被 demoted 闩锁锁死
    不会接管，集群进入"无领导者、无派发"的静默停滞（复审报告10 N1 最
    危险场景）。现统一经 guarded_cycle 防护：单次异常退避重试、连续
    异常升级告警、达上限留下"线程已死"日志与状态标记后退出。
    backoff_s 供测试缩短退避（生产默认 1s）。
    """
    monitor = rt.get_fb("standby_monitor")
    return [
        # 失联检测巡检：100ms 粒度（检测窗口 = timeout + 0~100ms）
        threading.Thread(target=guarded_cycle,
                         args=(stop_evt, 0.1,
                               lambda: monitor.handle_event("SWEEP", {})),
                         kwargs={"backoff_s": backoff_s},
                         name="sweep", daemon=True),
        # 接管后的派发泵：500ms 一轮（热备态下 pump_dispatch 直接返回）
        threading.Thread(target=guarded_cycle,
                         args=(stop_evt, 0.5, monitor.pump_dispatch),
                         kwargs={"backoff_s": backoff_s},
                         name="pump", daemon=True),
    ]


def main() -> None:
    """节点E主程序：热备待命 -> 巡检（100ms粒度）-> 必要时接管并派发。"""
    configure_logger("node_e")
    rt = build_runtime()
    monitor = rt.get_fb("standby_monitor")
    replica = rt.get_fb("queue_replica")
    rt.start()
    logger.info("========== 热备节点 node_e 已启动（等待主控心跳）==========")

    stop_evt = threading.Event()
    for t in spawn_cycle_threads(rt, stop_evt):
        t.start()

    try:
        while not stop_evt.is_set():
            mode = monitor.state.get("mode", "?")
            pending = replica.pending_count()
            logger.info("[热备状态] mode=%s 复制任务=%d 主控存活=%s",
                        mode, pending,
                        "是" if monitor.state.get("master_up") else "否")
            stop_evt.wait(5.0)
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，正在优雅停止……")
    finally:
        stop_evt.set()
        rt.stop()
        logger.info("========== 热备节点 node_e 已退出 ==========")


if __name__ == "__main__":
    main()
