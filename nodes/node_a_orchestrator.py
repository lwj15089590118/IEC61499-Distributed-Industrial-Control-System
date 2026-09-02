# -*- coding: utf-8 -*-
"""
================================================================================
 nodes/node_a_orchestrator.py —— 节点A：主控节点（Orchestrator）
================================================================================
职责（对应"分布式控制系统的大脑"）：
  1. 订单接收与分解：把生产订单按产品工艺路线拆解成可执行任务链；
  2. 任务队列管理：优先级队列 + 按节点健康状态派发 + 失败重试；
  3. 节点健康检查：汇聚各节点心跳，超时判定失联并产生告警；
  4. 故障切换触发：工作节点失联 -> 任务自动回队列重派；
     自身失联被热备接管 -> 依据领导者租约自动让位（防脑裂）。

功能块组成（全部由 core/nodes.yaml 参数化实例化）：
  OrderManagerFB      订单分解（ECC状态机演示）
  TaskQueueFB         任务队列与派发
  HealthMonitorFB     心跳汇聚与健康判定
  FailoverControllerFB 故障切换决策
================================================================================
"""

from __future__ import annotations

import heapq
import itertools
import logging
import random
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from communication.message_types import EventType, Priority, Topics  # noqa: E402
from core.distributed_runtime import (DistributedRuntime, apply_connections,  # noqa: E402
                                      configure_logger, current_leader,
                                      guarded_cycle, try_acquire_leader)
from core.function_block import (DataPort, ECC, ECCState, ECCTransition,  # noqa: E402
                                 EventPort, FunctionBlock)

logger = logging.getLogger("node_a")

# ==============================================================================
# 1. 任务动作 -> 执行节点 路由表（订单分解的结果按此映射派发）
# ==============================================================================

ACTION_ROUTING: Dict[str, str] = {
    "CARRY": "node_b",          # 搬运 -> 机器人节点
    "CALIBRATE": "node_b",      # 标定 -> 机器人节点
    "PLAN": "node_b",           # 轨迹规划 -> 机器人节点
    "CONVEYOR_RUN": "node_c",   # 传送带 -> PLC节点
    "CYLINDER_EXTEND": "node_c",# 气缸伸出 -> PLC节点
    "CYLINDER_RETRACT": "node_c",# 气缸缩回 -> PLC节点
    "SERVO_MOVE": "node_c",     # 伺服定位 -> PLC节点
    "INSPECT": "node_d",        # 视觉检测 -> 视觉节点
}

# 产品工艺路线（订单分解的依据：每件产品按此路线生成任务链）
PRODUCT_RECIPES: Dict[str, List[str]] = {
    "电机外壳": ["CARRY", "CONVEYOR_RUN", "INSPECT"],
    "铝支架":   ["CARRY", "SERVO_MOVE", "CONVEYOR_RUN", "INSPECT"],
    "传感器面板": ["CARRY", "CYLINDER_EXTEND", "CYLINDER_RETRACT", "INSPECT"],
    "精密齿轮": ["CARRY", "SERVO_MOVE", "PLAN", "INSPECT"],
}


# ==============================================================================
# 2. 订单管理功能块（ECC 状态机风格）
# ==============================================================================


class OrderManagerFB(FunctionBlock):
    """
    订单接收与分解功能块。

    ECC：
      Idle --NEW_ORDER--> Splitting --(entry: action_split 完成后自迁移)--> Idle
    行为：
      收到 NEW_ORDER 事件（随行 order_id/product/quantity/deadline）后，
      按产品工艺路线把订单拆解为任务链，并发出 ORDER_SPLIT 事件
      （随行任务列表），交由 TaskQueueFB 入队。
    """

    EVENT_INPUTS = [
        EventPort("NEW_ORDER", with_inputs=["order_id", "product",
                                            "quantity", "deadline", "priority"],
                  comment="新订单到达"),
        EventPort("DONE_SPLIT", comment="[内部] 分解完成，回到空闲"),
    ]
    EVENT_OUTPUTS = [
        EventPort("ORDER_SPLIT", with_outputs=["order_id", "tasks", "total"],
                  comment="订单已分解为任务链"),
        EventPort("ORDER_REJECT", with_outputs=["order_id", "reason"],
                  comment="订单被拒绝（未知产品）"),
    ]
    DATA_INPUTS = {
        "order_id": DataPort("order_id", "STRING", "", "订单编号"),
        "product": DataPort("product", "STRING", "", "产品类型"),
        "quantity": DataPort("quantity", "INT", 1, "数量"),
        "deadline": DataPort("deadline", "REAL", 0.0, "交付时限(秒,相对)"),
        "priority": DataPort("priority", "INT", 2, "订单优先级 0最高"),
        "split_granularity": DataPort("split_granularity", "INT", 1, "拆分粒度"),
        "default_priority": DataPort("default_priority", "INT", 2, "缺省优先级"),
    }
    DATA_OUTPUTS = {
        "order_id": DataPort("order_id", "STRING", "", "订单编号"),
        "tasks": DataPort("tasks", "ANY", None, "分解出的任务列表"),
        "total": DataPort("total", "INT", 0, "任务总数"),
        "reason": DataPort("reason", "STRING", "", "拒绝原因"),
    }

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        self.state["orders_processed"] = 0        # 已处理订单计数
        self.state["orders_rejected"] = 0
        self._task_seq = itertools.count(1)       # 任务号发生器

    # ------------------------------------------------------------ ECC 构建
    def build_ecc(self) -> ECC:
        ecc = ECC(initial_state="Idle")
        ecc.add_state(ECCState("Idle", comment="空闲，等待订单"))
        ecc.add_state(ECCState("Splitting", entry_actions=["action_split"],
                               comment="订单分解中"))
        # NEW_ORDER 触发分解；分解动作结束后注入内部事件 DONE_SPLIT 回到 Idle，
        # 保证状态机不滞留在 Splitting（否则后续订单事件会被忽略）
        ecc.add_transition(ECCTransition("Idle", "Splitting", event="NEW_ORDER",
                                         priority=1))
        ecc.add_transition(ECCTransition("Splitting", "Idle", event="DONE_SPLIT",
                                         priority=1))
        return ecc

    # ------------------------------------------------------------ 核心算法
    def action_split(self) -> None:
        """Splitting 状态的 entry 动作：按工艺路线分解订单。"""
        product = str(self.di["product"])
        quantity = max(1, int(self.di["quantity"] or 1))
        order_id = str(self.di["order_id"] or ("ORD-%d" % int(time.time())))

        recipe = PRODUCT_RECIPES.get(product)
        if not recipe:
            # 未知产品：发出拒绝事件（体现错误也是事件流的一部分）
            self.state["orders_rejected"] += 1
            self.emit("ORDER_REJECT", {"order_id": order_id,
                                       "reason": "未知产品类型: %s" % product})
            self.handle_event("DONE_SPLIT")    # 拒绝路径同样要回到 Idle
            return

        tasks: List[Dict[str, Any]] = []
        for item_no in range(quantity):
            for seq, action in enumerate(recipe):
                task_id = "T%06d" % next(self._task_seq)
                tasks.append({
                    "task_id": task_id,
                    "order_id": order_id,
                    "item_no": item_no + 1,
                    "seq": seq,                      # 工序号（同件产品顺序执行）
                    "action": action,
                    "target_node": ACTION_ROUTING[action],
                    "params": {"product": product},
                    "priority": int(self.di["priority"] or self.di["default_priority"]),
                    "attempts": 0,
                })
        # 交期折算成绝对时间戳，供调度紧迫度参考
        deadline_abs = time.time() + float(self.di["deadline"] or 0.0)

        self.state["orders_processed"] += 1
        self.state["last_order"] = order_id
        logger.info("订单 %s (%s x%d) 分解为 %d 个任务，交期 %.0f 秒",
                    order_id, product, quantity, len(tasks),
                    float(self.di["deadline"] or 0.0))
        self.emit("ORDER_SPLIT", {"order_id": order_id, "tasks": tasks,
                                  "total": len(tasks),
                                  "deadline_abs": deadline_abs})
        # 注入内部事件：分解完成，状态机回到 Idle（可重入锁保证安全）
        self.handle_event("DONE_SPLIT")


# ==============================================================================
# 3. 任务队列功能块（过程式风格）
# ==============================================================================


class TaskQueueFB(FunctionBlock):
    """
    任务队列管理功能块：优先级队列 + 派发 + 重试。

    事件输入：
      ENQUEUE     一批任务入队（来自订单分解）；
      TASK_RESULT 执行节点回报的任务结果（完成/失败）；
      REQUEUE     一批任务重新入队（故障切换时回收）；
      NODE_DOWN   某节点失联（暂停对其派发）；
      NODE_UP     某节点恢复（恢复对其派发）。
    事件输出：
      TASK_DISPATCHED 任务已派发至目标节点主题；
      TASK_FAILED     重试耗尽，最终失败告警。
    """

    EVENT_INPUTS = [
        EventPort("ENQUEUE", with_inputs=["tasks"], comment="任务入队"),
        EventPort("TASK_RESULT", with_inputs=["task_id", "status", "node",
                                              "result"], comment="任务结果"),
        EventPort("REQUEUE", with_inputs=["tasks"], comment="任务回队"),
        EventPort("NODE_DOWN", with_inputs=["node"], comment="节点失联"),
        EventPort("NODE_UP", with_inputs=["node"], comment="节点恢复"),
        EventPort("CHECK_INFLIGHT", comment="周期巡检：回收超时在途任务"),
    ]
    EVENT_OUTPUTS = [
        EventPort("TASK_DISPATCHED", with_outputs=["task_id", "target_node",
                                                   "action"], comment="任务已派发"),
        EventPort("TASK_FAILED", with_outputs=["task_id", "reason"], comment="任务最终失败"),
    ]
    DATA_INPUTS = {
        "max_queue_size": DataPort("max_queue_size", "INT", 200, "队列深度上限"),
        "max_retry": DataPort("max_retry", "INT", 3, "最大重试次数"),
        "dispatch_batch": DataPort("dispatch_batch", "INT", 4, "单轮派发批量"),
        "inflight_timeout_s": DataPort("inflight_timeout_s", "REAL", 120.0,
                                       "在途任务超时回收阈值(秒,0=关闭)"),
    }
    DATA_OUTPUTS = {
        "queued": DataPort("queued", "INT", 0, "当前排队数"),
        "inflight": DataPort("inflight", "INT", 0, "执行中任务数"),
    }

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        self._heap: List[tuple] = []                    # (priority, seq, task)
        self._seq = itertools.count(1)
        self._inflight: Dict[str, Dict] = {}            # task_id -> task（已派发未回报）
        self._inflight_since: Dict[str, float] = {}     # task_id -> 派发时刻（超时回收）
        self._offline_nodes: set = set()                # 失联节点（暂停派发）
        # 由装配代码注入：本节点被注入宕机/已让位时返回 True（派发静默闸门）
        self.halt_gate: Optional[Callable[[], bool]] = None
        self.state["dispatched"] = 0
        self.state["completed"] = 0
        self.state["failed"] = 0

    # ------------------------------------------------------------ 事件处理
    def execute(self, event_name: str) -> None:
        if event_name == "ENQUEUE":
            self._enqueue(self.state.get("_ext_tasks") or [])
        elif event_name == "TASK_RESULT":
            self._handle_result()
        elif event_name == "REQUEUE":
            self._requeue(self.state.get("_ext_tasks") or [])
        elif event_name == "NODE_DOWN":
            self._offline_nodes.add(str(self.state.get("_ext_node", "")))
            logger.warning("节点 %s 失联，暂停对其派发（滞留任务 %d 个）",
                           self.state.get("_ext_node"),
                           sum(1 for t in self._inflight.values()
                               if t["target_node"] == self.state.get("_ext_node")))
            self._dispatch()
        elif event_name == "NODE_UP":
            self._offline_nodes.discard(str(self.state.get("_ext_node", "")))
            logger.info("节点 %s 恢复在线，恢复派发", self.state.get("_ext_node"))
            self._dispatch()
        elif event_name == "CHECK_INFLIGHT":
            self._reclaim_stalled()

    def _enqueue(self, tasks: List[Dict]) -> None:
        """任务批量入队并尝试派发（队列满时丢弃最低优先级任务，丢低保高）。"""
        for task in tasks:
            if len(self._heap) >= int(self.di["max_queue_size"]):
                # 淘汰优先级数值最大（最低优先级）的一条，与总线层背压方向一致；
                # 同优先级时淘汰序号较大者（更晚入队）
                worst = max(range(len(self._heap)),
                            key=lambda i: (self._heap[i][0], self._heap[i][1]))
                _, _, dropped = self._heap.pop(worst)
                heapq.heapify(self._heap)
                logger.warning("队列已满，丢弃最低优先级任务 %s（priority=%d）",
                               dropped["task_id"], dropped.get("priority"))
            heapq.heappush(self._heap, (int(task.get("priority", 2)),
                                        next(self._seq), task))
        if tasks:
            logger.info("入队 %d 个任务，当前排队 %d", len(tasks), len(self._heap))
        self._dispatch()

    def _requeue(self, tasks: List[Dict]) -> None:
        """故障切换：回收在途任务重新入队（必须先移出在途表防永久滞留/双计）。"""
        for task in tasks:
            task = dict(task)
            task["attempts"] = int(task.get("attempts", 0)) + 1
            self._inflight.pop(str(task.get("task_id")), None)
            self._inflight_since.pop(str(task.get("task_id")), None)
            self._enqueue([task])
        logger.info("故障回收 %d 个任务重新入队", len(tasks))

    def _handle_result(self) -> None:
        """处理执行节点回报的结果：完成移除，失败重试或告警。"""
        task_id = str(self.state.get("_ext_task_id", ""))
        status = str(self.state.get("_ext_status", "COMPLETED"))
        task = self._inflight.pop(task_id, None)
        self._inflight_since.pop(task_id, None)
        if task is None:
            logger.debug("未知/重复结果 %s，忽略", task_id)
            return

        if status == "COMPLETED":
            self.state["completed"] += 1
            logger.info("任务完成 %s (%s@%s) 累计完成 %d",
                        task_id, task["action"], task["target_node"],
                        self.state["completed"])
        else:
            task["attempts"] = int(task.get("attempts", 0)) + 1
            if task["attempts"] <= int(self.di["max_retry"]):
                # 失败重试：换一个随机抖动避免惊群，重新入队
                logger.warning("任务失败 %s（第 %d 次），重新入队",
                               task_id, task["attempts"])
                self._enqueue([task])
            else:
                self.state["failed"] += 1
                logger.error("任务 %s 重试耗尽，判定最终失败", task_id)
                self.emit("TASK_FAILED", {"task_id": task_id,
                                          "reason": "重试%d次均失败"
                                                    % int(self.di["max_retry"])})
        self._dispatch()

    def _dispatch(self) -> None:
        """派发一轮：按优先级取可派发任务，跳过失联目标，批量发出 TASK_DISPATCHED。"""
        if self.state.get("paused"):
            return                      # 主控让位后暂停派发（防双主控并存）
        if self.halt_gate is not None and self.halt_gate():
            return                      # 本节点被注入宕机/已让位：停止派发（防旧纪元指令外流）
        batch = int(self.di["dispatch_batch"])
        dispatched = 0
        while dispatched < batch and self._heap:
            idx = self._first_dispatchable_idx()
            if idx is None:
                # 队列中所有任务的目标节点都失联：任务保留，等 NODE_UP 再派
                break
            priority, _, task = self._heap.pop(idx)
            heapq.heapify(self._heap)               # 移除任意位置后恢复堆序
            self._inflight[task["task_id"]] = task
            self._inflight_since[task["task_id"]] = time.time()
            self.state["dispatched"] += 1
            dispatched += 1
            # 派发事件：随行完整任务描述 + 当前优先级（fencing）
            self.emit("TASK_DISPATCHED", dict(task, epoch_priority=priority))
        self.do["queued"] = len(self._heap)
        self.do["inflight"] = len(self._inflight)

    def _reclaim_stalled(self) -> None:
        """
        在途任务超时回收：派发后超过 inflight_timeout_s 仍无结果的任务
        （执行节点忙碌丢弃/结果丢失/宕机）重新入队，消除"任务永久滞留"。
        """
        timeout = float(self.di["inflight_timeout_s"])
        if timeout <= 0:
            return
        now = time.time()
        stalled = [tid for tid, since in self._inflight_since.items()
                   if now - since > timeout]
        for tid in stalled:
            task = self._inflight.pop(tid, None)
            self._inflight_since.pop(tid, None)
            if task is None:
                continue
            task["attempts"] = int(task.get("attempts", 0)) + 1
            if task["attempts"] <= int(self.di["max_retry"]):
                logger.warning("任务 %s 在途超时 %.0fs，回收重派（第 %d 次尝试）",
                               tid, timeout, task["attempts"])
                self._enqueue([task])
            else:
                self.state["failed"] += 1
                logger.error("任务 %s 在途超时且重试耗尽，判定最终失败", tid)
                self.emit("TASK_FAILED", {"task_id": tid,
                                          "reason": "在途超时%d秒且重试耗尽"
                                                    % int(timeout)})
        self.do["inflight"] = len(self._inflight)

    def _first_dispatchable_idx(self) -> Optional[int]:
        """
        找到堆中优先级最高且目标节点在线的任务下标。

        原实现只看堆顶、目标失联就整体停摆——一个节点宕机会饿死全部
        其他健康节点的任务（与"按节点健康状态派发"的设计承诺相悖）。
        现改为跳过不可派发任务：失联目标的任务原地保留，其余照常流转。
        """
        best: Optional[int] = None
        for i, (priority, seq, task) in enumerate(self._heap):
            if task["target_node"] in self._offline_nodes:
                continue
            if best is None or (priority, seq) < (self._heap[best][0],
                                                  self._heap[best][1]):
                best = i
        return best

    # ------------------------------------------------------------ 快照（热备同步用）
    def snapshot_tasks(self) -> List[Dict]:
        """导出排队+在途任务的深拷贝（主备状态同步的负载）。"""
        pending = [dict(t) for _, _, t in self._heap]
        inflight = [dict(t) for t in self._inflight.values()]
        return pending + inflight


# ==============================================================================
# 4. 健康监测功能块
# ==============================================================================


class HealthMonitorFB(FunctionBlock):
    """
    节点健康检查功能块。

    事件输入：
      BEAT  收到某节点心跳（随行 node/role）；
      SWEEP 周期巡检事件（由 E_CYCLE 定时器注入，默认 500ms）。
    事件输出：
      NODE_STATUS 节点状态变化（含在线清单）；
      ALERT       节点失联告警。
    """

    EVENT_INPUTS = [
        EventPort("BEAT", with_inputs=["node", "role"], comment="节点心跳"),
        EventPort("SWEEP", comment="周期巡检（定时器注入）"),
    ]
    EVENT_OUTPUTS = [
        EventPort("NODE_STATUS", with_outputs=["nodes", "online", "offline"],
                  comment="全集群状态快照"),
        EventPort("NODE_OFFLINE", with_outputs=["node"], comment="节点失联"),
        EventPort("NODE_ONLINE", with_outputs=["node"], comment="节点恢复在线"),
    ]
    DATA_INPUTS = {
        "timeout_ms": DataPort("timeout_ms", "INT", 3000, "失联判定阈值"),
        "alert_level": DataPort("alert_level", "STRING", "HIGH", "告警级别"),
    }
    DATA_OUTPUTS = {
        "online": DataPort("online", "INT", 0, "在线节点数"),
        "offline": DataPort("offline", "INT", 0, "离线节点数"),
    }

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        # nodes: node_id -> {"last_seen": ts, "role": str, "beats": int, "online": bool}
        self.state["nodes"] = {}

    def execute(self, event_name: str) -> None:
        if event_name == "BEAT":
            node = str(self.state.get("_ext_node", ""))
            if not node:
                return
            table: Dict = self.state["nodes"]
            rec = table.setdefault(node, {"last_seen": 0.0, "role": "",
                                          "beats": 0, "online": False})
            rec["last_seen"] = time.time()
            rec["role"] = str(self.state.get("_ext_role", ""))
            rec["beats"] += 1
            if not rec["online"]:                      # 离线 -> 在线 沿
                rec["online"] = True
                logger.info("节点 %s (%s) 上线", node, rec["role"])
                self.emit("NODE_ONLINE", {"node": node})
        elif event_name == "SWEEP":
            self._sweep()

    def _sweep(self) -> None:
        """巡检：超时未心跳的节点标记失联并发事件（含告警）。"""
        now = time.time()
        timeout = float(self.di["timeout_ms"]) / 1000.0
        table: Dict = self.state["nodes"]
        online = [n for n, r in table.items() if r["online"]]
        offline = []
        for node, rec in table.items():
            if rec["online"] and now - rec["last_seen"] > timeout:
                rec["online"] = False
                offline.append(node)
                logger.error("节点 %s 心跳超时(%.0fms)判定失联！",
                             node, (now - rec["last_seen"]) * 1000)
                self.emit("NODE_OFFLINE", {"node": node,
                                           "last_seen": rec["last_seen"],
                                           "role": rec["role"]})
        self.do["online"] = len([n for n, r in table.items() if r["online"]])
        self.do["offline"] = len([n for n, r in table.items() if not r["online"]
                                  and r["beats"] > 0])
        # 周期发布集群状态快照（供Web/热备观测）
        self.emit("NODE_STATUS", {
            "nodes": {n: {"role": r["role"], "online": r["online"],
                          "beats": r["beats"]} for n, r in table.items()},
            "online": self.do["online"],
            "offline": self.do["offline"],
            "ts": round(now, 3),
        })

    def healthy_nodes(self) -> List[str]:
        """当前在线节点清单（供外部查询）。"""
        return [n for n, r in self.state["nodes"].items() if r["online"]]


# ==============================================================================
# 5. 故障切换控制功能块
# ==============================================================================


class FailoverControllerFB(FunctionBlock):
    """
    故障切换决策功能块。

    事件输入：
      NODE_OFFLINE  来自健康监测（工作节点失联）；
      FAILOVER_DONE 热备节点已完成接管（主控自我让位）。
    事件输出：
      REQUEUE       回收失联节点在途任务（发给任务队列）；
      FAILOVER_ACK  对外广播切换决策摘要。
    """

    EVENT_INPUTS = [
        EventPort("NODE_OFFLINE", with_inputs=["node", "role"], comment="节点失联"),
        EventPort("FAILOVER_DONE", with_inputs=["new_master"],
                  comment="热备节点接管完成"),
    ]
    EVENT_OUTPUTS = [
        EventPort("REQUEUE", with_outputs=["tasks"], comment="任务回收"),
        EventPort("FAILOVER_ACK", with_outputs=["action", "node"], comment="切换决策"),
    ]
    DATA_INPUTS = {
        "standby_node": DataPort("standby_node", "STRING", "node_e", "热备节点ID"),
        "requeue_on_worker_down": DataPort("requeue_on_worker_down", "BOOL", True,
                                           "工作节点失联是否回收任务"),
    }
    DATA_OUTPUTS = {}

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        self.task_queue: Optional[TaskQueueFB] = None   # 由装配代码注入
        self.state["demoted"] = False                   # 是否已被热备取代

    def execute(self, event_name: str) -> None:
        node = str(self.state.get("_ext_node", ""))
        if event_name == "NODE_OFFLINE":
            if not node:
                return
            role = str(self.state.get("_ext_role", ""))
            logger.error("故障切换：节点 %s(%s) 失联", node, role)
            if node == str(self.di["standby_node"]):
                # 热备节点自己挂了：主控只需告警（无任务损失）
                self.emit("FAILOVER_ACK", {"action": "standby-lost", "node": node})
                return
            # 工作节点失联：回收其在途任务 -> 重派到其他在线节点
            if self.di["requeue_on_worker_down"] and self.task_queue:
                lost = [t for t in list(self.task_queue._inflight.values())
                        if t["target_node"] == node]
                if lost:
                    # 重定向到同角色的备选节点（简化：留在原队列由调度器重派）
                    self.emit("REQUEUE", {"tasks": lost})
            self.emit("FAILOVER_ACK", {"action": "worker-down-requeue",
                                       "node": node})
        elif event_name == "FAILOVER_DONE":
            # 热备已接管：主控让位（停止派发，等待运维仲裁后重启为备用）
            new_master = str(self.state.get("_ext_new_master", "?"))
            self.state["demoted"] = True
            if self.task_queue is not None:
                self.task_queue.state["paused"] = True   # 暂停本侧任务派发
            logger.warning("主控让位：新主控为 %s，本节点停止任务派发", new_master)
            self.emit("FAILOVER_ACK", {"action": "master-demoted",
                                       "node": new_master})


# ==============================================================================
# 6. 节点A 装配与主程序
# ==============================================================================

# 内部互连主题（scope=local，只在节点内流转，不出网）
T_ORDER_SPLIT = "node_a/internal/order_split"
T_NODE_OFFLINE_EVT = "node_a/internal/node_offline"
T_NODE_ONLINE_EVT = "node_a/internal/node_online"
T_REQUEUE = "node_a/internal/requeue"

FB_REGISTRY = {"OrderManagerFB": OrderManagerFB, "TaskQueueFB": TaskQueueFB,
               "HealthMonitorFB": HealthMonitorFB,
               "FailoverControllerFB": FailoverControllerFB}


def build_runtime(config_path: Optional[str] = None) -> DistributedRuntime:
    """装配节点A：实例化FB -> 连接事件流 -> 订阅外部主题。"""
    rt = DistributedRuntime("node_a", config_path=config_path)
    order_mgr, task_q, health, failover = rt.autoload_fbs(FB_REGISTRY)

    # ---- 事件连接：优先 nodes.yaml connections 组态；缺省回退硬编码 ----
    # （动态主题路由与结果过滤订阅与组态无关，见下方"公共装配"）
    fb_index = {"order_manager": order_mgr, "task_queue": task_q,
                "health_monitor": health, "failover_ctrl": failover}
    conns = rt.node_cfg.get("connections")
    if conns:
        apply_connections(rt, conns, fb_index)
    else:
        _wire_node_a(rt, order_mgr, task_q, health, failover)

    # ---- 公共装配（动态/过滤类连接，不进组态）----
    # 任务派发：按任务的目标节点动态选择主题 factory/tasks/<node>
    rt.route_output(task_q, "TASK_DISPATCHED",
                    topic=lambda data: Topics.tasks_of(data.get("target_node", "unknown")),
                    event_type=EventType.TASK_DISPATCHED)

    failover.task_queue = task_q                      # 注入队列引用用于回收
    # 派发闸门：本节点被注入宕机即停发任务（防旧纪元指令在接管窗口期外流）
    task_q.halt_gate = lambda: rt.faults.halted()

    # 执行结果回流：只把 TASK_COMPLETED / TASK_FAILED 送入队列结果口，
    # 进度/标定/视觉等事件虽然共用 factory/events 主题但被过滤器忽略，
    # 避免把"进度消息"误判为"已完成"。
    def _on_task_event(msg) -> None:
        if msg.event_type == EventType.TASK_COMPLETED.value:
            task_q.handle_event("TASK_RESULT", dict(msg.payload,
                                                    status="COMPLETED"))
        elif msg.event_type == EventType.TASK_FAILED.value:
            task_q.handle_event("TASK_RESULT", dict(msg.payload,
                                                    status="FAILED"))
    rt.bus.subscribe(Topics.EVENTS, _on_task_event, name="task-result-filter")

    # ---- 状态快照引用（主循环里发布给热备节点）----
    rt._task_queue_fb = task_q
    rt._health_fb = health
    rt._failover_fb = failover
    return rt


def _wire_node_a(rt: DistributedRuntime, order_mgr, task_q, health,
                 failover) -> None:
    """硬编码事件连接（nodes.yaml 无 connections 段时的缺省回退，
    与 core/nodes.yaml 中 node_a.connections 组态等价）。"""
    # ---- 内部互连（等价于 IEC 61499 应用中的事件连接线）----
    rt.route_output(order_mgr, "ORDER_SPLIT", T_ORDER_SPLIT,
                    EventType.ORDER_SPLIT, scope="local")
    rt.bind_input(T_ORDER_SPLIT, task_q, "ENQUEUE")
    rt.route_output(task_q, "TASK_FAILED", Topics.ALERTS,
                    EventType.ALERT, priority=Priority.HIGH)

    # 健康监测：心跳入口 + 状态/失联出口（两路路由：告警外发 + 内部联动）
    rt.bind_input("factory/heartbeat/+", health, "BEAT")
    rt.route_output(health, "NODE_OFFLINE", Topics.ALERTS,
                    EventType.NODE_OFFLINE, priority=Priority.CRITICAL)
    rt.route_output(health, "NODE_OFFLINE", T_NODE_OFFLINE_EVT,
                    EventType.NODE_OFFLINE, scope="local")
    rt.bind_input(T_NODE_OFFLINE_EVT, task_q, "NODE_DOWN")
    rt.bind_input(T_NODE_OFFLINE_EVT, failover, "NODE_OFFLINE")
    rt.route_output(health, "NODE_ONLINE", T_NODE_ONLINE_EVT,
                    EventType.NODE_ONLINE, scope="local")
    rt.bind_input(T_NODE_ONLINE_EVT, task_q, "NODE_UP")

    # 故障切换：回收任务 -> 任务队列；感知热备接管 -> 主控让位
    rt.route_output(failover, "REQUEUE", T_REQUEUE,
                    EventType.TASK_FAILED, scope="local")
    rt.bind_input(T_REQUEUE, task_q, "REQUEUE")
    rt.bind_input("factory/failover", failover, "FAILOVER_DONE")

    # ---- 外部输入 ----
    rt.bind_input("factory/orders", order_mgr, "NEW_ORDER")       # 模拟器订单
    rt.bind_input("factory/web/orders", order_mgr, "NEW_ORDER")   # Web手动下单


def spawn_cycle_threads(rt: DistributedRuntime, stop_evt: threading.Event,
                        backoff_s: float = 1.0) -> List[threading.Thread]:
    """
    构造 node_a 的全部 E_CYCLE 周期线程（健康巡检/同步续租/在途巡检）。

    三条线程统一经 guarded_cycle 防护（复审报告10 N1）：FileLock/底层IO
    的单次异常退避重试不致命，连续异常升级告警，达上限留下"线程已死"
    日志与状态标记后退出——杜绝"一次异常杀死续租/巡检线程 -> 静默降级/
    任务永久滞留"。backoff_s 供测试缩短退避（生产默认 1s）。
    """
    # 1) 健康巡检（500ms 注入一次 SWEEP 事件 —— 仍是事件驱动而非循环扫描FB）
    health = rt._health_fb

    # 2) 领导者租约续约 + 状态快照同步（热备的数据源）
    task_q = rt._task_queue_fb
    failover_fb = rt._failover_fb

    def _demoted_or_down() -> bool:
        """
        主控"发号施令权"失效判定（P0 防重复派发的发送侧闸门）：
          - 本节点被注入宕机（halted：心跳已停，快照/续租同样必须停）；
          - 已感知热备接管（FailoverControllerFB 置 demoted）；
          - 租约当前归属其他节点（失去租约/进程重启后发现新领导者）。
        """
        leader = current_leader(rt.runtime_dir)
        return (rt.faults.halted()
                or bool(failover_fb.state.get("demoted"))
                or (leader is not None and leader != "node_a"))

    def _sync_and_renew() -> None:
        if _demoted_or_down():
            # 宕机/让位/失租后：停发 StateSync 快照、停续租约。
            # 否则旧主控会以（续租时 adopt 的）新纪元持续发布快照，
            # 反复覆盖热备活队列，造成接管后大规模重复派发（P0 根因）。
            return
        # 续约领导者租约（ttl=2s，探测周期500ms，失联后热备可强制接管）
        lease = try_acquire_leader("node_a", rt.runtime_dir, ttl_ms=2000)
        rt.epoch = int(lease.get("epoch", 0))
        # 发布状态快照给热备节点（队列深拷贝 + 集群健康 + 纪元）
        rt.publish(Topics.sync_of("node_a"), EventType.STATE_SYNC, {
            "epoch": rt.epoch,
            "queue": task_q.snapshot_tasks(),
            "dispatched": task_q.state["dispatched"],
            "completed": task_q.state["completed"],
            "failed": task_q.state["failed"],
            "online_nodes": health.healthy_nodes(),
        }, priority=Priority.HIGH)

    threads = [
        threading.Thread(target=guarded_cycle,
                         args=(stop_evt, 0.5,
                               lambda: health.handle_event("SWEEP", {})),
                         kwargs={"backoff_s": backoff_s},
                         name="cycle-sweep", daemon=True),
        threading.Thread(target=guarded_cycle,
                         args=(stop_evt, rt.sync_interval, _sync_and_renew),
                         kwargs={"backoff_s": backoff_s},
                         name="cycle-sync", daemon=True),
        # 3) 在途任务巡检：超时未回报的派发任务回收重派（消除静默丢失）
        threading.Thread(target=guarded_cycle,
                         args=(stop_evt, 1.0,
                               lambda: task_q.handle_event("CHECK_INFLIGHT", {})),
                         kwargs={"backoff_s": backoff_s},
                         name="cycle-inflight", daemon=True),
    ]
    return threads


def main() -> None:
    """节点A主程序：装配 -> 启动 -> 周期性(定时器事件)巡检/同步/续租。"""
    configure_logger("node_a")
    rt = build_runtime()
    rt.start()
    logger.info("========== 主控节点 node_a 已启动 ==========")

    stop_evt = threading.Event()
    for t in spawn_cycle_threads(rt, stop_evt):
        t.start()

    try:
        while not stop_evt.is_set():
            # 主控让位检测：若租约领导者已变为热备节点，则让位并暂停派发（防脑裂）
            leader = current_leader(rt.runtime_dir)
            if leader and leader != "node_a" \
                    and not failover_fb.state.get("demoted"):
                failover_fb.state["demoted"] = True
                task_q.state["paused"] = True
                logger.error("检测到新领导者 %s，node_a 已让位（可重启为备用）", leader)
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，正在优雅停止……")
    finally:
        stop_evt.set()
        rt.stop()
        logger.info("========== 主控节点 node_a 已退出 ==========")


if __name__ == "__main__":
    main()
