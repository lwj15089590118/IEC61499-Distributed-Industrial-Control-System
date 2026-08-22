# -*- coding: utf-8 -*-
"""
================================================================================
 communication/message_types.py —— 分布式控制系统消息类型定义
================================================================================
本模块定义了整个分布式工业控制系统中所有跨节点流转的"事件消息"格式，
是全部节点之间通信的"公共语言"。

设计要点（对应 IEC 61499 的事件驱动语义）：
  1. 每条消息本质上是一个"跨节点事件"（Event），携带着随事件传输的数据
     （对应 IEC 61499 中事件输入/输出与 WITH 关联的数据端口语义）；
  2. 每条消息带优先级，供事件总线的优先级队列调度使用；
  3. 每条消息带全局唯一 msg_id，供双通道（MQTT + 共享内存）传输时
     做幂等去重，避免同一事件被处理两次；
  4. 每条消息带 scope（local/global），节点内部功能块互连的事件
     不外发到网络，减少无效流量。

本模块不依赖任何第三方库，可被所有节点、模拟器、Web 控制台共同引用。
================================================================================
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Dict, Optional

# ==============================================================================
# 1. 事件类型枚举 —— 系统中所有事件的唯一登记处
# ==============================================================================


class EventType(str, Enum):
    """
    系统级事件类型枚举。

    采用 str 枚举，便于直接序列化为 JSON 字符串并在 MQTT 报文中
    保持人类可读性（例如 "OrderReceived"）。
    """

    # ---------------- 订单与任务生命周期 ----------------
    ORDER_RECEIVED = "OrderReceived"          # 模拟器/Web 下发新订单
    ORDER_SPLIT = "OrderSplit"                # 主控节点将订单分解为任务集
    TASK_DISPATCHED = "TaskDispatched"        # 任务被派发到执行节点
    TASK_STARTED = "TaskStarted"              # 执行节点开始执行任务
    TASK_PROGRESS = "TaskProgress"            # 任务执行进度（百分比）
    TASK_COMPLETED = "TaskCompleted"          # 任务执行成功
    TASK_FAILED = "TaskFailed"                # 任务执行失败（将触发重试）

    # ---------------- 节点健康与集群管理 ----------------
    HEARTBEAT = "Heartbeat"                   # 节点心跳（周期性上报状态）
    NODE_ONLINE = "NodeOnline"                # 节点恢复在线
    NODE_OFFLINE = "NodeOffline"              # 节点失联（超时未收到心跳）
    ALERT = "Alert"                           # 告警（温度异常/队列积压等）
    FAILOVER_TRIGGERED = "FailoverTriggered"  # 热备切换已触发
    STATE_SYNC = "StateSync"                  # 主控状态快照同步给备用节点

    # ---------------- 工艺执行结果 ----------------
    VISION_RESULT = "VisionResult"            # 视觉检测结果（OK/NG + 缺陷类别）
    CALIBRATION_DONE = "CalibrationDone"      # 机器人标定完成
    TRAJECTORY_PLANNED = "TrajectoryPlanned"  # 轨迹规划完成
    CONVEYOR_STATUS = "ConveyorStatus"        # 传送带状态上报
    SERVO_IN_POSITION = "ServoInPosition"     # 伺服到位完成
    CYCLE_DONE = "CycleDone"                  # 气缸动作循环完成

    # ---------------- 配置与运维 ----------------
    FB_CONFIG_UPDATED = "FBConfigUpdated"     # 功能块参数被在线修改
    SYSTEM_STATUS = "SystemStatus"            # 系统级状态汇总


# ==============================================================================
# 2. 消息优先级 —— 数值越小优先级越高（适配 heapq 最小堆）
# ==============================================================================


class Priority(IntEnum):
    """
    消息优先级枚举。

    数值越小，在优先级队列中越先被调度：
      - CRITICAL : 故障切换、安全停机等，必须最先处理；
      - HIGH     : 心跳、告警、订单接收；
      - NORMAL   : 常规任务派发与执行结果；
      - LOW      : 状态上报、日志类消息。
    """

    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3

    @classmethod
    def from_int(cls, value: int) -> "Priority":
        """将任意整数安全映射为合法优先级（越界时收敛到最近的档位）。"""
        return cls(max(cls.CRITICAL.value, min(cls.LOW.value, int(value))))


# ==============================================================================
# 3. MQTT 主题（Topic）命名规范
# ==============================================================================


class Topics:
    """
    MQTT 主题命名规范（同时作为共享内存通道中消息的路由键）。

    命名规则： factory / <域> / <子项> [ / <节点ID> ]
    支持通配符： '+' 匹配单层，'#' 匹配多层（与 MQTT 规范一致）。
    """

    ORDERS = "factory/orders"                       # 订单入口（模拟器/Web发布）
    WEB_ORDERS = "factory/web/orders"               # Web 控制台手动下单
    TASKS_TO_NODE = "factory/tasks/{node}"          # 面向指定节点的任务派发
    EVENTS = "factory/events"                       # 通用事件（任务结果等）
    HEARTBEAT = "factory/heartbeat/{node}"          # 节点心跳
    ALERTS = "factory/alerts"                       # 告警
    VISION_RESULTS = "factory/vision/results"       # 视觉检测结果
    STATE_SYNC = "factory/sync/{node}"              # 主控状态快照同步
    FAILOVER = "factory/failover"                   # 热备切换事件
    CONFIG_TO_NODE = "factory/config/{node}"        # 功能块参数下发
    ALL = "factory/#"                               # 订阅全部（调试/Web用）

    @classmethod
    def tasks_of(cls, node_id: str) -> str:
        """生成面向指定节点的任务主题，例如 factory/tasks/node_b。"""
        return cls.TASKS_TO_NODE.format(node=node_id)

    @classmethod
    def heartbeat_of(cls, node_id: str) -> str:
        """生成指定节点的心跳主题，例如 factory/heartbeat/node_a。"""
        return cls.HEARTBEAT.format(node=node_id)

    @classmethod
    def sync_of(cls, node_id: str) -> str:
        """生成指定节点的状态同步主题，例如 factory/sync/node_a。"""
        return cls.STATE_SYNC.format(node=node_id)

    @classmethod
    def config_of(cls, node_id: str) -> str:
        """生成面向指定节点的配置下发主题。"""
        return cls.CONFIG_TO_NODE.format(node=node_id)


# ==============================================================================
# 4. 消息信封（Message）—— 所有跨节点事件的统一封装
# ==============================================================================


@dataclass
class Message:
    """
    事件消息信封。

    字段说明：
      msg_id     : 全局唯一消息ID（uuid4），用于双通道传输去重；
      event_type : 事件类型（对应 EventType 枚举值）；
      source     : 发布者节点ID（例如 "node_a"、"web-ui"、"simulator"）；
      target     : 目标节点ID，"*" 表示广播；
      topic      : 主题（路由键），与 MQTT 主题一致；
      payload    : 事件随行数据（对应 WITH 关联的数据端口）；
      timestamp  : 消息生成时间戳（Unix 秒，保留毫秒精度）；
      priority   : 调度优先级（数值越小越先调度）；
      seq        : 同一进程内的单调递增序号，保证同优先级 FIFO；
      scope      : "local" 仅节点内部分发；"global" 允许外发到其他节点；
      epoch      : 领导者纪元（fencing token），用于热备切换后防止
                   旧主控的过期指令再次生效（脑裂防护）。
    """

    msg_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    event_type: str = EventType.SYSTEM_STATUS.value
    source: str = "unknown"
    target: str = "*"
    topic: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    priority: int = Priority.NORMAL.value
    seq: int = 0
    scope: str = "global"
    epoch: int = 0

    # ------------------------------------------------------------------ 序列化
    def to_dict(self) -> Dict[str, Any]:
        """转换为可直接 json 序列化的字典。"""
        return {
            "msg_id": self.msg_id,
            "event_type": self.event_type,
            "source": self.source,
            "target": self.target,
            "topic": self.topic,
            "payload": self.payload,
            "timestamp": round(self.timestamp, 6),
            "priority": int(self.priority),
            "seq": self.seq,
            "scope": self.scope,
            "epoch": self.epoch,
        }

    def to_json(self) -> str:
        """序列化为 JSON 字符串（单行，便于追加到事件日志文件）。"""
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))

    # ------------------------------------------------------------------ 反序列化
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        """从字典构建消息对象（容忍字段缺失/类型漂移）。"""
        return cls(
            msg_id=str(data.get("msg_id", uuid.uuid4().hex)),
            event_type=str(data.get("event_type", EventType.SYSTEM_STATUS.value)),
            source=str(data.get("source", "unknown")),
            target=str(data.get("target", "*")),
            topic=str(data.get("topic", "")),
            payload=dict(data.get("payload") or {}),
            timestamp=float(data.get("timestamp", time.time())),
            priority=Priority.from_int(int(data.get("priority", Priority.NORMAL.value))).value,
            seq=int(data.get("seq", 0)),
            scope=str(data.get("scope", "global")),
            epoch=int(data.get("epoch", 0)),
        )

    @classmethod
    def from_json(cls, text: str) -> "Message":
        """从 JSON 字符串解析消息对象；解析失败时抛出 ValueError。"""
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("消息JSON解析失败: %s" % exc) from exc
        if not isinstance(data, dict):
            raise ValueError("消息JSON必须为对象")
        return cls.from_dict(data)

    # ------------------------------------------------------------------ 便捷方法
    @property
    def priority_enum(self) -> Priority:
        """以枚举形式返回优先级。"""
        return Priority.from_int(self.priority)

    def age_ms(self) -> float:
        """消息自生成至今的时长（毫秒），用于测算端到端时延。"""
        return max(0.0, (time.time() - self.timestamp) * 1000.0)

    def __repr__(self) -> str:  # 便于日志打印
        return ("Message(<%s> %s -> %s topic=%s prio=%s payload=%s)"
                % (self.event_type, self.source, self.target,
                   self.topic, self.priority_enum.name, self.payload))


# ==============================================================================
# 5. 全局序号发生器 —— 保证同一进程内消息的 FIFO 顺序
# ==============================================================================


class _SeqGenerator:
    """线程安全的单调递增序号发生器（进程内唯一即可）。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counter = 0

    def next(self) -> int:
        with self._lock:
            self._counter += 1
            return self._counter


_SEQ = _SeqGenerator()


def next_seq() -> int:
    """获取下一个进程内消息序号。"""
    return _SEQ.next()


# ==============================================================================
# 6. 消息构造工厂 —— 各节点发布事件的统一入口
# ==============================================================================


def make_message(event_type: Any,
                 source: str,
                 topic: str = "",
                 payload: Optional[Dict[str, Any]] = None,
                 target: str = "*",
                 priority: Any = Priority.NORMAL,
                 scope: str = "global",
                 epoch: int = 0) -> Message:
    """
    构造一条事件消息。

    参数：
      event_type : EventType 枚举或字符串（如 "OrderReceived"）；
      source     : 发布者节点ID；
      topic      : 路由主题；
      payload    : 随事件传输的数据字典；
      target     : 目标节点（"*" 广播）；
      priority   : Priority 枚举或整数；
      scope      : "local"（仅节点内）或 "global"（跨节点）；
      epoch      : 领导者纪元（用于热备切换的 fencing 校验）。
    """
    et = event_type.value if isinstance(event_type, EventType) else str(event_type)
    pr = int(priority.value if isinstance(priority, Priority) else int(priority))
    return Message(
        event_type=et,
        source=source,
        target=target,
        topic=topic,
        payload=dict(payload or {}),
        priority=Priority.from_int(pr).value,
        seq=next_seq(),
        scope=scope,
        epoch=int(epoch),
    )


# ==============================================================================
# 7. 模块自检 —— python message_types.py 可快速验证序列化正确性
# ==============================================================================

if __name__ == "__main__":
    # 构造一条示例订单消息并验证 序列化 -> 反序列化 往返一致性
    demo = make_message(
        event_type=EventType.ORDER_RECEIVED,
        source="simulator",
        topic=Topics.ORDERS,
        payload={"order_id": "ORD-20260822-0001", "product": "电机外壳",
                 "quantity": 20, "deadline": 3600.0},
        priority=Priority.HIGH,
    )
    print("原始消息 :", demo)
    text = demo.to_json()
    print("JSON编码 :", text)
    restored = Message.from_json(text)
    assert restored.msg_id == demo.msg_id
    assert restored.payload["quantity"] == 20
    assert restored.priority_enum is Priority.HIGH
    print("自检通过：消息序列化/反序列化往返一致。")
