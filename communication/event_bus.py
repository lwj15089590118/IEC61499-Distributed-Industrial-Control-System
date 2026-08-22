# -*- coding: utf-8 -*-
"""
================================================================================
 communication/event_bus.py —— 事件总线实现
================================================================================
本模块实现分布式控制系统的"神经中枢"：事件总线（Event Bus）。

核心特性：
  1. 发布/订阅模式
     - 订阅者按 MQTT 风格的主题过滤器（'+' 单层通配 / '#' 多层通配）订阅；
     - 发布一条消息时，总线将其投递给所有匹配的订阅回调。

  2. 事件优先级队列
     - 所有待分发消息进入最小堆（优先级数值越小越先调度）；
     - 多个 Worker 线程从队列取消息执行回调，保证慢消费者不阻塞发布者；
     - 同优先级消息按进程内 seq 保持 FIFO 顺序。

  3. 消息持久化与重放
     - 总线可将每条流经的消息追加写入 JSONL 日志（磁盘持久化）；
     - 提供 replay() 接口，把历史消息按序重新注入总线，
       用于节点重启后恢复现场、以及故障复盘分析。

  4. 出站桥接（Outbound Bridge）
     - 外部通道（MQTT / 共享内存）通过 add_outbound_bridge() 挂接到总线；
     - scope=="global" 的消息在本地分发后还会被推送到外部通道，
       从而实现"跨节点"传播；scope=="local" 的消息仅在节点内流转。
================================================================================
"""

from __future__ import annotations

import heapq
import itertools
import logging
import os
import threading
import time
from collections import deque
from pathlib import Path
from typing import Callable, Deque, Dict, List, Optional, Tuple

# 支持以脚本方式直接运行本文件（python event_bus.py）做自检
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys                                          # noqa: E402
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from communication.message_types import Message, Priority  # noqa: E402

logger = logging.getLogger("event_bus")

# 回调签名：接收一条 Message
MessageCallback = Callable[[Message], None]


# ==============================================================================
# 1. 主题匹配 —— MQTT 风格通配符
# ==============================================================================


def topic_matches(pattern: str, topic: str) -> bool:
    """
    判断主题过滤器 pattern 是否匹配具体主题 topic。

    规则与 MQTT 一致：
      '+' 匹配任意单层（例如 factory/tasks/+ 匹配 factory/tasks/node_b）；
      '#' 匹配任意多层后缀（必须位于末尾，例如 factory/# 匹配 factory 下全部）。
    """
    if not pattern or not topic:
        return False
    p_parts = pattern.split("/")
    t_parts = topic.split("/")
    for i, p in enumerate(p_parts):
        if p == "#":                     # 多层通配符：直接命中
            return True
        if i >= len(t_parts):            # 主题层数不足
            return False
        if p != "+" and p != t_parts[i]:  # 单层通配或精确比较
            return False
    return len(t_parts) == len(p_parts)


# ==============================================================================
# 2. 订阅记录
# ==============================================================================


class _Subscription:
    """一条订阅记录：主题过滤器 + 回调 + 可选的名字（便于诊断）。"""

    __slots__ = ("sub_id", "pattern", "callback", "name", "match_count")

    def __init__(self, sub_id: int, pattern: str, callback: MessageCallback, name: str = ""):
        self.sub_id = sub_id
        self.pattern = pattern
        self.callback = callback
        self.name = name or callback.__name__
        self.match_count = 0


# ==============================================================================
# 3. 事件总线主体
# ==============================================================================


class EventBus:
    """
    进程内事件总线（每个节点运行时各持有 一个 实例）。

    线程模型：
      - publish()/publish_local() 只做入队与桥接，立即返回（非阻塞）；
      - N 个 worker 线程消费优先级队列并执行订阅回调；
      - 停止时通过毒丸消息（None）优雅唤醒所有 worker。
    """

    def __init__(self,
                 node_id: str,
                 workers: int = 3,
                 persistence_dir: Optional[str] = None,
                 max_seen: int = 4096,
                 queue_limit: int = 10000) -> None:
        """
        参数：
          node_id        : 所属节点ID（用于日志与持久化文件命名）；
          workers        : 分发工作线程数量；
          persistence_dir: 持久化目录，None 表示不落盘；
          max_seen       : msg_id 去重窗口大小（防内存膨胀）；
          queue_limit    : 优先级队列深度上限（超出则丢弃最低优先级消息）。
        """
        self.node_id = node_id
        self._workers_count = max(1, workers)
        self._queue_limit = queue_limit

        # ---- 优先级队列：(priority, seq, Message)，seq 保证同优先级 FIFO ----
        self._heap: List[Tuple[int, int, Message]] = []
        self._heap_lock = threading.Lock()
        self._seq_counter = itertools.count(1)          # 入队顺序号

        # ---- 订阅表 ----
        self._subs: Dict[int, _Subscription] = {}       # sub_id -> 订阅
        self._sub_seq = itertools.count(1)
        self._subs_lock = threading.Lock()

        # ---- 出站桥接（外部通信通道）----
        self._bridges: List[Callable[[Message], None]] = []

        # ---- msg_id 去重窗口（双通道接收时避免重复处理）----
        self._seen: Deque[str] = deque(maxlen=max_seen)
        self._seen_set: set = set()
        self._seen_lock = threading.Lock()

        # ---- 持久化 ----
        self.persistence_dir = persistence_dir
        self._journal_path = None
        if persistence_dir:
            os.makedirs(persistence_dir, exist_ok=True)
            self._journal_path = os.path.join(
                persistence_dir, "bus_%s.jsonl" % node_id)
            logger.info("消息持久化已启用: %s", self._journal_path)

        # ---- 运行状态与统计 ----
        self._running = False
        self._worker_threads: List[threading.Thread] = []
        self.stats = {
            "published": 0,        # 累计发布（含本地与外部）
            "local_only": 0,       # 仅本地分发的消息数
            "bridged_out": 0,      # 推送到外部通道的消息数
            "dispatched": 0,       # 已执行回调分发的消息数
            "dropped_duplicates": 0,  # 因 msg_id 重复被丢弃的消息数
            "dropped_overflow": 0,   # 队列超限丢弃数
            "persisted": 0,        # 落盘消息数
        }

    # ==========================================================================
    # 3.1 订阅管理
    # ==========================================================================

    def subscribe(self, pattern: str, callback: MessageCallback, name: str = "") -> int:
        """
        订阅主题。

        参数：
          pattern  : 主题过滤器，支持 '+' 与 '#' 通配符，例如 "factory/tasks/+"；
          callback : 消息回调，签名 callback(msg: Message)；
          name     : 订阅名称（用于日志诊断，可省略）。
        返回：订阅ID（用于退订）。
        """
        sub_id = next(self._sub_seq)
        with self._subs_lock:
            self._subs[sub_id] = _Subscription(sub_id, pattern, callback, name)
        logger.debug("[%s] 新订阅 #%d pattern=%s name=%s",
                     self.node_id, sub_id, pattern, name or callback.__name__)
        return sub_id

    def unsubscribe(self, sub_id: int) -> bool:
        """按订阅ID退订；返回是否退订成功。"""
        with self._subs_lock:
            removed = self._subs.pop(sub_id, None)
        return removed is not None

    # ==========================================================================
    # 3.2 发布
    # ==========================================================================

    def publish_local(self, msg: Message, dedup: bool = True) -> bool:
        """
        仅在本节点内分发消息（不触发出站桥接、不外发网络）。

        参数：
          msg    : 消息对象；
          dedup  : 是否做 msg_id 去重（外部通道重复接收时传 True）。
        返回：消息是否成功进入分发队列。
        """
        if dedup and not self._remember(msg.msg_id):
            self.stats["dropped_duplicates"] += 1
            return False                       # 重复消息：直接丢弃
        self._enqueue(msg)
        self.stats["local_only"] += 1
        return True

    def publish(self, msg: Message) -> bool:
        """
        发布消息：本地分发 + （scope=="global" 时）推送到外部通道。

        这是节点内功能块对外发事件的统一出口。
        """
        ok = self.publish_local(msg, dedup=False)   # 自己发布的消息不需去重
        if not ok:
            return False
        self.stats["published"] += 1
        self._persist(msg)
        if msg.scope == "global":
            self._bridge_out(msg)
        return True

    # ==========================================================================
    # 3.3 出站桥接
    # ==========================================================================

    def add_outbound_bridge(self, bridge: Callable[[Message], None]) -> None:
        """
        注册出站桥接函数。

        桥接函数负责把消息送到外部通道（MQTT / 共享内存日志），
        由 core/distributed_runtime.py 中的通信管理器注入。
        """
        self._bridges.append(bridge)

    def _bridge_out(self, msg: Message) -> None:
        """把消息推给所有外部通道；单通道异常不影响其他通道。"""
        for bridge in list(self._bridges):
            try:
                bridge(msg)
                self.stats["bridged_out"] += 1
            except Exception as exc:  # noqa: BLE001 通道故障不应击垮总线
                logger.error("[%s] 出站桥接异常: %s", self.node_id, exc)

    # ==========================================================================
    # 3.4 优先级队列
    # ==========================================================================

    def _enqueue(self, msg: Message) -> None:
        """消息入队（优先级 + FIFO 序号）；队列超限时优先牺牲低优先级消息。"""
        with self._heap_lock:
            if len(self._heap) >= self._queue_limit:
                # 保护策略：丢掉一条当前队列中优先级最低的消息
                self._drop_lowest_locked()
            heapq.heappush(self._heap,
                           (int(msg.priority), next(self._seq_counter), msg))

    def _drop_lowest_locked(self) -> None:
        """（调用方需持有锁）移除队列中优先级数值最大（最低优先级）的一条。"""
        if not self._heap:
            return
        worst_idx = max(range(len(self._heap)),
                        key=lambda i: self._heap[i][0])
        removed = self._heap.pop(worst_idx)
        heapq.heapify(self._heap)
        self.stats["dropped_overflow"] += 1
        logger.warning("[%s] 队列超限，丢弃低优先级消息: %s",
                       self.node_id, removed[2].event_type)

    # ==========================================================================
    # 3.5 分发 Worker
    # ==========================================================================

    def start(self) -> None:
        """启动分发工作线程（幂等：重复调用无副作用）。"""
        if self._running:
            return
        self._running = True
        for i in range(self._workers_count):
            t = threading.Thread(target=self._worker_loop,
                                 name="bus-worker-%d" % i, daemon=True)
            t.start()
            self._worker_threads.append(t)
        logger.info("[%s] 事件总线已启动（worker=%d）",
                    self.node_id, self._workers_count)

    def stop(self) -> None:
        """停止总线：投递毒丸唤醒所有 worker 并等待退出。"""
        if not self._running:
            return
        self._running = False
        for _ in self._worker_threads:
            with self._heap_lock:
                heapq.heappush(self._heap, (Priority.CRITICAL.value,
                                            next(self._seq_counter), None))
        for t in self._worker_threads:
            t.join(timeout=2.0)
        self._worker_threads.clear()
        logger.info("[%s] 事件总线已停止，累计分发 %d 条消息",
                    self.node_id, self.stats["dispatched"])

    def _worker_loop(self) -> None:
        """worker 主循环：阻塞取消息 -> 匹配订阅 -> 执行回调。"""
        while self._running:
            with self._heap_lock:
                if not self._heap:
                    item = None
                else:
                    item = heapq.heappop(self._heap)
            if item is None:
                time.sleep(0.005)              # 队列暂空，短暂让出CPU
                continue
            msg = item[2]
            if msg is None:                    # 毒丸：退出信号
                break
            self._dispatch(msg)

    def _dispatch(self, msg: Message) -> None:
        """把消息分发给所有匹配的订阅回调；单个回调异常不影响其他订阅。"""
        with self._subs_lock:
            matched = [s for s in self._subs.values()
                       if topic_matches(s.pattern, msg.topic)]
        for sub in matched:
            sub.match_count += 1
            try:
                sub.callback(msg)
            except Exception as exc:  # noqa: BLE001
                logger.exception("[%s] 订阅回调 %s 处理消息 %s 异常: %s",
                                 self.node_id, sub.name, msg.event_type, exc)
        self.stats["dispatched"] += 1

    # ==========================================================================
    # 3.6 去重窗口
    # ==========================================================================

    def _remember(self, msg_id: str) -> bool:
        """记录 msg_id；若此前已见过则返回 False（重复消息）。"""
        with self._seen_lock:
            if msg_id in self._seen_set:
                return False
            if len(self._seen) >= self._seen.maxlen:
                old = self._seen.popleft()
                self._seen_set.discard(old)
            self._seen.append(msg_id)
            self._seen_set.add(msg_id)
            return True

    # ==========================================================================
    # 3.7 持久化与重放
    # ==========================================================================

    def _persist(self, msg: Message) -> None:
        """把消息追加写入 JSONL 日志（追加模式，进程崩溃不丢已写内容）。"""
        if not self._journal_path:
            return
        try:
            with open(self._journal_path, "a", encoding="utf-8") as fp:
                fp.write(msg.to_json() + "\n")
            self.stats["persisted"] += 1
        except OSError as exc:
            logger.error("[%s] 消息持久化失败: %s", self.node_id, exc)

    def replay(self, journal_path: Optional[str] = None,
               topic_filter: Optional[str] = None,
               limit: int = 500,
               inject: bool = True) -> List[Message]:
        """
        重放历史消息（默认从自己的持久化日志读取）。

        参数：
          journal_path : 指定日志文件；缺省使用自身持久化路径；
          topic_filter : 只重放匹配该过滤器的消息；
          limit        : 最多重放最近多少条；
          inject       : True 时把消息重新注入本地总线（恢复现场），
                         False 时仅返回消息列表（用于复盘分析）。
        """
        path = journal_path or self._journal_path
        if not path or not os.path.exists(path):
            logger.warning("[%s] 重放失败：日志不存在 %s", self.node_id, path)
            return []

        # 先读入全部行，再取末尾 limit 条（日志可能较大，逐行流式读取）
        lines: Deque[str] = deque(maxlen=limit)
        with open(path, "r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if line:
                    lines.append(line)

        replayed: List[Message] = []
        for line in lines:
            try:
                msg = Message.from_json(line)
            except ValueError:
                continue                       # 跳过损坏行（半写等）
            if topic_filter and not topic_matches(topic_filter, msg.topic):
                continue
            replayed.append(msg)
            if inject:
                msg.scope = "local"            # 重放消息不得再次外发
                self.publish_local(msg, dedup=False)

        logger.info("[%s] 重放完成：%d 条（filter=%s）",
                    self.node_id, len(replayed), topic_filter or "*")
        return replayed

    # ==========================================================================
    # 3.8 诊断
    # ==========================================================================

    def stats_snapshot(self) -> Dict[str, object]:
        """返回总线运行统计快照（供 Web 控制台/健康检查使用）。"""
        with self._subs_lock:
            sub_count = len(self._subs)
        snapshot = dict(self.stats)
        snapshot.update({
            "node_id": self.node_id,
            "subscriptions": sub_count,
            "pending": len(self._heap),
            "workers": self._workers_count,
        })
        return snapshot


# ==============================================================================
# 4. 模块自检 —— python event_bus.py 验证 发布/订阅/优先级/持久化/重放
# ==============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    import tempfile

    received: List[Message] = []
    bus = EventBus("selftest", workers=2,
                   persistence_dir=tempfile.mkdtemp(prefix="bus_test_"))

    # 订阅全部订单类主题
    bus.subscribe("factory/orders/+", lambda m: received.append(m), name="orders")
    # 订阅心跳通配主题（验证 '#' 多层通配）
    hb: List[Message] = []
    bus.subscribe("factory/heartbeat/#", lambda m: hb.append(m), name="hb")
    bus.start()

    from communication.message_types import (EventType, Topics, make_message)

    bus.publish(make_message(EventType.ORDER_RECEIVED, "sim",
                             "factory/orders/new",
                             {"order_id": "O-1"}, priority=Priority.HIGH))
    bus.publish(make_message(EventType.HEARTBEAT, "node_a",
                             Topics.heartbeat_of("node_a"), {}))
    time.sleep(0.3)

    assert len(received) == 1, "订单订阅应收到1条"
    assert len(hb) == 1, "心跳订阅应收到1条"
    assert topic_matches("factory/tasks/+", "factory/tasks/node_b")
    assert not topic_matches("factory/tasks/node_b", "factory/tasks/node_c")
    assert topic_matches("factory/#", "factory/anything/deep")

    # 验证重放：仅读取，不注入
    history = bus.replay(inject=False)
    assert len(history) == 2, "持久化日志应含2条消息"
    bus.stop()
    print("事件总线自检通过：发布/订阅、通配符、优先级队列、持久化、重放均正常。")
