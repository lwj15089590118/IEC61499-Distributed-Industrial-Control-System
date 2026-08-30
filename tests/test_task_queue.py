# -*- coding: utf-8 -*-
"""
TaskQueueFB 用例：满队"丢低保高"背压、REQUEUE 移出在途表、
在途任务超时回收、宕机派发闸门。
"""

from __future__ import annotations

import time

import pytest

from nodes.node_a_orchestrator import TaskQueueFB


def make_task(task_id: str, priority: int = 2,
              target: str = "node_b", attempts: int = 0) -> dict:
    return {"task_id": task_id, "order_id": "O", "item_no": 1, "seq": 0,
            "action": "CARRY", "target_node": target,
            "params": {}, "priority": priority, "attempts": attempts}


@pytest.fixture()
def tq():
    fb = TaskQueueFB("tq", {"max_queue_size": 3, "max_retry": 3,
                            "dispatch_batch": 10, "inflight_timeout_s": 0.2})
    fb.events = []
    fb._emit_callback = lambda fb_, ev, data: fb.events.append((ev, dict(data or {})))
    return fb


def dispatched_ids(tq) -> list:
    return [d["task_id"] for ev, d in tq.events if ev == "TASK_DISPATCHED"]


def failed_ids(tq) -> list:
    return [d["task_id"] for ev, d in tq.events if ev == "TASK_FAILED"]


class TestFullQueueDropsLowestPriority:
    """队列满时淘汰优先级数值最大（最低优先级）的任务——丢低保高。"""

    def test_drop_lowest_keeps_high(self, tq):
        tq.state["paused"] = True              # 暂停派发，让任务在堆中积压
        tq.handle_event("ENQUEUE", {"tasks": [make_task("P2", priority=2)]})
        tq.handle_event("ENQUEUE", {"tasks": [make_task("P1", priority=1)]})
        tq.handle_event("ENQUEUE", {"tasks": [make_task("P3", priority=3)]})
        assert len(tq._heap) == 3
        # 第 4 个任务入队：堆满，应淘汰 P3（priority=3 最低优先级）
        tq.handle_event("ENQUEUE", {"tasks": [make_task("P0", priority=0)]})
        queued_priorities = sorted(p for p, _, _ in tq._heap)
        assert queued_priorities == [0, 1, 2]
        # 恢复派发：按优先级顺序全部派出，P3 从未派发
        tq.state["paused"] = False
        tq.handle_event("NODE_UP", {"node": "zzz"})   # 触发一轮 _dispatch
        assert dispatched_ids(tq) == ["P0", "P1", "P2"]

    def test_tie_breaks_by_later_seq(self, tq):
        tq.state["paused"] = True
        tq.handle_event("ENQUEUE", {"tasks": [make_task("A", priority=2),
                                              make_task("B", priority=2)]})
        tq.handle_event("ENQUEUE", {"tasks": [make_task("D", priority=0)]})
        assert len(tq._heap) == 3
        # 第 4 个任务入队触发淘汰：同优先级 (2) 时淘汰更晚入队者 B（seq 更大）
        tq.handle_event("ENQUEUE", {"tasks": [make_task("C", priority=2)]})
        tq.state["paused"] = False
        tq.handle_event("NODE_UP", {"node": "zzz"})
        assert dispatched_ids(tq) == ["D", "A", "C"]


class TestRequeueRemovesInflight:
    """REQUEUE 回收必须把任务移出在途表，防止永久滞留/快照双计。"""

    def test_requeue_moves_task_out_of_inflight(self, tq):
        tq.handle_event("ENQUEUE", {"tasks": [make_task("T1")]})
        assert "T1" in tq._inflight
        task = dict(tq._inflight["T1"])
        tq.handle_event("REQUEUE", {"tasks": [task]})
        # 回收后先出在途表、再重入队并立刻重派：在途表恰好一份
        assert list(tq._inflight) == ["T1"]
        assert list(tq._inflight_since) == ["T1"]
        assert tq._inflight["T1"]["attempts"] == 1
        assert dispatched_ids(tq) == ["T1", "T1"]      # 回收后重派一次


class TestInflightTimeoutReclaim:
    """在途任务超时回收：执行端静默丢失的任务不永久滞留。"""

    def test_stalled_task_reclaimed(self, tq):
        tq.handle_event("ENQUEUE", {"tasks": [make_task("S1")]})
        assert dispatched_ids(tq) == ["S1"]
        time.sleep(0.3)                                 # 超过 inflight_timeout_s=0.2
        tq.handle_event("CHECK_INFLIGHT", {})
        # 超时回收：重新入队并立即重派（attempts+1）
        assert dispatched_ids(tq) == ["S1", "S1"]
        assert tq._inflight["S1"]["attempts"] == 1
        assert tq.do["inflight"] == 1

    def test_fresh_task_not_reclaimed(self, tq):
        tq.handle_event("ENQUEUE", {"tasks": [make_task("F1")]})
        tq.handle_event("CHECK_INFLIGHT", {})
        assert dispatched_ids(tq) == ["F1"]

    def test_retries_exhausted_marks_failed(self, tq):
        tq.handle_event("ENQUEUE", {"tasks": [make_task("X1", attempts=3)]})
        time.sleep(0.3)
        tq.handle_event("CHECK_INFLIGHT", {})
        assert failed_ids(tq) == ["X1"]
        assert "X1" not in tq._inflight


class TestHaltGate:
    """节点被注入宕机/让位时，派发闸门静默（防旧纪元指令外流）。"""

    def test_gate_blocks_dispatch(self, tq):
        tq.halt_gate = lambda: True
        tq.handle_event("ENQUEUE", {"tasks": [make_task("G1"),
                                              make_task("G2")]})
        assert dispatched_ids(tq) == []
        assert len(tq._heap) == 2                       # 任务留在堆中
        # 闸门打开后恢复派发
        tq.halt_gate = lambda: False
        tq.handle_event("NODE_UP", {"node": "zzz"})
        assert dispatched_ids(tq) == ["G1", "G2"]


class TestSnapshot:
    """快照导出（热备复制负载）：排队 + 在途各一份，不重复。"""

    def test_snapshot_no_double_count(self, tq):
        tq.state["paused"] = True
        tq.handle_event("ENQUEUE", {"tasks": [make_task("Q1"),
                                              make_task("Q2")]})
        tq.state["paused"] = False
        tq.handle_event("NODE_UP", {"node": "zzz"})
        ids = sorted(t["task_id"] for t in tq.snapshot_tasks())
        assert ids == ["Q1", "Q2"]
