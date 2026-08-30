# -*- coding: utf-8 -*-
"""
ECC 迁移表用例：守卫迁移、无迁移保持状态、单容量 FB 忙碌排队（防静默丢失）、
订单分解状态机。
"""

from __future__ import annotations

import pytest

from core.function_block import FunctionBlock
from nodes.node_a_orchestrator import OrderManagerFB
from nodes.node_b_robot import MaterialHandlingFB


class Recorder:
    """捕获 FB 输出事件的最小回调容器。"""

    def __init__(self, fb: FunctionBlock):
        self.events = []
        fb._emit_callback = lambda fb_, ev, data: self.events.append((ev, dict(data or {})))

    def of(self, event_name: str):
        return [data for ev, data in self.events if ev == event_name]


@pytest.fixture()
def handling(monkeypatch) -> MaterialHandlingFB:
    """搬运 FB：禁用运动仿真线程，用例手动注入内部事件（确定性）。"""
    monkeypatch.setattr(MaterialHandlingFB, "_spawn", lambda self, fn: None)
    fb = MaterialHandlingFB("handling", {"speed_ratio": 2.0})
    return fb


class TestPumpECC:
    """ECC 基础语义（守卫 + 无迁移保持状态）。"""

    def test_guard_and_ignored_event_keep_state(self):
        from core.function_block import PumpFB
        fb = PumpFB("pump", {})
        rec = Recorder(fb)
        # 守卫不满足：状态保持 STOPPED
        fb.handle_event("START", {"enable": False, "target_flow": 5.0})
        assert fb.ecc.current_state == "STOPPED"
        assert rec.of("RUNNING") == []
        # 守卫满足：迁移并发出 RUNNING
        fb.handle_event("START", {"enable": True, "target_flow": 5.0})
        assert fb.ecc.current_state == "RUNNING"
        assert rec.of("RUNNING")[-1]["flow"] == 5.0
        # 回迁
        fb.handle_event("STOP")
        assert fb.ecc.current_state == "STOPPED"
        # 无匹配迁移：事件被忽略，状态保持（标准语义）
        assert fb.handle_event("STOP") is True           # 端口合法
        assert fb.ecc.current_state == "STOPPED"
        assert fb.handle_event("NOT_DECLARED") is False  # 未声明端口


class TestMaterialHandlingMigrationTable:
    """搬运 FB 迁移表：Idle --CARRY--> Moving --ARRIVED--> Placing --PLACED--> Idle。"""

    def carry(self, fb: MaterialHandlingFB, task_id: str) -> None:
        fb.handle_event("CARRY", {"task_id": task_id,
                                  "from": [0, 0, 0], "to": [100, 0, 0]})

    def test_full_cycle_migration(self, handling):
        rec = Recorder(handling)
        assert handling.ecc.current_state == "Idle"
        self.carry(handling, "T1")
        assert handling.ecc.current_state == "Moving"
        assert rec.of("TASK_STARTED")[-1]["task_id"] == "T1"
        handling.handle_event("ARRIVED")
        assert handling.ecc.current_state == "Placing"
        handling.handle_event("PLACED")
        assert handling.ecc.current_state == "Idle"
        assert [d["task_id"] for d in rec.of("TASK_COMPLETED")] == ["T1"]
        assert handling.do["cycles"] == 1

    def test_arrived_placed_ignored_in_idle(self, handling):
        # 内部事件迟到（线程冗余触发）：Idle 下无匹配迁移，状态保持
        handling.handle_event("ARRIVED")
        handling.handle_event("PLACED")
        assert handling.ecc.current_state == "Idle"


class TestBusyQueueing:
    """单容量执行 FB 的忙碌排队：并发 CARRY 不再被静默忽略。"""

    def carry(self, fb: MaterialHandlingFB, task_id: str) -> None:
        fb.handle_event("CARRY", {"task_id": task_id,
                                  "from": [0, 0, 0], "to": [100, 0, 0]})

    def test_second_carry_is_queued_then_executed(self, handling):
        rec = Recorder(handling)
        self.carry(handling, "T1")
        assert handling.ecc.current_state == "Moving"
        # 忙碌中的第二个并发 CARRY：被接受并排队（而非无迁移被忽略）
        assert handling.handle_event("CARRY", {"task_id": "T2",
                                               "from": [0, 0, 0],
                                               "to": [100, 0, 0]}) is True
        assert handling.ecc.current_state == "Moving"   # 状态不被第二个事件破坏
        # 完成第一个任务后，队列中的任务自动续跑
        handling.handle_event("ARRIVED")
        handling.handle_event("PLACED")
        assert handling.ecc.current_state == "Moving"   # T2 已接续进入 Moving
        handling.handle_event("ARRIVED")
        handling.handle_event("PLACED")
        assert handling.ecc.current_state == "Idle"
        # 零丢失：两个任务各完成一次，顺序与排队一致
        assert [d["task_id"] for d in rec.of("TASK_COMPLETED")] == ["T1", "T2"]
        assert [d["task_id"] for d in rec.of("TASK_STARTED")] == ["T1", "T2"]

    def test_three_concurrent_carries_no_loss(self, handling):
        rec = Recorder(handling)
        for tid in ("A", "B", "C"):
            self.carry(handling, tid)
        for _ in range(3):
            handling.handle_event("ARRIVED")
            handling.handle_event("PLACED")
        assert [d["task_id"] for d in rec.of("TASK_COMPLETED")] == ["A", "B", "C"]
        assert handling.ecc.current_state == "Idle"


class TestOrderManagerECC:
    """订单分解 ECC：Idle↔Splitting，不滞留中间态。"""

    def test_split_known_product(self):
        fb = OrderManagerFB("om", {})
        rec = Recorder(fb)
        fb.handle_event("NEW_ORDER", {"order_id": "O1", "product": "电机外壳",
                                      "quantity": 2, "priority": 1})
        # 分解动作内注入 DONE_SPLIT，状态机回到 Idle（不滞留 Splitting）
        assert fb.ecc.current_state == "Idle"
        splits = rec.of("ORDER_SPLIT")
        assert len(splits) == 1
        assert splits[0]["total"] == 6            # 2 件 x 3 工序
        actions = [t["action"] for t in splits[0]["tasks"]]
        assert actions == ["CARRY", "CONVEYOR_RUN", "INSPECT"] * 2
        assert fb.state["orders_processed"] == 1

    def test_unknown_product_rejected_and_returns_idle(self):
        fb = OrderManagerFB("om", {})
        rec = Recorder(fb)
        fb.handle_event("NEW_ORDER", {"order_id": "O2", "product": "不存在的工件",
                                      "quantity": 1, "priority": 2})
        assert fb.ecc.current_state == "Idle"
        assert len(rec.of("ORDER_REJECT")) == 1
        assert rec.of("ORDER_SPLIT") == []
        assert fb.state["orders_rejected"] == 1
