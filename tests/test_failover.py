# -*- coding: utf-8 -*-
"""
failover 集成回归测试（真实拉起五节点子进程，纯共享内存通道，不依赖 MQTT Broker）。

场景对应审查报告 P0：
  1. 全部节点以真实进程启动（临时运行时目录 + 测试专用 nodes.yaml）；
  2. 下单并等待任务全部完成；
  3. 复用 simulator/fault_injector 注入 node_a 宕机 -> node_e 热备接管；
  4. 接管后继续下单（node_a 进程随后被真实 kill）；
  5. 断言：全集群 TaskDispatched / TaskCompleted 按任务零重复，
     且 node_e 以更高 epoch 接管派发。
"""

from __future__ import annotations

import copy
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from communication.message_types import EventType, Priority, Topics, make_message  # noqa: E402
from conftest import read_events, wait_until  # noqa: E402

NODE_SCRIPTS = {
    "node_a": PROJECT_ROOT / "nodes" / "node_a_orchestrator.py",
    "node_b": PROJECT_ROOT / "nodes" / "node_b_robot.py",
    "node_c": PROJECT_ROOT / "nodes" / "node_c_plc.py",
    "node_d": PROJECT_ROOT / "nodes" / "node_d_vision.py",
    "node_e": PROJECT_ROOT / "nodes" / "node_e_standby.py",
}


# ==============================================================================
# 集群夹具
# ==============================================================================

class Cluster:
    """五个真实节点进程 + 临时共享内存目录。"""

    def __init__(self, tmp_path: Path):
        self.runtime_dir = tmp_path / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.config_path = self._write_config(tmp_path)
        self.procs: Dict[str, subprocess.Popen] = {}
        self.logs: Dict[str, object] = {}

    # ---- 配置：复用 core/nodes.yaml，覆盖为测试参数（可重复执行）----
    def _write_config(self, tmp_path: Path) -> Path:
        cfg = yaml.safe_load(
            (PROJECT_ROOT / "core" / "nodes.yaml").read_text(encoding="utf-8"))
        cfg["system"]["runtime_dir"] = str(self.runtime_dir)
        cfg["system"]["log_level"] = "WARNING"
        cfg["mqtt"]["enabled"] = False
        # 缩短检测窗口（默认值会让用例等待过久）
        cfg["defaults"]["heartbeat_interval_ms"] = 200
        cfg["defaults"]["heartbeat_timeout_ms"] = 600
        cfg["defaults"]["sync_interval_ms"] = 150
        for node in cfg["nodes"].values():
            node["heartbeat_interval_ms"] = 200
        for fb in cfg["nodes"]["node_a"]["function_blocks"]:
            if fb["name"] == "health_monitor":
                fb["params"]["timeout_ms"] = 900
        for fb in cfg["nodes"]["node_e"]["function_blocks"]:
            if fb["name"] == "standby_monitor":
                fb["params"]["timeout_ms"] = 700
        path = tmp_path / "nodes_test.yaml"
        path.write_text(yaml.safe_dump(cfg, allow_unicode=True),
                        encoding="utf-8")
        return path

    # ---- 进程管理 ----
    def start(self) -> None:
        env = dict(os.environ, IEC61499_CONFIG=str(self.config_path),
                   PYTHONPATH=str(PROJECT_ROOT))
        for node_id, script in NODE_SCRIPTS.items():
            log = open(self.runtime_dir / ("test_%s.log" % node_id), "w")
            self.logs[node_id] = log
            self.procs[node_id] = subprocess.Popen(
                [sys.executable, str(script)], cwd=str(PROJECT_ROOT), env=env,
                stdout=log, stderr=subprocess.STDOUT)

    def kill(self, node_id: str) -> None:
        proc = self.procs.get(node_id)
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    def stop(self) -> None:
        for node_id in list(self.procs):
            self.kill(node_id)
        for log in self.logs.values():
            try:
                log.close()
            except Exception:  # noqa: BLE001
                pass

    # ---- 事件交互（经共享内存通道直发，等价订单模拟器）----
    def publish(self, msg) -> None:
        from core.distributed_runtime import SharedMemoryChannel
        ch = SharedMemoryChannel("test-driver", self.runtime_dir,
                                 on_message=lambda m: None)
        try:
            assert ch.send(msg), "事件写入共享事件流失败"
        finally:
            ch.stop()

    def submit_order(self, order_id: str, product: str = "电机外壳",
                     quantity: int = 1) -> None:
        self.publish(make_message(
            EventType.ORDER_RECEIVED, "test-driver", Topics.ORDERS,
            {"order_id": order_id, "product": product, "quantity": quantity,
             "deadline": 300.0, "priority": 2, "source": "test-driver"},
            priority=Priority.HIGH))

    # ---- 事件流观察 ----
    # node_b 上电自检标定（task_id 前缀 CALIB-BOOT-）不经过主控派发链路，
    # 不会产生 TaskDispatched，观察派发/完成闭环时予以排除。
    BOOT_PREFIX = "CALIB-BOOT-"

    def events(self) -> List:
        return read_events(self.runtime_dir)

    def of_type(self, event_type: str) -> List:
        return [m for m in self.events() if m.event_type == event_type]

    def dispatch_ids(self) -> Dict[str, List[str]]:
        """task_id -> [派发来源节点]（TaskDispatched）。"""
        out: Dict[str, List[str]] = {}
        for m in self.of_type(EventType.TASK_DISPATCHED.value):
            out.setdefault(str(m.payload.get("task_id")), []).append(m.source)
        return out

    def completed_ids(self) -> Dict[str, List[str]]:
        out: Dict[str, List[str]] = {}
        for m in self.of_type(EventType.TASK_COMPLETED.value):
            task_id = str(m.payload.get("task_id"))
            if task_id.startswith(self.BOOT_PREFIX):
                continue
            out.setdefault(task_id, []).append(m.source)
        return out

    def wait_quiet(self, expected_tasks: int, timeout: float) -> None:
        """等待 expected_tasks 个任务全部完成且事件流短暂稳定。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            done = self.completed_ids()
            disp = self.dispatch_ids()
            if len(done) >= expected_tasks and set(disp) == set(done):
                time.sleep(1.0)                    # 静默窗：确认无新事件
                if len(self.completed_ids()) == len(done) \
                        and len(self.dispatch_ids()) == len(disp):
                    return
            time.sleep(0.2)
        raise AssertionError("等待 %d 个任务完成超时；已派发=%s 已完成=%s"
                             % (expected_tasks, sorted(self.dispatch_ids()),
                                sorted(self.completed_ids())))


@pytest.fixture()
def cluster(tmp_path):
    c = Cluster(tmp_path)
    c.start()
    try:
        # 就绪判据：五个节点的心跳都出现在共享事件流上
        wait_until(lambda: len({m.source for m in c.events()
                                if m.event_type == EventType.HEARTBEAT.value}) >= 5,
                   30, "五个节点心跳就绪")
        yield c
    finally:
        c.stop()


# ==============================================================================
# 用例
# ==============================================================================

def test_failover_zero_duplicate_dispatch(cluster):
    c = cluster

    # ---- 阶段一：主控在位，完成一笔订单（电机外壳 x1 = 3 个任务）----
    c.submit_order("ORD-T1")
    c.wait_quiet(expected_tasks=3, timeout=60)
    pre_dispatch = c.dispatch_ids()
    assert len(pre_dispatch) == 3
    assert all(s == ["node_a"] for s in pre_dispatch.values())

    # ---- 阶段二：注入 node_a 宕机（复用项目故障注入器）----
    import simulator.fault_injector as fi
    fi.RUNTIME_DIR = c.runtime_dir
    fi.DIRECTIVES_PATH = c.runtime_dir / "fault_directives.json"
    fi.set_directive("node_a", halted=True, duration_ms=120000)

    # node_e 热备接管（FailoverTriggered 出现在共享事件流）
    wait_until(lambda: any(m.source == "node_e" for m in
                           c.of_type(EventType.FAILOVER_TRIGGERED.value)),
               20, "node_e 热备接管")
    failover = [m for m in c.of_type(EventType.FAILOVER_TRIGGERED.value)
                if m.source == "node_e"][-1]
    assert int(failover.payload.get("epoch", 0)) >= 2

    # ---- 阶段三：接管态下单，任务由 node_e 派发并完成 ----
    # （node_e 直收订单为简化路线：CARRY + INSPECT 两个任务/件）
    c.submit_order("ORD-T2")
    wait_until(lambda: any(s == ["node_e"] for s in
                           c.dispatch_ids().values()),
               20, "node_e 以新纪元派发任务")
    c.wait_quiet(expected_tasks=5, timeout=60)

    # node_a 进程真实终止（宕机注入升级为进程死亡），再完成一笔订单
    c.kill("node_a")
    c.submit_order("ORD-T3")
    c.wait_quiet(expected_tasks=7, timeout=60)

    # ---- 断言：failover 前后全集群零重复派发 / 零重复执行 ----
    dispatched = c.dispatch_ids()
    completed = c.completed_ids()
    dup_disp = {k: v for k, v in dispatched.items() if len(v) > 1}
    dup_done = {k: v for k, v in completed.items() if len(v) > 1}
    assert not dup_disp, "存在重复派发的任务: %s" % dup_disp
    assert not dup_done, "存在重复完成（重复执行）的任务: %s" % dup_done
    assert set(dispatched) == set(completed), "派发与完成集合不一致"
    assert len(dispatched) == 7

    # node_e 接管后派发的消息携带更高纪元（fencing token 生效）
    node_e_epochs = [int(m.epoch) for m in c.of_type(EventType.TASK_DISPATCHED.value)
                     if m.source == "node_e"]
    assert node_e_epochs and min(node_e_epochs) >= 2

    # 旧主控 node_a 的 StateSync 不得晚于接管继续出现（停发快照）
    syncs = c.of_type(EventType.STATE_SYNC.value)
    takeover_ts = failover.timestamp
    late_syncs = [m for m in syncs if m.timestamp > takeover_ts + 0.5]
    assert not late_syncs, "接管后旧主控仍在发布 StateSync %d 条" % len(late_syncs)


def test_failover_is_reproducible(cluster):
    """同一集群配置的轻量幂等检查：夹具可重复启动（可重复执行性）。"""
    c = cluster
    c.submit_order("ORD-R1", product="电机外壳", quantity=1)
    c.wait_quiet(expected_tasks=3, timeout=60)
    assert len(c.dispatch_ids()) == 3
