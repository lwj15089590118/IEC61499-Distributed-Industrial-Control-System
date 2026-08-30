# -*- coding: utf-8 -*-
"""
运行时用例：接收侧 epoch fencing（本地已见最大纪元）、msg_id 去重窗口、
领导者租约文件锁、paho-mqtt 1.x/2.x 双兼容构造。
"""

from __future__ import annotations

import threading
import time

from communication.message_types import EventType, Priority, make_message
from core.distributed_runtime import (MQTTChannel, SharedMemoryChannel,
                                      current_leader, read_leader_lease,
                                      try_acquire_leader)


def make_dispatch(source: str, epoch: int, task_id: str):
    return make_message(EventType.TASK_DISPATCHED, source,
                        "factory/tasks/node_b",
                        {"task_id": task_id, "action": "CARRY",
                         "target_node": "node_b"},
                        priority=Priority.NORMAL, epoch=epoch)


class TestFencing:
    """接收侧 fencing：工作节点无需参与租约竞争也能拒绝旧纪元指令。"""

    def test_old_epoch_dispatch_rejected(self, minimal_config):
        from core.distributed_runtime import DistributedRuntime
        rt = DistributedRuntime("node_x", config_path=str(minimal_config))
        received = []
        rt.bus.subscribe("factory/#", lambda m: received.append(m), "t")
        rt.bus.start()
        try:
            # 先见到新纪元（epoch=2）的派发
            rt._on_external_message(make_dispatch("node_e", 2, "T-NEW"))
            time.sleep(0.2)
            assert rt._max_seen_epoch == 2
            assert [m.payload["task_id"] for m in received] == ["T-NEW"]
            # 旧纪元（epoch=1）的派发被接收侧拒绝
            rt._on_external_message(make_dispatch("node_a", 1, "T-OLD"))
            time.sleep(0.2)
            assert [m.payload["task_id"] for m in received] == ["T-NEW"]
        finally:
            rt.bus.stop()

    def test_equal_epoch_still_accepted(self, minimal_config):
        from core.distributed_runtime import DistributedRuntime
        rt = DistributedRuntime("node_x", config_path=str(minimal_config))
        received = []
        rt.bus.subscribe("factory/#", lambda m: received.append(m), "t")
        rt.bus.start()
        try:
            rt._on_external_message(make_dispatch("node_e", 2, "T-A"))
            rt._on_external_message(make_dispatch("node_e", 2, "T-B"))
            time.sleep(0.2)
            assert [m.payload["task_id"] for m in received] == ["T-A", "T-B"]
        finally:
            rt.bus.stop()

    def test_non_dispatch_events_not_fenced(self, minimal_config):
        # fencing 只作用于任务派发；心跳/事件等照常流转（纪元仍刷新基准）
        from core.distributed_runtime import DistributedRuntime
        rt = DistributedRuntime("node_x", config_path=str(minimal_config))
        received = []
        rt.bus.subscribe("factory/#", lambda m: received.append(m), "t")
        rt.bus.start()
        try:
            rt._on_external_message(make_dispatch("node_e", 2, "T-A"))
            hb = make_message(EventType.HEARTBEAT, "node_a",
                              "factory/heartbeat/node_a", {"node": "node_a"},
                              priority=Priority.HIGH, epoch=1)
            rt._on_external_message(hb)
            time.sleep(0.2)
            assert len(received) == 2
            assert rt._max_seen_epoch == 2
        finally:
            rt.bus.stop()


class TestDedupWindow:
    """msg_id 去重窗口（deque+set，最旧序淘汰）。"""

    def test_duplicate_dropped(self, minimal_config):
        from core.distributed_runtime import DistributedRuntime
        rt = DistributedRuntime("node_x", config_path=str(minimal_config))
        received = []
        rt.bus.subscribe("factory/#", lambda m: received.append(m), "t")
        rt.bus.start()
        try:
            msg = make_dispatch("node_a", 1, "T-DUP")
            rt._on_external_message(msg)
            rt._on_external_message(msg)      # 双通道重复到达（运行时层去重）
            time.sleep(0.2)
            assert len(received) == 1
        finally:
            rt.bus.stop()


class TestLeaderLease:
    """领导者租约：并发 force 接管不得出现同纪元双写（文件锁）。"""

    def test_concurrent_force_acquire_monotonic_epoch(self, runtime_dir):
        base = try_acquire_leader("node_a", runtime_dir, ttl_ms=60000,
                                  force=True)
        e0 = int(base["epoch"])
        # 8 个线程并发 force 接管：epoch 必须恰好单调 +8（读改写全程持锁）
        threads = [threading.Thread(target=try_acquire_leader,
                                    args=("node_e", runtime_dir, 60000, True))
                   for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        lease = try_acquire_leader("node_a", runtime_dir, ttl_ms=60000,
                                   force=True)
        assert int(lease["epoch"]) == e0 + 9

    def test_read_leader_lease_expiry(self, runtime_dir):
        try_acquire_leader("node_a", runtime_dir, ttl_ms=50)
        assert read_leader_lease(runtime_dir)["leader"] == "node_a"
        assert current_leader(runtime_dir) == "node_a"
        time.sleep(0.08)                       # 等租约过期
        assert read_leader_lease(runtime_dir) is None
        assert current_leader(runtime_dir) is None


class TestPahoCompat:
    """paho-mqtt 1.x/2.x 双兼容：客户端构造不得抛异常（1.6.1 曾崩溃）。"""

    def test_client_constructs_on_installed_paho(self):
        ch = MQTTChannel("node_t", "127.0.0.1", 1, on_message=lambda m: None)
        try:
            ch.start([])                       # broker 不存在：仅标记不可用
            assert ch._client is not None      # 客户端对象构造成功
            assert ch.available is False
        finally:
            ch.stop()


class TestBusRotation:
    """共享事件流大小轮转：超过上限滚动为 .1（保留一份历史）。"""

    def test_rotate_on_oversize(self, runtime_dir, monkeypatch):
        ch = SharedMemoryChannel("rot", runtime_dir, on_message=lambda m: None)
        monkeypatch.setattr(SharedMemoryChannel, "BUS_MAX_BYTES", 512)
        for i in range(20):
            msg = make_message(EventType.HEARTBEAT, "node_x",
                               "factory/heartbeat/node_x", {"pad": "x" * 100})
            assert ch.send(msg)
        bus = runtime_dir / "bus.jsonl"
        rotated = runtime_dir / "bus.jsonl.1"
        assert bus.exists()
        assert rotated.exists(), "超过 512B 后应滚动出历史文件"
        # 当前文件不超过"上限 + 单条余量"，历史文件非空，
        # 且两文件中每一行都是完整 JSON（滚动不打断追加写的原子性）
        assert bus.stat().st_size <= 512 + 300
        import json as _json
        for line in (bus.read_text(encoding="utf-8")
                     + rotated.read_text(encoding="utf-8")).splitlines():
            assert line.strip()
            _json.loads(line)
