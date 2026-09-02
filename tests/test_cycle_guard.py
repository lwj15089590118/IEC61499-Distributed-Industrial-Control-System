# -*- coding: utf-8 -*-
"""
周期线程统一异常防护（guarded_cycle）回归用例 —— 对应复审报告10 N1。

背景：node_a 的 sync（续租）/inflight（巡检）线程与 node_e 的 pump（派发
泵）线程原为 `while: fn(); wait()` 裸循环，FileLock 超时/底层 IO 一次异常
即永久杀死线程且静默失能（node_e pump 死后集群进入"无领导者、无派发"
停滞，无日志无告警）。本文件固化防护语义：

  1. 单次异常不致命：ERROR 日志含线程名与完整堆栈，退避后下一拍恢复；
  2. 连续异常升级：WARNING 升级告警；达阈值后 CRITICAL"线程已死"日志
     + 死亡状态标记（mark_cycle_thread_dead / 状态槽 dead_threads）；
  3. 成功一拍即重置连续计数，间歇性异常永不触发自杀；
  4. 真实代码路径：monkeypatch try_acquire_leader 注入 FileLock 超时/
     IO 异常，断言 node_e pump 与 node_a cycle-sync 线程存活并续拍。

全部用例进程内运行（不真起 5 节点进程）。
"""

from __future__ import annotations

import logging
import threading
import time
import traceback

import pytest
import yaml

import nodes.node_a_orchestrator as node_a_mod
import nodes.node_e_standby as node_e_mod
from conftest import wait_until
from core.distributed_runtime import (DEFAULT_CONFIG_PATH, dead_cycle_threads,
                                      guarded_cycle, reset_cycle_thread_guard)


@pytest.fixture(autouse=True)
def _clean_dead_marks():
    """每个用例前后清空死亡标记，保证断言互不污染。"""
    reset_cycle_thread_guard()
    yield
    reset_cycle_thread_guard()


def error_records(caplog, needle: str) -> list:
    """取包含指定文本的 ERROR 记录。"""
    return [r for r in caplog.records
            if r.levelno == logging.ERROR and needle in r.getMessage()]


def exc_text(record) -> str:
    """把记录携带的异常格式化为完整堆栈文本。"""
    assert record.exc_info is not None, "ERROR 记录必须携带堆栈"
    return "".join(traceback.format_exception(*record.exc_info))


class TestGuardedCycleUnit:
    """guarded_cycle 包装函数语义（单元级）。"""

    def test_single_exception_survives_and_recovers(self, caplog):
        """注入一次 FileLock 锁超时异常：线程存活、下一拍恢复工作。"""
        stop = threading.Event()
        ticks: list = []

        def flaky() -> None:
            ticks.append(time.monotonic())
            if len(ticks) == 1:
                # 与 FileLock.__enter__ 锁超时抛出一致的异常类型
                raise TimeoutError("文件锁获取超时: leader_lease.json.lock")

        t = threading.Thread(target=guarded_cycle,
                             args=(stop, 0.02, flaky, "ut-thread"),
                             kwargs={"backoff_s": 0.01},
                             name="ut-thread", daemon=True)
        t.start()
        try:
            wait_until(lambda: len(ticks) >= 3, 5.0, "异常后继续下一拍")
            assert t.is_alive(), "单次异常不得杀死周期线程"
        finally:
            stop.set()
            t.join(2.0)
        assert not t.is_alive()
        # ERROR 日志：含线程名与"第 1 次连续异常"，且带完整堆栈
        errs = error_records(caplog, "ut-thread")
        assert errs, "单次异常必须记录 ERROR 日志"
        assert "第 1 次连续异常" in errs[0].getMessage()
        assert "TimeoutError" in exc_text(errs[0])
        assert "ut-thread" not in dead_cycle_threads()

    def test_consecutive_exceptions_escalate_and_mark_dead(self, caplog):
        """连续异常：WARNING 升级告警 + CRITICAL 线程已死 + 死亡状态标记。"""
        stop = threading.Event()

        def always_fail() -> None:
            raise OSError("模拟底层IO故障（磁盘不可写）")

        t = threading.Thread(target=guarded_cycle,
                             args=(stop, 0.02, always_fail, "dead-thread"),
                             kwargs={"backoff_s": 0.005, "max_consecutive": 4,
                                     "escalation_after": 2},
                             name="dead-thread", daemon=True)
        t.start()
        t.join(5.0)
        assert not t.is_alive(), "达到连续异常阈值后线程应退出"
        # 死亡状态标记（snapshot_status dead_threads 的数据源），含死因
        marks = dead_cycle_threads()
        assert "dead-thread" in marks
        assert "OSError" in marks["dead-thread"]
        # CRITICAL "线程已死" 日志：自杀也不静默
        crit = [r for r in caplog.records
                if r.levelno == logging.CRITICAL
                and "dead-thread" in r.getMessage()]
        assert crit and "线程已死" in crit[0].getMessage()
        # WARNING 升级告警（连续异常达到 escalation_after 后出现）
        warns = [r for r in caplog.records
                 if r.levelno == logging.WARNING and "升级" in r.getMessage()
                 and "dead-thread" in r.getMessage()]
        assert warns, "连续异常必须出现升级告警"
        # 每次异常都有含线程名与堆栈的 ERROR
        errs = error_records(caplog, "dead-thread")
        assert len(errs) >= 4
        assert all(r.exc_info is not None for r in errs)

    def test_success_resets_consecutive_counter(self):
        """间歇性异常（连续2次后成功）：计数被成功拍重置，永不自杀。"""
        stop = threading.Event()
        calls: list = []

        def flaky() -> None:
            calls.append(1)
            if len(calls) % 3 != 0:      # 每3拍：失败2次、成功1次
                raise OSError("间歇性IO抖动")

        t = threading.Thread(target=guarded_cycle,
                             args=(stop, 0.01, flaky, "flap-thread"),
                             kwargs={"backoff_s": 0.005, "max_consecutive": 3,
                                     "escalation_after": 3},
                             name="flap-thread", daemon=True)
        t.start()
        try:
            wait_until(lambda: len(calls) >= 9, 5.0, "间歇异常下持续续拍")
            assert t.is_alive(), "间歇异常不应触发线程自杀"
            assert "flap-thread" not in dead_cycle_threads()
        finally:
            stop.set()
            t.join(2.0)


class TestRealCycleThreads:
    """真实节点线程路径：monkeypatch 注入 FileLock/IO 异常（进程内）。"""

    @staticmethod
    def _node_config(tmp_path, runtime_dir) -> str:
        """以真实 core/nodes.yaml 为底稿：指向临时 runtime 目录并关闭 MQTT。"""
        from core.distributed_runtime import load_config
        cfg = load_config(str(DEFAULT_CONFIG_PATH))
        cfg["system"]["runtime_dir"] = str(runtime_dir)
        cfg["mqtt"]["enabled"] = False
        path = tmp_path / "nodes_guard_test.yaml"
        path.write_text(yaml.safe_dump(cfg, allow_unicode=True),
                        encoding="utf-8")
        return str(path)

    def test_node_e_pump_survives_lock_timeout(self, tmp_path, runtime_dir,
                                               monkeypatch, caplog):
        """node_e 派发泵：首次续租抛 FileLock 超时后线程不死、下一拍续租成功。"""
        rt = node_e_mod.build_runtime(self._node_config(tmp_path, runtime_dir))
        monitor = rt.get_fb("standby_monitor")
        monitor.state["mode"] = "active"     # 泵仅在接管态执行续租+派发

        real_acquire = node_e_mod.try_acquire_leader
        calls: list = []

        def flaky_acquire(node_id, rdir, ttl_ms=3000, force=False):
            calls.append(1)
            if len(calls) == 1:
                raise TimeoutError("文件锁获取超时: leader_lease.json.lock")
            return real_acquire(node_id, rdir, ttl_ms=ttl_ms, force=force)

        monkeypatch.setattr(node_e_mod, "try_acquire_leader", flaky_acquire)
        stop = threading.Event()
        threads = {t.name: t for t in
                   node_e_mod.spawn_cycle_threads(rt, stop, backoff_s=0.05)}
        threads["pump"].start()
        try:
            wait_until(lambda: len(calls) >= 3, 5.0, "pump 异常后继续续租")
            assert threads["pump"].is_alive(), "pump 线程不得被一次锁超时杀死"
            assert "pump" not in dead_cycle_threads()
        finally:
            stop.set()
            threads["pump"].join(2.0)
        errs = error_records(caplog, "pump")
        assert errs and "TimeoutError" in exc_text(errs[0])

    def test_node_a_sync_thread_survives_io_exception(self, tmp_path,
                                                      runtime_dir,
                                                      monkeypatch, caplog):
        """node_a 同步/续租线程：首次续租抛 IO 异常后线程不死、下一拍恢复。"""
        rt = node_a_mod.build_runtime(self._node_config(tmp_path, runtime_dir))
        rt.sync_interval = 0.05              # 缩短测试节拍

        real_acquire = node_a_mod.try_acquire_leader
        calls: list = []

        def flaky_acquire(node_id, rdir, ttl_ms=3000, force=False):
            calls.append(1)
            if len(calls) == 1:
                raise PermissionError("模拟租约文件IO被拒")
            return real_acquire(node_id, rdir, ttl_ms=ttl_ms, force=force)

        monkeypatch.setattr(node_a_mod, "try_acquire_leader", flaky_acquire)
        stop = threading.Event()
        threads = {t.name: t for t in
                   node_a_mod.spawn_cycle_threads(rt, stop, backoff_s=0.05)}
        threads["cycle-sync"].start()
        try:
            wait_until(lambda: len(calls) >= 3, 5.0,
                       "cycle-sync 异常后继续续租/同步")
            assert threads["cycle-sync"].is_alive(), \
                "续租线程不得被一次IO异常杀死"
            assert "cycle-sync" not in dead_cycle_threads()
        finally:
            stop.set()
            threads["cycle-sync"].join(2.0)
        errs = error_records(caplog, "cycle-sync")
        assert errs and "PermissionError" in exc_text(errs[0])
