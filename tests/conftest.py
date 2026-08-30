# -*- coding: utf-8 -*-
"""
tests/conftest.py —— pytest 全局夹具。

把项目根加入 sys.path，使测试可以像节点入口一样直接导入
communication / core / nodes / simulator 包。
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Callable, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ==============================================================================
# 通用工具
# ==============================================================================

def wait_until(cond: Callable[[], bool], timeout: float, desc: str,
               interval: float = 0.1) -> None:
    """轮询等待条件成立；超时抛 AssertionError（供集成测试使用）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(interval)
    raise AssertionError("等待超时(%.0fs): %s" % (timeout, desc))


def read_events(runtime_dir: Path) -> List:
    """解析共享事件流 bus.jsonl 的全部消息（跳过损坏行）。"""
    from communication.message_types import Message
    path = Path(runtime_dir) / "bus.jsonl"
    if not path.exists():
        return []
    out: List = []
    with open(path, "r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(Message.from_json(line))
            except ValueError:
                continue
    return out


# ==============================================================================
# 公共夹具
# ==============================================================================

@pytest.fixture()
def runtime_dir(tmp_path) -> Path:
    """隔离的运行时目录（每个用例独立，保证测试可重复执行）。"""
    d = tmp_path / "runtime"
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.fixture()
def minimal_config(tmp_path, runtime_dir) -> Path:
    """最小节点配置：MQTT 关闭（纯共享内存），指向临时运行时目录。"""
    import yaml
    cfg = {
        "system": {"name": "IEC61499-TEST", "runtime_dir": str(runtime_dir),
                   "log_level": "WARNING"},
        "mqtt": {"enabled": False, "host": "127.0.0.1", "port": 1883},
        "defaults": {"heartbeat_interval_ms": 200,
                     "heartbeat_timeout_ms": 600,
                     "sync_interval_ms": 150},
        "nodes": {"node_x": {"role": "worker"}},
    }
    path = tmp_path / "nodes_test.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    return path
