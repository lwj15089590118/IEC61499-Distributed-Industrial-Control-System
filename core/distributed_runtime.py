# -*- coding: utf-8 -*-
"""
================================================================================
 core/distributed_runtime.py —— IEC 61499 分布式运行时（系统核心）
================================================================================
本模块实现每个控制节点共用的"分布式运行时"，职责包括：

  1. 功能块实例化与管理
     - 从 core/nodes.yaml 读取本节点配置；
     - 按配置中的功能块清单（type/name/params）自动实例化；
     - 维护 FB 注册表、输入绑定（主题->事件输入）与输出路由
       （事件输出->主题），等价于 IEC 61499 的"资源（Resource）"。

  2. 事件驱动调度引擎（非循环扫描！）
     - 节点平时完全静默，只有事件到达（来自总线）或 FB 发出事件时
       才会触发执行 —— 与传统 PLC 的周期扫描形成根本区别；
     - 调度基于事件总线的优先级队列，CRITICAL 消息（如故障切换）
       永远排在 NORMAL 任务之前。

  3. 跨节点通信（MQTT + 共享内存双通道）
     - MQTTChannel  : 基于 paho-mqtt 的网络通道（跨主机部署用）；
     - SharedMemoryChannel : 基于主机共享目录的事件日志通道
       （单机多进程部署零依赖可用，兼作 MQTT 断线时的降级备份）；
     - 双通道同时收发，依靠 msg_id 去重保证"恰好一次"处理语义。

  4. 节点生命周期管理
     - 周期心跳发布 + 状态槽（status_*.json）写入；
     - 故障注入指令（宕机/延迟/丢包）的实时生效；
     - 领导者租约（leader lease + fencing epoch）辅助热备切换。

运行时目录结构（runtime/，即"共享内存区"）：
  runtime/bus.jsonl            所有节点共享的事件流（追加写 + 文件锁）
  runtime/commands.jsonl       Web 控制台/外部工具写入的命令流
  runtime/status_<node>.json   每个节点一个状态槽（原子替换写）
  runtime/leader_lease.json    领导者租约（含 fencing epoch）
  runtime/fault_directives.json 故障注入指令
  runtime/logs/                各节点事件总线持久化日志（重放用）
================================================================================
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# ---- 保证无论从哪个目录启动，都能找到项目根下的包 ----
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from communication.event_bus import EventBus           # noqa: E402
from communication.message_types import (EventType, Message, Priority,  # noqa: E402
                                         Topics, make_message)
from core.function_block import FunctionBlock          # noqa: E402

logger = logging.getLogger("runtime")

# 共享内存通道的默认根目录（单机部署时所有节点共用）
RUNTIME_DIR = PROJECT_ROOT / "runtime"

# 默认配置文件路径
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "core" / "nodes.yaml"


# ==============================================================================
# 0. 日志工具
# ==============================================================================


def configure_logger(name: str, level: str = "INFO") -> None:
    """统一的控制台日志格式（各节点入口调用一次）。"""
    logging.basicConfig(
        level=getattr(logging, str(level).upper(), logging.INFO),
        format="%(asctime)s.%(msecs)03d [%(levelname)-7s] %(name)-18s | %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger(name)


# ==============================================================================
# 1. 跨平台文件锁（共享内存通道写入互斥）
# ==============================================================================


class FileLock:
    """
    跨平台进程互斥锁（Windows: msvcrt / POSIX: fcntl）。

    用于多个进程追加写同一个事件日志文件时的互斥；
    带超时与重试，避免死锁。
    """

    def __init__(self, path: str, timeout: float = 5.0) -> None:
        self.path = path
        self.timeout = timeout
        self._fd = None

    def __enter__(self) -> "FileLock":
        self._fd = open(self.path + ".lock", "a+")
        deadline = time.time() + self.timeout
        while True:
            try:
                if os.name == "nt":                       # Windows
                    import msvcrt
                    msvcrt.locking(self._fd.fileno(), msvcrt.LK_NBLCK, 1)
                else:                                     # Linux / macOS
                    import fcntl
                    fcntl.flock(self._fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except (OSError, PermissionError):
                if time.time() >= deadline:
                    self._fd.close()
                    raise TimeoutError("文件锁获取超时: %s" % self.path)
                time.sleep(0.002)                          # 2ms 重试间隔

    def __exit__(self, *exc_info) -> None:
        try:
            if os.name == "nt":
                import msvcrt
                self._fd.seek(0)
                msvcrt.locking(self._fd.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fd.fileno(), fcntl.LOCK_UN)
        finally:
            self._fd.close()
            self._fd = None


# ==============================================================================
# 2. 配置文件加载（yaml 优先，缺失时退化为内置 MiniYaml 解析器）
# ==============================================================================


def _parse_scalar(text: str) -> Any:
    """把 YAML 标量字符串转为 Python 值。"""
    text = text.strip()
    if text.startswith(("[", "{")):                     # 简单列表 [a, b]
        try:
            return json.loads(text.replace("'", '"'))
        except Exception:  # noqa: BLE001
            return [v.strip() for v in text.strip("[]").split(",") if v.strip()]
    if (text.startswith('"') and text.endswith('"')) or \
       (text.startswith("'") and text.endswith("'")):
        return text[1:-1]
    low = text.lower()
    if low in ("true", "yes", "on"):
        return True
    if low in ("false", "no", "off"):
        return False
    if low in ("null", "~", ""):
        return None
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text


def _strip_yaml_comment(line: str) -> str:
    """
    去除 YAML 行注释。

    规则与 YAML 规范一致：'#' 只有出现在行首或空白之后才是注释起点，
    因此 factory/orders/# 这类含 '#' 的主题字符串不会被误删。
    """
    for idx, ch in enumerate(line):
        if ch == "#" and (idx == 0 or line[idx - 1] in " \t"):
            return line[:idx].rstrip()
    return line.rstrip()


def _mini_yaml_load(text: str) -> Dict[str, Any]:
    """
    极简 YAML 子集解析器（无 PyYAML 环境的后备方案，递归下降实现）。

    支持：嵌套字典、列表项（- 标量 / - key: value 起头的字典项及其
    续行子块）、注释、引号字符串、内联数组 [a, b]。
    本项目的 nodes.yaml 严格落在该子集内。
    """
    # ---- 预处理：去注释/空行，得到 (缩进, 内容) 序列 ----
    lines: List[tuple] = []
    for raw in text.splitlines():
        line = _strip_yaml_comment(raw.rstrip())
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        lines.append((indent, line.strip()))
    if not lines:
        return {}

    def _parse_block(buf: List[tuple], idx: int, indent: int) -> tuple:
        """解析缓冲区 buf 中从 idx 开始、缩进为 indent 的块 -> (值, 下一行下标)。"""
        # ---------- 列表块 ----------
        if buf[idx][1].startswith("- "):
            items: List[Any] = []
            while idx < len(buf) and buf[idx][0] == indent \
                    and buf[idx][1].startswith("- "):
                first = buf[idx][1][2:].strip()          # 去掉 "- "
                idx += 1
                if ":" in first and first[0] not in "[{\"'":
                    # 字典项：首行转成 indent+2 的虚拟行，并收集更深缩进的续行
                    sub_block = [(indent + 2, first)]
                    while idx < len(buf) and buf[idx][0] > indent:
                        sub_block.append(buf[idx])
                        idx += 1
                    sub, _ = _parse_block(sub_block, 0, indent + 2)
                    items.append(sub)
                else:
                    items.append(_parse_scalar(first))
            return items, idx

        # ---------- 字典块 ----------
        result: Dict[str, Any] = {}
        while idx < len(buf) and buf[idx][0] == indent \
                and not buf[idx][1].startswith("- "):
            content = buf[idx][1]
            idx += 1
            key, sep, value = content.partition(":")
            key, value = key.strip(), value.strip()
            if not sep:
                continue
            if value:
                result[key] = _parse_scalar(value)
            else:
                # 'key:' 后跟随更深缩进的子块；否则值为空
                if idx < len(buf) and buf[idx][0] > indent:
                    result[key], idx = _parse_block(buf, idx, buf[idx][0])
                else:
                    result[key] = None
        return result, idx

    value, _ = _parse_block(lines, 0, lines[0][0])
    return value


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """
    加载 nodes.yaml 配置。

    优先使用 PyYAML；未安装时退化为 _mini_yaml_load（覆盖本项目配置子集）。
    """
    cfg_path = Path(path or DEFAULT_CONFIG_PATH)
    if not cfg_path.exists():
        logger.warning("配置文件不存在，使用内置默认配置: %s", cfg_path)
        return _default_config()
    text = cfg_path.read_text(encoding="utf-8")
    try:
        import yaml                                        # noqa: PLC0415
        data = yaml.safe_load(text)
        logger.info("配置已加载（PyYAML）: %s", cfg_path)
    except ImportError:
        data = _mini_yaml_load(text)
        logger.info("配置已加载（内置MiniYaml后备解析器）: %s", cfg_path)
    return data or _default_config()


def _default_config() -> Dict[str, Any]:
    """内置最小默认配置（找不到 nodes.yaml 时的兜底）。"""
    return {
        "system": {"name": "IEC61499-DICS", "runtime_dir": str(RUNTIME_DIR),
                   "log_level": "INFO"},
        "mqtt": {"enabled": True, "host": "127.0.0.1", "port": 1883},
        "defaults": {"heartbeat_interval_ms": 1000,
                     "heartbeat_timeout_ms": 3000, "sync_interval_ms": 500},
        "nodes": {},
    }


# ==============================================================================
# 3. 通道一：MQTT（paho-mqtt，可选依赖）
# ==============================================================================


class MQTTChannel:
    """
    MQTT 网络通道。

    - 发布：把 Message 序列化为 JSON 发到 msg.topic（QoS=1）；
    - 订阅：连接成功后自动订阅 join 时的过滤器列表，
      收到报文后回调 on_message(msg)；
    - paho-mqtt 未安装或 broker 不可达时自动进入"不可用"状态，
      系统依靠共享内存通道继续运行（双通道容错）。
    """

    def __init__(self, node_id: str, host: str, port: int,
                 on_message: Callable[[Message], None]) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.on_message = on_message
        self.available = False
        self._client = None
        self._subscriptions: List[str] = []

    def start(self, subscriptions: List[str]) -> None:
        """初始化并连接 broker（失败不抛异常，仅标记不可用）。"""
        try:
            import paho.mqtt.client as mqtt                 # noqa: PLC0415
        except ImportError:
            logger.warning("[%s] paho-mqtt 未安装，MQTT通道禁用（仅共享内存通道）",
                           self.node_id)
            return
        self._subscriptions = list(subscriptions)

        def _on_connect(client, userdata, flags, rc, *args):  # noqa: ANN001
            if rc == 0:
                self.available = True
                for pattern in self._subscriptions:
                    client.subscribe(pattern, qos=1)
                logger.info("[%s] MQTT已连接 %s:%d，订阅%d个主题",
                            self.node_id, self.host, self.port,
                            len(self._subscriptions))
            else:
                logger.warning("[%s] MQTT连接失败 rc=%s", self.node_id, rc)

        def _on_message(client, userdata, mqtt_msg):         # noqa: ANN001
            try:
                msg = Message.from_json(mqtt_msg.payload.decode("utf-8"))
                if msg.source != self.node_id:               # 忽略自己发出的回声
                    self.on_message(msg)
            except Exception as exc:  # noqa: BLE001
                logger.error("[%s] MQTT报文处理异常: %s", self.node_id, exc)

        self._client = mqtt.Client(client_id="%s-%d" % (self.node_id,
                                                        random.randint(1000, 9999)),
                                   callback_api_version=mqtt.CallbackAPIVersion.VERSION2
                                   if hasattr(mqtt, "CallbackAPIVersion") else None)
        self._client.on_connect = _on_connect
        self._client.on_message = _on_message
        try:
            self._client.connect_async(self.host, self.port, keepalive=30)
            self._client.loop_start()                        # 后台网络线程
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] MQTT连接异常（通道禁用）: %s", self.node_id, exc)

    def send(self, msg: Message) -> bool:
        """发布消息到 broker；通道不可用时返回 False。"""
        if not (self.available and self._client):
            return False
        try:
            info = self._client.publish(msg.topic, msg.to_json(), qos=1)
            return info.rc == 0
        except Exception as exc:  # noqa: BLE001
            logger.error("[%s] MQTT发布异常: %s", self.node_id, exc)
            return False

    def stop(self) -> None:
        """断开连接。"""
        if self._client:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:  # noqa: BLE001
                pass
            self.available = False


# ==============================================================================
# 4. 通道二：共享内存（共享目录事件日志，零第三方依赖）
# ==============================================================================


class SharedMemoryChannel:
    """
    共享内存通道（基于主机级共享目录实现）。

    单机多进程部署时，所有节点进程把事件追加写入 runtime/bus.jsonl，
    并由 watcher 线程 tail 该文件（含 Web 命令流 commands.jsonl），
    实现与 MQTT 等价的发布/订阅语义；
    跨主机部署时可将 runtime 目录放到共享存储，或直接依赖 MQTT 通道。

    写入互斥：FileLock；读取：记录每文件偏移量，增量解析新行。
    """

    BUS_FILE = "bus.jsonl"
    CMD_FILE = "commands.jsonl"

    def __init__(self, node_id: str, runtime_dir: Path,
                 on_message: Callable[[Message], None],
                 fault_provider: Optional[Callable[[], Dict[str, Any]]] = None) -> None:
        self.node_id = node_id
        self.runtime_dir = Path(runtime_dir)
        self.on_message = on_message
        self.fault_provider = fault_provider     # 注入故障指令查询函数
        self._offsets: Dict[str, int] = {}       # 每个被监听文件的读偏移
        self._stop_evt = threading.Event()
        self._watchers: List[threading.Thread] = []
        self.sent_count = 0

    # ------------------------------------------------------------------ 发送
    def send(self, msg: Message) -> bool:
        """
        把消息追加到共享事件流（bus.jsonl）。

        故障注入：若故障指令要求网络延迟/丢包，在此处模拟生效 ——
        这使得 fault_injector 无需接触节点进程即可施加网络故障。
        """
        directive = (self.fault_provider() or {}) if self.fault_provider else {}
        drop_rate = float(directive.get("drop_rate", 0.0))
        delay_ms = float(directive.get("delay_ms", 0.0))

        if drop_rate > 0 and random.random() < drop_rate:
            logger.warning("[%s] 故障注入：消息被丢弃 %s", self.node_id, msg.event_type)
            return False
        if delay_ms > 0:
            logger.warning("[%s] 故障注入：网络延迟 %.0fms", self.node_id, delay_ms)
            time.sleep(delay_ms / 1000.0)

        try:
            self.runtime_dir.mkdir(parents=True, exist_ok=True)
            path = str(self.runtime_dir / self.BUS_FILE)
            with FileLock(path):
                with open(path, "a", encoding="utf-8") as fp:
                    fp.write(msg.to_json() + "\n")
            self.sent_count += 1
            return True
        except OSError as exc:
            logger.error("[%s] 共享内存通道写入失败: %s", self.node_id, exc)
            return False

    # ------------------------------------------------------------------ 监听
    def start(self, watch_commands: bool = True) -> None:
        """启动 watcher 线程（tail 事件流与命令流）。"""
        targets = [self.BUS_FILE]
        if watch_commands:
            targets.append(self.CMD_FILE)
        for name in targets:
            t = threading.Thread(target=self._watch_loop, args=(name,),
                                 name="sm-watch-%s" % name, daemon=True)
            t.start()
            self._watchers.append(t)
        logger.info("[%s] 共享内存通道已启动 (dir=%s)", self.node_id, self.runtime_dir)

    def stop(self) -> None:
        """停止 watcher。"""
        self._stop_evt.set()
        for t in self._watchers:
            t.join(timeout=1.5)
        self._watchers.clear()

    def _watch_loop(self, filename: str) -> None:
        """
        tail 单个文件：增量读取完整行并回调（跳过自己发出的消息）。

        注意必须以二进制模式读取：偏移量按字节累计，若用文本模式，
        中文等多字节字符会造成"字符数当字节数"的错位，seek 落进
        多字节序列中间触发 UnicodeDecodeError 并杀死监视线程。
        """
        path = self.runtime_dir / filename
        while not self._stop_evt.is_set():
            try:
                if not path.exists():
                    time.sleep(0.05)
                    continue
                size = path.stat().st_size
                offset = self._offsets.get(filename, 0)
                if size < offset:                    # 文件被截断（重启清理）
                    offset = 0
                    self._offsets[filename] = 0
                if size == offset:
                    time.sleep(0.02)                # 20ms 轮询粒度
                    continue
                # 只消费到最后一个换行符为止：写入方正在追加的"半行"
                # 留在文件里下轮重读（读端不加锁，不能假设读到的一定是整行）
                with open(path, "rb") as fp:
                    fp.seek(offset)
                    data = fp.read()
                last_nl = data.rfind(b"\n")
                if last_nl < 0:
                    time.sleep(0.02)                # 尚无完整行：等写入方补齐
                    continue
                for raw in data[:last_nl].split(b"\n"):
                    line = raw.strip()
                    if not line:
                        continue
                    try:
                        msg = Message.from_json(line.decode("utf-8"))
                    except (ValueError, UnicodeDecodeError):
                        continue                # 损坏行：跳过，不阻塞后续
                    if msg.source != self.node_id:
                        self.on_message(msg)
                self._offsets[filename] = offset + last_nl + 1
            except OSError as exc:
                logger.error("[%s] watcher读取异常(%s): %s",
                             self.node_id, filename, exc)
                time.sleep(0.2)
            except Exception as exc:  # noqa: BLE001 监视线程绝不允许静默死亡
                logger.exception("[%s] watcher(%s)未预期异常: %s",
                                 self.node_id, filename, exc)
                time.sleep(0.2)

    # ------------------------------------------------------------------ 工具
    @staticmethod
    def read_journal(runtime_dir: Path, limit: int = 200) -> List[Message]:
        """读取共享事件流的最近 N 条消息（Web 控制台/复盘用）。"""
        path = Path(runtime_dir) / SharedMemoryChannel.BUS_FILE
        if not path.exists():
            return []
        from collections import deque
        lines: "deque" = deque(maxlen=max(1, limit))
        with open(path, "r", encoding="utf-8") as fp:
            for line in fp:
                if line.strip():
                    lines.append(line.strip())
        out: List[Message] = []
        for line in lines:
            try:
                out.append(Message.from_json(line))
            except ValueError:
                continue
        return out

    @staticmethod
    def write_command(runtime_dir: Path, msg: Message) -> None:
        """向命令流追加一条消息（Web 控制台/外部工具调用）。"""
        Path(runtime_dir).mkdir(parents=True, exist_ok=True)
        path = str(Path(runtime_dir) / SharedMemoryChannel.CMD_FILE)
        with FileLock(path):
            with open(path, "a", encoding="utf-8") as fp:
                fp.write(msg.to_json() + "\n")


# ==============================================================================
# 5. 状态槽与领导者租约（共享内存区上的小工具）
# ==============================================================================


def write_status(node_id: str, status: Dict[str, Any], runtime_dir: Path) -> None:
    """原子写本节点的状态槽（临时文件 + rename，读方永远不会看到半写）。"""
    Path(runtime_dir).mkdir(parents=True, exist_ok=True)
    tmp = Path(runtime_dir) / ("status_%s.json.tmp" % node_id)
    final = Path(runtime_dir) / ("status_%s.json" % node_id)
    tmp.write_text(json.dumps(status, ensure_ascii=False), encoding="utf-8")
    os.replace(str(tmp), str(final))              # 原子替换


def read_all_status(runtime_dir: Path) -> Dict[str, Dict[str, Any]]:
    """读取全部节点的状态槽（Web 控制台/健康检查用）。"""
    result: Dict[str, Dict[str, Any]] = {}
    if not Path(runtime_dir).exists():
        return result
    for f in sorted(Path(runtime_dir).glob("status_*.json")):
        try:
            result[f.stem.replace("status_", "")] = \
                json.loads(f.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
    return result


def try_acquire_leader(node_id: str, runtime_dir: Path,
                       ttl_ms: int = 3000, force: bool = False) -> Dict[str, Any]:
    """
    获取/续约领导者租约（热备切换的一致性基石）。

    - 正常获取：租约空闲或已过期时写入 {leader, epoch, expires_at}；
    - force=True：强制接管（epoch+1），供备用节点在主控失联时使用；
    - 返回最新租约内容。epoch 单调递增，作为 fencing token 附加在
      任务派发消息上，工作节点据此拒绝旧纪元的过期指令（防脑裂）。
    """
    Path(runtime_dir).mkdir(parents=True, exist_ok=True)
    path = Path(runtime_dir) / "leader_lease.json"
    lease: Dict[str, Any] = {}
    if path.exists():
        try:
            lease = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            lease = {}
    now = time.time() * 1000.0
    expired = lease.get("expires_at", 0) < now
    held_by_me = lease.get("leader") == node_id
    if force or expired or held_by_me or not lease:
        if not held_by_me or force:
            lease["epoch"] = int(lease.get("epoch", 0)) + 1
        lease["leader"] = node_id
        lease["acquired_at"] = now
        lease["expires_at"] = now + ttl_ms
        tmp = Path(runtime_dir) / "leader_lease.json.tmp"
        tmp.write_text(json.dumps(lease), encoding="utf-8")
        os.replace(str(tmp), str(path))
    return lease


def current_leader(runtime_dir: Path) -> Optional[str]:
    """查询当前有效的领导者（租约过期返回 None）。"""
    path = Path(runtime_dir) / "leader_lease.json"
    if not path.exists():
        return None
    try:
        lease = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return lease.get("leader") if lease.get("expires_at", 0) >= time.time() * 1000 else None


# ==============================================================================
# 6. 故障注入指令
# ==============================================================================


class FaultDirectives:
    """
    故障注入指令的读取端（节点侧）。

    simulator/fault_injector.py 写入 runtime/fault_directives.json：
      { "node_b": {"halted": true,  "delay_ms": 0,   "drop_rate": 0.0,
                    "until": 1755...} }
    本类由运行时周期轮询（500ms），指令过期自动失效。
    """

    def __init__(self, node_id: str, runtime_dir: Path) -> None:
        self.node_id = node_id
        self.path = Path(runtime_dir) / "fault_directives.json"
        self._current: Dict[str, Any] = {}

    def refresh(self) -> None:
        """重新加载指令文件并清理过期项。"""
        try:
            if self.path.exists():
                raw = json.loads(self.path.read_text(encoding="utf-8"))
                now = time.time() * 1000.0
                # 全局指令 "*" 与本节点指令叠加，过期即剔除
                active = {}
                for node, d in raw.items():
                    if node in ("*", self.node_id) and d.get("until", 0) >= now:
                        active.update({k: v for k, v in d.items() if k != "until"})
                self._current = active
            else:
                self._current = {}
        except (OSError, json.JSONDecodeError):
            self._current = {}

    def halted(self) -> bool:
        """本节点是否被注入"宕机"（心跳停止、状态槽标记故障）。"""
        return bool(self._current.get("halted", False))

    def as_dict(self) -> Dict[str, Any]:
        """供通道查询的网络类故障（延迟/丢包）。"""
        return {"delay_ms": self._current.get("delay_ms", 0),
                "drop_rate": self._current.get("drop_rate", 0.0)}


# ==============================================================================
# 7. 事件驱动调度引擎
# ==============================================================================


class SchedulerEngine:
    """
    事件驱动调度引擎：把"主题上的消息"翻译成"功能块的事件输入"。

    与传统 PLC 循环扫描的本质区别：
      - 没有 scan cycle：节点空闲时所有线程都阻塞在事件队列上，零CPU占用；
      - 每条消息按优先级插队执行，CRITICAL（故障切换）不被任务洪峰淹没；
      - 提供 dispatch 统计（每主题计数、平均时延）供 Web 图表展示。
    """

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self.bindings: List[Dict[str, Any]] = []       # 绑定表（诊断用）
        self.topic_counts: Dict[str, int] = {}         # 每主题消息计数
        self._latency_window: List[float] = []         # 端到端时延采样窗口

    def bind(self, topic_filter: str, fb: FunctionBlock, event_name: str,
             name: str = "") -> None:
        """
        绑定：主题过滤器 -> 功能块事件输入。

        消息到达匹配主题时，调度引擎调用 fb.handle_event(event_name, payload)，
        完成一次 IEC 61499 意义上的"事件触发执行"。
        """
        def _handler(msg: Message) -> None:
            self.topic_counts[msg.topic] = self.topic_counts.get(msg.topic, 0) + 1
            age = msg.age_ms()
            self._latency_window.append(age)
            if len(self._latency_window) > 500:
                del self._latency_window[:250]
            fb.handle_event(event_name, msg.payload)

        sub_id = self.bus.subscribe(topic_filter, _handler,
                                    name=name or "%s.%s" % (fb.name, event_name))
        self.bindings.append({"topic": topic_filter, "fb": fb.name,
                              "event": event_name, "sub_id": sub_id})

    def stats(self) -> Dict[str, Any]:
        """调度统计快照。"""
        lat = self._latency_window
        return {
            "bindings": len(self.bindings),
            "topic_counts": dict(self.topic_counts),
            "avg_latency_ms": round(sum(lat) / len(lat), 2) if lat else 0.0,
            "max_latency_ms": round(max(lat), 2) if lat else 0.0,
        }


# ==============================================================================
# 8. 分布式运行时主体
# ==============================================================================


class DistributedRuntime:
    """
    节点级分布式运行时（等价于 IEC 61499 的设备 Device + 资源 Resource）。

    组装关系：
      DistributedRuntime
        ├── EventBus             进程内事件总线（优先级调度核心）
        ├── SchedulerEngine      主题 -> FB事件 的绑定调度
        ├── MQTTChannel          跨节点通道一（网络）
        ├── SharedMemoryChannel  跨节点通道二（主机共享目录）
        ├── FaultDirectives      故障注入指令
        └── 功能块实例集合        由配置实例化或代码注册
    """

    def __init__(self, node_id: str,
                 config_path: Optional[str] = None,
                 role: Optional[str] = None) -> None:
        self.node_id = node_id
        self.config = load_config(config_path)
        sys_cfg = self.config.get("system", {})
        self.runtime_dir = Path(sys_cfg.get("runtime_dir", str(RUNTIME_DIR)))
        if not self.runtime_dir.is_absolute():
            self.runtime_dir = PROJECT_ROOT / self.runtime_dir
        self.log_dir = self.runtime_dir / "logs"

        # ---- 本节点的配置段 ----
        node_cfg = (self.config.get("nodes", {}) or {}).get(node_id, {})
        self.role = role or node_cfg.get("role", "worker")
        self.description = node_cfg.get("description", "")
        self.ip = node_cfg.get("ip", "127.0.0.1")
        # ---- 集群缺省参数（允许被节点级配置覆盖，见 nodes.yaml "可被节点级覆盖"）----
        defaults = self.config.get("defaults", {})

        def _cfg_ms(key: str, fallback: float) -> float:
            """先取节点级覆盖值，缺省回落到集群默认，最后落到内置兜底。"""
            return float(node_cfg.get(key, defaults.get(key, fallback)))

        self.heartbeat_interval = _cfg_ms("heartbeat_interval_ms", 1000) / 1000.0
        self.heartbeat_timeout = _cfg_ms("heartbeat_timeout_ms", 3000) / 1000.0
        self.sync_interval = _cfg_ms("sync_interval_ms", 500) / 1000.0

        # ---- 核心组件 ----
        self.bus = EventBus(node_id, workers=3,
                            persistence_dir=str(self.log_dir))
        self.scheduler = SchedulerEngine(self.bus)
        self.faults = FaultDirectives(node_id, self.runtime_dir)
        self._fbs: Dict[str, FunctionBlock] = {}

        # ---- 通道（on_message 统一走 _on_external_message 做去重入总线）----
        self._seen_external: set = set()
        self._seen_lock = threading.Lock()
        mqtt_cfg = self.config.get("mqtt", {})
        self.mqtt = MQTTChannel(node_id,
                                mqtt_cfg.get("host", "127.0.0.1"),
                                int(mqtt_cfg.get("port", 1883)),
                                on_message=self._on_external_message)
        self.shm = SharedMemoryChannel(node_id, self.runtime_dir,
                                       on_message=self._on_external_message,
                                       fault_provider=self.faults.as_dict)

        # ---- 生命周期 ----
        self._stop_evt = threading.Event()
        self._threads: List[threading.Thread] = []
        self._running = False
        self.epoch = 0                               # 当前领导纪元（fencing）

    # ==========================================================================
    # 8.1 功能块管理
    # ==========================================================================

    def register_fb(self, fb: FunctionBlock) -> FunctionBlock:
        """
        注册功能块实例：注入输出事件回调（FB 输出事件 -> 运行时路由）。
        """
        fb._emit_callback = self._on_fb_event
        self._fbs[fb.name] = fb
        logger.info("[%s] 注册功能块 %s (%s)，ECC状态=%s",
                    self.node_id, fb.name, fb.fb_type,
                    fb.ecc.current_state if fb.ecc else "-")
        return fb

    def get_fb(self, name: str) -> Optional[FunctionBlock]:
        """按实例名获取功能块。"""
        return self._fbs.get(name)

    @property
    def fbs(self) -> Dict[str, FunctionBlock]:
        """全部功能块实例。"""
        return dict(self._fbs)

    def autoload_fbs(self, registry: Dict[str, type]) -> List[FunctionBlock]:
        """
        按配置自动实例化本节点的功能块清单。

        参数 registry：{类型名: 类对象}，由节点入口脚本提供。
        配置示例：
          function_blocks:
            - type: OrderManagerFB
              name: order_manager
              params: {max_queue: 100}
        """
        node_cfg = (self.config.get("nodes", {}) or {}).get(self.node_id, {})
        created: List[FunctionBlock] = []
        for spec in node_cfg.get("function_blocks", []) or []:
            fb_type = spec.get("type", "")
            fb_name = spec.get("name", fb_type)
            params = spec.get("params", {}) or {}
            cls = registry.get(fb_type)
            if cls is None:
                logger.error("[%s] 配置引用了未注册的功能块类型 %s，已跳过",
                             self.node_id, fb_type)
                continue
            created.append(self.register_fb(cls(fb_name, params)))
        return created

    # ==========================================================================
    # 8.2 事件绑定（输入）与路由（输出）
    # ==========================================================================

    def bind_input(self, topic_filter: str, fb: FunctionBlock,
                   event_name: str, name: str = "") -> None:
        """订阅主题 -> 触发功能块事件输入（见 SchedulerEngine.bind）。"""
        self.scheduler.bind(topic_filter, fb, event_name, name)

    def route_output(self, fb: FunctionBlock, event_name: str,
                     topic: Any, event_type: Any = None,
                     priority: Any = Priority.NORMAL,
                     scope: str = "global") -> None:
        """
        为功能块输出事件配置路由：事件输出 -> 主题。

        参数 topic 可以是：
          - 字符串：固定主题（可用 {node} 等占位符，按 payload 填充）；
          - callable(data) -> str：按随行数据动态计算主题（如按目标节点路由）。
        同一事件输出可注册多条路由（全部触发）；不同事件互不干扰。
        """
        if not hasattr(self, "_routings"):
            # 路由表：(FB实例名, 事件输出名) -> [路由规格列表]
            self._routings: Dict[tuple, List[Dict[str, Any]]] = {}
        self._routings.setdefault((fb.name, event_name), []).append({
            "topic": topic, "event_type": event_type,
            "priority": priority, "scope": scope,
        })
        # 统一分发器（幂等：重复设置无副作用，且严格按事件名匹配路由）
        fb._emit_callback = self._dispatch_fb_event

    def _dispatch_fb_event(self, fb: FunctionBlock, event_name: str,
                           data: Optional[Dict]) -> None:
        """
        FB 输出事件统一分发器：只触发 (fb.name, event_name) 精确匹配的路由，
        保证不同事件输出的路由互不串扰。
        """
        routes = getattr(self, "_routings", {}).get((fb.name, event_name))
        if not routes:
            logger.debug("[%s] FB输出事件 %s.%s（未配置路由）data=%s",
                         self.node_id, fb.name, event_name, data)
            return
        for route in routes:
            topic_spec, event_type = route["topic"], route["event_type"]
            try:
                resolved = (topic_spec(data) if callable(topic_spec)
                            else topic_spec.format(**{
                                **{k: v for k, v in (data or {}).items()
                                   if isinstance(v, (str, int, float))},
                                "node": self.node_id}))
            except (KeyError, IndexError, ValueError):
                resolved = topic_spec if isinstance(topic_spec, str) else Topics.EVENTS
            et = event_type or event_name.upper()
            msg = make_message(et, self.node_id, resolved, data or {},
                               priority=route["priority"], scope=route["scope"],
                               epoch=self.epoch)
            self.bus.publish(msg)

    def _on_fb_event(self, fb: FunctionBlock, event_name: str,
                     data: Optional[Dict]) -> None:
        """缺省 FB 输出处理：仅记录调试日志（未被 route_output 覆盖时）。"""
        logger.debug("[%s] FB输出事件 %s.%s data=%s",
                     self.node_id, fb.name, event_name, data)

    # ==========================================================================
    # 8.3 通信：发布 / 外部消息入口
    # ==========================================================================

    def publish(self, topic: str, event_type: Any, payload: Dict[str, Any],
                target: str = "*", priority: Any = Priority.NORMAL,
                scope: str = "global") -> Message:
        """运行时级发布接口（节点业务代码直接调用）。"""
        msg = make_message(event_type, self.node_id, topic, payload,
                           target=target, priority=priority, scope=scope,
                           epoch=self.epoch)
        self.bus.publish(msg)
        return msg

    def _bridge_to_channels(self, msg: Message) -> None:
        """总线出站桥：global 消息双通道并发外发。"""
        if self.mqtt.available:
            self.mqtt.send(msg)
        self.shm.send(msg)

    def _on_external_message(self, msg: Message) -> None:
        """外部通道入口：msg_id 去重后注入本地总线。"""
        with self._seen_lock:
            if msg.msg_id in self._seen_external:
                return
            self._seen_external.add(msg.msg_id)
            if len(self._seen_external) > 2048:       # 防止窗口无限增长
                self._seen_external = set(list(self._seen_external)[-1024:])
        # fencing：忽略旧纪元的任务派发（防脑裂的接收侧防线）
        if msg.epoch and msg.event_type == EventType.TASK_DISPATCHED.value \
                and msg.epoch < self.epoch:
            logger.warning("[%s] 拒绝旧纪元(epoch=%d)指令", self.node_id, msg.epoch)
            return
        self.bus.publish_local(msg, dedup=True)

    # ==========================================================================
    # 8.4 生命周期：启动 / 停止 / 心跳 / 状态槽
    # ==========================================================================

    def start(self) -> None:
        """启动运行时：总线、双通道、心跳线程、故障指令轮询。"""
        if self._running:
            return
        self._running = True
        self._stop_evt.clear()

        self.bus.start()
        self.bus.add_outbound_bridge(self._bridge_to_channels)

        node_cfg = (self.config.get("nodes", {}) or {}).get(self.node_id, {})
        subscriptions = list(node_cfg.get("subscribe_topics", []) or []) \
            or [Topics.ALL]
        if self.config.get("mqtt", {}).get("enabled", True):
            self.mqtt.start(subscriptions)
        self.shm.start(watch_commands=True)

        # ---- 功能块参数在线配置入口：factory/config/<本节点> ----
        def _on_config_msg(msg: Message) -> None:
            if msg.event_type != EventType.FB_CONFIG_UPDATED.value:
                return
            fb_name = str(msg.payload.get("fb", ""))
            params = dict(msg.payload.get("params") or {})
            try:
                applied = self.apply_fb_config(fb_name, params)
                logger.info("[%s] 在线配置生效 fb=%s params=%s",
                            self.node_id, fb_name, applied)
                # 回执广播（Web 控制台据此展示生效结果）
                self.publish(Topics.EVENTS, EventType.FB_CONFIG_UPDATED,
                             {"node": self.node_id, "fb": fb_name,
                              "applied": applied, "by": msg.source})
            except KeyError as exc:
                logger.warning("[%s] 在线配置失败: %s", self.node_id, exc)
        self.bus.subscribe(Topics.config_of(self.node_id), _on_config_msg,
                           name="fb-config-input")

        t1 = threading.Thread(target=self._heartbeat_loop,
                              name="hb", daemon=True)
        t2 = threading.Thread(target=self._fault_poll_loop,
                              name="fault-poll", daemon=True)
        t1.start(); t2.start()
        self._threads.extend([t1, t2])
        logger.info("[%s] 运行时已启动 role=%s ip=%s 心跳间隔=%.0fms",
                    self.node_id, self.role, self.ip,
                    self.heartbeat_interval * 1000)

    def stop(self) -> None:
        """优雅停止：写最终状态槽，断开通道，停总线。"""
        if not self._running:
            return
        self._running = False
        self._stop_evt.set()
        for t in self._threads:
            t.join(timeout=2.0)
        self._threads.clear()
        self.mqtt.stop()
        self.shm.stop()
        self.bus.stop()
        write_status(self.node_id, self.snapshot_status(state="stopped"),
                     self.runtime_dir)
        logger.info("[%s] 运行时已停止", self.node_id)

    def _heartbeat_loop(self) -> None:
        """心跳线程：周期发布心跳消息 + 原子更新状态槽（宕机注入时静默）。"""
        while not self._stop_evt.is_set():
            if not self.faults.halted():
                self.publish(Topics.heartbeat_of(self.node_id),
                             EventType.HEARTBEAT,
                             {"node": self.node_id, "role": self.role,
                              "fb_count": len(self._fbs),
                              "leader_epoch": self.epoch},
                             priority=Priority.HIGH)
                write_status(self.node_id, self.snapshot_status(),
                             self.runtime_dir)
            else:
                # 故障注入"宕机"：写一个显式 halted 状态让 UI 立刻变红
                snap = self.snapshot_status(state="halted")
                snap["fault"] = "injected-down"
                write_status(self.node_id, snap, self.runtime_dir)
            self._stop_evt.wait(self.heartbeat_interval)

    def _fault_poll_loop(self) -> None:
        """故障指令轮询线程（500ms 刷新一次注入指令）。"""
        while not self._stop_evt.is_set():
            self.faults.refresh()
            self._stop_evt.wait(0.5)

    # ==========================================================================
    # 8.5 状态快照（心跳/状态槽/Web 控制台共用）
    # ==========================================================================

    def snapshot_status(self, state: str = "running") -> Dict[str, Any]:
        """生成节点状态快照（含功能块参数，供 Web 在线配置面板展示）。"""
        return {
            "node": self.node_id,
            "role": self.role,
            "ip": self.ip,
            "state": state,
            "ts": round(time.time(), 3),
            "epoch": self.epoch,
            "fb_count": len(self._fbs),
            "fbs": [fb.snapshot() for fb in self._fbs.values()],
            "scheduler": self.scheduler.stats(),
            "bus": self.bus.stats_snapshot(),
        }

    def apply_fb_config(self, fb_name: str, params: Dict[str, Any]) -> List[str]:
        """在线修改功能块参数（配置消息处理入口）。"""
        fb = self._fbs.get(fb_name)
        if fb is None:
            raise KeyError("功能块不存在: %s" % fb_name)
        return fb.configure(params)


# ==============================================================================
# 9. 演示入口：python distributed_runtime.py --node demo
# ==============================================================================


def _build_echo_class() -> type:
    """动态组装一个带端口的回显功能块类（演示端口声明规范）。"""
    from core.function_block import DataPort, EventPort  # noqa: PLC0415

    class _Echo(FunctionBlock):
        EVENT_INPUTS = [EventPort("REQ", with_inputs=["text"], comment="请求")]
        EVENT_OUTPUTS = [EventPort("CNF", with_outputs=["echo"], comment="确认")]
        DATA_INPUTS = {"text": DataPort("text", "STRING", "", "回显内容")}
        DATA_OUTPUTS = {"echo": DataPort("echo", "STRING", "", "回显结果")}

        def execute(self, event_name: str) -> None:
            self.do["echo"] = "echo:%s" % self.di["text"]
            self.emit("CNF", {"echo": self.do["echo"]})

    return _Echo


def main() -> None:
    """最小演示：启动 demo 节点 10 秒，自发自收一条消息。"""
    parser = argparse.ArgumentParser(description="分布式运行时演示")
    parser.add_argument("--node", default="demo", help="节点ID")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    args = parser.parse_args()

    configure_logger("demo")
    rt = DistributedRuntime(args.node, config_path=args.config, role="demo")
    echo = rt.register_fb(_build_echo_class()("echo_1", {"text": "hello"}))
    rt.bind_input("factory/demo/in", echo, "REQ")   # 精确匹配，避免回显主题自触发环路
    rt.route_output(echo, "CNF", "factory/demo/reply", "EchoReply")
    rt.start()

    received: List[Message] = []
    rt.bus.subscribe("factory/demo/reply", lambda m: received.append(m), "demo-reply")

    for i in range(3):                              # 事件驱动：只发事件，不轮询
        rt.publish("factory/demo/in", "EchoRequest",
                   {"text": "ping-%d" % i}, priority=Priority.NORMAL)
        time.sleep(0.3)

    time.sleep(1.0)
    rt.stop()
    print("演示完成，收到回显 %d 条：%s"
          % (len(received), [m.payload.get("echo") for m in received]))


if __name__ == "__main__":
    main()
