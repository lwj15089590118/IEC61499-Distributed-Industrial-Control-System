# -*- coding: utf-8 -*-
"""
================================================================================
 simulator/fault_injector.py —— 故障注入器（混沌工程工具）
================================================================================
职责：向运行中的集群注入可控故障，验证冗余切换与系统韧性。

支持的三类故障（写入 runtime/fault_directives.json，各节点 500ms 内生效）：
  1. 节点宕机（down）  ：目标节点停止心跳并把自己标记为 halted；
  2. 网络延迟（delay） ：目标节点发出的消息被延迟 N 毫秒；
  3. 消息丢失（loss）  ：目标节点发出的消息按概率 p 随机丢弃。

用法示例：
  python simulator/fault_injector.py list                  # 查看集群状态与指令
  python simulator/fault_injector.py down node_a --sec 20  # 主控宕机20s（触发热备）
  python simulator/fault_injector.py delay node_b 500      # 机器人节点延迟500ms
  python simulator/fault_injector.py loss node_d 0.3       # 视觉节点丢包30%
  python simulator/fault_injector.py clear                 # 清除全部故障
================================================================================
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.distributed_runtime import RUNTIME_DIR, read_all_status  # noqa: E402

DIRECTIVES_PATH = RUNTIME_DIR / "fault_directives.json"

# 合法的注入目标节点（防拼写错误）
VALID_NODES = {"node_a", "node_b", "node_c", "node_d", "node_e"}


# ==============================================================================
# 1. 指令文件读写
# ==============================================================================


def load_directives() -> Dict:
    """读取当前故障指令表（无文件返回空表）。"""
    if not DIRECTIVES_PATH.exists():
        return {}
    try:
        return json.loads(DIRECTIVES_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_directives(table: Dict) -> None:
    """原子写入故障指令表。"""
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    tmp = DIRECTIVES_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(table, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    tmp.replace(DIRECTIVES_PATH)


def set_directive(node: str, **fields) -> Dict:
    """设置/合并某节点的故障指令（自动附加过期时间，默认 30s）。"""
    table = load_directives()
    entry = table.get(node, {})
    entry.update(fields)
    entry["until"] = time.time() * 1000.0 + entry.get("duration_ms", 30000.0)
    table[node] = entry
    save_directives(table)
    return entry


def clear_directives(node: Optional[str] = None) -> None:
    """清除指定节点（或全部）的故障指令。"""
    if node is None:
        save_directives({})
    else:
        table = load_directives()
        table.pop(node, None)
        save_directives(table)


# ==============================================================================
# 2. 子命令实现
# ==============================================================================


def cmd_list(args: argparse.Namespace) -> None:
    """列出集群节点存活状态与当前生效的故障指令。"""
    now_ms = time.time() * 1000.0
    statuses = read_all_status(RUNTIME_DIR)
    print("=" * 66)
    print(" 集群节点状态（共享内存状态槽实时读取）")
    print("=" * 66)
    if not statuses:
        print(" （空）尚无节点写入状态槽 —— 请先启动各节点进程。")
    for node, st in sorted(statuses.items()):
        age = now_ms - float(st.get("ts", 0)) * 1000.0
        state = st.get("state", "?")
        # 状态槽超过 3 秒未刷新 => 离线；state=halted => 注入宕机
        if state == "halted":
            badge = "[故障-注入宕机]"
        elif age > 3000:
            badge = "[离线-心跳超时]"
        else:
            badge = "[在线]"
        print(" %-8s %-16s role=%-12s epoch=%-3s fb=%-2s %s"
              % (node, badge, st.get("role", "?"), st.get("epoch", 0),
                 st.get("fb_count", 0), "心跳 %.1fs 前" % (age / 1000.0)))

    table = load_directives()
    print("-" * 66)
    print(" 当前故障指令（%s）" % DIRECTIVES_PATH)
    if not table:
        print(" （无）")
    for node, d in table.items():
        remain = (d.get("until", 0) - now_ms) / 1000.0
        print(" %-8s halted=%-5s delay=%-4sms loss=%-4s 剩余%.0fs"
              % (node, d.get("halted", False), d.get("delay_ms", 0),
                 d.get("drop_rate", 0.0), max(0.0, remain)))
    print("=" * 66)


def cmd_down(args: argparse.Namespace) -> None:
    """注入节点宕机：该节点心跳停止、状态槽标记 halted。"""
    if args.node not in VALID_NODES:
        sys.exit("非法节点名 %s，可选：%s" % (args.node, sorted(VALID_NODES)))
    duration_ms = args.sec * 1000.0
    entry = set_directive(args.node, halted=True, duration_ms=duration_ms)
    print(">> 已注入 [宕机] %s，持续 %.0f 秒（%.0fms 后自动恢复）"
          % (args.node, args.sec, entry["until"] - time.time() * 1000.0))
    if args.node == "node_a":
        print(">> 提示：主控宕机将触发 node_e 热备接管（观察 factory/failover）")
    print(">> 观察方式：python simulator/fault_injector.py list  或  Web控制台")


def cmd_delay(args: argparse.Namespace) -> None:
    """注入网络延迟：该节点外发消息延迟 N 毫秒。"""
    if args.node not in VALID_NODES:
        sys.exit("非法节点名 %s" % args.node)
    set_directive(args.node, delay_ms=int(args.ms),
                  duration_ms=args.sec * 1000.0)
    print(">> 已注入 [网络延迟] %s = %dms，持续 %.0f 秒"
          % (args.node, args.ms, args.sec))


def cmd_loss(args: argparse.Namespace) -> None:
    """注入消息丢失：该节点外发消息按概率丢弃。"""
    if args.node not in VALID_NODES:
        sys.exit("非法节点名 %s" % args.node)
    if not 0.0 <= args.rate <= 1.0:
        sys.exit("丢失率必须在 [0,1] 区间")
    set_directive(args.node, drop_rate=float(args.rate),
                  duration_ms=args.sec * 1000.0)
    print(">> 已注入 [消息丢失] %s = %.0f%%，持续 %.0f 秒"
          % (args.node, args.rate * 100, args.sec))


def cmd_clear(args: argparse.Namespace) -> None:
    """清除故障指令（节点恢复正常通信与心跳）。"""
    clear_directives(args.node if args.node else None)
    print(">> 已清除故障指令：%s" % (args.node or "全部节点"))


# ==============================================================================
# 3. 命令行入口
# ==============================================================================


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="故障注入器：模拟节点宕机/网络延迟/消息丢失，测试冗余切换")
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="查看集群状态与生效中的故障指令")
    p_list.set_defaults(func=cmd_list)

    p_down = sub.add_parser("down", help="注入节点宕机（心跳停止）")
    p_down.add_argument("node", help="目标节点，如 node_a")
    p_down.add_argument("--sec", type=float, default=30.0,
                        help="持续秒数（默认30，超时自动恢复）")
    p_down.set_defaults(func=cmd_down)

    p_delay = sub.add_parser("delay", help="注入网络延迟(ms)")
    p_delay.add_argument("node", help="目标节点")
    p_delay.add_argument("ms", type=int, help="延迟毫秒数")
    p_delay.add_argument("--sec", type=float, default=30.0, help="持续秒数")
    p_delay.set_defaults(func=cmd_delay)

    p_loss = sub.add_parser("loss", help="注入消息丢失(0~1概率)")
    p_loss.add_argument("node", help="目标节点")
    p_loss.add_argument("rate", type=float, help="丢失率，如 0.3 表示30%%")
    p_loss.add_argument("--sec", type=float, default=30.0, help="持续秒数")
    p_loss.set_defaults(func=cmd_loss)

    p_clear = sub.add_parser("clear", help="清除故障指令")
    p_clear.add_argument("node", nargs="?", default=None,
                         help="指定节点；缺省清除全部")
    p_clear.set_defaults(func=cmd_clear)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
