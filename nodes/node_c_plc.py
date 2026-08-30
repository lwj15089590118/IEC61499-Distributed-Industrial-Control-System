# -*- coding: utf-8 -*-
"""
================================================================================
 nodes/node_c_plc.py —— 节点C：PLC节点（可编程逻辑控制器模拟）
================================================================================
职责（模拟传统PLC在分布式架构中的IO控制角色）：
  1. 传送带控制：软启动斜坡 + 惰行停车 + 位置推算（事件驱动惰性求值）；
  2. 气缸伸缩控制：双磁性开关到位检测 + 防抖 + 循环完成事件；
  3. 伺服电机定位：梯形速度曲线 + 到位判定（定位容差内报 IN_POSITION）。

设计说明：
  - 与传统PLC"每个扫描周期刷新IO"不同，本节点的物理量采用
    "事件到来时按物理方程推算"的惰性求值方式（无轮询循环）；
  - 动作完成通过内部定时事件注入（等价于PLC的定时器/中断组织块），
    全程维持 IEC 61499 的事件驱动语义。
================================================================================
"""

from __future__ import annotations

import logging
import math
import random
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from communication.message_types import EventType, Topics  # noqa: E402
from core.distributed_runtime import (DistributedRuntime, apply_connections,  # noqa: E402
                                      configure_logger)
from core.function_block import (DataPort, ECC, ECCState, ECCTransition,  # noqa: E402
                                 EventPort, FunctionBlock, QueuedExecutionFB)

logger = logging.getLogger("node_c")

# 节点内部互连主题
T_CONVEYOR = "node_c/internal/conveyor"
T_CYL_EXT = "node_c/internal/cyl_extend"
T_CYL_RET = "node_c/internal/cyl_retract"
T_SERVO = "node_c/internal/servo"


# ==============================================================================
# 1. 任务路由功能块
# ==============================================================================


class TaskRouterFB(FunctionBlock):
    """PLC节点任务路由：按 action 把任务分发到传送带/气缸/伺服功能块。"""

    EVENT_INPUTS = [
        EventPort("TASK", with_inputs=["task_id", "action", "params"],
                  comment="派发任务到达"),
    ]
    EVENT_OUTPUTS = [
        EventPort("CONVEYOR_REQ", with_outputs=["task_id", "duration", "speed"],
                  comment="传送带运转请求"),
        EventPort("CYL_EXT_REQ", with_outputs=["task_id"],
                  comment="气缸伸出请求"),
        EventPort("CYL_RET_REQ", with_outputs=["task_id"],
                  comment="气缸缩回请求"),
        EventPort("SERVO_REQ", with_outputs=["task_id", "target_mm"],
                  comment="伺服定位请求"),
        EventPort("UNKNOWN", with_outputs=["task_id", "action"], comment="未知动作"),
    ]
    DATA_INPUTS = {
        "task_id": DataPort("task_id", "STRING", "", "任务ID"),
        "action": DataPort("action", "STRING", "", "任务动作"),
        "params": DataPort("params", "ANY", {}, "任务参数"),
    }
    DATA_OUTPUTS = {}

    ACTION_MAP = {
        "CONVEYOR_RUN": "CONVEYOR_REQ",
        "CYLINDER_EXTEND": "CYL_EXT_REQ",
        "CYLINDER_RETRACT": "CYL_RET_REQ",
        "SERVO_MOVE": "SERVO_REQ",
    }

    def execute(self, event_name: str) -> None:
        action = str(self.di["action"])
        task_id = str(self.di["task_id"])
        params: Dict = self.di["params"] or {}
        evt = self.ACTION_MAP.get(action)
        if evt is None:
            self.emit("UNKNOWN", {"task_id": task_id, "action": action})
            return
        base = {"task_id": task_id, "order_id": self.state.get("_ext_order_id", "")}
        if evt == "CONVEYOR_REQ":
            base.update({"duration": params.get("duration_s", 3.0),
                         "speed": params.get("speed", None)})
        elif evt == "SERVO_REQ":
            base.update({"target_mm": params.get("target_mm", 120.0)})
        self.emit(evt, base)


# ==============================================================================
# 2. 传送带控制功能块
# ==============================================================================


class ConveyorControlFB(QueuedExecutionFB):
    """
    传送带控制功能块。

    物理模型（事件驱动惰性求值）：
      - RUN 事件启动皮带：记录启动时刻与速度（经软启动斜坡折算等效平均速度）；
      - 任意时刻查询位置：pos = last_pos + v_eff * elapsed（换向时重算基准）；
      - 停止/到位由定时线程注入 STOPPED 内部事件（等价定时器中断）。

    容量控制（WIP=1）：单条皮带，运转中的并发 RUN 请求进入等待队列。
    """

    BUSY_TRIGGER_EVENTS = ("RUN",)

    EVENT_INPUTS = [
        EventPort("RUN", with_inputs=["task_id", "duration", "speed"],
                  comment="运转请求"),
        EventPort("STOP", comment="立即停车"),
        EventPort("STOPPED", comment="[内部] 定时运转结束"),
        EventPort("STATUS_REQ", comment="状态查询"),
    ]
    EVENT_OUTPUTS = [
        EventPort("CONVEYOR_STATUS", with_outputs=["running", "position",
                                                   "speed"], comment="皮带状态"),
        EventPort("TASK_COMPLETED", with_outputs=["task_id", "action"],
                  comment="任务完成"),
    ]
    DATA_INPUTS = {
        "task_id": DataPort("task_id", "STRING", "", "任务ID"),
        "duration": DataPort("duration", "REAL", 3.0, "运转时长(s)"),
        "speed": DataPort("speed", "REAL", 0.0, "目标速度(0=额定, m/s)"),
        "rated_speed": DataPort("rated_speed", "REAL", 0.5, "额定速度m/s"),
        "length_m": DataPort("length_m", "REAL", 3.0, "皮带长度m"),
        "soft_start_s": DataPort("soft_start_s", "REAL", 0.8, "软启动时间"),
    }
    DATA_OUTPUTS = {
        "running": DataPort("running", "BOOL", False, "运转中"),
        "position": DataPort("position", "REAL", 0.0, "当前位置m"),
    }

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        self.state["_base_pos"] = 0.0            # 速度换向基准位置
        self.state["_base_ts"] = 0.0             # 基准时刻
        self.state["_cur_speed"] = 0.0           # 当前有效速度
        self.state["_run_task"] = ""
        self.state["total_m"] = 0.0              # 累计输送里程

    def build_ecc(self) -> ECC:
        """ECC：Stopped -> Running(定时停止) -> Stopped。"""
        ecc = ECC(initial_state="Stopped")
        ecc.add_state(ECCState("Stopped", entry_actions=["action_stop"],
                               comment="停止"))
        ecc.add_state(ECCState("Running", entry_actions=["action_run"],
                               comment="运转"))
        ecc.add_transition(ECCTransition("Stopped", "Running", event="RUN"))
        ecc.add_transition(ECCTransition("Running", "Stopped", event="STOP"))
        ecc.add_transition(ECCTransition("Running", "Stopped", event="STOPPED"))
        return ecc

    # ------------------------------------------------------------ 物理
    def _effective_speed(self, target: float) -> float:
        """软启动折算：把斜坡过程等效为 0.9 倍目标速度的匀速近似。"""
        rated = float(self.di["rated_speed"])
        v = target if target and target > 0 else rated
        return min(v, rated * 1.2) * 0.9

    def _position_now(self) -> float:
        """惰性求值当前位置（不上报不计算，零CPU占用）。"""
        if self.state["_cur_speed"] <= 0:
            return self.state["_base_pos"]
        dt = time.time() - self.state["_base_ts"]
        pos = self.state["_base_pos"] + self.state["_cur_speed"] * dt
        return min(pos, float(self.di["length_m"]))       # 到皮带末端封顶

    # ------------------------------------------------------------ 动作
    def action_run(self) -> None:
        """Running entry：设定速度基准并启动定时停止线程。"""
        task = str(self.di["task_id"] or self.state.get("_ext_task_id", "CONV"))
        duration = float(self.di["duration"] or 3.0)
        v = self._effective_speed(float(self.di["speed"] or 0.0))
        # 记录换向基准（先结算历史位置）
        self.state["_base_pos"] = self._position_now()
        self.state["_base_ts"] = time.time()
        self.state["_cur_speed"] = v
        self.state["_run_task"] = task
        self.do["running"] = True
        logger.info("传送带启动：任务%s 速度%.2fm/s 时长%.1fs", task, v, duration)

        def _timer() -> None:
            time.sleep(max(0.2, duration))
            self.handle_event("STOPPED")                   # 定时器事件注入

        threading.Thread(target=_timer, daemon=True, name="conv-timer").start()

    def action_stop(self) -> None:
        """Stopped entry：结算位置、上报状态；若是任务运转则报完成。"""
        final_pos = self._position_now()
        self.state["total_m"] += final_pos - self.state["_base_pos"]
        self.state["_cur_speed"] = 0.0
        self.do["running"] = False
        self.do["position"] = round(final_pos, 3)
        self.emit("CONVEYOR_STATUS", {"running": False,
                                      "position": round(final_pos, 3),
                                      "speed": 0.0})
        task = self.state.get("_run_task", "")
        if task:
            self.state["_run_task"] = ""
            logger.info("传送带停止于 %.2fm（累计输送 %.1fm）",
                        final_pos, self.state["total_m"])
            self.emit("TASK_COMPLETED", {"task_id": task, "action": "CONVEYOR_RUN",
                                         "node": "node_c", "position": final_pos})

    def execute(self, event_name: str) -> None:
        """过程式补充：STATUS_REQ 查询（演示惰性求值）。"""
        if event_name == "STATUS_REQ":
            self.emit("CONVEYOR_STATUS", {
                "running": bool(self.do["running"]),
                "position": round(self._position_now(), 3),
                "speed": round(self.state["_cur_speed"], 3)})


# ==============================================================================
# 3. 气缸控制功能块
# ==============================================================================


class CylinderControlFB(QueuedExecutionFB):
    """
    双作用气缸控制功能块（过程式风格，演示两种建模方式并存）。

    硬件抽象：
      电磁阀 YV1(伸出) / YV2(缩回)；磁性开关 B1(缩回位) / B2(伸出位)；
    行为：
      EXTEND/RETRACT 事件得电 -> 电磁阀换向 -> 行程时间后磁性开关动作
      （带防抖）-> 发出 CYCLE_DONE 与 TASK_COMPLETED。

    容量控制（WIP=1）：机械互锁由"忙碌排队"实现——动作中的新指令
    进入等待队列而非被丢弃，行程到位后自动续跑（_drain_pending）。
    """

    BUSY_TRIGGER_EVENTS = ("EXTEND", "RETRACT")

    EVENT_INPUTS = [
        EventPort("EXTEND", with_inputs=["task_id"], comment="伸出请求"),
        EventPort("RETRACT", with_inputs=["task_id"], comment="缩回请求"),
    ]
    EVENT_OUTPUTS = [
        EventPort("CYCLE_DONE", with_outputs=["task_id", "direction",
                                              "b1", "b2"], comment="动作循环完成"),
        EventPort("TASK_COMPLETED", with_outputs=["task_id", "action"],
                  comment="任务完成"),
    ]
    DATA_INPUTS = {
        "task_id": DataPort("task_id", "STRING", "", "任务ID"),
        "stroke_time_ms": DataPort("stroke_time_ms", "INT", 800, "单程行程时间"),
        "sensor_debounce_ms": DataPort("sensor_debounce_ms", "INT", 20,
                                       "磁性开关防抖时间"),
    }
    DATA_OUTPUTS = {
        "b1": DataPort("b1", "BOOL", True, "缩回位磁性开关"),
        "b2": DataPort("b2", "BOOL", False, "伸出位磁性开关"),
    }

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        self.state["cycles"] = 0
        self.state["position"] = "retracted"     # retracted / extending / ...

    def _is_idle(self) -> bool:
        """过程式 FB 的空闲判定：活塞处于两个止点之一（无 ECC）。"""
        return self.state["position"] in ("retracted", "extended")

    def execute(self, event_name: str) -> None:
        if event_name not in ("EXTEND", "RETRACT"):
            return
        task = str(self.di["task_id"] or "CYL")
        direction = "extend" if event_name == "EXTEND" else "retract"
        # 机械互锁（忙碌排队已保证到达此处时活塞处于止点）
        self.state["position"] = "extending" if direction == "extend" else "retracting"
        logger.info("气缸%s开始（任务%s，行程%.0fms）",
                    "伸出" if direction == "extend" else "缩回",
                    task, float(self.di["stroke_time_ms"]))
        threading.Thread(target=self._stroke, args=(task, direction),
                         daemon=True, name="cyl-stroke").start()

    def _stroke(self, task: str, direction: str) -> None:
        """行程仿真线程：行程时间 + 防抖后更新磁性开关并发出完成事件。"""
        stroke = float(self.di["stroke_time_ms"]) / 1000.0
        debounce = float(self.di["sensor_debounce_ms"]) / 1000.0
        time.sleep(stroke + debounce)
        extended = direction == "extend"
        self.do["b2"] = extended                 # B2：伸出位开关
        self.do["b1"] = not extended             # B1：缩回位开关
        self.state["position"] = "extended" if extended else "retracted"
        self.state["cycles"] += 1
        action = "CYLINDER_EXTEND" if extended else "CYLINDER_RETRACT"
        logger.info("气缸到位：%s（B1=%d B2=%d，累计%d循环）",
                    action, self.do["b1"], self.do["b2"], self.state["cycles"])
        self.emit("CYCLE_DONE", {"task_id": task, "direction": direction,
                                 "b1": self.do["b1"], "b2": self.do["b2"]})
        self.emit("TASK_COMPLETED", {"task_id": task, "action": action,
                                     "node": "node_c"})
        self._drain_pending()                    # 到位后续跑排队中的动作


# ==============================================================================
# 4. 伺服定位功能块
# ==============================================================================


class ServoPositionFB(QueuedExecutionFB):
    """
    伺服电机定位功能块（丝杠直线轴）。

    模型：
      - 最大线速度 v = max_rpm/60 * pitch_mm；
      - 梯形速度曲线运动到 target_mm，结束后叠加 ±0.005mm 随机重复定位误差；
      - |误差| <= positioning_tol 时报 SERVO_IN_POSITION，否则重定位一次。

    容量控制（WIP=1）：单轴，运动/整定中的并发 POSITION 请求进入等待队列。
    """

    BUSY_TRIGGER_EVENTS = ("POSITION",)

    EVENT_INPUTS = [
        EventPort("POSITION", with_inputs=["task_id", "target_mm"],
                  comment="定位请求"),
        EventPort("SETTLED", comment="[内部] 运动完成，进入整定"),
        EventPort("RESET", comment="[内部] 整定结束，回到待机"),
    ]
    EVENT_OUTPUTS = [
        EventPort("SERVO_IN_POSITION", with_outputs=["task_id", "actual_mm",
                                                     "error_mm"],
                  comment="伺服到位"),
        EventPort("TASK_PROGRESS", with_outputs=["task_id", "progress"],
                  comment="定位进度"),
        EventPort("TASK_COMPLETED", with_outputs=["task_id", "action"],
                  comment="任务完成"),
    ]
    DATA_INPUTS = {
        "task_id": DataPort("task_id", "STRING", "", "任务ID"),
        "target_mm": DataPort("target_mm", "REAL", 100.0, "目标位置mm"),
        "max_rpm": DataPort("max_rpm", "INT", 3000, "电机最高转速"),
        "pitch_mm": DataPort("pitch_mm", "REAL", 5.0, "丝杠导程mm"),
        "positioning_tol": DataPort("positioning_tol", "REAL", 0.02,
                                    "定位容差mm"),
    }
    DATA_OUTPUTS = {
        "actual_mm": DataPort("actual_mm", "REAL", 0.0, "实际位置mm"),
    }

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        self.state["current_mm"] = 0.0           # 当前绝对位置

    def build_ecc(self) -> ECC:
        """ECC：Standby -> Moving(运动) -> Settled(整定) -> Standby。"""
        ecc = ECC(initial_state="Standby")
        ecc.add_state(ECCState("Standby", comment="待机"))
        ecc.add_state(ECCState("Moving", entry_actions=["action_move"],
                               comment="运动中"))
        ecc.add_state(ECCState("Settled", entry_actions=["action_settle"],
                               comment="整定中"))
        ecc.add_transition(ECCTransition("Standby", "Moving", event="POSITION"))
        ecc.add_transition(ECCTransition("Moving", "Settled", event="SETTLED"))
        ecc.add_transition(ECCTransition("Settled", "Standby", event="RESET"))
        return ecc

    # ---------------- 动作 ----------------
    def action_move(self) -> None:
        """Moving entry：梯形曲线运动仿真（分片上报进度，结束注入 SETTLED）。"""
        task = str(self.di["task_id"] or "SVO")
        target = float(self.di["target_mm"] or 100.0)
        v_max = float(self.di["max_rpm"]) / 60.0 * float(self.di["pitch_mm"])
        start = self.state["current_mm"]
        delta = target - start
        duration = max(0.3, abs(delta) / v_max if v_max > 0 else 1.0)

        def _motion() -> None:
            logger.info("伺服定位 %s：%.2f -> %.2fmm（v=%.0fmm/s 预计%.2fs）",
                        task, start, target, v_max, duration)
            steps = 6
            for i in range(1, steps + 1):
                time.sleep(duration / steps)
                self.emit("TASK_PROGRESS", {"task_id": task,
                                            "progress": round(i * 100.0 / steps, 1)})
            self.handle_event("SETTLED")

        threading.Thread(target=_motion, daemon=True, name="servo-motion").start()

    def action_settle(self) -> None:
        """Settled entry：整定后判定定位容差，发出到位/完成事件。"""
        task = str(self.di["task_id"] or "SVO")
        target = float(self.di["target_mm"] or 100.0)
        tol = float(self.di["positioning_tol"])

        def _settle() -> None:
            time.sleep(0.1)                               # 整定时间
            error = random.gauss(0, 0.004)                # 重复定位精度仿真
            actual = target + error
            self.state["current_mm"] = actual
            self.do["actual_mm"] = round(actual, 4)
            within = abs(error) <= tol
            logger.info("伺服到位 %s：目标%.2f 实际%.4f 误差%+.4fmm %s",
                        task, target, actual, error,
                        "合格" if within else "超差")
            self.emit("SERVO_IN_POSITION", {"task_id": task,
                                            "actual_mm": round(actual, 4),
                                            "error_mm": round(error, 4),
                                            "within_tol": within})
            self.emit("TASK_COMPLETED", {"task_id": task, "action": "SERVO_MOVE",
                                         "node": "node_c",
                                         "actual_mm": round(actual, 4)})
            self.handle_event("RESET")           # 内部事件：回到待机

        threading.Thread(target=_settle, daemon=True, name="servo-settle").start()


# ==============================================================================
# 5. 节点C 装配与主程序
# ==============================================================================

FB_REGISTRY = {"TaskRouterFB": TaskRouterFB,
               "ConveyorControlFB": ConveyorControlFB,
               "CylinderControlFB": CylinderControlFB,
               "ServoPositionFB": ServoPositionFB}


def build_runtime(config_path: Optional[str] = None) -> DistributedRuntime:
    """装配节点C：任务入口 -> 路由 -> 三类IO控制FB -> 结果回流。"""
    rt = DistributedRuntime("node_c", config_path=config_path)
    router, conveyor, cylinder, servo = rt.autoload_fbs(FB_REGISTRY)

    # ---- 事件连接：优先 nodes.yaml connections 组态；缺省回退硬编码 ----
    fb_index = {"task_router": router, "conveyor": conveyor,
                "cylinder": cylinder, "servo": servo}
    conns = rt.node_cfg.get("connections")
    if conns:
        apply_connections(rt, conns, fb_index)
    else:
        _wire_node_c(rt, router, conveyor, cylinder, servo)
    return rt


def _wire_node_c(rt: DistributedRuntime, router, conveyor, cylinder,
                 servo) -> None:
    """硬编码事件连接（nodes.yaml 无 connections 段时的缺省回退）。"""
    rt.bind_input(Topics.tasks_of("node_c"), router, "TASK")
    rt.route_output(router, "CONVEYOR_REQ", T_CONVEYOR, scope="local")
    rt.route_output(router, "CYL_EXT_REQ", T_CYL_EXT, scope="local")
    rt.route_output(router, "CYL_RET_REQ", T_CYL_RET, scope="local")
    rt.route_output(router, "SERVO_REQ", T_SERVO, scope="local")
    rt.bind_input(T_CONVEYOR, conveyor, "RUN")
    rt.bind_input(T_CYL_EXT, cylinder, "EXTEND")
    rt.bind_input(T_CYL_RET, cylinder, "RETRACT")
    rt.bind_input(T_SERVO, servo, "POSITION")

    # 结果回流主控
    for fb in (conveyor, cylinder, servo):
        rt.route_output(fb, "TASK_COMPLETED", Topics.EVENTS, EventType.TASK_COMPLETED)
        rt.route_output(fb, "TASK_PROGRESS", Topics.EVENTS, EventType.TASK_PROGRESS)
    rt.route_output(conveyor, "CONVEYOR_STATUS", Topics.EVENTS,
                    EventType.CONVEYOR_STATUS)
    rt.route_output(cylinder, "CYCLE_DONE", Topics.EVENTS, EventType.CYCLE_DONE)
    rt.route_output(servo, "SERVO_IN_POSITION", Topics.EVENTS,
                    EventType.SERVO_IN_POSITION)


def main() -> None:
    """节点C主程序。"""
    configure_logger("node_c")
    rt = build_runtime()
    rt.start()
    logger.info("========== PLC节点 node_c 已启动 ==========")
    try:
        while True:
            time.sleep(1.0)              # 保活；控制逻辑全部由事件驱动
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，正在优雅停止……")
    finally:
        rt.stop()
        logger.info("========== PLC节点 node_c 已退出 ==========")


if __name__ == "__main__":
    main()
