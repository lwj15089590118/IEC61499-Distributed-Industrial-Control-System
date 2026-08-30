# -*- coding: utf-8 -*-
"""
================================================================================
 nodes/node_b_robot.py —— 节点B：机器人节点（Robot）
================================================================================
职责（模拟六轴工业机器人的三层能力）：
  1. 搬运任务执行：抓取 -> 移动 -> 放置 全流程模拟（含进度上报）；
  2. 坐标系标定：9点标定法模拟，输出手眼标定矩阵与 RMSE 精度；
  3. 轨迹规划：梯形速度曲线（S型简化）多段轨迹规划模拟。

功能块组成：
  TaskRouterFB        任务路由（把派发任务按 action 分发到对应FB）
  MaterialHandlingFB  搬运执行（ECC状态机：Idle->Moving->Placing->Idle）
  CalibrationFB       坐标系标定
  TrajectoryPlannerFB 轨迹规划
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
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from communication.message_types import EventType, Priority, Topics  # noqa: E402
from core.distributed_runtime import (DistributedRuntime, apply_connections,  # noqa: E402
                                      configure_logger)
from core.function_block import (DataPort, ECC, ECCState, ECCTransition,  # noqa: E402
                                 EventPort, FunctionBlock, QueuedExecutionFB)

logger = logging.getLogger("node_b")

# 节点内部互连主题（local 域）
T_CARRY = "node_b/internal/carry"
T_CALIB = "node_b/internal/calibrate"
T_PLAN = "node_b/internal/plan"


# ==============================================================================
# 1. 任务路由功能块
# ==============================================================================


class TaskRouterFB(FunctionBlock):
    """
    机器人节点任务路由：按任务 action 字段分发到对应执行功能块。

    这是节点侧的"任务入口FB"：主控派发的 TASK_DISPATCHED 统一进入本FB，
    再以节点内部事件分发到搬运/标定/规划功能块。
    """

    EVENT_INPUTS = [
        EventPort("TASK", with_inputs=["task_id", "action", "params",
                                       "order_id"], comment="派发任务到达"),
    ]
    EVENT_OUTPUTS = [
        EventPort("CARRY_REQ", with_outputs=["task_id", "from", "to", "params"],
                  comment="搬运请求"),
        EventPort("CALIB_REQ", with_outputs=["task_id"], comment="标定请求"),
        EventPort("PLAN_REQ", with_outputs=["task_id", "waypoints"],
                  comment="轨迹规划请求"),
        EventPort("UNKNOWN", with_outputs=["task_id", "action"], comment="未知动作"),
    ]
    DATA_INPUTS = {
        "task_id": DataPort("task_id", "STRING", "", "任务ID"),
        "action": DataPort("action", "STRING", "", "任务动作"),
        "params": DataPort("params", "ANY", {}, "任务参数"),
        "order_id": DataPort("order_id", "STRING", "", "所属订单"),
    }
    DATA_OUTPUTS = {}

    # action -> 输出事件名 的映射表
    ACTION_MAP = {
        "CARRY": "CARRY_REQ",
        "CALIBRATE": "CALIB_REQ",
        "PLAN": "PLAN_REQ",
    }

    def execute(self, event_name: str) -> None:
        action = str(self.di["action"])
        task_id = str(self.di["task_id"])
        params: Dict = self.di["params"] or {}
        evt = self.ACTION_MAP.get(action)
        if evt is None:
            logger.warning("机器人节点收到不支持的动作 %s（任务%s）", action, task_id)
            self.emit("UNKNOWN", {"task_id": task_id, "action": action})
            return
        base = {"task_id": task_id, "order_id": str(self.di["order_id"]),
                "params": params}
        if evt == "CARRY_REQ":
            base.update({"from": params.get("from", [350.0, -200.0, 100.0]),
                         "to": params.get("to", [450.0, 250.0, 50.0])})
        elif evt == "PLAN_REQ":
            base.update({"waypoints": params.get("waypoints",
                                                 [[350, -200], [400, 0],
                                                  [450, 250]])})
        self.emit(evt, base)


# ==============================================================================
# 2. 搬运执行功能块（ECC 状态机 + 内部定时事件）
# ==============================================================================


class MaterialHandlingFB(QueuedExecutionFB):
    """
    物料搬运功能块 —— ECC 演示核心。

    状态机：
      Idle  --CARRY-->  Moving --ARRIVED--> Placing --PLACED--> Idle
    其中 ARRIVED / PLACED 是"内部事件"：由搬运仿真线程在物理动作完成时
    注入（对应真实机器人控制器的运动完成中断），充分体现事件驱动：
    FB 的推进完全由事件触发，而非轮询查询。

    容量控制（WIP=1）：机器人单工位，Busy 时并发 CARRY 进入等待队列
    （QueuedExecutionFB），避免第二个任务被 ECC 静默忽略而永久丢失。

    对外事件：
      CARRY(入)  TASK_STARTED / TASK_PROGRESS / TASK_COMPLETED(出)
    """

    BUSY_TRIGGER_EVENTS = ("CARRY",)

    EVENT_INPUTS = [
        EventPort("CARRY", with_inputs=["task_id", "from", "to", "params"],
                  comment="搬运请求"),
        EventPort("ARRIVED", comment="[内部] 移动到位"),
        EventPort("PLACED", comment="[内部] 放置完成"),
    ]
    EVENT_OUTPUTS = [
        EventPort("TASK_STARTED", with_outputs=["task_id", "action"],
                  comment="任务开始"),
        EventPort("TASK_PROGRESS", with_outputs=["task_id", "progress"],
                  comment="搬运进度"),
        EventPort("TASK_COMPLETED", with_outputs=["task_id", "action",
                                                  "cycles"], comment="任务完成"),
    ]
    DATA_INPUTS = {
        "task_id": DataPort("task_id", "STRING", "", "任务ID"),
        "from": DataPort("from", "ANY", [0, 0, 0], "抓取点坐标 xyz(mm)"),
        "to": DataPort("to", "ANY", [0, 0, 0], "放置点坐标 xyz(mm)"),
        "params": DataPort("params", "ANY", {}, "任务参数"),
        "speed_ratio": DataPort("speed_ratio", "REAL", 1.0, "速度倍率 0.1~2.0"),
        "gripper_force": DataPort("gripper_force", "REAL", 35.0, "夹爪力(N)"),
    }
    DATA_OUTPUTS = {
        "cycles": DataPort("cycles", "INT", 0, "累计搬运次数"),
    }

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        self.do["cycles"] = 0
        self.state["current_task"] = None

    # ------------------------------------------------------------ ECC 构建
    def build_ecc(self) -> ECC:
        ecc = ECC(initial_state="Idle")
        # Idle 的 entry 动作只在"经迁移进入"时执行（构造时不会触发），
        # 因此 PLACED -> Idle 进入时正好完成搬运收尾上报
        ecc.add_state(ECCState("Idle", entry_actions=["action_finish_carry"],
                               comment="待机"))
        ecc.add_state(ECCState("Moving", entry_actions=["action_start_move"],
                               exit_actions=["log_exit_moving"],
                               comment="抓取并移动中"))
        ecc.add_state(ECCState("Placing", entry_actions=["action_place"],
                               comment="精确放置中"))
        ecc.add_transition(ECCTransition("Idle", "Moving", event="CARRY", priority=1))
        ecc.add_transition(ECCTransition("Moving", "Placing", event="ARRIVED",
                                         priority=1))
        ecc.add_transition(ECCTransition("Placing", "Idle", event="PLACED",
                                         priority=1))
        return ecc

    # ------------------------------------------------------------ 辅助
    def _spawn(self, fn) -> None:
        """启动守护线程执行物理动作仿真（动作完成后注入内部事件）。"""
        t = threading.Thread(target=fn, daemon=True, name="motion-sim")
        t.start()

    @staticmethod
    def _dist(a: List[float], b: List[float]) -> float:
        """三维欧氏距离。"""
        return math.sqrt(sum((float(x) - float(y)) ** 2 for x, y in zip(a, b)))

    # ------------------------------------------------------------ 状态动作
    def action_start_move(self) -> None:
        """Moving entry：上报开始，仿真抓取+移动（分段进度），到位注入 ARRIVED。"""
        task = str(self.di["task_id"] or "CARRY")
        src, dst = list(self.di["from"]), list(self.di["to"])
        ratio = max(0.1, min(2.0, float(self.di["speed_ratio"])))
        self.state["current_task"] = task
        self.emit("TASK_STARTED", {"task_id": task, "action": "CARRY"})

        def _motion() -> None:
            dist_mm = self._dist(src, dst)
            duration = max(0.4, (dist_mm / 800.0)) / ratio   # 800mm/s 基准速度
            steps = 8                                        # 8段进度仿真
            logger.info("搬运 %s : %s -> %s 距离%.0fmm 预计%.1fs",
                        task, src, dst, dist_mm, duration)
            for i in range(1, steps + 1):                    # 抓取(前2段)+移动
                time.sleep(duration / steps)
                self.emit("TASK_PROGRESS",
                          {"task_id": task,
                           "progress": round(i * 100.0 / steps, 1)})
            self.handle_event("ARRIVED")                     # 注入内部事件

        self._spawn(_motion)

    def log_exit_moving(self) -> None:
        """Moving exit 动作演示（记录轨迹完成时间）。"""
        self.state["last_move_end"] = round(time.time(), 3)

    def action_place(self) -> None:
        """Placing entry：仿真放置并注入 PLACED，随后发出任务完成。"""
        task = self.state.get("current_task", "?")

        def _place() -> None:
            time.sleep(0.15 / max(0.1, float(self.di["speed_ratio"])))
            self.handle_event("PLACED")

        self._spawn(_place)

        # PLACED 到来后的收尾放在守卫迁移后执行（见下），这里先占用夹爪力日志
        logger.debug("以 %.1fN 夹爪力放置 %s", float(self.di["gripper_force"]), task)

    def _ext(self, key: str, default: Any = None) -> Any:
        """读取随事件的扩展数据（基类把未知键存入 _ext_ 前缀状态）。"""
        return self.state.get("_ext_" + key, default)

    def action_finish_carry(self) -> None:
        """Idle entry（经 PLACED 迁移进入）：搬运收尾，上报任务完成。"""
        task = self.state.get("current_task")
        if not task:                        # 构造初始态等其他进入路径不收尾
            return
        self.do["cycles"] = int(self.do["cycles"]) + 1
        self.emit("TASK_COMPLETED", {"task_id": task, "action": "CARRY",
                                     "cycles": self.do["cycles"]})
        self.state["current_task"] = None


# ==============================================================================
# 3. 坐标系标定功能块
# ==============================================================================


class CalibrationFB(FunctionBlock):
    """
    坐标系标定功能块（9点标定法模拟）。

    流程：
      1. 在基座坐标系下生成 3x3 标定点阵（名义值）；
      2. 对每点叠加高斯噪声模拟实机测量误差；
      3. 求解"机器人法兰 -> 相机"的手眼变换（此处用最小二乘平移+旋转近似）；
      4. 计算重投影 RMSE，与验收阈值比较，发出 CALIBRATION_DONE。
    """

    EVENT_INPUTS = [
        EventPort("CALIBRATE", with_inputs=["task_id"], comment="启动标定"),
        EventPort("DONE_CAL", comment="[内部] 标定完成，回到空闲"),
    ]
    EVENT_OUTPUTS = [
        EventPort("CALIBRATION_DONE", with_outputs=["task_id", "rmse",
                                                    "matrix", "accepted"],
                  comment="标定完成"),
        EventPort("TASK_COMPLETED", with_outputs=["task_id", "action"],
                  comment="任务完成（路由统一口径）"),
    ]
    DATA_INPUTS = {
        "task_id": DataPort("task_id", "STRING", "", "任务ID"),
        "points": DataPort("points", "INT", 9, "标定点数(9/16)"),
        "acceptance_rmse": DataPort("acceptance_rmse", "REAL", 0.05,
                                    "验收 RMSE 阈值(mm)"),
    }
    DATA_OUTPUTS = {
        "rmse": DataPort("rmse", "REAL", 0.0, "标定残差RMSE(mm)"),
        "matrix": DataPort("matrix", "ANY", None, "手眼变换矩阵4x4"),
    }

    def build_ecc(self) -> ECC:
        """ECC：Idle -> Calibrating(执行标定) -> Idle（内部事件 DONE_CAL 迁回）。"""
        ecc = ECC(initial_state="Idle")
        ecc.add_state(ECCState("Idle"))
        ecc.add_state(ECCState("Calibrating", entry_actions=["action_calibrate"]))
        ecc.add_transition(ECCTransition("Idle", "Calibrating", event="CALIBRATE"))
        ecc.add_transition(ECCTransition("Calibrating", "Idle", event="DONE_CAL"))
        return ecc

    def action_calibrate(self) -> None:
        """执行9点标定仿真并发出结果。"""
        task = str(self.di["task_id"] or "CAL-0000")
        n_side = 3 if int(self.di["points"]) <= 9 else 4
        logger.info("启动 %d 点标定（任务%s）……", n_side * n_side, task)
        time.sleep(0.6)                                     # 模拟采图耗时

        # 名义点阵 + 噪声测量值
        errors: List[float] = []
        for r in range(n_side):
            for c in range(n_side):
                nominal = (100.0 * r, 100.0 * c, 0.0)
                measured = tuple(v + random.gauss(0, 0.012) for v in nominal)
                errors.append(math.dist(nominal, measured))

        rmse = math.sqrt(sum(e * e for e in errors) / len(errors))
        accepted = rmse <= float(self.di["acceptance_rmse"])
        # 4x4 齐次变换矩阵（模拟解算结果：旋转接近单位阵 + 微小平移）
        matrix = [[1.0001, -0.0002, 0.0000, 0.021],
                  [0.0002, 0.9999, -0.0001, -0.015],
                  [0.0000, 0.0001, 1.0000, 112.4],
                  [0.0, 0.0, 0.0, 1.0]]
        self.do["rmse"] = round(rmse, 5)
        self.do["matrix"] = matrix
        self.state["last_calibration"] = {"rmse": self.do["rmse"],
                                          "accepted": accepted}
        logger.info("标定完成 RMSE=%.4fmm %s", rmse,
                    "合格" if accepted else "超差(需复标)")
        self.emit("CALIBRATION_DONE", {"task_id": task, "rmse": round(rmse, 5),
                                       "matrix": matrix, "accepted": accepted})
        self.emit("TASK_COMPLETED", {"task_id": task, "action": "CALIBRATE",
                                     "node": "node_b"})
        self.handle_event("DONE_CAL")         # 内部事件：回到 Idle


# ==============================================================================
# 4. 轨迹规划功能块
# ==============================================================================


class TrajectoryPlannerFB(FunctionBlock):
    """
    轨迹规划功能块：梯形速度曲线多段规划（纯计算，无阻塞）。

    对每个相邻路径点区间求解：
      加速段 t_a = v_max / a_max；若剩余距离不足则退化为三角速度曲线；
      总时长 = 加速 + 匀速 + 减速。
    """

    EVENT_INPUTS = [
        EventPort("PLAN", with_inputs=["task_id", "waypoints"], comment="规划请求"),
    ]
    EVENT_OUTPUTS = [
        EventPort("TRAJECTORY_PLANNED", with_outputs=["task_id", "segments",
                                                      "total_time"],
                  comment="轨迹规划结果"),
        EventPort("TASK_COMPLETED", with_outputs=["task_id", "action"],
                  comment="任务完成"),
    ]
    DATA_INPUTS = {
        "task_id": DataPort("task_id", "STRING", "", "任务ID"),
        "waypoints": DataPort("waypoints", "ANY", [], "路径点序列[[x,y],...]"),
        "max_velocity": DataPort("max_velocity", "REAL", 1200.0, "最大速度mm/s"),
        "max_accel": DataPort("max_accel", "REAL", 8000.0, "最大加速度mm/s^2"),
        "blend_radius": DataPort("blend_radius", "REAL", 5.0, "拐角过渡半径mm"),
    }
    DATA_OUTPUTS = {
        "total_time": DataPort("total_time", "REAL", 0.0, "轨迹总时长(s)"),
    }

    def execute(self, event_name: str) -> None:
        if event_name != "PLAN":
            return
        task = str(self.di["task_id"] or "PLAN-0000")
        pts = self.di["waypoints"] or [[0, 0], [100, 0], [100, 100]]
        v_max = float(self.di["max_velocity"])
        a_max = float(self.di["max_accel"])
        blend = float(self.di["blend_radius"])

        segments: List[Dict[str, Any]] = []
        total = 0.0
        for i in range(len(pts) - 1):
            (x0, y0), (x1, y1) = pts[i], pts[i + 1]
            dist = math.hypot(float(x1) - float(x0), float(y1) - float(y0))
            dist = max(0.0, dist - (blend if 0 < i < len(pts) - 2 else 0.0))
            # 梯形速度曲线求解
            t_accel = v_max / a_max
            d_accel = 0.5 * a_max * t_accel * t_accel
            if dist >= 2 * d_accel:                          # 完整梯形
                t_cruise = (dist - 2 * d_accel) / v_max
                profile = "trapezoid"
            else:                                            # 三角形退化
                t_accel = math.sqrt(dist / a_max)
                t_cruise = 0.0
                profile = "triangle"
            seg_time = 2 * t_accel + t_cruise
            total += seg_time
            segments.append({"seg": i, "from": [x0, y0], "to": [x1, y1],
                             "dist_mm": round(dist, 2), "profile": profile,
                             "t_accel_s": round(t_accel, 4),
                             "t_cruise_s": round(t_cruise, 4),
                             "duration_s": round(seg_time, 4)})
        self.do["total_time"] = round(total, 4)
        logger.info("轨迹规划 %s：%d段 总时长%.3fs", task, len(segments), total)
        self.emit("TRAJECTORY_PLANNED", {"task_id": task, "segments": segments,
                                         "total_time": round(total, 4)})
        self.emit("TASK_COMPLETED", {"task_id": task, "action": "PLAN",
                                     "node": "node_b"})


# ==============================================================================
# 5. 节点B 装配与主程序
# ==============================================================================

FB_REGISTRY = {"TaskRouterFB": TaskRouterFB,
               "MaterialHandlingFB": MaterialHandlingFB,
               "CalibrationFB": CalibrationFB,
               "TrajectoryPlannerFB": TrajectoryPlannerFB}


def build_runtime(config_path: Optional[str] = None) -> DistributedRuntime:
    """装配节点B：任务入口 -> 路由FB -> 各执行FB -> 结果回流主控。"""
    rt = DistributedRuntime("node_b", config_path=config_path)
    router, handling, calib, planner = rt.autoload_fbs(FB_REGISTRY)

    # ---- 事件连接：优先 nodes.yaml connections 组态；缺省回退硬编码 ----
    fb_index = {"task_router": router, "handling": handling,
                "calibration": calib, "trajectory": planner}
    conns = rt.node_cfg.get("connections")
    if conns:
        apply_connections(rt, conns, fb_index)
    else:
        _wire_node_b(rt, router, handling, calib, planner)
    return rt


def _wire_node_b(rt: DistributedRuntime, router, handling, calib,
                 planner) -> None:
    """硬编码事件连接（nodes.yaml 无 connections 段时的缺省回退）。"""
    # 主控任务派发 -> 路由FB
    rt.bind_input(Topics.tasks_of("node_b"), router, "TASK")
    # 路由FB -> 各执行FB（节点内互连）
    rt.route_output(router, "CARRY_REQ", T_CARRY, scope="local")
    rt.route_output(router, "CALIB_REQ", T_CALIB, scope="local")
    rt.route_output(router, "PLAN_REQ", T_PLAN, scope="local")
    rt.bind_input(T_CARRY, handling, "CARRY")
    rt.bind_input(T_CALIB, calib, "CALIBRATE")
    rt.bind_input(T_PLAN, planner, "PLAN")

    # 执行结果 -> 主控（factory/events）
    for fb, ev in ((handling, "TASK_STARTED"), (handling, "TASK_PROGRESS"),
                   (handling, "TASK_COMPLETED"), (calib, "TASK_COMPLETED"),
                   (planner, "TASK_COMPLETED")):
        rt.route_output(fb, ev, Topics.EVENTS,
                        EventType.TASK_STARTED if ev == "TASK_STARTED"
                        else EventType.TASK_PROGRESS if ev == "TASK_PROGRESS"
                        else EventType.TASK_COMPLETED)
    rt.route_output(calib, "CALIBRATION_DONE", Topics.EVENTS,
                    EventType.CALIBRATION_DONE)
    rt.route_output(planner, "TRAJECTORY_PLANNED", Topics.EVENTS,
                    EventType.TRAJECTORY_PLANNED)


def main() -> None:
    """节点B主程序：启动后自动执行一次上电视准（演示标定链路）。"""
    configure_logger("node_b")
    rt = build_runtime()
    rt.start()
    logger.info("========== 机器人节点 node_b 已启动 ==========")

    # 上电自检：自动触发一次坐标标定（真实产线换型后的标准动作）
    time.sleep(1.0)
    calib = rt.get_fb("calibration")
    if calib:
        calib.handle_event("CALIBRATE", {"task_id": "CALIB-BOOT-%d"
                                         % int(time.time())})

    try:
        while True:
            time.sleep(1.0)              # 主线程仅保活；一切执行由事件驱动
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，正在优雅停止……")
    finally:
        rt.stop()
        logger.info("========== 机器人节点 node_b 已退出 ==========")


if __name__ == "__main__":
    main()
