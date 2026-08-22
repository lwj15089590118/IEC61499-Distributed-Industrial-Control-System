# -*- coding: utf-8 -*-
"""
================================================================================
 nodes/node_d_vision.py —— 节点D：视觉节点（Machine Vision + AI）
================================================================================
职责（模拟工业视觉检测的完整流水线）：
  1. 图像采集：模拟相机曝光、帧生成（含噪声与图像质量指标）；
  2. AI推理：模拟深度学习缺陷检测模型的推理时延与置信度输出；
  3. 缺陷分类：按置信度与阈值判定 OK/NG，输出缺陷类别；
  4. 结果反馈：VISION_RESULT 事件回传主控节点（进入事件流参与调度决策）。

流水线（全部事件驱动，节点内部以局部主题互连）：
  TASK_DISPATCHED -> TaskRouterFB -> ImageAcquisitionFB(ACQUIRE)
      -> AIInferenceFB(INFER) -> DefectClassifierFB(CLASSIFY) -> 结果外发
================================================================================
"""

from __future__ import annotations

import logging
import random
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from communication.message_types import EventType, Topics  # noqa: E402
from core.distributed_runtime import DistributedRuntime, configure_logger  # noqa: E402
from core.function_block import (DataPort, ECC, ECCState, ECCTransition,  # noqa: E402
                                 EventPort, FunctionBlock)

logger = logging.getLogger("node_d")

# 节点内部互连主题（local 域，流水线级联）
T_INSPECT = "node_d/internal/inspect"
T_CAPTURED = "node_d/internal/captured"
T_INFERRED = "node_d/internal/inferred"

# 模拟产品缺陷先验概率（AI推理结果采样的依据）
DEFECT_PRIORS: Dict[str, float] = {
    "OK": 0.90, "划痕": 0.045, "凹陷": 0.025, "污渍": 0.02, "边缘破损": 0.01,
}


# ==============================================================================
# 1. 任务路由功能块
# ==============================================================================


class TaskRouterFB(FunctionBlock):
    """视觉节点任务路由：INSPECT 动作 -> 启动检测流水线。"""

    EVENT_INPUTS = [
        EventPort("TASK", with_inputs=["task_id", "action", "params"],
                  comment="派发任务到达"),
    ]
    EVENT_OUTPUTS = [
        EventPort("INSPECT_REQ", with_outputs=["task_id", "params"],
                  comment="检测请求"),
        EventPort("UNKNOWN", with_outputs=["task_id", "action"], comment="未知动作"),
    ]
    DATA_INPUTS = {
        "task_id": DataPort("task_id", "STRING", "", "任务ID"),
        "action": DataPort("action", "STRING", "", "任务动作"),
        "params": DataPort("params", "ANY", {}, "任务参数"),
    }
    DATA_OUTPUTS = {}

    def execute(self, event_name: str) -> None:
        if str(self.di["action"]) != "INSPECT":
            self.emit("UNKNOWN", {"task_id": str(self.di["task_id"]),
                                  "action": str(self.di["action"])})
            return
        self.emit("INSPECT_REQ", {"task_id": str(self.di["task_id"]),
                                  "order_id": self.state.get("_ext_order_id", ""),
                                  "params": self.di["params"] or {}})


# ==============================================================================
# 2. 图像采集功能块
# ==============================================================================


class ImageAcquisitionFB(FunctionBlock):
    """
    图像采集功能块（模拟工业相机）。

    行为：
      ACQUIRE 事件 -> 模拟曝光/传输时延 -> 生成帧元数据（帧号/亮度/噪声）
      -> 发出 FRAME_READY，触发下游推理。
    """

    EVENT_INPUTS = [
        EventPort("ACQUIRE", with_inputs=["task_id", "params"], comment="采图请求"),
        EventPort("FRAME_SENT", comment="[内部] 帧传输完成"),
    ]
    EVENT_OUTPUTS = [
        EventPort("FRAME_READY", with_outputs=["task_id", "frame_id", "quality"],
                  comment="帧就绪"),
        EventPort("TASK_PROGRESS", with_outputs=["task_id", "progress"],
                  comment="检测进度(采集阶段)"),
    ]
    DATA_INPUTS = {
        "task_id": DataPort("task_id", "STRING", "", "任务ID"),
        "params": DataPort("params", "ANY", {}, "任务参数"),
        "exposure_us": DataPort("exposure_us", "INT", 8000, "曝光时间us"),
        "frame_width": DataPort("frame_width", "INT", 2448, "分辨率宽"),
        "frame_height": DataPort("frame_height", "INT", 2048, "分辨率高"),
    }
    DATA_OUTPUTS = {
        "frame_id": DataPort("frame_id", "STRING", "", "帧编号"),
        "quality": DataPort("quality", "REAL", 0.0, "图像质量分"),
    }

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        self.state["frames"] = 0                  # 累计采帧数

    def build_ecc(self) -> ECC:
        """ECC：Idle -> Exposing(曝光/传输) -> Idle（内部事件迁回）。"""
        ecc = ECC(initial_state="Idle")
        ecc.add_state(ECCState("Idle", comment="待机"))
        ecc.add_state(ECCState("Exposing", entry_actions=["action_expose"],
                               comment="曝光与传输"))
        ecc.add_transition(ECCTransition("Idle", "Exposing", event="ACQUIRE"))
        ecc.add_transition(ECCTransition("Exposing", "Idle", event="FRAME_SENT"))
        return ecc

    def action_expose(self) -> None:
        """Exposing entry：模拟曝光+帧传输，完成后注入 FRAME_SENT。"""
        task = str(self.di["task_id"] or "VIS")
        exposure_ms = float(self.di["exposure_us"]) / 1000.0
        transfer_ms = 30.0                         # GigE 传输时延模拟

        def _capture() -> None:
            time.sleep((exposure_ms + transfer_ms) / 1000.0)
            self.state["frames"] += 1
            frame_id = "FRM-%06d" % self.state["frames"]
            # 图像质量分：曝光充足度 + 随机噪声（0~1）
            quality = round(min(1.0, exposure_ms / 8.0) * random.uniform(0.85, 1.0), 3)
            self.do["frame_id"] = frame_id
            self.do["quality"] = quality
            logger.info("采帧 %s (%dx%d) 质量分%.2f（任务%s）",
                        frame_id, int(self.di["frame_width"]),
                        int(self.di["frame_height"]), quality, task)
            self.emit("TASK_PROGRESS", {"task_id": task, "progress": 30.0})
            self.emit("FRAME_READY", {"task_id": task, "frame_id": frame_id,
                                      "quality": quality})
            self.handle_event("FRAME_SENT")

        threading.Thread(target=_capture, daemon=True, name="capture").start()


# ==============================================================================
# 3. AI 推理功能块
# ==============================================================================


class AIInferenceFB(FunctionBlock):
    """
    AI推理功能块（模拟缺陷检测CNN）。

    行为：
      INFER 事件（随行 frame_id/quality）-> 模拟 GPU 推理时延
      -> 按缺陷先验分布采样真实类别 -> 生成与类别相关的置信度
      -> 发出 INFER_DONE（置信度 + 候选类别）。
    """

    EVENT_INPUTS = [
        EventPort("INFER", with_inputs=["task_id", "frame_id", "quality"],
                  comment="推理请求"),
        EventPort("INFER_DONE_EVT", comment="[内部] 推理完成"),
    ]
    EVENT_OUTPUTS = [
        EventPort("INFER_DONE", with_outputs=["task_id", "confidence",
                                              "raw_class"], comment="推理输出"),
        EventPort("TASK_PROGRESS", with_outputs=["task_id", "progress"],
                  comment="检测进度(推理阶段)"),
    ]
    DATA_INPUTS = {
        "task_id": DataPort("task_id", "STRING", "", "任务ID"),
        "frame_id": DataPort("frame_id", "STRING", "", "帧编号"),
        "quality": DataPort("quality", "REAL", 1.0, "图像质量分"),
        "model_version": DataPort("model_version", "STRING", "v2.3.1", "模型版本"),
        "infer_latency_ms": DataPort("infer_latency_ms", "INT", 45, "推理时延ms"),
    }
    DATA_OUTPUTS = {
        "confidence": DataPort("confidence", "REAL", 0.0, "置信度"),
        "raw_class": DataPort("raw_class", "STRING", "OK", "模型原始输出类别"),
    }

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        self.state["inferences"] = 0
        self.state["total_latency_ms"] = 0.0

    def build_ecc(self) -> ECC:
        """ECC：Idle -> Inferencing -> Idle（内部事件迁回）。"""
        ecc = ECC(initial_state="Idle")
        ecc.add_state(ECCState("Idle"))
        ecc.add_state(ECCState("Inferencing", entry_actions=["action_infer"]))
        ecc.add_transition(ECCTransition("Idle", "Inferencing", event="INFER"))
        ecc.add_transition(ECCTransition("Inferencing", "Idle",
                                         event="INFER_DONE_EVT"))
        return ecc

    @staticmethod
    def _sample_class() -> str:
        """按先验分布采样一个真实缺陷类别。"""
        classes = list(DEFECT_PRIORS)
        weights = list(DEFECT_PRIORS.values())
        return random.choices(classes, weights=weights, k=1)[0]

    def action_infer(self) -> None:
        """Inferencing entry：模拟推理时延后输出置信度。"""
        task = str(self.di["task_id"] or "VIS")
        latency = float(self.di["infer_latency_ms"]) * random.uniform(0.8, 1.3)

        def _infer() -> None:
            time.sleep(latency / 1000.0)
            truth = self._sample_class()
            quality = float(self.di["quality"] or 1.0)
            # 置信度模拟：真缺陷/良品都会给较高置信，图像质量差时置信下降
            conf = random.uniform(0.72, 0.99) * (0.6 + 0.4 * quality)
            conf = round(min(0.999, max(0.01, conf)), 3)
            self.do["confidence"] = conf
            self.do["raw_class"] = truth
            self.state["inferences"] += 1
            self.state["total_latency_ms"] += latency
            logger.info("推理%s：类别=%s 置信度=%.3f 时延=%.0fms（任务%s）",
                        self.di["model_version"], truth, conf, latency, task)
            self.emit("TASK_PROGRESS", {"task_id": task, "progress": 75.0})
            self.emit("INFER_DONE", {"task_id": task, "confidence": conf,
                                     "raw_class": truth, "frame_id":
                                     str(self.di["frame_id"])})
            self.handle_event("INFER_DONE_EVT")

        threading.Thread(target=_infer, daemon=True, name="infer").start()


# ==============================================================================
# 4. 缺陷分类功能块
# ==============================================================================


class DefectClassifierFB(FunctionBlock):
    """
    缺陷分类功能块：按阈值规则把推理输出转为质检判定。

    规则：
      - 置信度 >= ng_threshold 且类别 != OK  -> NG（缺陷类别上报）；
      - 其他情况 -> OK；
      - 置信度落在灰色地带（[th-0.1, th) 且类别!=OK）-> 复检请求。
    输出：
      VISION_RESULT 事件（发往 factory/vision/results，主控消费）。
    """

    EVENT_INPUTS = [
        EventPort("CLASSIFY", with_inputs=["task_id", "confidence", "raw_class"],
                  comment="分类请求"),
    ]
    EVENT_OUTPUTS = [
        EventPort("VISION_RESULT", with_outputs=["task_id", "verdict",
                                                 "defect", "confidence"],
                  comment="质检结果"),
        EventPort("TASK_COMPLETED", with_outputs=["task_id", "action",
                                                  "verdict"], comment="任务完成"),
    ]
    DATA_INPUTS = {
        "task_id": DataPort("task_id", "STRING", "", "任务ID"),
        "confidence": DataPort("confidence", "REAL", 0.0, "推理置信度"),
        "raw_class": DataPort("raw_class", "STRING", "OK", "模型输出类别"),
        "ng_threshold": DataPort("ng_threshold", "REAL", 0.65, "NG判定阈值"),
        "classes": DataPort("classes", "ANY", ["划痕", "凹陷", "污渍", "边缘破损"],
                            "缺陷类别表"),
    }
    DATA_OUTPUTS = {
        "verdict": DataPort("verdict", "STRING", "", "OK / NG / RECHECK"),
    }

    def __init__(self, name: str, params=None):
        super().__init__(name, params)
        self.state["ok_count"] = 0
        self.state["ng_count"] = 0
        self.state["recheck_count"] = 0

    def execute(self, event_name: str) -> None:
        if event_name != "CLASSIFY":
            return
        task = str(self.di["task_id"] or "VIS")
        conf = float(self.di["confidence"] or 0.0)
        raw = str(self.di["raw_class"] or "OK")
        threshold = float(self.di["ng_threshold"])

        if raw == "OK" or conf < threshold:
            verdict, defect = "OK", ""
            self.state["ok_count"] += 1
            # 灰色地带：非OK类别但置信不足 -> 请求复检
            if raw != "OK" and conf >= threshold - 0.1:
                verdict = "RECHECK"
                defect = raw
                self.state["recheck_count"] += 1
                self.state["ok_count"] -= 1
        else:
            verdict, defect = "NG", raw
            self.state["ng_count"] += 1

        self.do["verdict"] = verdict
        logger.info("质检判定 %s：verdict=%s defect=%s conf=%.3f（OK:%d NG:%d 复检:%d）",
                    task, verdict, defect or "-", conf,
                    self.state["ok_count"], self.state["ng_count"],
                    self.state["recheck_count"])
        # 结果双路输出：主控结果主题 + 通用事件流（任务完成）
        self.emit("VISION_RESULT", {"task_id": task, "verdict": verdict,
                                    "defect": defect, "confidence": conf,
                                    "model": self.state.get("_ext_frame_id", "")})
        self.emit("TASK_COMPLETED", {"task_id": task, "action": "INSPECT",
                                     "node": "node_d", "verdict": verdict})


# ==============================================================================
# 5. 节点D 装配与主程序
# ==============================================================================

FB_REGISTRY = {"TaskRouterFB": TaskRouterFB,
               "ImageAcquisitionFB": ImageAcquisitionFB,
               "AIInferenceFB": AIInferenceFB,
               "DefectClassifierFB": DefectClassifierFB}


def build_runtime(config_path: Optional[str] = None) -> DistributedRuntime:
    """装配节点D：路由 -> 采集 -> 推理 -> 分类 -> 结果回传主控。"""
    rt = DistributedRuntime("node_d", config_path=config_path)
    router, camera, infer, classifier = rt.autoload_fbs(FB_REGISTRY)

    # 任务入口 -> 检测流水线
    rt.bind_input(Topics.tasks_of("node_d"), router, "TASK")
    rt.route_output(router, "INSPECT_REQ", T_INSPECT, scope="local")
    rt.bind_input(T_INSPECT, camera, "ACQUIRE")

    # 流水线级联：采集 -> 推理 -> 分类（事件连接即产线节拍）
    rt.route_output(camera, "FRAME_READY", T_CAPTURED, scope="local")
    rt.bind_input(T_CAPTURED, infer, "INFER")
    rt.route_output(infer, "INFER_DONE", T_INFERRED, scope="local")
    rt.bind_input(T_INFERRED, classifier, "CLASSIFY")

    # 结果回传：视觉结果主题 + 通用事件流 + 进度
    rt.route_output(classifier, "VISION_RESULT", Topics.VISION_RESULTS,
                    EventType.VISION_RESULT)
    rt.route_output(classifier, "TASK_COMPLETED", Topics.EVENTS,
                    EventType.TASK_COMPLETED)
    rt.route_output(camera, "TASK_PROGRESS", Topics.EVENTS, EventType.TASK_PROGRESS)
    rt.route_output(infer, "TASK_PROGRESS", Topics.EVENTS, EventType.TASK_PROGRESS)
    return rt


def main() -> None:
    """节点D主程序。"""
    configure_logger("node_d")
    rt = build_runtime()
    rt.start()
    logger.info("========== 视觉节点 node_d 已启动 ==========")
    try:
        while True:
            time.sleep(1.0)              # 保活；检测流水线完全由事件驱动
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，正在优雅停止……")
    finally:
        rt.stop()
        logger.info("========== 视觉节点 node_d 已退出 ==========")


if __name__ == "__main__":
    main()
