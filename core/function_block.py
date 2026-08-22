# -*- coding: utf-8 -*-
"""
================================================================================
 core/function_block.py —— IEC 61499 功能块（Function Block）基类实现
================================================================================
本模块是整个系统对 IEC 61499 标准建模能力的"内核"级抽象，实现了：

  1. 事件输入 / 事件输出（Event Input / Event Output）
     - 事件是功能块执行的唯一触发源（事件驱动，而非周期扫描）；
     - 事件端口通过 WITH 关联声明"随事件刷新的数据端口"，
       精确对应标准中 Service Interface / Basic FB 的事件-数据关联语义。

  2. 数据输入 / 数据输出（Data Input / Data Output）
     - 数据端口带类型与初始值，可被外部在线配置（对应参数化 FB 实例）。

  3. ECC（Execution Control Chart，执行控制链）
     - 即 IEC 61499 Basic FB 内部的状态机：状态 + 迁移条件（事件+守卫）；
     - 每个状态可挂载 entry/exit 动作（算法），迁移由事件触发并可用
       guard（布尔守卫函数）进一步约束；
     - 本实现完全遵循"事件到来 -> 求值迁移 -> 执行动作 -> 发出输出事件"
       的标准执行序列。

  4. 执行逻辑接口
     - 子类既可以用 ECC 声明式地描述行为，也可以直接覆写 execute()
       编写过程式逻辑（服务接口型 FB 的常见写法）；
     - emit() 负责发出输出事件，运行时会注入回调把事件路由到事件总线。

设计取舍说明：
  - 每个功能块实例内部持有一把可重入锁，保证同一 FB 不会并发执行
    （模拟真实控制器中 FB 的原子执行语义）；
  - 所有端口信息可通过 describe() 导出，供 Web 控制台在线展示与配置。
================================================================================
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("function_block")


# ==============================================================================
# 1. 端口定义 —— 数据端口与事件端口
# ==============================================================================


class DataPort:
    """
    数据端口定义。

    属性：
      name    : 端口名（如 "quantity"、"target_pos"）；
      dtype   : 类型名字符串（"INT"/"REAL"/"STRING"/"BOOL"/"ANY"，仅作声明）；
      initial : 初始默认值；
      comment : 端口说明（导出描述时展示给用户）。
    """

    def __init__(self, name: str, dtype: str = "ANY",
                 initial: Any = 0, comment: str = "") -> None:
        self.name = name
        self.dtype = dtype
        self.initial = initial
        self.comment = comment

    def describe(self) -> Dict[str, Any]:
        """导出端口元信息（Web 配置界面使用）。"""
        return {"name": self.name, "type": self.dtype,
                "initial": self.initial, "comment": self.comment}


class EventPort:
    """
    事件端口定义。

    属性：
      name        : 事件名（如 "REQ"、"START"、"CNF"）；
      with_inputs : WITH 关联的数据输入名列表 —— 该事件到来时，
                    这些数据输入被视为"随事件刷新"（IEC 61499 WITH 语义）；
      with_outputs: WITH 关联的数据输出名列表 —— 该事件发出时，
                    随行携带的数据输出；
      comment     : 事件说明。
    """

    def __init__(self, name: str,
                 with_inputs: Optional[List[str]] = None,
                 with_outputs: Optional[List[str]] = None,
                 comment: str = "") -> None:
        self.name = name
        self.with_inputs = list(with_inputs or [])
        self.with_outputs = list(with_outputs or [])
        self.comment = comment

    def describe(self) -> Dict[str, Any]:
        """导出端口元信息。"""
        return {"name": self.name,
                "with_inputs": self.with_inputs,
                "with_outputs": self.with_outputs,
                "comment": self.comment}


# ==============================================================================
# 2. ECC（执行控制链）—— 状态、迁移与状态机
# ==============================================================================


class ECCTransition:
    """
    ECC 迁移：source --[事件 AND 守卫]--> target。

    说明：
      - event 为事件输入名；guard 为功能块上的守卫方法名（返回 bool），
        两者可只写其一（event=None 表示纯守卫迁移，guard=None 表示
        事件到来即迁移）；
      - priority 为同源迁移的求值顺序（小者先），对应标准中
        迁移条件的优先级排序。
    """

    def __init__(self, source: str, target: str,
                 event: Optional[str] = None,
                 guard: Optional[str] = None,
                 priority: int = 100) -> None:
        self.source = source
        self.target = target
        self.event = event
        self.guard = guard
        self.priority = priority


class ECCState:
    """
    ECC 状态：可挂载 entry（进入时）/ exit（离开时）动作。

    动作是功能块实例上的方法名字符串；典型的 entry 动作会执行算法
    并通过 emit() 发出输出事件。
    """

    def __init__(self, name: str,
                 entry_actions: Optional[List[str]] = None,
                 exit_actions: Optional[List[str]] = None,
                 comment: str = "") -> None:
        self.name = name
        self.entry_actions = list(entry_actions or [])
        self.exit_actions = list(exit_actions or [])
        self.comment = comment


class ECC:
    """
    执行控制链状态机。

    遵循 IEC 61499 的执行序列：
      事件输入到来
        -> 在当前状态的出边中按 priority 查找 event 匹配且 guard 为真的迁移
        -> 执行当前状态的 exit 动作
        -> 切换到目标状态并执行其 entry 动作
    若没有迁移被选中，则状态保持不变（事件被忽略，符合标准语义）。
    """

    def __init__(self, initial_state: str) -> None:
        self.states: Dict[str, ECCState] = {}
        self.transitions: List[ECCTransition] = []
        self.current_state: str = initial_state
        self._initial_state = initial_state

    # ------------------------------------------------------------------ 构建
    def add_state(self, state: ECCState) -> "ECC":
        """添加一个状态（支持链式调用）。"""
        self.states[state.name] = state
        return self

    def add_transition(self, transition: ECCTransition) -> "ECC":
        """添加一条迁移（支持链式调用）。"""
        self.transitions.append(transition)
        self.transitions.sort(key=lambda t: t.priority)  # 优先级排序
        return self

    # ------------------------------------------------------------------ 执行
    def process_event(self, event_name: str, fb: "FunctionBlock") -> bool:
        """
        用事件驱动状态机：成功发生状态迁移返回 True，事件被忽略返回 False。

        参数 fb 是宿主功能块实例：guard 与动作都在该实例上按名解析。
        """
        # 按优先级遍历当前状态的出边
        for trans in self.transitions:
            if trans.source != self.current_state:
                continue
            if trans.event is not None and trans.event != event_name:
                continue
            if trans.guard is not None:
                guard_fn = getattr(fb, trans.guard, None)
                if guard_fn is None or not guard_fn():
                    continue               # 守卫不满足：尝试下一条迁移

            # ---- 迁移命中：exit 旧状态 -> 切换 -> entry 新状态 ----
            old = self.states.get(self.current_state)
            if old:
                for action in old.exit_actions:
                    self._invoke(fb, action)
            self.current_state = trans.target
            new = self.states.get(self.current_state)
            if new:
                for action in new.entry_actions:
                    self._invoke(fb, action)
            return True
        return False

    @staticmethod
    def _invoke(fb: "FunctionBlock", action_name: str) -> None:
        """按名称调用功能块实例上的动作方法；异常只记录不上抛。"""
        fn = getattr(fb, action_name, None)
        if fn is None:
            logger.warning("[%s] ECC动作缺失: %s", fb.name, action_name)
            return
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 动作异常不应摧毁状态机
            logger.exception("[%s] ECC动作 %s 异常: %s", fb.name, action_name, exc)

    def reset(self) -> None:
        """状态机复位到初始状态（热备接管/在线复位时使用）。"""
        self.current_state = self._initial_state


# ==============================================================================
# 3. 功能块基类
# ==============================================================================


class FunctionBlock:
    """
    功能块基类 —— 所有具体功能块（订单分解、传送带控制……）的父类。

    子类声明方式（类属性）：
      EVENT_INPUTS  : List[EventPort]   事件输入端口表
      EVENT_OUTPUTS : List[EventPort]   事件输出端口表
      DATA_INPUTS   : Dict[str, DataPort]  数据输入端口表（名 -> 定义）
      DATA_OUTPUTS  : Dict[str, DataPort]  数据输出端口表

    运行时数据容器：
      self.di : 数据输入当前值（dict）
      self.do : 数据输出当前值（dict）
      self.ei_count / self.eo_count : 事件到达/发出计数（统计用）

    两种行为建模风格（二选一或混用）：
      A. 声明式：实现 build_ecc() 返回 ECC，用状态与迁移描述行为；
      B. 过程式：直接覆写 execute(event_name)，按事件名分支处理。
    """

    # ---- 端口声明（子类覆盖）----
    EVENT_INPUTS: List[EventPort] = []
    EVENT_OUTPUTS: List[EventPort] = []
    DATA_INPUTS: Dict[str, DataPort] = {}
    DATA_OUTPUTS: Dict[str, DataPort] = {}

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None) -> None:
        """
        参数：
          name   : 功能块实例名（节点内唯一，例如 "order_manager"）；
          params : 参数字典（覆盖数据输入端口的初始值，即"参数化实例"）。
        """
        self.name = name
        self.fb_type = self.__class__.__name__

        # ---- 端口索引（加速查找）----
        self._ei_ports: Dict[str, EventPort] = {p.name: p for p in self.EVENT_INPUTS}
        self._eo_ports: Dict[str, EventPort] = {p.name: p for p in self.EVENT_OUTPUTS}

        # ---- 数据容器初始化 ----
        self.di: Dict[str, Any] = {k: p.initial for k, p in self.DATA_INPUTS.items()}
        self.do: Dict[str, Any] = {k: p.initial for k, p in self.DATA_OUTPUTS.items()}
        self.ei_count: Dict[str, int] = {p.name: 0 for p in self.EVENT_INPUTS}
        self.eo_count: Dict[str, int] = {p.name: 0 for p in self.EVENT_OUTPUTS}

        # ---- 内部状态区（子类自由使用；snapshot 会导出）----
        self.state: Dict[str, Any] = {}

        # ---- ECC 构建（子类可覆写 build_ecc 返回 None 表示不使用状态机）----
        self.ecc: Optional[ECC] = self.build_ecc()

        # ---- 运行时注入的输出事件回调（由 DistributedRuntime 设置）----
        self._emit_callback: Optional[Callable[[FunctionBlock, str, Optional[Dict]], None]] = None

        # ---- 执行互斥锁：保证同一FB的事件处理串行（原子执行语义）----
        self._lock = threading.RLock()

        # ---- 执行统计 ----
        self.exec_count = 0
        self.last_exec_ts = 0.0
        self.total_exec_ms = 0.0

        # ---- 应用外部参数（在线配置）----
        if params:
            self.configure(params)

    # ==========================================================================
    # 3.1 子类可覆写的钩子
    # ==========================================================================

    def build_ecc(self) -> Optional[ECC]:
        """
        构建执行控制链；子类覆写以返回 ECC 实例。
        缺省返回 None 表示该 FB 采用过程式 execute() 风格。
        """
        return None

    def execute(self, event_name: str) -> None:
        """
        过程式执行入口（未使用 ECC 的子类覆写此方法）。

        参数 event_name 为触发本次执行的事件输入名。
        """
        logger.debug("[%s] 基类execute被调用（事件=%s），子类应覆写", self.name, event_name)

    # ==========================================================================
    # 3.2 事件处理（运行时调用）
    # ==========================================================================

    def handle_event(self, event_name: str,
                     data: Optional[Dict[str, Any]] = None) -> bool:
        """
        事件输入统一入口（由分布式运行时在事件到达时调用）。

        执行序列（对应 IEC 61499 Basic FB 的算法）：
          1. 校验事件端口合法性；
          2. 按 WITH 关联把随行数据刷入数据输入端口；
          3. 驱动 ECC（若有）求值迁移并执行动作；否则调用 execute()；
          4. 记录执行统计。
        返回：事件是否被接受（未知事件返回 False 并告警）。
        """
        with self._lock:
            port = self._ei_ports.get(event_name)
            if port is None:
                logger.warning("[%s] 收到未声明的事件输入 '%s'，已忽略",
                               self.name, event_name)
                return False

            # ---- WITH 语义：事件随行数据刷新 ----
            self.ei_count[event_name] += 1
            if data:
                for key, value in (data or {}).items():
                    if key in self.di:
                        self.di[key] = value
                    else:
                        # 允许携带扩展字段（放入内部状态区，不报错）
                        self.state["_ext_" + key] = value

            # ---- 触发执行 ----
            t0 = time.perf_counter()
            try:
                if self.ecc is not None:
                    self.ecc.process_event(event_name, self)
                else:
                    self.execute(event_name)
            except Exception as exc:  # noqa: BLE001 FB异常不上抛，避免拖垮调度器
                logger.exception("[%s] 处理事件 %s 异常: %s",
                                 self.name, event_name, exc)
            finally:
                self.exec_count += 1
                self.last_exec_ts = time.time()
                self.total_exec_ms += (time.perf_counter() - t0) * 1000.0
            return True

    # ==========================================================================
    # 3.3 事件输出（子类在动作/算法中调用）
    # ==========================================================================

    def emit(self, event_name: str,
             data: Optional[Dict[str, Any]] = None) -> None:
        """
        发出输出事件。

        参数：
          event_name : 事件输出端口名（须在 EVENT_OUTPUTS 中声明）；
          data       : 随事件携带的数据（一般对应 WITH 关联的数据输出）。
        """
        port = self._eo_ports.get(event_name)
        if port is None:
            logger.warning("[%s] 尝试发出未声明的事件输出 '%s'，已忽略",
                           self.name, event_name)
            return
        self.eo_count[event_name] += 1
        # 合并数据到输出容器（保留 WITH 声明端口的最新值语义）
        if data:
            for key, value in data.items():
                if key in self.do:
                    self.do[key] = value
        if self._emit_callback is not None:
            try:
                self._emit_callback(self, event_name, data or {})
            except Exception as exc:  # noqa: BLE001
                logger.exception("[%s] 输出事件回调异常: %s", self.name, exc)

    # ==========================================================================
    # 3.4 在线参数配置
    # ==========================================================================

    def configure(self, params: Dict[str, Any]) -> List[str]:
        """
        在线修改功能块参数（只允许覆盖已声明的数据输入端口）。

        返回实际生效的参数名列表；未知参数将被忽略并告警。
        """
        applied: List[str] = []
        with self._lock:
            for key, value in params.items():
                if key in self.di:
                    self.di[key] = value
                    applied.append(key)
                else:
                    logger.warning("[%s] 忽略未知参数 '%s'", self.name, key)
        logger.info("[%s] 参数已更新: %s", self.name, applied)
        return applied

    # ==========================================================================
    # 3.5 描述与快照（Web 控制台使用）
    # ==========================================================================

    def describe(self) -> Dict[str, Any]:
        """导出功能块完整描述：端口表 + 当前参数 + 统计信息。"""
        return {
            "name": self.name,
            "type": self.fb_type,
            "ecc_state": self.ecc.current_state if self.ecc else "-",
            "event_inputs": [p.describe() for p in self.EVENT_INPUTS],
            "event_outputs": [p.describe() for p in self.EVENT_OUTPUTS],
            "data_inputs": [dict(p.describe(), value=self.di[p.name])
                            for p in self.DATA_INPUTS.values()],
            "data_outputs": [dict(p.describe(), value=self.do[p.name])
                             for p in self.DATA_OUTPUTS.values()],
            "exec_count": self.exec_count,
            "avg_exec_ms": round(self.total_exec_ms / self.exec_count, 3)
                           if self.exec_count else 0.0,
        }

    def snapshot(self) -> Dict[str, Any]:
        """轻量状态快照（心跳/状态同步报文使用）。"""
        return {
            "name": self.name,
            "type": self.fb_type,
            "ecc_state": self.ecc.current_state if self.ecc else "-",
            "params": dict(self.di),
            "exec_count": self.exec_count,
        }


# ==============================================================================
# 4. 示例功能块 + 模块自检
# ==============================================================================


class PumpFB(FunctionBlock):
    """
    示例：水泵控制功能块（演示 ECC 状态机用法）。

    状态：  STOPPED --START--> RUNNING --STOP--> STOPPED
    守卫：  START 事件仅在 "enable" 为真时被接受（演示 guard 用法）。
    """

    EVENT_INPUTS = [
        EventPort("START", with_inputs=["enable", "target_flow"], comment="启动水泵"),
        EventPort("STOP", comment="停止水泵"),
    ]
    EVENT_OUTPUTS = [
        EventPort("RUNNING", with_outputs=["flow"], comment="已进入运行态"),
        EventPort("STOPPED", comment="已停机"),
    ]
    DATA_INPUTS = {
        "enable": DataPort("enable", "BOOL", False, "使能开关"),
        "target_flow": DataPort("target_flow", "REAL", 0.0, "目标流量 m3/h"),
    }
    DATA_OUTPUTS = {
        "flow": DataPort("flow", "REAL", 0.0, "实际输出流量"),
    }

    def guard_enabled(self) -> bool:
        """守卫函数：仅当使能为真时允许 START 迁移。"""
        return bool(self.di["enable"])

    def action_start(self) -> None:
        """RUNNING 状态 entry 动作：设置输出流量并发出 RUNNING 事件。"""
        self.do["flow"] = float(self.di["target_flow"])
        self.emit("RUNNING", {"flow": self.do["flow"]})

    def action_stop(self) -> None:
        """STOPPED 状态 entry 动作：清零流量并发出 STOPPED 事件。"""
        self.do["flow"] = 0.0
        self.emit("STOPPED", {"flow": 0.0})

    def build_ecc(self) -> ECC:
        """构建水泵 ECC：两个状态、两条受守卫约束的迁移。"""
        ecc = ECC(initial_state="STOPPED")
        ecc.add_state(ECCState("STOPPED", entry_actions=["action_stop"],
                               comment="停机状态"))
        ecc.add_state(ECCState("RUNNING", entry_actions=["action_start"],
                               comment="运行状态"))
        ecc.add_transition(ECCTransition("STOPPED", "RUNNING",
                                         event="START", guard="guard_enabled",
                                         priority=1))
        ecc.add_transition(ECCTransition("RUNNING", "STOPPED",
                                         event="STOP", priority=1))
        return ecc


if __name__ == "__main__":
    # ---- 自检：演示事件驱动执行与 ECC 状态迁移 ----
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    events_out: List[tuple] = []

    pump = PumpFB("pump_1", params={"target_flow": 12.5})

    # 捕获输出事件（模拟运行时注入回调）
    pump._emit_callback = lambda fb, ev, data: events_out.append((ev, data))

    # 1) 未使能时 START 应被守卫拦截，状态保持 STOPPED
    pump.handle_event("START", {"enable": False, "target_flow": 12.5})
    assert pump.ecc.current_state == "STOPPED", "守卫应拦截未使能的启动"

    # 2) 使能后 START 迁移到 RUNNING，并发出 RUNNING 事件
    pump.handle_event("START", {"enable": True, "target_flow": 12.5})
    assert pump.ecc.current_state == "RUNNING"
    assert events_out[-1][0] == "RUNNING" and events_out[-1][1]["flow"] == 12.5

    # 3) STOP 迁移回 STOPPED
    pump.handle_event("STOP")
    assert pump.ecc.current_state == "STOPPED"
    assert events_out[-1][0] == "STOPPED"

    # 4) 在线参数修改 + 未知事件忽略
    pump.configure({"target_flow": 20.0})
    assert pump.di["target_flow"] == 20.0
    assert pump.handle_event("UNKNOWN_EVENT") is False

    print("功能块自检通过：事件输入/输出、WITH数据关联、ECC状态机、"
          "守卫迁移、在线参数配置均工作正常。")
    print("功能块描述导出示例:", pump.describe())
