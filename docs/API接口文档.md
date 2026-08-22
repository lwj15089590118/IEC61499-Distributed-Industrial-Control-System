# API 接口文档

**适用版本**：v1.0.0
**文档范围**：Web 控制台 RESTful API、MQTT 主题（Topic）定义、消息信封（Message）格式、共享内存通道文件契约。

---

## 1. 总览

系统对外接口分三类：

| 接口类别 | 载体 | 默认端点/目录 | 认证 |
|----------|------|---------------|------|
| REST API | HTTP (Flask) | `http://<host>:5000/api/...` | 无（仿真环境） |
| MQTT Topic | MQTT Broker（可选） | `factory/#`，QoS 1 | 无（仿真环境） |
| 共享内存通道 | 本机文件 | `./runtime/` | 文件系统权限 |

所有跨节点消息使用统一的 **Message JSON 信封**（见第 4 章），REST 返回体为 JSON，统一包含 `ok` 布尔字段（写接口）或数据字段（读接口）。

---

## 2. RESTful API

### 2.1 `GET /` — 控制台页面

返回暗色科技风仪表盘 HTML（拓扑卡片 / 事件流 / ECharts 图表 / 配置表单）。

### 2.2 `GET /api/topology` — 系统拓扑与节点状态

**用途**：节点状态卡片、集群健康汇总数据源。前端每 2 秒轮询。

**响应示例**：

```json
{
  "ts": 1755852000.123,
  "leader": "node_a",
  "nodes": [
    {
      "node": "node_a",
      "state": "online",            // online | offline | fault
      "role": "orchestrator",
      "label": "主控节点",
      "ip": "192.168.10.101",
      "hb_age_s": 0.31,             // 距最近心跳秒数（null=从未上线）
      "fb_count": 4,
      "epoch": 3,
      "fbs": [
        {"name": "order_manager", "type": "OrderManagerFB",
         "ecc": "Idle", "params": {"split_granularity": 1}}
      ]
    }
  ],
  "summary": {"online": 5, "offline": 0, "fault": 0}
}
```

**状态判定规则**：`fault`=状态槽显式 halted（故障注入宕机）；`offline`=心跳超过 3 秒未刷新或节点停止；`online`=其余。

### 2.3 `GET /api/events` — 实时事件流

**查询参数**：

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| limit | int | 100 | 返回最近 N 条（上限 500） |
| type | string | 空 | 按事件类型过滤（如 `TaskCompleted`） |

**响应示例**：

```json
{
  "count": 3,
  "events": [
    {"id": "a1b2c3d4", "type": "TaskCompleted", "source": "node_b",
     "topic": "factory/events", "priority": "NORMAL",
     "payload": {"task_id": "T000012", "action": "CARRY", "cycles": 7},
     "ts": 1755852000.456},
    {"id": "e5f6a7b8", "type": "FailoverTriggered", "source": "node_e",
     "topic": "factory/failover", "priority": "CRITICAL",
     "payload": {"took_ms": 1.42, "epoch": 2, "recovered_tasks": 31},
     "ts": 1755851998.001}
  ]
}
```

事件按时间倒序（最新在前）。

### 2.4 `GET /api/fbs` — 功能块目录（配置面板数据源）

**响应示例**：

```json
{
  "catalog": [
    {"node": "node_d", "state": "online",
     "fbs": [
       {"name": "classifier", "type": "DefectClassifierFB", "ecc": "-",
        "params": {"ng_threshold": 0.65, "classes": ["划痕", "凹陷"]}}
     ]}
  ]
}
```

节点在线时返回**实时**参数（来自状态槽快照）；离线时回退到 `nodes.yaml` 静态声明。

### 2.5 `POST /api/fb/config` — 功能块参数在线配置

**请求体**：

```json
{
  "node": "node_d",
  "fb": "classifier",
  "params": {"ng_threshold": 0.55}
}
```

**行为**：向 `factory/config/node_d` 发布 `FBConfigUpdated` 消息 → 目标节点运行时应用参数 → 回执 `FBConfigUpdated` 到 `factory/events`（含 applied 字段）。

**响应**：

```json
{"ok": true, "node": "node_d", "fb": "classifier",
 "params": {"ng_threshold": 0.55},
 "message": "配置已下发，生效回执见事件流"}
```

**错误**（HTTP 400）：`{"ok": false, "error": "参数不完整（node/fb/params 必填）"}`

### 2.6 `POST /api/task/dispatch` — 手动触发任务下发

**请求体**：

```json
{
  "product": "精密齿轮",     // 电机外壳|铝支架|传感器面板|精密齿轮
  "quantity": 3,            // 1~100
  "priority": 1             // 0特急 1加急 2常规 3缓单
}
```

**行为**：生成 `WEB-HHMMSS-XXXX` 订单号，向 `factory/web/orders` 发布 `OrderReceived`，由主控（或接管态热备）分解派发。

**响应**：

```json
{"ok": true,
 "order": {"order_id": "WEB-122017-4174", "product": "精密齿轮",
           "quantity": 3, "deadline": 300.0, "priority": 1},
 "message": "订单已下发至 factory/web/orders"}
```

### 2.7 `GET /api/metrics` — 实时性能数据（ECharts）

**响应字段**：

| 字段 | 说明 |
|------|------|
| rates | 每秒各节点事件数序列（最近 120 秒） |
| sources | 出现过的消息来源列表 |
| latency | `{avg_ms, p95_ms, max_ms}` 事件端到端时延 |
| dist | 事件类型分布（Top10，饼图） |
| tasks | `{completed, failed}` 任务累计 |
| quality | `{ok, ng}` 视觉质检统计 |
| buffered | 事件缓冲区条数 |

### 2.8 `GET /api/overview` — 头部汇总

```json
{
  "summary": {"online": 5, "offline": 0, "fault": 0},
  "leader": "node_a",
  "total_events": 1523,
  "recent_alert": {"type": "NodeOffline",
                   "payload": {"node": "node_b"}, "ts": 1755851990.1},
  "ts": 1755852000.5
}
```

---

## 3. MQTT 主题（Topic）定义

主题命名规范：`factory/<域>/<子项>[/<节点ID>]`。通配符 `+`（单层）/ `#`（多层）与 MQTT 规范一致。

### 3.1 主题总表

| 主题 | 方向 | QoS | 事件类型 | 发布者 → 订阅者 | 说明 |
|------|------|-----|----------|-----------------|------|
| `factory/orders` | 发布 | 1 | OrderReceived | 模拟器 → 主控/热备 | 订单入口 |
| `factory/web/orders` | 发布 | 1 | OrderReceived | Web控制台 → 主控/热备 | 手动下单 |
| `factory/tasks/{node}` | 发布 | 1 | TaskDispatched | 主控/热备 → 目标节点 | 任务派发（携带 epoch） |
| `factory/events` | 发布 | 1 | TaskStarted / TaskProgress / TaskCompleted / TaskFailed / VisionResult / ConveyorStatus / ServoInPosition / CycleDone / CalibrationDone / TrajectoryPlanned / FBConfigUpdated | 工作节点 → 主控 | 通用事件回流 |
| `factory/heartbeat/{node}` | 发布 | 1 | Heartbeat | 各节点 → 主控/热备/Web | 节点心跳（HIGH） |
| `factory/vision/results` | 发布 | 1 | VisionResult | 视觉节点 → 主控 | 质检结果 |
| `factory/sync/{node}` | 发布 | 1 | StateSync | 主控 → 热备 | 队列快照同步（热备数据源） |
| `factory/failover` | 发布 | 1 | FailoverTriggered | 热备 → 全集群 | 热切换通告（CRITICAL） |
| `factory/config/{node}` | 发布 | 1 | FBConfigUpdated | Web控制台 → 目标节点 | 参数在线下发 |
| `factory/alerts` | 发布 | 1 | Alert / NodeOffline / TaskFailed | 主控 → 全集群 | 告警 |
| `factory/#` | 订阅 | 1 | — | 调试/Web | 全量订阅（诊断用） |

### 3.2 节点订阅矩阵（默认配置）

| 节点 | 订阅主题 |
|------|----------|
| node_a | factory/orders/#, factory/web/orders, factory/events, factory/heartbeat/#, factory/vision/results, factory/alerts, factory/config/node_a, factory/failover |
| node_b | factory/tasks/node_b, factory/config/node_b |
| node_c | factory/tasks/node_c, factory/config/node_c |
| node_d | factory/tasks/node_d, factory/config/node_d |
| node_e | factory/heartbeat/node_a, factory/sync/node_a, factory/tasks/#, factory/events, factory/config/node_e, factory/orders/# |

---

## 4. 消息信封（Message JSON）

所有 MQTT 载荷与共享内存事件流行均为如下单行 JSON：

```json
{
  "msg_id": "9f8e7d6c5b4a3210fedcba9876543210",
  "event_type": "TaskDispatched",
  "source": "node_a",
  "target": "node_b",
  "topic": "factory/tasks/node_b",
  "payload": { "...": "事件随行数据（WITH 关联）" },
  "timestamp": 1755852000.123456,
  "priority": 2,
  "seq": 1042,
  "scope": "global",
  "epoch": 3
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| msg_id | string | 全局唯一（uuid4 hex），双通道去重键 |
| event_type | string | 事件类型名（见 4.1 枚举表） |
| source / target | string | 发布者 / 目标节点（`*` 广播） |
| topic | string | 路由主题 |
| payload | object | 随行数据 |
| timestamp | float | Unix 秒（毫秒精度） |
| priority | int | 0=CRITICAL 1=HIGH 2=NORMAL 3=LOW |
| seq | int | 发布进程内单调序号（同优先级 FIFO） |
| scope | string | `local` 仅节点内 / `global` 跨节点 |
| epoch | int | 领导者纪元（fencing token；旧纪元派发指令会被拒绝） |

### 4.1 事件类型（EventType）枚举

| 类型 | 触发场景 | 典型 payload 字段 |
|------|----------|-------------------|
| OrderReceived | 模拟器/Web 下单 | order_id, product, quantity, deadline, priority |
| OrderSplit | 主控订单分解完成 | order_id, tasks[], total |
| TaskDispatched | 任务派发 | task_id, order_id, action, target_node, params, priority, attempts, epoch |
| TaskStarted | 执行节点开工 | task_id, action |
| TaskProgress | 执行进度 | task_id, progress(%) |
| TaskCompleted | 任务成功 | task_id, action, node, (verdict/actual_mm/position…) |
| TaskFailed | 任务最终失败 | task_id, reason |
| Heartbeat | 节点心跳 | node, role, fb_count, leader_epoch |
| NodeOnline / NodeOffline | 节点恢复/失联 | node, role, last_seen |
| Alert | 告警 | message / node / reason |
| VisionResult | 质检判定 | task_id, verdict(OK/NG/RECHECK), defect, confidence |
| CalibrationDone | 标定完成 | task_id, rmse, matrix, accepted |
| TrajectoryPlanned | 规划完成 | task_id, segments[], total_time |
| ConveyorStatus | 传送带状态 | running, position, speed |
| ServoInPosition | 伺服到位 | task_id, actual_mm, error_mm, within_tol |
| CycleDone | 气缸循环完成 | task_id, direction, b1, b2 |
| FailoverTriggered | 热备接管 | took_ms, epoch, recovered_tasks, old_master, new_master |
| StateSync | 主控状态快照 | epoch, queue[], dispatched, completed, failed, online_nodes[] |
| FBConfigUpdated | 参数配置/回执 | fb, params / node, fb, applied[] |
| SystemStatus | 系统状态汇总 | — |

---

## 5. 共享内存通道文件契约（`runtime/` 目录）

| 文件 | 写者 | 读者 | 契约 |
|------|------|------|------|
| `bus.jsonl` | 所有节点/模拟器/Web（FileLock 互斥追加） | 所有节点 watcher | 每行一个 Message JSON；读方记录偏移量增量消费；文件截断时从头重读 |
| `commands.jsonl` | 外部工具 | 所有节点 watcher | 命令消息流（Message 格式） |
| `status_<node>.json` | 各节点运行时 | Web/诊断工具 | 节点状态槽；临时文件 + 原子 rename 写入；含 fbs 参数快照 |
| `leader_lease.json` | 主控/热备 | 全集群 | 领导者租约 `{leader, epoch, acquired_at, expires_at}`；过期可竞争，force 接管 epoch+1 |
| `fault_directives.json` | 故障注入器 | 各节点（500ms 轮询） | `{node: {halted, delay_ms, drop_rate, until}}`；until 为毫秒时间戳，过期自动失效 |
| `logs/bus_<node>.jsonl` | 各节点事件总线 | 重放工具 | 节点自身消息持久化日志（EventBus.replay 用） |

### 5.1 故障注入指令字段

| 字段 | 类型 | 效果 |
|------|------|------|
| halted | bool | 节点停止心跳、状态槽标记 halted（UI 显示故障） |
| delay_ms | int | 该节点外发消息延迟 N 毫秒 |
| drop_rate | float [0,1] | 该节点外发消息按概率丢弃 |
| until | float(ms) | 指令过期时间戳 |

---

## 6. 错误码与约定

- REST 写接口失败返回 HTTP 400/500，`ok=false`，`error` 为中文可读信息；
- 节点收到未知事件类型：忽略并记录 WARNING 日志（不中断）；
- 节点收到旧纪元（`epoch` 小于本地已见值）的 TaskDispatched：拒绝执行并告警（防脑裂）；
- 功能块收到未声明的事件输入 / 未知参数：忽略并记录 WARNING（在线配置只接受已声明的数据输入端口名）。
