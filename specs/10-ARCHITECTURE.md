# Fustor 架构设计 V2

> 版本: 2.0.0  
> 日期: 2026-02-01  
> 状态: 设计中

## 1. 设计目标

### 1.1 核心原则

1. **完全松耦合**: Agent 和 Fusion 完全独立，第三方可单独使用任一端
2. **对称架构**: Agent 与 Fusion 的概念一一对应
3. **分层清晰**: 参考 Netty 架构，职责分离
4. **可扩展**: 支持多协议、多 Schema

### 1.2 术语对称表

| Agent 概念 | 职责 | Fusion 对应 | 职责 |
|-----------|------|------------|------|
| **Source** | 数据读取实现 (Handler Layer 3) | **View** | 数据处理实现 (Handler Layer 3) |
| **Sender** | 传输通道 (Transport Layer 2) | **Receiver** | 传输通道 (Transport Layer 2) |
| **Pipeline** | 运行时绑定 (Source→Sender) | **Pipeline** | 运行时绑定 (Receiver→View) |

---

## 2. 分层架构

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Fustor 分层架构                                          │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   Layer 5: Application (应用层)                                                       │
│   ┌─────────────────────────────┐       ┌─────────────────────────────┐             │
│   │     fustor-agent            │       │     fustor-fusion           │             │
│   └──────────────┬──────────────┘       └──────────────┬──────────────┘             │
│                  │                                      │                            │
│   Layer 4: Pipeline Engine (管道引擎层)                                               │
│   ┌──────────────▼──────────────┐       ┌──────────────▼──────────────┐             │
│   │     Pipeline Manager        │       │     Pipeline Manager        │             │
│   │     (调度 Source→Sender)    │       │     (调度 Receiver→View)    │             │
│   └──────────────┬──────────────┘       └──────────────┬──────────────┘             │
│                  │                                      │                            │
│   Layer 3: Handler (处理器层) - fustor-source-*, fustor-view-*                        │
│   ┌──────────────▼──────────────┐       ┌──────────────▼──────────────┐             │
│   │     Source Handler          │       │     View Handler            │             │
│   │     (读取数据，产生事件)     │       │     (消费事件，更新视图)     │             │
│   └──────────────┬──────────────┘       └──────────────┬──────────────┘             │
│                  │                                      │                            │
│   Layer 2: Transport (传输层) - fustor-sender-*, fustor-receiver-*                    │
│   ┌──────────────▼──────────────┐       ┌──────────────▼──────────────┐             │
│   │     Sender (HTTP/gRPC)      │══════▶│     Receiver (HTTP/gRPC)    │             │
│   └─────────────────────────────┘Network└─────────────────────────────┘             │
│                                                                                      │
│   Layer 1: Core (核心层) - core (原 fustor-core)                                      │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  Pipeline 抽象 │ Event 模型 │ Transport 抽象 │ LogicalClock │ Common Utils  │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
│   Layer 0: Schema (契约层) - fustor-schema-*                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  fustor-schema-fs (Pydantic 模型) │ fustor-schema-* (第三方可扩展)           │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

> **设计决策**: 虽然 Source 和 View 逻辑上是数据的起点和终点，但在架构上它们作为 `Handler` 插件运行在 Pipeline Engine 内部。Pipeline 负责生命周期调度，Handler 负责具体业务逻辑。这种反转控制保证了架构的统一性。

---

## 3. 包结构

### 3.1 核心包

```
/
├── core/                            # 核心抽象层 (原 fustor-core)
│   └── src/fustor_core/
│       ├── common/                  # 通用工具
│       │   ├── logging.py
│       │   ├── daemon.py
│       │   ├── paths.py
│       │   └── utils.py
│       ├── event/                   # 事件模型
│       │   ├── base.py              # EventBase
│       │   └── types.py             # EventType, MessageSource
│       ├── pipeline/                # Pipeline 抽象
│       │   ├── pipeline.py          # Pipeline ABC
│       │   ├── context.py           # PipelineContext
│       │   └── handler.py           # Handler ABC (Source/View 基类)
│       ├── transport/               # 传输抽象
│       │   ├── sender.py            # Sender ABC
│       │   └── receiver.py          # Receiver ABC
│       ├── clock/                   # 时钟接口 (具体实现可由 View 覆盖)
│       │   └── logical_clock.py
│       ├── config/                  # 配置模型
│       │   └── models.py
│       └── exceptions.py
│
├── packages/                        # 第三方扩展与实现
│   ├── fustor-agent-sdk/            # Agent 开发 SDK
│   ├── fustor-fusion-sdk/           # Fusion 开发 SDK
```

### 3.2 Schema 包

```
packages/
├── fustor-schema-fs/                # 文件系统 Schema
│   └── src/fustor_schema_fs/
│       ├── __init__.py
│       ├── event.py                 # FSEventRow (Pydantic 模型)
│       └── version.py               # SCHEMA_NAME, SCHEMA_VERSION
```

### 3.3 Handler 包 (Source/View)

```
packages/
├── fustor-source-fs/                # FS Source Driver
├── fustor-source-oss/               # OSS Source Driver
├── fustor-view-fs/                  # FS View Driver (含一致性逻辑)
│   └── src/fustor_view_fs/
│       ├── handler.py               # FSViewHandler
│       ├── arbitrator.py            # 一致性仲裁 (fs 特有)
│       ├── state.py                 # Suspect, Blind-spot, Tombstone
│       └── nodes.py                 # 内存树节点
```

### 3.4 Transport 包 (Sender/Receiver)

```
packages/
├── fustor-sender-http/              # HTTP Sender
├── fustor-sender-grpc/              # gRPC Sender
├── fustor-receiver-http/            # HTTP Receiver
├── fustor-receiver-grpc/            # gRPC Receiver
```

### 3.5 应用包

```
agent/                               # fustor-agent
fusion/                              # fustor-fusion
```

---

## 4. 组件关系

### 4.1 Agent 侧关系基数

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                      │
│   Agent 侧 (N:1:1:N)                                                                 │
│   ──────────────────                                                                 │
│                                                                                      │
│   Source (1) ◀─── Pipeline (N) ───▶ Sender (1)                                       │
│                                                                                      │
│   约束:                                                                              │
│   1. 一个 Pipeline 只能绑定 1 个 Source 和 1 个 Sender。                             │
│   2. 一个 Source 可以被多个 Pipeline 复用 (如不同 Pipeline 发往不同 Sender)。        │
│   3. 一个 Sender 可以被多个 Pipeline 复用 (多路复用传输)。                           │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Fusion 侧关系基数

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                      │
│   Fusion 侧 (1:N:1:N)                                                                │
│   ───────────────────                                                                │
│                                                                                      │
│   Receiver (1) ◀─── Pipeline (N) ───▶ View (1)                                       │
│                                                                                      │
│   约束:                                                                              │
│   1. 一个 Pipeline 只能绑定 1 个 Receiver 和 1 个 View。                             │
│   2. 一个 Receiver 可以服务多个 Pipeline (接收不同来源的数据)。                      │
│   3. 一个 View 可以接收多个 Pipeline 的数据 (多源汇聚，如 Agent-A + Agent-B)。       │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 消息同步机制 (Bus Only)

Agent Pipeline 统一使用 **EventBus 模式** 实现高吞吐消息同步。不再支持直连 Driver 模式。

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Agent 消息同步架构                                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   ┌──────────────┐         ┌─────────────────┐         ┌──────────────┐             │
│   │  FS Watch    │────────▶│    EventBus     │────────▶│  Pipeline    │──▶ Fusion   │
│   │   Thread     │  put()  │   (MemoryBus)   │get()    │  Consumer    │             │
│   └──────────────┘         └─────────────────┘         └──────────────┘             │
│         │                         │                                                  │
│         │                    ┌────┴────┐                                             │
│         │               subscriber1  subscriber2                                     │
│       异步入队              (Pipeline-A)  (Pipeline-B)                                │
│       (不阻塞)                                                                        │
│                                                                                      │
│                                                                                      │
│   特性:                                                                              │
│   1. 生产者-消费者完全解耦 (Source 产生事件不被推送阻塞)                                │
│   2. 200ms 轮询超时 (低负载时延迟 ~0ms, 最坏 200ms)                                   │
│   3. 批量获取已有事件 (有多少取多少, 不等待凑满 batch)                                  │
│   4. 同源 Pipeline 共享 Bus (节省资源, 减少重复读取)                                   │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. 配置管理 (No-DB)

### 5.1 配置原则
- **文件存储**: 所有配置位于 `$FUSTOR_HOME/config/` 下的 YAML 文件中。
- **类型识别**: 文件内容第一行 Key 定义配置类型 (sources, receivers, pipelines 等)。
- **无状态**: 应用启动时加载所有配置，运行时无内部数据库。
- **Hot Patch**: 通过 `add-config` / `del-config` 命令动态增删文件并重载。

### 5.2 配置格式示例

#### Source 配置
```yaml
sources:
  fs-research:
    driver: fs
    uri: /data/research
    enabled: true
    driver_params:
      throttle_interval_sec: 1.0
```

#### Receiver 配置
```yaml
receivers:
  http-main:
    driver: http
    bind: 0.0.0.0
    port: 8102
    credential:
      key: fk_research_key
    driver_params:
      max_request_size_mb: 16
```

#### View 配置
```yaml
views:
  view-research:
    driver: fs
    enabled: true
    hot_file_threshold: 30.0
    # blind_spot 是 fs driver 的内部逻辑，无需显式配置类型，由 driver 代码控制
```

#### Agent Pipeline 配置
```yaml
agent-pipelines:
  pipe-agent-1:
    source: fs-research
    sender: http-fusion
    enabled: true
    audit_interval_sec: 600
```

#### Fusion Pipeline 配置
```yaml
fusion-pipelines:
  pipe-fusion-1:
    receiver: http-main
    view: view-research  # 1:1 绑定
    enabled: true
    session_timeout_seconds: 30
```


每个订阅者独立跟踪消费进度：
- `last_consumed_index`: 已消费的最后一个事件索引
- `low_watermark`: 所有订阅者中最慢的位置 (用于缓冲区清理)

#### 4.3.3 EventBus 自动分裂

当快慢消费者差距过大时，自动分裂：

```
分裂触发条件: 最快消费者领先最慢消费者 >= 95% 缓冲区容量

分裂前:
  EventBus-1 [容量 1000]
    ├── Pipeline-A: index=900 (快)
    └── Pipeline-B: index=50  (慢)
    差距 = 850 events >= 95% × 1000 = 950? → 触发!

分裂后:
  EventBus-1 (保留) ── Pipeline-B (慢消费者)
  EventBus-2 (新建) ── Pipeline-A (快消费者)


## 5. Session 设计

### 5.1 Session 定义

Session 是 **Agent Pipeline** 和 **Fusion Pipeline** 之间的业务会话。

### 5.2 Session 数据结构

```python
@dataclass
class Session:
    session_id: str                    # 唯一会话 ID
    agent_task_id: str                 # Agent Pipeline 的 task_id
    fusion_pipeline_id: str            # Fusion Pipeline ID
    
    # 生命周期
    created_at: datetime
    last_active_at: datetime
    timeout_seconds: int               # 从 Pipeline 配置获取
    
    # 状态追踪
    latest_event_index: int            # 断点续传 (Pipeline 级别)
    
    # 认证
    receiver_id: str                   # 使用的 Receiver
    client_ip: str
```

### 5.3 Session 生命周期

```
Agent Pipeline 启动
    │
    ├── Sender.connect() ────────────────────▶ Receiver 验证 API Key
    │   POST /api/v1/pipe/sessions/              │
    │   {task_id: "..."}                         ▼
    │                                       Fusion Pipeline 创建 Session
    │                                            │
    │◀── 200 {session_id, timeout_seconds} ─────┤
    │                                            │
    ▼                                            ▼
事件推送 (携带 session_id)                    事件处理
心跳 (间隔 = timeout_seconds / 2)            刷新 last_active_at
    │                                            │
    ▼                                            ▼
Pipeline 停止 或 网络断开                     Session 超时检测
    │                                            │
    └── DELETE /sessions/{id} ──────────────────▶│ View.on_session_close()
                                                 │ View 自行决定状态处理
                                                 │ (live 类型清空，否则保留)
```

---

## 6. 一致性设计

### 6.1 组件层级

| 组件 | 层级 | 说明 |
|------|------|------|
| **LogicalClock** | View 级别 | 通用时间仲裁，不依赖特定 Schema |
| **Leader/Follower** | View 级别 | fs 特有，仅 view-fs 实现 |
| **审计周期** | View 级别 | fs 特有，由一致性方案决定哪个 Session 审计 |
| **Suspect/Blind-spot/Tombstone** | View 级别 | fs 特有，仅 view-fs 实现 |

> **与 L0 Goals 的关联**: Suspect 机制直接支撑 **Integrity Guarantee** (00-GOALS §1.2)：未经稳定性验证的文件必须标记为 `integrity_suspect=True`，确保 API 用户知晓数据可能不完整。

### 6.2 多 Session 并发写入

同一 View 接收多个 Session 的事件时，使用 LogicalClock 仲裁：
- 比较事件的 mtime
- 更新的事件覆盖旧事件
- fs 特有的 Leader/Follower 逻辑在 view-fs 中实现

---

### 8.2 Session 创建响应

```json
{
  "session_id": "sess_xxx",
  "timeout_seconds": 30,
}
```

Agent 收到响应后，设置心跳间隔为 `timeout_seconds / 2`。