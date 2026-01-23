# Fustor 平台架构

本文档描述了 Fustor Monorepo 的顶层设计哲学、核心服务交互以及关键内部机制。

## 1. 核心设计原则

*   **Monorepo**: 所有服务和共享库统一管理，简化依赖和版本控制。
*   **服务化**: 功能被拆分为独立的 Registry, Fusion, Agent 等服务。
*   **可扩展性**: 通过插件化驱动（Driver）机制，轻松扩展对新数据源和目标的支持。

## 2. 服务模块概览

*   **Registry 服务**:
    *   **职责**: 平台的控制平面。负责元数据管理、存储环境、数据存储库 (Datastore)、用户、API Key 和凭据管理。
    *   **核心模块**: `API`, `Database`, `Models`, `Security`。

*   **Fusion 服务**:
    *   **职责**: 数据的摄取与处理核心。负责从 Agent 接收数据，维护实时目录树，并提供查询接口。
    *   **核心模块**: `API` (数据摄取), `Processors` (数据处理), `Models`。
    *   **一致性仲裁**: 通过 Tombstone、Suspect List、Blind-spot List 实现多源数据的智能合并。

*   **Agent 服务**:
    *   **职责**: 轻量级、可扩展的实时数据供给工具。监听数据源变更，并根据 Fusion 的需求将数据推送到指定端点。
    *   **核心模块**: `API`, `Services` (业务逻辑), `Models`, `Drivers` (扩展核心)。
    *   **Leader/Follower 模式**: 采用"先到先得"的 Leader 选举机制，确保同一数据源只有一个 Agent 执行 Snapshot/Audit。

*   **Packages**:
    *   **职责**: 包含所有可共享的库和插件。
    *   `fustor_common`: 通用工具和模型。
    *   `fustor_core`: 驱动接口定义等核心抽象。
    *   `fustor_event_model`: 事件模型定义，包含 `message_source` 字段。
    *   `source_*` / `pusher_*`: 具体的源和目标驱动实现。

## 3. 关键架构模式

### 3.1. "瘦 Agent 感知 + 胖 Fusion 裁决"

Agent 负责数据采集和初步过滤，Fusion 负责最终的一致性仲裁：

*   **Agent 职责**: Realtime Sync, Snapshot Sync, Audit Sync, Sentinel Sweep
*   **Fusion 职责**: 维护内存树，执行 Smart Merge，管理 Tombstone/Suspect/Blind-spot List

详见 [一致性设计 (CONSISTENCY_DESIGN.md)](./CONSISTENCY_DESIGN.md)

### 3.2. Agent: "消息优先，并发快照" 模型

为兼顾最低的实时延迟和历史数据恢复能力，Agent 的所有同步任务均遵循此模型：

1.  **消息优先 (Message-First)**: 任务启动后，总是**立即**进入消息同步阶段，以最低延迟开始处理实时数据流。
2.  **并发补充快照 (Concurrent Snapshot Backfill)**: 历史数据的全量同步（快照）被设计成一个**补充性质的、并发的**后台任务。

### 3.3. Agent: Leader/Follower 模式

| 角色 | Realtime Sync | Snapshot Sync | Audit Sync | Sentinel Sweep |
|------|---------------|---------------|------------|----------------|
| **Leader** | ✅ | ✅ | ✅ | ✅ |
| **Follower** | ✅ | ❌ | ❌ | ❌ |

*   **先到先得**: 第一个建立 Session 的 Agent 成为 Leader
*   **故障转移**: 仅当 Leader 心跳超时后，Fusion 才释放 Leader 锁

### 3.4. Agent: 内置背压与内存管理

*   **背压处理**: 当数据推送速度跟不上数据生产速度时，事件总线会自动阻塞生产者，防止因缓冲区溢出导致的数据丢失。
*   **自动内存管理**：事件总线具备垃圾回收机制，能够自动清理已被所有订阅者消费过的事件数据。
