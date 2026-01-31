# Fustor 平台架构

本文档描述了 Fustor Monorepo 的顶层设计哲学、核心服务交互以及关键内部机制。

## 1. 核心设计原则

*   **Monorepo**: 所有服务和共享库统一管理，简化依赖和版本控制。
*   **去中心化配置**: YAML 驱动的分布式架构，部署灵活。
*   **服务化**: 功能被拆分为独立的 Fusion 和 Agent 服务。
*   **可扩展性**: 通过插件化驱动（Driver）机制，轻松扩展对新数据源和目标的支持。

## 2. 服务模块概览

*   **Fusion 服务**:
    *   **职责**: 数据的摄取、处理与视图提供。负责从 Agent 接收数据，维护实时目录树，并提供查询接口。
    *   **配置**: 通过 `datastores-config.yaml` 管理数据源凭据。
    *   **核心模块**: `API` (数据摄取), `Processors` (数据处理), `View Manager` (视图呈现)。
    *   **一致性仲裁**: 通过 Tombstone、Suspect List、Blind-spot List 实现多源数据的智能合并。

*   **Agent 服务**:
    *   **职责**: 轻量级、可扩展的实时数据采集工具。监听数据源变更，并将数据推送到指定 Fusion 端点。
    *   **配置**: 通过 `agent-config.yaml` 或 `syncs-config/*.yaml` 管理采集任务。
    *   **核心模块**: `API`, `Services` (业务逻辑), `Drivers` (扩展核心)。
    *   **Leader/Follower 模式**: 采用"先到先得"的 Leader 选举机制，确保同一数据源只有一个 Agent 执行 Snapshot/Audit。

*   **Packages**:
    *   **职责**: 包含所有可共享的库和插件。
    *   `fustor_common`: 通用工具和模型。
    *   `fustor_core`: 驱动接口定义等核心抽象。
    *   `fustor_event_model`: 事件模型定义。
    *   `source_*` / `pusher_*`: 具体的源和目标驱动实现。

## 3. 关键架构模式

### 3.1. "瘦 Agent 感知 + 胖 Fusion 裁决"

Agent 负责数据采集和初步过滤，Fusion 负责最终的一致性仲裁：

*   **Agent 职责**: Realtime Sync, Snapshot Sync, Audit Sync, Sentinel Sweep
*   **Fusion 职责**: 维护内存树，执行 Smart Merge，管理 Tombstone/Suspect/Blind-spot List

详见 [一致性设计 (CONSISTENCY_DESIGN.md)](./CONSISTENCY_DESIGN.md)

### 3.2. Agent: Leader/Follower 模式

| 角色 | Realtime Sync | Snapshot Sync | Audit Sync | Sentinel Sweep |
|------|---------------|---------------|------------|----------------|
| **Leader** | ✅ | ✅ | ✅ | ✅ |
| **Follower** | ✅ | ❌ | ❌ | ❌ |

*   **先到先得**: 第一个建立 Session 的 Agent 成为 Leader。
*   **故障转移**: 基于心跳机制，一旦 Leader 失效，其他 Agent 可竞争成为新 Leader。

### 3.3. 驱动扩展机制

Fustor 支持通过实现标准接口（`SourceInterface`, `PusherInterface`）来扩展功能。这使得平台能够轻松接入各种异构存储系统。
