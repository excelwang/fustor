# Fustor Agent Consumer 开发指南

## 1. 引言

本指南旨在帮助开发者构建一个可以作为 Fustor Agent 数据接收端的后端服务（以下称为“Consumer”）。一个合格的 Consumer 服务必须实现一套特定的API接口和交互逻辑，以便与 Fustor Agent 的 `sender_openapi` 驱动进行高效、可靠的数据同步，尤其是在涉及多个 Fustor Agent 实例的分布式部署场景中。

Fustor Agent 采用了“消息优先，消费者驱动并发快照”的先进架构。在此模型下，Consumer 不仅是被动的数据接收者，更是整个同步流程的“指挥者”之一，负责按需请求历史数据快照，并处理可能出现的重复数据。

---

## 2. 核心交互协议：信封协议 (Envelope Protocol)

Fustor Agent 的 `sender_openapi` 驱动与 Consumer 之间的所有数据推送，都遵循一个统一的“信封”JSON结构。您的批量接收端点必须能接收和处理这种格式的 `POST` 请求。

### 推送请求体 (Request Body)

```json
{
  "agent_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "task_id": "your_task_id",
  "snapshot_sync_suggested": false,
  "is_snapshot_end": false,
  "events": [
    { "field1": "value1", "field2": 123 },
    { "field1": "value2", "field2": 456 }
  ]
}
```

#### 字段详解

*   `agent_id` (`string`): **必需**。推送数据的 Fustor Agent 实例的全局唯一、持久化ID。Fustor Agent 应用在首次启动时会自动生成并保存此ID。

*   `task_id` (`string`): **必需**。当前正在推送数据的Pipeline 任务的唯一ID。`agent_id` 和 `task_id` 的**组合**，才能在分布式环境中唯一标识一个数据流。

*   `events` (`Array<Object>`): **必需**。一个事件数据对象的数组。每个对象就是一条需要被处理的数据记录。

*   `snapshot_sync_suggested` (`boolean`): **必需**。一个“建议”标志。当 Fustor Agent 的源驱动发生回退（例如，无法从请求的点位开始）时，此标志会被设为 `true`。这是一个明确的信号，**建议**您的 Consumer 应用请求一次快照同步。

*   `is_snapshot_end` (`boolean`): **必需**。一个“快照结束”标志。当一个后台的补充快照任务完成时，Fustor Agent会发送一个不包含 `events` 的请求，并将此标志设为 `true`。

### 推送响应体 (Response Body)

您的批量接收端点在成功处理数据后，其 HTTP 响应体**必须**是一个包含以下字段的JSON对象：

```json
{
  "snapshot_needed": false 
}
```

#### 字段详解

*   `snapshot_needed` (`boolean`): **必需**。这是 Consumer 向 Fustor Agent 发起快照请求的**唯一途径**。
    *   当您的应用逻辑决定需要一次全量数据回填时（例如，在收到 `snapshot_sync_suggested: true` 的建议后），就在响应中将此字段设为 `true`。
    *   在其他所有正常情况下，都应将其设为 `false`。

---

## 3. 必需实现的API端点

一个合格的 Consumer 服务必须实现以下两个API端点。

### 3.1. 批量数据接收端点

这是系统最核心的数据入口。

*   **路径**: 任意，但推荐使用 `/ingest-batch`。
*   **方法**: `POST`
*   **请求体**: 必须接受上文定义的“信封”JSON结构。
*   **响应体**: 必须返回包含 `snapshot_needed` 标志的JSON对象。
*   **行为**: 端点需要遍历 `events` 数组，并将数据持久化。推荐使用 `UPSERT` 操作以实现幂等性。

### 3.2. 状态与检查点端点

此端点是 Fustor Agent 实现断点续传和任务恢复的关键。

*   **路径**: 任意。您需要在您的服务的 OpenAPI 规范中，通过一个自定义字段 `x-fuagent-status-endpoint` 来声明此端点的路径。
*   **方法**: `GET`
*   **请求参数**: Fustor Agent 会通过查询参数 `?agent_id=...&task_id=...` 来指定要查询的任务。
*   **响应体**: 必须直接返回一个JSON格式的整数，代表该 `(agent_id, task_id)` 组合最后成功处理的事件的 `index`。
    *   如果从未处理过该组合的任何数据，**必须返回 `0`**。

---

## 4. 核心逻辑要求

### 4.1. 幂等性 (Idempotency)

由于“消息优先”架构可能会导致快照数据和实时数据并发推送，您的 `ingest-batch` 端点**必须**被设计为幂等的。通常这意味着您需要根据数据中的某个唯一标识符（Primary Key）来执行 `UPSERT` 操作，而不是简单的 `INSERT`。

### 4.2. 检查点持久化 (Checkpointing)

您的服务是“真理之源”。在每次成功处理一批事件后，您**必须**将这批事件中最新的 `index`，与 `(agent_id, task_id)` 这个**复合键**关联起来，并将其持久化存储。这是 `GET /pipeline-status` 端点的数据来源。

## 5. 认证

所有被 Fustor Agent 调用的端点都**必须**使用相同的认证方案。`sender_openapi` 驱动支持标准的 `Basic Auth` 和 `Bearer Token` (API Key) 认证。

## 6. 完整交互流程示例

1.  **启动**: Fustor Agent (Agent ID: `agent-A`) 启动一个新任务 (`task-1`)，首先调用 `GET /pipeline-status?agent_id=agent-A&task_id=task-1`，得到 `0`。
2.  **消息阶段**: Fustor Agent 开始向 `POST /ingest-batch` 推送包含 `agent_id: "agent-A"` 和 `task_id: "task-1"` 的实时事件。
3.  **源端回退**: Fustor Agent 的源驱动发现请求的点位丢失，通过信号链，最终导致 Fustor Agent 在一次 `push` 请求中发送了 `snapshot_sync_suggested: true`。
4.  **请求快照**: 您的 Consumer 服务收到了这个“建议”，决定需要回填历史数据。于是在这次 `push` 请求的响应中，返回 `{"snapshot_needed": true}`。
5.  **并发执行**: Fustor Agent 收到这个请求后，在后台异步启动一个快照任务。此时，您的 Consumer 会**同时**收到来自 `agent-A` 的两路数据流：一路是仍在继续的实时事件，另一路是来自后台快照任务的历史数据。
6.  **合并数据**: 您的服务需要能正确处理这两路并发的数据流，利用 `UPSERT` 实现去重和合并。
7.  **快照结束**: 后台快照任务完成，Fustor Agent 发送最后一次 `push` 请求，其中包含 `is_snapshot_end: true`。
8.  **稳态运行**: 您的服务收到这个结束标志。此后，来自 `agent-A` 的数据流恢复为单一的实时事件流。