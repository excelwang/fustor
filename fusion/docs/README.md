# Fustor Fusion 存储引擎服务

Fusion 是 Fustor 平台的核心存储与查询引擎。它负责接收来自 Agent 的异步事件流，并在内存中构建高效的元数据哈希索引，为上层应用提供极速的结构化查询能力。

## 核心特性

*   **In-Memory Hash Tree**: 针对文件系统层级结构优化的内存索引，支持 $O(1)$ 级节点定位。
*   **极速序列化**: 集成 `orjson` 引擎，支持百万级元数据的高并发 JSON 输出。
*   **最终一致性**: 通过异步摄取队列（Ingestion Queue）实现高吞吐写入，并在数据完全入库前通过 503 状态进行保护。

## API 接口参考

所有接口均需在 Header 中带上 `X-API-Key`。

### 1. 视图 API (Views)

用于检索已索引的文件系统元数据。

#### **GET `/views/fs/tree`**
递归获取目录树结构。

| 参数 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `path` | string | `/` | 目标起始路径。 |
| `recursive` | boolean | `true` | 是否递归返回子孙节点。 |
| `max_depth` | integer | `null` | 递归的最大深度（相对于 `path`）。 |
| `only_path` | boolean | `false` | 若为 `true`，则只返回路径，剔除 size/mtime/ctime 等元数据以减少带宽。 |

**响应结构 (JSON):**
```json
{
  "name": "dir1",
  "path": "/dir1",
  "content_type": "directory",
  "size": 0,
  "modified_time": 1705832400.0,
  "created_time": 1705832400.0,
  "children": { ... }
}
```

#### **GET `/views/fs/stats`**
获取当前存储库的全局统计信息。

*   **返回**: `total_files`, `total_directories`, `last_event_latency_ms`（系统同步延迟）。

---

### 2. 数据摄取 API (Ingestion)

主要供 Agent 或 Pusher 使用。

#### **POST `/ingestor-api/v1/events/`**
批量推送事件数据。

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `session_id` | string | 当前活跃的会话 ID。 |
| `events` | list | 包含 `UpdateEvent` 或 `DeleteEvent` 的数组。 |
| `source_type` | string | `snapshot` (全量快照) 或 `message` (实时增量)。 |
| `is_snapshot_end`| boolean | 快照结束标志位。 |

---

### 3. 会话管理 API (Sessions)

用于维护 Agent 与 Fusion 之间的同步契约。

#### **POST `/ingestor-api/v1/sessions/`**
创建新的同步会话。

*   **参数**: `task_id` (唯一任务标识)。
*   **特性**: 默认采用互斥模式（同一 Datastore 仅允许一个活跃会话），新会话的建立会自动触发旧会话的清理。

---

## 就绪状态判定 (READY Logic)

当存储库处于初始快照同步阶段时，视图 API 可能会返回 **503 Service Unavailable**。
只有同时满足以下三个条件时，API 才会转为 **200 OK**:
1.  **信号就绪**: 已接收到 `is_snapshot_end=true`。
2.  **队列就绪**: 内部 `memory_event_queue` 已全部清空。
3.  **解析就绪**: `ProcessingManager` 中的 Inflight 事件处理数为 0。

这确保了您通过 API 获取到的数据必然是物理存储上的完整快照。

## 性能优化建议

*   **批量推送**: 建议 Agent 每 1000 行聚合为一个 Event 发送，以最大化摄取效率。
*   **深度控制**: 在 UI 展示时，建议带上 `max_depth=1` 参数进行分页或按需加载，避免单次传输过大的 JSON 树。
