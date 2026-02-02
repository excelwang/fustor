# Fustor Fusion 存储引擎服务

Fusion 是 Fustor 平台的核心存储与查询引擎。它负责接收来自 Agent 的异步事件流，并在内存中构建高效的元数据哈希索引，为上层应用提供极速的结构化查询能力。

## 核心特性

*   **In-Memory Hash Tree**: 针对文件系统层级结构优化的内存索引，支持 $O(1)$ 级节点定位。
*   **极速序列化**: 集成 `orjson` 引擎，支持百万级元数据的高并发 JSON 输出。
*   **一致性仲裁**: 通过 Tombstone、Suspect List、Blind-spot List 实现多源数据的智能合并。

## API 接口参考

所有接口均需在 Header 中带上 `X-API-Key`。

### 1. 视图 API (Views)

用于检索已索引的数据视图（如文件系统、数据库镜像等）。视图 API 的路径采用动态前缀：
`/api/v1/views/{view_id}/...`

其中 `{view_id}` 是在 `fusion-config.yaml` 中配置的视图实例名称（见下文 "配置参考"）。

#### **GET `/views/{view_id}/tree`**
获取目录树结构（仅限 `fs` 驱动）。

| 参数 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `path` | string | `/` | 目标起始路径。 |
| `recursive` | boolean | `true` | 是否递归返回子孙节点。 |
| `max_depth` | integer | `null` | 递归的最大深度（相对于 `path`）。 |
| `only_path` | boolean | `false` | 若为 `true`，则只返回路径，剔除 size/mtime/ctime 等元数据。 |

**响应结构 (JSON):**
```json
{
  "name": "dir1",
  "path": "/dir1",
  "content_type": "directory",
  "children": { ... }
}
```

#### **GET `/views/{view_id}/stats`**
获取当前视图的指标和统计信息。

*   **返回**: `item_count`, `latency_ms`, `logical_now` 等。

---

## 配置参考 (Configuration)

Fusion 的视图配置采用目录结构管理，默认位于 `$FUSTOR_HOME/views-config/`。每个 `.yaml` 文件定义一个视图配置。

例如 `views-config/test-fs.yaml`:

```yaml
id: test-fs           # 视图实例名称，对应 API 路径 /views/test-fs/
                        # 同时也作为 view_id (Legacy: view_id)
driver: fs            # 使用的驱动程序名称
disabled: false
driver_params:
  hot_file_threshold: 30
```


---

### 2. 数据摄取 API (Ingestion)

主要供 Agent 或 Pusher 使用。

#### **POST `/ingestor-api/v1/events/`**
批量推送事件数据。

| 字段 | 类型 | 说明 |
| :--- | :--- | :--- |
| `session_id` | string | 当前活跃的会话 ID。 |
| `events` | list | 包含 `UpdateEvent` 或 `DeleteEvent` 的数组。 |
| `message_source` | string | `realtime` / `snapshot` / `audit`。 |
| `is_snapshot_end`| boolean | 快照结束标志位。 |

---

### 3. 会话管理 API (Sessions)

用于维护 Agent 与 Fusion 之间的同步契约。

#### **POST `/ingestor-api/v1/sessions/`**
创建新的同步会话。

*   **参数**: `task_id` (唯一任务标识)。
*   **特性**: 采用 **先到先得 (First-Come-First-Serve)** 模式，第一个建立 Session 的 Agent 成为 Leader。

---

## 就绪状态判定 (READY Logic)

当存储库处于初始快照同步阶段或 Live 模式下无活跃会话时，视图 API 会返回 **503 Service Unavailable**。

### 1. 核心就绪 (Core Readiness)
所有视图驱动必须满足以下条件方可访问：
1.  **信号就绪**: 已接收到 `is_snapshot_end=true`。
2.  **队列就绪**: 内部 `memory_event_queue` 已全部清空。
3.  **解析就绪**: `ProcessingManager` 中的 Inflight 事件处理数为 0。

### 2. Live 模式强制检查 (Live Session Enforcement)
若视图配置为 **Live 模式** (配置中 `mode: live` 或驱动属性 `requires_full_reset_on_session_close=True`)，则额外要求：
*   **活跃会话**: 必须至少有一个活跃的 Ingest Session。若所有 Session 关闭，视图会自动重置并返回 503 直到新 Session 建立。


### 1. Leader 会话锁 (Leader Session Lock)
Fusion 遵循 **"先到先得 (First-Come-First-Serve)"** 机制：
*   第一个建立 Session 的 Agent 成为 Leader，拥有 Snapshot/Audit/Sentinel 权限。
*   后续连接的 Agent 自动成为 Follower，仅执行 Realtime Sync。
*   仅当 Leader 心跳超时或断开后，Fusion 才释放 Leader 锁。

### 2. 一致性仲裁 (Consistency Arbitration)
Fusion 维护以下状态：
*   **Tombstone List**：记录被 Realtime 删除的文件，防止 Snapshot/Audit 使其"复活"
*   **Suspect List**：标记可能正在写入的文件 (`integrity_suspect: true`)
*   **Blind-spot List**：标记在无 Agent 客户端发生变更的文件 (`agent_missing: true`)

仲裁原则：**Realtime 优先，Mtime 仲裁**。详见 `docs/CONSISTENCY_DESIGN.md`。

### 3. 心跳存续依赖 (Heartbeat Availability Dependency)
为了防止权威 Agent 崩溃导致视图陈旧（Stale Data Risk）：
*   **硬链接可用性**：一旦权威 Agent 的 **心跳丢失 (Heartbeat Timeout)**，Fusion 必须立即将对应的 View 状态切换为 **503 Service Unavailable**。
*   **逻辑理由**：心跳丢失意味着实时事件流（inotify）可能已中断，此时 Fusion 看到的树结构不再是对物理事实的准确感知。

## 性能优化建议

*   **批量推送**: 建议 Agent 每 1000 行聚合为一个 Event 发送，以最大化摄取效率。
*   **深度控制**: 在 UI 展示时，建议带上 `max_depth=1` 参数进行分页或按需加载，避免单次传输过大的 JSON 树。
