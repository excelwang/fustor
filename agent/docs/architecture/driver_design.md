# Fustor Agent 驱动层详细设计

## 1. 设计理念

驱动层是 Fustor Agent 与外部世界交互的桥梁。它通过定义一套标准的抽象基类（Abstract Base Classes, ABCs），将 Fustor Agent 的核心业务逻辑（如任务调度、状态管理）与具体的数据系统（如 MySQL, PostgreSQL, 文件系统）完全解耦。

所有驱动都必须继承自 `SourceDriver` 或 `PusherDriver` 这两个 ABC 之一，并实现其定义的所有抽象方法。这种设计强制确保了所有驱动都遵循统一、明确的接口契约。

### 动态加载机制

驱动被打包为独立的 Python 包。Fustor Agent 使用标准的 `importlib.metadata.entry_points` 机制来发现和加载已安装的驱动插件。一个驱动通过在其 `pyproject.toml` 文件中定义一个特定的入口点（例如 `"fustor.drivers.sources"`）来将自己注册到系统中。这种方式无需任何手动的注册代码，极大地增强了系统的可扩展性。

---

## 2. SourceDriver (数据源驱动)

**`fustor_core.drivers.SourceDriver`**

数据源驱动的核心职责是：

1.  在**配置阶段**，通过一系列类方法与底层数据系统交互，执行连接测试、环境检查和权限设置。
2.  在**运行阶段**，其实例由 `SyncInstance` 控制器创建和持有，负责按需提供“快照数据流”和“实时消息流”。

一个完整的数据源驱动**类**必须实现以下方法：

### a. 核心运行时接口 (Runtime Interface)

这些方法由 `SyncInstance` 或其下属服务（如 `EventBus`）调用，是数据流的源头。

*   `def __init__(self, config: SourceConfig):`
    *   **构造函数**: 每个驱动实例在创建时都会接收其对应的 `SourceConfig` 配置。

*   `def get_snapshot_iterator(self, **kwargs) -> Iterator[EventBase]:`
    *   **阶段**: **补充性质的快照同步**。
    *   **职责**: 在被请求时，执行一次性的、有终点的批量历史数据同步。
    *   **模型**: 这是一个标准的 Python **生成器 (Generator)**。`SyncInstance` 会在一个独立的后台任务中迭代此生成器。
    *   **核心逻辑**: 与旧模型类似，驱动连接数据源，分批次拉取历史数据，并将每个批次封装在 `EventBase` 对象中 `yield` 出来。

*   `def get_message_iterator(self, start_position: int = -1, **kwargs) -> Iterator[EventBase]:`
    *   **阶段**: **实时消息同步**。
    *   **职责**: 提供一个无终点的、持续的实时增量事件流。
    *   **模型**: 这是一个**“尽力而为”**的、可能阻塞的长轮询生成器。`EventBus` 会在后台线程中持续迭代它。
    *   **核心逻辑**:
        1.  接收一个 `start_position` 参数，这被视为一个**“建议的”**起始点。
        2.  驱动**必须**检查 `is_position_available(start_position)`。如果点位有效，则从该点位开始监听。
        3.  如果 `start_position` 在数据源上已不可用（例如历史日志被清除），驱动**不能抛出异常**。相反，它必须从当前可用的**最新位置**开始监听。点位丢失的逻辑由上层服务(`EventBusService`)在订阅时检测到点位无法满足，会向上返回 `needed_position_lost=True` 的标志。
    *   **返回值**: 此方法仅返回 `EventBase` 对象的迭代器。

*   `def is_position_available(self, position: int) -> bool:`
    *   **职责**: 检查驱动是否可以从一个特定的 `position` 恢复。
    *   **核心逻辑**: 对于瞬态数据源，此方法应始终返回 `False`。对于持久化数据源，它应检查给定的点位是否仍然有效。

### b. UI 与配置接口 (Class Methods)

这些 `@classmethod` 由 `SourceDriverService` 在用户通过 Web UI 配置向导时调用，用于提供动态、交互式的配置体验。这部分接口保持不变。

*   `async def get_available_fields(cls, **kwargs) -> Dict[str, Any]:`
*   `async def test_connection(cls, **kwargs) -> Tuple[bool, str]:`
*   `async def check_privileges(cls, **kwargs) -> Tuple[bool, str]:`

---

## 3. PusherDriver (数据推送驱动)

**`fustor_core.drivers.PusherDriver`**

数据推送驱动的职责是连接到下游系统（Consumer），建立并维护一个同步会话，并遵循"信封协议"来批量推送事件数据。

一个完整的数据推送驱动**类**必须实现以下方法：

### a. 核心运行时接口

*   `def __init__(self, config: SenderConfig):`
    *   **构造函数**: 保持不变。

*   `async def create_session(self, task_id: str) -> str:`
    *   **职责**: 在同步任务启动时，向消费者请求创建一个新的同步会话。
    *   **返回值**: 返回一个包含会话信息的字典，其中必须包含 `session_id`。

*   `async def heartbeat(self, **kwargs) -> Dict:`
    *   **职责**: 定期向消费者发送心跳，以维持会话的活跃状态，防止会话因超时而失效。
    *   **调用参数**: `kwargs` 中会包含 `session_id`。

*   `async def push(self, events: List[EventBase], **kwargs) -> Dict:`
    *   **职责**: 将一批事件数据打包成"信封"JSON，推送到消费者端点。
    *   **调用参数**: `kwargs` 中会包含 `session_id` 和一个布尔值 `is_snapshot_end`，用于标识快照流的结束。
    *   **信封协议**: 发送的JSON载荷应包含以下字段：
        *   `session_id: str`: 当前同步会话的ID。
        *   `events: List[Dict]`: 事件数据行列表。
        *   `source_type: str`: 'message' 或 'snapshot'。
    *   **返回值**: 返回一个字典，包含推送结果信息。

*   `async def get_latest_committed_index(self, **kwargs) -> int:`
    *   **职责**: 在会话创建后，连接到消费者，查询并返回该会话关联的任务 (`task_id`) 的最后一次成功提交的数据点位 (`index`)。
    *   **调用参数**: `kwargs` 中会包含 `session_id`。
    *   **返回值**: 返回一个整数型点位。如果消费者没有该任务的记录，**必须返回 `-1`**，表示从新开始。

### b. UI 与配置接口

*   `async def get_needed_fields(cls, **kwargs) -> Dict[str, Any]:`
    *   **职责**: 保持不变。返回一个 JSON Schema，描述此接收器需要哪些字段以及它们的格式。

---

## 4. 文件系统驱动实现细节

### 4.1 共享实例模型
为了优化资源使用，`FSDriver` 实现了一个共享实例模式。基于 `uri` 和 `credential` 的组合来缓存和复用驱动实例。这意味着对于指向同一路径且使用相同凭证的多个同步任务，它们将共享同一个底层的 `_WatchManager` 和 `inotify` 实例，从而显著减少了文件句柄和内存的消耗。

### 4.2 智能动态监控与LRU策略

文件系统驱动实现了一套智能监控机制，以避免耗尽系统的 `inotify` 监控限制。

*   **目录级监控**: 驱动在每个发现的子目录上放置独立的、非递归的监控器。
*   **LRU驱逐策略**: 当达到监控器数量限制时，驱动会自动驱逐**最近最少使用**的监控器，为新监控器腾出空间。
*   **级联取消监控**: 当一个目录被驱逐时，系统不仅取消该目录的监控，还检查并取消任何子目录的监控，确保资源正确释放。

### 4.3 监控限制自适应调整

*   **错误检测**: 当系统 `inotify` 限制达到时（`errno 28`），驱动会捕获此错误并自动调整内部 `watch_limit`。
*   **递归重试**: 在调整限制后，驱动会立即重试监控调度，确保不会因资源限制而丢失事件。

### 4.4 事件生成模型

*   **稳定性信号**: 文件修改仅在 `on_closed` 事件发生时注册，确保只处理完全写入的文件。
*   **事件映射**:
    * `UpdateEvent`: 为快照阶段的所有预存文件以及实时监控中的 `created`, `closed` 和 `moved`（目标路径）事件生成。
    * `DeleteEvent`: 为 `deleted` 和 `moved`（源路径）事件生成。
    * `modified` 事件被故意忽略以等待 `closed` 信号。
*   **时间戳格式**: 所有事件载荷中的时间戳（`modified_time`, `created_time`）以原始浮点 Unix 时间戳格式发送，以优化下游消费者。

---

## 5. 驱动的健壮性要求

1.  **幂等性 (Idempotency)**: `push` 方法应尽可能设计为幂等的。
2.  **优雅停机 (Graceful Shutdown)**: `get_message_iterator` 返回的迭代器必须能响应 `stop_event` 信号。
3.  **明确的异常**: 驱动应在真正发生不可恢复的错误时（如认证失败）才抛出 `DriverError`。

## 6. 示例：MySQL 驱动的实现要点 (新模型)

### `get_message_iterator`

1.  **检查点位**: 接收 `start_position` (binlog file + pos)。调用 `is_position_available` 检查该点位是否在MySQL服务器的binlog索引中依然存在。
2.  **处理点位丢失**: 如果 `is_position_available` 返回 `False`，记录一条警告，并获取当前`MASTER STATUS`作为新的起始点。
3.  **创建复制流**: 使用 `pymysqlreplication.BinLogStreamReader` 从确定的起始点位开始监听。
4.  **事件转换与 `yield`**: 在循环中转换并 `yield` 事件。