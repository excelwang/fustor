# Fustor Agent API 参考手册

本手册提供了 Fustor Agent 后端 RESTful API 的详细参考。所有API端点都以 `/api` 为前缀。

## 1. 系统与运行时实例 (`/instances`)

这部分API用于监控和控制系统中的运行时实例。实例的状态现在遵循“消息优先”模型，一个补充性质的快照任务可以在消息同步期间，根据远端消费者的请求并发地运行。

| 方法 & 路径 | 描述 |
| :--- | :--- |
| `GET /instances/status` | 获取所有运行时实例（总线和同步任务）的聚合状态。同步任务的状态将明确显示为 `STOPPED`, `MESSAGE_SYNC` (主状态), `SNAPSHOT_SYNC` (并发状态), `RUNNING_CONF_OUTDATE`, `STOPPING`, 或 `ERROR` 等。 |
| `GET /instances/buses` | 列出所有事件总线实例（`EventBusInstanceRuntime`）的当前状态快照。 |
| `GET /instances/syncs` | 列出所有同步任务实例（`SyncInstance`）的当前状态快照，包含其主状态和可能的并发快照状态。 |
| `POST /instances/syncs/{id}/_actions/start` | 根据配置启动一个指定的同步任务实例。任务将**总是直接进入消息同步阶段**。如果远端消费者在后续的数据推送响应中请求快照，一个补充性质的快照任务将在后台并发启动。 |
| `POST /instances/syncs/{id}/_actions/stop` | 优雅地停止一个指定的、正在运行的同步任务实例，包括其可能正在并发运行的快照任务。 |
| `POST /instances/_actions/apply_changes` | 应用所有待定的配置变更。此端点会查找所有状态为 `RUNNING_CONF_OUTDATE` 的同步任务，并优雅地重启它们以应用新配置。 |

## 2. 配置管理 (`/configs/sources`, `/configs/pushers`, `/configs/syncs`)

这部分API用于对系统的静态配置进行增、删、改、查等操作。

### 2.1. 数据源配置 (`/configs/sources`)

| 方法 & 路径 | 描述 |
| :--- | :--- |
| `GET /` | 列出系统中所有已定义的数据源（Source）配置。 |
| `POST /{id}` | 添加一个新的数据源配置。可以同时提交通过向导发现的字段（`discovered_fields`）以进行缓存。 |
| `DELETE /{id}` | 删除一个数据源配置。这会同时停止并删除所有依赖于此数据源的同步任务。 |
| `POST /{id}/_actions/enable` | 启用一个已存在的数据源配置。 |
| `POST /{id}/_actions/disable` | 禁用一个已存在的数据源配置。 |
| `POST /_actions/cleanup` | 清理并删除所有未被任何同步任务使用的、且处于禁用状态的过时数据源配置。 |
| `GET /{id}/_actions/get_available_fields` | 获取指定数据源ID已缓存的可用字段列表。 |

### 2.2. 数据接收方配置 (`/configs/pushers`)

| 方法 & 路径 | 描述 |
| :--- | :--- |
| `GET /` | 列出系统中所有已定义的数据接收方（Pusher）配置。 |
| `POST /{id}` | 添加一个新的数据接收方配置。 |
| `DELETE /{id}` | 删除一个数据接收方配置。这会同时停止并删除所有依赖于此接收方的同步任务。 |
| `POST /{id}/_actions/enable` | 启用一个已存在的数据接收方配置。 |
| `POST /{id}/_actions/disable` | 禁用一个已存在的数据接收方配置。 |
| `POST /_actions/cleanup` | 清理并删除所有未被任何同步任务使用的、且处于禁用状态的过时数据接收方配置。 |
| `GET /{id}/_actions/get_needed_fields` | 连接到指定接收方配置的端点，并获取其需要的字段列表（例如，从OpenAPI规范中解析）。 |

### 2.3. 同步任务配置 (`/configs/syncs`)

| 方法 & 路径 | 描述 |
| :--- | :--- |
| `GET /` | 列出系统中所有已定义的同步任务（Sync）配置。 |
| `POST /{id}` | 添加一个新的同步任务配置。 |
| `DELETE /{id}` | 删除一个同步任务配置。 |
| `POST /{id}/_actions/enable` | 启用一个同步任务配置。 |
| `POST /{id}/_actions/disable` | 禁用一个同步任务配置。 |

## 3. 驱动交互 (`/drivers`)

这部分API用于在不保存配置的情况下，与驱动进行实时交互，主要用于Web UI中的配置向导。

| 方法 & 路径 | 描述 |
| :--- | :--- |
| `GET /sources/{driver_type}/wizard` | 获取指定类型的数据源驱动的Web UI向导定义。 |
| `GET /pushers/{driver_type}/wizard` | 获取指定类型的数据接收方驱动的Web UI向导定义。 |
| `POST /sources/{driver_type}/_actions/test_connection` | 测试与数据源的连接。 |
| `POST /sources/{driver_type}/_actions/check_params` | 检查数据源的运行时参数（如 `binlog` 设置）。 |
| `POST /sources/{driver_type}/_actions/create_agent_user` | 为Fustor Agent在数据源上创建一个专用的代理用户。 |
| `POST /sources/{driver_type}/_actions/check_privileges` | 检查代理用户的权限。 |
| `POST /sources/{driver_type}/_actions/discover_fields_no_cache` | 实时发现数据源的可用字段，不使用缓存。 |
| `POST /pushers/{driver_type}/_actions/test_connection` | 测试与数据接收方的连接。 |
| `POST /pushers/{driver_type}/_actions/check_privileges` | 检查访问数据接收方的凭证权限。 |
| `POST /pushers/{driver_type}/_actions/discover_fields`| 实时发现数据接收方所需的目标字段。|

## 4. 日志 (`/logs`)

| 方法 & 路径 | 描述 |
| :--- | :--- |
| `GET /` | 获取结构化的日志条目。支持通过 `limit` (数量), `level` (级别), `component` (组件名), 和 `before_line` (用于分页) 等查询参数进行强大的过滤和滚动加载。 |

## 5. 监控 (`/metrics`)

| 方法 & 路径 | 描述 |
| :--- | :--- |
| `GET /metrics` | 以 Prometheus 文本格式导出系统核心的监控指标，便于集成到现有的监控系统中。 |