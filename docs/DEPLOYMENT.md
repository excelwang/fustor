# Fustor 平台部署安装指南

本文档旨在指导管理员从零开始部署 Fustor 数据融合存储平台，包括 **Fusion（服务端）** 和 **Agent（采集端）** 的安装、配置与启动。

## 1. 环境准备

所有服务均基于 Python 3.11+ 开发。推荐使用 `uv` 进行现代化的 Python 包管理（速度更快），也可以使用传统的 `pip`。

*   **操作系统**: Linux / macOS
*   **Python 版本**: >= 3.11
*   **包管理器**: 推荐 `uv` (或 `pip`)

```bash
# 安装 uv (可选，推荐)
curl -LsSf https://astral.sh/uv/install.sh | sh
```

---

## 2. 目录结构说明

Fustor 使用统一的主目录来存放配置、日志和持久化数据。

*   **默认路径**: `~/.fustor`
*   **自定义路径**: 可通过环境变量 `FUSTOR_HOME` 指定其他路径。

*   **建议的目录结构：**
```text
~/.fustor/
├── fusion-config/       # Fusion 服务端配置文件
│   └── default.yaml     # 核心配置文件，包含全局设置和默认
├── agent-config/        # Agent 采集端配置文件
│   └── default.yaml     # 核心配置文件，包含日志和全局设置
├── logs/                # 运行日志
└── data/                # 持久化数据存储
```

---

## 3. 部署 Fusion (服务端)

Fusion 是 Fustor 的核心存储引擎，负责接收数据并提供查询视图。

### 3.1 安装

```bash
# 使用 pip 安装 Fusion 主程序及标准文件系统视图驱动
pip install fustor-fusion fustor-view-fs
# 或者使用 uv
uv pip install fustor-fusion fustor-view-fs
```

### 3.2 配置

在 `~/.fustor/fusion-config/` 目录下创建配置文件（例如 `default.yaml`）。该目录下所有 `.yaml` 文件会被合并加载，支持跨文件引用。

# 样例配置 (`~/.fustor/fusion-config/default.yaml`):

```yaml
# 0. 全局设置 (可选)
logging: "INFO"         # 日志级别: DEBUG, INFO, WARNING, ERROR, CRITICAL
host: "0.0.0.0"         # 管理 API 绑定地址
port: 8101              # 管理 API 端口

# 1. 定义接收器 (Receiver): 用于接收 Agent 推送的数据
receivers:
  http-main:
    driver: http          # 使用 HTTP 协议
    bind_host: "0.0.0.0"
    port: 18888           # 数据接收端口 (不同于管理端口)
    api_keys:
      - key: "my-secure-api-key"   # 设置鉴权 Key，Agent 端需匹配
        pipe_id: "research-sync"   # 绑定到指定的 Pipe ID

# 2. 定义视图 (View): 定义数据的存储和展示方式
views:
  research-view:
    driver: fs            # 使用文件系统驱动
    disabled: false
    driver_params:
      hot_file_threshold: 60.0 # 热文件判定阈值(秒)

# 3. 定义管道 (Pipe): 将接收器与视图绑定
pipes:
  research-sync:
    receiver: http-main   # 引用上面的 receiver id
    views:                # 数据写入哪些视图
      - research-view
    allow_concurrent_push: true    # 允许并发推送
    session_timeout_seconds: 30  # 会话超时时间
```


### 3.3 启动

```bash
# 前台启动（加载且仅加载 default.yaml 中的配置）
fustor-fusion start

# 启动特定配置文件中的 Pipes
fustor-fusion start my-custom-pipes.yaml

# 或后台启动
fustor-fusion start -D
```

---

## 4. 部署 Agent (采集端)

Agent 部署在数据源所在的机器上，负责监听数据源变更并推送给 Fusion。

### 4.1 安装

```bash
# 安装 Agent 主程序及相关驱动
pip install fustor-agent fustor-source-fs fustor-sender-http
# 或者使用 uv
uv pip install fustor-agent fustor-source-fs fustor-sender-http
```

### 4.2 配置

在 `~/.fustor/agent-config/` 目录下创建配置文件（例如 `default.yaml`）。

# 样例配置 (`~/.fustor/agent-config/default.yaml`):

```yaml
# 0. 全局设置 (可选)
logging: "INFO"

# 1. 定义数据源 (Source): 监控本地数据
sources:
  local-research-files:
    driver: fs            # 使用文件系统驱动
    uri: "/mnt/data/source_files"  # 需要监控的本地绝对路径
    max_queue_size: 1000  # 本地事件缓冲区大小
    disabled: false
    driver_params:
      file_pattern: "*"   # 监听文件模式
      min_monitoring_window_days: 30.0 # 最小监控窗口(天)

# 2. 定义发送器 (Sender): 推送目标
senders:
  fusion-server:
    driver: fusion        # 使用 Fusion HTTP 驱动
    uri: "http://<FUSION_SERVER_IP>:18888"  # Fusion 服务器接收端口
    batch_size: 1000      # 批量发送条数
    timeout_sec: 30       # 请求超时时间
    credential:
      key: "my-secure-api-key" # 必须与 Fusion 配置中的 key 一致

# 3. 定义同步管道 (Pipe): 绑定源与目标
pipes:
  research-sync-task:
    source: local-research-files  # 引用 source id
    sender: fusion-server         # 引用 sender id
    # 错误重试配置
    error_retry_interval: 5.0     # 初始重试间隔
    max_consecutive_errors: 5     # 最大连续错误数
```

### 4.3 启动

```bash
# 前台启动 (仅加载 default.yaml)
fustor-agent start

# 后台启动
fustor-agent start -D
```

---

## 5. 配置热重载 (Hot Reload)

Fustor 支持在不重启服务的情况下动态更新配置。

### 5.1 使用 CLI 触发
当你修改了 YAML 配置文件后，可以运行以下命令让正在运行的守护进程重新加载配置：

```bash
# 重载 Fusion 配置
fustor-fusion reload

# 重载 Agent 配置
fustor-agent reload
```

### 5.2 运行机制
*   **信号触发**: CLI 命令本质上是向后台进程发送了 `SIGHUP` 信号。
*   **增量更新**: 系统会计算配置差异，自动启动新增加的管道，停止已禁用的组件关联的管道，并更新全局参数（如日志级别），整个过程中已存在的活动连接不受影响。

*   **Agent 报错 "Connection refused"**: 检查 `senders` 配置中的 IP 和端口是否正确，确保 Fusion 服务器防火墙允许 18888 端口（Receiver 端口）通过。
*   **Fusion 报错 "Unauthorized"**: 检查 Agent 的 `api_key` 是否与 Fusion `receivers` 配置中的 Key 完全一致。
*   **配置未生效**: 确保配置文件扩展名为 `.yaml`。对于非 `default.yaml` 的配置，如果在启动时未明确指定文件，需在修改后运行 `reload` 命令或手动重启。默认情况下，`start` 不带参数仅会激活 `default.yaml` 中的管道。

---

## 6. 配置参数详解 (Configuration Reference)

### 6.1 通用全局参数 (Global)

| 参数路径 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `logging` | string/dict | `"INFO"` | 日志级别或详细配置 |
| `host` | string | `"0.0.0.0"` | (Fusion) 管理 API 监听地址 |
| `port` | int | `8101` | (Fusion) 管理 API 监听端口 |
| `session_cleanup_interval` | float | `60.0` | (Fusion) 会话清理间隔(秒) |


### 6.2 Fusion 配置参数

**接收器 (`receivers` 节)**
| 参数名 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `driver` | string | (无默认值) | 驱动类型，通常为 `"http"` |
| `bind_host` | string | `"0.0.0.0"` | 数据接收服务绑定地址 |
| `port` | int | `8102` | 数据接收服务端口 |
| `disabled` | bool | `false` | 是否禁用此接收器 |
| `api_keys` | list | `[]` | 鉴权密钥列表，每项包含 `key` 和 `pipe_id` |

**视图 (`views` 节)**
| 参数名 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `driver` | string | `"fs"` | 视图驱动类型 |
| `disabled` | bool | `false` | 是否禁用此视图 |
| `driver_params` | dict | `{}` | 驱动特定参数 |

**Fusion 管道 (`pipes` 节)**
| 参数名 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `receiver` | string | (必填) | 关联的 Receiver ID |
| `views` | list[str] | `[]` | 数据分发的目标 View ID 列表 |
| `allow_concurrent_push` | bool | `true` | 是否允许 Agent 并发推送数据 |
| `session_timeout_seconds` | int | `30` | 会话超时时间(秒) |

### 6.3 Agent 配置参数

**数据源 (`sources` 节)**
| 参数名 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `driver` | string | (必填) | 驱动类型，如 `"fs"` |
| `uri` | string | (必填) | 数据源连接 URI (如文件路径) |
| `max_queue_size` | int | `1000` | 内部事件缓冲队列最大长度 |
| `max_retries` | int | `10` | 驱动读取失败重试次数 |
| `retry_delay_sec` | int | `5` | 驱动重试等待时间(秒) |
| `disabled` | bool | `false` | 是否禁用此源 |
| `driver_params` | dict | `{}` | 驱动专属参数 |

**发送器 (`senders` 节)**
| 参数名 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `driver` | string | (必填) | 驱动类型，如 `"fusion"` |
| `uri` | string | (必填) | 目标地址 URI |
| `batch_size` | int | `1000` | 批量发送的最大事件数 |
| `max_retries` | int | `10` | 发送失败重试次数 |
| `retry_delay_sec` | int | `5` | 发送重试等待时间(秒) |
| `timeout_sec` | int | `30` | 网络请求超时时间(秒) |
| `disabled` | bool | `false` | 是否禁用此发送器 |

**Agent 管道 (`pipes` 节)**
| 参数名 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `source` | string | (必填) | 关联的 Source ID |
| `sender` | string | (必填) | 关联的 Sender ID |
| `error_retry_interval` | float | `5.0` | 管道级错误后的重试初始退避(秒) |
| `max_consecutive_errors`| int | `5` | 触发严重告警的连续错误次数阈值 |
| `backoff_multiplier` | float | `2.0` | 重试退避指数倍数 |
| `max_backoff_seconds` | float | `60.0` | 最大重试退避时间(秒) |
| `session_timeout_seconds` | float | `30.0` | (可选) 会话超时建议值 |
| `disabled` | bool | `false` | 是否禁用此管道 |
