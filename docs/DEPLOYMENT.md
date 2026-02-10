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
logging:
  level: "INFO"
fusion:
  port: 8101              # 管理 API 端口

# 1. 定义接收器 (Receiver): 用于接收 Agent 推送的数据
receivers:
  http-main:
    driver: http          # 使用 HTTP 协议
    bind_host: "0.0.0.0"
    port: 18888           # 数据端口
    api_keys:
      - key: "my-secure-api-key"   # 设置鉴权 Key，Agent 端需匹配
        pipe_id: "research-sync"   # 绑定到指定的 Pipe ID

# 2. 定义视图 (View): 定义数据的存储和展示方式
views:
  research-view:
    driver: fs            # 使用文件系统驱动
    disabled: false
    driver_params:
      hot_file_threshold: 60.0

# 3. 定义管道 (Pipe): 将接收器与视图绑定
pipes:
  research-sync:
    receiver: http-main   # 引用上面的 receiver id
    views:                # 数据写入哪些视图
      - research-view
    session_timeout_seconds: 3600
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

在 `~/.fustor/agent-config/` 目录下创建配置文件（例如 `deployment.yaml`）。

# 样例配置 (`~/.fustor/agent-config/default.yaml`):

```yaml
# 0. 全局设置 (可选)
logging:
  level: "INFO"

# 1. 定义数据源 (Source): 监控本地数据
sources:
  local-research-files:
    driver: fs            # 使用文件系统驱动
    uri: "/mnt/data/source_files"  # 需要监控的本地绝对路径
    disabled: false

# 2. 定义发送器 (Sender): 推送目标
senders:
  fusion-server:
    driver: fusion        # 使用 Fusion HTTP 驱动
    uri: "http://<FUSION_SERVER_IP>:18888"  # Fusion 服务器接收端口
    credential:
      key: "my-secure-api-key" # 必须与 Fusion 配置中的 key 一致

# 3. 定义同步管道 (Pipe): 绑定源与目标
pipes:
  research-sync-task:
    source: local-research-files  # 引用 source id
    sender: fusion-server         # 引用 sender id
    
    # 同步策略配置
    audit_interval_sec: 36000.0     # 每10小时进行一次全量审计
    sentinel_interval_sec: 120.0  # 哨兵扫描间隔
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
