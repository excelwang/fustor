# Fustor 数据管理平台

![](https://img.shields.io/badge/Python-3.11%2B-blue)
![](https://img.shields.io/badge/FastAPI-0.104.1-green)
![](https://img.shields.io/badge/SQLAlchemy-2.0.23-orange)

Fustor 是一个新一代科研数据融合存储平台，提供统一的元数据管理和数据检索服务。本文档旨在指导不同角色的用户从零开始部署和使用 Fustor 平台。

## 🚀 快速开始

### 1. 环境准备

所有服务均基于 Python 3.11+。推荐使用 `uv` 进行包管理，也可以使用 `pip`。

```bash
# 1. 安装 uv (推荐)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. 创建并激活虚拟环境
uv venv .venv
source .venv/bin/activate
```

### 2. 角色操作手册

请根据您的角色选择相应的操作指南：

#### 🧑‍💼 Registry Admin (平台管理员)
**职责**: 部署 Registry 服务，创建 Datastore，生成 API Key。

1.  **安装 Registry**:
    ```bash
    pip install fustor-registry
    # 或者在源码目录下: uv sync --extra registry
    ```

2.  **初始化配置**:
    ```bash
    mkdir -p ~/.fustor
    # 复制 .env.example 到 ~/.fustor/.env 并配置数据库连接等
    ```

3.  **启动 Registry 服务**:
    ```bash
    # 启动服务 (默认端口 8101)
    fustor-registry start -D
    ```

4.  **管理操作**:
    *   访问 Swagger UI: `http://localhost:8101/docs`
    *   **创建 Datastore**: 调用 `POST /api/v1/admin/datastores` 创建一个新的数据存储库 (例如 "My Research Data")。
    *   **生成 API Key**: 调用 `POST /api/v1/admin/apikeys` 为该 Datastore 生成一个 API Key。**请妥善保存此 Key，后续 Fusion 和 Agent 都需要用到。**

---

#### 👨‍🔧 Fusion Admin (融合服务管理员)
**职责**: 部署 Fusion 服务，连接 Registry，管理服务启停。

1.  **安装 Fusion**:
    ```bash
    pip install fustor-fusion
    # 或者在源码目录下: uv sync --extra fusion
    ```

2.  **配置连接**:
    在 `~/.fustor/.env` 中配置 Registry 的地址：
    ```bash
    FUSTOR_REGISTRY_URL=http://localhost:8101
    ```

3.  **启动 Fusion 服务**:
    ```bash
    # 启动服务 (默认端口 8102)
    fustor-fusion start -D
    
    # 停止服务
    fustor-fusion stop
    ```

---

#### 👷 Source Admin (数据源管理员)
**职责**: 部署 Agent 服务，配置数据源，将数据推送给 Fusion。

1.  **安装 Agent**:
    ```bash
    pip install fustor-agent
    # 以及你需要的数据源插件，例如:
    pip install fustor-source-fs
    ```

2.  **配置 Agent (`agent-config.yaml`)**:
    在 `~/.fustor/agent-config.yaml` 中配置 Source 和 Pusher。
    
    ```yaml
    # 示例配置：监控本地目录并推送到 Fusion
    
    sources:
      - id: "local-fs-source"
        type: "fs"
        config:
          uri: "/path/to/your/data"  # 监控的目录
          driver_params:
            min_monitoring_window_days: 30

    pushers:
      - id: "fusion-pusher"
        type: "fusion"
        config:
          # Fusion 的接收地址
          endpoint: "http://localhost:8102/ingestor-api/v1/events" 
          # 从 Registry Admin 处获取的 API Key
          credential: "YOUR_API_KEY_HERE" 

    syncs:
      - id: "sync-job-1"
        source_id: "local-fs-source"
        pusher_id: "fusion-pusher"
        # 自动启动
        enabled: true 
    ```

3.  **启动 Agent 服务**:
    ```bash
    # 启动服务 (默认端口 8100)
    fustor-agent start -D
    ```
    Agent 启动后会自动读取配置并开始同步数据。

---

#### 🕵️ Fusion User (数据用户)
**职责**: 查看数据，监控系统状态。

1.  **访问监控仪表盘 (Dashboard)**:
    *   打开浏览器访问: `http://localhost:8102/view`
    *   输入 **API Key** 进行连接。
    *   您将看到实时的网络拓扑图、数据吞吐量、同步延迟和陈旧度指标。

2.  **浏览文件目录**:
    *   Fusion 提供了文件系统风格的 API。
    *   **获取根目录**: `GET /views/fs/tree?path=/`
    *   **搜索文件**: `GET /views/fs/search?pattern=*.txt`
    *   *(注：需在请求 Header 中带上 `X-API-Key`)*

## 📦 模块详情

*   **Registry**: 核心元数据管理。详见 `registry/docs/README.md`。
*   **Fusion**: 数据摄取与处理。详见 `fusion/docs/README.md`。
*   **Agent**: 数据采集与推送。详见 `agent/README.md`。

## 🛠️ 开发与贡献

如果您想参与 Fustor 的开发，请参考 `development/DEVELOPER_GUIDE.md`。