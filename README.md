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

### 2. 初始化配置

Fustor 使用一个主目录来存放配置、日志和数据库。
*   **默认路径**: `~/.fustor`
*   **自定义路径**: 设置 `FUSTOR_HOME` 环境变量。

```bash
# 创建基础目录结构
mkdir -p ~/.fustor/views-config
touch ~/.fustor/receivers-config.yaml      # 创建接收端配置文件
mkdir -p ~/.fustor/agent-pipes-config     # 替代 syncs-config
```

---

### 3. 角色操作手册

#### 👨‍🔧 Fusion Admin (融合服务管理员)
**职责**: 配置接收端 (Receiver)、视图 (View) 和管道 (Pipeline)，启动 Fusion 服务。

1.  **安装 Fusion**:
    ```bash
    pip install fustor-fusion
    ```

2.  **配置 Receiver**:
    在 `~/.fustor/receivers-config.yaml` 中定义监听端口和 API Key：
    ```yaml
    receivers:
      default-http:
        driver: http
        port: 8102
        api_keys:
          your-secure-key-123:
            role: admin
            view_mappings: ["my-view"]
    ```

3.  **配置 View**:
    在 `~/.fustor/views-config/my-view.yaml` 中定义数据展示方式：
    ```yaml
    id: my-view
    driver: fs
    enabled: true
    driver_params:
      root_path: "/mnt/fusion-view"
    ```

4.  **配置 Pipeline**:
    在 `~/.fustor/fusion-pipes-config/pipe-1.yaml` 中绑定 Receiver 与 View。

5.  **启动 Fusion 服务**:
    ```bash
    fustor-fusion start -D
    ```

---

#### 👷 Source Admin (数据源管理员)
**职责**: 配置数据源和发送器 (Sender)，将数据推送给 Fusion。

1.  **安装 Agent**:
    ```bash
    pip install fustor-agent fustor-source-fs
    ```

2.  **配置同步任务**:
    在 `~/.fustor/agent-pipes-config/pipe-job.yaml` 中定义采集与推送逻辑。

3.  **启动 Agent**:
    ```bash
    fustor-agent start -D
    ```

---

#### 🕵️ Fusion User (数据用户)
**职责**: 访问和检索数据。

1.  **浏览文件目录**:
    *   Fusion 提供了文件系统风格的 API。
    *   **获取根目录**: `GET /api/v1/views/my-view/tree?path=/`
    *   *(注：需在请求 Header 中带上 `X-API-Key`)*

## 📦 模块详情

*   **Fusion**: 数据摄取、处理与视图提供。详见 `fusion/README.md`。
*   **Agent**: 数据采集与推送。详见 `agent/README.md`。
*   **Common**: 通用工具与基础库。

## 📖 核心文档

*   **[架构设计 V2 (最新)](docs/refactoring/1-ARCHITECTURE_V2.md)**: 了解 V2 架构的解耦设计、Pipeline 抽象与 Handler 适配器模式。
*   **[配置指南](docs/CONFIGURATION.md)**: 详细的 YAML 配置说明。
*   **[架构设计](docs/ARCHITECTURE.md)**: 了解 Fustor 的顶层设计和服务交互。
*   **[一致性设计](docs/CONSISTENCY_DESIGN.md)**: 了解多 Agent 环境下的数据一致性机制。