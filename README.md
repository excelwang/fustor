# Fustor 数据管理平台 (Monorepo)

![](https://img.shields.io/badge/Python-3.11%2B-blue)
![](https://img.shields.io/badge/FastAPI-0.104.1-green)
![](https://img.shields.io/badge/SQLAlchemy-2.0.23-orange)

Fustor 是一个新一代科研数据融合存储平台，采用 Monorepo 架构管理多个独立服务和可插拔组件。它提供统一的元数据管理和数据检索服务，包括统一目录、统一关联和统一监控服务。

## 📍 核心能力

### 1. 数据汇交流程
```
文件数据生命周期流程图：

[临时存储区] →（完整性检查） → [归档存储区] →（公开条件满足） → [公开存储区]
```

### 2. 数据检索流程
```
普通用户可通过关键字和图查询接口对元数据进行全文检索和关联检索。对于检索到的数据集。
- 公开数据集：直接下载其文件数据
- 受控数据集：需经管理员审批通过后可下载其文件数据
```

## 📦 功能模块与服务

Fustor Monorepo 包含以下主要服务和可插拔组件：

*   **Registry 服务**: 负责元数据管理、存储环境、数据存储库、用户、API Key 和凭据管理。提供统一的注册和发现机制。
    *   详细文档请参见 `registry/docs/README.md`。
*   **Agent 服务**: 轻量级、可扩展的实时数据供给工具。监听数据源变更，并根据需求方的 OpenAPI schema 将数据推送到指定端点。
    *   详细文档请参见 `agent/README.md`。
*   **Fusion 服务**: 负责数据摄取和处理。
    *   详细文档请参见 `fusion/docs/README.md`。
*   **Packages**: 包含 `fustor_common` (通用工具和模型)、`fustor_fusion_sdk` (Fusion 服务 SDK) 以及各种 `source_*` 和 `pusher_*` 驱动。
    *   详细文档请参见 `packages/README.md`。

## 🛠️ 技术栈

### 核心框架
- **FastAPI** - 高性能API框架
- **SQLAlchemy 2.0** - 异步ORM引擎
- **Pydantic v2** - 数据模型验证

### 数据库
- PostgreSQL 15+ (生产环境)
- SQLite (开发/测试环境)

### 生态工具
- uv - 极速Python包管理
- pytest - 测试框架
- dotenv - 环境变量管理

## 🔧 安装运行

本项目使用 `uv` 进行高效的依赖管理和环境设置。

1.  **安装 uv 包管理器**
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

2.  **创建并激活虚拟环境**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **同步开发环境**
    在项目根目录执行以下**单条命令**，即可将所有核心应用、本地插件以及测试工具全部安装到虚拟环境中。

    ```bash
    uv sync --extra dev
    ```

4.  **复制环境配置**
    ```bash
    cp .env.example .env  # 请按实际修改数据库配置
    ```

5.  **启动开发服务器 (示例: Registry 服务)**
    ```bash
    uv run python -m uvicorn registry.src.fustor_registry.main:app --reload
    ```
    *   要启动其他服务，请替换 `registry.src.fustor_registry.main:app` 为对应服务的入口点。

## 🧪 开发与测试

1.  **安装依赖**
    ```bash
    uv sync --extra dev
    ```

2.  **运行测试 (示例: Registry 服务)**
    ```bash
    uv run pytest registry/tests/
    ```
    *   要运行其他服务的测试，请替换 `registry/tests/` 为对应服务的测试目录。

## 📚 附加文档

*   **开发与贡献指南**: `development/DEVELOPER_GUIDE.md`
*   **Registry 服务文档**: `registry/docs/README.md`
*   **Agent 服务文档**: `agent/README.md`
*   **Fusion 服务文档**: `fusion/docs/README.md`
*   **Packages 文档**: `packages/README.md`

每个服务和包的详细文档都可以在其各自的 `docs/` 目录下找到。
