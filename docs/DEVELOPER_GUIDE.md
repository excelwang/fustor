# Fustor Monorepo 开发者指南

本文档是 Fustor 项目新开发者的**第一入口点**。它提供了快速搭建开发环境、了解代码结构、遵循开发规范以及提交贡献的通用指南。

在深入特定技术领域之前，请先阅读本指南。

## 1. 开发环境设置

本项目使用 `uv` 进行高效的依赖管理和环境设置。

1.  **克隆仓库**
    ```bash
    git clone <your_repo_url>
    cd fustor_monorepo
    ```

2.  **创建并激活虚拟环境**
    ```bash
    uv venv .venv
    source .venv/bin/activate
    ```

3.  **同步开发环境**
    在项目根目录执行以下**单条命令**，即可将所有核心应用、所有本地插件以及测试工具全部安装到虚拟环境中。
    ```bash
    uv sync --extra dev
    ```

4.  **启动开发服务器 (示例: Agent 服务)**
    ```bash
    uvicorn agent.src.fustor_agent.app:app --reload --port 8100
    ```
    *   要启动其他服务（如 Fusion），请替换 `agent.src.fustor_agent.app:app` 为对应服务的入口点。

## 2. 技术栈
- **核心框架**: FastAPI, SQLAlchemy 2.0 (Async), Pydantic v2
- **数据库**: PostgreSQL 15+ (生产), SQLite (开发/测试)
- **包管理**: uv

## 3. 测试

- **自动化测试**: 项目使用 `pytest` 和 `Playwright` 进行全自动测试。
- **运行测试**: `uv run pytest`
- **禁止阻塞**: 在编写测试用E时，**严禁使用 `pause()`** 或任何需要手动介入的函数。所有测试和开发流程都必须是全自动的。

## 4. 代码贡献流程

1.  **理解目标**: 仔细阅读相关模块的架构文档和设计目标。
2.  **制定计划**: 制定一个详细的、多步骤的开发/重构计划。
3.  **分步实施与验证**: 每完成一个步骤，都要立即运行并检查效果是否符合预期。
4.  **回顾与检查**: 当认为任务完成后，必须回顾方案文档，检查是否有遗漏的需求。

## 5. 深度文档链接

*   **[核心架构设计 (ARCHITECTURE.md)](./ARCHITECTURE.md)**: 了解 Fustor 的顶层设计和服务交互。
*   **[驱动开发指南 (DRIVER_DEVELOPMENT.md)](./DRIVER_DEVELOPMENT.md)**: 学习如何为 Agent 编写新的 Source 和 Pusher 插件。
*   **[Agent 内部机制 (AGENT_INTERNALS.md)](./AGENT_INTERNALS.md)**: 深入了解 Agent 的状态管理、缓存和调试方法。