# Fustor Monorepo 开发者与贡献者指南

欢迎加入 Fustor Monorepo 项目！本指南旨在帮助您快速搭建开发環境，理解项目的核心设计哲学，并顺利地参与到开发与贡献中来。

## 1. 核心架构与设计

### 1.1. Monorepo 与 UV 工作空间 (Workspace)

本项目采用 Monorepo（单仓库多项目）的组织方式，并利用 `uv` 的 **工作空间 (Workspace)** 特性进行管理。其核心目录结构如下：

```
/fustor_monorepo/
├── agent/                   <-- Agent 服务核心应用的代码
├── registry/                <-- Registry 服务核心应用的代码
├── fusion/                  <-- Fusion 服务核心应用的代码
├── packages/                <-- 存放所有独立的、可插拔的驱动插件和共享库
│   ├── fustor_common/
│   ├── fustor_core/
│   ├── source_mysql/
│   └── ...
└── pyproject.toml           <-- **项目总控中心**
```

`pyproject.toml` 文件通过 `[tool.uv.workspace]` 和 `[tool.uv.sources]` 等字段，定义了整个 Monorepo 的结构和本地包的依赖关系，是理解本项目开发流程的关键。

### 1.2. "消息优先，并发快照"模型 (Agent 服务)

Fustor Agent 服务的所有数据同步任务都遵循一个统一的、以实时性为核心的同步模型。

1.  **消息优先 (Message-First)**: 任务启动后，总是**立即**进入消息同步阶段，以最低延迟开始处理实时数据流。这取代了早期版本中的"快照-消息"两阶段同步模型，确保了实时数据的最小延迟。

2.  **并发补充快照 (Concurrent Snapshot Backfill)**: 历史数据的全量同步（快照）被设计成一个**补充性质的、并发的**后台任务。它由远端消费者按需请求，并且其执行**不会阻塞**主链路的实时数据同步。

这个设计确保了新数据的时效性，同时为历史数据回填提供了灵活、无阻塞的解决方案。

## 2. 开发环境设置

本项目使用 `uv` 进行高效的依赖管理和环境设置。

1.  **克隆仓库**
    ```bash
    git clone <your_repo_url>
    cd fustor_monorepo
    ```

2.  **创建并激活虚拟环境**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **同步开发环境**
    在项目根目录执行以下**单条命令**，即可将所有核心应用、所有本地插件以及测试工具全部安装到虚拟环境中。

    ```bash
    uv sync --extra dev
    ```

4.  **启动开发服务器 (示例: Agent 服务)**
    ```bash
    uv run python -m uvicorn agent.src.fustor_agent.main:app --reload
    ```
    *   要启动其他服务（如 Registry 或 Fusion），请替换 `agent.src.fustor_agent.main:app` 为对应服务的入口点。

## 3. 驱动开发指南 (Agent 服务)

### 3.1. 驱动开发契约 (Driver Contract)

所有驱动都遵循统一的开发契约。一个合格的驱动是一个独立的 Python 包，其中包含一个**必须继承**自 `SourceDriver` 或 `PusherDriver` 抽象基类（ABC）的**驱动类**。开发者必须实现该基类定义的所有抽象方法。

-   **核心基类**: `fustor_core.drivers.SourceDriver`, `fustor_core.drivers.PusherDriver`
-   **核心数据模型**: `fustor_core.models.event.EventBase`

**关键接口变更摘要**: 
*   **`SourceDriver.get_message_iterator`**: 此方法现在必须返回一个元组 `Tuple[Iterator[EventBase], bool]`。第二个布尔值 `needed_position_lost` 用于向系统发出信号，表明驱动无法从请求的点位开始，并已从最新位置回退启动。
*   **`PusherDriver.push`**: 此方法现在遵循"信封协议"。它接收 `is_snapshot_end` 和 `snapshot_sync_suggested` 等控制参数，并将其打包在发往远端的JSON"信封"中。同时，它必须解析远端响应，并返回一个包含 `snapshot_needed` 标志的字典。

*(所有方法的详细签名、参数和返回值，请参考 `agent/docs/driver_design.md`)*

---

### 3.2. 作为贡献者在本仓库中添加驱动

此流程适用于希望为 Fustor Agent 官方仓库贡献新驱动的开发者。它利用了 `uv` 的工作空间（Workspace）能力来简化本地开发。

1.  **创建插件包结构**: 在 `packages/` 目录下为新驱动创建一个符合 `[type]_[name]` 命名规范的目录。
    ```bash
    mkdir -p packages/source_postgres/src/fustor_source_postgres
    ```

2.  **创建 `pyproject.toml`**: 在 `packages/source_postgres/` 目录下创建 `pyproject.toml` 文件，定义包名、依赖和入口点。
    ```toml
    [project]
    name = "fustor-source-postgres"
    dependencies = [ "psycopg2-binary" ]

    # 将驱动类注册到系统的入口点
    [project.entry-points."fustor.drivers.sources"]
    postgres = "fustor_source_postgres:PostgresDriver"
    ```

3.  **实现驱动类**: 在 `.../__init__.py` 文件中，创建驱动类并实现所有抽象方法。可以参考其他驱动的实现。

4.  **注册到工作空间**: 打开**根目录**的 `pyproject.toml` 文件，将新驱动的路径 (`"packages/source_postgres"`) 添加到 `[tool.uv.workspace].members` 列表中。

5.  **更新环境**: 在**根目录**下重新运行 `uv sync`，`uv` 就会将您的新驱动以可编辑模式安装到环境中。

---

### 3.3. 开发独立的第三方驱动包

此流程适用于希望独立开发和分发自定义驱动的开发者。此方式**完全无需**修改 Fustor Monorepo 主仓库的任何代码。

1.  **创建标准 Python 项目**: 您的驱动就是一个标准的 Python 包。

2.  **定义依赖与入口点**: 在您的 `pyproject.toml` 文件中，声明对 `fustor-core` 的依赖，并定义好入口点。
    ```toml
    [project]
    name = "my-fustor-driver"
    dependencies = [
        "fustor-core", # 必须依赖核心包以获取ABC
        # ... 其他依赖
    ]

    [project.entry-points."fustor.drivers.sources"]
    my_driver = "my_driver:MyDriver"
    ```

3.  **实现驱动类**: 实现继承自 `SourceDriver` 或 `PusherDriver` 的驱动类。

4.  **本地测试**:
    -   创建一个虚拟环境: `python -m venv .venv`
    -   安装 Fustor Agent 主程序: `pip install fustor-agent`
    -   以可编辑模式安装您的驱动: `pip install -e .`
    -   现在，运行 `fustor-agent`，它应该能自动发现并使用您的驱动。

5.  **发布 (可选)**: 您可以将您的包构建并发布到 PyPI。

## 4. 调试与日志

- **日志路径**: 默认日志文件位于 `~/.fustor/fustor_agent.log`。您可以通过 `FUSTOR_CONFIG_DIR` 环境变量修改配置和日志的根目录。

## 5. 测试

- **自动化测试**: 项目使用 `pytest` 和 `Playwright` 进行全自动测试。
- **禁止阻塞**: 在编写测试用例时，**严禁使用 `pause()`** 或任何需要手动介入的函数。所有测试和开发流程都必须是全自动的。

## 6. 代码贡献流程

1.  **理解目标**: 仔细阅读文档中关于模块的设计目标和重构方案。
2.  **制定计划**: 制定一个详细的、多步骤的重构计划。
3.  **分步实施与验证**: 每完成一个步骤，都要立即运行并检查效果是否符合预期。
4.  **回顾与检查**: 当认为重构完成后，必须回顾方案文档，检查是否有遗漏的需求。

## 7. 开发者工具

### `dev-tools/project_merge.py`

这是一个辅助工具脚本，用于将项目中的多个代码和文档文件合并成一个单独的文件，方便进行整体分析和处理。

- **使用方法**:
  ```bash
  # 合并 src 目录下的 python 相关文件
  python dev-tools/project_merge.py ./src
  ```