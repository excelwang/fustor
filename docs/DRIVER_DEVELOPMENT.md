# Agent 驱动开发指南

本文档提供了为 Fustor Agent 开发自定义驱动（Source 和 Pusher）的完整指南。

## 1. 驱动开发契约 (Driver Contract)

所有驱动都遵循统一的开发契约。一个合格的驱动是一个独立的 Python 包，其中包含一个**必须继承**自 `SourceDriver` 或 `PusherDriver` 抽象基类（ABC）的**驱动类**。开发者必须实现该基类定义的所有抽象方法。

-   **核心基类**:
    -   `fustor_core.drivers.SourceDriver`
    -   `fustor_core.drivers.PusherDriver`
-   **核心数据模型**:
    -   `fustor_event_model.models.EventBase`

### 关键接口变更摘要

*   **`SourceDriver.get_message_iterator`**: 此方法现在必须返回一个元组 `Tuple[Iterator[EventBase], bool]`。第二个布尔值 `needed_position_lost` 用于向系统发出信号，表明驱动无法从请求的点位开始，并已从最新位置回退启动。
*   **`PusherDriver.push`**: 此方法现在遵循"信封协议"。它接收 `is_snapshot_end` 和 `snapshot_sync_suggested` 等控制参数，并将其打包在发往远端的JSON"信封"中。同时，它必须解析远端响应，并返回一个包含 `snapshot_needed` 标志的字典。

*(所有方法的详细签名、参数和返回值，请参考 `packages/fustor_core/src/fustor_core/drivers.py` 中的源码)*

---

## 2. 作为贡献者在本仓库中添加驱动

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

5.  **更新环境**: 在**根目录**下重新运行 `uv sync --extra dev`，`uv` 就会将您的新驱动以可编辑模式安装到环境中。

---

## 3. 开发独立的第三方驱动包

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
    -   创建一个虚拟环境: `uv venv .venv`
    -   安装 Fustor Agent 主程序: `pip install fustor-agent`
    -   以可编辑模式安装您的驱动: `pip install -e .`
    -   现在，运行 `fustor-agent`，它应该能自动发现并使用您的驱动。

5.  **发布 (可选)**: 您可以将您的包构建并发布到 PyPI。
