# Agent 驱动开发指南

本文档提供了为 Fustor Agent 开发自定义驱动（Source 和 Pusher）的完整指南。

## 1. 驱动开发契约 (Driver Contract)

所有驱动都遵循统一的开发契约。一个合格的驱动是一个独立的 Python 包，其中包含一个**必须继承**自 `SourceDriver` 或 `PusherDriver` 抽象基类（ABC）的**驱动类**。开发者必须实现该基类定义的所有抽象方法。

-   **核心基类**:
    -   `fustor_core.drivers.SourceDriver`
    -   `fustor_core.drivers.PusherDriver`
-   **核心数据模型**:
    -   `fustor_event_model.models.EventBase`

### 关键接口摘要

*   **`SourceDriver.get_message_iterator`**: 返回实时事件流迭代器。事件必须包含 `message_source='realtime'`。
*   **`SourceDriver.get_snapshot_iterator`**: 返回全量快照迭代器。事件必须包含 `message_source='snapshot'`。
*   **`SourceDriver.get_audit_iterator`** (可选): 返回审计差异迭代器。事件必须包含 `message_source='audit'` 和 `parent_mtime` 字段。
*   **`PusherDriver.push`**: 将事件推送到远端，必须正确传递 `message_source` 字段。

### 消息类型 (`message_source`)

| 类型 | 说明 | 必需字段 |
|------|------|----------|
| `realtime` | inotify 事件，优先级最高 | `path`, `mtime`, `size` |
| `snapshot` | 全量扫描 | `path`, `mtime`, `size` |
| `audit` | 审计差异 | `path`, `mtime`, `size`, **`parent_mtime`** |

详见 [一致性设计 (CONSISTENCY_DESIGN.md)](./CONSISTENCY_DESIGN.md)

---

## 2. Audit Sync Phase 接口

支持 Audit 的 SourceDriver 需要实现以下接口：

```python
def get_audit_iterator(self, **kwargs) -> Iterator[EventBase]:
    """
    返回审计差异事件流。
    
    每个事件必须包含:
    - message_source: "audit"
    - parent_mtime: 扫描时父目录的 mtime
    
    同时需要发送生命周期信号:
    - Audit-Start: 审计开始时
    - Audit-End: 审计结束时
    """
    pass
```

---

## 3. 作为贡献者在本仓库中添加驱动

此流程适用于希望为 Fustor Agent 官方仓库贡献新驱动的开发者。

1.  **创建插件包结构**: 在 `extensions/` 目录下为新驱动创建一个符合 `[type]-[name]` 命名规范的目录。
    ```bash
    mkdir -p extensions/source-postgres/src/fustor_source_postgres
    ```

2.  **创建 `pyproject.toml`**: 在 `extensions/source-postgres/` 目录下创建 `pyproject.toml` 文件。
    ```toml
    [project]
    name = "fustor-source-postgres"
    dependencies = [ "psycopg2-binary" ]

    [project.entry-points."fustor.drivers.sources"]
    postgres = "fustor_source_postgres:PostgresDriver"
    ```

3.  **实现驱动类**: 在 `.../__init__.py` 文件中，创建驱动类并实现所有抽象方法。

4.  **注册到工作空间**: 打开**根目录**的 `pyproject.toml` 文件，将新驱动的路径添加到 `[tool.uv.workspace].members` 列表中。

5.  **更新环境**: 在**根目录**下重新运行 `uv sync --extra dev`。

---

## 4. 开发独立的第三方驱动包

此流程适用于希望独立开发和分发自定义驱动的开发者。

1.  **创建标准 Python 项目**: 您的驱动就是一个标准的 Python 包。

2.  **定义依赖与入口点**:
    ```toml
    [project]
    name = "my-fustor-driver"
    dependencies = [
        "fustor-core",
        "fustor-event-model",
    ]

    [project.entry-points."fustor.drivers.sources"]
    my_driver = "my_driver:MyDriver"
    ```

3.  **实现驱动类**: 实现继承自 `SourceDriver` 的驱动类，确保正确设置 `message_source` 字段。

4.  **本地测试**:
    ```bash
    uv venv .venv
    pip install fustor-agent
    pip install -e .
    fustor-agent start
    ```

5.  **发布 (可选)**: 您可以将您的包构建并发布到 PyPI。
