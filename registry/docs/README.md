# Fustor Registry 服务文档

Fustor Registry 是平台的控制平面，负责管理元数据、权限和配置。

## 安装

```bash
pip install fustor-registry
```

## 配置

Fustor Registry 使用一个主目录来存放配置、日志和数据库。
*   **默认路径**: `~/.fustor`
*   **自定义路径**: 设置 `FUSTOR_HOME` 环境变量。

在 Fustor 主目录下的 `.env` 中配置数据库连接（支持 SQLite/PostgreSQL）：

```env
DATABASE_URL=sqlite+aiosqlite:///registry.db
```

## 命令指南

*   **启动服务**:
    ```bash
    fustor-registry start -D
    ```
    服务默认运行在端口 `8101`。

*   **停止服务**:
    ```bash
    fustor-registry stop
    ```

## 管理员操作

Registry 提供了 RESTful API 用于管理操作，建议通过 Swagger UI (`http://localhost:8101/docs`) 进行操作。

### 关键流程

1.  **创建 Datastore**:
    *   端点: `POST /api/v1/admin/datastores`
    *   作用: 定义一个逻辑存储库（例如 "Project X Data"）。

2.  **生成 API Key**:
    *   端点: `POST /api/v1/admin/apikeys`
    *   作用: 为指定的 Datastore 生成访问凭证。
    *   **重要**: 此 Key 是连接整个平台的纽带，Agent 用它来推送数据，Fusion 用它来验证权限，User 用它来查询数据。

3.  **配置管理**:
    *   端点: `PATCH /api/v1/admin/datastores/{id}/config`
    *   作用: 配置 Datastore 的策略（如是否允许并发推送、会话超时时间等）。