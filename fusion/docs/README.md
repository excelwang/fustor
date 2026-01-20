# Fustor Fusion 服务文档

Fustor Fusion 是平台的数据摄取核心，负责接收来自 Agent 的数据流，维护实时目录树，并提供查询和监控接口。

## 安装

```bash
pip install fustor-fusion
```

## 配置

Fusion 需要连接到 Registry 服务以验证 API Key。
Fustor Fusion 使用一个主目录来存放配置和日志。
*   **默认路径**: `~/.fustor`
*   **自定义路径**: 设置 `FUSTOR_HOME` 环境变量。

在 Fustor 主目录下的 `.env` 文件中配置：

```bash
FUSTOR_REGISTRY_URL=http://localhost:8101
```

## 命令指南

*   **启动服务**: 
    ```bash
    fustor-fusion start -D
    ```
    服务默认运行在端口 `8102`。

*   **停止服务**:
    ```bash
    fustor-fusion stop
    ```

## 功能与接口

### 1. 监控仪表盘 (Dashboard)
访问 `http://localhost:8102/view`。
这是一个实时可视化的监控页面，展示数据流拓扑、处理延迟和数据量。需要输入有效的 API Key 才能查看。

### 2. 数据查询 API
Fusion 提供了内存中的实时文件目录视图：

*   `GET /views/fs/tree`: 获取目录树结构。
    *   参数: `path` (默认 `/`)
*   `GET /views/fs/search`: 搜索文件。
    *   参数: `pattern` (例如 `*.log`)
*   `GET /views/fs/stats`: 获取目录统计信息（文件数、大小、延迟等）。

所有 API 请求都需要在 Header 中包含 `X-API-Key`。