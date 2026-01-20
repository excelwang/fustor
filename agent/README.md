# Fustor Agent 服务

Fustor Agent 是一款轻量、可扩展的数据采集与推送工具。它负责监听数据源变更，并将其实时推送到 Fustor Fusion 服务。

## 安装

```bash
pip install fustor-agent
# 安装文件系统源驱动
pip install fustor-source-fs
```

### 1. 配置

Fustor Agent 使用一个主目录来存放配置和状态。
*   **默认路径**: `~/.fustor`
*   **自定义路径**: 设置 `FUSTOR_HOME` 环境变量。

Agent 的核心配置文件位于 Fustor 主目录下的 `agent-config.yaml`。你需要定义 `sources` (数据源)、`pushers` (推送目标) 和 `syncs` (同步任务)。

### 1. 配置 Source (数据源)

以文件系统 (FS) 为例：

```yaml
sources:
  - id: "my-local-files"       # 唯一 ID
    type: "fs"                 # 驱动类型
    config:
      uri: "/data/research"    # 监控的绝对路径
      driver_params:
        # 可选：文件过滤模式
        file_pattern: "*"      
```

### 2. 配置 Pusher (推送目标)

通常推送到 Fusion 服务：

```yaml
pushers:
  - id: "to-fusion"            # 唯一 ID
    type: "fusion"             # 驱动类型
    config:
      # Fusion 服务的 Ingest API 地址
      endpoint: "http://localhost:8102/ingestor-api/v1/events"
      # 从 Registry 获取的 API Key，用于鉴权
      credential: "YOUR_API_KEY_HERE"
```

### 3. 配置 Sync (同步任务)

将 Source 和 Pusher 绑定：

```yaml
syncs:
  - id: "sync-files-to-fusion"
    source_id: "my-local-files"
    pusher_id: "to-fusion"
    enabled: true              # 设置为 true 以自动启动
```

## 命令指南

*   **启动服务**: `fustor-agent start -D` (后台运行) 或 `fustor-agent start` (前台运行)
*   **停止服务**: `fustor-agent stop`
*   **查看状态**: 访问 `http://localhost:8100` 查看 Web 控制台。

## 更多文档

*   **驱动开发**: 详见 `docs/driver_design.md`
