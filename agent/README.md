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

## 数据可靠性保证 (Data Reliability)

Agent 作为一个高效的数据分发管道，遵循 **“先感知、后推送”** 的原则。为了确保 Fusion 获取到的数据均是完整且稳定的，Agent 依赖于 Source Driver 实现以下逻辑：

1.  **静默期过滤**：Driver 必须负责识别并过滤掉处于活跃写入状态的文件元数据。
2.  **消息补偿**：对于快照扫描期间被跳过的活跃文件，Agent 必须通过实时消息流（如 inotify）进行最终补全推送。
3.  **状态隔离**：在初始快照完成前，Pusher 应向 Fusion 发送明确的 `source_type='snapshot'` 标识，Fusion 利用此标识通过 503 状态码保护下游查询。

## 更多文档

*   **驱动开发**: 详见 `docs/driver_design.md`
