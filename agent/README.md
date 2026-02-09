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

Agent 的核心配置文件位于 Fustor 主目录下的 `agent-config.yaml`。你需要定义 `sources` (数据源)、`senders` (推送目标) 和 `pipes` (同步任务)。

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

### 2. 配置 Sender (推送目标)

通常推送到 Fusion 服务：

```yaml
senders:
  - id: "to-fusion"            # 唯一 ID
    type: "fusion"             # 驱动类型
    config:
      # Fusion 服务的 Ingest API 基准地址
      endpoint: "http://localhost:8102"
      # 从 Fusion 管理员处获取的 API Key，用于鉴权
      credential: "YOUR_API_KEY_HERE"
```

### 3. 配置 Pipe (数据管道任务)

将 Source 和 Sender 绑定：

```yaml
pipes:
  - id: "phase-files-to-fusion"
    source_id: "my-local-files"
    sender_id: "to-fusion"
    enabled: true              # 设置为 true 以自动启动
```

## 命令指南

*   **启动服务**: `fustor-agent start -D` (后台运行) 或 `fustor-agent start` (前台运行)
*   **停止服务**: `fustor-agent stop`
*   **查看状态**: 访问 `http://localhost:8100` 查看 Web 控制台。

## 数据可靠性保证 (Data Reliability)

Agent 遵循 **"瘦 Agent 感知 + 胖 Fusion 裁决"** 架构。

### Leader/Follower 模式

| 角色 | Realtime Sync Phase | Snapshot Sync Phase | Audit Sync Phase | Sentinel Sweep |
|------|---------------|---------------|------------|----------------|
| **Leader** | ✅ | ✅ | ✅ | ✅ |
| **Follower** | ✅ | ❌ | ❌ | ❌ |

- **先到先得**：第一个建立 Session 的 Agent 成为 Leader
- **故障转移**：仅当 Leader 心跳超时后，Fusion 才释放 Leader 锁

### 消息类型 (`message_source`)

| 类型 | 说明 |
|------|------|
| `realtime` | inotify 事件，优先级最高 |
| `snapshot` | Agent 启动时全量扫描 |
| `audit` | 定时审计，发现盲区变更 |

详见 `docs/CONSISTENCY_DESIGN.md`。

## 更多文档

*   **驱动开发**: 详见 `docs/driver_design.md`
*   **一致性设计**: 详见 `docs/CONSISTENCY_DESIGN.md`
