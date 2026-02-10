# Fustor 文件同步场景部署指南 (FS Scenario)

本文档针对 **文件系统同步** 场景（Source-FS -> View-FS/Fusion），指导管理员部署 Fustor 平台。

## ⚠️ 关键系统要求 (Linux)

在部署 Agent 之前，**必须** 检查并增加系统的 `inotify` 监听数量上限。默认值通常较小（8192），不足以监控大量文件。请直接将其提升至 **1000 万** 以确保生产环境的稳定性。

### 检查当前限制
```bash
cat /proc/sys/fs/inotify/max_user_watches
```

### 修改限制 (使用 sudo)

1.  **临时生效 (立即生效，重启失效)**:
    ```bash
    sudo sysctl fs.inotify.max_user_watches=10000000
    ```

2.  **永久生效 (写入配置文件)**:
    ```bash
    echo "fs.inotify.max_user_watches=10000000" | sudo tee -a /etc/sysctl.conf
    sudo sysctl -p
    ```

> **关于 1000 万监控上限的说明**：
> Fustor Agent 内部默认支持最高 1000 万个监控项。调整 `max_user_watches` 参数仅是提高内核允许的配额上限，**并不会** 直接预分配内存。实际的内存消耗取决于**当前活跃的监控项数量**。每个被监控的文件/目录仅消耗约 0.5KB ~ 1KB 的内核内存。即使监控 100 万个文件，额外内存占用也仅约 1GB。对于现代服务器而言，将其上限设为高值（如 1000 万）是为了防止业务增长触顶，对空闲系统毫无性能影响，请放心调整。

---

## 1. 软件安装

推荐使用 `uv` 或 `pip` 安装。

### 服务端 (Fusion Node)
安装核心服务及文件系统视图驱动：
```bash
uv pip install fustor-fusion fustor-view-fs
```

### 采集端 (Source Node)
安装 Agent 及文件系统源驱动：
```bash
uv pip install fustor-agent fustor-source-fs fustor-sender-http
```

---

## 2. 架构与流程

### 2.1 多端部署架构图

下图展示了典型的多 Agent 协同部署架构，它们共同监控一个 NFS 存储，通过 HTTP 将数据推送到 Fusion，Fusion 在内存中构建统一的文件索引视图。

```ascii
+-----------------------+       +-----------------------+
|  Agent A (Primary)    |       |  Agent B (Secondary)  |
|  (Source: FS)         |       |  (Source: FS)         |
|                       |       |                       |
|  Monitors:            |       |  Monitors:            |
|  /mnt/data/share      |       |  /home/admin/share    |
+-----------+-----------+       +-----------+-----------+
            | Step 1: Capture & Push        |
            +------------+     +------------+
                         |     |
                         v     v
                 +-----------------------+
                 |    Fusion Server      |
                 |    (Receiver: HTTP)   |
                 +-----------+-----------+
                             |
                   Running "Pipe" Logic
                             |
                 +-----------v-----------+
                 |    View (In-Memory)   |
                 |    (Unified Index)    |
                 +-----------------------+
                             ^
                             | Step 2: Query
                     +-------+-------+
                     |    Client     |
                     |  (via API)    |
                     +---------------+
```

### 2.2 核心概念：Pipe (管道)

在 Fustor 中，**Pipe** 是连接数据源 (Source/Receiver) 与 数据目标 (Sender/View) 的逻辑通道。

*   **在 Agent 端**: Pipe 定义了 "从哪个本地目录读取 (Source)" 以及 "发送给哪个 Fusion 服务 (Sender)"，并包含同步策略（如心跳间隔、审计频率）。
*   **在 Fusion 端**: Pipe 定义了 "从哪个网络端口接收 (Receiver)" 以及 "写入哪个内存视图 (View)"，并包含鉴权绑定（API Key -> Pipe ID）。

简而言之，**Pipe 是数据流动的定义者**，它将解耦的组件（采集、传输、存储）串联起来形成完整的业务流。

---

## 3. 服务端配置 (Fusion)

目标：配置 Fusion 接收数据，并在内存中构建文件系统索引。

**注意**：Fusion 的 `view-fs` 驱动是 **纯内存 (In-Memory)** 索引，它**不会**在服务端磁盘上重建实际的文件结构。所有的元数据（路径、大小、修改时间等）均存储在内存哈希树中，以提供极致的查询性能。

配置文件路径: `~/.fustor/fusion-config/fs_setup.yaml`

```yaml
# 1. 配置 HTTP 接收器
receivers:
  http-receiver:
    driver: http
    bind_host: "0.0.0.0"
    port: 18888                # Agent 推送数据的端口
    api_keys:
      - key: "fs-sync-secret-key"    # 鉴权密钥
        pipe_id: "fs-sync-pipe"      # 绑定管道 ID

# 2. 配置 FS 视图 (View-FS)
views:
  backup-view:
    driver: fs
    driver_params:
      hot_file_threshold: 60.0
      max_tree_items: 100000   # 防止单次返回数据过大导致响应缓慢(默认 10W)

# 3. 绑定管道 (Pipe)
pipes:
  fs-sync-pipe:
    receiver: http-receiver  # 数据入口
    views:                   # 数据出口
      - backup-view
    session_timeout_seconds: 3600  # 会话超时时间 (也控制 Agent 掉线后的清除时间)
```

---

## 4. 采集端配置 (Agent) - 多节点协同示例

在生产环境中，通常会部署两个或以上的 Agent 来监控同一个 NFS/共享存储，以实现高可用 (HA) 和数据一致性校验。

**核心特性**：
*   **路径归一化**：Fustor Agent 自动计算相对路径。因此，**即使两台服务器对同一个 NFS 的挂载路径不同，Fusion 也能正确地将数据合并到同一个视图中**。
*   **协同工作**：多台 Agent 会自动协商选主 (Leader/Follower)，Leader 负责快照和审计，Followers 负责实时监听。

### 服务器 A (Agent-1)
*   **角色**: 主采集节点 (Primary)
*   **挂载点**: `/mnt/data/project_share`

配置文件路径: `~/.fustor/agent-config/source_node_a.yaml`

```yaml
sources:
  nfs-source-a:
    driver: fs
    uri: "/mnt/data/project_share"

senders:
  fusion-main:
    driver: fusion
    uri: "http://<FUSION_IP>:18888"  # 指向 Fusion 的 Receiver 端口
    credential:
      key: "fs-sync-secret-key"

pipes:
  sync-job:
    source: nfs-source-a
    sender: fusion-main
    
    # 心跳与审计策略
    heartbeat_interval_sec: 5.0
    audit_interval_sec: 600.0
    sentinel_interval_sec: 60.0
    session_timeout_seconds: 3600
```

### 服务器 B (Agent-2)
*   **角色**: 备采集节点 (Secondary) / 协同节点
*   **挂载点**: `/home/admin/mnt/share` (注意：挂载路径与 A 不同，但内容一致)

配置文件路径: `~/.fustor/agent-config/source_node_b.yaml`

```yaml
sources:
  nfs-source-b:
    driver: fs
    uri: "/home/admin/mnt/share"

senders:
  fusion-main:
    driver: fusion
    uri: "http://<FUSION_IP>:18888"
    credential:
      key: "fs-sync-secret-key"

pipes:
  sync-job:
    source: nfs-source-b
    sender: fusion-main
    
    # 策略配置保持与 A 一致
    heartbeat_interval_sec: 5.0
    audit_interval_sec: 600.0
    sentinel_interval_sec: 60.0
    session_timeout_seconds: 3600
```

---

## 5. 启动与验证

### 步骤 1: 启动 Fusion
```bash
# 后台运行
fustor-fusion start -D
```
*检查日志 `~/.fustor/logs/fusion.log` 确认无报错。*

### 步骤 2: 分别启动 Agent A 和 Agent B
```bash
# 在服务器 A
fustor-agent start -D

# 在服务器 B
fustor-agent start -D
```
*此时，先启动的 Agent 通常会成为 Leader，负责全量快照。后启动的 Agent 自动作为 Follower，提供冗余的实时监听。*

### 步骤 3: 验证同步与合并

1.  **在服务器 A 写入文件**:
    ```bash
    touch /mnt/data/project_share/from_node_a.txt
    ```

2.  **在服务器 B 写入文件**:
    ```bash
    touch /home/admin/mnt/share/from_node_b.txt
    ```

3.  **通过 Fusion API 查询**:
    即使源自不同路径，Fusion 视图中应同时包含这两个文件：

    ```bash
    curl -H "X-API-Key: fs-sync-secret-key" \
         "http://localhost:8101/api/v1/views/backup-view/tree?path=/"
    ```

    **预期的 JSON 响应**:
    ```json
    {
      "name": "",
      "content_type": "directory",
      "path": "/",
      "children": [
        {
          "name": "from_node_a.txt",
          "content_type": "file",
          "path": "/from_node_a.txt",
          "size": 0,
          "...": "..."
        },
        {
          "name": "from_node_b.txt",
          "content_type": "file",
          "path": "/from_node_b.txt",
          "size": 0,
          "...": "..."
        }
      ]
    }
    ```
