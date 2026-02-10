# Fustor 文件同步场景部署指南 (FS Scenario)

本文档针对 **文件系统同步** 场景（Source-FS -> View-FS/Fusion），指导管理员部署 Fustor 平台。

## ⚠️ 1. 环境准备 (Linux)

在部署之前，必须完成以下两项准备工作：

### 1.1 增加 inotify 监听上限
Agent 监控大量文件需要提高内核配额。请直接将其提升至 **1000 万**：
```bash
# 临时生效
sudo sysctl fs.inotify.max_user_watches=10000000
# 永久生效
echo "fs.inotify.max_user_watches=10000000" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

### 1.2 创建配置目录
Fustor 默认在用户家目录运行，请先创建必要的文件夹：
```bash
mkdir -p ~/.fustor/fusion-config
mkdir -p ~/.fustor/agent-config
mkdir -p ~/.fustor/logs
```

---

## 2. 软件安装

推荐使用 `uv` 或 `pip` 安装。

### 服务端 (Fusion Node)
```bash
uv pip install fustor-fusion fustor-view-fs
```

### 采集端 (Source Node)
```bash
uv pip install fustor-agent fustor-source-fs fustor-sender-http
```

---

## 3. 架构理解

### 3.1 端口说明
*   **8101 (管理/查询端口)**: 用户通过此端口调用 REST API 查询文件树或管理服务。
*   **18888 (数据接收端口)**: 由 `Receiver` 插件开启，专门用于接收 Agent 推送的数据。

### 3.2 路径归一化 (核心逻辑)
Fustor Agent 会将“本地挂载路径”转换为“逻辑路径”。
*   **服务器 A** 监控 `/mnt/data/share`，文件 `/mnt/data/share/1.txt` 转换后为 `/1.txt`。
*   **服务器 B** 监控 `/opt/nfs_share`，相同文件 `/opt/nfs_share/1.txt` 转换后也是 `/1.txt`。
*   **结果**: Fusion 会自动将两个不同挂载点的节点数据合并到同一个逻辑视图中。

---

## 4. 服务端配置 (Fusion)

目标：配置 Fusion 接收数据，并在内存中构建文件系统索引。

配置文件路径: `~/.fustor/fusion-config/default.yaml`

```yaml
# 1. 配置数据接收器 (监听 18888 端口)
receivers:
  http-receiver:
    driver: http
    port: 18888
    api_keys:
      - key: "fs-sync-secret-key"    # 鉴权密钥，Agent 必须持有此 Key
        pipe_id: "fs-sync-pipe"      # 绑定到下游管道

# 2. 配置 FS 视图 (内存索引)
views:
  backup-view:
    driver: fs
    driver_params:
      hot_file_threshold: 60.0

# 3. 绑定管道 (连接接收器与视图)
pipes:
  fs-sync-pipe:
    receiver: http-receiver
    views:
      - backup-view
```

**启动 Fusion**:
```bash
fustor-fusion start -D
```

---

## 5. 采集端配置 (Agent)

在每台需要同步的服务器上，创建一个 `default.yaml`。

### 服务器 A (Agent-1)
配置文件路径: `~/.fustor/agent-config/default.yaml`

```yaml
sources:
  nfs-source:
    driver: fs
    uri: "/mnt/data/project_share"  # 本地挂载点

senders:
  fusion-main:
    driver: fusion
    uri: "http://<FUSION_IP>:18888" # 指向 Fusion 的数据接收端口
    credential:
      key: "fs-sync-secret-key"     # 必须与 Fusion 配置的 API Key 一致

pipes:
  sync-job:
    source: nfs-source
    sender: fusion-main
```

### 服务器 B (Agent-2)
配置文件路径: `~/.fustor/agent-config/default.yaml`
*(配置与 A 基本一致，只需修改 `uri` 为 B 服务器对应的挂载路径即可)*

**启动 Agent**:
```bash
# 在各服务器分别执行
fustor-agent start -D
```

---

## 6. 验证与排查

### 6.1 验证同步
1.  在服务器 A 执行: `touch /mnt/data/project_share/node_a.txt`
2.  在服务器 B 执行: `touch /home/admin/mnt/share/node_b.txt`
3.  查询 Fusion API (端口 8101):
    ```bash
    curl -H "X-API-Key: fs-sync-secret-key" \
         "http://localhost:8101/api/v1/views/backup-view/tree?path=/"
    ```
    *预期结果：JSON 响应中同时包含 node_a.txt 和 node_b.txt。*

### 6.2 故障排查
*   **状态检查**: 执行 `fustor-agent status` 查看任务是否为 `RUNNING`。
*   **日志观察**:
    *   Agent: `tail -f ~/.fustor/logs/agent.log`
    *   Fusion: `tail -f ~/.fustor/logs/fusion.log`
*   **常见问题**:
    *   `Target pipes: []`: 检查配置文件名是否为 `default.yaml`。
    *   `401 Unauthorized`: 检查 Agent 配置的 API Key 是否与 Fusion 一致。
    *   `Connection Refused`: 检查 Fusion 的 `18888` 端口是否被防火墙拦截。
    *   `View Not Ready`: 首次同步大量文件时，请稍等片刻待 Snapshot 完成。