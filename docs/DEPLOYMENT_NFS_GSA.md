# Fustor NFS 高可用文件同步部署指南 (GSA 分布式)

本文档针对您提供的环境：多台挂载同一 NFS 的服务器作为 Agent，一台独立服务器 (`192.168.166.12`) 作为 Fusion 中心节点。

## 1. 系统调优 (所有服务器)

由于文件总数达到 **1000 万**，必须大幅提升 `inotify` 监听上限。

### 1.1 提升 Inotify 配额
在 **所有 Agent 服务器** 上执行：
```bash
sudo sysctl fs.inotify.max_user_watches=12000000
echo "fs.inotify.max_user_watches=12000000" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

### 1.2 创建工作目录
```bash
mkdir -p ~/.fustor/fusion-config ~/.fustor/agent-config ~/.fustor/logs
```

---

## 2. 软件安装 (使用 uv)

### 2.1 安装 uv (如果尚未安装)
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.cargo/env
```

### 2.2 安装 Fustor 组件
**在 Fusion 服务器上：**
```bash
uv pip install fustor-fusion fustor-view-fs fustor-receiver-http
```

**在所有 Agent 服务器上：**
```bash
uv pip install fustor-agent fustor-source-fs fustor-sender-http
```

---

## 3. 配置文件部署

### 3.1 Fusion 节点配置 (192.168.166.12)
创建 `~/.fustor/fusion-config/default.yaml`:

```yaml
# 全局管理端口
port: 8101

receivers:
  http-receiver:
    driver: http
    port: 8102  # 数据接收端口
    api_keys:
      - key: "gsa-shared-token"  # 统一推送 Token
        pipe_id: "gsa-pipe"

views:
  gsa-fs-tree:
    driver: fs
    api_keys:
      - "read-query-key" # 用于前端或 API 查询的 Key
    driver_params:
      hot_file_threshold: 60.0
      # 针对 30 万文件的超大目录调优
      # 默认 100,000，调大以允许一次性列出该目录所有文件
      max_tree_items: 500000 

pipes:
  gsa-pipe:
    receiver: http-receiver
    views: [gsa-fs-tree]
    # 须开启并发推送
    allow_concurrent_push: true 
```

### 3.2 Agent 节点配置 (所有 Agent)
创建 `~/.fustor/agent-config/default.yaml`:

```yaml
sources:
  nfs-storage:
    driver: fs
    uri: "/upload/submission"  # NFS 挂载并监控的路径

senders:
  fusion-center:
    driver: fusion
    uri: "http://192.168.166.12:8102"
    credential: { key: "gsa-shared-token" }

pipes:
  gsa-sync:
    source: nfs-storage
    sender: fusion-center
    # 针对千万级文件的审计调优：12小时扫描一次
    audit_interval_sec: 43200 
    # 热文件状态巡检：5分钟一次
    sentinel_interval_sec: 300
```

---

## 4. 服务启动与验证

### 4.1 启动 Fusion
```bash
fustor-fusion start -D
```

### 4.2 启动 Agent (逐台启动)
```bash
fustor-agent start -D
```

### 4.3 验证高可用状态
访问 Fusion 的管理面板或通过 API 查看选主情况：
```bash
# 查看所有 Session，应能看到多个 Agent，且其中一个是 Leader
curl http://localhost:8101/api/v1/views/gsa-fs-tree/sessions
```

### 4.4 监控大目录
针对 30 万文件的目录，可以验证查询性能：
```bash
curl -H "X-API-Key: read-query-key" \
     "http://192.168.166.12:8101/api/v1/views/gsa-fs-tree/tree?path=/&max_depth=1"
```

---

## 5. 运维建议
1. **日志监控**：观察 `~/.fustor/logs/agent.log`。初期 1000 万文件的初始扫描（Snapshot）可能需要较长时间，此时api会返回503，`detail` 确认进度。
2. **NFS 性能**：由于 NFS 的延迟特性，建议 Agent 服务器的 CPU 核心数不少于 4 核，以应对 `RecursiveScanner` 的并发压力。
3. **监控面板**：访问 `http://192.168.166.12:8101/view` 即可看到可视化的同步状态。
