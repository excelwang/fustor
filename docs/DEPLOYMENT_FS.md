# Fustor 文件同步场景部署指南 (FS Scenario)

本文档针对 **文件系统同步** 场景（Source-FS -> View-FS/Fusion），指导管理员部署 Fustor 平台。

## ⚠️ 1. 环境准备 (Linux)

### 1.1 增加 inotify 监听上限
Agent 监控大量文件需要提高内核配额。请将其提升至 **1000 万**：
```bash
sudo sysctl fs.inotify.max_user_watches=10000000
echo "fs.inotify.max_user_watches=10000000" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

### 1.2 创建配置目录
```bash
mkdir -p ~/.fustor/fusion-config ~/.fustor/agent-config ~/.fustor/logs
```

---

## 2. 软件安装 (v0.8.9+)

推荐使用 `uv` 从 PyPI 安装最新版本：

### 服务端 (Fusion Node)
```bash
uv pip install fustor-fusion fustor-view-fs fustor-receiver-http
```

### 采集端 (Source Node)
```bash
uv pip install fustor-agent fustor-source-fs fustor-sender-http
```

---

## 3. 架构与进阶特性

### 3.1 端口说明
*   **8101**: 管理与查询端口 (REST API)。
*   **18888**: 数据接收端口 (由 Receiver 插件开启)。

### 3.2 双活与高可用 (Active-Active vs HA)
*   **HA 模式 (默认)**: 多个 Agent 使用**相同**的 API Key 连接同一个 Pipe，Fusion 会自动选举一个 Leader 写入，其他作为备机。
*   **双活模式**: 多个 Agent 使用**不同**的 API Key 连接各自的 Pipe，但所有 Pipe 绑定到**同一个 View**。Fusion 会将多源数据实时合并到该统一视图中。

### 3.3 读写分离鉴权
*   **Ingestion Key**: 用于 Agent 推送，权限受限于绑定的 Pipe。
*   **Query Key**: 用于外部查询 View，不具备推送权限，安全性更高。

---

## 4. 服务端配置 (Fusion)

配置文件: `~/.fustor/fusion-config/default.yaml`

```yaml
# 1. 配置接收器与推送密钥 (写入端)
receivers:
  http-receiver:
    driver: http
    port: 18888
    api_keys:
      - key: "agent-1-push-key"
        pipe_id: "pipe-1"
      - key: "agent-2-push-key"
        pipe_id: "pipe-2"

# 2. 配置视图与查询密钥 (读取端)
views:
  unified-view:
    driver: fs
    api_keys:
      - "external-read-only-key" # 专门用于查询的 Key
    driver_params:
      hot_file_threshold: 60.0

# 3. 绑定关系 (多管道汇聚)
pipes:
  pipe-1:
    receiver: http-receiver
    views: [unified-view]
  pipe-2:
    receiver: http-receiver
    views: [unified-view]
```

---

## 5. 采集端配置 (Agent)

### 服务器 A (Agent-1)
`~/.fustor/agent-config/default.yaml`:
```yaml
sources:
  nfs-source:
    driver: fs
    uri: "/mnt/data/share_a"
senders:
  fusion-main:
    driver: fusion
    uri: "http://<FUSION_IP>:18888"
    credential: { key: "agent-1-push-key" }
pipes:
  sync-job: { source: nfs-source, sender: fusion-main }
```

### 服务器 B (Agent-2)
`~/.fustor/agent-config/default.yaml`:
```yaml
sources:
  nfs-source:
    driver: fs
    uri: "/opt/storage_b" # 不同挂载点
senders:
  fusion-main:
    driver: fusion
    uri: "http://<FUSION_IP>:18888"
    credential: { key: "agent-2-push-key" }
pipes:
  sync-job: { source: nfs-source, sender: fusion-main }
```

---

## 6. 验证与运维

### 6.1 查询统一视图
使用 **Query Key** 访问 API：
```bash
curl -H "X-API-Key: external-read-only-key" \
     "http://localhost:8101/api/v1/views/unified-view/tree?path=/"
```

### 6.2 健壮性说明
*   **启动容错**: 如果 Agent 监控的目录尚未挂载或不存在，Agent 会打印 Warning 并持续等待，**不会崩溃**。一旦目录准备就绪，系统会自动开始同步。
*   **性能监控**: 访问 `http://localhost:8101/view` 查看 Web Dashboard (需要使用有效的 API Key)。

### 6.3 故障排查
*   `401 Unauthorized`: 检查 `X-API-Key` 及其对应的权限类型（写入 vs 读取）。
*   `503 Service Unavailable`: 视图正在进行初始快照同步，请稍等或通过 `stats` 接口查看进度。
