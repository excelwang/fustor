# Fustor 文件同步场景部署指南 (FS Scenario)

本文档指导管理员如何部署 Fustor 平台以实现多源文件系统同步（Source-FS -> View-FS/Fusion）。

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

> **Python 版本要求**：Python ≥ 3.11

推荐使用 `uv` 从 PyPI 安装最新版本：

```bash
# 安装 uv（推荐）
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 服务端 (Fusion Node)
```bash
# 推荐使用 uv
uv pip install fustor-fusion fustor-view-fs fustor-receiver-http

# 或使用 pip
pip install fustor-fusion fustor-view-fs fustor-receiver-http
```

### 采集端 (Source Node)
```bash
# 推荐使用 uv
uv pip install fustor-agent fustor-source-fs fustor-sender-http

# 或使用 pip
pip install fustor-agent fustor-source-fs fustor-sender-http
```

---

## 3. 架构配置示例

### 3.1 服务端 (Fusion)
配置文件: `~/.fustor/fusion-config/default.yaml`

```yaml
receivers:
  http-receiver:
    driver: http
    port: 18888
    api_keys:
      - key: "agent-1-push-key"
        pipe_id: "pipe-1"
      - key: "agent-2-push-key"
        pipe_id: "pipe-2"

views:
  unified-view:
    driver: fs
    api_keys:
      - "external-read-only-key"
    driver_params:
      hot_file_threshold: 60.0  # 60秒内修改的文件标记为 suspect

pipes:
  pipe-1:
    receiver: http-receiver
    views: [unified-view]
    audit_interval_sec: 43200 # 12 hours
  pipe-2:
    receiver: http-receiver
    views: [unified-view]
    audit_interval_sec: 43200 # 12 hours
```

### 3.2 采集端 (Agent)
配置文件: `~/.fustor/agent-config/default.yaml`

```yaml
sources:
  nfs-source:
    driver: fs
    uri: "/mnt/data/share"
senders:
  fusion-main:
    driver: fusion
    uri: "http://<FUSION_IP>:18888"
    credential: { key: "agent-1-push-key" }
pipes:
  sync-job: { source: nfs-source, sender: fusion-main }
```

---


## 3. 运行服务

安装完成后，请使用 `start` 命令启动服务。默认情况下，服务在前台运行，建议在测试完成后使用 `-D` 参数转入后台。

### 3.1 启动 Fusion
```bash
# 前台启动（查看实时日志）
fustor-fusion start

# 后台启动
fustor-fusion start -D
```

### 3.2 启动 Agent
```bash
# 前台启动
fustor-agent start

# 后台启动
fustor-agent start -D
```

---

## 4. 生产环境部署 (systemd)

在生产环境中，建议使用 `systemd` 管理 Fustor 进程，确保服务在意外崩溃后能够自动重启（Self-Healing）。

### 4.1 Fustor Agent Service

创建 `/etc/systemd/system/fustor-agent.service`：

```ini
[Unit]
Description=Fustor Agent Service
After=network.target

[Service]
Type=simple
User=<USER>
Group=<USER>
# 确保 path 指向正确的 fustor-agent 可执行文件 (如 uv venv)
ExecStart=/path/to/venv/bin/fustor-agent start
WorkingDirectory=/home/<USER>

# 关键: 自动重启策略
Restart=always
RestartSec=5s
StartLimitInterval=0

# 环境配置
Environment=PYTHONUNBUFFERED=1
# 如有需要，可指定配置文件路径
# Environment=FUSTOR_CONFIG=/home/<USER>/.fustor/agent-config/default.yaml

[Install]
WantedBy=multi-user.target
```

### 4.2 Fustor Fusion Service

创建 `/etc/systemd/system/fustor-fusion.service`：

```ini
[Unit]
Description=Fustor Fusion Service
After=network.target

[Service]
Type=simple
User=<USER>
Group=<USER>
ExecStart=/path/to/venv/bin/fustor-fusion start
WorkingDirectory=/home/<USER>

# 关键: 自动重启策略
Restart=always
RestartSec=5s
StartLimitInterval=0

Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

### 4.3 启用与管理

```bash
# 重载配置
sudo systemctl daemon-reload

# 启用开机自启并立即启动
sudo systemctl enable --now fustor-fusion
sudo systemctl enable --now fustor-agent

# 查看状态
sudo systemctl status fustor-agent
```

---

## 5. 管理与进阶操作

### 5.1 实时查找 (Realtime Find)
如果怀疑某个子目录因为 inotify 失效而未同步，可以强制触发针对该路径的实时查找：
```bash
curl -H "X-API-Key: external-read-only-key" \
     "http://localhost:8101/api/v1/views/unified-view/tree?path=/target/dir&force_real_time=True"
```
响应字段说明：
*   **job_id**: 该查找任务的唯一标识。
*   **find_pending**: 如果为 `true`，表示指令已发送给 Agent，正在异步执行。

### 5.2 任务追踪
通过管理 API 追踪实时查找任务的完成状态：
```bash
# 列出所有任务
curl http://localhost:8101/api/v1/management/jobs

# 查询特定任务
curl http://localhost:8101/api/v1/management/jobs/<job_id>
```

### 5.3 理解 `integrity_suspect` 标记
在查询视图时，你可能会看到某些条目带有 `"integrity_suspect": true`。
*   **含义**: 该文件处于“可疑”状态。通常是因为文件太新（处于 `hot_file_threshold` 时间窗内）或通过实时查找发现。
*   **处理**: Fustor 会在文件“冷却”（停止修改一段时间）后自动通过 Sentinel 审计清除该标记。

---

## 6. 故障排查
*   `401 Unauthorized`: 检查 `X-API-Key` 是否与 `default.yaml` 中的配置匹配。
*   `503 Service Unavailable`: 视图正在进行初始快照同步，或 Agent 尚未建立 Leader 角色。若是由于 Agent 初始化耗时过长或其他异常导致服务不可用，建议使用 **On-Demand Scan** (参见 §5.1) 针对急需访问的目录提交定点扫描任务。
*   **文件不更新**: 检查 Linux 内核 `inotify` 配额是否已提升。
