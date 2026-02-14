# Fustor 多视图聚合部署指南 (Forest View)

本文档指导如何部署 Fustor 并启用 **Forest View** 特性。该特性允许 Fusion 将多个 Pipe 的数据流聚合到一个统一的逻辑视图 (Forest) 中，内部自动维护多棵文件系统树。

## 场景描述

假设您有 2 台存储服务器，分别挂载了不同的子目录，但逻辑上属于同一个数据集：

- **节点 A (`agent-1`)**: `/mnt/data/shard-01`
- **节点 B (`agent-2`)**: `/mnt/data/shard-02`

目标是在 Fusion 端通过单一路径 `/data` 访问所有数据，自动路由到正确的节点。

---

## 1. 环境准备

请参考 [部署基础指南](DEPLOYMENT_FS.md#1-环境准备-linux) 完成系统内核参数调优。

---

## 2. 软件安装

除了基础组件外，Fusion 端需要安装 `fustor-view-fs-forest` 扩展。

### 2.1 服务端 (Fusion Node)
```bash
# 安装核心组件及扩展
uv pip install fustor-fusion fustor-view-fs fustor-view-fs-forest fustor-receiver-http
```

### 2.2 采集端 (Source Agents)
```bash
# 常规 Agent 安装
uv pip install fustor-agent fustor-source-fs fustor-sender-http
```

---

## 3. 配置文件

### 3.1 采集端配置 (Agent)

**注意**: 从 v0.9.0 开始，**必须**在配置文件中显式设置 `agent_id`。

#### Agent 1 (Node A)
`~/.fustor/agent-config/default.yaml`:
```yaml
agent_id: "agent-shard-01"  # <--- 必须配置

sources:
  fs-source:
    driver: fs
    uri: "/mnt/data/shard-01"

senders:
  fusion-main:
    driver: fusion
    uri: "http://<FUSION_IP>:18888"
    credential: { key: "key-shard-01" }

pipes:
  sync-pipe:
    source: fs-source
    sender: fusion-main
```

#### Agent 2 (Node B)
`~/.fustor/agent-config/default.yaml`:
```yaml
agent_id: "agent-shard-02"  # <--- 必须配置

sources:
  fs-source:
    driver: fs
    uri: "/mnt/data/shard-02"

senders:
  fusion-main:
    driver: fusion
    uri: "http://<FUSION_IP>:18888"
    credential: { key: "key-shard-02" }

pipes:
  sync-pipe:
    source: fs-source
    sender: fusion-main
```

---

### 3.2 服务端配置 (Fusion)

在 Fusion 端，您只需要定义**一个**聚合视图 (`forest-fs` driver)，并将所有 Pipes 指向该视图。

**注意**: Forest View 会根据 `pipe_id` 自动在内部创建和管理子树。

`~/.fustor/fusion-config/default.yaml`:
```yaml
receivers:
  http-receiver:
    driver: http
    port: 18888
    api_keys:
      - key: "key-shard-01"
        pipe_id: "pipe-shard-01"  # <--- Pipe ID 将作为子树的唯一标识
      - key: "key-shard-02"
        pipe_id: "pipe-shard-02"

views:
  # 统一的森林视图
  global-view:
    driver: forest-fs   # <--- 使用森林驱动
    api_keys: ["public-read-key"]
    driver_params:
      hot_file_threshold: 60.0
      max_tree_items: 10000000

pipes:
  # 所有 Pipe 指向同一个森林 View
  pipe-shard-01:
    receiver: http-receiver
    views: [global-view]  # <--- 指向森林
  
  pipe-shard-02:
    receiver: http-receiver
    views: [global-view]  # <--- 指向森林
```

---

## 4. Docker Compose 部署示例

```yaml
services:
  fusion:
    image: python:3.11-slim
    command: fustor-fusion start
    volumes:
      - ./config/fusion:/root/.fustor/fusion-config
    ports:
      - "8101:8101" # API Port
      - "18888:18888" # Receiver Port

  agent-1:
    image: python:3.11-slim
    command: fustor-agent start
    volumes:
      - ./config/agent-1:/root/.fustor/agent-config
      - /mnt/data/shard-01:/data
    environment:
      # 注意：不再支持 FUSTOR_AGENT_ID 环境变量
      # 必须在 config/agent-1/default.yaml 中配置 agent_id
      PYTHONUNBUFFERED: 1

  agent-2:
    image: python:3.11-slim
    command: fustor-agent start
    volumes:
      - ./config/agent-2:/root/.fustor/agent-config
      - /mnt/data/shard-02:/data
```

---

## 5. 验证与使用

启动后，您可以通过 Fusion 的 API 访问聚合视图。

### 5.1 查询聚合目录树
```bash
curl -H "X-API-Key: public-read-key" \
     "http://localhost:8101/api/v1/views/global-view/tree?path=/"
```

**响应示例**:
```json
{
  "path": "/",
  "members": {
    "pipe-shard-01": {  // <--- 以 Pipe ID 为键
      "status": "ok",
      "data": { ... }
    },
    "pipe-shard-02": {
      "status": "ok",
      "data": { ... }
    }
  }
}
```

### 5.2 智能路由查询 (Best View)
如果您只关心“最新”或“最大”的分片数据，可以使用 `?best=<STRATEGY>` 参数，Fusion 会自动根据策略选择一个最佳视图返回，从而减少数据传输量。

**支持的策略**:
*   `latest_mtime`: 选择最后修改时间最新的分片（适用于热数据查询）。
*   `file_count`: 选择包含文件数最多的分片。
*   `total_size`: 选择总大小最大的分片。

**示例**:
```bash
# 获取 "最热" 的分片数据
curl -H "X-API-Key: public-read-key" \
     "http://localhost:8101/api/v1/views/global-view/tree?path=%2F&best=latest_mtime"
```
响应结构与 5.1 相同，但 `members` 中仅包含胜出的那个视图的数据，并附带 `best_view_selected` 字段说明选择原因。

### 5.3 数据来源识别 (Data Lineage)
在返回的目录树信息中，每个文件/目录节点都包含以下元数据字段，用于精确识别数据来源：
*   **last_agent_id**: 最后更新该文件的 Agent ID (即配置文件中设置的 `agent_id`)。
*   **source_uri**: 该文件在源 Agent 上的完整物理路径。

**示例响应片段**:
```json
{
  "name": "example.txt",
  "path": "/example.txt",             // <--- 视图中的逻辑路径
  "last_agent_id": "agent-shard-01",  // <--- 来源 Agent
  "source_uri": "/mnt/data/shard-01/example.txt", // <--- 物理源路径
  ...
}
```
通过这两个字段，即便是在聚合视图中，客户端也能清晰地知道每个文件具体来自于哪个节点的哪个路径。

---

## 6. 动态扩容 (Dynamic Scaling)

本节介绍如何在 **不停止服务** 的情况下，向现有 Agent 添加新的 NFS 挂载源，并使其出现在 Forest View 聚合视图中。

由于 Fusion 的 `API Key` 与 `Pipe` 是 1:1 绑定的，新增 Source 需要同时更新 Fusion 和 Agent 的配置。

### 步骤 1: 修改 Fusion 配置 (fusion.yaml)

在 Fusion 侧做好接收准备：
1.  **定义新 Pipe**: 路由数据到现有的 Forest View。
2.  **分配新 Key**: 在 Receiver 中为新 Pipe 分配专用 Key。

```yaml
views:
  # ... 原有 views ...
  # 无需修改 Views 配置! Forest View 会自动接纳新 Pipe 的数据。

pipes:
  # ... 原有 pipes ...
  pipe-new-nfs:           # [新增] 1. 定义新 Pipe
    receiver: http-main
    views: [global-view]  # <--- 指向现有的森林 View

receivers:
  http-main:
    driver: http
    api_keys:
      # ... 原有 keys ...
      - key: "new-nfs-key"      # [新增] 2. 分配新 Key
        pipe_id: "pipe-new-nfs" #       映射到新 Pipe
```

### 步骤 2: 热重载 Fusion

使用 CLI 命令让 Fusion 加载新配置 (View 和 Pipe 支持热添加)：

```bash
# 安全重载配置 (不会停止服务)
fustor-fusion reload
```

### 步骤 3: 修改 Agent 配置 (agent.yaml)

在 Agent 侧添加采集任务。注意我们需要定义一个 **新 Sender** 来使用上面分配的新 Key。

```yaml
sources:
  # ... 原有 sources ...
  source-new-nfs:         # [新增] 1. 定义新 Source
    driver: fs
    uri: "/mnt/new-nfs-path"

senders:
  # ... 原有 sender ...
  sender-for-new-nfs:     # [新增] 2. 定义新 Sender (为了用新 Key)
    driver: fusion
    uri: "http://fusion-ip:18101"
    credential:
      key: "new-nfs-key"  # <--- 对应 Fusion 侧的新 Key

pipes:
  # ... 原有 pipes ...
  pipe-new-nfs:           # [新增] 3. 定义新 Pipe
    source: source-new-nfs
    sender: sender-for-new-nfs
```

### 步骤 4: 热重载 Agent

让 Agent 启动新的采集管道：

```bash
# 安全重载配置 (不会停止服务)
fustor-agent reload
```

完成上述步骤后，新挂载点的数据将自动同步，并可通过 Forest View API 查询到（作为新的 tree key）。

---

## 7. 常见问题

**Q: Agent 启动报错 "Agent ID is not configured"?**
A: 请检查 Agent 的 YAML 配置文件中是否包含 `agent_id: "..."` 字段。这是 v0.9.0 引入的强制要求。

**Q: 聚合视图中成员 key 是什么?**
A: Forest View 使用 `pipe_id` 作为内部子树的 key。在查询 API 返回的 `members` 字典中，key 即为对应的 `pipe_id`。
