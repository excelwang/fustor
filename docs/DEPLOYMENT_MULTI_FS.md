# Fustor 多视图聚合部署指南 (Multi-FS View)

本文档指导如何部署 Fustor 并启用 **Multi-FS View** 特性。该特性允许 Fusion 将分布在不同 Agent 上的多个文件系统视图聚合成一个统一的逻辑视图，对外提供单一访问入口。

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

除了基础组件外，Fusion 端需要安装 `fustor-view-multi-fs` 扩展。

### 2.1 服务端 (Fusion Node)
```bash
# 安装核心组件及扩展
uv pip install fustor-fusion fustor-view-fs fustor-view-multi-fs fustor-receiver-http
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

在 Fusion 端，您需要先定义与每个 Agent 对应的普通视图 (`fs` driver)，然后定义一个聚合视图 (`multi-fs` driver)。

`~/.fustor/fusion-config/default.yaml`:
```yaml
receivers:
  http-receiver:
    driver: http
    port: 18888
    api_keys:
      - key: "key-shard-01"
        pipe_id: "pipe-shard-01"
      - key: "key-shard-02"
        pipe_id: "pipe-shard-02"

views:
  # 1. 基础视图 (对应各自的 Agent)
  view-shard-01:
    driver: fs
    api_keys: ["admin-key"]
  
  view-shard-02:
    driver: fs
    api_keys: ["admin-key"]

  # 2. 聚合视图 (Multi-FS)
  global-view:
    driver: multi-fs  # <--- 使用聚合驱动
    api_keys: ["public-read-key"]
    driver_params:
      members:        # <--- 指定成员视图 ID
        - view-shard-01
        - view-shard-02

pipes:
  # 绑定 Pipe 到对应的基础视图
  pipe-shard-01:
    receiver: http-receiver
    views: [view-shard-01]
  
  pipe-shard-02:
    receiver: http-receiver
    views: [view-shard-02]
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
    "view-shard-01": {
      "status": "ok",
      "data": { ... }
    },
    "view-shard-02": {
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

## 6. 常见问题

**Q: Agent 启动报错 "Agent ID is not configured"?**
A: 请检查 Agent 的 YAML 配置文件中是否包含 `agent_id: "..."` 字段。这是 v0.9.0 引入的强制要求。

**Q: 聚合视图中某个成员显示 "status": "error"?**
A: 这表示 Fusion 无法连接到对应的基础视图（可能是对应的 Agent 未连接，或视图未就绪）。聚合视图具有容错性，单个成员失败不会导致整个请求失败。
