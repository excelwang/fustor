# Multi-FS View 设计

> 版本: 1.0.0  
> 日期: 2026-02-13  
> 状态: 设计中  
> 前置依赖: 01-ARCHITECTURE, 02-CONSISTENCY_DESIGN

## 1. 需求背景

### 1.1 场景

多台计算服务器通过各自的 NFS 挂载访问同一共享存储（或多个独立存储）。用户需要：

1. **跨源比较**: 一次查询看到同一路径在多个 NFS 上的文件状态
2. **来源溯源**: 知道每份数据来自哪个 NFS
3. **最优选择**: 找到数据最完整的 NFS（例如：先在 NFS-A 上写了一半，后迁移到 NFS-B）

### 1.2 规模约束

| 参数 | 预期值 |
|:---|:---|
| NFS 服务器数量 | 10~20 |
| 每个 NFS 文件数量 | ~1000万 |
| 每个 NFS 写入速率 | ~1000 events/s |
| 总内存占用（所有 View） | ~50GB |

### 1.3 关键约束: 来源识别

在多 Agent 汇聚到同一 View (N:1) 的场景下，Fusion 必须能够区分每个文件/事件的具体来源 Agent。

- **强制要求**: 每个 Agent **必须**在配置文件中显式设置唯一的 `agent_id`。
- **机制**: Fusion 会话建立时会传输 `agent_id`，并将其作为元数据 (`last_agent_id`) 附加到文件节点上。
- **目的**: 即使多个 Agent 向同一个 View 推送数据（共享 View ID），系统也能通过 `agent_id` 精确区分数据来源，防止数据混淆并支持血缘追踪。

---

## 2. 架构决策

### 2.1 核心原则: View Driver 组合

Multi-FS 聚合功能作为一个**新的 View Driver** (`view-multi-fs`) 实现，不引入新的架构层级。

这一决策基于对称性分析：

```
Agent:   Source(data) → Pipe(orchestration) → Sender(transport)
Fusion:  Receiver(transport) → Pipe(orchestration) → View(data)
```

Agent 侧有不同的 Source Driver (`source-fs`, `source-oss` ...)，Fusion 侧对称地有不同的 View Driver (`view-fs`, `view-multi-fs` ...)。`view-multi-fs` 是一个**组合型 View Driver**——它不自己消费事件，而是聚合其他 `view-fs` 实例的查询结果。

```
                         ┌── view-fs (nfs-a) ◄── Pipe-A ◄── Receiver
view-multi-fs ──读取──►  │
  (聚合查询)              └── view-fs (nfs-b) ◄── Pipe-B ◄── Receiver
```

### 2.2 为什么不用其他方案

| 方案 | 否决原因 |
|:---|:---|
| 多 View + 独立聚合 API (`multi_views` 配置段) | 引入新的架构概念，破坏现有 `views` 配置模型 |
| 单 View 内多源仲裁 | 需重构一致性模型（Tombstone、Suspect 等多源化），风险极高 |
| Pipeline/Materializer 中间层 | 打破 Agent-Fusion 3 层对称性，过度工程化 |

---

## 3. 配置设计

### 3.1 Fusion 配置

```yaml
receivers:
  http-shared:
    driver: http
    bind_host: "0.0.0.0"
    port: 18881
    api_keys:
      - key: "agent-nfs-a-key"
        pipe_id: "pipe-nfs-a"
      - key: "agent-nfs-b-key"
        pipe_id: "pipe-nfs-b"
      - key: "agent-nfs-c-key"
        pipe_id: "pipe-nfs-c"

views:
  # 基础 View: 每个 NFS 一个
  nfs-a:
    driver: fs
    driver_params:
      hot_file_threshold: 60.0
      max_tree_items: 10000000
  nfs-b:
    driver: fs
    driver_params:
      hot_file_threshold: 60.0
      max_tree_items: 10000000
  nfs-c:
    driver: fs
    driver_params:
      hot_file_threshold: 60.0
      max_tree_items: 10000000

  # 聚合 View: 组合多个基础 View
  shared-storage:
    driver: multi-fs
    api_keys: ["query-key-shared"]
    driver_params:
      members: [nfs-a, nfs-b, nfs-c]

pipes:
  pipe-nfs-a:
    receiver: http-shared
    views: [nfs-a]
  pipe-nfs-b:
    receiver: http-shared
    views: [nfs-b]
  pipe-nfs-c:
    receiver: http-shared
    views: [nfs-c]
  # 注意: shared-storage 不绑定任何 Pipe (不直接消费事件)
```

### 3.2 Agent 配置 (每台 NFS 服务器)

每个 Agent 的配置结构不变，但即便是不同服务器，也**必须配置唯一的 `agent_id`**：

```yaml
# Agent-A (部署在 NFS-A 服务器上)
agent_id: "agent-nfs-a"  # <--- 必须配置且唯一

sources:
  nfs-a-src:
    driver: fs
    uri: "/mnt/shared-storage"

senders:
  fusion-1:
    driver: fusion
    uri: "http://fusion-host:18881"
    credential:
      key: "agent-nfs-a-key"

pipes:
  pipe-nfs-a:
    source: nfs-a-src
    sender: fusion-1
```

---

## 4. API 设计

`view-multi-fs` 通过 `fustor.view_api` entry point 注册以下端点（前缀由 view 名称决定，如 `/shared-storage/`）：

### 4.1 `GET /{view_name}/stats`

**轻量统计对比**。返回每个成员 View 在指定路径下的统计摘要。

| 参数 | 类型 | 默认 | 说明 |
|:---|:---|:---|:---|
| `path` | str | `/` | 查询路径 |
| `best` | str | 无 | 自动推荐策略: `file_count` / `total_size` / `latest_mtime` |

**响应**:

```json
{
  "path": "/data/experiment-42",
  "members": [
    {
      "view_id": "nfs-a",
      "status": "ok",
      "file_count": 5234,
      "dir_count": 120,
      "total_size": 1073741824,
      "latest_mtime": 1706000500
    },
    {
      "view_id": "nfs-b",
      "status": "ok",
      "file_count": 5100,
      "dir_count": 118,
      "total_size": 1048576000,
      "latest_mtime": 1706000300
    }
  ],
  "best": {
    "view_id": "nfs-a",
    "reason": "file_count",
    "value": 5234
  }
}
```

### 4.2 `GET /{view_name}/tree`

**详细树对比**。返回每个成员 View 在指定路径下的完整目录树。

| 参数 | 类型 | 默认 | 说明 |
|:---|:---|:---|:---|
| `path` | str | `/` | 查询路径 |
| `recursive` | bool | `true` | 是否递归 |
| `max_depth` | int | 无 | 最大深度（可选） |
| `only_path` | bool | `false` | 仅返回路径结构 |
| `best` | str | 无 | `file_count` / `total_size` / `latest_mtime`。指定时仅返回最优成员的树 |

当指定 `best` 参数时，先内部调用 `stats` 逻辑选出最优成员，只返回该成员的树数据，避免用户需要两次请求。

**响应**:

```json
{
  "path": "/data/logs",
  "members": {
    "nfs-a": {
      "status": "ok",
      "data": { "name": "logs", "content_type": "directory", "children": [...] }
    },
    "nfs-b": {
      "status": "error",
      "error": "Path not found"
    }
  }
}
```

---

## 5. 实现要点

### 5.1 `view-multi-fs` Driver 行为

- **不消费事件**: `process_event()` 直接返回 `True`（无操作）
- **查询代理**: 所有查询方法代理给成员 View Driver
- **并发查询**: 使用 `asyncio.gather()` 并发查询所有成员
- **容错**: 单个成员查询失败不影响其他成员的结果

### 5.2 `get_subtree_stats(path)` 方法

需要在 `FSViewDriver` 上新增一个**只读查询方法**（不影响现有逻辑）：

```python
async def get_subtree_stats(self, path: str) -> Dict[str, Any]:
    """遍历子树返回统计摘要。不修改任何状态。"""
    # 遍历内存树做计数:
    # → file_count, dir_count, total_size, latest_mtime
```

### 5.3 性能保证

| 操作 | 复杂度 | 预期延迟 |
|:---|:---|:---|
| `/stats` (10个成员并发) | O(子树节点数) × 1 (并发) | 10~100ms |
| `/tree` (depth=1) | O(直接子节点数) × 1 (并发) | <10ms |
| `/tree` (全量) | O(子树节点数) × 1 (并发序列化) | 取决于子树大小 |

### 5.4 包结构

```
extensions/
├── view-multi-fs/                   # 新增
│   └── src/fustor_view_multi_fs/
│       ├── __init__.py
│       ├── driver.py                # MultiFS ViewDriver
│       └── api.py                   # /stats, /tree 端点
```

---

## 6. 对架构的影响

### 6.1 不变的部分

- 3 层对称模型 (Source/Pipe/Sender ↔ Receiver/Pipe/View)
- 6 层分层架构
- 一致性模型 (Tombstone/Suspect/Blind-spot)
- 每个基础 View 的独立性

### 6.2 扩展的部分

- `views` 配置段支持 `driver: multi-fs` 类型
- 包结构新增 `fustor-view-multi-fs`
- View Driver 的组合模式（一个 View Driver 可引用其他 View Driver）

### 6.3 术语更新

| Agent 概念 | Fusion 对应 | 示例 |
|:---|:---|:---|
| Source (fs) | View (fs) | 单 NFS 视图 |
| Source (oss) | View (oss) | 对象存储视图 |
| — | View (multi-fs) | 多 NFS 聚合视图 |
