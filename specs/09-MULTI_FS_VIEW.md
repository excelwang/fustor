# Forest View 设计 (Multi-Source Aggregation)

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

### 1.3 来源识别

在多 Agent 汇聚到同一 View (N:1) 的场景下，Fusion 必须能够区分每个文件/事件的具体来源 Agent。

- **标识机制**: 每个 Agent 必须拥有全局唯一的 `agent_id`。
- **自动检测**: Agent 默认会根据出站流量自动检测本地 IP 作为 `agent_id`。
- **目的**: 即使多个 Agent 向同一个 View 推送数据（共享 View ID），系统也能通过 `agent_id` 精确区分数据来源，支持血缘追踪。

---

## 2. 架构决策

### 2.1 核心原则: Forest Pattern (单一视图，内部多树)

采用 **Forest View** 架构 (`view-fs-forest`)，对外表现为单一 View，对内管理多棵 FS Tree。

- **Symmetry**: 保持 Source ↔ View 的逻辑对等性，Forest View 内部为每组 Source (即每个 FusionPipe) 维护一个独立的子树。
- **Routing**: Forest View 接收所有 FusionPipe 的数据，根据 `fusion_pipe_id` 将事件路由到内部对应的子树。
- **Reuse**: 内部子树直接复用 `FSViewDriver` 的逻辑（Arbitration, Audit, Consistency）。

```
AgentPipe-A ─(fusion_pipe_id=A)─┐                    ┌─ Tree A (FSViewDriver)
                               │                    │
AgentPipe-B ─(fusion_pipe_id=B)─┼─► ForestView ────► ├─ Tree B (FSViewDriver)
                               │   (Router)         │
AgentPipe-C ─(fusion_pipe_id=C)─┘                    └─ Tree C (FSViewDriver)
```

### 2.2 为什么不用其他方案

| 方案 | 否决原因 |
|:---|:---|
| 多 View + 独立聚合 API | 配置繁琐（N+1个View），破坏 AgentPipe-FusionPipe 对称性 |
| 独立 View Driver 组合 | 旧方案 (`view-multi-fs`)，导致无法直接 ingest 数据，不仅破坏对称性还导致架构耦合 |

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
        fusion_pipe_id: "pipe-nfs-a"
      - key: "agent-nfs-b-key"
        fusion_pipe_id: "pipe-nfs-b"
      - key: "agent-nfs-c-key"
        fusion_pipe_id: "pipe-nfs-c"

views:
views:
  # 唯一的森林视图
  shared-storage:
    driver: forest-fs
    api_keys: ["query-key-shared"]
    driver_params:
      hot_file_threshold: 60.0    # 应用于所有子树
      max_tree_items: 10000000

pipes:
  pipe-nfs-a:
    receiver: http-shared
    views: [shared-storage]  # 指向森林
  pipe-nfs-b:
    receiver: http-shared
    views: [shared-storage]  # 指向森林
  pipe-nfs-c:
    receiver: http-shared
    views: [shared-storage]  # 指向森林
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

`view-fs-forest` 通过 `fustor.view_api` entry point 注册以下端点（前缀由 view 名称决定，如 `/shared-storage/`）。

**主要变化**: 原 `members` 列表中的 `view_id` 变为 `fusion_pipe_id`。

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
      "fusion_pipe_id": "pipe-nfs-a",  // <--- FusionPipe ID
      "status": "ok",
      "file_count": 5234,
      "dir_count": 120,
      "total_size": 1073741824,
      "latest_mtime": 1706000500
    },
    {
      "fusion_pipe_id": "pipe-nfs-b",  // <--- FusionPipe ID
      "status": "ok",
      "file_count": 5100,
      "dir_count": 118,
      "total_size": 1048576000,
      "latest_mtime": 1706000300
    }
  ],
  "best": {
    "fusion_pipe_id": "pipe-nfs-a",
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
    "pipe-nfs-a": {
      "status": "ok",
      "data": { "name": "logs", "content_type": "directory", "children": [...] }
    },
    "pipe-nfs-b": {
      "status": "error",
      "error": "Path not found"
    }
  }
}
```

---

## 5. 实现要点

### 5.1 `view-fs-forest` Driver 行为

- **Event Routing**: `process_event(event)` 读取 `event.metadata["fusion_pipe_id"]`，将事件路由给对应的内部 `FSViewDriver`。
- **Dynamic Tree**: 遇到未知的 `fusion_pipe_id` 自动创建新子树。
- **Query Aggregation**: 所有查询方法（stats, tree）遍历所有内部子树并聚合结果。
### 5.2 Session Lifecycle & Leader Election

Forest View 实际上是一个 View 容器，因此 Session 管理权必须下放：

- **Delegation**: `FusionPipe` -> `PipeSessionBridge` -> `ViewHandler.resolve_session_role()` -> `ViewDriver.resolve_session_role()`
- **Scoped Election**: `ForestFSViewDriver` 实现 `resolve_session_role`，从 `**kwargs` 中提取 `fusion_pipe_id`，构建 scoped election key (`{view_id}:{fusion_pipe_id}`)，确保每棵子树有独立的 Leader。
- **Lifecycle 通知**: `on_session_start(**kwargs)` / `on_session_close(**kwargs)` 由 `FusionPipe` 广播，ForestView 从 kwargs 中提取 `session_id` 和 `fusion_pipe_id` 进行内部路由，标准 View 忽略这些额外参数。
- **Snapshot 完成**: `on_snapshot_complete(session_id, **kwargs)` 由 `FusionPipe` 在 snapshot 结束时通知所有 handler。ForestView 在此回调中标记 scoped key（`{view_id}:{fusion_pipe_id}`），标准 View 无需实现此方法。
- **Cleanup**: `PipeSessionBridge` 负责根据 cache 清理所有相关的 election keys，无需感知具体策略。

### 5.3 标准层解耦契约 (`**kwargs` 穿透模式)

Forest View 的路由需求（`session_id`, `fusion_pipe_id`）不得侵入标准层。标准层只传递上下文，不解读它。

#### 5.3.1 核心接口签名

`core/drivers.py` 中的 `ViewDriver` 基类定义宽松的 `**kwargs` 接口：

```python
# 标准层只传递上下文，不解读它
class ViewDriver:
    async def on_session_start(self, **kwargs): pass
    async def on_session_close(self, **kwargs): pass
    async def resolve_session_role(self, session_id: str, **kwargs) -> Dict[str, Any]: ...
    async def on_snapshot_complete(self, session_id: str, **kwargs) -> None: pass
```

- **标准 View** (`FSViewDriver`): 签名匹配 `**kwargs`，但函数体不使用这些参数。
- **Forest View** (`ForestFSViewDriver`): 从 `kwargs` 中提取 `session_id`, `pipe_id` 进行内部路由。

#### 5.3.2 Adapter 层穿透规则

`ViewDriverAdapter` 和 `ViewManagerAdapter` 必须透传 `**kwargs`，**不得将 kwargs 拆解为 positional 参数**再传递：

```python
# ✅ 正确: 透传 **kwargs
async def resolve_session_role(self, session_id: str, **kwargs):
    return await self._driver.resolve_session_role(session_id, **kwargs)

# ❌ 错误: 拆解为 positional 参数，会导致接收端 kwargs.get("fusion_pipe_id") 取不到值
async def resolve_session_role(self, session_id: str, fusion_pipe_id=None):
    return await self._driver.resolve_session_role(session_id, fusion_pipe_id)
```

#### 5.3.3 Handler 查找规则

`FusionPipe` 提供 `find_handler_for_view(view_id)` 方法，按 view_id 语义查找 handler，**不依赖 handler_id 命名规则**（如 `view-manager-{view_id}` 前缀）：

```python
# ✅ 正确: 按语义查找
handler = self._pipe.find_handler_for_view(view_id)

# ❌ 错误: 硬编码命名前缀
handler = self._pipe.get_view_handler(f"view-manager-{view_id}")
```

#### 5.3.4 禁止事项 (Anti-patterns)

| 禁止 | 原因 | 正确做法 |
|:---|:---|:---|
| 在 `FusionPipe` 中写 scoped snapshot key | 将 Forest 细节泄漏到标准层 | 通知 `handler.on_snapshot_complete()`，由 Forest 自行处理 |
| 在 `FusionPipe._dispatch_to_handlers` 中注入 `pipe_id` 到 event metadata | Agent 端已在 metadata 中设置 pipe_id | 不需要 Fusion 端重复注入 |
| 在 `SessionBridge` 中猜测 handler_id 前缀 | 将 Adapter 内部命名规则泄漏到 Bridge | 使用 `find_handler_for_view()` |

### 5.4 `get_subtree_stats(path)` 方法

需要在 `FSViewDriver` 上新增一个**只读查询方法**（不影响现有逻辑）：

```python
async def get_subtree_stats(self, path: str) -> Dict[str, Any]:
    """遍历子树返回统计摘要。不修改任何状态。"""
    # 遍历内存树做计数:
    # → file_count, dir_count, total_size, latest_mtime
```

### 5.5 性能保证

| 操作 | 复杂度 | 预期延迟 |
|:---|:---|:---|
| `/stats` (10个成员并发) | O(子树节点数) × 1 (并发) | 10~100ms |
| `/tree` (depth=1) | O(直接子节点数) × 1 (并发) | <10ms |
| `/tree` (全量) | O(子树节点数) × 1 (并发序列化) | 取决于子树大小 |

### 5.6 包结构

```
extensions/
├── view-fs-forest/                  # 新增 (替代 view-multi-fs)
│   └── src/fustor_view_fs_forest/
│       ├── __init__.py
│       ├── driver.py                # ForestFSViewDriver
│       └── api.py                   # /stats, /tree 聚合端点
```

---

## 6. 对架构的影响

### 6.1 不变的部分

- 3 层对称模型 (Source/AgentPipe/Sender ↔ Receiver/FusionPipe/View)
- 6 层分层架构
- 一致性模型 (Tombstone/Suspect/Blind-spot)
- 每个基础 View 的独立性

### 6.2 扩展的部分

- `views` 配置段支持 `driver: forest-fs` 类型
- 包结构新增 `fustor-view-fs-forest`，删除 `fustor-view-multi-fs`
- 引入 Forest Pattern (1 View Driver : N Internal Trees)

### 6.3 术语更新

| Agent 概念 | Fusion 对应 | 示例 |
|:---|:---|:---|
| Source (fs) | View (fs) | 单 NFS 视图 |
| Source (oss) | View (oss) | 对象存储视图 |
| — | View (forest-fs) | 多 NFS 聚合视图 |
| AgentPipe | FusionPipe | 数据管道链路 |
