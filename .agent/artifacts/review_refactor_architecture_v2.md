# Fustor 架构 V2 重构评审报告

**评审日期:** 2026-02-02  
**评审分支:** `refactor/architecture-v2`  
**对比分支:** `master`  
**评审角色:** 高级开发者对初级开发者进行代码审查

---

## 一、总体评估

### 1.1 重构目标回顾

根据 `docs/top-level-specifications/01-ARCHITECTURE.md` 和 `04-Detailed_Decisions.md`，本次重构的核心目标是：

1. **解耦与对称化**: 将 Agent/Fusion 架构对称化，通过 Pipeline 统一编排 Source->Sender 和 Receiver->View 流程
2. **分层清晰**: Core 层提供抽象，Driver 层提供具体实现，Runtime 层负责编排
3. **术语统一**: `pusher` 重命名为 `sender`, `datastore_id` 重命名为 `view_id`
4. **可扩展性**: 通过 Schema 包定义数据契约，支持第三方扩展

### 1.2 总体评分

| 评估维度 | 评分 (1-10) | 说明 |
|---------|-------------|------|
| **架构设计** | 9/10 | 分层清晰，抽象合理，符合设计文档 |
| **代码质量** | 7/10 | 整体良好，但存在一些需要改进的地方 |
| **测试覆盖** | 8/10 | Agent 和 Fusion 单元测试充分，集成测试完备 |
| **文档完整性** | 8/10 | 设计文档详尽，但部分代码注释可以更详细 |
| **向后兼容性** | 7/10 | 存在兼容层，但有遗留代码需要清理 |

---

## 二、值得肯定的实现

### 2.1 优秀的 Pipeline 抽象设计

`fustor_core.pipeline.Pipeline` 基类设计得非常好：

```python
# packages/core/src/fustor_core/pipeline/pipeline.py
class PipelineState(IntFlag):
    """Pipeline state flags using bitmask for composite states."""
    STOPPED = 0
    INITIALIZING = auto()
    RUNNING = auto()
    SNAPSHOT_PHASE = auto()
    MESSAGE_PHASE = auto()
    AUDIT_PHASE = auto()
    # ...
```

**优点:**
- 使用 `IntFlag` 支持组合状态，例如 `RUNNING | SNAPSHOT_PHASE`
- 状态机设计清晰，便于追踪 Pipeline 生命周期
- 提供了良好的日志记录和状态转换追踪

### 2.2 Handler 适配器模式实现

`SenderHandlerAdapter` 和 `SourceHandlerAdapter` 采用适配器模式，优雅地桥接了旧的 Driver 接口和新的 Handler 接口：

```python
# agent/src/fustor_agent/runtime/sender_handler_adapter.py
class SenderHandlerAdapter(SenderHandler):
    """Adapts a Sender transport to the SenderHandler interface."""
    
    async def send_batch(self, session_id, events, batch_context):
        # Map phase to source_type
        source_type_map = {
            "snapshot": "snapshot",
            "realtime": "message",
            "audit": "audit",
        }
        # ... 委托给底层 Sender
```

**优点:**
- 渐进式迁移，不破坏现有功能
- 保持接口一致性的同时支持多种底层实现

### 2.3 Session Bridge 设计

`PipelineSessionBridge` 实现了新旧会话管理的平滑过渡：

```python
# fusion/src/fustor_fusion/runtime/session_bridge.py
class PipelineSessionBridge:
    """Bridge that synchronizes sessions between FusionPipeline and SessionManager."""
```

这允许在 Pipeline 模式下复用已验证的 `SessionManager` 逻辑。

### 2.4 Schema 包独立

`fustor-schema-fs` 包正确地只包含数据模型定义，符合设计决策 1.1：

```python
# packages/schema-fs/src/fustor_schema_fs/__init__.py
SCHEMA_NAME = "fs"
SCHEMA_VERSION = "1.0"
REQUIRED_FIELDS = ["path", "file_name", "size", "modified_time", "is_directory"]
```

---

## 三、需要改进的问题

### 3.1 ⚠️ 【严重】遗留代码未清理

#### 问题描述

在 `fusion/src/fustor_fusion/main.py` 中，仍然存在对旧模块的引用：

```python
# Line 17-20
from .queue_integration import queue_based_ingestor, get_events_from_queue
from .in_memory_queue import memory_event_queue
from .processing_manager import processing_manager
```

这些模块属于 Legacy 模式，应该在 Pipeline 模式完成后被移除或标记为 deprecated。

#### 建议修复

1. 在这些 import 上添加 `# LEGACY: TODO remove after full V2 migration` 注释
2. 创建 issue 跟踪清理任务
3. 如果 Legacy 模式仍需支持，考虑将其移入 `legacy/` 子目录

### 3.2 ⚠️ 【中等】AgentPipeline 中的硬编码时间常量

#### 问题描述

`AgentPipeline` 中的时间常量是类级别的硬编码：

```python
# agent/src/fustor_agent/runtime/agent_pipeline.py:41-50
class AgentPipeline(Pipeline):
    CONTROL_LOOP_INTERVAL = 1.0
    FOLLOWER_STANDBY_INTERVAL = 1.0
    ERROR_RETRY_INTERVAL = 5.0
    MAX_CONSECUTIVE_ERRORS = 5
    # ...
```

#### 建议修复

这些值应该从配置中读取，以便在不同环境下灵活调整：

```python
def __init__(self, ...):
    # ...
    self.control_loop_interval = config.get("control_loop_interval", 1.0)
    self.follower_standby_interval = config.get("follower_standby_interval", 1.0)
    self.error_retry_interval = config.get("error_retry_interval", 5.0)
```

### 3.3 ⚠️ 【中等】FusionPipeline 中的 datastore_id 兼容别名

#### 问题描述

`FusionPipeline` 同时维护了 `view_id` 和 `datastore_id` 两个属性：

```python
# fusion/src/fustor_fusion/runtime/fusion_pipeline.py:90-91
self.view_id = str(config.get("view_id", config.get("datastore_id", pipeline_id)))
self.datastore_id = self.view_id  # Alias for backward compatibility
```

这导致代码中可能同时使用两种命名，增加了认知负担。

#### 建议修复

1. 添加 `@property` 装饰器并发出 deprecation 警告：

```python
@property
def datastore_id(self) -> str:
    import warnings
    warnings.warn(
        "datastore_id is deprecated, use view_id instead",
        DeprecationWarning,
        stacklevel=2
    )
    return self.view_id
```

2. 在所有新代码中只使用 `view_id`

### 3.4 ⚠️ 【轻微】phases.py 中的错误处理链不完整

#### 问题描述

在 `phases.py` 的 `run_snapshot_sync` 中，当发送批次失败时没有处理逻辑：

```python
# agent/src/fustor_agent/runtime/pipeline/phases.py:33-39
success, response = await pipeline.sender_handler.send_batch(...)
if success:
    pipeline._update_role_from_response(response)
    pipeline.statistics["events_pushed"] += len(batch)
batch = []  # <-- 即使失败也清空了 batch！
```

如果发送失败，batch 被清空，这些事件就丢失了。

#### 建议修复

```python
success, response = await pipeline.sender_handler.send_batch(
    pipeline.session_id, batch, {"phase": "snapshot"}
)
if success:
    pipeline._update_role_from_response(response)
    pipeline.statistics["events_pushed"] += len(batch)
    batch = []  # 只有成功才清空
else:
    logger.warning(f"Failed to send snapshot batch, will retry...")
    raise RuntimeError("Snapshot batch send failed")  # 或实现重试逻辑
```

### 3.5 ⚠️ 【轻微】HTTPReceiver 中的 API Key 验证位于内存中

#### 问题描述

```python
# packages/receiver-http/src/fustor_receiver_http/__init__.py
class HTTPReceiver:
    def register_api_key(self, api_key: str, pipeline_id: str):
        """Register an API key for a pipeline."""
        self._api_keys[api_key] = pipeline_id
```

API Key 存储在内存字典中，没有持久化或服务重启恢复机制。

#### 建议修复

1. 从配置文件加载 API Key（当前设计已支持通过 `receivers-config.yaml`）
2. 考虑添加 API Key 轮换和过期机制
3. 在文档中明确说明 API Key 管理策略

### 3.6 ⚠️ 【轻微】`leader_session` 属性返回 None

#### 问题描述

`FusionPipeline.leader_session` 属性总是返回 `None`：

```python
# fusion/src/fustor_fusion/runtime/fusion_pipeline.py:448-458
@property
def leader_session(self) -> Optional[str]:
    # We don't have a sync way to get the leader from the async manager.
    return None
```

这会导致任何依赖此属性的代码无法正常工作。

#### 建议修复

移除此同步属性，或者维护一个缓存值：

```python
# 选项 1: 移除属性，只保留异步方法
# 选项 2: 在 on_session_created 时缓存
def __init__(self, ...):
    self._cached_leader_session: Optional[str] = None

async def on_session_created(self, session_id: str, is_leader: bool = False, **kwargs):
    if is_leader:
        self._cached_leader_session = session_id
    # ...

@property
def leader_session(self) -> Optional[str]:
    """Cached leader session. May be stale. Use get_dto() for accurate value."""
    return self._cached_leader_session
```

---

## 四、功能完整性检查

### 4.1 一致性特性保留情况

根据设计决策 6.x，以下功能需保留在 V2 中：

| 功能 | 状态 | 实现位置 |
|------|------|----------|
| Sentinel 验证 | ✅ 已实现 | `phases.run_sentinel_check()` |
| Suspect/Blind-spot/Tombstone Lists | ✅ 已实现 | `view-fs` 驱动 |
| 热点文件检测 | ⚠️ 未验证 | 需检查 `view-fs` |
| Audit 跳过优化 | ⚠️ 未验证 | 需检查 `source-fs` |
| 断点续传 | ✅ Pipeline 级别 | `AgentPipeline._run_control_loop` |

### 4.2 API 路径迁移

| 旧路径 | 新路径 | 状态 |
|--------|--------|------|
| `/api/v1/ingest/{datastore_id}` | `/api/v1/pipe/{view_id}` | ✅ 已迁移 |
| Session 创建 | `/api/v1/pipe/{view_id}/sessions` | ✅ 已迁移 |
| Heartbeat | `/api/v1/pipe/{view_id}/sessions/{session_id}/heartbeat` | ✅ 已迁移 |

---

## 五、TODO 清单

### 5.1 高优先级

- [ ] **清理 Legacy 代码**: 标记或移除 `fusion/src/fustor_fusion/` 中的 `queue_integration`、`in_memory_queue`、`processing_manager`
- [ ] **修复 phases.py 批次发送失败处理**: 确保失败时不丢失数据
- [ ] **datastore_id 废弃警告**: 在 `FusionPipeline.datastore_id` 添加 DeprecationWarning

### 5.2 中优先级

- [ ] **配置化时间常量**: 将 `AgentPipeline` 中的硬编码时间常量移至配置
- [ ] **修复 leader_session 属性**: 实现缓存机制或移除此属性
- [ ] **增加 E2E 测试**: 验证完整的 Agent -> Fusion Pipeline 流程

### 5.3 低优先级

- [ ] **代码注释增强**: 为 `phases.py` 中的各阶段添加更详细的 docstring
- [ ] **API Key 管理增强**: 考虑添加 Key 轮换和过期机制

---

## 六、测试运行结果

```
Agent tests: 76 passed
Fusion tests: 94 passed
```

所有现有测试通过，表明重构没有引入回归问题。

---

## 七、总结

本次 V2 架构重构整体上是成功的，代码结构更加清晰，分层更加合理。主要的改进空间在于：

1. **遗留代码清理**: 需要更彻底地移除或隔离 Legacy 模式代码
2. **错误处理**: 部分关键路径的错误处理需要加强
3. **术语统一**: `datastore_id` -> `view_id` 的迁移需要更彻底

建议在合并前完成高优先级的 TODO 项，并在后续迭代中逐步处理中低优先级项。

---

*评审人: AI Code Reviewer*  
*日期: 2026-02-02*

---

## 附录 A：关键文件清单

### A.1 Core 包 (`packages/core/`)

| 文件 | 用途 | 状态 |
|------|------|------|
| `pipeline/pipeline.py` | Pipeline 基类和 PipelineState 定义 | ✅ 完成 |
| `pipeline/handler.py` | Handler 抽象（SourceHandler, ViewHandler） | ✅ 完成 |
| `pipeline/sender.py` | SenderHandler 抽象 | ✅ 完成 |
| `transport/sender.py` | Sender 传输层抽象 | ✅ 完成 |
| `transport/receiver.py` | Receiver 传输层抽象 | ✅ 完成 |

### A.2 Agent 运行时 (`agent/src/fustor_agent/runtime/`)

| 文件 | 用途 | 状态 |
|------|------|------|
| `agent_pipeline.py` | AgentPipeline 实现 | ✅ 完成 |
| `sender_handler_adapter.py` | Sender -> SenderHandler 适配器 | ✅ 完成 |
| `source_handler_adapter.py` | Driver -> SourceHandler 适配器 | ✅ 完成 |
| `pipeline/phases.py` | Pipeline 阶段实现（snapshot, message, audit） | ✅ 完成 |
| `pipeline/worker.py` | 异步迭代器包装器 | ✅ 完成 |

### A.3 Fusion 运行时 (`fusion/src/fustor_fusion/runtime/`)

| 文件 | 用途 | 状态 |
|------|------|------|
| `fusion_pipeline.py` | FusionPipeline 实现 | ✅ 完成 |
| `pipeline_manager.py` | Pipeline 生命周期管理 | ✅ 完成 |
| `session_bridge.py` | Pipeline-SessionManager 桥接 | ✅ 完成 |
| `view_handler_adapter.py` | Driver -> ViewHandler 适配器 | ✅ 完成 |

### A.4 Transport 包

| 包 | 用途 | 状态 |
|---|------|------|
| `sender-http` | HTTP Sender 实现 | ✅ 完成 |
| `receiver-http` | HTTP Receiver 实现 | ✅ 完成 |

### A.5 Schema 包

| 包 | 用途 | 状态 |
|---|------|------|
| `schema-fs` | 文件系统 Schema 定义 | ✅ 完成 |

---

## 附录 B：术语映射表

| V1 术语 | V2 术语 | 说明 |
|---------|---------|------|
| `pusher` | `sender` | Agent 端事件发送组件 |
| `datastore_id` | `view_id` | Fusion 端视图标识符 |
| `SyncInstance` | `AgentPipeline` | Agent 端同步实例 |
| `ProcessingTask` | `FusionPipeline` | Fusion 端处理任务 |
| `/api/v1/ingest` | `/api/v1/pipe` | API 路径前缀 |

---

## 附录 C：Legacy 模块清单

以下模块属于 Legacy 模式，建议在完成迁移后移除：

```
fusion/src/fustor_fusion/
├── in_memory_queue.py      # Legacy: 内存队列实现
├── queue_integration.py     # Legacy: 队列适配层
├── processing_manager.py    # Legacy: 处理任务管理
└── datastore_event_manager.py  # Legacy: 事件管理
```

这些模块仍使用 `datastore_id` 术语，共有 **162+** 处引用。

---

## 附录 D：配置文件结构

### D.1 Agent 配置

```yaml
# sources-config.yaml
sources:
  <source_id>:
    driver: fs  # 驱动类型
    path: /data/files
    # ...

# senders-config.yaml  
senders:
  <sender_id>:
    driver: http
    endpoint: http://fusion:8080
    credential:
      key: <api-key>
    # ...

# agent-pipes-config/<sync_id>.yaml
sync_id: <sync_id>
source_id: <source_id>
sender_id: <sender_id>
session_timeout_seconds: 30
# ...
```

### D.2 Fusion 配置

```yaml
# receivers-config.yaml
receivers:
  <receiver_id>:
    driver: http
    bind_host: 0.0.0.0
    port: 8080
    # ...

# fusion-pipes-config/<pipeline_id>.yaml
pipeline_id: <pipeline_id>
receiver_id: <receiver_id>
view_id: <view_id>  # 新术语
# ...

# views-config/<view_id>.yaml
view_id: <view_id>
driver: fs
# ...
```

---

## 附录 E：状态机图

### E.1 AgentPipeline 状态机

```
STOPPED
    │
    ▼ start()
INITIALIZING ──── error ────┐
    │                       │
    ▼ session created       │
RUNNING                     │
    │                       │
    ├───► SNAPSHOT_PHASE    │
    │          │            │
    │          ▼ complete   │
    ├───► MESSAGE_PHASE ◄───┘ (retry)
    │          │
    │          ├──► AUDIT_PHASE (periodic)
    │          │
    │          ▼ role=follower
    └───► PAUSED (follower standby)
              │
              ▼ role=leader
         [back to SNAPSHOT_PHASE]
```

### E.2 FusionPipeline 状态机

```
STOPPED
    │
    ▼ start()
RUNNING ─────────────────────┐
    │                        │
    ├─► process_events()     │
    │                        │
    ├─► on_session_created() │
    │                        │
    ├─► on_session_closed()  │
    │                        │
    ▼ stop()                 │
PAUSED ──────────────────────┘
    │
    ▼ cleanup
STOPPED
```
