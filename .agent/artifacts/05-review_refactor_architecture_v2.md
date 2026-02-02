# Fustor 架构 V2 重构评审报告

**评审日期:** 2026-02-02 (更新)  
**评审分支:** `refactor/architecture-v2`  
**对比分支:** `master`  
**评审角色:** 高级开发者对初级开发者进行代码审查

> ✅ **重构任务已完成**
> 
> 初级开发者已按照评审建议完成了所有高优先级任务。Legacy 代码已彻底删除，测试全部通过。

---

## 一、总体评估

### 1.1 重构目标回顾

根据 `.agent/artifacts/01-ARCHITECTURE.md` 和 `.agent/artifacts/04-Detailed_Decisions.md`，本次重构的核心目标是：

1. **解耦与对称化**: 将 Agent/Fusion 架构对称化，通过 Pipeline 统一编排 Source→Sender 和 Receiver→View 流程
2. **分层清晰**: Core 层提供抽象，Driver 层提供具体实现，Runtime 层负责编排
3. **术语统一**: `pusher` 重命名为 `sender`, `datastore_id` 重命名为 `view_id`
4. **可扩展性**: 通过 Schema 包定义数据契约，支持第三方扩展
5. **一致性保障**: 保留 Leader/Follower、Sentinel、Suspect/Blind-spot/Tombstone 等 fs 特有机制

### 1.2 总体评分（更新后）

| 评估维度 | 评分 (1-10) | 说明 |
|---------|-------------|------|
| **架构设计** | 9/10 | 分层清晰，抽象合理，符合设计文档 |
| **代码质量** | 8/10 | ✅ 已修复批次发送失败处理，添加了 deprecation 警告 |
| **测试覆盖** | 8/10 | Agent 143 passed, Fusion 89 passed |
| **文档完整性** | 8/10 | 设计文档详尽，TODO.md 追踪完整 |
| **Legacy 清理** | 10/10 | ✅ 所有 Legacy 模块已彻底删除 |

---

## 二、初级开发者完成的任务

### 2.1 ✅ Legacy 代码彻底删除

以下文件已被成功删除：

| 文件 | 状态 | 提交 |
|------|------|------|
| `fusion/src/fustor_fusion/in_memory_queue.py` | ✅ 已删除 | feb270d |
| `fusion/src/fustor_fusion/queue_integration.py` | ✅ 已删除 | feb270d |
| `fusion/src/fustor_fusion/processing_manager.py` | ✅ 已删除 | feb270d |
| `fusion/src/fustor_fusion/runtime/datastore_event_manager.py` | ✅ 已删除 | feb270d |
| `fusion/src/fustor_fusion/api/ingestion.py` | ✅ 已删除 | feb270d |

删除统计：**-1223 行代码**，代码库更加精简。

### 2.2 ✅ phases.py 批次发送失败处理

**修复前的问题**：
```python
# 旧代码：即使发送失败也清空 batch
if success:
    pipeline._update_role_from_response(response)
    pipeline.statistics["events_pushed"] += len(batch)
batch = []  # ← 问题：失败时也清空了！
```

**修复后**：
```python
# 新代码：只有成功才清空 batch，失败则抛出异常
if success:
    pipeline._update_role_from_response(response)
    pipeline.statistics["events_pushed"] += len(batch)
    batch = []  # ← 修复：只有成功才清空
else:
    raise Exception("Snapshot batch send failed")  # ← 新增：失败时抛出异常
```

这个修复确保了批次发送失败时不会丢失数据。

### 2.3 ✅ datastore_id 废弃警告

`FusionPipeline` 现在正确地发出 `DeprecationWarning`：

```python
@property
def datastore_id(self) -> str:
    """Deprecated alias for view_id."""
    import warnings
    warnings.warn("datastore_id is deprecated, use view_id instead", DeprecationWarning, stacklevel=2)
    return self.view_id
```

测试运行时可以看到警告被正确触发：
```
DeprecationWarning: datastore_id is deprecated, use view_id instead
```

### 2.4 ✅ 新增 Pipeline 管理 API

在 `pipe.py` 中新增了以下管理端点：

| 端点 | 用途 |
|------|------|
| `GET /api/v1/pipe/pipelines` | 列出所有管理的 Pipeline |
| `GET /api/v1/pipe/pipelines/{pipeline_id}` | 获取特定 Pipeline 详情 |
| `GET /api/v1/pipe/session/` | 列出所有活跃 Session |

### 2.5 ✅ Leader Session 缓存

`FusionPipeline` 现在正确缓存 leader session：

```python
def __init__(self, ...):
    # ...
    self._cached_leader_session: Optional[str] = None

@property
def leader_session(self) -> Optional[str]:
    """Get the current leader session ID. Cached value."""
    return self._cached_leader_session
```

---

## 三、测试验证结果

### 3.1 Fusion 测试

```
======================== 89 passed, 6 warnings in 2.09s ========================
```

警告均为预期的 `DeprecationWarning`，表明废弃机制正常工作。

### 3.2 Agent 测试

```
============================= 143 passed in 7.02s ==============================
```

所有测试通过，无警告。

---

## 四、仍需注意的问题

### 4.1 ⚠️ session_bridge.py 中的 datastore_id 调用

```python
# fusion/src/fustor_fusion/runtime/session_bridge.py:101
view_id = str(self._pipeline.datastore_id)  # 触发了 DeprecationWarning
```

**建议修复**：将此处改为使用 `view_id`：
```python
view_id = str(self._pipeline.view_id)
```

### 4.2 ⚠️ pipe.py 中的调试注释

```python
# Line 75
# ... existing session listing ...

# Line 91-95
# Get all managed view_ids from datastore_state_manager or similar
# For simplicity, session_manager.get_all_sessions() returns EVERYTHING.
# Wait, does it have get_all_sessions()?

# Let's check session_manager.py
```

**建议**：清理这些调试注释，它们不应该出现在生产代码中。

### 4.3 ⚠️ 测试文件中的 datastore_id 使用

```python
# fusion/tests/runtime/test_fusion_pipeline.py:79
assert fusion_pipeline.datastore_id == "1"  # 触发 DeprecationWarning
```

**建议**：更新测试以使用 `view_id`，或在测试中显式忽略此警告。

---

## 五、更新后的 TODO 清单

### 5.1 ✅ 已完成（高优先级）

- [x] **彻底删除 Legacy 代码**: 完全移除以下文件和相关引用
- [x] **修复 phases.py 批次发送失败处理**: 确保失败时不丢失数据
- [x] **datastore_id 废弃警告**: 在 `FusionPipeline.datastore_id` 添加 DeprecationWarning
- [x] **leader_session 属性**: 实现缓存机制

### 5.2 待完成（低优先级）

- [ ] **清理 session_bridge.py**: 将 `datastore_id` 改为 `view_id`
- [ ] **清理 pipe.py 调试注释**: 移除开发过程中的临时注释
- [ ] **更新测试使用 view_id**: 避免测试中的 DeprecationWarning

---

## 六、一致性特性保留情况

根据 `.agent/artifacts/02-CONSISTENCY_DESIGN.md` 和 `.agent/artifacts/04-Detailed_Decisions.md` 设计决策 6.x，以下功能需保留在 V2 中：

| 功能 | 设计文档要求 | 实现位置 | 状态 |
|------|--------------|----------|------|
| Leader/Follower 机制 | fs 特有设计，保留在 view-fs/source-fs | `AgentPipeline`, `FusionPipeline` | ✅ 已实现 |
| Sentinel 巡检 | view-fs, source-fs | `phases.run_sentinel_check()` | ✅ 已实现 |
| Suspect List (热文件检测) | view-fs | `FSViewProvider` | ✅ 已实现 |
| Blind-spot List (盲区检测) | view-fs | `FSViewProvider` | ✅ 已实现 |
| Tombstone List (墓碑保护) | view-fs | `FSViewProvider` | ✅ 已实现 |
| Audit 跳过优化 | source-fs, view-fs | `source-fs` 驱动 | ✅ 已实现 |
| 断点续传 | Pipeline 级别 | `AgentPipeline` | ✅ 已实现 |
| LogicalClock | View 级别 | `fustor_core.clock` | ✅ 已实现 |
| 陈旧证据保护 | `last_updated_at` 字段 | `view-fs` | ✅ 已实现 |

---

## 七、总结

本次 V2 架构重构已成功完成。初级开发者按照评审建议完成了所有高优先级任务：

1. ✅ **Legacy 代码彻底清理**: 删除了 5 个主要文件，减少了 1223 行代码
2. ✅ **错误处理修复**: 批次发送失败时不再丢失数据
3. ✅ **术语迁移**: `datastore_id` 添加了废弃警告
4. ✅ **新增管理 API**: 支持 Pipeline 和 Session 的监控

**合并建议**: 可以合并到主分支。建议在后续迭代中完成低优先级的清理工作。

---

*评审人: AI Code Reviewer*  
*初审日期: 2026-02-02*  
*更新日期: 2026-02-02 16:10*

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
| `sender_handler_adapter.py` | Sender → SenderHandler 适配器 | ✅ 完成 |
| `source_handler_adapter.py` | Driver → SourceHandler 适配器 | ✅ 完成 |
| `pipeline/phases.py` | Pipeline 阶段实现（snapshot, message, audit） | ✅ 修复 |
| `pipeline/worker.py` | 异步迭代器包装器 | ✅ 完成 |

### A.3 Fusion 运行时 (`fusion/src/fustor_fusion/runtime/`)

| 文件 | 用途 | 状态 |
|------|------|------|
| `fusion_pipeline.py` | FusionPipeline 实现 | ✅ 修复 |
| `pipeline_manager.py` | Pipeline 生命周期管理 | ✅ 完成 |
| `session_bridge.py` | Pipeline-SessionManager 桥接 | ⚠️ 需更新 |
| `view_handler_adapter.py` | Driver → ViewHandler 适配器 | ✅ 完成 |

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

## 附录 C：已删除的 Legacy 模块

以下模块已被彻底删除：

| 文件路径 | 说明 | 删除日期 |
|----------|------|----------|
| `fusion/src/fustor_fusion/in_memory_queue.py` | Legacy 内存队列实现 | ✅ 2026-02-02 |
| `fusion/src/fustor_fusion/queue_integration.py` | Legacy 队列适配层 | ✅ 2026-02-02 |
| `fusion/src/fustor_fusion/processing_manager.py` | Legacy 处理任务管理 | ✅ 2026-02-02 |
| `fusion/src/fustor_fusion/runtime/datastore_event_manager.py` | Legacy 事件管理 | ✅ 2026-02-02 |
| `fusion/src/fustor_fusion/api/ingestion.py` | Legacy 摄入 API | ✅ 2026-02-02 |
| `fusion/tests/test_ingestion_processing.py` | Legacy 测试 | ✅ 2026-02-02 |
| `fusion/tests/test_session_cleanup.py` | Legacy 测试 | ✅ 2026-02-02 |
| `fusion/tests/test_session_concurrent.py` | Legacy 测试 | ✅ 2026-02-02 |

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
