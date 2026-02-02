# Fustor Architecture V2 重构代码评审报告

> 评审日期: 2026-02-02  
> 评审分支: `refactor/architecture-v2`  
> 对比基准: `master`

---

## 📊 总体评价

**重构进展**: ⭐⭐⭐⭐☆ (4/5) - 核心架构已完成，部分细节需要完善

**代码质量**: ⭐⭐⭐⭐☆ (4/5) - 结构清晰，但存在一些可改进之处

**测试覆盖**: ⭐⭐⭐⭐⭐ (5/5) - 136个运行时测试全部通过

---

## ✅ 已完成的重构内容

### 1. 核心包结构 (fustor-core)

| 模块 | 状态 | 说明 |
|------|------|------|
| `fustor_core/pipeline/` | ✅ 完成 | Pipeline, Handler, SenderHandler 抽象层 |
| `fustor_core/transport/` | ✅ 完成 | Sender, Receiver 抽象层 |
| `fustor_core/event/` | ✅ 完成 | EventBase, EventType, MessageSource |
| `fustor_core/clock/` | ✅ 完成 | LogicalClock 迁移 |
| `fustor_core/common/` | ✅ 完成 | 通用工具类迁移 |
| `fustor_core/exceptions.py` | ✅ 完成 | SessionObsoletedError 等 |

### 2. 传输层 (Transport)

| 包 | 状态 | 说明 |
|---|------|------|
| `fustor-sender-http` | ✅ 完成 | 替代 pusher-fusion |
| `fustor-sender-echo` | ✅ 完成 | 测试用 Sender |
| `fustor-receiver-http` | ✅ 完成 | 从 Fusion 抽取的接收器 |

### 3. Agent Pipeline

| 组件 | 状态 | 说明 |
|------|------|------|
| `AgentPipeline` | ✅ 完成 | 完整实现，包含所有阶段 |
| `SourceHandlerAdapter` | ✅ 完成 | 适配现有 Source Driver |
| `SenderHandlerAdapter` | ✅ 完成 | 适配 Sender 到 SenderHandler |
| `EventBus` | ✅ 完成 | 支持多订阅者、自动分片 |

### 4. Fusion Pipeline

| 组件 | 状态 | 说明 |
|------|------|------|
| `FusionPipeline` | ✅ 完成 | 事件处理、Session管理 |
| `ViewHandlerAdapter` | ✅ 完成 | 适配现有 View Driver |
| `PipelineManager` | ✅ 完成 | Pipeline 生命周期管理 |
| `SessionBridge` | ✅ 完成 | V2/Legacy Session 桥接 |

### 5. 术语重命名

| 旧术语 | 新术语 | 状态 |
|--------|--------|------|
| Pusher | Sender | ⚠️ 基本完成，有向后兼容代码 |
| syncs-config | agent-pipes-config | ✅ 完成 |
| datastores-config | ⚠️ 待废弃 | 仍在使用 |
| /api/v1/ingest | /api/v1/pipe | ⚠️ 双轨运行中 |

---

## ⚠️ 发现的问题和改进建议

### 1. 【高优先级】datastore_id 与 view_id 混用

**问题描述**: 根据设计文档，`datastore_id` 应该被 `view_id` 替代，但当前代码中仍大量使用 `datastore_id`。

**影响范围**:
- `fusion/src/fustor_fusion/api/` 下的所有路由文件
- `fusion/src/fustor_fusion/core/session_manager.py`
- `fusion/src/fustor_fusion/runtime/fusion_pipeline.py`

**建议修复**:
```python
# 当前代码
self.datastore_id = str(config.get("datastore_id", pipeline_id))

# 建议更改
self.view_id = str(config.get("view_id", config.get("datastore_id", pipeline_id)))
```

**TODO清单**:
- [ ] 更新 SessionManager 使用 `view_id` 替代 `datastore_id`
- [ ] 更新 API 依赖注入 `get_datastore_id_from_api_key` → `get_view_id_from_api_key`
- [ ] 保留向后兼容的别名

---

### 2. 【高优先级】Legacy 配置文件仍在使用

**问题描述**: 多个 Legacy 配置加载器仍在主流程中使用。

**影响范围**:
- `fusion/src/fustor_fusion/config/datastores.py` - 应该被废弃
- `agent/src/fustor_agent/config/pushers.py` - 应该迁移到 senders.py

**建议**:
```python
# 在 datastores.py 顶部添加废弃警告
import warnings
warnings.warn(
    "datastores-config.yaml is deprecated. "
    "Please migrate to views-config/ and fusion-pipes-config/",
    DeprecationWarning
)
```

**TODO清单**:
- [ ] 为 `datastores.py` 添加废弃警告
- [ ] 确保 `receivers-config.yaml` 正确加载
- [ ] 完全移除对 `pushers-config.yaml` 的引用

---

### 3. 【中优先级】AgentPipeline 文件过大

**问题描述**: `agent/src/fustor_agent/runtime/agent_pipeline.py` 有 803 行代码，职责过多。

**当前职责**:
1. Session 生命周期管理
2. Snapshot/Message/Audit 同步
3. Heartbeat 管理
4. 错误恢复与重连
5. EventBus 集成
6. 角色切换 (Leader/Follower)

**建议拆分**:
```
agent/src/fustor_agent/runtime/
├── agent_pipeline.py          # 主协调器 (~200行)
├── pipeline_phases/
│   ├── snapshot.py            # Snapshot 同步逻辑
│   ├── message.py             # Message 同步逻辑
│   └── audit.py               # Audit 同步逻辑
├── heartbeat.py               # Heartbeat 管理
└── error_recovery.py          # 错误恢复策略
```

**TODO清单**:
- [ ] 将 `_run_snapshot_sync` 抽取到独立模块
- [ ] 将 `_run_message_sync` 和 `_run_bus_message_sync` 抽取
- [ ] 将 `_run_audit_loop` 和 `_run_sentinel_loop` 抽取

---

### 4. 【中优先级】HTTPReceiver 回调未完全集成

**问题描述**: `fusion/src/fustor_fusion/api/pipe.py` 中的 V2 路由设置依赖运行时对象，但 `setup_pipe_v2_routers()` 可能在 `runtime_objects.pipeline_manager` 初始化前被调用。

**问题代码** (pipe.py:28-49):
```python
def setup_pipe_v2_routers():
    from .. import runtime_objects
    
    if runtime_objects.pipeline_manager:  # 可能为 None
        receiver = runtime_objects.pipeline_manager.get_receiver("http-main")
        ...
```

**建议修复**:
```python
def setup_pipe_v2_routers():
    """Mount V2 routers. Call this AFTER lifespan initialization."""
    from .. import runtime_objects
    
    if not runtime_objects.pipeline_manager:
        logger.error("setup_pipe_v2_routers called before pipeline_manager initialized")
        return False
    
    receiver = runtime_objects.pipeline_manager.get_receiver("http-main")
    ...
```

**TODO清单**:
- [ ] 确保 `setup_pipe_v2_routers` 只在 lifespan 初始化后调用
- [ ] 添加更明确的错误处理和日志

---

### 5. 【中优先级】重复的 Session 管理逻辑

**问题描述**: 存在两套并行的 Session 管理：
1. `fusion/src/fustor_fusion/core/session_manager.py` (Legacy)
2. `FusionPipeline._active_sessions` (V2)

**影响**:
- Session 状态可能不一致
- 清理逻辑重复

**建议**:
- V2 FusionPipeline 应该委托给统一的 SessionManager
- 或完全取代 Legacy SessionManager

**TODO清单**:
- [ ] 统一 Session 管理为单一来源
- [ ] 删除 FusionPipeline 内部的 `_active_sessions` 
- [ ] 使用 SessionBridge 作为唯一接口

---

### 6. 【低优先级】pusher 术语残留

**问题描述**: 代码中仍有 17 处 "pusher" 相关引用，主要是向后兼容代码和注释。

**影响文件**:
- `agent/src/fustor_agent/config/syncs.py` - 兼容旧配置
- `agent/src/fustor_agent/services/drivers/sender_driver.py` - 别名
- `agent/src/fustor_agent/services/configs/sender.py` - 文档

**建议**: 暂时保留用于向后兼容，但应在文档中标注废弃时间表。

---

### 7. 【低优先级】_aiter_sync 可能存在内存泄漏

**问题描述**: `AgentPipeline._aiter_sync` 中的生产者线程在某些边缘情况下可能不会正确终止。

**问题代码** (agent_pipeline.py:394-436):
```python
async def _aiter_sync(self, sync_iter: Iterator[Any], queue_size: int = 1000):
    ...
    thread = threading.Thread(target=_producer, name=f"PipelineSource-Producer-{self.id}", daemon=True)
    thread.start()
    
    try:
        while True:
            item = await queue.get()
            ...
    finally:
        stop_event.set()
        # 注意: 线程不会被 join，可能导致资源未释放
```

**建议修复**:
```python
finally:
    stop_event.set()
    # Give thread a chance to exit gracefully
    thread.join(timeout=1.0)
    if thread.is_alive():
        logger.warning(f"Producer thread {thread.name} did not exit cleanly")
```

**TODO清单**:
- [ ] 添加线程 join 以确保资源释放
- [ ] 添加超时处理避免阻塞

---

### 8. 【低优先级】缺少 Schema 包实现

**问题描述**: 根据设计文档，应该有 `fustor-schema-fs` 包，但当前实现中 Schema 定义仍然分散。

**TODO清单**:
- [ ] 完成 `packages/schema-fs/` 的测试覆盖
- [ ] 将 Event 模型从 `fustor_core.event` 迁移到 schema 包

---

### 9. 【建议】添加 Pipeline 状态机文档

**问题描述**: `PipelineState` 使用了 `IntFlag` 位掩码，状态组合较复杂，但缺少状态转换图文档。

**建议**: 在 `docs/refactoring/` 添加状态机图：

```
STOPPED ─────────────────────────────────────────────────────────────┐
    │                                                                 │
    ▼                                                                 │
INITIALIZING ─────┬───────────────────────────────────────────────────┤
    │             │                                                   │
    ▼             ▼ (error)                                           │
RUNNING ──────► ERROR ────────────────────────────────────────────────┤
    │             │                                                   │
    ├─► SNAPSHOT_PHASE                                                │
    │       │                                                         │
    │       ▼                                                         │
    ├─► MESSAGE_PHASE ◄─────────────────────────────────────────────┐ │
    │       │                                                       │ │
    │       ├─► AUDIT_PHASE                                         │ │
    │       │       │                                               │ │
    │       │       └──────────────────────────────────────────────►┘ │
    │       │                                                         │
    ├─► RECONNECTING (可与其他状态组合)                                │
    │       │                                                         │
    │       └───────────────────────────────────────────────────────►┘
    │
    └─► PAUSED (Follower mode)
```

---

## 📋 完整 TODO 清单

### 高优先级 (P0)

1. [ ] 统一 `datastore_id` → `view_id` 术语迁移
2. [ ] 废弃 `datastores-config.yaml`，完成配置迁移
3. [ ] 确保 V2 API 路由在正确时机初始化

### 中优先级 (P1)

4. [ ] 拆分 `AgentPipeline` 为多个模块 (500行以内)
5. [ ] 统一 Session 管理逻辑
6. [ ] 完善 HTTPReceiver 回调注册

### 低优先级 (P2)

7. [ ] 修复 `_aiter_sync` 线程资源释放
8. [ ] 完成 schema-fs 包测试
9. [ ] 添加 Pipeline 状态机文档
10. [ ] 清理 pusher 术语残留 (在兼容期结束后)

---

## 🎯 结论

本次重构已完成核心架构目标：

1. **对称架构**: Agent (Source → Sender) 与 Fusion (Receiver → View) 对称
2. **分层清晰**: Core → Transport → Handler → Pipeline → Application
3. **可扩展**: 支持多协议 (HTTP, 未来 gRPC)、多 Schema

**建议下一步行动**:

1. 优先处理 P0 级别问题，确保 V2 API 完全可用
2. 在主线程稳定后，逐步拆分大文件
3. 设定 Legacy 代码废弃时间表 (建议: 3个月后)

---

## 附录: 测试覆盖

```
agent/tests/runtime/ - 63 个测试用例 ✅
fusion/tests/runtime/ - 73 个测试用例 ✅
总计: 136 个测试，全部通过
```

**测试建议补充**:
- [ ] 添加 HTTPReceiver 与 HTTPSender 的端到端集成测试
- [ ] 添加 Pipeline Manager 的多 Receiver 测试
- [ ] 添加 Session 超时边界条件测试
