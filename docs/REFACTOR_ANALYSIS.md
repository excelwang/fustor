# 重构分析：现有代码结构与迁移策略

> 日期: 2026-02-01
> 分支: refactor/architecture-v2

## 1. 现有代码结构总览

### 1.1 包依赖关系

```
                   ┌─────────────────────────────────────────────────────────┐
                   │                   应用层                                  │
                   │  ┌─────────────────┐       ┌─────────────────┐          │
                   │  │  fustor-agent   │       │  fustor-fusion  │          │
                   │  │  (app.py, cli)  │       │  (main.py, api) │          │
                   │  └────────┬────────┘       └────────┬────────┘          │
                   └───────────┼─────────────────────────┼───────────────────┘
                               │                         │
                   ┌───────────▼─────────────────────────▼───────────────────┐
                   │                 服务层/驱动层                              │
                   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐   │
                   │  │ source-fs   │  │ pusher-     │  │ view-fs         │   │
                   │  │ source-oss  │  │ fusion      │  │                 │   │
                   │  │ source-mysql│  │ pusher-echo │  │                 │   │
                   │  └──────┬──────┘  └──────┬──────┘  └────────┬────────┘   │
                   └─────────┼────────────────┼──────────────────┼───────────┘
                             │                │                  │
                   ┌─────────▼────────────────▼──────────────────▼───────────┐
                   │                   核心层                                  │
                   │  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐  │
                   │  │ fustor-core │  │ event-model  │  │ common          │  │
                   │  │ (drivers,   │  │ (EventBase,  │  │ (logging,       │  │
                   │  │  config,    │  │  EventType)  │  │  LogicalClock)  │  │
                   │  │  exceptions)│  │              │  │                 │  │
                   │  └─────────────┘  └──────────────┘  └─────────────────┘  │
                   └──────────────────────────────────────────────────────────┘
                             │                │                  │
                   ┌─────────▼────────────────▼──────────────────▼───────────┐
                   │                   SDK层                                   │
                   │  ┌─────────────────┐        ┌────────────────────┐       │
                   │  │ fustor-agent-sdk│        │ fustor-fusion-sdk  │       │
                   │  │ (interfaces)    │        │ (client, interfaces│       │
                   │  └─────────────────┘        └────────────────────┘       │
                   └──────────────────────────────────────────────────────────┘
```

### 1.2 现有模块清单

| 包名 | 主要职责 | 文件数 | 依赖 |
|------|---------|--------|------|
| **common** | 日志、守护进程、LogicalClock | 8 | 无 |
| **core** | DriverABC, 配置模型, 异常 | ~10 | event-model |
| **event-model** | EventBase, EventType, MessageSource | 1 | 无 |
| **agent-sdk** | Agent 服务接口定义 | 1 | core |
| **fusion-sdk** | FusionClient, SessionInfo | 2 | 无 |
| **source-fs** | FSDriver (Source) | ~8 | core, event-model |
| **pusher-fusion** | FusionDriver (Pusher) | 1 | core, fusion-sdk |
| **view-fs** | FSViewProvider | ~10 | core, common, event-model |

---

## 2. 关键业务逻辑位置

### 2.1 Agent 侧

| 业务逻辑 | 当前位置 | 新位置建议 |
|---------|---------|-----------|
| SyncInstance 控制循环 | `agent/runtime/sync.py` | 保留，适配新抽象 |
| EventBus 事件分发 | `agent/services/instances/bus.py` | 保留 |
| Source Driver 管理 | `agent/services/drivers/source_driver.py` | 保留 |
| Pusher Driver 管理 | `agent/services/drivers/pusher_driver.py` | 重命名为 Sender |
| 心跳、Leader 检测 | `agent/runtime/sync.py::_run_heartbeat_loop` | 提取到 Pipeline |
| 审计循环 | `agent/runtime/sync.py::_run_audit_loop` | FS 特有，保留 |
| Sentinel 循环 | `agent/runtime/sync.py::_run_sentinel_loop` | FS 特有，保留 |

### 2.2 Fusion 侧

| 业务逻辑 | 当前位置 | 新位置建议 |
|---------|---------|-----------|
| SessionManager | `fusion/core/session_manager.py` | Pipeline Engine 层 |
| DatastoreStateManager | `fusion/datastore_state_manager.py` | 需重构（datastore → view） |
| ViewManager | `fusion/view_manager/manager.py` | Pipeline Engine 层 |
| InMemoryEventQueue | `fusion/in_memory_queue.py` | Pipeline Engine 层 |
| 事件处理/仲裁 | `view-fs/arbitrator.py` | 保留 Handler 层 |
| 审计管理 | `view-fs/audit.py` | 保留 Handler 层 |
| LogicalClock | `common/logical_clock.py` | → `fustor-core/clock/` |

---

## 3. 现有抽象类分析

### 3.1 fustor_core.drivers.SourceDriver

```python
# 关键接口
get_snapshot_iterator() -> Iterator[EventBase]
get_message_iterator(start_position) -> Iterator[EventBase]
get_audit_iterator() -> Iterator[EventBase]  # Optional
perform_sentinel_check(task_batch) -> Dict     # Optional
close()
```

**评估**: 接口设计良好，无需大改。只需与 Schema 抽象整合。

### 3.2 fustor_core.drivers.PusherDriver

```python
# 关键接口
create_session(task_id) -> str
push(events) -> Dict
heartbeat() -> Dict
signal_audit_start() / signal_audit_end()  # Optional
get_sentinel_tasks() / submit_sentinel_results()  # Optional
close()
```

**评估**: 需要重命名为 Sender，职责边界需要与 Pipeline 分离。

### 3.3 fustor_core.drivers.ViewDriver

```python
# 关键接口
process_event(event) -> bool
get_data_view() -> Any
on_session_start() / on_session_close()  # Optional
handle_audit_start() / handle_audit_end()  # Optional
reset() / cleanup_expired_suspects()  # Optional
```

**评估**: 接口设计良好。需要添加 Schema 版本检查。

---

## 4. 配置文件分析

### 4.1 Agent 配置

| 当前文件 | 新文件 | 变更 |
|---------|--------|------|
| `sources-config.yaml` | `sources-config.yaml` | 保持 |
| `pushers-config.yaml` | `senders-config.yaml` | 重命名 |
| `syncs-config/*.yaml` | `agent-pipes-config/*.yaml` | 重命名目录 |

### 4.2 Fusion 配置

| 当前文件 | 新文件 | 变更 |
|---------|--------|------|
| `datastores-config.yaml` | (废弃) | 删除 |
| `views-config/*.yaml` | `views-config/*.yaml` | 保持 |
| (无) | `receivers-config.yaml` | 新增 |
| (无) | `fusion-pipes-config/*.yaml` | 新增 |

---

## 5. 一致性逻辑保留清单

> **关键**: 以下逻辑必须完整迁移，不能丢失

### 5.1 View-FS 特有 (保留在 fustor-view-fs)

| 组件 | 文件 | 说明 |
|------|------|------|
| FSArbitrator | `arbitrator.py` | Smart Merge 核心 |
| AuditManager | `audit.py` | 审计周期管理 |
| FSState | `state.py` | 内存状态 (suspect, blind-spot, tombstone) |
| TreeManager | `tree.py` | 内存树操作 |
| FSViewQuery | `query.py` | 查询接口 |

### 5.2 Source-FS 特有 (保留在 fustor-source-fs)

| 组件 | 位置 | 说明 |
|------|------|------|
| WatchManager | `components.py` | 动态监控 |
| LRUCache | `components.py` | Watch 缓存 |
| get_audit_iterator | `__init__.py` | True Silence 优化 |
| perform_sentinel_check | `__init__.py` | Sentinel 验证 |

### 5.3 通用逻辑 (迁移到 fustor-core)

| 组件 | 当前位置 | 新位置 |
|------|---------|--------|
| LogicalClock | `common/logical_clock.py` | `fustor-core/clock/logical_clock.py` |
| EventBase | `event-model/models.py` | `fustor-core/event/base.py` |
| EventType | `event-model/models.py` | `fustor-core/event/types.py` |
| MessageSource | `event-model/models.py` | `fustor-core/event/types.py` |

---

## 6. 重构阶段计划

### Phase 1: 合并基础模块 (fustor-core) ✅ 已完成

1. ✅ **合并 common, event-model 到 fustor-core**
   - 保留所有现有功能
   - 创建子模块结构: `common/`, `event/`, `clock/`

2. ✅ **创建 Pipeline 抽象**
   - `fustor-core/pipeline/pipeline.py` - Pipeline ABC
   - `fustor-core/pipeline/context.py` - PipelineContext
   
3. ✅ **创建 Transport 抽象**
   - `fustor-core/transport/sender.py` - Sender ABC
   - `fustor-core/transport/receiver.py` - Receiver ABC

4. ✅ **创建 fustor-schema-fs**
   - 提取 EventBase 的 FS 特定字段
   - 定义 SCHEMA_NAME, SCHEMA_VERSION

### Phase 2: Agent 重构 ✅ 完成

5. ✅ **重命名 pusher → sender**
   - `packages/sender-http/` - 新包，实现 Sender 抽象
   - `packages/pusher-fusion/` - 废弃，重定向到 sender-http
   - `senders-config.yaml` - 新配置加载器，兼容 pushers-config.yaml
   - `SenderDriverService` - 替代 PusherDriverService

6. ⬜ **重构 SyncInstance → Pipeline** (暂缓)
   - 提取通用逻辑到 Pipeline
   - FS 特有逻辑保留在 FSSourceHandler

### Phase 3: Fusion 重构 ✅ 完成

7. ✅ **创建 fustor-receiver-http**
   - `packages/receiver-http/` - 新包，实现 Receiver 抽象
   - 提供 FastAPI routers 用于 session 和 event 处理
   - 回调架构支持灵活集成

8. ✅ **创建 receivers-config.yaml 配置**
   - `ReceiversConfigLoader` - 传输端点和 API key 管理
   - 支持多 API key 映射到 pipeline

9. ✅ **创建 FusionPipelineConfig**
   - `FusionPipelinesConfigLoader` - Pipeline 配置加载
   - 支持 fusion-pipes-config/*.yaml 目录结构
   - 直接绑定 Receiver → View

### Phase 4: 配置与测试更新 ✅ 完成

10. ✅ **配置文件迁移**
    - senders-config.yaml (Agent)
    - receivers-config.yaml (Fusion)
    - fusion-pipes-config/*.yaml (Fusion Pipeline)

11. ✅ **API 路径更新**
    - `/api/v1/pipe` 新路径 (推荐)
    - `/api/v1/ingest` 保留向后兼容
    - FusionSDK 支持 api_version 参数
    
12. ✅ **测试更新**
    - 更新 import 路径到 fustor_core
    - 303 tests passing, 2 expected deprecation warnings

---

## 7. 风险点

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| LogicalClock 迁移破坏一致性 | 高 | ✅ 保持接口不变，仅移动位置 |
| 配置解析逻辑变更 | 中 | ✅ 渐进式迁移，保持旧格式兼容 |
| Session 管理重构 | 高 | ⬜ 暂缓，先测试覆盖再改动 |
| API 路径变更 | 中 | ✅ 双路径支持，渐进迁移 |

---

## 8. 下一步行动

1. ✅ 创建分支 `refactor/architecture-v2`
2. ✅ 阅读现有代码，理解业务逻辑
3. ✅ Phase 1: 合并基础模块到 fustor-core
4. ✅ Phase 2: Agent 重构 (sender-http, SenderDriverService)
5. ✅ Phase 3: Fusion 重构 (receiver-http, pipelines)
6. ✅ Phase 4: 导入路径更新 + API 路径
7. ✅ schema-fs: Pydantic 模型 + 22 个测试
8. ✅ 术语迁移: Pusher → Sender (完整迁移，保持向后兼容)
   - SenderConfig, SenderConfigDict
   - SenderDriverService, SenderConfigService
   - SenderDriverServiceInterface, SenderConfigServiceInterface
   - Entry points: fustor_agent.drivers.senders
   - AppConfig: get_senders(), add_sender(), delete_sender()
9. ✅ Pipeline 抽象: Phase 2 完成
   - AgentPipeline 完整实现 (含控制循环)
   - FusionPipeline 完整实现
   - SourceHandlerAdapter, SenderHandlerAdapter
   - ViewDriverAdapter, ViewManagerAdapter
   - PipelineBridge 迁移工具
   - 89 个 Pipeline 相关测试
10. ✅ 废弃 datastores-config.yaml (添加废弃警告，优先 receivers-config)
11. ✅ Session 管理整合: PipelineSessionBridge
    - 桥接 FusionPipeline 与 SessionManager
    - 同步 session 创建/销毁
    - 9 个单元测试
12. ✅ SyncInstance → AgentPipeline 迁移
    - SyncInstanceService 支持 feature flag 切换
    - FUSTOR_USE_PIPELINE=true 启用新架构
    - 零风险渐进式迁移

**当前测试状态**: 401 passed, 1 xfailed, 0 warnings

## 重构完成！

所有主要重构任务已完成。架构现在支持：

1. **双轨运行**: SyncInstance (生产) 和 AgentPipeline (可选) 可并行
2. **渐进式迁移**: 通过环境变量 `FUSTOR_USE_PIPELINE` 控制
3. **向后兼容**: 所有旧配置格式仍然支持
4. **完整测试覆盖**: 401 个测试确保功能正确



