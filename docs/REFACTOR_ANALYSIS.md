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

### Phase 2: Agent 重构 🔄 进行中

5. ✅ **重命名 pusher → sender**
   - `packages/sender-http/` - 新包，实现 Sender 抽象
   - `packages/pusher-fusion/` - 废弃，重定向到 sender-http
   - `senders-config.yaml` - 新配置加载器，兼容 pushers-config.yaml

6. ⬜ **重构 SyncInstance → Pipeline** (暂缓)
   - 提取通用逻辑到 Pipeline
   - FS 特有逻辑保留在 FSSourceHandler

### Phase 3: Fusion 重构

7. **创建 fustor-receiver-http**
   - 从 fusion/api 抽取 HTTP 接收逻辑
   - 实现 Receiver 抽象

8. **重构 Datastore → View 映射**
   - 废弃 datastore_id 概念
   - Pipeline 直接绑定 View

9. **重构 Session 管理**
   - Session 属于 Pipeline
   - View 通过 Pipeline 接收 Session 事件

### Phase 4: 配置与测试更新

10. **配置文件迁移**
    - 目录重命名
    - 添加新配置文件

11. **API 路径更新**
    - `/api/v1/ingest` → `/api/v1/pipe`
    
12. **测试更新**
    - 更新 import 路径
    - 适配新 API

---

## 7. 风险点

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| LogicalClock 迁移破坏一致性 | 高 | 保持接口不变，仅移动位置 |
| 配置解析逻辑变更 | 中 | 渐进式迁移，保持旧格式兼容 |
| Session 管理重构 | 高 | 先测试覆盖再改动 |
| API 路径变更 | 中 | 一次性变更，清理旧路径 |

---

## 8. 下一步行动

1. ✅ 创建分支 `refactor/architecture-v2`
2. ✅ 阅读现有代码，理解业务逻辑
3. ⬜ 开始 Phase 1: 合并基础模块到 fustor-core
4. ⬜ 创建 Pipeline, Transport 抽象
5. ⬜ 创建 fustor-schema-fs
