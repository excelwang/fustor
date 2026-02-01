# Pipeline 重构计划

## 目标

将现有的 `SyncInstance` (909行) 重构为实现 `Pipeline` 抽象接口的 `AgentPipeline`。

## 现状分析

### 现有 SyncInstance 结构

```
SyncInstance (909 lines)
├── __init__() - 初始化，58行配置和状态
├── start() / stop() - 生命周期管理
├── _run_control_loop() - 主控制循环 (143行)
├── _run_heartbeat_loop() - 心跳管理
├── _run_snapshot_sync() - 快照同步 (90行)
├── _run_message_sync() - 消息同步 (90行)
├── _run_audit_loop() / _run_audit_sync() - 审计同步 (80行)
├── _run_sentinel_loop() / _run_sentinel_check() - 哨兵检查 (52行)
├── _handle_role_change() - Leader/Follower 角色切换
├── _compile_mapper_function() - 字段映射编译 (72行)
└── 其他辅助方法
```

### Pipeline 抽象接口

```python
class Pipeline(ABC):
    async def start() -> None
    async def stop() -> None
    async def restart() -> None
    async def on_session_created(session_id, **kwargs) -> None
    async def on_session_closed(session_id) -> None
    def is_running() -> bool
    def is_outdated() -> bool
    def get_dto() -> Dict[str, Any]
```

### Handler 接口

```python
class SourceHandler(Handler):
    def get_snapshot_iterator(**kwargs) -> Iterator
    def get_message_iterator(start_position, **kwargs) -> Iterator
    def get_audit_iterator(**kwargs) -> Iterator
    def perform_sentinel_check(task_batch) -> Dict
```

## 重构策略

### Phase 1: 创建 AgentPipeline 骨架 (低风险)

1. 创建 `agent/src/fustor_agent/runtime/agent_pipeline.py`
2. 实现 `Pipeline` 抽象接口
3. 委托现有 `SyncInstance` 的核心逻辑
4. 保持向后兼容

```python
class AgentPipeline(Pipeline):
    """Agent 端的 Pipeline 实现"""
    
    def __init__(self, config, source_handler, sender_handler):
        super().__init__(pipeline_id, config)
        self.source_handler = source_handler  # SourceHandler
        self.sender_handler = sender_handler  # SenderHandler (新接口)
    
    async def start(self):
        # 委托给现有实现
        pass
```

### Phase 2: 抽象 Sender 为 Handler (中等风险)

1. 创建 `SenderHandler` 类，继承 `Handler`
2. 将 `pusher_driver_instance.push()` 封装为 Handler 接口
3. 更新 `AgentPipeline` 使用新接口

```python
class SenderHandler(Handler):
    """Agent 端发送器的 Handler 抽象"""
    
    async def send_batch(self, events: List[Any]) -> bool:
        """发送一批事件"""
        pass
    
    async def create_session(self, **kwargs) -> str:
        """创建会话"""
        pass
    
    async def send_heartbeat(self, session_id) -> Dict:
        """发送心跳"""
        pass
```

### Phase 3: 迁移控制循环 (高风险)

1. 将 `_run_control_loop` 重构为可测试的状态机
2. 将 snapshot/message/audit 阶段抽象为独立方法
3. 增加单元测试覆盖

### Phase 4: 替换 SyncInstance (高风险)

1. 让 `SyncInstance` 继承 `AgentPipeline`
2. 逐步废弃直接使用 `SyncInstance`
3. 更新 `SyncInstanceService` 使用 `AgentPipeline`

## 向后兼容策略

```python
# 保持 SyncInstance 作为别名
class SyncInstance(AgentPipeline):
    """Deprecated: Use AgentPipeline instead."""
    
    def __init__(self, **kwargs):
        warnings.warn(
            "SyncInstance is deprecated. Use AgentPipeline instead.",
            DeprecationWarning
        )
        super().__init__(**kwargs)
```

## 时间估算

| 阶段 | 工作量 | 风险 | 测试需求 |
|------|--------|------|----------|
| Phase 1 | 2-3 小时 | 低 | 基本集成测试 |
| Phase 2 | 3-4 小时 | 中 | 单元测试 + 集成测试 |
| Phase 3 | 4-6 小时 | 高 | 全面回归测试 |
| Phase 4 | 2-3 小时 | 高 | 全面回归测试 |

**总计**: 11-16 小时

## 建议

由于这是一个高风险的重构，建议：

1. **先完成 Phase 1**，在独立分支验证
2. 增加测试覆盖率后再继续 Phase 2-4
3. 每个阶段完成后都要运行完整测试套件
4. 保持向后兼容，使用 Deprecation Warnings

## 当前状态

- [x] Pipeline 抽象已定义 (fustor_core/pipeline/)
- [x] Handler 抽象已定义 (SourceHandler, ViewHandler)
- [x] SenderHandler 已创建 (fustor_core/pipeline/sender.py)
- [x] AgentPipeline 骨架已创建 (fustor_agent/runtime/agent_pipeline.py)
- [x] SenderHandlerAdapter 已创建 (fustor_agent/runtime/sender_handler_adapter.py)
- [ ] SourceHandlerAdapter 待创建
- [ ] FusionPipeline 待创建 (对应 Fusion 端)
- [ ] SyncInstance → AgentPipeline 完整迁移
