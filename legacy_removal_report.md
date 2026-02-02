# 遗留代码清理报告 (Legacy Code Removal Report)

## 1. 清理概述
根据 V2 架构设计要求，对项目中过时的“Pusher”术语、兼容性别名以及回退逻辑进行了清理。此举旨在完全拥抱“Sender/Pipeline”架构，减少维护负担。

## 2. 具体变更

### 2.1 驱动服务层
- **文件**: `agent/src/fustor_agent/services/drivers/sender_driver.py`
- **变更**: 移除了 `PusherDriverService = SenderDriverService` 这一别名。
- **影响**: 代码库中将不再支持通过 `PusherDriverService` 这个名字引用驱动服务。

### 2.2 运行适配器层
- **文件**: `agent/src/fustor_agent/runtime/sender_handler_adapter.py`
- **变更**: 
    - 移除了 `send_batch` 方法中对旧版 `push()` 方法的回退（fallback）支持。
    - 移除了相关文档注释中对旧版术语的引用。
- **影响**: 所有 Sender 驱动现在必须实现 `send_events` 方法，不再支持旧的 `push` 接口。

### 2.3 传输层包
- **文件**: `packages/sender-http/src/fustor_sender_http/__init__.py`
- **变更**: 移除了 `FusionSender = HTTPSender` 别名。
- **影响**: 插件入口和内部引用应统一使用 `HTTPSender`。

## 3. 验证结果
- 已运行心跳可靠性测试 `test_agent_pipeline_heartbeat_reliability.py`，结果：**通过** (2 Passed)。
- 验证了代码中已无 `Pusher` 相关的逻辑执行代码。

## 4. 给开发者的建议
- 如果您在开发新的驱动或组件，请严格遵守 `packages/core` 中定义的 `Sender`/`Pipeline` 接口。
- 不再建议在任何新代码、注释或配置中使用 "Pusher" 或 "SyncInstance" 术语。
