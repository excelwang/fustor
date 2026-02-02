# Refactored Branch Review Report

## 核心元数据
* **评审日期**: 2026-02-02
* **评审人**: Antigravity (Senior Architect Agent)
* **Master Branch**: `6ac983c`
* **Refactor Branch**: `04390a2`

## 1. 架构与代码设计评审

| ID | 设计项 (Design Item) | 描述 (Description) | Master 实现 (Master Impl) | 重构后实现 (Refactor Impl) | 改进建议 (Suggestion) |
|---|---|---|---|---|---|
| D-01 | **Architecture** | Agent 内部核心结构 | `SyncInstance` 单体上帝类，耦合严重 | `AgentPipeline` (生命周期) + `Source/SenderHandlers` (业务逻辑) | **保留并继续完善**。重构后的职责分离 (SoC) 非常清晰，符合 V2 架构要求。 |
| D-02 | **Reliability** | 错误重试机制 | 固定次数循环 (hardcoded 5 retries) | 指数退避 (Exponential Backoff) | **保留**。显著提升了网络抖动下的稳定性。 |
| D-03 | **Lifecycle** | 状态管理 | 字符串/散乱的 `bool` 标志 | `PipelineState` 位掩码 (Bitmask) | **保留**。状态管理更加严谨，支持复合状态 (e.g. `RUNNING | SNAPSHOT`)。 |
| D-04 | **Data Integrity** | **CRITICAL: 断点续传 (Resume)** | 启动时调用 `pusher.get_latest_committed_index` 获取服务端偏移量 | **丢失**。`AgentPipeline` 仅从 `self.statistics` (内存) 读取，重启后丢失进度。 | **必须修复**。在 `start()` 或 `_run_message_sync()` 中必须调用 `sender_handler.get_latest_committed_index()`，否则重启会导致数据丢失或重复。 |
| D-05 | **Performance** | **CRITICAL: 增量审计 (Incremental Audit)** | 维护 `audit_mtime_cache`，对比 mtime 跳过未变更目录 (Silent Dirs) | **丢失**。不再维护 MTime Cache，导致每次审计都被迫进行全量扫描。 | **必须修复**。在 `AgentPipeline` 中引入 `audit_context/cache`，并传递给 `run_audit_sync`。这是 FS 驱动性能的核心。 |
| D-06 | **Correctness** | Driver Mode 启动位置 | 动态传递 `start_position` | 硬编码为 `-1` (Latest) | **必须修复**。`run_driver_message_sync` 必须接收并使用重传的 `start_position`，否则 Driver 模式下的断点续传失效。 |
| D-07 | **Common Lib** | 核心库结构 | 散落在 `packages/common`, `event-model` | 统一在 `packages/core` | **保留**。结构更合理，但需确保所有 Driver 的导入路径已更新。 |

## 2. 遗漏的非文档化功能 (Undocumented Functionality)

| ID | 功能点 (Functionality) | Master 实现 (Master Impl) | 重构后实现 (Refactor Impl) | 建议 (Suggestion) |
|---|---|---|---|---|
| U-01 | **Failover Trigger** | 发送 Fake Initial Event 以确保 Pusher 建立连接/获取角色 | **移除**。依赖显式的 `create_session` 调用 | **需验证**。如果 Fusion 侧依赖 Event 推送来触发某些状态变更，可能需要恢复；否则显式 Session 创建更好。 |
| U-02 | **Silent Dirs Handling** | Driver 返回 `(None, mtime_update)` 元组，SyncInstance 更新本地缓存 | Driver 返回元组，但 `phases.py` 遇到 `None` 直接 `continue`，**丢弃了 mtime 更新** | **必须修复**。当收到 `None` 事件时，必须提取并更新 `audit_mtime_cache`，否则"静默目录"在下一次扫描时会被误认为"无缓存的新目录"而重新扫描。 |
| U-03 | **Startup Mode** | 支持 `message-only` 模式 (仅实时流，无快照) | 逻辑在 Driver 内部，似乎保留了 | **确认测试**。确保 `startup_mode` 参数能正确传递给 Driver。 |
| U-04 | **Legacy Pusher Logic** | 包含大量针对旧版 `pusher-fusion` 的兼容代码 | 清除旧代码，使用 `SenderHandler` | **保留**。彻底清除历史包袱是本次重构的目标之一。 |

## 3. 总结与下一步

本次重构在架构分层和稳定性（退避重试）方面有显著进步，代码结构更加清晰。但是，**在数据一致性和性能优化方面存在严重回退**，特别是：
1.  **丢失断点续传能力**：重启后无法从 Fusion 获取上次点位。
2.  **丢失增量审计优化**：导致审计性能大幅下降（O(N) 全量 IO vs O(Updated) 增量 IO）。
3.  **遗漏 Driver 模式的 Offset 处理**。

建议立即着手修复 **D-04**, **D-05**, **D-06** 和 **U-02**，这四个问题是阻碍上线的 Critical Blocker。
