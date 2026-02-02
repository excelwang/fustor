# 重构分支评审报告 (Refactored Branch Review Report)

- **Master 分支 Commit**: `6ac983c3ababf9c4f20cf437ff0be8eed94b7fda`
- **重构分支 Commit**: `59d6fa81e449e164933e664202130056f4e41749`

## 1. 设计项对应情况表 (Design Items Review)

| ID | Design Item | Item Description | Master Implementation | Refactor Implementation | Suggestion |
|:---|:---|:---|:---|:---|:---|
| D-01 | Receiver : Pipeline = 1 : N | 一个 Receiver 可服务多个 Pipeline | 1:1 耦合（接收器固定处理特定 Datastore） | `HTTPReceiver.register_api_key(key, pipeline_id)` 实现多 Pipeline 映射 | **已完成**。符合 V2 架构要求。 |
| D-02 | Pipeline : View = 1 : N | 一个 Pipeline 可分发到多个 View | 分发逻辑硬编码在业务逻辑中 | `FusionPipeline` 持有 `List[ViewHandler]`，通过循环分发 | **已完成**。符合规范。 |
| D-03 | View : Pipeline = N : 1 | 一个 View 可接收多个 Pipeline 的事件 | 不支持（Datastore 隔离） | 通过 `view_id` 共享 `ViewManager` 实例实现聚合 | **已完成**。实现巧妙。 |
| D-04 | LogicalClock 层级 | 逻辑时钟应属于 View 级别 | View 级别 | `FSState` 持有 `LogicalClock`，View 级别 | **已完成**。一致性逻辑正确。 |
| D-05 | 审计周期层级 | 审计 start/end 是 View 级别的特殊设计 | 隐式关联 Datastore | 由 `FusionPipeline` 发送到对应的 `ViewHandler` (View 级别) | **已完成**。 |
| D-06 | 断点续传 (Resume) | 续传点位逻辑层级 | Datastore 级别 | 移动到 `SenderHandler.get_latest_committed_index` (Pipeline 级别) | **已修复**。之前初稿中遗漏，现已在 `AgentPipeline` 中实现。 |
| D-07 | 集成审计 (Incremental Audit) | 审计 mtime_cache 管理 | 存储在 `SyncInstance` 内存中 | 存储在 `AgentPipeline.audit_context` 中 | **已修复**。之前初稿中遗漏，导致每次审计全量扫描，现已修复。 |
| D-08 | 术语统一 (view_id) | 使用 `view_id` 替代 `datastore_id` | 大量使用 `datastore_id` | 大部分已替换为 `view_id`，保留属性别名以兼容旧代码 | **已完成**。符合规范。 |

## 2. Master 分支中未记录功能 (Undocumented Functionality in Master)

| ID | Functionality | Master Implementation | Refactor Implementation | Suggestion |
|:---|:---|:---|:---|:---|
| U-01 | 角色切换时的强制快照 | 当 Follower 晋升为 Leader 时，强制执行快照以确权 | 在 `SyncInstance._handle_role_change` 中启动 `_run_leader_sequence` | 在 `AgentPipeline._check_role_change` 中检测并触发 `_run_snapshot_sync` | **已完成**。逻辑对齐。 |
| U-02 | 审计静默目录保护 | 审计发现目录未变时，发送 `audit_skipped=True` 消息以保护子项 | `SourceFS` 驱动实现并发送事件 | 已保留原有驱动逻辑，且 Pipeline 正常透传 | **已完成**。保证了 NFS 一致性。 |
| U-03 | 时间单位启发式处理 | 自动识别并转换毫秒/秒级时间戳 | `FSArbitrator` 中包含 `agent_time > 1e11` 启发判定 | **已保留但建议移除**。规范要求通过协议强制秒级。 | 需要在后续步骤中彻底移除启发式逻辑，强制统一。 |
| U-04 | 初始触发事件 (Fake Event) | 启动时发送虚假事件以强制触达 Fusion | `_run_message_sync` 在空循环前发送一个 fake event | **未实现**。Refactor 中直接开始轮询。 | 考虑到 Refactor 在 Session 创建阶段已与 Fusion 交互，且有独立心跳，可能不需要此 Hack。 |

## 3. 缺陷与建议 (Defects and Improvement Suggestions)

### 3.1 核心缺陷

1.  **[D-06/D-07 修正确认]**：在本次评审前的自动修复中，已成功找回了 **断点续传** 和 **增量审计** 的能力。这是非常关键的，避免了重构导致的数据完整性和性能回退。
2.  **时间单位启发式代码残留**：`FSArbitrator` 中依然保留了 `if agent_time > 1e11: agent_time /= 1000.0` 的逻辑。这在 V2 架构中是不推崇的，应该在协议层强制要求单一位（秒）。
3.  **日志过度密集 (Log Spam)**：`FusionPipeline.py` 中存在大量 `print` 语句打印接收到的每一个事件和 Payload，在生产环境下会严重拖累性能并撑爆日志。

### 3.2 建议 TODO 事项清单

- [ ] **移除时间单位启发式判定**：在 `FSArbitrator` 中移除 `MS_THRESHOLD` 相关逻辑，并在 `SourceFS` 等驱动中确保输出始终为秒。
- [ ] **清理测试性打印**：移除 `FusionPipeline.py` 中的 `print("DEBUG_FUSION_EVENT: ...")` 等调试代码。
- [ ] **完善 Session 超时处理**：确保 `AgentPipeline` 能够正确接收并应用 `Fusion` 在 Session 创建响应中返回的 `suggested_heartbeat_interval_seconds`。
- [ ] **优化 Suspect 稳定性检查流程**：在 `FSViewProvider.update_suspect` 中，如果反馈的 mtime 发生了变化，应强制更新 `suspect_list` 中的 `recorded_mtime` 和过期时间。

---
评审专家：Antigravity (Senior Architect)
状态：待进一步修正
