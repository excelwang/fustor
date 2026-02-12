# Fustor 运行时行为与生命周期 (Runtime Behavior)

> 版本: 1.0.0
> 日期: 2026-02-04
> 补充: 02-CONSISTENCY_DESIGN.md

本文档记录了系统在运行时的动态行为细节，包括 Leader 选举、角色切换、审计缓存生命周期等。

## 1. Leader 选举与角色切换

### 1.1 选举策略: 先到先得 (First-Come-First-Serve)
Leader 的选举完全由 Fusion 端控制，采用非抢占式的锁机制。

- **获取锁**: 第一个连接到 View 的 Session 会立即获得 Leader 锁。
- **排队**: 后续连接的 Session 只能成为 Follower (Standby)。
- **释放锁**: 仅当 Leader Session 断开连接或显式销毁时，锁才会被释放。

### 1.2 故障转移 (Failover Promotion)
当 Leader 掉线时，Fusion 会执行 **First Response Promotion**：

1. 检测到 Leader Session 终止。
2. 释放 View 的 Leader 锁。
3. 选择**第一个成功获取锁**的 Follower（基于 `try_become_leader` 返回顺序）。
4. 立即将其提拔为新的 Leader。

> **设计理由**：响应快的节点通常意味着网络延迟低、负载轻，更适合承担 Leader 的 Snapshot/Audit 任务。此策略实现简单且自然地实现了负载均衡。

### 1.3 角色切换时的动作 (Agent 侧)

当 Agent 从 Follower 晋升为 Leader 时：

| 动作 | 行为 | 原因 |
|------|------|------|
| **Pre-scan** | **Skip** | 实时消息同步 (`_message_sync`) 在 Follower 期间已启动，监控已就绪。 |
| **Snapshot Sync** | **Trigger** | 确保捕获 Follower 期间可能遗漏的存量状态（仅限 Session 内首次当选）。 |
| **Audit Cache** | **Clear** | **强制清空**审计缓存，确保当选后的第一次审计是**全量扫描** (建立新基准)。 |
| **Sentinel** | **Start** | 启动后台巡检任务。 |

### 1.4 状态标志语义: 瞬态 vs 单调

| 标志类型 | 示例 | 语义 | 适用场景 |
|---------|------|------|----------|
| **瞬态标志** (Transient) | `PipeState.MESSAGE_SYNC` | 表示某个 Task **当前正在运行**。Task 完成、异常退出或重建时，标志会短暂清除。 | 内部控制逻辑 (如是否需要重启 task) |
| **单调标志** (Monotonic) | `is_realtime_ready` | 表示 Pipe **曾经成功连接过** EventBus 并处于就绪状态。一旦设为 `True`，除非 Pipe 重启不然不会变回 `False`。 | 外部状态判断 (如 Heartbeat `can_realtime`) |

> [!CAUTION]
> 避免在外部监控或测试中使用瞬态标志作为"服务就绪"的判据，这会导致因 Task 重启窗口期引发的 Flaky Test。

### 1.5 Session Timeout Negotiation (超时协商)

Session 超时时间由 **Client-Hint + Server-Default** 共同决定：

1. **Client Hint**: Agent 在 `CreateSessionRequest` 中携带 `session_timeout_seconds` (通常来自本地配置)。
2. **Server Decision**: Fusion 取 `max(client_hint, server_default)` 作为最终超时时间。
3. **Acknowledgment**: Fusion 在响应中返回最终决定的 `session_timeout_seconds`，Agent **必须** 采纳此值作为心跳间隔的基准。

### 1.6 Error Recovery Strategy (错误恢复策略)

控制循环针对不同类型的异常采取差异化的恢复策略：

| 异常类型 | 策略 | 原因 |
|---------|------|------|
| `SessionObsoletedError` | **Immediate Retry** | 会话被服务端主动终结 (如 Leader Failover)，应立即重连以竞选新 Leader。 |
| `RuntimeError` | **Exponential Backoff** | 连接超时、配置错误等环境问题，快速重试会加重系统负担。 |
| `CancelledError` (+ STOPPING) | **Break** | 正常的停止流程。 |
| `CancelledError` (- STOPPING) | **Continue** | 单个 Task (如 Snapshot) 被取消，但 Pipe 仍需运行 (见 §1.7)。 |
| Background Task Crash | **Count & Retry** | 记录连续错误计数，触发 Backoff，等待下一轮循环重启 Task。 |

### 1.7 Heartbeat Never Dies Invariant (心跳永不停止原则)

**核心不变量**: 只要 `AgentPipe.start()` 被调用，心跳循环 **永远不应因内部错误而停止**。

即使 Snapshot 崩溃、Message Sync 异常、Audit 失败，心跳也必须持续向 Fusion 报告状态 (通过 `can_realtime` 字段隐式传递健康度)。

**典型场景**: Leader → Follower 角色降级
1. Fusion 返回 `role=follower`。
2. Agent 取消 `snapshot`/`audit`/`sentinel` 任务 (抛出 `CancelledError`)。
3. 心跳任务 (`_heartbeat_task`) **保持运行**。
4. 控制循环捕获 `CancelledError`，因未设置 `STOPPING` 标志，选择 `continue` 而非退出的，确保管道存活。

---

## 2. 审计缓存 (Audit Mtime Cache)

### 2.1 定义
`Audit Context` (即 mtime cache) 是 Agent 端的**进程内存缓存**，用于实现增量审计 (Incremental Audit) 和 "True Silence" 优化。

**无需持久化**：此缓存仅存在于进程内存中，不需要跨重启保存。Agent 重启后首轮 audit 执行全量扫描是可接受的行为。

### 2.2 生命周期
- **存储位置**: `AgentPipe` 实例的内存堆（纯内存，不持久化）。
- **构建时机**: 当选 Leader 后的 **第一次 Audit** 过程中动态构建。
- **清空时机**:
    1. **Session 重建**: 显式清空。
    2. **角色切换**: 当选 Leader 时 (`_handle_role_change`) 显式清空。
    3. **进程重启**: 内存自然丢失，不做恢复。

### 2.3 行为影响
- **Leader 当选后首轮审计**: **全量扫描** (IO 较高，可接受)。
- **后续审计**: **增量扫描** (仅扫描 mtime 变化的目录，IO 极低)。

### 2.4 Audit Phase Protocol: Start/End Signal Contract

Audit 过程必须严格遵循 Start/End 信号契约，以确保 Fusion 端的一致性维护逻辑被触发。

1. **Start Signal**: 调用 `POST /consistency/audit/start`。Fusion 记录 `last_audit_start` 时间戳。
2. **Streaming**: 发送审计事件流。
3. **End Signal**: **必须** 调用 `POST /consistency/audit/end` (通过发送 `is_final=True` 的空批次)。

> [!IMPORTANT]
> **Must-Send 语义**: 即使 Audit 过程中发生异常 (如 IO 错误、网络中断)，`finally` 块中也 **必须** 发送 End Signal。
> 
> **后果**: 若 Fusion 未收到 End Signal，将认为审计未完成，因此 **永远不会触发 Tombstone 清理和 Blind-spot 检测**，导致已删除文件的"幽灵条目"永久残留。
- **后续审计**: **增量扫描** (仅扫描 mtime 变化的目录，IO 极低)。

---

## 3. 热文件监控 (Hot Watch Set)

### 3.1 逻辑保留
尽管 FS 驱动经过了重构，但 Hot Watch Set 的逻辑被严格保留：

- **扫描**: `FSScanner` 返回所有目录及其 mtime。
- **排序**: `FSDriver` 根据 mtime 倒序排列。
- **截断**: 仅对前 N 个（配置限制）活跃目录建立 inotify watch。
- **漂移计算**: 计算 NFS Server 与 Agent 本地时间的 `drift`，并在设置 Watch 时修正时间戳。

### 3.2 架构分离
- **FSScanner**: 负责 IO 和遍历 (无状态)。
- **FSDriver**: 负责策略 (Policy)、排序 (Sorting) 和 调度 (Scheduling)。

---

## 4. Command Execution (命令执行)

Fusion can issue commands to the Agent via the Heartbeat response channel.

### 4.1 Execution Priority
- **High Priority**: Commands (like `scan`) are processed immediately after the Heartbeat response is received.
- **Concurrency**: Commands are typically executed as asynchronous tasks, running in parallel with the main Event Loop.

### 4.2 Supported Commands
- **`scan`**: Triggers a recursive file system scan for a specific path.
    - **Trigger**: User requests `force-real-time=true` in Fusion View API.
    - **Behavior**: Agent performs an immediate `FSScanner` walk of the target path.
    - **Events**: Generated events are pushed to Fusion, updating the View state in real-time.
