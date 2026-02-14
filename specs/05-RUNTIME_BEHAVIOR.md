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
| **单调标志** (Monotonic) | `is_realtime_ready` | 表示记录 AgentPipe **曾经成功连接过** EventBus 并处于就绪状态。一旦设为 `True`，除非 AgentPipe 重启不然不会变回 `False`。 | 外部状态判断 (如 Heartbeat `can_realtime`) |

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
| `CancelledError` (- STOPPING) | **Continue** | 单个 Task (如 Snapshot) 被取消，但 AgentPipe 仍需运行 (见 §1.7)。 |
| Background Task Crash | **Count & Retry** | 记录连续错误计数，触发 Backoff，等待下一轮循环重启 Task。 |

### 1.7 Heartbeat Never Dies Invariant (心跳永不停止原则)

**核心不变量**: 只要 `AgentPipe.start()` 被调用，心跳循环 **永远不应因内部错误而停止**。

即使 Snapshot 崩溃、Message Sync 异常、Audit 失败，心跳也必须持续向 Fusion 报告状态 (通过 `can_realtime` 字段隐式传递健康度)。

**典型场景**: Leader → Follower 角色降级
1. Fusion 返回 `role=follower`。
2. Agent 取消 `snapshot`/`audit`/`sentinel` 任务 (抛出 `CancelledError`)。
3. 心跳任务 (`_heartbeat_task`) **保持运行**。
4. 控制循环捕获 `CancelledError`，因未设置 `STOPPING` 标志，选择 `continue` 而非退出的，确保 AgentPipe 实例存活。

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

#### Audit 超时保护

当 Fusion 收到 `audit_start` 但超过一定时间（建议为 `2 * audit_interval_sec`）未收到对应的 `audit_end` 时，应自动关闭该审计窗口并记录警告日志。这可以防止 Agent 崩溃在 Audit 中间时，Fusion 的审计窗口永远开启。

---

## 3. 热文件监控 (Hot Watch Set)

### 3.1 逻辑保留
尽管 FS 驱动经过了重构，但 Hot Watch Set 的逻辑被严格保留：

- **扫描**: `FSScanner` 返回所有目录及其 mtime。
- **排序**: `FSDriver` 根据 mtime 倒序排列。
- **截断**: 仅对前 N 个（配置限制）活跃目录建立 inotify watch。
- **漂移计算**: 计算 NFS Server 与 Agent 本地时间的 `drift`，并在设置 Watch 时修正时间戳。

### 3.2 架构分离
- **Audit Interval**: 12 hours (Long cycle)
- **Sentinel Interval**: 5 minutes (Short cycle)
- **FSScanner**: 负责 IO 和遍历 (无状态)。
- **FSDriver**: 负责策略 (Policy)、排序 (Sorting) 和 调度 (Scheduling)。

---

## 4. Command Execution (命令执行)

Fusion can issue commands to the Agent via the Heartbeat response channel.

### 4.1 Execution Priority & State Bypass

- **High Priority**: Commands (like `scan`) are processed immediately after the Heartbeat response is received.
- **Concurrency**: Commands are typically executed as asynchronous tasks, running in parallel with the main Event Loop.
- **State Bypass**: On-Demand tasks **MUST** be executable regardless of the AgentPipe's current state (e.g., Initializing, Follower, Error, Degraded).
    - Even if `is_realtime_ready=False` or the agent_pipe is stuck in `SNAPSHOT_SYNC`, the `scan` command must be processed to support troubleshooting.
    - The execution logic should typically bypass the shared `EventBus` and send results directly via `SenderHandler` to ensure delivery even if the local bus is broken.

### 4.2 Supported Commands
- **`scan`**: Triggers a recursive file system scan for a specific path.
    - **Trigger**: User requests `force-real-time=true` in Fusion View API.
    - **Behavior**: Agent performs an immediate `FSScanner` walk of the target path.
    - **Events**: Generated events are pushed to Fusion, updating the View state in real-time.

---

## 5. Reliability & Self-Healing (Always-On)

为满足 "Agent 永远在线 (Always On)" 的设计目标，系统必须具备在各类甚至由于代码缺陷导致的故障下的自愈能力。

### 5.1 Fault Tolerance Model (故障隔离模型)

| 故障层级 | 影响范围 | 恢复策略 |
|---------|----------|----------|
| **Task Level** (Snapshot/Audit) | 仅影响该阶段的数据更新 | **Task Restart**: Heartbeat 保持运行，Control Loop 经 Backoff 后重启 Task。 |
| **Component Level** (Driver/Bus) | 影响依赖该组件的所有 AgentPipe | **Component Reset**: 必须支持 Invalidate/Re-init (见 §5.2)。 |
| **Session Level** (Fusion Conn) | 影响数据传输 | **Re-Session**: 销毁旧 Session，重新从握手开始 (Backoff/Immediate)。 |
| **Process Level** (OOM/Crash) | 影响整个 Agent | **Service Restart**: 依赖外部 Supervisor (systemd/k8s) 重启进程。 |

### 5.2 Component Reset Protocol (组件重置契约)

对于有状态的单例组件 (如 `FSDriver`)，若发生不可恢复的错误 (如 `inotify` 句柄耗尽、挂载点失效)，简单的 Retry 无效。

**当前策略**：
基于我们的业务场景（NFS 文件监控），采用**简化的重置流程**：

1. **区分璯性错误与暂态错误**: 组件通过连续错误计数超阈值来识别（已实现）
2. **日志告警 + 最大退避重试**: 达到阈值后 CRITICAL 日志 + 最大退避间隔持续重试（已实现）
3. **外部进程 Supervisor**: 对于真正不可恢复的故障（如 OOM），依赖外部 Supervisor (`runner.py` / systemd) 重启进程

> [!NOTE]
> 完整的 Component Reset Protocol（清理缓存 → 重新初始化 → 验证健康）在当前 NFS 监控场景下不是必需的，
> 因为 FSDriver 的状态可以通过重启进程完全重建。未来若引入有状态的持久化组件（如本地数据库缓存），再考虑实现完整协议。

### 5.3 Degraded Mode (降级模式)

组件健康管理采用 **分层级联故障** 模型：

#### 最小粒度组件 (Health Unit)

以下组件是健康维护的最小粒度单元：

| 组件 | 层面 | 说明 |
|------|------|------|
| **Source** | Agent | 数据源驱动（如 FSDriver） |
| **AgentPipe** | Agent | 代理管道 |
| **Sender** | Agent | 上行发送通道 |
| **Receiver** | Fusion | 下行接收通道 |
| **FusionPipe** | Fusion | 融合管道 |
| **View** | Fusion | 视图驱动（如 FSViewDriver） |

#### 级联故障规则 (Cascading Failure Rules)

- **Pipe 失败不影响其他 Pipe**: 每个 Pipe 独立运行，某个 Pipe 崩溃不影响同进程内的其他 Pipe
- **Source/Sender 失败 → 关联 AgentPipe 全部失败**: Source 或 Sender 是共享组件，其故障影响所有依赖它的 AgentPipe
- **Receiver 失败 → 关联 FusionPipe 全部失败**: Receiver 是共享组件，其故障影响所有依赖它的 FusionPipe
- **FusionPipe 失败 → 中止下游视图**: FusionPipe 故障应中止其下游关联的 View 的数据写入，View 的查询 API 的可用性要根据view的类型进行判断。live类型的视图一旦任一FusionPipe失败则不可用（保留应急on-demand查询服务），其他类型的默认继续可用。
- 失败后，组件应尝试通过自行重启恢复运行，并记录错误日志。健康状态中记录失败重启次数。

#### 可观测性要求

所有组件的健康状态必须提供观测手段：
- Heartbeat 响应中携带组件健康摘要
- API 接口提供组件级别的健康检查 (`/health/components`)
- 日志中明确标记组件状态转换事件

### 5.4 Zombie Prevention (僵尸任务预防)

- **Liveness Probe**: Control Loop 不应仅检查 `task.done()`，还应检查 Task 是否在更新 `statistics` 或 `heartbeat` timestamp。
- **Timeout Kill**: 对于卡死 (Stuck) 的 Task，Control Loop 应主动 `cancel()` 并重启。

