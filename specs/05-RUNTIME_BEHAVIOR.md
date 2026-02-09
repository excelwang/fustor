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

---

## 2. 审计缓存 (Audit Mtime Cache)

### 2.1 定义
`Audit Context` (即 mtime cache) 是 Agent 端的内存缓存，用于实现增量审计 (Incremental Audit) 和 "True Silence" 优化。

### 2.2 生命周期
- **存储位置**: `AgentPipe` 实例的内存堆 (非持久化)。
- **构建时机**: 当选 Leader 后的 **第一次 Audit** 过程中动态构建。
- **清空时机**:
    1. **Session 重建**: 显式清空。
    2. **角色切换**: 当选 Leader 时 (`_handle_role_change`) 显式清空。
    3. **Pipe 重启**: 进程重启意味着内存丢失。

### 2.3 行为影响
- **Leader 当选后首轮审计**: **全量扫描** (IO 较高)。
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
