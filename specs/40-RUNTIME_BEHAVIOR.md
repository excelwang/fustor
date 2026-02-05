# Fustor 运行时行为与生命周期 (Runtime Behavior)

> 版本: 1.1.0
> 日期: 2026-02-05
> 补充: 10-ARCHITECTURE, 20-CORE_ALGORITHMS

本文档记录了系统在运行时的动态行为细节，包括 Leader 选举、角色切换、审计缓存生命周期等。

## 1. Leader 选举与角色切换

### 1.1 选举策略: FCFS + Fastest Response
Leader 的选举完全由 Fusion 端控制，采用非抢占式的锁机制。

- **获取锁**: 第一个连接到 View 的 Session 会立即获得 Leader 锁（First-Come-First-Serve）。
- **排队**: 后续连接的 Session 只能成为 Follower (Standby)。
- **释放锁**: 仅当 Leader Session 断开连接或显式销毁时，锁才会被释放。

### 1.2 故障转移 (Failover Promotion)
当 Leader 掉线时，Fusion 会执行 **Fastest Response Promotion**：

1. 检测到 Leader Session 终止。
2. 释放 View 的 Leader 锁。
3. 向所有 Follower 发送竞争请求（或使用最近心跳响应时间）。
4. 选择**响应最快**的 Follower 提拔为新的 Leader。

> **设计理由**: Fastest Response 优于 Oldest Follower，因为响应快通常意味着网络延迟低、负载轻，更适合承担 Leader 职责。

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
- **存储位置**: `AgentPipeline` 实例的内存堆 (非持久化)。
- **构建时机**: 当选 Leader 后的 **第一次 Audit** 过程中动态构建。
- **清空时机**:
    1. **Session 重建**: 显式清空。
    2. **角色切换**: 当选 Leader 时 (`_handle_role_change`) 显式清空。
    3. **Pipeline 重启**: 进程重启意味着内存丢失。

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

## 4. Agent 影子参考系 (Shadow Reference Frame)

Agent 内部维护一个**虚拟物理参考系**，用于保护本地资源管理逻辑不受 NFS 时钟跳变干扰。

### 4.1 问题背景

| 场景 | 影响 |
|------|------|
| NFS Server 被 `ntpdate` 强制同步 | Agent 的 LRU 排序和扫描调度被打乱 |
| 孤岛文件 mtime 跳到 2050 年 | 所有正常文件的归一化年龄被拉大，导致提前被踢出缓存 |

### 4.2 解决方案：P99 漂移采样

**算法**：
1. 收集所有目录的 mtime 值
2. 取 P99.9 分位数作为**稳定最新时间** (`latest_mtime_stable`)
3. 计算漂移：`drift = latest_mtime_stable - time.time()`
4. 归一化时间戳：`lru_timestamp = server_mtime - drift`

**效果**：
- 单个异常文件（如 2050 年 mtime）不会影响其他文件的排序
- LRU 调度和 Watch 优先级保持稳定

### 4.3 职责边界

| 范围 | 行为 |
|------|------|
| **Local Only** | 影子参考系仅用于 Agent 内部的 LRU/调度 |
| **消息不变** | Agent 发送的消息始终携带**原始 mtime**（不做归一化） |
| **全局由 Fusion 裁决** | 一致性判定由 Fusion 的 Logical Clock 负责 |

---

## 5. 稳定性原则 (Stability)

无论是 Agent 还是 Fusion，无论遇到哪种异常，都应该**不崩溃**，而是使用 `log.error()` 记录。

### 5.1 Agent 侧
- **文件系统异常**: 权限、路径、inotify、文件被占用等问题，应记录错误并**跳过该文件**。
- **关键代码位置**: `FSScanner.scan()`, `FSDriver.watch()`.

### 5.2 Fusion 侧
- **数据格式错误**: 记录错误并**跳过该事件**。
- **关键代码位置**: `Arbitrator.process_event()`, `ViewProvider.handle_batch()`.

> 详细错误处理策略请参考: 31-RELIABILITY
