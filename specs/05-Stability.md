# Fustor 稳定性设计 (Stability Design)

> 版本: 1.0.0
> 日期: 2026-02-09

本文档详细说明了 Fustor 系统为确保高可用性和数据完整性而实施的稳定性机制。

## 1. 错误处理与恢复 (Error Handling & Recovery)

### 1.1 连接重试 (Connection Retry)
- **机制**: 指数退避 (Exponential Backoff)
- **参数**: 
  - 初始重试间隔: 5.0s
  - 最大重试间隔: 60.0s
  - 最大连续错误次数: 5 (触发 ERROR 状态)
- **行为**: 当 Fusion 连接失败或 Session 创建失败时，Agent Pipe 会自动进入 `RECONNECTING` 状态，并在退避一段时间后重试。

### 1.2 异常隔离 (Exception Isolation)
- **原则**: 单个文件的处理失败不应导致整个任务崩溃。
- **实现**:
  - **Source-FS**: 在文件扫描 (`FSScanner`) 和事件处理 (`EventHandler`) 中捕获异常 (如 `PermissionError`, `FileNotFoundError`)，记录错误并跳过，确保后续文件继续处理。
  - **Fusion View**: 处理单个事件失败时记录错误，但不中断整个 Batch 的处理。

---

## 2. 数据完整性 (Data Integrity)

### 2.1 原子写完整性 (Atomic Write Integrity)
- **问题**: 大文件写入过程中产生多次 `IN_MODIFY` 事件，导致消费者读取到不完整数据。
- **机制**:
  - **标记**: `is_atomic_write` 字段在数据源头标记事件性质。
  - **部分写处理**: `is_atomic_write=False` 的事件在 Fusion 端被标记为 `integrity_suspect=True`。
  - **完整写确认**: 收到 `is_atomic_write=True` (如 `IN_CLOSE_WRITE`) 时，立即清除 Suspect 标记。

### 2.2 可疑文件判定 (Suspect Logic)
- **逻辑时钟**: 基于 `LogicalClock` 判定文件是否 "Too Young" (可能处于 NFS 缓存未刷新状态)。
- **哨兵巡检**: 周期性 (`Sentinel Sweep`) 检查 Suspect 文件的元数据稳定性，确保最终一致性。

---

## 3. 资源保护 (Resource Protection)

### 3.1 流量控制 (Flow Control)
- **批处理**: Agent 使用 `batch_size` (默认 100) 聚合事件发送，减少网络开销。
- **限流**: FileSystem Watcher 使用 `throttle_interval` (默认 1.0s) 合并频繁的 `IN_MODIFY` 事件。

### 3.2 内存管理
- **EventBus**: 使用固定大小的环形缓冲区 (默认 1000)，快慢消费者自动分裂，防止内存溢出。

# 4. 异常处理
无论是agent还是fusion，无论遇到哪种异常，都应该不崩溃，而是使用log error。例如agent的source-fs在扫描文件系统时，如果遇到文件权限、路径异常、inotify异常、文件被其他进程占用等等问题，应该记录错误并跳过该文件，而不是崩溃。再例如fusion在处理数据时，如果遇到数据格式错误，应该记录错误并跳过该数据，而不是崩溃。