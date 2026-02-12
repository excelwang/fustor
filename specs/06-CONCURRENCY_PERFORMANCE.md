# Fustor 并发模型与性能约束 (Concurrency & Performance)

> 版本: 1.0.0
> 日期: 2026-02-10
> 补充: 01-ARCHITECTURE.md, 05-Stability.md

本文档约束 Fustor 运行时的并发模型、锁策略和性能设计决策。
**任何修改锁粒度、读写策略或队列机制的变更，都必须参照本文档评估影响。**

---

## 1. 并发模型基本假设

### 1.1 单线程事件循环

Fustor (Agent & Fusion) 运行在 **asyncio 单线程事件循环**中。

**核心推论**:
- `dict.get`、`dict.__setitem__` 等同步操作在同一 event loop tick 内**不会被抢占**
- 纯同步函数（不含 `await` 的函数）执行期间不会发生协程切换
- 只读访问共享 dict **可以省略锁**（CPython GIL + asyncio 单线程双重保证）

> [!CAUTION]
> 如果未来引入 `asyncio` 多线程模式（如 `run_in_executor` 访问共享状态），以上假设将失效，
> 所有标记为"无锁读"的代码路径都需要重新评估。

### 1.2 不使用多线程共享内存

所有跨组件通信通过 asyncio 协程完成，不使用 `threading.Lock` 或 `concurrent.futures`
操作共享状态。CPU 密集操作（如 hash 计算）可以使用 `run_in_executor`，但
**不得在 executor 中访问任何共享可变状态。**

### 1.3 Thread Bridge for Sync Iterators

为桥接同步 IO（如 `os.scandir`、`MinIO SDK`）与异步管道，使用 **Thread Bridge Pattern**：

1. **Producer Thread**: 专用线程运行同步迭代器
2. **Async Queue**: `asyncio.Queue` 作为反向压力缓冲区
3. **Bridge Logic**:
    - Producer 使用 `run_coroutine_threadsafe(queue.put(...))` 投递数据
    - 必须使用 `threading.Event` (`stop_event`) 监听 Consumer 的退出信号
    - **死锁预防**: Producer 在 `queue.put()` 等待时无法感知 `stop_event`，因此 Consumer 在退出前必须 **排空队列 (Drain Queue)** 以解锁 Producer。

> [!WARNING]
> 禁止简单的 `run_in_executor(None, next, iter)` 模式，这会导致大量的上下文切换。
> Thread Bridge 模式通过批量缓冲显著降低了开销。

---

## 2. 锁粒度策略

### 2.1 Per-View 锁（ViewStateManager, SessionManager）

| 组件 | 锁对象 | 粒度 |
|------|--------|------|
| ViewStateManager | `_view_locks[view_id]` | per-view |
| SessionManager | `_view_locks[view_id]` | per-view |
| PipeManager | `_pipe_locks[pipe_id]` (session ops) | per-pipe |
| PipeManager | `_init_lock` (init/start/stop) | 全局 |

**约束**:
- 不同 view 的读写操作**互不阻塞**
- 同一 view 内的写操作通过 per-view `asyncio.Lock` 排他
- 只读方法（`get_state`, `is_leader`, `is_locked` 等）**不持锁**，直接 `dict.get` 返回
- `_get_view_lock` / `_get_pipe_lock` 使用 `dict.setdefault` 惰性创建

> [!WARNING]
> 禁止将 per-view/per-pipe 锁退化为全局锁。如果新功能需要跨 view 的原子操作，
> 应设计为先收集、后逐个加锁的两阶段模式，而不是引入全局锁。

### 2.2 View-FS 读写锁

View-FS 使用 `AsyncRWLock` 控制内存树的并发访问：

| 操作类型 | 锁模式 | 示例 |
|---------|--------|------|
| 事件处理 (process_event) | Read Lock | 多个 session 的事件可并发处理 |
| 查询 (query) | Read Lock | 多个 API 查询可并发执行 |
| 快照重建 (rebuild) | Write Lock | 排他，阻塞所有读写 |

**约束**:
- 读操作之间**完全并发**，不互斥
- 写操作**排他**，等待所有读操作完成后进入
- AsyncRWLock 的实现依赖 asyncio 单线程模型（见 §1.1）

> [!IMPORTANT]
> 不得将 `read_lock` 替换为 `write_lock`（"为了安全"）。这会将并发度降至 1，
> 在高负载下造成严重性能退化。只有实际修改树结构的操作才应使用 write_lock。

---

## 3. 队列与背压

### 3.1 事件队列隔离

```
Pipe A (view_id="view-a")  →  Queue A  →  Processing Loop A
Pipe B (view_id="view-b")  →  Queue B  →  Processing Loop B
```

**约束**:
- 每个 `FusionPipe` 有独立的事件队列和处理循环
- 不同 view 的事件处理**完全并行**，互不阻塞
- 禁止引入跨 pipe 的共享队列

### 3.2 队列容量与背压

| 参数 | 值 | 说明 |
|------|-----|------|
| `maxsize` | 10,000 | 队列满时 `put` 阻塞，上游 HTTP 请求将超时 |

**已知行为**: 当事件产生速度持续超过消费速度时，队列将满，`asyncio.Queue.put()`
将 block 直到队列有空间。这会导致上游 HTTP receiver 的请求超时（通常 30s），
Agent 会在下一个心跳周期重试。**这是预期的降级行为**。

### 3.3 队列排空信号

`FusionPipe._queue_drained` (`asyncio.Event`) 在队列清空时 set，入队时 clear。
用于 `signal_audit_end` 的等待机制，替代了轮询。

**约束**: 不得使用 `while not queue.empty(): await asyncio.sleep(N)` 的忙等待模式。
应使用 Event-based 的 `wait_for_drain()` 方法。

---

## 4. 缓存策略

### 4.1 配置缓存

| 缓存 | TTL | 位置 |
|------|-----|------|
| views-config YAML | 5 秒 | `api/views.py` |

**约束**: 对 YAML 配置文件的读取必须经过 TTL 缓存。禁止在请求处理路径中
每次都执行 `yaml.safe_load()`。

### 4.2 Leader 状态缓存

`SessionBridge._leader_cache` 缓存已知的 leader session，避免每次心跳都执行 `try_become_leader`。

**缓存验证**: 每 `_LEADER_VERIFY_INTERVAL`（5）次心跳，调用 `view_state_manager.is_leader()`
验证缓存有效性。若发现不一致则清除缓存、重新竞选。

**约束**: 不得永久信任 leader 缓存。必须有周期性验证机制，防止外部状态变更
导致缓存永远过期。

---

## 5. 性能设计容量

### 5.1 设计目标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 事件吞吐 | 1,000 events/sec (峰值) | 单 pipe 内串行处理即可满足 |
| 并发 session | 100+ | 通过 per-view/per-pipe 锁支撑 |
| 查询并发 | 无限制 | 读操作无锁 |

### 5.2 已知限制与接受的权衡

| 限制 | 影响 | 接受原因 |
|------|------|---------|
| 单 pipe 内事件串行处理 | 单 pipe 延迟与事件数线性相关 | 1000 events/sec 下仅占 5% 单核 CPU，10K+ 才需优化 |
| 单消费者 processing loop | 无法利用多核并行处理 | view-fs state 非线程安全，并行化需大量重构 |
| `_ensure_parent_chain` 逐文件调用 | 重复的 O(depth) dict 查找 | depth < 20，dict 查找为 O(1)，实际开销可忽略 |

> [!NOTE]
> 以上限制在 10K+ events/sec 场景下可能需要重新评估。
> 当时应考虑：多 worker processing loop + view-fs 并发安全改造。

---

## 6. 统计更新策略

`FusionPipe.statistics` 的更新（如 `events_received += N`）**不使用锁**。

**安全依据**: 在 asyncio 单线程模型下，`dict[key] += N` 在同一 event loop tick 内完成，
不存在竞争条件。`get_dto()` 读取统计信息时同样无需加锁。

**约束**: 如果统计字段需要跨协程的 read-modify-write（如条件更新），
仍需使用 `asyncio.Lock` 保护。
