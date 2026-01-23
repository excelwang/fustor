# fustor-source-fs

This package provides a `SourceDriver` implementation for the Fustor Agent service, enabling it to monitor and extract data from local file systems. It employs a "Smart Dynamic Monitoring" strategy to efficiently handle large directory structures, supporting both snapshot (initial scan) and real-time (event-driven) synchronization of file changes.

## Features

*   **File System Monitoring**: Utilizes `watchdog` to detect file creation, modification, deletion, and movement events in real-time.
*   **Smart Dynamic Monitoring**: Implements a sophisticated strategy to monitor large directory trees efficiently.
*   **Snapshot Synchronization**: Performs an initial scan of the configured directory to capture existing files as a snapshot.
*   **Real-time Message Synchronization**: Delivers file system events (create, update, delete) as they occur.

## 数据完整性约束 (Data Integrity Constraints)

为了确保 Fusion 视图的绝对可靠性，Source FS 驱动遵循以下三层过滤与收敛策略：

### 1. 活跃感知过滤 (Local Active Perception)
*   **原理**：利用消息流先于快照流启动的特性，在内存中维护 **活跃写入集合 (Active Writers Set)**。
*   **动作**：快照扫描遇到处于 `MODIFY` 状态且未 `CLOSE_WRITE` 的文件时，立即 **跳过 (Skip)**。

### 2. 热数据延后收敛 (Hot-data Deferred Convergence)
针对快照扫描时虽无事件但元数据仍不稳定的文件，实施 **“先推送冷数据，后循环校验热数据”** 机制：
*   **判定阈值**：设定热数据窗口（建议 600s/10分钟）。凡是 `now() - mtime < 10min` 的文件均标记为“热文件”。
*   **首轮扫描**：快照流优先推送所有“冷文件”。热文件不丢失，而是进入 **延后校验队列 (Deferred Queue)**。
*   **次轮校验**：在全量冷数据推送完毕后，驱动回过头来对队列中的热文件进行循环 `stat` 校验：
    *   若文件已稳定（`mtime` 停留在 10 分钟前），则执行补推。
    *   若文件依然在变热（`mtime` 持续更新），则继续保留在队列中，等待下一轮检查。
*   **优势**：该机制保证了 Fusion 树的结构完整性，同时避免了单个热文件阻塞整个快照进程。

### 3. 多客户端可见性边界 (Multi-Client Visibility Gap)
**场景分析**：若 Agent 部署在客户端 A，而另一台服务器 B 正在向 NFS 写入。
*   **可见性滞后**：由于 NFS 的属性缓存（`acregmax`），客户端 A 看到的目录结构可能不是实时的。
*   **空洞风险**：若 A 扫过目录 `/d/z` 后，B 在该目录下新建了 `/d/z/dd`，当前的快照扫描将 **遗漏** 该目录。
*   **解决契约**：
    *   **核心承诺**：Fusion 的 `READY` 状态代表的是 **“Agent 启动时刻的物理快照 + 实时增量流”** 的并集。
    *   **补全机制**：针对多客户端导致的“扫描后空洞”，Source FS 必须支持 **定期增量轮询 (Incremental Polling)** 或由业务触发 **局部深度扫描 (Deep Crawl)**，以确保远程非感知事件最终能被 Fusion 捕获。

### 4. 权威哨兵机制 (Authoritative Sentinel Mode)
在多 Agent 并存的环境中，Source FS 驱动必须支持 **“权威接管”** 逻辑：

1.  **权威 Session 声明**：Fusion 总是以最后启动并完成快照的 Agent 为该 Datastore 的唯一权威数据源。
2.  **热文件定期巡检 (Periodic Integrity Sweep)**：
    *   作为权威哨兵，驱动必须开启一个后台线程，每隔固定周期（如 1 分钟）对整个目录树进行 **热点扫描 (Hot-path Stat)**。
    *   **判定逻辑**：查找所有物理 `mtime` 在近期（如 10 分钟）内的文件。
3.  **一致性断路器 (Consistency Circuit Breaker)**：
    *   如果在巡检中发现任何“物理存在但内存树中缺失”或“元数据不匹配”的热文件，驱动必须立即向 Fusion 发送 **“状态异常”** 信号。
    *   **后果**：系统将立即触发告警，或将 Fusion 视图标记为 **不可用 (Service Unavailable)**，直到该差异被补全同步。

## 责任界定
Source FS 承诺：推送到 Fusion 的每一个字节都必须是“逻辑完备”的。权威 Agent 对数据视图的完整性负有实时存续责任。