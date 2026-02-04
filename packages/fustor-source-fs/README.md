# fustor-source-fs

This package provides a `SourceDriver` implementation for the Fustor Agent service, enabling it to monitor and extract data from local file systems. It employs a "Smart Dynamic Monitoring" strategy to efficiently handle large directory structures, supporting both snapshot (initial scan) and real-time (event-driven) synchronization of file changes.

## Features

*   **File System Monitoring**: Utilizes `watchdog` to detect file creation, modification, deletion, and movement events in real-time.
*   **Smart Dynamic Monitoring**: Implements a sophisticated strategy to monitor large directory trees efficiently.
*   **Snapshot Synchronization**: Performs an initial scan of the configured directory to capture existing files as a snapshot.
*   **Real-time Message Synchronization**: Delivers file system events (create, update, delete) as they occur.

## 数据完整性约束 (Data Integrity Constraints)

为了确保 Fusion 视图的绝对可靠性，Source FS 驱动遵循以下策略：

### 1. 活跃感知过滤 (Local Active Perception)
*   **原理**：利用消息流先于快照流启动的特性，在内存中维护 **活跃写入集合 (Active Writers Set)**。
*   **动作**：快照扫描遇到处于 `MODIFY` 状态且未 `CLOSE_WRITE` 的文件时，立即 **跳过 (Skip)**。

### 2. 热数据延后收敛 (Hot-data Deferred Convergence)
针对快照扫描时虽无事件但元数据仍不稳定的文件，实施 **"先推送冷数据，后循环校验热数据"** 机制：
*   **判定阈值**：设定热数据窗口（建议 600s/10分钟）。凡是 `now() - mtime < 10min` 的文件均标记为"热文件"。
*   **首轮扫描**：快照流优先推送所有"冷文件"。热文件不丢失，而是进入 **延后校验队列 (Deferred Queue)**。
*   **次轮校验**：在全量冷数据推送完毕后，驱动回过头来对队列中的热文件进行循环 `stat` 校验：
    *   若文件已稳定（`mtime` 停留在 10 分钟前），则执行补推。
    *   若文件依然在变热（`mtime` 持续更新），则继续保留在队列中，等待下一轮检查。
*   **优势**：该机制保证了 Fusion 树的结构完整性，同时避免了单个热文件阻塞整个快照进程。

### 3. 审计同步 (Audit Sync)
**解决场景**：多客户端 NFS 环境下，其他服务器的文件变更无法通过 inotify 感知。

*   **快速算法**：利用目录 mtime 跳过无变化的子树，仅扫描变化的目录
*   **消息格式**：打上 `message_source='audit'` 标签，携带 `parent_mtime` 字段
*   **生命周期**：发送 `Audit-Start` 和 `Audit-End` 信号，用于 Fusion 清理 Tombstone

详见 `docs/CONSISTENCY_DESIGN.md` 第 3 节。

### 4. 哨兵巡检 (Sentinel Sweep)

*   **触发者**：Leader Agent
*   **频率**：2 分钟/次
*   **目的**：更新 Suspect List 中文件的 mtime
*   **API**：`GET/PUT /api/view/fs/suspect-list`
*   **消息格式**：打上 `message_source='snapshot'` 标签

## 责任界定
Source FS 承诺：推送到 Fusion 的每一个字节都必须是"逻辑完备"的。Leader Agent 对数据视图的完整性负有实时存续责任。