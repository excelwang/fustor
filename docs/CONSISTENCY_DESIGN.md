# Fustor 强一致性设计方案：权威哨兵与 Fusion 双名单核验模式

## 1. 背景与目标
在 NFS 等环境下，利用 **“瘦 Agent 感知 + 胖 Fusion 裁决”** 架构，实现分布式存储下的感知空洞发现，保障 **“返回即完整、视图即真相”**。

## 2. 核心架构：最后启动者权威制 (Last-Agent-Wins)

### 2.1 权威 Leader 及其生命周期
1.  **即时夺权**：新 Agent 启动并建立 Session 后，即刻成为 Leader。
2.  **启动序列**：构建监控列表 -> 发起消息同步 -> 发起 **全量快照同步 (Full Snapshot Sync)**。
3.  **视图可用性**：视图状态仅取决于“第一次”全量同步的完成。切换 Leader 不会回退可用状态。
4.  **前任角色转变 (Obsolete as Follower)**：
    *   **心跳 419**：失效状态必须通过心跳请求返回。
    *   **Follower 模式**：收到 419 后，Agent 必须强制终止快照同步和 pre-scan，但 **必须保留消息同步任务 (inotify)** ，以确保fusion不会遗漏该agent所在服务器上的文件IO事件。

## 3. Fusion 状态控制：双名单机制

### 3.1 可疑名单 (Suspect List) —— 解决“正在写入”
*   **来源**：`source_type: "snapshot"` 推送且满足 `Estimated_Storage_Now - mtime < 10min`。
*   **淘汰**：10 分钟静默期后自动移除，或收到该路径的实时 `message` 消息后立即移除。

### 3.2 黑洞名单 (Black-hole List) —— 解决“漏掉增删”
*   **来源**：**仅限**定时全量快照同步 (Timed Snapshot Sync) 任务发现的结构性不一致。
*   **判定**：物理端已删除/新增，但内存树未感知。
*   **报告存证**：黑洞记录持续作为感知失效报告。新一轮定时同步开始时清空。

## 4. 监测机制

### 4.1 权威哨兵巡检 (Sentinel Sweep)
*   **频率**：2 分钟/次。
*   **职责**：仅更新可疑名单中文件的 $mtime$。

### 4.2 定时全量快照同步 (Timed Snapshot Sync)
*   **职责**：全量对齐并向黑洞名单填充结构性差异。

## 5. 技术细节：基于偏移量的逻辑时钟 (Time Offset)
为了对抗分布式时钟漂移并确保判定连续性，Fusion 维护一个时间偏移量：
*   **计算**：从实时消息流（Message Sync）中提取 $mtime$，计算 `Offset = mtime - Fusion_Server_Time`。
*   **映射**：`Estimated_Storage_Now = Fusion_Current_Time + Offset`。
*   **优势**：即使在消息静默期，随着 Fusion 服务器时间的流逝，逻辑时钟也会自动推进。

## 6. API 反馈
*   **全局级**：`Black-hole List` 不为空时，API 返回 `agent_missing: true`。
*   **文件级**：`Suspect List` 中的文件标记为 `integrity_suspect: true`。

## 7. 结论
本方案通过“逻辑时钟偏移量”和“温备 Follower”的设计，不仅解决了时钟对齐和感知一致性问题，还大幅缩短了 Leader 切换时的系统恢复时间，在复杂环境下具备极高的工程可靠性。