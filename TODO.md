# Fustor TODO List

## Agent
- [x] Snapshot sync phase: postpone files that are currently being written to (active writes) from being pushed to Fusion.

## source fs
- [x] Pre-scan、快照同步、审计、哨兵都进行并行扫描优化：多线程并行扫描不同子目录。
- [x] 核对各模块的逻辑时钟。
- [x] **审计通讯性能优化 (Performance & Efficiency)**:
  - [x] **实现“真正的静默” (True Silence)**: 在 Agent 的 Audit 模式下，如果目录的 `mtime` 与本地 `mtime_cache` 匹配，则不仅跳过其子文件扫描，还应**停止发送该目录节点本身**。假设只要在 `mtime_cache` 中存在，Fusion 就已经拥有该目录的元数据。这将极大地减少空闲期间的长尾流量。
  - [x] **增量式 `mtime_cache` 更新**: 重构 Agent 的缓存更新逻辑。废弃当前的“全量准备-整体提交”两阶段方案，改为**细粒度的增量更新**。只要某个目录及其直属子文件扫描完成并成功推送到 Fusion，就立即更新该目录的 `mtime_cache`。这不仅降低了内存峰值，还能在扫描意外中断时最大化地保留已完成工作的进度。
  - [ ] **通讯协议升级 (gRPC/Protobuf)**: 将 JSON Over HTTP 替换为二进制流式协议（如 gRPC），以支持流式推送、并发 Multiplexing，并降低路径名等重复字符串的序列化开销。
  - [x] **Fusion 侧细粒度并发**: 在 Fusion 的 `DirectoryStructureParser` 中用**路径分段锁**替换当前的全局大锁，提升多 Agent 同时推送时的裁决并发性能。

- [x] benchmark 增加 预扫描、快照同步、审计、哨兵的性能测试，（agent、fusion分别测耗时）。
- [x] 集成测试，将agent、fusion的容器的物理时间打乱，看是否能正确处理。
- [x] 重构 `fustor-view-fs` 逻辑：使用组合而非继承（`FSState`, `TreeManager`, `FSArbitrator`, `AuditManager`, `FSViewQuery`）。

## View & Consistency
- [ ] **解耦 NFS 特定逻辑**: 将 `audit_skipped`（基于 NFS 的 `mtime` 传播特性）等 NFS 特有状态从通用 `fustor-view-fs` 逻辑中解耦，通过驱动扩展或配置项实现，提升 View 驱动的通用性。


## YAML 格式
- [x] 各种id需要能直接用于url而无需encode。例如：`fustor-agent-01`、`fustor-view-01`。