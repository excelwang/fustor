# Fustor TODO List

## Agent
- [ ] Snapshot sync phase: postpone files that are currently being written to (active writes) from being pushed to Fusion.

## source fs
- [ ] 快照同步阶段增加并发扫描文件功能
- [ ] Pre-scan 并行扫描优化：多线程并行扫描不同子目录，加速 LRU 热点目录列表构建
  - 思路：将顶级目录分配给多个线程，每个线程独立扫描子树并收集 mtime
  - 实现：使用 `ThreadPoolExecutor`，线程数 = CPU 核心数
  - 预期效果：Pre-scan 时间降低 N 倍（N = 线程数）
- 核对、对其全局各模块的逻辑时钟。
- [ ] **审计通讯性能优化 (Performance & Efficiency)**:
  - **实现“真正的静默” (True Silence)**: 在 Agent 的 Audit 模式下，如果目录的 `mtime` 与本地 `mtime_cache` 匹配，则不仅跳过其子文件扫描，还应**停止发送该目录节点本身**。假设只要在 `mtime_cache` 中存在，Fusion 就已经拥有该目录的元数据。这将极大地减少空闲期间的长尾流量。
  - **增量式 `mtime_cache` 更新**: 重构 Agent 的缓存更新逻辑。废弃当前的“全量准备-整体提交”两阶段方案，改为**细粒度的增量更新**。只要某个目录及其直属子文件扫描完成并成功推送到 Fusion，就立即更新该目录的 `mtime_cache`。这不仅降低了内存峰值，还能在扫描意外中断时最大化地保留已完成工作的进度。
  - **通讯协议升级 (gRPC/Protobuf)**: 将 JSON Over HTTP 替换为二进制流式协议（如 gRPC），以支持流式推送、并发 Multiplexing，并降低路径名等重复字符串的序列化开销。
  - **Fusion 侧细粒度并发**: 在 Fusion 的 `DirectoryStructureParser` 中用**路径分段锁**替换当前的全局大锁，提升多 Agent 同时推送时的裁决并发性能。