# Fustor TODO List

## Agent
- [ ] Snapshot sync phase: Exclude files that are currently being written to (active writes) from being pushed to Fusion.

## source fs
- [ ] 快照同步阶段增加并发扫描文件功能
- [ ] Pre-scan 并行扫描优化：多线程并行扫描不同子目录，加速 LRU 热点目录列表构建
  - 思路：将顶级目录分配给多个线程，每个线程独立扫描子树并收集 mtime
  - 实现：使用 `ThreadPoolExecutor`，线程数 = CPU 核心数
  - 预期效果：Pre-scan 时间降低 N 倍（N = 线程数）
