# Fustor FS View Driver

文件系统视图驱动程序，为 Fustor Fusion 提供基于路径的树形结构索引和一致性判定逻辑。

## 核心特性
- **增量同步 (Incremental Sync)**: 支持全量快照、实时 inotify 事件以及定时审计的智能合并。
- **一致性保护 (Consistency Protection)**: 
  - **Tombstone List**: 记录被实时删除的文件，防止落后的快照/审计消息使其“复活”。
  - **Suspect List**: 标记正在写入或变更中的热点文件，待其冷却后正式转正。
  - **Blind-spot List**: 标记在无 Agent 节点处发生的文件变更。
- **高性能查询**: 针对千万级节点优化的内存哈希索引，支持 $O(1)$ 路径寻址。

## API 端点
当在 Fusion 中以此驱动作为 View 时，暴露的端点包括：
- `GET /tree`: 获取目录采样树
- `GET /stats`: 获取文件统计
- `GET /blind-spots`: 获取盲区摘要
- `GET /suspect-list`: 获取待巡检可疑名单
- `PUT /suspect-list`: 提交巡检结果 (Sentinel Sweep)

## 配置参数
在 `fusion-config.yaml` 中使用该驱动时的可选配置：
```yaml
views:
  my-fs:
    driver: "fs"
    driver_params:
      hot_item_threshold: 30  # 文件被视为“热”的时间阈值（秒）
```
