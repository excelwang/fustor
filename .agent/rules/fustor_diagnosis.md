# Fustor System Diagnosis (Project-Specific Rules)

These rules complement the generic `system-diagnosis` skill for the Fustor project.

## 1. 集成测试最佳实践 (Fustor IT)

- **容器交互**: 使用 `docker_manager.exec_in_container` 模拟真实 NFS 操作。
- **一致性验证**: 再验证 Audit 或 Sync 结果前，必须等待至少一个周期 (`wait_for_audit`)。
- **时钟偏差**: 注意测试环境中 Agent A/B 故意设置的时间偏差 (+2h/-1h)，不要被假时间欺骗。
- **角色转换监控**: 善用 `fusion_client.get_workstreams()` 实时观察 Agent 的角色转换（Leader/Follower）和 `is_realtime_ready` 状态。
- **环境复用与清理**:
  - **Trust Reuse**: 默认直接运行测试，系统会自动复用容器。仅在依赖变化时 rebuild。
  - **Hot Reload**: 修改 `src` 代码后可直接重跑测试，挂载卷会自动生效，无需重启容器。
  - **Clean State**: 仅在严重环境污染（如权限错误）时才执行 `docker-compose down -v`。
- **验收顺序 (Refactoring Gate)**:
  1. **A组 (Leader Election)**: 确保选举正常。
  2. **B组 (Blind Spot)**: 确保 NFS 盲区变动能被 Audit 扫到。
  3. **C/D组 (Consistency)**: 检查复杂冲突和 Tombstone 清理。

## 2. 日志分析指南

当集成测试失败时，优先检查以下容器日志：
- `docker logs fustor_fusion_1`
- `docker logs fustor_agent_a`
- `docker logs fustor_agent_b`

搜索关键字: `ERROR`, `EXCEPTION`, `Traceback`, `Deadlock`, `Watermark Regression`.
