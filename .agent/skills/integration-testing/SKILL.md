---
name: integration-testing-skill
description: Fustor 集成测试最佳实践：确保测试的高可靠性、可预测性和高效执行。
---

# Integration Testing Best Practices

遵循以下最佳实践，以确保 Fustor 集成测试在 NFS 多端挂载一致性环境下的稳定性和效率。

## 1. 测试设计原则

- **单文件单用例**: 严禁在一个文件中编写多个逻辑测试用例。保持文件小巧且职责单一，便于并行运行和故障定位。
- **状态零残留 (Reset First)**: 每个测试开始前必须确保环境处于干净状态。依赖 `reset_fusion_state` 固件自动清理 Agent 进程、状态文件和 Fusion Session。
- **异步驱动 (Wait, Don't Sleep)**: 禁止使用硬编码的 `time.sleep()`。必须使用框架提供的等待机制（如 `fusion_client.wait_for_view_ready`, `wait_for_audit`），并设置合理的 `timeout`。

## 2. 编写最佳实践

- **利用容器工具**: 优先使用 `docker_manager.exec_in_container` 在特定客户端执行命令，模拟真实的物理分发。
- **验证异步一致性**: 对于 Audit 发现或 Realtime 同步，验证前必须先触发或等待一个完整的周期（如 `wait_for_audit(1)`）。
- **包含场景描述**: 测试文件顶部必须有 `docstring`，清晰描述：
    1. 测试场景（Scenario）
    2. 预期行为（Expected Behavior）
    3. 验证锚点（Verification Points）

## 3. 环境与执行最佳实践

- **信任自动复用**: 默认直接运行 `uv run pytest`。系统会自动比对代码哈希，仅在必要时（依赖变化或配置修改）才执行耗时的 `build`。
- **热代码调试**: 修改 `src` 目录下的 Python 代码后，只需直接重新运行测试。代码会通过挂载卷自动生效，无需手动重启容器。
- **彻底清理**: 仅在遇到难以解释的权限错误或缓存污染时，执行 `docker-compose down -v` 并删除 `it/.env_state`。

## 4. 调试最佳实践

- **先查调用链，后查逻辑**: 预期行为未发生时，先通过 `docker logs` 确认对应组件（Fusion/Agent）是否收到了请求或事件。
- **善用 API 探测**: 利用 `fusion_client.get_sessions()` 实时观察 Agent 的角色转换（Leader/Follower）和就绪状态（`is_realtime_ready`）。
- **检查时钟偏差**: 在涉及 `mtime` 或 `tombstone` 的测试中，注意 Agent A/B 有故意设置的 FakeTime 偏差（A:+2h, B:-1h），验证逻辑时钟是否正确处理了这些偏差。

## 5. 重构验收指南

重构核心模块后，必须按以下顺序跑通集成测试：
1. **A组 (选举)**: 确保 Leader 选举和角色分配正常。
2. **B组 (发现)**: 确保 Audit 能正确扫描 NFS 盲区变动。
3. **C/D组 (一致性)**: 检查复杂冲突和墓碑清理逻辑。
