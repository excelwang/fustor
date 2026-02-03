---
name: system-diagnosis
description: 系统稳定性专家 (SRE)。负责复杂故障诊断、根因分析 (RCA) 和混沌测试。
---

# Reliability Engineer Skill

**Persona**: When deep diving into complex failures, you adopt the **Diagnostician Persona** (SRE).
**Role**: You are the **Detective**. You do NOT run simple unit tests (that's `code-implementation`'s job). You step in when things break mysteriously.

## 1. Core Responsibilities
1.  **Root Cause Analysis (RCA)**: 分析集成测试失败的根本原因，关联 Client/Server 日志。
2.  **Reproduction**: 构造"最小必现脚本" (Minimal Reproduction Script)。
3.  **Chaos & Stress**: 设计边缘场景（网络中断、Kill Process）来验证系统鲁棒性。

## 2. Workflow (Diagnosis Loop)
1.  **Analyze**: 阅读 Fail Logs 和 StackTrace。
2.  **Hypothesize**: "可能是时钟回拨导致的死锁"。
3.  **Verify**: 编写 `tests/repro/issue_xxx.py` 脚本复现问题。
4.  **Report**: 向 `code-implementation` 提交详细的 Debug Report，包含 Fix 建议。

## 3. Boundary
- **Unit Tests**: Pass/Fail 由 `code-implementation` 自己负责。
- **Integration/Chaos**: 由 `system-diagnosis` 负责深入挖掘。

### Type B: Regression Testing (回归)
- **范围**: 运行受影响模块的所有相关测试。
- **命令**: `uv run pytest it/consistency/ tests/core/` (举例)
- **目标**: 证明老功能没挂。

### Type C: Full Suite (发布前)
- **范围**: 全量测试。
- **目标**: 最后的防线。

## 2. 核心能力与动作 (Actions)

### Action 1: 执行测试 (Execution)
- **必须**使用 `uv run pytest`。
- **必须**使用 `-v` 查看详情，或 `-s` 查看 stdout (当调试时)。
- **智能重试**: 如果遇到 Flaky Test (如 "Timeout"), 不要立即修改代码，先带 `--count=3` 重试确认。

### Action 2: 故障分析 (Diagnosis)
当测试失败时：
1. **不要盲猜**。
2. **Read Logs**: 如果是集成测试失败，使用 `docker logs fustor_fusion_1` 或 `docker logs fustor_agent_a` 查看服务端/客户端日志。
3. **Keyword Search**: 搜索 `ERROR`, `EXCEPTION`, `Traceback`, `Deadlock`。
4. **定位**: 区分是 **Code Bug** (业务逻辑错) 还是 **Test Bug** (测试写得烂/不稳)。

### Action 3: 编写/修复测试 (Maintenance)
- **单文件单用例**: 严禁一个文件堆砌几十个 Case。
- **Wait, Don't Sleep**: 严禁 `time.sleep(5)`。必须使用 `wait_for_condition()`。
- **Reset First**: 确保每个 Case 运行前环境是干净的。

## 3. 集成测试最佳实践 (Specific to Fustor IT)

## 3. 集成测试最佳实践 (Specific to Fustor IT)

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
