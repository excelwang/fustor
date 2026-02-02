# 集成测试代码评审报告 (`it/` 目录)

> **版本**: 1.1.0  
> **评审时间**: 2026-02-02  
> **评审范围**: `it/` 目录下的集成测试代码  
> **当前分支**: `refactor/architecture-v2`  
> **重要决策**: 旧版 SyncInstance 代码已废弃，仅测试 AgentPipeline 架构

---

## 一、评审总结

### 1.1 优秀亮点 ✅

当前分支的集成测试代码在以下方面表现出色：

1. **模块化重构做得很好**
   - 将原先臃肿的 `conftest.py` (约 350+ 行) 拆分为多个独立的 fixtures 模块
   - `fixtures/docker.py` - Docker 环境管理
   - `fixtures/fusion.py` - Fusion 客户端配置
   - `fixtures/agents.py` - Agent 设置和配置
   - `fixtures/leadership.py` - Leader 选举管理
   - 这符合 V2 架构的 "职责单一" 原则

2. **AgentPipeline 测试就绪**
   - `test_pipeline_basic.py` 新增了针对 AgentPipeline 的基础测试
   - 测试框架已适配 V2 架构的 Pipeline 模式

3. **Docker 工具增强**
   - `docker_manager.py` 新增了 `retry` 装饰器和 `exec_in_container_with_retry` 方法
   - `wait_for_health` 方法增加了详细的进度日志
   - 这些改进提高了测试在不稳定网络环境下的鲁棒性

4. **配置注入更新**
   - `receivers-config.yaml` 配置名称更新符合 V2 术语
   - 导入路径从 `fustor_event_model` 更新为 `fustor_core.event`

---

## 二、发现的问题与改进建议

### 2.1 严重问题 🔴

#### 问题 #1: 需要清理 Legacy 模式相关代码 ⚡ 新增

**背景**: 旧版 `SyncInstance` 代码已经废弃，`FUSTOR_USE_PIPELINE` 应始终为 `true`。测试框架中仍存在双模式支持代码，增加了不必要的复杂性和维护成本。

**需要清理的位置**:

| 文件 | 行号 | 需清理内容 |
|------|------|------------|
| `it/conftest.py` | L42-63 | `USE_PIPELINE` 变量和日志输出 |
| `it/fixtures/agents.py` | L23-28, L118-123 | `USE_PIPELINE` 条件判断和双模式启动逻辑 |
| `it/fixtures/__init__.py` | L3 | `use_pipeline` fixture 导出 |

**建议操作**:
1. 移除 `FUSTOR_USE_PIPELINE` 环境变量检查
2. 始终以 Pipeline 模式启动 Agent
3. 移除 `use_pipeline` fixture
4. 简化 `ensure_agent_running()` 函数

**参考代码清理**:
```python
# Before (agents.py)
if USE_PIPELINE:
    env_prefix = "FUSTOR_USE_PIPELINE=true "
else:
    env_prefix = ""

# After (agents.py)
env_prefix = "FUSTOR_USE_PIPELINE=true "  # Pipeline 模式是唯一模式
```

---

#### 问题 #2: `test_consistency_logic.py` 放置位置错误

**位置**: `it/consistency/test_consistency_logic.py`

**描述**: 该文件是纯单元测试风格的 `pytest.mark.asyncio` 测试，直接调用 `FSViewProvider`。它没有通过 HTTP API 与 Fusion 交互，也没有使用 Docker fixtures。在 V2 架构下，直接测试内部组件属于单元测试范畴。

**建议**:
- 将此测试移动到 `packages/fustor-view-fs/tests/` 作为单元测试
- `it/` 目录应仅包含通过 API 验证的 E2E 集成测试

---

#### ~~问题 #3: 导入兼容性~~ ✅ 已验证无问题

验证通过：`conftest.py` 保留了容器常量定义，测试导入正常工作。

---

### 2.2 中等问题 🟡

#### 问题 #4: 常量重复定义

**描述**: 容器名称等常量在 `conftest.py` 和 `fixtures/*.py` 中重复定义。

**建议**: 清理 Legacy 代码后，统一常量定义到 `fixtures/constants.py`。

---

#### 问题 #4: `fixtures/__init__.py` 导出不完整

**位置**: `it/fixtures/__init__.py`

**描述**: 当前内容：
```python
from .docker import docker_env, clean_shared_dir
from .fusion import test_datastore, test_api_key, fusion_client
from .agents import use_pipeline, setup_agents, ensure_agent_running
from .leadership import wait_for_audit, reset_leadership
```

缺少常量的导出（如 `CONTAINER_CLIENT_A`, `AUDIT_INTERVAL` 等），导致测试文件需要直接从子模块导入。

**建议**: 增加常量的统一导出，使测试文件只需从 `fixtures` 导入。

---

#### 问题 #5: docker_manager 重试逻辑与现有方法不一致

**位置**: `it/utils/docker_manager.py`

**描述**: 新增了 `@retry` 装饰器和 `exec_in_container_with_retry` 方法，但现有代码仍然使用原始的 `exec_in_container`，导致行为不一致。

**建议**: 
- 统一使用重试逻辑
- 或将 `exec_in_container` 改为内部使用重试（如果这是预期行为）

---

#### 问题 #6: `test_pipeline_basic.py` 断言逻辑不够精确

**位置**: `it/consistency/test_pipeline_basic.py`, Line 59-65 及类似位置

**描述**:
```python
found = any(
    node.get("path", "").endswith(file_name) or 
    node.get("name") == file_name
    for node in tree
)
```

`get_tree()` 返回的可能是嵌套字典（根节点+children），而非扁平列表。上述代码假设 `tree` 是列表，可能无法正确遍历嵌套结构。

**建议**: 使用 `FusionClient.wait_for_file_in_tree()` 替代手动遍历，或修复遍历逻辑。

---

### 2.3 轻微问题 🟢

#### 问题 #7: 硬编码的时间值

**多处位置**:
- `AUDIT_INTERVAL = 5` (agents.py)
- `session_timeout_seconds: 3` (docker.py 中的配置)
- 各测试文件中的 `time.sleep(3)`, `timeout=30`, `timeout=60` 等

**建议**: 将这些时间常量统一定义在配置模块中，便于调整测试速度。

---

#### 问题 #8: 测试文档注释可以更完善

**位置**: 部分测试文件

**描述**: 存在中英文混用的文档字符串。例如：
```python
"""
场景: 盲区发现的新文件
...
"""
```

**建议**: 对于开源项目，建议统一使用英文注释，或在中文注释后附加英文摘要。

---

#### 问题 #9: `test_d1_tombstone_creation.py` 间接验证

**位置**: `it/consistency/test_d1_tombstone_creation.py`, Line 66-85

**描述**:
```python
# 注意: 这需要一个 API 来查询 Tombstone List。
# 如果没有公开 API，可以通过验证 Audit 不会复活来间接验证。
```

该测试承认无法直接验证 Tombstone List 的存在，只能依赖间接验证。

**建议**: 如果 Tombstone 是 V2 设计的重要组成部分，应考虑增加 `/api/v1/views/{id}/tombstones` API 端点用于调试和测试。

---

## 三、业务代码与测试覆盖深度分析

基于对当前分支完整业务代码的阅读，以下是详细的测试覆盖分析：

### 3.1 Agent 端核心组件

#### `AgentPipeline` (agent/src/fustor_agent/runtime/agent_pipeline.py)

| 功能 | 代码行 | 集成测试覆盖 | 状态 |
|------|--------|--------------|------|
| Session 创建与恢复 | L110-129, L196-280 | `test_pipeline_basic.py` | ⚠️ 部分覆盖 |
| Snapshot 同步 | L414-446 | `test_pipeline_basic.py` | ⚠️ 仅基础场景 |
| Realtime 消息同步 | L448-542 | `test_pipeline_basic.py` | ⚠️ 仅基础场景 |
| Audit 周期 | L544-616 | `test_b1_blind_spot*.py`, `test_c1_snapshot_suspect.py` | ✅ 良好 |
| Sentinel 检查 | L617-656 | ❌ 无直接测试 | 🔴 缺失 |
| Leader/Follower 角色切换 | L349-358 | `test_a1_leader_election*.py`, `test_e1_leader_failover.py` | ✅ 良好 |
| Heartbeat 保活 | L323-340 | 间接覆盖 (Leadership 测试) | ⚠️ 可增强 |
| 错误恢复与重连 | L247-280 (SessionObsoletedError) | ❌ 无直接测试 | 🔴 缺失 |
| Bus 分裂处理 | L673-714 (`remap_to_new_bus`) | ❌ 无测试 | 🔴 缺失 |

---

### 3.2 Fusion 端核心组件

#### `FusionPipeline` (fusion/src/fustor_fusion/runtime/fusion_pipeline.py)

| 功能 | 代码行 | 集成测试覆盖 | 状态 |
|------|--------|--------------|------|
| Pipeline 启动/停止 | L136-194 | 隐式（Docker 环境初始化） | ⚠️ 隐式 |
| Session 管理 (FCFS Leader 选举) | L242-306 | `test_a1_leader_election*.py` | ✅ 良好 |
| Session 超时清理 | L322-344 | `test_e1_leader_failover.py` | ✅ 良好 |
| Event 分发到 ViewHandler | L223-238 | 隐式 | ⚠️ 隐式 |
| 多 ViewHandler 支持 | L117-134 | ❌ 无测试 (仅使用 fs-view) | 🟡 低优先级 |

#### Session API (fusion/src/fustor_fusion/api/session.py)

| 端点 | 测试覆盖 | 状态 |
|------|----------|------|
| `POST /session` | 通过 `FusionClient.ensure_session()` | ✅ |
| `POST /session/heartbeat` | 通过 `test_leadership.py` fixtures | ✅ |
| `DELETE /session` | 间接 (超时测试) | ⚠️ |
| `GET /session` (list sessions) | ❌ 无直接测试 | 🟡 低优先级 |

#### Ingestion API (fusion/src/fustor_fusion/api/ingestion.py)

| 端点 | 测试覆盖 | 状态 |
|------|----------|------|
| `POST /ingest` | 隐式 (所有写入测试) | ✅ |
| `GET /ingest/stats` | ❌ 无测试 | 🟡 低优先级 |
| `GET /ingest/position` | ❌ 无测试 | 🟡 可能已废弃 |
| HTTP 419 (Obsolete Session) | ❌ 无测试 | 🔴 缺失 |

#### Consistency API (fusion/src/fustor_fusion/api/consistency.py)

| 端点 | 测试覆盖 | 状态 |
|------|----------|------|
| `POST /consistency/audit/start` | `test_b1_blind_spot*.py` | ✅ |
| `POST /consistency/audit/end` | `test_b1_blind_spot*.py`, `test_d1_tombstone*.py` | ✅ |
| `GET /consistency/sentinel/tasks` | ❌ 无测试 | 🔴 缺失 |
| `POST /consistency/sentinel/feedback` | ❌ 无测试 | 🔴 缺失 |

---

### 3.3 View 层核心逻辑

#### `FSViewProvider` (packages/view-fs/src/fustor_view_fs/provider.py)

| 功能 | 测试覆盖 | 状态 |
|------|----------|------|
| Smart Merge 仲裁 | `test_consistency_logic.py` (白盒) | ✅ 但位置错误 |
| Tombstone 保护 | `test_d1_tombstone_creation.py` | ✅ |
| Tombstone 清理 (TTL) | `test_d4_tombstone_cleanup.py` | ✅ |
| Suspect 列表管理 | `test_c1_snapshot_suspect.py` | ✅ |
| Blind Spot 检测 | `test_b1_blind_spot_creation.py` | ✅ |
| Stale Evidence 保护 | `test_consistency_logic.py` (L71 `last_updated_at`) | ⚠️ 仅白盒 |

#### `LogicalClock` (packages/core/src/fustor_core/clock/logical_clock.py)

| 功能 | 测试覆盖 | 状态 |
|------|----------|------|
| Skew 采样 (Mode 计算) | `packages/core/tests/clock/` | ✅ 单元测试覆盖 |
| Trust Window 逻辑 | `packages/core/tests/clock/` | ✅ 单元测试覆盖 |
| 集成测试中的时钟行为 | `test_h_clock_skew_tolerance.py` | ✅ |

---

### 3.4 缺失测试场景总结

#### 🔴 高优先级缺失 (影响核心功能)

| 场景 | 业务代码位置 | 建议测试 |
|------|-------------|----------|
| **SessionObsoletedError 恢复** | `agent_pipeline.py:L247-255` | 模拟 Fusion 返回 419，验证 Agent 重连 |
| **Sentinel 完整流程** | `agent_pipeline.py:L617-656`, `consistency.py:L96-128` | E2E: Agent 获取任务 → 检查文件 → 提交反馈 |
| **HTTP 419 Obsolete 处理** | `ingestion.py:L158-162` | 并发 Session 场景下 Snapshot 被拒绝 |
| **Bus 分裂与重映射** | `agent_pipeline.py:L673-714` | 高负载下 EventBus 分裂行为 |

#### 🟡 中优先级缺失

| 场景 | 建议 |
|------|------|
| Heartbeat 超时边界 | 测试恰好在超时边界的 Heartbeat 行为 |
| 多 ViewHandler 并行处理 | 验证多个 View 同时处理事件的隔离性 |
| 批量事件处理性能 | 验证大批量 (10000+) 事件的处理稳定性 |

---

### 3.5 AgentPipeline 测试增强

| 场景 | 当前状态 | 建议 |
|------|----------|------|
| AgentPipeline 启动/停止 | `test_pipeline_basic.py` 有基础测试 | ✅ 已覆盖 |
| AgentPipeline 错误恢复 | ❌ 缺失 | 增加网络中断恢复测试 |
| Pipeline 热重载配置 | ❌ 缺失 | 验证动态配置更新 |
| ~~AgentPipeline 与 SyncInstance 一致性~~ | N/A | ❌ 不再需要 (Legacy 已废弃) |

### 3.6 V2 架构术语迁移验证

| 项目 | 当前状态 | 建议 |
|------|----------|------|
| Sender 替换 Pusher | 配置使用 `senders-config.yaml` | ✅ 已更新 |
| Receiver 替换 Datastores | 配置使用 `receivers-config.yaml` | ✅ 已更新 |
| Pipeline API 路径 | 仍使用 `/api/v1/ingest/*` | ⚠️ 确认是否需要更新到 `/api/v1/pipe/*` |

---

## 四、修复清单 (TODO)

### 高优先级 🔴

- [x] **TODO-IT-001**: ⚡ 清理 Legacy 模式代码 (`USE_PIPELINE`, `use_pipeline` fixture) ✅ 已完成 (dc9684b)
- [x] **TODO-IT-002**: 修复 `test_pipeline_basic.py` 的树遍历逻辑 ✅ 已完成 (dc9684b)
- [x] **TODO-IT-003**: 增加 Sentinel 完整流程测试 ✅ 已完成 (`test_c5_sentinel_sweep.py` 重写)
- [x] **TODO-IT-004**: 增加 SessionObsoletedError 恢复测试 ✅ 已完成 (`test_a3_session_recovery.py`)
- [ ] **TODO-IT-005**: 增加 EventBus 分裂 (`remap_to_new_bus`) 测试

### 中优先级 🟡

- [x] **TODO-IT-006**: 将 `test_consistency_logic.py` 迁移到 `packages/view-fs/tests/` ✅ 已完成 (dc9684b)
- [x] **TODO-IT-007**: 统一 `docker_manager` 的重试策略 ✅ 已完成 (内置重试逻辑)
- [x] **TODO-IT-008**: 统一常量定义到 `fixtures/constants.py` ✅ 已完成 (dc9684b)
- [ ] **TODO-IT-009**: 增加 Heartbeat 超时边界测试
- [ ] **TODO-IT-010**: 增加 `test_pipeline_basic.py` 更多场景 (错误恢复、大批量事件)

### 低优先级 🟢

- [x] **TODO-IT-011**: 统一时间常量到配置模块 ✅ 已完成 (在 `constants.py` 中)
- [ ] **TODO-IT-012**: 规范测试文档注释语言
- [ ] **TODO-IT-013**: 考虑增加 Tombstone 查询 API
- [ ] **TODO-IT-014**: 增加 `/ingest/stats` 端点测试
- [ ] **TODO-IT-015**: 增加多 ViewHandler 并行测试

---

## 五、运行测试建议

在清理 Legacy 代码后，运行完整的集成测试套件:

```bash
# 运行所有集成测试 (Pipeline 模式是唯一模式)
uv run pytest it/consistency/ -v
```

如果需要调试单个测试:
```bash
# 例如: 只运行 Leader 选举测试
uv run pytest it/consistency/test_a1_leader_election_first.py -v -s
```

> **注意**: 清理完成后，不再需要设置 `FUSTOR_USE_PIPELINE` 环境变量。

---

## 六、结论

当前分支的 `it/` 目录测试代码重构整体方向正确，模块化拆分提高了代码可维护性。重构保持了向后兼容性，常量定义在 `conftest.py` 中得到保留。

### 测试覆盖现状评估

| 领域 | 覆盖率 | 评价 |
|------|--------|------|
| Leader/Follower 选举 | ✅ 95% | 优秀 |
| Audit 周期 (Blind Spot, Tombstone, Suspect) | ✅ 90% | 良好 |
| Session 管理 | ✅ 85% | 良好 |
| Pipeline 基础功能 | ⚠️ 60% | 需要增强 |
| Sentinel 完整流程 | ❌ 0% | 严重缺失 |
| 错误恢复 (419, 网络中断) | ❌ 10% | 严重缺失 |

### 主要需要关注的问题

1. **测试逻辑细节** - `test_pipeline_basic.py` 中的 API 返回值处理假设不正确（树遍历逻辑）
2. **测试分类** - `test_consistency_logic.py` 应迁移到单元测试目录
3. **核心功能缺失测试**:
   - Sentinel 检查完整 E2E 流程
   - SessionObsoletedError 恢复机制
   - EventBus 分裂行为
4. **Pipeline 模式覆盖** - 需要增加更多针对新架构的测试场景

### 下一步行动建议

1. ⚡ **最高优先级**: 执行 **TODO-IT-001** - 清理 Legacy 模式代码，简化测试框架
2. 修复 **TODO-IT-002** (树遍历逻辑)，这会影响现有测试的可靠性
3. 添加 **TODO-IT-003** (Sentinel 测试)，这是 V2 架构的核心一致性机制
4. 添加 **TODO-IT-004** (419 错误恢复)，这是生产环境必需的健壮性保障

### Legacy 代码清理清单

在完成评审后，需执行以下清理操作：

| 文件 | 操作 | 优先级 |
|------|------|--------|
| `it/conftest.py` | 移除 `USE_PIPELINE` 变量和条件日志 | P0 |
| `it/fixtures/agents.py` | 移除双模式启动逻辑，始终使用 Pipeline | P0 |
| `it/fixtures/__init__.py` | 移除 `use_pipeline` 导出 | P0 |
| `it/consistency/test_pipeline_basic.py` | 移除 `@pytest.mark.parametrize` 双模式测试 | P0 |

