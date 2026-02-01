# 集成测试重构分析

## 1. 现有架构

### 1.1 目录结构
```
it/
├── conftest.py              # 核心 fixtures (424 行)
├── docker-compose.yml       # Docker 环境配置
├── README.md
├── utils/
│   ├── docker_manager.py    # Docker 操作封装
│   └── fusion_client.py     # Fusion HTTP API 客户端
└── consistency/             # 一致性测试用例 (24 个)
    ├── test_a1_*.py         # Leader 选举测试
    ├── test_b*_*.py         # 盲区发现测试
    ├── test_c*_*.py         # 可疑文件测试
    ├── test_d*_*.py         # 墓碑防复活测试
    └── test_e*_*.py         # 故障转移测试
```

### 1.2 环境架构
```
┌────────────────────────────────────────────────────────────┐
│              Docker Compose Environment                     │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  ┌──────────────┐     ┌──────────────┐                    │
│  │   Fusion     │     │  NFS Server  │                    │
│  │   :18102     │     │   /exports   │                    │
│  └──────────────┘     └──────────────┘                    │
│         │                    │                             │
│         └────────┬───────────┘                             │
│                  │ NFS Mount (actimeo=1)                   │
│  ┌───────────────┼────────────────────────────────┐       │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐       │       │
│  │  │Client A  │ │Client B  │ │Client C  │       │       │
│  │  │(Agent A) │ │(Agent B) │ │(No Agent)│       │       │
│  │  │Leader    │ │Follower  │ │Blind-spot│       │       │
│  │  └──────────┘ └──────────┘ └──────────┘       │       │
│  └────────────────────────────────────────────────┘       │
└────────────────────────────────────────────────────────────┘
```

### 1.3 当前问题

| 问题 | 描述 | 影响 |
|------|------|------|
| **配置硬编码** | `datastores-config.yaml` 在 conftest.py 中硬编码 | 与新架构不兼容 |
| **术语过时** | 使用 `pusher` 而非 `sender` | 与主代码术语不一致 |
| **缺乏 Pipeline 测试** | 无法测试 `AgentPipeline` 模式 | 新架构无法验证 |
| **fixtures 过于集中** | conftest.py 424 行，职责过多 | 维护困难 |
| **环境重用脆弱** | 依赖容器健康检查 | 偶发失败 |

---

## 2. 重构目标

### 2.1 近期目标 (Phase 1)
1. **配置迁移**: 支持 `receivers-config.yaml` (去除 `datastores-config.yaml`)
2. **术语更新**: `pushers-config.yaml` → `senders-config.yaml`
3. **Pipeline 测试**: 添加 `FUSTOR_USE_PIPELINE` 测试模式

### 2.2 中期目标 (Phase 2)
1. **Fixtures 拆分**: 抽取可复用的 fixture 模块
2. **改进健壮性**: 增强容器健康检查和错误恢复
3. **测试覆盖**: 添加 Pipeline 特定的集成测试

### 2.3 长期目标 (Phase 3)
1. **CI/CD 集成**: 添加 GitHub Actions 集成测试工作流
2. **性能测试**: 添加基于 benchmark 的性能测试
3. **混沌测试**: 添加网络分区、延迟等场景

---

## 3. 重构方案

### 3.1 配置迁移 (High Priority)

**当前 conftest.py (第 71-77 行):**
```python
ds_config = """
integration-test-ds:
  api_key: "test-api-key-123"
  session_timeout_seconds: 3
  allow_concurrent_push: true
"""
docker_manager.create_file_in_container(
    CONTAINER_FUSION, 
    "/root/.fustor/datastores-config.yaml", 
    ds_config
)
```

**重构后:**
```python
# 新架构: receivers-config.yaml
receivers_config = """
http-receiver:
  driver: http
  bind_host: 0.0.0.0
  port: 8102
  session_timeout_seconds: 3
  api_keys:
    - key: "test-api-key-123"
      pipeline_id: "integration-test-ds"
"""
docker_manager.create_file_in_container(
    CONTAINER_FUSION, 
    "/root/.fustor/receivers-config.yaml", 
    receivers_config
)

# 保留旧配置用于过渡期
# (auth/dependencies.py 会优先使用 receivers-config)
```

### 3.2 Agent 配置更新

**当前 (第 189-200 行):**
```python
pushers_config = f"""
fusion:
  driver: "fusion"
  endpoint: "{fusion_endpoint}"
  ...
"""
docker_manager.create_file_in_container(
    container_name, 
    "/root/.fustor/pushers-config.yaml", 
    pushers_config
)
```

**重构后:**
```python
# 新术语: senders-config.yaml
senders_config = f"""
fusion:
  driver: "http"  # 更明确的驱动名称
  endpoint: "{fusion_endpoint}"
  ...
"""
docker_manager.create_file_in_container(
    container_name, 
    "/root/.fustor/senders-config.yaml", 
    senders_config
)

# 同时保留 pushers-config.yaml 用于向后兼容
```

### 3.3 Pipeline 测试模式

**添加新的 fixture:**
```python
@pytest.fixture(scope="session")
def use_pipeline():
    """Enable AgentPipeline mode for testing."""
    return os.getenv("FUSTOR_USE_PIPELINE", "false").lower() == "true"

def ensure_agent_running(container_name, api_key, datastore_id, use_pipeline=False):
    """Ensure agent is running with optional Pipeline mode."""
    # ... 现有代码 ...
    
    # 添加 Pipeline 模式支持
    env_vars = ""
    if use_pipeline:
        env_vars = "FUSTOR_USE_PIPELINE=true "
    
    docker_manager.exec_in_container(
        container_name, 
        ["sh", "-c", f"{env_vars}nohup fustor-agent start -V > /data/agent/console.log 2>&1 &"]
    )
```

### 3.4 Fixtures 模块化

**拆分 conftest.py:**
```
it/
├── conftest.py                    # 主入口，导入子模块
├── fixtures/
│   ├── __init__.py
│   ├── docker.py                  # docker_env, clean_shared_dir
│   ├── fusion.py                  # fusion_client, test_datastore, test_api_key
│   ├── agents.py                  # setup_agents, ensure_agent_running
│   └── leadership.py              # reset_leadership, wait_for_audit
└── ...
```

---

## 4. 实施计划

### Phase 1: 配置兼容性 ✅ (已完成 2026-02-01)

```
任务 1.1: 更新 conftest.py 配置注入
- [x] 添加 receivers-config.yaml 支持
- [x] 保留 datastores-config.yaml 作为 fallback
- [x] 更新 Agent 配置使用 senders-config.yaml

任务 1.2: 添加 Pipeline 测试标志
- [x] 添加 FUSTOR_USE_PIPELINE 环境变量支持
- [x] 修改 ensure_agent_running 函数
- [x] 添加 use_pipeline fixture

任务 1.3: 验证现有测试
- [ ] 运行所有 24 个测试确保无回归
- [ ] 修复任何因配置变更导致的失败
```

### Phase 2: Fixtures 重构 ✅ (2026-02-02 完成)

```
任务 2.1: 模块化 Fixtures ✅
- [x] 创建 fixtures/ 目录结构
- [x] 拆分 conftest.py 到子模块
- [x] 更新 conftest.py 导入

任务 2.2: 增强健壮性 ✅
- [x] 改进容器健康检查超时逻辑
- [x] 添加重试机制 (retry decorator + exec_with_retry)
- [x] 改进日志输出

任务 2.3: 添加 Pipeline 集成测试 ✅
- [x] 创建 test_pipeline_basic.py 测试
- [x] 验证 Pipeline 模式行为
```

### Phase 3: CI/CD 集成 (可选, 2-3 天)

```
任务 3.1: GitHub Actions 工作流
- [ ] 创建 .github/workflows/integration-test.yml
- [ ] 配置 Docker Compose 服务
- [ ] 设置测试矩阵 (Pipeline/Legacy)

任务 3.2: 测试报告
- [ ] 添加 pytest-html 报告
- [ ] 集成 coverage 报告
```

---

## 5. 风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| 配置变更破坏现有测试 | 高 | 保持双配置格式兼容 |
| Docker 环境不稳定 | 中 | 增加重试和健康检查 |
| Pipeline 模式行为差异 | 中 | 分离测试用例，渐进验证 |
| 重构引入新 Bug | 低 | 每步验证测试通过 |

---

## 6. 快速启动命令

```bash
# 运行现有测试 (Legacy 模式)
cd /home/huajin/fustor_monorepo
uv run pytest it/consistency/ -v

# 运行测试 (Pipeline 模式)
FUSTOR_USE_PIPELINE=true uv run pytest it/consistency/ -v

# 启动环境但不运行测试
cd it && docker-compose up -d

# 清理环境
cd it && docker-compose down -v
```

---

## 7. 决策点

1. **是否保持 datastores-config.yaml 兼容性?**
   - 推荐: 是，保持 6 个月过渡期

2. **Pipeline 测试是否与 Legacy 测试分离?**
   - 推荐: 否，通过环境变量控制，共用测试用例

3. **是否拆分 conftest.py?**
   - 推荐: 是，但作为 Phase 2 任务

请确认重构方向，我将开始实施。
