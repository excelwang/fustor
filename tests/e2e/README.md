# Fustor NFS Multi-Mount Consistency Integration Tests

本目录包含 Fustor 在 NFS 多端挂载场景下的一致性集成测试。测试使用真实的 Docker 容器环境模拟 NFS 多客户端场景。

## 架构概览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Docker Compose Environment                        │
2026架构 (Decentralized YAML-driven)                                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│                    ┌─────────────┐   ┌─────────────────────────────────┐│
│                    │   Fusion    │   │         NFS Server              ││
│                    │   :18102    │   │      /exports                   ││
│                    └─────────────┘   └─────────────────────────────────┘│
│                          │                         │                     │
│                          ├─────────────────────────┤                     │
│                          │                         │                     │
│  ┌───────────────────────┼─────────────────────────┼──────────────────┐ │
│  │                     NFS Mount (actimeo=1)                          │ │
│  │                                                                    │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐            │ │
│  │  │ NFS Client A │  │ NFS Client B │  │ NFS Client C │            │ │
│  │  │  (Agent A)   │  │  (Agent B)   │  │  (No Agent)  │            │ │
│  │  │   Leader     │  │   Follower   │  │  Blind-spot  │            │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘            │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## 前置条件

1. **Docker & Docker Compose**: 需要安装 Docker 和 Docker Compose v2
2. **权限**: 需要能够运行 privileged 容器（用于 NFS 挂载）
3. **Python 环境**: Python 3.11+ with pytest

## 运行测试

### 1. 运行命令

```bash
# 进入项目根目录
cd /home/huajin/fustor_monorepo

# 运行所有集成测试
uv run pytest tests/e2e/consistency/ -v

# 运行单个测试文件
uv run pytest tests/e2e/consistency/test_a1_leader_election_first.py -v
```

### 2. 自动环境管理 (重要)

本框架引入了**智能环境复用机制**
- **自动检测**: 每次启动测试时，系统会计算 `docker-compose.yml`、`Dockerfile` 以及所有 `pyproject.toml` 的哈希值。
- **智能复用**: 如果配置和依赖未发生变化，系统将直接复用运行中的容器，仅重启 Fusion 以应用最新配置（启动时间 ~5s）。
- **自动重建**: 如果检测到任何影响环境的变更，系统会自动执行 `docker-compose up --build` 进行冷启动。

## 环境管理与注意事项

### 1. 什么时候需要手动重建？
虽然有自动检测，但在以下情况建议执行彻底清理：
- 修改了 `tests/e2e/containers` 下的 `entrypoint.sh` 或其他构建脚本。
- 遇到了 Docker 卷挂载导致的权限问题。
- 怀疑 Docker 缓存层污染导致逻辑异常。

**彻底清理命令**:
```bash
docker-compose -f tests/e2e/docker-compose.yml down -v
rm tests/e2e/.env_state  # 强制刷新哈希状态
```

### 2. 代码生效机制
- **热生效**: 核心代码（`fustor_core`, `fustor_agent`, `fustor_fusion` 等）已通过 **Volume 挂载**。修改 `src` 目录代码后，测试固件会自动重启进程使代码生效，**无需重启容器**。
- **三方库变更**: 修改了 `pyproject.toml` 中的 `dependencies` 后，系统会自动触发镜像重建。

### 3. 排障与日志
测试失败时，可以通过以下命令查看实时日志：
- **Fusion 日志**: `docker logs -f fustor-fusion`
- **Agent 日志**: `docker exec client-a cat /tmp/fustor-agent.log`
- **查看状态**: `docker compose -f tests/e2e/docker-compose.yml ps`

## 测试用例清单

> **Note:** 可疑文件 (C1-C6)、墓碑 (D1-D4)、Parent Mtime (B5) 和 Future Timestamp (I) 的核心逻辑
> 已移至 `extensions/view-fs/tests/` 下的单元测试，运行时间 < 1s。
> 以下仅保留需要真实 Docker/NFS 环境的 E2E 测试。

### A. Leader/Follower 选举 & 会话管理
- A1: 第一个 Agent 成为 Leader
- A2: Follower 只发送 Realtime 事件
- A3: 组件崩溃隔离 / 会话恢复
- A4: 视图发现 & 并发控制

### B. 盲区发现 (跨挂载 NFS)
- B1: 盲区新增文件被 Audit 发现
- B2: 盲区删除文件被 Audit 发现
- B3: 盲区修改文件 mtime 被 Audit 更新
- B4: 每轮 Audit 开始时清空盲区名单

### E. 故障转移 (Failover)
- E1: Leader 宕机后 Follower 顺位接管角色
- E2: 新 Leader 恢复 Audit/Snapshot 职责

### F. 按需扫描
- F1: 强制实时查询恢复一致性

### H. 时钟偏移 (libfaketime)
- H: 分布式时钟偏移容忍度验证

### HB. 心跳超时
- HB1: Agent 心跳超时后自动恢复

### K. 高级部署
- K: Fan-Out / Aggregation / HA 动态配置

### Pipe 架构
- pipe_basic: AgentPipe 基本操作
- pipe_field_mapping: 字段映射

## 目录结构

```
tests/e2e/
├── docker-compose.yml          # 环境定义
├── conftest.py                 # Pytest 核心 Fixtures
├── .env_state                  # [自动生成] 环境哈希快照
├── utils/                      # DockerManager 与 API Client
├── fixtures/                   # 模块化测试组件 (Docker/Agent/Leadership)
└── consistency/                # 核心一致性测试用例
```

## 参考文档

- [CONSISTENCY_DESIGN.md](../../docs/CONSISTENCY_DESIGN.md) - 一致性设计文档
- [CONFIGURATION.md](../../docs/CONFIGURATION.md) - YAML 配置说明
- [integration-testing Skill](../../.agent/skills/integration-testing/SKILL.md) - AI 助手集成测试指南
