# Fustor NFS Multi-Mount Consistency Integration Tests

本目录包含 Fustor 在 NFS 多端挂载场景下的一致性集成测试。测试使用真实的 Docker 容器环境模拟 NFS 多客户端场景。

## 架构概览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Docker Compose Environment                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────────────────────────┐│
│  │   Registry  │   │   Fusion    │   │         NFS Server              ││
│  │   :18101    │   │   :18102    │   │      /exports                   ││
│  └─────────────┘   └─────────────┘   └─────────────────────────────────┘│
│         │                │                         │                     │
│         └────────────────┼─────────────────────────┤                     │
│                          │                         │                     │
│  ┌───────────────────────┼─────────────────────────┼──────────────────┐ │
│  │                     NFS Mount (actimeo=5)                          │ │
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

### 完整测试套件

```bash
# 进入项目根目录
cd /home/huajin/fustor_monorepo

# 安装测试依赖
pip install pytest pytest-asyncio requests

# 运行所有集成测试
pytest fusion/tests/integration/consistency/ -v

# 运行特定类别的测试
pytest fusion/tests/integration/consistency/test_b*.py -v  # 盲区测试
pytest fusion/tests/integration/consistency/test_c*.py -v  # 可疑文件测试
pytest fusion/tests/integration/consistency/test_d*.py -v  # 墓碑测试
pytest fusion/tests/integration/consistency/test_e*.py -v  # 故障转移测试
```

### 手动管理 Docker 环境

```bash
# 启动环境
cd fusion/tests/integration
docker compose up -d --build

# 查看日志
docker compose logs -f fusion
docker compose logs -f fustor-nfs-client-a

# 停止环境
docker compose down -v
```

## 测试用例清单

### A. Leader/Follower 选举 (2 用例)

| 用例 ID | 文件 | 描述 |
|---------|------|------|
| A1 | `test_a1_leader_election_first.py` | 第一个 Agent 成为 Leader |
| A2 | `test_a2_follower_io_isolation.py` | Follower 只发送 Realtime 事件 |

### B. 盲区发现 (5 用例)

| 用例 ID | 文件 | 描述 |
|---------|------|------|
| B1 | `test_b1_blind_spot_creation.py` | 盲区新增文件被 Audit 发现 |
| B2 | `test_b2_blind_spot_deletion.py` | 盲区删除文件被 Audit 发现 |
| B3 | `test_b3_blind_spot_modification.py` | 盲区修改文件 mtime 被 Audit 更新 |
| B4 | `test_b4_blind_spot_list_clearing.py` | 每轮 Audit 开始时清空盲区名单 |
| B5 | `test_b5_parent_mtime_check.py` | Parent Mtime 仲裁防止旧数据 |

### C. 可疑文件 (5 用例)

| 用例 ID | 文件 | 描述 |
|---------|------|------|
| C1 | `test_c1_snapshot_suspect.py` | Snapshot 标记近期文件为 Suspect |
| C2 | `test_c2_audit_suspect.py` | Audit 标记盲区近期文件为 Suspect |
| C3 | `test_c3_suspect_ttl_expiry.py` | 10 分钟 TTL 后 Suspect 标记过期 |
| C4 | `test_c4_realtime_removes_suspect.py` | Realtime 事件移除 Suspect 标记 |
| C5 | `test_c5_sentinel_sweep.py` | Leader 哨兵巡检更新 Suspect mtime |

### D. 墓碑防复活 (4 用例)

| 用例 ID | 文件 | 描述 |
|---------|------|------|
| D1 | `test_d1_tombstone_creation.py` | Realtime Delete 创建 Tombstone |
| D2 | `test_d2_snapshot_no_resurrect.py` | Snapshot 不复活 Tombstone 文件 |
| D3 | `test_d3_audit_no_resurrect.py` | Audit 不复活 Tombstone 文件 |
| D4 | `test_d4_tombstone_cleanup.py` | Audit-End 清理旧 Tombstone |

### E. 故障转移 (2 用例)

| 用例 ID | 文件 | 描述 |
|---------|------|------|
| E1 | `test_e1_leader_failover.py` | Leader 宕机后 Follower 接管 |
| E2 | `test_e2_new_leader_duties.py` | 新 Leader 恢复 Snapshot/Audit 职责 |

## 配置参数

通过环境变量配置测试行为：

| 变量 | 默认值 | 描述 |
|------|--------|------|
| `FUSTOR_TEST_TIMEOUT` | 120 | 测试超时秒数 |
| `FUSTOR_AUDIT_INTERVAL` | 30 | 等待 Audit 周期的秒数 |
| `FUSTOR_SKIP_LONG_TESTS` | false | 跳过需要长时间运行的测试 |

## 目录结构

```
fusion/tests/integration/
├── docker-compose.yml          # Docker Compose 配置
├── conftest.py                 # Pytest fixtures
├── README.md                   # 本文档
├── containers/
│   ├── fustor-services/
│   │   └── Dockerfile          # Registry & Fusion 镜像
│   └── nfs-client/
│       ├── Dockerfile          # NFS 客户端镜像
│       └── entrypoint.sh       # 容器启动脚本
├── utils/
│   ├── __init__.py
│   ├── docker_manager.py       # Docker 操作封装
│   ├── fusion_client.py        # Fusion API 客户端
│   └── registry_client.py      # Registry API 客户端
└── consistency/
    ├── __init__.py
    ├── test_a1_*.py            # Leader 选举测试
    ├── test_a2_*.py            # Follower 隔离测试
    ├── test_b1_*.py ~ test_b5_*.py  # 盲区测试
    ├── test_c1_*.py ~ test_c5_*.py  # 可疑文件测试
    ├── test_d1_*.py ~ test_d4_*.py  # 墓碑测试
    └── test_e1_*.py ~ test_e2_*.py  # 故障转移测试
```

## 故障排查

### 容器启动失败

```bash
# 查看详细日志
docker compose logs --tail=100 <service-name>

# 检查 NFS 挂载
docker exec fustor-nfs-client-a mount | grep nfs
```

### 测试超时

1. 增加 `FUSTOR_AUDIT_INTERVAL` 值
2. 检查 NFS 缓存配置 (`actimeo` 参数)
3. 检查 Agent 日志确认 Audit 正常运行

### 权限问题

确保 Docker 容器有足够权限：
- `privileged: true` 用于 NFS 挂载
- `cap_add: SYS_ADMIN` 用于系统操作

## 参考文档

- [CONSISTENCY_DESIGN.md](../../../docs/CONSISTENCY_DESIGN.md) - 一致性设计文档
- [Fustor Agent README](../../../agent/README.md) - Agent 配置说明
