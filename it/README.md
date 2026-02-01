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

### 完整测试套件

```bash
# 进入项目根目录
cd /home/huajin/fustor_monorepo

# 安装测试依赖
pip install pytest pytest-asyncio requests

# 运行所有集成测试
uv run pytest it/consistency/ -v
```

### 环境重用说明 (FUSTOR_REUSE_ENV)

本测试框架支持环境重用。如果环境已经在运行，测试会尝试直接连接并注入配置，大大节省时间。

## 测试用例清单

### A. Leader/Follower 选举 (2 用例)
- A1: 第一个 Agent 成为 Leader
- A2: Follower 只发送 Realtime 事件

### B. 盲区发现 (5 用例)
- B1: 盲区新增文件被 Audit 发现
- B2: 盲区删除文件被 Audit 发现
- B3: 盲区修改文件 mtime 被 Audit 更新
- B4: 每轮 Audit 开始时清空盲区名单
- B5: Parent Mtime 仲裁防止旧数据

### C. 可疑文件 (6 用例)
- C1 ~ C6: 可疑文件状态追踪与清理逻辑。

### D. 墓碑防复活 (4 用例)
- D1 ~ D4: 防止过期 Snapshot/Audit 数据复活已删除文件。

### E. 故障转移 (2 用例)
- E1: Leader 宕机后 Follower 接管。

## 目录结构

```
it/
├── docker-compose.yml          # Docker Compose 配置
├── conftest.py                 # Pytest fixtures (含配置注入逻辑)
├── README.md                   # 本文档
├── containers/
│   ├── fustor-services/
│   │   └── Dockerfile          # Fusion 镜像
│   └── nfs-client/
│       ├── Dockerfile          # NFS 客户端镜像 (Agent 已内置)
│       └── entrypoint.sh       # 容器启动脚本
└── consistency/                # 一致性核心测试用例
```

## 参考文档

- [CONSISTENCY_DESIGN.md](../../docs/CONSISTENCY_DESIGN.md) - 一致性设计文档
- [CONFIGURATION.md](../../docs/CONFIGURATION.md) - YAML 配置说明
