# 集中管理平面设计

> 版本: 1.0.0  
> 日期: 2026-02-13  
> 状态: 设计中  
> 前置依赖: 01-ARCHITECTURE, 05-RUNTIME_BEHAVIOR, 08-HOT_RELOAD

## 1. 需求背景

用户需要一个集中的管理界面来控制所有 Agent 和 Fusion 服务的配置、升级和运行状态。

### 1.1 目标能力

| 能力 | 说明 |
|:---|:---|
| **实时监控** | 查看所有 Agent/Pipe/View 的运行状态和健康指标 |
| **配置管理** | 集中查看和修改 Agent/Fusion 配置，远程下发 |
| **运维操作** | 远程执行 reload、stop pipe、on-demand scan 等操作 |
| **升级协调** | 触发 Agent 自升级（后续阶段） |

### 1.2 设计约束

- **零新组件**: 不引入新的 Controller 服务或独立进程
- **零新连接**: 复用 Agent ↔ Fusion 的已有 HTTP 通道
- **零新依赖**: 仅使用已安装的 FastAPI + 标准库

---

## 2. 架构决策

### 2.1 核心原则: Fusion 即管理平面

Fusion 进程已经是所有 Agent 的连接 Hub，具备作为管理平面的全部基础设施：

```
┌─────────────────────────────────────┐
│           Fusion Process            │
│                                     │
│  ┌────────┐ ┌────────┐ ┌────────┐  │
│  │Data API│ │Mgmt API│ │  UI    │  │
│  │ (已有)  │ │ (扩展)  │ │ (新增)  │  │
│  └────┬───┘ └────┬───┘ └────────┘  │
│       │          │                  │
│  ┌────┴──────────┴───────┐         │
│  │  Heartbeat 命令通道    │         │
│  │  (已有, 扩展 payload)  │         │
│  └───────────────────────┘         │
└──────────┬──────────────────────────┘
           │ HTTP (已有连接)
   ┌───────┼───────┐
 Agent-A Agent-B Agent-C
```

### 2.2 已有设施利用

| 设施 | 当前用途 | 管理平面复用方式 |
|:---|:---|:---|
| Heartbeat 响应的 `commands` 字段 | 下发 `scan` 命令 | 扩展命令类型 |
| `PipeCommandMixin._handle_commands` | 路由 `scan` 命令 | 新增命令处理器 |
| `management.py` (28行) | 仅 `GET /management/views` | 扩展为完整管理 API |
| `session_manager` | 管理 Agent 会话 | 缓存 Agent 上报的扩展状态 |
| SIGHUP 热加载 | 本地配置变更 | 通过命令通道远程触发 |

### 2.3 为什么不引入独立 Controller

| 方案 | 额外代价 | 否决原因 |
|:---|:---|:---|
| 新建 `fustor-controller` 进程 | 新部署、新通信协议、Agent 改连接目标 | 过度工程化 |
| Agent 轮询远程配置 | 重构 ConfigLoader、引入长轮询 | heartbeat 通道已能替代 |
| SSH/Ansible 外部管理 | 无实时状态、非代码内方案 | 运维体验差 |

---

## 3. 实施阶段

### Phase 1: 管理 API (~300 行)

扩展 `fusion/src/fustor_fusion/api/management.py`:

```python
# --- 仪表盘 ---
GET /management/dashboard
# 返回: 所有 view 状态、agent 会话列表、pipe 状态、最后心跳

# --- Agent 操作 ---
POST /management/agents/{agent_id}/command
# Body: {"type": "reload"} | {"type": "stop_pipe", "pipe_id": "..."} | ...
# 机制: 写入 session_manager 的命令队列，下次 heartbeat 时下发

# --- 配置查看 ---
GET /management/config
# 返回: Fusion 当前完整配置 (views, pipes, receivers)

# --- Fusion 自身操作 ---
POST /management/reload
# 触发 Fusion 自身的 SIGHUP 热加载
```

#### 命令通道扩展

新增命令类型（在 `PipeCommandMixin` 中添加处理器）:

| 命令类型 | 方向 | 用途 |
|:---|:---|:---|
| `scan` | Fusion → Agent | 已有，On-Demand 扫描 |
| `reload_config` | Fusion → Agent | **新增**，触发 Agent SIGHUP |
| `stop_pipe` | Fusion → Agent | **新增**，停止指定 Pipe |
| `report_status` | Fusion → Agent | **新增**，要求 Agent 立即上报详细状态 |

---

### Phase 2: Agent 状态上报 (~100 行)

扩展 heartbeat payload，让 Agent 主动上报运行状态:

#### Agent 侧 (heartbeat 请求体扩展)

```python
# 当前:
{"can_realtime": true}

# 扩展为:
{
    "can_realtime": true,
    "agent_status": {
        "agent_id": "agent-nfs-a",
        "version": "0.9.1",
        "uptime_seconds": 3600,
        "pipes": {
            "pipe-nfs-a": {
                "state": "RUNNING|MESSAGE_SYNC",
                "role": "leader",
                "events_pushed": 12345
            }
        }
    }
}
```

#### Fusion 侧 (session_manager 缓存)

在 `SessionInfo` 中新增 `last_agent_status` 字段，每次 heartbeat 更新。Dashboard API 直接读取此缓存。

---

### Phase 3: 管理界面 (~500 行)

单个 HTML 文件 + vanilla JS，由 Fusion 直接 serve:

```python
# main.py
from fastapi.staticfiles import StaticFiles
app.mount("/ui", StaticFiles(directory="ui", html=True))
```

#### 界面功能

| 面板 | 数据来源 |
|:---|:---|
| Agent 列表 (ID、IP、版本、状态) | `GET /management/dashboard` |
| Pipe 状态 (角色、事件数、错误) | 同上 |
| View 状态 (节点数、延迟、逻辑时钟) | `GET /management/views` + view stats |
| 操作按钮 (Reload/Stop) | `POST /management/agents/{id}/command` |

---

## 4. 对架构的影响

### 4.1 不变的部分

- 3 层对称模型 (Source/Pipe/Sender ↔ Receiver/Pipe/View)
- 6 层分层架构
- 一致性模型 (Tombstone/Suspect/Blind-spot)
- Agent ↔ Fusion 数据通道协议

### 4.2 扩展的部分

- Heartbeat payload 增加 `agent_status` 字段（向后兼容，可选字段）
- Heartbeat response `commands` 增加新命令类型（Agent 忽略未知命令）
- `management.py` 从 28 行扩展为 ~300 行
- 新增 `ui/` 静态资源目录

### 4.3 向后兼容性

所有扩展均为**可选字段追加**，不修改已有协议。旧版 Agent 连接新版 Fusion 不受影响（忽略新命令），新版 Agent 连接旧版 Fusion 也不受影响（`agent_status` 被忽略）。

---

## 5. 工作量估算

| Phase | 工作量 | 新增代码 | 涉及文件 |
|:---|:---|:---|:---|
| 1. 管理 API | 2-3 天 | ~300 行 | `management.py`, `command.py`, `session_manager.py` |
| 2. 状态上报 | 1 天 | ~100 行 | `agent_pipe.py`, `session.py`, `session_manager.py` |
| 3. Web UI | 2-3 天 | ~500 行 | `ui/index.html` (新建) |
| **总计** | **~6 天** | **~900 行** | |
