# 管理平面 API 测试大纲

> 对应 Spec: `specs/10-MANAGEMENT_PLANE.md`  
> 分支: `feature/management-plane`  
> 测试框架: pytest + httpx (AsyncClient)

---

## 0. 测试基础设施

### 0.1 Fixtures

| Fixture | 说明 |
|:---|:---|
| `fusion_app` | 创建 FastAPI app（带 management router） |
| `client` | `httpx.AsyncClient(app=fusion_app)` |
| `mock_session_manager` | Mock `session_manager`，预注入 sessions |
| `mock_pipe_manager` | Mock `pipe_manager`，预注入 pipes |
| `mock_fusion_config` | Mock `fusion_config`，可设置 `management_api_key` |

### 0.2 辅助函数

```python
def make_session_info(agent_id, view_id, **overrides) -> SessionInfo
def make_agent_status(agent_id, pipe_id, state="RUNNING", role="leader") -> dict
```

---

## 1. 认证 (`require_management_key`)

### 1.1 未配置密钥（开放模式）

| # | 用例 | 预期 |
|:---|:---|:---|
| A-01 | `management_api_key=None`，不带 header 请求 dashboard | 200 |
| A-02 | `management_api_key=None`，带任意 header 请求 dashboard | 200 |

### 1.2 已配置密钥

| # | 用例 | 预期 |
|:---|:---|:---|
| A-03 | 不带 `X-Management-Key` header | 401, body 含 "required" |
| A-04 | 带错误的 key | 403, body 含 "Invalid" |
| A-05 | 带正确的 key | 200 |
| A-06 | 密钥验证覆盖所有端点（dashboard / command / config / reload / config-push） | 均 401 |

---

## 2. Dashboard (`GET /management/dashboard`)

### 2.1 空状态

| # | 用例 | 预期 |
|:---|:---|:---|
| D-01 | 无 pipe、无 session、无 view | 200, `pipes={}`, `sessions={}`, `agents={}`, `views={}` |

### 2.2 有数据

| # | 用例 | 预期 |
|:---|:---|:---|
| D-02 | 1 pipe + 1 session (无 agent_status) | `agents` 按 task_id 聚合，`status=null` |
| D-03 | 1 pipe + 2 sessions (同 agent_id，不同 session) | `agents` 一个条目，`sessions` 列表长度=2 |
| D-04 | 2 pipe + sessions from 不同 agent_id | `agents` 两个条目 |
| D-05 | session 带 `agent_status` (含 role, events_pushed) | 返回中 `agent_status` 字段完整同步 |
| D-06 | session 的 `task_id` 不含 `:` | `agent_id` 回退为 `task_id` 本身 |
| D-07 | session 的 `task_id=None` | 该 session 不出现在 `agents` 聚合中 |

### 2.3 Session 元数据

| # | 用例 | 预期 |
|:---|:---|:---|
| D-08 | 验证 `age_seconds` 和 `idle_seconds` 计算准确 | `age = now - created_at`, `idle = now - last_activity` |
| D-09 | `can_realtime` 字段正确透传 | 与 `SessionInfo.can_realtime` 一致 |

---

## 3. Command Dispatch (`POST /management/agents/{agent_id}/command`)

### 3.1 正常流程

| # | 用例 | 预期 |
|:---|:---|:---|
| C-01 | `type=reload_config`，目标 agent 有 1 个 session | 200, `sessions_queued=1` |
| C-02 | `type=reload_config`，目标 agent 有 3 个 sessions | 200, `sessions_queued=3` |
| C-03 | `type=stop_pipe` + `pipe_id=xxx` | 200, 命令正确入队 |
| C-04 | `type=scan` + `path=/data` + `recursive=true` | 200, 命令正确入队 |

### 3.2 错误场景

| # | 用例 | 预期 |
|:---|:---|:---|
| C-05 | 目标 agent_id 不存在 | 404, "No active sessions" |
| C-06 | `queue_command` 返回 False（队列满） | 200, `sessions_queued=0` |
| C-07 | 空 `type` 字段 | 422 (Pydantic 校验) |

### 3.3 命令入队验证

| # | 用例 | 预期 |
|:---|:---|:---|
| C-08 | 验证 `session_manager.queue_command` 被调用次数等于匹配 session 数 | mock 断言 |
| C-09 | 验证命令 dict 包含原始 `type` 和额外参数 | mock 断言 `call_args` |

---

## 4. Config View (`GET /management/config`)

| # | 用例 | 预期 |
|:---|:---|:---|
| F-01 | 返回结构包含 `pipes`, `receivers`, `views` 三个顶层 key | 200 |
| F-02 | pipe 配置含 `receiver`, `views`, `disabled`, `session_timeout_seconds` 等字段 | 字段完整 |
| F-03 | receiver 配置含 `driver`, `port`, `disabled` | 字段完整 |
| F-04 | view 配置含 `driver`, `disabled`, `driver_params` | 字段完整 |
| F-05 | 多个 pipe/receiver/view 时全部返回 | 数量与配置一致 |

---

## 5. Fusion Reload (`POST /management/reload`)

| # | 用例 | 预期 |
|:---|:---|:---|
| R-01 | 正常调用 | 200, `status=ok` |
| R-02 | 验证 `os.kill(os.getpid(), SIGHUP)` 被调用 | mock `os.kill` 断言 |
| R-03 | SIGHUP 发送失败（mock 抛异常） | 500 |

---

## 6. Config Push (`POST /management/agents/{agent_id}/config`)

### 6.1 正常流程

| # | 用例 | 预期 |
|:---|:---|:---|
| P-01 | 推送合法 YAML，目标 agent 存在 | 200, `sessions_queued=1` |
| P-02 | 验证入队命令的 `type="update_config"` | mock 断言 |
| P-03 | 验证入队命令包含 `config_yaml` 和 `filename` | mock 断言 |
| P-04 | 自定义 `filename="custom.yaml"` | 返回中 `filename` 一致 |
| P-05 | 同 agent 多个 session，仅队列 1 个（`break` 逻辑） | `sessions_queued=1` |

### 6.2 错误场景

| # | 用例 | 预期 |
|:---|:---|:---|
| P-06 | 目标 agent_id 不存在 | 404 |
| P-07 | `config_yaml` 为空 | 422 (Pydantic) |

---

## 7. Agent 状态上报（Heartbeat 集成）

> 这部分测试 Fusion 侧的 heartbeat 端点对 `agent_status` 的缓存行为。

| # | 用例 | 预期 |
|:---|:---|:---|
| H-01 | heartbeat payload 含 `agent_status` | `SessionInfo.agent_status` 被更新 |
| H-02 | heartbeat payload 无 `agent_status` | `SessionInfo.agent_status` 保持不变 |
| H-03 | 连续两次 heartbeat，第二次覆盖第一次 status | 最新值生效 |
| H-04 | `agent_status` 含任意 dict 结构 | 原样存储，不做 schema 校验 |

---

## 8. Agent 命令处理器（`PipeCommandMixin`）

> 单元测试，直接调用 handler 方法，mock 外部依赖。

### 8.1 `_handle_command_reload`

| # | 用例 | 预期 |
|:---|:---|:---|
| M-01 | 正常调用 | `os.kill(pid, SIGHUP)` 被调用 |
| M-02 | `os.kill` 抛异常 | 日志记录 error，不 raise |

### 8.2 `_handle_command_stop_pipe`

| # | 用例 | 预期 |
|:---|:---|:---|
| M-03 | `pipe_id` 匹配当前 pipe | `self.stop()` 被调用 |
| M-04 | `pipe_id` 不匹配 | 不调用 `stop()` |
| M-05 | 缺少 `pipe_id` 字段 | 日志 warning，不 raise |

### 8.3 `_handle_command_update_config`

| # | 用例 | 预期 |
|:---|:---|:---|
| M-06 | 合法 YAML，文件不存在 | 直接写入，SIGHUP 触发 |
| M-07 | 合法 YAML，文件已存在 | 创建 `.bak` 备份后写入 |
| M-08 | 非法 YAML（语法错误） | 日志 error，不写入 |
| M-09 | `config_yaml` 为空 | 日志 warning，不写入 |
| M-10 | `filename` 含路径穿越 (`../../etc/passwd`) | `os.path.basename` 去除路径 |
| M-11 | `filename` 无 `.yaml` 后缀 | 自动追加 `.yaml` |
| M-12 | 写入失败（目录不存在 / 权限不足） | 日志 error，尝试恢复 backup |
| M-13 | 写入失败 + backup 恢复也失败 | 不 raise，两次 error 日志 |

### 8.4 `_handle_commands` 路由

| # | 用例 | 预期 |
|:---|:---|:---|
| M-14 | 未知命令类型 `type=foo` | 日志 warning，跳过 |
| M-15 | 命令列表含多个命令（scan + reload_config） | 按顺序执行 |
| M-16 | 某个命令 handler 抛异常 | 捕获，继续处理下一个命令 |

---

## 9. `_build_agent_status` 单元测试

| # | 用例 | 预期 |
|:---|:---|:---|
| S-01 | `task_id="agent-1:pipe-1"` | `agent_id="agent-1"` |
| S-02 | `task_id="agent-1"` (无冒号) | `agent_id=None` |
| S-03 | `task_id=None` | `agent_id=None` |
| S-04 | 返回 dict 包含 `pipe_id`, `state`, `role`, `events_pushed`, `is_realtime_ready` | 全字段覆盖 |

---

## 测试用例统计

| 模块 | 用例数 |
|:---|:---|
| 认证 | 6 |
| Dashboard | 9 |
| Command Dispatch | 9 |
| Config View | 5 |
| Fusion Reload | 3 |
| Config Push | 7 |
| Heartbeat 集成 | 4 |
| 命令处理器 | 16 |
| Agent Status | 4 |
| **总计** | **63** |
