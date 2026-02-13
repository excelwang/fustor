# `feature/multi-fs-view` 分支评审报告

> 评审日期: 2026-02-13  
> 分支: `feature/multi-fs-view` (6 commits)  
> 范围: 25 files, ~980 lines

---

## 1. 变更摘要

本分支引入 3 个特性和 1 个破坏性变更：

| 变更 | 关键文件 |
|:---|:---|
| **Multi-FS View** 聚合视图 | `extensions/view-multi-fs/`, `specs/09-MULTI_FS_VIEW.md` |
| **Data Lineage** 数据血缘追踪 | `core/event/base.py`, `fusion_pipe.py`, `view-fs/*` |
| **Agent ID** 改为 IP-based + 强制配置 | `agent-sdk/utils.py`, `agent/app.py`, `agent/config/unified.py` |
| 文档清理 | 删除 4 个 `under-review/` 草案 |

---

## 2. 需要注意的事项

### 2.1 ⚠️ 破坏性变更: `agent_id` 强制配置

`agent/app.py` 中 `agent_id` 现在是**必填项**，未配置时启动直接抛 `ValueError`。

**影响**: 所有现有 Agent 部署的配置文件需要新增 `agent_id` 字段，否则无法启动。

```yaml
# 必须在 Agent 配置文件中添加:
agent_id: "your-agent-name"
```

### 2.2 🧹 待清理: `utils.py` 死代码

`agent-sdk/utils.py` 中的 `get_or_generate_agent_id()` 函数已不再被调用（`app.py` 不再 import 它），但代码仍然保留。建议删除或标注为 deprecated。

另外函数内注释 `# 1. Check File` 后直接跳到 `# 3. Auto-generate`，缺少 `# 2`（原为环境变量优先级，已移除）。

---

## 3. 代码评审明细

### 3.1 Multi-FS View — ✅ 通过

| 检查项 | 结果 |
|:---|:---|
| 配置解析 (`config.get("members", [])`) | ✅ 正确，与 `ViewManager` 传参一致 |
| Schema 路由 (`target_schema = "multi-fs"`) | ✅ 不会被 `fs` 事件触发 |
| Readiness checker 跳过 | ✅ `multi-fs` 类型不检查 session/snapshot |
| 并发查询 (`asyncio.gather`) | ✅ 成员查询并行执行 |
| 容错 (单成员失败不影响其他) | ✅ 每个 `fetch_stats`/`fetch_tree` 独立 try-catch |
| Entry point 注册 | ✅ `fustor.view_drivers` + `fustor.view_api` 双注册 |
| 单元测试覆盖 | ✅ 聚合、best_view 选择、missing member 均已覆盖 |

**轻微建议**:
- `pyproject.toml` 缺少对 `fustor-fusion` 的依赖声明（`_get_member_driver` 间接 import `fustor_fusion.view_manager`）
- `api.py` 中 `get_driver_func` 参数缺少类型标注
- `TreeManager.get_subtree_stats` 返回空 `{}` 表示 path not found，API 层未做区分处理（会返回 `status: "ok"` 但无统计字段）

### 3.2 Data Lineage — ✅ 通过

| 检查项 | 结果 |
|:---|:---|
| `EventBase.metadata` 字段 | ✅ `Optional[Dict]`，向后兼容 |
| Lineage cache 生命周期 | ✅ session 创建时构建，关闭时清理 |
| `session_info` 获取时序 | ✅ `create_session_entry` 在 `on_session_created` 之前 |
| 节点持久化 (nodes, tree) | ✅ `last_agent_id` / `source_uri` 正确写入 |

**轻微建议**:
- 高吞吐时每个事件都新建 `metadata = {}` dict，可考虑预分配或共享引用
- `verify_lineage.py` 是独立脚本而非 pytest 测试，建议移到 `scripts/`

### 3.3 Agent ID 重构 — ✅ 通过（确认为预期行为）

| 检查项 | 结果 |
|:---|:---|
| IP 获取方式 (UDP socket trick) | ✅ 标准做法，不产生网络流量 |
| ID 格式 (`192-168-1-100-a1b2c3d4`) | ✅ 可读且唯一 |
| 配置优先级: 配置文件 > 其他 | ✅ |
| 缺少配置时行为 | ✅ 明确报错（`ValueError`） |

---

## 4. 总结

| 状态 | 说明 |
|:---|:---|
| 🔴 阻塞 | 0 |
| 🟡 待清理 | 2 项（`utils.py` 死代码、注释编号） |
| 🟢 建议 | 5 项（见各节轻微建议） |

**结论**: 可合并。`utils.py` 死代码清理可在后续 commit 中处理。
