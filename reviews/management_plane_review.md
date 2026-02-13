# `feature/management-plane` 分支评审

> 4 commits: `82e4f37` → `afd1a5d` → `df12f51` → `ee5b0fb`  
> 49 files, +3128 / -1055 lines  
> 评审日期: 2026-02-13

---

## 一、总体评价

**实现质量**：⭐⭐⭐（3/5）  
**实现完整性**：Phase 1-3 基本完成，Phase 4 (远程升级) 仅在 spec 中，未实现

管理平面的核心骨架已搭建完成：Dashboard API、Agent 状态上报、命令通道、远程配置推送、认证、和管理界面。但存在若干 **安全隐患**、**架构回退**、和 **实现质量问题** 需要在合并前解决。

---

## 二、代码质量评审

### 🔴 严重问题 (Must Fix)

#### Issue 1: `main.py` SIGHUP handler 回退导致热重载功能退化

**文件**: `fusion/src/fustor_fusion/main.py`

```diff
 def handle_reload():
     logger.info("Received SIGHUP - initiating hot reload")
     asyncio.create_task(pm.reload())
-    # 以下代码被完全删除：
-    # - setup_view_routers()        ← View API 路由刷新
-    # - ViewManager cache cleanup   ← 驱动实例释放
-    # - resource close() 调用       ← 资源泄漏防护
```

**影响**：SIGHUP 热重载现在仅重载 Pipes，不再刷新 View 路由和清理 ViewManager 缓存：
- 新增/删除 View 后，API 路由不会更新
- 旧的 ViewManager 驱动实例不会被 `close()`，造成**资源泄漏**
- 这不是管理平面新增逻辑，而是删除了既有的正确逻辑

**修复建议**：恢复 `setup_view_routers()` 和 ViewManager cleanup 逻辑。

---

#### Issue 2: 远程配置推送缺少语义验证

**文件**: `agent/src/fustor_agent/runtime/pipe/command.py` — `_handle_command_update_config`

```python
# 仅做了 YAML 语法校验
yaml.safe_load(config_yaml)
```

只验证 YAML 语法，不验证配置内容的合法性（如缺少必要字段、无效的 driver 名、端口冲突等）。恶意或错误的配置会直接写入文件并触发 SIGHUP，可能导致 Agent 崩溃且无法自愈。

**修复建议**：复用 `ConfigValidator` 进行语义校验，验证失败时拒绝写入。

---

#### Issue 3: 管理 API Key 采用明文比较，无时序攻击防护

**文件**: `fusion/src/fustor_fusion/api/management.py` L30-51

```python
if x_management_key != configured_key:  # 普通字符串比较
    raise HTTPException(status_code=403, ...)
```

**修复建议**：使用 `hmac.compare_digest()` 进行常量时间比较：

```python
import hmac
if not hmac.compare_digest(x_management_key, configured_key):
    raise HTTPException(...)
```

---

### 🟡 中等问题

#### Issue 4: Dashboard 从 session 提取 `agent_id` 的逻辑脆弱

**文件**: `fusion/src/fustor_fusion/api/management.py` — `get_dashboard()`

```python
if si.task_id and ":" in si.task_id:
    agent_id = si.task_id.split(":")[0]
elif si.task_id:
    agent_id = si.task_id
```

依赖 `task_id` 的格式约定 (`agent_id:pipe_id`)。如果格式变化或 `agent_id` 本身包含 `:` 则会解析错误。

**修复建议**：优先从 `agent_status.agent_id` 获取，`task_id` 解析作为 fallback。

---

#### Issue 5: `_build_agent_status` 仅上报单个 pipe 而非全 Agent 状态

**文件**: `agent/src/fustor_agent/runtime/agent_pipe.py` L131-144

```python
def _build_agent_status(self) -> Dict[str, Any]:
    return {
        "agent_id": agent_id,
        "pipe_id": self.id,          # ← 每个 pipe 只报告自己
        "state": str(self.state),
        ...
    }
```

Spec Phase 2 设计的是 Agent 级别的聚合状态（含所有 pipes），但实现中每个 pipe 独立上报自己的状态。当 Agent 有多个 pipes 时，Dashboard 上同一个 `agent_id` 的 `status` 会被**最后一次 heartbeat 的 pipe** 覆盖。

**修复建议**：在 Agent 应用层聚合所有 pipe 状态后注入 heartbeat，或在 Fusion dashboard 端按 pipe 维度聚合。

---

#### Issue 6: `agent_status` 读取方式不一致

**文件**:
- `fusion-sdk/src/fustor_fusion_sdk/interfaces.py` — 已添加 `agent_status` 字段定义
- `fusion/src/fustor_fusion/api/management.py` — 用 `getattr(si, "agent_status", None)` 做防御性读取

接口定义和使用代码之间有不一致的信心，既声明了字段又用 `getattr` 防御。

**修复建议**：统一风格 — 既然已在 `SessionInfo` 中声明字段，直接用 `si.agent_status` 访问。

---

#### Issue 7: 多处 `import` 放在函数内部且有重复

**文件**: `agent/src/fustor_agent/runtime/pipe/command.py` — `_handle_command_update_config`

```python
import yaml                                # 函数内
from fustor_core.common import get_fustor_home_dir  # 函数内
import shutil  # 出现两次（正常路径和异常恢复路径各一次）
```

**修复建议**：将 `import shutil` 统一到函数顶部或模块顶部。

---

### 🟢 小问题 / 建议

#### Issue 8: `pyproject.toml` 格式损坏

```diff
-    "pytest-cov>=7.0.0",
-]
+    "types-requests"]
```

删除 `pytest-cov` 依赖的同时破坏了列表缩进格式。

---

#### Issue 9: UI HTML 缺少 XSS 防护

**文件**: `fusion/src/fustor_fusion/ui/management.html`

直接拼接 `innerHTML`：

```javascript
return `<tr><td class="mono">${id}</td><td class="mono">${a.client_ip || '-'}</td>...`
```

`agent_id` 或 `client_ip` 含有 HTML 特殊字符时存在 XSS 风险。

**修复建议**：使用 `textContent` 或添加 HTML 转义函数。

---

#### Issue 10: 配置编辑器无回填

UI 的 "Edit Config" 弹窗打开时 `configEditor.value = ''`，用户无法看到 Agent 当前配置。

**修复建议**：通过 API 获取当前配置并回填。

---

#### Issue 11: UI 未支持 `management_api_key` 传递

`api()` 函数的 `fetch` 调用没有携带 `X-Management-Key` header。若启用了认证，UI 本身无法正常工作。

**修复建议**：在 UI 中增加 key 输入框，并在 `api()` 请求中携带 header。

---

## 三、实现完整性评估

| Spec Phase | 描述 | 状态 | 评估 |
|:---|:---|:---:|:---|
| **Phase 1** | 管理 API（Dashboard / Command / Config / Reload） | ✅ | 5 个端点均已实现 |
| **Phase 2** | Agent 状态上报 | ⚠️ | heartbeat 扩展完成，但只报告单 pipe 而非 Agent 级聚合（Issue #5） |
| **Phase 3** | 管理界面 | ✅ | 677 行 HTML，暗色主题，4 个面板 |
| **Phase 4** | 远程 Agent 升级 | ❌ | Spec 中详细设计但代码中无任何 `upgrade` 相关实现 |
| **认证** | Management API Key | ✅ | 可选 key 模式，向后兼容（但有时序攻击问题 Issue #3） |
| **远程配置推送** | Config Push + Backup + SIGHUP | ✅ | 语法校验 + 备份恢复 + 路径穿越防护，但缺语义校验（Issue #2） |
| **测试** | 测试用例 | ❌ | 仅有 228 行测试大纲，无实际测试代码 |

### 分支中未在 Spec 范围但实际做了的改动

| 变更 | 影响 |
|:---|:---|
| 删除 8 个测试文件 | `test_app_lifecycle.py`, `test_cli.py`×2, `test_daemon_launcher.py`, `test_tree_robustness.py`, `test_multi_fs_driver.py`, `test_view_manager_complex.py`, `test_view_manager_services.py`, `test_config_strict.py`, `test_sighup_lifecycle.py` — 削弱现有功能测试覆盖 |
| 删除 multi-fs on-demand scan | `api.py` 和 `driver.py` 中移除了 `on_demand_scan` 参数和 `trigger_on_demand_scan()` — 上一个 feature 分支刚加入的功能 |
| 简化 SIGHUP handler | 删除 ViewRouter 刷新和 ViewManager cleanup — 热重载功能退化 |

---

## 四、合并建议

**建议暂不合并**，需先解决以下 Action Items：

| 优先级 | Action Item | 涉及文件 |
|:---:|:---|:---|
| 🔴 P0 | 恢复 `main.py` SIGHUP handler 中的 ViewRouter 刷新和 ViewManager cleanup 逻辑 | `fusion/src/fustor_fusion/main.py` |
| 🔴 P0 | 为远程配置推送增加语义校验（复用 `ConfigValidator`） | `agent/runtime/pipe/command.py` |
| 🟡 P1 | 使用 `hmac.compare_digest` 进行 API Key 比较 | `fusion/api/management.py` |
| 🟡 P1 | 解决 UI 认证问题：当 `management_api_key` 已配置时，UI 需要能传递 key | `ui/management.html` |
| 🟡 P1 | 撤销删除既有测试文件，或提供替代测试 | 8 个被删除的测试文件 |
| 🟢 P2 | Phase 4 未实现可接受（分阶段交付），但 spec 中应标注 Phase 4 为 "Planned" | `specs/10-MANAGEMENT_PLANE.md` |
