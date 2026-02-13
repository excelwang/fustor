# Code Review: 最近 3 个 Commits

> `1c9f40a` → `2e858c2` → `7ea76f3`  
> Date: 2026-02-13  

---

## Commit 1: `1c9f40a` — test(robustness)

> Add TreeManager, SIGHUP lifecycle, and strict YAML validation tests

| 文件 | 变更 | 评价 |
|:---|:---|:---|
| `drivers.py` | `ViewDriver` 新增 `close()` | ✅ |
| `test_tree_robustness.py` | 深路径删除 + parent 一致性 | ✅ |
| `test_sighup_lifecycle.py` | SIGHUP cleanup 验证 | ✅ |
| `test_config_strict.py` | 严格 YAML 重复 key 扫描 | ⚠️ 路径层级错误（见 commit 3）|

**Issue**: `get_all_yaml_files()` 中 `Path(__file__).parent.parent.parent.parent` 多跳了一层，会扫描到项目根的**上级目录**。在 commit 3 中已修复。

---

## Commit 2: `2e858c2` — feat: On-demand scan for multi-FS

> Add on-demand scan capability to multi-FS view API endpoints and driver

| 文件 | 变更 | 评价 |
|:---|:---|:---|
| `pyproject.toml` | 新增 `fustor-view-fs`, `fustor-fusion` 依赖 | ⚠️ 见下 |
| `api.py` | stats/tree 端点新增 `on_demand_scan` 参数 | ⚠️ 有问题 |
| `driver.py` | `trigger_on_demand_scan` 广播到所有成员 | ✅ 设计合理 |
| `views.py` | 空行调整 | ✅ |

### 🔴 Issue 1: `api.py` 中有 bare `except`

```python
# L142-145
try:
    result["job_id"] = json.loads(job_id)
except:               # ← bare except，应为 except (json.JSONDecodeError, TypeError):
    result["job_id"] = job_id
```

bare `except` 会捕获所有异常（包括 `KeyboardInterrupt`, `SystemExit`），违反 PEP 8。

**修复**:
```diff
-        except:
+        except (json.JSONDecodeError, TypeError):
```

### 🟡 Issue 2: `api.py` 缩进不一致

```python
# L70
if triggered:
     logger.info(...)   # ← 5 空格缩进
```

出现在两处（stats 和 tree），均为 5 空格而非项目标准 4 空格。

### 🟡 Issue 3: `pyproject.toml` 新增 `fustor-fusion` 作为依赖

`view-multi-fs` 是 Fusion 的一个扩展，现在**反向依赖** `fustor-fusion`。这创建了一个潜在的**循环依赖**：

```
fustor-fusion → (entry_point) → fustor-view-multi-fs → fustor-fusion
```

虽然 Python 的 entry_point 机制不会立即爆炸（因为是运行时加载），但在 `pip install` 时可能制造安装顺序问题。

**建议**: 检查 `driver.py` 中是否真正需要 `fustor-fusion`。如果只是为 `_get_member_driver` 获取 `ViewManager`，应通过依赖注入或 `getattr` 延迟导入而非硬依赖。

### ✅ 亮点

- `trigger_on_demand_scan` 的设计：广播到所有成员 → 收集结果 → 返回 composite job_id，允许调用方跟踪每个成员的扫描进度。
- 使用 `hasattr(driver, 'trigger_on_demand_scan')` 做能力检测，向后兼容不支持扫描的 driver。

---

## Commit 3: `7ea76f3` — fix(test): correct project root

> correct project root detection in test_config_strict.py

| 文件 | 变更 | 评价 |
|:---|:---|:---|
| `test_config_strict.py` | `parent.parent.parent.parent` → `parent.parent.parent` | ✅ 正确修复 |

路径链：`fusion/tests/test_config_strict.py` → `.parent` = `fusion/tests/` → `.parent` = `fusion/` → `.parent` = project root ✅

---

## 总结

| 严重度 | 问题 | 文件 | 状态 |
|:---|:---|:---|:---|
| 🔴 | bare `except` | `view-multi-fs/api.py` L144 | 需修复 |
| 🟡 | 5 空格缩进 | `view-multi-fs/api.py` L70, L108 | 建议修复 |
| 🟡 | 循环依赖风险 | `view-multi-fs/pyproject.toml` | 需评估 |
