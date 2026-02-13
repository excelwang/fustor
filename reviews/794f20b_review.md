# Code Review: `794f20b`

> **Title**: feat: Add AGENT_ID to agent configuration and correct Fusion config path in hot reload tests  
> **Author**: excel  
> **Date**: 2026-02-13  
> **Scope**: 10 files, +194 / -126

---

## 变更概览

| 文件 | 变更类型 | 说明 |
|:---|:---|:---|
| `agent/cli.py` | 清理 | 移除 `reload()` 中重复的 `import os, signal` |
| `view-fs/tree.py` | Bug Fix | `remove_node` 文件删除路径补查 parent |
| `fusion/main.py` | 新功能 | SIGHUP handler 增加 view router 热刷新 |
| `e2e/config/agent-config/default.yaml` | 配置 | 新增 `agent_id` 环境变量 |
| `e2e/config/fusion-config/default.yaml` | 配置 | 新增 `global-multi-fs` view |
| `e2e/fixtures/agents.py` | 测试 | 传递 `AGENT_ID` 环境变量 |
| `e2e/test_i8_hot_reload.py` | 重构 | 大幅重构热加载测试 |
| `Dockerfile` | 配置 | 加装 `view-multi-fs` |
| `docker-compose.yml` | 配置 | 挂载 `view-multi-fs` 源码 |
| `.env_state` | 自动 | 环境指纹更新 |

---

## 🔴 Issue 1: YAML 配置重复 key（必须修复）

**文件**: `tests/e2e/config/fusion-config/default.yaml` L34-36

```yaml
  archive-fanout:
    driver: fs
    driver_params:        # ← 第一个 driver_params（无值，解析为 null）
    driver_params:        # ← 第二个 driver_params（覆盖前者）
      root_dir: /data/fusion/archive_fanout
```

**问题**: YAML 中同一层级出现两个 `driver_params` key。YAML spec 规定后者覆盖前者，所以最终值是 `{root_dir: ...}`（恰好正确），但这是明显的编辑残留，可能导致：
- 代码审查混淆
- 某些严格 YAML 解析器报错

**修复**:

```diff
  archive-fanout:
    driver: fs
-   driver_params:
    driver_params:
      root_dir: /data/fusion/archive_fanout
```

---

## 🟡 Issue 2: SIGHUP handler 缩进不一致

**文件**: `fusion/src/fustor_fusion/main.py` L114-125

```python
    def handle_reload():
        logger.info("Received SIGHUP - initiating hot reload")
        # Reload Pipes
        asyncio.create_task(pm.reload())
        
        try:
             from .api.views import setup_view_routers  # 5空格缩进
             setup_view_routers()
             
             if hasattr(runtime_objects, 'view_managers'):
                 runtime_objects.view_managers.clear()    # 5空格
                 logger.info("Cleared ViewManager cache")
                 
             logger.info("Refreshed View API routers")
        except Exception as e:
             logger.error(f"Failed to refresh view routers: {e}")  # 5空格
```

**问题**: `try` 块内部使用了 **5 空格缩进**（13 spaces = 4+4+5），与项目标准的 4 空格不一致。Python 不会报错（因为 consistent within block），但违反了 PEP 8 和项目约定。

**修复**:

```diff
         try:
-             from .api.views import setup_view_routers
-             setup_view_routers()
-             
-             # Clear ViewManager cache to force re-init of drivers (e.g. multi-fs members)
-             if hasattr(runtime_objects, 'view_managers'):
-                 runtime_objects.view_managers.clear()
-                 logger.info("Cleared ViewManager cache")
-                 
-             logger.info("Refreshed View API routers")
+            from .api.views import setup_view_routers
+            setup_view_routers()
+            
+            # Clear ViewManager cache to force re-init of drivers (e.g. multi-fs members)
+            if hasattr(runtime_objects, 'view_managers'):
+                runtime_objects.view_managers.clear()
+                logger.info("Cleared ViewManager cache")
+                
+            logger.info("Refreshed View API routers")
         except Exception as e:
-             logger.error(f"Failed to refresh view routers: {e}")
+            logger.error(f"Failed to refresh view routers: {e}")
```

---

## 🟡 Issue 3: ViewManager 缓存清理未调用 cleanup

**文件**: `fusion/src/fustor_fusion/main.py` L119-120

```python
if hasattr(runtime_objects, 'view_managers'):
    runtime_objects.view_managers.clear()
```

**问题**: 直接 `clear()` 了 `view_managers` 字典，但没有对已有的 ViewManager 实例调用任何 cleanup / close 方法。如果 View Driver 内部持有：
- 文件描述符（如 multi-fs 的成员连接）
- 后台任务（如 polling tasks）
- 缓存的数据结构

这些资源会泄漏。

**修复建议**:

```python
if hasattr(runtime_objects, 'view_managers'):
    for name, mgr in runtime_objects.view_managers.items():
        try:
            if hasattr(mgr, 'close'):
                await mgr.close()  # 注意: handle_reload 是同步函数
        except Exception as e:
            logger.warning(f"Error closing view manager {name}: {e}")
    runtime_objects.view_managers.clear()
    logger.info("Cleared ViewManager cache")
```

> **注意**: `handle_reload` 当前是同步函数（`def handle_reload`），若 driver 的 cleanup 是 async 的，需要额外处理。可参考 `pm.reload()` 已经使用 `asyncio.create_task()` 的模式，将 cleanup 也包装成 task。

---

## ✅ 亮点

### tree.py Bug Fix（L172）

```python
+ parent = self.state.directory_path_map.get(parent_path)
  if parent:
      parent.children.pop(name, None)
```

这是一个**关键修复**。之前文件节点删除路径中 `parent` 变量**未赋值**，`if parent:` 要么引用了上方 `dir_node` 分支中的 `parent`（逻辑错误），要么直接 `NameError`。这导致删除文件时不会从父目录的 `children` 中移除，产生**幽灵引用**。

审查确认：目录删除分支（L162）已有正确的 `parent` 查询，此修复使文件删除分支对齐，✅ 正确。

### test_i8 重构

- 增加 `try/finally` 确保 cleanup
- 重试次数从 10 → 15
- 移除过于严格的 `status == "ok"` 断言
- 使用 `restore_file` 恢复配置

测试健壮性显著提升，✅ Good practice。

### AGENT_ID 引入

Agent config 模板和 fixture 中统一传递 `AGENT_ID`，使 heartbeat 中 `agent_status.agent_id` 有可靠来源，与管理平面的 agent 聚合逻辑对齐。✅ 正确。
