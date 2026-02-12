# Multi-FS View 分支修复指南

> 分支: `feature/multi-fs-view`  
> 日期: 2026-02-13  
> 面向: 初级开发者

本文档列出合并前必须修复的 3 个阻塞性 Bug，每个都附带了**问题原因、定位方式、修复方案和验证方法**。

---

## BUG-1: 配置双重嵌套导致 `members` 为空

### 问题

`MultiFSViewDriver` 初始化时取不到 `members` 配置，聚合查询永远返回空结果。

### 原因

Fusion 的 `ViewManager` 在创建 Driver 实例时，传入的 `config` 参数**已经是 `driver_params` 的内容**：

```python
# fusion/src/fustor_fusion/view_manager/manager.py, 约 L107-L113
driver_params = config.driver_params   # ← 已经提取出 driver_params
driver_instance = driver_cls(
    id=view_name,
    view_id=self.view_id,
    config=driver_params                # ← 传入的是 driver_params 本身
)
```

但 `MultiFSViewDriver.__init__` 又尝试从中再取一层 `driver_params`：

```python
# extensions/view-multi-fs/src/fustor_view_multi_fs/driver.py, L18
self.members = config.get("driver_params", {}).get("members", [])
#                         ^^^^^^^^^^^^^^^^ 多取了一层，永远是空 dict
```

### 修复

**文件**: `extensions/view-multi-fs/src/fustor_view_multi_fs/driver.py`

```diff
- self.members: List[str] = config.get("driver_params", {}).get("members", [])
+ self.members: List[str] = config.get("members", [])
```

### 验证

修改后运行单元测试：

```bash
cd extensions/view-multi-fs
pytest tests/ -v
```

确认 `test_driver_aggregation` 中 `len(stats_result["members"]) == 3` 通过。

> [!TIP]
> 查看同类 Driver（如 `FSViewDriver.__init__`）如何使用 `config` 参数可以帮助理解这个模式。所有 ViewDriver 收到的 `config` 都是已经解包的 `driver_params` dict。

---

## BUG-2: `target_schema` 未显式设置

### 问题

`MultiFSViewDriver` 设置了 `schema_name = "multi-fs"`，但没有设置 `target_schema`。这可能导致 Driver 被注册到错误的 schema key 下。

### 原因

`_load_view_drivers()` 在注册 Driver 时使用：

```python
# fusion/src/fustor_fusion/view_manager/manager.py, L47
key = getattr(driver_cls, 'target_schema', None) or ep.name
```

由于 `MultiFSViewDriver` 没有定义 `target_schema`，会 fallback 到 entry point name `"multi-fs"`，**这部分恰好正确**。

但在事件处理路由中：

```python
# manager.py, L154
target = getattr(driver_instance, "target_schema", "")
if target and target != event.event_schema:
    continue  # 跳过不匹配的 driver
```

因为 `target_schema` 未定义（从父类继承默认值 `""`），事件不会被过滤——**Multi-FS View 的 `process_event` 会被调用，虽然它只返回 `True`，但这是不必要的开销**。

### 修复

**文件**: `extensions/view-multi-fs/src/fustor_view_multi_fs/driver.py`

在类定义中显式声明 `target_schema`，同时保留 `schema_name` 用于 entry point 注册：

```diff
  class MultiFSViewDriver(ViewDriver):
      schema_name = "multi-fs"
+     target_schema = "multi-fs"  # 不匹配 "fs" 事件，避免无意义调用
```

### 验证

设置 `target_schema = "multi-fs"` 后，`ViewManager.process_event()` 在处理 `event_schema="fs"` 的事件时会跳过此 Driver（因为 `"multi-fs" != "fs"`），符合 Spec §5.1 "不消费事件" 的设计。

---

## BUG-3: Readiness Checker 导致 Multi-FS View API 返回 503

### 问题

所有对 Multi-FS View 的 API 请求（`/shared-storage/stats`、`/shared-storage/tree`）都会被 `503 Service Unavailable` 拒绝。

### 原因

`setup_view_routers()` 为**每个配置的 View** 都创建了 readiness checker：

```python
# fusion/src/fustor_fusion/api/views.py, 约 L201
checker = make_readiness_checker(view_name)
```

`make_readiness_checker` 内部检查：

1. **是否有 Leader Session** — Multi-FS View 不绑定 Pipe，没有 Agent 连接，所以没有 session → **检查失败**
2. **Snapshot 是否完成** — Multi-FS View 不做 snapshot → **检查失败**

### 修复

**文件**: `fusion/src/fustor_fusion/api/views.py`

在 `setup_view_routers()` 中，对 `multi-fs` 类型的 View 跳过 readiness checker：

```diff
  for view_name, cfg in current_view_configs.items():
      driver_name = cfg.driver
      create_func = available_factories.get(driver_name)
      
      if create_func:
          try:
              async def get_driver_instance_for_instance(view_id: str, _key=view_name):
                  return await get_view_driver_instance(_key, _key)
              
-             checker = make_readiness_checker(view_name)
-             limit_checker = make_metadata_limit_checker(view_name)
+             # 聚合型 View（如 multi-fs）不绑定 Pipe，无需 readiness/limit 检查
+             if driver_name in ("multi-fs",):
+                 checker = None
+                 limit_checker = None
+             else:
+                 checker = make_readiness_checker(view_name)
+                 limit_checker = make_metadata_limit_checker(view_name)
```

同时，`create_multi_fs_router` 工厂函数（`api.py`）需要处理 `check_snapshot_func=None` 的情况——**当前实现已经不使用该参数**，所以无需额外修改。

### 验证

1. 启动 Fusion，配置一个 `driver: multi-fs` 的 View
2. 请求 `GET /shared-storage/stats?path=/` 
3. 预期：返回 200（而非 503）

> [!IMPORTANT]
> 修复时要注意，不要简单地全局跳过 readiness check。只对明确标记为聚合型的 Driver 类型跳过。未来如果有更多聚合型 Driver，可以考虑在 `ViewDriver` 基类中加一个 `is_aggregation_view: bool = False` 属性。

---

## 修复顺序建议

1. **先修 BUG-1**（最简单，一行改动），然后跑单元测试确认
2. **再修 BUG-2**（一行改动），理解 schema routing 逻辑
3. **最后修 BUG-3**（需要理解 API 注册流程），可以参考 `setup_view_routers()` 的完整代码

修完后，用以下命令跑全部相关测试：

```bash
# Multi-FS View 单元测试
pytest tests/view_multi_fs/ -v

# Lineage 验证
python tests/verify_lineage.py
```
