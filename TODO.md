# Fustor TODO List

---

## 🔴 P0 - Critical (Commit fb376fb + 8d8fe1b Review)

- [x] ~~**BUG: session_manager.py 变量名不一致**~~ (已修复 @ fb376fb)

- [ ] **BUG: 缺少 `get_leader` 方法** (fusion/src/fustor_fusion/datastore_state_manager.py)
  - `fusion_pipeline.py:382` 调用 `datastore_state_manager.get_leader(self.view_id)`
  - 但 `DatastoreStateManager` 没有 `get_leader` 方法
  - **影响**: `FusionPipeline.get_dto()` 崩溃

- [ ] **BUG: Leader 角色未正确传递**
  - `FusionPipeline.get_session_role()` 总是返回 "follower"
  - `on_session_created()` 没有调用 `try_become_leader()`
  - **影响**: 4个测试失败
    - `test_session_created_first_is_leader`
    - `test_session_created_second_is_follower`
    - `test_leader_election_on_close`
    - `test_dto`

---

## 🟡 P1 - Medium

- [x] ~~**缺少 __init__.py**~~ (已修复 @ 8d8fe1b)

- [x] ~~**phases.py 缺少异常处理**~~ (已修复 @ fb376fb)

---

## 🟢 P2 - Low

- [ ] **中文注释错误**: session_manager.py:61
  - `更新现有会话的活跃时间并重置其清理任务任务。` → 删除重复的 "任务"

- [ ] **FusionPipeline.leader_session 属性返回 None**
  - 移除了内部 `_leader_session` 后，属性直接返回 `None`
  - 应该改为 async 方法或移除此属性

---

## 📋 功能待办

### source fs
- [ ] **通讯协议升级 (gRPC/Protobuf)**: 将 JSON Over HTTP 替换为二进制流式协议（如 gRPC），以支持流式推送、并发 Multiplexing，并降低路径名等重复字符串的序列化开销。