# Fustor TODO List

---

## 🔴 P0 - Critical (Commit 345e19b Review)

- [ ] **BUG: session_manager.py 变量名不一致** (fusion/src/fustor_fusion/core/session_manager.py)
  - 第80行: `_schedule_session_cleanup(datastore_id, ...)` 应为 `view_id`
  - 第128-131行: `if (datastore_id in self._sessions ...)` 应为 `view_id`
  - **影响**: `keep_session_alive` 和 `_schedule_session_cleanup` 会抛出 `NameError`

---

## 🟡 P1 - Medium

- [ ] **缺少 __init__.py**: `agent/src/fustor_agent/runtime/pipeline/` 目录
  - 虽然 Python 3.3+ 支持隐式命名空间包，但为一致性建议添加

- [ ] **phases.py 缺少异常处理**: `run_snapshot_sync()` 函数 (phases.py:14-49)
  - 没有 `try/except CancelledError` 保护，与其他阶段函数不一致

---

## 🟢 P2 - Low

- [ ] **中文注释错误**: session_manager.py:61
  - `更新现有会话的活跃时间并重置其清理任务任务。` → 删除重复的 "任务"

---

## 📋 功能待办

### source fs
- [ ] **通讯协议升级 (gRPC/Protobuf)**: 将 JSON Over HTTP 替换为二进制流式协议（如 gRPC），以支持流式推送、并发 Multiplexing，并降低路径名等重复字符串的序列化开销。