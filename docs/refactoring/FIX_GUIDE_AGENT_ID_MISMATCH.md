# 测试修复开发指导

> **针对**: Commit dc9684b 的两个失败测试  
> **失败测试**: `test_c5_sentinel_sweep.py`, `test_a3_session_recovery.py`  
> **创建时间**: 2026-02-02 11:10

---

## 一、问题诊断

### 1.1 测试运行结果

```
FAILED test_c5_sentinel_sweep.py::TestSentinelSweep::test_follower_does_not_perform_sentinel
FAILED test_a3_session_recovery.py::TestSessionRecovery::test_agent_recovers_after_session_terminated
```

### 1.2 根本原因

**两个测试失败的原因相同**：Agent ID 格式不匹配。

在 `agents.py` 中，Agent ID 的格式为：
```python
agent_id = f"{container_name.replace('fustor-nfs-', '')}-{os.urandom(2).hex()}"
# 结果: "client-a-1787:sync-task-1"
```

但测试代码错误地使用 `"agent-a"` 进行匹配：
```python
# 错误：
assert "agent-a" in leader_session.get("agent_id", "")  # ❌ 找不到

# 正确：
assert "client-a" in leader_session.get("agent_id", "")  # ✅ 匹配成功
```

---

## 二、具体错误分析

### 2.1 错误 #1: `test_follower_does_not_perform_sentinel`

**文件**: `it/consistency/test_c5_sentinel_sweep.py`, Line 97

**错误信息**:
```
AssertionError: assert 'agent-a' in 'client-a-1787:sync-task-1'
```

**问题代码**:
```python
def test_follower_does_not_perform_sentinel(self, setup_agents, fusion_client):
    leader_session = fusion_client.get_leader_session()
    assert leader_session is not None, "Leader session must exist"
    assert "agent-a" in leader_session.get("agent_id", "")  # ❌ 错误
```

**修复方案**: 将 `"agent-a"` 改为 `"client-a"`

---

### 2.2 错误 #2: `test_agent_recovers_after_session_terminated`

**文件**: `it/consistency/test_a3_session_recovery.py`, Line 34, 58

**错误信息**:
```
AssertionError: Agent A must be leader initially
assert None is not None
```

**问题代码**: 测试在多个位置使用了错误的匹配模式
```python
# Line 34
leader = next((s for s in sessions if "agent-a" in s.get("agent_id", "")), None)  # ❌

# Line 58
agent_a_sessions = [s for s in sessions if "agent-a" in s.get("agent_id", "")]  # ❌
```

**修复方案**: 将所有 `"agent-a"` 改为 `"client-a"`

---

## 三、修复代码

### 3.1 修复 `test_c5_sentinel_sweep.py`

在文件 Line 97 做如下修改：

```python
# Before（第 97 行）
assert "agent-a" in leader_session.get("agent_id", "")

# After
assert "client-a" in leader_session.get("agent_id", "")
```

---

### 3.2 修复 `test_a3_session_recovery.py`

在文件 Line 34 和 Line 58 做修改：

```python
# ========== Line 34 ==========
# Before
leader = next((s for s in sessions if "agent-a" in s.get("agent_id", "")), None)

# After
leader = next((s for s in sessions if "client-a" in s.get("agent_id", "")), None)


# ========== Line 58 ==========
# Before
agent_a_sessions = [s for s in sessions if "agent-a" in s.get("agent_id", "")]

# After
agent_a_sessions = [s for s in sessions if "client-a" in s.get("agent_id", "")]
```

---

## 四、如何执行修复

### 方法 1: 手动修改
打开文件手动修改上述行。

### 方法 2: 使用 sed 命令
```bash
# 修复 test_c5_sentinel_sweep.py
sed -i 's/"agent-a"/"client-a"/g' it/consistency/test_c5_sentinel_sweep.py

# 修复 test_a3_session_recovery.py  
sed -i 's/"agent-a"/"client-a"/g' it/consistency/test_a3_session_recovery.py
```

---

## 五、验证修复

运行测试确认修复成功：

```bash
cd it && uv run pytest consistency/test_c5_sentinel_sweep.py consistency/test_a3_session_recovery.py -vs
```

预期结果：
```
==================== 3 passed in XX.XXs ====================
```

---

## 六、经验教训

### 6.1 为什么会犯这个错误？

1. **命名不一致**：`agents.py` 中的 `ensure_agent_running` 使用 `container_name.replace('fustor-nfs-', '')`，生成的 ID 是 `client-a`，而不是 `agent-a`。
2. **未阅读关联代码**：在写测试时没有先确认 Agent ID 的实际格式。

### 6.2 如何避免类似错误？

1. **使用常量**：将匹配模式定义为常量，例如在 `constants.py` 中添加：
   ```python
   AGENT_A_ID_PATTERN = "client-a"
   AGENT_B_ID_PATTERN = "client-b"
   ```

2. **阅读上游代码**：在编写测试前，先阅读 `agents.py` 中的 `agent_id` 生成逻辑。

3. **打印调试**：添加日志打印实际的 `agent_id` 值，便于调试：
   ```python
   logger.debug(f"Sessions: {sessions}")
   ```

---

## 七、所有需要修复的文件清单

经过搜索，以下文件都使用了错误的 `"agent-a"` / `"agent-b"` 模式：

| 文件 | 行号 | 需修改内容 |
|------|------|-----------|
| `test_a1_leader_election_first.py` | 45 | `"agent-a"` → `"client-a"` |
| `test_a2_follower_io_isolation.py` | 37 | `"agent-b"` → `"client-b"` |
| `test_a3_session_recovery.py` | 34, 58 | `"agent-a"` → `"client-a"` |
| `test_c5_sentinel_sweep.py` | 97 | `"agent-a"` → `"client-a"` |
| `test_e1_leader_failover.py` | 41, 43, 72 | 全部替换 |
| `test_e2_new_leader_duties.py` | 67, 221-231 | 全部替换 |

### 快速修复命令

```bash
cd it/consistency

# 批量替换所有文件
sed -i 's/"agent-a"/"client-a"/g' test_*.py
sed -i 's/"agent-b"/"client-b"/g' test_*.py
```

### 验证修复

```bash
# 检查是否还有遗漏
grep -rn '"agent-a"\|"agent-b"' .

# 运行所有相关测试
cd .. && uv run pytest consistency/test_a1*.py consistency/test_a2*.py consistency/test_a3*.py consistency/test_c5*.py consistency/test_e1*.py consistency/test_e2*.py -v
```

---

## 八、修复后的代码审查

修复后请确保：
1. 所有测试通过
2. 代码风格与现有代码保持一致
3. 考虑添加更清晰的注释说明 Agent ID 格式
