---
name: loopi
description: 实现“编码-测试-评审”的自动化闭环。严格遵循 Spec，通过反复的 Review 迭代直到代码完全符合设计要求。
---

# Iterative Impl Skill

你是“执法者”。你只负责两件事：
1. 把 Spec 变成 Code。
2. 确保 Code 通过 Review。

## 1. 核心逻辑 (The Loop)

这是一个死循环，直到 `code-review-expert` 说 "PASS"。

```mermaid
graph TD
    A[Start] --> B(Alignment Check)
    B --> C{Coding Phase}
    C --> D(Testing Phase)
    D --> E(Call Skill: code-review-expert)
    E --> F{Review Passed?}
    F -- No (Fail) --> G[Analyze Fix Plan]
    G --> C
    F -- Yes (Pass) --> H[Stop]
```

## 2. Session Manager (Identity & Resume)

在开始任何工作前，必须先确定"我是谁"。

### Step 0: Session Identification
**Action**:
1. 读取 `.agent/sessions/active/` 目录下的所有 JSON 文件。
2. **List & Ask**: 向用户展示当前活跃的 Session 列表。
   - Option [R]: **Resume** <Session_ID>
   - Option [N]: **New Session** (创建一个新会话)
     > **Smart Recommendation (Context Affinity)**:
     > 如果刚完成任务或有遗留上下文，优先推荐亲和度高的任务：
     > - **Score Rule**: 
     >   - `+20` Same Parent Task/Sequence
     >   - `+10` Same Domain Spec
     >   - `+5`  Overlapping File Paths
     > - **Display**: "[HIGH AFFINITY] Task_002 (Reuses loaded context)"
   - Option [C]: **Clear/Wipe** (强制清除某些过期的僵尸 Session)

### Step 1: Initialization
- **If Resume**:
  - 读取选定 Session 的 JSON 文件。
  - 恢复 `Base Commit` 和 `Iteration Count`。
  - 读取 `.agent/tasks/active_task.md` (如果存在且匹配)。
- **If New Session**:
  - 生成新的 Session ID (e.g., `sess_{timestamp}_{random}`).
  - 创建 `.agent/sessions/active/{session_id}.json`，状态标记为 `Created`。
  - 询问用户要认领哪个 Task (Spec)。

### Step 2: Spec Alignment (归位)

**Case A: 全新开发**
- 前置：必须先运行 `soarch` 输出 Task 文档至 `.agent/tasks/backlog/`。
- 启动：认领 Task，移动至 `.agent/tasks/active/`，直接进入 Coding Phase。

**Case B: 既有代码接手 (Refactoring/Continuing)**
- **Step 0: Task Alignment (归位)**
   1. 检查 `.agent/tasks/` 下是否存在对应的 `TASK_[ID].md`。
   2. **如果不存在**：调用 `soarch`，逆向生成 Task 文档。
   3. **如果存在**：阅读 Task 和引用的 Spec，建立基准认知。
- **Step 1: Baseline Review (基线审查)**
   - 运行 `cre` (Mode B) 对比代码与 Domain Spec。

### Step 1.5: Task Refinement (动态调整)
如果在编码过程中发现任务过大或被阻塞：
- **Action**: 调用 `soarch` REQUEST_SPLIT。
- **Result**: 当前任务 Paused，拆分为新的小任务。重新开始 Step 0。

## 3. 详细执行步骤 (Loop Execution)

### Step 2: Coding Loop (积攒提交)
- **Commit Strategy (提交粒度)**:
  - 遵循 **"逻辑完整性 (Logical Completeness)"** 原则。
  - 不要改一行就提交，也不要等完全部做完才提交。
  - **Action**: 每完成一个独立的子任务（Sub-task，如"定义数据结构"、"实现核心算法"、"完成单测"）后，**必须调用 `tester` skill 执行验证**。测试通过后，**立即执行 `git commit`**。这作为 Checkpoint，防止后续搞砸。
  - **Loop Condition**: 如果当前 Spec Step 或功能模块尚未全部完成，继续执行 Step 2，积攒更多的 Commits。仅当一个完整的 Feature 或 Step 完成时，才进入 Step 3。

### Step 3: Self-Review (批量审查)
- **Review Scope (审查范围)**:
  - 必须审查 **Accumulated Diff (累积差异)**，即从任务开始时的基准点 (`Base Commit`) 到当前的 `HEAD`。
  - Command: `git diff <Base_Commit_ID>...HEAD`
  - 这样可以一次性审查那一批积攒的 Commits，且自动过滤掉中间过程的反复修改（如 A 加了又在 B 删了）。
- **Auto-Select Mode (智能模式选择)**:
  1. 执行 `git branch --show-current` 获取当前分支名。
  2. **Mode B (Refactor/Migration)**: 如果分支名匹配 `refactor/*`, `migration/*` 或 `fix/legacy-*`。
     - 重点：与 Master 分支进行 Feature Parity 对比。
  3. **Mode A (Feature/Bugfix)**: 如果分支名匹配 `feature/*`, `feat/*`, `fix/*` (非 legacy)。
     - 重点：与 Spec 进行 Design Compliance 对比。
  4. **Mode C (Test Only)**: 如果仅修改了 `tests/` 或 `it/` 目录下的文件。
- **Action**: 主动调用 `code-review-expert` skill，传入上述 Diff 内容。

### Step 4: Decision (判决)
阅读 Review 输出的两个表格：
1. **Case 1: Table 1 (Consistency) 有差异** -> **REPEAT**。
   - 读取 Suggestion，制定 Fix Plan。
   - 回到 Step 2 进行修复。
2. **Case 2: Table 2 (Gaps) 有缺失** -> **REPEAT**。
   - 补充缺失的功能。
   - 回到 Step 2。
3. **Case 3: Tables clean / Review OK** -> **COMMIT & STOP**。

## 4. 状态持久化 (Task Persistence)

为了支持断点续做，必须在关键节点（调用专家前后、流程结束时）更新 `.agent/current_task.md`。
**原则**: 只记录当前最新快照，不记流水账，节省 Token。

**Trigger Points**:
1. 调用 `soarch`, `tester`, `code-review-expert` **之前**。
2. 收到专家反馈 **之后**。
3. 流程 **结束时**。

**File Template**:
```markdown
# Current Task: [Task Description]
> Last Update: 202X-XX-XX HH:MM
> Global Status: Coding | Reviewing | Fixing

## Current Focus
- Branch: refactor/xxx
- Base Commit: [Commit Hash] (Critical for Review Diff)
- Spec: specs/yyy.md

## Latest Checkpoint
- [x] Spec Created
- [ ] Coding (Sub-task: A, B Done)
- [ ] Test Verification (Pending)
- [ ] Review Status (Last: FAIL - 3 issues remaining)
```
