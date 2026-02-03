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

## 2. 启动条件与初始化 (Entry & Init)

**Case A: 全新开发**
- 前置：必须先运行 `soarch` 产出 Spec。
- 启动：直接进入 Coding Phase。

**Case B: 既有代码接手 (Refactoring/Continuing)**
- **Step 0: Spec Alignment (归位)**
   1. 检查是否存在对应的 `specs/xxx.md`。
   2. **如果不存在**：立即调用 `soarch`，通过**逆向工程** (Reverse Engineering) 阅读现有代码和需求，补全 Spec。
   3. **如果存在**：阅读 Spec 和当前代码，建立基准认知。
- **Step 1: Baseline Review (基线审查)**
   - 在修改任何代码前，先运行一次 `code-review-expert` (Mode B)。
   - 目的：明确当前代码与 Spec 的差距，生成初始的任务清单。

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
