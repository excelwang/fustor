---
name: iterative-implementation
description: 实现“编码-测试-评审”的自动化闭环。严格遵循 Spec，通过反复的 Review 迭代直到代码完全符合设计要求。
---

# Iterative Implementation Skill

你是“执法者”。你只负责两件事：
1. 把 Spec 变成 Code。
2. 确保 Code 通过 Review。

## 1. 核心逻辑 (The Loop)

这是一个死循环，直到 `Code Review Expert` 说 "PASS"。

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
- 前置：必须先运行 `solution-architect` 产出 Spec。
- 启动：直接进入 Coding Phase。

**Case B: 既有代码接手 (Refactoring/Continuing)**
- **Step 0: Spec Alignment (归位)**
   1. 检查是否存在对应的 `specifications/xxx.md`。
   2. **如果不存在**：立即调用 `solution-architect`，通过**逆向工程** (Reverse Engineering) 阅读现有代码和需求，补全 Spec。
   3. **如果存在**：阅读 Spec 和当前代码，建立基准认知。
- **Step 1: Baseline Review (基线审查)**
   - 在修改任何代码前，先运行一次 `code-review-expert` (Mode B)。
   - 目的：明确当前代码与 Spec 的差距，生成初始的任务清单。

## 3. 详细执行步骤 (Loop Execution)

### Step 2: Coding (执行)
- 编写代码。包含业务逻辑和对应的单元/集成测试。
- **Call Skill**: 调用 `test-engineer` 执行测试与验证。
  - Type A: 新写功能测一遍。
  - Type B: 跑一遍相关回归。
- **Self-Correction**: 如果 `test-engineer` 报告失败，直接在 Step 2 内部循环修复，直到测试通过，**然后再**进入 Step 3。

### Step 3: Self-Review (司法介入)
- **必须主动调用** `code-review-expert` skill。
- **Auto-Select Mode (智能模式选择)**:
  1. 执行 `git branch --show-current` 获取当前分支名。
  2. **Mode B (Refactor/Migration)**: 如果分支名匹配 `refactor/*`, `migration/*` 或 `fix/legacy-*`。
     - 重点：与 Master 分支进行 Feature Parity 对比。
  3. **Mode A (Feature/Bugfix)**: 如果分支名匹配 `feature/*`, `feat/*`, `fix/*` (非 legacy)。
     - 重点：与 Spec 进行 Design Compliance 对比。
  4. **Mode C (Test Only)**: 如果仅修改了 `tests/` 或 `it/` 目录下的文件。
- 传入参数：当前分支 vs Spec文档。

### Step 4: Decision (判决)
阅读 Review 输出的两个表格：
1. **Case 1: Table 1 (Consistency) 有差异** -> **REPEAT**。
   - 读取 Suggestion，制定 Fix Plan。
   - 回到 Step 2 进行修复。
2. **Case 2: Table 2 (Gaps) 有缺失** -> **REPEAT**。
   - 补充缺失的功能。
   - 回到 Step 2。
3. **Case 3: Tables clean / Review OK** -> **COMMIT & STOP**。

## 3. 防死循环机制
- 在内存中记录迭代次数 `Iteration_Count`。
- 如果 `Iteration_Count >= 3` 且 Review 仍未通过：
  - **Stop**。
  - 向用户报错："无法满足设计要求，设计可能存在逻辑漏洞或实现难度过大，请人工介入。"
