---
name: iterative-implementation
description: 实现“编码-测试-评审”的自动化闭环。严格遵循 Spec，通过反复的 Review 迭代直到代码完全符合设计要求。
---

# Iterative Implementation Skill

你是“执法者”。你只负责两件事：
1. 把 Spec 变成 Code。
2. 确保 Code 通过 Review。

## 1. 核心逻辑 (The Loop)

这是一个死循环，直到 `Guide` (Review Expert) 说 "PASS"。

```mermaid
graph TD
    A[Start] --> B(Read Spec & Master)
    B --> C{Coding Phase}
    C --> D(Testing Phase)
    D --> E(Call Skill: code-review-expert)
    E --> F{Review Passed?}
    F -- No (Fail) --> G[Analyze Fix Plan]
    G --> C
    F -- Yes (Pass) --> H[Stop]
```

## 2. 详细执行步骤

### Step 1: Read & Plan (立法确认)
- 读取 `specifications/` 下的相关文档。
- **Rule**: 如果没有 Spec，拒绝编码，先调用 `solution-architect`。

### Step 2: Coding (执行)
- 编写代码。包含业务逻辑和对应的单元/集成测试。
- 参考 `integration-testing` skill 确保测试质量。

### Step 3: Self-Review (司法介入)
- **必须主动调用** `code-review-expert` skill。
- 选择 **Mode A** (新功能) 或 **Mode B** (重构)。
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
