---
name: refactored-test-review-skill
description: 详细评审当前分支的测试代码，并给出评审报告和改进建议
---

# refactored test review skill

When reviewing refactored test code, follow these steps:

## Review checklist

1. **Edge cases**: Are error conditions handled?
2. **Style**: Does it follow project conventions?
3. **Performance**: Are there obvious inefficiencies?
4. **Over-testing**: Are there redundant tests?
5. **Test coverage**: Are there missing tests?

## How to use it

1. 请深入理解一下原版(master分支)的测试代码；
2. 请深入理解一下当前分支的测试代码；
3. 比较两者，找出当前分支的缺陷和修正建议，梳理成完整的、具体的todo事项清单和开发建议。

## How to provide feedback

- Be specific about what needs to change
- Explain why, not just what
- Suggest alternatives when possible
- write in a document in chinese for the newbie developer to read.
