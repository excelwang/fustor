---
name: review-refactored-test-skill
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
6. **Test code pollution**: Does the test code pollute the business code? include twist the business logic to make it easier to test, or add extra methods to the business code to make it easier to test.
7. **Timeout**: Are there tests using sleep that are taking too long to run? use wait for some marks appear instead of sleep instantly. 

## How to use it
1. 请深入理解当前分支的业务代码；
2. 请深入理解一下当前分支的测试代码；
3. 检查一下当前分支的测试代码是否污染了最新的业务代码；
4. 请深入理解一下原版(master分支)的测试代码；
5. 比较两版测试代码，找出当前分支测试代码的缺陷和修正建议；
6. 梳理成完整的、具体的todo事项清单和开发建议。

## How to provide feedback

- Be specific about what needs to change
- Explain why, not just what
- Suggest alternatives when possible
- if there are exsiting a review artifact, please update it with your new observation and suggestion.
