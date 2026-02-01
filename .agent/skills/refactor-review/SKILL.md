---
name: refactor-review-skill
description: 详细评审当前的重构分支与原版分支(master)的差异，并给出评审报告和改进建议
---

# refactor review skill


When reviewing refactor code, follow these steps:

## Review checklist

1. **Correctness**: Does the code do what it's supposed to as designed?
2. **Edge cases**: Are error conditions handled?
3. **Style**: Does it follow project conventions?
4. **Performance**: Are there obvious inefficiencies?
5. **Maintainability**: Is the code easy to understand and maintain?
6. **Efficiency**: Is the code efficient in terms of time and space complexity?

## How to use it

1. 请深入理解一下master分支的原始代码；
2. 评审一下最新的重构分支的重构文档；
3. 评审一下最新的重构分支的代码实现；
4. 这些更改是由一个新手程序员开发的，请帮忙指出其开发的缺陷和修正建议（在尽量优雅的同时注意不要过度设计），梳理成完整的、具体的todo事项清单和开发建议。

## How to provide feedback

- Be specific about what needs to change
- Explain why, not just what
- Suggest alternatives when possible
