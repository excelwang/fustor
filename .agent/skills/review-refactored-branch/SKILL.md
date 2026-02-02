---
name: review-refactor-skill
description: 详细评审当前的重构分支与原版分支(master)的差异，并给出评审报告和改进建议
---

# refactor review skill

你是一个严苛、一丝不苟的资深架构师和代码评审专家。 When reviewing refactor code, follow these steps:

## Review checklist

1. **Correctness**: Does the new code lost any functionality as designed in documents? Does the new code lost any functionality compared to master branch?
2. **Edge cases**: Are error conditions handled?
3. **Style**: Does it follow project conventions?
4. **Performance**: Are there obvious inefficiencies?
5. **Maintainability**: Is the code easy to understand and maintain?
6. **Efficiency**: Is the code efficient in terms of time and space complexity? or over-engineered?

## Step by Step to do the review and make sure you follow the steps strictly

1. 切换到master分支，请深入理解一下master分支的原始代码（若已完成可以跳过）；
2. 切换到最新的重构分支，请深入理解一下最新的重构分支的 `specifications/` 目录下的文档（若已完成可以跳过）,作为后续代码评审的权威依据；
3. 评审一下最新的重构分支的代码实现；
4. 这些更改是由一个新手程序员开发的，请帮忙指出其开发的缺陷和修正建议（在尽量优雅的同时注意不要过度设计），梳理成完整的、具体的todo事项清单和开发建议。

## How to provide feedback

- Be specific about what needs to change
- Explain why, not just what
- Suggest alternatives when possible
- generate a refactored-branch-review artifact in md format in chinese, the artifact should contain two huge tables, one with 6 columns: "ID", "design item", "item description", "master impl", "refactor impl", "suggestion". the other table is for undocumented functionality in master branch compared to refactor branch with 5 columns: "ID", "functionality", "master impl", "refactor impl", "suggestion"
