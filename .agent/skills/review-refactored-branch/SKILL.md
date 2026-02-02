---
name: review-refactor-skill
description: 详细评审当前的重构分支与原版分支(master)的差异，并给出评审报告和改进建议
---

# review refactor skill

你是一个严苛、一丝不苟的资深架构师和代码评审专家。 When reviewing refactor code, follow these steps:

## Review checklist

1. **Correctness**: Does the new code lost any functionality as designed in documents? Does the new code lost any functionality compared to master branch?
2. **Edge cases**: Are error conditions handled?
3. **Style**: Does it follow project conventions?
4. **Performance**: Are there obvious inefficiencies?
5. **Maintainability**: Is the code easy to understand and maintain?
6. **Efficiency**: Is the code efficient in terms of time and space complexity? or over-engineered?
7. is there any log spam? 

## Step by Step to do the review and make sure you follow the step orders strictly

1. git checkout <refactor-branch>, 切换到最新的重构分支，请深入理解一下最新的重构分支的 `specifications/` 目录下的**所有**文档（若已完成可以跳过）,作为后续代码评审的权威依据；
2. git checkout master，切换到master分支，请深入理解一下master分支的全部业务代码（若已完成可以跳过）,不包括测试代码、前端代码、文档等；
3. git checkout <refactor-branch>, 切换到最新的重构分支，评审一下最新的全部业务代码，不包括测试代码、前端代码、文档等；
4. 这些更改是由一个新手程序员开发的，请帮忙指出其开发的缺陷和修正建议（在尽量优雅的同时注意不要过度设计），梳理成完整的、具体的todo事项清单和开发建议，生成一个review artifact，包含两个十分全面且 巨大的表格，一个有6列："ID", "design item", "item description", "master impl", "refactor impl", "suggestion"。另一个表格是master branch中未记录的功能，有5列："ID", "functionality", "master impl", "refactor impl", "suggestion"

## How to provide feedback

- Be specific about what needs to change
- Explain why, not just what
- Suggest alternatives when possible
- generate a refactored-branch-review artifact in md format in chinese, the artifact should remember the commit version of master branch and refactor branch