---
name: review-recent-changes-skill
description: 详细评审当前分支的测试代码，并给出评审报告和改进建议
---

# review recent changes skill

When reviewing changes code, follow these steps:

## Review checklist

1. **Correctness**: Does the code do what it's supposed to do as designed in documents?
2. **Edge cases**: Are error conditions handled?
3. **Style**: Does it follow project conventions?
4. **Performance**: Are there obvious inefficiencies?
5. **Maintainability**: Is the code easy to understand and maintain?
6. **Over-testing**: Are there redundant tests?
7. **Test coverage**: Are there missing tests?
8. **Test code pollution**: Does the test code pollute the business code? include twist the business logic to make it easier to test, or add extra methods to the business code to make it easier to test.

## How to use it

1. 请深入理解一下变更代码；
2. 这些更改是由一个新手程序员开发的，请帮忙指出其开发的缺陷和修正建议（在尽量优雅的同时注意不要过度设计），梳理成完整的、具体的todo事项清单和开发建议。


## How to provide feedback

- Be specific about what needs to change
- Explain why, not just what
- Suggest alternatives when possible
- if there are exsiting a review artifact, please update it with your new observation and suggestion, and change the commit id of the review branch to the latest commit id you have reviewed, and contain two huge tables, one with 6 columns: "ID", "design item", "item description", "master impl", "refactor impl", "suggestion". the other table is for undocumented functionality in master branch compared to refactor branch with 5 columns: "ID", "functionality", "master impl", "refactor impl", "suggestion"