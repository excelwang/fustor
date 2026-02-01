---
name: remove-legacy-code-skill
description: 详细评审当前的重构分支与原版分支(master)的差异，并给出评审报告和改进建议
---

# remove legacy code skill


When removing legacy code, follow these steps:

## code removability checklist

1. **Correctness**: Does the legacy code has been replaced by the new code?
2. **Edge cases**: Does the new code handle all edge cases?
 
## How to use it

1. list all the files that have been changed in the current branch;
2. for each file, check if the legacy code has been replaced by the new code;
3. if the legacy code has been replaced by the new code, check if the new code handles all edge cases;
4. if the legacy code has not been replaced by the new code, check if the new code handles all edge cases;
5. remove the legacy code if it is not used;
6. do not consider to keep compatibility with the existing configuration or api;
7. do not use legacy file name if the modular has been refactored;
8. do not forget to update documents;

## How to provide feedback

- Be specific about what needs to change
- Explain why, not just what
- Suggest alternatives when possible
- write in a document in chinese for the newbie developer to read
