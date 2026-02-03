---
trigger: always_on
---

Debug时如果日志尾部看不到错误信息，注意尝试grep error|error|fatal|exception|exit (case insensitive) 再找找看。要先grep再tail, 不要先tail再grep。