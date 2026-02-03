# Code Review Report

## 1. Summary
- **Verdict**: [PASS | FAIL | BLOCKED]
- **Rating**: [0-100]
- **Mode**: [Feature | Refactor | Test]

## 2. Findings (Table 1: Consistency)

| Severity | File | Line | Issue | Suggestion |
| :--- | :--- | :--- | :--- | :--- |
| High | `src/auth.py` | 42 | Hardcoded secret | Use env var |
| Medium | `tests/test_x.py` | 10 | Flaky test | Increase timeout |

## 3. Implementation Gaps (Table 2: Completeness)

| Requirement (Spec/Ticket) | Status | Comment |
| :--- | :--- | :--- |
| "Must support OAuth" | Missing | Not implemented yet |
| "Latency < 200ms" | Pending | No benchmark provided |
