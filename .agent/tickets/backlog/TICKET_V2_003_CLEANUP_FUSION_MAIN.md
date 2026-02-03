# TICKET_V2_003_CLEANUP_FUSION_MAIN: Clean up Fusion entry point and API routing

> **Status**: Backlog
> **Type**: Refactor
> **Workstream**: refactor/architecture-v2

## 1. Context
`fusion/src/fustor_fusion/main.py` contains deprecated legacy code and redundant router setups. It needs to be modernized and simplified.

## 2. Requirements

### A. Functional (Must Have)
- [ ] Remove all "ProcessingManager" and "Legacy mode" references.
- [ ] Standardize router registration (avoid double setup calls).
- [ ] Fix double-logging initialization.

## 3. Implementation Plan
1. Clean up imports and global variables.
2. Refactor `lifespan` to be more concise.
3. Organize API routes logically.

## 4. Acceptance Criteria (DoD)
- [ ] `main.py` is under 150 lines.
- [ ] All API tests pass.
