# Refactor Global TODO

Based on `refactored-branch-review.md`.

## Status Overview

| Step ID | Task Name | Status |
|---------|-----------|--------|
| step-01 | Fix EventMapper Security and Logic | [x] Done |
| step-02 | Create Config Migration Guide | [x] Done |
| step-03 | Document Reliability/Retry Mechanisms | [x] Done |
| step-04 | Observability Interfaces | [x] Done |

## Detailed Plan

### Step 01: Fix EventMapper Security and Logic
- **Goal**:
    - Remove `exec` security vulnerability.
    - Implement safe closure-based mapping logic.
    - Add support for `hardcoded_value`.
    - Add missing unit tests for `EventMapper`.
- **Files**:
    - `packages/core/src/fustor_core/pipeline/mapper.py`
    - `packages/core/tests/pipeline/test_mapper.py` (New)

### Step 02: Create Config Migration Guide
- **Goal**:
    - Document mapping from `datastores-config.yaml` to `views-config/*.yaml`.
    - Ensure no legacy config code remains.
- **Files**: `docs/migration-guide.md`

### Step 03: Document Reliability/Retry Mechanisms
- **Goal**:
    - Explicitly document session and retry policies.
    - Expose configurable timeouts if missing.
- **Files**: `packages/core/src/fustor_core/pipeline/context.py` (or similar)

### Step 04: Observability Interfaces
- **Goal**:
    - Add `Metrics` abstraction in Core.
- **Files**: `packages/core/src/fustor_core/common/metrics.py`
