# TICKET_V2_001_REF_SOURCE_FS: Refactor Source-FS Driver

> **Status**: Active
> **Type**: Refactor
> **Workstream**: refactor/architecture-v2

## 1. Context
`packages/source-fs/src/fustor_source_fs/__init__.py` has grown to over 690 lines (31KB), violating the project's architectural guidelines (GEMINI.md Rule 9). It contains redundant scanning logic for pre-scan, snapshot, and audit phases.

## 2. Requirements

### A. Functional (Must Have)
- [ ] Extract scanning logic into a unified `Scanner` component.
- [ ] Merge `_perform_pre_scan_and_schedule` and `get_snapshot_iterator` logic to use the same underlying recursive worker.
- [ ] Keep `FSDriver` as a thin coordinator (Facade).
- [ ] Ensure all existing integration tests in `it/consistency/` pass.

### B. Non-Functional (Should Have)
- [ ] Maintainability: Single file size < 400 lines.
- [ ] Testability: Scanner can be tested independently with mocks.

## 3. Implementation Plan

> **Strategy**: Refactor-Verify

1.  **Phase 1: Decomposition**
    - Create `scanner.py` to hold the core recursive scanning logic.
    - Create `manager.py` to hold the `WatchManager` and related orchestration if needed.
2.  **Phase 2: Refactoring FSDriver**
    - Migrate `get_snapshot_iterator` and `get_audit_iterator` to use the new `Scanner`.
    - Simplify `_perform_pre_scan_and_schedule`.
3.  **Phase 3: Cleanup**
    - Remove legacy code from `__init__.py`.
    - Verify with existing tests.

## 4. Acceptance Criteria (DoD)
- [ ] `packages/source-fs/src/fustor_source_fs/__init__.py` is reduced in size.
- [ ] Integration tests `it/consistency/test_a*.py`, `it/consistency/test_b*.py`, etc. pass.
- [ ] No regression in performance (Scan time).
