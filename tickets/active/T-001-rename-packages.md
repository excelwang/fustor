# Ticket: Rename Packages to Fustor V2 Convention

> **Status**: Backlog
> **Type**: Refactor
> **Workstream**: refactor/package-renaming

## 1. Context
The `specs/01-ARCHITECTURE.md` defines a specific package naming convention (`fustor-*`) which is not currently reflected in the `packages/` directory. This ticket addresses this debt.

## 2. Requirements

### A. Functional (Must Have)
- [ ] Rename `packages/core` -> `packages/fustor-core`
- [ ] Rename `packages/agent-sdk` -> `packages/fustor-agent-sdk`
- [ ] Rename `packages/fusion-sdk` -> `packages/fustor-fusion-sdk`
- [ ] Rename `packages/schema-fs` -> `packages/fustor-schema-fs`
- [ ] Rename `packages/source-fs` -> `packages/fustor-source-fs`
- [ ] Rename `packages/source-oss` -> `packages/fustor-source-oss`
- [ ] Rename `packages/view-fs` -> `packages/fustor-view-fs`
- [ ] Rename `packages/sender-http` -> `packages/fustor-sender-http`
- [ ] Rename `packages/receiver-http` -> `packages/fustor-receiver-http`
- [ ] Update `pyproject.toml` workspace members to reflect new paths.
- [ ] Update internal imports to match new package names.

### B. Non-Functional (Should Have)
- [ ] Ensure all tests pass after rename.

## 3. Implementation Plan

> **Strategy**: Rename and Sed

1.  **Phase 1: Rename Directories**
    - Use `mv` to rename folders.
2.  **Phase 2: Update Configuration**
    - Edit root `pyproject.toml`.
3.  **Phase 3: Update Imports**
    - Global search and replace for package imports.

## 4. Acceptance Criteria (DoD)
- [ ] Directory structure matches `specs/01-ARCHITECTURE.md`.
- [ ] `pytest` passes.
