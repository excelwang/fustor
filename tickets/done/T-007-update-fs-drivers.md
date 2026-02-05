# Ticket: Update FS Source and View Drivers (V2)

> **Status**: Backlog
> **Type**: Refactor
> **Workstream**: refactor/fs-drivers-v2

## 1. Context
Phase 3 requires `fustor-source-fs` and `fustor-view-fs` to align with the new Core abstractions.

## 2. Requirements

### A. Functional (Must Have)
- [ ] `fustor-source-fs` must implement `SourceHandler`.
- [ ] `fustor-view-fs` must implement `ViewHandler`.
- [ ] Verify LogicalClock usage in View.
- [ ] Verify EventBus integration in Source (if applicable).

## 3. Implementation Plan
1.  **Audit**: Check packages.
2.  **Refactor**: Update imports and inheritance.

## 4. Acceptance Criteria (DoD)
- [ ] Drivers work with new Core.
- [ ] Tests pass.
