# Ticket: Implement Session Manager Logic (V2)

> **Status**: Backlog
> **Type**: Feature
> **Workstream**: feat/session-manager-v2

## 1. Context
Phase 2 requires a centralized `SessionManager` in Fusion to handle:
- Session Registry (Agent <-> Fusion binding).
- Heartbeat tracking and timeout detection.
- Session ID generation.

## 2. Requirements

### A. Functional (Must Have)
- [ ] Verify `SessionManager` (in `fusion/src/fustor_fusion/core/session_manager.py`).
- [ ] Implement `create_session`, `update_heartbeat`, `close_session`.
- [ ] Implement timeout cleanup task.

## 3. Implementation Plan
1.  **Audit**: Read `session_manager.py`.
2.  **Implement**: Add missing logic.

## 4. Acceptance Criteria (DoD)
- [ ] Session lifecycle fully managed.
- [ ] Timeouts work.
