# Ticket: Refactor Sender HTTP (V2)

> **Status**: Backlog
> **Type**: Refactor
> **Workstream**: refactor/sender-http-v2

## 1. Context
Phase 3 requires renaming and refactoring `pusher-fusion` to `fustor-sender-http`. This component implements the `SenderHandler` interface and handles HTTP transport.

## 2. Requirements

### A. Functional (Must Have)
- [ ] Ensure package is named `fustor-sender-http` (Already done in T-001).
- [ ] Implement `fustor_core.transport.Sender` interface.
- [ ] Support API Key authentication.
- [ ] Support Batch sending.
- [ ] Support Session lifecycle (create/heartbeat/close).

### B. Non-Functional (Should Have)
- [ ] Mypy type safety.
- [ ] Unit tests.

## 3. Implementation Plan
1.  **Audit**: Check `packages/fustor-sender-http/src`.
2.  **Refactor**: Ensure it inherits from `SenderHandler` (or `Sender` abstraction).
3.  **Test**: Verify connectivity.

## 4. Acceptance Criteria (DoD)
- [ ] `SenderHTTP` implements `Sender` interface.
- [ ] Tests pass.
