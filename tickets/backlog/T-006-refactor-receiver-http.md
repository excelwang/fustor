# Ticket: Refactor Receiver HTTP (V2)

> **Status**: Backlog
> **Type**: Refactor
> **Workstream**: refactor/receiver-http-v2

## 1. Context
Phase 3 requires extracting HTTP receiving logic into `fustor-receiver-http`. This component implements `Receiver` interface and forwards requests to `PipelineManager`.

## 2. Requirements

### A. Functional (Must Have)
- [ ] Ensure package is named `fustor-receiver-http` (Already done in T-001).
- [ ] Implement `fustor_core.transport.Receiver` interface.
- [ ] Handle `/api/v1/pipe/*` endpoints.
- [ ] Authenticate via API Key.
- [ ] Forward events/sessions to callbacks.

## 3. Implementation Plan
1.  **Audit**: Check `packages/fustor-receiver-http/src`.
2.  **Refactor**: Ensure clean separation from Fusion core.

## 4. Acceptance Criteria (DoD)
- [ ] `ReceiverHTTP` works independently.
- [ ] Tests pass.
