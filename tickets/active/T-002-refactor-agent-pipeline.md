# Ticket: Refactor Agent Pipeline Architecture (V2)

> **Status**: Active
> **Type**: Refactor
> **Workstream**: refactor/agent-pipeline-v2

## 1. Context
Phase 2 of V2 Architecture requires a robust `AgentPipeline` implementation. This pipeline must support:
- EventBus-based message sync (High throughput).
- Session lifecycle management (Create/Heartbeat/Close).
- Automatic EventBus splitting when consumers diverge.
- Leader/Follower role management.

## 2. Requirements

### A. Functional (Must Have)
- [ ] Verify `AgentPipeline` (in `agent/src/fustor_agent/runtime/agent_pipeline.py`) inherits from `fustor_core.pipeline.Pipeline`.
- [ ] Verify EventBus integration (`_run_message_sync` uses `EventBus`).
- [ ] Verify Automatic Splitting logic in `EventBus` (`_check_for_split`).
- [ ] Verify Session Creation uses `fustor-sender-http` correctly.
- [ ] Ensure `on_session_created` handles role assignment.
- [ ] Ensure `_run_leader_sequence` handles Snapshot, Audit, and Sentinel tasks.

### B. Non-Functional (Should Have)
- [ ] Code must be type-safe (mypy).
- [ ] Unit tests for `AgentPipeline` logic.

## 3. Implementation Plan
1.  **Audit**: Read existing `agent_pipeline.py` and `bus.py`.
2.  **Refactor**: Adjust logic to match `specs/01-ARCHITECTURE.md` Section 4.3 strictly.
3.  **Test**: Run `pytest agent/tests/` to verify behavior.

## 4. Acceptance Criteria (DoD)
- [ ] `AgentPipeline` matches V2 Spec.
- [ ] Tests pass.
