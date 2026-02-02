# Refactor Global TODO

Based on `refactored-branch-review.md` (2026-02-02).

## Critical Repairs (Blockers for V2 Release)

### 1. Restore Resume Capability (Data Integrity)
- [x] **D-04/D-06**: Implement `get_latest_committed_index` in `SenderHandler` (Agent-side) and ensure `AgentPipeline` queries it on startup.
- [x] **D-06**: Pass the retrieved `start_position` to `_run_driver_message_sync` (currently hardcoded to `-1`) to enable correct resume behavior in Driver Mode.

### 2. Fix Incremental Audit (Performance)
- [x] **D-05**: Introduce `audit_context` (containing `mtime_cache`) in `AgentPipeline` to persist state across audit cycles.
- [x] **U-02**: Update `phases.py` / `run_audit_sync` to correctly handle `(Event, mtime_update)` tuples from Source Driver, ensuring `mtime_cache` is updated even when Event is `None` (Silent Directory).

## Completed
- [x] **Consistency**: Add property-based tests for `FSArbitrator` to cover edge cases. <!-- id: 1 -->
- [x] **Event Model**: Remove legacy dict-passing code and ensure `EventBase` usage everywhere. <!-- id: 2 -->
- [x] **Reliability**: Expose configurable timeout and retry parameters in `PipelineManager` / Config models. <!-- id: 3 -->
- [x] **Configuration**: Create migration guide/script for old `datastores-config.yaml`. <!-- id: 4 -->
- [x] **Observability**: Add unified Metrics interface in `fustor-core` and integrated into Agent. <!-- id: 5 -->

## Low Priority (Documentation/DevOps)
- [ ] **Architecture**: Improve SDK documentation. <!-- id: 6 -->
- [ ] **Packages**: Standardize CI/CD workflows. <!-- id: 7 -->
