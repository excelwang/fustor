# Refactor Global TODO

Based on `refactored-branch-review.md` (2026-02-02).

## Completed
- [x] **Resume Capability**: Implement `get_latest_committed_index` and pass to message sync. <!-- id: 8 -->
- [x] **Incremental Audit**: Persist `audit_context` and handle silent directories. <!-- id: 9 -->
- [x] **Consistency Fix**: Fix `test_arbitrator_convergence` by aligning Oracle and Fuzzer. <!-- id: 10 -->
- [x] **Time Unit Standardization**: Enforced **Seconds** across all layers (SourceFS, Arbitrator, Fuzzer). Removed all ms/s heuristics. <!-- id: 4.1 -->
- [x] **Log Cleanup**: Remove print-based log spam in `FusionPipeline`. <!-- id: 4.2 -->
- [x] **Suspect Optimization**: Update suspect stability check to renew TTL on mtime change. <!-- id: 4.3 -->
- [x] **Architecture**: Improved SDK documentation for `agent-sdk` and `fusion-sdk` reflecting V2 Pipeline architecture. <!-- id: 6 -->
- [x] **Packages**: Standardized package structures (tests, __init__.py versioning) for core SDKs. <!-- id: 7 -->
- [x] **Consistency**: Add property-based tests for `FSArbitrator` to cover edge cases. <!-- id: 1 -->
- [x] **Event Model**: Remove legacy dict-passing code and ensure `EventBase` usage everywhere. <!-- id: 2 -->
- [x] **Reliability**: Expose configurable timeout and retry parameters in `PipelineManager` / Config models. <!-- id: 3 -->
- [x] **Configuration**: Create migration guide/script for old `datastores-config.yaml`. <!-- id: 4 -->
- [x] **Observability**: Add unified Metrics interface in `fustor-core` and integrated into Agent. <!-- id: 5 -->

## Next Steps
