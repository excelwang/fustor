# Refactor Global TODO - architecture-v2

## Phase 4: Polish and Integration (Current focus)

- [ ] **Fix Functionality Regressions**
    - [ ] Restore `fields_mapping` support in `run_driver_message_sync` [Source: review report]
    - [ ] Consolidate `Mapper` logic to avoid duplication between Bus and Driver modes
- [ ] **Address Performance/Robustness Suggestions**
    - [ ] Refine mtime heuristic in `FSArbitrator` (remove magic numbers) [Source: review report]
    - [ ] Add explicit time unit support in `fustor-schema-fs`
- [ ] **Verification & Validation**
    - [ ] Ensure all `it/consistency` tests pass with the new Pipeline architecture
    - [ ] Add a specific integration test for `fields_mapping` in both Bus and Driver modes
- [ ] **Documentation & Cleanup**
    - [ ] Update READMEs in `agent/` and `fusion/` to reflect new config paths
    - [ ] Remove any remaining "pusher"/"sync" legacy code in non-sdk packages

## Recently Completed
- [x] Architecture Review and Gap Analysis
- [x] Core Pipeline implementation
- [x] FS Consistency logic (FSArbitrator)
- [x] Logical Clock (Robust Skew Mode)
- [x] Base modules consolidation (fustor-core)
