# Refactor Global TODO

Based on `refactored-branch-review.md`.

## High Priority

- [x] **Consistency**: Add property-based tests for `FSArbitrator` to cover edge cases. <!-- id: 1 -->
- [x] **Event Model**: Remove legacy dict-passing code and ensure `EventBase` usage everywhere. <!-- id: 2 -->
- [x] **Reliability**: Expose configurable timeout and retry parameters in `PipelineManager` / Config models. <!-- id: 3 -->

## Medium Priority

- [ ] **Configuration**: Create migration guide/script for old `datastores-config.yaml`. <!-- id: 4 -->
- [x] **Observability**: Add unified Metrics interface in `fustor-core`. <!-- id: 5 -->

## Low Priority (Documentation/DevOps)

- [ ] **Architecture**: Improve SDK documentation. <!-- id: 6 -->
- [ ] **Packages**: Standardize CI/CD workflows. <!-- id: 7 -->
