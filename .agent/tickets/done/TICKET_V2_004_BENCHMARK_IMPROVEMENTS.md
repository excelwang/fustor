# Ticket: V2 Benchmark Improvements

> **Status**: Backlog
> **Type**: Chore
> **Workstream**: chore/benchmark-cleanup

## 1. Context
Following the review of `fustor-benchmark` (fustor-benchmark-run), we identified improved cleanup logic for zombie processes and a need to verify V2 API compatibility for stats.

## 2. Requirements

### A. Functional (Must Have)
- [ ] Refactor `services.py`: `stop_all` should remove processes from the tracking list.
- [ ] Verify `runner.py`: Ensure `/api/v1/views/fs/stats` connects to a valid V2 endpoint in Fusion.

### B. Non-Functional (Should Have)
- [ ] Logic should be robust against partial `terminate()` failures (fallback to `kill` is already there, just ensure list consistency).

## 3. Implementation Plan

> **Strategy**: Test-First

1.  **Phase 1: Verification**
    - Run `fustor-benchmark` locally.
    - Check if stats endpoint returns 404 or valid JSON.

2.  **Phase 2: Fix**
    - Update `services.py` to clear properties list.
    - Update `runner.py` if endpoint is different in V2.

## 4. Acceptance Criteria (DoD)
- [ ] `fustor-benchmark` runs without error.
- [ ] No orphan processes left after Ctrl+C or completion.
