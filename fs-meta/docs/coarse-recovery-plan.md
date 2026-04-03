# fs-meta Coarse Recovery Plan

## Goal

Stabilize generation cutover and peer convergence without changing the external
fs-meta operator/query experience:

- keep one stable facade API surface
- keep `trusted-materialized` / `fresh` query semantics
- keep old-generation service until the successor is truly ready
- reduce cross-generation continuity seams by using coarser recovery rules

## Stop-Loss Principles

1. Do not publish facade-dependent query ownership early.
2. Do not drive long cleanup tails through source/sink workers unless strictly required.
3. After fail-closed control failure, prefer coarse local rebuild over repeated fine-grained replay loops.
4. Separate internal readiness from external publication.
5. Keep user-visible routes and response contracts unchanged.

## Target Architecture Shift

### 1. Two-Phase Publication Barrier

Split successor generation progression into two phases:

- `phase A`: internal recovery
  - source ready
  - sink ready
  - facade claim ready
- `phase B`: external publication
  - only after phase A succeeds may the app publish facade-dependent
    `QUERY` / `QUERY_PEER` ownership and consider the successor externally ready

### 2. Coarse Fail-Closed Recovery

When a source/sink control failure is classified fail-closed:

- mark runtime uninitialized
- clear or quarantine stale shared client state
- rebuild from retained authoritative inputs
- avoid repeated partial replay loops on the same stale client

### 3. Cleanup Tail Localization

Cleanup-only followups should:

- mutate retained state, claims, and gates locally
- avoid re-entering source/sink worker control paths unless the followup is
  genuinely required for the next ready state

### 4. Readiness Barrier

Define one explicit internal successor-ready condition before external route
publication:

- `source_ready`
- `sink_ready`
- `facade_claim_ready`

If any are false:

- successor remains internally recovering
- predecessor facade stays authoritative for external UX

## Workstreams

### Workstream A: Facade Publication Barrier

Write scope:

- `fs-meta/app/src/runtime_app.rs`

Focus:

- fixed-bind predecessor claim release / successor handoff
- suppress facade-dependent route publication until facade claim is acquired
- make successor facade publication depend on explicit internal readiness

### Workstream B: Source Coarse Recovery

Write scope:

- `fs-meta/app/src/workers/source.rs`
- `fs-meta/app/src/runtime_app.rs`

Focus:

- stale shared source client discard on fail-closed / retryable reset
- keep grant-only retries grant-only
- avoid replay loops that widen retained state or reuse poisoned clients

### Workstream C: Owning Coverage / Narrow Reproducer

Write scope:

- `fs-meta/app/src/runtime_app.rs`
- `fs-meta/tests/e2e/test_release_upgrade.rs`
- `fs-meta/tests/fs_meta_api_e2e.rs`

Focus:

- add coarse-grained owning tests matching the new publication barrier
- keep a narrower reproducer for convergence failures smaller than the full exact
- rerun preserved exact after each real local closure

## Sequencing

1. Land the plan-level publication/readiness barrier in tests first.
2. Land source coarse recovery rules needed for those tests.
3. Re-run the narrow reproducer.
4. Re-run the preserved exact.
5. Only then consider any additional sink-side coarse recovery changes.

## Success Criteria

1. Successor generation does not publish facade-dependent query ownership while the predecessor still holds the fixed bind claim.
2. Fail-closed source recovery does not reuse stale shared clients across replay-required reactivation.
3. Cleanup-only tails stay local and do not create new worker continuity seams.
4. The preserved exact progresses past the current convergence seam without changing user-facing API semantics.
