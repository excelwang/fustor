# fs-meta Root-Fix Discipline

This document defines the required bug-fix loop for fs-meta demo, mini-cluster,
full-cluster, and CI failures. The goal is simple, direct, and robust behavior:
fix the owner of the broken product rule, not the symptom seen by one test.

## Fix Loop

1. Capture the failing command, suite, environment, first raw error, and the
   affected domain object: root, group, source, sink, facade, query, management
   write path, publication, or observation evidence.
2. Name the domain state that failed. Examples: root authority not current,
   grants not current, source evidence incomplete, sink materialization stale,
   facade alive but management writes not ready, or runtime route unavailable.
3. Assign the first raw failing boundary to one owner before editing code.
4. Fix that owner. If the capability is generic runtime behavior, route behavior,
   worker-host behavior, artifact loading, retry/cancel policy, or reusable
   host-fs behavior, fix it in capanix. Keep the fustor app layer focused on
   fs-meta domain rules.
5. If the specs are incomplete or wrong, revise the specs first and record the
   product reason. Then update source and tests to match.
6. Re-run the lowest affected test rung first, then climb the ladder until the
   original environment passes.

## Forbidden Workarounds

Do not use these to pass a demo or cluster test:

- Skipping the failing assertion or weakening the expected product result.
- Adding a demo-only branch to production code.
- Returning fake readiness when source, sink, management write, or publication
  evidence is not actually ready.
- Reusing cached success after roots, grants, source generation, sink generation,
  or facade/runtime generation changed.
- Treating missing file metadata as complete metadata.
- Using blind sleeps instead of an explicit readiness or timeout rule.
- Increasing a timeout without a bounded-latency or degraded-state rule.
- Hiding a capanix runtime problem with fustor-specific retry logic when the
  same capability should be reusable by other apps.

## Test Ladder

Run from cheapest proof to most realistic proof:

| Rung | Command suite | Meaning |
| --- | --- | --- |
| L0 | contract-fast | Specs, API names, CLI scope, and module boundaries agree. |
| L1 | core-unit-fast | Query, management write, and worker domain state machines pass locally. |
| L2 | runtime-scope-gate | Runtime-scope behavior works across the worker-host boundary. |
| L3 | mini-real-nfs-smoke | The 5-node/5-NFS topology converges on separate mini exports with 10 files each. |
| L4 | real-nfs-api-core | Full real-NFS P0 API and management behavior pass on real data. |
| L5 | real-nfs-ops, real-nfs-component, real-nfs-upgrade | Operational churn, component diagnostics, and release continuity pass. |

If L4 or L5 finishes suspiciously fast, verify that the test was not skipped,
`--ignored` was used intentionally, the filter matched real tests,
`CAPANIX_REAL_NFS_E2E=1` was set, and the mounted paths are the intended real
NFS exports.

## Work Log

Each non-trivial failure should leave a short work-log entry with:

- Command and suite.
- Environment rung.
- First raw error.
- Domain state at failure.
- Owning boundary and repository.
- Specs rule used or specs change made.
- Fix summary.
- Validation commands re-run from lowest affected rung upward.
