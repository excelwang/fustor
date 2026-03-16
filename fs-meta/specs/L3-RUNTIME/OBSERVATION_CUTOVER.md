version: 1.0.0
---

# L3 Runtime: fs-meta Observation Cutover

fs-meta authoring remains centered on `capanix-app-sdk`; even
boundary-conversion seams stay behind `app-sdk` raw helpers;
`capanix-kernel-api` remains below that line as a low-level mirror.

Product-facing execution language remains `embedded | external` worker mode over
one `fs-meta` app product container with four worker roles. This L3 document
may refer to runtime bootstrap details only as implementation context beneath
that worker model.

## [workflow] AuthoritativeTruthReplay

**Steps**

1. new generation starts under normal runtime bind/run and generation fencing.
2. app consumes current monitoring roots, runtime grants, generation/lease, and authoritative journal continuation from higher-authority boundaries.
3. app treats those inputs as authoritative truth sources; successful HTTP readiness alone does not create new truth.
4. source/sink/projection state is refreshed against that truth before the app can report observation evidence to the runtime-owned trusted exposure gate.

## [workflow] ProjectionCatchUp

**Steps**

1. app rebuilds in-memory sink/query observation state through scan/audit/rescan and realtime replay.
2. for active scan-enabled primary groups, app waits until the first audit has completed and the sink has processed that audit through materialized state before treating `/tree` and `/stats` as eligible.
3. until that catch-up completes, materialized `/tree` and `/stats` remain explicitly not-ready or degraded rather than silently claiming current observation state.
4. rebuilt observations remain projections; they are not promoted to authoritative truth by virtue of being queryable, and query readability or internal route availability is not sufficient to treat the generation as current truth.

## [workflow] FacadeExposureGate

**Steps**

1. HTTP facade process/listener readiness is necessary but not sufficient for trusted exposure.
2. app evaluates `observation_eligible` from initial-audit completion on active scan-enabled primary groups, materialized degraded/overflow status, and stale-writer fencing evidence.
3. package-local status/health surfaces expose the materialized readiness evidence through source coverage and audit timing plus sink `initial_audit_completed` and `overflow_pending_audit`; these signals support cutover diagnostics but do not become a competing truth source.
4. when facade activation remains pending because runtime exposure proof is still outstanding or listener spawn is retrying, package-local status/health surfaces also expose optional `facade.pending` diagnostics so operators can distinguish cutover waiting from generic process liveness.
5. runtime consumes generation/bind/route proof to decide which process owns the resource-scoped one-cardinality HTTP facade, while the app uses `observation_eligible` to decide when materialized `/tree` and `/stats` may answer as current observation.
6. when `observation_eligible` is absent or proof is incomplete, the app keeps `/tree` and `/stats` explicitly not-ready or degraded; `/on-demand-force-find` stays a freshness path and may be externally available earlier.

## [workflow] StaleGenerationFence

**Steps**

1. app validates control/data exposure against runtime generation fencing before serving unit-scoped work.
2. stale activation or deactivation envelopes are ignored according to package-local generation high-water behavior.
3. after a newer generation becomes active, older observations are not allowed to reclaim facade/query ownership or supply stale evidence to the runtime-owned trusted exposure gate.
4. stale-writer fencing protects observation evidence; it does not redefine platform bind/route authority.

## [workflow] FailureIsolationBoundary

**Steps**

1. product-facing failure domains are expressed only as `embedded` versus `external` workers.
2. the current baseline keeps `facade-worker=embedded` and `source-worker=external`, `scan-worker=external`, `sink-worker=external`.
3. realization details such as artifact bootstrap, loader handoff, or runtime bridge wiring remain internal and do not redefine worker roles.
4. constructor/bootstrap faults return typed `CnxError` or `init_error` before the app claims healthy external exposure.
5. API bootstrap, in-process projection bootstrap, and worker/bootstrap faults stay on explicit error/log paths rather than routine `expect!`/panic control flow, and runtime task join failures are reported as typed app errors.
