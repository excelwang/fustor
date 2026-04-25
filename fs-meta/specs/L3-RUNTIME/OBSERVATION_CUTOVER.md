---
version: 3.0.0
---

# L3 Runtime: fs-meta Observation Cutover

fs-meta stateful authoring and runtime participation consume bounded
app-facing observation, service, and runtime-entry surfaces above the
low-level kernel mirror. Exact repo/package allowlists, helper-crate
layering, and wrapper API names remain engineering-governance or
implementation material rather than formal runtime contracts.

Product-facing execution language remains `embedded | external` worker mode over
one `fs-meta` app product container with three worker roles. This L3 document
may refer to runtime bootstrap details only as implementation context beneath
that worker model.

Cutover and exposure semantics in this file consume the domain states from [STATE_MODEL.md](./STATE_MODEL.md), especially `QueryObservationState`, `FacadeServiceState`, `GroupServiceState`, and `RolloutGenerationState`. Replay, lease, retained, bridge, and local gate signals remain diagnostic reasons rather than competing public state names.

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

1. HTTP facade listener readiness is necessary but not sufficient for trusted exposure.
2. HTTP facade listener readiness is also not sufficient for management writes; roots apply, manual rescan, and query-api-key mutation require Management Write Ready from the current-epoch active control stream.
3. app evaluates `observation_eligible` from the same package-local observation evaluator and materialized observation evidence that feeds query `observation_status`: initial-audit completion on active scan-enabled primary groups, materialized degraded/overflow status, and stale-writer fencing evidence.
4. the worker-facing runtime wrapper lowers runtime loading through the bounded app/runtime entry surface; exact helper API names and wrapper composition remain implementation detail as long as the observation evaluator stays app-owned.
5. package-local status/health surfaces expose the materialized readiness evidence through source coverage and audit timing plus sink `initial_audit_completed` and `overflow_pending_materialization`; these signals support cutover diagnostics but do not become a competing truth source.
6. when facade activation remains pending because runtime exposure proof is still outstanding or listener spawn is retrying, package-local status/health surfaces also expose optional `facade.pending` diagnostics so operators can distinguish cutover waiting from generic host-boundary liveness.
7. runtime consumes generation/bind/route proof to decide which active facade unit owns the resource-scoped one-cardinality HTTP facade, while the app uses Management Write Ready to decide whether management writes may be accepted and `observation_eligible` to decide when `trusted-materialized` `/tree` and `/stats` may answer as current observation.
8. when `observation_eligible` is absent or proof is incomplete, the app keeps `trusted-materialized` `/tree` and `/stats` explicitly not-ready and surfaces weaker materialized reads as `materialized-untrusted`; `/on-demand-force-find` stays a freshness path and may be externally available earlier.

## [workflow] MaterializedReadinessEvidenceFanIn

**Steps**

1. source/sink status fan-in produces Materialized Readiness Evidence for each group inside the current Authority Epoch.
2. when same-epoch evidence has already completed for a group, transient fan-in absence, delayed status delivery, or a temporarily missing status sample does not by itself downgrade that group from ready.
3. explicit same-epoch contradictory evidence, including overflow pending materialization, audit invalidation, stale-writer fencing, sink loss, or host/object unavailability, downgrades or closes readiness according to the reported failure.
4. monitoring-root signature changes, runtime-grant signature changes, source stream generation changes, sink materialization generation changes, or facade/runtime generation changes create a new Authority Epoch and invalidate the previous monotonic evidence.
5. after invalidation, the group must rebuild Materialized Readiness Evidence before it can serve trusted materialized observation in the new epoch.

## [workflow] QueryAvailabilityOrderingAcrossCutover

**Steps**

1. during catch-up, cutover, drain, or retire, `/on-demand-force-find` MAY become externally available before trusted-materialized `/tree` and `/stats`, as soon as the facade is serving or degraded and a fresh source execution path exists for the target group.
2. during the same interval, materialized `/tree` and `/stats` MAY answer once materialized observation is readable, even if that observation is still `materialized-untrusted`.
3. trusted-materialized `/tree` and `/stats` MUST remain closed until `observation_eligible` is satisfied and `QueryObservationState=trusted-materialized`.
4. the system MUST preserve this ordering throughout recovery and failover: freshness availability first, materialized readability second, trusted-materialized serving last.
5. regressions in trusted observation MUST close the trusted-materialized window before they close the wider freshness window, unless the facade itself or fresh source execution becomes unavailable.

## [workflow] StaleGenerationFence

**Steps**

1. app validates control/data exposure against runtime generation fencing before serving unit-scoped work.
2. stale activation or deactivation envelopes are ignored according to package-local generation high-water behavior.
3. after a newer generation becomes active, older observations are not allowed to reclaim facade/query ownership or supply stale evidence to the runtime-owned trusted exposure gate.
4. stale-writer fencing protects observation evidence; it does not redefine platform bind/route authority.

## [workflow] FailureIsolationBoundary

**Steps**

1. product-facing failure domains are expressed only as `embedded` versus `external` workers.
2. the current baseline keeps `facade-worker=embedded`, `source-worker=external`, and `sink-worker=external`.
3. realization details such as artifact bootstrap, loader handoff, or runtime bridge wiring remain internal and do not redefine worker roles.
4. runtime-managed worker realization consumes compiled runtime worker bindings rather than raw realization-specific startup fields during app load.
5. constructor/bootstrap faults return typed `CnxError` or `init_error` before the app claims healthy external exposure.
6. API bootstrap, embedded projection bootstrap, and worker/bootstrap faults stay on explicit error/log paths rather than routine `expect!`/panic control flow, and runtime task join failures are reported as typed app errors.
