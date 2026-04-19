---
version: 3.0.0
---

# L3 Runtime: fs-meta Domain State Model

State semantics in fs-meta are owned by domain-facing concepts, not by
implementation booleans inside `runtime_app`, `workers/source`,
`workers/sink`, or `query/api`.

This file is the single L3 runtime authority for user-visible and
operator-visible fs-meta state names.

## [decision] DomainStateModelAuthority

1. fs-meta MUST model product/runtime-visible state in terms of domain objects that users and operators recognize: `group`, `query/path result`, `facade/service`, `rollout generation`, and `node participation`.
2. This file is the only authority for those domain-facing state names and their transitions.
3. Internal implementation flags, replay markers, retained-state markers, tick fast-path markers, and transport/status classifier quadrants MUST NOT be promoted to product-facing or operator-facing state names.
4. The hard-cut state-model rewrite does not preserve legacy internal state vocabulary as a compatibility layer. Legacy implementation signals MAY survive only as diagnostics or reasons beneath the domain state machines defined here.

## [state-machine] GroupServiceState

**Rationale**

Users think in terms of whether a logical group is selected, serving,
degraded, or leaving service. They do not reason in terms of retained
replay flags or worker-local control lanes.

**Type Signature**

```text
GroupServiceState =
  | not-selected
  | selected-pending
  | serving-trusted
  | serving-degraded
  | retiring
  | retired
```

**State Meaning**

1. `not-selected`: the logical group is outside the current monitoring/service scope.
2. `selected-pending`: the group is selected, but current observation/service readiness has not yet reached serving state.
3. `serving-trusted`: the group is selected and currently serves trusted observation.
4. `serving-degraded`: the group is selected and partially serving, but current trust/completeness has degraded.
5. `retiring`: the group is being removed from current service responsibility.
6. `retired`: the group has finished retirement for the current rollout state.

**Allowed Transitions**

| From | To | Trigger |
|---|---|---|
| `not-selected` | `selected-pending` | runtime grants, monitoring roots, or rollout target now include the group |
| `selected-pending` | `serving-trusted` | source, sink, and observation evidence are sufficient for trusted serving |
| `selected-pending` | `serving-degraded` | partial observation or service evidence exists, but trusted serving is not yet satisfied |
| `serving-trusted` | `serving-degraded` | trusted observation/service evidence regresses |
| `serving-degraded` | `serving-trusted` | degraded evidence recovers to trusted serving |
| `selected-pending` | `retiring` | the group is removed before it fully enters service |
| `serving-trusted` | `retiring` | retirement begins for a trusted-serving group |
| `serving-degraded` | `retiring` | retirement begins for a degraded-serving group |
| `retiring` | `retired` | retirement/drain/withdraw completes |
| `retiring` | `selected-pending` | retirement is interrupted by renewed selection |
| `retired` | `selected-pending` | the group is selected again in a later rollout |

**User Question This Answers**

1. Is this group currently selected?
2. Is it serving trusted results?
3. Is it serving in a degraded way?
4. Is it leaving service?

## [state-machine] QueryObservationState

**Rationale**

The caller cares whether a path can be read, whether the answer is only
fresh, whether materialized data exists but is not yet fully trusted, or
whether a trusted materialized result is available.

**Type Signature**

```text
QueryObservationState =
  | unavailable
  | fresh-only
  | materialized-untrusted
  | trusted-materialized
```

**State Meaning**

1. `unavailable`: the query path cannot currently produce a valid observation result.
2. `fresh-only`: only a live/fresh result path is currently available.
3. `materialized-untrusted`: materialized observation exists, but trust/coverage/stability is not sufficient for trusted serving.
4. `trusted-materialized`: current materialized observation is trusted for external use.

**Allowed Transitions**

| From | To | Trigger |
|---|---|---|
| `unavailable` | `fresh-only` | live/fresh path becomes available before trusted materialized readiness |
| `unavailable` | `materialized-untrusted` | materialized observation becomes readable, but not trusted |
| `fresh-only` | `materialized-untrusted` | materialized observation becomes readable |
| `materialized-untrusted` | `trusted-materialized` | observation evidence becomes trusted |
| `trusted-materialized` | `materialized-untrusted` | trusted observation evidence regresses |
| `fresh-only` | `unavailable` | live/fresh path is no longer available |
| `materialized-untrusted` | `unavailable` | materialized path fails closed or becomes unreadable |
| `trusted-materialized` | `unavailable` | trusted path must fail closed because current observation is no longer readable |

**User Question This Answers**

1. Can I read this path now?
2. Is the answer only fresh/live?
3. Is the materialized answer trustworthy yet?

## [decision] QueryServiceAvailabilityWindows

1. `/on-demand-force-find` MUST use the widest query-side availability window in fs-meta. It is a freshness service, not a trusted-materialized service.
2. `/on-demand-force-find` is unavailable only when one of the following holds:
   1. `FacadeServiceState=unavailable`
   2. `FacadeServiceState=pending`
   3. the target group is outside current service scope (`GroupServiceState=not-selected|retired`)
   4. no fresh source execution path is currently available for the target group
3. `/on-demand-force-find` MUST NOT be blocked solely by materialized-observation trust gates such as initial-audit incompleteness, overflow-pending-materialization, or the absence of `trusted-materialized` observation.
4. materialized `/tree` and `/stats` requests (`read_class=materialized`) use a narrower availability window than `/on-demand-force-find`. They are available only when all of the following hold:
   1. `FacadeServiceState=serving|degraded`
   2. the target group remains inside current service scope (`GroupServiceState` is not `not-selected` or `retired`)
   3. `QueryObservationState=materialized-untrusted|trusted-materialized`
5. trusted-materialized `/tree` and `/stats` requests (`read_class=trusted-materialized`) use the narrowest availability window. They are available only when all of the following hold:
   1. `FacadeServiceState=serving|degraded`
   2. the target group remains inside current service scope (`GroupServiceState` is not `not-selected` or `retired`)
   3. `QueryObservationState=trusted-materialized`
6. When `QueryObservationState=materialized-untrusted`, `read_class=materialized` MAY answer and `read_class=trusted-materialized` MUST fail closed with explicit not-ready semantics.
7. Availability ordering is a product contract, not an implementation accident:
   1. `/on-demand-force-find` availability window MUST be a superset of materialized `/tree` and `/stats` availability
   2. materialized `/tree` and `/stats` availability MUST be a superset of trusted-materialized `/tree` and `/stats` availability
8. Internal replay, retained, tick-fast-path, bridge-reset, or cache-fallback diagnostics MAY explain why a request is unavailable, but they MUST NOT define a competing service-level availability window outside the rules above.

## [state-machine] FacadeServiceState

**Rationale**

Users and operators care whether the fs-meta facade is unavailable,
coming up, serving, or degraded. They do not care about internal lease,
bridge, replay, or republish mechanics as first-class states.

**Type Signature**

```text
FacadeServiceState =
  | unavailable
  | pending
  | serving
  | degraded
```

**State Meaning**

1. `unavailable`: the facade cannot currently serve.
2. `pending`: the facade is in startup/cutover/recovery and not yet ready to serve as the current owner.
3. `serving`: the facade is actively serving as the current owner.
4. `degraded`: the facade is still present but operating under degraded evidence or degraded continuity.

**Allowed Transitions**

| From | To | Trigger |
|---|---|---|
| `unavailable` | `pending` | activation/startup/cutover begins |
| `pending` | `serving` | current facade ownership and serving readiness are established |
| `pending` | `unavailable` | activation/startup/cutover fails closed |
| `serving` | `degraded` | serving continues but continuity/readiness evidence regresses |
| `degraded` | `serving` | degraded serving recovers |
| `serving` | `unavailable` | service is lost or fenced |
| `degraded` | `unavailable` | degraded service can no longer serve |
| `serving` | `pending` | ownership handoff or new rollout begins |
| `degraded` | `pending` | degraded owner begins replacement or re-establishment |

**User Question This Answers**

1. Is the fs-meta API up?
2. Is it still warming up?
3. Is it currently degraded?

## [state-machine] RolloutGenerationState

**Rationale**

Users and operators understand rollouts as a change window with a
candidate generation catching up, becoming eligible, taking over,
draining, retiring the old generation, and eventually becoming stable.

**Type Signature**

```text
RolloutGenerationState =
  | catch-up
  | eligible
  | cutover
  | drain
  | retire
  | stable
```

**State Meaning**

1. `catch-up`: a generation exists but is still catching up to required state.
2. `eligible`: the generation is caught up enough to be considered for cutover.
3. `cutover`: ownership transfer to this generation is in progress.
4. `drain`: the older serving side is draining after cutover starts.
5. `retire`: previous generation retirement is in progress.
6. `stable`: the current generation is fully established as the stable serving generation.

**Allowed Transitions**

| From | To | Trigger |
|---|---|---|
| `stable` | `catch-up` | a newer generation is introduced |
| `catch-up` | `eligible` | catch-up and readiness requirements are satisfied |
| `eligible` | `cutover` | cutover begins |
| `cutover` | `drain` | old serving side begins drain |
| `drain` | `retire` | retirement of older generation begins |
| `retire` | `stable` | retirement completes and the current generation is established |
| `catch-up` | `catch-up` | a newer generation supersedes the in-flight candidate |
| `eligible` | `catch-up` | a newer generation supersedes the eligible candidate |
| `cutover` | `catch-up` | a newer generation interrupts the in-flight cutover |
| `drain` | `catch-up` | a newer generation interrupts the in-flight drain |
| `retire` | `catch-up` | a newer generation interrupts the in-flight retirement |

**User Question This Answers**

1. Which rollout stage is the current generation in?
2. Has the new generation fully taken over?
3. Is the old generation still draining or retiring?

## [state-machine] NodeParticipationState

**Rationale**

Operational scenarios talk about nodes joining, serving, degrading, and
retiring. Those are the states operators recognize during failover and
membership churn.

**Type Signature**

```text
NodeParticipationState =
  | absent
  | joining
  | serving
  | degraded
  | retiring
  | retired
```

**State Meaning**

1. `absent`: the node is not currently part of the active participation set for the relevant service/group context.
2. `joining`: the node is entering participation but is not yet stably serving.
3. `serving`: the node is actively serving in the relevant service/group context.
4. `degraded`: the node is participating but under degraded evidence or capability.
5. `retiring`: the node is leaving participation.
6. `retired`: the node has completed retirement from participation.

**Allowed Transitions**

| From | To | Trigger |
|---|---|---|
| `absent` | `joining` | node is admitted into the active target set |
| `joining` | `serving` | participation becomes stably serving |
| `joining` | `degraded` | participation exists but stable serving is not achieved |
| `serving` | `degraded` | serving evidence regresses |
| `degraded` | `serving` | degraded participation recovers |
| `serving` | `retiring` | node retirement begins |
| `degraded` | `retiring` | degraded node retirement begins |
| `retiring` | `retired` | retirement completes |
| `retired` | `joining` | node is reintroduced later |

**User Question This Answers**

1. Is this node joining, serving, degraded, or retiring?
2. Has this node already left service?

## [decision] LegacyImplementationSignalsAreDiagnosticsOnly

1. The following implementation signals MUST be treated as diagnostic reasons only and MUST NOT appear as first-class user-visible or operator-visible states:
   1. `control_initialized`
   2. `source_state_replay_required`
   3. `sink_state_replay_required`
   4. `retained_*`
   5. `ordinary_*_tick_only_steady_noop`
   6. `sink_tick_recovery_requires_status_republish`
   7. `scheduled_zero_uninitialized`
   8. `missing_group_rows_after_stream_evidence`
   9. `partially_stale_split`
2. Those signals MAY appear only as implementation-local `reason`, `diagnostic`, or lower-raw evidence supporting one of the five domain state machines in this file.
3. No fs-meta implementation module may define a competing public state vocabulary for service readiness, path readability, rollout phase, or node participation outside this file.

## [decision] ImplementationStateOwnership

1. `runtime_app` MUST fold source, sink, facade, and rollout facts into the domain state machines defined in this file.
2. `workers/source` MUST emit source-side facts and recovery outcomes without inventing a competing service-state vocabulary.
3. `workers/sink` MUST emit sink-side observation/readiness facts without inventing a competing query/service-state vocabulary.
4. `query/api` MUST project query results from `QueryObservationState` plus group facts rather than from implementation-local replay or fallback quadrants.
5. `capanix` platform state machines remain upstream truth for platform-owned phases such as runtime readiness, activation, trusted exposure, and lifecycle phase; fs-meta consumes those truths and folds them into this file's domain states instead of redefining platform state meaning.
