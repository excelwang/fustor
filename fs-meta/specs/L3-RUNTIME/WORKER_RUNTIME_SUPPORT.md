---
version: 3.4.0
---

# L3 Runtime: Worker Runtime Support

## [workflow] ExternalWorkerBootstrapTransport

**Steps**

1. Lower external-worker hosting realization consumes compiled runtime worker bindings emitted by the fs-meta release compiler and owns the helper-side work needed to bring source/sink workers up.
2. app packages do not parse raw startup transport fields during load; they consume compiled worker bindings and bounded helper surfaces layered above them.
3. helper-side bootstrap uses platform-owned worker-control handshake; app-owned `OnControlFrame` remains a normal typed worker RPC, not a bootstrap control step.
4. source/sink worker bootstrap is not considered complete until the required platform-owned handshake finishes and the worker control plane is ready for runtime traffic before the worker client is returned to the main app.
5. the lightweight preparation phase may capture node/config state but MUST NOT delay acknowledgement on full runtime materialization.
6. runtime materialization may be deferred to a later bootstrap phase, but the acknowledgement that opens runtime traffic MUST mean the worker control plane is ready; worker startup MUST NOT acknowledge first and defer required endpoint readiness behind the reply.
7. shutdown uses the platform-owned worker-close control and tears down helper-owned worker-hosting state and transport resources.
8. product-specific worker-binding normalization, including the rule that scan remains a source-side unit under `source-worker` and generation-to-generation worker-mode changes, is resolved by the fs-meta release compiler before runtime boot; the app runtime host only validates the compiled result and MUST NOT backfill missing realization details.

## [workflow] ExternalWorkerRetryAndErrorClassification

**Steps**

1. startup/management worker control requests are encoded into platform-owned bootstrap control envelopes; operational worker RPC requests, including app-owned `OnControlFrame`, remain normal typed worker RPC traffic.
2. transport-context labeling may add worker-specific context to messages, but it MUST preserve canonical `Timeout` and `TransportClosed` categories instead of collapsing them into generic peer errors.
3. worker response payloads may project worker-owned business failures into typed worker response errors, but transport failures remain transport-classified `CnxError`.
4. retry loops compute an absolute wall-clock deadline from the configured total timeout.
5. each RPC attempt uses the smaller of the per-call timeout and remaining deadline as its effective timeout budget; one oversized per-call timeout MUST NOT extend the wall-clock retry budget.
6. once the wall-clock deadline expires, retry stops immediately with the last timeout or transport failure instead of silently sleeping or extending the worker bring-up window.

## [workflow] ExternalWorkerAuthorityCommitOrdering

**Steps**

1. An external worker applies and retains accepted domain control facts before returning its process-level control acknowledgement.
2. If recording those facts requires a reverse `StateCell` authority-journal read or write, the control callback enqueues the journal mutation in a bounded FIFO and MUST NOT synchronously wait for that reverse primitive before becoming acknowledgement-eligible.
3. One single-flight asynchronous flusher loads the existing authority snapshot, appends queued mutations in order, and writes the resulting bounded journal after the control callback can return; concurrent control mutations join the same queue instead of creating one blocking thread or flush task per mutation.
4. Queue compaction uses the same retained-tail bound as the authority journal and emits explicit compaction evidence. Transient flush failures retain the queued mutations for bounded automatic retry or a later explicit drain instead of reporting a false commit.
5. Orderly worker close explicitly drains deferred authority commits after stopping endpoint work. Process-level acknowledgement decoupling changes carrier timing only; it does not weaken the authority journal's truth-ledger role or permit accepted mutations to disappear silently.
6. This ordering is app-owned use of a generic runtime state-carrier boundary. It does not require Capanix to know fs-meta roles or add an app-specific activation path.

## [workflow] ExternalWorkerStateCarrierExecutorIsolation

**Steps**

1. Worker bootstrap may synchronously recover its initial state before opening request traffic, but an already-running worker treats `StateBoundary` access as asynchronous I/O.
2. Source status recovery awaits the authoritative logical-roots read without blocking a Tokio executor thread; cancellation or timeout therefore leaves the worker IPC reader/writer schedulable.
3. Sink status, materialized-query, and roots-control request hot paths consume the current local logical-roots projection already delivered by management apply or roots-control propagation. They MUST NOT call a blocking `StateCell` bridge while serving a request.
4. The accepted management roots update owns the single authoritative logical-roots replace. Its source roots-control propagation sends a typed follower declaration containing the accepted generation; each worker reconciles local roots and records that generation locally without writing the authoritative cell again. Stale generations are no-ops and same-generation content conflicts fail closed.
5. A later authority repair remains available as an explicit asynchronous recovery operation. Until it completes, status reports the current local projection and query remains scoped by current runtime placement instead of inventing roots or blocking unrelated worker RPC acknowledgements.
6. Regression coverage holds deferred authority I/O open while management roots and status requests complete within their own reply budget, separately proves that cached status/query evaluation performs no new authoritative state read, and blocks any second logical-roots write while a propagated source declaration still acknowledges and advances its local generation.
7. This executor-isolation rule is fs-meta scheduling policy over the generic Capanix state boundary; it neither changes StateCell ownership nor adds fs-meta knowledge to Capanix.

## [workflow] SharedWorkerModuleRoleDispatch

**Steps**

1. `source-worker` and `sink-worker` remain the distinct external worker roles even when they share one worker module artifact.
2. the platform generic external worker hosting path loads the shared fs-meta worker runtime surface and passes runtime-owned launch payload data into it; exact payload encoding and transport plumbing remain upstream implementation detail.
3. the shared worker surface dispatches the requested role to the product-local source or sink worker-server entry.
4. `runtime.exec.scan` remains a source-side unit and reuses the `source-worker` server/runtime surface rather than defining a third external worker role.
5. runtime/orchestration still distinguishes `runtime.exec.source` from `runtime.exec.scan` when decoding control/tick envelopes and applying source-side control signals.
6. if scan ever needs a distinct server surface, worker artifact, mode, or supervision policy, it must first be promoted back into its own worker role by updating `L2`/`L3`, then changing the manifests and entrypoints.
