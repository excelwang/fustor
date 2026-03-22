---
version: 3.0.0
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

## [workflow] SharedWorkerModuleRoleDispatch

**Steps**

1. `source-worker` and `sink-worker` remain the distinct external worker roles even when they share one worker module artifact.
2. the platform generic external worker hosting path loads the shared fs-meta worker runtime surface and passes runtime-owned launch payload data into it; exact payload encoding and transport plumbing remain upstream implementation detail.
3. the shared worker surface dispatches the requested role to the product-local source or sink worker-server entry.
4. `runtime.exec.scan` remains a source-side unit and reuses the `source-worker` server/runtime surface rather than defining a third external worker role.
5. runtime/orchestration still distinguishes `runtime.exec.source` from `runtime.exec.scan` when decoding control/tick envelopes and applying source-side control signals.
6. if scan ever needs a distinct server surface, worker artifact, mode, or supervision policy, it must first be promoted back into its own worker role by updating `L2`/`L3`, then changing the manifests and entrypoints.
