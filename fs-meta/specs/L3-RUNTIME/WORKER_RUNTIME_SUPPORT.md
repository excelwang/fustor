---
version: 3.0.0
---

# L3 Runtime: Worker Runtime Support

## [workflow] ExternalWorkerBootstrapTransport

**Steps**

1. helper-only `capanix-worker-runtime-support` consumes compiled `__cnx_runtime.workers` bindings emitted by the fs-meta release compiler, then materializes absolute worker control/data socket paths plus stdout/stderr log paths under the resolved socket directory.
2. `capanix-worker-runtime-support` spawns the platform generic `capanix-worker-host` process with `--worker-role <role-id>`, `--worker-module <absolute-path>`, `--worker-control-socket <absolute-path>`, and `--worker-data-socket <absolute-path>`, and redirects worker stdout/stderr into helper-owned log files; app packages do not parse raw `workers.<role>.startup.path/socket_dir` during load.
3. `capanix-worker-runtime-support` starts the low-level worker runtime bridge on a dedicated bridge thread with separate control/data sockets; the upstream external-worker bridge remains only the low-level carrier glue.
4. `capanix-worker-runtime-support` uses the upstream host bridge control-plane handle as the canonical startup/management path (`Init` / `Start` / `Ping` / `Close`) and keeps one bound route client only as the operational worker RPC transport handle; app-owned `OnControlFrame` remains a normal typed worker RPC, not a bootstrap control step.
5. source/sink worker bootstrap is not considered complete until the worker has accepted the platform-owned bootstrap envelope handshake; required `Init` / `Start` / `Ping` control envelopes MUST finish before the worker client is returned to the main app.
6. source-worker `Init` is a lightweight control-plane preparation step that may capture node/config state but MUST NOT delay its acknowledgement on full runtime materialization.
7. source-worker runtime materialization may be deferred to `Start`, but a worker `Start` acknowledgement MUST mean the worker control plane is ready for runtime traffic; worker startup MUST NOT acknowledge first and defer required endpoint readiness behind the reply.
8. shutdown sends a direct control-plane `Close` envelope, closes the operational route client, joins the bridge thread, kills/waits the worker child if still running, and removes both worker socket paths.
9. product-specific worker-binding normalization, including `source-worker` / `scan-worker` shared-module constraints and generation-to-generation worker-mode changes, is resolved by the fs-meta release compiler before runtime boot; the app runtime host only validates the compiled result and MUST NOT backfill missing module paths or modes.

## [workflow] ExternalWorkerRetryAndErrorClassification

**Steps**

1. startup/management worker control requests (`Init` / `Start` / `Ping` / `Close`) are encoded into platform-owned bootstrap control envelopes and sent over the helper-managed control socket; operational worker RPC requests, including app-owned `OnControlFrame`, are encoded into bytes and sent over the helper-managed bound route client.
2. transport-context labeling may add worker-specific context to messages, but it MUST preserve canonical `Timeout` and `TransportClosed` categories instead of collapsing them into generic peer errors.
3. worker response payloads may project worker-owned business failures into typed worker response errors, but transport failures remain transport-classified `CnxError`.
4. retry loops compute an absolute wall-clock deadline from `total_timeout`.
5. each RPC attempt uses `min(rpc_timeout, deadline-now)` as its effective timeout budget; one oversized per-call timeout MUST NOT extend the wall-clock retry budget.
6. once the wall-clock deadline expires, retry stops immediately with the last timeout or transport failure instead of silently sleeping or extending the worker bring-up window.

## [workflow] SharedWorkerModuleRoleDispatch

**Steps**

1. `source-worker`, `scan-worker`, and `sink-worker` remain distinct external worker roles even when they share one worker module artifact.
2. the platform generic `capanix-worker-host` loads the shared `fs-meta/worker-facade/` worker module and passes `worker_role`, route key, and socket paths through the module launch payload.
3. the shared worker module dispatches `worker_role` to the role-local helper entries `run_source_worker_server(...)`, `run_scan_worker_server(...)`, or `run_sink_worker_server(...)`.
4. shared lower-level source-runtime reuse is valid only while `source-worker` and `scan-worker` share the same realization mode and worker module path constraints.
5. runtime/orchestration still distinguishes `runtime.exec.source` from `runtime.exec.scan` when decoding control/tick envelopes and applying source-side control signals.
6. if `scan-worker` needs a distinct server surface, worker module path, mode, or supervision policy, this shared-dispatch workflow MUST be removed by first updating `L2`/`L3`, then changing the manifests and entrypoints.
