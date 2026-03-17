---
version: 1.0.0
---

# L3 Runtime: Worker Runtime Support

## [workflow] ExternalWorkerBootstrapTransport

**Steps**

1. `runtime-support` materializes absolute worker control/data socket paths plus stdout/stderr log paths under the configured socket directory.
2. `runtime-support` spawns the worker process with `--worker-control-socket <absolute-path>` and `--worker-data-socket <absolute-path>` and redirects worker stdout/stderr into those runtime-support-owned log files.
3. `runtime-support` starts the low-level `run_unit_runtime_bridge_loop(...)` helper on a dedicated bridge thread with separate control/data sockets; the upstream external-worker bridge remains only the low-level carrier glue.
4. `runtime-support` uses the upstream host bridge control-plane handle as the canonical startup/management path (`Ping` / `Init` / `Start` / `Close` / `OnControlFrame`) and keeps one bound route client only as the operational worker data-plane transport handle.
5. source/sink worker bootstrap is not considered complete until the worker has acknowledged its direct control-plane startup handshake; required `Ping` / `Init` / `Start` control frames MUST finish before the worker client is returned to the main app.
6. source-worker `Init` is a lightweight control-plane preparation step that may capture node/config state but MUST NOT delay its acknowledgement on full runtime materialization.
7. source-worker runtime materialization may be deferred to `Start`, but a worker `Start` acknowledgement MUST mean the worker control plane is ready for runtime traffic; worker startup MUST NOT acknowledge first and defer required endpoint readiness behind the reply.
8. shutdown sends a direct control-plane `Close` frame, closes the operational route client, joins the bridge thread, kills/waits the worker child if still running, and removes both worker socket paths.

## [workflow] ExternalWorkerRetryAndErrorClassification

**Steps**

1. startup/management worker control requests (`Ping` / `Init` / `Start` / `Close` / `OnControlFrame`) are encoded into direct control-plane frames and sent over the runtime-support-managed control socket; operational worker RPC requests are encoded into bytes and sent over the runtime-support-managed bound route client.
2. transport-context labeling may add worker-specific context to messages, but it MUST preserve canonical `Timeout` and `TransportClosed` categories instead of collapsing them into generic peer errors.
3. worker response payloads may project worker-owned business failures into typed worker response errors, but transport failures remain transport-classified `CnxError`.
4. retry loops compute an absolute wall-clock deadline from `total_timeout`.
5. each RPC attempt uses `min(rpc_timeout, deadline-now)` as its effective timeout budget; one oversized per-call timeout MUST NOT extend the wall-clock retry budget.
6. once the wall-clock deadline expires, retry stops immediately with the last timeout or transport failure instead of silently sleeping or extending the worker bring-up window.

## [workflow] ScanWorkerAliasBootstrap

**Steps**

1. `scan-worker` remains a distinct external worker artifact identity and operator-visible role.
2. in the current baseline, `worker-scan` keeps its own `run_scan_worker_server(...)` entry and reuses lower-level source-runtime helpers internally via `run_scan_worker_runtime_loop(...)`.
3. shared lower-level source-runtime reuse is valid only while `source-worker` and `scan-worker` share the same realization mode and binary path constraints.
4. runtime/orchestration still distinguishes `runtime.exec.source` from `runtime.exec.scan` when decoding control/tick envelopes and applying source-side control signals.
5. if `scan-worker` needs a distinct server surface, binary path, mode, or supervision policy, this alias bootstrap MUST be removed by first updating `L2`/`L3`, then changing the manifests and entrypoints.
