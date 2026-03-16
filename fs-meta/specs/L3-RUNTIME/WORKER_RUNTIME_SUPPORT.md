version: 1.0.0
---

# L3 Runtime: Worker Runtime Support

## [workflow] ExternalWorkerBootstrapTransport

**Steps**

1. `runtime-support` materializes one absolute worker socket path plus stdout/stderr log paths under the configured socket directory.
2. `runtime-support` spawns the worker process with `--worker-socket <absolute-path>` and redirects worker stdout/stderr into those runtime-support-owned log files.
3. `runtime-support` starts the low-level `run_unit_runtime_bridge_loop(...)` helper on a dedicated bridge thread; the upstream external-worker bridge remains only the low-level carrier glue.
4. `runtime-support` opens one bound route client against the configured `route_key` + `node_id` and keeps that client as the canonical worker transport handle.
5. shutdown closes the route client, sends the worker close request, joins the bridge thread, kills/waits the worker child if still running, and removes the worker socket path.

## [workflow] ExternalWorkerRetryAndErrorClassification

**Steps**

1. worker RPC requests are encoded into bytes and sent over the runtime-support-managed bound route client.
2. transport-context labeling may add worker-specific context to messages, but it MUST preserve canonical `Timeout` and `TransportClosed` categories instead of collapsing them into generic peer errors.
3. worker response payloads may project worker-owned business failures into typed worker response errors, but transport failures remain transport-classified `CnxError`.
4. retry loops compute an absolute wall-clock deadline from `total_timeout`.
5. each RPC attempt uses `min(rpc_timeout, deadline-now)` as its effective timeout budget; one oversized per-call timeout MUST NOT extend the wall-clock retry budget.
6. once the wall-clock deadline expires, retry stops immediately with the last timeout or transport failure instead of silently sleeping or extending the worker bring-up window.

## [workflow] ScanWorkerAliasBootstrap

**Steps**

1. `scan-worker` remains a distinct external worker artifact identity and operator-visible role.
2. in the current baseline, `worker-scan` reuses the `worker-source` server bootstrap by invoking `run_source_worker_server(...)`.
3. shared bootstrap is valid only while `source-worker` and `scan-worker` share the same realization mode and binary path constraints.
4. runtime/orchestration still distinguishes `runtime.exec.source` from `runtime.exec.scan` when decoding control/tick envelopes and applying source-side control signals.
5. if `scan-worker` needs a distinct server surface, binary path, mode, or supervision policy, this alias bootstrap MUST be removed by first updating `L2`/`L3`, then changing the manifests and entrypoints.
