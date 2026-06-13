# TODO

## fs-meta giant-root public API parameter governance 2026-06-13

- Front-of-queue precondition before further high-NFS gate work: assume each configured monitoring root may contain more than 1 billion files. Every public fs-meta HTTP endpoint must be reviewed and, where needed, refactored so unsafe or unbounded client parameters cannot force full-tree traversal, unbounded fanout, repeated background jobs, giant response bodies, or request-timeout-based control flow.
- Required public API policy: invalid, unknown, mutually exclusive, out-of-range, non-opaque cursor, or otherwise unbounded query parameters fail fast with HTTP `400 Bad Request`. The server must not silently widen the request, auto-clamp into a misleading "complete" result, or convert permanent parameter invalidity into retryable `503`.
- Required reasonable-query policy: `/tree`, `/on-demand-force-find`, and `/stats` must serve bounded requests through explicit read classes, PIT/cursor continuation, group paging, entry paging, group selection/ranking, caller deadlines, and internal route byte budgets. A root-level recursive request over a giant root is acceptable only as a paged observation session; it must not imply one-response full-root enumeration or full fresh traversal.
- Required management/job policy: `/index/rescan` is a background rescan-intent submission and delivery-proof interface, not a synchronous scan/audit/materialization executor. Repeated requests for the same roots generation and scope must observe or reuse the same active job/lane evidence instead of starting duplicate billion-file scans.
- Required status/diagnostics policy: `/status` and diagnostics endpoints are observation surfaces for readiness, bounded summaries, and recovery/job lane progress. They must not execute file-count-dependent work and must expose enough state for operators to distinguish healthy bounded queries from backpressure, pending materialization, source scan/audit progress, and invalid-client-parameter rejection.
- Required specs-first step: before implementing or tuning timeouts, add/adjust L1/L3 specs for giant-root API parameter governance, including exact bad-parameter status codes, bounded response/fanout semantics, and tests for query/rescan/status non-execution boundaries.
- Local implementation progress 2026-06-13: query endpoints now reject unknown public query parameters and fail fast on route-unsupported `/stats` paging/depth/PIT axes plus malformed/out-of-range tree/force-find paging inputs; `/index/rescan` now uses a shared server-side response budget and schedules source-repair recovery as an observable background lane instead of canceling it through the HTTP wait.

## fs-meta status/recovery design correction 2026-06-13

- Precondition before further `/status` implementation changes: specs now make `/status` an observation endpoint, not the synchronous owner of source/sink retained replay, worker lifecycle repair, roots-control full replay, manual-rescan execution, scan/audit traversal, or sink materialization catch-up.
- Required design fix: move status-coupled repair work into explicit asynchronous recovery lanes owned by the relevant source/sink/facade recovery path. `/status` may read lane state, return closed/degraded readiness, and issue nonblocking wakes; it must not wait for large-NFS/file-count-dependent work or worker start/retry to finish.
- Required observability fix: every background lane that can affect API facade liveness, management write readiness, source repair readiness, trusted observation readiness, source/sink owner fan-in completeness, or materialized group visibility must expose machine-readable state through `/status.repair_lanes[]` or an equivalent documented read-only diagnostics endpoint. Logs alone are insufficient.
- Required lane-state shape: each lane report carries `lane`, `owner`, `state`, `trigger`, `blocking`, `signature`, `updated_at_us`, plus `generation`, `attempt`, `deadline_at_us`, `route_outcome`, and `reason` when meaningful. `state` follows the new `RecoveryLaneState` vocabulary: `idle`, `scheduled`, `inflight`, `blocked`, `completed`, `failed`, `timed-out`, `skipped`.
- Required duplicate-control fix: `/status` nonblocking wakes must be single-flight per recovery lane diagnostic signature. If a source/sink/facade repair lane is already `scheduled` or `inflight`, another status poll observes that lane evidence instead of starting a second retained replay, roots-control replay, worker recovery, source repair, or management-write recovery turn for the same signature.
- Implementation TODO before rerunning the high-NFS `/status` gate: audit current code paths for hidden synchronous status repair; ensure retained generation-cutover replay, roots-control replay, worker recovery, manual rescan execution, scan/audit traversal, and sink materialization catch-up publish lane evidence; add/adjust contract tests so `/status` observes these lanes instead of driving them inline.
- Fresh high-NFS gate update 2026-06-13: first rerun failed before the `/status` assertion at `fs_meta_business_query_real_nfs` initial `POST /index/rescan`; the client hit the 75s HTTP timeout instead of receiving retryable readiness evidence. New front-of-queue design fix: `/index/rescan` must use one bounded response budget across source-repair observation, current-roots fan-in, scoped/generic source-rescan delivery, and source-status delivery-confirmation fallback; it must return accepted or retryable `503 NOT_READY` before the transport timeout, while already scheduled Source Repair recovery continues as an observable background lane.
- Local progress 2026-06-13:
  - Specs now forbid `/status` from synchronously running sink observation repair; the previous narrow status-observation exception is a nonblocking single-flight wake.
  - `/status` source-repair and management-write recovery preflight now schedule background lanes instead of awaiting the recovery hook.
  - status-triggered sink observation repair now schedules a background `status-triggered-sink-observation-repair` lane with a stable source/sink scope signature and returns scheduled/inflight/completed evidence without blocking the response.
  - source scan/audit admission release now schedules a background `source-scan-audit-admission-release` lane keyed by stable source/sink scope evidence; repeated polls observe the lane/cache instead of reissuing the same owner-routed release.
  - Management `/status` deployment-scoped fan-in, owner-scoped fan-in, and sink-status recollect now use short observation budgets instead of serial serving-state 3s waits. The sink prepublication fail-closed fixture no longer waits for repair completion and dropped from about 15s to about 2.8s locally while still preserving partial/fail-closed evidence.
  - `/index/rescan` current-roots preflight now treats scoped source-status cache/degraded/pending or not-receive-armed evidence as immediate retryable not-ready when no partial scoped-delivery proof exists, preserves repair-lane evidence on the returned `503`, and bounds the follow-up source-status target-proof phase with the same response budget.
  - `/index/rescan` manual-rescan current-roots roots-control duplicate suppression no longer expires on the roots-put 2s resend interval; same roots/scope retries now observe the already submitted replay while newly discovered target nodes can still trigger a scoped send.
  - `/index/rescan` no longer waits synchronously for the opportunistic current-roots source-repair observation wake; it runs as a bounded background wake, while scoped source-status route-gap evidence from the actual source owner fails fast as retryable `503` with `source-repair` lane evidence before unrelated peer probes can consume the client timeout.
  - Focused local validation passed: `fs-meta/scripts/validate_specs.sh`, `cargo check -p fs-meta-runtime`, `cargo test --workspace --no-run`, app_specs management API contract, and focused source/management/sink-observation/scan-audit/status-fan-in lane tests.
  - Remaining before high-NFS rerun: run the fresh high-NFS `/status` gate against this candidate and classify any remaining latency as route fan-in, materialization catch-up observation, or unrelated runtime pressure.

## fs-meta Resource-overhead root cause 2026-06-04

- Scope: official `fsmeta-stable` deployment on `10.0.82.144~148`. No more live sampling is needed for this incident; existing artifacts already identify the abnormal process family and state shape.
- Full-root monitoring is a required product state. Each root must remain `subpath_scope="/"`, `watch=true`, and `scan=true`; this is not the root cause.
- The abnormal RSS/CPU came from `/home/wanghuajin/fsmeta-stable/run/bin/capanixd`: samples showed roughly `270GB~306GB` RSS per node with `fs-meta-shared-*` and `tokio-rt-worker` CPU activity.
- Specs recheck:
  - fustor release/specs declare every root group's `runtime.exec.sink` as `eligibility=resource_visible_nodes` plus `cardinality=one`.
  - `runtime.exec.source`, `runtime.exec.scan`, and `runtime.exec.query-peer` are group/member fanout lanes and may be one per root member.
  - capanix runtime L1 requires exactly one eligible node for a one-cardinality unit and fail-closed behavior when resource-visible truth is incomplete.
  - capanix runtime owns the optional state-carrier side boundary; the daemon default installs an in-memory state boundary. `InMemoryStateBoundary` keeps current statecell payload plus up to `256` revision payloads.
- Existing artifacts prove the sink contract was violated: source/scan scheduling was one root per node, but `sink.debug.scheduled_groups_by_node` reported `panda145`, `panda146`, `panda147`, and `panda148` each scheduled for all five groups while `sink.primary_host_ref_by_group` still mapped one primary per group.
- Code-level root cause: sink logical-root refresh calls `sync_active_scopes(SINK_RUNTIME_UNIT_ID, roots)` with all roots and empty `resource_ids`; `RuntimeUnitGate::unit_state()` then treats these authoritative scopes as active sink scopes, and `scheduled_group_ids_from_bound_scopes()` admits them through root selector/grant matching. This bypasses runtime's `resource_visible_nodes + cardinality=one` sink placement and lets non-owner sink workers materialize/report all groups.
- Fustor repair landed locally and was deployed to `10.0.82.144~148`: sink logical-root refresh no longer mints all logical roots as sink scopes. Instead, runtime-managed sink refresh derives authoritative sink scopes only from local active grants that match the current logical roots and local `host_ref`/node id, carrying explicit `object_ref` resource ids such as `panda145::nfs-145`. Management apply, roots-control stream, and authoritative roots-cell refresh update the business root list without widening sink ownership to foreign roots.
- Focused validation passed: local `cargo check -p fs-meta-runtime`; remote deploy-work `cargo check -p fs-meta-runtime`; `roots_control_stream_ignores_older_declaration_after_newer_authoritative_roots`; `full_root_logical_roots_refresh_preserves_runtime_owned_single_sink_group`; `management_logical_roots_update_derives_local_grant_sink_scope`; `status_snapshot_refreshes_logical_roots_without_expanding_sink_schedule`.
- Deployment result on 2026-06-04: built `/home/wanghuajin/fsmeta-stable-src/deploy-work-20260604-214120/fustor/target/release/libfs_meta_runtime.so` with SHA256 `61d9c553095d91c90074945588345461dbeff99ca6934b271638298380fc0d37`; replaced `/home/wanghuajin/fsmeta-stable/run/bin/libfs_meta_runtime.so` on all five nodes; backed up previous SHA256 `279561e459981af7f377a132b7e68705ca12b64545857222447745e16f74e7c7` as `libfs_meta_runtime.so.bak-20260604234658-279561e4`; restarted all five `capanixd`; re-announced 6 resources; redeployed fs-meta app with 162 binds and 81 route plans; re-applied `/home/wanghuajin/fsmeta-stable/run/monitoring-roots-5group.local.json` and got `roots_count=5`.
- Post-deploy API verification artifacts: `/home/wanghuajin/fsmeta-stable/run/artifacts/upgrade-20260604-214120-panda145/api-runtime-grants-after-deploy-20260604-235215.json`, `api-monitoring-roots-after-deploy-20260604-235215.json`, and `api-status-after-deploy-20260604-235215.json`. Verification showed 6 active grants (5 NFS grants plus `panda145::fs-meta-tcp-listener`), 5 full roots with `subpath_scope="/"`, `watch=true`, `scan=true`, and exact one-to-one schedules:
  - sink: `{"panda144":["nfs-144"],"panda145":["nfs-145"],"panda146":["nfs-146"],"panda147":["nfs-147"],"panda148":["nfs-148"]}`
  - source: `{"panda144":["nfs-144"],"panda145":["nfs-145"],"panda146":["nfs-146"],"panda147":["nfs-147"],"panda148":["nfs-148"]}`
  - scan: `{"panda144":["nfs-144"],"panda145":["nfs-145"],"panda146":["nfs-146"],"panda147":["nfs-147"],"panda148":["nfs-148"]}`
- Post-deploy local-node verification: recent daemon logs on all five nodes show each sink scheduling exactly one local group and one local stream target, e.g. `scheduled_groups=Some({"nfs-146"}) scheduled_stream_targets=Some({"panda146::nfs-146"})`. Stream classification logs show each sink applying only its own origin (`apply_events incoming ... origins={"panda146::nfs-146": ...}`) while dropping foreign root events. This directly verifies the sink-scope repair and proves the previous multi-owner sink expansion is gone.
- Post-deploy facade `/status` nuance: while full-root materialization catch-up is under heavy load, repeated facade `/status` polls can still intermittently omit one or more remote nodes from `scheduled_*_groups_by_node` and show that group's `live_nodes=0`; the missing node changes across polls and local node logs still show correct one-owner sink scheduling. Treat this as a separate status remote fan-in/catch-up visibility hardening item, not as recurrence of the sink-scope root cause.
- Post-deploy resource observation: shortly after roots apply, each `capanixd` RSS was about `435MB~538MB`; during active full-root catch-up it rose to about `1.8GB~2.5GB` with scan/materialization CPU around `180%~190%`. That is still far below the previous `270GB~306GB` runaway, and the sink scope remains one-owner during the load.
- Memory mechanism: fustor deploy enables state carrier for `runtime.exec.source` and `runtime.exec.sink`; sink persists a full `PersistedSinkState` after event batches, including all live groups and retained groups. With sink tree heap reported around `1.27GB`, retaining `current + 256` full payload revisions in the in-memory statecell explains the observed `270GB~306GB` RSS scale. This memory is outside `sink.estimated_heap_bytes`.
- Amplifiers, not primary root causes: root-scope manual rescan produced million-level valid data events; repeated sink cutover/repair/facade endpoint rebuild loops amplified CPU/log churn; `force_find_inflight=[]`, so no force-find request was the stuck owner.

Correct expected state for `10.0.82.144~148`:

| host | root | object_ref | NFS mount | source/scan owner | sink owner | sink statecell expectation |
| --- | --- | --- | --- | --- | --- | --- |
| `10.0.82.144` / `panda144` | `nfs-144` | `panda144::nfs-144` | `/mnt/fustor-peers/nfs145` from `10.0.82.145:/data/fustor-nfs` | `panda144` | `panda144` | only `nfs-144` plus bounded legitimate handoff-retained state |
| `10.0.82.145` / `panda145` | `nfs-145` | `panda145::nfs-145` | `/mnt/fustor-peers/nfs146` from `10.0.82.146:/data/fustor-nfs` | `panda145` | `panda145` | only `nfs-145` plus bounded legitimate handoff-retained state |
| `10.0.82.146` / `panda146` | `nfs-146` | `panda146::nfs-146` | `/mnt/fustor-peers/nfs147` from `10.0.82.147:/data/fustor-nfs` | `panda146` | `panda146` | only `nfs-146` plus bounded legitimate handoff-retained state |
| `10.0.82.147` / `panda147` | `nfs-147` | `panda147::nfs-147` | `/mnt/fustor-peers/nfs148` from `10.0.82.148:/data/fustor-nfs` | `panda147` | `panda147` | only `nfs-147` plus bounded legitimate handoff-retained state |
| `10.0.82.148` / `panda148` | `nfs-148` | `panda148::nfs-148` | `/mnt/fustor-peers/nfs144` from `10.0.82.144:/data/fustor-nfs` | `panda148` | `panda148` | only `nfs-148` plus bounded legitimate handoff-retained state |

Repair direction:

- Sink-owner bug is fixed in fustor and verified on `10.0.82.144~148`.
- Remaining hardening: bound statecell retention by bytes or avoid full-snapshot revision retention for giant sink payloads; retaining `256` full sink snapshots is unsafe as a default for large statecells even after the sink-owner bug is fixed.
- Separate follow-up: harden facade `/status` remote fan-in during high-load materialization catch-up so debug maps do not intermittently omit otherwise healthy local owners.

## fs-meta Legacy Refactor Context

### 重构原因

- 当前 `fs-meta` 的主控制流长期由多层 helper、generic runtime loop、组合态 phase 和预算式 retry 叠出来，产品语义被分散在 wrapper、executor、runtime、call site 几层之间。
- 这套设计的核心问题不是“代码有点旧”，而是它本身就晦涩、绕、难推理，而且天然易碎。
- 这种结构的直接后果是：
  - 状态机真相不单一，容易出现“外层壳子改薄了，但更深 owner 继续活着”的情况。
  - retry / fallback / wait / timeout 语义经常由基础设施代码拥有，而不是由领域状态机拥有。
  - 一旦出问题，first raw failing boundary 会不断往更深处移动，调试和交接成本都很高。
  - 分层剥壳式重构虽然每轮都有进展，但总会暴露出下一层 legacy seam，看起来永远“还剩最后一截”。
- 换句话说，旧设计的问题是：
  - 不直观
  - 不稳定
  - 不容易证明正确
  - 不适合作为长期维护的状态机模型
- 所以这次不再接受“继续往里推 owner”的渐进式修补，而是要直接替换掉当前最深的 policy owner。

### 重构目标

- 把 `fs-meta` 剩余的核心控制流统一收成少量显式、领域化、可读的状态机。
- 目标不是“更现代一点”或“更抽象一点”，而是明确追求：
  - 简单
  - 直接
  - 鲁棒
  - 易于理解
- 让每条产品路径只保留一个真正的领域状态机 owner：
  - machine 决定策略
  - dispatcher 只做 IO
  - wrapper 不再持有 retry / fallback / policy
- 去掉 generic helper loop、组合态 phase、基础设施型 retry kernel 作为架构核心，改成简单、直接、鲁棒、稳定、易懂的领域真相。
- 让 timeout 只作为 terminal safety bound，而不是主行为 owner；让 wait 只由领域进度释放，而不是由 generic backoff / generic gate 释放。
- 如果某个 bug 的 first raw failing boundary 已经明确证明根因在 `capanix`，则允许直接跨仓修订 `capanix`，而不是继续把问题留在 `fustor` 侧绕行或包壳。
- 一旦进入这种跨仓修订场景，必须使用专门的 `fs-meta-cross-repo-gate` 技能和共享 blocker-state 文档 `/root/repo/capanix/todo.md` 来协调，不再做临时口头同步或 ad hoc 交接。
- `fustor` repo-local closure 和 `capanix` cross-repo 确认必须分开记录；只有 first raw boundary 真的跨到 `capanix` 之后，才把 active owner 切过去。
- 最终交付标准是：
  - deepest owner 被真正替换
  - legacy seam 从代码里删除，而不是只被包得更深
  - 新 session 接手时可以直接按领域状态机继续推进，而不是重新猜哪一层才是真 owner
  - 读代码的人可以直接看懂系统当前处于什么阶段、为什么等待、为什么重试、为什么失败

## fs-meta / capanix Boundary Follow-ups

- Evaluate lifting `fs-meta/app/src/runtime/unit_gate.rs` into a generic `capanix` runtime helper. It is the highest-value reusable mechanism: unit allowlist, activate/deactivate/tick acceptance, generation fencing, and active-scope merge. Keep it out of default SDK authoring surfaces unless `capanix` explicitly blesses a runtime-unit control cache/helper layer.
- Consider splitting the generic shell out of facade pending activation in `fs-meta/app/src/runtime_app.rs`: pending record, retry scheduling, retry error bookkeeping, and exposure-confirmed cutover tracking. Keep `observation_eligible` gating and facade-specific cutover semantics in `fs-meta`.
- Keep query observation evidence assembly in `fs-meta`. `capanix-managed-state-sdk` already owns the shared evaluator; `fs-meta` should continue owning candidate-group selection, degraded/overflow evidence shaping, and source/sink status merge policy.
- Keep source planner and sink runtime-group policy in `fs-meta`. Logical-root fanout, source-primary selection, candidate/draining handoff state, and host-grant-to-runtime-group reconciliation are product semantics, not generic platform policy.

## fs-meta Legacy Hard-Cut Handoff

### Why the earlier approach was wrong

- The earlier rollout kept doing `shell-removal + keep-green`:
  - remove one outer helper/wrapper seam
  - immediately force touched suites back to green
  - stop as soon as the next raw failing boundary appeared
- In this repo that always exposed the next deeper owner, so the visible result was:
  - each turn made real progress
  - but the next deepest legacy seam kept surfacing
  - so the branch never felt “one-shot cut over”
- Current rule for the remaining work:
  - target the deepest owner directly
  - allow short-lived red in the middle of the refactor
  - only validate at the end of each meaningful subsystem cut

### What has already landed

- `runtime_app`:
  - `RuntimeControlState` is no longer the old `AwaitingControl* / Recovering* / Ready` combination enum.
  - It is now structural truth:
    - `RuntimeLifecyclePhase`
    - `RuntimeControlState { lifecycle, source_replay_required, sink_replay_required }`
  - local republish has already been thinned into:
    - `LocalSinkStatusRepublishMachine`
    - `LocalSinkStatusRepublishObservation`
    - `LocalSinkStatusRepublishStep`
- `workers/source`:
  - detached reconnect is gone from product paths
  - fixed backoff sleep is gone from the main refresh/retry paths
  - refresh waiting is now progress-gated rather than blind-sleep gated
- `workers/sink`:
  - close-drain is notifier/watch driven
  - detached retry reset is gone; retry reset now uses awaited restart
- `query/api`:
  - `force-find` route has been pulled out of the shared generic collect loop
  - `ForceFindRouteMachine` now owns selected-runner planning, fallback, wait, and terminal error inside `execute_force_find_group_route_collect(...)`

### Current deepest owners still alive

- `query/api` route layer
  - `execute_status_route_collect(...)`
  - `execute_tree_pit_proxy_route_collect(...)`
  - shared `execute_runtime_collect_loop(...)`
- `query/api` PIT/session layer
  - `execute_tree_pit_ranked_session_parts(...)`
  - `execute_force_find_ranked_session_parts(...)`
  - `build_pit_session_parts(...)`
- `runtime_app`
  - `execute_local_sink_status_republish_until_terminal(...)`
  - `dispatch_local_sink_status_republish_step(...)`
  - this is thinner than before, but still a helper-local recovery loop
- `workers/source`
  - `SourceRefreshRuntimeAction`
  - `wait_for_source_progress_after(...)`
  - `on_control_frame_with_timeouts(...)`
  - current model is cleaner, but still an infrastructure-heavy retry kernel rather than a small domain machine
- `workers/sink`
  - no longer the main battle
  - remaining residue is mostly `on_control_frame_with_timeouts(...)` + deadline-owned awaited restart

### Concrete implementation plan for the next session

1. Finish `query/api` route hard cut.
   - Replace the remaining generic route owner with two explicit machines:
     - `StatusRouteMachine`
     - `ProxyRouteMachine`
   - Keep the rule already used for `ForceFindRouteMachine`:
     - machine owns retry/fallback/wait/error policy
     - wrapper only builds request and decodes response
   - After this cut, delete:
     - `execute_runtime_collect_loop(...)`
     - `RuntimeCollectLoopStep`
     - `StatusRouteRuntime`
     - `TreePitProxyRouteRuntime`

2. Finish `query/api` session hard cut.
   - Replace `execute_*_ranked_session_parts(...)` with explicit session machines:
     - `TreePitSessionMachine`
     - `ForceFindPitSessionMachine`
   - Final `PitSession` assembly must live in one shared builder only.
   - Delete:
     - `PitSessionParts`
     - `build_pit_session_parts(...)`
     - `Prepared*RankedSession`
     - `execute_*_ranked_session_parts(...)`

3. Finish `runtime_app` local republish hard cut.
   - Collapse the current `machine + observation + step + dispatch` layering into one true `LocalRepublishMachine`.
   - Remove helper-local terminal dispatch ownership:
     - delete `dispatch_local_sink_status_republish_step(...)`
     - keep one single entrypoint that runs the machine to completion
   - cached-ready fast path, retained replay, manual-rescan scheduling, and timeout interpretation must all live inside the machine phase model.

4. Finish `workers/source` hard cut.
   - Replace the current `SourceRefreshRuntimeAction + mask/cursor` model with a simpler domain machine:
     - `SourceRetryMachine`
     - `SourceStateSnapshot`
     - `SourceWaitFor`
   - `SourceWaitFor` should be domain reasons only:
     - control retention or reconnect
     - scheduled-groups or reconnect
     - observability or reconnect
     - started or reconnect
     - logical-roots or reconnect
     - force-find readiness
   - `on_control_frame_with_timeouts(...)` must only do:
     - decode
     - classify
     - dispatch machine step
   - It must stop owning retry/reconnect/wait lanes directly.

5. Only if needed after source/query/runtime cuts, finish `workers/sink`.
   - If sink still contains the last obvious imperative retry hotspot, fold it into:
     - `SinkControlRecoveryMachine`
     - `SinkCloseMachine`
   - Do not reopen a broad sink redesign unless the upstream cuts expose it as the new first raw boundary.

### Validation baseline to keep using

- Core checks
  - `cargo check -p fs-meta-runtime`
- Runtime app exacts
  - `cargo test -p fs-meta-runtime runtime_control_state_ -- --nocapture`
  - `cargo test -p fs-meta-runtime local_sink_status_republish_runtime_ -- --nocapture`
  - `cargo test -p fs-meta-runtime wait_for_local_sink_status_republish_requiring_probe_ -- --nocapture`
- Query exacts
  - `cargo test -p fs-meta-runtime force_find_route_machine_ -- --nocapture`
  - `cargo test -p fs-meta-runtime force_find_route_runtime_ -- --nocapture`
  - `cargo test -p fs-meta-runtime selected_group_force_find_route_ -- --nocapture`
- Source/sink exacts already used earlier in this branch remain relevant if their owners move again:
  - source handoff / start / logical-roots / close-wait targeted suites
  - sink nonblocking / scheduled-group / control-frame targeted suites

### Session handoff notes

- The worktree is very dirty. Do not revert unrelated changes.
- If a newly captured first raw failing boundary proves the blocker has crossed into `capanix`, switch to the dedicated `fs-meta-cross-repo-gate` workflow immediately.
  - Treat `/root/repo/capanix/todo.md` as the shared blocker-state document.
  - Keep repo-local `fustor` closure and cross-repo `capanix` confirmation as separate states.
- A concurrent `cargo` build/test run has produced a transient sink-worker fenced failure in `wait_for_local_sink_status_republish_requiring_probe_`.
  - When rerun alone, that suite passed.
  - Treat that as transient target-dir interference unless it reproduces in an isolated rerun.
- Latest known stable validation before handoff:
  - `cargo check -p fs-meta-runtime`
  - `runtime_control_state_` green
  - `local_sink_status_republish_runtime_` green
  - `wait_for_local_sink_status_republish_requiring_probe_` green in isolated rerun
  - `force_find_route_machine_` green
  - `force_find_route_runtime_` green
  - `selected_group_force_find_route_` green
