# TODO

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
