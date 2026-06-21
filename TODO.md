# TODO

## Trace Authority

- `/root/repo/fustor/TODO.md` is the single rolling goal trace for the current fs-meta active-three gate.
- Current state: no active first raw failing boundary. The fustor `/tree /` trusted-materialized cold-path latency repair has passed local, aibox, compatible-builder, official redeploy, materialization, and official root-tree pagination validation.
- Do not call `POST /api/fs-meta/v1/index/rescan` unless the user explicitly authorizes it or fresh first-boundary evidence proves stale/incorrect accepted-scope/source/scan repair state and this trace names rescan as the next authorized repair. `pending-materialization`, `initial_audit_completed=false`, `trusted_observation_readiness=false`, and low or absent final index readiness during otherwise healthy catch-up are not rescan triggers.

## Final Target

- Cluster participants are exactly `panda144` / `10.0.82.144`, `panda145` / `10.0.82.145`, and `panda146` / `10.0.82.146`.
- Active logical roots are exactly `nfs-144`, `nfs-145`, and `nfs-146`.
- Expected ownership is one-to-one:
  - `panda144::nfs-144` -> source/scan/sink owner `panda144`.
  - `panda145::nfs-145` -> source/scan/sink owner `panda145`.
  - `panda146::nfs-146` -> source/scan/sink owner `panda146`.
- Each `panda14x` NFS client must mount its own NFS server: `144->144`, `145->145`, `146->146`.
- Capanix is authoritative for bind/run realization and accepted scopes. fs-meta must not fabricate ownership/materialization from local grants, stale cache, or status-only projections.

## Completed Repair

- Local fustor fix in `fs-meta/app/src/query/api.rs`: trusted-ready materialized status cache is no longer discarded before `/tree` settle, and trusted-readiness status load short-circuits when the first route fan-in already proves source/sink trusted readiness. This avoids paying the 5s readiness-settle/redundant source-owner fan-in on a ready tree request while preserving fail-closed request-scoped sink-status gates for broad trusted root `/tree`.
- Request-scoped omitted-ready trusted root pages still fail closed or require bounded materialized entry evidence; the latency fix must not reintroduce empty trusted root pages for ready groups.
- Local validation passed on current worktree: `trusted_materialized_cache_ready_is_not_discarded_before_tree_settle`, `trusted_materialized_status_load_short_circuits_when_route_fanin_is_ready`, `root_tree`, `trusted_tree`, `materialized_status_cache`, `materialized_status_load_plan`, `selected_group_materialized_route_`, plus prior impacted `tree_pit_stage_runtime_`, `selected_group_materialized_route_`, `cargo fmt --check`, and `git diff --check`.
- Spec check: `trusted-materialized` query admission and public `trusted_observation_readiness` use the same per-group source/sink service-readiness evidence. Source `last_audit_completed_at_us` is an exposed status field, but sink `initial_audit_completed=true`, sink `materialization_readiness=ready`, and `trusted_observation_readiness=true` are the hard materialization gates for this boundary.

## Deployment Evidence

- Aibox gate `fullsrc-20260621065338` passed for the local `/tree /` cold-path latency fix. Log: `/tmp/aibox-fsmeta-build/panda145/current/fullsrc-20260621065338-gate.log`.
- CentOS-compatible builder gate `fullsrc-20260621070757` passed. Log: `/tmp/fsmeta-compatible-builder-fullsrc-20260621070757.log`; root: `/home/wanghuajin/fsmeta-build/current/fullsrc-20260621070757`; all deployed ELFs reported `MAX_GLIBC=GLIBC_2.16`.
- Official clean-log redeploy `fullsrc-20260621070757-official-20260621072410-clean1` passed with staged/live hash verification, clean log replacement, node-owned starts, resource announce/deploy/roots apply, self-NFS `144->144`, `145->145`, `146->146`, and no `/index/rescan`.
- Official materialization completed through valid 300s read-only `/status` windows. Final sample `fullsrc-20260621070757-official-20260621072410-clean1-followup22` reported `sink.live_nodes=603000837`, with `nfs-144=201000279`, `nfs-145=201000279`, and `nfs-146=201000279`. All sink groups are `serving-trusted` / ready / `initial_audit_completed=true`; `trusted_observation_readiness=true`; primary host map is one-to-one; no per-root/peer live-node data is missing; count is stable above `200000000`.
- The final `+617209` delta at about `1846/s` is final-tail completion, not a growth-slow defect, because every root reached `201000279` and all sink/trusted readiness gates are satisfied.
- Source roots are active/serving with no error, no overflow, and no rescan pending. Under data-event accounting there is no positive source publication gap blocking trusted readiness; raw `last_audit_completed_at_us=null` remains explanatory source status evidence, not a materialization blocker for this boundary.

## Official Root Tree Acceptance

- Official smoke `fullsrc-20260621070757-official-20260621072410-clean1-followup22-root-tree-smoke1` passed.
- Preflight `/status`: HTTP 200, `sink.live_nodes=603000837`, all three per-root counts `201000279`, trusted readiness true, no status anomalies.
- Query: authenticated `GET /api/fs-meta/v1/tree?path=/&recursive=true&group_order=group-key&group_page_size=1&entry_page_size=20&read_class=trusted-materialized`.
- Result: HTTP 200 in `2734.26ms` under the `5000ms` budget, status `ok`, `read_class=trusted-materialized`, observation `trusted-materialized`, one returned group (`nfs-144`), 20 first-page entries, entry continuation cursor, group continuation cursor, `next_entry_after`, root `/` exists and has children, no anomalies, no full-descendant enumeration, and no `/index/rescan`.

## Long Wait Discipline

- Waiting for materialization means one local foreground command with `sleep 300`, then one bounded authenticated `/status` sample. If the execution tool yields a session id before the terminal sentinel, continue only with empty `write_stdin` on that same session using `yield_time_ms=300000`.
- No short polling, no repeated SSH/status probes, no log tails, no process checks, no date checks, and no fixed `900s` sleeps during an active wait.
- `pending-materialization` is normal on high-cardinality full-root NFS while topology and reachability are healthy. Use patient 300s read-only `/status` observation windows; do not call `/index/rescan` to hurry materialization.
- `/tree /` must not be tested on the official cluster until `/status` is complete/trusted enough, stable or increasing, and the status-returned live-node count is above `200000000`.

## Active Wait

- No active wait.

## Final Acceptance

- `cnxctl cluster status` shows exactly `panda144`, `panda145`, and `panda146` active, reachable, gossip-converged, and `cluster_ready`.
- `cnxctl config dump` shows `fs-meta.__cnx_runtime.app_scopes` includes exactly the three active NFS roots plus the legitimate `fs-meta-tcp-listener` scope.
- Authenticated `/api/fs-meta/v1/status` returns HTTP 200 with complete trusted evidence, or otherwise carries explicit degraded/partial/error evidence.
- `/status` shows one-to-one sink/source/scan ownership and one-to-one `sink.primary_host_ref_by_group`.
- Source concrete roots for all three groups have current owner evidence and are active/serving with no error, no overflow, no rescan pending, and no positive data-publication gap blocking trusted readiness.
- Sink groups for all three roots leave `pending-materialization` and become `serving-trusted` with `initial_audit_completed=true`, `materialization_readiness=ready`, and `trusted_observation_readiness=true`.
- Final complete/non-partial authenticated `/api/fs-meta/v1/status` evidence must show the status-returned live-node index entry count is stable/increasing and exceeds `200000000`, using an explicit global alias if present or aggregate `sink.live_nodes` otherwise.
- Final official `/api/fs-meta/v1/tree` retrieval passes for the widest root path `/`: authenticated `GET /tree?path=/&recursive=true&group_order=group-key&group_page_size=1&entry_page_size=20&read_class=trusted-materialized` returns HTTP 200 with bounded first-page entry data and pagination/continuation evidence when more entries remain; the first page is normal low latency, does not timeout or 503, and does not require full descendant enumeration before responding.

## Superseded

- Older official/aibox/builder iteration details, closed fustor/capanix seams, stale artifact hash lists, wrapper bugs, historical rescan mistakes, and old same-deploy growth calculations are superseded. Reopen them only with fresh raw evidence that reproduces the same boundary.
