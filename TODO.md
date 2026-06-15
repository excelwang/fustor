# TODO

## Trace Authority

- This file, `/root/repo/fustor/TODO.md`, is the only canonical goal trace for the current fs-meta work.
- Do not create, update, or rely on `/root/repo/capanix/todo.md`; that file was removed to prevent split-brain goal state.
- The active owner is the side named by the current first raw failing boundary. Record any Capanix-owned evidence here by absolute path only if fresh raw evidence moves ownership across repos.
- The target deployment/status goals below were recovered from deleted `/root/repo/capanix/todo.md` at `../capanix` commit `3131bf53` (parent of removal commit `11b94b02`). Only target architecture and acceptance goals are imported; stale blocker ownership from that deleted file is not active state.

## Final Target: Active-Three fsmeta-stable

- Target cluster: exactly three active Capanix/fs-meta participants: `10.0.82.144` / `panda144`, `10.0.82.145` / `panda145`, and `10.0.82.146` / `panda146`.
- Excluded nodes: `10.0.82.147` / `panda147` may remain the external NFS data server for `panda146`'s mounted source, and `10.0.82.148` / `panda148` may be an external NFS data server only if a future active-three mount plan explicitly uses it. Neither node may run `capanixd`, host fs-meta runtime processes, or be selected as a source/scan/sink owner for this target.
- Active logical roots are exactly `nfs-144`, `nfs-145`, and `nfs-146`.
- Owner identity follows the app-visible host object that makes a root visible to the runtime, not the NFS server IP.
- Capanix remains the authority for bind/run realization and runtime accepted scopes. fs-meta consumes accepted scope evidence and must not fabricate sink/source/scan ownership from local grants, stale cache, or status-only projections.
- `/api/fs-meta/v1/status` must not publish a smaller complete total when owner fan-in is partial. It must aggregate all active owner evidence or explicitly mark the response degraded, partial, or failing.
- Final success is measured by correctness, stable ownership/materialization, complete/non-partial `/status` evidence, and stable index growth, not by CPU-efficiency targets.

Expected active-three topology:

| host | logical group | host object | mounted source | expected source/scan owner | expected sink owner |
| --- | --- | --- | --- | --- | --- |
| `10.0.82.144` / `panda144` | `nfs-144` | `panda144::nfs-144` | `/mnt/fustor-peers/nfs145` from `10.0.82.145:/data/fustor-nfs` | `panda144` | `panda144` |
| `10.0.82.145` / `panda145` | `nfs-145` | `panda145::nfs-145` | `/mnt/fustor-peers/nfs146` from `10.0.82.146:/data/fustor-nfs` | `panda145` | `panda145` |
| `10.0.82.146` / `panda146` | `nfs-146` | `panda146::nfs-146` | `/mnt/fustor-peers/nfs147` from `10.0.82.147:/data/fustor-nfs` | `panda146` | `panda146` |

Excluded-node role constraints:

| host | allowed role in this target | disallowed role |
| --- | --- | --- |
| `10.0.82.147` / `panda147` | external NFS server for `panda146`'s mounted source | Capanix peer, fs-meta app owner, source/scan/sink owner |
| `10.0.82.148` / `panda148` | external NFS server only if a future active-three mount plan explicitly uses it | Capanix peer, fs-meta app owner, source/scan/sink owner |

Final acceptance criteria:

- `cnxctl cluster status` shows exactly three active peers: `panda144`, `panda145`, and `panda146`.
- `panda147` and `panda148` have no running `capanixd`, `capanix_worker_host`, `libfs_meta_runtime.so`, or fs-meta source/sink worker processes for this deployment.
- `cnxctl config dump` shows `fs-meta.__cnx_runtime.app_scopes` includes exactly the three active NFS logical roots plus only legitimate bootstrap/listener scopes.
- Authenticated `/api/fs-meta/v1/status` returns HTTP 200 only for complete evidence, or otherwise carries explicit degraded/partial/error evidence.
- After steady-state convergence, each active node has exactly one current fs-meta runtime process and no live draining predecessor for the same app identity.
- `/status` shows one-to-one `sink.primary_host_ref_by_group` for all three active NFS groups.
- `/status` shows one-to-one `sink.debug.scheduled_groups_by_node`, `source.debug.scheduled_groups_by_node`, and `scan.debug.scheduled_groups_by_node`; no node claims all three sink groups.
- Each sink statecell contains only its scheduled group plus bounded legitimate handoff-retained state.
- All three sink groups accumulate live nodes under their expected owners.
- Source concrete roots for all three groups have current owner evidence and completed current-audit evidence: `last_audit_started_at_us != null`, `last_audit_completed_at_us != null`, and completion belongs to the current revision/generation.
- Sink groups for all three roots leave `pending-materialization` and become trusted/materialized: `initial_audit_completed=true`, `materialization_readiness=ready`, `service_state=serving-trusted`, and `trusted_observation_readiness=true`.
- Stable dashboard/API totals, including `/api/fs-meta/v1/status` totals, do not flash backward between complete and partial aggregates.
- Complete/non-partial authenticated `/api/fs-meta/v1/status` evidence shows global live-node index entries continue stable forward growth and exceed `200,000,000`; any lower or stalled sample is acceptable only when explicitly marked degraded, partial, or failing.

## Active Gate: Runtime Retained Sink Route Scope Repair Pending Aibox/Redeploy/Final Verify

- Active owner: `fustor`.
- Current first raw boundary: official `20260615105851-external-clean` active-three deployment used the current high-fanout sink-tree fix and correct compatible artifacts, but did not converge past owner-scoped stream delivery. After a local 300s wait, `panda145:/home/wanghuajin/fsmeta-stable/run/.fsmeta-state/validation/20260615105851-external-clean/status-20260615112026.json` returned HTTP 200 with no explicit `degraded`, `partial`, or non-empty `error` field, correct one-to-one sink/source/scan maps, `runtime_artifact.sha256=ce89a1feec4a5da54718f7c5ec3bb537491acd37c200d071bb9a663ffbd76e61`, and readiness planes `api_facade_liveness=true`, `management_write_readiness=true`, `source_repair_readiness=true`, `trusted_observation_readiness=false`. `sink.live_nodes` advanced only from `3172568` to `11641327`, entirely under `nfs-144`; `nfs-145` and `nfs-146` stayed `live_nodes=0`, `selected-pending`, and `pending-materialization`. Source publication evidence advanced on all three nodes (`source.published_events_by_node={"panda144":11641580,"panda145":9363200,"panda146":11568490}`), while sink receive evidence existed only for `panda144` (`sink.debug.received_events_by_node={"panda144":11641329}`) and `panda145/panda146` had no sink received/applied events. Process checks showed `panda144`, `panda145`, and `panda146` each still had one `capanixd`, one sink worker, and one source worker, with no active stable-run runtime on `panda147/148`. Focused status/log evidence shows `panda145` retained sink control signals for sink event/status/root-control routes were repeatedly bootstrap-only (`__fsmeta_empty_roots_bootstrap`) despite current roots containing `nfs-145`.
- Current exact seam: runtime-app retained sink route state must not let a later bootstrap-only route-liveness activate overwrite an existing retained business sink route scope while that logical root is still current. The worker-level sink gate already preserves business schedules across bootstrap-only activate waves; runtime-app retained state must match that semantic so source-scoped sink replay and local republish do not rebuild owner routes from bootstrap-only scopes. Empty-roots deploy must still be able to clear noncurrent business scopes.
- Latest local repair: `fs-meta/app/src/runtime_app.rs` now preserves retained business sink route scopes when a bootstrap-only activate arrives for the same sink route and the business group is still present in the current source logical roots, while rebasing the retained signal to the newer generation. When current logical roots no longer contain the business group, bootstrap-only activation is still allowed to clear the retained business scope, preserving empty-roots semantics. The prior high-fanout sink-tree repair in `fs-meta/app/src/sink/tree.rs` remains closed locally.
- Latest focused/local validation passed for this seam: `cargo test -p fs-meta-runtime retained_sink_state_ -- --nocapture --test-threads=1`; `cargo test -p fs-meta-runtime source_scoped_sink -- --nocapture --test-threads=1`; `cargo test -p fs-meta-runtime bootstrap_only_sink_activate_does_not_clear_existing_business_route_scope -- --nocapture --test-threads=1`; `cargo test -p fs-meta-runtime sink_recovery_machine_post_recovery_ignores_bootstrap_only_sink_scope -- --nocapture --test-threads=1`; `cargo test -p fs-meta-runtime owner_scoped_roots_control_replay_arms_events_stream_and_ingests_owner_events -- --nocapture --test-threads=1`; `cargo test -p fs-meta-runtime active_stream_route_refreshes_dynamic_object_refs_and_accepts_new_origin_batches -- --nocapture --test-threads=1`; `cargo fmt -p fs-meta-runtime -- --check`. The pre-existing `is_receive_polling` dead-code warnings remain unchanged.
- Latest closed candidate gates now superseded by the retained-sink-state repair: `fullsrc-20260615103335` aibox build/focused/high-NFS passed, `fullsrc-20260615104414` `panda145` compatible-builder passed with `GLIBC_2.16` artifacts, and official `20260615105851-external-clean` clean-log redeploy/restore completed on `panda144-146` only. Do not treat those artifacts as final candidates because the official `/status` evidence above exposed the new fustor-owned retained route-scope boundary.
- Latest closed fustor seams: high-fanout sink tree inserts update cached aggregates by delta instead of full sibling recompute/prune; spawned-and-gated sink stream endpoints now publish receive readiness without claiming received/applied events or trusted materialization; status-observation timeout log labeling no longer emits `operation timed out`; bootstrap-only sink activate waves no longer clear an existing runtime-managed business sink schedule; `/index/rescan` no longer waits indefinitely in current-roots readiness; manual-rescan current-roots readiness no longer waits for stalled optional local source observation after source-status fan-in completes; deferred sink-generation-cutover repair no longer retries `tx busy`/no-progress worker-control failures at runtime-state-change cadence; replay/apply-pending owner-scoped sink-status route-cache returns business stream/applied telemetry even when group rows are missing; API `/status` response timeout no longer waits for stuck internal status cancellation/drop and pending-facade owner-scoped status probes use the short observation budget; request-scoped source publication waits for downstream-accepted audit `EpochEnd`; unsplittable source publication timeout/backpressure fails closed; stale grant attachment retry count no longer resets on idle timeout; scan/audit admission-release cache key no longer churns on sink progress/readiness or route/control generation for the same stable scope; source scan/audit admission release no longer times out an owner-scoped source-status route by spending a second full live observation budget after release. Reopen these only with fresh raw evidence.
- Next bounded iteration: rerun the complete-source aibox build/focused/high-NFS clean-log gate from the current worktree, then rerun the `panda145` CentOS 7-compatible builder gate, then clean-log redeploy/restore on `panda144-146` only and verify final complete/non-partial `/status` trusted/materialized evidence with stable live-node growth past `200,000,000`. If any long wait is needed, wait locally and then run one short SSH sampling command; do not put `sleep` inside SSH scripts or repeatedly poll local sleep status. Do not include `panda147` or `panda148` in active runtime.

Governing policy for the remaining gate:

- Sink materialized tree lifecycle is hosting-bound and rebuildable; fs-meta must not require a durable full-tree snapshot to complete startup, scan/audit, or materialization.
- Scan/audit publication backpressure is valid bounded flow control, but it must recover and continue making progress. It must not become a permanent stall at a routine checkpoint.
- `/status` and diagnostics are observation surfaces. They may issue documented nonblocking single-flight wakes, but must not synchronously run retained replay, worker lifecycle repair, roots-control full replay, manual-rescan execution, scan/audit traversal, or sink materialization catch-up.
- `/status` must fail closed or explicitly mark partial/degraded evidence when source/sink owner fan-in or transport/materialization evidence is incomplete; it must not fabricate sink ownership or materialization from grants, route activation, stale cache, or status-only projections.
- A replay/apply-pending sink-status route-cache fallback may expose bounded progress telemetry, but must not claim trusted/materialized readiness from schedule-only evidence.
- Trusted/materialized readiness requires matching source publication and sink transport/materialization evidence. Source publication, route activation, grants, cached status, or schedule-only evidence must not claim trusted readiness when a sink owner has zero received/applied events.
- Source scan/audit admission release may occur only after current sink accepted-scope schedule evidence covers the same root groups or a permitted sink observation repair completes. That release is not source-to-sink delivery proof.

## Validation Order

1. Re-read this file and the governing specs before each bounded iteration.
2. Closed locally for the current boundary: runtime-app retained sink route state preserves current business route scopes across bootstrap-only activate waves while still allowing empty-roots cleanup.
3. Next: rerun fresh complete-source aibox build/focused/high-NFS clean-log gate for the current worktree.
4. Then rerun fresh `panda145` CentOS 7-compatible builder artifact gate and prove deployable `GLIBC_2.17` or lower artifacts.
5. Then perform clean-log active-three external-worker redeploy/restore on `panda144-146` only.
6. Final verification by authenticated `/api/fs-meta/v1/status`; evidence must be complete/non-partial, trusted/materialized, active-three only, and show stable live-node index growth past `200,000,000`.

## Closed / Superseded

- Older small/high gate failures, five-node ownership/resource-overhead state, deleted `/root/repo/capanix/todo.md` active-boundary entries, and prior locally closed fustor/capanix sub-seams have been removed from the active TODO. Reopen only with fresh raw evidence.
- Superseded by `fullsrc-20260614230500` fresh high-NFS evidence and local validation: the `fullsrc-20260614223525` node-b `/status` id `16` liveness failure with the tight deferred `sink-generation-cutover` `tx busy` retry loop. Reopen only if fresh raw evidence again shows `/status` blocked behind immediate sink repair retries.
- Closed by `fullsrc-20260614180253` fresh real-small/high-NFS evidence: the `fullsrc-20260614170553` node-b facade/API liveness failure during materialization polling and the source-worker `OwnershipFast` retained-replay status race. Reopen only if fresh raw evidence again shows `/status` or `/session/login` liveness failure.
- Closed by `fullsrc-20260614170553` raw evidence and local validation: the `fullsrc-20260614163227` small-gate source/status fan-in `nfs-144` coverage gap. Reopen only if a fresh run again reports non-empty source or scan coverage gaps.
- Superseded by `fullsrc-20260614163227`: the `fullsrc-20260614160056` small-pass/high-fail boundary. The newer node-c route-cache telemetry gap is tracked in Active Gate above and is closed locally pending fresh high-NFS validation.
- Closed by `fullsrc-20260614135801` small-gate progress: the prior node-b `/status` id `33` facade stall during pending materialization and the source scan/audit admission-release wait-for-observed-epoch seam. Reopen only if fresh raw evidence shows `/status` again waiting for scan/audit/materialization work.
- Closed locally and crossed by fresh aibox build/focused/high validation: sink full-tree statecell checkpoint blocking and generic sink-status replay/apply-pending no-reply. Reopen only if fresh raw evidence points back to checkpoint/statecell persistence or `/status` liveness.
- Closed: earlier facade owner-provenance loss and the prior `panda145` compatible-builder gate. Reopen only with fresh raw evidence.
- Closed: active-three artifact staging, clean-log live replacement/restart, resource announce, fs-meta app deploy, and roots apply on `panda144-146` for `20260614122654`. Reopen deploy mechanics only with fresh official-cluster evidence.
- Closed: official roots file schema repair and accepted-scope followup. Reopen only if fresh preview/config/status evidence shows incorrect root/grant matching or missing active-three `app_scopes`.
