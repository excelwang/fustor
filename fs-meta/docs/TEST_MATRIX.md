# fs-meta Test Matrix

This matrix is organized by failure-localization depth. A higher rung should
add exactly one class of risk; it must not rediscover semantics that should have
failed earlier.

## Ladder

| Rung | Suite | Environment | Boundary proved |
| --- | --- | --- | --- |
| L1 | `contracts-fast` | local, no runtime, no real NFS | Domain contracts and pure source/sink/query semantics. |
| L2 | `single-process-closed-loop` | local tmpfs/mock worker | Roots → source audit → sink materialization → status/query in one app boundary. |
| L3 | `runtime-local-multinode` | local capanix runtime/worker-host | Route scope, route direction, owner fan-in, control replay, and worker handoff. |
| L4 | `nfs-environment-gate` | mini/full real NFS | NFS mounts, grants, roots assets, bounded readiness, and data-volume tolerance. |
| L5 | `l5` | full real cluster | Deploy/upgrade, API management, source/sink readiness, query, resilience, and operational regressions. |

Compatibility aliases stay available while reports move to the new names:

- `business-fast` = L1 + L2 compatibility bundle.
- `business-mini-nfs` = L4 mini-NFS subgate.
- `environment-full-nfs` = L4 full-NFS subgate.
- `operations-local` = L3 compatibility alias.

Legacy L5 public names are not retained. Use `l5` for the full L5 gate,
`l5 acceptance` for the seven acceptance stages, and `l5 ops` for operational
regression groups.

## Order

1. `contracts-fast`: catch product/spec mistakes before any runtime or NFS noise.
2. `single-process-closed-loop`: prove the app boundary can close roots/source/sink/query locally.
3. `runtime-local-multinode`: prove capanix route/control behavior without real-NFS variables.
4. `nfs-environment-gate`: prove real mounts and roots assets without full operational recovery.
5. `l5`: prove the deployed cluster is usable, resilient, and covered by the
   operational regression set.

When L5 fails, do not keep repeating L5. Capture the first raw boundary, then
drop to the lowest rung that can reproduce it.

## L5 Stages

`l5` has twelve visible stage positions. The first seven are acceptance stages:

1. `preflight`: SSH/process/socket/NFS/env prerequisites are present.
2. `deploy-upgrade`: release upgrade/deploy path applies a new generation.
3. `management-api`: login, grants, roots preview/apply, and key management work.
4. `source-audit`: source owners accept rescan and publish initial audit evidence.
5. `sink-materialization`: sink owners materialize and report ready groups.
6. `query`: `/tree`, `/stats`, and `/on-demand-force-find` return successful data-plane responses.
7. `resilience`: restart/failover/upgrade-continuity paths keep the cluster usable.

The final five are operational regression groups:

8. `foundation-real-runtime`: selected-group proxy, force-find semantics, and facade live visibility.
9. `upgrade-core`: peer source-control completion, sink control roles, and runtime scope.
10. `topology-change`: new NFS join, root path modify, NFS retire, and upgrade-window join.
11. `recovery-switch`: facade/resource switch and activation-scope recovery checks.
12. `resource-budget`: upgrade CPU budget.

Each stage prints `l5_stage=x/12`, `l5_phase=acceptance|ops`, and
`boundary=<name>`. The default `l5` command runs all twelve stage positions.
It covers 10 concrete acceptance test filters plus 15 concrete ops filters,
with `preflight` as an environment marker. Focused reruns use `l5 acceptance`,
`l5 ops`, a stage group, or a canonical atomic ops stage.

## Public Commands

Use `fs-meta/docs/examples/test-matrix-commands.sh` from the repository root:

```bash
fs-meta/docs/examples/test-matrix-commands.sh contracts-fast
fs-meta/docs/examples/test-matrix-commands.sh single-process-closed-loop
fs-meta/docs/examples/test-matrix-commands.sh runtime-local-multinode
fs-meta/docs/examples/test-matrix-commands.sh nfs-environment-gate mini
FSMETA_FULL_NFS_ROOTS_FILE=/path/to/roots.json \
  fs-meta/docs/examples/test-matrix-commands.sh nfs-environment-gate full
FSMETA_FULL_NFS_ROOTS_FILE=/path/to/roots.json \
  fs-meta/docs/examples/test-matrix-commands.sh l5
```

Composite suites:

- `progressive-business` = L1 + L2.
- `progressive-environment` = L1 + L2 + L3 + L4.
- `progressive-operations` = L1 + L2 + L3 + L4 + L5.

## Entrypoints

| Suite | Entrypoints |
| --- | --- |
| `contracts-fast` | app/cli/spec contract tests plus root-relative `subpath_scope` and sink scope-reset tests. |
| `single-process-closed-loop` | local source initial audit, selected-group, force-find, status-stats, and roots-put runtime tests. |
| `runtime-local-multinode` | source/sink worker control, sink status, and `runtime_scope_e2e`. |
| `nfs-environment-gate mini` | `fs_meta_business_mini_real_nfs`. |
| `nfs-environment-gate full` | full real-NFS query/environment/live-only rescan gates using `FSMETA_FULL_NFS_ROOTS_FILE`. |
| `l5` | full L5 gate: seven acceptance stages plus five operational regression groups backed by real-NFS runtime/API filters. |

## Deployment Regression Mapping

Recent real-cluster deployment failures must be caught at the lowest owner layer:

| Failure class | Lowest rung | Required proof |
| --- | --- | --- |
| Root subpath appears as a materialized output prefix | L1 `contracts-fast` | `subpath_scope` selects the host subtree but emitted paths remain root-relative. |
| Updating unchanged roots rebuilds unchanged materialized groups | L1 `contracts-fast` | Sink materialization resets only when the covered root scope actually changes. |
| Source audit stays pending after roots apply | L2 `single-process-closed-loop` | Root task wakes source audit and reaches observable readiness locally. |
| Sink is scheduled but no trusted/query evidence appears | L2 `single-process-closed-loop` | Roots → source → sink → trusted/query closes without fake visible rows. |
| Owner status or control reply is lost across nodes | L3 `runtime-local-multinode` | Runtime routes, owner fan-in, and control replay are visible without real NFS. |
| Full source audit depends on unpredictable file volume | L4 `nfs-environment-gate` | Environment gate reports bounded/degraded evidence, not a hard-coded file-count budget. |
| Upgrade, failover, or topology recovery breaks a deployed cluster | L5 `l5` | Twelve-stage L5 progress identifies the first real-cluster boundary. |

## Real-NFS Rules

All real-NFS tests stay ignored by default. They require
`CAPANIX_REAL_NFS_E2E=1`, a valid `CAPANIX_WORKER_HOST_BINARY`, and matrix-script
execution.

Concrete demo host addresses, passwords, query keys, and mount paths stay out
of the repository. Use local env files or templates for environment-specific
values.

Mini NFS and full NFS must use separate exports. Full-NFS data volume is
unpredictable, so L4 validates topology, grants, bounded readiness, coverage
mode, and degraded evidence without hard-coded file-count or fixed-time
budgets. Strict trusted materialization belongs to deterministic small gates or
L5.

## Root-Fix Discipline

A failing suite must report:

- `current_ladder_position`
- `first_raw_boundary`
- `first_raw_error`
- `domain_state`
- `progress_effect`

Forbidden workarounds include skipped assertions, weakened semantics, fixed
large sleeps, fake readiness, stale cached success across changed epochs,
demo-only production branches, and hiding missing metadata or management-write
failures.
