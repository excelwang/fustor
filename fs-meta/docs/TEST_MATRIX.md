# fs-meta Test Matrix

This matrix is organized by execution order: business functionality first,
environment adaptation second, and operational recovery last. Operations tests
(worker restart, upgrade, failover, handoff, and generation-skew scenarios) do
not block the business/API/NFS baseline unless their own suite fails.

## Matrix

| Stage | Suite | Environment | Covers |
| --- | --- | --- | --- |
| 1 | `business-fast` | local/tmpfs/mock worker | Contracts, query, force-find, status, and management roots apply/rescan. |
| 2 | `business-mini-nfs` | 5-node mini real NFS with about 10 files per export | Login, status, roots apply/rescan, query, force-find, and key API behavior. |
| 3 | `environment-full-nfs` | 5-node / 5-NFS full demo validation environment | Real topology, real grants, large-data readiness, coverage mode, and degraded evidence. |
| 4 | `operations-local` | local worker/runtime | Source/sink control, control replay, handoff, and generation-skew style runtime behavior. |
| 5 | `operations-real-nfs` | 5-node / 5-NFS full demo validation environment | Real-runtime foundation, release upgrade, topology changes, recovery switches, and CPU budget. |

Composite suites:

- `progressive-business` = `business-fast` + `business-mini-nfs`
- `progressive-environment` = `progressive-business` + `environment-full-nfs`
- `progressive-operations` = `progressive-environment` + `operations-local` + `operations-real-nfs`

## De-duplication and Order

Cross-environment repetition is intentional when it proves that the same core
business capability survives a stricter environment. Same-environment
same-semantics repetition should be avoided. The ladder order is:

1. `business-fast`: local business/API semantics.
2. `business-mini-nfs`: the same business API closed over small real NFS data.
3. `environment-full-nfs`: full demo environment readiness, grants, coverage,
   and degraded evidence without replaying the complete mini business matrix.
4. `operations-local`: local worker/runtime control semantics before real-NFS
   operational recovery.
5. `operations-real-nfs`: full real-NFS operations, ordered from foundational
   runtime data paths to upgrade, topology change, recovery, then CPU budget.

Within `operations-real-nfs`, run groups in this order:

1. `foundation-real-runtime`: runtime proxy, worker-owned manual rescan,
   force-find, and sink-selection basics.
2. `upgrade-core`: generation-two apply, HTTP continuity, source/sink/runtime
   scope, roots, stats, and tree materialization.
3. `topology-change`: new NFS join, root path modify, retire, and upgrade-window
   join.
4. `recovery-switch`: sink/facade failover and activation-scope preservation.
5. `resource-budget`: CPU budget after correctness is stable.

## Public Commands

Use `fs-meta/docs/examples/test-matrix-commands.sh <suite>` from the repository
root. The script intentionally exposes only the suites listed below:

- `business-fast`
- `business-mini-nfs`
- `environment-full-nfs`
- `operations-local`
- `operations-real-nfs`
- `progressive-business`
- `progressive-environment`
- `progressive-operations`

Each stage prints the suite name, environment type, whether full real NFS is
allowed, and whether operations are included.

## Entrypoints

| Suite | Entrypoints |
| --- | --- |
| `business-fast` | `app_specs`, `cli_specs`, `specs_fs_meta_contract_fast`, selected-group, force-find, status-stats, and roots-put runtime tests. |
| `business-mini-nfs` | `fs_meta_business_mini_real_nfs` |
| `environment-full-nfs` | `fs_meta_business_query_real_nfs`, `fs_meta_environment_full_real_nfs`, `fs_meta_environment_live_only_rescan_real_nfs` |
| `operations-local` | Explicit source/sink worker control tests plus `runtime_scope_e2e`. |
| `operations-real-nfs` | Ordered L5 groups plus `fs_meta_operations_*_real_nfs` and ignored runtime component diagnostics. |

## Responsibilities

- `tests/e2e/test_http_api_matrix.rs` owns business/API matrix behavior only.
- `tests/e2e/test_operational_scenarios.rs` owns operational topology and
  recovery scenarios only.
- `tests/e2e/test_release_upgrade.rs` owns release-upgrade scenarios, which are
  executed through the operations layer.

## Real-NFS Rules

All real-NFS tests stay ignored by default. They require
`CAPANIX_REAL_NFS_E2E=1`, a valid `CAPANIX_WORKER_HOST_BINARY`, and explicit
`--ignored` cargo execution from the matrix script.

Full real-NFS suites must be validated against the current 5-node demo
environment, but concrete demo host addresses stay outside the fustor main
branch. Set `FSMETA_FULL_NFS_ROOTS_FILE` to a local demo roots asset before
running full real-NFS suites. The matrix script verifies that the asset describes
at least five host entries, and the full API harness emits a
`demo_evidence_result` for `full-5node-5nfs-demo`.

Full real-NFS operations must use a runtime channel budget for the full
five-node route set plus one release-upgrade generation overlap. Mini/debug
channel caps are valid only for smaller debug environments and must not be used
as the `operations-real-nfs` gate.

Mini NFS and full real NFS must use separate exports. Mini NFS must not reuse or
modify full-NFS data. Full real NFS contains large data sets, so tests must use
bounded readiness, coverage mode, and degraded evidence instead of synchronous
full-tree audit as a blocking readiness condition.

## Root-Fix Discipline

A failing suite must produce a root fix, not a workaround:

1. Record the suite, environment, first raw error, affected domain object, and
   observed domain state.
2. Identify the first raw failing boundary before changing code: source, sink,
   root authority, group authority, facade, management write path, observation
   evidence, route, or runtime worker-host boundary.
3. Fix the owner of that boundary. Generic runtime, worker-host, route delivery,
   artifact loading, retry/cancel, and reusable host-fs capability issues belong
   in capanix rather than fustor app logic.
4. If specs are incomplete or wrong, update specs first, then source and tests.
5. Re-run from the lowest affected stage upward.

Forbidden workarounds include skipped assertions, weakened semantics, unbounded
timeouts, blind sleeps, fake readiness, cached success across changed epochs,
demo-only production branches, test-only API behavior, and hiding missing
metadata or management-write failures.
