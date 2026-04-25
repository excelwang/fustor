# fs-meta Test Matrix

This document groups the existing fs-meta tests by two decision axes:

- Feature priority: what product risk the suite protects.
- Test environment: how realistic and expensive the suite is.

The purpose is to keep failures diagnosable. A suite should answer one clear
question instead of mixing fast contracts, local state machines, real-NFS smoke,
operational churn, and upgrade continuity into one bucket.

## Axes

### Feature Priority

| Priority | Meaning | Examples |
| --- | --- | --- |
| P0 startup and control plane | The product can start, accept a release, expose the facade, authenticate, report status, and safely accept management writes. | release apply, login, status, roots/grants/control stream readiness |
| P0 query correctness | The core user-facing query paths return correct results. | tree, stats, force-find, selected group, trusted-materialized, live-only freshness |
| P1 operational changes | Online topology and visibility changes behave correctly. | new NFS join, root path change, visibility change, sink/facade failover, NFS retire |
| P1 upgrade continuity | Release generation changes preserve the user-visible service and control roles. | upgrade, roots persistence, tree stats stability, source/sink control roles |
| P2 contracts and internal state machines | Specs, module boundaries, and local state machines do not drift. | spec contracts, API boundary, CLI scope, roots_put, source/sink workers, status fan-in |
| P3 soak, capacity, and performance | Large or long-running workloads stay healthy. | large audit, long watch, CPU budget, full-NFS capacity paths |

### Test Environment

| Environment | Meaning | Cost |
| --- | --- | --- |
| E0 static contract | Reads specs/source/contracts without starting a cluster. | lowest |
| E1 local unit/integration | Runs local tests with mock boundaries, tempdirs, or in-memory route state. | low |
| E2 runtime scope | Uses runtime-scope e2e semantics without requiring full real-NFS. | medium |
| E3 mini real-NFS | Uses a real 5-node topology with separate mini NFS exports and 10 files per export. | medium-high |
| E4 full real-NFS | Uses the full 5-node real-NFS environment and production-like data shape. | highest |

## Suites

| Suite | Priority | Environment | Gate role | Existing entrypoints | Failure meaning |
| --- | --- | --- | --- | --- | --- |
| contract-fast | P2 | E0 | default gate | `app_specs`, `cli_specs`, `specs_fs_meta_contract_fast` | Specs, API boundary, module boundary, or CLI scope drifted. |
| contract-data-boundary-slow | P2 | E1/E2 | diagnostic or nightly | data-boundary realtime/slow contract tests | Data-boundary realtime behavior or metadata continuity regressed. |
| core-query-fast | P0/P2 | E1 | default gate | `fs-meta-runtime` query, selected-group, force-find, status-stats tests | Query routing, selected-group, force-find, or status fan-in regressed. |
| core-management-fast | P0/P2 | E1 | default gate | `fs-meta-runtime` roots_put and management readiness tests | Roots apply, management write readiness, or control gate behavior regressed. |
| core-worker-fast | P0/P2 | E1 | default gate | Explicit `fs-meta-runtime` worker entrypoints for source control, source force-find worker, sink control, and sink status fan-in | Source/sink worker generation, control-frame, force-find, status evidence, or local materialization behavior regressed. |
| runtime-scope-gate | P0 | E2 | integration gate | `runtime_scope_e2e` | Runtime scope, online roots apply, or distributed force-find semantics regressed. |
| mini-real-nfs-smoke | P0 | E3 | optional validation | `fs_meta_http_api_matrix_mini_real_nfs` when present in validation assets | Real NFS topology, mount, release apply, login, status, rescan, readiness, or evidence failed in the fast real-NFS environment. |
| real-nfs-api-core | P0 | E4 | pre-merge gate | `fs_meta_http_api_matrix_real_nfs`, `fs_meta_http_api_matrix_query_baseline_real_nfs`, `fs_meta_http_api_matrix_live_only_rescan_real_nfs` | Full real-NFS API behavior, query correctness, management roots, or live freshness regressed. |
| real-nfs-ops | P1 | E4 | nightly or release gate | `fs_meta_operational_*_real_nfs`, including activation-scope entrypoints | Online operational changes or activation-scope capture behavior regressed. |
| real-nfs-component | P0/P2 | E4 | diagnostic or nightly | `fs-meta-runtime` ignored real-NFS source/runtime_app component tests | Component-level real-NFS source, runtime_app, or selected-group proxy behavior regressed. |
| real-nfs-upgrade | P1/P3 | E4 | nightly or release gate | `fs_meta_release_upgrade_*_real_nfs` | Upgrade continuity, control-role recovery, or budget behavior regressed. |
| real-nfs-capacity-soak | P3 | E4 | future placeholder | not implemented | Future large audit, long watch, and capacity behavior should be validated here. |


## Progressive Test Ladder

Run the matrix from cheap domain proof to expensive real-NFS proof. Do not start
with the full cluster when a lower rung can expose the same product failure.

| Rung | Purpose | Suites | Stop rule |
| --- | --- | --- | --- |
| L0 contract proof | Prove specs, API names, CLI scope, and module boundaries still agree. | contract-fast | Any failure blocks higher rungs until the contract or implementation is corrected. |
| L1 local domain proof | Prove query, management-write, and worker state machines without real NFS noise. | core-query-fast, core-management-fast, core-worker-fast | Any failure is a product semantic failure, not a demo-environment issue. |
| L2 distributed runtime proof | Prove runtime-scope behavior across the worker-host boundary. | runtime-scope-gate | Any failure must be assigned to the first raw failing boundary before real-NFS tests continue. |
| L3 mini real-NFS proof | Prove the same 5-node/5-NFS topology with separate mini exports and 10 files per export. | mini-real-nfs-smoke | Any failure means the full cluster is not yet a useful signal. Fix the root cause or the mini environment. |
| L4 full real-NFS API proof | Prove the full data shape for P0 API and management behavior. | real-nfs-api-core | Any fast success must be checked for accidental skip, missing `--ignored`, wrong filter, or non-real NFS. |
| L5 operational and release proof | Prove online changes, component diagnostics, and release continuity. | real-nfs-ops, real-nfs-component, real-nfs-upgrade | Failures stay in their owning suite; do not hide them in a broader all-in-one run. |

`progressive-fast` runs L0-L2. `progressive-real` runs L3-L4.
`progressive-full` runs L0-L5 and should be used only when the mini and full
real-NFS environments are intentionally available.

## Suite Boundaries

`contract-fast` is a default developer and CI gate. It must not require real-NFS,
sudo, external cluster state, demo artifacts, or realtime data-boundary slow
paths. It uses the dedicated `specs_fs_meta_contract_fast` target so it does not
depend on test-name filters to avoid slow data-boundary contracts. Slow
data-boundary contracts belong in `contract-data-boundary-slow`.

`core-query-fast`, `core-management-fast`, and `core-worker-fast` are default
developer and CI gates. `core-unit-fast` may be used as a convenience alias for
running all three. Keeping the sub-suites separate makes failures easier to
route: query semantics, management readiness, and worker state machines have
different owners and diagnosis paths.

`core-worker-fast` must use explicit worker-domain entrypoints rather than broad
name filters such as `source` or `sink`. Broad filters pull in runtime_app,
query, and API tests whose names mention source/sink, making the worker rung
slow, noisy, and hard to diagnose. Runtime_app/API coverage belongs in
`core-query-fast`, `core-management-fast`, or `runtime-scope-gate` depending on
the owning behavior.

`runtime-scope-gate` is a P0 distributed-semantics gate. It requires
`CAPANIX_RUNTIME_SCOPE_E2E=1` and should stay separate from real-NFS suites so a
runtime-scope regression is not confused with NFS environment failure.

`mini-real-nfs-smoke` is an optional validation-suite smoke test. It proves that
the real 5-node/5-NFS topology can boot and converge cheaply. It does not claim
full API coverage, full audit coverage, or production-scale data coverage. On a
branch that does not define `fs_meta_http_api_matrix_mini_real_nfs`, command
templates must report that the suite is validation-assets only instead of
pretending the main branch covered it.

`real-nfs-api-core` is the full-environment API gate. It covers the full HTTP API
matrix, full-capacity trusted-materialized fail-closed behavior, and
live-only/rescan freshness. It is not a soak suite and must not require a
synchronous full-tree audit before API readiness can be assessed. Trusted
materialized query correctness belongs in `mini-real-nfs-smoke`, where each NFS
has bounded data.

`real-nfs-ops` owns online operational changes and activation-scope capture.
Keep these tests out of the core API suite because their failures usually
require a different diagnosis path. Activation-scope checks are exposed through
top-level `fs_meta_operational_*_real_nfs` entrypoints so the suite does not
depend on private module test names.

`real-nfs-component` owns component-level real-NFS diagnostics. It is not a user
API gate; it is the right follow-up when API or ops failures need to be localized
to runtime_app, source worker, selected-group proxy, or manual rescan behavior.

`real-nfs-upgrade` owns release-generation continuity. Keep these tests out of
the operational suite because upgrade failures involve generation, persistence,
and control-role recovery rather than ordinary topology churn.

## Recommended Cadence

| Cadence | Suites |
| --- | --- |
| Every PR / local fast gate | contract-fast, core-query-fast, core-management-fast, core-worker-fast |
| P0 integration gate | runtime-scope-gate, mini-real-nfs-smoke when the validation environment is available |
| Pre-merge or protected branch gate | real-nfs-api-core |
| Nightly or release gate | contract-data-boundary-slow, real-nfs-ops, real-nfs-component, real-nfs-upgrade |
| Capacity campaign | real-nfs-capacity-soak when implemented |


## Root-Fix Discipline

A failing suite must produce a root fix, not a workaround.
`ROOT_FIX_DISCIPLINE.md` is the canonical operator checklist; this section keeps
the matrix-level summary close to the suite definitions. The fixing loop is:

1. Record the failing ladder rung, suite, command, first raw error, affected
   domain object, and observed domain state.
2. Identify the first raw failing boundary before changing code: source, sink,
   root authority, group authority, facade, management write path, observation
   evidence, route, or runtime worker-host boundary.
3. Fix the owner of that boundary. If the boundary is a generic capanix runtime
   capability, move the fix down to capanix instead of adding fustor app logic.
4. If the specs are wrong or incomplete, update the specs first and record why
   the product rule changed. Then update source and tests to match the revised
   rule.
5. Re-run from the lowest affected rung upward. Do not jump straight to the full
   cluster unless all lower affected rungs already pass.

Forbidden workarounds include skipped assertions, wider timeouts without a
latency rule, blind sleeps, fake readiness, cached success across a changed
epoch, demo-only production branches, test-only API behavior, treating missing
metadata as complete metadata, and ignoring a failing management write path
because the read facade is reachable.

Root ownership should stay domain-oriented. Fustor owns fs-meta roots, grants,
groups, source/sink observation evidence, query semantics, management-write
readiness, and publication meaning. Capanix owns generic runtime transport,
worker-host process behavior, route delivery, artifact loading, retry/cancel
policy, and reusable host-fs capability.

Every root fix must leave a validation trail: the failing command, the fixed
owner, the specs rule used or amended, and the lowest-to-highest suites re-run
after the change.

## Real-NFS Rules

All real-NFS suites must stay ignored by default and require explicit preflight.
They must require `CAPANIX_REAL_NFS_E2E=1` and a valid worker host binary. They
must not run from ordinary `cargo test` without `--ignored`.

Mini and full real-NFS environments must use separate NFS exports. Mini smoke
must not modify existing full-NFS data. Full real-NFS API, ops, and upgrade
suites must not depend on demo-only binaries or demo-only paths.

## Current Known Gap

`real-nfs-capacity-soak` is only a placeholder today. The full real-NFS API suite
validates production-like topology and API behavior, but it must not be treated
as proof that large audit, long watch, or high-file-count capacity behavior is
covered. A future capacity suite should at minimum report audit completion,
watch stability, metadata coverage, and CPU/memory budget evidence.

For production-scale NFS data sets, including the current five `10.0.82.*` demo
servers with roughly two billion files in total, the test matrix must not depend
on a synchronous full-tree walk as a readiness condition. Control-plane and
management APIs must converge independently of data volume. Data-plane checks
must be bounded by explicit roots, paths, hotsets, or samples, and any unfinished
large audit must be exposed as domain coverage or degraded evidence rather than
hidden behind a wider timeout.
