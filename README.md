# fustor

Standalone `fustor` workspace extracted from `datanix`.

`fustor` is the top-level project name. `fs-meta` remains the product/domain
package under `fs-meta/`, while the repository root hosts the workspace root
package and repository-level tests.

## DataView Roadmap

The active cross-source research target is Fustor DataView v0.4: a
deployment-global, demand-materialized association view built from complete
results of accepted bounded actions and exposed only through policy-scoped
projections. Its functional stack is `Source -> SourceProjection (optional) ->
DataView`; private discovery is the access/control plane rather than another
data tier. New results require one-shot bounded actions, while separately
bounded streams may maintain only already-tracked results. Public revision ids
are opaque; retained exact revisions may be reprojected read-only under current
authorization, but never executed as historical branches. The current Union
Graph runtime, `data-view`, `data-view-normalizers`, and private-discovery code
implement the revisioned DataView contract and retain the legacy Union Graph API
only as an internal compatibility surface.

The implemented Source matrix covers PostgreSQL, MySQL, Elasticsearch, S3,
Local FS, CSV, JSONL, graph, and fs-meta. Each connector exposes source-owned bounded
discovery/read capabilities and observation receipts through app-neutral bound
routes; Fustor-owned normalizers admit verified results into DataView. The first
seven connectors are exercised by disposable native-source acceptance, while
fs-meta is exercised by the disposable `active-three` plus `business-mini-nfs`
container staging gate. Neither gate uses the official `198.51.100.144-146`
service.

- [TODO.md](./TODO.md) is the only active engineering ledger.
- [research/main.md](./research/main.md) defines the caller-agnostic research and
  ownership boundary.

## Layout

- `source-kit` and `*-source`: authoritative source contracts, adapters, and
  runtime apps
- optional source-local projections: rebuildable acceleration state with
  explicit apply evidence; never source authority
- `data-view`: caller-agnostic DataView v0.4 contracts, bounded action and
  streaming subscription registries, normalization, revision, and projection
  core
- `data-view-normalizers`: Fustor-owned schema/version adapters for heterogeneous
  source payloads; concrete source dependencies stay outside `data-view`
- `union-graph`: current cross-source identity, relationship, and evidence
  implementation; `DataView` is its target public semantic name
- `private-discovery`: source authority, freshness, non-committing discovery,
  bounded actions, budgets, scoped projection, and BFS frontier tooling
- `tools/fustor_boundary.py`: configuration-driven acceptance executable for
  generic read-only graph observation and declared external-result admission;
  it is a conformance surface, not a workflow engine or a replacement for the
  production source/DataView APIs
- `specs`: repository-level Fustor contracts
- `research`: research boundary and value model
- `fs-meta`: product container directory holding the `fs-meta`, `fs-meta-runtime`,
  `fs-meta-deploy`, and `fs-meta-tooling` packages plus fixtures and specs
- `fs-meta/fixtures`: module fixtures, manifests, and test apps
- `fs-meta/specs`: product and contract specs
- `fs-meta/tests`: domain-level specs and end-to-end tests

## Passive source inspection

`tools/source-inspect` provides `fustor-source-inspect`, a one-shot peer-source
reader using native services, grants, drivers and SourceKit. It is not another daemon,
workflow engine or deployment manager. Build with
`./scripts/resource-guard.sh cargo build -p fustor-source-inspect -p fustor-data-view-inspect`;
see [the CLI contract](specs/L3-RUNTIME/PASSIVE_OPERATIONS.md).

```text
fustor-source-inspect read --kind jsonl|csv|local-fs|s3|mysql|es|pg|graph --config native.json < source-envelope.json
fustor-source-inspect validate --kind <kind> --config - < proposed-config.json
fustor-source-inspect describe --kind <kind> --config native.json
fustor-source-inspect budget < budget-request.json
```

One-shot reads require versioned execution context, explicit bounds and native
grants. Native MessagePack receipts are verified before JSON presentation; PG /
MySQL snapshots, ES PIT and graph transactions close before output, so returned
continuation cannot be reused. Configuration
validation and passive latency/byte estimation do not read data or change files.
Callers retain bounded comparable receipt history; Fustor does not host that
history or perform retries. FS/S3 metadata and bounded JSONL/CSV content are
different operations. A prefix digest is not a complete-file digest; S3 ETag is
opaque version evidence. `graph-source` is a native configured-query owner with
mandatory object/query/epoch/parameter/budget/read-only gates, without inferred
upstream/cache alignment. Backend read-only credentials are optional defense in
depth; a configuration label never certifies those privileges.
The public native CLI passed real PPG 0.118a Bolt 5.3 queries and native refusals
in two sequential real OMIX/GSA-Human candidate workflows on 147/148. Scoped
DataView admission and six-family reads also passed. See the application-owned
[real acceptance and limitations](../bio_elinks_ppg-dashboard/coox/dashboard/REAL-ACCEPTANCE.md).
This is neither blanket connector/protocol certification nor a claim of PPG
backend account permissions or initialized-graph restart recovery.

`fustor-data-view-inspect scoped|admission-issue|declared-submit|validate --config native.json`
exposes existing native DataView persistence, generic declared policies and fixed
scoped reads. It retains manifests, not ETL products or graph bytes. The Python
`fustor_boundary.py` remains for historical conformance, never a silent fallback.

## Common Commands

```bash
./scripts/resource-guard.sh cargo check
./scripts/resource-guard.sh cargo test -p fs-meta --test app_specs -- --test-threads=1
./scripts/resource-guard.sh cargo test -p fs-meta-tooling -- --test-threads=1
./scripts/workspace-test-gate.sh
./scripts/test-source-connector-acceptance-resource-limits.sh
./scripts/source-connector-container-acceptance.sh
./fs-meta/scripts/fs-meta-container-staging-gate.sh run all
```

All repository test and acceptance entrypoints use
[`scripts/resource-guard.sh`](./scripts/resource-guard.sh). It limits the whole
child process tree to two logical CPUs by default, sets bounded Cargo/libtest/
Tokio/Rayon concurrency, constrains fs-meta scan workers to two and its
process-wide shared host-fs operation executor to two workers, and lowers CPU
and I/O scheduling priority. Override
the hard affinity only through `FUSTOR_CPU_LIMIT`; do not run the 2,000+ test
`fs-meta-runtime` target with unrestricted `cargo test --workspace` or
`--nocapture`.

The repository also keeps conservative direct-Cargo defaults in
[`/.cargo/config.toml`](./.cargo/config.toml): two build jobs, one libtest
thread, and two Tokio/Rayon workers when a test does not specify its own
runtime size. This file is versioned so a fresh checkout receives the same
defaults. The resource guard remains mandatory for full-workspace and container
acceptance runs because it caps the complete descendant process tree.
Disposable source acceptance additionally validates an aggregate 3.5-CPU
container ceiling, verifies each running container's Docker CPU quota, wraps
every Cargo invocation with the process-tree watchdog, and removes its
containers and volumes on every exit path.

The full-workspace gate additionally runs under
[`scripts/process-tree-watchdog.sh`](./scripts/process-tree-watchdog.sh). The
watchdog fails with status `125`, prints a bounded process/thread/RSS snapshot,
and terminates only that test process tree when it exceeds 512 threads by
default. Use `FUSTOR_WORKSPACE_TEST_MAX_THREADS` to lower the threshold for a
constrained machine; raising it requires an explicit resource-budget decision.
The gate checks the 16 MiB QUIC UDP socket-buffer ceiling before the long test
run, temporarily raises low `rmem_max`/`wmem_max` ceilings, and restores their
exact previous values on every exit path.
`scripts/data-action-map-local-gate.sh` exercises both the CPU guard and the
process-tree watchdog self-tests before running repository tests. It also runs
the fs-meta container staging resource self-test, which proves CPU affinity and
hard wall-clock timeout behavior without starting Docker. That self-test also
proves watchdog cleanup leaves the temporary NFS server alive until workload
termination and that the normal-exit finalizer subsequently stops it.

## Dependency Overrides

Workspace-internal crates inherit `capanix-*` dependencies from the root
[`Cargo.toml`](./Cargo.toml) via `workspace = true`.

This repository does not default unresolved `capanix-*` dependencies to any
local checkout. When unpublished upstream crates need to come from a local
checkout such as `../capanix`, override them only at the workspace root with
`[patch.crates-io]`. Do not add `../capanix` path dependencies inside member
crates.

```toml
[patch.crates-io]
capanix-kernel-api = { path = "../capanix/crates/kernel-api" }
capanix-runtime-api = { path = "../capanix/crates/runtime-api" }
```
