---
version: 2.8.0
---

# L2: fs-meta Domain Architecture

> Focus: fs-meta domain externally behaves as one app product container; internal
> execution is organized as `facade-worker`, `source-worker`, `scan-worker`,
> and `sink-worker`, with product-facing worker mode vocabulary limited to
> `embedded | external`.

## ARCHITECTURE.COMPONENTS

1. **Shared Types Component**:
   1. Inputs: domain type evolution requirements.
   2. Outputs: shared wire contracts for Unix/FS atomic facts and shared vocabulary (`FileMetaRecord`, `UnixStat`, `UnixDirEntry`, `StabilityMode`, `UnreliableReason`, and related passthrough/control carriers).
   3. Role: fs-meta shared MessagePack carrier single source of truth for internal event/control and Unix/FS field constraints; fs-meta app-local query request DTOs and fs-meta query/result payload schemas remain app/domain-owned.

2. **Single fs-meta App Boundary**:
   1. Inputs: filesystem change observations, domain queries, runtime control frames, runtime host object grants.
   2. Outputs: `query/find` responses, domain events, and app-owned port boundaries.
   3. Role: encapsulate source/sink semantics, four worker roles (`facade-worker`, `source-worker`, `scan-worker`, `sink-worker`), thin runtime ABI participation, app-owned opaque ports, and resource-scoped HTTP facade semantics behind one app package boundary while allowing runtime to schedule multiple worker instances.
   3.1. Constraint: this boundary is a downstream app product container, not a generic reusable shared library package.
   4. Canonical boundary path for ordinary app-facing runtime typing is `capanix-app-sdk` (authoring facade) -> `capanix-runtime-api` (runtime-owned typed surface) -> `capanix-kernel-api` (kernel-owned mirror only).
   5. Product-facing execution vocabulary stays at `embedded | external`; realization mechanics remain internal.

3. **Meta-Index Service (App Internal)**:
   1. Inputs: realtime watch stream, initial full scan batches, audit pass results, sentinel feedback.
   2. Outputs: materialized metadata index + repair actions.
   3. Role: own fs-meta index lifecycle; not kernel-owned state.
   4. Baseline: materialized index is in-memory observation/projection state (process-bound), not durable checkpoint state and not the domain truth ledger itself.
   5. Sink arbitration/build baseline: single-tree per logical group, no member-level output boundary.
   6. State split baseline: source/sink keep authoritative journal inputs separate from projection/materialized state (in-memory tree/runtime maps).
   7. Authority state baseline: fs-meta authority journal remains the domain authoritative truth ledger; projection/materialized state remains rebuildable observation state.
   8. Runtime-owned state carriers may host authoritative-journal cells and scratch/projection-adjacent cells, but fs-meta remains the owner of truth meaning and observation eligibility.
   9. Execution hosting choices for sink remain internal execution carriers inside one declared fs-meta app package boundary and do not create additional app/package or platform authority.
   10. Sink live-tree baseline is a query-ready exact-path arena with placeholder ancestors, stable child ordering, and per-directory aggregate caches; it is optimized for `/tree`/`/stats`/PIT creation rather than for generic immutable snapshot storage.

4. **Group Planner (App Internal)**:
   1. Inputs: runtime host object grants + descriptor selectors + admin config + query/find request context.
   2. Outputs: app-defined groups, source-primary executor choice, per-group worker-role execution shape, and force-find dispatch decisions.
   3. Role: domain grouping and per-group execution strategy owner; runtime remains grouping-policy neutral.

5. **Host Adapter SDK Bridge**:
   1. Inputs: app-domain host-object calls.
   2. Outputs: public host-fs facade over host-local adaptation on the bound host plus kernel generic route/channel calls for app-to-app coordination.
   3. Role: translation boundary owner; fs-meta business layer does not carry host-operation ABI details, distributed host-operation semantics, or local/remote backend pairing inline.

6. **Product HTTP API Boundary**:
   1. Inputs: product-management HTTP requests + domain-local auth records + source/sink runtime state.
   2. Outputs: fs-meta product API responses for session, status, runtime grants, monitoring roots, rescan, and projection query boundaries.
   3. Role: expose one bounded product API on a resource-scoped one-cardinality fs-meta app package boundary while keeping capanix privileged mutation-path details out of the fs-meta user workflow.
   4. Invariant: fs-meta product UI and `fsmeta` CLI talk only to this bounded API or to fs-meta deployment helpers, not directly to capanix internals.

7. **Observation Eligibility Path (App Internal)**:
   1. Inputs: authoritative journal revision, replayed monitoring roots/runtime grants, current projection/materialized-tree status, stale-writer fencing evidence.
   2. Outputs: `observation_eligible` evidence for HTTP facade exposure, degraded observation state, and release/failover cutover readiness.
   3. Role: decide when externally visible observation can be trusted as caught up to current authoritative truth.
   4. Until eligibility is reached, trusted external exposure stays on the previous eligible generation or returns explicit degraded observation state; readable partial projection output does not become authoritative truth.

## ARCHITECTURE.INTERACTIONS

1. **Materialized Query Path**:
   1. client issues `query`.
   2. fs-meta app reads `meta-index`.
   3. app returns fs-meta app-owned query/stats payloads built from shared Unix/FS field constraints and shared reliability/stability vocabulary; projection may further shape them into the public `/tree` or `/stats` HTTP response.

2. **Fresh Find Path**:
   1. client issues `find`.
   2. projection/app derive candidate groups for the current scope and order them by `group_order=group-key|file-count|file-age`.
   3. app executes live probe; when request is non-targeted (`selected_group` absent), app performs deterministic intra-group member arbitration and keeps one member snapshot per logical group.
   4. app returns freshness observation results that projection shapes into the public multi-group paged `/on-demand-force-find` response.

3. **Projection HTTP Query Facade Path**:
   1. client issues HTTP query on `/api/fs-meta/v1/{tree|stats|on-demand-force-find}`.
   2. projection layer normalizes request defaults (`path=/`, `recursive=true`, `group_order=group-key`, `group_page_size=64`, `entry_page_size=1000`, `stability_mode=none`, `metadata_mode=full`) and dispatches to query/find backend.
   3. the first materialized `/tree` or live `/on-demand-force-find` request freezes one grouped query result into a short-lived PIT session; later pages read that PIT instead of recomputing group ranking or rerunning live scans.
   4. `group_order` remains a pure ranking axis; it orders candidate groups but does not define path stability or metadata withholding behavior.
   5. when caller requests `stability_mode=quiet-window` on the materialized tree path, projection evaluates each returned group/path stability after bucket selection and before metadata shaping.
   6. quiet-window evaluation uses materialized subtree observed-change evidence plus coverage recovery state; periodic sync-refresh updates that do not change the effective subtree result do not reset the quiet window.
   7. `/tree` returns a grouped bucket envelope with top-level `pit`, `groups[]`, and `group_page`; `/on-demand-force-find` returns the same grouped-envelope family with `stability=not-evaluated` per group.
   8. `/tree` and `/on-demand-force-find` both use PIT-owned cursor families: `group_after` is a top-level bucket offset cursor inside one PIT, while `entry_after` is an opaque per-group entry cursor bundle inside that same PIT.
   9. projection publishes transport diagnostics via `/api/fs-meta/v1/bound-route-metrics`.

4. **Group Bind/Run Path**:
   1. app group planner derives opaque groups from host/object descriptors and admin config.
   2. runtime binds/runs worker roles on those groups without interpreting grouping semantics.
   3. internal desired-state wiring for those groups remains app-private execution shape rather than product vocabulary.
   4. kernel/runtime participate only as generic routing and bind/run mechanisms.

5. **Management API Path**:
   1. client issues fs-meta API request on bounded HTTP namespace.
   2. API service authenticates/authorizes request with domain-local passwd/shadow + group policy.
   3. API service reads or mutates source/sink state and returns structured domain response.
   4. runtime grant discovery is exposed through `/api/fs-meta/v1/runtime/grants`; product workflows generate roots from runtime-visible grants rather than from legacy exports/fanout diagnostics.
   5. query/find payload shaping is modeled on query-path parameters and path-specific response contracts (for example `/tree` and `/on-demand-force-find` use `pit_id/group_page_size/group_after/entry_page_size/entry_after`), not management request-body size fields.

6. **Product Config And Release Path**:
   1. deployment helper consumes thin deploy config (`api/auth` bootstrap only) and generates internal desired-state/execution details.
   2. operators discover runtime grants, preview monitoring roots, and apply business monitoring scope online through bounded product APIs.
   3. upgrading fs-meta submits a higher target generation at the same single app package boundary; current monitoring roots and runtime grants are replayed into the candidate generation.
   4. newly activated generation rebuilds in-memory observation state through baseline scan/audit/rescan rather than expecting durable tree carry-over.
   5. observation-eligibility path evaluates whether rebuilt observation has caught up far enough to be trusted as the current external result source.
   6. trusted external exposure remains on the previous eligible generation or explicit degraded observation state until the candidate reaches app-owned `observation_eligible`.

## ARCHITECTURE.OWNERSHIP

1. **Domain Ownership**:
   1. user-facing `query/find` contracts: fs-meta app.
   2. index lifecycle (`realtime/full-scan/audit/sentinel`): fs-meta app internal services.
   3. group formation and source-primary selection policy: fs-meta app internal planner.

2. **Boundary Rule**:
   1. deployment config MUST realize one fs-meta app package boundary.
   2. app desired-state document is an internal fs-meta deployment concern; user-facing deployment is mediated by the independent `fsmeta` product CLI.
   3. kernel MUST remain domain-neutral and strategy-neutral.
   4. host-local facade/ABI adaptation MUST stay in host-adapter SDK layer.
   5. operator-visible business configuration is split between thin deploy bootstrap and online runtime-grant-derived monitoring scope APIs.
   6. API facade handover or sink instance handover rebuilds in-memory index state for the affected groups via baseline scan/audit/rescan.
   7. generation replacement cuts over one single app package boundary generation at a time rather than duplicating user-visible app desired-state documents per node.
8. multi-peer sink execution is realized by runtime group-aware worker-role bind/run realization under one fs-meta app package boundary, not by whole-app sharding semantics.
9. external sink-worker hosting MUST remain internal to one declared fs-meta app package boundary and MUST NOT create externally visible multi-app semantics.
   10. materialized quiet-window stability MUST track “when did the observed subtree result last change” separately from file mtime ranking data such as `latest_file_mtime_us`.
   11. periodic scan/audit refresh that preserves the effective materialized subtree result MAY refresh coverage evidence but MUST NOT reset quiet-window timing; write-significant subtree changes and coverage recovery do reset it.

## ARCHITECTURE.APP_PACKAGE

1. One `fs-meta` app product container hosts four worker roles: `facade-worker`, `source-worker`, `scan-worker`, and `sink-worker`.
2. `facade-worker` owns HTTP/API ingress, auth, PIT lifecycle, response shaping, and current query orchestration; `query` remains inside `facade-worker` for now.
3. `source-worker` owns live watch, live force-find, and source-side host/grant interaction.
4. `scan-worker` owns initial full scan, periodic audit, and overflow/recovery rescans.
5. `sink-worker` owns materialized tree/index maintenance plus `/tree` and `/stats` materialized-query backend duties.
6. Current baseline defaults: `facade-worker=embedded`; `source-worker=external`; `scan-worker=external`; `sink-worker=external`.
7. This split is intentionally `4`, not `3`, so heavy scan/audit work stays separate from live source/watch work.
8. This split is intentionally `4`, not `5`, because query orchestration remains packaged with the facade until a separate query worker is justified.
9. Product-facing worker modes are only `embedded | external`; operator-visible worker config uses `workers.facade.mode`, `workers.source.mode`, `workers.scan.mode`, and `workers.sink.mode`.
10. Product-facing L0-L2 specs stay in worker/mode vocabulary; realization mechanics stay internal.
11. `fs-meta/` is the product container directory only; it is not a Cargo package and exists to host the product app package, worker artifact packages, specs, docs, fixtures, and testdata.
12. `fs-meta/app` is the main product app package for this container; it stays `publish = false`, owns package-local API/query/orchestration/business composition plus config/types, and does not define a generic reusable fs-meta library surface.
13. Ordinary app-facing business/runtime modules default to the `capanix-app-sdk` curated re-export / raw-helper path; production business/runtime paths in the app package do not keep direct `capanix-runtime-api`, `capanix-kernel-api`, or `capanix-unit-sidecar` imports, while explicit test/dev fixtures may still depend on `capanix-runtime-api` for protocol assertions.
14. The app package consumes `app-sdk`, product-owned `runtime-support`, `host-adapter-fs-meta`, `host-fs-types`, and `route-proto` as upstream authorities/support carriers; `product` remains the bounded product-facing CLI/tooling namespace, while `query`, `product::release_doc`, and `workers` may remain public package-local operational/test support surfaces without becoming product or platform authority.
15. External-worker realization helpers remain confined to explicit worker runtime seams and low-level support crates; the app package may consume only the bounded typed worker transport/client surface from `runtime-support`, not raw bridge/bootstrap primitives.
16. The upstream bridge-realization seam is low-level carrier glue only; `fs-meta/runtime-support/` owns worker child-process bootstrap, log/socket ownership, retry clipping, and lifecycle supervision, while worker artifact crates own only their explicit server entry/bootstrap functions.
17. The canonical worker transport contract preserves `Timeout` / `TransportClosed` categories and applies wall-clock total-timeout clipping before retry; transport failures are not flattened into a generic peer-error bucket.
18. `embedded` workers share the host process; `external` workers run in dedicated worker processes.
19. Constructor/bootstrap faults surface as typed `CnxError` / `init_error`; runtime task join failures and worker/bootstrap faults are handled as app/task-local errors or explicit degraded behavior rather than routine production `panic!` / `expect!` flow.
20. Package-local implementation env seams remain bounded tuning knobs; `FS_META_SOURCE_SCAN_WORKERS`, `FS_META_SOURCE_AUDIT_INTERVAL_MS`, `FS_META_SOURCE_THROTTLE_INTERVAL_MS`, `FS_META_SINK_TOMBSTONE_TTL_MS`, `FS_META_SINK_TOMBSTONE_TOLERANCE_US`, and `FS_META_SOURCE_AUDIT_DEEP_INTERVAL_ROUNDS` tune implementation behavior without redefining truth, cutover, or query-surface contracts.

## ARCHITECTURE.CLI_AND_TOOLING_BOUNDARY

1. CLI command layer parses deploy/undeploy/local/grants/roots command workflows and targets the bounded fs-meta product boundary; login/rescan remain bounded HTTP/API workflows consumed inside operator sessions, tests, and support clients rather than top-level `fsmeta` subcommands.
2. Request composition layer uses fs-meta bounded `product` types, bounded HTTP facade endpoints, external `cnxctl` deploy/control commands, and shared kernel-owned auth vocabulary without importing app package-local runtime/orchestration internals.
3. Upgrade workflow submits a new release generation through the same deploy boundary instead of exposing manual internal release-doc editing.
4. Local-dev helpers, including the workspace package `capanix-app-fs-meta-tooling` and the product-scoped `capanixd-fs-meta` launcher, remain tooling only, are feature-gated away from the default CLI install, and do not create new platform authority or alter daemon ownership.
5. The optional `capanixd-fs-meta` launcher composes `capanix_daemon::run_with_host_passthrough_bootstrap(...)` with `capanix_host_adapter_fs_meta::spawn_host_passthrough_endpoint`; it does not bind sockets, construct runtime control services, or start runtime directly.
6. CLI remains a request client only: it does not own bind/run realization, route convergence, route target selection, or `state/effect observation plane` meaning such as `observation_eligible`.

## ARCHITECTURE.REPOSITORY_TOPOLOGY

1. `fs-meta/specs/` is the only formal fs-meta specification tree; `specs/app/` and `specs/cli/` are not parallel authority roots.
2. Formal specs are limited to `L0-GLOSSARY`, `L0-VISION`, `L1-CONTRACTS`, `L2-ARCHITECTURE`, and `L3-RUNTIME/*`.
3. Product/operator documentation lives under `fs-meta/docs/`; deployment examples live under `fs-meta/docs/examples/`.
4. Contract-test fixtures, release examples, and regression materials live under `fs-meta/testdata/specs/`; they are not part of the formal specification tree.
5. Runnable fixture apps, fixture manifests, and runtime artifacts live under `fs-meta/fixtures/`; they are not part of the formal specification tree but are first-class repo topology for tests and examples.
6. The specs validation helper lives under `fs-meta/scripts/validate_specs.sh` and validates the single formal spec tree against fs-meta contract tests.

## ARCHITECTURE.CRATE_OWNERSHIP

1. `fs-meta/` is the product container only; it is not a Cargo package and owns no code-level business authority.
2. `fs-meta/app/` is the only product app package; it owns package-local config/types plus API/query/orchestration/business composition and MUST stay `publish = false`.
3. `fs-meta/runtime-support/` is the only crate that owns worker child-process bootstrap, socket/log path materialization, retry clipping, and low-level external-worker transport supervision for fs-meta.
4. `fs-meta/worker-facade/` owns the embedded `facade-worker` artifact entry and fixture binary surface; it does not own business/query semantics.
5. `fs-meta/worker-source/` owns the `source-worker` external artifact/runtime entry and `run_source_worker_server(...)` bootstrap.
6. `fs-meta/worker-sink/` owns the `sink-worker` external artifact/runtime entry and `run_sink_worker_server(...)` bootstrap.
7. `fs-meta/worker-scan/` owns the `scan-worker` executable artifact identity and `run_scan_worker_server(...)` entry; current implementation may reuse lower-level source-worker runtime helpers internally without reusing source-worker artifact identity.
8. `fs-meta/tooling/` owns operator CLI binaries and optional local-dev daemon composition only; it does not own worker bootstrap/runtime planning semantics.

## ARCHITECTURE.DEPENDENCY_RULES

1. `fs-meta/app/` MUST NOT depend on `capanix-kernel-api`, worker artifact crates, the low-level external-worker bridge crate, or the embedded-entry macro crate; it MAY depend on the bounded typed transport/client surface in `fs-meta/runtime-support/`.
2. `fs-meta/tooling/` MAY depend on bounded `product` types and optional daemon/bootstrap seams, but MUST NOT depend on worker runtime internals or reimplement worker bootstrap.
3. `fs-meta/runtime-support/` MAY depend on the low-level external-worker bridge crate only as the bridge runner; it owns fs-meta worker bootstrap semantics and MUST preserve canonical `Timeout` / `TransportClosed` categories plus wall-clock timeout clipping.
4. `fs-meta/worker-facade/` MAY depend on `fs-meta/app` and the embedded-entry macro crate for embedded artifact realization, but realization wiring MUST stay artifact-local.
5. `fs-meta/worker-source/` and `fs-meta/worker-sink/` MAY depend on `fs-meta/app` and the low-level external-worker bridge crate to host external worker servers; app business modules MUST NOT depend back on those artifact crates.
6. `fs-meta/worker-scan/` MAY depend on `worker-source` lower-level runtime helpers while the scan server entry intentionally forwards into shared source-runtime implementation; if scan runtime semantics diverge, `L2` MUST be updated before changing that dependency.

## ARCHITECTURE.WORKER_ROLE_TO_ARTIFACT_MAP

1. `facade-worker` maps to the `fs-meta/worker-facade/` artifact crate and remains the only embedded worker in the current baseline.
2. `source-worker` maps to `fs-meta/worker-source/` and owns the dedicated external worker binary `fs_meta_source_worker`.
3. `scan-worker` is a distinct operator-visible worker role; in the current baseline it maps to the separate artifact crate `fs-meta/worker-scan/` and dedicated `run_scan_worker_server(...)` entry while still sharing lower-level source-runtime helpers internally.
4. `sink-worker` maps to `fs-meta/worker-sink/` and owns the dedicated external worker binary `fs_meta_sink_worker`.
5. Any future change from shared-helper `scan-worker` hosting to a fully distinct scan runtime surface MUST update this section first, then the dependent manifests and tests.
