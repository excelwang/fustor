---
version: 3.0.0
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
   4. Canonical boundary path for stateful fs-meta authoring is `capanix-managed-state-sdk` (shared observation declarations/evaluator) -> `capanix-service-sdk` (service-first runtime host/lowering) -> `capanix-app-sdk` (Boundary Toolkit) -> `capanix-runtime-api` (runtime-owned typed surface) -> `capanix-kernel-api` (kernel-owned mirror only).
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
   2. facade/query layer resolves target logical groups and fans out only to the sink-side execution partition that owns each group; worker process identity is hosting-only and is not the fan-out contract.
   3. sink-side group execution reads its materialized subtree from `meta-index`.
   4. facade/query layer merges the per-group sink results and returns fs-meta app-owned query/stats payloads built from shared Unix/FS field constraints and shared reliability/stability vocabulary; projection may further shape them into the public `/tree` or `/stats` HTTP response.

2. **Fresh Find Path**:
   1. client issues `find`.
   2. projection/app derive candidate groups for the current scope and order them by `group_order=group-key|file-count|file-age`.
   3. facade/query layer fans out one live `force-find` execution per target logical group; when caller does not pin a group, the fan-out target set is the groups resolved for the current path scope.
   4. inside each target logical group, app performs deterministic intra-group runner arbitration and executes the live probe on exactly one bound source-side execution partition at a time, falling back to the next runner only after failure.
   5. source executes only the local group-scoped work it was selected for and MUST NOT forward the request to remote source execution partitions.
   6. facade/query layer merges the per-group freshness observations and returns the public multi-group paged `/on-demand-force-find` response.

3. **Projection HTTP Query Facade Path**:
   1. client issues HTTP query on `/api/fs-meta/v1/{tree|stats|on-demand-force-find}`.
   2. projection layer normalizes request defaults (`path=/`, `recursive=true`, `group_order=group-key`, `group_page_size=64`, `entry_page_size=1000`, `read_class=trusted-materialized`) and dispatches to materialized or freshness backend.
   3. the first materialized `/tree` or live `/on-demand-force-find` request freezes one grouped query result into a short-lived PIT session; later pages read that PIT instead of recomputing group ranking or rerunning live scans.
   4. `group_order` remains a pure ranking axis; it orders candidate groups but does not define read trust or observation gating behavior.
   5. `read_class=fresh` delegates to the freshness path, `read_class=materialized` returns current materialized observation with explicit observation status, and `read_class=trusted-materialized` is gated by package-local trusted observation evidence.
   6. the same package-local observation evaluator feeds query `observation_status` and cutover `observation_eligible`; transport failure remains an availability concern rather than a trust state.
   7. `/tree` returns a grouped bucket envelope with top-level `read_class`, `observation_status`, `pit`, `groups[]`, and `group_page`; `/on-demand-force-find` returns the same grouped-envelope family with `observation_status.state=fresh-only`.
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
9. Product-facing worker modes are only `embedded | external`; operator-visible worker config uses `workers.facade.mode`, `workers.source.mode`, `workers.scan.mode`, and `workers.sink.mode`, while app/runtime composition consumes only compiled `__cnx_runtime.workers` bindings emitted by the fs-meta release compiler.
10. Product-facing L0-L2 specs stay in worker/mode vocabulary; realization mechanics stay internal, but downstream architecture docs explicitly preserve the root `worker / process / unit` split.
11. In fs-meta terms, `worker` names the product-facing role (`facade/source/scan/sink`), `process` names the hosting container selected by `embedded | external`, and `unit` remains the runtime-owned finest bind/run, activation, tick, and state-boundary identity.
12. `fs-meta/` is the product container directory only; it is not a Cargo package and exists to host the product app package, worker artifact packages, specs, docs, fixtures, and testdata.
13. `fs-meta/app` is the main product app package for this container; it stays `publish = false`, owns package-local API/query/orchestration/business composition plus config/types, and does not define a generic reusable fs-meta library surface.
14. Ordinary app-facing business modules use the SDK family rooted in `capanix-app-sdk`: shared observation authoring consumes `capanix-managed-state-sdk` declarations/evaluator helpers, top-level runtime-host authoring lowers through `capanix-service-sdk`, lower toolkit helpers consume `capanix-app-sdk`, worker-process client/server support consumes the helper-only `capanix-worker-runtime-support`, and narrow runtime glue, orchestration, and protocol-boundary seams MAY directly consume `capanix-runtime-api`, while production business/runtime modules in the app package do not depend on `capanix-kernel-api` or `capanix-unit-sidecar`.
15. The service-sdk runtime host is primitive-first only: fs-meta initialization is triggered only by runtime orchestration controls (`activate`, `deactivate`, `tick`, and trusted-exposure confirmation) and must fail closed for `request/stream` before that control arrives; grant-change, manual-rescan, or passthrough controls do not implicitly bootstrap the app, and fs-meta does not rely on a fifth public start-hook seam from the platform.
16. The app package consumes `managed-state-sdk`, `service-sdk`, `app-sdk`, helper-only `worker-runtime-support`, `host-adapter-fs`, `host-fs-types`, and `route-proto` as upstream authorities/support carriers; `managed-state-sdk` remains declaration/evaluator-only, `service-sdk` remains the runtime host/lowering owner, `product` remains the bounded product-facing CLI/tooling namespace, while `query`, `product::release_doc`, and `workers` may remain public package-local operational/test support surfaces without becoming product or platform authority.
17. External-worker realization helpers remain confined to explicit worker runtime seams and helper-only upstream support crates; the app package may consume only the bounded typed worker handle/transport surface from `capanix-worker-runtime-support`, not raw bridge/bootstrap primitives.
18. The upstream bridge-realization seam is low-level carrier glue only; helper-only `capanix-worker-runtime-support` owns worker child-process bootstrap, control/data socket ownership, direct control-plane startup/management, retry clipping, and lifecycle supervision, while fs-meta worker artifact crates own only their explicit bootstrap-session/server entry functions and request handling logic.
19. The canonical worker transport contract MUST preserve canonical `Timeout` / `TransportClosed` categories plus wall-clock timeout clipping before retry; transport failures are not flattened into a generic peer-error bucket.
20. `embedded` workers share the host process; `external` workers run in dedicated worker processes.
21. Constructor/bootstrap faults surface as typed `CnxError` / `init_error`; runtime local-execution join failures and worker/bootstrap faults are handled as app-local or local-execution-local errors or explicit degraded behavior rather than routine production `panic!` / `expect!` flow.
22. Package-local implementation env seams remain bounded tuning knobs; `FS_META_SOURCE_SCAN_WORKERS`, `FS_META_SOURCE_AUDIT_INTERVAL_MS`, `FS_META_SOURCE_THROTTLE_INTERVAL_MS`, `FS_META_SINK_TOMBSTONE_TTL_MS`, `FS_META_SINK_TOMBSTONE_TOLERANCE_US`, and `FS_META_SOURCE_AUDIT_DEEP_INTERVAL_ROUNDS` tune implementation behavior without redefining truth, cutover, or query-surface contracts.

## ARCHITECTURE.CLI_AND_TOOLING_BOUNDARY

1. CLI command layer parses deploy/undeploy/local/grants/roots command workflows and targets the bounded fs-meta product boundary; login/rescan remain bounded HTTP/API workflows consumed inside operator sessions, tests, and support clients rather than top-level `fsmeta` subcommands.
2. Request composition layer uses fs-meta bounded `product` types, bounded HTTP facade endpoints, external `cnxctl` deploy/control commands, and shared kernel-owned auth vocabulary without importing app package-local runtime/orchestration internals.
3. Upgrade workflow submits a new release generation through the same deploy boundary instead of exposing manual internal release-doc editing.
4. Local-dev helpers, including the workspace package `capanix-app-fs-meta-tooling` and the product-scoped `capanixd-fs-meta` launcher, remain tooling only, are feature-gated away from the default CLI install, and do not create new platform authority or alter daemon ownership.
5. The optional `capanixd-fs-meta` launcher composes `capanix_daemon::run_with_host_passthrough_bootstrap(...)` with `capanix_host_adapter_fs::spawn_host_passthrough_endpoint`; it does not bind sockets, construct runtime control services, or start runtime directly.
6. CLI remains a request client only: it does not own bind/run realization, route convergence, route target selection, or `state/effect observation plane` meaning such as `observation_eligible`.

## ARCHITECTURE.GOVERNANCE_REFERENCE

1. Repo topology, crate ownership, dependency rules, and validation workflow are engineering-governance material rather than formal runtime architecture.
2. Those constraints live in `fs-meta/docs/ENGINEERING_GOVERNANCE.md`; formal architecture keeps only product/runtime ownership and interaction boundaries.

## ARCHITECTURE.WORKER_ROLE_TO_ARTIFACT_MAP

1. `facade-worker` maps to the `fs-meta/worker-facade/` artifact crate and remains the only embedded worker in the current baseline.
2. `source-worker`, `scan-worker`, and `sink-worker` remain distinct operator-visible worker roles, but their external realization is hosted by the platform generic `capanix-worker-host` loading the shared `fs-meta/worker-facade/` worker module.
3. `fs-meta/worker-source/`, `fs-meta/worker-sink/`, and `fs-meta/worker-scan/` remain role-local helper crates that own `run_*_worker_server(...)` entry functions and request handling logic; they are not standalone executable artifact identities.
4. `scan-worker` may continue to reuse lower-level source-runtime helpers internally while it shares the same external worker module path and supervision surface as `source-worker`.
5. Any future change from the shared worker module to a role-specific module or artifact split MUST update this section first, then the dependent manifests and tests.
