---
version: 3.0.0
---

# L2: fs-meta Domain Architecture

> Focus: fs-meta domain externally behaves as one app product container; internal
> execution is organized as `facade-worker`, `source-worker`, and
> `sink-worker`, with product-facing worker mode vocabulary limited to
> `embedded | external`.

## ARCHITECTURE.COMPONENTS

1. **Shared Types Component**:
   1. Inputs: domain type evolution requirements.
   2. Outputs: shared wire contracts for Unix/FS atomic facts and shared vocabulary (`FileMetaRecord`, `UnixStat`, `UnixDirEntry`, `StabilityMode`, `UnreliableReason`, and related passthrough/control carriers).
   3. Role: fs-meta shared MessagePack carrier single source of truth for internal event/control and Unix/FS field constraints; fs-meta app-local query request DTOs and fs-meta query/result payload schemas remain app/domain-owned.

2. **Single fs-meta App Boundary**:
   1. Inputs: filesystem change observations, domain queries, runtime control frames, runtime host object grants.
   2. Outputs: `query/find` responses, domain events, and app-owned port boundaries.
   3. Role: encapsulate source/sink semantics, three worker roles (`facade-worker`, `source-worker`, `sink-worker`), thin runtime ABI participation, app-owned opaque ports, and resource-scoped HTTP facade semantics behind one app package boundary while allowing runtime to schedule multiple worker instances.
   3.1. Constraint: this boundary is a downstream app product container, not a generic reusable shared library package.
   4. Stateful fs-meta authoring and runtime participation consume bounded upstream observation, service, and runtime-entry surfaces above the low-level kernel mirror; exact crate topology and helper layering remain engineering-governance material.
   5. Product-facing execution vocabulary stays at `embedded | external`; realization mechanics remain internal.

3. **Meta-Index Service (App Internal)**:
   1. Inputs: realtime watch stream, initial full scan batches, audit pass results, sentinel feedback.
   2. Outputs: materialized metadata index + repair actions.
   3. Role: own fs-meta index lifecycle; not kernel-owned state.
   4. Baseline: materialized index is in-memory observation/projection state (hosting-bound), not durable checkpoint state and not the domain truth ledger itself.
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
   2. facade/query layer resolves target logical groups and fans out only to the sink-side execution partition that owns each group; worker hosting identity is hosting-only and is not the fan-out contract.
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

1. One `fs-meta` app product container hosts three worker roles: `facade-worker`, `source-worker`, and `sink-worker`.
2. `facade-worker` owns HTTP/API ingress, auth, PIT lifecycle, response shaping, and current query orchestration; `query` remains inside `facade-worker` for now.
3. `source-worker` owns live watch, live force-find, initial full scan, periodic audit, overflow/recovery rescans, and source-side host/grant interaction.
4. `sink-worker` owns materialized tree/index maintenance plus `/tree` and `/stats` materialized-query backend duties.
5. Current baseline defaults: `facade-worker=embedded`; `source-worker=external`; `sink-worker=external`.
6. This split is intentionally `3`, not `2`, so sink/materialized ownership stays separate from source-side observation while query orchestration remains packaged with the facade.
7. This split is intentionally `3`, not `4`, because scan stays a source-side unit responsibility rather than a standalone worker role.
8. Product-facing worker modes are only `embedded | external`; operator-visible worker config uses `workers.facade.mode`, `workers.source.mode`, and `workers.sink.mode`, while app/runtime composition consumes compiled runtime worker bindings rather than raw realization-specific startup fields.
9. Product-facing L0-L2 specs stay in worker/mode vocabulary; runtime-facing architecture keeps only the `worker / unit` split, and hosting realization remains deployment-only.
10. In fs-meta terms, `worker` names the product-facing role (`facade/source/sink`), `runtime.exec.scan` remains a source-side unit, and `unit` stays the runtime-owned finest bind/run, activation, tick, and state-boundary identity.
11. The product boundary separates bounded authoring/domain, runtime artifact, deploy compilation, and operator tooling surfaces; exact repo topology and package naming are engineering-governance material.
12. The developer-facing authoring surface owns shared declarations, bounded config/types, and product compile-time declarations without taking runtime or deploy authority.
13. The runtime artifact surface owns worker entry, runtime overlay decoding, and the assembled source/sink/query/api product implementation.
14. The deploy compilation surface owns release-document generation and worker-binding compilation without taking runtime artifact implementation authority.
15. App-facing business modules consume bounded upstream authoring, runtime, and service surfaces; exact crate chains, helper names, and dependency allowlists remain governance material.
16. Initialization is control-driven: activation and trusted-exposure controls may bootstrap the app, while request/stream traffic must fail closed before that point; grant-change, manual-rescan, or passthrough controls do not implicitly bootstrap the app.
17. Runtime overlays such as granted hosts/resources remain app-consumed inputs, but the exact overlay field names and wrapper helpers are implementation detail beneath this architecture.
18. External-worker realization helpers remain confined to explicit worker runtime seams; lower-level support layers remain implementation-only and do not define product contracts.
19. Worker bootstrap, lifecycle supervision, and transport classification remain below the business-module boundary; the runtime artifact owns explicit server entry and request handling logic.
20. The canonical worker transport contract MUST preserve canonical `Timeout` / `TransportClosed` categories plus wall-clock timeout clipping before retry; transport failures are not flattened into a generic peer-error bucket.
21. `embedded` workers stay inside the shared host boundary; `external` workers use isolated external worker hosting.
22. Constructor/bootstrap faults surface as typed `CnxError` / `init_error`; runtime local-execution join failures and worker/bootstrap faults are handled as app-local or local-execution-local errors or explicit degraded behavior rather than routine production `panic!` / `expect!` flow.
23. Package-local implementation tuning knobs may exist, but they remain bounded implementation controls and MUST NOT redefine truth, cutover, or query-surface contracts.

## ARCHITECTURE.CLI_AND_TOOLING_BOUNDARY

1. CLI command layer parses deploy/undeploy/local/grants/roots command workflows and targets the bounded fs-meta product boundary; login/rescan remain bounded HTTP/API workflows consumed inside operator sessions, tests, and support clients rather than top-level `fsmeta` subcommands.
2. Request composition layer uses fs-meta bounded deploy/authoring types, bounded HTTP facade endpoints, external `cnxctl` deploy/control commands, and shared kernel-owned auth vocabulary without importing runtime/orchestration internals from `fs-meta/app`.
3. Upgrade workflow submits a new release generation through the same deploy boundary instead of exposing manual internal release-doc editing.
4. Local-dev helpers remain tooling only, are feature-gated away from the default CLI install, and do not create new platform authority or alter daemon ownership.
5. The optional local-dev launcher composes host-passthrough bootstrap with host-fs passthrough adaptation; it does not bind sockets, construct runtime control services, or start runtime directly.
6. CLI remains a request client only: it does not own bind/run realization, route convergence, route target selection, or `state/effect observation plane` meaning such as `observation_eligible`.

## ARCHITECTURE.GOVERNANCE_REFERENCE

1. Repo topology, crate ownership, dependency rules, and validation workflow are engineering-governance material rather than formal runtime architecture.
2. Those constraints live in `fs-meta/docs/ENGINEERING_GOVERNANCE.md`; formal architecture keeps only product/runtime ownership, externally relevant package boundaries, and interaction seams that materially affect fs-meta domain semantics.

## ARCHITECTURE.WORKER_ROLE_TO_ARTIFACT_MAP

1. `facade-worker` remains the only embedded worker in the current baseline and is hosted inside the main fs-meta runtime artifact.
2. `source-worker` and `sink-worker` remain the two operator-visible external worker roles, and their external realization is hosted through the platform generic external worker hosting path loading the shared fs-meta worker runtime surface.
3. Role-local external worker request handling and bootstrap-session logic are runtime-package implementation detail, not standalone artifact identities.
4. `runtime.exec.scan` remains a source-side unit and reuses the source-server/runtime surface under `source-worker` rather than defining a standalone worker role.
5. Any future change from the shared worker surface to a role-specific artifact split, or any future promotion of scan into its own worker role, MUST update this section first, then the dependent manifests and tests.
