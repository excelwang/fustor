# TODO

## 1. Authority And Scope

This file is the only active engineering ledger for Fustor DataView v0.4.
Historical phase notes are not active TODOs.

The canonical functional stack is:

```text
Source -> SourceProjection (optional) -> DataView
                                      -> private discovery access/control plane
```

- `Source` denotes the underlying source authority domain and is the unique
  authority for current source-native state, source revision, source-owned
  permission and capability facts, fingerprints, executable source operations,
  and source revalidation evidence.
- Fustor decides effective DataView visibility and actionability by evaluating
  source-owned facts together with current grants, tenant/principal/purpose
  policy, freshness requirements, disclosure limits, and execution/discovery
  budgets.
- `SourceProjection` is optional source-local derived state. It may improve cost
  or latency, but it never replaces source authority.
- `DataView` is Fustor's deployment-global, demand-materialized association
  view. The `union-graph` crate name may remain an internal implementation term.
- Private discovery is the access and control plane over authorized DataView
  evidence and bounded source capabilities. It is not another durable data tier.
- New DataView results enter only through a one-shot `BoundedSourceAction` and
  an accepted `ActionResultSubmission`.
- Continuous maintenance uses a separate `StreamingSubscription`. A stream may
  refresh, invalidate, or delete only results already tracked when the
  subscription is established or explicitly rebased; it cannot discover a new
  DataView object.
- Every one-shot action and streaming subscription explicitly selects `direct`
  or `projection-backed`. Fustor does not silently fan out, switch, or fall back
  between those paths.
- Fustor owns result normalization, evidence validation, DataView identity,
  graph assertions, revisioning, retention, and scoped projection.
- Fustor remains a passive, caller-agnostic tool. It embeds no Coox runtime,
  starts no upper-layer workflow, and calls no planner, session, or application
  callback.
- Agentic Map may call Fustor through a caller-owned thin addon inside Coox. The
  addon transports generic Fustor requests and responses only.
- High-frequency source streaming bypasses Coox and enters Fustor through the
  generic streaming contract. Upper layers observe the changed head on a later
  read or reprojection.
- Capanix remains an app-neutral carrier and is not aware of Fustor, DataView,
  SourceProjection, or Agentic Map semantics.

An unchecked item below is open work.

## 2. Closed Foundation

- [x] Capanix/Fustor neutrality and one-way dependency boundaries are locally
  gated.
- [x] Agent/session/route/CLI ownership has been removed from reusable Fustor
  private-discovery APIs.
- [x] Source authority, freshness guards, existence non-commitment, bounded
  actions, discovery budgets, and BFS-first frontier primitives exist.
- [x] PostgreSQL, MySQL, S3, Elasticsearch, local-fs, CSV, and JSONL source
  payload adapters exist.
- [x] Union Graph has global identities, graph nodes and edges, native anchors,
  and basic evidence references.
- [x] Local and disposable-container gates exist without touching official
  `198.51.100.144-146` services.

The legacy `UnionGraphStore` ingest/query API remains an internal compatibility
and source-adapter test surface. It is not the deployed DataView authority. The
v0.4 runtime uses `DataViewStore`, revisioned `GraphDeltaRecord`, Fustor-owned
normalizers, and `ScopedDataViewProjector`; new callers must not treat legacy
unversioned graph snapshots as a DataView write or read contract.

## 3. P0: Freeze The Specification Model

### 3.1 Root Fustor Specs

- [x] Replace the two competing L2 documents with one
  `specs/L2-ARCHITECTURE.md`.
- [x] Define `Source`, optional `SourceProjection`, `DataView`, and private
  discovery as distinct functional roles.
- [x] Define `BoundedSourceAction` as the only one-shot materialization path.
- [x] Define `StreamingSubscription` as a separate lease over pre-tracked
  results, not as an action mode.
- [x] Require explicit `direct | projection-backed` path selection and
  projection apply evidence for projection-backed observations.
- [x] Assign source-specific result normalization to Fustor's
  `ResultNormalizerRegistry`, not producers or callers.
- [x] Define one deployment-global logical DataView with opaque revisions,
  complete bounded payload retention, evidence-gated atomic commit,
  deterministic concurrency handling, dependency invalidation, and retention.
- [x] Define latest/current-actionable and exact/historical-replay scoped
  projection under current authorization and disclosure policy.
- [x] Preserve source authority, existence non-commitment, caller neutrality,
  Capanix neutrality, and no-source-runtime-orchestration contracts.
- [x] Calibrate the formal L0/L1 source-authority wording so unqualified
  `permissions` and `revisions` cannot be read as source ownership of effective
  DataView authorization or of SourceProjection/DataView revisions.
- [x] Forbid unrestricted ingestion, implicit path fallback, historical branch
  execution, source-specific DataView APIs, and upper-layer callbacks.

### 3.2 Root L3 Runtime Specs

- [x] Define caller-agnostic interfaces for action execution context, complete
  result submission, projection evidence, streaming subscription and refresh,
  commit outcome, and scoped projection.
- [x] Define validation, normalization, commit, safe-merge, head-selection,
  invalidation, streaming-refresh, scoped-projection, and retention algorithms.
- [x] Define direct one-shot, projection-backed one-shot, stream maintenance,
  latest projection, historical projection, and pull-only caller workflows.
- [x] Record authority hierarchy, projection optionality, demand boundary,
  normalization ownership, external orchestration, pull-only visibility,
  opaque revisions, historical projection, Capanix neutrality, and operation
  extensibility decisions.
- [x] Keep active Fustor specs free of Agentic Map- or Coox-owned request,
  session, lifecycle, and callback semantics.

### 3.3 fs-meta Mapping

- [x] Map source-side live observation plus the authoritative journal to the
  `SourceProvider` authority side.
- [x] Map the sink-side materialized tree to an optional `SourceProjection`
  without promoting it to a truth ledger.
- [x] Map `/on-demand-force-find` to the direct path and materialized read
  classes to the projection-backed path.
- [x] Specify schema-tagged source evidence and projection apply evidence while
  keeping DataView identity, graph assertions, and normalization outside
  fs-meta.
- [x] Specify stream export as refresh/invalidate/delete over externally tracked
  result references only.
- [x] Promote the three fs-meta integration contracts to system verification
  through direct, projection-backed, and tracked-stream implementation tests.

### 3.4 Spec Verification

- [x] Replace `tests/specs_private_discovery.rs` with
  `tests/specs_fustor_architecture.rs`.
- [x] Update local and cross-repository gates to use the consolidated spec tree.
- [x] Update formal spec readmes and remove references to deleted L2/L3 files.
- [x] Root Fustor specification test passes.
- [x] Root Vibespec traceability validation passes for all L1 contracts.
- [x] fs-meta specification validation preserves all previous system anchors and
  includes honest system anchors for the implemented integration contracts.

P0 exit criteria:

- [x] One L2 owns the full Source -> SourceProjection -> DataView architecture.
- [x] No active spec both forbids complete accepted bounded results and requires
  DataView materialization.
- [x] One-shot materialization and continuous maintenance have separate,
  non-overlapping contracts.
- [x] Specs contain no implicit projection fallback or stream-based discovery.
- [x] Fustor is caller-agnostic and Capanix remains app-neutral.
- [x] Formal source-authority contracts distinguish source-owned facts and
  evidence from Fustor-owned DataView visibility and actionability decisions.

## 4. P1: Implement Source And Projection Admission

### 4.1 Public Model

- [x] Add Fustor-owned `SourceProviderRef`, `SourceProjectionRef`, observation
  path, source evidence, projection apply evidence, schema identity, native
  anchors, source revision, and authority epoch types.
- [x] Add versioned `BoundedSourceAction`, `ActionExecutionContext`,
  `ActionResultSubmission`, and non-disclosing typed result outcomes.
- [x] Add versioned `StreamingSubscription` and
  `StreamingRefreshSubmission` types with tracked result refs, lease, sequence,
  watermark, schema, grant, policy, rate, batch, byte, and concurrency bounds.
- [x] Reject unsupported wire or payload schema versions before state mutation.

### 4.2 Registries And Path Enforcement

- [x] Implement `BoundedActionRegistry` issuance, expiry, idempotency, budget,
  policy-context, grant-epoch, and base-revision binding.
- [x] Implement `StreamingSubscriptionRegistry` creation from already
  materialized tracked results, explicit rebasing, pause, expiry, revocation,
  sequence, watermark, and lease enforcement.
- [x] Reject missing projection evidence on projection-backed paths.
- [x] Reject projection evidence on an undeclared direct path when it would
  alter path semantics.
- [x] Reject implicit direct fallback, projection fallback, path switching, or
  hidden dual-path fanout.
- [x] Prove direct-only sources work without a SourceProjection.

### 4.3 Result Normalization

- [x] Implement Fustor-owned `ResultNormalizerRegistry` dispatch by source
  schema and version.
- [x] Normalize complete source payloads into canonical DataView identities,
  payload versions, relationship assertions, dependencies, and evidence without
  dropping source-native fields.
- [x] Reject unknown schemas atomically.
- [x] Keep DataView core free of source-specific public write variants.
- [x] Add heterogeneous normalization fixtures for at least PostgreSQL, S3,
  Elasticsearch, and fs-meta direct/projection-backed results.

## 5. P2: Implement The Revisioned DataView Core

### 5.1 Store And Revision Model

- [x] Add `DataViewRef`, opaque revision identity, immutable revisions, payload
  versions, evidence records, invalidations, dependency refs, and revisioned
  graph deltas.
- [x] Introduce a storage abstraction with an in-memory deterministic reference
  implementation and a spec-compatible deployment implementation.
- [x] Initialize an empty genesis revision.
- [x] Store payload, evidence, invalidation, dependency, and delta records
  append-only while exposing one mutable logical head.
- [x] Keep public revision ids opaque and non-enumerable; internal ordering must
  remain private.
- [x] Persist issued action executions, idempotency outcomes, active streaming
  subscriptions, processed batch outcomes, and scoped revision aliases under a
  restart-safe boundary compatible with the durable DataView store.

### 5.2 Atomic Commit And Concurrency

- [x] Validate action/subscription identity, execution context, source
  authority, evidence, policy, grants, schema, limits, budget, base revision,
  freshness, path, and idempotency before commit.
- [x] Make commit transactional: every required record and head change appears
  together, or no externally visible state changes.
- [x] Accept stale-base submissions only through deterministic safe merge of
  disjoint updates.
- [x] Retain conflicting versions and forbid blind last-write-wins.
- [x] Select the current head deterministically from source authority, source
  revision, invalidation, freshness, confidence, and derivation state.
- [x] Prevent duplicate submission from double-spending budget or creating a
  duplicate logical revision.
- [x] Return accepted, safe-merged, requires-revalidation, or rejected outcomes
  without disclosing hidden competing state.

### 5.3 Streaming Maintenance

- [x] Resolve every stream entry to a tracked result ref before normalization or
  commit.
- [x] Support only refresh, invalidate, and delete operations.
- [x] Reject unknown identities without creating payload, relationship, count,
  coverage, revision, or timing evidence about them.
- [x] Enforce sequence, watermark, batch, bytes, events, lease, policy, grants,
  path, and projection-evidence continuity.
- [x] Make duplicate batches idempotent and watermark gaps explicit.
- [x] Ensure stream processing never invokes Coox or any upper-layer callback.

## 6. P3: Implement ScopedDataView And Private Discovery

- [x] Add `ScopedDataView` containing one `DataViewRef`, visible results,
  scoped discovery graph, frontier, coverage, limiting reasons, visible
  continuation action ids, and freshness constraints.
- [x] Implement latest/current-actionable and exact/historical-replay projection
  through one projector without persisting caller-specific projections.
- [x] Read one immutable DataView revision per response.
- [x] Apply tenant, principal, purpose, grants, policy, disclosure budget,
  discovery budget, and freshness before emitting payload, relationship,
  coverage, continuation, error, or trace information.
- [x] Compute `complete-for-scope | partial | unknown` only from authorized,
  observable evidence; never claim source-global or deployment-global
  completeness.
- [x] Preserve visible relationship lineage while redacting denied contributors
  and evidence.
- [x] Permit continuation only for bounded actions already visible in the
  returned frontier.
- [x] Rank authorized next actions breadth-first across source families under
  remaining time, calls, rows, bytes, fanout, cost, disclosure, and discovery
  budgets before selective deepening.
- [x] Reproject exact revisions under current authorization and policy, issue no
  executable historical action, perform no source call, and mutate no head.
- [x] Collapse malformed, unknown, unauthorized, evicted, and
  compliance-redacted exact refs to `revision-not-available`.

## 7. P4: Implement Retention And Compliance

- [x] Add actionable, historical, invalidated, and tombstoned lifecycle states
  for payload versions and relationship evidence.
- [x] Implement TTL, capacity, source-policy, tenant-policy, dependency, and
  compliance deletion rules.
- [x] Mark stale or invalidated data non-actionable before eviction.
- [x] Propagate invalidation to derived payloads through dependency links.
- [x] Prevent eviction from leaving actionable derived results without required
  evidence or breaking deterministic current-head selection.
- [x] Retain only policy-permitted tombstones and provenance.
- [x] Prove deleted or evicted content cannot reappear through indexes,
  projections, errors, traces, counts, or continuation metadata.

## 8. P5: Runtime And Cross-Repository Migration

### 8.1 Fustor Runtime

- [x] Expose generic action issuance, result submission, streaming subscription,
  streaming refresh, DataView reference, and scoped projection operations.
- [x] Replace unversioned caller-supplied graph snapshots with revisioned
  DataView reads.
- [x] Keep source execution outside DataView, private discovery, and result
  normalization components.
- [x] Add restart, concurrency, crash-recovery, and durable-store tests required
  by the selected storage guarantee.

### 8.2 fs-meta Runtime Mapping

- [x] Implement schema-tagged direct source evidence export for bounded
  force-find results.
- [x] Implement projection apply evidence export for materialized and
  trusted-materialized reads.
- [x] Implement tracked-result streaming refresh export without new-object
  discovery.
- [x] Keep fs-meta-native authority, PIT, pagination, observation eligibility,
  and source/sink recovery semantics unchanged.
- [x] Promote the three fs-meta v3.1 integration anchors from logic to system
  only after real implementation and integration tests pass.
- [x] Preserve giant-root public API parameter governance for deployments with
  more than 1 billion files: invalid or conflicting public query parameters must
  fail fast with HTTP `400 Bad Request`, and status or parameter validation must
  not perform file-count-dependent query or repair work.

### 8.3 Agentic Map Handoff

Agentic Map work is owned in `/opt/fusiongraph/src/agentic-map/TODO-from-fustor.md`; the
compiled Fustor v0.4 boundary and migration are now closed by cross-repository
tests.

- [x] Route interactive DataActionMap materialize, revalidate, and apply flows
  through Coox using a caller-owned thin Fustor addon.
- [x] Limit the addon to generic request/response transport; it must not
  validate, normalize, merge, retain DataView payloads, or emit authoritative
  graph deltas.
- [x] Replace task-persisted DataView payload/cache ownership with Fustor
  `DataViewRef` values and process-local scoped projections.
- [x] Transport Fustor coverage, relationship evidence, revisions, deltas, and
  typed residuals unchanged.
- [x] Observe stream-driven head changes only on a later read or reprojection;
  add no Fustor callback.
- [x] Keep streaming hot-path delivery outside Coox and Agentic Map lifecycle
  calls.

### 8.4 Capanix Boundary

- [x] No Capanix spec or source change is required for DataView v0.4.
- [x] Keep Capanix work closed unless a failing integration test proves a missing
  app-neutral carrier invariant; an app-specific convenience is insufficient.

## 9. Discovery Operation Extensibility Gate

- [x] Do not add a source- or product-specific core discovery operation for one
  integration example.
- [x] Require at least two heterogeneous domains that cannot express the
  behavior through existing bounded actions, schemas, normalizers, and
  expansion rules.
- [x] If the gate is met, prefer a namespaced capability descriptor with generic
  risk, side-effect, breadth/depth rank, hard limits, continuation, schema, and
  evidence requirements.
- [x] Prove the current v0.4 integration requires no new upper-layer lifecycle
  request variant.

## 10. Required Acceptance Scenarios

- [x] A direct-only source completes a one-shot action without a
  SourceProjection.
- [x] A projection-backed action without valid apply evidence fails without
  direct fallback or state mutation.
- [x] Direct and projection-backed paths preserve the same source-native meaning
  while retaining distinct source and projection revisions.
- [x] A bounded action stores its complete result with action, producer, source,
  anchor, revision, schema, evidence, policy, grants, freshness, derivation,
  continuation, and actual cost metadata.
- [x] Two tasks contribute disjoint bounded results and both survive at the next
  global head.
- [x] Conflicting observations retain both evidence versions and produce a
  deterministic current head.
- [x] A stale-base disjoint update commits only through proven safe merge;
  overlapping updates retain conflict versions under deterministic head
  selection or return revalidation, never blind overwrite.
- [x] A stream refreshes, invalidates, and deletes tracked results with ordered,
  idempotent watermark progression.
- [x] A stream event for an untracked object creates no DataView identity,
  payload, relationship, count, coverage, revision, error, trace, or useful
  timing signal.
- [x] Partial source coverage returns `partial` or `unknown` plus only visible
  continuation actions.
- [x] A caller cannot infer inaccessible co-resident data through existence,
  count, revision, relation, error, trace, or timing behavior.
- [x] Exact retained revisions are reprojected under current grants and policy;
  revoked content remains hidden and frontier entries are replay-only.
- [x] Historical projection performs no source execution, subscription,
  mutation, or executable branch issuance.
- [x] Unknown, unauthorized, evicted, and compliance-redacted revision refs are
  externally indistinguishable.
- [x] Public revision ids reveal no global ordering, commit count, tenant
  activity, or hidden source activity.
- [x] Relationship evidence and freshness survive scoped projection without
  exposing hidden contributors.
- [x] fs-meta direct, projection-backed, and tracked-stream mappings preserve its
  authoritative-journal/materialized-tree distinction.
- [x] Agentic Map's interactive path uses Coox transport while Fustor remains
  passive and its streaming path bypasses Coox.

## 11. Verification Gates

Run narrow gates after each phase and the complete cross-repository gate before
claiming v0.4 conformance:

```bash
cargo test --test specs_fustor_architecture -- --nocapture
python3 /data/repo/vibespec/src/skills/vibespec/scripts/validate.py specs/
(cd fs-meta && ./scripts/validate_specs.sh)
cargo test -p union-graph -- --nocapture
cargo test -p union-graph-runtime -- --nocapture
cargo test -p private-discovery -- --nocapture
./scripts/data-action-map-local-gate.sh
./scripts/data-action-map-cross-repo-gate.sh
fs-meta/scripts/fs-meta-container-staging-gate.sh run all
```

The implementation gate must additionally prove:

- [x] serialization round trips preserve every required metadata field;
- [x] rejected operations leave state byte-for-byte or logically unchanged;
- [x] concurrency tests cover lost update, safe merge, conflict, duplicate
  submission, invalidation, and stream watermark races;
- [x] projection tests cover cross-tenant, cross-principal, purpose, grant,
  freshness, disclosure-budget, hidden-lineage, and historical-read boundaries;
- [x] retention tests cover stale history, dependency-safe eviction, compliance
  deletion, and tombstone disclosure;
- [x] Agentic Map owns no global DataView store, normalizer, merge policy, or
  authoritative graph delta;
- [x] every local/cross-repository gate records
  `official_stable=not_touched hosts=198.51.100.144,198.51.100.145,198.51.100.146`.
- [x] The disposable container staging entrypoint covers active-three
  external-worker replay over three temporary real NFS exports plus a blocking
  business-mini-nfs smoke; the current clean-state pass requirement is tracked
  only in section 13.5 below.

Official fs-meta stable deployment acceptance remains a separate operational
workflow. Use the repository-owned skills and
`fs-meta/docs/CONTAINER_STAGING_GATE.md`; do not duplicate those runbooks here.

## 12. Definition Of Done

DataView v0.4 is complete only when:

- [x] P1-P5 and every required acceptance scenario are closed;
- [x] specs, source, runtime wire, tests, research material, and cross-repository
  integrations describe the same ownership and semantics;
- [x] SourceProjection remains optional and never becomes source authority;
- [x] new DataView objects require one-shot bounded actions and streams maintain
  only pre-tracked results;
- [x] latest and exact reads share one Fustor-owned projector and historical
  reads cannot create executable branches;
- [x] Agentic Map uses Fustor as a passive tool through a thin Coox addon and
  persists no global DataView payload state;
- [x] Capanix remains unaware of Fustor and DataView;
- [x] No DataView-v0.4 semantic item through this section remains unchecked;
  the later Source connector acceptance phase is tracked independently in
  section 13.

## 13. Source Connector Completion And Acceptance

This is the active post-DataView-v0.4 implementation phase. A connector is not
complete merely because its crate compiles, a fake driver answers a probe, or a
native test is permanently ignored.

### 13.1 Shared Source Contract

- [x] Implement versioned source-neutral request and response envelopes in
  `source-kit` without importing DataView, private-discovery, Agentic Map, or
  caller workflow types.
- [x] Enforce request id, grant epoch, deadline, rows, bytes, calls, fanout,
  latency, and cost limits and return typed non-secret errors.
- [x] Issue tamper-detectable `SourceObservationReceipt` values containing
  source revision, anchors, evidence, observation time, continuation state, and
  actual accounting.
- [x] Reject legacy or incomplete source payloads for new DataView
  materialization while retaining only explicitly bounded diagnostic
  compatibility where required.
- [x] Close stateful native sessions on terminal page, explicit close, expiry,
  grant/config change, route deactivation, runtime shutdown, and continuation
  failure.

### 13.2 Connector Capability Matrix

- [x] PostgreSQL: replace process-per-query pseudo-snapshots with one real
  repeatable-read read-only native transaction; cover probe, schema discovery,
  keyset paging, close, expiry, grant change, restart loss, timeout, and errors.
- [x] MySQL: retain one consistent-snapshot transaction and add receipt,
  timeout, grant-change cleanup, service-level native paging, and failure tests.
- [x] Elasticsearch: add real disposable-service probe, mapping discovery, PIT
  paging and close, ordered poll, equal-timestamp tie breaking, timeout,
  throttling, authorization, and receipt tests.
- [x] S3: add real disposable-service probe, metadata discovery, continuation,
  bounded poll scanning, ordered checkpoint, retry/error classification,
  timeout, and receipt tests.
- [x] Local FS: add real bound-route probe, field discovery, file/directory
  paging, traversal/symlink escape rejection, mutation/revision behavior,
  byte limits, and receipt tests.
- [x] CSV: add real bound-route discovery and paging for quoted, headerless,
  malformed, large, and changing files with stable row continuation and receipt
  tests.
- [x] JSONL: add real bound-route sampled discovery and byte-offset paging for
  blank, malformed, large, UTF-8, and changing files with receipt tests.
- [x] fs-meta: retain direct, projection-backed, and tracked-stream mappings.
- [x] fs-meta: rerun blocking disposable `active-three` plus
  `business-mini-nfs` staging from a clean local container state. The blocking
  `run all` gate passed on 2026-07-30 with the active-three and mini logs rooted
  at `/tmp/fsmeta-container-staging-gate-all-20260730073447.log`.

### 13.3 Fustor Mapping

- [x] Register Fustor-owned normalizers for PostgreSQL, MySQL, Elasticsearch,
  S3, Local FS, CSV, JSONL, fs-meta direct, and fs-meta projection schemas.
- [x] Validate source response receipts against generic
  `ActionResultSubmission` authority, revision, anchors, evidence, grant epoch,
  observation time, continuation, payload bytes, and actual accounting.
- [x] Prove every source payload reaches DataView through the generic schema
  registry without adding source-specific DataView request variants.
- [x] Add route-to-source-to-normalizer-to-DataView end-to-end tests for every
  connector.

### 13.4 Disposable Source Acceptance

- [x] Add one blocking repository-owned source acceptance gate with pinned
  PostgreSQL, MySQL, Elasticsearch, and S3-compatible container images plus
  temporary Local FS, CSV, and JSONL fixtures.
- [x] Run native tests through real Capanix bound routes; backend-only health or
  direct-driver smoke is insufficient.
- [x] Remove permanent `ignore` dependence from critical native connector paths;
  the gate must explicitly run every environment-gated test it owns.
- [x] Emit per-source and final PASS/FAIL sentinels, preserve bounded logs, and
  clean containers, volumes, credentials, processes, and temporary data on
  every exit path.
- [x] Record
  `official_stable=not_touched hosts=198.51.100.144,198.51.100.145,198.51.100.146`
  and never use official fs-meta stable state as source-connector evidence.

### 13.5 Completion Gates

- [x] Repository-owned Cargo, workspace-test, Source acceptance, cross-repo,
  local deployment, and fs-meta container-staging entrypoints enforce bounded
  build/test concurrency plus a hard two-CPU host-process limit by default;
  disposable containers keep their independent explicit CPU quota. Guarded
  fs-meta tests additionally use two scan workers and one shared two-thread
  process-wide host-fs operation executor; each source close independently
  rejects new work even while object clones remain allocated, without
  multiplying worker threads or disrupting other live source instances. Shared
  worker handles and route claims are scoped to the same concrete module/socket
  binding, preventing stale runtime state from leaking into an unrelated test
  or deployment instance. The full-workspace gate also terminates its own
  process tree with bounded diagnostics at 512 threads, preventing panic-path
  thread leaks from exhausting host PID or memory capacity; both guard
  self-tests pass.
- [x] Root and fs-meta Vibespec validators pass with source contracts anchored
  to real black-box/system assertions.
- [x] Source crates pass format, `clippy -D warnings`, unit, contract,
  serialization, concurrency, fault, bound-route, and native integration tests.
- [x] Fustor local and cross-repository gates pass after the expanded source
  matrix is included.
- [x] The full workspace `--all-targets` gate passes under the two-CPU and
  process-tree limits; its QUIC socket-buffer preflight raises low host ceilings
  only for the bounded run and restores the exact prior values on every exit.
- [x] Disposable source acceptance passes from clean temporary state with real
  PostgreSQL, MySQL, Elasticsearch, MinIO, Local FS, CSV, JSONL, and
  Source-to-DataView routes.
- [x] fs-meta container staging `run all` passes from clean temporary state.
  The blocking 2026-07-30 run ended with
  `CONTAINER_STAGING_GATE_PASS suite=all`; active-three peaked at 222 threads
  and 15 processes, mini peaked at 413 threads and 22 processes, and both stayed
  below the 512-thread ceiling. Both disposable NFS environments completed
  normal-exit cleanup, restored the exact prior socket-buffer and inotify
  ceilings, and left the official `198.51.100.144-146` service untouched. Evidence:
  `/tmp/fsmeta-container-staging-gate-all-20260730073447.log`.
- [x] README, research status, specs, source wire, tests, and TODO describe the
  same completed connector behavior and no unchecked source item remains. The
  final consistency pass covers all eight Source families, generic source wire
  receipts, Fustor-owned normalizers, Source-to-DataView system tests, fs-meta
  disposable staging, and the caller/Capanix ownership boundary.
