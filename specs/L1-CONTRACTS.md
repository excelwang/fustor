---
version: 0.6.2
---

# L1: Fustor Contracts

## CONTRACTS.NATIVE_CONFIGURATION

- **NATIVE_SOURCE_CONFIGURATION**: System MUST validate, save and deactivate native source connection declarations inside an explicitly configured owner scope, preserving unknown native fields and immutable prior configurations. Saves MUST revalidate expected revisions, reject path/symlink escape and unsupported credentials or targets, and return owner-native receipts. Registration MUST NOT access source data, deploy providers, issue grants or mutate DataView. The caller owns authentication; source configuration has no Agent, workflow or solution-instance semantics.
  A declaration MAY select multiple named object scopes on the same connection, each independently validated by the native inspector. Existing single-scope declarations MUST remain compatible. Provider status endpoints MUST remain owner-pinned to the selected physical endpoint; callers MUST NOT supply arbitrary status URLs or silently retain a previous endpoint's status identity.
  > Covers L0: VISION.NATIVE_SOURCE_CONFIGURATION
  > Responsibility: Source application configuration tooling, outside DataView and private-discovery.
  > Verification: Native public-tool tests prove registration, revision conflicts, deactivation, retained native bytes, forbidden fields and zero provider I/O; failed validation leaves the active configuration unchanged.

## CONTRACTS.INSPECTION

- **CATALOG_METADATA**: System MUST expose bounded PostgreSQL catalog inspection under the intersection of installed product scope, current grants and native visibility. It MUST use a read-only transaction, report actual cluster/database identity, object kinds and explicitly estimated row counts, and return a native receipt. Views and unknown statistics MUST NOT imply exact entity counts. Caller SQL, payload scans and writes are forbidden; exceeding the declared object bound MUST fail explicitly without returning a complete inventory.
  > Covers L0: VISION.NATIVE_INSPECTION_PARITY, VISION.DATA_REALIZATION.SOURCE_AUTHORITY
  > Responsibility: Generic PostgreSQL SourceProvider, independent of business classification and checking schedules.
  > Verification: Public native-provider tests prove scoped objects, native identity, null unknown/view estimates, bound refusal, unchanged provider data and source-issued receipt integrity.

- **NATIVE_INSPECTION_PARITY**: System MUST expose capability-aware configuration validation and bounded native inspection for relational, search, filesystem, object and graph sources, preserve source-native payloads and verified receipts, and reject unsupported operations rather than silently substitute another capability. Proposed configuration validation MUST require neither a pre-registered business instance nor source I/O and MUST NOT claim provider readiness or grant new authority. Structured file/object samples MUST enforce source grants, version evidence and byte/record/deadline bounds before returning complete records. A graph status revision response MUST consume the same request byte budget and MAY exceed 64 KiB up to the smaller of the remaining budget and a 1 MiB driver ceiling; exhaustion MUST fail explicitly before any graph query result is claimed.
  > Covers L0: VISION.NATIVE_INSPECTION_PARITY
  > Responsibility: Source-native tooling; no caller-specific workflow or deployment behavior.
  > Verification: Public CLI tests cover all native kinds, proposed configuration before registry creation, no source I/O during validation, capability reporting, invalid grants/configuration, structured reads, exact consumed configuration revision distinct from source revision, version drift and closed one-shot sessions. Agent-first AF-02 is separate caller-integration acceptance, not implied by existing tool tests.

## CONTRACTS.OPERATIONS

- **ONE_SHOT_INSPECTION**: System MUST provide a generic one-shot source inspector that reuses native configuration, grant validation, drivers and SourceResponseEnvelope receipts; it MUST reject unbounded requests and close native sessions on exit without materializing a DataView or running transformations.
  > Covers L0: VISION.ONE_SHOT_INSPECTION
  > Responsibility: Source-owned passive inspection.
  > Verification: Public executable tests validate real bounded JSONL reads, PG session closure, grant/scope/budget errors and native receipt integrity.
- **PASSIVE_BUDGET_ESTIMATE**: System MUST passively estimate only latency and byte allocations from supplied comparable receipt usage, keep declared row/call/fanout/cost axes unchanged, intersect all hard bounds, use native defaults under ten successes and 1.5x nearest-rank P95 thereafter, and apply explicit failure floors without retries or authority expansion.
  > Covers L0: VISION.PASSIVE_BUDGET_ESTIMATE
  > Responsibility: Caller-neutral source budget estimation.
  > Verification: Public estimator tests cover 24h/100-access retention, scope/revision grouping, valid usage, exhaustion, overrides and cap insufficiency.

## CONTRACTS.SOURCE

- **HETEROGENEOUS_SOURCE_RESULTS**: System MUST represent every source-produced result with a stable source identity, schema reference, bounded payload, native anchors, source revision, evidence, grant epoch, observation time, continuation, and actual resource accounting.
  > Responsibility: Source-provider contracts and shared source result vocabulary.
  > Verification: Source contract tests cover PostgreSQL, MySQL, S3, Elasticsearch, local files, CSV, JSONL, and FS/NFS result families without requiring DataView source-specific request variants.
  > Covers L0: VISION.DATA_REALIZATION.HETEROGENEOUS_SOURCE_RESULTS

- **SOURCE_AUTHORITY**: System MUST preserve the underlying source authority domain as the unique authority for current source-native state, source revision, source-owned permission and capability facts, fingerprints, executable source operations, and source revalidation evidence; System MUST treat those permission facts as inputs rather than as the final effective DataView authorization decision.
  > Responsibility: Source-provider and source-authority contracts preserve source-owned facts; scoped projection and commit policy own effective DataView visibility and actionability decisions.
  > Verification: Tests require native anchors, source revisions, source-owned permission/capability facts, grant epochs, freshness guards, and source-owned revalidation operations, then prove that current grants, tenant/principal/purpose policy, disclosure limits, and budgets can further restrict DataView visibility and actionability.
  > Covers L0: VISION.DATA_REALIZATION.SOURCE_AUTHORITY

- **BOUNDED_SOURCE_ACTIONS**: System MUST describe every interactive source operation as a `BoundedSourceAction` before execution, MUST make `action_id` identify the complete immutable action contract, and MUST carry its permission, freshness, rows, bytes, calls, fanout, latency, cost, continuation, side-effect, and failure limits unchanged through result submission. Recomputing an identical contract MUST preserve the same id, while changing freshness, authority, observation path, operation, permission, any hard limit, continuation, side effect, failure mode, expansion rule, or information-gain semantics MUST produce a different id without invalidating an already issued context.
  > Responsibility: Private discovery and bounded-action registry.
  > Verification: Tests cover action compilation, identical re-projection idempotency, freshness-driven contract replacement under a distinct id, coexistence with the previously registered action, and rejection when submitted actual usage exceeds any declared hard limit.
  > Covers L0: VISION.BOUNDED_INTERACTION.BOUNDED_SOURCE_ACTIONS

- **EVIDENCE_AND_PROVENANCE**: System MUST keep source authority, concrete native anchors, source revision, evidence, observed time, grant epoch, policy labels, freshness, schema, producer context, and derivation metadata attached to every accepted source result.
  > Responsibility: Source result contract and DataView commit gate.
  > Verification: Serialization and commit tests require every field and reject incomplete provenance without mutation.
  > Covers L0: VISION.DATA_GOVERNANCE.EVIDENCE_AND_PROVENANCE

- **SOURCE_EXECUTION_ENVELOPE**: System MUST transport each materializable source request in a versioned source-neutral execution envelope that carries an opaque request id, grant epoch, deadline, and hard row, byte, call, fanout, latency, and cost limits; a SourceProvider MUST account each real native request at the driver boundary and MUST fail closed before starting work that would exceed the envelope or current source grant.
  > Responsibility: Shared source-provider contract and each source runtime adapter.
  > Verification: Contract tests exercise every limit independently, expired deadlines, stale grant epochs, malformed envelopes, multi-page native calls, terminal cleanup reservation, and pre-call overspend rejection without introducing DataView types into source packages.
  > Covers L0: VISION.DATA_REALIZATION.SOURCE_CONNECTOR_COMPLETENESS, VISION.BOUNDED_INTERACTION.BOUNDED_SOURCE_ACTIONS

- **SOURCE_OBSERVATION_RECEIPT**: System MUST return every successful materializable source response in a versioned source-neutral envelope containing a source-issued receipt with source kind, authority object ref, operation, source revision, native anchors, evidence refs, observation time, grant epoch, continuation state, actual row, byte, call, fanout, latency, and cost accounting, plus deterministic payload and receipt integrity digests covering those facts; when the source payload supplies a nonempty source-native revision token the receipt MUST preserve that token exactly and MUST keep it independent from payload integrity; Fustor MUST copy the concrete authority object ref, source revision, and continuation from the verified receipt into `ActionResultSubmission`, and callers MUST NOT manufacture or widen these facts.
  > Responsibility: SourceProvider response boundary and Fustor-owned source-result adapter.
  > Verification: Round-trip and tamper tests cover all source families, require exact native-revision preservation, receipt-to-payload and receipt-to-submission consistency, and reject empty native revisions, legacy payloads, or incomplete materialization receipts atomically.
  > Covers L0: VISION.DATA_REALIZATION.SOURCE_AUTHORITY, VISION.DATA_REALIZATION.SOURCE_CONNECTOR_COMPLETENESS, VISION.DATA_GOVERNANCE.EVIDENCE_AND_PROVENANCE

- **SOURCE_SESSION_LIFECYCLE**: System MUST keep stateful source snapshots bound to one authority object, scope, grant epoch, source-native revision, and expiry in a capacity-bounded registry; terminal pages, explicit close, active expiry without a follow-up request, grant or configuration change, route deactivation, native continuation failure, and runtime shutdown MUST close or invalidate the native session; stale or mismatched cursors and capacity overflow MUST fail closed without leaking a native session.
  > Responsibility: Stateful source runtime adapters for transactional or point-in-time reads.
  > Verification: PostgreSQL, MySQL, and Elasticsearch tests cover same-revision continuation, terminal and explicit close, idle active expiry, registry capacity overflow, restart loss, scope mismatch, grant change, and failure cleanup with native close counts.
  > Covers L0: VISION.DATA_REALIZATION.SOURCE_CONNECTOR_COMPLETENESS, VISION.DATA_REALIZATION.SOURCE_AUTHORITY

- **SOURCE_INCREMENTAL_CONTINUITY**: System MUST expose source-native incremental poll or stream only where the source can provide an ordered continuation or watermark, MUST preserve tie-breaking and non-duplication semantics across pages, and MUST report unsupported incremental capability explicitly rather than simulating it with an unbounded snapshot.
  > Responsibility: Incremental-capable SourceProviders and source capability reporting.
  > Verification: Elasticsearch and S3 poll tests and fs-meta stream tests cover ordered continuation, equal-timestamp tie breaking, replay idempotency, bounded scanning, and explicit unsupported capability for snapshot-only sources.
  > Covers L0: VISION.DATA_REALIZATION.SOURCE_CONNECTOR_COMPLETENESS, VISION.DATA_REALIZATION.REALTIME_EXISTING_RESULT_FRESHNESS

- **SOURCE_CREDENTIAL_AND_SCOPE_ISOLATION**: System MUST keep secret material outside manifests, endpoint URIs, and response payloads, resolve only declared local credential references, require the source operation's declared runtime-grant interface, intersect product scope with current runtime grants, and reject unknown, inactive, widened, cross-object, missing-interface, or stale-grant requests before invoking a source driver. Registration and endpoint rebinding MUST preserve installed authenticated-transport requirements; invalid peer identity or trust MUST fail without plaintext fallback.
  > Responsibility: Shared source configuration contract and each source runtime adapter.
  > Verification: Every source family has fail-closed credential, credential-bearing URI, duplicate-ref, unknown-grant, inactive-grant, missing-read-interface, scope-widening, out-of-scope, and no-driver-call tests. Configured MySQL TLS survives registration/rebinding; real secure-provider reads accept the installed CA and reject wrong CA/peer identity without weakening server transport policy.
  > Covers L0: VISION.DATA_REALIZATION.SOURCE_AUTHORITY, VISION.BOUNDED_INTERACTION.EXISTENCE_NON_COMMITMENT

- **SOURCE_CONNECTOR_PARITY**: System MUST provide status, capability discovery, bounded schema or field discovery, bounded snapshot or point lookup, continuation, typed errors, source-issued receipts, and Fustor-owned normalization for PostgreSQL, MySQL, Elasticsearch, S3, Local FS, CSV, JSONL, and fs-meta, with incremental behavior enabled only for source families that satisfy `SOURCE_INCREMENTAL_CONTINUITY`.
  > Responsibility: SourceProvider implementations and ResultNormalizerRegistry.
  > Verification: One capability matrix and black-box contract suite prove every required cell or an explicit source-native not-supported result; every materializable schema is registered in the Fustor normalizer registry, and disposable native bound-route responses for network sources plus native temporary-file responses reach DataView only through that registry.
  > Covers L0: VISION.DATA_REALIZATION.HETEROGENEOUS_SOURCE_RESULTS, VISION.DATA_REALIZATION.SOURCE_CONNECTOR_COMPLETENESS

- **GRAPH_SOURCE_PARITY**: System MUST allow a configured graph query service or graph database to implement the same SourceProvider boundary as any other source, using source-neutral envelopes, configured bounded read operation ids, hard budgets, typed errors, and source-issued revision evidence. Query transport and optional revision/status transport MAY bind different deployment-owned credential references; neither credential value is part of the request or observation. Its Fustor-facing capability surface MUST be read-only and MUST NOT expose raw unbounded query text, schema upload, cache refresh, loading, mutation, administration, activation, or promotion. A Domain graph revision MUST remain source state and MUST NOT be treated as a DataView revision.
  > Responsibility: Generic graph SourceProvider adapter and source capability policy.
  > Verification: Contract fixtures bind two graph backends through configuration to the same bounded read request and result schema, reject undeclared query ids and every write/admin operation before a driver call, preserve provider revision evidence, and contain no DataView admission dependency.
  > Covers L0: VISION.DATA_REALIZATION.GRAPH_SOURCE_PARITY, VISION.DATA_REALIZATION.SOURCE_AUTHORITY, VISION.BOUNDED_INTERACTION.BOUNDED_SOURCE_ACTIONS

- **NATIVE_READ_AUTHORIZATION**: System MUST enforce graph object grants, current epoch, approved read-only query ids, parameter bounds and hard budgets independently of backend account privileges. Backend read-only identities MAY add defense in depth, but a configuration assertion MUST NOT be reported as verified provider permission or become a prerequisite for native bounded reads; absent provider proof MUST remain unverified.
  > Responsibility: Native graph source authorization; deployment protects credentials and direct provider access.
  > Verification: Real native reads succeed under authorized grants with an unverified backend identity; unauthorized objects, query ids, writes, stale epochs and budget overflow fail closed. Provider capability disclosures remain distinct from Fustor enforcement.
  > Covers L0: VISION.DATA_REALIZATION.GRAPH_SOURCE_PARITY, VISION.DATA_REALIZATION.SOURCE_AUTHORITY, VISION.BOUNDED_INTERACTION.BOUNDED_SOURCE_ACTIONS

## CONTRACTS.SOURCE_PROJECTION

- **OPTIONAL_SOURCE_ACCELERATION**: System MAY use a `SourceProjection` for a source or operation, but MUST preserve a valid direct path whenever the selected bounded action declares direct source observation.
  > Responsibility: Source-projection registry and action path selection.
  > Verification: Architecture and workflow tests cover sources with no projection, direct actions beside an existing projection, and projection-backed actions.
  > Covers L0: VISION.DATA_REALIZATION.OPTIONAL_SOURCE_ACCELERATION

- **PROJECTION_SEMANTIC_CONTINUITY**: System MUST make direct and projection-backed results preserve the same source-native meaning while distinguishing source revision, projection revision, projection coverage, freshness, and provenance.
  > Responsibility: SourceProjection result contract and Fustor result normalizers.
  > Verification: Equivalent direct and projection-backed fixtures normalize to compatible graph assertions while retaining different observation paths.
  > Covers L0: VISION.DATA_REALIZATION.PROJECTION_SEMANTIC_CONTINUITY

- **PROJECTION_AUTHORITY_SEPARATION**: System MUST treat `SourceProjection` as source-derived, rebuildable observation state and MUST NOT let projection revision or readability replace source revision or source authority.
  > Responsibility: SourceProjection contracts and DataView acceptance policy.
  > Verification: Specs and tests reject projection-only authority claims and preserve the native source anchor through projection-backed submissions.
  > Covers L0: VISION.DATA_REALIZATION.SOURCE_AUTHORITY, VISION.NON_GOALS.NO_MANDATORY_SOURCE_PROJECTION

- **PROJECTION_PATH_EXPLICITNESS**: System MUST bind each one-shot execution or streaming subscription to exactly one declared observation path, `direct` or `projection-backed`, and MUST NOT silently fan out, fall back, or switch paths.
  > Responsibility: Bounded-action and streaming-subscription contracts.
  > Verification: Tests reject missing projection evidence on projection-backed paths and reject implicit direct fallback when a projection path is unavailable.
  > Covers L0: VISION.DATA_REALIZATION.PROJECTION_SEMANTIC_CONTINUITY

## CONTRACTS.DATA_VIEW

- **GLOBAL_DATA_VIEW**: System MUST maintain one deployment-global logical `DataView` shared across tasks, backed by append-only payload-version, evidence, revision, invalidation, dependency, and graph-delta records.
  > Responsibility: DataView store.
  > Verification: Specs and tests require an empty genesis revision, one logical current head, and reproducible revision history across independent task updates.
  > Covers L0: VISION.DATA_REALIZATION.GLOBAL_DATA_VIEW

- **DEMAND_MATERIALIZATION**: System MUST add a new DataView result only through either an accepted `ActionResultSubmission` bound to a previously issued `BoundedSourceAction` or an accepted `DeclaredResultSubmission` bound to a previously issued `DeclaredResultAdmissionContext`. Initialization or import MUST use one of those explicit bounded demands rather than an unrestricted ingest bypass.
  > Responsibility: Bounded-action registry, declared-result admission registry, and DataView commit gate.
  > Verification: Tests reject unsolicited payloads, unbound source batches, undeclared external manifests, and whole-source ingest requests while accepting both independently authorized bounded branches.
  > Covers L0: VISION.DATA_REALIZATION.DEMAND_MATERIALIZATION, VISION.NON_GOALS.NO_UNBOUNDED_INGESTION

- **DECLARED_EXTERNAL_RESULT_ADMISSION**: System MUST expose one caller-agnostic `DeclaredResultSubmission` for a complete bounded external-result manifest whose schema, external ref, revision, digest, size, producer, lineage, evidence, validation, policy, freshness, retention, base DataView revision, and idempotency identity are bound by a current `DeclaredResultAdmissionContext`. This branch MUST NOT require or fabricate a `BoundedSourceAction`, source receipt, native source anchors, or source revision. Fustor MUST select a configured normalizer, validate the whole manifest atomically, persist only the bounded manifest and normalized DataView facts rather than referenced product bytes, and leave the head unchanged on rejection.
  System MUST support read-only lookup of the original committed admission outcome under the same installed producer/policy/grant identity, without issuing an admission, resubmitting a manifest, registering an action, creating a revision alias or initializing missing state. Unknown keys and changed or unauthorized policies MUST disclose no outcome; a historic accepted outcome MUST NOT imply current visibility or authorize another write.
  System MUST allow exact-identity read-only inspection of the current owner-installed admission capability independently of an earlier result. This inspection MUST issue no context, reserve no write, mutate no state and grant no external-source privilege. A missing capability or producer/schema mismatch MUST disclose no policy; execution MUST still obtain and validate its own current admission context.
  > Responsibility: Declared-result admission registry, ResultNormalizerRegistry, and DataView commit gate.
  > Verification: Tests admit two heterogeneous external-result schemas through the same generic request shape, reject unknown schema, stale context, digest/ref/size/evidence mismatch, caller-authored graph assertions, oversize manifest, and duplicate idempotency conflicts atomically, and prove referenced table or graph bytes remain outside DataView.
  > Covers L0: VISION.DATA_REALIZATION.DECLARED_EXTERNAL_RESULT, VISION.DATA_REALIZATION.DEMAND_MATERIALIZATION, VISION.DATA_GOVERNANCE.EVIDENCE_AND_PROVENANCE, VISION.NON_GOALS.NO_SOURCE_SPECIFIC_DATA_VIEW_API, VISION.NON_GOALS.NO_UNBOUNDED_INGESTION

- **RESULT_NORMALIZATION_OWNERSHIP**: System MUST select a Fustor-owned result normalizer by accepted result schema and version, preserve the complete bounded source payload or declared external-result manifest, and derive canonical DataView identities and graph assertions before commit.
  > Responsibility: Result-normalizer registry.
  > Verification: Tests reject unknown schemas atomically and show that DataView core contracts contain no source-specific write variant.
  > Covers L0: VISION.DATA_REALIZATION.HETEROGENEOUS_SOURCE_RESULTS, VISION.NON_GOALS.NO_SOURCE_SPECIFIC_DATA_VIEW_API

- **FULL_BOUNDED_RESULT_MATERIALIZATION**: System MUST retain the complete accepted result carrier: either the bounded-action payload with action identity, producer context, source authority, anchors, revisions, schema, evidence, policy, grants, freshness, derivation, continuation, and actual size/cost accounting, or the bounded declared-result manifest with admission identity, producer, external ref and revision, digest, size, schema, lineage, validation, policy, freshness, retention, and derivation.
  > Responsibility: DataView store.
  > Verification: Round-trip tests preserve complete PostgreSQL, S3, and Elasticsearch bounded payload examples without expanding beyond the originating action limits and preserve complete external table and graph-result manifests without copying referenced bytes.
  > Covers L0: VISION.DATA_REALIZATION.GLOBAL_DATA_VIEW, VISION.DATA_GOVERNANCE.EVIDENCE_AND_PROVENANCE

- **EVIDENCE_GATED_ATOMIC_COMMIT**: System MUST validate the selected admission branch, its action or declared-result context, authority or producer evidence, policy, schema, limits, budget, base revision, freshness, and idempotency before committing, and any rejection MUST leave the logical head unchanged.
  > Responsibility: DataView commit gate.
  > Verification: Failure-injection tests compare head and append-only records before and after every rejection class.
  > Covers L0: VISION.DATA_GOVERNANCE.EVIDENCE_AND_PROVENANCE, VISION.DATA_GOVERNANCE.CONCURRENT_REVISION_SAFETY

- **CONCURRENT_REVISION_SAFETY**: System MUST safely merge disjoint stale-base submissions, retain separate versions for conflicting observations, forbid blind last-write-wins, and select the current head deterministically from authority, revision, invalidation, freshness, confidence, and derivation state.
  > Responsibility: DataView revision and head-selection algorithms.
  > Verification: Concurrent tests cover disjoint merge, conflict retention, duplicate submission, and reproducible head selection.
  > Covers L0: VISION.DATA_GOVERNANCE.CONCURRENT_REVISION_SAFETY

- **OPAQUE_REVISION_IDENTITY**: System MUST expose every DataView revision through an opaque, non-enumerable `revision_id`. The identifier MUST NOT encode or disclose deployment-global commit order, revision count, tenant activity, hidden source activity, or authorization, although Fustor MAY retain internal ordering for storage and merge algorithms.
  > Responsibility: DataView public reference contract and store.
  > Verification: Serialization and security tests treat revision ids as opaque values, reject public numeric sequencing assumptions, and prove that possessing a ref does not bypass scoped authorization.
  > Covers L0: VISION.DATA_GOVERNANCE.OPAQUE_REVISION_IDENTITY, VISION.BOUNDED_INTERACTION.EXISTENCE_NON_COMMITMENT

- **REVISION_PINNED_PROJECTION**: System MUST accept `latest` or an exact previously issued `DataViewRef` for scoped projection. Exact projection MUST freeze the retained immutable data revision, reapply current tenant, principal, purpose, grants, policy, disclosure budget, discovery budget, and freshness constraints, recompute the scoped graph, coverage, and frontier, mark the result `historical-replay`, and perform no source execution or head mutation. Unknown, unauthorized, evicted, or compliance-redacted revisions MUST collapse to `revision-not-available`.
  > Responsibility: DataView store and ScopedDataView projector.
  > Verification: Projection tests cover deterministic exact-revision reads, current-policy redaction, replay-only frontier, no source calls, no budget spend for source execution, no head mutation, and retention-aware non-disclosure.
  > Covers L0: VISION.DATA_GOVERNANCE.REVISION_PINNED_PROJECTION, VISION.DATA_GOVERNANCE.RETENTION_AND_COMPLIANCE

- **REALTIME_EXISTING_RESULT_FRESHNESS**: System MUST allow a valid `StreamingSubscription` to refresh, invalidate, or delete only DataView results that were already tracked when the subscription scope was established; an event for an untracked source object MUST NOT create a DataView identity, payload, relationship, count, or existence signal.
  > Responsibility: Streaming-subscription registry and DataView commit gate.
  > Verification: Stream tests update tracked results and prove that new source objects remain absent from DataView and external coverage signals.
  > Covers L0: VISION.DATA_REALIZATION.REALTIME_EXISTING_RESULT_FRESHNESS, VISION.DATA_REALIZATION.DEMAND_MATERIALIZATION

- **STREAMING_SUBSCRIPTION_BOUNDARY**: System MUST keep `StreamingSubscription` separate from `BoundedSourceAction` and bind it to tracked result refs, one explicit observation path, schema versions, sequence/watermark state, rate, batch, byte, concurrency, lease-expiry, policy, and grant limits.
  > Responsibility: Streaming-subscription registry.
  > Verification: Tests reject expired leases, scope widening, unsupported schemas, over-limit batches, duplicate sequence effects, watermark gaps, and path switching.
  > Covers L0: VISION.DATA_REALIZATION.REALTIME_EXISTING_RESULT_FRESHNESS, VISION.NON_GOALS.NO_UNBOUNDED_INGESTION

- **RELATIONSHIP_EVIDENCE_PRESERVATION**: System MUST preserve edge kind, authority refs, evidence refs or explicit derivation rule, confidence, observed revision, freshness, and invalidation state for every materialized or projected relationship.
  > Responsibility: DataView graph records and scoped projector.
  > Verification: Relationship tests retain visible lineage while withholding denied contributors and evidence.
  > Covers L0: VISION.DATA_GOVERNANCE.EVIDENCE_AND_PROVENANCE

- **RETENTION_AND_COMPLIANCE**: System MUST make stale or invalidated payloads non-actionable before TTL, capacity, source-policy, tenant-policy, dependency, redaction, or compliance deletion, and MUST retain only policy-permitted tombstones.
  > Responsibility: DataView retention controller.
  > Verification: Tests cover dependency-safe eviction, redaction, deletion, tombstone disclosure, and non-reappearance through indexes or projections.
  > Covers L0: VISION.DATA_GOVERNANCE.RETENTION_AND_COMPLIANCE

## CONTRACTS.PRIVATE_DISCOVERY

- **PRIVATE_DATA_DISCOVERY**: System MUST derive visible source possibilities and bounded next actions from caller intent, policy, source capabilities, DataView evidence, freshness, and budgets without requiring deep traversal of every source.
  > Responsibility: Private-discovery service.
  > Verification: Tests produce useful fragments and actions before any high-cost source detail operation.
  > Covers L0: VISION.BOUNDED_INTERACTION.PRIVATE_DATA_DISCOVERY

- **SCOPED_DATA_VIEW**: System MUST return a non-persisted `ScopedDataView` over exactly one selected immutable DataView revision after filtering tenant, principal, purpose, grants, policy, disclosure budget, discovery budget, and freshness. The projection MUST state whether it is `current-actionable` or `historical-replay`.
  > Responsibility: ScopedDataView projector.
  > Verification: Projection tests forbid mixed revisions and cross-tenant, cross-principal, cross-purpose, grant, budget, and freshness leakage.
  > Covers L0: VISION.BOUNDED_INTERACTION.SCOPED_DATA_VIEW

- **EXISTENCE_NON_COMMITMENT**: System MUST normalize unauthorized-existing, invisible-existing, filtered-existing, and nonexistent resources to the same externally visible class and MUST withhold hidden names, counts, relations, revisions, errors, traces, and useful timing distinctions.
  > Responsibility: Private discovery and scoped projection boundary.
  > Verification: Security tests compare all hidden and missing classes across payload, error, trace, count, relation, revision, and timing surfaces.
  > Covers L0: VISION.BOUNDED_INTERACTION.EXISTENCE_NON_COMMITMENT

- **BUDGETED_BREADTH_FIRST_DISCOVERY**: System MUST rank authorized bounded actions breadth-first across source families under remaining time, calls, rows, bytes, fanout, cost, and optional token budgets before selective deepening.
  > Responsibility: Discovery-frontier scheduler.
  > Verification: 5-minute, 30-minute, and 2-hour budgets produce distinct valid frontiers without bypassing authority or freshness.
  > Covers L0: VISION.BOUNDED_INTERACTION.BUDGETED_BREADTH_FIRST_DISCOVERY

- **COVERAGE_STATE**: System MUST classify each scoped projection as `complete-for-scope`, `partial`, or `unknown` using only authorized observable coverage evidence, and continuation MUST reference only already-visible bounded actions.
  > Responsibility: ScopedDataView coverage computation.
  > Verification: Tests prevent deployment-global completeness claims and hidden continuation references.
  > Covers L0: VISION.BOUNDED_INTERACTION.SCOPED_DATA_VIEW, VISION.BOUNDED_INTERACTION.EXISTENCE_NON_COMMITMENT

## CONTRACTS.BOUNDARY

- **CALLER_AGNOSTIC_TOOLING**: System MUST keep all Fustor domain APIs independent of caller type and MUST NOT depend on upper-layer session, planning, callback, route, or CLI/repl models. Caller authentication, instruction-to-operation history and business configuration inventory MUST remain outside these domain APIs; generic native request ids and receipts MUST NOT acquire mandatory Agent/package/Trace fields.
  > Responsibility: Fustor public contracts and dependency policy.
  > Verification: Specs and dependency gates forbid named upper-layer application or agent-runtime ownership in Fustor reusable contracts.
  > Covers L0: VISION.BOUNDARY.CALLER_AGNOSTIC_TOOLING

- **EXTERNAL_ORCHESTRATION_DECOUPLING**: System MUST NOT embed, start, or require an external orchestration or experience-graph runtime; such a runtime MAY invoke Fustor through a thin caller-owned adapter over the same public API used by any other caller.
  > Responsibility: Fustor runtime boundary.
  > Verification: Dependency and spec tests require no orchestration-runtime package, process, primitive, or session semantics in Fustor core.
  > Covers L0: VISION.BOUNDARY.EXTERNAL_ORCHESTRATION_DECOUPLING

- **PULL_BASED_REPROJECTION**: System MUST expose current DataView references and
  latest or exact read-only projections on request and MUST NOT call upper-layer
  callers when the head changes, a stream pauses, or revalidation is required.
  > Responsibility: DataView and scoped-projection public boundary.
  > Verification: Workflow tests require callers to read or reproject the current head and forbid application-specific callback contracts.
  > Covers L0: VISION.NON_GOALS.NO_UPPER_LAYER_CALLBACK

- **CAPANIX_NEUTRALITY**: System MUST treat Capanix as an app-neutral context, deadline, grant, route-evidence, event, and transport carrier and MUST NOT assign Fustor domain meaning to Capanix.
  > Responsibility: Fustor/Capanix integration boundary.
  > Verification: Cross-repository gates forbid source, projection, DataView, discovery, authority, or freshness ownership in Capanix specs.
  > Covers L0: VISION.BOUNDARY.CAPANIX_NEUTRALITY

- **LOCAL_ACCEPTANCE**: System MUST provide local and cross-repository acceptance gates that record official stable services as not touched.
  > Responsibility: Fustor validation scripts.
  > Verification: Gates emit `official_stable=not_touched` for `198.51.100.144-146`.
  > Covers L0: VISION.BOUNDARY.LOCAL_ACCEPTANCE

- **DISPOSABLE_SOURCE_ACCEPTANCE**: System MUST provide one blocking local source-acceptance gate that starts disposable PostgreSQL, MySQL, Elasticsearch, and S3-compatible services plus temporary Local FS, CSV, and JSONL fixtures, executes real source drivers through Capanix bound routes into Fustor-owned normalization and DataView commit, runs fs-meta through its disposable container staging gate, and cleans up all processes, containers, credentials, and data on success or failure.
  > Responsibility: Repository-owned source acceptance tooling.
  > Verification: The gate emits per-source and final pass/fail sentinels, runs previously environment-gated native tests without `ignore`, records cleanup evidence, and records `official_stable=not_touched hosts=198.51.100.144,198.51.100.145,198.51.100.146`.
  > Covers L0: VISION.BOUNDARY.LOCAL_ACCEPTANCE, VISION.BOUNDARY.REAL_SOURCE_ACCEPTANCE

## CONTRACTS.NON_GOALS

- **NO_MANDATORY_SOURCE_PROJECTION**: System MUST NOT require a SourceProjection for every source, source instance, or action.
  > Responsibility: Source and projection architecture boundary.
  > Verification: Direct-only source fixtures remain valid.
  > Covers L0: VISION.NON_GOALS.NO_MANDATORY_SOURCE_PROJECTION

- **NO_UNBOUNDED_INGESTION**: System MUST NOT expose an unrestricted public ingest operation, accept an external-result manifest without a current bounded admission context, or permit a streaming subscription to discover new DataView objects.
  > Responsibility: DataView public write boundary.
  > Verification: Spec and API tests reject unbound typed batches, whole-source CDC, and untracked stream objects.
  > Covers L0: VISION.NON_GOALS.NO_UNBOUNDED_INGESTION

- **NO_SOURCE_SPECIFIC_DATA_VIEW_API**: System MUST NOT add source-, graph-backend-, product-, workflow-, or analysis-specific DataView write variants when `ActionResultSubmission` or the generic `DeclaredResultSubmission` plus a registered normalizer can express the behavior.
  > Responsibility: DataView API governance.
  > Verification: Specs forbid source-specific ingest variant names and require a heterogeneous-schema admission test.
  > Covers L0: VISION.NON_GOALS.NO_SOURCE_SPECIFIC_DATA_VIEW_API

- **NO_UPPER_LAYER_CALLBACK**: System MUST NOT call a planner, agent, session, or application callback from DataView, SourceProjection, stream, or recovery components.
  > Responsibility: Fustor component call direction.
  > Verification: Specs and dependency tests require output/status surfaces instead of upward component calls.
  > Covers L0: VISION.NON_GOALS.NO_UPPER_LAYER_CALLBACK

- **NO_SOURCE_RUNTIME_ORCHESTRATION**: System MUST NOT make private discovery, DataView storage, or result normalization directly execute source runtimes; callers or source-owned streaming producers supply results through declared contracts.
  > Responsibility: Source execution boundary.
  > Verification: Tests and dependency rules prevent source driver invocation from DataView and private-discovery components.
  > Covers L0: VISION.NON_GOALS.NO_SOURCE_RUNTIME_ORCHESTRATION

- **NO_HISTORICAL_BRANCH_EXECUTION**: System MUST NOT issue executable action contexts from a `historical-replay` projection, accept action or streaming submissions against it as a branch, restore data hidden by current policy, or create another mutable DataView head.
  > Responsibility: ScopedDataView projector, bounded-action registry, and DataView commit gate.
  > Verification: Tests reject historical action issuance and submissions, preserve the current head byte-for-byte/logically, and prove current revocation overrides historical visibility.
  > Covers L0: VISION.NON_GOALS.NO_HISTORICAL_BRANCH_EXECUTION
