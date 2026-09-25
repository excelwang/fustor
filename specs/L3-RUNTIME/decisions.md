---
version: 0.5.0
---

# L3: Fustor Runtime Decisions

## [decision] SourceReceiptAuthorityBoundary

**Rationale**: A caller can transport source facts but cannot be their authority.

- SourceRuntimeAdapter issues source revision, native anchors, evidence,
  observation time, continuation state, grant epoch, and actual usage in one
  receipt whose payload and receipt digests bind the complete payload and every
  copied receipt fact.
- Callers may add policy and orchestration context only in the generic Fustor
  submission; they cannot replace or widen source-issued receipt fields.
- Source packages remain independent of DataView types; Fustor-owned adapters
  validate and translate the source-neutral receipt.
- Legacy source payloads without a valid receipt are diagnostic compatibility
  data only and are not eligible for new DataView materialization.

## [decision] SourceCapabilityHonesty

**Rationale**: Unsupported incremental or consistency behavior must be explicit rather than approximated by a misleading implementation.

- PostgreSQL and MySQL transactional snapshots keep one real native transaction
  across continuation pages.
- Elasticsearch snapshot uses one PIT boundary; Elasticsearch and S3 expose
  bounded ordered poll continuations where their configured authority can do so.
- Local FS, CSV, and JSONL are snapshot-only unless a future source-native
  ordered change capability is explicitly specified and tested.
- fs-meta retains its authoritative journal, optional materialized projection,
  and tracked-result streaming semantics.
- SourceRuntimeAdapter performs no hidden path fallback and reports unsupported
  capabilities through a machine-readable capability report and typed errors.

## [decision] AuthorityHierarchy

**Rationale**: Source, projection, and DataView revisions prove different facts and must not compete for one truth role.

- Native source state and source revision remain source-authoritative.
- SourceProjection revision proves projection application only.
- DataView revision proves accepted Fustor association state only.
- No higher layer may promote its revision into a lower layer's truth authority.

## [decision] SourceProjectionOptionality

**Rationale**: Source-local acceleration must improve cost or latency without becoming a mandatory semantic layer.

- A SourceProjection is optional per source and per operation.
- An action or subscription declares `direct` or `projection-backed` before execution.
- Fustor never silently fan-outs, switches paths, or falls back after failure.
- A multi-source materialization is not a SourceProjection; it is a DataView-derived result with explicit dependencies.

## [decision] DataViewDemandBoundary

**Rationale**: Demand materialization is lost if administrative or streaming paths can silently become whole-source ingestion.

- New DataView results require either an accepted one-shot bounded source action
  or an accepted generic declared-result submission under a previously issued
  bounded admission context.
- Streaming subscriptions maintain only previously materialized tracked results.
- Whole-source CDC, unsolicited typed batches, and unbound manifest seeds are not public DataView write contracts.
- Bootstrap and migration use explicitly bounded source actions or declared-result
  admission contexts subject to the same evidence gate.

## [decision] GraphSourceAndDataViewSeparation

**Rationale**: A graph-shaped source and DataView's association graph have
different authorities, mutations, and revisions.

- A configured graph service implements an ordinary read-only SourceProvider;
  its Domain graph remains source state.
- Graph observation does not imply DataView admission, and declared-result
  admission does not require graph observation.
- Provider schema, cache, load, mutation, activation, and promotion operations
  remain outside Fustor's graph source and DataView APIs.
- A Domain graph revision, source observation receipt, and DataView revision are
  never aliases for one another.

## [decision] ResultNormalizationBoundary

**Rationale**: Heterogeneous source support must not couple DataView core or callers to every source payload type.

- Source, SourceProjection, and declared-result producers emit schema-tagged complete results or manifests, not DataView graph assertions.
- Fustor owns source-specific normalizers behind a registry.
- DataView store and commit algorithms consume only normalized commits.
- A new source schema extends the normalizer registry without adding a public DataView request variant.

## [decision] ExternalOrchestrationBoundary

**Rationale**: Durable DataView and high-frequency streaming must remain independent of optional short-lived workflow runtimes.

- Fustor embeds no external orchestration or experience-graph runtime.
- Any caller may wrap Fustor public commands in its own adapter or workflow.
- Such an adapter transports requests and responses only; it does not validate, normalize, merge, emit authoritative deltas, or retain DataView payloads.
- Fustor never invokes the adapter for recovery or streaming events.

## [decision] PullOnlyCallerVisibility

**Rationale**: Fustor remains passive when callers observe current state through normal reads rather than application callbacks.

- A changed DataView head is observed through a later read, validation, or projection request.
- Fustor may expose generic status or delta references, but does not push application-specific session updates.
- Stale caller revisions are handled by deterministic safe merge or typed revalidation requirements.

## [decision] OpaqueRevisionBoundary

**Rationale**: Revision references are needed for consistency and replay, but
public sequencing would leak deployment-global activity.

- Public `DataViewRef` values carry an opaque non-enumerable `revision_id`.
- Fustor may use internal sequence, parent, or commit-order fields for storage,
  concurrency, and deterministic merge; those fields are not caller-visible.
- A revision ref does not grant visibility and is always rechecked against the
  current projection context.
- Unknown, unauthorized, evicted, and compliance-redacted refs are externally indistinguishable.

## [decision] HistoricalProjectionBoundary

**Rationale**: Reproducible data-state reads must not become authorization time
travel or another mutable DataView branch.

- `latest` projection is `current-actionable`; exact revision projection is
  always `historical-replay`.
- Historical projection pins data state but uses current policy, grants,
  purpose, disclosure constraints, and freshness requirements.
- Scoped graph, coverage, and frontier are recomputed; Fustor does not persist
  or restore a past caller-specific `ScopedDataView` response.
- Historical frontier entries are evidence for audit/comparison only. Fustor
  issues no executable context and accepts no action, stream, or branch update from them.
- Projection performs no source execution and does not mutate the current head.

## [decision] CapanixCarrierBoundary

**Rationale**: Generic transport carriers must not acquire Fustor domain ownership.

- Capanix carries generic event metadata, auth evidence, grants, deadlines, correlation, routes, and transport.
- Fustor defines and validates all SourceProjection, action, subscription, DataView, evidence, freshness, and projection semantics.

## [decision] DiscoveryOperationExtensibility

**Rationale**: A stable core operation vocabulary should expand only for demonstrated cross-domain requirements.

- A single source or product example is insufficient to add a core discovery operation variant.
- Require at least two heterogeneous domains that cannot express the behavior through existing bounded actions, schemas, normalizers, and expansion rules.
- Prefer a namespaced capability descriptor with generic risk, side-effect, limit, continuation, schema, and evidence fields.
- Adding a capability must not require a new upper-layer lifecycle operation.
