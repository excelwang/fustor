---
version: 0.6.1
---

# L2: Fustor Architecture

## ARCHITECTURE.INTENT

Fustor realizes one passive data chain:

```text
SourceProvider -> SourceProjection? ---------+
                                           ResultNormalizerRegistry -> DataView
DeclaredResultAdmissionRegistry -------------+                 |
                                policy + budget + freshness -> ScopedDataView
```

`SourceProjection` is an optional source-local acceleration role. `DataView` is
the deployment-global admitted-result association state. Private discovery and
scoped projection form a control/access plane rather than another durable data
layer.

## ARCHITECTURE.COMPONENTS

### Components.PassiveSourceInspection

**Component**: Generic one-shot source inspection outside DataView and private discovery.
- Input: proposed or current native manifest configuration, explicit versioned execution envelope and source request; no business registry prerequisite.
- Output: configuration validation/capability facts or native source-family SourceResponseEnvelope with validated receipt or typed failure, plus exact consumed configuration revision where reported and session-close-on-exit notice.
- Boundary: Reuses services and native drivers, not test transport. No workflow, transform, loader, admission, persistent DataView, deployment or application-specific configuration.
- Covers: CONTRACTS.OPERATIONS.ONE_SHOT_INSPECTION, CONTRACTS.INSPECTION.NATIVE_INSPECTION_PARITY.

Caller-owned adapters may validate new connection configuration before registering
it and later compare saved configuration with native consumption evidence. The
optional source-application configuration tool owns native source declarations
and their revisions, not caller business registries. It neither persists caller
instructions nor hosts authentication or cross-application transactions. Validation establishes
configuration validity only, not provider availability, ingestion or DataView
materialization. A configuration digest is neither source revision nor grant proof.
This boundary implements CONTRACTS.BOUNDARY.CALLER_AGNOSTIC_TOOLING and
EXTERNAL_ORCHESTRATION_DECOUPLING without adding a workflow component.

### Components.NativeSourceConfiguration

**Component**: Owner-native source configuration tooling outside the data/view core.
- Input: Explicit owner scope, installed source capability policy, source declaration and expected revision.
- Output: Validated configuration, immutable version and operation receipt, or typed refusal.
- Boundary: No HTTP host, provider access/provisioning, workflow, instruction history or DataView mutation. Native validation and native scope checking remain in the existing source inspector.
- Covers: CONTRACTS.NATIVE_CONFIGURATION.NATIVE_SOURCE_CONFIGURATION.

One declaration may expose multiple named, independently validated scopes over
one connection. Native configuration and grants remain authoritative; a scoped
operation is not a new source connection. An owner-installed endpoint/status
association preserves revision inspection during registration without allowing
caller-controlled URLs or cross-target health evidence.

### Components.PassiveBudgetEstimator

**Component**: Caller-neutral deterministic source-kit budget estimator.
- Input: defaults, hard-bound intersection, comparable receipt usage and per-field overrides.
- Output: byte/latency allocations with mode, basis, sample count and capped-demand explanation.
- Boundary: No history store, source execution, retry, scheduling, permission grant or query selection.
- Covers: CONTRACTS.OPERATIONS.PASSIVE_BUDGET_ESTIMATE.

### Components.SourceProvider

#### SourceProvider

**Component**: Exposes one source authority domain through bounded operations and schema-tagged results.

- Input: source configuration, current grants, a versioned source-neutral execution envelope, or source-owned change state.
- Output: source capabilities and versioned source response envelopes containing complete bounded results plus source-issued observation receipts with native anchors, source-native or source-observation revisions, evidence, continuations, and actual native-call resource accounting.
- Boundary: Does not know DataView identities, graph relations, scoped projections, caller type, or orchestration runtime.

### Components.SourceRuntimeAdapter

PostgreSQL catalog inspection belongs to SourceProvider: it returns bounded native
metadata and identity, not a business inventory or a judgment of data completeness.
The operation has the same grant and receipt boundary as other source reads and
does not enter DataView without a separate explicit submission. This implements
CONTRACTS.INSPECTION.CATALOG_METADATA.

#### SourceRuntimeAdapter

**Component**: Binds one source-native driver and lifecycle to app-neutral Capanix request/reply routes.

- Input: typed source request envelope, product configuration, local credential references, runtime grants, route lifecycle, and source-native cursor or session state.
- Output: typed source response envelope, machine-readable status/capability report, source-issued observation receipt, or typed non-disclosing error.
- Boundary: Does not import DataView, private-discovery, or upper-layer caller workflow types; it does not create graph identities, rewrite source authority facts, replace a supplied source-native revision with a payload digest, or leave unbounded native sessions active.

#### GraphSourceAdapter

**Component**: Configured SourceRuntimeAdapter specialization that exposes a graph query service or graph database as an ordinary bounded read-only source.

- Input: source-neutral execution envelope around a configured query id and bounded parameters, graph-source configuration, current grants, and hard result budgets.
- Output: schema-tagged tabular or record result plus source-issued graph-provider revision, native anchors, evidence, continuation, and actual usage in the ordinary source response envelope.
- Boundary: Does not expose raw unbounded query text or any load, mutation, schema, cache, administration, activation, or promotion operation; does not depend on DataView admission or treat the Domain graph as DataView storage.

### Components.SourceProjection

#### SourceProjection

**Component**: Optionally maintains a source-local, rebuildable query projection for one source authority domain.

- Input: source observations or source change events under source-owned ordering and grant rules.
- Output: schema-tagged projection results and `ProjectionApplyEvidence` carrying source revision, projection revision, coverage, freshness, and applied change identity.
- Boundary: Does not replace source authority, span multiple source authority domains, mint global identities, or become mandatory for direct actions.

### Components.BoundedActionRegistry

#### BoundedActionRegistry

**Component**: Records one-shot actions exposed by private discovery and the execution context required to submit their results.

- Input: visible source capabilities, policy, freshness, task scope, discovery
  budget, and a `current-actionable` scoped revision.
- Output: `BoundedSourceAction` and `ActionExecutionContext` records.
- Boundary: Does not execute the source action or select an action for a caller,
  and does not issue executable contexts from `historical-replay` projections.

### Components.StreamingSubscriptionRegistry

#### StreamingSubscriptionRegistry

**Component**: Maintains renewable bounded subscriptions that refresh existing DataView results from source changes.

- Input: tracked DataView result refs, explicit direct or projection-backed path, source/projection identity, schemas, limits, policy, grants, and lease duration.
- Output: `StreamingSubscription` state and typed active, paused, expired, revoked, or revalidation-required status.
- Boundary: Does not discover or enroll a new source object implicitly.

### Components.ResultNormalizerRegistry

#### ResultNormalizerRegistry

**Component**: Converts schema-tagged source/projection results or declared external-result manifests into canonical DataView payload attachments and graph assertions.

- Input: accepted candidate submission, its source/projection provenance or declared-result admission evidence, schema ref, version, encoding, and complete bounded payload or manifest.
- Output: canonical identities, properties, relationships, dependencies, payload attachment, and normalization evidence.
- Boundary: Schema-specific adapters live behind this registry; DataView core does not depend on source, graph-backend, product, workflow, or analysis types.

### Components.DeclaredResultAdmissionRegistry

#### DeclaredResultAdmissionRegistry

**Component**: Issues bounded contexts for caller-declared external results independently of source actions.

- Input: configured admission capability, producer identity, allowed schema/version, external-ref policy, manifest/byte limits, current grants and policy, freshness/retention bounds, current DataView head, and idempotency identity.
- Output: `DeclaredResultAdmissionContext`, exact current capability or retained commit outcome on distinct read-only identity lookups, or typed non-disclosing refusal.
- Boundary: Does not execute a source or workflow, fetch product bytes, infer lineage, accept graph assertions, or fabricate source receipts and revisions.

### Components.DataViewCommitGate

#### DataViewCommitGate

**Component**: Validates one-shot and streaming submissions and atomically commits accepted normalized results.

- Input: `ActionResultSubmission`, `DeclaredResultSubmission`, or `StreamingRefreshSubmission`, current head, matching action/admission/subscription state, and registered normalizer.
- Output: `DataViewCommitOutcome`, append-only records, and an optional graph-delta ref.
- Boundary: Does not execute sources, call upper layers, or partially mutate state on rejection.

### Components.DataViewStore

#### DataViewStore

**Component**: Owns complete bounded payload versions, global identities, graph facts, relationships, evidence, dependencies, invalidations, opaque revision identities, retention records, an empty genesis revision, and one logical current head.

- Input: atomically accepted normalized commits and maintenance decisions.
- Output: opaque `DataViewRef` values, immutable retained revision snapshots,
  graph deltas, and internal history required for deterministic head selection.
- Boundary: Is admitted result and association state, not source-native or external-product truth, an exhaustive catalog, product-byte store, or existence oracle.

### Components.ScopedDataViewProjector

#### ScopedDataViewProjector

**Component**: Builds a non-persisted security-scoped projection over the latest
or one exact retained DataView revision.

- Input: `DataViewRevisionSelector`, immutable revision snapshot, tenant,
  principal, purpose, grants, policy, disclosure budget, discovery budget, and
  freshness requirements.
- Output: `ScopedDataView` with `current-actionable` or `historical-replay` mode,
  visible payloads and relationships, coverage state, limiting reasons, and
  visible continuation action ids.
- Boundary: Filters before exposing payload, relationship, evidence, count, revision, error, trace, or continuation information.

### Components.PrivateDiscoveryService

#### PrivateDiscoveryService

**Component**: Produces visible source possibilities and a breadth-first bounded action frontier.

- Input: caller intent, policy context, discovery scope, source capabilities, current scoped DataView evidence, freshness, and budget.
- Output: visible fragments, source authority records, `ScopedDiscoveryGraph`, `DiscoveryFrontier`, discovery budget, and non-disclosing trace.
- Boundary: Does not directly execute source runtimes, own caller sessions, or persist another DataView.

### Components.RetentionController

#### RetentionController

**Component**: Transitions stale observations to non-actionable history and applies bounded eviction, redaction, and compliance deletion.

- Input: payload lifecycle, dependencies, TTL, capacity, source policy, tenant policy, and compliance decisions.
- Output: invalidations, retained history, evictions, redactions, and minimum permitted tombstones.
- Boundary: Does not leave actionable derived data without required evidence or re-expose removed information through projections or indexes.

## ARCHITECTURE.INTERACTIONS

1. A caller obtains a scoped projection and one-shot `BoundedSourceAction` values from Fustor.
2. The caller or a caller-selected executor performs the source action outside Fustor and submits the result with its `ActionExecutionContext`.
3. `DataViewCommitGate` validates the submission, selects a normalizer, and commits all accepted records atomically to `DataViewStore`.
4. A direct action carries source evidence only. A projection-backed action additionally carries `ProjectionApplyEvidence`; missing or stale projection evidence fails closed.
5. A streaming producer submits micro-batches directly to Fustor under `StreamingSubscription`; no external orchestration runtime is in the event path.
6. A stream refresh may update, invalidate, or delete only tracked result refs. New source objects may update a SourceProjection but cannot enter DataView through the subscription.
7. A caller observes streaming head changes only by requesting a current `DataViewRef` or new scoped projection. Fustor never calls the caller.
8. A caller may request an exact previously issued opaque `DataViewRef`; Fustor
   loads the retained immutable revision, reapplies current scope, and returns a
   `historical-replay` projection without changing the current head.
9. Private discovery may recompute the historical scoped graph, coverage, and
   frontier for comparison, but BoundedActionRegistry does not issue executable
   action contexts from that projection.
10. A caller may separately request a bounded `DeclaredResultAdmissionContext`
    and submit one complete external-result manifest. This path does not require
    or manufacture a source action or receipt; the same normalizer and atomic
    DataView commit boundary derives all DataView facts.
11. GraphSourceAdapter remains on the source side of the architecture. A caller
    may observe a Domain graph without admitting any result, or admit a declared
    external result without using a graph source; configuration does not couple
    the two paths.

## ARCHITECTURE.OWNERSHIP

- SourceProvider exposes source-native execution, source revisions, source-owned permission and capability facts, anchors, and source evidence from one underlying source authority domain.
- SourceProjection owns only source-local projection lifecycle, revision, coverage, and apply evidence.
- ResultNormalizerRegistry owns source-schema-to-DataView conversion policy.
- DataViewStore owns global identities, accepted payload history, relationship semantics, revisions, and head selection.
- PrivateDiscoveryService and ScopedDataViewProjector own bounded disclosure,
  current-actionable versus historical-replay projection mode, and action
  visibility and effective DataView actionability, not durable global content.
- External callers own intent interpretation, action selection, source execution coordination, session behavior, and conclusions.
- SourceRuntimeAdapter owns source-native connection, timeout, cursor/session, and receipt construction while preserving the SourceProvider contract.
- GraphSourceAdapter owns only configured bounded read translation and graph-provider revision evidence as a peer SourceProvider.
- DeclaredResultAdmissionRegistry owns bounded external-result admission contexts; it owns no external result bytes or producer workflow.
- Capanix owns generic carriers only.

## ARCHITECTURE.DEPENDENCY_DIRECTION

```text
source contracts ------> optional projection contracts --+
       |                                                |
       +-------- action result submission --------------+
                                                        v
declared-result context --> external manifest --> Fustor normalization and DataView
                         |
                         v
              scoped projection and discovery
```

Source and SourceProjection contracts do not depend on DataView storage types.
DataView core does not depend on source product crates. Caller adapters may
depend on Fustor public contracts; Fustor does not depend on those adapters.
