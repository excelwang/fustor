---
version: 0.5.0
---

# L3: Fustor Runtime Workflows

## [workflow] SourceConnectorOneShotExecution

**Purpose**: Execute one bounded source-native operation and preserve its authoritative receipt through DataView commit.

**Steps**:

1. Caller converts the selected `BoundedSourceAction` and current grant into a
   source-neutral `SourceExecutionEnvelope` around one typed source request.
2. Capanix transports the envelope without interpreting Fustor or source-domain
   semantics.
3. SourceRuntimeAdapter validates scope and limits, executes the native driver,
   and returns a typed payload with `SourceObservationReceipt`.
4. A Fustor-owned source-result adapter validates the complete source response
   and constructs the generic `ActionResultSubmission` from the receipt plus the
   previously issued `ActionExecutionContext`.
5. DataView normalizes and commits through the existing one-shot evidence gate.

## [workflow] DisposableSourceAcceptance

**Purpose**: Prove every connector against disposable real dependencies without touching official services.

**Steps**:

1. Create isolated temporary state, credentials, source fixtures, and a private
   container network; start PostgreSQL, MySQL, Elasticsearch, and an S3-compatible
   service with pinned images and explicit per-container CPU quotas whose
   effective aggregate is validated before startup and whose runtime quota is
   verified after startup.
2. Create real Local FS, CSV, and JSONL fixtures and start only temporary Capanix
   route surfaces needed by the test.
3. Run every build and test command through the repository CPU guard and
   process-tree watchdog. For every source, run status and machine-readable capability discovery,
   probe, field/schema discovery, bounded paging, continuation or explicit
   unsupported incremental behavior, error/scope cases, source receipt
   validation, Fustor normalization, and DataView commit. The exact response
   bytes returned by the real native driver through the bound route are the
   bytes submitted to the Fustor adapter and DataView gate; a separate fake
   driver test is not equivalent evidence.
4. Run fs-meta through the repository-owned disposable container staging gate.
5. Emit per-source and final sentinels only from commands whose execution and
   watchdog evidence is present, record official stable as not touched,
   and clean all containers, processes, credentials, volumes, and temporary data
   on success or failure.

## [workflow] DirectOneShotMaterialization

**Purpose**: Materialize a direct source result through a caller-agnostic Fustor commit.

**Steps**:

1. Caller requests a scoped projection and receives a direct `BoundedSourceAction` plus `ActionExecutionContext`.
2. Caller selects and executes the action through any source-capable execution surface outside Fustor.
3. Caller submits the complete `ActionResultSubmission` without graph assertions.
4. Fustor validates, normalizes, commits, and returns `DataViewCommitOutcome`.
5. Caller requests a new `ScopedDataView` at the returned head when it needs current visible content.

## [workflow] GraphSourceReadOnlyObservation

**Purpose**: Observe a graph query service as an ordinary bounded data source
without coupling its Domain graph to DataView or exposing administration.

**Steps**:

1. Caller selects a configured graph SourceProvider capability and a declared
   `configured_query_id`; arbitrary backend query text is not accepted.
2. Fustor issues the ordinary source-neutral envelope with current grant,
   deadline, record, byte, call, fanout, latency, and cost bounds.
3. GraphSourceAdapter maps the query id and bounded parameters to one read-only
   provider operation and rejects undeclared, mutation, schema, cache, load,
   administration, activation, or promotion behavior before a driver call.
4. The adapter returns a schema-tagged bounded result and ordinary
   SourceObservationReceipt carrying provider revision evidence.
5. The caller may use the observation without DataView admission or submit it
   through the ordinary action-result path; no graph-specific admission path is
   introduced.

## [workflow] DeclaredExternalResultAdmission

**Purpose**: Admit one bounded manifest for an externally stored result without
pretending that Fustor executed or observed the producing workflow.

**Steps**:

1. Caller requests a `DeclaredResultAdmissionContext` for a configured producer,
   schema/version, ref policy, manifest/product size bounds, grants, policy,
   freshness, retention, base DataView revision, and idempotency identity.
2. Fustor issues the context only when the current admission capability permits
   the exact request; it performs no source or workflow execution.
3. Caller submits one complete `DeclaredResultSubmission`. It includes the
   external manifest and derivation but no source receipt, source anchors,
   DataView identities, or graph assertions.
4. Fustor validates the whole manifest, resolves its generic schema normalizer,
   derives DataView facts, and commits atomically without copying referenced
   product bytes.
5. Fustor returns `DataViewCommitOutcome`; the caller explicitly requests a new
   scoped projection when it needs the visible result.

## [workflow] ProjectionBackedOneShotMaterialization

**Purpose**: Materialize a source result whose declared observation path uses one SourceProjection.

**Steps**:

1. Caller receives a projection-backed action naming one `SourceProjectionRef`.
2. SourceProjection serves or applies the source observation and returns `ProjectionApplyEvidence`.
3. Caller submits the complete result, source evidence, and projection evidence.
4. Fustor rejects missing, stale, mismatched, or discontinuous projection evidence without falling back to a direct path.
5. Accepted data follows the same normalization, commit, and scoped reprojection path as a direct result.

## [workflow] DirectStreamingRefresh

**Purpose**: Refresh existing DataView results directly from an ordered source stream.

**Steps**:

1. A bounded control request creates a direct `StreamingSubscription` over existing tracked result refs.
2. Source producer submits ordered micro-batches directly to Fustor under the lease.
3. Fustor refreshes, invalidates, or deletes only tracked results and advances subscription watermark atomically with the DataView commit.
4. Events for new source objects cannot materialize DataView content; discovery of those objects requires a new one-shot action.

## [workflow] ProjectionBackedStreamingRefresh

**Purpose**: Refresh existing DataView results only after a declared SourceProjection applies the source change.

**Steps**:

1. A bounded control request creates a projection-backed subscription naming one projection.
2. Source changes update SourceProjection according to source-local ordering.
3. SourceProjection emits apply evidence and a refresh batch for already tracked DataView results.
4. Fustor commits only after validating source and projection continuity and never implicitly switches to the direct path.
5. Projection may contain additional source objects, but those objects remain outside DataView until explicitly materialized.

## [workflow] PrivateDiscoveryAndScopedProjection

**Purpose**: Produce a non-disclosing scoped view and breadth-first action frontier.

**Steps**:

1. Caller supplies intent, policy context, scope, disclosure budget, discovery budget, and freshness requirements.
2. Caller selects `latest`; Fustor freezes the current DataView head and filters
   all graph content before exposure.
3. Private discovery combines visible DataView evidence with visible source capabilities.
4. Fustor returns a `current-actionable` `ScopedDataView`, coverage state, and
   breadth-first `DiscoveryFrontier` plus action execution contexts.
5. Caller alone decides whether and how to use a returned action.

## [workflow] ExactRevisionReadOnlyProjection

**Purpose**: Reproject a retained historical DataView data state for audit and
comparison without restoring old permissions or creating an executable branch.

**Steps**:

1. Caller supplies an exact previously issued opaque `DataViewRef` plus current
   intent, policy context, scope, disclosure budget, discovery budget, and
   freshness requirements.
2. Fustor resolves the retained immutable revision or returns the generic
   `revision-not-available` outcome.
3. Fustor applies current tenant, principal, purpose, grants, policy, disclosure,
   budget, and freshness filtering before deriving any external field.
4. Private discovery recomputes visible evidence, coverage, and the frontier
   against the historical data snapshot and supplied request.
5. Fustor returns a `historical-replay` `ScopedDataView`. Frontier entries remain
   visible for comparison, but no action execution context is issued.
6. Fustor performs no source call, streaming registration, result commit, or
   head mutation. The caller must request `latest` before any execution.

## [workflow] PullBasedHeadRefresh

**Purpose**: Let callers observe stream-driven head changes without Fustor callbacks.

**Steps**:

1. Source streaming may advance the global head independently of caller sessions.
2. Fustor emits no application callback and starts no external orchestration process.
3. On the next materialize, refresh, validate, snapshot, or projection request,
   caller supplies its last observed opaque revision id when relevant.
4. Fustor returns the current `DataViewRef`, a new scoped projection, or a non-disclosing revalidation requirement.

## [workflow] StreamPauseAndRecovery

**Purpose**: Fail closed on stream discontinuity and resume only through explicit bounded control.

**Steps**:

1. Fustor pauses a subscription on lease expiry, grant change, schema drift, sequence gap, watermark gap, projection discontinuity, or bounded-limit violation.
2. Fustor exposes typed status and residual evidence without calling an upper layer.
3. An external controller may renew, rebase, revoke, or repair the subscription through normal Fustor commands.
4. Refresh resumes only after the subscription is valid for the current tracked results and source/projection revision boundary.
