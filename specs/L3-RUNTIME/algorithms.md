---
version: 0.5.0
---

# L3: Fustor Runtime Algorithms

## [algorithm] BoundedSourceExecution

**Rationale**: A source request must remain within its declared grant, deadline, and resource envelope before its result can be trusted or materialized.

```text
SourceExecutionEnvelope + SourceConfiguration + CurrentGrants + SourceDriver
  -> SourceResponseEnvelope | TypedSourceError
```

1. Decode and validate the source wire version, opaque request id, issued time,
   deadline, positive hard limits, and source-native request.
2. Resolve the configured authority object, reject credential-bearing endpoint
   URIs, require the operation's runtime-grant interface, and intersect product
   scope with the current active runtime grant before resolving credentials or
   invoking a driver.
3. Reject a stale grant epoch, inactive route, unsupported operation, malformed
   cursor, or requested page limit wider than the execution envelope.
4. Execute exactly the selected source-native operation; do not silently switch
   to another source, projection, snapshot, poll, or stream path.
5. Before each real native request, reserve and consume its call and cost units,
   including additional pages and required terminal cleanup; reject before the
   request when the remaining allowance is insufficient. Bound continuation
   work, check the remaining deadline and byte allowance inside scan/read loops,
   reject filesystem traversal and symlink escapes before reading target
   metadata, and classify timeout, throttling, access, invalid request,
   unavailable, and peer failures without exposing secret material.
6. Build the complete typed payload, source revision, anchors, evidence,
   continuation state, and actual usage from the observed source result.
7. Encode the response envelope and reject it instead of returning a partial
   materializable payload when any hard limit is exceeded.

## [algorithm] SourceSessionLifecycle

**Rationale**: Stateful snapshots and point-in-time handles must not survive authority, scope, grant, or lifecycle changes.

```text
SourceSessionEvent + SourceSessionRegistry + CurrentAuthorityState
  -> SourceSessionRegistry + SessionOutcome
```

1. Bind each native session to source kind, authority object ref, granted scope,
   grant epoch, native snapshot revision, and expiry in a fixed-capacity
   registry; close a rejected candidate before reporting capacity exhaustion.
2. Validate every continuation cursor against the bound session before a native
   read.
3. Close and remove the session after a terminal page or explicit close.
4. Actively close expired sessions at their deadline even when no later request
   arrives, and invalidate and close sessions on grant/config replacement,
   route deactivation, runtime shutdown, or native continuation failure.
5. Treat restart-lost, already-closed, expired, and mismatched sessions as typed
   non-disclosing unavailable results; never reopen them under the same cursor.

## [algorithm] SourceObservationReceiptConstruction

**Rationale**: The receipt must be deterministic for the observed source result and impossible for a caller to widen without detection.

```text
SourceExecutionContext + EncodedSourcePayload + NativeObservation
  -> SourceObservationReceipt + EncodedSourceResponse
```

1. Derive source-native anchors and a nonempty revision token from the actual
   source observation or native cursor/watermark boundary. Preserve an explicit
   source-native revision exactly; use a deterministic bounded-observation
   revision only when the source exposes no stronger token.
2. Bind evidence to source kind, authority object ref, operation, payload digest,
   grant epoch, observation time, continuation state, and the exact source-issued
   continuation digest.
3. Record actual rows, encoded bytes, native calls, fanout, latency, and cost
   after the source operation and before returning the response; zero is valid
   only when no native request occurred.
4. Compute a receipt digest over request identity and every receipt fact, encode
   until `actual_bytes` equals the complete response envelope length, recompute
   the fixed-length digest after each accounting change, and then return the
   immutable bytes.
5. Fustor rejects any receipt whose payload or receipt digest, request id,
   authority, revision, anchors, evidence, grant epoch, observation time,
   continuation, or actual accounting disagrees with the submitted result.

## [algorithm] OneShotResultCommit

**Rationale**: Complete one-shot results must pass one deterministic gate and commit atomically.

```text
ActionResultSubmission + DataViewState -> DataViewCommitOutcome + DataViewState
```

1. Authenticate the generic ingress context without assigning caller-product semantics.
2. Resolve `ActionExecutionContext` by action, execution, expiry, and idempotency key.
3. Verify action contract digest, policy context digest, grant epoch, and opaque base revision id.
4. Verify producer identity, source authority, native anchors, source revision, evidence, freshness, schema, continuation, and actual limits.
5. Require valid `ProjectionApplyEvidence` only when the action path is projection-backed; reject projection evidence on a direct path.
6. Resolve the result normalizer by schema ref and version.
7. Normalize the complete payload into payload attachment, identities, graph assertions, relationships, dependencies, and normalization evidence.
8. Check current head. Safely merge a stale-base submission only when its affected identities and dependencies are disjoint from intervening commits.
9. Atomically append payload, evidence, version, invalidation, dependency, revision, and graph-delta records and advance or recompute the logical head.
10. Mark the execution idempotency key consumed only as part of the successful atomic commit.

Any failure before step 9 leaves the head and append-only records unchanged.

## [algorithm] DeclaredExternalResultCommit

**Rationale**: A result produced outside a Fustor source action must enter
DataView without forged source evidence, unbounded ingestion, or caller-authored
graph state.

```text
DeclaredResultSubmission + DeclaredResultAdmissionState + DataViewState
  -> DataViewCommitOutcome + DataViewState
```

1. Authenticate the generic ingress context and resolve the exact unexpired
   `DeclaredResultAdmissionContext` by admission id and idempotency key.
2. Verify admission-contract, policy-context, grant-epoch, producer, base-revision,
   schema/version, external-ref policy, manifest-byte, and external-size bounds.
3. Require one complete `ExternalResultManifest`; reject source-action fields,
   source receipts, native anchors, DataView identities, or graph assertions.
4. Verify the manifest's external ref, revision, digest, declared size, producer,
   lineage, evidence, validation, policy, freshness, and retention according to
   the configured admission capability without copying referenced product bytes.
5. Resolve the result normalizer by manifest schema and version, then derive the
   bounded manifest attachment, canonical identities, relationships,
   dependencies, and normalization evidence.
6. Apply the same stale-base safe-merge and deterministic-head rules as one-shot
   source results.
7. Atomically append all accepted records and consume the idempotency key; a
   retry of the identical submission returns the original outcome, while a
   conflicting reuse fails without mutation.

Any rejection leaves the head and append-only records unchanged.

## [algorithm] StreamingRefreshCommit

**Rationale**: Streaming maintenance must preserve ordering and boundedness while refreshing only tracked results.

```text
StreamingRefreshSubmission + StreamingSubscription + DataViewState
  -> DataViewCommitOutcome + StreamingSubscription + DataViewState
```

1. Resolve an active, unexpired `StreamingSubscription` and verify policy and grant epochs.
2. Verify batch idempotency, sequence continuity, previous watermark, schema, path, rate, batch, byte, and in-flight limits.
3. For projection-backed subscriptions, require projection evidence whose source revision, projection revision, applied change id, and coverage advance the declared path without a gap.
4. Resolve every refresh target by `tracked_result_ref` before inspecting payload semantics.
5. Ignore no unknown target: reject the batch atomically when any entry is untracked so the response cannot reveal which source object was new.
6. Normalize refresh payloads, invalidations, and deletions through the registered normalizer.
7. Append accepted versions and invalidations atomically, advance the DataView head, and then advance subscription sequence and watermark in the same commit boundary.
8. Treat an exact duplicate batch as idempotent success with the original outcome.
9. Pause and require revalidation on watermark gap, projection discontinuity, unsupported schema, expired lease, grant change, or scope drift.

## [algorithm] DeterministicHeadSelection

**Rationale**: Identical accepted history must produce an identical logical head regardless of arrival interleaving.

```text
AppendOnlyDataViewHistory -> DataViewHead
```

1. Remove versions made non-actionable by authoritative invalidation, grant mismatch, compliance deletion, or missing required dependency evidence.
2. Order remaining source observations by source authority and source revision semantics.
3. Apply freshness and confidence only after authority and invalidation.
4. Keep inferred or derived versions below compatible observed versions unless an explicit derivation policy states otherwise.
5. Preserve all conflicting evidence versions even when one version is selected for the current head.
6. Produce the same head from the same append-only records regardless of arrival interleaving.

## [algorithm] DependencyInvalidation

**Rationale**: Stale source evidence must make dependent derived payloads non-actionable without erasing audit history.

```text
Invalidation + DependencyGraph + DataViewState -> GraphDelta + DataViewState
```

1. Mark an invalidated source payload non-actionable.
2. Traverse only recorded dependency refs under a bounded work budget.
3. Downgrade dependent derived payloads to non-actionable or requires-revalidation.
4. Emit a revisioned graph delta without deleting audit evidence.
5. Schedule later bounded retention independently from invalidation.

## [algorithm] ScopedDataViewProjection

**Rationale**: Authorization and disclosure filtering must occur before any externally visible field is derived.

```text
ScopedDataViewProjectionRequest + DataViewState
  -> ScopedDataViewProjectionOutcome
```

1. Resolve `latest` to the current head or `exact(DataViewRef)` to one retained
   immutable revision of the same logical DataView.
2. For malformed, unknown, unauthorized, evicted, or compliance-redacted exact
   refs, return `revision-not-available` without exposing which condition applied.
3. Freeze the selected immutable DataView revision snapshot. Mark latest as
   `current-actionable` and exact as `historical-replay`.
4. Apply current tenant, principal, purpose, grants, policy, disclosure budget,
   discovery budget, and freshness filters; never restore historical grants.
5. Remove denied payloads, nodes, relationships, evidence refs, continuations,
   counts, and revision details before computing external fields.
6. Preserve visible relationship semantics and allowed lineage with
   non-disclosing redaction state for denied contributors.
7. Compute `complete-for-scope`, `partial`, or `unknown` from authorized declared
   scope and observable coverage evidence only.
8. Recompute private discovery and the frontier from the selected snapshot plus
   the supplied intent, scope, and budgets. Derive each action id from its
   visible logical anchor and complete immutable contract, register an identical
   contract idempotently, and retain an older contract under its prior id when
   freshness or another contract field changes. Include continuation ids only
   when the same action is already visible in the returned frontier.
9. For `historical-replay`, retain the frontier for audit but do not issue
   `ActionExecutionContext`, start source work, spend source-execution budget, or
   permit update/stream registration against the selected revision.
10. Return the ephemeral projection without changing the logical current head.

## [algorithm] RetentionAndCompliance

**Rationale**: Historical value, dependency safety, and deletion obligations require a deterministic lifecycle.

```text
RetentionPolicy + DataViewHistory -> RetentionDelta + DataViewHistory
```

1. Transition stale or invalidated payloads from actionable to historical before eviction.
2. Protect evidence or payload versions required by an actionable current or derived version.
3. Apply TTL, capacity, source, tenant, and dependency-reachability policy.
4. For compliance deletion, remove or redact protected payload and evidence and retain only the minimum permitted tombstone.
5. Remove derived indexes and projection entries that could re-expose deleted content.
6. Make exact refs whose required snapshot content is no longer retained resolve
   through the generic `revision-not-available` projection outcome.
