---
version: 0.5.2
---

# L3: Fustor Runtime Interfaces

## [interface] SourceExecutionEnvelope

**Rationale**: Source runtimes need one source-neutral bounded execution carrier without importing DataView or caller-specific types.

```text
SourceExecutionEnvelope<TRequest> {
  wire_version
  context {
    request_id
    grant_epoch
    issued_at
    deadline
    max_rows
    max_bytes
    max_calls
    max_fanout
    max_latency_ms
    max_cost_units
  }
  request: TRequest
}
```

`TRequest` remains source-native. The envelope carries no DataView identity,
graph assertion, planner/session identity, or source credential bytes. Probe and
status surfaces may use a separately bounded administrative context, but every
result eligible for DataView materialization requires this envelope.

## [interface] SourceCapabilityReport

**Rationale**: Callers must be able to distinguish real source-native behavior from unsupported behavior without probing by failure or parsing diagnostics text.

```text
SourceCapabilityReport {
  source_kind
  operations[]: probe | field-discovery | bounded-snapshot | point-lookup |
                configured-bounded-query | incremental-poll | ordered-stream |
                explicit-session-close
  consistency: stateless-page | stable-revision-cursor |
               transaction-snapshot | point-in-time
  continuation: none | cursor | native-session
  supports_status
}
```

The report is machine-readable and is returned with the source status surface.
An absent capability is unsupported; a connector cannot simulate it by silently
switching to another operation.

## [interface] GraphSourceRequest

**Rationale**: Graph engines are peer sources, but their observation surface must
remain bounded and read-only instead of becoming a graph administration API.

```text
GraphSourceRequest {
  configured_query_id
  parameters
  expected_result_schema_ref
  max_records
  max_bytes
  continuation?
}
```

The selected source configuration maps `configured_query_id` to backend-native
read behavior. The request never transports unrestricted backend query text.
Only queries declared read-only by configuration and capability policy are
eligible for execution; schema upload, refresh, load, mutation, administration,
activation, and promotion are not SourceProvider operations.

## [interface] SourceObservationReceipt

**Rationale**: Source authority, evidence, continuation, and actual usage must be issued by the SourceProvider rather than reconstructed by a caller.

```text
SourceResponseEnvelope<TPayload> {
  wire_version
  request_id
  payload: TPayload
  receipt? {
    source_kind
    authority_object_ref
    operation
    source_revision
    native_anchors
    evidence_refs
    observed_at
    grant_epoch
    continuation_state: terminal | continuation-available | session-closed
    continuation_digest?
    actual_rows
    actual_bytes
    actual_calls
    actual_fanout
    actual_latency_ms
    actual_cost_units
    payload_digest
    receipt_digest
  }
}
```

A successful materializable payload always has a receipt. Status and typed error
payloads carry no materialization receipt. Fustor-owned source-result adapters
validate the receipt against the complete encoded response and copy its facts
into `ActionResultSubmission`; they do not invent replacements. The payload
digest covers the exact typed payload bytes. If `TPayload` supplies a nonempty
source-native revision, `source_revision` is that exact source-issued token;
the payload digest is not a substitute for it. A source with no stronger native
token may issue a deterministic revision for its bounded observation. The
receipt digest covers request identity plus every receipt field other than the
digest itself, including the source revision, payload digest, continuation
digest, and actual accounting.

## [interface] BoundedSourceAction

**Rationale**: One-shot source work must be bounded and path-explicit before execution.

```text
BoundedSourceAction {
  action_id
  source_authority_ref
  observation_path: direct | projection-backed(SourceProjectionRef)
  operation
  permission_scope
  freshness_precondition
  max_rows
  max_bytes
  max_calls
  max_fanout
  max_latency_ms
  max_cost_units
  side_effect_level
  continuation_policy
  failure_modes
  expected_information_gain
  explanation
}
```

`BoundedSourceAction` is one-shot. Continuous source refresh uses
`StreamingSubscription`, not an action compatibility mode.
`action_id` is a deterministic content identity for the complete immutable
contract plus its visible logical action anchor. Re-projecting the same contract
reuses the id; changing any contract field emits a new id, so an outstanding
`ActionExecutionContext` continues to resolve the exact contract under which it
was issued.

## [interface] ActionExecutionContext

**Rationale**: A submitted result must be tied to the exact action, policy, grants, limits, and DataView revision observed before execution.

```text
ActionExecutionContext {
  action_id
  execution_id
  base_revision_id
  action_contract_digest
  policy_context_digest
  grant_epoch
  issued_at
  expires_at
  idempotency_key
}
```

The context is issued by Fustor before source execution and binds a later result
to the exact action, scope, policy, grants, limits, and observed DataView head.
Fustor issues this context only from a `current-actionable` projection.

## [interface] SourceProjectionEvidence

**Rationale**: Projection-backed observations need explicit projection continuity without replacing source authority.

```text
SourceProjectionRef {
  projection_id
  source_authority_ref
  projection_kind
}

ProjectionApplyEvidence {
  projection: SourceProjectionRef
  source_revision
  projection_revision
  applied_change_id
  coverage
  freshness
  observed_at
  evidence_refs
}
```

Projection evidence supplements but never replaces source-native anchors,
source revision, or source authority.

## [interface] ActionResultSubmission

**Rationale**: Any caller must be able to transport a complete source result through one caller-agnostic Fustor contract.

```text
ActionResultSubmission {
  submission_id
  execution: ActionExecutionContext
  source_authority_ref
  authority_object_ref
  producer_id
  producer_kind: source | source-projection
  projection_evidence?
  schema_ref
  schema_version
  encoding
  payload
  native_anchors
  source_revision
  evidence_refs
  observed_at
  policy_labels
  grant_epoch
  freshness
  derivation
  continuation?
  actual_rows
  actual_bytes
  actual_calls
  actual_fanout
  actual_latency_ms
  actual_cost_units
}
```

`authority_object_ref` is copied from the source-issued receipt and identifies
the concrete authority object resolved for the execution; it is not inferred
from `source_authority_ref` or supplied independently by the caller. `payload`
is the complete bounded source result. Public submissions do not carry
caller-authored DataView identities or graph assertions.

## [interface] DeclaredResultSubmission

**Rationale**: An externally produced result needs a generic admission path even
when no Fustor-issued source action produced it, while remaining bounded,
evidence-gated, and independent from any caller or workflow product.

The native Rust context embeds the revision-pinned `DeclaredResultPolicy` under
`admission.policy` (producer, result authority for disclosure, schema, external-ref
prefix, size caps, policy digest, grant epoch and required labels). Its digest,
base revision, issue/expiry and idempotency key are issued and durably registered
by DataViewEngine. Trusted owner configuration registers policies and manifest
normalizers; callers cannot register them over the query/command route. Public
`IssueDeclaredAdmission` and `SubmitDeclaredResult` are command-lane operations.
Manifest schema is a typed `{schema_ref, schema_version}` and timestamps are
integer epoch milliseconds. Existing source-action and query requests remain
unchanged. The reference conformance JSON spelling below is an external adapter
shape, not permission to fabricate source actions for the native implementation.

The query-lane `InspectDeclaredOutcome {profile_id, idempotency_key}` returns
`DeclaredOutcome(Option<DataViewCommitOutcome>)`. Its caller boundary supplies
trusted current owner policy registration; inspection requires an exact retained
admission policy, producer and idempotency key. It exposes no global listing and
does not issue, renew, submit, project, register or persist anything. Null is
unknown/unavailable, never proof that an external effect did not happen. A
returned historic outcome is neither a fresh grant nor current view visibility.

The separate query-lane `InspectDeclaredCapability {profile_id, producer_id,
schema}` returns `DeclaredCapability(Option<DeclaredResultCapability>)`, containing
the current DataView identity, exact owner-registered policy and its native policy
digest. It requires an exact producer/schema match and an installed normalizer;
unknown or mismatched identities return null, not a global policy listing. It
does not inspect historical outcomes, issue a context, select an idempotency key,
reserve a writer, or guarantee a later commit. The issuing/submission commands
still recheck the current policy. A policy change changes the returned digest.

`fustor-data-view-inspect declared-capability --config native.json` exposes that
same native query through bounded stdin. Its private installed configuration is
the trusted admission-policy boundary, not a caller-supplied grant. It opens the
existing DataView state read-only, does not take the writer lock and fails without
creating state when the native owner is uninitialized. An initialized empty view
is valid. This capability concerns admission of a bounded external manifest, not
provider reads/writes or current viewer visibility; the independent scoped-view
request and `declared-inspect` disclosure filters retain their existing behavior.

```text
DeclaredResultAdmissionContext {
  admission_id
  admission_contract_digest
  producer_id
  allowed_schema_ref
  allowed_schema_version
  allowed_external_ref_policy
  max_manifest_bytes
  max_external_size_bytes
  base_revision_id
  policy_context_digest
  grant_epoch
  issued_at
  expires_at
  idempotency_key
}

ExternalResultManifest {
  external_ref
  external_revision
  digest_algorithm
  digest
  size_bytes
  schema_ref
  schema_version
  media_type
  producer_id
  producer_version
  lineage_refs
  evidence_refs
  validation_refs
  observed_or_produced_at
  policy_labels
  freshness
  retention
}

DeclaredResultSubmission {
  submission_id
  admission: DeclaredResultAdmissionContext
  manifest: ExternalResultManifest
  derivation
}
```

The manifest is complete and bounded; referenced product bytes remain in their
owning store. The submission contains no `ActionExecutionContext`, source
receipt, native source anchors, caller-authored DataView identity, or graph
assertion. A registered normalizer derives all DataView identities and
relationships from the accepted manifest.

## [interface] StreamingSubscription

**Rationale**: Continuous refresh needs an independently bounded lease over already-materialized results.

```text
StreamingSubscription {
  subscription_id
  tracked_result_refs
  source_authority_ref
  observation_path: direct | projection-backed(SourceProjectionRef)
  allowed_schema_versions
  source_watermark
  max_events_per_batch
  max_bytes_per_batch
  max_batches_per_window
  batch_window_ms
  max_in_flight
  lease_issued_at
  lease_expires_at
  policy_context_digest
  grant_epoch
  status: active | paused | expired | revoked | requires-revalidation
}
```

Tracked result refs are resolved from already-materialized DataView state when
the subscription is created or explicitly rebased.

## [interface] StreamingRefreshSubmission

**Rationale**: Stream micro-batches need ordered, idempotent refresh semantics without discovering new DataView objects.

```text
StreamingRefreshSubmission {
  submission_id
  subscription_id
  batch_id
  sequence
  previous_watermark
  resulting_watermark
  schema_ref
  schema_version
  encoding
  refreshes[] {
    tracked_result_ref
    operation: refresh | invalidate | delete
    payload?
    native_anchors
    source_revision
    evidence_refs
    observed_at
  }
  projection_evidence?
  actual_bytes
  actual_events
}
```

An entry whose native identity is not already represented by a tracked result
ref is outside the subscription and cannot be committed or disclosed.

## [interface] DataViewCommitOutcome

**Rationale**: Callers need a typed, non-disclosing result for accepted, merged, stale, and rejected submissions.

```text
DataViewCommitOutcome {
  submission_id
  status: accepted | safe-merged | requires-revalidation | rejected
  data_view: DataViewRef
  delta_ref?
  conflict_version_refs
  residual_code?
  retryable
}

DataViewRef {
  data_view_id
  revision_id
}
```

Rejection and revalidation-required outcomes contain no hidden object, count,
relationship, source revision, or competing-version detail.
`revision_id` is opaque and non-enumerable. It is a reference, not an authority
token or a public commit sequence.

## [interface] DataViewRevisionProjection

**Rationale**: Callers need a passive way to choose the current data state or
reproject an exact retained revision without creating another view store.

```text
DataViewRevisionSelector = latest | exact(DataViewRef)

ScopedDataViewProjectionRequest {
  revision_selector: DataViewRevisionSelector
  tenant
  principal
  purpose
  grants
  policy
  disclosure_budget
  discovery_budget
  freshness_requirements
  discovery_intent
  discovery_scope
}

ScopedDataViewProjectionOutcome {
  status: projected | revision-not-available
  scoped_data_view?
}
```

The exact selector accepts only a previously issued ref for the same logical
DataView. Unknown, malformed, unauthorized, evicted, and compliance-redacted
refs share the same non-disclosing unavailable outcome. Current authorization
is always evaluated; a historical ref cannot restore revoked visibility.

## [interface] ScopedDataView

**Rationale**: Callers may observe only an ephemeral policy-scoped projection over one immutable head revision.

```text
ScopedDataView {
  data_view: DataViewRef
  projection_mode: current-actionable | historical-replay
  visible_results
  scoped_discovery_graph
  discovery_frontier
  coverage: complete-for-scope | partial | unknown
  limiting_reasons
  visible_continuation_action_ids
  freshness_constraints
}
```

The projection is ephemeral and bound to exactly one immutable selected
revision. For `historical-replay`, Fustor recomputes visible results, scoped
relationships, coverage, and frontier from the exact data revision plus the
current projection request. The frontier is returned for audit and comparison,
but Fustor emits no `ActionExecutionContext` for it and accepts no submission as
a historical branch.

## [interface] ResultNormalizer

**Rationale**: Source-specific payload interpretation must remain outside DataView core and outside callers.

```text
ResultNormalizerRegistry.resolve(schema_ref, schema_version)
  -> ResultNormalizer

ResultNormalizer.normalize(source_or_projection_result | declared_external_result)
  -> NormalizedDataViewCommit {
       complete_payload_attachment
       canonical_identities
       graph_assertions
       relationship_assertions
       dependency_refs
       normalization_evidence
     }
```

The registry is extensible by result schema. DataView public request enums do
not grow source-, graph-backend-, product-, workflow-, or analysis-specific
ingest variants.
