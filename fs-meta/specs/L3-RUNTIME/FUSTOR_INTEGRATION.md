---
version: 3.1.0
---

# L3: fs-meta Fustor Integration

## [interface] FsMetaSourceProviderEvidence

**Rationale**: A direct fs-meta result must preserve source-owned identity and revision evidence for a bounded external consumer.

```text
FsMetaSourceProviderEvidence {
  observation_path: direct
  source_authority_ref
  change_id
  source_revision
  authority_epoch
  schema_ref
  schema_version
  payload
  native_anchors
  evidence_refs
  observed_at
}
```

The payload contains fs-meta domain data only. It carries no external DataView
identity, global graph identity, or caller-authored relationship assertion.

## [interface] FsMetaProjectionApplyEvidence

**Rationale**: A materialized read must prove which authoritative source state the rebuildable projection has applied.

```text
FsMetaProjectionApplyEvidence {
  observation_path: projection-backed
  source_authority_ref
  source_revision
  authority_epoch
  projection_ref
  projection_revision
  applied_change_id
  coverage
  readiness
  schema_ref
  schema_version
  native_anchors
  evidence_refs
  observed_at
}
```

Projection evidence supplements source authority. It cannot promote the sink
materialized tree into an independent truth ledger.

## [interface] FsMetaStreamingRefreshBatch

**Rationale**: Continuous fs-meta change delivery must remain bounded to external results selected before the stream begins.

```text
FsMetaStreamingRefreshBatch {
  subscription_id
  batch_id
  sequence
  previous_watermark
  resulting_watermark
  entries[] {
    tracked_result_ref
    operation: refresh | invalidate | delete
    schema_ref
    schema_version
    payload?
    native_anchors
    change_id
    source_revision
    authority_epoch
    observed_at
  }
  projection_evidence?
}
```

Every entry requires a tracked result reference supplied by the external
subscription boundary. An untracked filesystem identity cannot be represented
as a stream-created external result.

## [workflow] FsMetaDirectResultExport

**Steps**:
1. The caller explicitly selects the `direct` observation path and supplies a bounded query scope.
2. fs-meta executes `/on-demand-force-find` against the selected source-side logical groups.
3. fs-meta freezes the bounded result under its normal PIT and pagination semantics.
4. The export adapter emits `FsMetaSourceProviderEvidence` with native anchors, change identity, source revision, authority epoch, schema identity, and observation time.
5. The external consumer performs source-specific normalization and commit policy outside fs-meta.

## [workflow] FsMetaProjectionBackedResultExport

**Steps**:
1. The caller explicitly selects `projection-backed` with either `materialized` or `trusted-materialized` read class.
2. fs-meta reads the sink-side materialized tree under the selected PIT and observation status.
3. fs-meta binds the result to source revision, projection revision, applied change identity, coverage, and readiness evidence.
4. The export adapter emits the schema-tagged payload plus `FsMetaProjectionApplyEvidence`.
5. Missing or inconsistent projection apply evidence fails the projection-backed export; fs-meta does not silently retry through the direct path.

## [workflow] FsMetaTrackedResultStreamingRefresh

**Steps**:
1. The external consumer establishes a bounded subscription with a fixed set of tracked result references and an initial source watermark.
2. fs-meta maps eligible realtime changes only to those tracked references.
3. fs-meta emits ordered `refresh`, `invalidate`, or `delete` entries with watermark and source revision continuity.
4. A change that cannot be mapped to a tracked result reference remains outside the export batch even if fs-meta ingests or materializes it locally.
5. Discovery of that object requires a later explicit bounded direct or projection-backed query.

## [decision] FsMetaSourceProjectionAuthorityBoundary

**Rationale**: fs-meta already separates authoritative journal truth from rebuildable materialized observation state, and the external integration must preserve that distinction.

- Treat source-side live observations plus the authoritative journal as the `SourceProvider` authority.
- Treat the sink-side materialized tree only as an optional `SourceProjection`.
- Require explicit observation-path selection and projection apply evidence.
- Keep normalization and DataView identity outside fs-meta.
- Restrict streaming export to pre-tracked external results.
- Direct reads retain source authority, projection-backed reads gain low-latency evidence without becoming truth, and continuous refresh cannot expand an external consumer's discovered object set.
