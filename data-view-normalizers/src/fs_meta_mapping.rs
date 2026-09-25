use std::collections::BTreeSet;

use data_view::{
    ActionExecutionContext, ActionResultSubmission, CoverageState, DATA_VIEW_WIRE_VERSION,
    DataViewContractError, DerivationEvidence, FreshnessState, NativeAnchor, PayloadEncoding,
    ProducerKind, ProjectionApplyEvidence, ProjectionCoverage, SchemaVersionRef,
    SourceAuthorityRef, SourceProjectionRef, StreamingRefreshEntry, StreamingRefreshOperation,
    StreamingRefreshSubmission,
};
use fs_meta::fustor_integration::{
    FS_META_DIRECT_SCHEMA_REF, FS_META_FUSTOR_SCHEMA_VERSION, FS_META_PROJECTION_SCHEMA_REF,
    FsMetaAuthorityEpochEvidence, FsMetaNativeAnchor, FsMetaProjectionApplyEvidence,
    FsMetaProjectionCoverageState, FsMetaProjectionReadiness, FsMetaProjectionResultExport,
    FsMetaSourceProviderEvidence, FsMetaStreamingOperation, FsMetaStreamingRefreshBatch,
};
use fs_meta::query::ReadClass;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsMetaActionSubmissionContext {
    pub submission_id: String,
    pub execution: ActionExecutionContext,
    pub producer_id: String,
    pub policy_labels: BTreeSet<String>,
    pub grant_epoch: u64,
    pub freshness: FreshnessState,
    pub derivation: DerivationEvidence,
    pub continuation: Option<Vec<u8>>,
    pub actual_calls: u64,
    pub actual_fanout: u64,
    pub actual_latency_ms: u64,
    pub actual_cost_units: u64,
}

pub fn map_fs_meta_direct_submission(
    context: FsMetaActionSubmissionContext,
    export: FsMetaSourceProviderEvidence,
) -> Result<ActionResultSubmission, DataViewContractError> {
    if export.schema_ref != FS_META_DIRECT_SCHEMA_REF
        || export.schema_version != FS_META_FUSTOR_SCHEMA_VERSION
        || !matches!(
            export.observation_path,
            fs_meta::fustor_integration::FsMetaObservationPath::Direct
        )
    {
        return Err(DataViewContractError::ObservationPathMismatch);
    }
    validate_action_context(&context)?;
    let source_authority_ref = SourceAuthorityRef::new(export.source_authority_ref.clone())?;
    let actual_bytes = export.payload.len() as u64;
    if actual_bytes != export.actual_bytes {
        return Err(DataViewContractError::InvalidContract(
            "fs-meta actual_bytes",
        ));
    }
    Ok(ActionResultSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: context.submission_id,
        execution: context.execution,
        source_authority_ref,
        authority_object_ref: export.source_authority_ref,
        producer_id: context.producer_id,
        producer_kind: ProducerKind::Source,
        projection_evidence: None,
        schema: SchemaVersionRef {
            schema_ref: export.schema_ref,
            schema_version: export.schema_version,
        },
        encoding: PayloadEncoding::MessagePack,
        payload: export.payload,
        native_anchors: export.native_anchors.into_iter().map(map_anchor).collect(),
        source_revision: export.source_revision,
        evidence_refs: append_authority_epoch_evidence(
            export.evidence_refs,
            &export.authority_epoch,
            &export.change_id,
        ),
        observed_at_ms: micros_to_millis(export.observed_at_us),
        policy_labels: context.policy_labels,
        grant_epoch: context.grant_epoch,
        freshness: context.freshness,
        derivation: context.derivation,
        continuation: context.continuation,
        actual_rows: export.actual_rows,
        actual_bytes,
        actual_calls: context.actual_calls,
        actual_fanout: context.actual_fanout,
        actual_latency_ms: context.actual_latency_ms,
        actual_cost_units: context.actual_cost_units,
    })
}

pub fn map_fs_meta_projection_submission(
    context: FsMetaActionSubmissionContext,
    export: FsMetaProjectionResultExport,
) -> Result<ActionResultSubmission, DataViewContractError> {
    validate_action_context(&context)?;
    let evidence = &export.projection_evidence;
    if evidence.schema_ref != FS_META_PROJECTION_SCHEMA_REF
        || evidence.schema_version != FS_META_FUSTOR_SCHEMA_VERSION
        || !matches!(
            evidence.observation_path,
            fs_meta::fustor_integration::FsMetaObservationPath::ProjectionBacked
        )
    {
        return Err(DataViewContractError::ObservationPathMismatch);
    }
    let source_authority_ref = SourceAuthorityRef::new(evidence.source_authority_ref.clone())?;
    let projection_evidence = map_projection_evidence(evidence)?;
    let actual_bytes = export.payload.len() as u64;
    if actual_bytes != export.actual_bytes {
        return Err(DataViewContractError::InvalidContract(
            "fs-meta actual_bytes",
        ));
    }
    Ok(ActionResultSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: context.submission_id,
        execution: context.execution,
        source_authority_ref,
        authority_object_ref: evidence.source_authority_ref.clone(),
        producer_id: context.producer_id,
        producer_kind: ProducerKind::SourceProjection,
        projection_evidence: Some(projection_evidence),
        schema: SchemaVersionRef {
            schema_ref: evidence.schema_ref.clone(),
            schema_version: evidence.schema_version.clone(),
        },
        encoding: PayloadEncoding::MessagePack,
        payload: export.payload,
        native_anchors: evidence
            .native_anchors
            .iter()
            .cloned()
            .map(map_anchor)
            .collect(),
        source_revision: evidence.source_revision.clone(),
        evidence_refs: append_authority_epoch_evidence(
            evidence.evidence_refs.clone(),
            &evidence.authority_epoch,
            &evidence.applied_change_id,
        ),
        observed_at_ms: micros_to_millis(evidence.observed_at_us),
        policy_labels: context.policy_labels,
        grant_epoch: context.grant_epoch,
        freshness: context.freshness,
        derivation: context.derivation,
        continuation: context.continuation,
        actual_rows: export.actual_rows,
        actual_bytes,
        actual_calls: context.actual_calls,
        actual_fanout: context.actual_fanout,
        actual_latency_ms: context.actual_latency_ms,
        actual_cost_units: context.actual_cost_units,
    })
}

pub fn map_fs_meta_stream_submission(
    submission_id: impl Into<String>,
    batch: FsMetaStreamingRefreshBatch,
) -> Result<StreamingRefreshSubmission, DataViewContractError> {
    let submission_id = submission_id.into();
    if submission_id.trim().is_empty() || batch.entries.is_empty() {
        return Err(DataViewContractError::InvalidContract(
            "fs-meta stream submission",
        ));
    }
    let schema = SchemaVersionRef {
        schema_ref: batch.entries[0].schema_ref.clone(),
        schema_version: batch.entries[0].schema_version.clone(),
    };
    if batch.entries.iter().any(|entry| {
        entry.schema_ref != schema.schema_ref || entry.schema_version != schema.schema_version
    }) {
        return Err(DataViewContractError::UnsupportedSchema);
    }
    let refreshes = batch
        .entries
        .into_iter()
        .map(|entry| {
            let operation = match entry.operation {
                FsMetaStreamingOperation::Refresh => StreamingRefreshOperation::Refresh,
                FsMetaStreamingOperation::Invalidate => StreamingRefreshOperation::Invalidate,
                FsMetaStreamingOperation::Delete => StreamingRefreshOperation::Delete,
            };
            Ok(StreamingRefreshEntry {
                tracked_result_ref: data_view::TrackedResultRef(entry.tracked_result_ref),
                operation,
                payload: entry.payload,
                native_anchors: entry.native_anchors.into_iter().map(map_anchor).collect(),
                source_revision: entry.source_revision,
                evidence_refs: append_authority_epoch_evidence(
                    Vec::new(),
                    &entry.authority_epoch,
                    &entry.change_id,
                ),
                observed_at_ms: micros_to_millis(entry.observed_at_us),
            })
        })
        .collect::<Result<Vec<_>, DataViewContractError>>()?;
    let actual_bytes = refreshes
        .iter()
        .map(|entry| {
            entry
                .payload
                .as_ref()
                .map_or(0, |payload| payload.len() as u64)
        })
        .sum::<u64>();
    if actual_bytes != batch.actual_bytes {
        return Err(DataViewContractError::InvalidContract(
            "fs-meta stream actual_bytes",
        ));
    }
    let projection_evidence = batch
        .projection_evidence
        .as_ref()
        .map(map_projection_evidence)
        .transpose()?;
    Ok(StreamingRefreshSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id,
        subscription_id: batch.subscription_id,
        batch_id: batch.batch_id,
        sequence: batch.sequence,
        previous_watermark: batch.previous_watermark,
        resulting_watermark: batch.resulting_watermark,
        schema,
        encoding: PayloadEncoding::MessagePack,
        actual_events: refreshes.len() as u64,
        refreshes,
        projection_evidence,
        actual_bytes,
    })
}

pub fn source_projection_ref_from_fs_meta(
    evidence: &FsMetaProjectionApplyEvidence,
) -> Result<SourceProjectionRef, DataViewContractError> {
    let projection_kind = match evidence.read_class {
        ReadClass::Materialized => "fs-meta-materialized",
        ReadClass::TrustedMaterialized => "fs-meta-trusted-materialized",
        ReadClass::Fresh => {
            return Err(DataViewContractError::ObservationPathMismatch);
        }
    };
    Ok(SourceProjectionRef {
        projection_id: evidence.projection_ref.clone(),
        source_authority_ref: SourceAuthorityRef::new(evidence.source_authority_ref.clone())?,
        projection_kind: projection_kind.into(),
    })
}

fn map_projection_evidence(
    evidence: &FsMetaProjectionApplyEvidence,
) -> Result<ProjectionApplyEvidence, DataViewContractError> {
    let state = match evidence.coverage.state {
        FsMetaProjectionCoverageState::CompleteForScope => CoverageState::CompleteForScope,
        FsMetaProjectionCoverageState::Partial => CoverageState::Partial,
        FsMetaProjectionCoverageState::Unknown => CoverageState::Unknown,
    };
    let freshness = match evidence.readiness {
        FsMetaProjectionReadiness::Ready => FreshnessState::Fresh,
        FsMetaProjectionReadiness::MaterializedUntrusted => FreshnessState::StaleButPlannable,
    };
    Ok(ProjectionApplyEvidence {
        projection: source_projection_ref_from_fs_meta(evidence)?,
        source_revision: evidence.source_revision.clone(),
        projection_revision: evidence.projection_revision.clone(),
        applied_change_id: evidence.applied_change_id.clone(),
        coverage: ProjectionCoverage {
            state,
            scope_ref: evidence.coverage.scope_ref.clone(),
            limiting_reasons: evidence.coverage.limiting_reasons.clone(),
        },
        freshness,
        observed_at_ms: micros_to_millis(evidence.observed_at_us),
        evidence_refs: evidence.evidence_refs.clone(),
    })
}

fn validate_action_context(
    context: &FsMetaActionSubmissionContext,
) -> Result<(), DataViewContractError> {
    if context.submission_id.trim().is_empty()
        || context.producer_id.trim().is_empty()
        || context.actual_calls == 0
        || context.actual_fanout == 0
        || context.actual_latency_ms == 0
        || context.actual_cost_units == 0
    {
        return Err(DataViewContractError::InvalidContract(
            "fs-meta action context",
        ));
    }
    Ok(())
}

fn map_anchor(anchor: FsMetaNativeAnchor) -> NativeAnchor {
    NativeAnchor {
        anchor_kind: anchor.anchor_kind,
        native_ref: anchor.native_ref,
        native_pointer: Some(anchor.native_pointer),
        native_version: Some(anchor.native_version),
    }
}

fn append_authority_epoch_evidence(
    mut evidence_refs: Vec<String>,
    epoch: &FsMetaAuthorityEpochEvidence,
    change_id: &str,
) -> Vec<String> {
    evidence_refs.push(format!("fs-meta:change:{change_id}"));
    evidence_refs.push(format!("fs-meta:roots-signature:{}", epoch.roots_signature));
    evidence_refs.push(format!(
        "fs-meta:grants-signature:{}",
        epoch.grants_signature
    ));
    evidence_refs.push(format!(
        "fs-meta:journal-sequence:{}",
        epoch.journal_sequence
    ));
    if let Some(generation) = epoch.source_stream_generation {
        evidence_refs.push(format!("fs-meta:source-stream-generation:{generation}"));
    }
    evidence_refs.sort();
    evidence_refs.dedup();
    evidence_refs
}

fn micros_to_millis(value: u64) -> u64 {
    value.saturating_add(999) / 1000
}
