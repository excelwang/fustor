use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::query::{ForceFindQueryPayload, MaterializedQueryPayload, QueryScope, ReadClass};
use crate::shared_types::{EventKind, FileMetaRecord};

pub const FS_META_FUSTOR_SCHEMA_VERSION: &str = "1";
pub const FS_META_DIRECT_SCHEMA_REF: &str = "fustor.fs-meta.force-find";
pub const FS_META_PROJECTION_SCHEMA_REF: &str = "fustor.fs-meta.materialized";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FsMetaExportError {
    Invalid(&'static str),
    LimitExceeded(&'static str),
    Serialization,
}

impl fmt::Display for FsMetaExportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(field) => write!(formatter, "invalid fs-meta export field: {field}"),
            Self::LimitExceeded(limit) => {
                write!(formatter, "fs-meta export limit exceeded: {limit}")
            }
            Self::Serialization => formatter.write_str("fs-meta export serialization failed"),
        }
    }
}

impl std::error::Error for FsMetaExportError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FsMetaObservationPath {
    Direct,
    ProjectionBacked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetaAuthorityEpochEvidence {
    pub roots_signature: String,
    pub grants_signature: String,
    pub source_stream_generation: Option<u64>,
    pub journal_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetaAuthorityCursor {
    pub source_authority_ref: String,
    pub change_id: String,
    pub source_revision: String,
    pub authority_epoch: FsMetaAuthorityEpochEvidence,
    pub observed_at_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetaNativeAnchor {
    pub anchor_kind: String,
    pub native_ref: String,
    pub native_pointer: String,
    pub native_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetaExportLimits {
    pub max_rows: u64,
    pub max_payload_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetaSourceProviderEvidence {
    pub observation_path: FsMetaObservationPath,
    pub source_authority_ref: String,
    pub change_id: String,
    pub source_revision: String,
    pub authority_epoch: FsMetaAuthorityEpochEvidence,
    pub schema_ref: String,
    pub schema_version: String,
    pub payload: Vec<u8>,
    pub native_anchors: Vec<FsMetaNativeAnchor>,
    pub evidence_refs: Vec<String>,
    pub observed_at_us: u64,
    pub actual_rows: u64,
    pub actual_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FsMetaProjectionCoverageState {
    CompleteForScope,
    Partial,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetaProjectionCoverage {
    pub state: FsMetaProjectionCoverageState,
    pub scope_ref: String,
    pub limiting_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FsMetaProjectionReadiness {
    Ready,
    MaterializedUntrusted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetaProjectionCursor {
    pub authority: FsMetaAuthorityCursor,
    pub projection_ref: String,
    pub projection_revision: String,
    pub applied_change_id: String,
    pub coverage: FsMetaProjectionCoverage,
    pub readiness: FsMetaProjectionReadiness,
    pub read_class: ReadClass,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetaProjectionApplyEvidence {
    pub observation_path: FsMetaObservationPath,
    pub source_authority_ref: String,
    pub source_revision: String,
    pub authority_epoch: FsMetaAuthorityEpochEvidence,
    pub projection_ref: String,
    pub projection_revision: String,
    pub applied_change_id: String,
    pub coverage: FsMetaProjectionCoverage,
    pub readiness: FsMetaProjectionReadiness,
    pub read_class: ReadClass,
    pub schema_ref: String,
    pub schema_version: String,
    pub native_anchors: Vec<FsMetaNativeAnchor>,
    pub evidence_refs: Vec<String>,
    pub observed_at_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetaProjectionResultExport {
    pub payload: Vec<u8>,
    pub projection_evidence: FsMetaProjectionApplyEvidence,
    pub actual_rows: u64,
    pub actual_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FsMetaStreamingOperation {
    Refresh,
    Invalidate,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetaTrackedResultBinding {
    pub tracked_result_ref: String,
    #[serde(with = "serde_bytes")]
    pub native_path: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetaStreamingChange {
    pub record: FileMetaRecord,
    pub change_id: String,
    pub source_revision: String,
    pub authority_epoch: FsMetaAuthorityEpochEvidence,
    pub observed_at_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetaStreamingRefreshEntry {
    pub tracked_result_ref: String,
    pub operation: FsMetaStreamingOperation,
    pub schema_ref: String,
    pub schema_version: String,
    pub payload: Option<Vec<u8>>,
    pub native_anchors: Vec<FsMetaNativeAnchor>,
    pub change_id: String,
    pub source_revision: String,
    pub authority_epoch: FsMetaAuthorityEpochEvidence,
    pub observed_at_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsMetaStreamingRefreshBatch {
    pub subscription_id: String,
    pub batch_id: String,
    pub sequence: u64,
    pub previous_watermark: String,
    pub resulting_watermark: String,
    pub entries: Vec<FsMetaStreamingRefreshEntry>,
    pub projection_evidence: Option<FsMetaProjectionApplyEvidence>,
    pub actual_bytes: u64,
}

pub fn export_direct_result(
    payload: ForceFindQueryPayload,
    scope: &QueryScope,
    authority: FsMetaAuthorityCursor,
    limits: &FsMetaExportLimits,
) -> Result<FsMetaSourceProviderEvidence, FsMetaExportError> {
    validate_authority(&authority)?;
    validate_limits(limits)?;
    let actual_rows = payload_rows_force_find(&payload);
    if actual_rows > limits.max_rows {
        return Err(FsMetaExportError::LimitExceeded("rows"));
    }
    let payload =
        rmp_serde::to_vec_named(&payload).map_err(|_| FsMetaExportError::Serialization)?;
    if payload.len() as u64 > limits.max_payload_bytes {
        return Err(FsMetaExportError::LimitExceeded("payload-bytes"));
    }
    let anchors = vec![scope_anchor(scope, &authority)];
    Ok(FsMetaSourceProviderEvidence {
        observation_path: FsMetaObservationPath::Direct,
        source_authority_ref: authority.source_authority_ref.clone(),
        change_id: authority.change_id.clone(),
        source_revision: authority.source_revision.clone(),
        authority_epoch: authority.authority_epoch.clone(),
        schema_ref: FS_META_DIRECT_SCHEMA_REF.into(),
        schema_version: FS_META_FUSTOR_SCHEMA_VERSION.into(),
        payload,
        native_anchors: anchors,
        evidence_refs: authority_evidence_refs(&authority),
        observed_at_us: authority.observed_at_us,
        actual_rows,
        actual_bytes: 0,
    }
    .with_actual_bytes())
}

pub fn export_projection_result(
    payload: MaterializedQueryPayload,
    scope: &QueryScope,
    cursor: FsMetaProjectionCursor,
    limits: &FsMetaExportLimits,
) -> Result<FsMetaProjectionResultExport, FsMetaExportError> {
    validate_projection_cursor(&cursor)?;
    validate_limits(limits)?;
    let actual_rows = payload_rows_materialized(&payload);
    if actual_rows > limits.max_rows {
        return Err(FsMetaExportError::LimitExceeded("rows"));
    }
    let payload =
        rmp_serde::to_vec_named(&payload).map_err(|_| FsMetaExportError::Serialization)?;
    if payload.len() as u64 > limits.max_payload_bytes {
        return Err(FsMetaExportError::LimitExceeded("payload-bytes"));
    }
    let anchors = vec![scope_anchor(scope, &cursor.authority)];
    let mut evidence_refs = authority_evidence_refs(&cursor.authority);
    evidence_refs.push(format!("fs-meta:projection:{}", cursor.projection_revision));
    evidence_refs.push(format!(
        "fs-meta:applied-change:{}",
        cursor.applied_change_id
    ));
    let projection_evidence = FsMetaProjectionApplyEvidence {
        observation_path: FsMetaObservationPath::ProjectionBacked,
        source_authority_ref: cursor.authority.source_authority_ref.clone(),
        source_revision: cursor.authority.source_revision.clone(),
        authority_epoch: cursor.authority.authority_epoch.clone(),
        projection_ref: cursor.projection_ref,
        projection_revision: cursor.projection_revision,
        applied_change_id: cursor.applied_change_id,
        coverage: cursor.coverage,
        readiness: cursor.readiness,
        read_class: cursor.read_class,
        schema_ref: FS_META_PROJECTION_SCHEMA_REF.into(),
        schema_version: FS_META_FUSTOR_SCHEMA_VERSION.into(),
        native_anchors: anchors,
        evidence_refs,
        observed_at_us: cursor.authority.observed_at_us,
    };
    Ok(FsMetaProjectionResultExport {
        actual_bytes: payload.len() as u64,
        payload,
        projection_evidence,
        actual_rows,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn export_tracked_stream_batch(
    subscription_id: impl Into<String>,
    batch_id: impl Into<String>,
    sequence: u64,
    previous_watermark: impl Into<String>,
    resulting_watermark: impl Into<String>,
    bindings: &[FsMetaTrackedResultBinding],
    changes: Vec<FsMetaStreamingChange>,
    projection_evidence: Option<FsMetaProjectionApplyEvidence>,
    max_events: u64,
    max_payload_bytes: u64,
) -> Result<Option<FsMetaStreamingRefreshBatch>, FsMetaExportError> {
    let subscription_id = subscription_id.into();
    let batch_id = batch_id.into();
    let previous_watermark = previous_watermark.into();
    let resulting_watermark = resulting_watermark.into();
    if subscription_id.trim().is_empty()
        || batch_id.trim().is_empty()
        || sequence == 0
        || previous_watermark.trim().is_empty()
        || resulting_watermark.trim().is_empty()
        || max_events == 0
        || max_payload_bytes == 0
    {
        return Err(FsMetaExportError::Invalid("stream batch"));
    }
    let mut tracked_by_path = BTreeMap::<Vec<u8>, String>::new();
    let mut tracked_refs = BTreeSet::new();
    for binding in bindings {
        if binding.tracked_result_ref.trim().is_empty()
            || binding.native_path.is_empty()
            || !tracked_refs.insert(binding.tracked_result_ref.clone())
            || tracked_by_path
                .insert(
                    binding.native_path.clone(),
                    binding.tracked_result_ref.clone(),
                )
                .is_some()
        {
            return Err(FsMetaExportError::Invalid("tracked result bindings"));
        }
    }

    let mut entries = Vec::new();
    let mut actual_bytes = 0_u64;
    for change in changes {
        validate_stream_change(&change)?;
        let Some(tracked_result_ref) = tracked_by_path.get(&change.record.path) else {
            continue;
        };
        let (operation, payload) = match change.record.event_kind {
            EventKind::Delete => (FsMetaStreamingOperation::Delete, None),
            EventKind::Update if change.record.audit_skipped => {
                (FsMetaStreamingOperation::Invalidate, None)
            }
            EventKind::Update => {
                let payload = rmp_serde::to_vec_named(&change.record)
                    .map_err(|_| FsMetaExportError::Serialization)?;
                actual_bytes = actual_bytes.saturating_add(payload.len() as u64);
                (FsMetaStreamingOperation::Refresh, Some(payload))
            }
        };
        entries.push(FsMetaStreamingRefreshEntry {
            tracked_result_ref: tracked_result_ref.clone(),
            operation,
            schema_ref: FS_META_DIRECT_SCHEMA_REF.into(),
            schema_version: FS_META_FUSTOR_SCHEMA_VERSION.into(),
            payload,
            native_anchors: vec![record_anchor(&change.record, &change.source_revision)],
            change_id: change.change_id,
            source_revision: change.source_revision,
            authority_epoch: change.authority_epoch,
            observed_at_us: change.observed_at_us,
        });
    }
    if entries.is_empty() {
        return Ok(None);
    }
    if entries.len() as u64 > max_events {
        return Err(FsMetaExportError::LimitExceeded("events"));
    }
    if actual_bytes > max_payload_bytes {
        return Err(FsMetaExportError::LimitExceeded("payload-bytes"));
    }
    Ok(Some(FsMetaStreamingRefreshBatch {
        subscription_id,
        batch_id,
        sequence,
        previous_watermark,
        resulting_watermark,
        entries,
        projection_evidence,
        actual_bytes,
    }))
}

impl FsMetaSourceProviderEvidence {
    fn with_actual_bytes(mut self) -> Self {
        self.actual_bytes = self.payload.len() as u64;
        self
    }
}

fn validate_authority(authority: &FsMetaAuthorityCursor) -> Result<(), FsMetaExportError> {
    if authority.source_authority_ref.trim().is_empty()
        || authority.change_id.trim().is_empty()
        || authority.source_revision.trim().is_empty()
        || authority.observed_at_us == 0
        || authority.authority_epoch.roots_signature.trim().is_empty()
        || authority.authority_epoch.grants_signature.trim().is_empty()
        || authority.authority_epoch.journal_sequence == 0
    {
        return Err(FsMetaExportError::Invalid("authority cursor"));
    }
    Ok(())
}

fn validate_projection_cursor(cursor: &FsMetaProjectionCursor) -> Result<(), FsMetaExportError> {
    validate_authority(&cursor.authority)?;
    if cursor.projection_ref.trim().is_empty()
        || cursor.projection_revision.trim().is_empty()
        || cursor.applied_change_id.trim().is_empty()
        || cursor.coverage.scope_ref.trim().is_empty()
    {
        return Err(FsMetaExportError::Invalid("projection cursor"));
    }
    match (cursor.read_class, &cursor.readiness) {
        (ReadClass::TrustedMaterialized, FsMetaProjectionReadiness::Ready)
        | (ReadClass::Materialized, FsMetaProjectionReadiness::Ready)
        | (ReadClass::Materialized, FsMetaProjectionReadiness::MaterializedUntrusted) => Ok(()),
        _ => Err(FsMetaExportError::Invalid("projection readiness")),
    }
}

fn validate_limits(limits: &FsMetaExportLimits) -> Result<(), FsMetaExportError> {
    if limits.max_rows == 0 || limits.max_payload_bytes == 0 {
        return Err(FsMetaExportError::Invalid("export limits"));
    }
    Ok(())
}

fn validate_stream_change(change: &FsMetaStreamingChange) -> Result<(), FsMetaExportError> {
    if change.record.path.is_empty()
        || change.change_id.trim().is_empty()
        || change.source_revision.trim().is_empty()
        || change.observed_at_us == 0
        || change.authority_epoch.journal_sequence == 0
    {
        return Err(FsMetaExportError::Invalid("stream change"));
    }
    Ok(())
}

fn payload_rows_force_find(payload: &ForceFindQueryPayload) -> u64 {
    match payload {
        ForceFindQueryPayload::Tree(tree) => tree.entries.len() as u64 + 1,
        ForceFindQueryPayload::Stats(_) => 1,
    }
}

fn payload_rows_materialized(payload: &MaterializedQueryPayload) -> u64 {
    match payload {
        MaterializedQueryPayload::Tree(tree) => tree.entries.len() as u64 + 1,
        MaterializedQueryPayload::Stats(_) => 1,
    }
}

fn authority_evidence_refs(authority: &FsMetaAuthorityCursor) -> Vec<String> {
    vec![
        format!("fs-meta:change:{}", authority.change_id),
        format!("fs-meta:source-revision:{}", authority.source_revision),
        format!(
            "fs-meta:authority-journal:{}",
            authority.authority_epoch.journal_sequence
        ),
    ]
}

fn scope_anchor(scope: &QueryScope, authority: &FsMetaAuthorityCursor) -> FsMetaNativeAnchor {
    let path = native_path_ref(&scope.path);
    let group = scope.selected_group.as_deref().unwrap_or("all-groups");
    FsMetaNativeAnchor {
        anchor_kind: "fs-meta-query-scope".into(),
        native_ref: format!("fs-meta://{group}{path}"),
        native_pointer: format!("group={group};path={path}"),
        native_version: authority.source_revision.clone(),
    }
}

fn record_anchor(record: &FileMetaRecord, source_revision: &str) -> FsMetaNativeAnchor {
    let path = native_path_ref(&record.path);
    FsMetaNativeAnchor {
        anchor_kind: if record.unix_stat.is_dir {
            "fs-directory".into()
        } else {
            "fs-file".into()
        },
        native_ref: path.clone(),
        native_pointer: path,
        native_version: source_revision.into(),
    }
}

fn native_path_ref(path: &[u8]) -> String {
    if let Ok(path) = std::str::from_utf8(path) {
        return path.to_string();
    }
    let mut encoded = String::from("bytes:");
    for byte in path {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}
