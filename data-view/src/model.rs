use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::DataViewContractError;

pub const DATA_VIEW_WIRE_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceAuthorityRef(pub String);

impl SourceAuthorityRef {
    pub fn new(value: impl Into<String>) -> Result<Self, DataViewContractError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(DataViewContractError::InvalidContract(
                "source_authority_ref",
            ));
        }
        Ok(Self(value))
    }
}

impl From<String> for SourceAuthorityRef {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for SourceAuthorityRef {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl AsRef<str> for SourceAuthorityRef {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for SourceAuthorityRef {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for SourceAuthorityRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl PartialEq<String> for SourceAuthorityRef {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<SourceAuthorityRef> for String {
    fn eq(&self, other: &SourceAuthorityRef) -> bool {
        *self == other.0
    }
}

impl PartialEq<str> for SourceAuthorityRef {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceProviderRef {
    pub provider_id: String,
    pub source_authority_ref: SourceAuthorityRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SourceProjectionRef {
    pub projection_id: String,
    pub source_authority_ref: SourceAuthorityRef,
    pub projection_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ObservationPath {
    Direct,
    ProjectionBacked { projection: SourceProjectionRef },
}

impl ObservationPath {
    pub fn projection(&self) -> Option<&SourceProjectionRef> {
        match self {
            Self::Direct => None,
            Self::ProjectionBacked { projection } => Some(projection),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProjectionMode {
    CurrentActionable,
    HistoricalReplay,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiscoveryOperation {
    SourceStatus,
    SchemaProbe,
    FieldDiscovery,
    LimitedSample,
    AggregateProbe,
    ObjectList,
    ObjectHead,
    MappingProbe,
    TreeProbe,
    JoinCandidateProbe,
    Revalidate,
    Extension(String),
}

impl DiscoveryOperation {
    pub fn breadth_rank(&self) -> u8 {
        match self {
            Self::SourceStatus => 0,
            Self::SchemaProbe | Self::ObjectList | Self::MappingProbe | Self::TreeProbe => 1,
            Self::FieldDiscovery | Self::ObjectHead => 2,
            Self::JoinCandidateProbe => 3,
            Self::LimitedSample | Self::AggregateProbe => 4,
            Self::Revalidate => 5,
            Self::Extension(_) => 6,
        }
    }

    pub fn is_high_risk(&self) -> bool {
        matches!(
            self,
            Self::LimitedSample
                | Self::AggregateProbe
                | Self::JoinCandidateProbe
                | Self::ObjectHead
        )
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::SourceStatus => "source-status",
            Self::SchemaProbe => "schema-probe",
            Self::FieldDiscovery => "field-discovery",
            Self::LimitedSample => "limited-sample",
            Self::AggregateProbe => "aggregate-probe",
            Self::ObjectList => "object-list",
            Self::ObjectHead => "object-head",
            Self::MappingProbe => "mapping-probe",
            Self::TreeProbe => "tree-probe",
            Self::JoinCandidateProbe => "join-candidate-probe",
            Self::Revalidate => "revalidate",
            Self::Extension(value) => value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FreshnessState {
    Fresh,
    StaleButPlannable,
    RequiresRevalidation,
    Invalidated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SideEffectLevel {
    None,
    ReadOnly,
    WritesSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ContinuationPolicy {
    None,
    CursorBounded,
    AskUser,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FailureMode {
    NotDisclosed,
    NeedsRevalidation,
    BudgetExceeded,
    SourceUnavailable,
    UnsupportedSchema,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundedSourceAction {
    pub wire_version: u16,
    pub action_id: String,
    pub source_authority_ref: SourceAuthorityRef,
    pub observation_path: ObservationPath,
    pub operation: DiscoveryOperation,
    pub permission_scope: String,
    pub freshness_precondition: FreshnessState,
    pub max_rows: u64,
    pub max_bytes: u64,
    pub max_calls: u64,
    pub max_fanout: u64,
    pub max_latency_ms: u64,
    pub max_cost_units: u64,
    pub side_effect_level: SideEffectLevel,
    pub continuation_policy: ContinuationPolicy,
    #[serde(default)]
    pub failure_modes: Vec<FailureMode>,
    #[serde(default)]
    pub expansion_rules: Vec<String>,
    pub expected_information_gain: u8,
    pub explanation: String,
}

impl BoundedSourceAction {
    pub fn validate(&self) -> Result<(), DataViewContractError> {
        if self.wire_version != DATA_VIEW_WIRE_VERSION {
            return Err(DataViewContractError::InvalidContract("wire_version"));
        }
        if self.action_id.trim().is_empty() {
            return Err(DataViewContractError::InvalidContract("action_id"));
        }
        if self.source_authority_ref.0.trim().is_empty() {
            return Err(DataViewContractError::InvalidContract(
                "source_authority_ref",
            ));
        }
        if self.permission_scope.trim().is_empty() {
            return Err(DataViewContractError::InvalidContract("permission_scope"));
        }
        if self.max_calls == 0 {
            return Err(DataViewContractError::InvalidContract("max_calls"));
        }
        if self.max_latency_ms == 0 {
            return Err(DataViewContractError::InvalidContract("max_latency_ms"));
        }
        if self.max_cost_units == 0 {
            return Err(DataViewContractError::InvalidContract("max_cost_units"));
        }
        if self.expected_information_gain > 100 {
            return Err(DataViewContractError::InvalidContract(
                "expected_information_gain",
            ));
        }
        if let Some(projection) = self.observation_path.projection()
            && projection.source_authority_ref != self.source_authority_ref
        {
            return Err(DataViewContractError::ObservationPathMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DataViewRef {
    pub data_view_id: String,
    pub revision_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionExecutionContext {
    pub wire_version: u16,
    pub action_id: String,
    pub execution_id: String,
    pub base_revision: DataViewRef,
    pub action_contract_digest: String,
    pub policy_context_digest: String,
    pub grant_epoch: u64,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SchemaVersionRef {
    pub schema_ref: String,
    pub schema_version: String,
}

impl SchemaVersionRef {
    pub fn validate(&self) -> Result<(), DataViewContractError> {
        if self.schema_ref.trim().is_empty() {
            return Err(DataViewContractError::InvalidContract("schema_ref"));
        }
        if self.schema_version.trim().is_empty() {
            return Err(DataViewContractError::InvalidContract("schema_version"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CoverageState {
    CompleteForScope,
    Partial,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCoverage {
    pub state: CoverageState,
    pub scope_ref: String,
    #[serde(default)]
    pub limiting_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionApplyEvidence {
    pub projection: SourceProjectionRef,
    pub source_revision: String,
    pub projection_revision: String,
    pub applied_change_id: String,
    pub coverage: ProjectionCoverage,
    pub freshness: FreshnessState,
    pub observed_at_ms: u64,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProducerKind {
    Source,
    SourceProjection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PayloadEncoding {
    Json,
    MessagePack,
    Utf8,
    Bytes,
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeAnchor {
    pub anchor_kind: String,
    pub native_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_pointer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivationEvidence {
    pub derivation_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule_ref: Option<String>,
    #[serde(default)]
    pub dependency_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionResultSubmission {
    pub wire_version: u16,
    pub submission_id: String,
    pub execution: ActionExecutionContext,
    pub source_authority_ref: SourceAuthorityRef,
    pub authority_object_ref: String,
    pub producer_id: String,
    pub producer_kind: ProducerKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_evidence: Option<ProjectionApplyEvidence>,
    pub schema: SchemaVersionRef,
    pub encoding: PayloadEncoding,
    pub payload: Vec<u8>,
    #[serde(default)]
    pub native_anchors: Vec<NativeAnchor>,
    pub source_revision: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub observed_at_ms: u64,
    #[serde(default)]
    pub policy_labels: BTreeSet<String>,
    pub grant_epoch: u64,
    pub freshness: FreshnessState,
    pub derivation: DerivationEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<Vec<u8>>,
    pub actual_rows: u64,
    pub actual_bytes: u64,
    pub actual_calls: u64,
    pub actual_fanout: u64,
    pub actual_latency_ms: u64,
    pub actual_cost_units: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TrackedResultRef(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StreamingSubscriptionStatus {
    Active,
    Paused,
    Expired,
    Revoked,
    RequiresRevalidation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateStreamingSubscription {
    pub wire_version: u16,
    pub tracked_result_refs: BTreeSet<TrackedResultRef>,
    pub source_authority_ref: SourceAuthorityRef,
    pub observation_path: ObservationPath,
    pub allowed_schema_versions: BTreeSet<SchemaVersionRef>,
    pub source_watermark: String,
    pub max_events_per_batch: u64,
    pub max_bytes_per_batch: u64,
    pub max_batches_per_window: u64,
    pub batch_window_ms: u64,
    pub max_in_flight: u64,
    pub lease_issued_at_ms: u64,
    pub lease_expires_at_ms: u64,
    pub policy_context_digest: String,
    pub grant_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamingSubscription {
    pub wire_version: u16,
    pub subscription_id: String,
    pub tracked_result_refs: BTreeSet<TrackedResultRef>,
    pub source_authority_ref: SourceAuthorityRef,
    pub observation_path: ObservationPath,
    pub allowed_schema_versions: BTreeSet<SchemaVersionRef>,
    pub source_watermark: String,
    pub next_sequence: u64,
    pub max_events_per_batch: u64,
    pub max_bytes_per_batch: u64,
    pub max_batches_per_window: u64,
    pub batch_window_ms: u64,
    pub max_in_flight: u64,
    pub window_started_at_ms: u64,
    pub committed_batches_in_window: u64,
    pub in_flight: u64,
    pub lease_issued_at_ms: u64,
    pub lease_expires_at_ms: u64,
    pub policy_context_digest: String,
    pub grant_epoch: u64,
    pub status: StreamingSubscriptionStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebaseStreamingSubscription {
    pub wire_version: u16,
    pub subscription_id: String,
    pub tracked_result_refs: BTreeSet<TrackedResultRef>,
    pub source_watermark: String,
    pub policy_context_digest: String,
    pub grant_epoch: u64,
    pub lease_issued_at_ms: u64,
    pub lease_expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StreamingRefreshOperation {
    Refresh,
    Invalidate,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamingRefreshEntry {
    pub tracked_result_ref: TrackedResultRef,
    pub operation: StreamingRefreshOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    #[serde(default)]
    pub native_anchors: Vec<NativeAnchor>,
    pub source_revision: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamingRefreshSubmission {
    pub wire_version: u16,
    pub submission_id: String,
    pub subscription_id: String,
    pub batch_id: String,
    pub sequence: u64,
    pub previous_watermark: String,
    pub resulting_watermark: String,
    pub schema: SchemaVersionRef,
    pub encoding: PayloadEncoding,
    #[serde(default)]
    pub refreshes: Vec<StreamingRefreshEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_evidence: Option<ProjectionApplyEvidence>,
    pub actual_bytes: u64,
    pub actual_events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceProviderEvidence {
    pub provider: SourceProviderRef,
    pub schema: SchemaVersionRef,
    pub source_revision: String,
    pub authority_epoch: u64,
    pub change_id: String,
    #[serde(default)]
    pub native_anchors: Vec<NativeAnchor>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub observed_at_ms: u64,
    #[serde(default)]
    pub source_owned_permission_facts: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalIdentity {
    pub result_ref: TrackedResultRef,
    pub canonical_id: String,
    pub entity_kind: String,
    pub source_authority_ref: SourceAuthorityRef,
    #[serde(default)]
    pub native_anchors: Vec<NativeAnchor>,
    #[serde(default)]
    pub properties: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RelationshipConfidence {
    Observed,
    Declared,
    #[default]
    Inferred,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationshipAssertion {
    pub relationship_id: String,
    pub source_result_ref: TrackedResultRef,
    pub target_result_ref: TrackedResultRef,
    pub relationship_kind: String,
    #[serde(default)]
    pub authority_refs: Vec<SourceAuthorityRef>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derivation_rule: Option<String>,
    pub confidence: RelationshipConfidence,
    pub observed_revision: String,
    pub freshness: FreshnessState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedDataViewCommit {
    pub complete_payload_attachment: Vec<u8>,
    #[serde(default)]
    pub canonical_identities: Vec<CanonicalIdentity>,
    #[serde(default)]
    pub relationship_assertions: Vec<RelationshipAssertion>,
    #[serde(default)]
    pub dependency_refs: Vec<String>,
    #[serde(default)]
    pub normalization_evidence: Vec<String>,
    #[serde(default)]
    pub source_revision_order_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DataViewCommitStatus {
    Accepted,
    SafeMerged,
    RequiresRevalidation,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataViewCommitOutcome {
    pub submission_id: String,
    pub status: DataViewCommitStatus,
    pub data_view: DataViewRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_ref: Option<String>,
    #[serde(default)]
    pub conflict_version_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residual_code: Option<String>,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PayloadLifecycle {
    Actionable,
    Historical,
    Invalidated,
    Tombstoned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidationRecord {
    pub invalidation_id: String,
    pub result_ref: TrackedResultRef,
    pub reason_code: String,
    pub source_revision: String,
    pub observed_at_ms: u64,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub now_ms: u64,
    pub actionable_ttl_ms: u64,
    pub history_ttl_ms: u64,
    pub max_payload_versions: usize,
    pub max_payload_bytes: usize,
    #[serde(default)]
    pub source_actionable_ttl_ms: BTreeMap<SourceAuthorityRef, u64>,
    #[serde(default)]
    pub tenant_policy_labels_to_delete: BTreeSet<String>,
    #[serde(default)]
    pub compliance_delete_result_refs: BTreeSet<TrackedResultRef>,
    pub retain_minimum_tombstones: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RetentionReason {
    ActionableTtlExpired,
    HistoryTtlExpired,
    Capacity,
    SourcePolicy,
    TenantPolicy,
    ComplianceDeletion,
    DependencyInvalidated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionRecord {
    pub retention_id: String,
    pub version_ref: String,
    pub result_ref: TrackedResultRef,
    pub from: PayloadLifecycle,
    pub to: PayloadLifecycle,
    pub reason: RetentionReason,
    pub applied_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TombstoneRecord {
    pub tombstone_id: String,
    pub result_ref: TrackedResultRef,
    pub reason: RetentionReason,
    pub applied_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionOutcome {
    pub data_view: DataViewRef,
    #[serde(default)]
    pub retired_result_refs: Vec<TrackedResultRef>,
    #[serde(default)]
    pub evicted_version_refs: Vec<String>,
    #[serde(default)]
    pub tombstone_refs: Vec<String>,
}
