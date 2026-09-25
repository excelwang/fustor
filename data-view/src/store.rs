use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};

use crate::{
    ActionResultSubmission, CanonicalIdentity, DataViewCommitOutcome, DataViewCommitStatus,
    DataViewContractError, DataViewPersistence, DataViewRef, DerivationEvidence, FreshnessState,
    IdGenerator, InvalidationRecord, NativeAnchor, NormalizedDataViewCommit, PayloadEncoding,
    PayloadLifecycle, RelationshipAssertion, RelationshipConfidence, RetentionOutcome,
    RetentionPolicy, RetentionReason, RetentionRecord, SchemaVersionRef, SourceAuthorityRef,
    StreamingRefreshOperation, StreamingRefreshSubmission, StreamingSubscription,
    SystemIdGenerator, TombstoneRecord, TrackedResultRef, VolatileDataViewPersistence,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadAttachmentRecord {
    pub attachment_id: String,
    pub submission_id: String,
    pub schema: SchemaVersionRef,
    pub encoding: PayloadEncoding,
    pub payload: Vec<u8>,
}

// Branch-neutral storage carrier. Declared results leave source revision/anchors
// empty; their external revision and admission remain in the full manifest carrier.
struct CommitCarrier {
    submission_id: String, base_revision: DataViewRef, schema: SchemaVersionRef,
    encoding: PayloadEncoding, source_authority_ref: SourceAuthorityRef, source_revision: String,
    native_anchors: Vec<NativeAnchor>, evidence_refs: Vec<String>, policy_labels: BTreeSet<String>,
    grant_epoch: u64, freshness: FreshnessState, derivation: DerivationEvidence,
    observed_at_ms: u64, actual_cost_units: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadVersionRecord {
    pub version_ref: String,
    pub result_ref: TrackedResultRef,
    pub canonical_identity: CanonicalIdentity,
    pub attachment_ref: String,
    pub source_authority_ref: SourceAuthorityRef,
    pub source_revision: String,
    pub source_revision_order_key: Vec<u8>,
    pub schema: SchemaVersionRef,
    #[serde(default)]
    pub native_anchors: Vec<NativeAnchor>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub policy_labels: BTreeSet<String>,
    pub grant_epoch: u64,
    pub freshness: FreshnessState,
    #[serde(default)]
    pub confidence: RelationshipConfidence,
    pub derivation: DerivationEvidence,
    pub observed_at_ms: u64,
    pub lifecycle: PayloadLifecycle,
    #[serde(default)]
    pub dependency_refs: Vec<String>,
    #[serde(default)]
    pub normalization_evidence: Vec<String>,
    pub committed_revision_id: String,
    pub selection_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationshipVersionRecord {
    pub version_ref: String,
    pub assertion: RelationshipAssertion,
    pub committed_revision_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphDeltaRecord {
    pub delta_ref: String,
    pub submission_id: String,
    pub base_revision: DataViewRef,
    pub resulting_revision: DataViewRef,
    #[serde(default)]
    pub added_result_refs: Vec<TrackedResultRef>,
    #[serde(default)]
    pub refreshed_result_refs: Vec<TrackedResultRef>,
    #[serde(default)]
    pub invalidated_result_refs: Vec<TrackedResultRef>,
    #[serde(default)]
    pub deleted_result_refs: Vec<TrackedResultRef>,
    #[serde(default)]
    pub conflict_version_refs: Vec<String>,
    pub actual_cost_units: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataViewSnapshot {
    pub data_view: DataViewRef,
    pub selected_payload_versions: BTreeMap<TrackedResultRef, String>,
    pub selected_relationship_versions: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataViewStoreMetrics {
    pub revision_count: usize,
    pub attachment_count: usize,
    pub payload_version_count: usize,
    pub relationship_version_count: usize,
    pub invalidation_count: usize,
    pub retention_record_count: usize,
    pub tombstone_count: usize,
    pub delta_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataViewRevisionRecord {
    data_view: DataViewRef,
    parent_revision_id: Option<String>,
    internal_sequence: u64,
    selected_payload_versions: BTreeMap<TrackedResultRef, String>,
    selected_relationship_versions: BTreeMap<String, String>,
    changed_result_refs: BTreeSet<TrackedResultRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScopedRevisionAliasRecord {
    public_ref: DataViewRef,
    internal_ref: DataViewRef,
    subject_scope_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedDataViewCommit {
    attachments: Vec<PayloadAttachmentRecord>,
    payload_versions: Vec<PayloadVersionRecord>,
    relationship_versions: Vec<RelationshipVersionRecord>,
    invalidations: Vec<InvalidationRecord>,
    revision: DataViewRevisionRecord,
    delta: GraphDeltaRecord,
    pub(crate) outcome: DataViewCommitOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedDataViewState {
    data_view_id: String,
    head_revision_id: String,
    revisions: BTreeMap<String, DataViewRevisionRecord>,
    attachments: BTreeMap<String, PayloadAttachmentRecord>,
    payload_versions: BTreeMap<String, PayloadVersionRecord>,
    relationship_versions: BTreeMap<String, RelationshipVersionRecord>,
    invalidations: BTreeMap<String, InvalidationRecord>,
    #[serde(default)]
    retention_records: BTreeMap<String, RetentionRecord>,
    #[serde(default)]
    tombstones: BTreeMap<String, TombstoneRecord>,
    #[serde(default)]
    unavailable_revisions: BTreeSet<String>,
    deltas: BTreeMap<String, GraphDeltaRecord>,
    #[serde(default)]
    control_state: Vec<u8>,
    #[serde(default)]
    scoped_revision_aliases: BTreeMap<String, ScopedRevisionAliasRecord>,
    #[serde(default)]
    scoped_aliases_by_subject: BTreeMap<String, BTreeMap<String, String>>,
}

pub struct DataViewStore {
    id_generator: Arc<dyn IdGenerator>,
    persistence: Arc<dyn DataViewPersistence>,
    data_view_id: String,
    head_revision_id: String,
    revisions: BTreeMap<String, DataViewRevisionRecord>,
    attachments: BTreeMap<String, PayloadAttachmentRecord>,
    payload_versions: BTreeMap<String, PayloadVersionRecord>,
    relationship_versions: BTreeMap<String, RelationshipVersionRecord>,
    invalidations: BTreeMap<String, InvalidationRecord>,
    retention_records: BTreeMap<String, RetentionRecord>,
    tombstones: BTreeMap<String, TombstoneRecord>,
    unavailable_revisions: BTreeSet<String>,
    deltas: BTreeMap<String, GraphDeltaRecord>,
    control_state: Vec<u8>,
    scoped_revision_aliases: BTreeMap<String, ScopedRevisionAliasRecord>,
    scoped_aliases_by_subject: BTreeMap<String, BTreeMap<String, String>>,
}

impl DataViewStore {
    pub fn new(data_view_id: impl Into<String>) -> Result<Self, DataViewContractError> {
        Self::with_id_generator(data_view_id, Arc::new(SystemIdGenerator))
    }

    pub fn with_id_generator(
        data_view_id: impl Into<String>,
        id_generator: Arc<dyn IdGenerator>,
    ) -> Result<Self, DataViewContractError> {
        Self::with_persistence(
            data_view_id,
            id_generator,
            Arc::new(VolatileDataViewPersistence),
        )
    }

    pub fn with_persistence(
        data_view_id: impl Into<String>,
        id_generator: Arc<dyn IdGenerator>,
        persistence: Arc<dyn DataViewPersistence>,
    ) -> Result<Self, DataViewContractError> {
        let data_view_id = data_view_id.into();
        if data_view_id.trim().is_empty() {
            return Err(DataViewContractError::InvalidContract("data_view_id"));
        }
        if let Some(encoded) = persistence.load()? {
            let state: PersistedDataViewState = serde_json::from_slice(&encoded)
                .map_err(|_| DataViewContractError::PersistenceFailed)?;
            if state.data_view_id != data_view_id
                || !state.revisions.contains_key(&state.head_revision_id)
            {
                return Err(DataViewContractError::PersistenceFailed);
            }
            return Ok(Self::from_persisted_state(state, id_generator, persistence));
        }
        let genesis_revision_id = id_generator.next_id("revision")?;
        let genesis_ref = DataViewRef {
            data_view_id: data_view_id.clone(),
            revision_id: genesis_revision_id.clone(),
        };
        let genesis = DataViewRevisionRecord {
            data_view: genesis_ref.clone(),
            parent_revision_id: None,
            internal_sequence: 0,
            selected_payload_versions: BTreeMap::new(),
            selected_relationship_versions: BTreeMap::new(),
            changed_result_refs: BTreeSet::new(),
        };
        let mut revisions = BTreeMap::new();
        revisions.insert(genesis_revision_id.clone(), genesis);
        let store = Self {
            id_generator,
            persistence,
            data_view_id,
            head_revision_id: genesis_revision_id,
            revisions,
            attachments: BTreeMap::new(),
            payload_versions: BTreeMap::new(),
            relationship_versions: BTreeMap::new(),
            invalidations: BTreeMap::new(),
            retention_records: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            unavailable_revisions: BTreeSet::new(),
            deltas: BTreeMap::new(),
            control_state: Vec::new(),
            scoped_revision_aliases: BTreeMap::new(),
            scoped_aliases_by_subject: BTreeMap::new(),
        };
        store.persist_current_state()?;
        Ok(store)
    }

    pub fn head_ref(&self) -> DataViewRef {
        DataViewRef {
            data_view_id: self.data_view_id.clone(),
            revision_id: self.head_revision_id.clone(),
        }
    }

    pub fn snapshot(&self, data_view: &DataViewRef) -> Option<DataViewSnapshot> {
        if data_view.data_view_id != self.data_view_id {
            return None;
        }
        if self.unavailable_revisions.contains(&data_view.revision_id) {
            return None;
        }
        self.revisions
            .get(&data_view.revision_id)
            .map(|record| DataViewSnapshot {
                data_view: record.data_view.clone(),
                selected_payload_versions: record.selected_payload_versions.clone(),
                selected_relationship_versions: record.selected_relationship_versions.clone(),
            })
    }

    pub fn payload_version(&self, version_ref: &str) -> Option<&PayloadVersionRecord> {
        self.payload_versions.get(version_ref)
    }

    pub fn relationship_version(&self, version_ref: &str) -> Option<&RelationshipVersionRecord> {
        self.relationship_versions.get(version_ref)
    }

    pub fn attachment(&self, attachment_ref: &str) -> Option<&PayloadAttachmentRecord> {
        self.attachments.get(attachment_ref)
    }

    pub fn graph_delta(&self, delta_ref: &str) -> Option<&GraphDeltaRecord> {
        self.deltas.get(delta_ref)
    }

    pub(crate) fn control_state(&self) -> &[u8] {
        &self.control_state
    }

    pub(crate) fn persist_control_state(
        &mut self,
        control_state: Vec<u8>,
    ) -> Result<(), DataViewContractError> {
        let mut next = self.persisted_state();
        next.control_state = control_state;
        self.persist_and_replace(next)
    }

    pub(crate) fn resolve_scoped_revision_alias(
        &self,
        public_ref: &DataViewRef,
        subject_scope_id: &str,
    ) -> Option<DataViewRef> {
        self.scoped_revision_aliases
            .get(&public_ref.revision_id)
            .filter(|record| {
                record.public_ref == *public_ref && record.subject_scope_id == subject_scope_id
            })
            .map(|record| record.internal_ref.clone())
    }

    pub(crate) fn get_or_create_scoped_revision_alias(
        &mut self,
        subject_scope_id: &str,
        visible_digest: &str,
        internal_ref: &DataViewRef,
    ) -> Result<DataViewRef, DataViewContractError> {
        if subject_scope_id.trim().is_empty()
            || visible_digest.trim().is_empty()
            || internal_ref.data_view_id != self.data_view_id
            || self.snapshot(internal_ref).is_none()
        {
            return Err(DataViewContractError::InvalidContract(
                "scoped revision alias",
            ));
        }
        if let Some(public_revision_id) = self
            .scoped_aliases_by_subject
            .get(subject_scope_id)
            .and_then(|aliases| aliases.get(visible_digest))
            && let Some(record) = self.scoped_revision_aliases.get(public_revision_id)
            && self.snapshot(&record.internal_ref).is_some()
        {
            return Ok(record.public_ref.clone());
        }

        let public_ref = DataViewRef {
            data_view_id: self.data_view_id.clone(),
            revision_id: self.id_generator.next_id("scoped-revision")?,
        };
        let mut next = self.persisted_state();
        next.scoped_revision_aliases.insert(
            public_ref.revision_id.clone(),
            ScopedRevisionAliasRecord {
                public_ref: public_ref.clone(),
                internal_ref: internal_ref.clone(),
                subject_scope_id: subject_scope_id.to_string(),
            },
        );
        next.scoped_aliases_by_subject
            .entry(subject_scope_id.to_string())
            .or_default()
            .insert(visible_digest.to_string(), public_ref.revision_id.clone());
        self.persist_and_replace(next)?;
        Ok(public_ref)
    }

    pub fn tracked_result_refs(&self) -> BTreeSet<TrackedResultRef> {
        self.revisions
            .get(&self.head_revision_id)
            .map(|record| record.selected_payload_versions.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn metrics(&self) -> DataViewStoreMetrics {
        DataViewStoreMetrics {
            revision_count: self.revisions.len(),
            attachment_count: self.attachments.len(),
            payload_version_count: self.payload_versions.len(),
            relationship_version_count: self.relationship_versions.len(),
            invalidation_count: self.invalidations.len(),
            retention_record_count: self.retention_records.len(),
            tombstone_count: self.tombstones.len(),
            delta_count: self.deltas.len(),
        }
    }

    pub fn tombstone(&self, tombstone_ref: &str) -> Option<&TombstoneRecord> {
        self.tombstones.get(tombstone_ref)
    }

    pub fn apply_retention(
        &mut self,
        policy: &RetentionPolicy,
    ) -> Result<RetentionOutcome, DataViewContractError> {
        if policy.actionable_ttl_ms == 0
            || policy.history_ttl_ms == 0
            || policy.max_payload_versions == 0
            || policy.max_payload_bytes == 0
        {
            return Err(DataViewContractError::InvalidContract("retention policy"));
        }
        let mut next = self.persisted_state();
        let current_head = next
            .revisions
            .get(&next.head_revision_id)
            .cloned()
            .ok_or(DataViewContractError::NotFound("head revision"))?;
        let mut retire_reasons = BTreeMap::<TrackedResultRef, RetentionReason>::new();
        for (result_ref, version_ref) in &current_head.selected_payload_versions {
            let version = next
                .payload_versions
                .get(version_ref)
                .ok_or(DataViewContractError::NotFound("selected payload version"))?;
            let reason = if policy.compliance_delete_result_refs.contains(result_ref) {
                Some(RetentionReason::ComplianceDeletion)
            } else if version
                .policy_labels
                .iter()
                .any(|label| policy.tenant_policy_labels_to_delete.contains(label))
            {
                Some(RetentionReason::TenantPolicy)
            } else {
                let source_ttl = policy
                    .source_actionable_ttl_ms
                    .get(&version.source_authority_ref)
                    .copied();
                let ttl = source_ttl.unwrap_or(policy.actionable_ttl_ms);
                (policy.now_ms.saturating_sub(version.observed_at_ms) >= ttl).then_some(
                    if source_ttl.is_some() {
                        RetentionReason::SourcePolicy
                    } else {
                        RetentionReason::ActionableTtlExpired
                    },
                )
            };
            if let Some(reason) = reason {
                retire_reasons.insert(result_ref.clone(), reason);
            }
        }
        let dependency_roots = retire_reasons.keys().cloned().collect::<Vec<_>>();
        for root in dependency_roots {
            for dependent in dependent_results_in_state(
                &next,
                &root,
                &current_head.selected_payload_versions,
                10_000,
            )? {
                retire_reasons
                    .entry(dependent)
                    .or_insert(RetentionReason::DependencyInvalidated);
            }
        }

        let mut retired_result_refs = Vec::new();
        let mut tombstone_refs = Vec::new();
        if !retire_reasons.is_empty() {
            let revision_id = self.id_generator.next_id("revision")?;
            let delta_ref = self.id_generator.next_id("delta")?;
            let resulting_revision = DataViewRef {
                data_view_id: next.data_view_id.clone(),
                revision_id: revision_id.clone(),
            };
            let mut selected_payload_versions = current_head.selected_payload_versions.clone();
            let mut selected_relationship_versions =
                current_head.selected_relationship_versions.clone();
            let mut changed_result_refs = BTreeSet::new();
            for (result_ref, reason) in &retire_reasons {
                let Some(version_ref) = selected_payload_versions.remove(result_ref) else {
                    continue;
                };
                let version = next
                    .payload_versions
                    .get(&version_ref)
                    .ok_or(DataViewContractError::NotFound("payload version"))?;
                let invalidation_id = self.id_generator.next_id("invalidation")?;
                next.invalidations.insert(
                    invalidation_id.clone(),
                    InvalidationRecord {
                        invalidation_id,
                        result_ref: result_ref.clone(),
                        reason_code: retention_reason_code(reason).into(),
                        source_revision: version.source_revision.clone(),
                        observed_at_ms: policy.now_ms,
                        evidence_refs: Vec::new(),
                    },
                );
                let retention_id = self.id_generator.next_id("retention")?;
                next.retention_records.insert(
                    retention_id.clone(),
                    RetentionRecord {
                        retention_id,
                        version_ref: version_ref.clone(),
                        result_ref: result_ref.clone(),
                        from: PayloadLifecycle::Actionable,
                        to: if matches!(
                            reason,
                            RetentionReason::ComplianceDeletion | RetentionReason::TenantPolicy
                        ) {
                            PayloadLifecycle::Tombstoned
                        } else {
                            PayloadLifecycle::Historical
                        },
                        reason: reason.clone(),
                        applied_at_ms: policy.now_ms,
                    },
                );
                if policy.retain_minimum_tombstones
                    && matches!(
                        reason,
                        RetentionReason::ComplianceDeletion | RetentionReason::TenantPolicy
                    )
                {
                    let tombstone_id = self.id_generator.next_id("tombstone")?;
                    next.tombstones.insert(
                        tombstone_id.clone(),
                        TombstoneRecord {
                            tombstone_id: tombstone_id.clone(),
                            result_ref: result_ref.clone(),
                            reason: reason.clone(),
                            applied_at_ms: policy.now_ms,
                        },
                    );
                    tombstone_refs.push(tombstone_id);
                }
                selected_relationship_versions.retain(|_, relationship_version_ref| {
                    next.relationship_versions
                        .get(relationship_version_ref)
                        .map(|relationship| {
                            relationship.assertion.source_result_ref != *result_ref
                                && relationship.assertion.target_result_ref != *result_ref
                        })
                        .unwrap_or(false)
                });
                changed_result_refs.insert(result_ref.clone());
                retired_result_refs.push(result_ref.clone());
            }
            let revision = DataViewRevisionRecord {
                data_view: resulting_revision.clone(),
                parent_revision_id: Some(current_head.data_view.revision_id.clone()),
                internal_sequence: current_head.internal_sequence + 1,
                selected_payload_versions,
                selected_relationship_versions,
                changed_result_refs,
            };
            next.deltas.insert(
                delta_ref.clone(),
                GraphDeltaRecord {
                    delta_ref,
                    submission_id: "retention".into(),
                    base_revision: current_head.data_view.clone(),
                    resulting_revision: resulting_revision.clone(),
                    added_result_refs: Vec::new(),
                    refreshed_result_refs: Vec::new(),
                    invalidated_result_refs: retired_result_refs.clone(),
                    deleted_result_refs: retire_reasons
                        .iter()
                        .filter(|(_, reason)| {
                            matches!(
                                reason,
                                RetentionReason::ComplianceDeletion | RetentionReason::TenantPolicy
                            )
                        })
                        .map(|(result_ref, _)| result_ref.clone())
                        .collect(),
                    conflict_version_refs: Vec::new(),
                    actual_cost_units: 0,
                },
            );
            next.head_revision_id = revision_id.clone();
            next.revisions.insert(revision_id, revision);
        }

        let head = next
            .revisions
            .get(&next.head_revision_id)
            .cloned()
            .ok_or(DataViewContractError::NotFound("head revision"))?;
        let protected_versions = protected_payload_versions(&next, &head)?;
        let mut evict_candidates = next
            .payload_versions
            .values()
            .filter(|version| !protected_versions.contains(&version.version_ref))
            .map(|version| {
                let mandatory = policy
                    .compliance_delete_result_refs
                    .contains(&version.result_ref)
                    || version
                        .policy_labels
                        .iter()
                        .any(|label| policy.tenant_policy_labels_to_delete.contains(label));
                let history_expired =
                    policy.now_ms.saturating_sub(version.observed_at_ms) >= policy.history_ttl_ms;
                (
                    version.version_ref.clone(),
                    version.result_ref.clone(),
                    version.observed_at_ms,
                    mandatory,
                    history_expired,
                )
            })
            .collect::<Vec<_>>();
        evict_candidates.sort_by_key(|candidate| candidate.2);

        let mut selected_for_eviction = BTreeSet::new();
        for (version_ref, _, _, mandatory, history_expired) in &evict_candidates {
            if *mandatory || *history_expired {
                selected_for_eviction.insert(version_ref.clone());
            }
        }
        let mut remaining_count = next
            .payload_versions
            .len()
            .saturating_sub(selected_for_eviction.len());
        let mut remaining_bytes = payload_bytes_excluding(&next, &selected_for_eviction);
        for (version_ref, _, _, _, _) in &evict_candidates {
            if remaining_count <= policy.max_payload_versions
                && remaining_bytes <= policy.max_payload_bytes
            {
                break;
            }
            if selected_for_eviction.insert(version_ref.clone()) {
                remaining_count = remaining_count.saturating_sub(1);
                remaining_bytes = payload_bytes_excluding(&next, &selected_for_eviction);
            }
        }

        let mut evicted_version_refs = Vec::new();
        for version_ref in selected_for_eviction {
            let Some(version) = next.payload_versions.remove(&version_ref) else {
                continue;
            };
            let reason = if policy
                .compliance_delete_result_refs
                .contains(&version.result_ref)
            {
                RetentionReason::ComplianceDeletion
            } else if version
                .policy_labels
                .iter()
                .any(|label| policy.tenant_policy_labels_to_delete.contains(label))
            {
                RetentionReason::TenantPolicy
            } else if policy.now_ms.saturating_sub(version.observed_at_ms) >= policy.history_ttl_ms
            {
                RetentionReason::HistoryTtlExpired
            } else {
                RetentionReason::Capacity
            };
            let retention_id = self.id_generator.next_id("retention")?;
            next.retention_records.insert(
                retention_id.clone(),
                RetentionRecord {
                    retention_id,
                    version_ref: version_ref.clone(),
                    result_ref: version.result_ref.clone(),
                    from: PayloadLifecycle::Historical,
                    to: if matches!(
                        reason,
                        RetentionReason::ComplianceDeletion | RetentionReason::TenantPolicy
                    ) {
                        PayloadLifecycle::Tombstoned
                    } else {
                        PayloadLifecycle::Historical
                    },
                    reason: reason.clone(),
                    applied_at_ms: policy.now_ms,
                },
            );
            if policy.retain_minimum_tombstones
                && !next
                    .tombstones
                    .values()
                    .any(|tombstone| tombstone.result_ref == version.result_ref)
            {
                let tombstone_id = self.id_generator.next_id("tombstone")?;
                next.tombstones.insert(
                    tombstone_id.clone(),
                    TombstoneRecord {
                        tombstone_id: tombstone_id.clone(),
                        result_ref: version.result_ref.clone(),
                        reason,
                        applied_at_ms: policy.now_ms,
                    },
                );
                tombstone_refs.push(tombstone_id);
            }
            let attachment_ref = version.attachment_ref;
            if !next
                .payload_versions
                .values()
                .any(|retained| retained.attachment_ref == attachment_ref)
            {
                next.attachments.remove(&attachment_ref);
            }
            for revision in next.revisions.values() {
                if revision
                    .selected_payload_versions
                    .values()
                    .any(|selected| selected == &version_ref)
                    && revision.data_view.revision_id != next.head_revision_id
                {
                    next.unavailable_revisions
                        .insert(revision.data_view.revision_id.clone());
                }
            }
            next.relationship_versions.retain(|_, relationship| {
                relationship.assertion.source_result_ref != version.result_ref
                    && relationship.assertion.target_result_ref != version.result_ref
            });
            evicted_version_refs.push(version_ref);
        }

        let encoded =
            serde_json::to_vec(&next).map_err(|_| DataViewContractError::SerializationFailed)?;
        self.persistence.persist(&encoded)?;
        let data_view = DataViewRef {
            data_view_id: next.data_view_id.clone(),
            revision_id: next.head_revision_id.clone(),
        };
        self.replace_persisted_state(next);
        retired_result_refs.sort();
        retired_result_refs.dedup();
        evicted_version_refs.sort();
        tombstone_refs.sort();
        tombstone_refs.dedup();
        Ok(RetentionOutcome {
            data_view,
            retired_result_refs,
            evicted_version_refs,
            tombstone_refs,
        })
    }

    pub(crate) fn prepare_action_commit(
        &self,
        submission: &ActionResultSubmission,
        normalized: NormalizedDataViewCommit,
    ) -> Result<PreparedDataViewCommit, DataViewContractError> {
        self.prepare_result_commit(&CommitCarrier {
            submission_id: submission.submission_id.clone(), base_revision: submission.execution.base_revision.clone(),
            schema: submission.schema.clone(), encoding: submission.encoding.clone(), source_authority_ref: submission.source_authority_ref.clone(),
            source_revision: submission.source_revision.clone(), native_anchors: submission.native_anchors.clone(), evidence_refs: submission.evidence_refs.clone(),
            policy_labels: submission.policy_labels.clone(), grant_epoch: submission.grant_epoch, freshness: submission.freshness.clone(),
            derivation: submission.derivation.clone(), observed_at_ms: submission.observed_at_ms, actual_cost_units: submission.actual_cost_units,
        }, normalized)
    }

    pub(crate) fn prepare_declared_commit(&self, submission: &crate::DeclaredResultSubmission, normalized: NormalizedDataViewCommit) -> Result<PreparedDataViewCommit, DataViewContractError> {
        let manifest = &submission.manifest;
        self.prepare_result_commit(&CommitCarrier {
            submission_id: submission.submission_id.clone(), base_revision: submission.admission.base_revision.clone(), schema: manifest.schema.clone(),
            encoding: PayloadEncoding::Json, source_authority_ref: submission.admission.policy.result_authority_ref.clone(),
            source_revision: String::new(), native_anchors: Vec::new(),
            evidence_refs: manifest.evidence_refs.iter().chain(&manifest.validation_refs).chain(&manifest.lineage_refs).cloned().collect(),
            policy_labels: manifest.policy_labels.clone(), grant_epoch: submission.admission.policy.grant_epoch, freshness: manifest.freshness.clone(),
            derivation: submission.derivation.clone(), observed_at_ms: manifest.observed_or_produced_at_ms, actual_cost_units: 0,
        }, normalized)
    }

    fn prepare_result_commit(&self, submission: &CommitCarrier, normalized: NormalizedDataViewCommit) -> Result<PreparedDataViewCommit, DataViewContractError> {
        let base_revision = self.resolve_revision(&submission.base_revision)?;
        let head = self
            .revisions
            .get(&self.head_revision_id)
            .ok_or(DataViewContractError::NotFound("head revision"))?;
        if !self.is_ancestor(
            &base_revision.data_view.revision_id,
            &head.data_view.revision_id,
        ) {
            return Err(DataViewContractError::RequiresRevalidation);
        }
        let candidate_refs = normalized
            .canonical_identities
            .iter()
            .map(|identity| identity.result_ref.clone())
            .collect::<BTreeSet<_>>();
        if candidate_refs.is_empty()
            || candidate_refs.len() != normalized.canonical_identities.len()
        {
            return Err(DataViewContractError::InvalidContract(
                "canonical_identities",
            ));
        }
        validate_dependencies(&normalized.dependency_refs, &candidate_refs, head, self)?;
        validate_relationships(&normalized.relationship_assertions, &candidate_refs, head)?;

        let intervening_refs = self.changed_results_after(base_revision.internal_sequence);
        let stale = base_revision.data_view.revision_id != head.data_view.revision_id;
        let stale_overlap = candidate_refs
            .intersection(&intervening_refs)
            .cloned()
            .collect::<BTreeSet<_>>();

        let attachment_id = self.id_generator.next_id("attachment")?;
        let revision_id = self.id_generator.next_id("revision")?;
        let delta_ref = self.id_generator.next_id("delta")?;
        let resulting_revision = DataViewRef {
            data_view_id: self.data_view_id.clone(),
            revision_id: revision_id.clone(),
        };
        let attachment = PayloadAttachmentRecord {
            attachment_id: attachment_id.clone(),
            submission_id: submission.submission_id.clone(),
            schema: submission.schema.clone(),
            encoding: submission.encoding.clone(),
            payload: normalized.complete_payload_attachment,
        };

        let mut selected_payload_versions = head.selected_payload_versions.clone();
        let mut payload_versions = Vec::with_capacity(normalized.canonical_identities.len());
        let mut conflict_version_refs = Vec::new();
        let mut added_result_refs = Vec::new();
        let mut refreshed_result_refs = Vec::new();
        for identity in normalized.canonical_identities {
            let version_ref = self.id_generator.next_id("payload-version")?;
            let selection_key = payload_selection_key(
                &identity,
                &submission.source_revision,
                &normalized.source_revision_order_key,
                &attachment.payload,
                &submission.evidence_refs,
            );
            let candidate = PayloadVersionRecord {
                version_ref: version_ref.clone(),
                result_ref: identity.result_ref.clone(),
                canonical_identity: identity.clone(),
                attachment_ref: attachment_id.clone(),
                source_authority_ref: submission.source_authority_ref.clone(),
                source_revision: submission.source_revision.clone(),
                source_revision_order_key: normalized.source_revision_order_key.clone(),
                schema: submission.schema.clone(),
                native_anchors: submission.native_anchors.clone(),
                evidence_refs: submission.evidence_refs.clone(),
                policy_labels: submission.policy_labels.clone(),
                grant_epoch: submission.grant_epoch,
                freshness: submission.freshness.clone(),
                confidence: confidence_from_derivation(&submission.derivation),
                derivation: submission.derivation.clone(),
                observed_at_ms: submission.observed_at_ms,
                lifecycle: PayloadLifecycle::Actionable,
                dependency_refs: normalized.dependency_refs.clone(),
                normalization_evidence: normalized.normalization_evidence.clone(),
                committed_revision_id: revision_id.clone(),
                selection_key,
            };
            if let Some(existing_ref) = selected_payload_versions.get(&identity.result_ref) {
                refreshed_result_refs.push(identity.result_ref.clone());
                let existing = self
                    .payload_versions
                    .get(existing_ref)
                    .ok_or(DataViewContractError::NotFound("selected payload version"))?;
                if existing.selection_key != candidate.selection_key {
                    conflict_version_refs.push(existing.version_ref.clone());
                    conflict_version_refs.push(candidate.version_ref.clone());
                }
                if select_candidate(existing, &candidate) {
                    selected_payload_versions
                        .insert(identity.result_ref.clone(), candidate.version_ref.clone());
                }
            } else {
                added_result_refs.push(identity.result_ref.clone());
                selected_payload_versions
                    .insert(identity.result_ref.clone(), candidate.version_ref.clone());
            }
            payload_versions.push(candidate);
        }

        let mut selected_relationship_versions = head.selected_relationship_versions.clone();
        let mut relationship_versions = Vec::new();
        for assertion in normalized.relationship_assertions {
            let version_ref = self.id_generator.next_id("relationship-version")?;
            selected_relationship_versions
                .insert(assertion.relationship_id.clone(), version_ref.clone());
            relationship_versions.push(RelationshipVersionRecord {
                version_ref,
                assertion,
                committed_revision_id: revision_id.clone(),
            });
        }

        conflict_version_refs.sort();
        conflict_version_refs.dedup();
        let status = if !stale {
            DataViewCommitStatus::Accepted
        } else if stale_overlap.is_empty() {
            DataViewCommitStatus::SafeMerged
        } else {
            DataViewCommitStatus::Accepted
        };
        let revision = DataViewRevisionRecord {
            data_view: resulting_revision.clone(),
            parent_revision_id: Some(head.data_view.revision_id.clone()),
            internal_sequence: head.internal_sequence + 1,
            selected_payload_versions,
            selected_relationship_versions,
            changed_result_refs: candidate_refs,
        };
        let delta = GraphDeltaRecord {
            delta_ref: delta_ref.clone(),
            submission_id: submission.submission_id.clone(),
            base_revision: submission.base_revision.clone(),
            resulting_revision: resulting_revision.clone(),
            added_result_refs,
            refreshed_result_refs,
            invalidated_result_refs: Vec::new(),
            deleted_result_refs: Vec::new(),
            conflict_version_refs: conflict_version_refs.clone(),
            actual_cost_units: submission.actual_cost_units,
        };
        let outcome = DataViewCommitOutcome {
            submission_id: submission.submission_id.clone(),
            status,
            data_view: resulting_revision,
            delta_ref: Some(delta_ref),
            conflict_version_refs,
            residual_code: None,
            retryable: false,
        };
        Ok(PreparedDataViewCommit {
            attachments: vec![attachment],
            payload_versions,
            relationship_versions,
            invalidations: Vec::new(),
            revision,
            delta,
            outcome,
        })
    }

    pub(crate) fn prepare_stream_commit(
        &self,
        subscription: &StreamingSubscription,
        submission: &StreamingRefreshSubmission,
        mut normalized_refreshes: BTreeMap<TrackedResultRef, NormalizedDataViewCommit>,
    ) -> Result<PreparedDataViewCommit, DataViewContractError> {
        let head = self
            .revisions
            .get(&self.head_revision_id)
            .ok_or(DataViewContractError::NotFound("head revision"))?;
        let revision_id = self.id_generator.next_id("revision")?;
        let delta_ref = self.id_generator.next_id("delta")?;
        let resulting_revision = DataViewRef {
            data_view_id: self.data_view_id.clone(),
            revision_id: revision_id.clone(),
        };
        let mut selected_payload_versions = head.selected_payload_versions.clone();
        let mut selected_relationship_versions = head.selected_relationship_versions.clone();
        let mut attachments = Vec::new();
        let mut payload_versions = Vec::new();
        let mut relationship_versions = Vec::new();
        let mut invalidations = Vec::new();
        let mut changed_result_refs = BTreeSet::new();
        let mut refreshed_result_refs = Vec::new();
        let mut invalidated_result_refs = Vec::new();
        let mut deleted_result_refs = Vec::new();
        let mut conflict_version_refs = Vec::new();

        for entry in &submission.refreshes {
            let existing_ref = selected_payload_versions
                .get(&entry.tracked_result_ref)
                .cloned()
                .ok_or(DataViewContractError::TrackedResultUnavailable)?;
            changed_result_refs.insert(entry.tracked_result_ref.clone());
            match entry.operation {
                StreamingRefreshOperation::Refresh => {
                    let normalized = normalized_refreshes
                        .remove(&entry.tracked_result_ref)
                        .ok_or(DataViewContractError::InvalidContract(
                            "normalized stream refresh",
                        ))?;
                    if normalized.canonical_identities.len() != 1
                        || normalized.canonical_identities[0].result_ref != entry.tracked_result_ref
                        || normalized.canonical_identities[0].source_authority_ref
                            != subscription.source_authority_ref
                    {
                        return Err(DataViewContractError::InvalidContract(
                            "stream refresh identity",
                        ));
                    }
                    let identity = normalized.canonical_identities[0].clone();
                    let attachment_id = self.id_generator.next_id("attachment")?;
                    let attachment = PayloadAttachmentRecord {
                        attachment_id: attachment_id.clone(),
                        submission_id: submission.submission_id.clone(),
                        schema: submission.schema.clone(),
                        encoding: submission.encoding.clone(),
                        payload: normalized.complete_payload_attachment,
                    };
                    let version_ref = self.id_generator.next_id("payload-version")?;
                    let freshness = submission
                        .projection_evidence
                        .as_ref()
                        .map(|evidence| evidence.freshness.clone())
                        .unwrap_or(FreshnessState::Fresh);
                    let selection_key = payload_selection_key(
                        &identity,
                        &entry.source_revision,
                        &normalized.source_revision_order_key,
                        &attachment.payload,
                        &entry.evidence_refs,
                    );
                    let candidate = PayloadVersionRecord {
                        version_ref: version_ref.clone(),
                        result_ref: entry.tracked_result_ref.clone(),
                        canonical_identity: identity,
                        attachment_ref: attachment_id,
                        source_authority_ref: subscription.source_authority_ref.clone(),
                        source_revision: entry.source_revision.clone(),
                        source_revision_order_key: normalized.source_revision_order_key.clone(),
                        schema: submission.schema.clone(),
                        native_anchors: entry.native_anchors.clone(),
                        evidence_refs: entry.evidence_refs.clone(),
                        policy_labels: BTreeSet::new(),
                        grant_epoch: subscription.grant_epoch,
                        freshness,
                        confidence: RelationshipConfidence::Observed,
                        derivation: DerivationEvidence {
                            derivation_kind: "stream-refresh".into(),
                            rule_ref: None,
                            dependency_refs: normalized.dependency_refs.clone(),
                        },
                        observed_at_ms: entry.observed_at_ms,
                        lifecycle: PayloadLifecycle::Actionable,
                        dependency_refs: normalized.dependency_refs,
                        normalization_evidence: normalized.normalization_evidence,
                        committed_revision_id: revision_id.clone(),
                        selection_key,
                    };
                    let existing = self
                        .payload_versions
                        .get(&existing_ref)
                        .ok_or(DataViewContractError::NotFound("selected payload version"))?;
                    if existing.selection_key != candidate.selection_key {
                        conflict_version_refs.push(existing.version_ref.clone());
                        conflict_version_refs.push(candidate.version_ref.clone());
                    }
                    if select_candidate(existing, &candidate) {
                        selected_payload_versions.insert(
                            entry.tracked_result_ref.clone(),
                            candidate.version_ref.clone(),
                        );
                    }
                    validate_relationships(
                        &normalized.relationship_assertions,
                        &BTreeSet::from([entry.tracked_result_ref.clone()]),
                        head,
                    )?;
                    for assertion in normalized.relationship_assertions {
                        let relationship_version_ref =
                            self.id_generator.next_id("relationship-version")?;
                        selected_relationship_versions.insert(
                            assertion.relationship_id.clone(),
                            relationship_version_ref.clone(),
                        );
                        relationship_versions.push(RelationshipVersionRecord {
                            version_ref: relationship_version_ref,
                            assertion,
                            committed_revision_id: revision_id.clone(),
                        });
                    }
                    attachments.push(attachment);
                    payload_versions.push(candidate);
                    refreshed_result_refs.push(entry.tracked_result_ref.clone());
                }
                StreamingRefreshOperation::Invalidate | StreamingRefreshOperation::Delete => {
                    let invalidation_id = self.id_generator.next_id("invalidation")?;
                    let deleted = entry.operation == StreamingRefreshOperation::Delete;
                    invalidations.push(InvalidationRecord {
                        invalidation_id,
                        result_ref: entry.tracked_result_ref.clone(),
                        reason_code: if deleted {
                            "source-delete".into()
                        } else {
                            "source-invalidate".into()
                        },
                        source_revision: entry.source_revision.clone(),
                        observed_at_ms: entry.observed_at_ms,
                        evidence_refs: entry.evidence_refs.clone(),
                    });
                    selected_payload_versions.remove(&entry.tracked_result_ref);
                    selected_relationship_versions.retain(|_, version_ref| {
                        self.relationship_versions
                            .get(version_ref)
                            .map(|version| {
                                version.assertion.source_result_ref != entry.tracked_result_ref
                                    && version.assertion.target_result_ref
                                        != entry.tracked_result_ref
                            })
                            .unwrap_or(false)
                    });
                    if deleted {
                        deleted_result_refs.push(entry.tracked_result_ref.clone());
                    } else {
                        invalidated_result_refs.push(entry.tracked_result_ref.clone());
                    }

                    let dependent_results = self.dependent_results_transitive(
                        &entry.tracked_result_ref,
                        &selected_payload_versions,
                        10_000,
                    )?;
                    for dependent in dependent_results {
                        if !selected_payload_versions.contains_key(&dependent) {
                            continue;
                        }
                        let dependency_invalidation_id =
                            self.id_generator.next_id("invalidation")?;
                        invalidations.push(InvalidationRecord {
                            invalidation_id: dependency_invalidation_id,
                            result_ref: dependent.clone(),
                            reason_code: "dependency-invalidated".into(),
                            source_revision: entry.source_revision.clone(),
                            observed_at_ms: entry.observed_at_ms,
                            evidence_refs: entry.evidence_refs.clone(),
                        });
                        selected_payload_versions.remove(&dependent);
                        selected_relationship_versions.retain(|_, version_ref| {
                            self.relationship_versions
                                .get(version_ref)
                                .map(|version| {
                                    version.assertion.source_result_ref != dependent
                                        && version.assertion.target_result_ref != dependent
                                })
                                .unwrap_or(false)
                        });
                        changed_result_refs.insert(dependent.clone());
                        invalidated_result_refs.push(dependent);
                    }
                }
            }
        }
        if !normalized_refreshes.is_empty() {
            return Err(DataViewContractError::InvalidContract(
                "unexpected normalized stream refresh",
            ));
        }

        conflict_version_refs.sort();
        conflict_version_refs.dedup();
        let revision = DataViewRevisionRecord {
            data_view: resulting_revision.clone(),
            parent_revision_id: Some(head.data_view.revision_id.clone()),
            internal_sequence: head.internal_sequence + 1,
            selected_payload_versions,
            selected_relationship_versions,
            changed_result_refs,
        };
        let delta = GraphDeltaRecord {
            delta_ref: delta_ref.clone(),
            submission_id: submission.submission_id.clone(),
            base_revision: head.data_view.clone(),
            resulting_revision: resulting_revision.clone(),
            added_result_refs: Vec::new(),
            refreshed_result_refs,
            invalidated_result_refs,
            deleted_result_refs,
            conflict_version_refs: conflict_version_refs.clone(),
            actual_cost_units: 0,
        };
        let outcome = DataViewCommitOutcome {
            submission_id: submission.submission_id.clone(),
            status: DataViewCommitStatus::Accepted,
            data_view: resulting_revision,
            delta_ref: Some(delta_ref),
            conflict_version_refs,
            residual_code: None,
            retryable: false,
        };
        Ok(PreparedDataViewCommit {
            attachments,
            payload_versions,
            relationship_versions,
            invalidations,
            revision,
            delta,
            outcome,
        })
    }

    pub(crate) fn apply_prepared_commit_with_control(
        &mut self,
        prepared: PreparedDataViewCommit,
        control_state: Vec<u8>,
    ) -> Result<DataViewCommitOutcome, DataViewContractError> {
        let outcome = prepared.outcome.clone();
        let mut next = self.persisted_state();
        for attachment in prepared.attachments {
            next.attachments
                .insert(attachment.attachment_id.clone(), attachment);
        }
        for version in prepared.payload_versions {
            next.payload_versions
                .insert(version.version_ref.clone(), version);
        }
        for version in prepared.relationship_versions {
            next.relationship_versions
                .insert(version.version_ref.clone(), version);
        }
        for invalidation in prepared.invalidations {
            next.invalidations
                .insert(invalidation.invalidation_id.clone(), invalidation);
        }
        next.deltas
            .insert(prepared.delta.delta_ref.clone(), prepared.delta);
        next.head_revision_id = prepared.revision.data_view.revision_id.clone();
        next.revisions.insert(
            prepared.revision.data_view.revision_id.clone(),
            prepared.revision,
        );
        next.control_state = control_state;
        self.persist_and_replace(next)?;
        Ok(outcome)
    }

    fn resolve_revision(
        &self,
        data_view: &DataViewRef,
    ) -> Result<&DataViewRevisionRecord, DataViewContractError> {
        if data_view.data_view_id != self.data_view_id {
            return Err(DataViewContractError::NotFound("base revision"));
        }
        self.revisions
            .get(&data_view.revision_id)
            .ok_or(DataViewContractError::NotFound("base revision"))
    }

    fn changed_results_after(&self, internal_sequence: u64) -> BTreeSet<TrackedResultRef> {
        self.revisions
            .values()
            .filter(|revision| revision.internal_sequence > internal_sequence)
            .flat_map(|revision| revision.changed_result_refs.iter().cloned())
            .collect()
    }

    fn is_ancestor(&self, ancestor_revision_id: &str, revision_id: &str) -> bool {
        let mut cursor = Some(revision_id);
        while let Some(current) = cursor {
            if current == ancestor_revision_id {
                return true;
            }
            cursor = self
                .revisions
                .get(current)
                .and_then(|revision| revision.parent_revision_id.as_deref());
        }
        false
    }

    fn dependent_results_transitive(
        &self,
        root: &TrackedResultRef,
        selected_payload_versions: &BTreeMap<TrackedResultRef, String>,
        max_results: usize,
    ) -> Result<BTreeSet<TrackedResultRef>, DataViewContractError> {
        let mut pending = vec![root.0.clone()];
        let mut dependents = BTreeSet::new();
        while let Some(dependency) = pending.pop() {
            for (result_ref, version_ref) in selected_payload_versions {
                if result_ref == root || dependents.contains(result_ref) {
                    continue;
                }
                let version = self
                    .payload_versions
                    .get(version_ref)
                    .ok_or(DataViewContractError::NotFound("selected payload version"))?;
                if version
                    .dependency_refs
                    .iter()
                    .any(|value| value == &dependency)
                {
                    dependents.insert(result_ref.clone());
                    pending.push(result_ref.0.clone());
                    pending.push(version.version_ref.clone());
                    if dependents.len() > max_results {
                        return Err(DataViewContractError::LimitExceeded(
                            "dependency invalidation",
                        ));
                    }
                }
            }
        }
        Ok(dependents)
    }

    fn persisted_state(&self) -> PersistedDataViewState {
        PersistedDataViewState {
            data_view_id: self.data_view_id.clone(),
            head_revision_id: self.head_revision_id.clone(),
            revisions: self.revisions.clone(),
            attachments: self.attachments.clone(),
            payload_versions: self.payload_versions.clone(),
            relationship_versions: self.relationship_versions.clone(),
            invalidations: self.invalidations.clone(),
            retention_records: self.retention_records.clone(),
            tombstones: self.tombstones.clone(),
            unavailable_revisions: self.unavailable_revisions.clone(),
            deltas: self.deltas.clone(),
            control_state: self.control_state.clone(),
            scoped_revision_aliases: self.scoped_revision_aliases.clone(),
            scoped_aliases_by_subject: self.scoped_aliases_by_subject.clone(),
        }
    }

    fn persist_current_state(&self) -> Result<(), DataViewContractError> {
        let encoded = serde_json::to_vec(&self.persisted_state())
            .map_err(|_| DataViewContractError::SerializationFailed)?;
        self.persistence.persist(&encoded)
    }

    fn replace_persisted_state(&mut self, state: PersistedDataViewState) {
        self.data_view_id = state.data_view_id;
        self.head_revision_id = state.head_revision_id;
        self.revisions = state.revisions;
        self.attachments = state.attachments;
        self.payload_versions = state.payload_versions;
        self.relationship_versions = state.relationship_versions;
        self.invalidations = state.invalidations;
        self.retention_records = state.retention_records;
        self.tombstones = state.tombstones;
        self.unavailable_revisions = state.unavailable_revisions;
        self.deltas = state.deltas;
        self.control_state = state.control_state;
        self.scoped_revision_aliases = state.scoped_revision_aliases;
        self.scoped_aliases_by_subject = state.scoped_aliases_by_subject;
    }

    fn persist_and_replace(
        &mut self,
        state: PersistedDataViewState,
    ) -> Result<(), DataViewContractError> {
        let encoded =
            serde_json::to_vec(&state).map_err(|_| DataViewContractError::SerializationFailed)?;
        self.persistence.persist(&encoded)?;
        self.replace_persisted_state(state);
        Ok(())
    }

    fn from_persisted_state(
        state: PersistedDataViewState,
        id_generator: Arc<dyn IdGenerator>,
        persistence: Arc<dyn DataViewPersistence>,
    ) -> Self {
        Self {
            id_generator,
            persistence,
            data_view_id: state.data_view_id,
            head_revision_id: state.head_revision_id,
            revisions: state.revisions,
            attachments: state.attachments,
            payload_versions: state.payload_versions,
            relationship_versions: state.relationship_versions,
            invalidations: state.invalidations,
            retention_records: state.retention_records,
            tombstones: state.tombstones,
            unavailable_revisions: state.unavailable_revisions,
            deltas: state.deltas,
            control_state: state.control_state,
            scoped_revision_aliases: state.scoped_revision_aliases,
            scoped_aliases_by_subject: state.scoped_aliases_by_subject,
        }
    }
}

fn validate_relationships(
    relationships: &[RelationshipAssertion],
    candidate_refs: &BTreeSet<TrackedResultRef>,
    head: &DataViewRevisionRecord,
) -> Result<(), DataViewContractError> {
    let known = head
        .selected_payload_versions
        .keys()
        .cloned()
        .chain(candidate_refs.iter().cloned())
        .collect::<BTreeSet<_>>();
    for relationship in relationships {
        if relationship.relationship_id.trim().is_empty()
            || !known.contains(&relationship.source_result_ref)
            || !known.contains(&relationship.target_result_ref)
            || relationship.evidence_refs.is_empty()
        {
            return Err(DataViewContractError::InvalidContract(
                "relationship_assertions",
            ));
        }
    }
    Ok(())
}

fn validate_dependencies(
    dependency_refs: &[String],
    candidate_refs: &BTreeSet<TrackedResultRef>,
    head: &DataViewRevisionRecord,
    store: &DataViewStore,
) -> Result<(), DataViewContractError> {
    let known_result_refs = head
        .selected_payload_versions
        .keys()
        .map(|result_ref| result_ref.0.as_str())
        .chain(
            candidate_refs
                .iter()
                .map(|result_ref| result_ref.0.as_str()),
        )
        .collect::<BTreeSet<_>>();
    for dependency in dependency_refs {
        if !known_result_refs.contains(dependency.as_str())
            && !store.payload_versions.contains_key(dependency)
        {
            return Err(DataViewContractError::InvalidContract("dependency_refs"));
        }
    }
    Ok(())
}

fn retention_reason_code(reason: &RetentionReason) -> &'static str {
    match reason {
        RetentionReason::ActionableTtlExpired => "actionable-ttl-expired",
        RetentionReason::HistoryTtlExpired => "history-ttl-expired",
        RetentionReason::Capacity => "capacity",
        RetentionReason::SourcePolicy => "source-policy",
        RetentionReason::TenantPolicy => "tenant-policy",
        RetentionReason::ComplianceDeletion => "compliance-deletion",
        RetentionReason::DependencyInvalidated => "dependency-invalidated",
    }
}

fn dependent_results_in_state(
    state: &PersistedDataViewState,
    root: &TrackedResultRef,
    selected_payload_versions: &BTreeMap<TrackedResultRef, String>,
    max_results: usize,
) -> Result<BTreeSet<TrackedResultRef>, DataViewContractError> {
    let mut pending = vec![root.0.clone()];
    let mut dependents = BTreeSet::new();
    while let Some(dependency) = pending.pop() {
        for (result_ref, version_ref) in selected_payload_versions {
            if result_ref == root || dependents.contains(result_ref) {
                continue;
            }
            let version = state
                .payload_versions
                .get(version_ref)
                .ok_or(DataViewContractError::NotFound("selected payload version"))?;
            if version
                .dependency_refs
                .iter()
                .any(|value| value == &dependency)
            {
                dependents.insert(result_ref.clone());
                pending.push(result_ref.0.clone());
                pending.push(version.version_ref.clone());
                if dependents.len() > max_results {
                    return Err(DataViewContractError::LimitExceeded("dependency retention"));
                }
            }
        }
    }
    Ok(dependents)
}

fn protected_payload_versions(
    state: &PersistedDataViewState,
    head: &DataViewRevisionRecord,
) -> Result<BTreeSet<String>, DataViewContractError> {
    let mut protected = head
        .selected_payload_versions
        .values()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut changed = true;
    while changed {
        changed = false;
        let protected_snapshot = protected.clone();
        for version_ref in protected_snapshot {
            let version = state
                .payload_versions
                .get(&version_ref)
                .ok_or(DataViewContractError::NotFound("protected payload version"))?;
            for dependency in &version.dependency_refs {
                if state.payload_versions.contains_key(dependency) {
                    changed |= protected.insert(dependency.clone());
                }
                if let Some(dependency_version) = head
                    .selected_payload_versions
                    .get(&TrackedResultRef(dependency.clone()))
                {
                    changed |= protected.insert(dependency_version.clone());
                }
            }
        }
    }
    Ok(protected)
}

fn payload_bytes_excluding(
    state: &PersistedDataViewState,
    excluded_versions: &BTreeSet<String>,
) -> usize {
    let retained_attachment_refs = state
        .payload_versions
        .iter()
        .filter(|(version_ref, _)| !excluded_versions.contains(*version_ref))
        .map(|(_, version)| version.attachment_ref.as_str())
        .collect::<BTreeSet<_>>();
    retained_attachment_refs
        .iter()
        .filter_map(|attachment_ref| state.attachments.get(*attachment_ref))
        .map(|attachment| attachment.payload.len())
        .sum()
}

fn select_candidate(existing: &PayloadVersionRecord, candidate: &PayloadVersionRecord) -> bool {
    payload_rank(candidate) > payload_rank(existing)
}

fn payload_rank(record: &PayloadVersionRecord) -> (u8, u8, &[u8], u64, &str) {
    (
        freshness_rank(&record.freshness),
        confidence_rank(&record.confidence),
        &record.source_revision_order_key,
        record.observed_at_ms,
        &record.selection_key,
    )
}

fn confidence_from_derivation(derivation: &DerivationEvidence) -> RelationshipConfidence {
    match derivation.derivation_kind.as_str() {
        "observed" | "stream-refresh" => RelationshipConfidence::Observed,
        "declared" => RelationshipConfidence::Declared,
        _ => RelationshipConfidence::Inferred,
    }
}

fn confidence_rank(confidence: &RelationshipConfidence) -> u8 {
    match confidence {
        RelationshipConfidence::Observed => 3,
        RelationshipConfidence::Declared => 2,
        RelationshipConfidence::Inferred => 1,
    }
}

fn freshness_rank(state: &FreshnessState) -> u8 {
    match state {
        FreshnessState::Fresh => 4,
        FreshnessState::StaleButPlannable => 3,
        FreshnessState::RequiresRevalidation => 2,
        FreshnessState::Invalidated => 1,
    }
}

fn payload_selection_key(
    identity: &CanonicalIdentity,
    source_revision: &str,
    source_revision_order_key: &[u8],
    payload: &[u8],
    evidence_refs: &[String],
) -> String {
    let mut material = Vec::new();
    material.extend_from_slice(identity.result_ref.0.as_bytes());
    material.push(0);
    material.extend_from_slice(source_revision.as_bytes());
    material.push(0);
    material.extend_from_slice(source_revision_order_key);
    material.push(0);
    material.extend_from_slice(payload);
    material.push(0);
    for evidence in evidence_refs {
        material.extend_from_slice(evidence.as_bytes());
        material.push(0);
    }
    let hash = digest(&SHA256, &material);
    let mut output = String::with_capacity(hash.as_ref().len() * 2);
    for byte in hash.as_ref() {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}
