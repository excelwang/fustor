use crate::{
    CanonicalIdentity, DataViewContractError, DataViewRef, DerivationEvidence, FreshnessState,
    NormalizedDataViewCommit, SchemaVersionRef, SourceAuthorityRef, TrackedResultRef,
};
use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredResultPolicy {
    pub profile_id: String,
    pub producer_id: String,
    pub result_authority_ref: SourceAuthorityRef,
    pub schema: SchemaVersionRef,
    pub allowed_external_ref_prefix: String,
    pub max_manifest_bytes: u64,
    pub max_external_size_bytes: u64,
    pub policy_context_digest: String,
    pub grant_epoch: u64,
    pub required_policy_labels: BTreeSet<String>,
    pub max_ttl_ms: u64,
}
impl DeclaredResultPolicy {
    pub fn validate(&self) -> Result<(), DataViewContractError> {
        self.schema.validate()?;
        if self.profile_id.is_empty()
            || self.producer_id.is_empty()
            || self.result_authority_ref.is_empty()
            || self.policy_context_digest.is_empty()
            || self.grant_epoch == 0
            || self.max_ttl_ms == 0
            || self.max_ttl_ms > 3_600_000
            || self.max_manifest_bytes == 0
            || self.max_manifest_bytes > 1_048_576
            || self.max_external_size_bytes == 0
            || !self.allowed_external_ref_prefix.ends_with('/')
            || !self.allowed_external_ref_prefix.contains("://")
        {
            return Err(DataViewContractError::InvalidContract(
                "declared result policy",
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredResultCapability {
    pub data_view_id: String,
    pub policy: DeclaredResultPolicy,
    pub policy_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredResultAdmissionContext {
    pub admission_id: String,
    pub admission_contract_digest: String,
    pub policy: DeclaredResultPolicy,
    pub base_revision: DataViewRef,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub idempotency_key: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalResultManifest {
    pub external_ref: String,
    pub external_revision: String,
    pub digest_algorithm: String,
    pub digest: String,
    pub size_bytes: u64,
    pub schema: SchemaVersionRef,
    pub media_type: String,
    pub producer_id: String,
    pub producer_version: String,
    pub lineage_refs: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub validation_refs: Vec<String>,
    pub observed_or_produced_at_ms: u64,
    pub policy_labels: BTreeSet<String>,
    pub freshness: FreshnessState,
    pub retention: BTreeMap<String, String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredResultSubmission {
    pub submission_id: String,
    pub admission: DeclaredResultAdmissionContext,
    pub manifest: ExternalResultManifest,
    pub derivation: DerivationEvidence,
}

pub(crate) fn declared_digest<T: Serialize>(value: &T) -> Result<String, DataViewContractError> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| DataViewContractError::SerializationFailed)?;
    Ok(digest(&SHA256, &bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub(crate) fn validate_declared_manifest(
    submission: &DeclaredResultSubmission,
    now_ms: u64,
) -> Result<(), DataViewContractError> {
    let manifest = &submission.manifest;
    let policy = &submission.admission.policy;
    let reference = &manifest.external_ref;
    if submission.submission_id.is_empty()
        || reference.len() > 4096
        || !reference.starts_with(&policy.allowed_external_ref_prefix)
        || reference.contains(['?', '#', '\\', '\0'])
        || reference.contains('%')
        || reference.split('/').any(|part| part == "." || part == "..")
        || manifest.external_revision.is_empty()
        || manifest.digest_algorithm != "sha256"
        || manifest.digest.len() != 64
        || !manifest
            .digest
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        || manifest.media_type.is_empty()
        || manifest.producer_version.is_empty()
        || manifest.retention.is_empty()
        || manifest.lineage_refs.is_empty()
        || manifest.evidence_refs.is_empty()
        || manifest.validation_refs.is_empty()
        || manifest.observed_or_produced_at_ms == 0
        || manifest.observed_or_produced_at_ms > now_ms
        || !policy
            .required_policy_labels
            .is_subset(&manifest.policy_labels)
    {
        return Err(DataViewContractError::InvalidContract(
            "external result manifest evidence",
        ));
    }
    if manifest.schema != policy.schema || manifest.producer_id != policy.producer_id {
        return Err(DataViewContractError::UnsupportedSchema);
    }
    if manifest.freshness != FreshnessState::Fresh {
        return Err(DataViewContractError::FreshnessRequiresRevalidation);
    }
    if manifest.size_bytes > policy.max_external_size_bytes
        || serde_json::to_vec(submission)
            .map_err(|_| DataViewContractError::SerializationFailed)?
            .len() as u64
            > policy.max_manifest_bytes
    {
        return Err(DataViewContractError::LimitExceeded("declared manifest"));
    }
    for values in [
        &manifest.lineage_refs,
        &manifest.evidence_refs,
        &manifest.validation_refs,
        &submission.derivation.dependency_refs,
    ] {
        if values.len() > 128
            || values.iter().any(|value| {
                value.is_empty() || value.len() > 4096 || value.contains(['\0', '\r', '\n'])
            })
        {
            return Err(DataViewContractError::InvalidContract(
                "declared evidence refs",
            ));
        }
    }
    Ok(())
}

pub(crate) fn normalize_declared_manifest(
    submission: &DeclaredResultSubmission,
) -> Result<NormalizedDataViewCommit, DataViewContractError> {
    let manifest = &submission.manifest;
    let result_ref = TrackedResultRef(format!(
        "declared-result:{}",
        declared_digest(&(
            manifest.producer_id.clone(),
            manifest.external_ref.clone(),
            manifest.schema.clone()
        ))?
    ));
    Ok(NormalizedDataViewCommit {
        complete_payload_attachment: serde_json::to_vec(submission)
            .map_err(|_| DataViewContractError::SerializationFailed)?,
        canonical_identities: vec![CanonicalIdentity {
            canonical_id: result_ref.0.clone(),
            result_ref,
            entity_kind: "external-result-manifest".into(),
            source_authority_ref: submission.admission.policy.result_authority_ref.clone(),
            native_anchors: Vec::new(),
            properties: BTreeMap::from([
                (
                    "external_ref".into(),
                    serde_json::json!(manifest.external_ref),
                ),
                (
                    "external_revision".into(),
                    serde_json::json!(manifest.external_revision),
                ),
                ("digest".into(), serde_json::json!(manifest.digest)),
                (
                    "producer_id".into(),
                    serde_json::json!(manifest.producer_id),
                ),
            ]),
        }],
        relationship_assertions: Vec::new(),
        // External refs are evidence, not invented DataView dependency identities.
        dependency_refs: Vec::new(),
        normalization_evidence: vec!["normalizer:declared-result-manifest:v1".into()],
        source_revision_order_key: manifest.observed_or_produced_at_ms.to_be_bytes().to_vec(),
    })
}
