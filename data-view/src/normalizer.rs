use std::collections::BTreeMap;
use std::sync::Arc;

use ring::digest::{SHA256, digest};

use crate::{
    ActionResultSubmission, CanonicalIdentity, DataViewContractError, NormalizedDataViewCommit,
    SchemaVersionRef, StreamingRefreshEntry, TrackedResultRef,
};

pub trait ResultNormalizer: Send + Sync {
    fn schema(&self) -> &SchemaVersionRef;

    fn normalize_declared(&self, _submission: &crate::DeclaredResultSubmission) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        Err(DataViewContractError::UnsupportedSchema)
    }

    fn normalize_action(
        &self,
        submission: &ActionResultSubmission,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError>;

    fn normalize_stream_refresh(
        &self,
        source_authority_ref: &crate::SourceAuthorityRef,
        tracked_result_ref: &TrackedResultRef,
        entry: &StreamingRefreshEntry,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError>;
}

/// Explicitly registered per admitted manifest schema. Never fetches external bytes
/// and never normalizes Coox internals or provider-specific graph meanings.
pub struct DeclaredManifestNormalizer { schema: SchemaVersionRef }
impl DeclaredManifestNormalizer {
    pub fn new(schema: SchemaVersionRef) -> Self { Self { schema } }
}
impl ResultNormalizer for DeclaredManifestNormalizer {
    fn schema(&self) -> &SchemaVersionRef { &self.schema }
    fn normalize_declared(&self, submission: &crate::DeclaredResultSubmission) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        if submission.manifest.schema != self.schema { return Err(DataViewContractError::UnsupportedSchema); }
        crate::declared::normalize_declared_manifest(submission)
    }
    fn normalize_action(&self, _: &ActionResultSubmission) -> Result<NormalizedDataViewCommit, DataViewContractError> { Err(DataViewContractError::UnsupportedSchema) }
    fn normalize_stream_refresh(&self, _: &crate::SourceAuthorityRef, _: &TrackedResultRef, _: &StreamingRefreshEntry) -> Result<NormalizedDataViewCommit, DataViewContractError> { Err(DataViewContractError::UnsupportedSchema) }
}

#[derive(Default)]
pub struct ResultNormalizerRegistry {
    normalizers: BTreeMap<SchemaVersionRef, Arc<dyn ResultNormalizer>>,
}

impl ResultNormalizerRegistry {
    pub fn register(
        &mut self,
        normalizer: Arc<dyn ResultNormalizer>,
    ) -> Result<(), DataViewContractError> {
        normalizer.schema().validate()?;
        let schema = normalizer.schema().clone();
        if self.normalizers.contains_key(&schema) {
            return Err(DataViewContractError::AlreadyExists("result normalizer"));
        }
        self.normalizers.insert(schema, normalizer);
        Ok(())
    }

    pub fn resolve(
        &self,
        schema: &SchemaVersionRef,
    ) -> Result<&Arc<dyn ResultNormalizer>, DataViewContractError> {
        self.normalizers
            .get(schema)
            .ok_or(DataViewContractError::UnsupportedSchema)
    }

    pub fn contains(&self, schema: &SchemaVersionRef) -> bool {
        self.normalizers.contains_key(schema)
    }
}

#[derive(Debug, Clone)]
pub struct SingleResultNormalizer {
    schema: SchemaVersionRef,
    entity_kind: String,
}

impl SingleResultNormalizer {
    pub fn new(schema: SchemaVersionRef, entity_kind: impl Into<String>) -> Self {
        Self {
            schema,
            entity_kind: entity_kind.into(),
        }
    }
}

impl ResultNormalizer for SingleResultNormalizer {
    fn schema(&self) -> &SchemaVersionRef {
        &self.schema
    }

    fn normalize_action(
        &self,
        submission: &ActionResultSubmission,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        let anchor = submission
            .native_anchors
            .first()
            .ok_or(DataViewContractError::InvalidContract("native_anchors"))?;
        let result_ref = derived_result_ref(
            submission.source_authority_ref.as_ref(),
            &self.schema,
            &anchor.native_ref,
        );
        Ok(NormalizedDataViewCommit {
            complete_payload_attachment: submission.payload.clone(),
            canonical_identities: vec![CanonicalIdentity {
                canonical_id: result_ref.0.clone(),
                result_ref,
                entity_kind: self.entity_kind.clone(),
                source_authority_ref: submission.source_authority_ref.clone(),
                native_anchors: submission.native_anchors.clone(),
                properties: BTreeMap::new(),
            }],
            relationship_assertions: Vec::new(),
            dependency_refs: submission.derivation.dependency_refs.clone(),
            normalization_evidence: vec![format!(
                "normalizer:{}:{}",
                self.schema.schema_ref, self.schema.schema_version
            )],
            source_revision_order_key: submission.source_revision.as_bytes().to_vec(),
        })
    }

    fn normalize_stream_refresh(
        &self,
        source_authority_ref: &crate::SourceAuthorityRef,
        tracked_result_ref: &TrackedResultRef,
        entry: &StreamingRefreshEntry,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        let payload = entry
            .payload
            .as_ref()
            .ok_or(DataViewContractError::InvalidContract("refresh payload"))?;
        Ok(NormalizedDataViewCommit {
            complete_payload_attachment: payload.clone(),
            canonical_identities: vec![CanonicalIdentity {
                result_ref: tracked_result_ref.clone(),
                canonical_id: tracked_result_ref.0.clone(),
                entity_kind: self.entity_kind.clone(),
                source_authority_ref: source_authority_ref.clone(),
                native_anchors: entry.native_anchors.clone(),
                properties: BTreeMap::new(),
            }],
            relationship_assertions: Vec::new(),
            dependency_refs: Vec::new(),
            normalization_evidence: vec![format!(
                "normalizer:{}:{}",
                self.schema.schema_ref, self.schema.schema_version
            )],
            source_revision_order_key: entry.source_revision.as_bytes().to_vec(),
        })
    }
}

fn derived_result_ref(
    authority_ref: &str,
    schema: &SchemaVersionRef,
    native_ref: &str,
) -> TrackedResultRef {
    let material = format!(
        "{}\u{0}{}\u{0}{}\u{0}{}",
        authority_ref, schema.schema_ref, schema.schema_version, native_ref
    );
    let hash = digest(&SHA256, material.as_bytes());
    let mut encoded = String::with_capacity(7 + hash.as_ref().len() * 2);
    encoded.push_str("result:");
    for byte in hash.as_ref() {
        encoded.push_str(&format!("{byte:02x}"));
    }
    TrackedResultRef(encoded)
}
