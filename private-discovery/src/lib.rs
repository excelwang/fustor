use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

pub use data_view::{
    BoundedSourceAction, ContinuationPolicy, DiscoveryOperation, FailureMode, FreshnessState,
    ObservationPath, SideEffectLevel,
};
use data_view::{DATA_VIEW_WIRE_VERSION, SourceAuthorityRef};
use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use union_graph::{NativeAnchor, NodeKind, SourceKind};

mod scoped_data_view;

pub use scoped_data_view::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SourceInstanceKey {
    pub source_kind: SourceKind,
    pub source_instance: String,
}

impl SourceInstanceKey {
    pub fn new(source_kind: SourceKind, source_instance: impl Into<String>) -> Self {
        Self {
            source_kind,
            source_instance: source_instance.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryIntent {
    pub task: String,
    #[serde(default)]
    pub priorities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyContext {
    pub user_id: String,
    pub tenant_id: String,
    pub task_purpose: String,
    #[serde(default)]
    pub allowed_source_instances: BTreeSet<SourceInstanceKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
    #[serde(default)]
    pub request_context: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct PolicyContextBuilder {
    policy: PolicyContext,
}

impl PolicyContextBuilder {
    pub fn new(
        user_id: impl Into<String>,
        tenant_id: impl Into<String>,
        task_purpose: impl Into<String>,
    ) -> Self {
        Self {
            policy: PolicyContext {
                user_id: user_id.into(),
                tenant_id: tenant_id.into(),
                task_purpose: task_purpose.into(),
                allowed_source_instances: BTreeSet::new(),
                grant_epoch: None,
                request_context: BTreeMap::new(),
            },
        }
    }

    pub fn allow_source(
        mut self,
        source_kind: SourceKind,
        source_instance: impl Into<String>,
    ) -> Self {
        self.policy
            .allowed_source_instances
            .insert(SourceInstanceKey::new(source_kind, source_instance));
        self
    }

    pub fn grant_epoch(mut self, grant_epoch: u64) -> Self {
        self.policy.grant_epoch = Some(grant_epoch);
        self
    }

    pub fn request_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.policy.request_context.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> PolicyContext {
        self.policy
    }
}

impl PolicyContext {
    pub fn allows(&self, source: &SourceInstanceKey) -> bool {
        self.allowed_source_instances.is_empty() || self.allowed_source_instances.contains(source)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryScope {
    #[serde(default)]
    pub source_instances: BTreeSet<SourceInstanceKey>,
    pub max_fragments: usize,
}

impl DiscoveryScope {
    pub fn allows(&self, source: &SourceInstanceKey) -> bool {
        self.source_instances.is_empty() || self.source_instances.contains(source)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisclosureBudget {
    pub max_fragments: usize,
    pub max_names: usize,
    pub max_relation_hints: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateDataDiscoveryRequest {
    pub discovery_intent: DiscoveryIntent,
    pub policy_context: PolicyContext,
    pub discovery_scope: DiscoveryScope,
    pub time_budget_ms: u64,
    pub disclosure_budget: DisclosureBudget,
    #[serde(default)]
    pub candidate_fragments: Vec<SourceFragmentObservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scoped_data_view: Option<ScopedDataView>,
    #[serde(default)]
    pub source_capabilities: Vec<SourceCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateDataDiscoveryResponse {
    pub visible_fragments: Vec<VisibleFragment>,
    pub source_authority_records: Vec<SourceAuthorityRecord>,
    pub scoped_discovery_graph: ScopedDiscoveryGraph,
    pub discovery_frontier: DiscoveryFrontier,
    pub discovery_budget: DiscoveryBudget,
    pub execution_trace: ExecutionTrace,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceFragmentObservation {
    pub source_kind: SourceKind,
    pub source_instance: String,
    pub local_id: String,
    pub node_kind: NodeKind,
    pub native_ref: String,
    pub display_name: String,
    pub native_pointer: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    pub observed_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
}

impl SourceFragmentObservation {
    pub fn source_key(&self) -> SourceInstanceKey {
        SourceInstanceKey::new(self.source_kind.clone(), self.source_instance.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibleFragment {
    pub fragment_id: String,
    pub source_kind: SourceKind,
    pub source_instance: String,
    pub local_ref: String,
    pub display_label: String,
    pub fragment_kind: NodeKind,
    pub source_authority_ref: String,
    pub confidence: FragmentConfidence,
}

#[derive(Debug, Default, Clone)]
pub struct VisibleFragmentNormalizer;

impl VisibleFragmentNormalizer {
    pub fn normalize(&self, observation: &SourceFragmentObservation) -> VisibleFragment {
        visible_fragment_from_observation(observation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FragmentConfidence {
    Observed,
    Declared,
    Inferred,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthorityMode {
    NativeSource,
    SourceSpecificCache,
    ReadThroughAdapter,
    ObservationOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceAuthorityRecord {
    pub authority_ref: String,
    pub source_kind: SourceKind,
    pub source_instance: String,
    pub native_anchor: NativeAnchor,
    pub observed_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
    pub freshness_window_ms: u64,
    pub revalidation_action: String,
    pub authority_mode: AuthorityMode,
}

impl SourceAuthorityRecord {
    pub fn from_observation(
        observation: &SourceFragmentObservation,
        freshness_window_ms: u64,
        authority_mode: AuthorityMode,
    ) -> Self {
        let authority_ref = authority_ref(
            &observation.source_kind,
            &observation.source_instance,
            &observation.local_id,
        );
        Self {
            authority_ref: authority_ref.clone(),
            source_kind: observation.source_kind.clone(),
            source_instance: observation.source_instance.clone(),
            native_anchor: NativeAnchor {
                source_kind: observation.source_kind.clone(),
                source_instance: observation.source_instance.clone(),
                native_ref: observation.native_ref.clone(),
                native_pointer: observation.native_pointer.clone(),
                native_version: observation.fingerprint.clone(),
                cache_pointer: None,
                grant_epoch: observation.grant_epoch,
            },
            observed_at_ms: observation.observed_at_ms,
            source_revision: observation.fingerprint.clone(),
            fingerprint: observation.fingerprint.clone(),
            grant_epoch: observation.grant_epoch,
            freshness_window_ms,
            revalidation_action: format!("revalidate:{authority_ref}"),
            authority_mode,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct SourceAuthorityRegistry;

impl SourceAuthorityRegistry {
    pub fn attach(
        &self,
        observation: &SourceFragmentObservation,
        authority_mode: AuthorityMode,
    ) -> SourceAuthorityRecord {
        SourceAuthorityRecord::from_observation(
            observation,
            freshness_window_for(&observation.node_kind),
            authority_mode,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshnessGuard {
    pub source_authority_ref: String,
    pub state: FreshnessState,
    pub checked_at_ms: u64,
    pub reason: String,
    pub required_before_execution: bool,
}

impl FreshnessGuard {
    pub fn evaluate(
        record: &SourceAuthorityRecord,
        now_ms: u64,
        policy_grant_epoch: Option<u64>,
        high_risk: bool,
    ) -> Self {
        let age_ms = now_ms.saturating_sub(record.observed_at_ms);
        let grant_mismatch = policy_grant_epoch.is_some()
            && record.grant_epoch.is_some()
            && policy_grant_epoch != record.grant_epoch;
        let (state, reason, required_before_execution) = if grant_mismatch {
            (
                FreshnessState::Invalidated,
                "grant epoch mismatch".to_string(),
                true,
            )
        } else if high_risk {
            (
                FreshnessState::RequiresRevalidation,
                "high-risk action requires source revalidation".to_string(),
                true,
            )
        } else if age_ms <= record.freshness_window_ms {
            (
                FreshnessState::Fresh,
                "inside freshness window".to_string(),
                false,
            )
        } else if age_ms <= record.freshness_window_ms.saturating_mul(4).max(1) {
            (
                FreshnessState::StaleButPlannable,
                "outside freshness window but still useful for planning".to_string(),
                false,
            )
        } else {
            (
                FreshnessState::RequiresRevalidation,
                "freshness window expired".to_string(),
                true,
            )
        };
        Self {
            source_authority_ref: record.authority_ref.clone(),
            state,
            checked_at_ms: now_ms,
            reason,
            required_before_execution,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCapability {
    pub source_kind: SourceKind,
    pub source_instance: String,
    #[serde(default)]
    pub operations: Vec<DiscoveryOperation>,
    pub max_rows: u64,
    pub max_bytes: u64,
    pub max_calls: u64,
    pub max_fanout: u64,
    pub max_latency_ms: u64,
}

impl SourceCapability {
    pub fn source_key(&self) -> SourceInstanceKey {
        SourceInstanceKey::new(self.source_kind.clone(), self.source_instance.clone())
    }
}

#[derive(Debug, Clone)]
pub struct SourceAdapterDiscoveryRequest {
    pub policy_context: PolicyContext,
    pub discovery_scope: DiscoveryScope,
    pub max_fragments: usize,
    pub now_ms: u64,
}

pub trait DiscoverySourceAdapter {
    fn source_key(&self) -> SourceInstanceKey;

    fn discover(&self, request: &SourceAdapterDiscoveryRequest) -> Vec<ProbeOutcome>;

    fn capabilities(&self) -> Vec<SourceCapability> {
        let source = self.source_key();
        vec![default_capability_for_source(
            source.source_kind,
            source.source_instance,
        )]
    }
}

#[derive(Debug, Clone)]
pub struct SourcePayloadDiscoveryAdapter {
    source: SourceInstanceKey,
    outcomes: Vec<ProbeOutcome>,
    capabilities: Vec<SourceCapability>,
}

impl SourcePayloadDiscoveryAdapter {
    pub fn from_pg_fields(
        catalog: &pg_source::PgFieldCatalog,
        observed_at_ms: u64,
        grant_epoch: Option<u64>,
    ) -> Self {
        let source = SourceInstanceKey::new(SourceKind::Pg, catalog.object_ref.clone());
        let mut tables = BTreeMap::<(String, String), Vec<&pg_source::PgFieldDescriptor>>::new();
        for field in &catalog.fields {
            tables
                .entry((field.schema.clone(), field.table.clone()))
                .or_default()
                .push(field);
        }

        let mut outcomes = Vec::new();
        for ((schema, table), fields) in tables {
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::Pg,
                source_instance: catalog.object_ref.clone(),
                local_id: schema.clone(),
                node_kind: NodeKind::Namespace,
                native_ref: format!("pg://{}/{}", catalog.object_ref, schema),
                display_name: schema.clone(),
                native_pointer: format!("pg://{}/{}", catalog.object_ref, schema),
                fingerprint: None,
                observed_at_ms,
                grant_epoch,
            }));
            let table_id = format!("{schema}/{table}");
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::Pg,
                source_instance: catalog.object_ref.clone(),
                local_id: table_id.clone(),
                node_kind: NodeKind::Collection,
                native_ref: format!("pg://{}/{}/{}", catalog.object_ref, schema, table),
                display_name: table.clone(),
                native_pointer: format!("pg://{}/{}/{}", catalog.object_ref, schema, table),
                fingerprint: Some(format!("fields={}", fields.len())),
                observed_at_ms,
                grant_epoch,
            }));
            for field in fields {
                let field_id = format!("{schema}/{table}/{}", field.column);
                outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                    source_kind: SourceKind::Pg,
                    source_instance: catalog.object_ref.clone(),
                    local_id: field_id,
                    node_kind: NodeKind::Field,
                    native_ref: format!(
                        "pg://{}/{}/{}/{}",
                        catalog.object_ref, schema, table, field.column
                    ),
                    display_name: field.column.clone(),
                    native_pointer: format!(
                        "pg://{}/{}/{}/{}",
                        catalog.object_ref, schema, table, field.column
                    ),
                    fingerprint: field.column_type.clone(),
                    observed_at_ms,
                    grant_epoch,
                }));
            }
        }
        Self::from_visible_outcomes(source, outcomes)
    }

    pub fn from_pg_rows(batch: &pg_source::PgRowBatch, observed_at_ms: u64) -> Self {
        let source = SourceInstanceKey::new(SourceKind::Pg, batch.object_ref.clone());
        let table_id = format!("{}/{}", batch.schema, batch.table);
        let mut outcomes = vec![visible_source_payload_outcome(SourceFragmentObservation {
            source_kind: SourceKind::Pg,
            source_instance: batch.object_ref.clone(),
            local_id: table_id.clone(),
            node_kind: NodeKind::Collection,
            native_ref: format!("pg://{}/{}/{}", batch.object_ref, batch.schema, batch.table),
            display_name: batch.table.clone(),
            native_pointer: format!("pg://{}/{}/{}", batch.object_ref, batch.schema, batch.table),
            fingerprint: Some(format!(
                "rows={};fields={}",
                batch.rows.len(),
                batch.fields.len()
            )),
            observed_at_ms,
            grant_epoch: batch.grant_epoch,
        })];
        for field in &batch.fields {
            let field_id = format!("{table_id}/{field}");
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::Pg,
                source_instance: batch.object_ref.clone(),
                local_id: field_id,
                node_kind: NodeKind::Field,
                native_ref: format!(
                    "pg://{}/{}/{}/{}",
                    batch.object_ref, batch.schema, batch.table, field
                ),
                display_name: field.clone(),
                native_pointer: format!(
                    "pg://{}/{}/{}/{}",
                    batch.object_ref, batch.schema, batch.table, field
                ),
                fingerprint: None,
                observed_at_ms,
                grant_epoch: batch.grant_epoch,
            }));
        }
        for (idx, _) in batch.rows.iter().enumerate() {
            let row_number = idx + 1;
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::Pg,
                source_instance: batch.object_ref.clone(),
                local_id: format!("{table_id}/_row/{row_number}"),
                node_kind: NodeKind::Entity,
                native_ref: format!(
                    "pg://{}/{}/{}#row={row_number}",
                    batch.object_ref, batch.schema, batch.table
                ),
                display_name: format!("row {row_number}"),
                native_pointer: format!("{}:{}:{row_number}", batch.schema, batch.table),
                fingerprint: Some(format!("row_index={row_number}")),
                observed_at_ms,
                grant_epoch: batch.grant_epoch,
            }));
        }
        Self::from_visible_outcomes(source, outcomes)
    }

    pub fn from_mysql_fields(
        catalog: &mysql_source::MysqlFieldCatalog,
        observed_at_ms: u64,
        grant_epoch: Option<u64>,
    ) -> Self {
        let source = SourceInstanceKey::new(SourceKind::Mysql, catalog.object_ref.clone());
        let mut tables =
            BTreeMap::<(String, String), Vec<&mysql_source::MysqlFieldDescriptor>>::new();
        for field in &catalog.fields {
            tables
                .entry((field.schema.clone(), field.table.clone()))
                .or_default()
                .push(field);
        }

        let mut outcomes = Vec::new();
        for ((schema, table), fields) in tables {
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::Mysql,
                source_instance: catalog.object_ref.clone(),
                local_id: schema.clone(),
                node_kind: NodeKind::Namespace,
                native_ref: format!("mysql://{}/{}", catalog.object_ref, schema),
                display_name: schema.clone(),
                native_pointer: format!("mysql://{}/{}", catalog.object_ref, schema),
                fingerprint: None,
                observed_at_ms,
                grant_epoch,
            }));
            let table_id = format!("{schema}/{table}");
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::Mysql,
                source_instance: catalog.object_ref.clone(),
                local_id: table_id.clone(),
                node_kind: NodeKind::Collection,
                native_ref: format!("mysql://{}/{}/{}", catalog.object_ref, schema, table),
                display_name: table.clone(),
                native_pointer: format!("mysql://{}/{}/{}", catalog.object_ref, schema, table),
                fingerprint: Some(format!("fields={}", fields.len())),
                observed_at_ms,
                grant_epoch,
            }));
            for field in fields {
                let field_id = format!("{schema}/{table}/{}", field.column);
                outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                    source_kind: SourceKind::Mysql,
                    source_instance: catalog.object_ref.clone(),
                    local_id: field_id,
                    node_kind: NodeKind::Field,
                    native_ref: format!(
                        "mysql://{}/{}/{}/{}",
                        catalog.object_ref, schema, table, field.column
                    ),
                    display_name: field.column.clone(),
                    native_pointer: format!(
                        "mysql://{}/{}/{}/{}",
                        catalog.object_ref, schema, table, field.column
                    ),
                    fingerprint: field.column_type.clone(),
                    observed_at_ms,
                    grant_epoch,
                }));
            }
        }
        Self::from_visible_outcomes(source, outcomes)
    }

    pub fn from_mysql_rows(batch: &mysql_source::MysqlRowBatch, observed_at_ms: u64) -> Self {
        let source = SourceInstanceKey::new(SourceKind::Mysql, batch.object_ref.clone());
        let table_id = format!("{}/{}", batch.schema, batch.table);
        let mut outcomes = vec![visible_source_payload_outcome(SourceFragmentObservation {
            source_kind: SourceKind::Mysql,
            source_instance: batch.object_ref.clone(),
            local_id: table_id.clone(),
            node_kind: NodeKind::Collection,
            native_ref: format!(
                "mysql://{}/{}/{}",
                batch.object_ref, batch.schema, batch.table
            ),
            display_name: batch.table.clone(),
            native_pointer: format!(
                "mysql://{}/{}/{}",
                batch.object_ref, batch.schema, batch.table
            ),
            fingerprint: Some(format!(
                "rows={};fields={}",
                batch.rows.len(),
                batch.fields.len()
            )),
            observed_at_ms,
            grant_epoch: batch.grant_epoch,
        })];
        for field in &batch.fields {
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::Mysql,
                source_instance: batch.object_ref.clone(),
                local_id: format!("{table_id}/{field}"),
                node_kind: NodeKind::Field,
                native_ref: format!(
                    "mysql://{}/{}/{}/{}",
                    batch.object_ref, batch.schema, batch.table, field
                ),
                display_name: field.clone(),
                native_pointer: format!(
                    "mysql://{}/{}/{}/{}",
                    batch.object_ref, batch.schema, batch.table, field
                ),
                fingerprint: None,
                observed_at_ms,
                grant_epoch: batch.grant_epoch,
            }));
        }
        for (idx, _) in batch.rows.iter().enumerate() {
            let row_number = idx + 1;
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::Mysql,
                source_instance: batch.object_ref.clone(),
                local_id: format!("{table_id}/_row/{row_number}"),
                node_kind: NodeKind::Entity,
                native_ref: format!(
                    "mysql://{}/{}/{}#row={row_number}",
                    batch.object_ref, batch.schema, batch.table
                ),
                display_name: format!("row {row_number}"),
                native_pointer: format!("{}:{}:{row_number}", batch.schema, batch.table),
                fingerprint: Some(format!("row_index={row_number}")),
                observed_at_ms,
                grant_epoch: batch.grant_epoch,
            }));
        }
        Self::from_visible_outcomes(source, outcomes)
    }

    pub fn from_s3_objects(batch: &s3_source::S3ObjectBatch) -> Self {
        let source = SourceInstanceKey::new(SourceKind::S3, batch.object_ref.clone());
        let observed_at_ms = batch
            .objects
            .iter()
            .map(|object| object.last_modified_unix_ms)
            .max()
            .unwrap_or_default();
        let mut outcomes = vec![visible_source_payload_outcome(SourceFragmentObservation {
            source_kind: SourceKind::S3,
            source_instance: batch.object_ref.clone(),
            local_id: batch.bucket.clone(),
            node_kind: NodeKind::Root,
            native_ref: format!("s3://{}", batch.bucket),
            display_name: batch.bucket.clone(),
            native_pointer: format!("s3://{}", batch.bucket),
            fingerprint: None,
            observed_at_ms,
            grant_epoch: batch.grant_epoch,
        })];

        let prefix = batch.prefix.trim_matches('/');
        if !prefix.is_empty() {
            let prefix_id = format!("{}/{}", batch.bucket, prefix);
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::S3,
                source_instance: batch.object_ref.clone(),
                local_id: prefix_id.clone(),
                node_kind: NodeKind::Namespace,
                native_ref: format!("s3://{}/{}", batch.bucket, prefix),
                display_name: prefix.to_string(),
                native_pointer: format!("s3://{}/{}", batch.bucket, prefix),
                fingerprint: None,
                observed_at_ms,
                grant_epoch: batch.grant_epoch,
            }));
        }

        for object in &batch.objects {
            let local_id = format!("{}/{}", batch.bucket, object.key);
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::S3,
                source_instance: batch.object_ref.clone(),
                local_id,
                node_kind: NodeKind::Asset,
                native_ref: format!("s3://{}/{}", batch.bucket, object.key),
                display_name: object.key.clone(),
                native_pointer: format!("s3://{}/{}", batch.bucket, object.key),
                fingerprint: object.etag.clone().or_else(|| {
                    Some(format!(
                        "size={};mtime={}",
                        object.size, object.last_modified_unix_ms
                    ))
                }),
                observed_at_ms: object.last_modified_unix_ms,
                grant_epoch: batch.grant_epoch,
            }));
        }
        Self::from_visible_outcomes(source, outcomes)
    }

    pub fn from_s3_fields(
        catalog: &s3_source::S3FieldCatalog,
        observed_at_ms: u64,
        grant_epoch: Option<u64>,
    ) -> Self {
        Self::from_flat_fields(
            SourceKind::S3,
            &catalog.object_ref,
            "_metadata",
            "object metadata",
            catalog
                .fields
                .iter()
                .map(|field| (field.name.as_str(), field.field_type.as_str())),
            observed_at_ms,
            grant_epoch,
        )
    }

    pub fn from_es_fields(
        catalog: &es_source::EsFieldCatalog,
        observed_at_ms: u64,
        grant_epoch: Option<u64>,
    ) -> Self {
        let source = SourceInstanceKey::new(SourceKind::Es, catalog.object_ref.clone());
        let mut outcomes = vec![visible_source_payload_outcome(SourceFragmentObservation {
            source_kind: SourceKind::Es,
            source_instance: catalog.object_ref.clone(),
            local_id: catalog.index.clone(),
            node_kind: NodeKind::Collection,
            native_ref: format!("es://{}/{}", catalog.object_ref, catalog.index),
            display_name: catalog.index.clone(),
            native_pointer: format!("es://{}/{}", catalog.object_ref, catalog.index),
            fingerprint: Some(format!("fields={}", catalog.fields.len())),
            observed_at_ms,
            grant_epoch,
        })];
        for field in &catalog.fields {
            let local_id = format!("{}/_field/{}", catalog.index, field.name);
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::Es,
                source_instance: catalog.object_ref.clone(),
                local_id,
                node_kind: NodeKind::Field,
                native_ref: format!(
                    "es://{}/{}/_field/{}",
                    catalog.object_ref, catalog.index, field.name
                ),
                display_name: field.name.clone(),
                native_pointer: format!(
                    "es://{}/{}/_mapping/{}",
                    catalog.object_ref, catalog.index, field.name
                ),
                fingerprint: field.field_type.clone(),
                observed_at_ms,
                grant_epoch,
            }));
        }
        Self::from_visible_outcomes(source, outcomes)
    }

    pub fn from_es_batch(batch: &es_source::EsSourceBatch, observed_at_ms: u64) -> Self {
        let source = SourceInstanceKey::new(SourceKind::Es, batch.object_ref.clone());
        let mut outcomes = vec![visible_source_payload_outcome(SourceFragmentObservation {
            source_kind: SourceKind::Es,
            source_instance: batch.object_ref.clone(),
            local_id: batch.index.clone(),
            node_kind: NodeKind::Collection,
            native_ref: format!("es://{}/{}", batch.object_ref, batch.index),
            display_name: batch.index.clone(),
            native_pointer: format!("es://{}/{}", batch.object_ref, batch.index),
            fingerprint: Some(format!("docs={}", batch.documents.len())),
            observed_at_ms,
            grant_epoch: batch.grant_epoch,
        })];
        for document in &batch.documents {
            let local_id = format!("{}/_doc/{}", batch.index, document.id);
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::Es,
                source_instance: batch.object_ref.clone(),
                local_id,
                node_kind: NodeKind::Asset,
                native_ref: format!(
                    "es://{}/{}/_doc/{}",
                    batch.object_ref, batch.index, document.id
                ),
                display_name: document.id.clone(),
                native_pointer: format!(
                    "es://{}/{}/_doc/{}",
                    batch.object_ref, batch.index, document.id
                ),
                fingerprint: Some(format!(
                    "seq_no={:?};primary_term={:?}",
                    document.seq_no, document.primary_term
                )),
                observed_at_ms,
                grant_epoch: batch.grant_epoch,
            }));
        }
        Self::from_visible_outcomes(source, outcomes)
    }

    pub fn from_csv_fields(
        catalog: &csv_source::CsvFieldCatalog,
        observed_at_ms: u64,
        grant_epoch: Option<u64>,
    ) -> Self {
        Self::from_flat_fields(
            SourceKind::Csv,
            &catalog.object_ref,
            &catalog.path,
            &catalog.path,
            catalog
                .fields
                .iter()
                .map(|field| (field.name.as_str(), field.field_type.as_str())),
            observed_at_ms,
            grant_epoch,
        )
    }

    pub fn from_csv_records(batch: &csv_source::CsvRecordBatch, observed_at_ms: u64) -> Self {
        let mut adapter = Self::from_flat_fields(
            SourceKind::Csv,
            &batch.object_ref,
            &batch.path,
            &batch.path,
            batch.fields.iter().map(|field| (field.as_str(), "string")),
            observed_at_ms,
            batch.grant_epoch,
        );
        for record in &batch.records {
            adapter
                .outcomes
                .push(visible_source_payload_outcome(SourceFragmentObservation {
                    source_kind: SourceKind::Csv,
                    source_instance: batch.object_ref.clone(),
                    local_id: format!("{}/_row/{}", batch.path, record.row_number),
                    node_kind: NodeKind::Entity,
                    native_ref: format!(
                        "csv://{}/{}#row={}",
                        batch.object_ref, batch.path, record.row_number
                    ),
                    display_name: format!("row {}", record.row_number),
                    native_pointer: format!("{}:{}", batch.path, record.row_number),
                    fingerprint: Some(format!("row_number={}", record.row_number)),
                    observed_at_ms,
                    grant_epoch: batch.grant_epoch,
                }));
        }
        adapter
    }

    pub fn from_jsonl_fields(
        catalog: &jsonl_source::JsonlFieldCatalog,
        observed_at_ms: u64,
        grant_epoch: Option<u64>,
    ) -> Self {
        Self::from_flat_fields(
            SourceKind::Jsonl,
            &catalog.object_ref,
            &catalog.path,
            &catalog.path,
            catalog
                .fields
                .iter()
                .map(|field| (field.name.as_str(), field.field_type.as_str())),
            observed_at_ms,
            grant_epoch,
        )
    }

    pub fn from_jsonl_records(batch: &jsonl_source::JsonlRecordBatch, observed_at_ms: u64) -> Self {
        let mut adapter = Self::from_flat_fields(
            SourceKind::Jsonl,
            &batch.object_ref,
            &batch.path,
            &batch.path,
            std::iter::empty::<(&str, &str)>(),
            observed_at_ms,
            batch.grant_epoch,
        );
        for record in &batch.records {
            adapter
                .outcomes
                .push(visible_source_payload_outcome(SourceFragmentObservation {
                    source_kind: SourceKind::Jsonl,
                    source_instance: batch.object_ref.clone(),
                    local_id: format!("{}/_line/{}", batch.path, record.line_number),
                    node_kind: NodeKind::Entity,
                    native_ref: format!(
                        "jsonl://{}/{}#line={}",
                        batch.object_ref, batch.path, record.line_number
                    ),
                    display_name: format!("line {}", record.line_number),
                    native_pointer: format!("{}:{}", batch.path, record.line_number),
                    fingerprint: Some(format!("line_number={}", record.line_number)),
                    observed_at_ms,
                    grant_epoch: batch.grant_epoch,
                }));
        }
        adapter
    }

    pub fn from_local_fs_entries(
        batch: &local_fs_source::LocalFsEntryBatch,
        observed_at_ms: u64,
    ) -> Self {
        let source = SourceInstanceKey::new(SourceKind::LocalFs, batch.object_ref.clone());
        let mut outcomes = vec![visible_source_payload_outcome(SourceFragmentObservation {
            source_kind: SourceKind::LocalFs,
            source_instance: batch.object_ref.clone(),
            local_id: batch.root_path.clone(),
            node_kind: NodeKind::Root,
            native_ref: format!("local-fs://{}/{}", batch.object_ref, batch.root_path),
            display_name: batch.root_path.clone(),
            native_pointer: batch.root_path.clone(),
            fingerprint: None,
            observed_at_ms,
            grant_epoch: batch.grant_epoch,
        })];
        for entry in &batch.entries {
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: SourceKind::LocalFs,
                source_instance: batch.object_ref.clone(),
                local_id: entry.path.clone(),
                node_kind: if entry.is_dir {
                    NodeKind::Namespace
                } else {
                    NodeKind::Asset
                },
                native_ref: format!("local-fs://{}/{}", batch.object_ref, entry.path),
                display_name: entry.file_name.clone(),
                native_pointer: format!("{}/{}", batch.root_path.trim_end_matches('/'), entry.path),
                fingerprint: Some(format!(
                    "size={};mtime={:?}",
                    entry.size, entry.modified_unix_ms
                )),
                observed_at_ms: entry.modified_unix_ms.unwrap_or(observed_at_ms),
                grant_epoch: batch.grant_epoch,
            }));
        }
        Self::from_visible_outcomes(source, outcomes)
    }

    pub fn from_local_fs_fields(
        catalog: &local_fs_source::LocalFsFieldCatalog,
        observed_at_ms: u64,
        grant_epoch: Option<u64>,
    ) -> Self {
        Self::from_flat_fields(
            SourceKind::LocalFs,
            &catalog.object_ref,
            "_metadata",
            "filesystem metadata",
            catalog
                .fields
                .iter()
                .map(|field| (field.name.as_str(), field.field_type.as_str())),
            observed_at_ms,
            grant_epoch,
        )
    }

    pub fn from_flat_fields<'a, I>(
        source_kind: SourceKind,
        object_ref: &str,
        collection_id: &str,
        collection_label: &str,
        fields: I,
        observed_at_ms: u64,
        grant_epoch: Option<u64>,
    ) -> Self
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let source = SourceInstanceKey::new(source_kind.clone(), object_ref.to_string());
        let scheme = source_kind.as_str();
        let mut outcomes = vec![visible_source_payload_outcome(SourceFragmentObservation {
            source_kind: source_kind.clone(),
            source_instance: object_ref.to_string(),
            local_id: collection_id.to_string(),
            node_kind: NodeKind::Collection,
            native_ref: format!("{scheme}://{object_ref}/{collection_id}"),
            display_name: collection_label.to_string(),
            native_pointer: collection_id.to_string(),
            fingerprint: None,
            observed_at_ms,
            grant_epoch,
        })];
        for (field_name, field_type) in fields {
            let field_id = format!("{collection_id}/_field/{field_name}");
            outcomes.push(visible_source_payload_outcome(SourceFragmentObservation {
                source_kind: source_kind.clone(),
                source_instance: object_ref.to_string(),
                local_id: field_id,
                node_kind: NodeKind::Field,
                native_ref: format!("{scheme}://{object_ref}/{collection_id}/_field/{field_name}"),
                display_name: field_name.to_string(),
                native_pointer: format!("{collection_id}:{field_name}"),
                fingerprint: Some(field_type.to_string()),
                observed_at_ms,
                grant_epoch,
            }));
        }
        Self::from_visible_outcomes(source, outcomes)
    }

    pub fn from_visible_outcomes(source: SourceInstanceKey, outcomes: Vec<ProbeOutcome>) -> Self {
        let capabilities = vec![default_capability_for_source(
            source.source_kind.clone(),
            source.source_instance.clone(),
        )];
        Self {
            source,
            outcomes,
            capabilities,
        }
    }
}

impl DiscoverySourceAdapter for SourcePayloadDiscoveryAdapter {
    fn source_key(&self) -> SourceInstanceKey {
        self.source.clone()
    }

    fn discover(&self, request: &SourceAdapterDiscoveryRequest) -> Vec<ProbeOutcome> {
        self.outcomes
            .iter()
            .take(request.max_fragments)
            .cloned()
            .collect()
    }

    fn capabilities(&self) -> Vec<SourceCapability> {
        self.capabilities.clone()
    }
}

fn visible_source_payload_outcome(observation: SourceFragmentObservation) -> ProbeOutcome {
    ProbeOutcome {
        kind: ProbeOutcomeKind::Visible,
        visible_fragment: Some(observation),
        internal_diagnostic: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FileSourceMode {
    LocalTree,
    CsvFile,
    JsonlFile,
}

#[derive(Debug, Clone)]
pub struct FileSourceAdapter {
    source_kind: SourceKind,
    source_instance: String,
    root: PathBuf,
    mode: FileSourceMode,
    max_entries: usize,
    grant_epoch: Option<u64>,
}

impl FileSourceAdapter {
    pub fn local_fs(source_instance: impl Into<String>, root: impl Into<PathBuf>) -> Self {
        Self {
            source_kind: SourceKind::LocalFs,
            source_instance: source_instance.into(),
            root: root.into(),
            mode: FileSourceMode::LocalTree,
            max_entries: 64,
            grant_epoch: None,
        }
    }

    pub fn csv_file(source_instance: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            source_kind: SourceKind::Csv,
            source_instance: source_instance.into(),
            root: path.into(),
            mode: FileSourceMode::CsvFile,
            max_entries: 64,
            grant_epoch: None,
        }
    }

    pub fn jsonl_file(source_instance: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            source_kind: SourceKind::Jsonl,
            source_instance: source_instance.into(),
            root: path.into(),
            mode: FileSourceMode::JsonlFile,
            max_entries: 64,
            grant_epoch: None,
        }
    }

    pub fn max_entries(mut self, max_entries: usize) -> Self {
        self.max_entries = max_entries;
        self
    }

    pub fn grant_epoch(mut self, grant_epoch: u64) -> Self {
        self.grant_epoch = Some(grant_epoch);
        self
    }
}

impl DiscoverySourceAdapter for FileSourceAdapter {
    fn source_key(&self) -> SourceInstanceKey {
        SourceInstanceKey::new(self.source_kind.clone(), self.source_instance.clone())
    }

    fn discover(&self, request: &SourceAdapterDiscoveryRequest) -> Vec<ProbeOutcome> {
        let max_fragments = request.max_fragments.min(self.max_entries);
        if max_fragments == 0 {
            return Vec::new();
        }
        match self.root.metadata() {
            Ok(metadata) => match self.mode {
                FileSourceMode::LocalTree => self.discover_local_tree(request, max_fragments),
                FileSourceMode::CsvFile => {
                    self.discover_tabular_file(request, max_fragments, metadata, TabularMode::Csv)
                }
                FileSourceMode::JsonlFile => {
                    self.discover_tabular_file(request, max_fragments, metadata, TabularMode::Jsonl)
                }
            },
            Err(error) => vec![ProbeOutcome {
                kind: probe_kind_from_io_error(&error),
                visible_fragment: None,
                internal_diagnostic: Some(format!(
                    "{}:{}:{}",
                    self.source_kind.as_str(),
                    self.source_instance,
                    error
                )),
            }],
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TabularMode {
    Csv,
    Jsonl,
}

impl FileSourceAdapter {
    fn discover_local_tree(
        &self,
        request: &SourceAdapterDiscoveryRequest,
        max_fragments: usize,
    ) -> Vec<ProbeOutcome> {
        let mut outcomes = Vec::new();
        outcomes.push(self.visible_file_outcome(&self.root, ".", NodeKind::Namespace, request));
        if outcomes.len() >= max_fragments {
            return outcomes;
        }

        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(error) => {
                return vec![ProbeOutcome {
                    kind: probe_kind_from_io_error(&error),
                    visible_fragment: None,
                    internal_diagnostic: Some(format!(
                        "local-fs:{}:{}",
                        self.source_instance, error
                    )),
                }];
            }
        };
        let mut paths = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        paths.sort();

        for path in paths {
            if outcomes.len() >= max_fragments {
                break;
            }
            let relative = path
                .strip_prefix(&self.root)
                .ok()
                .and_then(Path::to_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| path.to_string_lossy().into_owned());
            let node_kind = match path.metadata() {
                Ok(metadata) if metadata.is_dir() => NodeKind::Namespace,
                Ok(_) => NodeKind::Asset,
                Err(error) => {
                    outcomes.push(ProbeOutcome {
                        kind: probe_kind_from_io_error(&error),
                        visible_fragment: None,
                        internal_diagnostic: Some(format!(
                            "local-fs:{}:{}",
                            self.source_instance, error
                        )),
                    });
                    continue;
                }
            };
            outcomes.push(self.visible_file_outcome(&path, &relative, node_kind, request));
        }
        outcomes
    }

    fn discover_tabular_file(
        &self,
        request: &SourceAdapterDiscoveryRequest,
        max_fragments: usize,
        metadata: fs::Metadata,
        mode: TabularMode,
    ) -> Vec<ProbeOutcome> {
        if !metadata.is_file() {
            return vec![ProbeOutcome {
                kind: ProbeOutcomeKind::Nonexistent,
                visible_fragment: None,
                internal_diagnostic: Some(format!(
                    "{}:{} is not a file",
                    self.source_kind.as_str(),
                    self.source_instance
                )),
            }];
        }

        let mut outcomes = vec![
            self.visible_file_outcome(
                &self.root,
                self.root
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("file"),
                NodeKind::Collection,
                request,
            ),
        ];
        if outcomes.len() >= max_fragments {
            return outcomes;
        }

        let fields = match mode {
            TabularMode::Csv => discover_csv_fields(&self.root),
            TabularMode::Jsonl => discover_jsonl_fields(&self.root),
        };
        match fields {
            Ok(fields) => {
                for field in fields.into_iter().take(max_fragments - outcomes.len()) {
                    outcomes.push(self.visible_field_outcome(&field, request));
                }
            }
            Err(error) => outcomes.push(ProbeOutcome {
                kind: probe_kind_from_io_error(&error),
                visible_fragment: None,
                internal_diagnostic: Some(format!(
                    "{}:{}:{}",
                    self.source_kind.as_str(),
                    self.source_instance,
                    error
                )),
            }),
        }
        outcomes
    }

    fn visible_file_outcome(
        &self,
        path: &Path,
        local_id: &str,
        node_kind: NodeKind,
        request: &SourceAdapterDiscoveryRequest,
    ) -> ProbeOutcome {
        ProbeOutcome {
            kind: ProbeOutcomeKind::Visible,
            visible_fragment: Some(SourceFragmentObservation {
                source_kind: self.source_kind.clone(),
                source_instance: self.source_instance.clone(),
                local_id: local_id.to_string(),
                node_kind,
                native_ref: local_id.to_string(),
                display_name: local_id.to_string(),
                native_pointer: file_native_pointer(path),
                fingerprint: file_fingerprint(path),
                observed_at_ms: request.now_ms,
                grant_epoch: self.grant_epoch.or(request.policy_context.grant_epoch),
            }),
            internal_diagnostic: None,
        }
    }

    fn visible_field_outcome(
        &self,
        field_name: &str,
        request: &SourceAdapterDiscoveryRequest,
    ) -> ProbeOutcome {
        let local_id = format!("{}#{}", self.root.display(), field_name);
        ProbeOutcome {
            kind: ProbeOutcomeKind::Visible,
            visible_fragment: Some(SourceFragmentObservation {
                source_kind: self.source_kind.clone(),
                source_instance: self.source_instance.clone(),
                local_id,
                node_kind: NodeKind::Field,
                native_ref: field_name.to_string(),
                display_name: field_name.to_string(),
                native_pointer: format!("{}#{}", file_native_pointer(&self.root), field_name),
                fingerprint: file_fingerprint(&self.root),
                observed_at_ms: request.now_ms,
                grant_epoch: self.grant_epoch.or(request.policy_context.grant_epoch),
            }),
            internal_diagnostic: None,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct SourceActionCompiler;

impl SourceActionCompiler {
    pub fn compile(
        &self,
        fragments: &[VisibleFragment],
        authority_records: &[SourceAuthorityRecord],
        guards: &[FreshnessGuard],
        capabilities: &[SourceCapability],
    ) -> Vec<BoundedSourceAction> {
        compile_bounded_source_actions(fragments, authority_records, guards, capabilities)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceNode {
    pub resource_id: String,
    pub fragment_id: String,
    pub source_authority_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionNode {
    pub action_id: String,
    pub operation: DiscoveryOperation,
    pub source_authority_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionEdge {
    pub from: String,
    pub to: String,
    pub edge_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedDiscoveryGraph {
    #[serde(default)]
    pub resource_nodes: Vec<ResourceNode>,
    #[serde(default)]
    pub action_nodes: Vec<ActionNode>,
    #[serde(default)]
    pub visibility_edges: Vec<PermissionEdge>,
    #[serde(default)]
    pub executable_edges: Vec<PermissionEdge>,
    #[serde(default)]
    pub expansion_edges: Vec<PermissionEdge>,
    #[serde(default)]
    pub source_authority_refs: Vec<String>,
    #[serde(default)]
    pub policy_constraints: Vec<String>,
    #[serde(default)]
    pub freshness_constraints: Vec<FreshnessGuard>,
    pub expires_at_ms: u64,
}

#[derive(Debug, Default, Clone)]
pub struct ScopedDiscoveryGraphBuilder;

impl ScopedDiscoveryGraphBuilder {
    pub fn build(
        &self,
        fragments: &[VisibleFragment],
        authority_records: &[SourceAuthorityRecord],
        guards: &[FreshnessGuard],
        actions: &[BoundedSourceAction],
        expires_at_ms: u64,
        policy: &PolicyContext,
    ) -> ScopedDiscoveryGraph {
        build_scoped_discovery_graph(
            fragments,
            authority_records,
            guards,
            actions,
            expires_at_ms,
            policy,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryBudget {
    pub time_budget_ms: u64,
    pub remaining_time_ms: u64,
    pub remaining_calls: u64,
    pub remaining_rows: u64,
    pub remaining_bytes: u64,
    pub remaining_fanout: u64,
    pub remaining_cost_units: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_budget: Option<u64>,
}

impl DiscoveryBudget {
    pub fn for_time_budget(time_budget_ms: u64) -> Self {
        let (calls, rows, bytes, fanout, cost_units) = if time_budget_ms <= 5 * 60 * 1000 {
            (8, 0, 0, 20, 10)
        } else if time_budget_ms <= 30 * 60 * 1000 {
            (40, 1_000, 10 * 1024 * 1024, 100, 100)
        } else {
            (200, 10_000, 100 * 1024 * 1024, 1_000, 500)
        };
        Self {
            time_budget_ms,
            remaining_time_ms: time_budget_ms,
            remaining_calls: calls,
            remaining_rows: rows,
            remaining_bytes: bytes,
            remaining_fanout: fanout,
            remaining_cost_units: cost_units,
            token_budget: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiscoveryFrontierStrategy {
    ShortBroadScan,
    BalancedBreadthThenDepth,
    DeepenAndVerify,
}

impl DiscoveryFrontierStrategy {
    pub fn for_time_budget(time_budget_ms: u64) -> Self {
        if time_budget_ms <= 5 * 60 * 1000 {
            Self::ShortBroadScan
        } else if time_budget_ms <= 30 * 60 * 1000 {
            Self::BalancedBreadthThenDepth
        } else {
            Self::DeepenAndVerify
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankedSourceAction {
    pub rank: usize,
    pub action: BoundedSourceAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryFrontier {
    pub strategy: DiscoveryFrontierStrategy,
    #[serde(default)]
    pub ranked_actions: Vec<RankedSourceAction>,
    #[serde(default)]
    pub skipped_branches: Vec<String>,
    pub explanation: String,
}

#[derive(Debug, Default, Clone)]
pub struct DiscoveryFrontierScheduler;

impl DiscoveryFrontierScheduler {
    pub fn schedule(
        &self,
        actions: Vec<BoundedSourceAction>,
        time_budget_ms: u64,
        ledger: &DiscoveryBudget,
        priorities: &[String],
        skipped: &BTreeSet<String>,
    ) -> DiscoveryFrontier {
        schedule_frontier(actions, time_budget_ms, ledger, priorities, skipped)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphDelta {
    pub delta_id: String,
    pub source_authority_ref: String,
    #[serde(default)]
    pub added_fragments: Vec<String>,
    #[serde(default)]
    pub refreshed_fragments: Vec<String>,
    #[serde(default)]
    pub invalidated_fragments: Vec<String>,
    #[serde(default)]
    pub retired_fragments: Vec<String>,
    pub budget_delta: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceActionResult {
    pub source_authority_ref: String,
    #[serde(default)]
    pub added_fragments: Vec<String>,
    #[serde(default)]
    pub refreshed_fragments: Vec<String>,
    #[serde(default)]
    pub invalidated_fragments: Vec<String>,
    #[serde(default)]
    pub retired_fragments: Vec<String>,
    pub cost_units_spent: u64,
}

#[derive(Debug, Default, Clone)]
pub struct GraphDeltaEmitter;

impl GraphDeltaEmitter {
    pub fn emit(&self, action_id: &str, result: SourceActionResult) -> GraphDelta {
        GraphDelta {
            delta_id: format!("delta:{}", stable_id(action_id)),
            source_authority_ref: result.source_authority_ref,
            added_fragments: result.added_fragments,
            refreshed_fragments: result.refreshed_fragments,
            invalidated_fragments: result.invalidated_fragments,
            retired_fragments: result.retired_fragments,
            budget_delta: -(result.cost_units_spent as i64),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionTrace {
    pub trace_id: String,
    #[serde(default)]
    pub steps: Vec<String>,
    #[serde(default)]
    pub normalized_probe_outcomes: Vec<NormalizedProbeOutcome>,
    #[serde(default)]
    pub budget_events: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProbeOutcomeKind {
    Visible,
    UnauthorizedExisting,
    InvisibleExisting,
    FilteredExisting,
    Nonexistent,
    SourceUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeOutcome {
    pub kind: ProbeOutcomeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visible_fragment: Option<SourceFragmentObservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_diagnostic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DisclosureClass {
    Disclosed,
    NotDisclosed,
    RetryLater,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedProbeOutcome {
    pub class: DisclosureClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visible_fragment: Option<VisibleFragment>,
    pub external_message: String,
}

#[derive(Debug, Default, Clone)]
pub struct NonCommittingProbeEngine;

impl NonCommittingProbeEngine {
    pub fn normalize(&self, outcome: ProbeOutcome) -> NormalizedProbeOutcome {
        match outcome.kind {
            ProbeOutcomeKind::Visible => {
                let visible_fragment = outcome
                    .visible_fragment
                    .map(|fragment| visible_fragment_from_observation(&fragment));
                NormalizedProbeOutcome {
                    class: DisclosureClass::Disclosed,
                    visible_fragment,
                    external_message: "discovery fragment available".into(),
                }
            }
            ProbeOutcomeKind::SourceUnavailable => NormalizedProbeOutcome {
                class: DisclosureClass::RetryLater,
                visible_fragment: None,
                external_message: "source cannot be evaluated within the current budget".into(),
            },
            ProbeOutcomeKind::UnauthorizedExisting
            | ProbeOutcomeKind::InvisibleExisting
            | ProbeOutcomeKind::FilteredExisting
            | ProbeOutcomeKind::Nonexistent => NormalizedProbeOutcome {
                class: DisclosureClass::NotDisclosed,
                visible_fragment: None,
                external_message: "no disclosable fragment under the current policy".into(),
            },
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PrivateDiscoveryOrchestrator {
    probe_engine: NonCommittingProbeEngine,
}

impl PrivateDiscoveryOrchestrator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn discover(
        &self,
        request: PrivateDataDiscoveryRequest,
        now_ms: u64,
    ) -> PrivateDataDiscoveryResponse {
        let visible_observations =
            self.filter_observations(&request, request.candidate_fragments.clone());
        let mut authority_records: Vec<_> = visible_observations
            .iter()
            .map(|observation| {
                SourceAuthorityRecord::from_observation(
                    observation,
                    freshness_window_for(&observation.node_kind),
                    AuthorityMode::NativeSource,
                )
            })
            .collect();
        let mut visible_fragments: Vec<_> = visible_observations
            .iter()
            .map(visible_fragment_from_observation)
            .collect();
        if let Some(scoped_data_view) = request.scoped_data_view.as_ref() {
            let mut known_fragment_ids = visible_fragments
                .iter()
                .map(|fragment| fragment.fragment_id.clone())
                .collect::<BTreeSet<_>>();
            for result in &scoped_data_view.visible_results {
                let Some((source, fragment, authority)) =
                    scoped_result_evidence(result, request.policy_context.grant_epoch)
                else {
                    continue;
                };
                if request.policy_context.allows(&source)
                    && request.discovery_scope.allows(&source)
                    && known_fragment_ids.insert(fragment.fragment_id.clone())
                {
                    visible_fragments.push(fragment);
                    authority_records.push(authority);
                }
            }
        }
        let guards: Vec<_> = authority_records
            .iter()
            .map(|record| {
                FreshnessGuard::evaluate(record, now_ms, request.policy_context.grant_epoch, false)
            })
            .collect();
        let bounded_source_actions = compile_bounded_source_actions(
            &visible_fragments,
            &authority_records,
            &guards,
            &request.source_capabilities,
        );
        let discovery_budget = DiscoveryBudget::for_time_budget(request.time_budget_ms);
        let discovery_frontier = schedule_frontier(
            bounded_source_actions.clone(),
            request.time_budget_ms,
            &discovery_budget,
            &request.discovery_intent.priorities,
            &BTreeSet::new(),
        );
        let scoped_discovery_graph = build_scoped_discovery_graph(
            &visible_fragments,
            &authority_records,
            &guards,
            &bounded_source_actions,
            now_ms + request.time_budget_ms,
            &request.policy_context,
        );
        let execution_trace = ExecutionTrace {
            trace_id: format!("trace:{}:{}", request.policy_context.user_id, now_ms),
            steps: vec![
                "normalized candidate fragments".into(),
                "built source authority records".into(),
                "compiled bounded source actions".into(),
                "scheduled BFS frontier".into(),
            ],
            normalized_probe_outcomes: Vec::new(),
            budget_events: vec![format!("initial_time_budget_ms={}", request.time_budget_ms)],
        };

        PrivateDataDiscoveryResponse {
            visible_fragments,
            source_authority_records: authority_records,
            scoped_discovery_graph,
            discovery_frontier,
            discovery_budget,
            execution_trace,
        }
    }

    pub fn discover_with_adapters(
        &self,
        mut request: PrivateDataDiscoveryRequest,
        adapters: &[&dyn DiscoverySourceAdapter],
        now_ms: u64,
    ) -> PrivateDataDiscoveryResponse {
        let max_fragments = request
            .disclosure_budget
            .max_fragments
            .min(request.discovery_scope.max_fragments);
        let mut adapter_observations = Vec::new();
        let mut normalized_probe_outcomes = Vec::new();
        let mut adapter_capabilities = Vec::new();

        for adapter in adapters {
            let source = adapter.source_key();
            if !request.policy_context.allows(&source) || !request.discovery_scope.allows(&source) {
                continue;
            }
            adapter_capabilities.extend(adapter.capabilities());
            let remaining = max_fragments.saturating_sub(adapter_observations.len());
            if remaining == 0 {
                break;
            }
            let adapter_request = SourceAdapterDiscoveryRequest {
                policy_context: request.policy_context.clone(),
                discovery_scope: request.discovery_scope.clone(),
                max_fragments: remaining,
                now_ms,
            };
            for outcome in adapter.discover(&adapter_request) {
                let visible_observation = if matches!(outcome.kind, ProbeOutcomeKind::Visible) {
                    outcome.visible_fragment.clone()
                } else {
                    None
                };
                normalized_probe_outcomes.push(self.probe_engine.normalize(outcome));
                if let Some(observation) = visible_observation {
                    adapter_observations.push(observation);
                    if adapter_observations.len() >= max_fragments {
                        break;
                    }
                }
            }
        }

        request.candidate_fragments.extend(adapter_observations);
        request.source_capabilities.extend(adapter_capabilities);
        let mut response = self.discover(request, now_ms);
        if !normalized_probe_outcomes.is_empty() {
            response
                .execution_trace
                .steps
                .insert(0, "queried source adapters".into());
            response.execution_trace.normalized_probe_outcomes = normalized_probe_outcomes;
        }
        response
    }

    pub fn normalize_probe(&self, outcome: ProbeOutcome) -> NormalizedProbeOutcome {
        self.probe_engine.normalize(outcome)
    }

    fn filter_observations(
        &self,
        request: &PrivateDataDiscoveryRequest,
        observations: Vec<SourceFragmentObservation>,
    ) -> Vec<SourceFragmentObservation> {
        let max_fragments = request
            .disclosure_budget
            .max_fragments
            .min(request.discovery_scope.max_fragments);
        observations
            .into_iter()
            .filter(|observation| {
                let source = observation.source_key();
                request.policy_context.allows(&source) && request.discovery_scope.allows(&source)
            })
            .take(max_fragments)
            .collect()
    }
}

fn visible_fragment_from_observation(observation: &SourceFragmentObservation) -> VisibleFragment {
    let authority = authority_ref(
        &observation.source_kind,
        &observation.source_instance,
        &observation.local_id,
    );
    VisibleFragment {
        fragment_id: fragment_id(
            &observation.source_kind,
            &observation.source_instance,
            &observation.local_id,
        ),
        source_kind: observation.source_kind.clone(),
        source_instance: observation.source_instance.clone(),
        local_ref: observation.native_ref.clone(),
        display_label: observation.display_name.clone(),
        fragment_kind: observation.node_kind.clone(),
        source_authority_ref: authority,
        confidence: FragmentConfidence::Observed,
    }
}

fn scoped_result_evidence(
    result: &ScopedVisibleResult,
    grant_epoch: Option<u64>,
) -> Option<(SourceInstanceKey, VisibleFragment, SourceAuthorityRecord)> {
    let (source_kind, source_instance) =
        source_key_from_authority_ref(result.source_authority_ref.as_ref())?;
    let source = SourceInstanceKey::new(source_kind.clone(), source_instance.clone());
    let anchor = result.native_anchors.first()?;
    let node_kind = node_kind_from_entity_kind(&result.entity_kind);
    let display_label = ["name", "key", "table", "index", "path"]
        .iter()
        .find_map(|key| {
            result
                .properties
                .get(*key)
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or(&anchor.native_ref)
        .to_string();
    let fragment = VisibleFragment {
        fragment_id: result.result_ref.0.clone(),
        source_kind: source_kind.clone(),
        source_instance: source_instance.clone(),
        local_ref: anchor.native_ref.clone(),
        display_label,
        fragment_kind: node_kind.clone(),
        source_authority_ref: result.source_authority_ref.0.clone(),
        confidence: FragmentConfidence::Observed,
    };
    let authority = SourceAuthorityRecord {
        authority_ref: result.source_authority_ref.0.clone(),
        source_kind: source_kind.clone(),
        source_instance: source_instance.clone(),
        native_anchor: NativeAnchor {
            source_kind,
            source_instance,
            native_ref: anchor.native_ref.clone(),
            native_pointer: anchor
                .native_pointer
                .clone()
                .unwrap_or_else(|| anchor.native_ref.clone()),
            native_version: anchor.native_version.clone(),
            cache_pointer: None,
            grant_epoch,
        },
        observed_at_ms: result.observed_at_ms,
        source_revision: anchor.native_version.clone(),
        fingerprint: anchor.native_version.clone(),
        grant_epoch,
        freshness_window_ms: freshness_window_for(&node_kind),
        revalidation_action: format!("revalidate:{}", result.source_authority_ref),
        authority_mode: AuthorityMode::ObservationOnly,
    };
    Some((source, fragment, authority))
}

fn source_key_from_authority_ref(authority_ref: &str) -> Option<(SourceKind, String)> {
    let parts = authority_ref.split(':').collect::<Vec<_>>();
    let kind_index = match parts.first().copied() {
        Some("authority" | "source") => 1,
        _ => return None,
    };
    let kind = match parts.get(kind_index).copied()? {
        "fs" => SourceKind::Fs,
        "local-fs" => SourceKind::LocalFs,
        "s3" => SourceKind::S3,
        "es" => SourceKind::Es,
        "mysql" => SourceKind::Mysql,
        "pg" => SourceKind::Pg,
        "csv" => SourceKind::Csv,
        "jsonl" => SourceKind::Jsonl,
        other => SourceKind::Other(other.to_string()),
    };
    let source_instance = parts.get(kind_index + 1).copied().unwrap_or(authority_ref);
    Some((kind, source_instance.to_string()))
}

fn node_kind_from_entity_kind(entity_kind: &str) -> NodeKind {
    if entity_kind.ends_with("field") || entity_kind.ends_with("column") {
        NodeKind::Field
    } else if entity_kind.ends_with("table")
        || entity_kind.ends_with("index")
        || entity_kind.ends_with("prefix")
        || entity_kind.ends_with("directory")
    {
        NodeKind::Collection
    } else if entity_kind.ends_with("source") {
        NodeKind::Namespace
    } else {
        NodeKind::Asset
    }
}

fn compile_bounded_source_actions(
    fragments: &[VisibleFragment],
    authority_records: &[SourceAuthorityRecord],
    guards: &[FreshnessGuard],
    capabilities: &[SourceCapability],
) -> Vec<BoundedSourceAction> {
    let authority_by_ref: BTreeMap<_, _> = authority_records
        .iter()
        .map(|record| (record.authority_ref.as_str(), record))
        .collect();
    let guard_by_ref: BTreeMap<_, _> = guards
        .iter()
        .map(|guard| (guard.source_authority_ref.as_str(), guard))
        .collect();
    let mut actions = Vec::new();
    for fragment in fragments {
        let source =
            SourceInstanceKey::new(fragment.source_kind.clone(), &fragment.source_instance);
        let matching_capabilities = capabilities
            .iter()
            .filter(|capability| capability.source_key() == source)
            .cloned()
            .collect::<Vec<_>>();
        let capability_rows = if matching_capabilities.is_empty() {
            vec![default_capability_for(fragment)]
        } else {
            matching_capabilities
        };
        for capability in capability_rows {
            for operation in &capability.operations {
                let authority_ref = fragment.source_authority_ref.clone();
                let guard = guard_by_ref
                    .get(authority_ref.as_str())
                    .copied()
                    .expect("guard exists for authority");
                let precondition = if operation.is_high_risk() {
                    FreshnessState::RequiresRevalidation
                } else {
                    guard.state.clone()
                };
                let mut failure_modes =
                    vec![FailureMode::NotDisclosed, FailureMode::BudgetExceeded];
                if matches!(
                    precondition,
                    FreshnessState::RequiresRevalidation | FreshnessState::Invalidated
                ) {
                    failure_modes.push(FailureMode::NeedsRevalidation);
                }
                let record = authority_by_ref
                    .get(authority_ref.as_str())
                    .expect("authority exists for fragment");
                let action = BoundedSourceAction {
                    wire_version: DATA_VIEW_WIRE_VERSION,
                    action_id: format!(
                        "action:{}:{}",
                        operation_name(operation),
                        stable_id(&fragment.fragment_id)
                    ),
                    operation: operation.clone(),
                    source_authority_ref: SourceAuthorityRef(authority_ref),
                    observation_path: ObservationPath::Direct,
                    freshness_precondition: precondition,
                    permission_scope: format!(
                        "{}:{}",
                        fragment.source_kind.as_str(),
                        fragment.source_instance
                    ),
                    max_rows: capability.max_rows,
                    max_bytes: capability.max_bytes,
                    max_calls: capability.max_calls,
                    max_fanout: capability.max_fanout,
                    max_latency_ms: capability.max_latency_ms,
                    max_cost_units: capability.max_calls.max(1),
                    side_effect_level: SideEffectLevel::ReadOnly,
                    continuation_policy: if capability.max_fanout > 1 {
                        ContinuationPolicy::CursorBounded
                    } else {
                        ContinuationPolicy::None
                    },
                    failure_modes,
                    expansion_rules: vec![record.revalidation_action.clone()],
                    expected_information_gain: expected_information_gain(operation, fragment),
                    explanation: format!(
                        "{} on visible {} fragment from {}",
                        operation_name(operation),
                        fragment.fragment_kind.as_str(),
                        fragment.source_instance
                    ),
                };
                actions.push(content_address_action(action));
            }
        }
    }
    actions
}

fn content_address_action(mut action: BoundedSourceAction) -> BoundedSourceAction {
    let logical_id = std::mem::take(&mut action.action_id);
    let encoded = serde_json::to_vec(&action)
        .expect("serializing a BoundedSourceAction into identity material cannot fail");
    let mut material = Vec::with_capacity(logical_id.len() + encoded.len() + 1);
    material.extend_from_slice(logical_id.as_bytes());
    material.push(0);
    material.extend_from_slice(&encoded);
    let hash = digest(&SHA256, &material);
    let suffix = hash
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    action.action_id = format!("{logical_id}:{suffix}");
    action
}

fn schedule_frontier(
    mut actions: Vec<BoundedSourceAction>,
    time_budget_ms: u64,
    ledger: &DiscoveryBudget,
    priorities: &[String],
    skipped: &BTreeSet<String>,
) -> DiscoveryFrontier {
    let strategy = DiscoveryFrontierStrategy::for_time_budget(time_budget_ms);
    actions.retain(|action| {
        !skipped
            .iter()
            .any(|skip| action.permission_scope.contains(skip))
    });
    if matches!(strategy, DiscoveryFrontierStrategy::ShortBroadScan) {
        actions.retain(|action| {
            action.max_calls <= ledger.remaining_calls
                && action.max_fanout <= ledger.remaining_fanout
                && !matches!(
                    action.operation,
                    DiscoveryOperation::LimitedSample | DiscoveryOperation::AggregateProbe
                )
        });
    }
    actions.sort_by_key(|action| {
        let priority_boost = if priorities
            .iter()
            .any(|priority| action.permission_scope.contains(priority))
        {
            0
        } else {
            1
        };
        (
            priority_boost,
            action.operation.breadth_rank(),
            Reverse(action.expected_information_gain),
            action.permission_scope.clone(),
            action.action_id.clone(),
        )
    });
    let ranked_actions = actions
        .into_iter()
        .enumerate()
        .map(|(idx, action)| RankedSourceAction {
            rank: idx + 1,
            action,
        })
        .collect();
    DiscoveryFrontier {
        strategy,
        ranked_actions,
        skipped_branches: skipped.iter().cloned().collect(),
        explanation: "ranked BFS-first under permission, freshness, and budget constraints".into(),
    }
}

fn build_scoped_discovery_graph(
    fragments: &[VisibleFragment],
    authority_records: &[SourceAuthorityRecord],
    guards: &[FreshnessGuard],
    actions: &[BoundedSourceAction],
    expires_at_ms: u64,
    policy: &PolicyContext,
) -> ScopedDiscoveryGraph {
    let resource_nodes = fragments
        .iter()
        .map(|fragment| ResourceNode {
            resource_id: fragment.fragment_id.clone(),
            fragment_id: fragment.fragment_id.clone(),
            source_authority_ref: fragment.source_authority_ref.clone(),
        })
        .collect::<Vec<_>>();
    let action_nodes = actions
        .iter()
        .map(|action| ActionNode {
            action_id: action.action_id.clone(),
            operation: action.operation.clone(),
            source_authority_ref: action.source_authority_ref.0.clone(),
        })
        .collect::<Vec<_>>();
    let visibility_edges = fragments
        .iter()
        .map(|fragment| PermissionEdge {
            from: policy.user_id.clone(),
            to: fragment.fragment_id.clone(),
            edge_kind: "visible".into(),
        })
        .collect::<Vec<_>>();
    let executable_edges = actions
        .iter()
        .map(|action| PermissionEdge {
            from: action.source_authority_ref.0.clone(),
            to: action.action_id.clone(),
            edge_kind: "executable-under-budget".into(),
        })
        .collect::<Vec<_>>();
    let expansion_edges = actions
        .iter()
        .flat_map(|action| {
            action.expansion_rules.iter().map(|rule| PermissionEdge {
                from: action.action_id.clone(),
                to: rule.clone(),
                edge_kind: "expands-by".into(),
            })
        })
        .collect::<Vec<_>>();
    ScopedDiscoveryGraph {
        resource_nodes,
        action_nodes,
        visibility_edges,
        executable_edges,
        expansion_edges,
        source_authority_refs: authority_records
            .iter()
            .map(|record| record.authority_ref.clone())
            .collect(),
        policy_constraints: vec![
            format!("tenant={}", policy.tenant_id),
            format!("task_purpose={}", policy.task_purpose),
        ],
        freshness_constraints: guards.to_vec(),
        expires_at_ms,
    }
}

fn default_capability_for(fragment: &VisibleFragment) -> SourceCapability {
    default_capability_for_source(
        fragment.source_kind.clone(),
        fragment.source_instance.clone(),
    )
}

fn default_capability_for_source(
    source_kind: SourceKind,
    source_instance: impl Into<String>,
) -> SourceCapability {
    let operations = default_operations_for_source(&source_kind);
    SourceCapability {
        source_kind,
        source_instance: source_instance.into(),
        operations,
        max_rows: 100,
        max_bytes: 64 * 1024,
        max_calls: 1,
        max_fanout: 10,
        max_latency_ms: 1_000,
    }
}

fn default_operations_for_source(source_kind: &SourceKind) -> Vec<DiscoveryOperation> {
    match source_kind {
        SourceKind::Pg | SourceKind::Mysql => vec![
            DiscoveryOperation::SourceStatus,
            DiscoveryOperation::SchemaProbe,
            DiscoveryOperation::FieldDiscovery,
            DiscoveryOperation::LimitedSample,
            DiscoveryOperation::AggregateProbe,
        ],
        SourceKind::S3 | SourceKind::Fs | SourceKind::LocalFs => vec![
            DiscoveryOperation::SourceStatus,
            DiscoveryOperation::ObjectList,
            DiscoveryOperation::ObjectHead,
            DiscoveryOperation::TreeProbe,
        ],
        SourceKind::Es => vec![
            DiscoveryOperation::SourceStatus,
            DiscoveryOperation::MappingProbe,
            DiscoveryOperation::FieldDiscovery,
            DiscoveryOperation::AggregateProbe,
        ],
        _ => vec![
            DiscoveryOperation::SourceStatus,
            DiscoveryOperation::SchemaProbe,
            DiscoveryOperation::JoinCandidateProbe,
        ],
    }
}

fn expected_information_gain(operation: &DiscoveryOperation, fragment: &VisibleFragment) -> u8 {
    let base: u8 = match operation {
        DiscoveryOperation::SourceStatus => 4,
        DiscoveryOperation::SchemaProbe
        | DiscoveryOperation::ObjectList
        | DiscoveryOperation::MappingProbe
        | DiscoveryOperation::TreeProbe => 8,
        DiscoveryOperation::FieldDiscovery | DiscoveryOperation::ObjectHead => 6,
        DiscoveryOperation::JoinCandidateProbe => 5,
        DiscoveryOperation::LimitedSample | DiscoveryOperation::AggregateProbe => 3,
        DiscoveryOperation::Revalidate => 2,
        DiscoveryOperation::Extension(_) => 1,
    };
    if matches!(
        fragment.fragment_kind,
        NodeKind::Collection | NodeKind::Namespace
    ) {
        base.saturating_add(1)
    } else {
        base
    }
}

fn freshness_window_for(node_kind: &NodeKind) -> u64 {
    match node_kind {
        NodeKind::Root | NodeKind::Namespace => 5 * 60 * 1000,
        NodeKind::Collection | NodeKind::Field => 10 * 60 * 1000,
        NodeKind::Asset | NodeKind::Entity => 2 * 60 * 1000,
        NodeKind::Other(_) => 60 * 1000,
    }
}

fn authority_ref(source_kind: &SourceKind, source_instance: &str, local_id: &str) -> String {
    format!(
        "authority:{}:{}:{}",
        source_kind.as_str(),
        stable_id(source_instance),
        stable_id(local_id)
    )
}

fn fragment_id(source_kind: &SourceKind, source_instance: &str, local_id: &str) -> String {
    format!(
        "fragment:{}:{}:{}",
        source_kind.as_str(),
        stable_id(source_instance),
        stable_id(local_id)
    )
}

fn operation_name(operation: &DiscoveryOperation) -> &str {
    operation.as_str()
}

fn discover_csv_fields(path: &Path) -> io::Result<Vec<String>> {
    let file = fs::File::open(path)?;
    let mut lines = io::BufReader::new(file).lines();
    let Some(header) = lines.next().transpose()? else {
        return Ok(Vec::new());
    };
    Ok(header
        .split(',')
        .map(str::trim)
        .filter(|field| !field.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

fn discover_jsonl_fields(path: &Path) -> io::Result<Vec<String>> {
    let file = fs::File::open(path)?;
    let mut fields = BTreeSet::new();
    for line in io::BufReader::new(file).lines().take(8) {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value: JsonValue = serde_json::from_str(&line)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if let Some(object) = value.as_object() {
            fields.extend(object.keys().cloned());
            if !fields.is_empty() {
                break;
            }
        }
    }
    Ok(fields.into_iter().collect())
}

fn file_native_pointer(path: &Path) -> String {
    match path.canonicalize() {
        Ok(path) => format!("file://{}", path.display()),
        Err(_) => format!("file://{}", path.display()),
    }
}

fn file_fingerprint(path: &Path) -> Option<String> {
    let metadata = path.metadata().ok()?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok());
    let modified_ns = modified
        .map(|duration| duration.as_nanos().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    Some(format!("file:{}:{}", metadata.len(), modified_ns))
}

fn probe_kind_from_io_error(error: &io::Error) -> ProbeOutcomeKind {
    match error.kind() {
        io::ErrorKind::NotFound => ProbeOutcomeKind::Nonexistent,
        io::ErrorKind::PermissionDenied => ProbeOutcomeKind::UnauthorizedExisting,
        _ => ProbeOutcomeKind::SourceUnavailable,
    }
}

fn stable_id(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            let ch = byte as char;
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch.to_string()
            } else {
                format!("~{byte:02x}")
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::fs;

    fn policy() -> PolicyContext {
        PolicyContext {
            user_id: "alice".into(),
            tenant_id: "tenant-a".into(),
            task_purpose: "research".into(),
            allowed_source_instances: BTreeSet::from([
                SourceInstanceKey::new(SourceKind::Pg, "warehouse"),
                SourceInstanceKey::new(SourceKind::S3, "corp-bucket"),
            ]),
            grant_epoch: Some(12),
            request_context: BTreeMap::from([("trace_id".into(), "trace-1".into())]),
        }
    }

    fn observation(
        source_kind: SourceKind,
        source_instance: &str,
        local_id: &str,
    ) -> SourceFragmentObservation {
        SourceFragmentObservation {
            source_kind,
            source_instance: source_instance.into(),
            local_id: local_id.into(),
            node_kind: NodeKind::Collection,
            native_ref: local_id.into(),
            display_name: local_id.into(),
            native_pointer: format!("native://{source_instance}/{local_id}"),
            fingerprint: Some("fp-1".into()),
            observed_at_ms: 1_000,
            grant_epoch: Some(12),
        }
    }

    fn request(time_budget_ms: u64) -> PrivateDataDiscoveryRequest {
        PrivateDataDiscoveryRequest {
            discovery_intent: DiscoveryIntent {
                task: "find risk evidence".into(),
                priorities: Vec::new(),
            },
            policy_context: policy(),
            discovery_scope: DiscoveryScope {
                source_instances: BTreeSet::new(),
                max_fragments: 10,
            },
            time_budget_ms,
            disclosure_budget: DisclosureBudget {
                max_fragments: 10,
                max_names: 10,
                max_relation_hints: 0,
            },
            candidate_fragments: vec![
                observation(SourceKind::Pg, "warehouse", "public.orders"),
                observation(SourceKind::S3, "corp-bucket", "finance/"),
                observation(SourceKind::Mysql, "hidden-db", "secret.table"),
            ],
            scoped_data_view: None,
            source_capabilities: Vec::new(),
        }
    }

    #[test]
    fn non_committing_probe_normalizes_hidden_and_missing() {
        let engine = NonCommittingProbeEngine;
        let outcomes = [
            ProbeOutcomeKind::UnauthorizedExisting,
            ProbeOutcomeKind::InvisibleExisting,
            ProbeOutcomeKind::FilteredExisting,
            ProbeOutcomeKind::Nonexistent,
        ];
        for kind in outcomes {
            let normalized = engine.normalize(ProbeOutcome {
                kind,
                visible_fragment: None,
                internal_diagnostic: Some("secret/path".into()),
            });
            assert_eq!(normalized.class, DisclosureClass::NotDisclosed);
            assert!(normalized.visible_fragment.is_none());
            assert!(!normalized.external_message.contains("secret"));
        }
    }

    #[test]
    fn freshness_guard_expired_and_grant_epoch_requires_revalidation() {
        let record = SourceAuthorityRecord::from_observation(
            &observation(SourceKind::Pg, "warehouse", "public.orders"),
            100,
            AuthorityMode::NativeSource,
        );
        let stale = FreshnessGuard::evaluate(&record, 1_500, Some(12), false);
        assert_eq!(stale.state, FreshnessState::RequiresRevalidation);
        assert!(stale.required_before_execution);

        let invalid = FreshnessGuard::evaluate(&record, 1_010, Some(99), false);
        assert_eq!(invalid.state, FreshnessState::Invalidated);
    }

    #[test]
    fn action_identity_is_stable_for_identical_contract_and_changes_with_freshness() {
        let orchestrator = PrivateDiscoveryOrchestrator::new();
        let first = orchestrator.discover(request(30 * 60 * 1000), 1_100);
        let repeated = orchestrator.discover(request(30 * 60 * 1000), 1_100);
        let stale = orchestrator.discover(request(30 * 60 * 1000), 10_000_000);
        let select = |response: &PrivateDataDiscoveryResponse| {
            response
                .discovery_frontier
                .ranked_actions
                .iter()
                .find(|ranked| {
                    ranked.action.permission_scope == "s3:corp-bucket"
                        && ranked.action.operation == DiscoveryOperation::ObjectList
                })
                .expect("S3 object-list action")
                .action
                .clone()
        };

        let first = select(&first);
        let repeated = select(&repeated);
        let stale = select(&stale);
        assert_eq!(first, repeated);
        assert_eq!(first.freshness_precondition, FreshnessState::Fresh);
        assert_eq!(
            stale.freshness_precondition,
            FreshnessState::RequiresRevalidation
        );
        assert_ne!(first.action_id, stale.action_id);
    }

    #[test]
    fn budget_frontiers_differ_by_time_budget() {
        let orchestrator = PrivateDiscoveryOrchestrator::new();
        let short = orchestrator.discover(request(5 * 60 * 1000), 1_100);
        let medium = orchestrator.discover(request(30 * 60 * 1000), 1_100);
        let long = orchestrator.discover(request(2 * 60 * 60 * 1000), 1_100);

        assert_eq!(
            short.discovery_frontier.strategy,
            DiscoveryFrontierStrategy::ShortBroadScan
        );
        assert_eq!(
            medium.discovery_frontier.strategy,
            DiscoveryFrontierStrategy::BalancedBreadthThenDepth
        );
        assert_eq!(
            long.discovery_frontier.strategy,
            DiscoveryFrontierStrategy::DeepenAndVerify
        );
        assert!(short.discovery_budget.remaining_calls < medium.discovery_budget.remaining_calls);
        assert!(medium.discovery_budget.remaining_calls < long.discovery_budget.remaining_calls);
        assert!(
            short.discovery_frontier.ranked_actions.len()
                <= medium.discovery_frontier.ranked_actions.len()
        );
    }

    #[test]
    fn orchestrator_builds_bfs_scoped_discovery_graph_without_direct_reads() {
        let orchestrator = PrivateDiscoveryOrchestrator::new();
        let response = orchestrator.discover(request(5 * 60 * 1000), 1_100);

        assert_eq!(response.visible_fragments.len(), 2);
        assert!(
            response
                .visible_fragments
                .iter()
                .all(|fragment| fragment.source_instance != "hidden-db")
        );
        assert_eq!(
            response.scoped_discovery_graph.resource_nodes.len(),
            response.visible_fragments.len()
        );
        assert!(
            response
                .discovery_frontier
                .ranked_actions
                .iter()
                .any(|ranked| ranked.action.permission_scope == "pg:warehouse")
        );
        assert!(
            response
                .discovery_frontier
                .ranked_actions
                .iter()
                .any(|ranked| ranked.action.permission_scope == "s3:corp-bucket")
        );
        assert!(
            response
                .discovery_frontier
                .ranked_actions
                .iter()
                .all(|ranked| ranked.action.max_calls > 0)
        );
    }

    #[test]
    fn discovery_intent_priorities_rank_source_without_bypassing_freshness() {
        let orchestrator = PrivateDiscoveryOrchestrator::new();
        let mut request = request(30 * 60 * 1000);
        request
            .discovery_intent
            .priorities
            .push("s3:corp-bucket".into());
        let response = orchestrator.discover(request, 1_100);
        assert_eq!(
            response.discovery_frontier.ranked_actions[0]
                .action
                .permission_scope,
            "s3:corp-bucket"
        );
        let high_risk = response
            .discovery_frontier
            .ranked_actions
            .iter()
            .find(|ranked| {
                ranked.action.freshness_precondition == FreshnessState::RequiresRevalidation
            })
            .expect("high-risk revalidation action");
        assert!(
            high_risk
                .action
                .failure_modes
                .contains(&FailureMode::NeedsRevalidation)
        );
    }

    #[test]
    fn policy_context_builder_preserves_capanix_carriers_as_domain_context() {
        let policy = PolicyContextBuilder::new("alice", "tenant-a", "risk-research")
            .allow_source(SourceKind::Pg, "warehouse")
            .grant_epoch(12)
            .request_context("trace_id", "trace-1")
            .request_context("deadline_ms", "300000")
            .build();

        assert!(policy.allows(&SourceInstanceKey::new(SourceKind::Pg, "warehouse")));
        assert!(!policy.allows(&SourceInstanceKey::new(SourceKind::S3, "corp-bucket")));
        assert_eq!(policy.grant_epoch, Some(12));
        assert_eq!(
            policy
                .request_context
                .get("deadline_ms")
                .map(String::as_str),
            Some("300000")
        );
    }

    #[test]
    fn frontier_explanation_and_graph_delta_emitter_are_available() {
        let orchestrator = PrivateDiscoveryOrchestrator::new();
        let response = orchestrator.discover(request(30 * 60 * 1000), 1_100);
        let ranked = response.discovery_frontier.ranked_actions[0].clone();
        let explanation = ranked.action.explanation.clone();

        assert!(explanation.contains("visible"));

        let delta = GraphDeltaEmitter.emit(
            &ranked.action.action_id,
            SourceActionResult {
                source_authority_ref: ranked.action.source_authority_ref.0.clone(),
                added_fragments: vec!["fragment:new".into()],
                refreshed_fragments: vec![ranked.action.permission_scope.clone()],
                invalidated_fragments: vec!["fragment:stale".into()],
                retired_fragments: vec!["fragment:old".into()],
                cost_units_spent: 3,
            },
        );

        assert_eq!(
            delta.source_authority_ref,
            ranked.action.source_authority_ref
        );
        assert_eq!(delta.added_fragments, vec!["fragment:new"]);
        assert_eq!(delta.invalidated_fragments, vec!["fragment:stale"]);
        assert_eq!(delta.retired_fragments, vec!["fragment:old"]);
        assert_eq!(delta.budget_delta, -3);
    }

    #[test]
    fn real_file_source_adapters_feed_visible_fragments_into_frontier() {
        let temp = tempfile::tempdir().expect("tempdir");
        let csv_path = temp.path().join("orders.csv");
        let jsonl_path = temp.path().join("events.jsonl");
        let local_root = temp.path().join("workspace");
        fs::create_dir(&local_root).expect("local root");
        fs::write(&csv_path, "order_id,amount\n1,42\n").expect("csv fixture");
        fs::write(&jsonl_path, "{\"event_id\":\"e1\",\"severity\":\"high\"}\n")
            .expect("jsonl fixture");
        fs::write(local_root.join("readme.txt"), "visible").expect("local file");

        let request = PrivateDataDiscoveryRequest {
            discovery_intent: DiscoveryIntent {
                task: "find operational risk evidence".into(),
                priorities: Vec::new(),
            },
            policy_context: PolicyContextBuilder::new("alice", "tenant-a", "risk-research")
                .allow_source(SourceKind::Csv, "reports")
                .allow_source(SourceKind::Jsonl, "events")
                .allow_source(SourceKind::LocalFs, "workspace")
                .grant_epoch(7)
                .request_context("trace_id", "trace-file-adapters")
                .build(),
            discovery_scope: DiscoveryScope {
                source_instances: BTreeSet::new(),
                max_fragments: 16,
            },
            time_budget_ms: 5 * 60 * 1000,
            disclosure_budget: DisclosureBudget {
                max_fragments: 16,
                max_names: 16,
                max_relation_hints: 0,
            },
            candidate_fragments: Vec::new(),
            scoped_data_view: None,
            source_capabilities: Vec::new(),
        };
        let csv = FileSourceAdapter::csv_file("reports", &csv_path).grant_epoch(7);
        let jsonl = FileSourceAdapter::jsonl_file("events", &jsonl_path).grant_epoch(7);
        let local = FileSourceAdapter::local_fs("workspace", &local_root).grant_epoch(7);
        let adapters: [&dyn DiscoverySourceAdapter; 3] = [&csv, &jsonl, &local];

        let response =
            PrivateDiscoveryOrchestrator::new().discover_with_adapters(request, &adapters, 2_000);

        let labels = response
            .visible_fragments
            .iter()
            .map(|fragment| fragment.display_label.as_str())
            .collect::<BTreeSet<_>>();
        assert!(labels.contains("order_id"));
        assert!(labels.contains("amount"));
        assert!(labels.contains("event_id"));
        assert!(labels.contains("severity"));
        assert!(labels.contains("readme.txt"));
        assert!(
            response
                .discovery_frontier
                .ranked_actions
                .iter()
                .any(|ranked| {
                    ranked.action.permission_scope == "csv:reports"
                        && ranked.action.operation == DiscoveryOperation::SchemaProbe
                })
        );
        assert!(
            response
                .discovery_frontier
                .ranked_actions
                .iter()
                .any(|ranked| {
                    ranked.action.permission_scope == "jsonl:events"
                        && ranked.action.operation == DiscoveryOperation::SchemaProbe
                })
        );
        assert!(
            response
                .discovery_frontier
                .ranked_actions
                .iter()
                .any(|ranked| {
                    ranked.action.permission_scope == "local-fs:workspace"
                        && ranked.action.operation == DiscoveryOperation::ObjectList
                })
        );
        assert!(
            response
                .execution_trace
                .steps
                .iter()
                .any(|step| step == "queried source adapters")
        );
        assert!(
            response
                .execution_trace
                .normalized_probe_outcomes
                .iter()
                .all(|outcome| !outcome
                    .external_message
                    .contains(temp.path().to_string_lossy().as_ref()))
        );
    }

    #[test]
    fn typed_source_payload_adapters_feed_authoritative_fragments_into_frontier() {
        let pg = pg_source::PgFieldCatalog {
            object_ref: "warehouse".into(),
            fields: vec![
                pg_source::PgFieldDescriptor {
                    schema: "public".into(),
                    table: "orders".into(),
                    column: "order_id".into(),
                    ordinal_position: Some(1),
                    column_type: Some("int8".into()),
                },
                pg_source::PgFieldDescriptor {
                    schema: "public".into(),
                    table: "orders".into(),
                    column: "risk_score".into(),
                    ordinal_position: Some(2),
                    column_type: Some("numeric".into()),
                },
            ],
        };
        let s3 = s3_source::S3ObjectBatch {
            object_ref: "corp-bucket".into(),
            bucket: "corp-bucket".into(),
            prefix: "finance/2026/".into(),
            source_revision: "s3-list-observation:risk".into(),
            objects: vec![s3_source::S3ObjectEvent {
                key: "finance/2026/risk.csv".into(),
                size: 4_096,
                last_modified_unix_ms: 2_000,
                etag: Some("etag-risk".into()),
                storage_class: Some("STANDARD".into()),
                owner_id: None,
                owner_display_name: None,
                metadata: JsonValue::Null,
            }],
            next_cursor: None,
            has_more: false,
            grant_epoch: Some(12),
            diagnostics: None,
        };
        let request = PrivateDataDiscoveryRequest {
            discovery_intent: DiscoveryIntent {
                task: "find cross-source risk evidence".into(),
                priorities: Vec::new(),
            },
            policy_context: PolicyContextBuilder::new("alice", "tenant-a", "risk-research")
                .allow_source(SourceKind::Pg, "warehouse")
                .allow_source(SourceKind::S3, "corp-bucket")
                .grant_epoch(12)
                .request_context("trace_id", "trace-source-payload-adapters")
                .build(),
            discovery_scope: DiscoveryScope {
                source_instances: BTreeSet::new(),
                max_fragments: 16,
            },
            time_budget_ms: 5 * 60 * 1000,
            disclosure_budget: DisclosureBudget {
                max_fragments: 16,
                max_names: 16,
                max_relation_hints: 0,
            },
            candidate_fragments: Vec::new(),
            scoped_data_view: None,
            source_capabilities: Vec::new(),
        };
        let pg_adapter = SourcePayloadDiscoveryAdapter::from_pg_fields(&pg, 2_000, Some(12));
        let s3_adapter = SourcePayloadDiscoveryAdapter::from_s3_objects(&s3);
        let adapters: [&dyn DiscoverySourceAdapter; 2] = [&pg_adapter, &s3_adapter];

        let response =
            PrivateDiscoveryOrchestrator::new().discover_with_adapters(request, &adapters, 2_000);

        let labels = response
            .visible_fragments
            .iter()
            .map(|fragment| fragment.display_label.as_str())
            .collect::<BTreeSet<_>>();
        assert!(labels.contains("orders"));
        assert!(labels.contains("risk_score"));
        assert!(labels.contains("finance/2026/risk.csv"));
        assert!(
            response
                .source_authority_records
                .iter()
                .all(|record| record.grant_epoch == Some(12))
        );
        assert!(
            response
                .discovery_frontier
                .ranked_actions
                .iter()
                .any(|ranked| {
                    ranked.action.permission_scope == "pg:warehouse"
                        && ranked.action.operation == DiscoveryOperation::SchemaProbe
                })
        );
        assert!(
            response
                .discovery_frontier
                .ranked_actions
                .iter()
                .any(|ranked| {
                    ranked.action.permission_scope == "s3:corp-bucket"
                        && ranked.action.operation == DiscoveryOperation::ObjectList
                })
        );
        assert!(
            response
                .execution_trace
                .steps
                .iter()
                .any(|step| step == "queried source adapters")
        );
    }

    #[test]
    fn all_typed_source_payload_adapters_feed_visible_fragments() {
        let mysql = mysql_source::MysqlFieldCatalog {
            object_ref: "mysql-prod".into(),
            fields: vec![mysql_source::MysqlFieldDescriptor {
                schema: "app".into(),
                table: "users".into(),
                column: "email".into(),
                ordinal_position: Some(1),
                column_type: Some("varchar".into()),
            }],
        };
        let es = es_source::EsFieldCatalog {
            object_ref: "es-prod".into(),
            index: "events".into(),
            fields: vec![es_source::EsFieldDescriptor {
                name: "severity".into(),
                field_type: Some("keyword".into()),
            }],
        };
        let csv = csv_source::CsvRecordBatch {
            object_ref: "csv-prod".into(),
            root_path: "/tmp".into(),
            path: "orders.csv".into(),
            source_revision: "fs:orders".into(),
            fields: vec!["order_id".into()],
            records: vec![csv_source::CsvRecord {
                row_number: 1,
                values: JsonValue::Null,
            }],
            next_cursor: None,
            has_more: false,
            grant_epoch: Some(12),
            diagnostics: None,
        };
        let jsonl = jsonl_source::JsonlRecordBatch {
            object_ref: "jsonl-prod".into(),
            root_path: "/tmp".into(),
            path: "events.jsonl".into(),
            source_revision: "fs:events".into(),
            records: vec![jsonl_source::JsonlRecord {
                line_number: 1,
                values: JsonValue::Null,
            }],
            next_cursor: None,
            has_more: false,
            grant_epoch: Some(12),
            diagnostics: None,
        };
        let local_fs = local_fs_source::LocalFsEntryBatch {
            object_ref: "workspace".into(),
            root_path: "/workspace".into(),
            path: ".".into(),
            source_revision: "fs:workspace".into(),
            entries: vec![local_fs_source::LocalFsEntry {
                path: "readme.txt".into(),
                file_name: "readme.txt".into(),
                is_dir: false,
                size: 7,
                modified_unix_ms: Some(2_000),
            }],
            next_cursor: None,
            has_more: false,
            grant_epoch: Some(12),
            diagnostics: None,
        };
        let request = PrivateDataDiscoveryRequest {
            discovery_intent: DiscoveryIntent {
                task: "map all source families".into(),
                priorities: Vec::new(),
            },
            policy_context: PolicyContextBuilder::new("alice", "tenant-a", "research")
                .allow_source(SourceKind::Mysql, "mysql-prod")
                .allow_source(SourceKind::Es, "es-prod")
                .allow_source(SourceKind::Csv, "csv-prod")
                .allow_source(SourceKind::Jsonl, "jsonl-prod")
                .allow_source(SourceKind::LocalFs, "workspace")
                .grant_epoch(12)
                .build(),
            discovery_scope: DiscoveryScope {
                source_instances: BTreeSet::new(),
                max_fragments: 32,
            },
            time_budget_ms: 30 * 60 * 1000,
            disclosure_budget: DisclosureBudget {
                max_fragments: 32,
                max_names: 32,
                max_relation_hints: 0,
            },
            candidate_fragments: Vec::new(),
            scoped_data_view: None,
            source_capabilities: Vec::new(),
        };
        let mysql_adapter =
            SourcePayloadDiscoveryAdapter::from_mysql_fields(&mysql, 2_000, Some(12));
        let es_adapter = SourcePayloadDiscoveryAdapter::from_es_fields(&es, 2_000, Some(12));
        let csv_adapter = SourcePayloadDiscoveryAdapter::from_csv_records(&csv, 2_000);
        let jsonl_adapter = SourcePayloadDiscoveryAdapter::from_jsonl_records(&jsonl, 2_000);
        let local_adapter = SourcePayloadDiscoveryAdapter::from_local_fs_entries(&local_fs, 2_000);
        let adapters: [&dyn DiscoverySourceAdapter; 5] = [
            &mysql_adapter,
            &es_adapter,
            &csv_adapter,
            &jsonl_adapter,
            &local_adapter,
        ];

        let response =
            PrivateDiscoveryOrchestrator::new().discover_with_adapters(request, &adapters, 2_000);

        let labels = response
            .visible_fragments
            .iter()
            .map(|fragment| fragment.display_label.as_str())
            .collect::<BTreeSet<_>>();
        assert!(labels.contains("users"));
        assert!(labels.contains("severity"));
        assert!(labels.contains("row 1"));
        assert!(labels.contains("line 1"));
        assert!(labels.contains("readme.txt"));
        assert!(
            response
                .discovery_frontier
                .ranked_actions
                .iter()
                .any(|ranked| {
                    ranked.action.permission_scope == "mysql:mysql-prod"
                        && ranked.action.operation == DiscoveryOperation::SchemaProbe
                })
        );
        assert!(
            response
                .discovery_frontier
                .ranked_actions
                .iter()
                .any(|ranked| {
                    ranked.action.permission_scope == "es:es-prod"
                        && ranked.action.operation == DiscoveryOperation::MappingProbe
                })
        );
    }

    #[test]
    fn unauthorized_source_adapter_is_not_called_before_policy_filtering() {
        struct CountingAdapter {
            calls: Cell<usize>,
        }

        impl DiscoverySourceAdapter for CountingAdapter {
            fn source_key(&self) -> SourceInstanceKey {
                SourceInstanceKey::new(SourceKind::Mysql, "hidden-db")
            }

            fn discover(&self, _request: &SourceAdapterDiscoveryRequest) -> Vec<ProbeOutcome> {
                self.calls.set(self.calls.get() + 1);
                vec![ProbeOutcome {
                    kind: ProbeOutcomeKind::Visible,
                    visible_fragment: Some(observation(
                        SourceKind::Mysql,
                        "hidden-db",
                        "secret.table",
                    )),
                    internal_diagnostic: None,
                }]
            }
        }

        let adapter = CountingAdapter {
            calls: Cell::new(0),
        };
        let request = PrivateDataDiscoveryRequest {
            discovery_intent: DiscoveryIntent {
                task: "risk research".into(),
                priorities: Vec::new(),
            },
            policy_context: PolicyContextBuilder::new("alice", "tenant-a", "risk-research")
                .allow_source(SourceKind::Csv, "reports")
                .build(),
            discovery_scope: DiscoveryScope {
                source_instances: BTreeSet::new(),
                max_fragments: 10,
            },
            time_budget_ms: 5 * 60 * 1000,
            disclosure_budget: DisclosureBudget {
                max_fragments: 10,
                max_names: 10,
                max_relation_hints: 0,
            },
            candidate_fragments: Vec::new(),
            scoped_data_view: None,
            source_capabilities: Vec::new(),
        };

        let response =
            PrivateDiscoveryOrchestrator::new().discover_with_adapters(request, &[&adapter], 2_000);

        assert_eq!(adapter.calls.get(), 0);
        assert!(response.visible_fragments.is_empty());
        assert!(response.discovery_frontier.ranked_actions.is_empty());
    }
}
