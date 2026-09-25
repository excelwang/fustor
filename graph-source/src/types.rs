use serde::{Deserialize, Serialize};
use serde_json::Value;
use source_kit::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GraphOperation {
    ProbeConnection,
    ConfiguredQuery,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphSourceRequest {
    pub object_ref: String,
    pub operation: GraphOperation,
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub configured_query_id: Option<String>,
    #[serde(default)]
    pub expected_result_schema_ref: Option<String>,
    #[serde(default)]
    pub parameters: BTreeMap<String, Value>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphObservation {
    pub object_ref: String,
    pub source_revision: String,
    pub provider_revision: Option<String>,
    pub provider_revision_evidence_digest: Option<String>,
    pub configured_query_id: Option<String>,
    pub result_schema_ref: Option<String>,
    pub rows: Vec<Value>,
    pub reachable: bool,
    pub grant_epoch: u64,
    pub complete: bool,
    pub generation_alignment: String,
    pub fustor_read_only_enforcement: String,
    pub backend_account_permissions: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "kebab-case")]
pub enum GraphSourcePayload {
    Rows(GraphObservation),
    Probe(GraphObservation),
    Status(SourceRuntimeStatus),
    Error(SourceErrorPayload),
}
impl SourceRequestMetadata for GraphSourceRequest {
    fn source_kind(&self) -> &'static str {
        "graph"
    }
    fn authority_object_ref(&self) -> &str {
        &self.object_ref
    }
    fn operation_name(&self) -> &'static str {
        match self.operation {
            GraphOperation::ProbeConnection => "probe-connection",
            GraphOperation::ConfiguredQuery => "configured-query",
        }
    }
    fn materializable(&self) -> bool {
        true
    }
}
impl SourcePayloadMetadata for GraphSourcePayload {
    fn source_native_revision(&self) -> Option<&str> {
        match self {
            Self::Rows(value) | Self::Probe(value) => Some(&value.source_revision),
            _ => None,
        }
    }
    fn source_observation(&self) -> Option<SourcePayloadObservation> {
        let value = match self {
            Self::Rows(value) | Self::Probe(value) => value,
            _ => return None,
        };
        Some(SourcePayloadObservation {
            authority_object_ref: value.object_ref.clone(),
            grant_epoch: Some(value.grant_epoch),
            native_anchors: vec![SourceNativeAnchor {
                anchor_kind: "graph-query-observation".into(),
                native_ref: value.object_ref.clone(),
                native_pointer: value.configured_query_id.clone(),
                native_version: value.provider_revision.clone(),
            }],
            continuation_state: SourceContinuationState::Terminal,
            continuation: None,
            actual_rows: value.rows.len() as u64,
        })
    }
}
