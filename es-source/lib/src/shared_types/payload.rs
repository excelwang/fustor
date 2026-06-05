use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::EsSourceCursor;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "kebab-case")]
pub enum EsSourcePayload {
    Batch(EsSourceBatch),
    Fields(EsFieldCatalog),
    Probe(EsProbeStatus),
    Error(EsSourceErrorPayload),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EsSourceBatch {
    pub object_ref: String,
    pub index: String,
    #[serde(default)]
    pub documents: Vec<EsDocumentEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<EsSourceCursor>,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EsDocumentEventKind {
    SnapshotInsert,
    PollUpdate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EsDocumentEvent {
    pub event_kind: EsDocumentEventKind,
    pub index: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_value: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq_no: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_term: Option<i64>,
    #[serde(default)]
    pub source: Value,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsFieldCatalog {
    pub object_ref: String,
    pub index: String,
    #[serde(default)]
    pub fields: Vec<EsFieldDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsFieldDescriptor {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsProbeStatus {
    pub object_ref: String,
    pub endpoint_uri: String,
    pub reachable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsSourceErrorPayload {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl EsSourceErrorPayload {
    pub fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }
}
