use serde::{Deserialize, Serialize};
use serde_json::Value;
use source_kit::{SourceErrorPayload, SourceRuntimeStatus};

use super::JsonlSourceCursor;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "kebab-case")]
pub enum JsonlSourcePayload {
    Records(JsonlRecordBatch),
    Fields(JsonlFieldCatalog),
    Probe(JsonlProbeStatus),
    Status(SourceRuntimeStatus),
    Error(SourceErrorPayload),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonlRecordBatch {
    pub object_ref: String,
    pub root_path: String,
    pub path: String,
    #[serde(default)]
    pub source_revision: String,
    #[serde(default)]
    pub records: Vec<JsonlRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<JsonlSourceCursor>,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonlRecord {
    pub line_number: u64,
    pub values: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonlFieldCatalog {
    pub object_ref: String,
    pub path: String,
    #[serde(default)]
    pub fields: Vec<JsonlFieldDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonlFieldDescriptor {
    pub name: String,
    pub field_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonlProbeStatus {
    pub object_ref: String,
    pub root_path: String,
    pub reachable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}
