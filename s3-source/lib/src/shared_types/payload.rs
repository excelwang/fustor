use serde::{Deserialize, Serialize};
use serde_json::Value;
use source_kit::SourceErrorPayload;

use super::S3SourceCursor;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "kebab-case")]
pub enum S3SourcePayload {
    Objects(S3ObjectBatch),
    Fields(S3FieldCatalog),
    Probe(S3ProbeStatus),
    Error(SourceErrorPayload),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct S3ObjectBatch {
    pub object_ref: String,
    pub bucket: String,
    pub prefix: String,
    #[serde(default)]
    pub objects: Vec<S3ObjectEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<S3SourceCursor>,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct S3ObjectEvent {
    pub key: String,
    pub size: u64,
    pub last_modified_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_display_name: Option<String>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3FieldCatalog {
    pub object_ref: String,
    #[serde(default)]
    pub fields: Vec<S3FieldDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3FieldDescriptor {
    pub name: String,
    pub field_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3ProbeStatus {
    pub object_ref: String,
    pub endpoint_uri: String,
    pub bucket: String,
    pub reachable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}
