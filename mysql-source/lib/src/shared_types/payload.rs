use serde::{Deserialize, Serialize};
use serde_json::Value;
use source_kit::SourceErrorPayload;

use super::MysqlSourceCursor;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "kebab-case")]
pub enum MysqlSourcePayload {
    Rows(MysqlRowBatch),
    Fields(MysqlFieldCatalog),
    Probe(MysqlProbeStatus),
    Error(SourceErrorPayload),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MysqlRowBatch {
    pub object_ref: String,
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default)]
    pub rows: Vec<MysqlRowEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<MysqlSourceCursor>,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MysqlRowEvent {
    pub values: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MysqlFieldCatalog {
    pub object_ref: String,
    #[serde(default)]
    pub fields: Vec<MysqlFieldDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MysqlFieldDescriptor {
    pub schema: String,
    pub table: String,
    pub column: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordinal_position: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MysqlProbeStatus {
    pub object_ref: String,
    pub endpoint_uri: String,
    pub reachable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}
