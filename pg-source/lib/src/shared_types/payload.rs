use serde::{Deserialize, Serialize};
use serde_json::Value;
use source_kit::{SourceErrorPayload, SourceRuntimeStatus};

use super::PgSourceCursor;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "kebab-case")]
pub enum PgSourcePayload {
    Rows(PgRowBatch),
    Fields(PgFieldCatalog),
    Catalog(PgCatalogMetadata),
    Probe(PgProbeStatus),
    Status(SourceRuntimeStatus),
    Error(SourceErrorPayload),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgRowBatch {
    pub object_ref: String,
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub source_revision: String,
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default)]
    pub rows: Vec<PgRowEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<PgSourceCursor>,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgRowEvent {
    pub values: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgFieldCatalog {
    pub object_ref: String,
    #[serde(default)]
    pub fields: Vec<PgFieldDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgFieldDescriptor {
    pub schema: String,
    pub table: String,
    pub column: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordinal_position: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_type: Option<String>,
}

/// A bounded catalog observation, not a source-payload count or business revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgCatalogMetadata {
    pub object_ref: String,
    pub database: String,
    pub database_oid: u32,
    pub system_identifier: Option<String>,
    pub read_only: bool,
    pub complete: bool,
    pub grant_epoch: Option<u64>,
    pub objects: Vec<PgCatalogObject>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgCatalogObject {
    pub schema: String,
    pub name: String,
    pub oid: u32,
    pub kind: String,
    pub estimated_rows: Option<u64>,
    pub row_count_method: String,
    pub view_definition_md5: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgProbeStatus {
    pub object_ref: String,
    pub endpoint_uri: String,
    pub reachable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}
