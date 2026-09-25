use serde::{Deserialize, Serialize};
use serde_json::Value;
use source_kit::{SourceErrorPayload, SourceRuntimeStatus};

use super::CsvSourceCursor;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "kebab-case")]
pub enum CsvSourcePayload {
    Records(CsvRecordBatch),
    Fields(CsvFieldCatalog),
    Probe(CsvProbeStatus),
    Status(SourceRuntimeStatus),
    Error(SourceErrorPayload),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CsvRecordBatch {
    pub object_ref: String,
    pub root_path: String,
    pub path: String,
    #[serde(default)]
    pub source_revision: String,
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default)]
    pub records: Vec<CsvRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<CsvSourceCursor>,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CsvRecord {
    pub row_number: u64,
    pub values: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvFieldCatalog {
    pub object_ref: String,
    pub path: String,
    #[serde(default)]
    pub fields: Vec<CsvFieldDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvFieldDescriptor {
    pub name: String,
    pub field_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvProbeStatus {
    pub object_ref: String,
    pub root_path: String,
    pub reachable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}
