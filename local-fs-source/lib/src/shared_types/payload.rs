use serde::{Deserialize, Serialize};
use source_kit::{SourceErrorPayload, SourceRuntimeStatus};

use super::LocalFsSourceCursor;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "kebab-case")]
pub enum LocalFsSourcePayload {
    Entries(LocalFsEntryBatch),
    Content(source_kit::SourceContentSample),
    Fields(LocalFsFieldCatalog),
    Probe(LocalFsProbeStatus),
    Status(SourceRuntimeStatus),
    Error(SourceErrorPayload),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalFsEntryBatch {
    pub object_ref: String,
    pub root_path: String,
    pub path: String,
    #[serde(default)]
    pub source_revision: String,
    #[serde(default)]
    pub entries: Vec<LocalFsEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<LocalFsSourceCursor>,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFsEntry {
    pub path: String,
    pub file_name: String,
    pub is_dir: bool,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFsFieldCatalog {
    pub object_ref: String,
    #[serde(default)]
    pub fields: Vec<LocalFsFieldDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFsFieldDescriptor {
    pub name: String,
    pub field_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFsProbeStatus {
    pub object_ref: String,
    pub root_path: String,
    pub reachable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<String>,
}
