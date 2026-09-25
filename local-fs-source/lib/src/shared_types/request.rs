use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LocalFsSourceOperation {
    SnapshotPage,
    DiscoverFields,
    ProbeConnection,
    SampleJsonl,
    SampleCsv,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFsSourceRequest {
    pub object_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub operation: LocalFsSourceOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<LocalFsSourceCursor>,
}

impl LocalFsSourceRequest {
    pub fn snapshot_page(object_ref: impl Into<String>) -> Self {
        Self {
            object_ref: object_ref.into(),
            path: None,
            operation: LocalFsSourceOperation::SnapshotPage,
            limit: None,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum LocalFsSourceCursor {
    Snapshot(LocalFsSnapshotCursor),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFsSnapshotCursor {
    pub offset: usize,
    #[serde(default)]
    pub source_revision: String,
}
