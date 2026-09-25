use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CsvSourceOperation {
    SnapshotPage,
    DiscoverFields,
    ProbeConnection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvSourceRequest {
    pub object_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub operation: CsvSourceOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<CsvSourceCursor>,
}

impl CsvSourceRequest {
    pub fn snapshot_page(object_ref: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            object_ref: object_ref.into(),
            path: Some(path.into()),
            operation: CsvSourceOperation::SnapshotPage,
            limit: None,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum CsvSourceCursor {
    Snapshot(CsvSnapshotCursor),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvSnapshotCursor {
    pub row_offset: u64,
    #[serde(default)]
    pub source_revision: String,
}
