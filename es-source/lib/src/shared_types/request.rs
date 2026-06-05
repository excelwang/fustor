use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EsSourceOperation {
    SnapshotPage,
    PollSince,
    DiscoverFields,
    ProbeConnection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EsSourceRequest {
    pub object_ref: String,
    pub index: String,
    pub operation: EsSourceOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<EsSourceCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_after: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub field_filter: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_filter: Option<Value>,
}

impl EsSourceRequest {
    pub fn snapshot_page(object_ref: impl Into<String>, index: impl Into<String>) -> Self {
        Self {
            object_ref: object_ref.into(),
            index: index.into(),
            operation: EsSourceOperation::SnapshotPage,
            limit: None,
            cursor: None,
            timestamp_after: None,
            field_filter: Vec::new(),
            query_filter: None,
        }
    }

    pub fn poll_since(object_ref: impl Into<String>, index: impl Into<String>) -> Self {
        Self {
            operation: EsSourceOperation::PollSince,
            ..Self::snapshot_page(object_ref, index)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum EsSourceCursor {
    Snapshot(EsSnapshotCursor),
    Poll(EsPollCursor),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EsSnapshotCursor {
    pub pit_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub search_after: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EsPollCursor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_after: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub search_after: Vec<Value>,
}
