use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum S3SourceOperation {
    SnapshotPage,
    PollSince,
    DiscoverFields,
    ProbeConnection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3SourceRequest {
    pub object_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    pub operation: S3SourceOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<S3SourceCursor>,
}

impl S3SourceRequest {
    pub fn snapshot_page(object_ref: impl Into<String>) -> Self {
        Self {
            object_ref: object_ref.into(),
            bucket: None,
            prefix: None,
            operation: S3SourceOperation::SnapshotPage,
            limit: None,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum S3SourceCursor {
    Snapshot(S3SnapshotCursor),
    Poll(S3PollCursor),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3SnapshotCursor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3PollCursor {
    pub last_modified_unix_ms: u64,
    pub last_key: String,
}
