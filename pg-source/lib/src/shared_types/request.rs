use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PgSourceOperation {
    SnapshotOpen,
    SnapshotPage,
    SnapshotClose,
    DiscoverFields,
    CatalogMetadata,
    ProbeConnection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgSourceRequest {
    pub object_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    pub operation: PgSourceOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<PgSourceCursor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub field_filter: Vec<String>,
}

impl PgSourceRequest {
    pub fn snapshot_open(
        object_ref: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            object_ref: object_ref.into(),
            schema: Some(schema.into()),
            table: Some(table.into()),
            operation: PgSourceOperation::SnapshotOpen,
            limit: None,
            cursor: None,
            field_filter: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum PgSourceCursor {
    Snapshot(PgSnapshotCursor),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgSnapshotCursor {
    pub snapshot_session_id: String,
    pub schema: String,
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_key: Option<Value>,
    pub offset: u64,
    pub expires_at_unix_ms: u64,
}
