mod payload;
mod request;

pub use payload::{
    MysqlFieldCatalog, MysqlFieldDescriptor, MysqlProbeStatus, MysqlRowBatch, MysqlRowEvent,
    MysqlSourcePayload,
};
pub use request::{
    MysqlSnapshotCursor, MysqlSourceCursor, MysqlSourceOperation, MysqlSourceRequest,
};

use source_kit::{
    SourceContinuationState, SourceNativeAnchor, SourcePayloadMetadata, SourcePayloadObservation,
    SourceRequestMetadata,
};

impl SourceRequestMetadata for MysqlSourceRequest {
    fn source_kind(&self) -> &'static str {
        "mysql"
    }

    fn authority_object_ref(&self) -> &str {
        &self.object_ref
    }

    fn operation_name(&self) -> &'static str {
        match self.operation {
            MysqlSourceOperation::SnapshotOpen => "snapshot-open",
            MysqlSourceOperation::SnapshotPage => "snapshot-page",
            MysqlSourceOperation::SnapshotClose => "snapshot-close",
            MysqlSourceOperation::DiscoverFields => "discover-fields",
            MysqlSourceOperation::ProbeConnection => "probe-connection",
        }
    }

    fn materializable(&self) -> bool {
        !matches!(self.operation, MysqlSourceOperation::SnapshotClose)
    }
}

impl SourcePayloadMetadata for MysqlSourcePayload {
    fn source_observation(&self) -> Option<SourcePayloadObservation> {
        match self {
            MysqlSourcePayload::Rows(batch) => Some(SourcePayloadObservation {
                authority_object_ref: batch.object_ref.clone(),
                native_anchors: vec![anchor(
                    "mysql-table",
                    format!(
                        "mysql://{}/{}/{}",
                        batch.object_ref, batch.schema, batch.table
                    ),
                )],
                grant_epoch: batch.grant_epoch,
                continuation_state: if batch.has_more {
                    SourceContinuationState::ContinuationAvailable
                } else if batch.diagnostics.as_deref()
                    == Some("transaction snapshot session closed")
                {
                    SourceContinuationState::SessionClosed
                } else {
                    SourceContinuationState::Terminal
                },
                continuation: encode_continuation(batch.next_cursor.as_ref()),
                actual_rows: batch.rows.len() as u64,
            }),
            MysqlSourcePayload::Fields(catalog) => Some(SourcePayloadObservation {
                authority_object_ref: catalog.object_ref.clone(),
                native_anchors: vec![anchor("mysql-catalog", catalog.object_ref.clone())],
                grant_epoch: None,
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: catalog.fields.len() as u64,
            }),
            MysqlSourcePayload::Probe(probe) => Some(SourcePayloadObservation {
                authority_object_ref: probe.object_ref.clone(),
                native_anchors: vec![anchor("mysql-source", probe.object_ref.clone())],
                grant_epoch: None,
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: 0,
            }),
            MysqlSourcePayload::Status(_) | MysqlSourcePayload::Error(_) => None,
        }
    }

    fn source_native_revision(&self) -> Option<&str> {
        match self {
            MysqlSourcePayload::Rows(batch) => Some(batch.source_revision.as_str()),
            MysqlSourcePayload::Fields(_)
            | MysqlSourcePayload::Probe(_)
            | MysqlSourcePayload::Status(_)
            | MysqlSourcePayload::Error(_) => None,
        }
    }
}

fn encode_continuation<T: serde::Serialize>(cursor: Option<&T>) -> Option<Vec<u8>> {
    cursor.and_then(|cursor| {
        source_kit::encode_msgpack(cursor)
            .ok()
            .map(|bytes| bytes.to_vec())
    })
}

fn anchor(kind: &str, native_ref: String) -> SourceNativeAnchor {
    SourceNativeAnchor {
        anchor_kind: kind.into(),
        native_ref,
        native_pointer: None,
        native_version: None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use source_kit::{decode_msgpack, encode_msgpack};

    use super::*;

    #[test]
    fn request_roundtrip_preserves_snapshot_cursor() {
        let request = MysqlSourceRequest {
            object_ref: "mysql-prod".into(),
            schema: Some("app".into()),
            table: Some("users".into()),
            operation: MysqlSourceOperation::SnapshotPage,
            limit: Some(100),
            cursor: Some(MysqlSourceCursor::Snapshot(MysqlSnapshotCursor {
                snapshot_session_id: "snap-1".into(),
                schema: "app".into(),
                table: "users".into(),
                last_key: Some(json!(42)),
                offset: 100,
                expires_at_unix_ms: 1_800_000_000_000,
            })),
            field_filter: vec!["id".into(), "name".into()],
        };

        let encoded = encode_msgpack(&request).expect("encode");
        let restored: MysqlSourceRequest = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, request);
    }

    #[test]
    fn payload_roundtrip_preserves_rows() {
        let payload = MysqlSourcePayload::Rows(MysqlRowBatch {
            object_ref: "mysql-prod".into(),
            schema: "app".into(),
            table: "users".into(),
            source_revision: "mysql-consistent-snapshot:test".into(),
            fields: vec!["id".into(), "name".into()],
            rows: vec![MysqlRowEvent {
                values: json!({"id": 1, "name": "alice"}),
            }],
            next_cursor: None,
            has_more: false,
            grant_epoch: Some(2),
            diagnostics: None,
        });

        let encoded = encode_msgpack(&payload).expect("encode");
        let restored: MysqlSourcePayload = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, payload);
    }
}
