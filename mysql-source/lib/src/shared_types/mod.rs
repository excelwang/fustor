mod payload;
mod request;

pub use payload::{
    MysqlFieldCatalog, MysqlFieldDescriptor, MysqlProbeStatus, MysqlRowBatch, MysqlRowEvent,
    MysqlSourcePayload,
};
pub use request::{
    MysqlSnapshotCursor, MysqlSourceCursor, MysqlSourceOperation, MysqlSourceRequest,
};

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
