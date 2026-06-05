mod payload;
mod request;

pub use payload::{
    S3FieldCatalog, S3FieldDescriptor, S3ObjectBatch, S3ObjectEvent, S3ProbeStatus, S3SourcePayload,
};
pub use request::{
    S3PollCursor, S3SnapshotCursor, S3SourceCursor, S3SourceOperation, S3SourceRequest,
};

#[cfg(test)]
mod tests {
    use serde_json::json;
    use source_kit::{decode_msgpack, encode_msgpack};

    use super::*;

    #[test]
    fn request_roundtrip_preserves_poll_cursor() {
        let request = S3SourceRequest {
            object_ref: "s3-prod".into(),
            bucket: Some("logs".into()),
            prefix: Some("2026/".into()),
            operation: S3SourceOperation::PollSince,
            limit: Some(100),
            cursor: Some(S3SourceCursor::Poll(S3PollCursor {
                last_modified_unix_ms: 1_800_000_000_000,
                last_key: "2026/a.json".into(),
            })),
        };

        let encoded = encode_msgpack(&request).expect("encode");
        let restored: S3SourceRequest = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, request);
    }

    #[test]
    fn payload_roundtrip_preserves_object_metadata() {
        let payload = S3SourcePayload::Objects(S3ObjectBatch {
            object_ref: "s3-prod".into(),
            bucket: "logs".into(),
            prefix: "2026/".into(),
            objects: vec![S3ObjectEvent {
                key: "2026/a.json".into(),
                size: 12,
                last_modified_unix_ms: 1_800_000_000_000,
                etag: Some("\"abc\"".into()),
                storage_class: Some("STANDARD".into()),
                owner_id: None,
                owner_display_name: None,
                metadata: json!({"source": "test"}),
            }],
            next_cursor: None,
            has_more: false,
            grant_epoch: Some(5),
            diagnostics: None,
        });

        let encoded = encode_msgpack(&payload).expect("encode");
        let restored: S3SourcePayload = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, payload);
    }
}
