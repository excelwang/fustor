use bytes::Bytes;
use capanix_app_sdk::{CnxError, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;

mod payload;
mod request;

pub use payload::{
    EsDocumentEvent, EsDocumentEventKind, EsFieldCatalog, EsFieldDescriptor, EsProbeStatus,
    EsSourceBatch, EsSourceErrorPayload, EsSourcePayload,
};
pub use request::{
    EsPollCursor, EsSnapshotCursor, EsSourceCursor, EsSourceOperation, EsSourceRequest,
};

pub fn encode_msgpack<T: Serialize>(value: &T) -> Result<Bytes> {
    rmp_serde::to_vec_named(value)
        .map(Bytes::from)
        .map_err(|err| CnxError::Internal(format!("encode es-source msgpack payload: {err}")))
}

pub fn decode_msgpack<T: DeserializeOwned>(payload: &[u8]) -> Result<T> {
    rmp_serde::from_slice(payload)
        .map_err(|err| CnxError::InvalidInput(format!("decode es-source msgpack payload: {err}")))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn request_roundtrip_preserves_cursor_and_unicode() {
        let request = EsSourceRequest {
            object_ref: "es-prod".into(),
            index: "日志-2026".into(),
            operation: EsSourceOperation::SnapshotPage,
            limit: Some(128),
            cursor: Some(EsSourceCursor::Snapshot(EsSnapshotCursor {
                pit_id: "pit-1".into(),
                search_after: vec![json!("2026-05-08T00:00:00Z"), json!(42)],
                expires_at_unix_ms: Some(1_777_777_777_000),
            })),
            timestamp_after: None,
            field_filter: vec!["message".into(), "用户".into()],
            query_filter: Some(json!({"term": {"level": "error"}})),
        };

        let encoded = encode_msgpack(&request).expect("encode request");
        let restored: EsSourceRequest = decode_msgpack(&encoded).expect("decode request");

        assert_eq!(restored, request);
    }

    #[test]
    fn payload_roundtrip_preserves_raw_source() {
        let payload = EsSourcePayload::Batch(EsSourceBatch {
            object_ref: "es-prod".into(),
            index: "logs".into(),
            documents: vec![EsDocumentEvent {
                event_kind: EsDocumentEventKind::SnapshotInsert,
                index: "logs".into(),
                id: "doc-1".into(),
                timestamp_value: Some(json!("2026-05-08T00:00:00Z")),
                sort: vec![json!("2026-05-08T00:00:00Z"), json!("doc-1")],
                seq_no: Some(7),
                primary_term: Some(1),
                source: json!({"message": "hello", "nested": {"v": 1}}),
                metadata: Default::default(),
            }],
            next_cursor: Some(EsSourceCursor::Poll(EsPollCursor {
                timestamp_after: Some(json!("2026-05-08T00:00:00Z")),
                search_after: vec![json!("2026-05-08T00:00:00Z"), json!("doc-1")],
            })),
            has_more: true,
            grant_epoch: Some(3),
            diagnostics: None,
        });

        let encoded = encode_msgpack(&payload).expect("encode payload");
        let restored: EsSourcePayload = decode_msgpack(&encoded).expect("decode payload");

        assert_eq!(restored, payload);
    }
}
