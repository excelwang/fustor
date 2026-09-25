use bytes::Bytes;
use capanix_app_sdk::{CnxError, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;

mod payload;
mod request;

pub use payload::{
    EsDocumentEvent, EsDocumentEventKind, EsFieldCatalog, EsFieldDescriptor, EsProbeStatus,
    EsSourceBatch, EsSourceErrorPayload, EsSourcePayload, EsSourceStatus,
};
pub use request::{
    EsPollCursor, EsSnapshotCursor, EsSourceCursor, EsSourceOperation, EsSourceRequest,
};
use source_kit::{
    SourceContinuationState, SourceNativeAnchor, SourcePayloadMetadata, SourcePayloadObservation,
    SourceRequestMetadata,
};

impl SourceRequestMetadata for EsSourceRequest {
    fn source_kind(&self) -> &'static str {
        "elasticsearch"
    }

    fn authority_object_ref(&self) -> &str {
        &self.object_ref
    }

    fn operation_name(&self) -> &'static str {
        match self.operation {
            EsSourceOperation::SnapshotPage => "snapshot-page",
            EsSourceOperation::SnapshotClose => "snapshot-close",
            EsSourceOperation::PollSince => "poll-since",
            EsSourceOperation::DiscoverFields => "discover-fields",
            EsSourceOperation::ProbeConnection => "probe-connection",
        }
    }

    fn materializable(&self) -> bool {
        self.operation != EsSourceOperation::SnapshotClose
    }
}

impl SourcePayloadMetadata for EsSourcePayload {
    fn source_observation(&self) -> Option<SourcePayloadObservation> {
        match self {
            EsSourcePayload::Batch(batch) => Some(SourcePayloadObservation {
                authority_object_ref: batch.object_ref.clone(),
                native_anchors: vec![anchor(
                    "es-index",
                    format!("es://{}/{}", batch.object_ref, batch.index),
                )],
                grant_epoch: batch.grant_epoch,
                continuation_state: if batch.next_cursor.is_some() {
                    SourceContinuationState::ContinuationAvailable
                } else {
                    SourceContinuationState::Terminal
                },
                continuation: encode_continuation(batch.next_cursor.as_ref()),
                actual_rows: batch.documents.len() as u64,
            }),
            EsSourcePayload::Fields(catalog) => Some(SourcePayloadObservation {
                authority_object_ref: catalog.object_ref.clone(),
                native_anchors: vec![anchor(
                    "es-index",
                    format!("es://{}/{}", catalog.object_ref, catalog.index),
                )],
                grant_epoch: None,
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: catalog.fields.len() as u64,
            }),
            EsSourcePayload::Probe(probe) => Some(SourcePayloadObservation {
                authority_object_ref: probe.object_ref.clone(),
                native_anchors: vec![anchor("es-source", probe.object_ref.clone())],
                grant_epoch: None,
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: 0,
            }),
            EsSourcePayload::Status(_) | EsSourcePayload::Error(_) => None,
        }
    }

    fn source_native_revision(&self) -> Option<&str> {
        match self {
            EsSourcePayload::Batch(batch) => Some(batch.source_revision.as_str()),
            EsSourcePayload::Fields(_)
            | EsSourcePayload::Probe(_)
            | EsSourcePayload::Status(_)
            | EsSourcePayload::Error(_) => None,
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
            source_revision: "es-poll-observation:test".into(),
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

    #[test]
    fn status_payload_roundtrip_preserves_routes_and_diagnostics() {
        let payload = EsSourcePayload::Status(EsSourceStatus {
            node_id: "es-source-node-a".into(),
            fetch_route: "es-source.fetch.es_source_node_a:v1.req".into(),
            status_route: "es-source.status.es_source_node_a:v1.req".into(),
            configured_endpoints: 1,
            active_grants: 1,
            ready: true,
            capabilities: source_kit::SourceCapabilityReport::default(),
            diagnostics: vec!["fetch route is request/reply only".into()],
        });

        let encoded = encode_msgpack(&payload).expect("encode status payload");
        let restored: EsSourcePayload = decode_msgpack(&encoded).expect("decode status payload");

        assert_eq!(restored, payload);
    }
}
