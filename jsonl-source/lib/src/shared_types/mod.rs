mod payload;
mod request;

pub use payload::{
    JsonlFieldCatalog, JsonlFieldDescriptor, JsonlProbeStatus, JsonlRecord, JsonlRecordBatch,
    JsonlSourcePayload,
};
pub use request::{
    JsonlSnapshotCursor, JsonlSourceCursor, JsonlSourceOperation, JsonlSourceRequest,
};

use source_kit::{
    SourceContinuationState, SourceNativeAnchor, SourcePayloadMetadata, SourcePayloadObservation,
    SourceRequestMetadata,
};

impl SourceRequestMetadata for JsonlSourceRequest {
    fn source_kind(&self) -> &'static str {
        "jsonl"
    }

    fn authority_object_ref(&self) -> &str {
        &self.object_ref
    }

    fn operation_name(&self) -> &'static str {
        match self.operation {
            JsonlSourceOperation::SnapshotPage => "snapshot-page",
            JsonlSourceOperation::DiscoverFields => "discover-fields",
            JsonlSourceOperation::ProbeConnection => "probe-connection",
        }
    }

    fn materializable(&self) -> bool {
        true
    }
}

impl SourcePayloadMetadata for JsonlSourcePayload {
    fn source_observation(&self) -> Option<SourcePayloadObservation> {
        match self {
            JsonlSourcePayload::Records(batch) => Some(SourcePayloadObservation {
                authority_object_ref: batch.object_ref.clone(),
                native_anchors: vec![anchor(
                    "jsonl-file",
                    join_path(&batch.root_path, &batch.path),
                )],
                grant_epoch: batch.grant_epoch,
                continuation_state: if batch.has_more {
                    SourceContinuationState::ContinuationAvailable
                } else {
                    SourceContinuationState::Terminal
                },
                continuation: encode_continuation(batch.next_cursor.as_ref()),
                actual_rows: batch.records.len() as u64,
            }),
            JsonlSourcePayload::Fields(catalog) => Some(SourcePayloadObservation {
                authority_object_ref: catalog.object_ref.clone(),
                native_anchors: vec![anchor("jsonl-file", catalog.path.clone())],
                grant_epoch: None,
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: catalog.fields.len() as u64,
            }),
            JsonlSourcePayload::Probe(probe) => Some(SourcePayloadObservation {
                authority_object_ref: probe.object_ref.clone(),
                native_anchors: vec![anchor("jsonl-root", probe.root_path.clone())],
                grant_epoch: None,
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: 0,
            }),
            JsonlSourcePayload::Status(_) | JsonlSourcePayload::Error(_) => None,
        }
    }

    fn source_native_revision(&self) -> Option<&str> {
        match self {
            JsonlSourcePayload::Records(batch) => Some(&batch.source_revision),
            _ => None,
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

fn join_path(root: &str, path: &str) -> String {
    format!(
        "{}/{}",
        root.trim_end_matches(['/', '\\']),
        path.trim_start_matches(['/', '\\'])
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use source_kit::{decode_msgpack, encode_msgpack};

    use super::*;

    #[test]
    fn request_roundtrip_preserves_snapshot_cursor() {
        let request = JsonlSourceRequest {
            object_ref: "jsonl".into(),
            path: Some("datasets/a.jsonl".into()),
            operation: JsonlSourceOperation::SnapshotPage,
            limit: Some(100),
            cursor: Some(JsonlSourceCursor::Snapshot(JsonlSnapshotCursor {
                byte_offset: 256,
                line_number: 10,
                source_revision: "fs:revision".into(),
            })),
        };

        let encoded = encode_msgpack(&request).expect("encode");
        let restored: JsonlSourceRequest = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, request);
    }

    #[test]
    fn payload_roundtrip_preserves_records() {
        let payload = JsonlSourcePayload::Records(JsonlRecordBatch {
            object_ref: "jsonl".into(),
            root_path: "/data".into(),
            path: "datasets/a.jsonl".into(),
            source_revision: "fs:test".into(),
            records: vec![JsonlRecord {
                line_number: 1,
                values: json!({"id": 1, "name": "alice"}),
            }],
            next_cursor: None,
            has_more: false,
            grant_epoch: Some(5),
            diagnostics: None,
        });

        let encoded = encode_msgpack(&payload).expect("encode");
        let restored: JsonlSourcePayload = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, payload);
    }
}
