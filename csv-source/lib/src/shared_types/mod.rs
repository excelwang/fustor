mod payload;
mod request;

pub use payload::{
    CsvFieldCatalog, CsvFieldDescriptor, CsvProbeStatus, CsvRecord, CsvRecordBatch,
    CsvSourcePayload,
};
pub use request::{CsvSnapshotCursor, CsvSourceCursor, CsvSourceOperation, CsvSourceRequest};

use source_kit::{
    SourceContinuationState, SourceNativeAnchor, SourcePayloadMetadata, SourcePayloadObservation,
    SourceRequestMetadata,
};

impl SourceRequestMetadata for CsvSourceRequest {
    fn source_kind(&self) -> &'static str {
        "csv"
    }

    fn authority_object_ref(&self) -> &str {
        &self.object_ref
    }

    fn operation_name(&self) -> &'static str {
        match self.operation {
            CsvSourceOperation::SnapshotPage => "snapshot-page",
            CsvSourceOperation::DiscoverFields => "discover-fields",
            CsvSourceOperation::ProbeConnection => "probe-connection",
        }
    }

    fn materializable(&self) -> bool {
        true
    }
}

impl SourcePayloadMetadata for CsvSourcePayload {
    fn source_observation(&self) -> Option<SourcePayloadObservation> {
        match self {
            CsvSourcePayload::Records(batch) => Some(SourcePayloadObservation {
                authority_object_ref: batch.object_ref.clone(),
                native_anchors: vec![anchor("csv-file", join_path(&batch.root_path, &batch.path))],
                grant_epoch: batch.grant_epoch,
                continuation_state: if batch.has_more {
                    SourceContinuationState::ContinuationAvailable
                } else {
                    SourceContinuationState::Terminal
                },
                continuation: encode_continuation(batch.next_cursor.as_ref()),
                actual_rows: batch.records.len() as u64,
            }),
            CsvSourcePayload::Fields(catalog) => Some(SourcePayloadObservation {
                authority_object_ref: catalog.object_ref.clone(),
                native_anchors: vec![anchor("csv-file", catalog.path.clone())],
                grant_epoch: None,
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: catalog.fields.len() as u64,
            }),
            CsvSourcePayload::Probe(probe) => Some(SourcePayloadObservation {
                authority_object_ref: probe.object_ref.clone(),
                native_anchors: vec![anchor("csv-root", probe.root_path.clone())],
                grant_epoch: None,
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: 0,
            }),
            CsvSourcePayload::Status(_) | CsvSourcePayload::Error(_) => None,
        }
    }

    fn source_native_revision(&self) -> Option<&str> {
        match self {
            CsvSourcePayload::Records(batch) => Some(&batch.source_revision),
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
        let request = CsvSourceRequest {
            object_ref: "csv".into(),
            path: Some("datasets/a.csv".into()),
            operation: CsvSourceOperation::SnapshotPage,
            limit: Some(100),
            cursor: Some(CsvSourceCursor::Snapshot(CsvSnapshotCursor {
                row_offset: 100,
                source_revision: "fs:revision".into(),
            })),
        };

        let encoded = encode_msgpack(&request).expect("encode");
        let restored: CsvSourceRequest = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, request);
    }

    #[test]
    fn payload_roundtrip_preserves_records() {
        let payload = CsvSourcePayload::Records(CsvRecordBatch {
            object_ref: "csv".into(),
            root_path: "/data".into(),
            path: "datasets/a.csv".into(),
            source_revision: "fs:test".into(),
            fields: vec!["id".into(), "name".into()],
            records: vec![CsvRecord {
                row_number: 2,
                values: json!({"id": "1", "name": "alice"}),
            }],
            next_cursor: None,
            has_more: false,
            grant_epoch: Some(5),
            diagnostics: None,
        });

        let encoded = encode_msgpack(&payload).expect("encode");
        let restored: CsvSourcePayload = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, payload);
    }
}
