mod payload;
mod request;

pub use payload::{
    LocalFsEntry, LocalFsEntryBatch, LocalFsFieldCatalog, LocalFsFieldDescriptor,
    LocalFsProbeStatus, LocalFsSourcePayload,
};
pub use request::{
    LocalFsSnapshotCursor, LocalFsSourceCursor, LocalFsSourceOperation, LocalFsSourceRequest,
};

use source_kit::{
    SourceContinuationState, SourceNativeAnchor, SourcePayloadMetadata, SourcePayloadObservation,
    SourceRequestMetadata,
};

impl SourceRequestMetadata for LocalFsSourceRequest {
    fn source_kind(&self) -> &'static str {
        "local-fs"
    }

    fn authority_object_ref(&self) -> &str {
        &self.object_ref
    }

    fn operation_name(&self) -> &'static str {
        match self.operation {
            LocalFsSourceOperation::SnapshotPage => "snapshot-page",
            LocalFsSourceOperation::DiscoverFields => "discover-fields",
            LocalFsSourceOperation::ProbeConnection => "probe-connection",
            LocalFsSourceOperation::SampleJsonl => "sample-jsonl",
            LocalFsSourceOperation::SampleCsv => "sample-csv",
        }
    }

    fn materializable(&self) -> bool {
        true
    }
}

impl SourcePayloadMetadata for LocalFsSourcePayload {
    fn source_observation(&self) -> Option<SourcePayloadObservation> {
        match self {
            LocalFsSourcePayload::Content(sample) => Some(sample.observation()),
            LocalFsSourcePayload::Entries(batch) => Some(SourcePayloadObservation {
                authority_object_ref: batch.object_ref.clone(),
                native_anchors: vec![anchor(
                    "local-fs-scope",
                    join_path(&batch.root_path, &batch.path),
                )],
                grant_epoch: batch.grant_epoch,
                continuation_state: if batch.has_more {
                    SourceContinuationState::ContinuationAvailable
                } else {
                    SourceContinuationState::Terminal
                },
                continuation: encode_continuation(batch.next_cursor.as_ref()),
                actual_rows: batch.entries.len() as u64,
            }),
            LocalFsSourcePayload::Fields(catalog) => Some(SourcePayloadObservation {
                authority_object_ref: catalog.object_ref.clone(),
                native_anchors: vec![anchor("local-fs-schema", catalog.object_ref.clone())],
                grant_epoch: None,
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: catalog.fields.len() as u64,
            }),
            LocalFsSourcePayload::Probe(probe) => Some(SourcePayloadObservation {
                authority_object_ref: probe.object_ref.clone(),
                native_anchors: vec![anchor("local-fs-root", probe.root_path.clone())],
                grant_epoch: None,
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: 0,
            }),
            LocalFsSourcePayload::Status(_) | LocalFsSourcePayload::Error(_) => None,
        }
    }

    fn source_native_revision(&self) -> Option<&str> {
        match self {
            LocalFsSourcePayload::Content(sample) => Some(&sample.source_revision),
            LocalFsSourcePayload::Entries(batch) => Some(&batch.source_revision),
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
    if path.is_empty() {
        root.into()
    } else {
        format!(
            "{}/{}",
            root.trim_end_matches(['/', '\\']),
            path.trim_start_matches(['/', '\\'])
        )
    }
}

#[cfg(test)]
mod tests {
    use source_kit::{decode_msgpack, encode_msgpack};

    use super::*;

    #[test]
    fn request_roundtrip_preserves_snapshot_cursor() {
        let request = LocalFsSourceRequest {
            object_ref: "local".into(),
            path: Some("datasets".into()),
            operation: LocalFsSourceOperation::SnapshotPage,
            limit: Some(100),
            cursor: Some(LocalFsSourceCursor::Snapshot(LocalFsSnapshotCursor {
                offset: 100,
                source_revision: "fs:revision".into(),
            })),
        };

        let encoded = encode_msgpack(&request).expect("encode");
        let restored: LocalFsSourceRequest = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, request);
    }

    #[test]
    fn payload_roundtrip_preserves_entries() {
        let payload = LocalFsSourcePayload::Entries(LocalFsEntryBatch {
            object_ref: "local".into(),
            root_path: "/data".into(),
            path: "datasets".into(),
            source_revision: "fs:test".into(),
            entries: vec![LocalFsEntry {
                path: "datasets/a.csv".into(),
                file_name: "a.csv".into(),
                is_dir: false,
                size: 12,
                modified_unix_ms: Some(1_800_000_000_000),
            }],
            next_cursor: None,
            has_more: false,
            grant_epoch: Some(5),
            diagnostics: None,
        });

        let encoded = encode_msgpack(&payload).expect("encode");
        let restored: LocalFsSourcePayload = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, payload);
    }
}
