mod payload;
mod request;

pub use payload::{
    S3FieldCatalog, S3FieldDescriptor, S3ObjectBatch, S3ObjectEvent, S3ProbeStatus, S3SourcePayload,
};
pub use request::{
    S3PollCursor, S3SnapshotCursor, S3SourceCursor, S3SourceOperation, S3SourceRequest,
};

use source_kit::{
    SourceContinuationState, SourceNativeAnchor, SourcePayloadMetadata, SourcePayloadObservation,
    SourceRequestMetadata,
};

impl SourceRequestMetadata for S3SourceRequest {
    fn source_kind(&self) -> &'static str {
        "s3"
    }

    fn authority_object_ref(&self) -> &str {
        &self.object_ref
    }

    fn operation_name(&self) -> &'static str {
        match self.operation {
            S3SourceOperation::SnapshotPage => "snapshot-page",
            S3SourceOperation::PollSince => "poll-since",
            S3SourceOperation::DiscoverFields => "discover-fields",
            S3SourceOperation::ProbeConnection => "probe-connection",
            S3SourceOperation::SampleJsonl => "sample-jsonl",
            S3SourceOperation::SampleCsv => "sample-csv",
        }
    }

    fn materializable(&self) -> bool {
        true
    }
}

impl SourcePayloadMetadata for S3SourcePayload {
    fn source_observation(&self) -> Option<SourcePayloadObservation> {
        match self {
            S3SourcePayload::Content(sample) => Some(sample.observation()),
            S3SourcePayload::Objects(batch) => Some(SourcePayloadObservation {
                authority_object_ref: batch.object_ref.clone(),
                native_anchors: vec![anchor(
                    "s3-prefix",
                    format!("s3://{}/{}", batch.bucket, batch.prefix),
                )],
                grant_epoch: batch.grant_epoch,
                continuation_state: if batch.next_cursor.is_some() {
                    SourceContinuationState::ContinuationAvailable
                } else {
                    SourceContinuationState::Terminal
                },
                continuation: encode_continuation(batch.next_cursor.as_ref()),
                actual_rows: batch.objects.len() as u64,
            }),
            S3SourcePayload::Fields(catalog) => Some(SourcePayloadObservation {
                authority_object_ref: catalog.object_ref.clone(),
                native_anchors: vec![anchor("s3-schema", catalog.object_ref.clone())],
                grant_epoch: None,
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: catalog.fields.len() as u64,
            }),
            S3SourcePayload::Probe(probe) => Some(SourcePayloadObservation {
                authority_object_ref: probe.object_ref.clone(),
                native_anchors: vec![anchor("s3-bucket", format!("s3://{}", probe.bucket))],
                grant_epoch: None,
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: 0,
            }),
            S3SourcePayload::Status(_) | S3SourcePayload::Error(_) => None,
        }
    }

    fn source_native_revision(&self) -> Option<&str> {
        match self {
            S3SourcePayload::Content(sample) => Some(&sample.source_revision),
            S3SourcePayload::Objects(batch) => Some(batch.source_revision.as_str()),
            S3SourcePayload::Fields(_)
            | S3SourcePayload::Probe(_)
            | S3SourcePayload::Status(_)
            | S3SourcePayload::Error(_) => None,
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
    use serde::Serialize;
    use serde_json::json;
    use source_kit::{decode_msgpack, encode_msgpack};

    use super::*;

    #[derive(Serialize)]
    struct LegacyPollCursor {
        last_modified_unix_ms: u64,
        last_key: String,
    }

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
                continuation_token: None,
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
            source_revision: "s3-list-observation:test".into(),
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

    #[test]
    fn legacy_poll_cursor_without_continuation_token_decodes() {
        let encoded = encode_msgpack(&LegacyPollCursor {
            last_modified_unix_ms: 1_800_000_000_000,
            last_key: "2026/a.json".into(),
        })
        .expect("encode legacy cursor");
        let restored: S3PollCursor = decode_msgpack(&encoded).expect("decode legacy cursor");

        assert_eq!(restored.last_modified_unix_ms, 1_800_000_000_000);
        assert_eq!(restored.last_key, "2026/a.json");
        assert_eq!(restored.continuation_token, None);
    }
}
