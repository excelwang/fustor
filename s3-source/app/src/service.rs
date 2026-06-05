use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event, Result};
use s3_source::shared_types::{
    S3FieldDescriptor, S3ObjectBatch, S3SourceCursor, S3SourceOperation, S3SourcePayload,
    S3SourceRequest,
};
use s3_source::{ResolvedS3Endpoint, S3SourceRuntimeConfig};
use source_kit::{SourceErrorPayload, decode_msgpack, encode_msgpack};

use crate::driver::{S3ObjectPage, S3SourceDriver};

#[derive(Clone)]
pub struct S3SourceService {
    config: S3SourceRuntimeConfig,
    driver: Arc<dyn S3SourceDriver>,
    origin_id: NodeId,
}

impl S3SourceService {
    pub fn new(
        config: S3SourceRuntimeConfig,
        driver: Arc<dyn S3SourceDriver>,
        origin_id: NodeId,
    ) -> Self {
        Self {
            config,
            driver,
            origin_id,
        }
    }

    pub fn handle_events(&self, events: Vec<Event>) -> Vec<Event> {
        events
            .into_iter()
            .map(|event| self.handle_event(event))
            .collect()
    }

    pub fn handle_event(&self, event: Event) -> Event {
        let correlation_id = event.metadata().correlation_id;
        let payload = match self.handle_event_payload(event.payload_bytes()) {
            Ok(payload) => payload,
            Err(err) => {
                S3SourcePayload::Error(source_kit::error_payload_from_cnx(err, "s3-transport"))
            }
        };
        self.reply_event(payload, correlation_id)
            .unwrap_or_else(|err| self.fallback_error_event(err, correlation_id))
    }

    fn handle_event_payload(&self, payload: &[u8]) -> Result<S3SourcePayload> {
        let request: S3SourceRequest = decode_msgpack(payload)?;
        match request.operation {
            S3SourceOperation::ProbeConnection => self.handle_probe(&request),
            S3SourceOperation::DiscoverFields => self.handle_discover_fields(&request),
            S3SourceOperation::SnapshotPage => self.handle_snapshot_page(&request),
            S3SourceOperation::PollSince => self.handle_poll_since(&request),
        }
    }

    fn handle_probe(&self, request: &S3SourceRequest) -> Result<S3SourcePayload> {
        let endpoint = self.config.endpoint_for(
            &request.object_ref,
            request.bucket.as_deref(),
            request.prefix.as_deref(),
        )?;
        let credential = self
            .config
            .resolve_credential(endpoint.credential_ref.as_deref())?;
        Ok(S3SourcePayload::Probe(
            self.driver.probe(&endpoint, &credential)?,
        ))
    }

    fn handle_discover_fields(&self, request: &S3SourceRequest) -> Result<S3SourcePayload> {
        let endpoint = self.config.endpoint_for(
            &request.object_ref,
            request.bucket.as_deref(),
            request.prefix.as_deref(),
        )?;
        let credential = self
            .config
            .resolve_credential(endpoint.credential_ref.as_deref())?;
        let mut catalog = self.driver.discover_fields(&endpoint, &credential)?;
        if catalog.fields.is_empty() {
            catalog.fields = default_s3_fields();
        }
        Ok(S3SourcePayload::Fields(catalog))
    }

    fn handle_snapshot_page(&self, request: &S3SourceRequest) -> Result<S3SourcePayload> {
        let endpoint = self.config.endpoint_for(
            &request.object_ref,
            request.bucket.as_deref(),
            request.prefix.as_deref(),
        )?;
        let credential = self
            .config
            .resolve_credential(endpoint.credential_ref.as_deref())?;
        let cursor = match request.cursor.as_ref() {
            Some(S3SourceCursor::Snapshot(cursor)) => Some(cursor),
            Some(S3SourceCursor::Poll(_)) => {
                return Err(CnxError::InvalidInput(
                    "snapshot page cursor must be a snapshot cursor".into(),
                ));
            }
            None => None,
        };
        let page = self.driver.snapshot_page(
            &endpoint,
            &credential,
            self.config.effective_limit(request.limit),
            cursor,
        )?;
        Ok(S3SourcePayload::Objects(object_batch_from_page(
            &endpoint,
            page,
            S3CursorKind::Snapshot,
        )))
    }

    fn handle_poll_since(&self, request: &S3SourceRequest) -> Result<S3SourcePayload> {
        let endpoint = self.config.endpoint_for(
            &request.object_ref,
            request.bucket.as_deref(),
            request.prefix.as_deref(),
        )?;
        let credential = self
            .config
            .resolve_credential(endpoint.credential_ref.as_deref())?;
        let cursor = match request.cursor.as_ref() {
            Some(S3SourceCursor::Poll(cursor)) => Some(cursor),
            Some(S3SourceCursor::Snapshot(_)) => {
                return Err(CnxError::InvalidInput(
                    "poll cursor must be a poll cursor".into(),
                ));
            }
            None => None,
        };
        let page = self.driver.poll_since(
            &endpoint,
            &credential,
            self.config.effective_limit(request.limit),
            cursor,
        )?;
        Ok(S3SourcePayload::Objects(object_batch_from_page(
            &endpoint,
            page,
            S3CursorKind::Poll,
        )))
    }

    fn reply_event(&self, payload: S3SourcePayload, correlation_id: Option<u64>) -> Result<Event> {
        Ok(Event::new(
            EventMetadata {
                origin_id: self.origin_id.clone(),
                timestamp_us: now_us(),
                logical_ts: None,
                correlation_id,
                ingress_auth: None,
                trace: None,
            },
            encode_msgpack(&payload)?,
        ))
    }

    fn fallback_error_event(&self, err: CnxError, correlation_id: Option<u64>) -> Event {
        let payload = S3SourcePayload::Error(SourceErrorPayload::new(
            "encode-error",
            err.to_string(),
            false,
        ));
        let bytes = rmp_serde::to_vec_named(&payload).unwrap_or_else(|_| b"{}".to_vec());
        Event::new(
            EventMetadata {
                origin_id: self.origin_id.clone(),
                timestamp_us: now_us(),
                logical_ts: None,
                correlation_id,
                ingress_auth: None,
                trace: None,
            },
            bytes.into(),
        )
    }
}

enum S3CursorKind {
    Snapshot,
    Poll,
}

fn object_batch_from_page(
    endpoint: &ResolvedS3Endpoint,
    page: S3ObjectPage,
    cursor_kind: S3CursorKind,
) -> S3ObjectBatch {
    let next_cursor = match cursor_kind {
        S3CursorKind::Snapshot => page.next_snapshot_cursor.map(S3SourceCursor::Snapshot),
        S3CursorKind::Poll => page.next_poll_cursor.map(S3SourceCursor::Poll),
    };
    S3ObjectBatch {
        object_ref: endpoint.object_ref.clone(),
        bucket: endpoint.bucket.clone(),
        prefix: endpoint.prefix.clone(),
        objects: page.objects,
        next_cursor,
        has_more: page.has_more,
        grant_epoch: endpoint.grant_epoch,
        diagnostics: page.diagnostics,
    }
}

fn default_s3_fields() -> Vec<S3FieldDescriptor> {
    [
        ("Key", "string"),
        ("Size", "u64"),
        ("LastModified", "unix-ms"),
        ("ETag", "string"),
        ("StorageClass", "string"),
        ("OwnerId", "string"),
        ("OwnerDisplayName", "string"),
    ]
    .into_iter()
    .map(|(name, field_type)| S3FieldDescriptor {
        name: name.into(),
        field_type: field_type.into(),
    })
    .collect()
}

fn now_us() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_micros() as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use capanix_app_sdk::runtime::ConfigValue;
    use s3_source::shared_types::{
        S3FieldCatalog, S3ObjectEvent, S3PollCursor, S3ProbeStatus, S3SnapshotCursor,
    };
    use s3_source::{
        GrantedS3Endpoint, ResolvedCredential, S3EndpointConfig, S3SourceConfig,
        S3SourceRuntimeInputs,
    };
    use serde_json::json;

    use super::*;

    #[derive(Default)]
    struct FakeDriver {
        calls: Mutex<Vec<FakeCall>>,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct FakeCall {
        operation: &'static str,
        endpoint: ResolvedS3Endpoint,
        credential: ResolvedCredential,
        limit: usize,
        snapshot_cursor: Option<S3SnapshotCursor>,
        poll_cursor: Option<S3PollCursor>,
    }

    impl S3SourceDriver for FakeDriver {
        fn probe(
            &self,
            endpoint: &ResolvedS3Endpoint,
            credential: &ResolvedCredential,
        ) -> Result<S3ProbeStatus> {
            self.calls.lock().unwrap().push(FakeCall {
                operation: "probe",
                endpoint: endpoint.clone(),
                credential: credential.clone(),
                limit: 0,
                snapshot_cursor: None,
                poll_cursor: None,
            });
            Ok(S3ProbeStatus {
                object_ref: endpoint.object_ref.clone(),
                endpoint_uri: endpoint.endpoint_uri.clone(),
                bucket: endpoint.bucket.clone(),
                reachable: true,
                diagnostics: None,
            })
        }

        fn discover_fields(
            &self,
            endpoint: &ResolvedS3Endpoint,
            credential: &ResolvedCredential,
        ) -> Result<S3FieldCatalog> {
            self.calls.lock().unwrap().push(FakeCall {
                operation: "fields",
                endpoint: endpoint.clone(),
                credential: credential.clone(),
                limit: 0,
                snapshot_cursor: None,
                poll_cursor: None,
            });
            Ok(S3FieldCatalog {
                object_ref: endpoint.object_ref.clone(),
                fields: Vec::new(),
            })
        }

        fn snapshot_page(
            &self,
            endpoint: &ResolvedS3Endpoint,
            credential: &ResolvedCredential,
            limit: usize,
            cursor: Option<&S3SnapshotCursor>,
        ) -> Result<S3ObjectPage> {
            self.calls.lock().unwrap().push(FakeCall {
                operation: "snapshot",
                endpoint: endpoint.clone(),
                credential: credential.clone(),
                limit,
                snapshot_cursor: cursor.cloned(),
                poll_cursor: None,
            });
            Ok(S3ObjectPage {
                objects: vec![fake_object("logs/2026/a.json", 1)],
                next_snapshot_cursor: Some(S3SnapshotCursor {
                    continuation_token: Some("token-2".into()),
                }),
                next_poll_cursor: None,
                has_more: true,
                diagnostics: Some("snapshot listed".into()),
            })
        }

        fn poll_since(
            &self,
            endpoint: &ResolvedS3Endpoint,
            credential: &ResolvedCredential,
            limit: usize,
            cursor: Option<&S3PollCursor>,
        ) -> Result<S3ObjectPage> {
            self.calls.lock().unwrap().push(FakeCall {
                operation: "poll",
                endpoint: endpoint.clone(),
                credential: credential.clone(),
                limit,
                snapshot_cursor: None,
                poll_cursor: cursor.cloned(),
            });
            Ok(S3ObjectPage {
                objects: vec![fake_object("logs/2026/b.json", 2)],
                next_snapshot_cursor: None,
                next_poll_cursor: Some(S3PollCursor {
                    last_modified_unix_ms: 2,
                    last_key: "logs/2026/b.json".into(),
                }),
                has_more: false,
                diagnostics: Some("poll listed".into()),
            })
        }
    }

    fn fake_object(key: &str, last_modified_unix_ms: u64) -> S3ObjectEvent {
        S3ObjectEvent {
            key: key.into(),
            size: 12,
            last_modified_unix_ms,
            etag: Some("etag".into()),
            storage_class: Some("STANDARD".into()),
            owner_id: None,
            owner_display_name: None,
            metadata: json!({ "source": "fake" }),
        }
    }

    fn test_config() -> S3SourceRuntimeConfig {
        S3SourceRuntimeConfig {
            product: S3SourceConfig {
                endpoints: vec![S3EndpointConfig {
                    object_ref: "s3-prod".into(),
                    endpoint_uri: "https://s3.internal".into(),
                    bucket: "logs".into(),
                    region: Some("us-east-1".into()),
                    prefix: "logs/".into(),
                    credential_ref: None,
                    bucket_scopes: vec!["logs".into()],
                    prefix_scopes: vec!["logs/2026/*".into()],
                    active: true,
                }],
                default_page_size: 50,
                max_page_size: 100,
                ..S3SourceConfig::default()
            },
            runtime: S3SourceRuntimeInputs {
                endpoint_grants: vec![GrantedS3Endpoint {
                    object_ref: "s3-prod".into(),
                    endpoint_ref: Some("bucket-a".into()),
                    endpoint_uri: None,
                    bucket: None,
                    credential_ref: None,
                    bucket_scopes: Vec::new(),
                    prefix_scopes: vec!["logs/2026/*".into()],
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(5),
                }],
            },
        }
    }

    fn service_with(driver: Arc<FakeDriver>) -> S3SourceService {
        S3SourceService::new(test_config(), driver, NodeId("s3-test".into()))
    }

    fn event_for(request: &S3SourceRequest) -> Event {
        Event::new(
            EventMetadata {
                origin_id: NodeId("caller".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: Some(42),
                ingress_auth: None,
                trace: None,
            },
            encode_msgpack(request).expect("encode request"),
        )
    }

    #[test]
    fn snapshot_page_returns_object_batch_and_next_cursor() {
        let driver = Arc::new(FakeDriver::default());
        let service = service_with(driver.clone());
        let mut request = S3SourceRequest::snapshot_page("s3-prod");
        request.prefix = Some("logs/2026/06/".into());
        request.limit = Some(1_000);

        let reply = service.handle_event(event_for(&request));
        let payload: S3SourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        assert_eq!(reply.metadata().correlation_id, Some(42));
        match payload {
            S3SourcePayload::Objects(batch) => {
                assert_eq!(batch.object_ref, "s3-prod");
                assert_eq!(batch.bucket, "logs");
                assert_eq!(batch.prefix, "logs/2026/06/");
                assert_eq!(batch.objects[0].key, "logs/2026/a.json");
                assert_eq!(batch.grant_epoch, Some(5));
                assert!(batch.has_more);
                assert!(matches!(
                    batch.next_cursor,
                    Some(S3SourceCursor::Snapshot(S3SnapshotCursor {
                        continuation_token: Some(_)
                    }))
                ));
            }
            other => panic!("expected objects, got {other:?}"),
        }
        let calls = driver.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].operation, "snapshot");
        assert_eq!(calls[0].limit, 100);
    }

    #[test]
    fn poll_since_forwards_poll_cursor_and_returns_checkpoint() {
        let driver = Arc::new(FakeDriver::default());
        let service = service_with(driver.clone());
        let request = S3SourceRequest {
            object_ref: "s3-prod".into(),
            bucket: None,
            prefix: Some("logs/2026/06/".into()),
            operation: S3SourceOperation::PollSince,
            limit: Some(25),
            cursor: Some(S3SourceCursor::Poll(S3PollCursor {
                last_modified_unix_ms: 1,
                last_key: "logs/2026/a.json".into(),
            })),
        };

        let reply = service.handle_event(event_for(&request));
        let payload: S3SourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            S3SourcePayload::Objects(batch) => {
                assert!(!batch.has_more);
                assert!(matches!(
                    batch.next_cursor,
                    Some(S3SourceCursor::Poll(S3PollCursor {
                        last_modified_unix_ms: 2,
                        ..
                    }))
                ));
            }
            other => panic!("expected objects, got {other:?}"),
        }
        let calls = driver.calls.lock().unwrap();
        assert_eq!(calls[0].operation, "poll");
        assert_eq!(
            calls[0]
                .poll_cursor
                .as_ref()
                .map(|cursor| cursor.last_key.as_str()),
            Some("logs/2026/a.json")
        );
    }

    #[test]
    fn out_of_scope_prefix_is_rejected_before_driver_call() {
        let driver = Arc::new(FakeDriver::default());
        let service = service_with(driver.clone());
        let mut request = S3SourceRequest::snapshot_page("s3-prod");
        request.prefix = Some("logs/2025/".into());

        let reply = service.handle_event(event_for(&request));
        let payload: S3SourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            S3SourcePayload::Error(err) => assert_eq!(err.code, "scope-denied"),
            other => panic!("expected error, got {other:?}"),
        }
        assert!(driver.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn discover_fields_uses_default_object_metadata_catalog() {
        let driver = Arc::new(FakeDriver::default());
        let service = service_with(driver);
        let request = S3SourceRequest {
            object_ref: "s3-prod".into(),
            bucket: None,
            prefix: Some("logs/2026/".into()),
            operation: S3SourceOperation::DiscoverFields,
            limit: None,
            cursor: None,
        };

        let reply = service.handle_event(event_for(&request));
        let payload: S3SourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            S3SourcePayload::Fields(catalog) => {
                assert_eq!(catalog.object_ref, "s3-prod");
                assert!(catalog.fields.iter().any(|field| field.name == "Key"));
                assert!(
                    catalog
                        .fields
                        .iter()
                        .any(|field| field.name == "LastModified")
                );
            }
            other => panic!("expected fields, got {other:?}"),
        }
    }

    #[test]
    fn manifest_config_parses_runtime_grant() {
        let cfg = std::collections::HashMap::from([
            ("default_page_size".to_string(), ConfigValue::Int(10)),
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(std::collections::HashMap::from([
                    (
                        "object_ref".to_string(),
                        ConfigValue::String("s3-prod".into()),
                    ),
                    (
                        "endpoint_uri".to_string(),
                        ConfigValue::String("https://s3.local".into()),
                    ),
                    ("bucket".to_string(), ConfigValue::String("logs".into())),
                    (
                        "prefix_scopes".to_string(),
                        ConfigValue::Array(vec![ConfigValue::String("logs/*".into())]),
                    ),
                ]))]),
            ),
            (
                "__cnx_runtime".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([(
                    "resource_grants".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(std::collections::HashMap::from([
                        (
                            "resource_kind".to_string(),
                            ConfigValue::String("s3".into()),
                        ),
                        (
                            "object_ref".to_string(),
                            ConfigValue::String("s3-prod".into()),
                        ),
                        ("grant_epoch".to_string(), ConfigValue::Int(9)),
                    ]))]),
                )])),
            ),
        ]);

        let parsed = S3SourceRuntimeConfig::from_manifest_config(&cfg).expect("config");

        assert_eq!(parsed.product.default_page_size, 10);
        assert_eq!(parsed.runtime.endpoint_grants[0].grant_epoch, Some(9));
    }
}
