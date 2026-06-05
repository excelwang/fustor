use std::sync::Arc;

use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event, Result};
use es_source::EsSourceRuntimeConfig;
use es_source::shared_types::{
    EsSourceErrorPayload, EsSourcePayload, EsSourceRequest, decode_msgpack, encode_msgpack,
};

use crate::driver::EsSourceDriver;

#[derive(Clone)]
pub struct EsSourceService {
    config: EsSourceRuntimeConfig,
    driver: Arc<dyn EsSourceDriver>,
    origin_id: NodeId,
}

impl EsSourceService {
    pub fn new(
        config: EsSourceRuntimeConfig,
        driver: Arc<dyn EsSourceDriver>,
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
            Err(err) => error_payload_from_cnx(err),
        };
        self.reply_event(payload, correlation_id)
            .unwrap_or_else(|err| self.fallback_error_event(err, correlation_id))
    }

    fn handle_event_payload(&self, payload: &[u8]) -> Result<EsSourcePayload> {
        let request: EsSourceRequest = decode_msgpack(payload)?;
        let endpoint = self
            .config
            .endpoint_for(&request.object_ref, &request.index)?;
        let credential = self
            .config
            .resolve_credential(endpoint.credential_ref.as_deref())?;
        let limit = self.config.effective_limit(request.limit);
        self.driver
            .execute(&endpoint, &credential, &request, limit)
            .map_err(|err| match err {
                CnxError::AccessDenied(_)
                | CnxError::InvalidInput(_)
                | CnxError::ScopeDenied(_)
                | CnxError::PeerError(_)
                | CnxError::LinkError(_) => err,
                other => CnxError::PeerError(other.to_string()),
            })
    }

    fn reply_event(&self, payload: EsSourcePayload, correlation_id: Option<u64>) -> Result<Event> {
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
        let payload = EsSourcePayload::Error(EsSourceErrorPayload::new(
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

pub fn error_payload_from_cnx(err: CnxError) -> EsSourcePayload {
    let (code, retryable) = match &err {
        CnxError::AccessDenied(_) => ("access-denied", false),
        CnxError::ScopeDenied(_) => ("scope-denied", false),
        CnxError::InvalidInput(_) | CnxError::ProtocolViolation(_) => ("invalid-request", false),
        CnxError::Timeout
        | CnxError::Backpressure
        | CnxError::ChannelClosed
        | CnxError::NotReady(_) => ("transient-runtime", true),
        CnxError::LinkError(_) | CnxError::TransportClosed(_) => ("es-transport", true),
        CnxError::PeerError(_) => ("es-peer", false),
        _ => ("internal", true),
    };
    EsSourcePayload::Error(EsSourceErrorPayload::new(code, err.to_string(), retryable))
}

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_micros() as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use capanix_app_sdk::runtime::ConfigValue;
    use es_source::shared_types::{
        EsProbeStatus, EsSourceOperation, decode_msgpack, encode_msgpack,
    };
    use es_source::{
        EsEndpointConfig, EsSourceConfig, EsSourceRuntimeInputs, GrantedEsEndpoint,
        ResolvedCredential, ResolvedEsEndpoint,
    };

    use super::*;

    #[derive(Default)]
    struct FakeDriver {
        calls: Mutex<
            Vec<(
                ResolvedEsEndpoint,
                ResolvedCredential,
                EsSourceRequest,
                usize,
            )>,
        >,
    }

    impl EsSourceDriver for FakeDriver {
        fn execute(
            &self,
            endpoint: &ResolvedEsEndpoint,
            credential: &ResolvedCredential,
            request: &EsSourceRequest,
            limit: usize,
        ) -> Result<EsSourcePayload> {
            self.calls.lock().unwrap().push((
                endpoint.clone(),
                credential.clone(),
                request.clone(),
                limit,
            ));
            Ok(EsSourcePayload::Probe(EsProbeStatus {
                object_ref: endpoint.object_ref.clone(),
                endpoint_uri: endpoint.endpoint_uri.clone(),
                reachable: true,
                cluster_name: Some("fake".into()),
                diagnostics: None,
            }))
        }
    }

    fn test_service(driver: Arc<FakeDriver>) -> EsSourceService {
        EsSourceService::new(
            EsSourceRuntimeConfig {
                product: EsSourceConfig {
                    endpoints: vec![EsEndpointConfig {
                        object_ref: "es-prod".into(),
                        endpoint_uri: "https://es.internal".into(),
                        credential_ref: None,
                        index_scopes: vec!["logs-*".into()],
                        timestamp_field: "@timestamp".into(),
                        tie_breaker_field: "_id".into(),
                        active: true,
                    }],
                    default_page_size: 50,
                    max_page_size: 100,
                    ..EsSourceConfig::default()
                },
                runtime: EsSourceRuntimeInputs {
                    endpoint_grants: vec![GrantedEsEndpoint {
                        object_ref: "es-prod".into(),
                        endpoint_ref: Some("cluster-a".into()),
                        endpoint_uri: None,
                        credential_ref: None,
                        index_scopes: vec!["logs-2026-*".into()],
                        interfaces: vec!["read".into()],
                        active: true,
                        grant_epoch: Some(9),
                    }],
                },
            },
            driver,
            NodeId("es-source-test".into()),
        )
    }

    fn request_event(request: &EsSourceRequest) -> Event {
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
    fn handle_event_returns_msgpack_reply_with_correlation() {
        let driver = Arc::new(FakeDriver::default());
        let service = test_service(driver.clone());
        let mut request = EsSourceRequest::snapshot_page("es-prod", "logs-2026-05");
        request.operation = EsSourceOperation::ProbeConnection;
        request.limit = Some(1_000);

        let reply = service.handle_event(request_event(&request));
        let payload: EsSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        assert_eq!(reply.metadata().correlation_id, Some(42));
        assert!(matches!(payload, EsSourcePayload::Probe(_)));
        let calls = driver.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].3, 100);
        assert_eq!(calls[0].0.grant_epoch, Some(9));
    }

    #[test]
    fn handle_event_rejects_out_of_scope_index_before_driver_call() {
        let driver = Arc::new(FakeDriver::default());
        let service = test_service(driver.clone());
        let request = EsSourceRequest::snapshot_page("es-prod", "logs-2025-05");

        let reply = service.handle_event(request_event(&request));
        let payload: EsSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            EsSourcePayload::Error(err) => assert_eq!(err.code, "scope-denied"),
            other => panic!("expected error payload, got {other:?}"),
        }
        assert!(driver.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn malformed_msgpack_returns_invalid_request_payload() {
        let driver = Arc::new(FakeDriver::default());
        let service = test_service(driver);
        let event = Event::new(
            EventMetadata {
                origin_id: NodeId("caller".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            bytes::Bytes::from_static(b"not-msgpack"),
        );

        let reply = service.handle_event(event);
        let payload: EsSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            EsSourcePayload::Error(err) => assert_eq!(err.code, "invalid-request"),
            other => panic!("expected error payload, got {other:?}"),
        }
    }

    #[test]
    fn manifest_shape_can_build_service_config() {
        let cfg = std::collections::HashMap::from([
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(std::collections::HashMap::from([
                    (
                        "object_ref".to_string(),
                        ConfigValue::String("es-prod".into()),
                    ),
                    (
                        "endpoint_uri".to_string(),
                        ConfigValue::String("https://es.internal".into()),
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
                            ConfigValue::String("elasticsearch".into()),
                        ),
                        (
                            "object_ref".to_string(),
                            ConfigValue::String("es-prod".into()),
                        ),
                    ]))]),
                )])),
            ),
        ]);

        let config = EsSourceRuntimeConfig::from_manifest_config(&cfg).expect("manifest config");

        assert!(config.endpoint_for("es-prod", "anything").is_ok());
    }
}
