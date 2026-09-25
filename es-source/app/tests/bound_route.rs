use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use capanix_app_sdk::raw::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
};
use capanix_app_sdk::runtime::{NodeId, RouteKey};
use capanix_app_sdk::{BoundRouteClient, BoundRouteServer, CnxError, Event, Result};
use es_source::product_model::routes::{fetch_request_route_for, status_request_route_for};
use es_source::shared_types::{
    EsProbeStatus, EsSourceOperation, EsSourcePayload, EsSourceRequest, EsSourceStatus,
    decode_msgpack,
};
use es_source::{
    EsEndpointConfig, EsSourceConfig, EsSourceRuntimeConfig, EsSourceRuntimeInputs,
    GrantedEsEndpoint, ResolvedCredential, ResolvedEsEndpoint,
};
use es_source_runtime::{EsSourceDriver, EsSourceService};
use serde_json::Value;
use source_kit::{
    DecodedSourceResponse, SourceExecutionContext, SourceExecutionEnvelope,
    SourceOperationCapability, decode_source_response, encode_source_request, source_now_ms,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct InMemoryChannelIo {
    queues: Mutex<HashMap<String, VecDeque<Vec<Event>>>>,
    closed: Mutex<HashSet<String>>,
    notify: Notify,
}

#[async_trait]
impl ChannelIoSubset for InMemoryChannelIo {
    async fn channel_send(&self, _ctx: BoundaryContext, request: ChannelSendRequest) -> Result<()> {
        if self.closed.lock().unwrap().contains(&request.channel_key.0) {
            return Err(CnxError::ChannelClosed);
        }
        self.queues
            .lock()
            .unwrap()
            .entry(request.channel_key.0)
            .or_default()
            .push_back(request.events);
        self.notify.notify_waiters();
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> Result<Vec<Event>> {
        let channel = request.channel_key.0;
        let deadline = request
            .timeout_ms
            .map(|timeout_ms| Instant::now() + Duration::from_millis(timeout_ms));
        loop {
            if let Some(batch) = self
                .queues
                .lock()
                .unwrap()
                .get_mut(&channel)
                .and_then(VecDeque::pop_front)
            {
                return Ok(batch);
            }
            if self.closed.lock().unwrap().contains(&channel) {
                return Err(CnxError::ChannelClosed);
            }
            match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(CnxError::Timeout);
                    }
                    if tokio::time::timeout(deadline - now, self.notify.notified())
                        .await
                        .is_err()
                    {
                        return Err(CnxError::Timeout);
                    }
                }
                None => self.notify.notified().await,
            }
        }
    }

    fn channel_close(&self, _ctx: BoundaryContext, channel: ChannelKey) -> Result<()> {
        self.closed.lock().unwrap().insert(channel.0);
        self.notify.notify_waiters();
        Ok(())
    }
}

struct ProbeDriver {
    calls: AtomicUsize,
}

impl ProbeDriver {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

impl EsSourceDriver for ProbeDriver {
    fn execute(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        request: &EsSourceRequest,
        _limit: usize,
    ) -> Result<EsSourcePayload> {
        assert_eq!(credential, &ResolvedCredential::None);
        assert_eq!(request.operation, EsSourceOperation::ProbeConnection);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(EsSourcePayload::Probe(EsProbeStatus {
            object_ref: endpoint.object_ref.clone(),
            endpoint_uri: endpoint.endpoint_uri.clone(),
            reachable: true,
            cluster_name: Some("route-test".into()),
            diagnostics: Some(format!("grant_epoch={:?}", endpoint.grant_epoch)),
        }))
    }
}

fn test_config() -> EsSourceRuntimeConfig {
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
                grant_epoch: Some(7),
            }],
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_bound_route_returns_typed_msgpack_reply() {
    let route: RouteKey = fetch_request_route_for("es-source-node-a");
    let channel = Arc::new(InMemoryChannelIo::default());
    let driver = Arc::new(ProbeDriver::new());
    let service = Arc::new(EsSourceService::new(
        test_config(),
        driver.clone(),
        NodeId("es-source-test".into()),
    ));
    let server = BoundRouteServer::open(channel.clone(), route.clone()).expect("server");
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server_task = tokio::spawn({
        let service = service.clone();
        async move {
            server
                .bind_concurrent_until(&server_shutdown, 2, move |events| {
                    service.handle_events(events)
                })
                .await
        }
    });

    let client = BoundRouteClient::open(channel.clone(), route.clone(), NodeId("caller".into()))
        .expect("client");
    let request = EsSourceRequest {
        object_ref: "es-prod".into(),
        index: "logs-2026-05".into(),
        operation: EsSourceOperation::ProbeConnection,
        limit: Some(10),
        cursor: None,
        timestamp_after: None,
        field_filter: Vec::new(),
        query_filter: None::<Value>,
    };

    let now_ms = source_now_ms();
    let request = SourceExecutionEnvelope::new(
        SourceExecutionContext::one_shot(
            "es-bound-route-probe",
            7,
            now_ms,
            now_ms + 2_000,
            10,
            64 * 1024,
        ),
        request,
    );
    let replies = client
        .ask(
            encode_source_request(&request)
                .expect("encode request")
                .into(),
            Duration::from_secs(2),
        )
        .await
        .expect("route ask");

    assert_eq!(replies.len(), 1);
    assert!(
        replies[0].metadata().correlation_id.is_some(),
        "server must preserve bound-route correlation id"
    );
    let DecodedSourceResponse::Envelope(response) =
        decode_source_response::<EsSourcePayload>(replies[0].payload_bytes()).expect("reply")
    else {
        panic!("expected versioned source response");
    };
    assert_eq!(response.request_id, "es-bound-route-probe");
    let receipt = response.receipt.expect("source receipt");
    assert_eq!(receipt.source_kind, "elasticsearch");
    assert_eq!(receipt.grant_epoch, 7);
    assert_eq!(
        receipt.actual.bytes,
        replies[0].payload_bytes().len() as u64
    );
    let payload = response.payload;
    match payload {
        EsSourcePayload::Probe(status) => {
            assert_eq!(status.object_ref, "es-prod");
            assert_eq!(status.cluster_name.as_deref(), Some("route-test"));
            assert_eq!(status.diagnostics.as_deref(), Some("grant_epoch=Some(7)"));
        }
        other => panic!("expected probe payload, got {other:?}"),
    }
    assert_eq!(driver.calls.load(Ordering::SeqCst), 1);

    shutdown.cancel();
    drop(client);
    channel
        .channel_close(BoundaryContext::default(), ChannelKey(route.0.clone()))
        .expect("close request route");
    let server_result = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server shutdown")
        .expect("server join");
    assert!(server_result.is_ok(), "server_result={server_result:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_bound_route_returns_typed_local_readiness_without_driver_call() {
    let bound_fetch_route: RouteKey = fetch_request_route_for("es-source-node-a");
    let bound_status_route: RouteKey = status_request_route_for("es-source-node-a");
    let channel = Arc::new(InMemoryChannelIo::default());
    let driver = Arc::new(ProbeDriver::new());
    let service = Arc::new(EsSourceService::new(
        test_config(),
        driver.clone(),
        NodeId("es-source-test".into()),
    ));
    let server =
        BoundRouteServer::open(channel.clone(), bound_status_route.clone()).expect("server");
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let fetch_route_key = bound_fetch_route.0.clone();
    let status_route_key = bound_status_route.0.clone();
    let server_task = tokio::spawn({
        let service = service.clone();
        async move {
            server
                .bind_concurrent_until(&server_shutdown, 2, move |events| {
                    service.handle_status_events(events, &fetch_route_key, &status_route_key)
                })
                .await
        }
    });

    let client = BoundRouteClient::open(
        channel.clone(),
        bound_status_route.clone(),
        NodeId("caller".into()),
    )
    .expect("client");

    let replies = client
        .ask(
            bytes::Bytes::from_static(b"ignored"),
            Duration::from_secs(2),
        )
        .await
        .expect("route ask");

    assert_eq!(replies.len(), 1);
    assert!(
        replies[0].metadata().correlation_id.is_some(),
        "server must preserve bound-route correlation id"
    );
    let payload: EsSourcePayload = decode_msgpack(replies[0].payload_bytes()).expect("reply");
    match payload {
        EsSourcePayload::Status(EsSourceStatus {
            node_id,
            fetch_route,
            status_route,
            configured_endpoints,
            active_grants,
            ready,
            capabilities,
            ..
        }) => {
            assert_eq!(node_id, "es-source-test");
            assert_eq!(fetch_route, bound_fetch_route.0);
            assert_eq!(status_route, bound_status_route.0);
            assert_eq!(configured_endpoints, 1);
            assert_eq!(active_grants, 1);
            assert!(ready);
            assert!(capabilities.supports(SourceOperationCapability::BoundedSnapshot));
            assert!(capabilities.supports(SourceOperationCapability::IncrementalPoll));
            assert!(capabilities.supports(SourceOperationCapability::ExplicitSessionClose));
        }
        other => panic!("expected status payload, got {other:?}"),
    }
    assert_eq!(driver.calls.load(Ordering::SeqCst), 0);

    shutdown.cancel();
    drop(client);
    channel
        .channel_close(
            BoundaryContext::default(),
            ChannelKey(bound_status_route.0.clone()),
        )
        .expect("close request route");
    let server_result = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server shutdown")
        .expect("server join");
    assert!(server_result.is_ok(), "server_result={server_result:?}");
}
