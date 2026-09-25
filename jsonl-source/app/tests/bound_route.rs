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
use jsonl_source::product_model::routes::{fetch_request_route_for, status_request_route_for};
use jsonl_source::shared_types::{
    JsonlFieldCatalog, JsonlProbeStatus, JsonlSnapshotCursor, JsonlSourceOperation,
    JsonlSourcePayload, JsonlSourceRequest,
};
use jsonl_source::{
    GrantedJsonlEndpoint, JsonlEndpointConfig, JsonlSourceConfig, JsonlSourceRuntimeConfig,
    JsonlSourceRuntimeInputs, ResolvedJsonlEndpoint, decode_msgpack,
};
use jsonl_source_runtime::{JsonlRecordPage, JsonlSourceDriver, JsonlSourceService};
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

impl JsonlSourceDriver for ProbeDriver {
    fn probe(&self, endpoint: &ResolvedJsonlEndpoint) -> Result<JsonlProbeStatus> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(JsonlProbeStatus {
            object_ref: endpoint.object_ref.clone(),
            root_path: endpoint.root_path.clone(),
            reachable: true,
            diagnostics: Some(format!("grant_epoch={:?}", endpoint.grant_epoch)),
        })
    }

    fn discover_fields(
        &self,
        endpoint: &ResolvedJsonlEndpoint,
        path: &str,
    ) -> Result<JsonlFieldCatalog> {
        Ok(JsonlFieldCatalog {
            object_ref: endpoint.object_ref.clone(),
            path: path.to_string(),
            fields: Vec::new(),
        })
    }

    fn snapshot_page(
        &self,
        _endpoint: &ResolvedJsonlEndpoint,
        _path: &str,
        _limit: usize,
        _cursor: Option<&JsonlSnapshotCursor>,
    ) -> Result<JsonlRecordPage> {
        Err(CnxError::NotSupported(
            "bound route test only exercises probe".into(),
        ))
    }
}

fn test_config() -> JsonlSourceRuntimeConfig {
    JsonlSourceRuntimeConfig {
        product: JsonlSourceConfig {
            endpoints: vec![JsonlEndpointConfig {
                object_ref: "jsonl".into(),
                root_path: "/tmp".into(),
                path_scopes: vec!["datasets/*".into()],
                active: true,
            }],
            default_page_size: 50,
            max_page_size: 100,
            field_sample_limit: 100,
        },
        runtime: JsonlSourceRuntimeInputs {
            endpoint_grants: vec![GrantedJsonlEndpoint {
                object_ref: "jsonl".into(),
                path_scopes: Vec::new(),
                interfaces: vec!["read".into()],
                active: true,
                grant_epoch: Some(9),
            }],
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_bound_route_returns_typed_msgpack_reply() {
    let route: RouteKey = fetch_request_route_for("jsonl-source-node-a");
    let channel = Arc::new(InMemoryChannelIo::default());
    let driver = Arc::new(ProbeDriver::new());
    let service = Arc::new(JsonlSourceService::new(
        test_config(),
        driver.clone(),
        NodeId("jsonl-source-test".into()),
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
    let request = JsonlSourceRequest {
        object_ref: "jsonl".into(),
        path: Some("datasets/a.jsonl".into()),
        operation: JsonlSourceOperation::ProbeConnection,
        limit: None,
        cursor: None,
    };
    let now_ms = source_now_ms();
    let request = SourceExecutionEnvelope::new(
        SourceExecutionContext::one_shot(
            "jsonl-bound-route-probe",
            9,
            now_ms,
            now_ms + 2_000,
            1,
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
        decode_source_response::<JsonlSourcePayload>(replies[0].payload_bytes()).expect("reply")
    else {
        panic!("versioned request must return a versioned source response")
    };
    assert_eq!(response.request_id, "jsonl-bound-route-probe");
    let receipt = response.receipt.expect("source receipt");
    assert_eq!(receipt.source_kind, "jsonl");
    assert_eq!(receipt.grant_epoch, 9);
    assert_eq!(
        receipt.actual.bytes,
        replies[0].payload_bytes().len() as u64
    );
    let payload = response.payload;
    match payload {
        JsonlSourcePayload::Probe(status) => {
            assert_eq!(status.object_ref, "jsonl");
            assert_eq!(status.diagnostics.as_deref(), Some("grant_epoch=Some(9)"));
        }
        other => panic!("expected probe payload, got {other:?}"),
    }
    assert_eq!(driver.calls.load(Ordering::SeqCst), 1);

    shutdown.cancel();
    server_task.await.expect("join").expect("server close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_bound_route_returns_readiness_payload() {
    let route: RouteKey = status_request_route_for("jsonl-source-node-a");
    let channel = Arc::new(InMemoryChannelIo::default());
    let service = Arc::new(JsonlSourceService::new(
        test_config(),
        Arc::new(ProbeDriver::new()),
        NodeId("jsonl-source-test".into()),
    ));
    let server = BoundRouteServer::open(channel.clone(), route.clone()).expect("server");
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server_task = tokio::spawn({
        let service = service.clone();
        async move {
            server
                .bind_concurrent_until(&server_shutdown, 2, move |events| {
                    service.handle_status_events(
                        events,
                        "jsonl-source.fetch.node_a:v1.req",
                        "jsonl-source.status.node_a:v1.req",
                    )
                })
                .await
        }
    });

    let client = BoundRouteClient::open(channel.clone(), route.clone(), NodeId("caller".into()))
        .expect("client");
    let replies = client
        .ask(Vec::new().into(), Duration::from_secs(2))
        .await
        .expect("route ask");

    let payload: JsonlSourcePayload = decode_msgpack(replies[0].payload_bytes()).expect("reply");
    match payload {
        JsonlSourcePayload::Status(status) => {
            assert!(status.ready);
            assert_eq!(status.source_kind, "jsonl");
            assert!(
                status
                    .capabilities
                    .supports(SourceOperationCapability::FieldDiscovery)
            );
            assert!(
                status
                    .capabilities
                    .supports(SourceOperationCapability::BoundedSnapshot)
            );
            assert!(
                !status
                    .capabilities
                    .supports(SourceOperationCapability::IncrementalPoll)
            );
        }
        other => panic!("expected status payload, got {other:?}"),
    }

    shutdown.cancel();
    server_task.await.expect("join").expect("server close");
}
