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
use s3_source::product_model::routes::{fetch_request_route_for, status_request_route_for};
use s3_source::shared_types::{
    S3FieldCatalog, S3PollCursor, S3ProbeStatus, S3SnapshotCursor, S3SourceOperation,
    S3SourcePayload, S3SourceRequest,
};
use s3_source::{
    GrantedS3Endpoint, ResolvedCredential, ResolvedS3Endpoint, S3EndpointConfig, S3SourceConfig,
    S3SourceRuntimeConfig, S3SourceRuntimeInputs, decode_msgpack,
};
use s3_source_runtime::{S3ObjectPage, S3SourceDriver, S3SourceService};
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

impl S3SourceDriver for ProbeDriver {
    fn probe(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
    ) -> Result<S3ProbeStatus> {
        assert_eq!(credential, &ResolvedCredential::None);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(S3ProbeStatus {
            object_ref: endpoint.object_ref.clone(),
            endpoint_uri: endpoint.endpoint_uri.clone(),
            bucket: endpoint.bucket.clone(),
            reachable: true,
            diagnostics: Some(format!("grant_epoch={:?}", endpoint.grant_epoch)),
        })
    }

    fn discover_fields(
        &self,
        endpoint: &ResolvedS3Endpoint,
        _credential: &ResolvedCredential,
    ) -> Result<S3FieldCatalog> {
        Ok(S3FieldCatalog {
            object_ref: endpoint.object_ref.clone(),
            fields: Vec::new(),
        })
    }

    fn snapshot_page(
        &self,
        _endpoint: &ResolvedS3Endpoint,
        _credential: &ResolvedCredential,
        _limit: usize,
        _cursor: Option<&S3SnapshotCursor>,
    ) -> Result<S3ObjectPage> {
        Err(CnxError::NotSupported(
            "bound route test only exercises probe".into(),
        ))
    }

    fn poll_since(
        &self,
        _endpoint: &ResolvedS3Endpoint,
        _credential: &ResolvedCredential,
        _limit: usize,
        _cursor: Option<&S3PollCursor>,
    ) -> Result<S3ObjectPage> {
        Err(CnxError::NotSupported(
            "bound route test only exercises probe".into(),
        ))
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
                prefix_scopes: Vec::new(),
                interfaces: vec!["read".into()],
                active: true,
                grant_epoch: Some(9),
            }],
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_bound_route_returns_typed_msgpack_reply() {
    let route: RouteKey = fetch_request_route_for("s3-source-node-a");
    let channel = Arc::new(InMemoryChannelIo::default());
    let driver = Arc::new(ProbeDriver::new());
    let service = Arc::new(S3SourceService::new(
        test_config(),
        driver.clone(),
        NodeId("s3-source-test".into()),
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
    let request = S3SourceRequest {
        object_ref: "s3-prod".into(),
        bucket: None,
        prefix: Some("logs/2026/06/".into()),
        operation: S3SourceOperation::ProbeConnection,
        limit: None,
        cursor: None,
    };

    let now_ms = source_now_ms();
    let request = SourceExecutionEnvelope::new(
        SourceExecutionContext::one_shot(
            "s3-bound-route-probe",
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
        decode_source_response::<S3SourcePayload>(replies[0].payload_bytes()).expect("reply")
    else {
        panic!("expected versioned source response");
    };
    assert_eq!(response.request_id, "s3-bound-route-probe");
    let receipt = response.receipt.expect("source receipt");
    assert_eq!(receipt.source_kind, "s3");
    assert_eq!(receipt.grant_epoch, 9);
    assert_eq!(
        receipt.actual.bytes,
        replies[0].payload_bytes().len() as u64
    );
    let payload = response.payload;
    match payload {
        S3SourcePayload::Probe(status) => {
            assert_eq!(status.object_ref, "s3-prod");
            assert_eq!(status.bucket, "logs");
            assert_eq!(status.diagnostics.as_deref(), Some("grant_epoch=Some(9)"));
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
    let fetch_route: RouteKey = fetch_request_route_for("s3-source-node-a");
    let status_route: RouteKey = status_request_route_for("s3-source-node-a");
    let channel = Arc::new(InMemoryChannelIo::default());
    let driver = Arc::new(ProbeDriver::new());
    let service = Arc::new(S3SourceService::new(
        test_config(),
        driver.clone(),
        NodeId("s3-source-test".into()),
    ));
    let server = BoundRouteServer::open(channel.clone(), status_route.clone()).expect("server");
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let fetch_route_key = fetch_route.0.clone();
    let status_route_key = status_route.0.clone();
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
        status_route.clone(),
        NodeId("caller".into()),
    )
    .expect("client");
    let replies = client
        .ask(Vec::<u8>::new().into(), Duration::from_secs(2))
        .await
        .expect("route ask");

    assert_eq!(replies.len(), 1);
    let payload: S3SourcePayload = decode_msgpack(replies[0].payload_bytes()).expect("reply");
    match payload {
        S3SourcePayload::Status(status) => {
            assert!(status.ready);
            assert_eq!(status.source_kind, "s3");
            assert_eq!(status.configured_endpoints, 1);
            assert_eq!(status.active_grants, 1);
            assert!(
                status
                    .capabilities
                    .supports(SourceOperationCapability::BoundedSnapshot)
            );
            assert!(
                status
                    .capabilities
                    .supports(SourceOperationCapability::IncrementalPoll)
            );
        }
        other => panic!("expected status payload, got {other:?}"),
    }
    assert_eq!(driver.calls.load(Ordering::SeqCst), 0);

    shutdown.cancel();
    drop(client);
    channel
        .channel_close(
            BoundaryContext::default(),
            ChannelKey(status_route.0.clone()),
        )
        .expect("close status route");
    let server_result = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server shutdown")
        .expect("server join");
    assert!(server_result.is_ok(), "server_result={server_result:?}");
}
