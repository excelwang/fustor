use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use capanix_app_sdk::raw::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
};
use capanix_app_sdk::runtime::{NodeId, RouteKey};
use capanix_app_sdk::{BoundRouteClient, BoundRouteServer, CnxError, Event, Result};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use union_graph::product_model::routes::{ingest_request_route_for, query_request_route_for};
use union_graph::{
    NodeKind, SourceGraphNode, SourceGraphSkeleton, SourceKind, UnionGraphPayload,
    UnionGraphRequest, UnionGraphStore, decode_msgpack, encode_msgpack,
};
use union_graph_runtime::UnionGraphService;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_then_query_routes_cover_all_source_types() {
    let channel = Arc::new(InMemoryChannelIo::default());
    let service = Arc::new(UnionGraphService::new(
        UnionGraphStore::default(),
        NodeId("union-graph-test".into()),
    ));
    let ingest_route: RouteKey = ingest_request_route_for("union-graph-node-a");
    let query_route: RouteKey = query_request_route_for("union-graph-node-a");
    let ingest_server = BoundRouteServer::open(channel.clone(), ingest_route.clone()).unwrap();
    let query_server = BoundRouteServer::open(channel.clone(), query_route.clone()).unwrap();
    let shutdown = CancellationToken::new();
    let ingest_shutdown = shutdown.clone();
    let ingest_service = service.clone();
    let ingest_task = tokio::spawn(async move {
        ingest_server
            .bind_concurrent_until(&ingest_shutdown, 2, move |events| {
                ingest_service.handle_events(events)
            })
            .await
    });
    let query_shutdown = shutdown.clone();
    let query_service = service.clone();
    let query_task = tokio::spawn(async move {
        query_server
            .bind_concurrent_until(&query_shutdown, 2, move |events| {
                query_service.handle_events(events)
            })
            .await
    });

    let ingest_client =
        BoundRouteClient::open(channel.clone(), ingest_route, NodeId("caller".into())).unwrap();
    let query_client =
        BoundRouteClient::open(channel.clone(), query_route, NodeId("caller".into())).unwrap();

    let replies = ingest_client
        .ask(
            encode_msgpack(&UnionGraphRequest::IngestSkeleton {
                skeleton: skeleton_for_all_sources(),
            })
            .unwrap(),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    let payload: UnionGraphPayload = decode_msgpack(replies[0].payload_bytes()).unwrap();
    match payload {
        UnionGraphPayload::IngestAck(ack) => assert_eq!(ack.nodes_upserted, 5),
        other => panic!("expected ingest ack, got {other:?}"),
    }

    let replies = query_client
        .ask(
            encode_msgpack(&UnionGraphRequest::SourceCoverage).unwrap(),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    let payload: UnionGraphPayload = decode_msgpack(replies[0].payload_bytes()).unwrap();
    match payload {
        UnionGraphPayload::Coverage(coverage) => {
            assert_eq!(coverage.source_counts[&SourceKind::Fs], 1);
            assert_eq!(coverage.source_counts[&SourceKind::S3], 1);
            assert_eq!(coverage.source_counts[&SourceKind::Es], 1);
            assert_eq!(coverage.source_counts[&SourceKind::Mysql], 1);
            assert_eq!(coverage.source_counts[&SourceKind::Pg], 1);
        }
        other => panic!("expected coverage, got {other:?}"),
    }

    shutdown.cancel();
    let _ = ingest_task.await;
    let _ = query_task.await;
}

fn skeleton_for_all_sources() -> SourceGraphSkeleton {
    SourceGraphSkeleton {
        nodes: vec![
            seed_node(
                SourceKind::Fs,
                "nfs-a",
                "/data/a.fastq",
                "fs://nfs-a/data/a.fastq",
            ),
            seed_node(SourceKind::S3, "s3-a", "bucket/a.txt", "s3://bucket/a.txt"),
            seed_node(
                SourceKind::Es,
                "es-a",
                "reads/_doc/1",
                "es://es-a/reads/_doc/1",
            ),
            seed_node(
                SourceKind::Mysql,
                "mysql-a",
                "lab/sample",
                "mysql://mysql-a/lab/sample",
            ),
            seed_node(
                SourceKind::Pg,
                "pg-a",
                "lab/public/sample",
                "pg://pg-a/lab/public/sample",
            ),
        ],
        edges: Vec::new(),
        cross_edges: Vec::new(),
        evidence: Vec::new(),
    }
}

fn seed_node(
    source_kind: SourceKind,
    source_instance: &str,
    local_id: &str,
    native_ref: &str,
) -> SourceGraphNode {
    SourceGraphNode {
        source_kind,
        source_instance: source_instance.into(),
        local_id: local_id.into(),
        node_kind: NodeKind::Asset,
        native_ref: native_ref.into(),
        display_name: local_id.into(),
        native_pointer: native_ref.into(),
        fingerprint: None,
        observed_at: 1,
        grant_epoch: None,
    }
}
