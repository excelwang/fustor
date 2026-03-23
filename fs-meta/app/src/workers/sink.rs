use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use capanix_app_sdk::runtime::{ControlEnvelope, NodeId, RecvOpts, RuntimeWorkerBinding};
use capanix_app_sdk::{CnxError, Event, Result};
use capanix_host_adapter_fs::{HostAdapter, exchange_host_adapter_from_channel_boundary};
use capanix_runtime_entry_sdk::advanced::boundary::ChannelIoSubset;
use capanix_runtime_entry_sdk::worker_runtime::{
    RuntimeWorkerClientFactory, TypedRuntimeWorkerClient, TypedWorkerClient, TypedWorkerInit,
};

use crate::query::models::{HealthStats, QueryNode};
use crate::query::path::root_file_name_bytes;
use crate::query::request::{InternalQueryRequest, MaterializedQueryPayload, QueryOp, QueryScope};
use crate::runtime::orchestration::{SinkControlSignal, sink_control_signals_from_envelopes};
use crate::runtime::routes::{METHOD_FIND, ROUTE_TOKEN_FS_META, default_route_bindings};
use crate::sink::{SinkFileMeta, SinkStatusSnapshot, VisibilityLagSample};
use crate::source::config::{GrantedMountRoot, SourceConfig};
use crate::workers::sink_ipc::{
    SinkWorkerInitConfig, SinkWorkerRequest, SinkWorkerResponse, decode_request, decode_response,
    encode_request, encode_response,
};

const SINK_WORKER_UPDATE_ROOTS_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const SINK_WORKER_FORCE_FIND_TIMEOUT: Duration = Duration::from_secs(60);
const SINK_WORKER_FORCE_FIND_REPLY_IDLE_GRACE: Duration = Duration::from_secs(5);
const SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT: Duration = Duration::from_secs(60);

fn decode_exact_query_node(events: Vec<Event>, path: &[u8]) -> Result<Option<QueryNode>> {
    let mut selected = None::<QueryNode>;
    for event in &events {
        let payload = rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
            .map_err(|e| CnxError::Internal(format!("decode query_node response failed: {e}")))?;
        let MaterializedQueryPayload::Tree(response) = payload else {
            return Err(CnxError::Internal(
                "unexpected stats payload for query_node".into(),
            ));
        };
        let mut consider = |node: QueryNode| {
            if node.path != path {
                return;
            }
            match selected.as_mut() {
                Some(existing) if existing.modified_time_us > node.modified_time_us => {}
                Some(existing) => *existing = node,
                None => selected = Some(node),
            }
        };
        if response.root.exists {
            consider(QueryNode {
                path: response.root.path.clone(),
                file_name: root_file_name_bytes(&response.root.path),
                size: response.root.size,
                modified_time_us: response.root.modified_time_us,
                is_dir: response.root.is_dir,
                monitoring_attested: response.reliability.reliable,
                is_suspect: false,
                is_blind_spot: false,
            });
        }
        for entry in response.entries {
            consider(QueryNode {
                file_name: root_file_name_bytes(&entry.path),
                path: entry.path,
                size: entry.size,
                modified_time_us: entry.modified_time_us,
                is_dir: entry.is_dir,
                monitoring_attested: response.reliability.reliable,
                is_suspect: false,
                is_blind_spot: false,
            });
        }
    }
    Ok(selected)
}

#[derive(Clone)]
pub struct SinkWorkerClientHandle {
    node_id: NodeId,
    config: SourceConfig,
    worker_factory: RuntimeWorkerClientFactory,
    worker_binding: RuntimeWorkerBinding,
    worker: TypedRuntimeWorkerClient<SinkWorkerRpc, SourceConfig>,
    logical_roots_cache: Arc<Mutex<Vec<crate::source::config::RootSpec>>>,
    status_cache: Arc<Mutex<SinkStatusSnapshot>>,
}

impl SinkWorkerClientHandle {
    pub(crate) fn new(
        node_id: NodeId,
        config: SourceConfig,
        worker_binding: RuntimeWorkerBinding,
        worker_factory: RuntimeWorkerClientFactory,
    ) -> Result<Self> {
        let logical_roots_cache = Arc::new(Mutex::new(config.roots.clone()));
        Ok(Self {
            worker: worker_factory.connect(
                node_id.clone(),
                config.clone(),
                worker_binding.clone(),
            )?,
            node_id,
            config,
            worker_factory,
            worker_binding,
            logical_roots_cache,
            status_cache: Arc::new(Mutex::new(SinkStatusSnapshot::default())),
        })
    }

    async fn existing_client(&self) -> Result<Option<TypedWorkerClient<SinkWorkerRpc>>> {
        self.worker.existing_client().await
    }

    fn cached_logical_roots(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        self.logical_roots_cache
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| CnxError::Internal("sink worker logical roots cache lock poisoned".into()))
    }

    fn update_cached_logical_roots(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
    ) -> Result<()> {
        let mut guard = self.logical_roots_cache.lock().map_err(|_| {
            CnxError::Internal("sink worker logical roots cache lock poisoned".into())
        })?;
        *guard = roots;
        Ok(())
    }

    fn cached_status_snapshot(&self) -> Result<SinkStatusSnapshot> {
        self.status_cache
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| CnxError::Internal("sink worker status cache lock poisoned".into()))
    }

    fn update_cached_status_snapshot(&self, snapshot: SinkStatusSnapshot) -> Result<()> {
        let mut guard = self
            .status_cache
            .lock()
            .map_err(|_| CnxError::Internal("sink worker status cache lock poisoned".into()))?;
        *guard = snapshot;
        Ok(())
    }

    async fn with_started_retry<T, F, Fut>(&self, op: F) -> Result<T>
    where
        F: Fn(TypedWorkerClient<SinkWorkerRpc>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.worker.with_started_retry(op).await
    }

    async fn client(&self) -> Result<TypedWorkerClient<SinkWorkerRpc>> {
        self.worker.client().await
    }

    async fn call_worker(
        client: &TypedWorkerClient<SinkWorkerRpc>,
        request: SinkWorkerRequest,
        timeout: Duration,
    ) -> Result<SinkWorkerResponse> {
        client.call_with_timeout(request, timeout).await
    }

    pub async fn ensure_started(&self) -> Result<()> {
        eprintln!(
            "fs_meta_sink_worker_client: ensure_started begin node={}",
            self.node_id.0
        );
        self.worker.ensure_started().await.map(|_| {
            eprintln!(
                "fs_meta_sink_worker_client: ensure_started ok node={}",
                self.node_id.0
            );
        })
    }

    pub async fn update_logical_roots(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
        host_object_grants: Vec<GrantedMountRoot>,
    ) -> Result<()> {
        self.with_started_retry(|client| {
            let roots = roots.clone();
            let host_object_grants = host_object_grants.clone();
            async move {
            match Self::call_worker(
                &client,
                SinkWorkerRequest::UpdateLogicalRoots {
                    roots: roots.clone(),
                    host_object_grants: host_object_grants.clone(),
                },
                SINK_WORKER_UPDATE_ROOTS_RPC_TIMEOUT,
            )
            .await?
            {
                SinkWorkerResponse::Ack => Ok(()),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker response for update roots: {:?}",
                    other
                ))),
            }
            }
        })
        .await?;
        self.update_cached_logical_roots(roots)?;
        self.update_cached_status_snapshot(SinkStatusSnapshot::default())
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        self.cached_logical_roots()
    }

    pub async fn logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        let roots = match Self::call_worker(
            &self.client().await?,
            SinkWorkerRequest::LogicalRootsSnapshot,
            Duration::from_secs(5),
        )
        .await?
        {
            SinkWorkerResponse::LogicalRoots(roots) => roots,
            other => {
                return Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker response for logical roots: {:?}",
                    other
                )));
            }
        };
        self.update_cached_logical_roots(roots.clone())?;
        Ok(roots)
    }

    pub async fn health(&self) -> Result<HealthStats> {
        match Self::call_worker(
            &self.client().await?,
            SinkWorkerRequest::Health,
            Duration::from_secs(5),
        )
        .await?
        {
            SinkWorkerResponse::Health(health) => Ok(health),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for health: {:?}",
                other
            ))),
        }
    }

    pub async fn status_snapshot(&self) -> Result<SinkStatusSnapshot> {
        let snapshot = match Self::call_worker(
            &self.client().await?,
            SinkWorkerRequest::StatusSnapshot,
            Duration::from_secs(5),
        )
        .await?
        {
            SinkWorkerResponse::StatusSnapshot(snapshot) => snapshot,
            other => {
                return Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker response for status snapshot: {:?}",
                    other
                )));
            }
        };
        self.update_cached_status_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }

    pub async fn status_snapshot_nonblocking(&self) -> Result<SinkStatusSnapshot> {
        match self.existing_client().await? {
            Some(client) => match Self::call_worker(
                &client,
                SinkWorkerRequest::StatusSnapshot,
                Duration::from_secs(5),
            )
            .await
            {
                Ok(snapshot) => {
                    let SinkWorkerResponse::StatusSnapshot(snapshot) = snapshot else {
                        return Err(CnxError::ProtocolViolation(format!(
                            "unexpected sink worker response for status snapshot: {:?}",
                            snapshot
                        )));
                    };
                    self.update_cached_status_snapshot(snapshot.clone())?;
                    Ok(snapshot)
                }
                Err(_) => self.cached_status_snapshot(),
            },
            None => self.cached_status_snapshot(),
        }
    }

    pub async fn scheduled_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        match Self::call_worker(
            &self.client().await?,
            SinkWorkerRequest::ScheduledGroupIds,
            Duration::from_secs(5),
        )
        .await?
        {
            SinkWorkerResponse::ScheduledGroupIds(groups) => {
                Ok(groups.map(|groups| groups.into_iter().collect()))
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for scheduled groups: {:?}",
                other
            ))),
        }
    }

    pub async fn visibility_lag_samples_since(
        &self,
        since_us: u64,
    ) -> Result<Vec<VisibilityLagSample>> {
        match Self::call_worker(
            &self.client().await?,
            SinkWorkerRequest::VisibilityLagSamplesSince { since_us },
            Duration::from_secs(5),
        )
        .await?
        {
            SinkWorkerResponse::VisibilityLagSamples(samples) => Ok(samples),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for visibility lag samples: {:?}",
                other
            ))),
        }
    }

    pub async fn query_node(&self, path: Vec<u8>) -> Result<Option<QueryNode>> {
        self.with_started_retry(|client| {
            let path = path.clone();
            async move {
            let request = InternalQueryRequest::materialized(
                QueryOp::Tree,
                QueryScope {
                    path: path.clone(),
                    recursive: false,
                    max_depth: Some(0),
                    selected_group: None,
                },
                None,
            );
            decode_exact_query_node(
                match Self::call_worker(
                    &client,
                    SinkWorkerRequest::MaterializedQuery { request },
                    SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
                )
                .await?
                {
                    SinkWorkerResponse::Events(events) => events,
                    other => {
                        return Err(CnxError::ProtocolViolation(format!(
                            "unexpected sink worker response for materialized query: {:?}",
                            other
                        )));
                    }
                },
                &path,
            )
            }
        })
        .await
    }

    pub async fn materialized_query(&self, request: InternalQueryRequest) -> Result<Vec<Event>> {
        self.with_started_retry(|client| {
            let request = request.clone();
            async move {
            match Self::call_worker(
                &client,
                SinkWorkerRequest::MaterializedQuery {
                    request: request.clone(),
                },
                SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
            )
            .await?
            {
                SinkWorkerResponse::Events(events) => Ok(events),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker response for materialized query: {:?}",
                    other
                ))),
            }
            }
        })
        .await
    }

    pub async fn materialized_query_nonblocking(
        &self,
        request: InternalQueryRequest,
    ) -> Result<Vec<Event>> {
        match self.existing_client().await? {
            Some(client) => match Self::call_worker(
                &client,
                SinkWorkerRequest::MaterializedQuery { request },
                SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
            )
            .await?
            {
                SinkWorkerResponse::Events(events) => Ok(events),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker response for materialized query: {:?}",
                    other
                ))),
            },
            None => Ok(Vec::new()),
        }
    }

    pub async fn subtree_stats(&self, path: Vec<u8>) -> Result<Vec<Event>> {
        self.with_started_retry(|client| {
            let path = path.clone();
            async move {
            match Self::call_worker(
                &client,
                SinkWorkerRequest::MaterializedQuery {
                    request: InternalQueryRequest::materialized(
                        QueryOp::Stats,
                        QueryScope {
                            path: path.clone(),
                            recursive: true,
                            max_depth: None,
                            selected_group: None,
                        },
                        None,
                    ),
                },
                SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
            )
            .await?
            {
                SinkWorkerResponse::Events(events) => Ok(events),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker response for materialized query: {:?}",
                    other
                ))),
            }
            }
        })
        .await
    }

    pub async fn force_find_proxy(&self, request: InternalQueryRequest) -> Result<Vec<Event>> {
        self.ensure_started().await?;
        eprintln!(
            "fs-meta sink worker proxy: force_find start node={} path={:?} recursive={}",
            self.node_id.0, request.scope.path, request.scope.recursive
        );
        let adapter = exchange_host_adapter_from_channel_boundary(
            self.worker_factory.io_boundary(),
            self.node_id.clone(),
            default_route_bindings(),
        );
        let payload = rmp_serde::to_vec(&request).map_err(|err| {
            CnxError::Internal(format!("sink worker force-find encode failed: {err}"))
        })?;
        let result = adapter
            .call_collect(
                ROUTE_TOKEN_FS_META,
                METHOD_FIND,
                Bytes::from(payload),
                SINK_WORKER_FORCE_FIND_TIMEOUT,
                SINK_WORKER_FORCE_FIND_REPLY_IDLE_GRACE,
            )
            .await;
        eprintln!(
            "fs-meta sink worker proxy: force_find end node={} result={:?}",
            self.node_id.0,
            result
                .as_ref()
                .map(|events| events.len())
                .map_err(|err| err.to_string())
        );
        result
    }

    pub async fn send(&self, events: Vec<Event>) -> Result<()> {
        self.with_started_retry(|client| {
            let events = events.clone();
            async move {
            match Self::call_worker(
                &client,
                SinkWorkerRequest::Send {
                    events: events.clone(),
                },
                Duration::from_secs(5),
            )
            .await?
            {
                SinkWorkerResponse::Ack => Ok(()),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker response for send: {:?}",
                    other
                ))),
            }
            }
        })
        .await
    }

    pub async fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        let timeout_ms = opts.timeout.map(|d| d.as_millis() as u64);
        let limit = opts.limit;
        self.with_started_retry(|client| async move {
            match Self::call_worker(
                &client,
                SinkWorkerRequest::Recv {
                    timeout_ms,
                    limit,
                },
                Duration::from_secs(5),
            )
            .await?
            {
                SinkWorkerResponse::Events(events) => Ok(events),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker response for recv: {:?}",
                    other
                ))),
            }
        })
        .await
    }

    pub async fn on_control_frame(&self, envelopes: Vec<ControlEnvelope>) -> Result<()> {
        eprintln!(
            "fs_meta_sink_worker_client: on_control_frame begin node={} envelopes={}",
            self.node_id.0,
            envelopes.len()
        );
        self.ensure_started().await?;
        let result = match Self::call_worker(
            &self.client().await?,
            SinkWorkerRequest::OnControlFrame { envelopes },
            Duration::from_secs(5),
        )
        .await?
        {
            SinkWorkerResponse::Ack => Ok(()),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for on_control_frame: {:?}",
                other
            ))),
        };
        eprintln!(
            "fs_meta_sink_worker_client: on_control_frame done node={} ok={}",
            self.node_id.0,
            result.is_ok()
        );
        result
    }

    pub async fn close(&self) -> Result<()> {
        self.worker.shutdown(Duration::from_secs(2)).await
    }
}

#[derive(Clone)]
pub enum SinkFacade {
    Local(Arc<SinkFileMeta>),
    Worker(Arc<SinkWorkerClientHandle>),
}

impl SinkFacade {
    pub fn local(sink: Arc<SinkFileMeta>) -> Self {
        Self::Local(sink)
    }

    pub fn worker(client: Arc<SinkWorkerClientHandle>) -> Self {
        Self::Worker(client)
    }

    pub fn is_worker(&self) -> bool {
        matches!(self, Self::Worker(_))
    }

    pub async fn ensure_started(&self) -> Result<()> {
        match self {
            Self::Local(_) => Ok(()),
            Self::Worker(client) => client.ensure_started().await,
        }
    }

    pub fn start_stream_endpoint(
        &self,
        boundary: Arc<dyn ChannelIoSubset>,
        node_id: NodeId,
    ) -> Result<()> {
        match self {
            Self::Local(sink) => sink.start_stream_endpoint(boundary, node_id),
            Self::Worker(_) => Ok(()),
        }
    }

    pub async fn update_logical_roots(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
        host_object_grants: &[GrantedMountRoot],
    ) -> Result<()> {
        match self {
            Self::Local(sink) => sink.update_logical_roots(roots, host_object_grants),
            Self::Worker(client) => {
                client
                    .update_logical_roots(roots, host_object_grants.to_vec())
                    .await
            }
        }
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        match self {
            Self::Local(sink) => sink.logical_roots_snapshot(),
            Self::Worker(client) => client.cached_logical_roots_snapshot(),
        }
    }

    pub async fn logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        match self {
            Self::Local(sink) => sink.logical_roots_snapshot(),
            Self::Worker(client) => client.logical_roots_snapshot().await,
        }
    }

    pub async fn health(&self) -> Result<HealthStats> {
        match self {
            Self::Local(sink) => sink.health(),
            Self::Worker(client) => client.health().await,
        }
    }

    pub async fn status_snapshot(&self) -> Result<SinkStatusSnapshot> {
        match self {
            Self::Local(sink) => sink.status_snapshot(),
            Self::Worker(client) => client.status_snapshot().await,
        }
    }

    pub async fn status_snapshot_nonblocking(&self) -> Result<SinkStatusSnapshot> {
        match self {
            Self::Local(sink) => sink.status_snapshot(),
            Self::Worker(client) => client.status_snapshot_nonblocking().await,
        }
    }

    pub async fn scheduled_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self {
            Self::Local(sink) => sink.scheduled_group_ids_snapshot(),
            Self::Worker(client) => client.scheduled_group_ids().await,
        }
    }

    pub async fn shadow_time_us(&self) -> Result<u64> {
        match self {
            Self::Local(sink) => sink.shadow_time_us(),
            Self::Worker(client) => client.health().await.map(|stats| stats.shadow_time_us),
        }
    }

    pub async fn visibility_lag_samples_since(&self, since_us: u64) -> Vec<VisibilityLagSample> {
        match self {
            Self::Local(sink) => sink.visibility_lag_samples_since(since_us),
            Self::Worker(client) => client
                .visibility_lag_samples_since(since_us)
                .await
                .unwrap_or_default(),
        }
    }

    pub async fn query_node(&self, path: &[u8]) -> Result<Option<QueryNode>> {
        match self {
            Self::Local(sink) => {
                let request = InternalQueryRequest::materialized(
                    QueryOp::Tree,
                    QueryScope {
                        path: path.to_vec(),
                        recursive: false,
                        max_depth: Some(0),
                        selected_group: None,
                    },
                    None,
                );
                decode_exact_query_node(sink.materialized_query(&request)?, path)
            }
            Self::Worker(client) => client.query_node(path.to_vec()).await,
        }
    }

    pub async fn materialized_query(&self, request: &InternalQueryRequest) -> Result<Vec<Event>> {
        match self {
            Self::Local(sink) => sink.materialized_query(request),
            Self::Worker(client) => client.materialized_query(request.clone()).await,
        }
    }

    pub async fn materialized_query_nonblocking(
        &self,
        request: &InternalQueryRequest,
    ) -> Result<Vec<Event>> {
        match self {
            Self::Local(sink) => sink.materialized_query(request),
            Self::Worker(client) => client.materialized_query_nonblocking(request.clone()).await,
        }
    }

    pub async fn materialized_query_via_node(
        &self,
        node_id: &NodeId,
        request: &InternalQueryRequest,
    ) -> Result<Vec<Event>> {
        match self {
            Self::Local(sink) => sink.materialized_query(request),
            Self::Worker(client) => {
                if client.node_id == *node_id {
                    client.materialized_query(request.clone()).await
                } else {
                    let remote = SinkWorkerClientHandle::new(
                        node_id.clone(),
                        client.config.clone(),
                        client.worker_binding.clone(),
                        client.worker_factory.clone(),
                    )?;
                    remote.materialized_query(request.clone()).await
                }
            }
        }
    }

    pub async fn subtree_stats(&self, path: &[u8]) -> Result<Vec<Event>> {
        match self {
            Self::Local(sink) => sink.materialized_query(&InternalQueryRequest::materialized(
                QueryOp::Stats,
                QueryScope {
                    path: path.to_vec(),
                    recursive: true,
                    max_depth: None,
                    selected_group: None,
                },
                None,
            )),
            Self::Worker(client) => client.subtree_stats(path.to_vec()).await,
        }
    }

    pub async fn force_find_proxy(&self, request: &InternalQueryRequest) -> Result<Vec<Event>> {
        match self {
            Self::Local(_) => Err(CnxError::InvalidInput(
                "force-find proxy requires sink worker execution mode".into(),
            )),
            Self::Worker(client) => client.force_find_proxy(request.clone()).await,
        }
    }

    pub async fn send(&self, events: &[Event]) -> Result<()> {
        match self {
            Self::Local(sink) => sink.send(events).await,
            Self::Worker(client) => client.send(events.to_vec()).await,
        }
    }

    pub async fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        match self {
            Self::Local(sink) => sink.recv(opts).await,
            Self::Worker(client) => client.recv(opts).await,
        }
    }

    pub async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        let signals = sink_control_signals_from_envelopes(envelopes)?;
        self.apply_orchestration_signals(&signals).await
    }

    pub(crate) async fn apply_orchestration_signals(
        &self,
        signals: &[SinkControlSignal],
    ) -> Result<()> {
        match self {
            Self::Local(sink) => sink.apply_orchestration_signals(signals).await,
            Self::Worker(client) => {
                let envelopes = signals
                    .iter()
                    .map(SinkControlSignal::envelope)
                    .collect::<Vec<_>>();
                client.on_control_frame(envelopes).await
            }
        }
    }

    pub async fn close(&self) -> Result<()> {
        match self {
            Self::Local(sink) => sink.close().await,
            Self::Worker(client) => client.close().await,
        }
    }
}

capanix_runtime_entry_sdk::worker_runtime::define_typed_worker_rpc! {
    pub struct SinkWorkerRpc {
        request: SinkWorkerRequest,
        response: SinkWorkerResponse,
        encode_request: encode_request,
        decode_request: decode_request,
        encode_response: encode_response,
        decode_response: decode_response,
        invalid_input: SinkWorkerResponse::InvalidInput,
        error: SinkWorkerResponse::Error,
        unavailable: "sink worker unavailable",
    }
}

impl TypedWorkerInit<SourceConfig> for SinkWorkerRpc {
    type InitPayload = SinkWorkerInitConfig;

    fn init_payload(_node_id: &NodeId, config: &SourceConfig) -> Result<Self::InitPayload> {
        Ok(SinkWorkerInitConfig {
            roots: config.roots.clone(),
            host_object_grants: config.host_object_grants.clone(),
            sink_tombstone_ttl_ms: config.sink_tombstone_ttl.as_millis() as u64,
            sink_tombstone_tolerance_us: config.sink_tombstone_tolerance_us,
        })
    }
}
