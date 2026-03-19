use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use capanix_app_fs_meta_runtime_support::{
    TypedWorkerClient, TypedWorkerRpc, encode_control_frame,
};
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::{ControlEnvelope, NodeId, RecvOpts};
use capanix_app_sdk::{CnxError, Event, Result, RuntimeBoundary, RuntimeBoundaryApp};
use capanix_host_adapter_fs_meta::HostAdapter;

use crate::query::models::{HealthStats, QueryNode};
use crate::query::path::root_file_name_bytes;
use crate::query::request::{InternalQueryRequest, MaterializedQueryPayload, QueryOp, QueryScope};
use crate::runtime::orchestration::{SinkControlSignal, sink_control_signals_from_envelopes};
use crate::runtime::routes::{METHOD_FIND, ROUTE_TOKEN_FS_META, default_route_bindings};
use crate::runtime::seam::exchange_host_adapter;
use crate::sink::{SinkFileMeta, SinkStatusSnapshot, VisibilityLagSample};
use crate::source::config::{GrantedMountRoot, SinkExecutionMode, SourceConfig};
use crate::workers::sink_ipc::{
    SINK_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND, SinkWorkerInitConfig, SinkWorkerRequest,
    SinkWorkerResponse, decode_response, encode_request, sink_worker_control_route_key_for,
};

const SINK_WORKER_INIT_RPC_TIMEOUT: Duration = Duration::from_secs(60);
const SINK_WORKER_INIT_TOTAL_TIMEOUT: Duration = Duration::from_secs(120);
const SINK_WORKER_START_RPC_TIMEOUT: Duration = Duration::from_secs(60);
const SINK_WORKER_START_TOTAL_TIMEOUT: Duration = Duration::from_secs(120);
const SINK_WORKER_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(15);
const SINK_WORKER_UPDATE_ROOTS_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const SINK_WORKER_FORCE_FIND_TIMEOUT: Duration = Duration::from_secs(60);
const SINK_WORKER_FORCE_FIND_REPLY_IDLE_GRACE: Duration = Duration::from_secs(5);
const SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT: Duration = Duration::from_secs(25);

fn is_sink_worker_not_initialized(err: &CnxError) -> bool {
    matches!(err, CnxError::PeerError(message) if message == "sink worker not initialized")
}

fn sink_worker_bootstrap_frame(request: &SinkWorkerRequest) -> Result<ControlEnvelope> {
    Ok(encode_control_frame(
        SINK_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND,
        encode_request(request)?,
    ))
}

#[derive(Default)]
struct SinkWorkerHandleState {
    client: Arc<Mutex<Option<Arc<SinkWorkerClient>>>>,
    spawn_guard: Arc<Mutex<()>>,
    status_cache: Arc<Mutex<SinkStatusSnapshot>>,
}

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
    boundary: Arc<crate::runtime_app::RuntimeBoundaryPair>,
    client: Arc<Mutex<Option<Arc<SinkWorkerClient>>>>,
    spawn_guard: Arc<Mutex<()>>,
    logical_roots_cache: Arc<Mutex<Vec<crate::source::config::RootSpec>>>,
    status_cache: Arc<Mutex<SinkStatusSnapshot>>,
}

impl SinkWorkerClientHandle {
    pub(crate) fn new(
        node_id: NodeId,
        config: SourceConfig,
        boundary: Arc<crate::runtime_app::RuntimeBoundaryPair>,
    ) -> Self {
        let shared = Arc::new(SinkWorkerHandleState::default());
        let logical_roots_cache = Arc::new(Mutex::new(config.roots.clone()));
        Self {
            node_id,
            config,
            boundary,
            client: shared.client.clone(),
            spawn_guard: shared.spawn_guard.clone(),
            logical_roots_cache,
            status_cache: shared.status_cache.clone(),
        }
    }

    fn existing_client(&self) -> Result<Option<Arc<SinkWorkerClient>>> {
        self.client
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| CnxError::Internal("sink worker handle lock poisoned".into()))
    }

    fn reset_client(&self) -> Result<()> {
        let client = {
            let mut guard = self
                .client
                .lock()
                .map_err(|_| CnxError::Internal("sink worker handle lock poisoned".into()))?;
            guard.take()
        };
        if let Some(client) = client {
            eprintln!(
                "fs_meta_runtime_app: resetting sink worker client for {}",
                self.node_id.0
            );
            let _ = client.close();
        }
        Ok(())
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

    fn with_retry<T>(&self, op: impl Fn(&SinkWorkerClient) -> Result<T>) -> Result<T> {
        let deadline = std::time::Instant::now() + SINK_WORKER_START_TOTAL_TIMEOUT;
        loop {
            let client = self.client()?;
            match op(client.as_ref()) {
                Err(CnxError::TransportClosed(_)) | Err(CnxError::Timeout)
                    if std::time::Instant::now() < deadline =>
                {
                    self.reset_client()?;
                    let restarted = self.client()?;
                    restarted.ensure_started(&self.node_id, &self.config)?;
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(err)
                    if is_sink_worker_not_initialized(&err)
                        && std::time::Instant::now() < deadline =>
                {
                    client.note_not_initialized();
                    client.ensure_started(&self.node_id, &self.config)?;
                    std::thread::sleep(Duration::from_millis(100));
                }
                other => return other,
            }
        }
    }

    fn with_started_retry<T>(&self, op: impl Fn(&SinkWorkerClient) -> Result<T>) -> Result<T> {
        self.ensure_started()?;
        self.with_retry(op)
    }

    fn client(&self) -> Result<Arc<SinkWorkerClient>> {
        if let Some(client) = self.existing_client()? {
            return Ok(client);
        }

        let _spawn_guard = self
            .spawn_guard
            .lock()
            .map_err(|_| CnxError::Internal("sink worker spawn guard lock poisoned".into()))?;

        if let Some(client) = self.existing_client()? {
            return Ok(client);
        }

        let spawned = Arc::new(SinkWorkerClient::spawn(
            &self.node_id,
            &self.config,
            self.boundary.clone(),
        )?);
        eprintln!(
            "fs_meta_runtime_app: sink worker spawn backtrace\n{:?}",
            std::backtrace::Backtrace::capture()
        );
        let client = {
            let mut guard = self
                .client
                .lock()
                .map_err(|_| CnxError::Internal("sink worker handle lock poisoned".into()))?;
            if let Some(existing) = guard.as_ref() {
                existing.clone()
            } else {
                *guard = Some(spawned.clone());
                spawned
            }
        };
        Ok(client)
    }

    pub fn ensure_started(&self) -> Result<()> {
        self.with_retry(|client| client.ensure_started(&self.node_id, &self.config))
    }

    pub fn update_logical_roots(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
        host_object_grants: Vec<GrantedMountRoot>,
    ) -> Result<()> {
        self.with_started_retry(|client| {
            client.update_logical_roots(roots.clone(), host_object_grants.clone())
        })?;
        self.update_cached_logical_roots(roots)
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        self.cached_logical_roots()
    }

    pub fn logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        let roots = self.client()?.logical_roots_snapshot()?;
        self.update_cached_logical_roots(roots.clone())?;
        Ok(roots)
    }

    pub fn health(&self) -> Result<HealthStats> {
        self.client()?.health()
    }

    pub fn status_snapshot(&self) -> Result<SinkStatusSnapshot> {
        let snapshot = self.client()?.status_snapshot()?;
        self.update_cached_status_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }

    pub fn status_snapshot_nonblocking(&self) -> Result<SinkStatusSnapshot> {
        match self.existing_client()? {
            Some(client) => match client.status_snapshot() {
                Ok(snapshot) => {
                    self.update_cached_status_snapshot(snapshot.clone())?;
                    Ok(snapshot)
                }
                Err(_) => self.cached_status_snapshot(),
            },
            None => self.cached_status_snapshot(),
        }
    }

    pub fn scheduled_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.client()?.scheduled_group_ids()
    }

    pub fn visibility_lag_samples_since(&self, since_us: u64) -> Result<Vec<VisibilityLagSample>> {
        self.client()?.visibility_lag_samples_since(since_us)
    }

    pub fn query_node(&self, path: Vec<u8>) -> Result<Option<QueryNode>> {
        self.with_started_retry(|client| client.query_node(path.clone()))
    }

    pub fn materialized_query(&self, request: InternalQueryRequest) -> Result<Vec<Event>> {
        self.with_started_retry(|client| client.materialized_query(request.clone()))
    }

    pub fn materialized_query_nonblocking(&self, request: InternalQueryRequest) -> Result<Vec<Event>> {
        match self.existing_client()? {
            Some(client) => client.materialized_query(request),
            None => Ok(Vec::new()),
        }
    }

    pub fn subtree_stats(&self, path: Vec<u8>) -> Result<Vec<Event>> {
        self.with_started_retry(|client| client.subtree_stats(path.clone()))
    }

    pub fn force_find_proxy(&self, request: InternalQueryRequest) -> Result<Vec<Event>> {
        self.ensure_started()?;
        eprintln!(
            "fs-meta sink worker proxy: force_find start node={} path={:?} recursive={}",
            self.node_id.0, request.scope.path, request.scope.recursive
        );
        let data_boundary: Arc<dyn ChannelIoSubset> = self.boundary.clone();
        let adapter = exchange_host_adapter(
            data_boundary,
            self.node_id.clone(),
            default_route_bindings(),
        );
        let payload = rmp_serde::to_vec(&request).map_err(|err| {
            CnxError::Internal(format!("sink worker force-find encode failed: {err}"))
        })?;
        let result = adapter.call_collect(
            ROUTE_TOKEN_FS_META,
            METHOD_FIND,
            Bytes::from(payload),
            SINK_WORKER_FORCE_FIND_TIMEOUT,
            SINK_WORKER_FORCE_FIND_REPLY_IDLE_GRACE,
        );
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

    pub fn send(&self, events: Vec<Event>) -> Result<()> {
        self.with_started_retry(|client| client.send(events.clone()))
    }

    pub fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        self.with_started_retry(|client| client.recv(opts.clone()))
    }

    pub fn on_control_frame(&self, envelopes: Vec<ControlEnvelope>) -> Result<()> {
        self.ensure_started()?;
        self.client()?.on_control_frame(envelopes)
    }

    pub fn close(&self) -> Result<()> {
        self.reset_client()
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

    pub fn ensure_started(&self) -> Result<()> {
        match self {
            Self::Local(_) => Ok(()),
            Self::Worker(client) => client.ensure_started(),
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

    pub fn update_logical_roots(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
        host_object_grants: &[GrantedMountRoot],
    ) -> Result<()> {
        match self {
            Self::Local(sink) => sink.update_logical_roots(roots, host_object_grants),
            Self::Worker(client) => client.update_logical_roots(roots, host_object_grants.to_vec()),
        }
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        match self {
            Self::Local(sink) => sink.logical_roots_snapshot(),
            Self::Worker(client) => client.cached_logical_roots_snapshot(),
        }
    }

    pub fn logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        match self {
            Self::Local(sink) => sink.logical_roots_snapshot(),
            Self::Worker(client) => client.logical_roots_snapshot(),
        }
    }

    pub fn health(&self) -> Result<HealthStats> {
        match self {
            Self::Local(sink) => sink.health(),
            Self::Worker(client) => client.health(),
        }
    }

    pub fn status_snapshot(&self) -> Result<SinkStatusSnapshot> {
        match self {
            Self::Local(sink) => sink.status_snapshot(),
            Self::Worker(client) => client.status_snapshot(),
        }
    }

    pub fn status_snapshot_nonblocking(&self) -> Result<SinkStatusSnapshot> {
        match self {
            Self::Local(sink) => sink.status_snapshot(),
            Self::Worker(client) => client.status_snapshot_nonblocking(),
        }
    }

    pub fn scheduled_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self {
            Self::Local(sink) => sink.scheduled_group_ids_snapshot(),
            Self::Worker(client) => client.scheduled_group_ids(),
        }
    }

    pub fn shadow_time_us(&self) -> Result<u64> {
        match self {
            Self::Local(sink) => sink.shadow_time_us(),
            Self::Worker(client) => client.health().map(|stats| stats.shadow_time_us),
        }
    }

    pub fn visibility_lag_samples_since(&self, since_us: u64) -> Vec<VisibilityLagSample> {
        match self {
            Self::Local(sink) => sink.visibility_lag_samples_since(since_us),
            Self::Worker(client) => client
                .visibility_lag_samples_since(since_us)
                .unwrap_or_default(),
        }
    }

    pub fn query_node(&self, path: &[u8]) -> Result<Option<QueryNode>> {
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
            Self::Worker(client) => client.query_node(path.to_vec()),
        }
    }

    pub fn materialized_query(&self, request: &InternalQueryRequest) -> Result<Vec<Event>> {
        match self {
            Self::Local(sink) => sink.materialized_query(request),
            Self::Worker(client) => client.materialized_query(request.clone()),
        }
    }

    pub fn materialized_query_nonblocking(&self, request: &InternalQueryRequest) -> Result<Vec<Event>> {
        match self {
            Self::Local(sink) => sink.materialized_query(request),
            Self::Worker(client) => client.materialized_query_nonblocking(request.clone()),
        }
    }

    pub fn materialized_query_via_node(
        &self,
        node_id: &NodeId,
        request: &InternalQueryRequest,
    ) -> Result<Vec<Event>> {
        match self {
            Self::Local(sink) => sink.materialized_query(request),
            Self::Worker(client) => {
                if client.node_id == *node_id {
                    client.materialized_query(request.clone())
                } else {
                    let remote = SinkWorkerClientHandle::new(
                        node_id.clone(),
                        client.config.clone(),
                        client.boundary.clone(),
                    );
                    remote.materialized_query(request.clone())
                }
            }
        }
    }

    pub fn subtree_stats(&self, path: &[u8]) -> Result<Vec<Event>> {
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
            Self::Worker(client) => client.subtree_stats(path.to_vec()),
        }
    }

    pub fn force_find_proxy(&self, request: &InternalQueryRequest) -> Result<Vec<Event>> {
        match self {
            Self::Local(_) => Err(CnxError::InvalidInput(
                "force-find proxy requires sink worker execution mode".into(),
            )),
            Self::Worker(client) => client.force_find_proxy(request.clone()),
        }
    }

    pub async fn send(&self, events: &[Event]) -> Result<()> {
        match self {
            Self::Local(sink) => sink.send(events).await,
            Self::Worker(client) => client.send(events.to_vec()),
        }
    }

    pub async fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        match self {
            Self::Local(sink) => sink.recv(opts).await,
            Self::Worker(client) => client.recv(opts),
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
                client.on_control_frame(envelopes)
            }
        }
    }

    pub async fn close(&self) -> Result<()> {
        match self {
            Self::Local(sink) => sink.close().await,
            Self::Worker(client) => client.close(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkBootstrapState {
    Connected,
    Initialized,
    Started,
}

pub struct SinkWorkerClient {
    conn: TypedWorkerClient<SinkWorkerRpc>,
    bootstrap_state: Mutex<SinkBootstrapState>,
}

struct SinkWorkerRpc;

impl TypedWorkerRpc for SinkWorkerRpc {
    type Request = SinkWorkerRequest;
    type Response = SinkWorkerResponse;

    fn encode_request(request: &Self::Request) -> Result<Vec<u8>> {
        encode_request(request)
    }

    fn decode_response(payload: &[u8]) -> Result<Self::Response> {
        decode_response(payload)
    }

    fn into_result(response: Self::Response) -> Result<Self::Response> {
        match response {
            SinkWorkerResponse::InvalidInput(message) => Err(CnxError::InvalidInput(message)),
            SinkWorkerResponse::Error(message) => Err(CnxError::PeerError(message)),
            other => Ok(other),
        }
    }

    fn unavailable_label() -> &'static str {
        "sink worker unavailable"
    }
}

impl SinkWorkerClient {
    pub fn spawn<T>(node_id: &NodeId, config: &SourceConfig, boundary: Arc<T>) -> Result<Self>
    where
        T: RuntimeBoundary + ChannelIoSubset + 'static,
    {
        if config.sink_execution_mode != SinkExecutionMode::WorkerProcess {
            return Err(CnxError::InvalidInput(
                "sink worker client requires sink_execution_mode=worker-process".into(),
            ));
        }
        let bin_path = config
            .sink_worker_bin_path
            .as_ref()
            .ok_or_else(|| {
                CnxError::InvalidInput(
                    "sink_worker_bin_path is required when sink_execution_mode=worker-process"
                        .into(),
                )
            })?
            .clone();
        let socket_dir = config
            .sink_worker_socket_dir
            .clone()
            .unwrap_or_else(std::env::temp_dir);
        let route_key = sink_worker_control_route_key_for(&node_id.0);
        eprintln!(
            "fs_meta_runtime_app: spawning sink worker client for {} bin={} socket_dir={}",
            node_id.0,
            bin_path.display(),
            socket_dir.display()
        );
        let conn = TypedWorkerClient::spawn(
            node_id,
            capanix_app_sdk::runtime::RouteKey(route_key.clone()),
            &bin_path,
            &socket_dir,
            "fs-meta-sink-worker",
            boundary,
        )?;

        Ok(Self {
            conn,
            bootstrap_state: Mutex::new(SinkBootstrapState::Connected),
        })
    }

    fn is_started(&self) -> bool {
        match self.bootstrap_state.lock() {
            Ok(state) => matches!(*state, SinkBootstrapState::Started),
            Err(poisoned) => matches!(*poisoned.into_inner(), SinkBootstrapState::Started),
        }
    }

    fn note_not_initialized(&self) {
        match self.bootstrap_state.lock() {
            Ok(mut state) => {
                if matches!(*state, SinkBootstrapState::Started) {
                    *state = SinkBootstrapState::Initialized;
                }
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                if matches!(*state, SinkBootstrapState::Started) {
                    *state = SinkBootstrapState::Initialized;
                }
            }
        }
    }

    fn ping(&self) -> Result<()> {
        self.conn.control_frames(
            &[sink_worker_bootstrap_frame(&SinkWorkerRequest::Ping)?],
            SINK_WORKER_CONTROL_RPC_TIMEOUT,
        )
    }

    pub fn ensure_started(&self, node_id: &NodeId, config: &SourceConfig) -> Result<()> {
        let mut state = self
            .bootstrap_state
            .lock()
            .map_err(|_| CnxError::Internal("sink worker bootstrap state lock poisoned".into()))?;
        let route_key = sink_worker_control_route_key_for(&node_id.0);

        if matches!(*state, SinkBootstrapState::Started) {
            return Ok(());
        }

        if matches!(*state, SinkBootstrapState::Connected) {
            let init = SinkWorkerInitConfig {
                roots: config.roots.clone(),
                host_object_grants: config.host_object_grants.clone(),
                sink_tombstone_ttl_ms: config.sink_tombstone_ttl.as_millis() as u64,
                sink_tombstone_tolerance_us: config.sink_tombstone_tolerance_us,
            };
            eprintln!(
                "fs-meta sink worker: sending Init route_key={} roots={} grants={}",
                route_key,
                init.roots.len(),
                init.host_object_grants.len()
            );
            self.conn.retry_control_until(
                || {
                    Ok(vec![sink_worker_bootstrap_frame(
                        &SinkWorkerRequest::Init {
                            node_id: node_id.0.clone(),
                            config: init.clone(),
                        },
                    )?])
                },
                SINK_WORKER_INIT_RPC_TIMEOUT,
                SINK_WORKER_INIT_TOTAL_TIMEOUT,
            )?;
            *state = SinkBootstrapState::Initialized;
        }

        if matches!(*state, SinkBootstrapState::Initialized) {
            eprintln!("fs_meta_runtime_app: sending sink Start for {}", node_id.0);
            self.conn.retry_control_until(
                || {
                    Ok(vec![sink_worker_bootstrap_frame(
                        &SinkWorkerRequest::Start,
                    )?])
                },
                SINK_WORKER_START_RPC_TIMEOUT,
                SINK_WORKER_START_TOTAL_TIMEOUT,
            )?;
            self.ping()?;
            *state = SinkBootstrapState::Started;
            return Ok(());
        }

        Ok(())
    }

    pub fn update_logical_roots(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
        host_object_grants: Vec<GrantedMountRoot>,
    ) -> Result<()> {
        match self.conn.call_with_timeout(
            SinkWorkerRequest::UpdateLogicalRoots {
                roots,
                host_object_grants,
            },
            SINK_WORKER_UPDATE_ROOTS_RPC_TIMEOUT,
        )? {
            SinkWorkerResponse::Ack => Ok(()),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for update roots: {:?}",
                other
            ))),
        }
    }

    pub fn logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        match self.conn.call_with_timeout(
            SinkWorkerRequest::LogicalRootsSnapshot,
            Duration::from_secs(5),
        )? {
            SinkWorkerResponse::LogicalRoots(roots) => Ok(roots),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for logical roots: {:?}",
                other
            ))),
        }
    }

    pub fn health(&self) -> Result<HealthStats> {
        match self
            .conn
            .call_with_timeout(SinkWorkerRequest::Health, Duration::from_secs(5))?
        {
            SinkWorkerResponse::Health(health) => Ok(health),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for health: {:?}",
                other
            ))),
        }
    }

    pub fn status_snapshot(&self) -> Result<SinkStatusSnapshot> {
        match self
            .conn
            .call_with_timeout(SinkWorkerRequest::StatusSnapshot, Duration::from_secs(5))?
        {
            SinkWorkerResponse::StatusSnapshot(snapshot) => Ok(snapshot),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for status snapshot: {:?}",
                other
            ))),
        }
    }

    pub fn scheduled_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self
            .conn
            .call_with_timeout(SinkWorkerRequest::ScheduledGroupIds, Duration::from_secs(5))?
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

    pub fn visibility_lag_samples_since(&self, since_us: u64) -> Result<Vec<VisibilityLagSample>> {
        match self.conn.call_with_timeout(
            SinkWorkerRequest::VisibilityLagSamplesSince { since_us },
            Duration::from_secs(5),
        )? {
            SinkWorkerResponse::VisibilityLagSamples(samples) => Ok(samples),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for visibility lag samples: {:?}",
                other
            ))),
        }
    }

    pub fn query_node(&self, path: Vec<u8>) -> Result<Option<QueryNode>> {
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
        decode_exact_query_node(self.materialized_query(request)?, &path)
    }

    pub fn materialized_query(&self, request: InternalQueryRequest) -> Result<Vec<Event>> {
        match self.conn.call_with_timeout(
            SinkWorkerRequest::MaterializedQuery { request },
            SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
        )? {
            SinkWorkerResponse::Events(events) => Ok(events),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for materialized query: {:?}",
                other
            ))),
        }
    }

    pub fn subtree_stats(&self, path: Vec<u8>) -> Result<Vec<Event>> {
        self.materialized_query(InternalQueryRequest::materialized(
            QueryOp::Stats,
            QueryScope {
                path,
                recursive: true,
                max_depth: None,
                selected_group: None,
            },
            None,
        ))
    }

    pub fn send(&self, events: Vec<Event>) -> Result<()> {
        match self
            .conn
            .call_with_timeout(SinkWorkerRequest::Send { events }, Duration::from_secs(5))?
        {
            SinkWorkerResponse::Ack => Ok(()),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for send: {:?}",
                other
            ))),
        }
    }

    pub fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        match self.conn.call_with_timeout(
            SinkWorkerRequest::Recv {
                timeout_ms: opts.timeout.map(|d| d.as_millis() as u64),
                limit: opts.limit,
            },
            Duration::from_secs(5),
        )? {
            SinkWorkerResponse::Events(events) => Ok(events),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for recv: {:?}",
                other
            ))),
        }
    }

    pub fn on_control_frame(&self, envelopes: Vec<ControlEnvelope>) -> Result<()> {
        self.conn.control_frames(
            &[sink_worker_bootstrap_frame(
                &SinkWorkerRequest::OnControlFrame { envelopes },
            )?],
            Duration::from_secs(5),
        )
    }

    pub fn close(&self) -> Result<()> {
        self.conn.control_frames(
            &[sink_worker_bootstrap_frame(&SinkWorkerRequest::Close)?],
            Duration::from_secs(2),
        )?;
        self.conn.shutdown()
    }
}
