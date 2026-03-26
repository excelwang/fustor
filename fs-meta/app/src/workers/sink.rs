use std::sync::atomic::{AtomicUsize, Ordering};
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

fn debug_control_scope_capture_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
}

fn summarize_bound_scopes(
    bound_scopes: &[capanix_runtime_entry_sdk::control::RuntimeBoundScope],
) -> Vec<String> {
    bound_scopes
        .iter()
        .map(|scope| format!("{}=>{}", scope.scope_id, scope.resource_ids.join("|")))
        .collect()
}

fn summarize_sink_control_signals(signals: &[SinkControlSignal]) -> Vec<String> {
    signals
        .iter()
        .map(|signal| match signal {
            SinkControlSignal::Activate {
                unit,
                route_key,
                generation,
                bound_scopes,
                ..
            } => format!(
                "activate unit={} route={} generation={} scopes={:?}",
                unit.unit_id(),
                route_key,
                generation,
                summarize_bound_scopes(bound_scopes)
            ),
            SinkControlSignal::Deactivate {
                unit,
                route_key,
                generation,
                ..
            } => format!(
                "deactivate unit={} route={} generation={}",
                unit.unit_id(),
                route_key,
                generation
            ),
            SinkControlSignal::Tick {
                unit,
                route_key,
                generation,
                ..
            } => format!(
                "tick unit={} route={} generation={}",
                unit.unit_id(),
                route_key,
                generation
            ),
            SinkControlSignal::RuntimeHostGrantChange { .. } => "host_grant_change".into(),
            SinkControlSignal::Passthrough(_) => "passthrough".into(),
        })
        .collect()
}

fn summarize_groups_by_node(
    groups: &std::collections::BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    groups
        .iter()
        .map(|(node_id, groups)| format!("{node_id}={}", groups.join("|")))
        .collect()
}

fn summarize_sink_status_snapshot(snapshot: &SinkStatusSnapshot) -> String {
    format!(
        "groups={} scheduled={:?} control={:?}",
        snapshot.groups.len(),
        summarize_groups_by_node(&snapshot.scheduled_groups_by_node),
        summarize_groups_by_node(&snapshot.last_control_frame_signals_by_node)
    )
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
    worker_factory: RuntimeWorkerClientFactory,
    worker: TypedRuntimeWorkerClient<SinkWorkerRpc, SourceConfig>,
    logical_roots_cache: Arc<Mutex<Vec<crate::source::config::RootSpec>>>,
    status_cache: Arc<Mutex<SinkStatusSnapshot>>,
    control_ops_inflight: Arc<AtomicUsize>,
}

struct InflightControlOpGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for InflightControlOpGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
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
            worker_factory,
            logical_roots_cache,
            status_cache: Arc::new(Mutex::new(SinkStatusSnapshot::default())),
            control_ops_inflight: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn begin_control_op(&self) -> InflightControlOpGuard {
        self.control_ops_inflight.fetch_add(1, Ordering::Relaxed);
        InflightControlOpGuard {
            counter: self.control_ops_inflight.clone(),
        }
    }

    fn control_op_inflight(&self) -> bool {
        self.control_ops_inflight.load(Ordering::Relaxed) > 0
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
        let _inflight = self.begin_control_op();
        eprintln!(
            "fs_meta_sink_worker_client: update_logical_roots begin node={} roots={} grants={}",
            self.node_id.0,
            roots.len(),
            host_object_grants.len()
        );
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
                    SinkWorkerResponse::Ack => {
                        eprintln!(
                            "fs_meta_sink_worker_client: update_logical_roots ok node={} roots={} grants={}",
                            self.node_id.0,
                            roots.len(),
                            host_object_grants.len()
                        );
                        Ok(())
                    }
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
        if debug_control_scope_capture_enabled() {
            eprintln!(
                "fs_meta_sink_worker_client: status_snapshot reply node={} {}",
                self.node_id.0,
                summarize_sink_status_snapshot(&snapshot)
            );
        }
        self.update_cached_status_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }

    pub async fn status_snapshot_nonblocking(&self) -> Result<SinkStatusSnapshot> {
        if self.control_op_inflight() {
            let snapshot = self.cached_status_snapshot()?;
            if debug_control_scope_capture_enabled() {
                eprintln!(
                    "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason=control_inflight {}",
                    self.node_id.0,
                    summarize_sink_status_snapshot(&snapshot)
                );
            }
            return Ok(snapshot);
        }
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
                    if debug_control_scope_capture_enabled() {
                        eprintln!(
                            "fs_meta_sink_worker_client: status_snapshot reply node={} {}",
                            self.node_id.0,
                            summarize_sink_status_snapshot(&snapshot)
                        );
                    }
                    self.update_cached_status_snapshot(snapshot.clone())?;
                    Ok(snapshot)
                }
                Err(err) => {
                    let snapshot = self.cached_status_snapshot()?;
                    if debug_control_scope_capture_enabled() {
                        eprintln!(
                            "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason=worker_unavailable err={} {}",
                            self.node_id.0,
                            err,
                            summarize_sink_status_snapshot(&snapshot)
                        );
                    }
                    Ok(snapshot)
                }
            },
            None => {
                let snapshot = self.cached_status_snapshot()?;
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason=not_started {}",
                        self.node_id.0,
                        summarize_sink_status_snapshot(&snapshot)
                    );
                }
                Ok(snapshot)
            }
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
                SinkWorkerRequest::Recv { timeout_ms, limit },
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
        let _inflight = self.begin_control_op();
        eprintln!(
            "fs_meta_sink_worker_client: on_control_frame begin node={} envelopes={}",
            self.node_id.0,
            envelopes.len()
        );
        if debug_control_scope_capture_enabled() {
            match sink_control_signals_from_envelopes(&envelopes) {
                Ok(signals) => eprintln!(
                    "fs_meta_sink_worker_client: on_control_frame summary node={} signals={:?}",
                    self.node_id.0,
                    summarize_sink_control_signals(&signals)
                ),
                Err(err) => eprintln!(
                    "fs_meta_sink_worker_client: on_control_frame summary node={} decode_err={}",
                    self.node_id.0, err
                ),
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use capanix_app_sdk::raw::{
        BoundaryContext, ChannelBoundary, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
        StateBoundary,
    };
    use capanix_app_sdk::runtime::{
        LogLevel, RuntimeWorkerBinding, RuntimeWorkerLauncherKind, in_memory_state_boundary,
    };
    use capanix_app_sdk::worker::WorkerMode;
    use capanix_runtime_entry_sdk::control::{
        RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, encode_runtime_exec_control,
    };
    use futures_util::StreamExt;
    use std::collections::{HashMap, HashSet};
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex as StdMutex, OnceLock};
    use tempfile::tempdir;
    use tokio::sync::{Mutex as AsyncMutex, Notify};
    use tokio::time::Duration;

    use crate::source::FSMetaSource;
    use crate::source::config::RootSpec;
    use crate::runtime::routes::ROUTE_KEY_QUERY;

    #[derive(Default)]
    struct LoopbackWorkerBoundary {
        channels: AsyncMutex<HashMap<String, Vec<Event>>>,
        closed: StdMutex<HashSet<String>>,
        changed: Notify,
    }

    #[async_trait]
    impl ChannelIoSubset for LoopbackWorkerBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> Result<()> {
            {
                let mut channels = self.channels.lock().await;
                channels
                    .entry(request.channel_key.0)
                    .or_default()
                    .extend(request.events);
            }
            self.changed.notify_waiters();
            Ok(())
        }

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: ChannelRecvRequest,
        ) -> Result<Vec<Event>> {
            let deadline = request
                .timeout_ms
                .map(Duration::from_millis)
                .map(|timeout| tokio::time::Instant::now() + timeout);
            loop {
                {
                    let mut channels = self.channels.lock().await;
                    if let Some(events) = channels.remove(&request.channel_key.0)
                        && !events.is_empty()
                    {
                        return Ok(events);
                    }
                }
                {
                    let closed = self.closed.lock().expect("loopback closed lock");
                    if closed.contains(&request.channel_key.0) {
                        return Err(CnxError::ChannelClosed);
                    }
                }
                let notified = self.changed.notified();
                if let Some(deadline) = deadline {
                    match tokio::time::timeout_at(deadline, notified).await {
                        Ok(()) => {}
                        Err(_) => return Err(CnxError::Timeout),
                    }
                } else {
                    notified.await;
                }
            }
        }

        fn channel_close(&self, _ctx: BoundaryContext, channel: ChannelKey) -> Result<()> {
            self.closed
                .lock()
                .expect("loopback closed lock")
                .insert(channel.0);
            self.changed.notify_waiters();
            Ok(())
        }
    }

    impl ChannelBoundary for LoopbackWorkerBoundary {
        fn log(&self, _ctx: BoundaryContext, _level: LogLevel, _msg: &str) {}
    }

    impl StateBoundary for LoopbackWorkerBoundary {}

    fn fs_meta_runtime_lib_filename() -> &'static str {
        #[cfg(target_os = "macos")]
        {
            "libfs_meta_runtime.dylib"
        }
        #[cfg(target_os = "windows")]
        {
            "fs_meta_runtime.dll"
        }
        #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
        {
            "libfs_meta_runtime.so"
        }
    }

    fn fs_meta_runtime_workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root")
            .to_path_buf()
    }

    fn fs_meta_worker_module_path() -> PathBuf {
        static BIN: OnceLock<PathBuf> = OnceLock::new();
        BIN.get_or_init(|| {
            for name in ["CAPANIX_FS_META_APP_BINARY", "DATANIX_FS_META_APP_SO"] {
                if let Ok(path) = std::env::var(name) {
                    let resolved = PathBuf::from(path);
                    if resolved.exists() {
                        return resolved;
                    }
                }
            }
            let root = fs_meta_runtime_workspace_root();
            let lib_name = fs_meta_runtime_lib_filename();
            for candidate in [
                root.join("target/debug").join(lib_name),
                root.join("target/debug/deps").join(lib_name),
                root.join(".target/debug").join(lib_name),
                root.join(".target/debug/deps").join(lib_name),
            ] {
                if candidate.exists() {
                    return candidate;
                }
            }
            panic!("fs-meta worker module not found; set CAPANIX_FS_META_APP_BINARY");
        })
        .clone()
    }

    fn external_sink_worker_binding(socket_dir: &Path) -> RuntimeWorkerBinding {
        RuntimeWorkerBinding {
            role_id: "sink".to_string(),
            mode: WorkerMode::External,
            launcher_kind: RuntimeWorkerLauncherKind::WorkerHost,
            module_path: Some(fs_meta_worker_module_path()),
            socket_dir: Some(socket_dir.to_path_buf()),
        }
    }

    fn sink_worker_root(id: &str, path: &Path) -> RootSpec {
        let mut root = RootSpec::new(id, path);
        root.watch = false;
        root.scan = true;
        root
    }

    fn sink_worker_export(
        object_ref: &str,
        host_ref: &str,
        host_ip: &str,
        mount_point: PathBuf,
    ) -> GrantedMountRoot {
        GrantedMountRoot {
            object_ref: object_ref.to_string(),
            host_ref: host_ref.to_string(),
            host_ip: host_ip.to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: Default::default(),
            mount_point,
            fs_source: object_ref.to_string(),
            fs_type: "nfs".to_string(),
            mount_options: vec![],
            interfaces: vec![],
            active: true,
        }
    }

    fn selected_group_request(path: &[u8], group_id: &str) -> InternalQueryRequest {
        InternalQueryRequest::materialized(
            QueryOp::Tree,
            QueryScope {
                path: path.to_vec(),
                recursive: false,
                max_depth: Some(0),
                selected_group: Some(group_id.to_string()),
            },
            None,
        )
    }

    fn bound_scope_with_resources(scope_id: &str, resource_ids: &[&str]) -> RuntimeBoundScope {
        RuntimeBoundScope {
            scope_id: scope_id.to_string(),
            resource_ids: resource_ids.iter().map(|id| (*id).to_string()).collect(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_sink_worker_materializes_each_local_primary_root_from_source_batches() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a")
            .expect("seed nfs1");
        std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b")
            .expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                sink_worker_root("nfs1", &nfs1),
                sink_worker_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                sink_worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                sink_worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
            .expect("init source");
        let mut stream = source.pub_().await.expect("start source pub stream");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory = RuntimeWorkerClientFactory::new(
            boundary.clone(),
            boundary.clone(),
            state_boundary,
        );
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < initial_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => sink.send(batch).await.expect("apply initial batch"),
                Ok(None) => break,
                Err(_) => continue,
            }
            let nfs1_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1"),
                b"/force-find-stress",
            )
            .expect("decode nfs1")
            .is_some();
            let nfs2_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                    .await
                    .expect("query nfs2"),
                b"/force-find-stress",
            )
            .expect("decode nfs2")
            .is_some();
            if nfs1_ready && nfs2_ready {
                break;
            }
        }

        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1 after initial"),
                b"/force-find-stress",
            )
            .expect("decode nfs1 after initial")
            .is_some(),
            "nfs1 initial materialization should exist",
        );
        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                    .await
                    .expect("query nfs2 after initial"),
                b"/force-find-stress",
            )
            .expect("decode nfs2 after initial")
            .is_some(),
            "nfs2 initial materialization should exist",
        );

        std::fs::write(
            nfs1.join("force-find-stress").join("after-rescan.txt"),
            b"aa",
        )
        .expect("append nfs1 file");
        std::fs::write(
            nfs2.join("force-find-stress").join("after-rescan.txt"),
            b"bb",
        )
        .expect("append nfs2 file");
        source
            .publish_manual_rescan_signal()
            .await
            .expect("publish manual rescan");

        let rescan_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < rescan_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => sink.send(batch).await.expect("apply rescan batch"),
                Ok(None) => break,
                Err(_) => continue,
            }
            let nfs1_done = decode_exact_query_node(
                sink.materialized_query(selected_group_request(
                    b"/force-find-stress/after-rescan.txt",
                    "nfs1",
                ))
                .await
                .expect("query nfs1 rescan"),
                b"/force-find-stress/after-rescan.txt",
            )
            .expect("decode nfs1 rescan")
            .is_some();
            let nfs2_done = decode_exact_query_node(
                sink.materialized_query(selected_group_request(
                    b"/force-find-stress/after-rescan.txt",
                    "nfs2",
                ))
                .await
                .expect("query nfs2 rescan"),
                b"/force-find-stress/after-rescan.txt",
            )
            .expect("decode nfs2 rescan")
            .is_some();
            if nfs1_done && nfs2_done {
                break;
            }
        }

        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(
                    b"/force-find-stress/after-rescan.txt",
                    "nfs1",
                ))
                .await
                .expect("query nfs1 final"),
                b"/force-find-stress/after-rescan.txt",
            )
            .expect("decode nfs1 final")
            .is_some(),
            "nfs1 should materialize its post-rescan file",
        );
        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(
                    b"/force-find-stress/after-rescan.txt",
                    "nfs2",
                ))
                .await
                .expect("query nfs2 final"),
                b"/force-find-stress/after-rescan.txt",
            )
            .expect("decode nfs2 final")
            .is_some(),
            "nfs2 should materialize its post-rescan file",
        );

        source.close().await.expect("close source");
        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_sink_worker_schedules_group_before_it_materializes_until_batches_arrive() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a")
            .expect("seed nfs1");
        std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b")
            .expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                sink_worker_root("nfs1", &nfs1),
                sink_worker_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                sink_worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                sink_worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
            .expect("init source");
        let mut stream = source.pub_().await.expect("start source pub stream");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory = RuntimeWorkerClientFactory::new(
            boundary.clone(),
            boundary.clone(),
            state_boundary,
        );
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");
        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("activate sink groups");

        let selected_file = b"/force-find-stress/seed.txt";
        let mut deferred_nfs2_batches = Vec::<Vec<Event>>::new();
        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < initial_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => {
                    let mut nfs1_batch = Vec::new();
                    let mut nfs2_batch = Vec::new();
                    for event in batch {
                        match event.metadata().origin_id.0.as_str() {
                            "node-a::nfs1" => nfs1_batch.push(event),
                            "node-a::nfs2" => nfs2_batch.push(event),
                            _ => {}
                        }
                    }
                    if !nfs2_batch.is_empty() {
                        deferred_nfs2_batches.push(nfs2_batch);
                    }
                    if !nfs1_batch.is_empty() {
                        sink.send(nfs1_batch)
                            .await
                            .expect("apply nfs1-only batch");
                    }
                }
                Ok(None) => break,
                Err(_) => continue,
            }
            let nfs1_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs1"))
                    .await
                    .expect("query nfs1 while nfs2 withheld"),
                selected_file,
            )
            .expect("decode nfs1 while nfs2 withheld")
            .is_some();
            if nfs1_ready {
                break;
            }
        }

        let status = sink
            .status_snapshot()
            .await
            .expect("status snapshot after nfs1-only send");
        assert_eq!(
            status
                .scheduled_groups_by_node
                .get("node-a")
                .cloned()
                .unwrap_or_default(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
            "scheduled group coverage should reflect both roots before nfs2 materializes",
        );
        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs1"))
                    .await
                    .expect("query nfs1 after nfs1-only send"),
                selected_file,
            )
            .expect("decode nfs1 after nfs1-only send")
            .is_some(),
            "nfs1 should materialize once its batches are sent",
        );
        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                    .await
                    .expect("query nfs2 before batches arrive"),
                selected_file,
            )
            .expect("decode nfs2 before batches arrive")
            .is_none(),
            "nfs2 should stay empty until its own batches are sent",
        );

        for batch in deferred_nfs2_batches.drain(..) {
            sink.send(batch).await.expect("apply deferred nfs2 batch");
        }
        let nfs2_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < nfs2_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => {
                    let nfs2_batch = batch
                        .into_iter()
                        .filter(|event| event.metadata().origin_id.0 == "node-a::nfs2")
                        .collect::<Vec<_>>();
                    if !nfs2_batch.is_empty() {
                        sink.send(nfs2_batch)
                            .await
                            .expect("apply remaining nfs2 batch");
                    }
                }
                Ok(None) => {}
                Err(_) => continue,
            }
            let nfs2_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                    .await
                    .expect("query nfs2 after deferred batches"),
                selected_file,
            )
            .expect("decode nfs2 after deferred batches")
            .is_some();
            if nfs2_ready {
                break;
            }
        }

        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                    .await
                    .expect("query nfs2 final"),
                selected_file,
            )
            .expect("decode nfs2 final")
            .is_some(),
            "nfs2 should materialize on the same sink-worker seam once its batches arrive",
        );

        source.close().await.expect("close source");
        sink.close().await.expect("close sink worker");
    }
}
