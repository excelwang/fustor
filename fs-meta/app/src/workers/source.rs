use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use capanix_app_fs_meta_runtime_support::{
    TypedWorkerClient, TypedWorkerRpc, encode_control_frame,
};
use capanix_app_sdk::raw::{BoundaryContext, ChannelIoSubset, ChannelKey, ChannelSendRequest};
use capanix_app_sdk::runtime::{ControlEnvelope, NodeId};
use capanix_app_sdk::{CnxError, Event, Result, RuntimeBoundary, RuntimeBoundaryApp};
use capanix_host_adapter_fs_meta::HostAdapter;
use futures_util::StreamExt;
use tokio::task::JoinHandle;

use crate::query::request::InternalQueryRequest;
use crate::runtime::orchestration::SourceControlSignal;
use crate::runtime::routes::{
    METHOD_SOURCE_FIND, ROUTE_KEY_EVENTS, ROUTE_TOKEN_FS_META_INTERNAL, default_route_bindings,
};
use crate::runtime::seam::exchange_host_adapter;
use crate::source::config::{GrantedMountRoot, RootSpec, SourceConfig, SourceExecutionMode};
use crate::source::{FSMetaSource, SourceStatusSnapshot};
use crate::workers::sink::SinkFacade;
use crate::workers::source_ipc::{
    SOURCE_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND, SourceWorkerRequest, SourceWorkerResponse,
    decode_response, encode_request, source_worker_control_route_key_for,
};

const SOURCE_WORKER_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_WORKER_FORCE_FIND_TIMEOUT: Duration = Duration::from_secs(60);
const SOURCE_WORKER_FORCE_FIND_REPLY_IDLE_GRACE: Duration = Duration::from_secs(5);
const SOURCE_WORKER_INIT_RPC_TIMEOUT: Duration = Duration::from_secs(120);
const SOURCE_WORKER_INIT_TOTAL_TIMEOUT: Duration = Duration::from_secs(240);
const SOURCE_WORKER_START_RPC_TIMEOUT: Duration = Duration::from_secs(180);
const SOURCE_WORKER_START_TOTAL_TIMEOUT: Duration = Duration::from_secs(300);
const SOURCE_WORKER_UPDATE_ROOTS_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT: Duration = Duration::from_secs(90);
const SOURCE_WORKER_RETRY_BACKOFF: Duration = Duration::from_millis(100);
const SOURCE_WORKER_DEGRADED_STATE: &str = "degraded_worker_unreachable";
const SOURCE_WORKER_DEGRADED_ROOT_KEY: &str = "source-worker";

fn is_source_worker_not_initialized(err: &CnxError) -> bool {
    matches!(err, CnxError::PeerError(message) if message == "source worker not initialized")
}

fn source_worker_bootstrap_frame(request: &SourceWorkerRequest) -> Result<ControlEnvelope> {
    Ok(encode_control_frame(
        SOURCE_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND,
        encode_request(request)?,
    ))
}

#[derive(Default)]
struct SourceWorkerHandleState {
    client: Arc<Mutex<Option<Arc<SourceWorkerClient>>>>,
    spawn_guard: Arc<Mutex<()>>,
}

#[derive(Debug, Clone, Default)]
struct SourceWorkerSnapshotCache {
    lifecycle_state: Option<String>,
    host_object_grants_version: Option<u64>,
    grants: Option<Vec<GrantedMountRoot>>,
    logical_roots: Option<Vec<RootSpec>>,
    status: Option<SourceStatusSnapshot>,
    source_primary_by_group: Option<std::collections::BTreeMap<String, String>>,
    last_force_find_runner_by_group: Option<std::collections::BTreeMap<String, String>>,
    force_find_inflight_groups: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct SourceWorkerClientHandle {
    node_id: NodeId,
    config: SourceConfig,
    boundary: Arc<crate::runtime_app::RuntimeBoundaryPair>,
    client: Arc<Mutex<Option<Arc<SourceWorkerClient>>>>,
    spawn_guard: Arc<Mutex<()>>,
}

impl SourceWorkerClientHandle {
    pub(crate) fn new(
        node_id: NodeId,
        config: SourceConfig,
        boundary: Arc<crate::runtime_app::RuntimeBoundaryPair>,
    ) -> Self {
        let shared = Arc::new(SourceWorkerHandleState::default());
        Self {
            node_id,
            config,
            boundary,
            client: shared.client.clone(),
            spawn_guard: shared.spawn_guard.clone(),
        }
    }

    fn existing_client(&self) -> Result<Option<Arc<SourceWorkerClient>>> {
        self.client
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| CnxError::Internal("source worker handle lock poisoned".into()))
    }

    fn reset_client(&self) -> Result<()> {
        let client = {
            let mut guard = self
                .client
                .lock()
                .map_err(|_| CnxError::Internal("source worker handle lock poisoned".into()))?;
            guard.take()
        };
        if let Some(client) = client {
            eprintln!(
                "fs_meta_runtime_app: resetting source worker client for {}",
                self.node_id.0
            );
            let _ = client.close();
        }
        Ok(())
    }

    fn with_retry<T>(&self, op: impl Fn(&SourceWorkerClient) -> Result<T>) -> Result<T> {
        let deadline = std::time::Instant::now() + SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT;
        loop {
            let client = self.client()?;
            match op(client.as_ref()) {
                Err(CnxError::TransportClosed(_)) | Err(CnxError::Timeout)
                    if std::time::Instant::now() < deadline =>
                {
                    self.reset_client()?;
                    let restarted = self.client()?;
                    restarted.ensure_started(&self.config)?;
                    std::thread::sleep(SOURCE_WORKER_RETRY_BACKOFF);
                }
                Err(err)
                    if is_source_worker_not_initialized(&err)
                        && std::time::Instant::now() < deadline =>
                {
                    client.note_not_initialized();
                    client.ensure_started(&self.config)?;
                    std::thread::sleep(SOURCE_WORKER_RETRY_BACKOFF);
                }
                other => return other,
            }
        }
    }

    fn with_started_retry<T>(&self, op: impl Fn(&SourceWorkerClient) -> Result<T>) -> Result<T> {
        self.start()?;
        self.with_retry(op)
    }

    fn client(&self) -> Result<Arc<SourceWorkerClient>> {
        if let Some(client) = self.existing_client()? {
            return Ok(client);
        }

        let _spawn_guard = self
            .spawn_guard
            .lock()
            .map_err(|_| CnxError::Internal("source worker spawn guard lock poisoned".into()))?;

        if let Some(client) = self.existing_client()? {
            return Ok(client);
        }

        let spawned = Arc::new(SourceWorkerClient::spawn(
            &self.node_id,
            &self.config,
            self.boundary.clone(),
        )?);
        let client = {
            let mut guard = self
                .client
                .lock()
                .map_err(|_| CnxError::Internal("source worker handle lock poisoned".into()))?;
            if let Some(existing) = guard.as_ref() {
                existing.clone()
            } else {
                *guard = Some(spawned.clone());
                spawned
            }
        };
        Ok(client)
    }

    pub fn start(&self) -> Result<()> {
        self.with_retry(|client| client.ensure_started(&self.config))
    }

    pub fn update_logical_roots(&self, roots: Vec<RootSpec>) -> Result<()> {
        self.with_started_retry(|client| client.update_logical_roots(roots.clone()))
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        match self.existing_client()? {
            Some(client) => client.cached_logical_roots_snapshot(),
            None => Ok(self.config.roots.clone()),
        }
    }

    pub fn logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        self.client()?.logical_roots_snapshot()
    }

    pub fn cached_host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        match self.existing_client()? {
            Some(client) => client.cached_host_object_grants_snapshot(),
            None => Ok(self.config.host_object_grants.clone()),
        }
    }

    pub fn host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        self.client()?.host_object_grants_snapshot()
    }

    pub fn host_object_grants_version_snapshot(&self) -> Result<u64> {
        self.client()?.host_object_grants_version_snapshot()
    }

    pub fn status_snapshot(&self) -> Result<SourceStatusSnapshot> {
        self.client()?.status_snapshot()
    }

    pub fn lifecycle_state_label(&self) -> Result<String> {
        self.client()?.lifecycle_state_label()
    }

    pub fn scheduled_source_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.client()?.scheduled_source_group_ids()
    }

    pub fn scheduled_scan_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.client()?.scheduled_scan_group_ids()
    }

    pub fn source_primary_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        self.client()?.source_primary_by_group_snapshot()
    }

    pub fn last_force_find_runner_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        self.client()?.last_force_find_runner_by_group_snapshot()
    }

    pub fn force_find_inflight_groups_snapshot(&self) -> Result<Vec<String>> {
        self.client()?.force_find_inflight_groups_snapshot()
    }

    pub fn resolve_group_id_for_object_ref(&self, object_ref: &str) -> Result<Option<String>> {
        self.client()?.resolve_group_id_for_object_ref(object_ref)
    }

    pub fn force_find(&self, params: InternalQueryRequest) -> Result<Vec<Event>> {
        self.with_started_retry(|client| client.force_find(params.clone()))
    }

    pub fn on_control_frame(&self, envelopes: Vec<ControlEnvelope>) -> Result<()> {
        self.start()?;
        self.client()?.on_control_frame(envelopes)
    }

    pub fn trigger_rescan_when_ready(&self) -> Result<()> {
        self.with_started_retry(|client| client.trigger_rescan_when_ready())
    }

    pub fn close(&self) -> Result<()> {
        self.reset_client()
    }

    pub(crate) fn observability_snapshot(&self) -> Result<SourceObservabilitySnapshot> {
        self.client()?.observability_snapshot()
    }

    pub(crate) fn degraded_observability_snapshot(
        &self,
        reason: impl Into<String>,
    ) -> SourceObservabilitySnapshot {
        let reason = reason.into();
        match self.client() {
            Ok(client) => client.degraded_observability_snapshot(reason),
            Err(_) => build_degraded_worker_observability_snapshot(
                &SourceWorkerSnapshotCache {
                    lifecycle_state: Some(SOURCE_WORKER_DEGRADED_STATE.to_string()),
                    host_object_grants_version: Some(0),
                    grants: Some(self.config.host_object_grants.clone()),
                    logical_roots: Some(self.config.roots.clone()),
                    status: Some(SourceStatusSnapshot::default()),
                    source_primary_by_group: Some(Default::default()),
                    last_force_find_runner_by_group: Some(Default::default()),
                    force_find_inflight_groups: Some(Vec::new()),
                },
                reason,
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SourceObservabilitySnapshot {
    pub lifecycle_state: String,
    pub host_object_grants_version: u64,
    pub grants: Vec<GrantedMountRoot>,
    pub logical_roots: Vec<RootSpec>,
    pub status: SourceStatusSnapshot,
    pub source_primary_by_group: std::collections::BTreeMap<String, String>,
    pub last_force_find_runner_by_group: std::collections::BTreeMap<String, String>,
    pub force_find_inflight_groups: Vec<String>,
}

fn build_degraded_worker_observability_snapshot(
    cache: &SourceWorkerSnapshotCache,
    reason: impl Into<String>,
) -> SourceObservabilitySnapshot {
    let reason = reason.into();
    let mut status = cache.status.clone().unwrap_or_default();
    status
        .degraded_roots
        .push((SOURCE_WORKER_DEGRADED_ROOT_KEY.to_string(), reason));
    SourceObservabilitySnapshot {
        lifecycle_state: SOURCE_WORKER_DEGRADED_STATE.to_string(),
        host_object_grants_version: cache.host_object_grants_version.unwrap_or_default(),
        grants: cache.grants.clone().unwrap_or_default(),
        logical_roots: cache.logical_roots.clone().unwrap_or_default(),
        status,
        source_primary_by_group: cache.source_primary_by_group.clone().unwrap_or_default(),
        last_force_find_runner_by_group: cache
            .last_force_find_runner_by_group
            .clone()
            .unwrap_or_default(),
        force_find_inflight_groups: cache.force_find_inflight_groups.clone().unwrap_or_default(),
    }
}

#[derive(Clone)]
pub enum SourceFacade {
    Local(Arc<FSMetaSource>),
    Worker(Arc<SourceWorkerClientHandle>),
}

impl SourceFacade {
    pub fn local(source: Arc<FSMetaSource>) -> Self {
        Self::Local(source)
    }

    pub fn worker(client: Arc<SourceWorkerClientHandle>) -> Self {
        Self::Worker(client)
    }

    pub fn is_worker(&self) -> bool {
        matches!(self, Self::Worker(_))
    }

    pub async fn start(
        &self,
        sink: Arc<SinkFacade>,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> Result<Option<JoinHandle<()>>> {
        match self {
            Self::Local(source) => {
                let stream = source.pub_().await?;
                Ok(Some(spawn_local_source_pump(stream, sink, boundary)))
            }
            Self::Worker(client) => {
                client.start()?;
                Ok(None)
            }
        }
    }

    pub(crate) async fn apply_orchestration_signals(
        &self,
        signals: &[SourceControlSignal],
    ) -> Result<()> {
        match self {
            Self::Local(source) => source.apply_orchestration_signals(signals).await,
            Self::Worker(client) => {
                let envelopes = signals
                    .iter()
                    .map(SourceControlSignal::envelope)
                    .collect::<Vec<_>>();
                client.on_control_frame(envelopes)
            }
        }
    }

    pub async fn update_logical_roots(&self, roots: Vec<RootSpec>) -> Result<()> {
        match self {
            Self::Local(source) => source.update_logical_roots(roots).await,
            Self::Worker(client) => client.update_logical_roots(roots),
        }
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        match self {
            Self::Local(source) => Ok(source.logical_roots_snapshot()),
            Self::Worker(client) => client.cached_logical_roots_snapshot(),
        }
    }

    pub fn logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        match self {
            Self::Local(source) => Ok(source.logical_roots_snapshot()),
            Self::Worker(client) => client.logical_roots_snapshot(),
        }
    }

    pub fn cached_host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        match self {
            Self::Local(source) => Ok(source.host_object_grants_snapshot()),
            Self::Worker(client) => client.cached_host_object_grants_snapshot(),
        }
    }

    pub fn host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        match self {
            Self::Local(source) => Ok(source.host_object_grants_snapshot()),
            Self::Worker(client) => client.host_object_grants_snapshot(),
        }
    }

    pub fn host_object_grants_version_snapshot(&self) -> Result<u64> {
        match self {
            Self::Local(source) => Ok(source.host_object_grants_version_snapshot()),
            Self::Worker(client) => client.host_object_grants_version_snapshot(),
        }
    }

    pub fn status_snapshot(&self) -> Result<SourceStatusSnapshot> {
        match self {
            Self::Local(source) => Ok(source.status_snapshot()),
            Self::Worker(client) => client.status_snapshot(),
        }
    }

    pub fn lifecycle_state_label(&self) -> Result<String> {
        match self {
            Self::Local(source) => Ok(format!("{:?}", source.state()).to_ascii_lowercase()),
            Self::Worker(client) => client.lifecycle_state_label(),
        }
    }

    pub fn scheduled_source_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self {
            Self::Local(source) => source.scheduled_source_group_ids(),
            Self::Worker(client) => client.scheduled_source_group_ids(),
        }
    }

    pub fn scheduled_scan_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self {
            Self::Local(source) => source.scheduled_scan_group_ids(),
            Self::Worker(client) => client.scheduled_scan_group_ids(),
        }
    }

    pub fn source_primary_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        match self {
            Self::Local(source) => Ok(source.source_primary_by_group_snapshot()),
            Self::Worker(client) => client.source_primary_by_group_snapshot(),
        }
    }

    pub fn last_force_find_runner_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        match self {
            Self::Local(source) => Ok(source.last_force_find_runner_by_group_snapshot()),
            Self::Worker(client) => client.last_force_find_runner_by_group_snapshot(),
        }
    }

    pub fn force_find_inflight_groups_snapshot(&self) -> Result<Vec<String>> {
        match self {
            Self::Local(source) => Ok(source.force_find_inflight_groups_snapshot()),
            Self::Worker(client) => client.force_find_inflight_groups_snapshot(),
        }
    }

    pub fn resolve_group_id_for_object_ref(&self, object_ref: &str) -> Result<Option<String>> {
        match self {
            Self::Local(source) => Ok(source.resolve_group_id_for_object_ref(object_ref)),
            Self::Worker(client) => client.resolve_group_id_for_object_ref(object_ref),
        }
    }

    pub fn force_find(&self, params: &InternalQueryRequest) -> Result<Vec<Event>> {
        match self {
            Self::Local(source) => source.force_find(params),
            Self::Worker(client) => client.force_find(params.clone()),
        }
    }

    pub fn trigger_rescan_when_ready_sync(&self) -> Result<()> {
        match self {
            Self::Local(_) => Err(CnxError::InvalidInput(
                "sync rescan path only supports worker execution mode".to_string(),
            )),
            Self::Worker(client) => client.trigger_rescan_when_ready(),
        }
    }

    pub async fn trigger_rescan_when_ready(&self) -> Result<()> {
        match self {
            Self::Local(source) => {
                source.trigger_rescan_when_ready().await;
                Ok(())
            }
            Self::Worker(client) => client.trigger_rescan_when_ready(),
        }
    }

    pub async fn close(&self) -> Result<()> {
        match self {
            Self::Local(source) => source.close().await,
            Self::Worker(client) => client.close(),
        }
    }

    pub(crate) fn observability_snapshot(&self) -> Result<SourceObservabilitySnapshot> {
        match self {
            Self::Local(source) => Ok(SourceObservabilitySnapshot {
                lifecycle_state: format!("{:?}", source.state()).to_ascii_lowercase(),
                host_object_grants_version: source.host_object_grants_version_snapshot(),
                grants: source.host_object_grants_snapshot(),
                logical_roots: source.logical_roots_snapshot(),
                status: source.status_snapshot(),
                source_primary_by_group: source.source_primary_by_group_snapshot(),
                last_force_find_runner_by_group: source.last_force_find_runner_by_group_snapshot(),
                force_find_inflight_groups: source.force_find_inflight_groups_snapshot(),
            }),
            Self::Worker(client) => Ok(SourceObservabilitySnapshot {
                lifecycle_state: client.lifecycle_state_label()?,
                host_object_grants_version: client.host_object_grants_version_snapshot()?,
                grants: client.host_object_grants_snapshot()?,
                logical_roots: client.logical_roots_snapshot()?,
                status: client.status_snapshot()?,
                source_primary_by_group: client.source_primary_by_group_snapshot()?,
                last_force_find_runner_by_group: client
                    .last_force_find_runner_by_group_snapshot()?,
                force_find_inflight_groups: client.force_find_inflight_groups_snapshot()?,
            }),
        }
    }

    pub(crate) fn degraded_observability_snapshot(
        &self,
        reason: impl Into<String>,
    ) -> SourceObservabilitySnapshot {
        let reason = reason.into();
        match self {
            Self::Local(source) => {
                let mut snapshot = SourceObservabilitySnapshot {
                    lifecycle_state: format!("{:?}", source.state()).to_ascii_lowercase(),
                    host_object_grants_version: source.host_object_grants_version_snapshot(),
                    grants: source.host_object_grants_snapshot(),
                    logical_roots: source.logical_roots_snapshot(),
                    status: source.status_snapshot(),
                    source_primary_by_group: source.source_primary_by_group_snapshot(),
                    last_force_find_runner_by_group: source
                        .last_force_find_runner_by_group_snapshot(),
                    force_find_inflight_groups: source.force_find_inflight_groups_snapshot(),
                };
                snapshot
                    .status
                    .degraded_roots
                    .push((SOURCE_WORKER_DEGRADED_ROOT_KEY.to_string(), reason));
                snapshot
            }
            Self::Worker(client) => client.degraded_observability_snapshot(reason),
        }
    }
}

fn spawn_local_source_pump(
    stream: std::pin::Pin<Box<dyn futures_core::Stream<Item = Vec<Event>> + Send>>,
    sink: Arc<SinkFacade>,
    boundary: Option<Arc<dyn ChannelIoSubset>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio::pin!(stream);
        while let Some(batch) = stream.next().await {
            if let Some(boundary) = boundary.as_ref() {
                if let Err(err) = boundary.channel_send(
                    BoundaryContext::default(),
                    ChannelSendRequest {
                        channel_key: ChannelKey(format!("{}.stream", ROUTE_KEY_EVENTS)),
                        events: batch,
                    },
                ) {
                    log::error!(
                        "fs-meta app pump failed to publish source batch on stream route: {:?}",
                        err
                    );
                }
            } else if let Err(err) = sink.send(&batch).await {
                log::error!("fs-meta app pump failed to apply batch: {:?}", err);
            }
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceBootstrapState {
    Connected,
    Initialized,
    Started,
}

pub struct SourceWorkerClient {
    conn: TypedWorkerClient<SourceWorkerRpc>,
    node_id: NodeId,
    data_boundary: Arc<dyn ChannelIoSubset>,
    cache: Arc<Mutex<SourceWorkerSnapshotCache>>,
    bootstrap_state: Mutex<SourceBootstrapState>,
}

struct SourceWorkerRpc;

impl TypedWorkerRpc for SourceWorkerRpc {
    type Request = SourceWorkerRequest;
    type Response = SourceWorkerResponse;

    fn encode_request(request: &Self::Request) -> Result<Vec<u8>> {
        encode_request(request)
    }

    fn decode_response(payload: &[u8]) -> Result<Self::Response> {
        decode_response(payload)
    }

    fn into_result(response: Self::Response) -> Result<Self::Response> {
        match response {
            SourceWorkerResponse::InvalidInput(message) => Err(CnxError::InvalidInput(message)),
            SourceWorkerResponse::Error(message) => Err(CnxError::PeerError(message)),
            other => Ok(other),
        }
    }

    fn unavailable_label() -> &'static str {
        "source worker unavailable"
    }
}

impl SourceWorkerClient {
    pub fn spawn<T>(node_id: &NodeId, config: &SourceConfig, boundary: Arc<T>) -> Result<Self>
    where
        T: RuntimeBoundary + ChannelIoSubset + 'static,
    {
        if config.source_execution_mode != SourceExecutionMode::WorkerProcess {
            return Err(CnxError::InvalidInput(
                "source worker client requires source_execution_mode=worker-process".into(),
            ));
        }
        let bin_path = config
            .source_worker_bin_path
            .as_ref()
            .ok_or_else(|| {
                CnxError::InvalidInput(
                    "source_worker_bin_path is required when source_execution_mode=worker-process"
                        .into(),
                )
            })?
            .clone();
        let socket_dir = config
            .source_worker_socket_dir
            .clone()
            .unwrap_or_else(std::env::temp_dir);
        eprintln!(
            "fs_meta_runtime_app: spawning source worker client for {}",
            node_id.0
        );
        eprintln!(
            "fs_meta_runtime_app: source worker control route_key={} socket_dir={} bin={}",
            source_worker_control_route_key_for(&node_id.0),
            socket_dir.display(),
            bin_path.display()
        );
        let conn = TypedWorkerClient::spawn(
            node_id,
            capanix_app_sdk::runtime::RouteKey(source_worker_control_route_key_for(&node_id.0)),
            &bin_path,
            &socket_dir,
            "fs-meta-source-worker",
            boundary.clone(),
        )?;
        let data_boundary: Arc<dyn ChannelIoSubset> = boundary.clone();

        Ok(Self {
            conn,
            node_id: node_id.clone(),
            data_boundary,
            cache: Arc::new(Mutex::new(SourceWorkerSnapshotCache {
                grants: Some(config.host_object_grants.clone()),
                logical_roots: Some(config.roots.clone()),
                ..SourceWorkerSnapshotCache::default()
            })),
            bootstrap_state: Mutex::new(SourceBootstrapState::Connected),
        })
    }

    fn is_started(&self) -> bool {
        match self.bootstrap_state.lock() {
            Ok(state) => matches!(*state, SourceBootstrapState::Started),
            Err(poisoned) => matches!(*poisoned.into_inner(), SourceBootstrapState::Started),
        }
    }

    fn note_not_initialized(&self) {
        match self.bootstrap_state.lock() {
            Ok(mut state) => {
                if matches!(*state, SourceBootstrapState::Started) {
                    *state = SourceBootstrapState::Initialized;
                }
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                if matches!(*state, SourceBootstrapState::Started) {
                    *state = SourceBootstrapState::Initialized;
                }
            }
        }
    }

    fn ping(&self) -> Result<()> {
        self.conn.control_frames(
            &[source_worker_bootstrap_frame(&SourceWorkerRequest::Ping)?],
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
    }

    pub fn ensure_started(&self, config: &SourceConfig) -> Result<()> {
        let mut state = self.bootstrap_state.lock().map_err(|_| {
            CnxError::Internal("source worker bootstrap state lock poisoned".into())
        })?;

        if matches!(*state, SourceBootstrapState::Started) {
            return Ok(());
        }

        if matches!(*state, SourceBootstrapState::Connected) {
            eprintln!(
                "fs_meta_runtime_app: sending source Init for {}",
                self.node_id.0
            );
            self.conn.retry_control_until(
                || {
                    Ok(vec![source_worker_bootstrap_frame(
                        &SourceWorkerRequest::Init {
                            node_id: self.node_id.0.clone(),
                            config: config.clone(),
                        },
                    )?])
                },
                SOURCE_WORKER_INIT_RPC_TIMEOUT,
                SOURCE_WORKER_INIT_TOTAL_TIMEOUT,
            )?;
            *state = SourceBootstrapState::Initialized;
        }

        if matches!(*state, SourceBootstrapState::Initialized) {
            eprintln!(
                "fs_meta_runtime_app: sending source Start for {}",
                self.node_id.0
            );
            self.conn.retry_control_until(
                || {
                    Ok(vec![source_worker_bootstrap_frame(
                        &SourceWorkerRequest::Start,
                    )?])
                },
                SOURCE_WORKER_START_RPC_TIMEOUT,
                SOURCE_WORKER_START_TOTAL_TIMEOUT,
            )?;
            self.ping()?;
            *state = SourceBootstrapState::Started;
            return Ok(());
        }

        Ok(())
    }

    fn with_cache_mut<T>(&self, f: impl FnOnce(&mut SourceWorkerSnapshotCache) -> T) -> T {
        let mut guard = match self.cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                log::warn!("source worker cache lock poisoned; recovering cached snapshot state");
                poisoned.into_inner()
            }
        };
        f(&mut guard)
    }

    fn degraded_observability_snapshot(
        &self,
        reason: impl Into<String>,
    ) -> SourceObservabilitySnapshot {
        let guard = match self.cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                log::warn!("source worker cache lock poisoned; recovering cached snapshot state");
                poisoned.into_inner()
            }
        };
        build_degraded_worker_observability_snapshot(&guard, reason)
    }

    fn observability_snapshot(&self) -> Result<SourceObservabilitySnapshot> {
        eprintln!(
            "fs_meta_runtime_app: source worker observability_snapshot begin node_id={}",
            self.node_id.0
        );
        let snapshot = SourceObservabilitySnapshot {
            lifecycle_state: self.lifecycle_state_label()?,
            host_object_grants_version: self.host_object_grants_version_snapshot()?,
            grants: self.host_object_grants_snapshot()?,
            logical_roots: self.logical_roots_snapshot()?,
            status: self.status_snapshot()?,
            source_primary_by_group: self.source_primary_by_group_snapshot()?,
            last_force_find_runner_by_group: self.last_force_find_runner_by_group_snapshot()?,
            force_find_inflight_groups: self.force_find_inflight_groups_snapshot()?,
        };
        eprintln!(
            "fs_meta_runtime_app: source worker observability_snapshot done node_id={} grants={} roots={}",
            self.node_id.0,
            snapshot.grants.len(),
            snapshot.logical_roots.len()
        );
        Ok(snapshot)
    }

    pub fn update_logical_roots(&self, roots: Vec<RootSpec>) -> Result<()> {
        let deadline = std::time::Instant::now() + SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT;
        loop {
            let attempt_timeout = std::cmp::min(
                SOURCE_WORKER_UPDATE_ROOTS_RPC_TIMEOUT,
                deadline.saturating_duration_since(std::time::Instant::now()),
            );
            if attempt_timeout.is_zero() {
                return Err(CnxError::Timeout);
            }
            match self.conn.call_with_timeout(
                SourceWorkerRequest::UpdateLogicalRoots {
                    roots: roots.clone(),
                },
                attempt_timeout,
            ) {
                Ok(SourceWorkerResponse::Ack) => {
                    self.with_cache_mut(|cache| {
                        cache.logical_roots = Some(roots.clone());
                    });
                    return Ok(());
                }
                Err(CnxError::PeerError(message))
                    if message == "source worker not initialized"
                        && std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(SOURCE_WORKER_RETRY_BACKOFF);
                }
                Err(CnxError::InvalidInput(message)) => {
                    return Err(CnxError::InvalidInput(message));
                }
                Ok(other) => {
                    return Err(CnxError::ProtocolViolation(format!(
                        "unexpected source worker response for update roots: {:?}",
                        other
                    )));
                }
                Err(CnxError::Timeout) | Err(CnxError::TransportClosed(_))
                    if std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(SOURCE_WORKER_RETRY_BACKOFF);
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        self.with_cache_mut(|cache| {
            cache.logical_roots.clone().ok_or_else(|| {
                CnxError::TransportClosed(
                    "source worker cached logical roots unavailable before bootstrap".into(),
                )
            })
        })
    }

    pub fn logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        eprintln!(
            "fs_meta_runtime_app: source worker logical_roots_snapshot request node_id={}",
            self.node_id.0
        );
        match self.conn.call_with_timeout(
            SourceWorkerRequest::LogicalRootsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        ) {
            Ok(SourceWorkerResponse::LogicalRoots(roots)) => {
                self.with_cache_mut(|cache| {
                    cache.logical_roots = Some(roots.clone());
                });
                eprintln!(
                    "fs_meta_runtime_app: source worker logical_roots_snapshot ok node_id={} roots={}",
                    self.node_id.0,
                    roots.len()
                );
                Ok(roots)
            }
            Ok(other) => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for logical roots: {:?}",
                other
            ))),
            Err(err) => Err(err),
        }
    }

    pub fn cached_host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        self.with_cache_mut(|cache| {
            cache.grants.clone().ok_or_else(|| {
                CnxError::TransportClosed(
                    "source worker cached host-object grants unavailable before bootstrap".into(),
                )
            })
        })
    }

    pub fn host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        eprintln!(
            "fs_meta_runtime_app: source worker host_object_grants_snapshot request node_id={}",
            self.node_id.0
        );
        match self.conn.call_with_timeout(
            SourceWorkerRequest::HostObjectGrantsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        ) {
            Ok(SourceWorkerResponse::HostObjectGrants(grants)) => {
                self.with_cache_mut(|cache| {
                    cache.grants = Some(grants.clone());
                });
                eprintln!(
                    "fs_meta_runtime_app: source worker host_object_grants_snapshot ok node_id={} grants={}",
                    self.node_id.0,
                    grants.len()
                );
                Ok(grants)
            }
            Ok(other) => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for host object grants: {:?}",
                other
            ))),
            Err(err) => Err(err),
        }
    }

    pub fn host_object_grants_version_snapshot(&self) -> Result<u64> {
        eprintln!(
            "fs_meta_runtime_app: source worker host_object_grants_version_snapshot request node_id={}",
            self.node_id.0
        );
        match self.conn.call_with_timeout(
            SourceWorkerRequest::HostObjectGrantsVersionSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        ) {
            Ok(SourceWorkerResponse::HostObjectGrantsVersion(version)) => {
                self.with_cache_mut(|cache| {
                    cache.host_object_grants_version = Some(version);
                });
                eprintln!(
                    "fs_meta_runtime_app: source worker host_object_grants_version_snapshot ok node_id={} version={}",
                    self.node_id.0, version
                );
                Ok(version)
            }
            Ok(other) => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for host object grants version: {:?}",
                other
            ))),
            Err(err) => Err(err),
        }
    }

    pub fn status_snapshot(&self) -> Result<SourceStatusSnapshot> {
        match self.conn.call_with_timeout(
            SourceWorkerRequest::StatusSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        ) {
            Ok(SourceWorkerResponse::StatusSnapshot(snapshot)) => {
                self.with_cache_mut(|cache| {
                    cache.status = Some(snapshot.clone());
                });
                Ok(snapshot)
            }
            Ok(other) => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for status snapshot: {:?}",
                other
            ))),
            Err(err) => Err(err),
        }
    }

    pub fn lifecycle_state_label(&self) -> Result<String> {
        match self.conn.call_with_timeout(
            SourceWorkerRequest::LifecycleState,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        ) {
            Ok(SourceWorkerResponse::LifecycleState(state)) => {
                self.with_cache_mut(|cache| {
                    cache.lifecycle_state = Some(state.clone());
                });
                Ok(state)
            }
            Ok(other) => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for lifecycle state: {:?}",
                other
            ))),
            Err(err) => Err(err),
        }
    }

    pub fn scheduled_source_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self.conn.call_with_timeout(
            SourceWorkerRequest::ScheduledSourceGroupIds,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )? {
            SourceWorkerResponse::ScheduledGroupIds(groups) => {
                Ok(groups.map(|groups| groups.into_iter().collect()))
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for scheduled source groups: {:?}",
                other
            ))),
        }
    }

    pub fn scheduled_scan_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self.conn.call_with_timeout(
            SourceWorkerRequest::ScheduledScanGroupIds,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )? {
            SourceWorkerResponse::ScheduledGroupIds(groups) => {
                Ok(groups.map(|groups| groups.into_iter().collect()))
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for scheduled scan groups: {:?}",
                other
            ))),
        }
    }

    pub fn source_primary_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        match self.conn.call_with_timeout(
            SourceWorkerRequest::SourcePrimaryByGroupSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )? {
            SourceWorkerResponse::SourcePrimaryByGroup(groups) => {
                self.with_cache_mut(|cache| {
                    cache.source_primary_by_group = Some(groups.clone());
                });
                Ok(groups)
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for primary groups: {:?}",
                other
            ))),
        }
    }

    pub fn last_force_find_runner_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        match self.conn.call_with_timeout(
            SourceWorkerRequest::LastForceFindRunnerByGroupSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )? {
            SourceWorkerResponse::LastForceFindRunnerByGroup(groups) => {
                self.with_cache_mut(|cache| {
                    cache.last_force_find_runner_by_group = Some(groups.clone());
                });
                Ok(groups)
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for last force-find runner: {:?}",
                other
            ))),
        }
    }

    pub fn force_find_inflight_groups_snapshot(&self) -> Result<Vec<String>> {
        match self.conn.call_with_timeout(
            SourceWorkerRequest::ForceFindInflightGroupsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )? {
            SourceWorkerResponse::ForceFindInflightGroups(groups) => {
                self.with_cache_mut(|cache| {
                    cache.force_find_inflight_groups = Some(groups.clone());
                });
                Ok(groups)
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for force-find inflight groups: {:?}",
                other
            ))),
        }
    }

    pub fn resolve_group_id_for_object_ref(&self, object_ref: &str) -> Result<Option<String>> {
        match self.conn.call_with_timeout(
            SourceWorkerRequest::ResolveGroupIdForObjectRef {
                object_ref: object_ref.to_string(),
            },
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )? {
            SourceWorkerResponse::ResolveGroupIdForObjectRef(group) => Ok(group),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for resolve group: {:?}",
                other
            ))),
        }
    }

    pub fn force_find(&self, params: InternalQueryRequest) -> Result<Vec<Event>> {
        let adapter = exchange_host_adapter(
            self.data_boundary.clone(),
            self.node_id.clone(),
            default_route_bindings(),
        );
        let payload = rmp_serde::to_vec(&params).map_err(|err| {
            CnxError::Internal(format!("source worker force-find encode failed: {err}"))
        })?;
        adapter.call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SOURCE_FIND,
            Bytes::from(payload),
            SOURCE_WORKER_FORCE_FIND_TIMEOUT,
            SOURCE_WORKER_FORCE_FIND_REPLY_IDLE_GRACE,
        )
    }

    pub fn on_control_frame(&self, envelopes: Vec<ControlEnvelope>) -> Result<()> {
        self.conn.control_frames(
            &[source_worker_bootstrap_frame(
                &SourceWorkerRequest::OnControlFrame { envelopes },
            )?],
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
    }

    pub fn trigger_rescan_when_ready(&self) -> Result<()> {
        eprintln!(
            "fs_meta_runtime_app: trigger_rescan_when_ready begin node={} timeout={:?}",
            self.node_id.0, SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
        );
        let result = self.conn.control_frames(
            &[source_worker_bootstrap_frame(
                &SourceWorkerRequest::TriggerRescanWhenReady,
            )?],
            SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
        );
        eprintln!(
            "fs_meta_runtime_app: trigger_rescan_when_ready end node={} result={:?}",
            self.node_id.0,
            result.as_ref().map(|_| ()).map_err(|err| err.to_string())
        );
        result
    }

    pub fn close(&self) -> Result<()> {
        self.conn.control_frames(
            &[source_worker_bootstrap_frame(&SourceWorkerRequest::Close)?],
            Duration::from_secs(2),
        )?;
        self.conn.shutdown()?;
        self.with_cache_mut(|cache| {
            cache.lifecycle_state = Some("closed".to_string());
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn source_worker_rpc_preserves_invalid_input_response_category() {
        let err = SourceWorkerRpc::into_result(SourceWorkerResponse::InvalidInput(
            "duplicate source root id 'dup'".to_string(),
        ))
        .expect_err("invalid input response must become an error");
        match err {
            CnxError::InvalidInput(message) => assert!(message.contains("duplicate")),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    fn degraded_worker_observability_uses_cached_snapshot() {
        let cache = SourceWorkerSnapshotCache {
            lifecycle_state: Some("ready".to_string()),
            host_object_grants_version: Some(7),
            grants: Some(vec![GrantedMountRoot {
                object_ref: "obj-a".to_string(),
                host_ref: "host-a".to_string(),
                host_ip: "10.0.0.1".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: Default::default(),
                mount_point: PathBuf::from("/mnt/nfs-a"),
                fs_source: "nfs://server/export".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: vec![],
                interfaces: vec![],
                active: true,
            }]),
            logical_roots: Some(vec![RootSpec::new("root-a", "/mnt/nfs-a")]),
            status: Some(SourceStatusSnapshot {
                logical_roots: vec![],
                concrete_roots: vec![],
                degraded_roots: vec![("existing-root".to_string(), "already degraded".to_string())],
            }),
            source_primary_by_group: Some(std::collections::BTreeMap::from([(
                "group-a".to_string(),
                "obj-a".to_string(),
            )])),
            last_force_find_runner_by_group: Some(std::collections::BTreeMap::from([(
                "group-a".to_string(),
                "obj-a".to_string(),
            )])),
            force_find_inflight_groups: Some(vec!["group-a".to_string()]),
        };

        let snapshot =
            build_degraded_worker_observability_snapshot(&cache, "source worker unavailable");

        assert_eq!(snapshot.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE);
        assert_eq!(snapshot.host_object_grants_version, 7);
        assert_eq!(snapshot.grants.len(), 1);
        assert_eq!(snapshot.logical_roots.len(), 1);
        assert_eq!(
            snapshot.source_primary_by_group.get("group-a"),
            Some(&"obj-a".to_string())
        );
        assert_eq!(
            snapshot.last_force_find_runner_by_group.get("group-a"),
            Some(&"obj-a".to_string())
        );
        assert_eq!(
            snapshot.force_find_inflight_groups,
            vec!["group-a".to_string()]
        );
        assert_eq!(snapshot.status.degraded_roots.len(), 2);
        assert_eq!(
            snapshot.status.degraded_roots[1],
            (
                SOURCE_WORKER_DEGRADED_ROOT_KEY.to_string(),
                "source worker unavailable".to_string()
            )
        );
    }
}
