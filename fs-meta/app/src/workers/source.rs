use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use capanix_app_sdk::runtime::{ControlEnvelope, NodeId, RuntimeWorkerBinding};
use capanix_app_sdk::{CnxError, Event, Result};
use capanix_runtime_entry_sdk::advanced::boundary::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelSendRequest,
};
use capanix_runtime_entry_sdk::worker_runtime::{
    RuntimeWorkerClientFactory, TypedRuntimeWorkerClient, TypedWorkerClient, TypedWorkerInit,
};
use futures_util::StreamExt;
use tokio::task::JoinHandle;

use crate::query::request::InternalQueryRequest;
use crate::runtime::orchestration::SourceControlSignal;
use crate::runtime::routes::ROUTE_KEY_EVENTS;
use crate::source::config::{GrantedMountRoot, RootSpec, SourceConfig};
use crate::source::{FSMetaSource, SourceStatusSnapshot};
use crate::workers::sink::SinkFacade;
use crate::workers::source_ipc::{
    SourceWorkerRequest, SourceWorkerResponse, decode_request, decode_response, encode_request,
    encode_response,
};

const SOURCE_WORKER_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(15);
const SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_WORKER_FORCE_FIND_TIMEOUT: Duration = Duration::from_secs(60);
const SOURCE_WORKER_UPDATE_ROOTS_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT: Duration = Duration::from_secs(90);
const SOURCE_WORKER_RETRY_BACKOFF: Duration = Duration::from_millis(100);
const SOURCE_WORKER_DEGRADED_STATE: &str = "degraded_worker_unreachable";
const SOURCE_WORKER_DEGRADED_ROOT_KEY: &str = "source-worker";

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
    worker_factory: RuntimeWorkerClientFactory,
    worker_binding: RuntimeWorkerBinding,
    worker: TypedRuntimeWorkerClient<SourceWorkerRpc, SourceConfig>,
    cache: Arc<Mutex<SourceWorkerSnapshotCache>>,
}

impl SourceWorkerClientHandle {
    pub(crate) fn new(
        node_id: NodeId,
        config: SourceConfig,
        worker_binding: RuntimeWorkerBinding,
        worker_factory: RuntimeWorkerClientFactory,
    ) -> Result<Self> {
        Ok(Self {
            worker: worker_factory.connect(
                node_id.clone(),
                config.clone(),
                worker_binding.clone(),
            )?,
            node_id,
            worker_factory,
            worker_binding,
            cache: Arc::new(Mutex::new(SourceWorkerSnapshotCache {
                grants: Some(config.host_object_grants.clone()),
                logical_roots: Some(config.roots.clone()),
                ..SourceWorkerSnapshotCache::default()
            })),
            config,
        })
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

    fn degraded_observability_snapshot_from_cache(
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

    async fn with_started_retry<T, F, Fut>(&self, op: F) -> Result<T>
    where
        F: Fn(TypedWorkerClient<SourceWorkerRpc>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.worker.with_started_retry(op).await
    }

    async fn client(&self) -> Result<TypedWorkerClient<SourceWorkerRpc>> {
        self.worker.client().await
    }

    async fn call_worker(
        client: &TypedWorkerClient<SourceWorkerRpc>,
        request: SourceWorkerRequest,
        timeout: Duration,
    ) -> Result<SourceWorkerResponse> {
        client.call_with_timeout(request, timeout).await
    }

    pub async fn start(&self) -> Result<()> {
        eprintln!(
            "fs_meta_source_worker_client: ensure_started begin node={}",
            self.node_id.0
        );
        self.worker.ensure_started().await
            .map(|_| {
                eprintln!(
                    "fs_meta_source_worker_client: ensure_started ok node={}",
                    self.node_id.0
                );
            })
    }

    pub async fn update_logical_roots(&self, roots: Vec<RootSpec>) -> Result<()> {
        self.with_started_retry(|client| {
            let roots = roots.clone();
            async move {
            let deadline = std::time::Instant::now() + SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT;
            loop {
                let attempt_timeout = std::cmp::min(
                    SOURCE_WORKER_UPDATE_ROOTS_RPC_TIMEOUT,
                    deadline.saturating_duration_since(std::time::Instant::now()),
                );
                if attempt_timeout.is_zero() {
                    return Err(CnxError::Timeout);
                }
                match Self::call_worker(
                    &client,
                    SourceWorkerRequest::UpdateLogicalRoots {
                        roots: roots.clone(),
                    },
                    attempt_timeout,
                )
                .await
                {
                    Ok(SourceWorkerResponse::Ack) => {
                        self.with_cache_mut(|cache| {
                            cache.logical_roots = Some(roots.clone());
                        });
                        return Ok(());
                    }
                    Err(CnxError::PeerError(message))
                        if message == "worker not initialized"
                            && std::time::Instant::now() < deadline =>
                    {
                        tokio::time::sleep(SOURCE_WORKER_RETRY_BACKOFF).await;
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
                        tokio::time::sleep(SOURCE_WORKER_RETRY_BACKOFF).await;
                    }
                    Err(err) => return Err(err),
                }
            }
            }
        })
        .await
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        self.with_cache_mut(|cache| {
            Ok(cache
                .logical_roots
                .clone()
                .unwrap_or_else(|| self.config.roots.clone()))
        })
    }

    pub async fn logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::LogicalRootsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        {
            Ok(SourceWorkerResponse::LogicalRoots(roots)) => {
                self.with_cache_mut(|cache| {
                    cache.logical_roots = Some(roots.clone());
                });
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
            Ok(cache
                .grants
                .clone()
                .unwrap_or_else(|| self.config.host_object_grants.clone()))
        })
    }

    pub async fn host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::HostObjectGrantsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        {
            Ok(SourceWorkerResponse::HostObjectGrants(grants)) => {
                self.with_cache_mut(|cache| {
                    cache.grants = Some(grants.clone());
                });
                Ok(grants)
            }
            Ok(other) => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for host object grants: {:?}",
                other
            ))),
            Err(err) => Err(err),
        }
    }

    pub async fn host_object_grants_version_snapshot(&self) -> Result<u64> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::HostObjectGrantsVersionSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        {
            Ok(SourceWorkerResponse::HostObjectGrantsVersion(version)) => {
                self.with_cache_mut(|cache| {
                    cache.host_object_grants_version = Some(version);
                });
                Ok(version)
            }
            Ok(other) => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for host object grants version: {:?}",
                other
            ))),
            Err(err) => Err(err),
        }
    }

    pub async fn status_snapshot(&self) -> Result<SourceStatusSnapshot> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::StatusSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        {
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

    pub async fn lifecycle_state_label(&self) -> Result<String> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::LifecycleState,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        {
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

    pub async fn scheduled_source_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::ScheduledSourceGroupIds,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::ScheduledGroupIds(groups) => {
                Ok(groups.map(|groups| groups.into_iter().collect()))
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for scheduled source groups: {:?}",
                other
            ))),
        }
    }

    pub async fn scheduled_scan_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::ScheduledScanGroupIds,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::ScheduledGroupIds(groups) => {
                Ok(groups.map(|groups| groups.into_iter().collect()))
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for scheduled scan groups: {:?}",
                other
            ))),
        }
    }

    pub async fn source_primary_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::SourcePrimaryByGroupSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
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

    pub async fn last_force_find_runner_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::LastForceFindRunnerByGroupSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
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

    pub async fn force_find_inflight_groups_snapshot(&self) -> Result<Vec<String>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::ForceFindInflightGroupsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
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

    pub async fn resolve_group_id_for_object_ref(
        &self,
        object_ref: &str,
    ) -> Result<Option<String>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::ResolveGroupIdForObjectRef {
                object_ref: object_ref.to_string(),
            },
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::ResolveGroupIdForObjectRef(group) => Ok(group),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for resolve group: {:?}",
                other
            ))),
        }
    }

    pub async fn force_find(&self, params: InternalQueryRequest) -> Result<Vec<Event>> {
        self.with_started_retry(|client| {
            let params = params.clone();
            async move {
            match Self::call_worker(
                &client,
                SourceWorkerRequest::ForceFind {
                    request: params.clone(),
                },
                SOURCE_WORKER_FORCE_FIND_TIMEOUT,
            )
            .await?
            {
                SourceWorkerResponse::Events(events) => Ok(events),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected source worker response for force-find: {:?}",
                    other
                ))),
            }
            }
        })
        .await
    }

    pub async fn publish_manual_rescan_signal(&self) -> Result<()> {
        self.with_started_retry(|client| async move {
            match Self::call_worker(
                &client,
                SourceWorkerRequest::PublishManualRescanSignal,
                SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
            )
            .await?
            {
                SourceWorkerResponse::Ack => Ok(()),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected source worker response for publish_manual_rescan_signal: {:?}",
                    other
                ))),
            }
        })
        .await
    }

    pub async fn on_control_frame(&self, envelopes: Vec<ControlEnvelope>) -> Result<()> {
        eprintln!(
            "fs_meta_source_worker_client: on_control_frame begin node={} envelopes={}",
            self.node_id.0,
            envelopes.len()
        );
        self.start().await?;
        let result = match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::OnControlFrame { envelopes },
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::Ack => Ok(()),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for on_control_frame: {:?}",
                other
            ))),
        };
        eprintln!(
            "fs_meta_source_worker_client: on_control_frame done node={} ok={}",
            self.node_id.0,
            result.is_ok()
        );
        result
    }

    pub async fn trigger_rescan_when_ready(&self) -> Result<()> {
        self.with_started_retry(|client| async move {
            match Self::call_worker(
                &client,
                SourceWorkerRequest::TriggerRescanWhenReady,
                SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
            )
            .await?
            {
                SourceWorkerResponse::Ack => Ok(()),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected source worker response for trigger_rescan_when_ready: {:?}",
                    other
                ))),
            }
        })
        .await
    }

    pub async fn close(&self) -> Result<()> {
        self.worker.shutdown(Duration::from_secs(2)).await?;
        self.with_cache_mut(|cache| {
            cache.lifecycle_state = Some("closed".to_string());
        });
        Ok(())
    }

    pub(crate) async fn observability_snapshot(&self) -> Result<SourceObservabilitySnapshot> {
        Ok(SourceObservabilitySnapshot {
            lifecycle_state: self.lifecycle_state_label().await?,
            host_object_grants_version: self.host_object_grants_version_snapshot().await?,
            grants: self.host_object_grants_snapshot().await?,
            logical_roots: self.logical_roots_snapshot().await?,
            status: self.status_snapshot().await?,
            source_primary_by_group: self.source_primary_by_group_snapshot().await?,
            last_force_find_runner_by_group: self
                .last_force_find_runner_by_group_snapshot()
                .await?,
            force_find_inflight_groups: self.force_find_inflight_groups_snapshot().await?,
        })
    }

    pub(crate) async fn degraded_observability_snapshot(
        &self,
        reason: impl Into<String>,
    ) -> SourceObservabilitySnapshot {
        let reason = reason.into();
        match self.client().await {
            Ok(_) => self
                .observability_snapshot()
                .await
                .unwrap_or_else(|_| self.degraded_observability_snapshot_from_cache(reason)),
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
                client.start().await?;
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
                client.on_control_frame(envelopes).await
            }
        }
    }

    pub async fn update_logical_roots(&self, roots: Vec<RootSpec>) -> Result<()> {
        match self {
            Self::Local(source) => source.update_logical_roots(roots).await,
            Self::Worker(client) => client.update_logical_roots(roots).await,
        }
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        match self {
            Self::Local(source) => Ok(source.logical_roots_snapshot()),
            Self::Worker(client) => client.cached_logical_roots_snapshot(),
        }
    }

    pub async fn logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        match self {
            Self::Local(source) => Ok(source.logical_roots_snapshot()),
            Self::Worker(client) => client.logical_roots_snapshot().await,
        }
    }

    pub fn cached_host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        match self {
            Self::Local(source) => Ok(source.host_object_grants_snapshot()),
            Self::Worker(client) => client.cached_host_object_grants_snapshot(),
        }
    }

    pub async fn host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        match self {
            Self::Local(source) => Ok(source.host_object_grants_snapshot()),
            Self::Worker(client) => client.host_object_grants_snapshot().await,
        }
    }

    pub async fn host_object_grants_version_snapshot(&self) -> Result<u64> {
        match self {
            Self::Local(source) => Ok(source.host_object_grants_version_snapshot()),
            Self::Worker(client) => client.host_object_grants_version_snapshot().await,
        }
    }

    pub async fn status_snapshot(&self) -> Result<SourceStatusSnapshot> {
        match self {
            Self::Local(source) => Ok(source.status_snapshot()),
            Self::Worker(client) => client.status_snapshot().await,
        }
    }

    pub async fn lifecycle_state_label(&self) -> Result<String> {
        match self {
            Self::Local(source) => Ok(format!("{:?}", source.state()).to_ascii_lowercase()),
            Self::Worker(client) => client.lifecycle_state_label().await,
        }
    }

    pub async fn scheduled_source_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self {
            Self::Local(source) => source.scheduled_source_group_ids(),
            Self::Worker(client) => client.scheduled_source_group_ids().await,
        }
    }

    pub async fn scheduled_scan_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self {
            Self::Local(source) => source.scheduled_scan_group_ids(),
            Self::Worker(client) => client.scheduled_scan_group_ids().await,
        }
    }

    pub async fn source_primary_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        match self {
            Self::Local(source) => Ok(source.source_primary_by_group_snapshot()),
            Self::Worker(client) => client.source_primary_by_group_snapshot().await,
        }
    }

    pub async fn last_force_find_runner_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        match self {
            Self::Local(source) => Ok(source.last_force_find_runner_by_group_snapshot()),
            Self::Worker(client) => client.last_force_find_runner_by_group_snapshot().await,
        }
    }

    pub async fn force_find_inflight_groups_snapshot(&self) -> Result<Vec<String>> {
        match self {
            Self::Local(source) => Ok(source.force_find_inflight_groups_snapshot()),
            Self::Worker(client) => client.force_find_inflight_groups_snapshot().await,
        }
    }

    pub async fn resolve_group_id_for_object_ref(
        &self,
        object_ref: &str,
    ) -> Result<Option<String>> {
        match self {
            Self::Local(source) => Ok(source.resolve_group_id_for_object_ref(object_ref)),
            Self::Worker(client) => client.resolve_group_id_for_object_ref(object_ref).await,
        }
    }

    pub async fn force_find(&self, params: &InternalQueryRequest) -> Result<Vec<Event>> {
        match self {
            Self::Local(source) => source.force_find(params),
            Self::Worker(client) => client.force_find(params.clone()).await,
        }
    }

    pub async fn force_find_via_node(
        &self,
        node_id: &NodeId,
        params: &InternalQueryRequest,
    ) -> Result<Vec<Event>> {
        match self {
            Self::Local(source) => source.force_find(params),
            Self::Worker(client) => {
                if client.node_id == *node_id {
                    client.force_find(params.clone()).await
                } else {
                    let remote = SourceWorkerClientHandle::new(
                        node_id.clone(),
                        client.config.clone(),
                        client.worker_binding.clone(),
                        client.worker_factory.clone(),
                    )?;
                    remote.force_find(params.clone()).await
                }
            }
        }
    }

    pub async fn publish_manual_rescan_signal(&self) -> Result<()> {
        match self {
            Self::Local(source) => source.publish_manual_rescan_signal().await,
            Self::Worker(client) => client.publish_manual_rescan_signal().await,
        }
    }

    pub async fn trigger_rescan_when_ready(&self) -> Result<()> {
        match self {
            Self::Local(source) => {
                source.trigger_rescan_when_ready().await;
                Ok(())
            }
            Self::Worker(client) => client.trigger_rescan_when_ready().await,
        }
    }

    pub async fn close(&self) -> Result<()> {
        match self {
            Self::Local(source) => source.close().await,
            Self::Worker(client) => client.close().await,
        }
    }

    pub(crate) async fn observability_snapshot(&self) -> Result<SourceObservabilitySnapshot> {
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
                lifecycle_state: client.lifecycle_state_label().await?,
                host_object_grants_version: client.host_object_grants_version_snapshot().await?,
                grants: client.host_object_grants_snapshot().await?,
                logical_roots: client.logical_roots_snapshot().await?,
                status: client.status_snapshot().await?,
                source_primary_by_group: client.source_primary_by_group_snapshot().await?,
                last_force_find_runner_by_group: client
                    .last_force_find_runner_by_group_snapshot()
                    .await?,
                force_find_inflight_groups: client.force_find_inflight_groups_snapshot().await?,
            }),
        }
    }

    pub(crate) async fn degraded_observability_snapshot(
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
            Self::Worker(client) => client.degraded_observability_snapshot(reason).await,
        }
    }
}

fn spawn_local_source_pump(
    stream: std::pin::Pin<Box<dyn futures_core::Stream<Item = Vec<Event>> + Send>>,
    sink: Arc<SinkFacade>,
    boundary: Option<Arc<dyn ChannelIoSubset>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Some(boundary) = boundary {
            let mut lanes = HashMap::<
                String,
                (
                    tokio::sync::mpsc::UnboundedSender<Vec<Event>>,
                    JoinHandle<()>,
                ),
            >::new();
            tokio::pin!(stream);
            while let Some(batch) = stream.next().await {
                let lane = batch
                    .first()
                    .map(|event| event.metadata().origin_id.0.clone())
                    .unwrap_or_else(|| "__empty__".to_string());
                let lane_tx = if let Some((tx, _)) = lanes.get(&lane) {
                    tx.clone()
                } else {
                    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<Event>>();
                    let send_boundary = boundary.clone();
                    let lane_name = lane.clone();
                    let task = tokio::spawn(async move {
                        while let Some(batch) = rx.recv().await {
                            if let Err(err) = send_boundary
                                .channel_send(
                                    BoundaryContext::default(),
                                    ChannelSendRequest {
                                        channel_key: ChannelKey(format!(
                                            "{}.stream",
                                            ROUTE_KEY_EVENTS
                                        )),
                                        events: batch,
                                        timeout_ms: Some(
                                            Duration::from_secs(5).as_millis() as u64
                                        ),
                                    },
                                )
                                .await
                            {
                                log::error!(
                                    "fs-meta app pump failed to publish source batch on stream route lane={}: {:?}",
                                    lane_name,
                                    err
                                );
                                break;
                            }
                        }
                    });
                    lanes.insert(lane.clone(), (tx.clone(), task));
                    tx
                };
                if lane_tx.send(batch).is_err() {
                    break;
                }
            }
            let mut tasks = Vec::with_capacity(lanes.len());
            for (_, (_, task)) in lanes.drain() {
                tasks.push(task);
            }
            for task in tasks {
                let _ = task.await;
            }
        } else {
            tokio::pin!(stream);
            while let Some(batch) = stream.next().await {
                if let Err(err) = sink.send(&batch).await {
                    log::error!("fs-meta app pump failed to apply batch: {:?}", err);
                }
            }
        }
    })
}

capanix_runtime_entry_sdk::worker_runtime::define_typed_worker_rpc! {
    pub struct SourceWorkerRpc {
        request: SourceWorkerRequest,
        response: SourceWorkerResponse,
        encode_request: encode_request,
        decode_request: decode_request,
        encode_response: encode_response,
        decode_response: decode_response,
        invalid_input: SourceWorkerResponse::InvalidInput,
        error: SourceWorkerResponse::Error,
        unavailable: "source worker unavailable",
    }
}

impl TypedWorkerInit<SourceConfig> for SourceWorkerRpc {
    type InitPayload = SourceConfig;

    fn init_payload(_node_id: &NodeId, config: &SourceConfig) -> Result<Self::InitPayload> {
        Ok(config.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use capanix_runtime_entry_sdk::worker_runtime::TypedWorkerRpc;
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

    #[test]
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
