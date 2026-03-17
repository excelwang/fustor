use std::sync::{Arc, Mutex};
use std::time::Duration;

use capanix_app_fs_meta_runtime_support::{
    TypedWorkerClient, define_typed_worker_rpc, encode_control_frame,
};
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::{ControlEnvelope, NodeId, RecvOpts};
use capanix_app_sdk::{CnxError, Event, Result, RuntimeBoundary, RuntimeBoundaryApp};

use crate::query::models::{HealthStats, QueryNode};
use crate::query::path::root_file_name_bytes;
use crate::query::request::{InternalQueryRequest, MaterializedQueryPayload, QueryOp, QueryScope};
use crate::runtime::orchestration::{SinkControlSignal, sink_control_signals_from_envelopes};
use crate::sink::{SinkFileMeta, SinkStatusSnapshot, VisibilityLagSample};
use crate::source::config::{GrantedMountRoot, SinkExecutionMode, SourceConfig};
use crate::workers::sink_ipc::{
    SINK_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND, SinkWorkerInitConfig, SinkWorkerRequest,
    SinkWorkerResponse, decode_response, encode_request, sink_worker_control_route_key_for,
};

const SINK_WORKER_INIT_RPC_TIMEOUT: Duration = Duration::from_secs(60);
const SINK_WORKER_INIT_TOTAL_TIMEOUT: Duration = Duration::from_secs(120);
const SINK_WORKER_START_RPC_TIMEOUT: Duration = Duration::from_secs(15);
const SINK_WORKER_START_TOTAL_TIMEOUT: Duration = Duration::from_secs(60);

fn sink_worker_bootstrap_frame(request: &SinkWorkerRequest) -> Result<ControlEnvelope> {
    Ok(encode_control_frame(
        SINK_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND,
        encode_request(request)?,
    ))
}

#[derive(Default)]
struct SinkWorkerHandleState {
    client: Arc<Mutex<Option<Arc<SinkWorkerClient>>>>,
    pending_control: Arc<Mutex<Vec<ControlEnvelope>>>,
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
    pending_control: Arc<Mutex<Vec<ControlEnvelope>>>,
}

impl SinkWorkerClientHandle {
    pub(crate) fn new(
        node_id: NodeId,
        config: SourceConfig,
        boundary: Arc<crate::runtime_app::RuntimeBoundaryPair>,
    ) -> Self {
        let shared = Arc::new(SinkWorkerHandleState::default());
        Self {
            node_id,
            config,
            boundary,
            client: shared.client.clone(),
            pending_control: shared.pending_control.clone(),
        }
    }

    fn existing_client(&self) -> Result<Option<Arc<SinkWorkerClient>>> {
        self.client
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| CnxError::Internal("sink worker handle lock poisoned".into()))
    }

    fn flush_pending_control(&self, client: &SinkWorkerClient) -> Result<()> {
        let pending = {
            let mut guard = self.pending_control.lock().map_err(|_| {
                CnxError::Internal("sink worker pending control lock poisoned".into())
            })?;
            if guard.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *guard)
        };
        client.on_control_frame(pending)
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
            eprintln!("fs_meta_runtime_app: resetting sink worker client for {}", self.node_id.0);
            let _ = client.close();
        }
        Ok(())
    }

    fn with_retry<T>(&self, op: impl Fn(&SinkWorkerClient) -> Result<T>) -> Result<T> {
        let client = self.client()?;
        match op(client.as_ref()) {
            Err(CnxError::TransportClosed(_)) => {
                self.reset_client()?;
                let client = self.client()?;
                op(client.as_ref())
            }
            Err(CnxError::Timeout) => Err(CnxError::Timeout),
            other => other,
        }
    }

    fn with_existing_snapshot_fallback<T>(
        &self,
        op: impl Fn(&SinkWorkerClient) -> Result<T>,
        fallback: impl FnOnce() -> T,
    ) -> Result<T> {
        match self.existing_client()? {
            Some(client) => match op(client.as_ref()) {
                Ok(value) => Ok(value),
                Err(CnxError::TransportClosed(_)) | Err(CnxError::Timeout) => {
                    log::warn!(
                        "sink worker snapshot fallback after transport failure for {}",
                        self.node_id.0
                    );
                    Ok(fallback())
                }
                Err(err) => Err(err),
            },
            None => Ok(fallback()),
        }
    }

    fn client(&self) -> Result<Arc<SinkWorkerClient>> {
        if let Some(client) = self.existing_client()? {
            self.flush_pending_control(client.as_ref())?;
            return Ok(client);
        }
        let mut guard = self
            .client
            .lock()
            .map_err(|_| CnxError::Internal("sink worker handle lock poisoned".into()))?;
        if let Some(client) = guard.as_ref() {
            let client = client.clone();
            drop(guard);
            self.flush_pending_control(client.as_ref())?;
            return Ok(client);
        }
        let client = Arc::new(SinkWorkerClient::spawn(
            &self.node_id,
            &self.config,
            self.boundary.clone(),
        )?);
        *guard = Some(client.clone());
        drop(guard);
        self.flush_pending_control(client.as_ref())?;
        Ok(client)
    }

    pub fn ensure_started(&self) -> Result<()> {
        self.client().map(|_| ())
    }

    pub fn update_logical_roots(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
        host_object_grants: Vec<GrantedMountRoot>,
    ) -> Result<()> {
        self.with_retry(|client| {
            client.update_logical_roots(roots.clone(), host_object_grants.clone())
        })
    }

    pub fn logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        self.with_existing_snapshot_fallback(
            |client| client.logical_roots_snapshot(),
            || self.config.roots.clone(),
        )
    }

    pub fn health(&self) -> Result<HealthStats> {
        self.with_existing_snapshot_fallback(|client| client.health(), HealthStats::default)
    }

    pub fn status_snapshot(&self) -> Result<SinkStatusSnapshot> {
        self.with_existing_snapshot_fallback(
            |client| client.status_snapshot(),
            SinkStatusSnapshot::default,
        )
    }

    pub fn scheduled_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.with_existing_snapshot_fallback(|client| client.scheduled_group_ids(), || None)
    }

    pub fn visibility_lag_samples_since(&self, since_us: u64) -> Result<Vec<VisibilityLagSample>> {
        self.with_existing_snapshot_fallback(
            |client| client.visibility_lag_samples_since(since_us),
            Vec::new,
        )
    }

    pub fn query_node(&self, path: Vec<u8>) -> Result<Option<QueryNode>> {
        self.with_retry(|client| client.query_node(path.clone()))
    }

    pub fn materialized_query(&self, request: InternalQueryRequest) -> Result<Vec<Event>> {
        self.with_retry(|client| client.materialized_query(request.clone()))
    }

    pub fn subtree_stats(&self, path: Vec<u8>) -> Result<Vec<Event>> {
        self.with_retry(|client| client.subtree_stats(path.clone()))
    }

    pub fn send(&self, events: Vec<Event>) -> Result<()> {
        self.with_retry(|client| client.send(events.clone()))
    }

    pub fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        self.with_retry(|client| client.recv(opts.clone()))
    }

    pub fn on_control_frame(&self, envelopes: Vec<ControlEnvelope>) -> Result<()> {
        if let Some(client) = self.existing_client()? {
            return client.on_control_frame(envelopes);
        }
        let mut guard = self
            .pending_control
            .lock()
            .map_err(|_| CnxError::Internal("sink worker pending control lock poisoned".into()))?;
        guard.extend(envelopes);
        Ok(())
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

#[derive(Clone)]
pub struct SinkWorkerClient {
    conn: TypedWorkerClient<SinkWorkerRpc>,
}

define_typed_worker_rpc! {
    struct SinkWorkerRpc {
        request: SinkWorkerRequest,
        response: SinkWorkerResponse,
        encode: encode_request,
        decode: decode_response,
        error: SinkWorkerResponse::Error,
        unavailable: "sink worker unavailable",
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

        let client = Self { conn };

        eprintln!(
            "fs_meta_runtime_app: probing sink worker control plane for {}",
            node_id.0
        );
        client.conn.retry_control_until(
            || Ok(vec![sink_worker_bootstrap_frame(&SinkWorkerRequest::Ping)?]),
            SINK_WORKER_INIT_RPC_TIMEOUT,
            SINK_WORKER_INIT_TOTAL_TIMEOUT,
        )?;

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
        client.conn.retry_control_until(
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

        eprintln!("fs_meta_runtime_app: sending sink Start for {}", node_id.0);
        client.conn.retry_control_until(
            || {
                Ok(vec![sink_worker_bootstrap_frame(
                    &SinkWorkerRequest::Start,
                )?])
            },
            SINK_WORKER_START_RPC_TIMEOUT,
            SINK_WORKER_START_TOTAL_TIMEOUT,
        )?;

        Ok(client)
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
            Duration::from_secs(5),
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
            Duration::from_secs(5),
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
