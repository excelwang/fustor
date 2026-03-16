use std::sync::Arc;
use std::time::Duration;

use capanix_app_fs_meta_runtime_support::{TypedWorkerClient, define_typed_worker_rpc};
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
    SINK_WORKER_ROUTE_KEY, SinkWorkerInitConfig, SinkWorkerRequest, SinkWorkerResponse,
    decode_response, encode_request,
};

const SINK_WORKER_INIT_RPC_TIMEOUT: Duration = Duration::from_secs(60);
const SINK_WORKER_INIT_TOTAL_TIMEOUT: Duration = Duration::from_secs(120);
const SINK_WORKER_START_RPC_TIMEOUT: Duration = Duration::from_secs(15);

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
pub enum SinkFacade {
    Local(Arc<SinkFileMeta>),
    Worker(Arc<SinkWorkerClient>),
}

impl SinkFacade {
    pub fn local(sink: Arc<SinkFileMeta>) -> Self {
        Self::Local(sink)
    }

    pub fn worker(client: Arc<SinkWorkerClient>) -> Self {
        Self::Worker(client)
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
        let conn = TypedWorkerClient::spawn(
            node_id,
            capanix_app_sdk::runtime::RouteKey(SINK_WORKER_ROUTE_KEY.to_string()),
            &bin_path,
            &socket_dir,
            "fs-meta-sink-worker",
            boundary,
        )?;

        let client = Self { conn };

        let init = SinkWorkerInitConfig {
            roots: config.roots.clone(),
            host_object_grants: config.host_object_grants.clone(),
            sink_tombstone_ttl_ms: config.sink_tombstone_ttl.as_millis() as u64,
            sink_tombstone_tolerance_us: config.sink_tombstone_tolerance_us,
        };
        client.conn.retry_until(
            || SinkWorkerRequest::Init {
                node_id: node_id.0.clone(),
                config: init.clone(),
            },
            SINK_WORKER_INIT_RPC_TIMEOUT,
            SINK_WORKER_INIT_TOTAL_TIMEOUT,
            |response| match response {
                SinkWorkerResponse::Ack => Ok(()),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker init response: {:?}",
                    other
                ))),
            },
        )?;

        match client
            .conn
            .call_with_timeout(SinkWorkerRequest::Start, SINK_WORKER_START_RPC_TIMEOUT)?
        {
            SinkWorkerResponse::Ack => {}
            other => {
                return Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker response for start: {:?}",
                    other
                )));
            }
        }

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
        match self.conn.call_with_timeout(
            SinkWorkerRequest::OnControlFrame { envelopes },
            Duration::from_secs(5),
        )? {
            SinkWorkerResponse::Ack => Ok(()),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for control frame: {:?}",
                other
            ))),
        }
    }

    pub fn close(&self) -> Result<()> {
        self.conn
            .close(SinkWorkerRequest::Close, Duration::from_secs(2))
    }
}
