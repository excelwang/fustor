use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use capanix_app_sdk::runtime::{ControlEnvelope, NodeId};
use capanix_app_sdk::{CnxError, Event, Result};
use capanix_runtime_entry_sdk::advanced::boundary::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelSendRequest, StateBoundary,
};
use capanix_runtime_entry_sdk::worker_runtime::{
    TypedWorkerBootstrapSession, TypedWorkerSession, WorkerLoopControl, WorkerSessionContext,
    run_worker_sidecar_server,
};
use futures_util::StreamExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::source::FSMetaSource;
use crate::source::config::SourceConfig;
use crate::workers::source::SourceWorkerRpc;
use crate::workers::source_ipc::{SourceWorkerRequest, SourceWorkerResponse};

const ROUTE_KEY_EVENTS: &str = "fs-meta.events:v1";
const SOURCE_WORKER_STOP_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const SOURCE_WORKER_STOP_ABORT_TIMEOUT: Duration = Duration::from_millis(250);

struct SourceWorkerState {
    source: Option<Arc<FSMetaSource>>,
    pending_init: Option<(NodeId, SourceConfig)>,
    pump_task: Option<JoinHandle<()>>,
}

fn classify_source_worker_error(err: CnxError) -> SourceWorkerResponse {
    match err {
        CnxError::InvalidInput(message) => SourceWorkerResponse::InvalidInput(message),
        other => SourceWorkerResponse::Error(other.to_string()),
    }
}

async fn stop_source_runtime_with_timeouts(
    state: &mut SourceWorkerState,
    join_timeout: Duration,
    abort_timeout: Duration,
) {
    if let Some(source) = state.source.as_ref() {
        let _ = source.close().await;
    }
    if let Some(mut handle) = state.pump_task.take() {
        if tokio::time::timeout(join_timeout, &mut handle)
            .await
            .is_err()
        {
            handle.abort();
            let _ = tokio::time::timeout(abort_timeout, handle).await;
        }
    }
    state.source = None;
}

async fn stop_source_runtime(state: &mut SourceWorkerState) {
    stop_source_runtime_with_timeouts(
        state,
        SOURCE_WORKER_STOP_WAIT_TIMEOUT,
        SOURCE_WORKER_STOP_ABORT_TIMEOUT,
    )
    .await;
}

fn start_source_pump_with_stream<S>(stream: S, boundary: Arc<dyn ChannelIoSubset>) -> JoinHandle<()>
where
    S: futures_util::Stream<Item = Vec<Event>> + Send + 'static,
{
    tokio::spawn(async move {
        let mut lanes = HashMap::<
            String,
            (
                tokio::sync::mpsc::UnboundedSender<Vec<Event>>,
                JoinHandle<()>,
            ),
        >::new();

        futures_util::pin_mut!(stream);
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
                                    channel_key: ChannelKey(format!("{}.stream", ROUTE_KEY_EVENTS)),
                                    events: batch,
                                    timeout_ms: Some(Duration::from_secs(5).as_millis() as u64),
                                },
                            )
                            .await
                        {
                            log::error!(
                                "source worker pump failed to publish source batch on stream route lane={}: {:?}",
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
    })
}

fn bootstrap_not_ready() -> CnxError {
    CnxError::NotReady("worker not initialized".into())
}

async fn bootstrap_init_source_runtime(
    node_id: NodeId,
    config: SourceConfig,
    state: &mut SourceWorkerState,
) {
    let _ = stop_source_runtime(state).await;
    state.pending_init = Some((node_id, config));
}

async fn bootstrap_start_source_runtime(
    state: &mut SourceWorkerState,
    boundary: Arc<dyn ChannelIoSubset>,
    state_boundary: Arc<dyn StateBoundary>,
) -> Result<()> {
    let should_restart = state
        .pump_task
        .as_ref()
        .is_some_and(tokio::task::JoinHandle::is_finished);
    if should_restart {
        let _ = stop_source_runtime(state).await;
        return Err(CnxError::NotReady("worker not initialized".to_string()));
    }
    if state.source.is_none() {
        let Some((node_id, config)) = state.pending_init.clone() else {
            return Err(bootstrap_not_ready());
        };
        match FSMetaSource::with_boundaries_and_state(config, node_id, None, state_boundary) {
            Ok(inner) => {
                state.source = Some(Arc::new(inner));
            }
            Err(err) => return Err(err),
        }
    }
    if state.pump_task.is_none() {
        let Some(source) = state.source.as_ref() else {
            return Err(CnxError::Internal(
                "source worker runtime missing during start".into(),
            ));
        };
        source.start_runtime_endpoints(boundary.clone()).await?;
        let source = source.clone();
        let stream = source.pub_().await?;
        state.pump_task = Some(start_source_pump_with_stream(stream, boundary));
    }
    Ok(())
}

async fn process_worker_request(
    request: SourceWorkerRequest,
    state: &mut SourceWorkerState,
) -> (SourceWorkerResponse, bool) {
    match request {
        SourceWorkerRequest::UpdateLogicalRoots { roots } => match state.source.as_ref() {
            Some(source) => match source.update_logical_roots(roots).await {
                Ok(_) => (SourceWorkerResponse::Ack, false),
                Err(err) => (classify_source_worker_error(err), false),
            },
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::LogicalRootsSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::LogicalRoots(source.logical_roots_snapshot()),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::HostObjectGrantsSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::HostObjectGrants(source.host_object_grants_snapshot()),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::HostObjectGrantsVersionSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::HostObjectGrantsVersion(
                    source.host_object_grants_version_snapshot(),
                ),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::StatusSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::StatusSnapshot(source.status_snapshot()),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::LifecycleState => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::LifecycleState(
                    format!("{:?}", source.state()).to_ascii_lowercase(),
                ),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ScheduledSourceGroupIds => match state.source.as_ref() {
            Some(source) => match source.scheduled_source_group_ids() {
                Ok(groups) => (
                    SourceWorkerResponse::ScheduledGroupIds(
                        groups.map(|group_ids| group_ids.into_iter().collect()),
                    ),
                    false,
                ),
                Err(err) => (SourceWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ScheduledScanGroupIds => match state.source.as_ref() {
            Some(source) => match source.scheduled_scan_group_ids() {
                Ok(groups) => (
                    SourceWorkerResponse::ScheduledGroupIds(
                        groups.map(|group_ids| group_ids.into_iter().collect()),
                    ),
                    false,
                ),
                Err(err) => (SourceWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::SourcePrimaryByGroupSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::SourcePrimaryByGroup(
                    source.source_primary_by_group_snapshot(),
                ),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::LastForceFindRunnerByGroupSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::LastForceFindRunnerByGroup(
                    source.last_force_find_runner_by_group_snapshot(),
                ),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ForceFindInflightGroupsSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::ForceFindInflightGroups(
                    source.force_find_inflight_groups_snapshot(),
                ),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ForceFind { request } => match state.source.as_ref() {
            Some(source) => match source.force_find(&request) {
                Ok(events) => (SourceWorkerResponse::Events(events), false),
                Err(err) => (SourceWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ResolveGroupIdForObjectRef { object_ref } => {
            match state.source.as_ref() {
                Some(source) => (
                    SourceWorkerResponse::ResolveGroupIdForObjectRef(
                        source.resolve_group_id_for_object_ref(&object_ref),
                    ),
                    false,
                ),
                None => (
                    SourceWorkerResponse::Error("worker not initialized".into()),
                    false,
                ),
            }
        }
        SourceWorkerRequest::PublishManualRescanSignal => match state.source.as_ref() {
            Some(source) => match source.publish_manual_rescan_signal().await {
                Ok(()) => (SourceWorkerResponse::Ack, false),
                Err(err) => (classify_source_worker_error(err), false),
            },
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::TriggerRescanWhenReady => match state.source.as_ref() {
            Some(source) => {
                let source = source.clone();
                tokio::spawn(async move {
                    source.trigger_rescan_when_ready().await;
                });
                (SourceWorkerResponse::Ack, false)
            }
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::OnControlFrame { envelopes } => match state.source.as_ref() {
            Some(source) => match source.on_control_frame(&envelopes).await {
                Ok(_) => (SourceWorkerResponse::Ack, false),
                Err(err) => (SourceWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
    }
}

pub fn run_source_worker_server(
    control_socket_path: &Path,
    data_socket_path: &Path,
) -> std::io::Result<()> {
    let state = Arc::new(Mutex::new(SourceWorkerState {
        source: None,
        pending_init: None,
        pump_task: None,
    }));
    run_worker_sidecar_server::<SourceWorkerRpc, _, SourceConfig>(
        control_socket_path,
        data_socket_path,
        SourceWorkerSession { state },
    )
}

struct SourceWorkerSession {
    state: Arc<Mutex<SourceWorkerState>>,
}

#[async_trait::async_trait]
impl TypedWorkerSession<SourceWorkerRpc> for SourceWorkerSession {
    async fn handle_request(
        &mut self,
        request: SourceWorkerRequest,
        _context: &WorkerSessionContext,
    ) -> capanix_app_sdk::Result<WorkerLoopControl<SourceWorkerResponse>> {
        let mut guard = self.state.lock().await;
        let (response, stop) = process_worker_request(request, &mut guard).await;
        Ok(if stop {
            WorkerLoopControl::Stop(response)
        } else {
            WorkerLoopControl::Continue(response)
        })
    }

    async fn on_runtime_control(
        &mut self,
        envelopes: &[ControlEnvelope],
        _context: &WorkerSessionContext,
    ) -> capanix_app_sdk::Result<()> {
        let guard = self.state.lock().await;
        let Some(source) = guard.source.as_ref() else {
            return Err(CnxError::NotReady(
                "worker runtime not initialized for runtime control frames".into(),
            ));
        };
        source.on_control_frame(envelopes).await
    }
}

#[async_trait::async_trait]
impl TypedWorkerBootstrapSession<SourceConfig> for SourceWorkerSession {
    async fn on_init(
        &mut self,
        node_id: NodeId,
        payload: SourceConfig,
        _context: &WorkerSessionContext,
    ) -> capanix_app_sdk::Result<()> {
        let mut guard = self.state.lock().await;
        bootstrap_init_source_runtime(node_id, payload, &mut guard).await;
        Ok(())
    }

    async fn on_start(&mut self, context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        let mut guard = self.state.lock().await;
        bootstrap_start_source_runtime(&mut guard, context.io_boundary(), context.state_boundary())
            .await
    }

    async fn on_ping(&mut self, _context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        let guard = self.state.lock().await;
        if guard.source.is_some() {
            Ok(())
        } else {
            Err(bootstrap_not_ready())
        }
    }

    async fn on_close(&mut self, _context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        let mut guard = self.state.lock().await;
        stop_source_runtime(&mut guard).await;
        Ok(())
    }
}
