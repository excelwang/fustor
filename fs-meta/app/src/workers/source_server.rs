use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
use crate::runtime::orchestration::{SourceControlSignal, source_control_signals_from_envelopes};
use crate::workers::source::SourceObservabilitySnapshot;
use crate::workers::source::SourceWorkerRpc;
use crate::workers::source_ipc::{SourceWorkerRequest, SourceWorkerResponse};

const ROUTE_KEY_EVENTS: &str = "fs-meta.events:v1";
const SOURCE_WORKER_STOP_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const SOURCE_WORKER_STOP_ABORT_TIMEOUT: Duration = Duration::from_millis(250);

struct SourceWorkerState {
    source: Option<Arc<FSMetaSource>>,
    pending_init: Option<(NodeId, SourceConfig)>,
    pump_task: Option<JoinHandle<()>>,
    last_control_frame_signals: Vec<String>,
}

enum SourceWorkerAction {
    Immediate(SourceWorkerResponse, bool),
    UpdateLogicalRoots {
        source: Arc<FSMetaSource>,
        roots: Vec<crate::source::config::RootSpec>,
    },
    PublishManualRescanSignal {
        source: Arc<FSMetaSource>,
    },
    OnControlFrame {
        source: Arc<FSMetaSource>,
        envelopes: Vec<ControlEnvelope>,
    },
}

fn next_source_worker_request_seq() -> u64 {
    static NEXT_SEQ: AtomicU64 = AtomicU64::new(1);
    NEXT_SEQ.fetch_add(1, Ordering::Relaxed)
}

fn source_worker_request_label(request: &SourceWorkerRequest) -> &'static str {
    match request {
        SourceWorkerRequest::UpdateLogicalRoots { .. } => "UpdateLogicalRoots",
        SourceWorkerRequest::LogicalRootsSnapshot => "LogicalRootsSnapshot",
        SourceWorkerRequest::HostObjectGrantsSnapshot => "HostObjectGrantsSnapshot",
        SourceWorkerRequest::HostObjectGrantsVersionSnapshot => "HostObjectGrantsVersionSnapshot",
        SourceWorkerRequest::StatusSnapshot => "StatusSnapshot",
        SourceWorkerRequest::ObservabilitySnapshot => "ObservabilitySnapshot",
        SourceWorkerRequest::LifecycleState => "LifecycleState",
        SourceWorkerRequest::ScheduledSourceGroupIds => "ScheduledSourceGroupIds",
        SourceWorkerRequest::ScheduledScanGroupIds => "ScheduledScanGroupIds",
        SourceWorkerRequest::SourcePrimaryByGroupSnapshot => "SourcePrimaryByGroupSnapshot",
        SourceWorkerRequest::LastForceFindRunnerByGroupSnapshot => {
            "LastForceFindRunnerByGroupSnapshot"
        }
        SourceWorkerRequest::ForceFindInflightGroupsSnapshot => "ForceFindInflightGroupsSnapshot",
        SourceWorkerRequest::ForceFind { .. } => "ForceFind",
        SourceWorkerRequest::ResolveGroupIdForObjectRef { .. } => "ResolveGroupIdForObjectRef",
        SourceWorkerRequest::PublishManualRescanSignal => "PublishManualRescanSignal",
        SourceWorkerRequest::TriggerRescanWhenReady => "TriggerRescanWhenReady",
        SourceWorkerRequest::OnControlFrame { .. } => "OnControlFrame",
    }
}

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

fn summarize_source_control_signals(signals: &[SourceControlSignal]) -> Vec<String> {
    signals
        .iter()
        .map(|signal| match signal {
            SourceControlSignal::Activate {
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
            SourceControlSignal::Deactivate {
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
            SourceControlSignal::Tick {
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
            SourceControlSignal::RuntimeHostGrantChange { .. } => "host_grant_change".into(),
            SourceControlSignal::ManualRescan { .. } => "manual_rescan".into(),
            SourceControlSignal::Passthrough(_) => "passthrough".into(),
        })
        .collect()
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
    state.last_control_frame_signals.clear();
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
        futures_util::pin_mut!(stream);
        while let Some(batch) = stream.next().await {
            let origin = batch
                .first()
                .map(|event| event.metadata().origin_id.0.clone())
                .unwrap_or_else(|| "__empty__".to_string());
            if let Err(err) = boundary
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
                    "source worker pump failed to publish source batch on stream route origin={}: {:?}",
                    origin,
                    err
                );
                break;
            }
        }
    })
}

fn bootstrap_not_ready() -> CnxError {
    CnxError::NotReady("worker not initialized".into())
}

fn last_control_frame_signals_by_node(
    node_id: &NodeId,
    signals: &[String],
) -> std::collections::BTreeMap<String, Vec<String>> {
    if signals.is_empty() {
        return std::collections::BTreeMap::new();
    }
    std::collections::BTreeMap::from([(node_id.0.clone(), signals.to_vec())])
}

fn source_observability_snapshot(
    source: &FSMetaSource,
    last_control_frame_signals: &[String],
) -> SourceObservabilitySnapshot {
    let node_id = source.node_id();
    SourceObservabilitySnapshot {
        lifecycle_state: format!("{:?}", source.state()).to_ascii_lowercase(),
        host_object_grants_version: source.host_object_grants_version_snapshot(),
        grants: source.host_object_grants_snapshot(),
        logical_roots: source.logical_roots_snapshot(),
        status: source.status_snapshot(),
        source_primary_by_group: source.source_primary_by_group_snapshot(),
        last_force_find_runner_by_group: source.last_force_find_runner_by_group_snapshot(),
        force_find_inflight_groups: source.force_find_inflight_groups_snapshot(),
        scheduled_source_groups_by_node: source
            .scheduled_source_group_ids()
            .ok()
            .flatten()
            .filter(|groups| !groups.is_empty())
            .map(|groups| {
                std::collections::BTreeMap::from([(node_id.0.clone(), groups.into_iter().collect())])
            })
            .unwrap_or_default(),
        scheduled_scan_groups_by_node: source
            .scheduled_scan_group_ids()
            .ok()
            .flatten()
            .filter(|groups| !groups.is_empty())
            .map(|groups| {
                std::collections::BTreeMap::from([(node_id.0.clone(), groups.into_iter().collect())])
            })
            .unwrap_or_default(),
        last_control_frame_signals_by_node: last_control_frame_signals_by_node(
            &node_id,
            last_control_frame_signals,
        ),
    }
}

async fn bootstrap_init_source_runtime(
    node_id: NodeId,
    config: SourceConfig,
    state: &mut SourceWorkerState,
) {
    eprintln!(
        "fs_meta_source_worker_server: bootstrap_init begin node={} roots={} grants={}",
        node_id.0,
        config.roots.len(),
        config.host_object_grants.len()
    );
    let _ = stop_source_runtime(state).await;
    state.pending_init = Some((node_id, config));
    state.last_control_frame_signals.clear();
    eprintln!("fs_meta_source_worker_server: bootstrap_init ok");
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
        eprintln!(
            "fs_meta_source_worker_server: bootstrap_start build_source begin node={} roots={} grants={}",
            node_id.0,
            config.roots.len(),
            config.host_object_grants.len()
        );
        match FSMetaSource::with_boundaries_and_state(config, node_id, None, state_boundary) {
            Ok(inner) => {
                state.source = Some(Arc::new(inner));
                eprintln!("fs_meta_source_worker_server: bootstrap_start build_source ok");
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
        eprintln!("fs_meta_source_worker_server: bootstrap_start endpoints begin");
        source.start_runtime_endpoints(boundary.clone()).await?;
        eprintln!("fs_meta_source_worker_server: bootstrap_start endpoints ok");
        let source = source.clone();
        eprintln!("fs_meta_source_worker_server: bootstrap_start pub begin");
        let stream = source.pub_().await?;
        eprintln!("fs_meta_source_worker_server: bootstrap_start pub ok");
        state.pump_task = Some(start_source_pump_with_stream(stream, boundary));
        eprintln!("fs_meta_source_worker_server: bootstrap_start pump ok");
    }
    eprintln!("fs_meta_source_worker_server: bootstrap_start ok");
    Ok(())
}

fn plan_worker_request(
    request: SourceWorkerRequest,
    state: &mut SourceWorkerState,
) -> SourceWorkerAction {
    match request {
        SourceWorkerRequest::UpdateLogicalRoots { roots } => match state.source.clone() {
            Some(source) => SourceWorkerAction::UpdateLogicalRoots { source, roots },
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::LogicalRootsSnapshot => match state.source.as_ref() {
            Some(source) => SourceWorkerAction::Immediate(
                SourceWorkerResponse::LogicalRoots(source.logical_roots_snapshot()),
                false,
            ),
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::HostObjectGrantsSnapshot => match state.source.as_ref() {
            Some(source) => SourceWorkerAction::Immediate(
                SourceWorkerResponse::HostObjectGrants(source.host_object_grants_snapshot()),
                false,
            ),
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::HostObjectGrantsVersionSnapshot => match state.source.as_ref() {
            Some(source) => SourceWorkerAction::Immediate(
                SourceWorkerResponse::HostObjectGrantsVersion(
                    source.host_object_grants_version_snapshot(),
                ),
                false,
            ),
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::StatusSnapshot => match state.source.as_ref() {
            Some(source) => SourceWorkerAction::Immediate(
                SourceWorkerResponse::StatusSnapshot(source.status_snapshot()),
                false,
            ),
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ObservabilitySnapshot => match state.source.as_ref() {
            Some(source) => SourceWorkerAction::Immediate(
                SourceWorkerResponse::ObservabilitySnapshot(source_observability_snapshot(
                    source,
                    &state.last_control_frame_signals,
                )),
                false,
            ),
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::LifecycleState => match state.source.as_ref() {
            Some(source) => SourceWorkerAction::Immediate(
                SourceWorkerResponse::LifecycleState(
                    format!("{:?}", source.state()).to_ascii_lowercase(),
                ),
                false,
            ),
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ScheduledSourceGroupIds => match state.source.as_ref() {
            Some(source) => match source.scheduled_source_group_ids() {
                Ok(groups) => SourceWorkerAction::Immediate(
                    SourceWorkerResponse::ScheduledGroupIds(
                        groups.map(|group_ids| group_ids.into_iter().collect()),
                    ),
                    false,
                ),
                Err(err) => SourceWorkerAction::Immediate(
                    SourceWorkerResponse::Error(err.to_string()),
                    false,
                ),
            },
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ScheduledScanGroupIds => match state.source.as_ref() {
            Some(source) => match source.scheduled_scan_group_ids() {
                Ok(groups) => SourceWorkerAction::Immediate(
                    SourceWorkerResponse::ScheduledGroupIds(
                        groups.map(|group_ids| group_ids.into_iter().collect()),
                    ),
                    false,
                ),
                Err(err) => SourceWorkerAction::Immediate(
                    SourceWorkerResponse::Error(err.to_string()),
                    false,
                ),
            },
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::SourcePrimaryByGroupSnapshot => match state.source.as_ref() {
            Some(source) => SourceWorkerAction::Immediate(
                SourceWorkerResponse::SourcePrimaryByGroup(
                    source.source_primary_by_group_snapshot(),
                ),
                false,
            ),
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::LastForceFindRunnerByGroupSnapshot => match state.source.as_ref() {
            Some(source) => SourceWorkerAction::Immediate(
                SourceWorkerResponse::LastForceFindRunnerByGroup(
                    source.last_force_find_runner_by_group_snapshot(),
                ),
                false,
            ),
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ForceFindInflightGroupsSnapshot => match state.source.as_ref() {
            Some(source) => SourceWorkerAction::Immediate(
                SourceWorkerResponse::ForceFindInflightGroups(
                    source.force_find_inflight_groups_snapshot(),
                ),
                false,
            ),
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ForceFind { request } => match state.source.as_ref() {
            Some(source) => match source.force_find(&request) {
                Ok(events) => {
                    SourceWorkerAction::Immediate(SourceWorkerResponse::Events(events), false)
                }
                Err(err) => SourceWorkerAction::Immediate(
                    SourceWorkerResponse::Error(err.to_string()),
                    false,
                ),
            },
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ResolveGroupIdForObjectRef { object_ref } => {
            match state.source.as_ref() {
                Some(source) => SourceWorkerAction::Immediate(
                    SourceWorkerResponse::ResolveGroupIdForObjectRef(
                        source.resolve_group_id_for_object_ref(&object_ref),
                    ),
                    false,
                ),
                None => SourceWorkerAction::Immediate(
                    SourceWorkerResponse::Error("worker not initialized".into()),
                    false,
                ),
            }
        }
        SourceWorkerRequest::PublishManualRescanSignal => match state.source.clone() {
            Some(source) => SourceWorkerAction::PublishManualRescanSignal { source },
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::TriggerRescanWhenReady => match state.source.clone() {
            Some(source) => {
                tokio::spawn(async move {
                    source.trigger_rescan_when_ready().await;
                });
                SourceWorkerAction::Immediate(SourceWorkerResponse::Ack, false)
            }
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::OnControlFrame { envelopes } => match state.source.clone() {
            Some(source) => {
                let summary = match source_control_signals_from_envelopes(&envelopes) {
                    Ok(signals) => summarize_source_control_signals(&signals),
                    Err(err) => vec![format!("decode_err={err}")],
                };
                state.last_control_frame_signals = summary.clone();
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_source_worker_server: on_control_frame summary node={} signals={:?}",
                        source.node_id().0,
                        summary
                    );
                }
                SourceWorkerAction::OnControlFrame { source, envelopes }
            }
            None => SourceWorkerAction::Immediate(
                SourceWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
    }
}

async fn execute_worker_action(action: SourceWorkerAction) -> (SourceWorkerResponse, bool) {
    match action {
        SourceWorkerAction::Immediate(response, stop) => (response, stop),
        SourceWorkerAction::UpdateLogicalRoots { source, roots } => {
            eprintln!(
                "fs_meta_source_worker_server: update_logical_roots begin roots={}",
                roots.len()
            );
            match source.update_logical_roots(roots).await {
                Ok(_) => {
                    eprintln!("fs_meta_source_worker_server: update_logical_roots ok");
                    (SourceWorkerResponse::Ack, false)
                }
                Err(err) => {
                    eprintln!(
                        "fs_meta_source_worker_server: update_logical_roots err={}",
                        err
                    );
                    (classify_source_worker_error(err), false)
                }
            }
        }
        SourceWorkerAction::PublishManualRescanSignal { source } => {
            match source.publish_manual_rescan_signal().await {
                Ok(()) => (SourceWorkerResponse::Ack, false),
                Err(err) => (classify_source_worker_error(err), false),
            }
        }
        SourceWorkerAction::OnControlFrame { source, envelopes } => {
            match source.on_control_frame(&envelopes).await {
                Ok(_) => (SourceWorkerResponse::Ack, false),
                Err(err) => (SourceWorkerResponse::Error(err.to_string()), false),
            }
        }
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
        last_control_frame_signals: Vec::new(),
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
        let request_seq = next_source_worker_request_seq();
        let request_label = source_worker_request_label(&request);
        eprintln!(
            "fs_meta_source_worker_server: handle_request begin seq={} request={}",
            request_seq, request_label
        );
        let action = {
            let mut guard = self.state.lock().await;
            plan_worker_request(request, &mut guard)
        };
        let (response, stop) = execute_worker_action(action).await;
        eprintln!(
            "fs_meta_source_worker_server: handle_request done seq={} request={} stop={}",
            request_seq, request_label, stop
        );
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
        eprintln!(
            "fs_meta_source_worker_server: on_runtime_control begin envelopes={}",
            envelopes.len()
        );
        let source = {
            let guard = self.state.lock().await;
            guard.source.clone()
        };
        let Some(source) = source else {
            return Err(CnxError::NotReady(
                "worker runtime not initialized for runtime control frames".into(),
            ));
        };
        let result = source.on_control_frame(envelopes).await;
        eprintln!(
            "fs_meta_source_worker_server: on_runtime_control done envelopes={} ok={}",
            envelopes.len(),
            result.is_ok()
        );
        result
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
        eprintln!(
            "fs_meta_source_worker_server: on_init node={} roots={} grants={}",
            node_id.0,
            payload.roots.len(),
            payload.host_object_grants.len()
        );
        let mut guard = self.state.lock().await;
        bootstrap_init_source_runtime(node_id, payload, &mut guard).await;
        Ok(())
    }

    async fn on_start(&mut self, context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        eprintln!("fs_meta_source_worker_server: on_start begin");
        let mut guard = self.state.lock().await;
        let result = bootstrap_start_source_runtime(
            &mut guard,
            context.io_boundary(),
            context.state_boundary(),
        )
        .await;
        eprintln!(
            "fs_meta_source_worker_server: on_start done ok={}",
            result.is_ok()
        );
        result
    }

    async fn on_ping(&mut self, _context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        eprintln!("fs_meta_source_worker_server: on_ping begin");
        let guard = self.state.lock().await;
        if guard.source.is_some() {
            eprintln!("fs_meta_source_worker_server: on_ping ok");
            Ok(())
        } else {
            eprintln!("fs_meta_source_worker_server: on_ping not_ready");
            Err(bootstrap_not_ready())
        }
    }

    async fn on_close(&mut self, _context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        let mut guard = self.state.lock().await;
        stop_source_runtime(&mut guard).await;
        Ok(())
    }
}
