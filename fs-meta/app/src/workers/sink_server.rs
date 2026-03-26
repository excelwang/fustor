use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use capanix_app_sdk::runtime::{ControlEnvelope, NodeId};
use capanix_app_sdk::{CnxError, Event};
use capanix_runtime_entry_sdk::advanced::boundary::{ChannelIoSubset, StateBoundary};
use capanix_runtime_entry_sdk::worker_runtime::{
    TypedWorkerBootstrapSession, TypedWorkerSession, WorkerLoopControl, WorkerSessionContext,
    run_worker_sidecar_server,
};
use tokio::sync::Mutex;

use crate::sink::SinkFileMeta;
use crate::runtime::orchestration::{SinkControlSignal, sink_control_signals_from_envelopes};
use crate::source::config::SourceConfig;
use crate::workers::sink::SinkWorkerRpc;
use crate::workers::sink_ipc::{
    SinkWorkerInitConfig, SinkWorkerRequest, SinkWorkerResponse, recv_opts,
};

struct SinkWorkerState {
    sink: Option<Arc<SinkFileMeta>>,
    node_id: Option<NodeId>,
    pending_init: Option<(NodeId, SinkWorkerInitConfig)>,
    endpoints_started: bool,
    send_tx: Option<tokio::sync::mpsc::UnboundedSender<Vec<Event>>>,
    last_control_frame_signals: Vec<String>,
}

enum SinkWorkerAction {
    Immediate(SinkWorkerResponse, bool),
    Send {
        sink: Arc<SinkFileMeta>,
        send_tx: Option<tokio::sync::mpsc::UnboundedSender<Vec<Event>>>,
        events: Vec<Event>,
    },
    Recv {
        sink: Arc<SinkFileMeta>,
        timeout_ms: Option<u64>,
        limit: Option<usize>,
    },
    OnControlFrame {
        sink: Arc<SinkFileMeta>,
        envelopes: Vec<ControlEnvelope>,
    },
}

fn classify_sink_worker_error(err: CnxError) -> SinkWorkerResponse {
    match err {
        CnxError::InvalidInput(message) => SinkWorkerResponse::InvalidInput(message),
        other => SinkWorkerResponse::Error(other.to_string()),
    }
}

fn bootstrap_not_ready() -> CnxError {
    CnxError::NotReady("worker not initialized".into())
}

fn debug_control_scope_capture_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
}

fn debug_sink_query_flow_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_SINK_QUERY_FLOW").is_some()
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

fn summarize_event_origins(events: &[Event]) -> Vec<String> {
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for event in events {
        *counts.entry(event.metadata().origin_id.0.clone()).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(origin, count)| format!("{origin}={count}"))
        .collect()
}

fn summarize_sink_snapshot_groups(snapshot: &crate::sink::SinkStatusSnapshot) -> Vec<String> {
    snapshot
        .groups
        .iter()
        .map(|group| {
            format!(
                "{}:total={} live={} audit={} init={}",
                group.group_id,
                group.total_nodes,
                group.live_nodes,
                group.overflow_pending_audit,
                group.initial_audit_completed
            )
        })
        .collect()
}

async fn stop_sink_runtime(state: &mut SinkWorkerState) {
    bootstrap_stop_sink_runtime(state).await;
    state.pending_init = None;
    state.last_control_frame_signals.clear();
}

async fn bootstrap_init_sink_runtime(
    node_id: NodeId,
    config: SinkWorkerInitConfig,
    state: &mut SinkWorkerState,
) -> Result<(), CnxError> {
    eprintln!(
        "fs_meta_sink_worker_server: bootstrap_init begin node={} roots={} grants={}",
        node_id.0,
        config.roots.len(),
        config.host_object_grants.len()
    );
    let _ = stop_sink_runtime(state).await;
    state.pending_init = Some((node_id.clone(), config));
    state.node_id = Some(node_id);
    state.endpoints_started = false;
    state.send_tx = None;
    state.last_control_frame_signals.clear();
    eprintln!("fs_meta_sink_worker_server: bootstrap_init ok");
    Ok(())
}

fn bootstrap_start_sink_runtime(
    state: &mut SinkWorkerState,
    io_boundary: Arc<dyn ChannelIoSubset>,
    state_boundary: Arc<dyn StateBoundary>,
) -> Result<(), CnxError> {
    match state.sink.as_ref() {
        Some(_) if state.endpoints_started => Ok(()),
        Some(_) | None => {
            if state.sink.is_none() {
                let Some((node_id, config)) = state.pending_init.clone() else {
                    return Err(bootstrap_not_ready());
                };
                let mut source_cfg = SourceConfig::default();
                source_cfg.roots = config.roots;
                source_cfg.host_object_grants = config.host_object_grants;
                source_cfg.sink_tombstone_ttl =
                    Duration::from_millis(config.sink_tombstone_ttl_ms.max(1));
                source_cfg.sink_tombstone_tolerance_us = config.sink_tombstone_tolerance_us;
                let inner = SinkFileMeta::with_boundaries_and_state_deferred_authority(
                    node_id.clone(),
                    None,
                    state_boundary,
                    source_cfg,
                )?;
                state.sink = Some(Arc::new(inner));
            }
            let Some(sink) = state.sink.as_ref() else {
                return Err(bootstrap_not_ready());
            };
            let Some(node_id) = state.node_id.clone() else {
                return Err(CnxError::Internal(
                    "sink worker missing node_id during start".into(),
                ));
            };
            eprintln!(
                "fs_meta_sink_worker_server: bootstrap_start begin node={} endpoints_started={}",
                node_id.0, state.endpoints_started
            );
            sink.start_runtime_endpoints(io_boundary, node_id)?;
            eprintln!("fs_meta_sink_worker_server: bootstrap_start endpoints ok");
            if state.send_tx.is_none() {
                let (send_tx, mut send_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<Event>>();
                let sink_for_send = sink.clone();
                tokio::spawn(async move {
                    while let Some(events) = send_rx.recv().await {
                        if debug_sink_query_flow_enabled() {
                            eprintln!(
                                "fs_meta_sink_worker_server: async_send_apply begin origins={:?}",
                                summarize_event_origins(&events)
                            );
                        }
                        if let Err(err) = sink_for_send.send(&events).await {
                            eprintln!("fs_meta_sink_worker: async Send apply failed: {err}");
                        } else if debug_sink_query_flow_enabled()
                            && let Ok(snapshot) = sink_for_send.status_snapshot()
                        {
                            eprintln!(
                                "fs_meta_sink_worker_server: async_send_apply ok groups={:?}",
                                summarize_sink_snapshot_groups(&snapshot)
                            );
                        }
                    }
                });
                state.send_tx = Some(send_tx);
            }
            state.endpoints_started = true;
            sink.enable_stream_receive();
            eprintln!("fs_meta_sink_worker_server: bootstrap_start ok");
            Ok(())
        }
    }
}

async fn bootstrap_stop_sink_runtime(state: &mut SinkWorkerState) {
    state.send_tx.take();
    if let Some(sink) = state.sink.as_ref() {
        let _ = sink.close().await;
    }
    state.sink = None;
    state.node_id = None;
    state.endpoints_started = false;
}

fn plan_worker_request(request: SinkWorkerRequest, state: &mut SinkWorkerState) -> SinkWorkerAction {
    match request {
        SinkWorkerRequest::UpdateLogicalRoots {
            roots,
            host_object_grants,
        } => match state.sink.as_ref() {
            Some(sink) => {
                eprintln!(
                    "fs_meta_sink_worker_server: update_logical_roots begin roots={} grants={}",
                    roots.len(),
                    host_object_grants.len()
                );
                match sink.update_logical_roots(roots, &host_object_grants) {
                    Ok(_) => {
                        eprintln!("fs_meta_sink_worker_server: update_logical_roots ok");
                        SinkWorkerAction::Immediate(SinkWorkerResponse::Ack, false)
                    }
                    Err(err) => {
                        eprintln!(
                            "fs_meta_sink_worker_server: update_logical_roots err={}",
                            err
                        );
                        SinkWorkerAction::Immediate(classify_sink_worker_error(err), false)
                    }
                }
            }
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::LogicalRootsSnapshot => match state.sink.as_ref() {
            Some(sink) => match sink.logical_roots_snapshot() {
                Ok(roots) => {
                    SinkWorkerAction::Immediate(SinkWorkerResponse::LogicalRoots(roots), false)
                }
                Err(err) => {
                    SinkWorkerAction::Immediate(SinkWorkerResponse::Error(err.to_string()), false)
                }
            },
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::ScheduledGroupIds => match state.sink.as_ref() {
            Some(sink) => match sink.scheduled_group_ids_snapshot() {
                Ok(groups) => SinkWorkerAction::Immediate(
                    SinkWorkerResponse::ScheduledGroupIds(
                        groups.map(|groups| groups.into_iter().collect()),
                    ),
                    false,
                ),
                Err(err) => {
                    SinkWorkerAction::Immediate(SinkWorkerResponse::Error(err.to_string()), false)
                }
            },
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Health => match state.sink.as_ref() {
            Some(sink) => match sink.health() {
                Ok(health) => {
                    SinkWorkerAction::Immediate(SinkWorkerResponse::Health(health), false)
                }
                Err(err) => {
                    SinkWorkerAction::Immediate(SinkWorkerResponse::Error(err.to_string()), false)
                }
            },
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::StatusSnapshot => match state.sink.as_ref() {
            Some(sink) => match sink.status_snapshot() {
                Ok(mut snapshot) => {
                    if let Some(node_id) = state.node_id.as_ref() {
                        snapshot.last_control_frame_signals_by_node =
                            if state.last_control_frame_signals.is_empty() {
                                std::collections::BTreeMap::new()
                            } else {
                                std::collections::BTreeMap::from([(
                                    node_id.0.clone(),
                                    state.last_control_frame_signals.clone(),
                                )])
                            };
                    }
                    SinkWorkerAction::Immediate(SinkWorkerResponse::StatusSnapshot(snapshot), false)
                }
                Err(err) => {
                    SinkWorkerAction::Immediate(SinkWorkerResponse::Error(err.to_string()), false)
                }
            },
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::VisibilityLagSamplesSince { since_us } => match state.sink.as_ref() {
            Some(sink) => SinkWorkerAction::Immediate(
                SinkWorkerResponse::VisibilityLagSamples(
                    sink.visibility_lag_samples_since(since_us),
                ),
                false,
            ),
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::MaterializedQuery { request } => match state.sink.as_ref() {
            Some(sink) => {
                if debug_sink_query_flow_enabled()
                    && let Ok(snapshot) = sink.status_snapshot()
                {
                    eprintln!(
                        "fs_meta_sink_worker_server: materialized_query begin selected_group={:?} path={} groups={:?}",
                        request.scope.selected_group,
                        String::from_utf8_lossy(&request.scope.path),
                        summarize_sink_snapshot_groups(&snapshot)
                    );
                }
                match sink.materialized_query(&request) {
                    Ok(events) => {
                        if debug_sink_query_flow_enabled() {
                            eprintln!(
                                "fs_meta_sink_worker_server: materialized_query ok selected_group={:?} path={} origins={:?}",
                                request.scope.selected_group,
                                String::from_utf8_lossy(&request.scope.path),
                                summarize_event_origins(&events)
                            );
                        }
                    SinkWorkerAction::Immediate(SinkWorkerResponse::Events(events), false)
                    }
                    Err(err) => {
                        SinkWorkerAction::Immediate(SinkWorkerResponse::Error(err.to_string()), false)
                    }
                }
            }
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Send { events } => match state.sink.clone() {
            Some(sink) => SinkWorkerAction::Send {
                sink,
                send_tx: state.send_tx.clone(),
                events,
            },
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Recv { timeout_ms, limit } => match state.sink.clone() {
            Some(sink) => SinkWorkerAction::Recv {
                sink,
                timeout_ms,
                limit,
            },
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::OnControlFrame { envelopes } => match state.sink.clone() {
            Some(sink) => {
                let node_id = state
                    .node_id
                    .as_ref()
                    .map(|node_id| node_id.0.as_str())
                    .unwrap_or("__missing_node_id__");
                let summary = match sink_control_signals_from_envelopes(&envelopes) {
                    Ok(signals) => summarize_sink_control_signals(&signals),
                    Err(err) => vec![format!("decode_err={err}")],
                };
                state.last_control_frame_signals = summary.clone();
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_sink_worker_server: on_control_frame summary node={} signals={:?}",
                        node_id,
                        summary
                    );
                }
                SinkWorkerAction::OnControlFrame { sink, envelopes }
            }
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
    }
}

async fn execute_worker_action(action: SinkWorkerAction) -> (SinkWorkerResponse, bool) {
    match action {
        SinkWorkerAction::Immediate(response, stop) => (response, stop),
        SinkWorkerAction::Send {
            sink,
            send_tx,
            events,
        } => match send_tx {
            Some(send_tx) => match send_tx.send(events) {
                Ok(()) => (SinkWorkerResponse::Ack, false),
                Err(err) => (
                    SinkWorkerResponse::Error(format!(
                        "sink worker send queue unavailable: {} event(s) dropped",
                        err.0.len()
                    )),
                    false,
                ),
            },
            None => match sink.send(&events).await {
                Ok(_) => (SinkWorkerResponse::Ack, false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
        },
        SinkWorkerAction::Recv {
            sink,
            timeout_ms,
            limit,
        } => match sink.recv(recv_opts(timeout_ms, limit)).await {
            Ok(events) => (SinkWorkerResponse::Events(events), false),
            Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
        },
        SinkWorkerAction::OnControlFrame { sink, envelopes } => {
            match sink.on_control_frame(&envelopes).await {
                Ok(_) => (SinkWorkerResponse::Ack, false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            }
        }
    }
}

pub fn run_sink_worker_server(
    control_socket_path: &Path,
    data_socket_path: &Path,
) -> std::io::Result<()> {
    let state = Arc::new(Mutex::new(SinkWorkerState {
        sink: None,
        node_id: None,
        pending_init: None,
        endpoints_started: false,
        send_tx: None,
        last_control_frame_signals: Vec::new(),
    }));
    run_worker_sidecar_server::<SinkWorkerRpc, _, SinkWorkerInitConfig>(
        control_socket_path,
        data_socket_path,
        SinkWorkerSession { state },
    )
}

struct SinkWorkerSession {
    state: Arc<Mutex<SinkWorkerState>>,
}

#[async_trait::async_trait]
impl TypedWorkerSession<SinkWorkerRpc> for SinkWorkerSession {
    async fn handle_request(
        &mut self,
        request: SinkWorkerRequest,
        _context: &WorkerSessionContext,
    ) -> capanix_app_sdk::Result<WorkerLoopControl<SinkWorkerResponse>> {
        let action = {
            let mut guard = self.state.lock().await;
            plan_worker_request(request, &mut guard)
        };
        let (response, stop) = execute_worker_action(action).await;
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
        let sink = {
            let guard = self.state.lock().await;
            guard.sink.clone()
        };
        let Some(sink) = sink else {
            return Err(CnxError::NotReady(
                "worker runtime not initialized for runtime control frames".into(),
            ));
        };
        sink.on_control_frame(envelopes).await
    }
}

#[async_trait::async_trait]
impl TypedWorkerBootstrapSession<SinkWorkerInitConfig> for SinkWorkerSession {
    async fn on_init(
        &mut self,
        node_id: NodeId,
        payload: SinkWorkerInitConfig,
        _context: &WorkerSessionContext,
    ) -> capanix_app_sdk::Result<()> {
        eprintln!(
            "fs_meta_sink_worker_server: on_init node={} roots={} grants={}",
            node_id.0,
            payload.roots.len(),
            payload.host_object_grants.len()
        );
        let mut guard = self.state.lock().await;
        bootstrap_init_sink_runtime(node_id, payload, &mut guard).await
    }

    async fn on_start(&mut self, _context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        eprintln!("fs_meta_sink_worker_server: on_start begin");
        let mut guard = self.state.lock().await;
        let result = bootstrap_start_sink_runtime(
            &mut guard,
            _context.io_boundary(),
            _context.state_boundary(),
        );
        eprintln!(
            "fs_meta_sink_worker_server: on_start done ok={}",
            result.is_ok()
        );
        result
    }

    async fn on_ping(&mut self, _context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        eprintln!("fs_meta_sink_worker_server: on_ping begin");
        let guard = self.state.lock().await;
        if guard.sink.is_some() {
            eprintln!("fs_meta_sink_worker_server: on_ping ok");
            Ok(())
        } else {
            eprintln!("fs_meta_sink_worker_server: on_ping not_ready");
            Err(bootstrap_not_ready())
        }
    }

    async fn on_close(&mut self, _context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        let mut guard = self.state.lock().await;
        stop_sink_runtime(&mut guard).await;
        Ok(())
    }
}
