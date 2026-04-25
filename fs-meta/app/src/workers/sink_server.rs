use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use capanix_app_sdk::runtime::{ControlEnvelope, NodeId};
use capanix_app_sdk::{CnxError, Event};
use capanix_runtime_entry_sdk::advanced::boundary::{ChannelIoSubset, StateBoundary};
use capanix_runtime_entry_sdk::worker_runtime::{
    TypedWorkerBootstrapSession, TypedWorkerSession, WorkerLoopControl, WorkerSessionContext,
    run_worker_sidecar_server,
};
use tokio::sync::Mutex;

use crate::runtime::orchestration::{SinkControlSignal, sink_control_signals_from_envelopes};
use crate::runtime::routes::ROUTE_KEY_SINK_QUERY_INTERNAL;
use crate::sink::SinkFileMeta;
use crate::source::config::SourceConfig;
use crate::workers::sink::SinkFailure;
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
    received_stats: Arc<StdMutex<ReceivedBatchStats>>,
}

enum SinkWorkerAction {
    Immediate(SinkWorkerResponse, bool),
    Send {
        sink: Arc<SinkFileMeta>,
        send_tx: Option<tokio::sync::mpsc::UnboundedSender<Vec<Event>>>,
        events: Vec<Event>,
        received_stats: Arc<StdMutex<ReceivedBatchStats>>,
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

#[derive(Default)]
struct ReceivedBatchStats {
    batch_count: u64,
    event_count: u64,
    control_event_count: u64,
    data_event_count: u64,
    last_received_at_us: Option<u64>,
    last_received_origins: Vec<String>,
    received_origin_counts: std::collections::BTreeMap<String, u64>,
}

struct ReceivedBatchUpdate {
    event_count: u64,
    control_event_count: u64,
    data_event_count: u64,
    last_received_at_us: Option<u64>,
    last_received_origins: Vec<String>,
    received_origin_counts: std::collections::BTreeMap<String, u64>,
}

fn classify_sink_worker_error(err: CnxError) -> SinkWorkerResponse {
    match err {
        CnxError::InvalidInput(message) => SinkWorkerResponse::InvalidInput(message),
        other => SinkWorkerResponse::Error(other.to_string()),
    }
}

fn classify_sink_worker_failure(err: SinkFailure) -> SinkWorkerResponse {
    classify_sink_worker_error(err.into_error())
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

fn debug_sink_query_route_trace_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_SINK_QUERY_ROUTE_TRACE").is_some()
}

fn is_per_peer_sink_query_request_route(route_key: &str) -> bool {
    let Some(request_route) = route_key.strip_suffix(".req") else {
        return false;
    };
    let Some((stem, version)) = ROUTE_KEY_SINK_QUERY_INTERNAL.rsplit_once(':') else {
        return request_route.starts_with(&format!("{ROUTE_KEY_SINK_QUERY_INTERNAL}."));
    };
    let Some(route_stem) = request_route.strip_suffix(&format!(":{version}")) else {
        return false;
    };
    route_stem.starts_with(&format!("{stem}."))
}

fn traced_per_peer_sink_query_routes(signals: &[SinkControlSignal]) -> Vec<String> {
    signals
        .iter()
        .filter_map(|signal| match signal {
            SinkControlSignal::Activate { route_key, .. }
            | SinkControlSignal::Deactivate { route_key, .. }
                if is_per_peer_sink_query_request_route(route_key) =>
            {
                Some(route_key.clone())
            }
            _ => None,
        })
        .collect()
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
        *counts
            .entry(event.metadata().origin_id.0.clone())
            .or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(origin, count)| format!("{origin}={count}"))
        .collect()
}

fn lock_received_stats<'a>(
    stats: &'a Arc<StdMutex<ReceivedBatchStats>>,
) -> std::sync::MutexGuard<'a, ReceivedBatchStats> {
    match stats.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            log::warn!("sink worker received stats lock poisoned; recovering state");
            poisoned.into_inner()
        }
    }
}

fn summarize_received_batch(batch: &[Event]) -> ReceivedBatchUpdate {
    let mut control = 0u64;
    let mut data = 0u64;
    let mut last_received_at_us = None::<u64>;
    for event in batch {
        last_received_at_us = Some(
            last_received_at_us
                .map(|current| current.max(event.metadata().timestamp_us))
                .unwrap_or(event.metadata().timestamp_us),
        );
        if rmp_serde::from_slice::<crate::ControlEvent>(event.payload_bytes()).is_ok() {
            control += 1;
        } else {
            data += 1;
        }
    }
    ReceivedBatchUpdate {
        event_count: batch.len() as u64,
        control_event_count: control,
        data_event_count: data,
        last_received_at_us,
        last_received_origins: summarize_event_origins(batch),
        received_origin_counts: batch.iter().fold(
            std::collections::BTreeMap::<String, u64>::new(),
            |mut acc, event| {
                *acc.entry(event.metadata().origin_id.0.clone()).or_default() += 1;
                acc
            },
        ),
    }
}

fn update_received_stats(stats: &Arc<StdMutex<ReceivedBatchStats>>, update: &ReceivedBatchUpdate) {
    let mut guard = lock_received_stats(stats);
    guard.batch_count = guard.batch_count.saturating_add(1);
    guard.event_count = guard.event_count.saturating_add(update.event_count);
    guard.control_event_count = guard
        .control_event_count
        .saturating_add(update.control_event_count);
    guard.data_event_count = guard
        .data_event_count
        .saturating_add(update.data_event_count);
    guard.last_received_at_us = update.last_received_at_us;
    guard.last_received_origins = update.last_received_origins.clone();
    for (origin, count) in &update.received_origin_counts {
        *guard
            .received_origin_counts
            .entry(origin.clone())
            .or_default() += *count;
    }
}

fn received_stats_snapshot(
    stats: &Arc<StdMutex<ReceivedBatchStats>>,
) -> (u64, u64, u64, u64, Option<u64>, Vec<String>, Vec<String>) {
    let guard = lock_received_stats(stats);
    (
        guard.batch_count,
        guard.event_count,
        guard.control_event_count,
        guard.data_event_count,
        guard.last_received_at_us,
        guard.last_received_origins.clone(),
        guard
            .received_origin_counts
            .iter()
            .map(|(origin, count)| format!("{origin}={count}"))
            .collect(),
    )
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
                group.overflow_pending_materialization,
                group.is_ready()
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
    *lock_received_stats(&state.received_stats) = ReceivedBatchStats::default();
    eprintln!("fs_meta_sink_worker_server: bootstrap_init ok");
    Ok(())
}

fn bootstrap_start_sink_runtime_with_failure(
    state: &mut SinkWorkerState,
    io_boundary: Arc<dyn ChannelIoSubset>,
    state_boundary: Arc<dyn StateBoundary>,
) -> std::result::Result<(), SinkFailure> {
    if state.sink.is_none() {
        let Some((node_id, config)) = state.pending_init.clone() else {
            return Err(SinkFailure::from(bootstrap_not_ready()));
        };
        let mut source_cfg = SourceConfig::default();
        source_cfg.roots = config.roots;
        source_cfg.host_object_grants = config.host_object_grants;
        source_cfg.sink_tombstone_ttl = Duration::from_millis(config.sink_tombstone_ttl_ms.max(1));
        source_cfg.sink_tombstone_tolerance_us = config.sink_tombstone_tolerance_us;
        let inner = SinkFileMeta::with_boundaries_and_state_deferred_authority_with_failure(
            node_id.clone(),
            None,
            state_boundary,
            source_cfg,
        )?;
        state.sink = Some(Arc::new(inner));
    }
    let Some(sink) = state.sink.as_ref() else {
        return Err(SinkFailure::from(bootstrap_not_ready()));
    };
    let Some(node_id) = state.node_id.clone() else {
        return Err(SinkFailure::from(CnxError::Internal(
            "sink worker missing node_id during start".into(),
        )));
    };
    eprintln!(
        "fs_meta_sink_worker_server: bootstrap_start begin node={} endpoints_started={}",
        node_id.0, state.endpoints_started
    );
    sink.start_runtime_endpoints_with_failure(io_boundary, node_id)?;
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
                if let Err(err) = sink_for_send.send_with_failure(&events).await {
                    eprintln!(
                        "fs_meta_sink_worker: async Send apply failed: {}",
                        err.as_error()
                    );
                } else if debug_sink_query_flow_enabled()
                    && let Ok(snapshot) = sink_for_send.status_snapshot_with_failure()
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

fn bootstrap_start_sink_runtime(
    state: &mut SinkWorkerState,
    io_boundary: Arc<dyn ChannelIoSubset>,
    state_boundary: Arc<dyn StateBoundary>,
) -> Result<(), CnxError> {
    bootstrap_start_sink_runtime_with_failure(state, io_boundary, state_boundary)
        .map_err(SinkFailure::into_error)
}

async fn bootstrap_stop_sink_runtime(state: &mut SinkWorkerState) {
    state.send_tx.take();
    if let Some(sink) = state.sink.as_ref() {
        let _ = sink.close_with_failure().await;
    }
    state.sink = None;
    state.node_id = None;
    state.endpoints_started = false;
}

fn plan_worker_request(
    request: SinkWorkerRequest,
    state: &mut SinkWorkerState,
) -> SinkWorkerAction {
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
                match sink.update_logical_roots_with_failure(roots, &host_object_grants) {
                    Ok(_) => {
                        eprintln!("fs_meta_sink_worker_server: update_logical_roots ok");
                        SinkWorkerAction::Immediate(SinkWorkerResponse::Ack, false)
                    }
                    Err(err) => {
                        eprintln!(
                            "fs_meta_sink_worker_server: update_logical_roots err={}",
                            err.as_error()
                        );
                        SinkWorkerAction::Immediate(classify_sink_worker_failure(err), false)
                    }
                }
            }
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::LogicalRootsSnapshot => match state.sink.as_ref() {
            Some(sink) => match sink.logical_roots_snapshot_with_failure() {
                Ok(roots) => {
                    SinkWorkerAction::Immediate(SinkWorkerResponse::LogicalRoots(roots), false)
                }
                Err(err) => SinkWorkerAction::Immediate(classify_sink_worker_failure(err), false),
            },
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::ScheduledGroupIds => match state.sink.as_ref() {
            Some(sink) => match sink.scheduled_group_ids_snapshot_with_failure() {
                Ok(groups) => SinkWorkerAction::Immediate(
                    SinkWorkerResponse::ScheduledGroupIds(
                        groups.map(|groups| groups.into_iter().collect()),
                    ),
                    false,
                ),
                Err(err) => SinkWorkerAction::Immediate(classify_sink_worker_failure(err), false),
            },
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Health => match state.sink.as_ref() {
            Some(sink) => match sink.health_with_failure() {
                Ok(health) => {
                    SinkWorkerAction::Immediate(SinkWorkerResponse::Health(health), false)
                }
                Err(err) => SinkWorkerAction::Immediate(classify_sink_worker_failure(err), false),
            },
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::StatusSnapshot => match state.sink.as_ref() {
            Some(sink) => match sink.status_snapshot_with_failure() {
                Ok(mut snapshot) => {
                    if let Some(node_id) = state.node_id.as_ref() {
                        let (
                            received_batches,
                            received_events,
                            received_control,
                            received_data,
                            last_received_at_us,
                            last_received_origins,
                            received_origin_counts,
                        ) = received_stats_snapshot(&state.received_stats);
                        snapshot.last_control_frame_signals_by_node =
                            if state.last_control_frame_signals.is_empty() {
                                std::collections::BTreeMap::new()
                            } else {
                                std::collections::BTreeMap::from([(
                                    node_id.0.clone(),
                                    state.last_control_frame_signals.clone(),
                                )])
                            };
                        snapshot.received_batches_by_node = if received_batches == 0 {
                            std::collections::BTreeMap::new()
                        } else {
                            std::collections::BTreeMap::from([(
                                node_id.0.clone(),
                                received_batches,
                            )])
                        };
                        snapshot.received_events_by_node = if received_events == 0 {
                            std::collections::BTreeMap::new()
                        } else {
                            std::collections::BTreeMap::from([(node_id.0.clone(), received_events)])
                        };
                        snapshot.received_control_events_by_node = if received_control == 0 {
                            std::collections::BTreeMap::new()
                        } else {
                            std::collections::BTreeMap::from([(
                                node_id.0.clone(),
                                received_control,
                            )])
                        };
                        snapshot.received_data_events_by_node = if received_data == 0 {
                            std::collections::BTreeMap::new()
                        } else {
                            std::collections::BTreeMap::from([(node_id.0.clone(), received_data)])
                        };
                        snapshot.last_received_at_us_by_node = last_received_at_us
                            .map(|value| {
                                std::collections::BTreeMap::from([(node_id.0.clone(), value)])
                            })
                            .unwrap_or_default();
                        snapshot.last_received_origins_by_node = if last_received_origins.is_empty()
                        {
                            std::collections::BTreeMap::new()
                        } else {
                            std::collections::BTreeMap::from([(
                                node_id.0.clone(),
                                last_received_origins,
                            )])
                        };
                        snapshot.received_origin_counts_by_node =
                            if received_origin_counts.is_empty() {
                                std::collections::BTreeMap::new()
                            } else {
                                std::collections::BTreeMap::from([(
                                    node_id.0.clone(),
                                    received_origin_counts,
                                )])
                            };
                    }
                    SinkWorkerAction::Immediate(SinkWorkerResponse::StatusSnapshot(snapshot), false)
                }
                Err(err) => SinkWorkerAction::Immediate(classify_sink_worker_failure(err), false),
            },
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::VisibilityLagSamplesSince { since_us } => match state.sink.as_ref() {
            Some(sink) => match sink.visibility_lag_samples_since_with_failure(since_us) {
                Ok(samples) => SinkWorkerAction::Immediate(
                    SinkWorkerResponse::VisibilityLagSamples(samples),
                    false,
                ),
                Err(err) => SinkWorkerAction::Immediate(classify_sink_worker_failure(err), false),
            },
            None => SinkWorkerAction::Immediate(
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::MaterializedQuery { request } => match state.sink.as_ref() {
            Some(sink) => {
                if debug_sink_query_flow_enabled()
                    && let Ok(snapshot) = sink.status_snapshot_with_failure()
                {
                    eprintln!(
                        "fs_meta_sink_worker_server: materialized_query begin selected_group={:?} path={} groups={:?}",
                        request.scope.selected_group,
                        String::from_utf8_lossy(&request.scope.path),
                        summarize_sink_snapshot_groups(&snapshot)
                    );
                }
                match sink.materialized_query_with_failure(&request) {
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
                        SinkWorkerAction::Immediate(classify_sink_worker_failure(err), false)
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
                received_stats: state.received_stats.clone(),
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
                        node_id, summary
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
            received_stats,
        } => match send_tx {
            Some(send_tx) => {
                let update = summarize_received_batch(&events);
                match send_tx.send(events) {
                    Ok(()) => {
                        update_received_stats(&received_stats, &update);
                        (SinkWorkerResponse::Ack, false)
                    }
                    Err(err) => (
                        SinkWorkerResponse::Error(format!(
                            "sink worker send queue unavailable: {} event(s) dropped",
                            err.0.len()
                        )),
                        false,
                    ),
                }
            }
            None => {
                let update = summarize_received_batch(&events);
                match sink.send_with_failure(&events).await {
                    Ok(_) => {
                        update_received_stats(&received_stats, &update);
                        (SinkWorkerResponse::Ack, false)
                    }
                    Err(err) => (classify_sink_worker_failure(err), false),
                }
            }
        },
        SinkWorkerAction::Recv {
            sink,
            timeout_ms,
            limit,
        } => match sink.recv_with_failure(recv_opts(timeout_ms, limit)).await {
            Ok(events) => (SinkWorkerResponse::Events(events), false),
            Err(err) => (classify_sink_worker_failure(err), false),
        },
        SinkWorkerAction::OnControlFrame { sink, envelopes } => {
            let traced_routes = if debug_sink_query_route_trace_enabled() {
                sink_control_signals_from_envelopes(&envelopes)
                    .ok()
                    .map(|signals| traced_per_peer_sink_query_routes(&signals))
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            if debug_sink_query_route_trace_enabled() && !traced_routes.is_empty() {
                let route_states: Vec<String> = traced_routes
                    .iter()
                    .filter_map(|route| sink.debug_traced_route_state(route).ok())
                    .collect();
                eprintln!(
                    "fs_meta_sink_worker_server: traced_on_control_frame begin routes={:?} states={:?}",
                    traced_routes, route_states
                );
            }
            match sink.on_control_frame_with_failure(&envelopes).await {
                Ok(_) => {
                    if debug_sink_query_route_trace_enabled() && !traced_routes.is_empty() {
                        let route_states: Vec<String> = traced_routes
                            .iter()
                            .filter_map(|route| sink.debug_traced_route_state(route).ok())
                            .collect();
                        eprintln!(
                            "fs_meta_sink_worker_server: traced_on_control_frame done routes={:?} ok=true states={:?}",
                            traced_routes, route_states
                        );
                    }
                    (SinkWorkerResponse::Ack, false)
                }
                Err(err) => {
                    if debug_sink_query_route_trace_enabled() && !traced_routes.is_empty() {
                        let route_states: Vec<String> = traced_routes
                            .iter()
                            .filter_map(|route| sink.debug_traced_route_state(route).ok())
                            .collect();
                        eprintln!(
                            "fs_meta_sink_worker_server: traced_on_control_frame done routes={:?} ok=false err={} states={:?}",
                            traced_routes,
                            err.as_error(),
                            route_states
                        );
                    }
                    (classify_sink_worker_failure(err), false)
                }
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
        received_stats: Arc::new(StdMutex::new(ReceivedBatchStats::default())),
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
        sink.on_control_frame_with_failure(envelopes)
            .await
            .map_err(SinkFailure::into_error)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routes::{
        METHOD_QUERY, METHOD_STREAM, ROUTE_KEY_QUERY, ROUTE_TOKEN_FS_META_EVENTS,
        default_route_bindings,
    };
    use crate::source::config::{GrantedMountRoot, RootSpec};
    use capanix_runtime_entry_sdk::control::{
        RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, encode_runtime_exec_control,
    };
    use tempfile::tempdir;
    use tokio::sync::Notify;

    struct FailingBoundary;

    #[async_trait::async_trait]
    impl ChannelIoSubset for FailingBoundary {
        async fn channel_recv(
            &self,
            _ctx: capanix_runtime_entry_sdk::advanced::boundary::BoundaryContext,
            _request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            Err(CnxError::TransportClosed(
                "IPC control transport closed".to_string(),
            ))
        }
    }

    struct TimeoutBoundary;

    #[async_trait::async_trait]
    impl ChannelIoSubset for TimeoutBoundary {
        async fn channel_recv(
            &self,
            _ctx: capanix_runtime_entry_sdk::advanced::boundary::BoundaryContext,
            _request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            Err(CnxError::Timeout)
        }
    }

    #[derive(Default)]
    struct StreamRecvCountingBoundary {
        recv_counts: StdMutex<std::collections::BTreeMap<String, usize>>,
        notify: Notify,
    }

    impl StreamRecvCountingBoundary {
        fn recv_count(&self, route: &str) -> usize {
            self.recv_counts
                .lock()
                .expect("stream recv counting boundary lock")
                .get(route)
                .copied()
                .unwrap_or(0)
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for StreamRecvCountingBoundary {
        async fn channel_recv(
            &self,
            _ctx: capanix_runtime_entry_sdk::advanced::boundary::BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            *self
                .recv_counts
                .lock()
                .expect("stream recv counting boundary lock")
                .entry(request.channel_key.0)
                .or_default() += 1;
            self.notify.notify_waiters();
            Err(CnxError::Timeout)
        }
    }

    fn root(id: &str, path: &str) -> RootSpec {
        RootSpec::new(id, std::path::PathBuf::from(path))
    }

    fn granted_mount_root(
        object_ref: &str,
        host_ref: &str,
        host_ip: &str,
        mount_point: impl Into<std::path::PathBuf>,
        active: bool,
    ) -> GrantedMountRoot {
        let mount_point = mount_point.into();
        GrantedMountRoot {
            object_ref: object_ref.to_string(),
            host_ref: host_ref.to_string(),
            host_ip: host_ip.to_string(),
            host_name: Some(host_ref.to_string()),
            site: None,
            zone: None,
            host_labels: Default::default(),
            mount_point: mount_point.clone(),
            fs_source: mount_point.display().to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
            active,
        }
    }

    fn bound_scope_with_resources(scope_id: &str, resource_ids: &[&str]) -> RuntimeBoundScope {
        RuntimeBoundScope {
            scope_id: scope_id.to_string(),
            resource_ids: resource_ids.iter().map(|id| (*id).to_string()).collect(),
        }
    }

    #[tokio::test]
    async fn worker_on_control_frame_ack_waits_until_events_stream_enters_first_recv_after_deferred_authority_startup_for_local_split_primary_scope()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        let nfs3 = tmp.path().join("nfs3");
        for dir in [&nfs1, &nfs2, &nfs3] {
            std::fs::create_dir_all(dir.join("data")).expect("create data dir");
        }

        let node_id = NodeId("node-b".to_string());
        let mut state = SinkWorkerState {
            sink: None,
            node_id: None,
            pending_init: None,
            endpoints_started: false,
            send_tx: None,
            last_control_frame_signals: Vec::new(),
            received_stats: Arc::new(StdMutex::new(ReceivedBatchStats::default())),
        };
        bootstrap_init_sink_runtime(
            node_id.clone(),
            SinkWorkerInitConfig {
                roots: vec![
                    root("nfs1", &nfs1.display().to_string()),
                    root("nfs2", &nfs2.display().to_string()),
                    root("nfs3", &nfs3.display().to_string()),
                ],
                host_object_grants: vec![
                    granted_mount_root("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone(), true),
                    granted_mount_root("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone(), true),
                    granted_mount_root("node-b::nfs3", "node-b", "10.0.0.13", nfs3.clone(), true),
                ],
                sink_tombstone_ttl_ms: 60_000,
                sink_tombstone_tolerance_us: 0,
            },
            &mut state,
        )
        .await
        .expect("bootstrap init sink runtime");

        let boundary = Arc::new(StreamRecvCountingBoundary::default());
        bootstrap_start_sink_runtime(
            &mut state,
            boundary.clone(),
            capanix_app_sdk::runtime::in_memory_state_boundary(),
        )
        .expect("bootstrap start sink runtime");

        let events_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM)
            .expect("resolve events route")
            .0;
        let action = plan_worker_request(
            SinkWorkerRequest::OnControlFrame {
                envelopes: vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 1,
                            expires_at_ms: 1,
                            bound_scopes: vec![bound_scope_with_resources(
                                "nfs3",
                                &["node-b::nfs3"],
                            )],
                        },
                    ))
                    .expect("encode sink query activate"),
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: events_route.clone(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 1,
                            expires_at_ms: 1,
                            bound_scopes: vec![bound_scope_with_resources(
                                "nfs3",
                                &["node-b::nfs3"],
                            )],
                        },
                    ))
                    .expect("encode sink stream activate"),
                ],
            },
            &mut state,
        );

        let (response, stop) = execute_worker_action(action).await;
        assert!(!stop, "worker on_control_frame should not stop the session");
        assert!(
            matches!(response, SinkWorkerResponse::Ack),
            "worker on_control_frame should still succeed in the narrow first-recv seam: {response:?}"
        );
        assert!(
            boundary.recv_count(&events_route) > 0,
            "worker-backed sink server on_control_frame must not Ack before the local events stream enters its first recv attempt after deferred-authority startup: route={} recv_count={}",
            events_route,
            boundary.recv_count(&events_route),
        );

        bootstrap_stop_sink_runtime(&mut state).await;
    }

    #[tokio::test]
    async fn bootstrap_start_must_repair_endpoint_continuity_even_when_endpoints_started_flag_is_true()
     {
        let node_id = NodeId("node-a".to_string());
        let mut source_cfg = SourceConfig::default();
        source_cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
        source_cfg.host_object_grants = vec![granted_mount_root(
            "node-a::exp",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];
        let sink = Arc::new(
            SinkFileMeta::with_boundaries(node_id.clone(), None, source_cfg)
                .expect("build sink runtime"),
        );

        sink.start_stream_endpoint(Arc::new(FailingBoundary), node_id.clone())
            .expect("seed terminal stream endpoint");

        let stream_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM)
            .expect("resolve stream route")
            .0;
        sink.on_control_frame(&[encode_runtime_exec_control(&RuntimeExecControl::Activate(
            RuntimeExecActivate {
                route_key: stream_route.clone(),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: Vec::new(),
                }],
            },
        ))
        .expect("encode sink events activate")])
            .await
            .expect("activate sink events route before terminal seeding");

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let traced = sink
                .debug_traced_route_state(&stream_route)
                .expect("trace route state");
            if traced.contains("finished=true") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "failed to deterministically seed a terminal stream endpoint before bootstrap restart check"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let mut state = SinkWorkerState {
            sink: Some(sink.clone()),
            node_id: Some(node_id),
            pending_init: None,
            endpoints_started: true,
            send_tx: None,
            last_control_frame_signals: Vec::new(),
            received_stats: Arc::new(StdMutex::new(ReceivedBatchStats::default())),
        };

        bootstrap_start_sink_runtime(
            &mut state,
            Arc::new(TimeoutBoundary),
            capanix_app_sdk::runtime::in_memory_state_boundary(),
        )
        .expect("bootstrap start should re-check endpoint continuity");

        let traced_after = sink
            .debug_traced_route_state(&stream_route)
            .expect("trace route state after bootstrap restart");
        assert!(
            !traced_after.contains("finished=true"),
            "bootstrap start must not skip endpoint continuity repair when endpoints_started=true and a predecessor stream endpoint already terminated: {traced_after}"
        );
        assert!(
            traced_after.contains("finished=false"),
            "bootstrap start should restore a live stream endpoint after pruning terminal predecessor endpoint tasks: {traced_after}"
        );

        bootstrap_stop_sink_runtime(&mut state).await;
    }
}
