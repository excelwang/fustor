use std::path::Path;
use std::sync::Mutex as StdMutex;
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
use crate::query::path::is_under_query_path;
use crate::runtime::orchestration::{SourceControlSignal, source_control_signals_from_envelopes};
use crate::workers::source::SourceObservabilitySnapshot;
use crate::workers::source::SourceWorkerRpc;
use crate::workers::source_ipc::{SourceWorkerRequest, SourceWorkerResponse};
use crate::FileMetaRecord;

const ROUTE_KEY_EVENTS: &str = "fs-meta.events:v1";
const SOURCE_WORKER_STOP_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const SOURCE_WORKER_STOP_ABORT_TIMEOUT: Duration = Duration::from_millis(250);

struct SourceWorkerState {
    source: Option<Arc<FSMetaSource>>,
    pending_init: Option<(NodeId, SourceConfig)>,
    pump_task: Option<JoinHandle<()>>,
    last_control_frame_signals: Vec<String>,
    published_stats: Arc<StdMutex<PublishedBatchStats>>,
}

#[derive(Default)]
struct PublishedBatchStats {
    batch_count: u64,
    event_count: u64,
    control_event_count: u64,
    data_event_count: u64,
    last_published_at_us: Option<u64>,
    last_published_origins: Vec<String>,
    published_origin_counts: std::collections::BTreeMap<String, u64>,
    published_path_origin_counts: std::collections::BTreeMap<String, u64>,
}

struct PublishedBatchUpdate {
    event_count: u64,
    control_event_count: u64,
    data_event_count: u64,
    last_published_at_us: Option<u64>,
    last_published_origins: Vec<String>,
    published_origin_counts: std::collections::BTreeMap<String, u64>,
    published_path_origin_counts: std::collections::BTreeMap<String, u64>,
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

fn debug_source_batch_flow_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_SOURCE_BATCH_FLOW").is_some()
}

fn debug_stream_path_capture_target() -> Option<Vec<u8>> {
    static TARGET: std::sync::OnceLock<Option<Vec<u8>>> = std::sync::OnceLock::new();
    TARGET
        .get_or_init(|| match std::env::var("FSMETA_DEBUG_STREAM_PATH_CAPTURE") {
            Ok(value) if value.trim().is_empty() => Some(b"/force-find-stress".to_vec()),
            Ok(value) => Some(value.into_bytes()),
            Err(_) => None,
        })
        .clone()
}

fn summarize_published_batch_path_counts(batch: &[Event], query_path: &[u8]) -> Vec<String> {
    let mut counts = std::collections::BTreeMap::<String, u64>::new();
    for event in batch {
        let Ok(record) = rmp_serde::from_slice::<FileMetaRecord>(event.payload_bytes()) else {
            continue;
        };
        if is_under_query_path(&record.path, query_path) {
            *counts.entry(event.metadata().origin_id.0.clone()).or_default() += 1;
        }
    }
    counts
        .into_iter()
        .map(|(origin, count)| format!("{origin}={count}"))
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

fn summarize_event_batch(events: &[Event]) -> String {
    let mut control = 0usize;
    let mut data = 0usize;
    for event in events {
        if rmp_serde::from_slice::<crate::ControlEvent>(event.payload_bytes()).is_ok() {
            control += 1;
        } else {
            data += 1;
        }
    }
    format!(
        "len={} control={} data={} origins={:?}",
        events.len(),
        control,
        data,
        summarize_event_origins(events)
    )
}

fn lock_publish_stats<'a>(
    stats: &'a Arc<StdMutex<PublishedBatchStats>>,
) -> std::sync::MutexGuard<'a, PublishedBatchStats> {
    match stats.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            log::warn!("source worker publish stats lock poisoned; recovering state");
            poisoned.into_inner()
        }
    }
}

fn summarize_published_batch(batch: &[Event]) -> PublishedBatchUpdate {
    let mut control = 0u64;
    let mut data = 0u64;
    let mut last_published_at_us = None::<u64>;
    for event in batch {
        last_published_at_us = Some(
            last_published_at_us
                .map(|current| current.max(event.metadata().timestamp_us))
                .unwrap_or(event.metadata().timestamp_us),
        );
        if rmp_serde::from_slice::<crate::ControlEvent>(event.payload_bytes()).is_ok() {
            control += 1;
        } else {
            data += 1;
        }
    }
    PublishedBatchUpdate {
        event_count: batch.len() as u64,
        control_event_count: control,
        data_event_count: data,
        last_published_at_us,
        last_published_origins: summarize_event_origins(batch),
        published_origin_counts: batch.iter().fold(
            std::collections::BTreeMap::<String, u64>::new(),
            |mut acc, event| {
                *acc.entry(event.metadata().origin_id.0.clone()).or_default() += 1;
                acc
            },
        ),
        published_path_origin_counts: debug_stream_path_capture_target()
            .as_deref()
            .map(|target| {
                summarize_published_batch_path_counts(batch, target)
                    .into_iter()
                    .filter_map(|entry| {
                        let (origin, count) = entry.rsplit_once('=')?;
                        Some((origin.to_string(), count.parse::<u64>().ok()?))
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn update_published_stats(
    stats: &Arc<StdMutex<PublishedBatchStats>>,
    update: &PublishedBatchUpdate,
) {
    let mut guard = lock_publish_stats(stats);
    guard.batch_count = guard.batch_count.saturating_add(1);
    guard.event_count = guard.event_count.saturating_add(update.event_count);
    guard.control_event_count = guard
        .control_event_count
        .saturating_add(update.control_event_count);
    guard.data_event_count = guard
        .data_event_count
        .saturating_add(update.data_event_count);
    guard.last_published_at_us = update.last_published_at_us;
    guard.last_published_origins = update.last_published_origins.clone();
    for (origin, count) in &update.published_origin_counts {
        *guard.published_origin_counts.entry(origin.clone()).or_default() += *count;
    }
    for (origin, count) in &update.published_path_origin_counts {
        *guard
            .published_path_origin_counts
            .entry(origin.clone())
            .or_default() += *count;
    }
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

fn start_source_pump_with_stream<S>(
    stream: S,
    boundary: Arc<dyn ChannelIoSubset>,
    published_stats: Arc<StdMutex<PublishedBatchStats>>,
) -> JoinHandle<()>
where
    S: futures_util::Stream<Item = Vec<Event>> + Send + 'static,
{
    tokio::spawn(async move {
        futures_util::pin_mut!(stream);
        while let Some(batch) = stream.next().await {
            let summary = if debug_source_batch_flow_enabled() {
                Some(summarize_event_batch(&batch))
            } else {
                None
            };
            let publish_update = summarize_published_batch(&batch);
            let origin = batch
                .first()
                .map(|event| event.metadata().origin_id.0.clone())
                .unwrap_or_else(|| "__empty__".to_string());
            if let Some(summary) = &summary {
                eprintln!(
                    "fs_meta_source_worker_server: publish_batch begin origin={} {}",
                    origin, summary
                );
            }
            if let Some(target) = debug_stream_path_capture_target() {
                eprintln!(
                    "fs_meta_source_worker_server: publish_batch path_capture origin={} target={} matches={:?}",
                    origin,
                    String::from_utf8_lossy(&target),
                    summarize_published_batch_path_counts(&batch, &target),
                );
            }
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
            update_published_stats(&published_stats, &publish_update);
            if let Some(summary) = &summary {
                eprintln!(
                    "fs_meta_source_worker_server: publish_batch ok origin={} {}",
                    origin, summary
                );
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
    published_stats: &Arc<StdMutex<PublishedBatchStats>>,
) -> SourceObservabilitySnapshot {
    let node_id = source.node_id();
    let published = lock_publish_stats(published_stats);
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
        published_batches_by_node: std::collections::BTreeMap::from([(
            node_id.0.clone(),
            published.batch_count,
        )]),
        published_events_by_node: std::collections::BTreeMap::from([(
            node_id.0.clone(),
            published.event_count,
        )]),
        published_control_events_by_node: std::collections::BTreeMap::from([(
            node_id.0.clone(),
            published.control_event_count,
        )]),
        published_data_events_by_node: std::collections::BTreeMap::from([(
            node_id.0.clone(),
            published.data_event_count,
        )]),
        last_published_at_us_by_node: published
            .last_published_at_us
            .map(|ts| std::collections::BTreeMap::from([(node_id.0.clone(), ts)]))
            .unwrap_or_default(),
        last_published_origins_by_node: (!published.last_published_origins.is_empty())
            .then(|| {
                std::collections::BTreeMap::from([(
                    node_id.0.clone(),
                    published.last_published_origins.clone(),
                )])
            })
            .unwrap_or_default(),
        published_origin_counts_by_node: (!published.published_origin_counts.is_empty())
            .then(|| {
                std::collections::BTreeMap::from([(
                    node_id.0.clone(),
                    published
                        .published_origin_counts
                        .iter()
                        .map(|(origin, count)| format!("{origin}={count}"))
                        .collect::<Vec<_>>(),
                )])
            })
            .unwrap_or_default(),
        published_path_capture_target: debug_stream_path_capture_target()
            .map(|target| String::from_utf8_lossy(&target).into_owned()),
        published_path_origin_counts_by_node: (!published.published_path_origin_counts.is_empty())
            .then(|| {
                std::collections::BTreeMap::from([(
                    node_id.0.clone(),
                    published
                        .published_path_origin_counts
                        .iter()
                        .map(|(origin, count)| format!("{origin}={count}"))
                        .collect::<Vec<_>>(),
                )])
            })
            .unwrap_or_default(),
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
    *lock_publish_stats(&state.published_stats) = PublishedBatchStats::default();
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
        state.pump_task = Some(start_source_pump_with_stream(
            stream,
            boundary,
            state.published_stats.clone(),
        ));
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
                    &state.published_stats,
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
        published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
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
