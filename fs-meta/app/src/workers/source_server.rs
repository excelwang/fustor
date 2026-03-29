use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
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

use crate::FileMetaRecord;
use crate::query::path::is_under_query_path;
use crate::runtime::orchestration::{SourceControlSignal, source_control_signals_from_envelopes};
use crate::source::FSMetaSource;
use crate::source::config::SourceConfig;
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
    summarized_path_origin_counts: std::collections::BTreeMap<String, u64>,
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

fn debug_force_find_route_capture_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_FORCE_FIND_ROUTE_CAPTURE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn host_ref_matches_node_id(host_ref: &str, node_id: &NodeId) -> bool {
    host_ref == node_id.0
        || node_id
            .0
            .strip_prefix(host_ref)
            .is_some_and(|suffix| suffix.starts_with('-'))
}

fn stable_host_ref_for_node_id(node_id: &NodeId, grants: &[crate::source::config::GrantedMountRoot]) -> String {
    let host_refs = grants
        .iter()
        .filter(|grant| host_ref_matches_node_id(&grant.host_ref, node_id))
        .map(|grant| grant.host_ref.clone())
        .collect::<std::collections::BTreeSet<_>>();
    match host_refs.len() {
        1 => host_refs.into_iter().next().unwrap_or_else(|| node_id.0.clone()),
        _ => node_id.0.clone(),
    }
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

fn summarize_event_counts_by_origin(events: &[Event]) -> Vec<String> {
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

fn summarize_group_string_map(groups: &std::collections::BTreeMap<String, String>) -> Vec<String> {
    groups
        .iter()
        .map(|(group, value)| format!("{group}={value}"))
        .collect()
}

fn summarize_published_batch_path_counts(batch: &[Event], query_path: &[u8]) -> Vec<String> {
    let mut counts = std::collections::BTreeMap::<String, u64>::new();
    for event in batch {
        let Ok(record) = rmp_serde::from_slice::<FileMetaRecord>(event.payload_bytes()) else {
            continue;
        };
        if is_under_query_path(&record.path, query_path) {
            *counts
                .entry(event.metadata().origin_id.0.clone())
                .or_default() += 1;
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
        *counts
            .entry(event.metadata().origin_id.0.clone())
            .or_default() += 1;
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

fn published_path_origin_counts_for_target(
    batch: &[Event],
    path_capture_target: Option<&[u8]>,
) -> std::collections::BTreeMap<String, u64> {
    path_capture_target
        .map(|target| {
            summarize_published_batch_path_counts(batch, target)
                .into_iter()
                .filter_map(|entry| {
                    let (origin, count) = entry.rsplit_once('=')?;
                    Some((origin.to_string(), count.parse::<u64>().ok()?))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn summarize_published_batch_with_target(
    batch: &[Event],
    path_capture_target: Option<&[u8]>,
) -> PublishedBatchUpdate {
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
        published_path_origin_counts: published_path_origin_counts_for_target(
            batch,
            path_capture_target,
        ),
    }
}

fn summarize_published_batch(batch: &[Event]) -> PublishedBatchUpdate {
    summarize_published_batch_with_target(batch, debug_stream_path_capture_target().as_deref())
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
        *guard
            .published_origin_counts
            .entry(origin.clone())
            .or_default() += *count;
    }
    for (origin, count) in &update.published_path_origin_counts {
        *guard
            .published_path_origin_counts
            .entry(origin.clone())
            .or_default() += *count;
    }
}

fn record_summarized_path_stats(
    stats: &Arc<StdMutex<PublishedBatchStats>>,
    update: &PublishedBatchUpdate,
) {
    let mut guard = lock_publish_stats(stats);
    for (origin, count) in &update.published_path_origin_counts {
        *guard
            .summarized_path_origin_counts
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
            record_summarized_path_stats(&published_stats, &publish_update);
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
    let grants = source.host_object_grants_snapshot();
    let stable_host_ref = stable_host_ref_for_node_id(&node_id, &grants);
    let published = lock_publish_stats(published_stats);
    let enqueued_path_origin_counts = source.enqueued_path_origin_counts_snapshot();
    let pending_path_origin_counts = source.pending_path_origin_counts_snapshot();
    let yielded_path_origin_counts = source.yielded_path_origin_counts_snapshot();
    SourceObservabilitySnapshot {
        lifecycle_state: format!("{:?}", source.state()).to_ascii_lowercase(),
        host_object_grants_version: source.host_object_grants_version_snapshot(),
        grants,
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
                std::collections::BTreeMap::from([(
                    stable_host_ref.clone(),
                    groups.into_iter().collect(),
                )])
            })
            .unwrap_or_default(),
        scheduled_scan_groups_by_node: source
            .scheduled_scan_group_ids()
            .ok()
            .flatten()
            .filter(|groups| !groups.is_empty())
            .map(|groups| {
                std::collections::BTreeMap::from([(
                    stable_host_ref.clone(),
                    groups.into_iter().collect(),
                )])
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
        enqueued_path_origin_counts_by_node: (!enqueued_path_origin_counts.is_empty())
            .then(|| {
                std::collections::BTreeMap::from([(
                    node_id.0.clone(),
                    enqueued_path_origin_counts
                        .iter()
                        .map(|(origin, count)| format!("{origin}={count}"))
                        .collect::<Vec<_>>(),
                )])
            })
            .unwrap_or_default(),
        pending_path_origin_counts_by_node: (!pending_path_origin_counts.is_empty())
            .then(|| {
                std::collections::BTreeMap::from([(
                    node_id.0.clone(),
                    pending_path_origin_counts
                        .iter()
                        .map(|(origin, count)| format!("{origin}={count}"))
                        .collect::<Vec<_>>(),
                )])
            })
            .unwrap_or_default(),
        yielded_path_origin_counts_by_node: (!yielded_path_origin_counts.is_empty())
            .then(|| {
                std::collections::BTreeMap::from([(
                    node_id.0.clone(),
                    yielded_path_origin_counts
                        .iter()
                        .map(|(origin, count)| format!("{origin}={count}"))
                        .collect::<Vec<_>>(),
                )])
            })
            .unwrap_or_default(),
        summarized_path_origin_counts_by_node: (!published
            .summarized_path_origin_counts
            .is_empty())
        .then(|| {
            std::collections::BTreeMap::from([(
                node_id.0.clone(),
                published
                    .summarized_path_origin_counts
                    .iter()
                    .map(|(origin, count)| format!("{origin}={count}"))
                    .collect::<Vec<_>>(),
            )])
        })
        .unwrap_or_default(),
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
            Some(source) => {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_source_worker_server: force_find begin node={} selected_group={:?} recursive={} max_depth={:?} path={}",
                        source.node_id().0,
                        request.scope.selected_group,
                        request.scope.recursive,
                        request.scope.max_depth,
                        String::from_utf8_lossy(&request.scope.path)
                    );
                }
                match source.force_find(&request) {
                    Ok(events) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_source_worker_server: force_find done node={} events={} origins={:?} last_runner={:?} inflight={:?}",
                                source.node_id().0,
                                events.len(),
                                summarize_event_counts_by_origin(&events),
                                summarize_group_string_map(
                                    &source.last_force_find_runner_by_group_snapshot()
                                ),
                                source.force_find_inflight_groups_snapshot()
                            );
                        }
                        SourceWorkerAction::Immediate(SourceWorkerResponse::Events(events), false)
                    }
                    Err(err) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_source_worker_server: force_find failed node={} err={} last_runner={:?} inflight={:?}",
                                source.node_id().0,
                                err,
                                summarize_group_string_map(
                                    &source.last_force_find_runner_by_group_snapshot()
                                ),
                                source.force_find_inflight_groups_snapshot()
                            );
                        }
                        SourceWorkerAction::Immediate(
                            SourceWorkerResponse::Error(err.to_string()),
                            false,
                        )
                    }
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use capanix_app_sdk::runtime::in_memory_state_boundary;
    use capanix_runtime_entry_sdk::control::{
        RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, encode_runtime_exec_control,
    };
    use std::path::PathBuf;

    use futures_util::StreamExt;
    use tempfile::tempdir;

    use crate::source::config::GrantedMountRoot;
    use crate::source::config::RootSpec;
    use crate::runtime::execution_units::{SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID};
    use crate::runtime::routes::{
        ROUTE_KEY_SOURCE_RESCAN_CONTROL, ROUTE_KEY_SOURCE_RESCAN_INTERNAL,
        ROUTE_KEY_SOURCE_ROOTS_CONTROL,
    };

    struct NoopBoundary;

    #[async_trait::async_trait]
    impl ChannelIoSubset for NoopBoundary {}

    fn test_root(id: &str, path: PathBuf) -> RootSpec {
        let mut root = RootSpec::new(id, path);
        root.watch = false;
        root.scan = true;
        root
    }

    fn test_watch_scan_root(id: &str, path: PathBuf) -> RootSpec {
        RootSpec::new(id, path)
    }

    fn test_export(object_ref: &str, mount_point: PathBuf) -> GrantedMountRoot {
        GrantedMountRoot {
            object_ref: object_ref.to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.11".to_string(),
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

    #[tokio::test]
    async fn bootstrap_start_builds_runtime_managed_source_for_real_source_route_wave() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                test_watch_scan_root("nfs1", nfs1.clone()),
                test_watch_scan_root("nfs2", nfs2.clone()),
            ],
            host_object_grants: vec![
                test_export("node-a::nfs1", nfs1.clone()),
                test_export("node-a::nfs2", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };

        let mut state = SourceWorkerState {
            source: None,
            pending_init: Some((
                NodeId("node-a-29775285406139598021591041".to_string()),
                cfg,
            )),
            pump_task: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        };

        bootstrap_start_source_runtime(
            &mut state,
            Arc::new(NoopBoundary),
            in_memory_state_boundary(),
        )
        .await
        .expect("bootstrap start source runtime");

        let source = state
            .source
            .as_ref()
            .cloned()
            .expect("source should be initialized");
        source
            .on_control_frame(&[
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["node-a::nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["node-a::nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source roots activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["node-a::nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["node-a::nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source rescan-control activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["node-a::nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["node-a::nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source rescan activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["node-a::nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["node-a::nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source scan activate"),
            ])
            .await
            .expect("apply real source route wave");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = source
                .scheduled_source_group_ids()
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = source
                .scheduled_scan_group_ids()
                .expect("scheduled scan groups")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for runtime-managed source schedule after bootstrap: source={source_groups:?} scan={scan_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        stop_source_runtime(&mut state).await;
    }

    #[tokio::test]
    async fn observability_snapshot_normalizes_instance_suffixed_node_id_to_host_ref() {
        let tmp = tempdir().expect("create temp dir");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![test_watch_scan_root("nfs2", nfs2.clone())],
            host_object_grants: vec![GrantedMountRoot {
                object_ref: "node-d::nfs2".to_string(),
                host_ref: "node-d".to_string(),
                host_ip: "10.0.0.41".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: Default::default(),
                mount_point: nfs2.clone(),
                fs_source: "node-d::nfs2".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: vec![],
                interfaces: vec![],
                active: true,
            }],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(
            cfg,
            NodeId("node-d-29775443922859927994892289".to_string()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init source");
        source
            .on_control_frame(&[
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["node-d::nfs2".to_string()],
                    }],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["node-d::nfs2".to_string()],
                    }],
                }))
                .expect("encode scan activate"),
            ])
            .await
            .expect("apply control");

        let snapshot = source_observability_snapshot(
            &source,
            &[],
            &Arc::new(StdMutex::new(PublishedBatchStats::default())),
        );
        let expected = vec!["nfs2".to_string()];
        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-d"),
            Some(&expected)
        );
        assert_eq!(
            snapshot.scheduled_scan_groups_by_node.get("node-d"),
            Some(&expected)
        );
        assert!(
            !snapshot
                .scheduled_source_groups_by_node
                .contains_key("node-d-29775443922859927994892289"),
            "instance-suffixed node id should not leak into scheduled source groups: {:?}",
            snapshot.scheduled_source_groups_by_node
        );
        assert!(
            !snapshot
                .scheduled_scan_groups_by_node
                .contains_key("node-d-29775443922859927994892289"),
            "instance-suffixed node id should not leak into scheduled scan groups: {:?}",
            snapshot.scheduled_scan_groups_by_node
        );
    }

    #[tokio::test]
    async fn published_path_accounting_counts_newly_seeded_subtree_for_each_primary_root() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1 data");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2 data");

        let cfg = SourceConfig {
            roots: vec![
                test_root("nfs1", nfs1.clone()),
                test_root("nfs2", nfs2.clone()),
            ],
            host_object_grants: vec![
                test_export("node-a::nfs1", nfs1.clone()),
                test_export("node-a::nfs2", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let mut stream = source.pub_().await.expect("start source pub stream");

        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut initial_counts = std::collections::BTreeMap::<String, usize>::new();
        while tokio::time::Instant::now() < initial_deadline {
            let Some(batch) = tokio::time::timeout(Duration::from_millis(250), stream.next())
                .await
                .expect("initial batch wait should not time out")
            else {
                break;
            };
            for event in batch {
                if rmp_serde::from_slice::<crate::ControlEvent>(event.payload_bytes()).is_ok() {
                    continue;
                }
                *initial_counts
                    .entry(event.metadata().origin_id.0.clone())
                    .or_insert(0) += 1;
            }
            if ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| initial_counts.get(*origin).copied().unwrap_or(0) > 0)
            {
                break;
            }
        }
        assert!(
            ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| initial_counts.get(*origin).copied().unwrap_or(0) > 0),
            "initial stream should emit baseline data for both primary roots: {initial_counts:?}"
        );

        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 subtree");
        std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 subtree");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"aa")
            .expect("seed nfs1 subtree");
        std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"bb")
            .expect("seed nfs2 subtree");
        source
            .publish_manual_rescan_signal()
            .await
            .expect("publish manual rescan");

        let force_find_target = b"/force-find-stress";
        let published_stats = Arc::new(StdMutex::new(PublishedBatchStats::default()));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            let batch = match tokio::time::timeout(Duration::from_millis(250), stream.next()).await
            {
                Ok(Some(batch)) => batch,
                Ok(None) => break,
                Err(_) => continue,
            };
            let update = summarize_published_batch_with_target(&batch, Some(force_find_target));
            update_published_stats(&published_stats, &update);

            let guard = lock_publish_stats(&published_stats);
            let ready = ["node-a::nfs1", "node-a::nfs2"].iter().all(|origin| {
                guard
                    .published_path_origin_counts
                    .get(*origin)
                    .copied()
                    .unwrap_or(0)
                    > 0
            });
            drop(guard);
            if ready {
                break;
            }
        }

        source.close().await.expect("close source");

        let published_counts = lock_publish_stats(&published_stats)
            .published_path_origin_counts
            .clone();
        assert!(
            published_counts.get("node-a::nfs1").copied().unwrap_or(0) > 0,
            "published-path accounting should include newly seeded subtree for nfs1: {published_counts:?}"
        );
        assert!(
            published_counts.get("node-a::nfs2").copied().unwrap_or(0) > 0,
            "published-path accounting should include newly seeded subtree for nfs2: {published_counts:?}"
        );
    }

    #[test]
    fn observability_snapshot_request_preserves_live_published_path_counts_from_stats() {
        unsafe {
            std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", "/force-find-stress");
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                test_root("nfs1", nfs1.clone()),
                test_root("nfs2", nfs2.clone()),
            ],
            host_object_grants: vec![
                test_export("node-a::nfs1", nfs1),
                test_export("node-a::nfs2", nfs2),
            ],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let published_stats = Arc::new(StdMutex::new(PublishedBatchStats {
            batch_count: 12,
            event_count: 345,
            control_event_count: 6,
            data_event_count: 339,
            last_published_at_us: Some(123456),
            last_published_origins: vec!["node-a::nfs2=4".to_string()],
            published_origin_counts: std::collections::BTreeMap::from([
                ("node-a::nfs1".to_string(), 335),
                ("node-a::nfs2".to_string(), 10),
            ]),
            summarized_path_origin_counts: std::collections::BTreeMap::from([
                ("node-a::nfs1".to_string(), 333),
                ("node-a::nfs2".to_string(), 7),
            ]),
            published_path_origin_counts: std::collections::BTreeMap::from([
                ("node-a::nfs1".to_string(), 333),
                ("node-a::nfs2".to_string(), 7),
            ]),
        }));
        let mut state = SourceWorkerState {
            source: Some(Arc::new(source)),
            pending_init: None,
            pump_task: None,
            last_control_frame_signals: vec!["tick unit=runtime.exec.scan".to_string()],
            published_stats,
        };

        let action = plan_worker_request(SourceWorkerRequest::ObservabilitySnapshot, &mut state);
        let SourceWorkerAction::Immediate(
            SourceWorkerResponse::ObservabilitySnapshot(snapshot),
            false,
        ) = action
        else {
            panic!("observability snapshot request should return immediate snapshot response");
        };

        assert_eq!(
            snapshot.published_path_capture_target.as_deref(),
            Some("/force-find-stress")
        );
        let counts = snapshot
            .published_path_origin_counts_by_node
            .get("node-a")
            .cloned()
            .unwrap_or_default();
        let summarized = snapshot
            .summarized_path_origin_counts_by_node
            .get("node-a")
            .cloned()
            .unwrap_or_default();
        assert!(
            summarized.iter().any(|entry| entry == "node-a::nfs1=333"),
            "snapshot should preserve nfs1 summarized path counts from live stats: {summarized:?}"
        );
        assert!(
            summarized.iter().any(|entry| entry == "node-a::nfs2=7"),
            "snapshot should preserve nfs2 summarized path counts from live stats: {summarized:?}"
        );
        assert!(
            counts.iter().any(|entry| entry == "node-a::nfs1=333"),
            "snapshot should preserve nfs1 published path counts from live stats: {counts:?}"
        );
        assert!(
            counts.iter().any(|entry| entry == "node-a::nfs2=7"),
            "snapshot should preserve nfs2 published path counts from live stats: {counts:?}"
        );
    }
}
