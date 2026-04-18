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
use crate::workers::source::{
    SourceObservabilityLiveAugment, build_source_observability_live_source_context,
    build_source_observability_snapshot_from_live_context, control_signals_by_node,
};
use crate::workers::source_ipc::{SourceWorkerRequest, SourceWorkerResponse};

const ROUTE_KEY_EVENTS: &str = "fs-meta.events:v1";
const SOURCE_WORKER_STOP_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const SOURCE_WORKER_STOP_ABORT_TIMEOUT: Duration = Duration::from_millis(250);
const SOURCE_WORKER_BOOTSTRAP_FENCED_RETRY_TIMEOUT: Duration = Duration::from_secs(2);
const SOURCE_WORKER_BOOTSTRAP_FENCED_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const SOURCE_WORKER_UPDATE_ROOTS_FENCED_RETRY_TIMEOUT: Duration = Duration::from_secs(1);

struct SourceWorkerState {
    source: Option<Arc<FSMetaSource>>,
    pending_init: Option<(NodeId, SourceConfig)>,
    pump_task: Option<JoinHandle<()>>,
    pump_boundary: Option<Arc<StdMutex<Arc<dyn ChannelIoSubset>>>>,
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

fn is_transient_statecell_fenced_init_error(err: &CnxError) -> bool {
    matches!(err, CnxError::InvalidInput(message)
        if message.contains("statecell")
            && message.contains("status=fenced"))
}

fn is_transient_logical_roots_fenced_write_error(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::Internal(message)
            if message.contains("statecell_write returned non-committed status for logical roots")
                && message.contains("fenced")
    )
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

fn stable_host_ref_for_node_id(
    node_id: &NodeId,
    grants: &[crate::source::config::GrantedMountRoot],
) -> String {
    let host_refs = grants
        .iter()
        .filter(|grant| host_ref_matches_node_id(&grant.host_ref, node_id))
        .map(|grant| grant.host_ref.clone())
        .collect::<std::collections::BTreeSet<_>>();
    match host_refs.len() {
        1 => host_refs
            .into_iter()
            .next()
            .unwrap_or_else(|| node_id.0.clone()),
        _ => node_id.0.clone(),
    }
}

fn debug_stream_path_capture_target() -> Option<Vec<u8>> {
    #[cfg(test)]
    if let Some(target) = debug_stream_path_capture_target_override_for_tests()
        .lock()
        .expect("lock debug stream path capture target override")
        .clone()
    {
        return target;
    }
    static TARGET: std::sync::OnceLock<Option<Vec<u8>>> = std::sync::OnceLock::new();
    TARGET
        .get_or_init(|| match std::env::var("FSMETA_DEBUG_STREAM_PATH_CAPTURE") {
            Ok(value) if value.trim().is_empty() => Some(b"/force-find-stress".to_vec()),
            Ok(value) => Some(value.into_bytes()),
            Err(_) => None,
        })
        .clone()
}

#[cfg(test)]
fn debug_stream_path_capture_target_override_for_tests()
-> &'static StdMutex<Option<Option<Vec<u8>>>> {
    static TARGET: std::sync::OnceLock<StdMutex<Option<Option<Vec<u8>>>>> =
        std::sync::OnceLock::new();
    TARGET.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn set_debug_stream_path_capture_target_override_for_tests(value: Option<&str>) {
    *debug_stream_path_capture_target_override_for_tests()
        .lock()
        .expect("lock debug stream path capture target override") =
        Some(value.map(|target| target.as_bytes().to_vec()));
}

#[cfg(test)]
fn clear_debug_stream_path_capture_target_override_for_tests() {
    *debug_stream_path_capture_target_override_for_tests()
        .lock()
        .expect("lock debug stream path capture target override") = None;
}

#[cfg(test)]
fn debug_stream_path_capture_target_override_serial_guard() -> &'static StdMutex<()> {
    static GUARD: std::sync::OnceLock<StdMutex<()>> = std::sync::OnceLock::new();
    GUARD.get_or_init(|| StdMutex::new(()))
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

fn replace_pump_boundary_target(
    target: &Arc<StdMutex<Arc<dyn ChannelIoSubset>>>,
    boundary: Arc<dyn ChannelIoSubset>,
) {
    match target.lock() {
        Ok(mut guard) => *guard = boundary,
        Err(poisoned) => {
            log::warn!("source worker pump boundary lock poisoned; recovering state");
            *poisoned.into_inner() = boundary;
        }
    }
}

fn clone_pump_boundary_target(
    target: &Arc<StdMutex<Arc<dyn ChannelIoSubset>>>,
) -> Arc<dyn ChannelIoSubset> {
    match target.lock() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => {
            log::warn!("source worker pump boundary lock poisoned; recovering state");
            poisoned.into_inner().clone()
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
    state.pump_boundary = None;
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
    boundary: Arc<StdMutex<Arc<dyn ChannelIoSubset>>>,
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
            let boundary = clone_pump_boundary_target(&boundary);
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

fn source_worker_runtime_ready_for_ping(state: &SourceWorkerState) -> bool {
    state.source.is_some()
        && state
            .pump_task
            .as_ref()
            .is_some_and(|task| !tokio::task::JoinHandle::is_finished(task))
}

fn request_requires_live_publish_pump(request: &SourceWorkerRequest) -> bool {
    matches!(
        request,
        SourceWorkerRequest::UpdateLogicalRoots { .. }
            | SourceWorkerRequest::PublishManualRescanSignal
            | SourceWorkerRequest::TriggerRescanWhenReady
            | SourceWorkerRequest::ObservabilitySnapshot
            | SourceWorkerRequest::OnControlFrame { .. }
    )
}

async fn fail_closed_if_publish_pump_dead(
    state: &mut SourceWorkerState,
    request_label: &str,
) -> bool {
    if state.source.is_some() && !source_worker_runtime_ready_for_ping(state) {
        eprintln!(
            "fs_meta_source_worker_server: fail_closed request={} reason=publish_pump_dead",
            request_label
        );
        stop_source_runtime(state).await;
        return true;
    }
    false
}

fn source_observability_snapshot(
    source: &FSMetaSource,
    last_control_frame_signals: &[String],
    published_stats: &Arc<StdMutex<PublishedBatchStats>>,
) -> SourceObservabilitySnapshot {
    let context = build_source_observability_live_source_context(source);
    let published = lock_publish_stats(published_stats);
    let stable_host_ref = context.recovery.stable_host_ref.clone();
    let has_local_source_grants = context
        .grants
        .iter()
        .any(|grant| grant.host_ref.eq_ignore_ascii_case(&stable_host_ref));
    let allow_control_summary = context
        .recovery
        .allow_control_summary(has_local_source_grants);
    let control_summary = if !last_control_frame_signals.is_empty() {
        if allow_control_summary {
            last_control_frame_signals.to_vec()
        } else {
            Vec::new()
        }
    } else if allow_control_summary {
        context.last_control_frame_signals_snapshot.clone()
    } else {
        Vec::new()
    };
    let enqueued_path_origin_counts = source.enqueued_path_origin_counts_snapshot();
    let pending_path_origin_counts = source.pending_path_origin_counts_snapshot();
    let yielded_path_origin_counts = source.yielded_path_origin_counts_snapshot();
    let has_explicit_zero_publication_counters = published.batch_count == 0
        && published.event_count == 0
        && published.control_event_count == 0
        && published.data_event_count == 0;
    build_source_observability_snapshot_from_live_context(
        context,
        SourceObservabilityLiveAugment {
            last_control_frame_signals_by_node: control_signals_by_node(
                &stable_host_ref,
                &control_summary,
            ),
            published_batches_by_node: std::collections::BTreeMap::from([(
                stable_host_ref.clone(),
                published.batch_count,
            )]),
            published_events_by_node: std::collections::BTreeMap::from([(
                stable_host_ref.clone(),
                published.event_count,
            )]),
            published_control_events_by_node: std::collections::BTreeMap::from([(
                stable_host_ref.clone(),
                published.control_event_count,
            )]),
            published_data_events_by_node: std::collections::BTreeMap::from([(
                stable_host_ref.clone(),
                published.data_event_count,
            )]),
            last_published_at_us_by_node: (!has_explicit_zero_publication_counters)
                .then_some(published.last_published_at_us)
                .flatten()
                .map(|ts| std::collections::BTreeMap::from([(stable_host_ref.clone(), ts)]))
                .unwrap_or_default(),
            last_published_origins_by_node: (!has_explicit_zero_publication_counters
                && !published.last_published_origins.is_empty())
            .then_some({
                std::collections::BTreeMap::from([(
                    stable_host_ref.clone(),
                    published.last_published_origins.clone(),
                )])
            })
            .unwrap_or_default(),
            published_origin_counts_by_node: (!has_explicit_zero_publication_counters
                && !published.published_origin_counts.is_empty())
            .then_some({
                std::collections::BTreeMap::from([(
                    stable_host_ref.clone(),
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
                        stable_host_ref.clone(),
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
                        stable_host_ref.clone(),
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
                        stable_host_ref.clone(),
                        yielded_path_origin_counts
                            .iter()
                            .map(|(origin, count)| format!("{origin}={count}"))
                            .collect::<Vec<_>>(),
                    )])
                })
                .unwrap_or_default(),
            summarized_path_origin_counts_by_node: (!has_explicit_zero_publication_counters
                && !published.summarized_path_origin_counts.is_empty())
            .then_some({
                std::collections::BTreeMap::from([(
                    stable_host_ref.clone(),
                    published
                        .summarized_path_origin_counts
                        .iter()
                        .map(|(origin, count)| format!("{origin}={count}"))
                        .collect::<Vec<_>>(),
                )])
            })
            .unwrap_or_default(),
            published_path_origin_counts_by_node: (!has_explicit_zero_publication_counters
                && !published.published_path_origin_counts.is_empty())
            .then_some({
                std::collections::BTreeMap::from([(
                    stable_host_ref.clone(),
                    published
                        .published_path_origin_counts
                        .iter()
                        .map(|(origin, count)| format!("{origin}={count}"))
                        .collect::<Vec<_>>(),
                )])
            })
            .unwrap_or_default(),
        },
    )
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
        let deadline = std::time::Instant::now() + SOURCE_WORKER_BOOTSTRAP_FENCED_RETRY_TIMEOUT;
        loop {
            eprintln!(
                "fs_meta_source_worker_server: bootstrap_start build_source begin node={} roots={} grants={}",
                node_id.0,
                config.roots.len(),
                config.host_object_grants.len()
            );
            match FSMetaSource::with_boundaries_and_state(
                config.clone(),
                node_id.clone(),
                None,
                state_boundary.clone(),
            ) {
                Ok(inner) => {
                    state.source = Some(Arc::new(inner));
                    eprintln!("fs_meta_source_worker_server: bootstrap_start build_source ok");
                    break;
                }
                Err(err)
                    if is_transient_statecell_fenced_init_error(&err)
                        && std::time::Instant::now() < deadline =>
                {
                    eprintln!(
                        "fs_meta_source_worker_server: bootstrap_start build_source retry err={err}"
                    );
                    tokio::time::sleep(SOURCE_WORKER_BOOTSTRAP_FENCED_RETRY_BACKOFF).await;
                }
                Err(err) => {
                    eprintln!(
                        "fs_meta_source_worker_server: bootstrap_start build_source err={err}"
                    );
                    return Err(err);
                }
            }
        }
    }
    let Some(source) = state.source.as_ref() else {
        return Err(CnxError::Internal(
            "source worker runtime missing during start".into(),
        ));
    };
    eprintln!("fs_meta_source_worker_server: bootstrap_start endpoints begin");
    source.start_runtime_endpoints(boundary.clone()).await?;
    eprintln!("fs_meta_source_worker_server: bootstrap_start endpoints ok");
    let pump_boundary = state
        .pump_boundary
        .get_or_insert_with(|| Arc::new(StdMutex::new(boundary.clone())))
        .clone();
    replace_pump_boundary_target(&pump_boundary, boundary);
    if state.pump_task.is_none() {
        let source = source.clone();
        eprintln!("fs_meta_source_worker_server: bootstrap_start pub begin");
        let stream = source.pub_().await?;
        eprintln!("fs_meta_source_worker_server: bootstrap_start pub ok");
        state.pump_task = Some(start_source_pump_with_stream(
            stream,
            pump_boundary,
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
            let deadline =
                tokio::time::Instant::now() + SOURCE_WORKER_UPDATE_ROOTS_FENCED_RETRY_TIMEOUT;
            loop {
                match source.update_logical_roots(roots.clone()).await {
                    Ok(_) => {
                        eprintln!("fs_meta_source_worker_server: update_logical_roots ok");
                        return (SourceWorkerResponse::Ack, false);
                    }
                    Err(err)
                        if is_transient_logical_roots_fenced_write_error(&err)
                            && tokio::time::Instant::now() < deadline =>
                    {
                        eprintln!(
                            "fs_meta_source_worker_server: update_logical_roots retry err={}",
                            err
                        );
                        tokio::time::sleep(SOURCE_WORKER_BOOTSTRAP_FENCED_RETRY_BACKOFF).await;
                    }
                    Err(err) => {
                        eprintln!(
                            "fs_meta_source_worker_server: update_logical_roots err={}",
                            err
                        );
                        return (classify_source_worker_error(err), false);
                    }
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
        pump_boundary: None,
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
            if request_requires_live_publish_pump(&request)
                && fail_closed_if_publish_pump_dead(&mut guard, request_label).await
            {
                SourceWorkerAction::Immediate(
                    SourceWorkerResponse::Error("worker not initialized".into()),
                    false,
                )
            } else {
                plan_worker_request(request, &mut guard)
            }
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
        let (source, summary) = {
            let mut guard = self.state.lock().await;
            if fail_closed_if_publish_pump_dead(&mut guard, "runtime_control").await {
                (None, Vec::new())
            } else {
                let summary = match source_control_signals_from_envelopes(envelopes) {
                    Ok(signals) => summarize_source_control_signals(&signals),
                    Err(err) => vec![format!("decode_err={err}")],
                };
                (guard.source.clone(), summary)
            }
        };
        let Some(source) = source else {
            return Err(CnxError::NotReady(
                "worker runtime not initialized for runtime control frames".into(),
            ));
        };
        let result = source.on_control_frame(envelopes).await;
        if result.is_ok() {
            let mut guard = self.state.lock().await;
            guard.last_control_frame_signals = summary.clone();
            if debug_control_scope_capture_enabled() {
                eprintln!(
                    "fs_meta_source_worker_server: on_runtime_control summary node={} signals={:?}",
                    source.node_id().0,
                    summary
                );
            }
        }
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
            "fs_meta_source_worker_server: on_start done ok={} err={}",
            result.is_ok(),
            result
                .as_ref()
                .err()
                .map(|err| err.to_string())
                .unwrap_or_else(|| "none".to_string())
        );
        result
    }

    async fn on_ping(&mut self, _context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        eprintln!("fs_meta_source_worker_server: on_ping begin");
        let mut guard = self.state.lock().await;
        if source_worker_runtime_ready_for_ping(&guard) {
            eprintln!("fs_meta_source_worker_server: on_ping ok");
            Ok(())
        } else {
            if guard.source.is_some() {
                stop_source_runtime(&mut guard).await;
            }
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
    use capanix_app_sdk::raw::ChannelBoundary;
    use capanix_app_sdk::runtime::{
        KernelResultEnvelope, LogLevel, StateCellReadRequest, StateCellWatchRequest,
        StateCellWriteRequest, in_memory_state_boundary,
    };
    use capanix_runtime_entry_sdk::control::{
        RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, encode_runtime_exec_control,
    };
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::StreamExt;
    use tempfile::tempdir;

    use crate::runtime::execution_units::{SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID};
    use crate::runtime::routes::{
        ROUTE_KEY_SOURCE_RESCAN_CONTROL, ROUTE_KEY_SOURCE_RESCAN_INTERNAL,
        ROUTE_KEY_SOURCE_ROOTS_CONTROL,
    };
    use crate::source::config::GrantedMountRoot;
    use crate::source::config::RootSpec;

    struct NoopBoundary;

    #[async_trait::async_trait]
    impl ChannelIoSubset for NoopBoundary {}

    impl ChannelBoundary for NoopBoundary {
        fn log(&self, _ctx: BoundaryContext, _level: LogLevel, _msg: &str) {}
    }

    impl StateBoundary for NoopBoundary {}

    #[derive(Default)]
    struct TerminatingRecvBoundary {
        recv_counts: StdMutex<std::collections::BTreeMap<String, usize>>,
    }

    impl TerminatingRecvBoundary {
        fn recv_count(&self, route: &str) -> usize {
            self.recv_counts
                .lock()
                .expect("recv_count lock")
                .get(route)
                .copied()
                .unwrap_or(0)
        }

        fn recv_counts_snapshot(&self) -> std::collections::BTreeMap<String, usize> {
            self.recv_counts
                .lock()
                .expect("recv_counts_snapshot lock")
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for TerminatingRecvBoundary {
        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> Result<Vec<Event>> {
            let route = request.channel_key.0;
            *self
                .recv_counts
                .lock()
                .expect("channel_recv recv_counts lock")
                .entry(route)
                .or_default() += 1;
            Err(CnxError::TransportClosed(
                "IPC control transport closed".to_string(),
            ))
        }

        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            _request: ChannelSendRequest,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct CountingTimeoutBoundary {
        recv_counts: StdMutex<std::collections::BTreeMap<String, usize>>,
    }

    impl CountingTimeoutBoundary {
        fn recv_count(&self, route: &str) -> usize {
            self.recv_counts
                .lock()
                .expect("recv_count lock")
                .get(route)
                .copied()
                .unwrap_or(0)
        }

        fn recv_counts_snapshot(&self) -> std::collections::BTreeMap<String, usize> {
            self.recv_counts
                .lock()
                .expect("recv_counts_snapshot lock")
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for CountingTimeoutBoundary {
        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> Result<Vec<Event>> {
            let route = request.channel_key.0;
            *self
                .recv_counts
                .lock()
                .expect("channel_recv recv_counts lock")
                .entry(route)
                .or_default() += 1;
            Err(CnxError::Timeout)
        }

        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            _request: ChannelSendRequest,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct SentBatchSummary {
        route: String,
        origins: std::collections::BTreeMap<String, u64>,
        control_events: u64,
        data_events: u64,
    }

    #[derive(Default)]
    struct PublishCaptureBoundary {
        recv_counts: StdMutex<std::collections::BTreeMap<String, usize>>,
        sent_batches: StdMutex<Vec<SentBatchSummary>>,
    }

    #[derive(Default)]
    struct LoopbackPublishBoundary {
        channels: tokio::sync::Mutex<std::collections::BTreeMap<String, Vec<Event>>>,
        changed: tokio::sync::Notify,
    }

    impl PublishCaptureBoundary {
        fn sent_data_events_for_route(&self, route: &str) -> u64 {
            self.sent_batches
                .lock()
                .expect("sent_data_events_for_route lock")
                .iter()
                .filter(|batch| batch.route == route)
                .map(|batch| batch.data_events)
                .sum()
        }

        fn sent_origin_counts_for_route(
            &self,
            route: &str,
        ) -> std::collections::BTreeMap<String, u64> {
            let mut counts = std::collections::BTreeMap::new();
            for batch in self
                .sent_batches
                .lock()
                .expect("sent_origin_counts_for_route lock")
                .iter()
                .filter(|batch| batch.route == route)
            {
                for (origin, count) in &batch.origins {
                    *counts.entry(origin.clone()).or_default() += *count;
                }
            }
            counts
        }

        fn recv_counts_snapshot(&self) -> std::collections::BTreeMap<String, usize> {
            self.recv_counts
                .lock()
                .expect("recv_counts_snapshot lock")
                .clone()
        }

        fn sent_batches_snapshot(&self) -> Vec<SentBatchSummary> {
            self.sent_batches
                .lock()
                .expect("sent_batches_snapshot lock")
                .clone()
        }
    }

    impl LoopbackPublishBoundary {
        async fn recv_route(&self, route: &str, timeout_ms: u64) -> Result<Vec<Event>> {
            self.channel_recv(
                BoundaryContext::default(),
                capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest {
                    channel_key: ChannelKey(route.to_string()),
                    timeout_ms: Some(timeout_ms),
                },
            )
            .await
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for PublishCaptureBoundary {
        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> Result<Vec<Event>> {
            let route = request.channel_key.0;
            *self
                .recv_counts
                .lock()
                .expect("channel_recv recv_counts lock")
                .entry(route)
                .or_default() += 1;
            Err(CnxError::Timeout)
        }

        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> Result<()> {
            let mut origins = std::collections::BTreeMap::<String, u64>::new();
            let mut control_events = 0_u64;
            let mut data_events = 0_u64;
            for event in &request.events {
                *origins
                    .entry(event.metadata().origin_id.0.clone())
                    .or_default() += 1;
                if rmp_serde::from_slice::<crate::ControlEvent>(event.payload_bytes()).is_ok() {
                    control_events += 1;
                } else {
                    data_events += 1;
                }
            }
            self.sent_batches
                .lock()
                .expect("channel_send sent_batches lock")
                .push(SentBatchSummary {
                    route: request.channel_key.0,
                    origins,
                    control_events,
                    data_events,
                });
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for LoopbackPublishBoundary {
        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> Result<Vec<Event>> {
            let deadline = request
                .timeout_ms
                .map(Duration::from_millis)
                .map(|timeout| tokio::time::Instant::now() + timeout);
            loop {
                {
                    let mut channels = self.channels.lock().await;
                    if let Some(events) = channels.remove(&request.channel_key.0)
                        && !events.is_empty()
                    {
                        return Ok(events);
                    }
                }
                let notified = self.changed.notified();
                if let Some(deadline) = deadline {
                    match tokio::time::timeout_at(deadline, notified).await {
                        Ok(()) => {}
                        Err(_) => return Err(CnxError::Timeout),
                    }
                } else {
                    notified.await;
                }
            }
        }

        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> Result<()> {
            {
                let mut channels = self.channels.lock().await;
                channels
                    .entry(request.channel_key.0)
                    .or_default()
                    .extend(request.events);
            }
            self.changed.notify_waiters();
            Ok(())
        }
    }

    impl ChannelBoundary for LoopbackPublishBoundary {
        fn log(&self, _ctx: BoundaryContext, _level: LogLevel, _msg: &str) {}
    }

    impl StateBoundary for LoopbackPublishBoundary {}

    #[derive(Default)]
    struct FailingPublishBoundary {
        send_count: StdMutex<u64>,
    }

    impl FailingPublishBoundary {
        fn send_count(&self) -> u64 {
            *self.send_count.lock().expect("send_count lock")
        }
    }

    fn record_path_data_counts(
        path_counts: &mut std::collections::BTreeMap<String, usize>,
        batch: &[Event],
        target: &[u8],
    ) {
        for event in batch {
            let Ok(record) = rmp_serde::from_slice::<crate::FileMetaRecord>(event.payload_bytes())
            else {
                continue;
            };
            if is_under_query_path(&record.path, target) {
                *path_counts
                    .entry(event.metadata().origin_id.0.clone())
                    .or_insert(0) += 1;
            }
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for FailingPublishBoundary {
        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            _request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> Result<Vec<Event>> {
            Err(CnxError::Timeout)
        }

        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            _request: ChannelSendRequest,
        ) -> Result<()> {
            let mut guard = self
                .send_count
                .lock()
                .expect("channel_send send_count lock");
            *guard += 1;
            Err(CnxError::PeerError(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof".to_string(),
            ))
        }
    }

    struct FencedThenOkStateBoundary {
        inner: Arc<dyn StateBoundary>,
        authority_fenced_reads_remaining: AtomicUsize,
    }

    impl FencedThenOkStateBoundary {
        fn new(inner: Arc<dyn StateBoundary>, authority_fenced_reads_remaining: usize) -> Self {
            Self {
                inner,
                authority_fenced_reads_remaining: AtomicUsize::new(
                    authority_fenced_reads_remaining,
                ),
            }
        }
    }

    #[async_trait::async_trait]
    impl StateBoundary for FencedThenOkStateBoundary {
        async fn statecell_read(
            &self,
            ctx: BoundaryContext,
            request: StateCellReadRequest,
        ) -> Result<KernelResultEnvelope> {
            if request.handle.cell_id == "fs-meta.authority.runtime.exec.source"
                && self
                    .authority_fenced_reads_remaining
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                        if remaining > 0 {
                            Some(remaining - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
            {
                return Ok(KernelResultEnvelope {
                    correlation_id: None,
                    status: "fenced".to_string(),
                    payload: Vec::new(),
                    diagnostics: Some(
                        "runtime state carrier fenced stale caller on statecell_read".to_string(),
                    ),
                });
            }
            self.inner.statecell_read(ctx, request).await
        }

        async fn statecell_write(
            &self,
            ctx: BoundaryContext,
            request: StateCellWriteRequest,
        ) -> Result<KernelResultEnvelope> {
            self.inner.statecell_write(ctx, request).await
        }

        async fn statecell_watch(
            &self,
            ctx: BoundaryContext,
            request: StateCellWatchRequest,
        ) -> Result<KernelResultEnvelope> {
            self.inner.statecell_watch(ctx, request).await
        }
    }

    struct FencedThenOkLogicalRootsWriteBoundary {
        inner: Arc<dyn StateBoundary>,
        logical_roots_writes_to_passthrough_before_fence: AtomicUsize,
        logical_roots_fenced_writes_remaining: AtomicUsize,
    }

    impl FencedThenOkLogicalRootsWriteBoundary {
        fn new(
            inner: Arc<dyn StateBoundary>,
            logical_roots_writes_to_passthrough_before_fence: usize,
            logical_roots_fenced_writes_remaining: usize,
        ) -> Self {
            Self {
                inner,
                logical_roots_writes_to_passthrough_before_fence: AtomicUsize::new(
                    logical_roots_writes_to_passthrough_before_fence,
                ),
                logical_roots_fenced_writes_remaining: AtomicUsize::new(
                    logical_roots_fenced_writes_remaining,
                ),
            }
        }
    }

    #[async_trait::async_trait]
    impl StateBoundary for FencedThenOkLogicalRootsWriteBoundary {
        async fn statecell_read(
            &self,
            ctx: BoundaryContext,
            request: StateCellReadRequest,
        ) -> Result<KernelResultEnvelope> {
            self.inner.statecell_read(ctx, request).await
        }

        async fn statecell_write(
            &self,
            ctx: BoundaryContext,
            request: StateCellWriteRequest,
        ) -> Result<KernelResultEnvelope> {
            if request.handle.cell_id == "fs-meta.logical-roots.runtime.exec.source" {
                let passthrough_before_fence = self
                    .logical_roots_writes_to_passthrough_before_fence
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                        if remaining > 0 {
                            Some(remaining - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok();
                if !passthrough_before_fence
                    && self
                        .logical_roots_fenced_writes_remaining
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                            if remaining > 0 {
                                Some(remaining - 1)
                            } else {
                                None
                            }
                        })
                        .is_ok()
                {
                    return Ok(KernelResultEnvelope {
                        correlation_id: None,
                        status: "fenced".to_string(),
                        payload: Vec::new(),
                        diagnostics: Some(
                            "runtime state carrier fenced stale caller on statecell_write"
                                .to_string(),
                        ),
                    });
                }
            }
            self.inner.statecell_write(ctx, request).await
        }

        async fn statecell_watch(
            &self,
            ctx: BoundaryContext,
            request: StateCellWatchRequest,
        ) -> Result<KernelResultEnvelope> {
            self.inner.statecell_watch(ctx, request).await
        }
    }

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
            pending_init: Some((NodeId("node-a-29775285406139598021591041".to_string()), cfg)),
            pump_task: None,
            pump_boundary: None,
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
    async fn trigger_rescan_when_ready_publishes_baseline_after_zero_grant_runtime_managed_watch_scan_wave()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                test_watch_scan_root("nfs1", nfs1.clone()),
                test_watch_scan_root("nfs2", nfs2.clone()),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };

        let mut state = SourceWorkerState {
            source: None,
            pending_init: Some((NodeId("node-c-local-sink-status-helper".to_string()), cfg)),
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        };

        let boundary = Arc::new(PublishCaptureBoundary::default());
        bootstrap_start_source_runtime(&mut state, boundary.clone(), in_memory_state_boundary())
            .await
            .expect("bootstrap start source runtime");

        let source_wave = |generation| {
            vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source roots activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source rescan-control activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source rescan activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };

        let (response, stop) = execute_worker_action(plan_worker_request(
            SourceWorkerRequest::OnControlFrame {
                envelopes: source_wave(2),
            },
            &mut state,
        ))
        .await;
        assert!(matches!(response, SourceWorkerResponse::Ack));
        assert!(
            !stop,
            "source worker should stay alive after zero-grant watch-scan wave"
        );

        let (response, stop) = execute_worker_action(plan_worker_request(
            SourceWorkerRequest::TriggerRescanWhenReady,
            &mut state,
        ))
        .await;
        assert!(matches!(response, SourceWorkerResponse::Ack));
        assert!(
            !stop,
            "trigger_rescan_when_ready should not stop the worker in zero-grant watch-scan mode"
        );

        let event_route = format!("{}.stream", ROUTE_KEY_EVENTS);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let origin_counts = boundary.sent_origin_counts_for_route(&event_route);
            let data_events = boundary.sent_data_events_for_route(&event_route);
            if origin_counts.get("nfs1").copied().unwrap_or(0) > 0
                && origin_counts.get("nfs2").copied().unwrap_or(0) > 0
                && data_events > 0
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "zero-grant runtime-managed watch-scan trigger_rescan_when_ready must publish baseline events for both local roots through the source worker server publish pump: origin_counts={origin_counts:?} data_events={data_events} sent_batches={:?}",
                boundary.sent_batches_snapshot(),
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        stop_source_runtime(&mut state).await;
    }

    #[tokio::test]
    async fn trigger_rescan_when_ready_publishes_baseline_to_loopback_boundary_after_zero_grant_runtime_managed_watch_scan_wave()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                test_watch_scan_root("nfs1", nfs1.clone()),
                test_watch_scan_root("nfs2", nfs2.clone()),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };

        let mut state = SourceWorkerState {
            source: None,
            pending_init: Some((NodeId("node-c-local-sink-status-helper".to_string()), cfg)),
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        };

        let boundary = Arc::new(LoopbackPublishBoundary::default());
        bootstrap_start_source_runtime(&mut state, boundary.clone(), in_memory_state_boundary())
            .await
            .expect("bootstrap start source runtime");

        let source_wave = |generation| {
            vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source roots activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source rescan-control activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source rescan activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };

        let (response, stop) = execute_worker_action(plan_worker_request(
            SourceWorkerRequest::OnControlFrame {
                envelopes: source_wave(2),
            },
            &mut state,
        ))
        .await;
        assert!(matches!(response, SourceWorkerResponse::Ack));
        assert!(
            !stop,
            "source worker should stay alive after zero-grant watch-scan wave"
        );

        let (response, stop) = execute_worker_action(plan_worker_request(
            SourceWorkerRequest::TriggerRescanWhenReady,
            &mut state,
        ))
        .await;
        assert!(matches!(response, SourceWorkerResponse::Ack));
        assert!(
            !stop,
            "trigger_rescan_when_ready should not stop the worker in zero-grant watch-scan mode"
        );

        let event_route = format!("{}.stream", ROUTE_KEY_EVENTS);
        let baseline_target = b"/data";
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut counts = std::collections::BTreeMap::<String, usize>::new();
        loop {
            match boundary.recv_route(&event_route, 50).await {
                Ok(batch) => record_path_data_counts(&mut counts, &batch, baseline_target),
                Err(CnxError::Timeout) => {}
                Err(err) => panic!("loopback publish recv failed: {err}"),
            }
            if counts.get("nfs1").copied().unwrap_or(0) > 0
                && counts.get("nfs2").copied().unwrap_or(0) > 0
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "zero-grant runtime-managed watch-scan trigger_rescan_when_ready must publish baseline /data for both local roots through the source worker server loopback boundary: counts={counts:?}"
            );
        }

        stop_source_runtime(&mut state).await;
    }

    #[tokio::test]
    async fn session_handle_request_populates_concrete_roots_after_zero_grant_runtime_managed_watch_scan_wave()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                test_watch_scan_root("nfs1", nfs1.clone()),
                test_watch_scan_root("nfs2", nfs2.clone()),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };

        let state = Arc::new(Mutex::new(SourceWorkerState {
            source: None,
            pending_init: None,
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        }));
        let mut session = SourceWorkerSession {
            state: state.clone(),
        };
        let boundary = Arc::new(LoopbackPublishBoundary::default());
        let context = WorkerSessionContext::new(
            boundary.clone(),
            boundary.clone(),
            in_memory_state_boundary(),
        );

        session
            .on_init(
                NodeId("node-c-session-zero-grant-watch-scan-status".to_string()),
                cfg,
                &context,
            )
            .await
            .expect("init source worker session");
        session
            .on_start(&context)
            .await
            .expect("start source worker session");

        let source_wave = |generation| {
            vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source roots activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source rescan-control activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source rescan activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        RuntimeBoundScope {
                            scope_id: "nfs1".to_string(),
                            resource_ids: vec!["nfs1".to_string()],
                        },
                        RuntimeBoundScope {
                            scope_id: "nfs2".to_string(),
                            resource_ids: vec!["nfs2".to_string()],
                        },
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };

        let response = session
            .handle_request(
                SourceWorkerRequest::OnControlFrame {
                    envelopes: source_wave(2),
                },
                &context,
            )
            .await
            .expect("handle zero-grant watch-scan on_control_frame");
        assert!(matches!(
            response,
            WorkerLoopControl::Continue(SourceWorkerResponse::Ack)
        ));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let response = session
                .handle_request(SourceWorkerRequest::StatusSnapshot, &context)
                .await
                .expect("handle status snapshot after zero-grant watch-scan wave");
            let WorkerLoopControl::Continue(SourceWorkerResponse::StatusSnapshot(status)) =
                response
            else {
                panic!("unexpected status snapshot response from source worker session");
            };
            let logical_root_ids = status
                .concrete_roots
                .iter()
                .map(|root| root.logical_root_id.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            let all_running = status
                .concrete_roots
                .iter()
                .filter(|root| root.logical_root_id == "nfs1" || root.logical_root_id == "nfs2")
                .all(|root| root.status == "running");
            if logical_root_ids.contains("nfs1") && logical_root_ids.contains("nfs2") && all_running
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "source worker session zero-grant watch-scan wave must populate running concrete roots before any external client wrapper is involved: concrete_roots={:?}",
                status.concrete_roots,
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        session
            .on_close(&context)
            .await
            .expect("close source worker session");
    }

    #[tokio::test]
    async fn bootstrap_start_retries_transient_fenced_authority_statecell_read() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![test_watch_scan_root("nfs1", nfs1.clone())],
            host_object_grants: vec![test_export("node-a::nfs1", nfs1)],
            ..SourceConfig::default()
        };

        let mut state = SourceWorkerState {
            source: None,
            pending_init: Some((NodeId("node-a-boot-fenced".to_string()), cfg)),
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        };

        let state_boundary: Arc<dyn StateBoundary> = Arc::new(FencedThenOkStateBoundary::new(
            in_memory_state_boundary(),
            1,
        ));

        bootstrap_start_source_runtime(&mut state, Arc::new(NoopBoundary), state_boundary)
            .await
            .expect(
                "bootstrap start should recover when authority statecell read is transiently fenced during same-runtime-incarnation reopen",
            );

        assert!(
            state.source.is_some(),
            "bootstrap start should initialize source after transient fenced authority read recovers",
        );
    }

    #[tokio::test]
    async fn update_logical_roots_retries_transient_fenced_logical_roots_statecell_write() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![test_watch_scan_root("nfs1", nfs1.clone())],
            host_object_grants: vec![
                test_export("node-a::nfs1", nfs1.clone()),
                test_export("node-a::nfs2", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };

        let state = Arc::new(Mutex::new(SourceWorkerState {
            source: None,
            pending_init: None,
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        }));
        let mut session = SourceWorkerSession {
            state: state.clone(),
        };
        let boundary = Arc::new(LoopbackPublishBoundary::default());
        let state_boundary: Arc<dyn StateBoundary> = Arc::new(
            FencedThenOkLogicalRootsWriteBoundary::new(in_memory_state_boundary(), 1, 1),
        );
        let context = WorkerSessionContext::new(boundary.clone(), boundary.clone(), state_boundary);

        session
            .on_init(
                NodeId("node-a-update-roots-fenced-write".to_string()),
                cfg,
                &context,
            )
            .await
            .expect("init source worker session");
        session
            .on_start(&context)
            .await
            .expect("start source worker session");

        let response = session
            .handle_request(
                SourceWorkerRequest::UpdateLogicalRoots {
                    roots: vec![
                        test_watch_scan_root("nfs1", nfs1),
                        test_watch_scan_root("nfs2", nfs2),
                    ],
                },
                &context,
            )
            .await
            .expect("handle update logical roots request");

        let WorkerLoopControl::Continue(SourceWorkerResponse::Ack) = response else {
            panic!(
                "update_logical_roots should recover when logical-roots statecell_write is transiently fenced during the same source worker session"
            );
        };

        let roots = session
            .handle_request(SourceWorkerRequest::LogicalRootsSnapshot, &context)
            .await
            .expect("logical roots snapshot after fenced write retry");
        let WorkerLoopControl::Continue(SourceWorkerResponse::LogicalRoots(roots)) = roots else {
            panic!("logical roots snapshot should succeed after fenced write retry");
        };
        assert_eq!(
            roots.into_iter().map(|root| root.id).collect::<Vec<_>>(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
            "logical roots should reflect the post-retry update after transient fenced statecell_write recovery",
        );

        session
            .on_close(&context)
            .await
            .expect("close source worker session");
    }

    #[tokio::test]
    async fn bootstrap_start_repairs_control_stream_continuity_even_when_pump_task_is_alive() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![test_watch_scan_root("nfs1", nfs1.clone())],
            host_object_grants: vec![test_export("node-a::nfs1", nfs1)],
            ..SourceConfig::default()
        };

        let mut state = SourceWorkerState {
            source: None,
            pending_init: Some((NodeId("node-a-bootstrap-restart".to_string()), cfg)),
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        };

        let first_boundary = Arc::new(TerminatingRecvBoundary::default());
        bootstrap_start_source_runtime(
            &mut state,
            first_boundary.clone(),
            in_memory_state_boundary(),
        )
        .await
        .expect("first bootstrap start source runtime");

        let roots_control_route = format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL);
        let rescan_control_route = format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL);
        let source = state
            .source
            .as_ref()
            .cloned()
            .expect("source runtime after first bootstrap");
        source
            .on_control_frame(&[
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: rescan_control_route.clone(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["node-a::nfs1".to_string()],
                    }],
                }))
                .expect("encode source rescan-control activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: roots_control_route.clone(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["node-a::nfs1".to_string()],
                    }],
                }))
                .expect("encode source roots-control activate"),
            ])
            .await
            .expect("activate source control stream routes");
        let first_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let saw_roots = first_boundary.recv_count(&roots_control_route) > 0;
            let saw_rescan = first_boundary.recv_count(&rescan_control_route) > 0;
            if saw_roots && saw_rescan {
                break;
            }
            assert!(
                tokio::time::Instant::now() < first_deadline,
                "first bootstrap should start source control stream recv loops after activation before continuity-repair check: counts={:?}",
                first_boundary.recv_counts_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let pump_task = state
            .pump_task
            .as_ref()
            .expect("source worker pump task should exist after first bootstrap");
        assert!(
            !pump_task.is_finished(),
            "source worker pump must stay alive while runtime-boundary stream endpoints terminate so second bootstrap tests continuity repair with live pump_task"
        );

        let second_boundary = Arc::new(CountingTimeoutBoundary::default());
        bootstrap_start_source_runtime(
            &mut state,
            second_boundary.clone(),
            in_memory_state_boundary(),
        )
        .await
        .expect("second bootstrap start source runtime");

        let second_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let roots_count = second_boundary.recv_count(&roots_control_route);
            let rescan_count = second_boundary.recv_count(&rescan_control_route);
            if roots_count > 0 && rescan_count > 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < second_deadline,
                "second bootstrap must refresh source control stream recv continuity even when pump_task stays alive; roots_count={roots_count} rescan_count={rescan_count} counts={:?}",
                second_boundary.recv_counts_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        stop_source_runtime(&mut state).await;
    }

    #[tokio::test]
    async fn restart_without_new_control_preserves_observability_control_summary_when_active_state_recovers()
     {
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
                test_export("node-a::nfs1", nfs1),
                test_export("node-a::nfs2", nfs2),
            ],
            ..SourceConfig::default()
        };

        let boundary = Arc::new(LoopbackPublishBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let context =
            WorkerSessionContext::new(boundary.clone(), boundary.clone(), state_boundary.clone());
        let state = Arc::new(Mutex::new(SourceWorkerState {
            source: None,
            pending_init: None,
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        }));
        let mut session = SourceWorkerSession {
            state: state.clone(),
        };

        let source_wave = |generation| {
            [
                (
                    ROUTE_KEY_SOURCE_ROOTS_CONTROL.to_string(),
                    SOURCE_RUNTIME_UNIT_ID.to_string(),
                ),
                (
                    ROUTE_KEY_SOURCE_RESCAN_CONTROL.to_string(),
                    SOURCE_RUNTIME_UNIT_ID.to_string(),
                ),
                (
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    SOURCE_RUNTIME_UNIT_ID.to_string(),
                ),
                (
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                ),
            ]
            .into_iter()
            .map(|(route_key, unit_id)| {
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key,
                    unit_id,
                    lease: None,
                    generation,
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
                .expect("encode source activate")
            })
            .collect::<Vec<_>>()
        };

        session
            .on_init(
                NodeId("node-a-observability-restart-preserve".to_string()),
                cfg.clone(),
                &context,
            )
            .await
            .expect("init source worker");
        session
            .on_start(&context)
            .await
            .expect("start source worker");
        session
            .on_runtime_control(&source_wave(2), &context)
            .await
            .expect("apply initial runtime control");

        let WorkerLoopControl::Continue(SourceWorkerResponse::ObservabilitySnapshot(initial)) =
            session
                .handle_request(SourceWorkerRequest::ObservabilitySnapshot, &context)
                .await
                .expect("fetch initial observability")
        else {
            panic!("initial observability request should return live snapshot");
        };
        assert!(
            initial
                .last_control_frame_signals_by_node
                .get("node-a")
                .is_some_and(|signals| !signals.is_empty()),
            "initial snapshot should capture accepted control summary before restart: {:?}",
            initial.last_control_frame_signals_by_node
        );

        session
            .on_close(&context)
            .await
            .expect("close source worker for restart");
        session
            .on_init(
                NodeId("node-a-observability-restart-preserve".to_string()),
                cfg,
                &context,
            )
            .await
            .expect("re-init source worker");
        session
            .on_start(&context)
            .await
            .expect("restart source worker");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let WorkerLoopControl::Continue(SourceWorkerResponse::ObservabilitySnapshot(snapshot)) =
                session
                    .handle_request(SourceWorkerRequest::ObservabilitySnapshot, &context)
                    .await
                    .expect("fetch restarted observability")
            else {
                panic!("restarted observability request should return live snapshot");
            };

            let active_recovered = snapshot.status.current_stream_generation.is_some()
                || snapshot.status.concrete_roots.iter().any(|entry| {
                    entry.active
                        || entry.current_stream_generation.is_some()
                        || entry.emitted_batch_count > 0
                        || entry.forwarded_batch_count > 0
                });
            if active_recovered {
                assert!(
                    snapshot
                        .last_control_frame_signals_by_node
                        .get("node-a")
                        .is_some_and(|signals| !signals.is_empty()),
                    "restarted active observability must preserve last_control_frame_signals_by_node even before a new control wave arrives: current_stream_generation={:?} source={:?} scan={:?} control={:?}",
                    snapshot.status.current_stream_generation,
                    snapshot.scheduled_source_groups_by_node,
                    snapshot.scheduled_scan_groups_by_node,
                    snapshot.last_control_frame_signals_by_node
                );
                break;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for restarted worker to recover active observability without a new control wave: source={:?} scan={:?} current_stream_generation={:?}",
                snapshot.scheduled_source_groups_by_node,
                snapshot.scheduled_scan_groups_by_node,
                snapshot.status.current_stream_generation
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn restart_without_new_control_recovers_observability_scheduled_groups_from_active_state()
    {
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
                test_export("node-a::nfs1", nfs1),
                test_export("node-a::nfs2", nfs2),
            ],
            ..SourceConfig::default()
        };

        let boundary = Arc::new(LoopbackPublishBoundary::default());
        let context =
            WorkerSessionContext::new(boundary.clone(), boundary, in_memory_state_boundary());
        let state = Arc::new(Mutex::new(SourceWorkerState {
            source: None,
            pending_init: None,
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        }));
        let mut session = SourceWorkerSession {
            state: state.clone(),
        };

        let source_wave = |generation| {
            [
                (
                    ROUTE_KEY_SOURCE_ROOTS_CONTROL.to_string(),
                    SOURCE_RUNTIME_UNIT_ID.to_string(),
                ),
                (
                    ROUTE_KEY_SOURCE_RESCAN_CONTROL.to_string(),
                    SOURCE_RUNTIME_UNIT_ID.to_string(),
                ),
                (
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    SOURCE_RUNTIME_UNIT_ID.to_string(),
                ),
                (
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                ),
            ]
            .into_iter()
            .map(|(route_key, unit_id)| {
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key,
                    unit_id,
                    lease: None,
                    generation,
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
                .expect("encode source activate")
            })
            .collect::<Vec<_>>()
        };

        session
            .on_init(
                NodeId("node-a-observability-restart-groups".to_string()),
                cfg.clone(),
                &context,
            )
            .await
            .expect("init source worker");
        session
            .on_start(&context)
            .await
            .expect("start source worker");
        session
            .on_runtime_control(&source_wave(2), &context)
            .await
            .expect("apply initial runtime control");
        session
            .on_close(&context)
            .await
            .expect("close source worker for restart");
        session
            .on_init(
                NodeId("node-a-observability-restart-groups".to_string()),
                cfg,
                &context,
            )
            .await
            .expect("re-init source worker");
        session
            .on_start(&context)
            .await
            .expect("restart source worker");

        let expected_groups = vec!["nfs1".to_string(), "nfs2".to_string()];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let WorkerLoopControl::Continue(SourceWorkerResponse::ObservabilitySnapshot(snapshot)) =
                session
                    .handle_request(SourceWorkerRequest::ObservabilitySnapshot, &context)
                    .await
                    .expect("fetch restarted observability")
            else {
                panic!("restarted observability request should return live snapshot");
            };

            let active_recovered = snapshot.status.current_stream_generation.is_some()
                || snapshot.status.concrete_roots.iter().any(|entry| {
                    entry.active
                        || entry.current_stream_generation.is_some()
                        || entry.emitted_batch_count > 0
                        || entry.forwarded_batch_count > 0
                });
            if active_recovered {
                assert_eq!(
                    snapshot.scheduled_source_groups_by_node.get("node-a"),
                    Some(&expected_groups),
                    "restarted active observability must recover scheduled source groups from active status before a new control wave arrives: current_stream_generation={:?} source={:?} scan={:?}",
                    snapshot.status.current_stream_generation,
                    snapshot.scheduled_source_groups_by_node,
                    snapshot.scheduled_scan_groups_by_node
                );
                assert_eq!(
                    snapshot.scheduled_scan_groups_by_node.get("node-a"),
                    Some(&expected_groups),
                    "restarted active observability must recover scheduled scan groups from active status before a new control wave arrives: current_stream_generation={:?} source={:?} scan={:?}",
                    snapshot.status.current_stream_generation,
                    snapshot.scheduled_source_groups_by_node,
                    snapshot.scheduled_scan_groups_by_node
                );
                break;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for restarted worker to recover active observability without a new control wave: source={:?} scan={:?} current_stream_generation={:?}",
                snapshot.scheduled_source_groups_by_node,
                snapshot.scheduled_scan_groups_by_node,
                snapshot.status.current_stream_generation
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn ping_reports_not_ready_after_publish_pump_dies_and_primary_roots_turn_output_closed() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
        std::fs::write(nfs1.join("seed.txt"), b"a").expect("seed nfs1 data");
        std::fs::write(nfs2.join("seed.txt"), b"b").expect("seed nfs2 data");

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

        let failing_boundary = Arc::new(FailingPublishBoundary::default());
        let mut state = SourceWorkerState {
            source: None,
            pending_init: Some((NodeId("node-a-ping-output-closed".to_string()), cfg)),
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        };

        bootstrap_start_source_runtime(
            &mut state,
            failing_boundary.clone(),
            in_memory_state_boundary(),
        )
        .await
        .expect("bootstrap start source runtime");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut manual_rescan_sent = false;
        loop {
            let pump_finished = state
                .pump_task
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished);
            if pump_finished && !manual_rescan_sent {
                state
                    .source
                    .as_ref()
                    .expect("source after bootstrap")
                    .publish_manual_rescan_signal()
                    .await
                    .expect("publish manual rescan after pump exit");
                manual_rescan_sent = true;
            }
            let statuses = state
                .source
                .as_ref()
                .expect("source after bootstrap")
                .status_snapshot()
                .concrete_roots;
            let any_primary_closed = statuses
                .iter()
                .filter(|root| root.is_group_primary)
                .filter(|root| {
                    root.object_ref == "node-a::nfs1" || root.object_ref == "node-a::nfs2"
                })
                .any(|root| root.status == "output_closed");
            if pump_finished && any_primary_closed && failing_boundary.send_count() > 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "publish pump should fail closed and drive primary roots into output_closed after sidecar early-eof send errors: send_count={} manual_rescan_sent={} statuses={statuses:?}",
                failing_boundary.send_count(),
                manual_rescan_sent,
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert!(
            !source_worker_runtime_ready_for_ping(&state),
            "ping readiness must fail closed once the publish pump is dead and primary roots are output_closed",
        );

        stop_source_runtime(&mut state).await;
    }

    #[tokio::test]
    async fn on_control_frame_fails_closed_after_publish_pump_dies_and_primary_roots_turn_output_closed()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
        std::fs::write(nfs1.join("seed.txt"), b"a").expect("seed nfs1 data");
        std::fs::write(nfs2.join("seed.txt"), b"b").expect("seed nfs2 data");

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

        let failing_boundary = Arc::new(FailingPublishBoundary::default());
        let mut state = SourceWorkerState {
            source: None,
            pending_init: Some((NodeId("node-a-control-output-closed".to_string()), cfg)),
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        };

        bootstrap_start_source_runtime(
            &mut state,
            failing_boundary.clone(),
            in_memory_state_boundary(),
        )
        .await
        .expect("bootstrap start source runtime");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut manual_rescan_sent = false;
        loop {
            let pump_finished = state
                .pump_task
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished);
            if pump_finished && !manual_rescan_sent {
                state
                    .source
                    .as_ref()
                    .expect("source after bootstrap")
                    .publish_manual_rescan_signal()
                    .await
                    .expect("publish manual rescan after pump exit");
                manual_rescan_sent = true;
            }
            let statuses = state
                .source
                .as_ref()
                .expect("source after bootstrap")
                .status_snapshot()
                .concrete_roots;
            let any_primary_closed = statuses
                .iter()
                .filter(|root| root.is_group_primary)
                .filter(|root| {
                    root.object_ref == "node-a::nfs1" || root.object_ref == "node-a::nfs2"
                })
                .any(|root| root.status == "output_closed");
            if pump_finished && any_primary_closed && failing_boundary.send_count() > 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "publish pump should fail closed before follow-up control-frame check: send_count={} manual_rescan_sent={} statuses={statuses:?}",
                failing_boundary.send_count(),
                manual_rescan_sent,
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let state = Arc::new(Mutex::new(state));
        let mut session = SourceWorkerSession {
            state: state.clone(),
        };
        let boundary = Arc::new(NoopBoundary);
        let context =
            WorkerSessionContext::new(boundary.clone(), boundary.clone(), boundary.clone());
        let response = session
            .handle_request(
                SourceWorkerRequest::OnControlFrame {
                    envelopes: vec![
                        encode_runtime_exec_control(&RuntimeExecControl::Activate(
                            RuntimeExecActivate {
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
                            },
                        ))
                        .expect("encode source activate"),
                    ],
                },
                &context,
            )
            .await
            .expect("handle follow-up control frame after pump failure");

        let WorkerLoopControl::Continue(SourceWorkerResponse::Error(message)) = response else {
            panic!(
                "follow-up control frame must fail closed once the publish pump is dead and primary roots are output_closed"
            );
        };
        assert_eq!(
            message, "worker not initialized",
            "follow-up control frame should return worker-not-initialized so the client replays Start instead of preserving stale schedules"
        );

        let guard = state.lock().await;
        assert!(
            guard.source.is_none(),
            "fail-closed control frame should stop the dead source runtime before later retries"
        );
        assert!(
            guard.pump_task.is_none(),
            "fail-closed control frame should clear the dead publish pump task before later retries"
        );
    }

    #[tokio::test]
    async fn observability_snapshot_fails_closed_after_publish_pump_dies_and_primary_roots_turn_output_closed()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
        std::fs::write(nfs1.join("seed.txt"), b"a").expect("seed nfs1 data");
        std::fs::write(nfs2.join("seed.txt"), b"b").expect("seed nfs2 data");

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

        let failing_boundary = Arc::new(FailingPublishBoundary::default());
        let mut state = SourceWorkerState {
            source: None,
            pending_init: Some((
                NodeId("node-a-observability-output-closed".to_string()),
                cfg,
            )),
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        };

        bootstrap_start_source_runtime(
            &mut state,
            failing_boundary.clone(),
            in_memory_state_boundary(),
        )
        .await
        .expect("bootstrap start source runtime");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut manual_rescan_sent = false;
        loop {
            let pump_finished = state
                .pump_task
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished);
            if pump_finished && !manual_rescan_sent {
                state
                    .source
                    .as_ref()
                    .expect("source after bootstrap")
                    .publish_manual_rescan_signal()
                    .await
                    .expect("publish manual rescan after pump exit");
                manual_rescan_sent = true;
            }
            let statuses = state
                .source
                .as_ref()
                .expect("source after bootstrap")
                .status_snapshot()
                .concrete_roots;
            let any_primary_closed = statuses
                .iter()
                .filter(|root| root.is_group_primary)
                .filter(|root| {
                    root.object_ref == "node-a::nfs1" || root.object_ref == "node-a::nfs2"
                })
                .any(|root| root.status == "output_closed");
            if pump_finished && any_primary_closed && failing_boundary.send_count() > 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "publish pump should fail closed before observability snapshot check: send_count={} manual_rescan_sent={} statuses={statuses:?}",
                failing_boundary.send_count(),
                manual_rescan_sent,
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let state = Arc::new(Mutex::new(state));
        let mut session = SourceWorkerSession {
            state: state.clone(),
        };
        let boundary = Arc::new(NoopBoundary);
        let context =
            WorkerSessionContext::new(boundary.clone(), boundary.clone(), boundary.clone());
        let response = session
            .handle_request(SourceWorkerRequest::ObservabilitySnapshot, &context)
            .await
            .expect("handle observability snapshot after pump failure");

        let WorkerLoopControl::Continue(SourceWorkerResponse::Error(message)) = response else {
            panic!(
                "observability snapshot must fail closed once the publish pump is dead and primary roots are output_closed"
            );
        };
        assert_eq!(
            message, "worker not initialized",
            "observability snapshot should return worker-not-initialized so status/readiness callers replay Start instead of consuming stale scheduled-group evidence"
        );

        let guard = state.lock().await;
        assert!(
            guard.source.is_none(),
            "fail-closed observability snapshot should stop the dead source runtime before later retries"
        );
        assert!(
            guard.pump_task.is_none(),
            "fail-closed observability snapshot should clear the dead publish pump task before later retries"
        );
    }

    #[tokio::test]
    async fn bootstrap_start_rebinds_publish_pump_to_latest_boundary_when_previous_pump_is_alive() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
        std::fs::write(nfs1.join("initial.txt"), b"a").expect("seed nfs1 data");
        std::fs::write(nfs2.join("initial.txt"), b"b").expect("seed nfs2 data");

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
            pending_init: Some((NodeId("node-a-bootstrap-publish-rebind".to_string()), cfg)),
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        };

        let event_route = format!("{}.stream", ROUTE_KEY_EVENTS);
        let first_boundary = Arc::new(PublishCaptureBoundary::default());
        bootstrap_start_source_runtime(
            &mut state,
            first_boundary.clone(),
            in_memory_state_boundary(),
        )
        .await
        .expect("first bootstrap start source runtime");

        let first_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let counts = first_boundary.sent_origin_counts_for_route(&event_route);
            if ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| counts.get(*origin).copied().unwrap_or(0) > 0)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < first_deadline,
                "first bootstrap should publish initial data for both primary roots before rebind check: sent={:?}",
                first_boundary.sent_batches_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let pump_task = state
            .pump_task
            .as_ref()
            .expect("source worker pump task should exist after first bootstrap");
        assert!(
            !pump_task.is_finished(),
            "publish pump must stay alive so second bootstrap exercises a live pump rebind"
        );

        let second_boundary = Arc::new(PublishCaptureBoundary::default());
        bootstrap_start_source_runtime(
            &mut state,
            second_boundary.clone(),
            in_memory_state_boundary(),
        )
        .await
        .expect("second bootstrap start source runtime");

        std::fs::write(nfs1.join("after-rebind.txt"), b"aa").expect("write nfs1 rebind data");
        std::fs::write(nfs2.join("after-rebind.txt"), b"bb").expect("write nfs2 rebind data");
        state
            .source
            .as_ref()
            .expect("source after second bootstrap")
            .publish_manual_rescan_signal()
            .await
            .expect("publish manual rescan after second bootstrap");

        let second_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
        loop {
            let counts = second_boundary.sent_origin_counts_for_route(&event_route);
            if ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| counts.get(*origin).copied().unwrap_or(0) > 0)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < second_deadline,
                "second bootstrap must rebind the live publish pump to the replacement boundary; first_sent={:?} second_sent={:?} first_recv={:?} second_recv={:?}",
                first_boundary.sent_batches_snapshot(),
                second_boundary.sent_batches_snapshot(),
                first_boundary.recv_counts_snapshot(),
                second_boundary.recv_counts_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(
            second_boundary.sent_data_events_for_route(&event_route) > 0,
            "replacement boundary should receive published data after second bootstrap: sent={:?}",
            second_boundary.sent_batches_snapshot()
        );

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
            &["activate unit=runtime.exec.source route=source-logical-roots-control:v1.stream generation=2 scopes=[\"nfs2=>node-d::nfs2\"]".to_string()],
            &Arc::new(StdMutex::new(PublishedBatchStats {
                batch_count: 12,
                event_count: 345,
                control_event_count: 6,
                data_event_count: 339,
                last_published_at_us: Some(123456),
                last_published_origins: vec!["node-d::nfs2=4".to_string()],
                published_origin_counts: std::collections::BTreeMap::from([
                    ("node-d::nfs2".to_string(), 10),
                ]),
                summarized_path_origin_counts: std::collections::BTreeMap::from([
                    ("node-d::nfs2".to_string(), 7),
                ]),
                published_path_origin_counts: std::collections::BTreeMap::from([
                    ("node-d::nfs2".to_string(), 7),
                ]),
            })),
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
        assert_eq!(
            snapshot.last_control_frame_signals_by_node.get("node-d"),
            Some(&vec!["activate unit=runtime.exec.source route=source-logical-roots-control:v1.stream generation=2 scopes=[\"nfs2=>node-d::nfs2\"]".to_string()])
        );
        assert_eq!(snapshot.published_batches_by_node.get("node-d"), Some(&12));
        assert_eq!(snapshot.published_events_by_node.get("node-d"), Some(&345));
        assert_eq!(
            snapshot.published_control_events_by_node.get("node-d"),
            Some(&6)
        );
        assert_eq!(
            snapshot.published_data_events_by_node.get("node-d"),
            Some(&339)
        );
        assert_eq!(
            snapshot.last_published_at_us_by_node.get("node-d"),
            Some(&123456)
        );
        assert!(
            snapshot
                .last_published_origins_by_node
                .get("node-d")
                .is_some_and(|origins| origins.iter().any(|entry| entry == "node-d::nfs2=4"))
        );
        assert!(
            snapshot
                .published_path_origin_counts_by_node
                .get("node-d")
                .is_some_and(|counts| counts.iter().any(|entry| entry == "node-d::nfs2=7"))
        );
        for leaked in [
            &snapshot.last_control_frame_signals_by_node,
            &snapshot.last_published_origins_by_node,
            &snapshot.published_origin_counts_by_node,
            &snapshot.summarized_path_origin_counts_by_node,
            &snapshot.published_path_origin_counts_by_node,
        ] {
            assert!(
                !leaked.contains_key("node-d-29775443922859927994892289"),
                "instance-suffixed node id should not leak into observability export maps: {leaked:?}"
            );
        }
        for leaked in [
            &snapshot.published_batches_by_node,
            &snapshot.published_events_by_node,
            &snapshot.published_control_events_by_node,
            &snapshot.published_data_events_by_node,
            &snapshot.last_published_at_us_by_node,
        ] {
            assert!(
                !leaked.contains_key("node-d-29775443922859927994892289"),
                "instance-suffixed node id should not leak into observability export maps: {leaked:?}"
            );
        }
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

    #[tokio::test]
    async fn runtime_control_updates_observability_control_summary_before_observability_snapshot() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![test_watch_scan_root("nfs1", nfs1.clone())],
            host_object_grants: vec![test_export("node-a::nfs1", nfs1)],
            ..SourceConfig::default()
        };

        let state = Arc::new(Mutex::new(SourceWorkerState {
            source: None,
            pending_init: None,
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        }));
        let mut session = SourceWorkerSession {
            state: state.clone(),
        };
        let boundary = Arc::new(LoopbackPublishBoundary::default());
        let context = WorkerSessionContext::new(
            boundary.clone(),
            boundary.clone(),
            in_memory_state_boundary(),
        );

        session
            .on_init(
                NodeId("node-a-runtime-control-observability".to_string()),
                cfg,
                &context,
            )
            .await
            .expect("init source worker session");
        session
            .on_start(&context)
            .await
            .expect("start source worker session");

        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-a::nfs1".to_string()],
                }],
            }))
            .expect("encode source activate");
        session
            .on_runtime_control(&[activate], &context)
            .await
            .expect("apply runtime control activate");

        let response = session
            .handle_request(SourceWorkerRequest::ObservabilitySnapshot, &context)
            .await
            .expect("fetch observability snapshot after runtime control");

        let WorkerLoopControl::Continue(SourceWorkerResponse::ObservabilitySnapshot(snapshot)) =
            response
        else {
            panic!("observability snapshot request should return live snapshot response");
        };

        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-a"),
            Some(&vec!["nfs1".to_string()]),
            "runtime control activation should surface scheduled source groups in worker observability snapshot",
        );
        assert!(
            snapshot
                .last_control_frame_signals_by_node
                .get("node-a")
                .is_some_and(|signals| !signals.is_empty()),
            "runtime control activation should surface a non-empty control summary in worker observability snapshot: {:?}",
            snapshot.last_control_frame_signals_by_node
        );
    }

    #[tokio::test]
    async fn observability_snapshot_request_falls_back_to_source_control_summary_when_worker_state_summary_is_empty()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![test_root("nfs1", nfs1.clone())],
            host_object_grants: vec![test_export("node-a::nfs1", nfs1)],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::new(cfg, NodeId("node-a".to_string())).expect("init source");
        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: crate::runtime::routes::ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-a::nfs1".to_string()],
                }],
            }))
            .expect("encode source activate");
        source
            .on_control_frame(&[activate])
            .await
            .expect("apply source activate");

        let mut state = SourceWorkerState {
            source: Some(Arc::new(source)),
            pending_init: None,
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
            published_stats: Arc::new(StdMutex::new(PublishedBatchStats::default())),
        };

        let action = plan_worker_request(SourceWorkerRequest::ObservabilitySnapshot, &mut state);
        let SourceWorkerAction::Immediate(
            SourceWorkerResponse::ObservabilitySnapshot(snapshot),
            false,
        ) = action
        else {
            panic!("observability snapshot request should return immediate snapshot response");
        };

        assert!(
            snapshot
                .last_control_frame_signals_by_node
                .get("node-a")
                .is_some_and(|signals| !signals.is_empty()),
            "worker observability must fall back to the source-owned accepted control summary when worker-side cached summary is empty: {:?}",
            snapshot.last_control_frame_signals_by_node
        );
    }

    #[tokio::test]
    async fn observability_snapshot_request_does_not_fall_back_to_source_control_summary_without_recovered_active_state()
     {
        let source = FSMetaSource::new(SourceConfig::default(), NodeId("node-a".to_string()))
            .expect("init source");
        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: crate::runtime::routes::ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-a::nfs1".to_string()],
                }],
            }))
            .expect("encode source activate");
        source
            .on_control_frame(&[activate])
            .await
            .expect("apply source activate");

        assert!(
            !source.last_control_frame_signals_snapshot().is_empty(),
            "fixture must keep a non-empty source-owned control summary before active-state gating is checked"
        );
        let snapshot = source_observability_snapshot(
            &source,
            &[],
            &Arc::new(StdMutex::new(PublishedBatchStats::default())),
        );

        assert!(
            snapshot.last_control_frame_signals_by_node.is_empty(),
            "worker observability must not fall back to stale source-owned control summary when no recovered active state exists: status={:?} source={:?} scan={:?} control={:?}",
            snapshot.status,
            snapshot.scheduled_source_groups_by_node,
            snapshot.scheduled_scan_groups_by_node,
            snapshot.last_control_frame_signals_by_node
        );
    }

    #[test]
    fn observability_snapshot_request_preserves_live_published_path_counts_from_stats() {
        let _serial = debug_stream_path_capture_target_override_serial_guard()
            .lock()
            .expect("lock debug stream path capture target serial guard");

        struct DebugStreamPathCaptureTargetOverrideReset;

        impl Drop for DebugStreamPathCaptureTargetOverrideReset {
            fn drop(&mut self) {
                clear_debug_stream_path_capture_target_override_for_tests();
            }
        }

        let _reset = DebugStreamPathCaptureTargetOverrideReset;
        set_debug_stream_path_capture_target_override_for_tests(Some("/force-find-stress"));

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
            pump_boundary: None,
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

    #[test]
    fn observability_snapshot_request_clears_zero_counter_published_stats_metadata_without_hiding_debug_target()
     {
        let _serial = debug_stream_path_capture_target_override_serial_guard()
            .lock()
            .expect("lock debug stream path capture target serial guard");

        struct DebugStreamPathCaptureTargetOverrideReset;

        impl Drop for DebugStreamPathCaptureTargetOverrideReset {
            fn drop(&mut self) {
                clear_debug_stream_path_capture_target_override_for_tests();
            }
        }

        let _reset = DebugStreamPathCaptureTargetOverrideReset;
        set_debug_stream_path_capture_target_override_for_tests(Some("/force-find-stress"));

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![test_root("nfs1", nfs1.clone())],
            host_object_grants: vec![test_export("node-a::nfs1", nfs1)],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let published_stats = Arc::new(StdMutex::new(PublishedBatchStats {
            batch_count: 0,
            event_count: 0,
            control_event_count: 0,
            data_event_count: 0,
            last_published_at_us: Some(123456),
            last_published_origins: vec!["node-a::nfs1=4".to_string()],
            published_origin_counts: std::collections::BTreeMap::from([(
                "node-a::nfs1".to_string(),
                4,
            )]),
            summarized_path_origin_counts: std::collections::BTreeMap::from([(
                "node-a::nfs1".to_string(),
                4,
            )]),
            published_path_origin_counts: std::collections::BTreeMap::from([(
                "node-a::nfs1".to_string(),
                4,
            )]),
        }));
        let mut state = SourceWorkerState {
            source: Some(Arc::new(source)),
            pending_init: None,
            pump_task: None,
            pump_boundary: None,
            last_control_frame_signals: Vec::new(),
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
            snapshot.published_batches_by_node.get("node-a"),
            Some(&0),
            "explicit zero batch counters must remain visible in the live worker snapshot: {:?}",
            snapshot.published_batches_by_node
        );
        assert_eq!(
            snapshot.published_events_by_node.get("node-a"),
            Some(&0),
            "explicit zero event counters must remain visible in the live worker snapshot: {:?}",
            snapshot.published_events_by_node
        );
        assert!(
            snapshot.last_published_at_us_by_node.is_empty(),
            "explicit zero publication counters must suppress stale last_published_at_us export in the worker snapshot: {:?}",
            snapshot.last_published_at_us_by_node
        );
        assert!(
            snapshot.last_published_origins_by_node.is_empty(),
            "explicit zero publication counters must suppress stale last_published_origins export in the worker snapshot: {:?}",
            snapshot.last_published_origins_by_node
        );
        assert!(
            snapshot.published_origin_counts_by_node.is_empty(),
            "explicit zero publication counters must suppress stale published_origin_counts export in the worker snapshot: {:?}",
            snapshot.published_origin_counts_by_node
        );
        assert!(
            snapshot.summarized_path_origin_counts_by_node.is_empty(),
            "explicit zero publication counters must suppress stale summarized_path_origin_counts export in the worker snapshot: {:?}",
            snapshot.summarized_path_origin_counts_by_node
        );
        assert!(
            snapshot.published_path_origin_counts_by_node.is_empty(),
            "explicit zero publication counters must suppress stale published_path_origin_counts export in the worker snapshot: {:?}",
            snapshot.published_path_origin_counts_by_node
        );
        assert_eq!(
            snapshot.published_path_capture_target.as_deref(),
            Some("/force-find-stress"),
            "worker live debug target should remain visible even when zero publication counters suppress stale stats-derived metadata"
        );
    }

    #[tokio::test]
    async fn observability_snapshot_request_preserves_explicit_empty_scheduled_groups_without_recovering_active_groups()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![test_watch_scan_root("nfs1", nfs1.clone())],
            host_object_grants: vec![GrantedMountRoot {
                object_ref: "node-b::nfs1".to_string(),
                host_ref: "node-b".to_string(),
                host_ip: "10.0.0.21".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: Default::default(),
                mount_point: nfs1,
                fs_source: "node-b::nfs1".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: vec![],
                interfaces: vec![],
                active: true,
            }],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::new(cfg, NodeId("node-a".to_string())).expect("init source");
        let control = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                }],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                }],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                }],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                }],
            }))
            .expect("encode source scan activate"),
        ];
        source
            .on_control_frame(&control)
            .await
            .expect("apply non-local source activate");

        assert_eq!(
            source.scheduled_source_group_ids().expect("source groups"),
            Some(std::collections::BTreeSet::new()),
            "fixture must surface explicit empty scheduled source groups before worker-side snapshot folding"
        );
        assert_eq!(
            source.scheduled_scan_group_ids().expect("scan groups"),
            Some(std::collections::BTreeSet::new()),
            "fixture must surface explicit empty scheduled scan groups before worker-side snapshot folding"
        );

        let snapshot = source_observability_snapshot(
            &source,
            &[],
            &Arc::new(StdMutex::new(PublishedBatchStats::default())),
        );

        assert_eq!(
            snapshot.scheduled_source_groups_by_node,
            std::collections::BTreeMap::from([("node-a".to_string(), Vec::new())]),
            "worker observability must preserve explicit empty scheduled source groups instead of recovering them from active status: current_stream_generation={:?} source={:?}",
            snapshot.status.current_stream_generation,
            snapshot.scheduled_source_groups_by_node
        );
        assert_eq!(
            snapshot.scheduled_scan_groups_by_node,
            std::collections::BTreeMap::from([("node-a".to_string(), Vec::new())]),
            "worker observability must preserve explicit empty scheduled scan groups instead of recovering them from active status: current_stream_generation={:?} scan={:?}",
            snapshot.status.current_stream_generation,
            snapshot.scheduled_scan_groups_by_node
        );
    }

    #[tokio::test]
    async fn observability_snapshot_request_does_not_recover_worker_control_summary_from_explicit_empty_scheduled_groups()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![test_watch_scan_root("nfs1", nfs1.clone())],
            host_object_grants: vec![GrantedMountRoot {
                object_ref: "node-b::nfs1".to_string(),
                host_ref: "node-b".to_string(),
                host_ip: "10.0.0.21".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: Default::default(),
                mount_point: nfs1,
                fs_source: "node-b::nfs1".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: vec![],
                interfaces: vec![],
                active: true,
            }],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::new(cfg, NodeId("node-a".to_string())).expect("init source");
        let control = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                }],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                }],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                }],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                }],
            }))
            .expect("encode source scan activate"),
        ];
        source
            .on_control_frame(&control)
            .await
            .expect("apply non-local source activate");

        assert_eq!(
            source.scheduled_source_group_ids().expect("source groups"),
            Some(std::collections::BTreeSet::new()),
            "fixture must surface explicit empty scheduled source groups before worker-side control-summary recovery"
        );
        assert_eq!(
            source.scheduled_scan_group_ids().expect("scan groups"),
            Some(std::collections::BTreeSet::new()),
            "fixture must surface explicit empty scheduled scan groups before worker-side control-summary recovery"
        );

        let snapshot = source_observability_snapshot(
            &source,
            &["tick unit=runtime.exec.scan generation=2 groups=[\"nfs1\"]".to_string()],
            &Arc::new(StdMutex::new(PublishedBatchStats::default())),
        );

        assert!(
            snapshot.last_control_frame_signals_by_node.is_empty(),
            "explicit empty scheduled groups must not resurrect worker control summary as recovered active state: source={:?} scan={:?} control={:?}",
            snapshot.scheduled_source_groups_by_node,
            snapshot.scheduled_scan_groups_by_node,
            snapshot.last_control_frame_signals_by_node
        );
    }

    #[tokio::test]
    async fn observability_snapshot_request_does_not_fall_back_to_source_control_summary_from_explicit_empty_scheduled_groups()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![test_watch_scan_root("nfs1", nfs1.clone())],
            host_object_grants: vec![GrantedMountRoot {
                object_ref: "node-b::nfs1".to_string(),
                host_ref: "node-b".to_string(),
                host_ip: "10.0.0.21".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: Default::default(),
                mount_point: nfs1,
                fs_source: "node-b::nfs1".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: vec![],
                interfaces: vec![],
                active: true,
            }],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::new(cfg, NodeId("node-a".to_string())).expect("init source");
        let control = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                }],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                }],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                }],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                }],
            }))
            .expect("encode source scan activate"),
        ];
        source
            .on_control_frame(&control)
            .await
            .expect("apply non-local source activate");

        assert_eq!(
            source.scheduled_source_group_ids().expect("source groups"),
            Some(std::collections::BTreeSet::new()),
            "fixture must surface explicit empty scheduled source groups before source-summary fallback recovery"
        );
        assert_eq!(
            source.scheduled_scan_group_ids().expect("scan groups"),
            Some(std::collections::BTreeSet::new()),
            "fixture must surface explicit empty scheduled scan groups before source-summary fallback recovery"
        );
        let source_summary = source.last_control_frame_signals_snapshot();
        assert!(
            !source_summary.is_empty(),
            "fixture must keep a non-empty source-owned control summary so the fallback path is actually exercised"
        );
        assert!(
            source
                .source_primary_by_group_snapshot()
                .values()
                .all(|primary| !primary.starts_with("node-a::")),
            "fixture must have no local source-primary ownership so any control summary would be recovered from explicit-empty active state instead of local-primary truth"
        );

        let snapshot = source_observability_snapshot(
            &source,
            &[],
            &Arc::new(StdMutex::new(PublishedBatchStats::default())),
        );

        assert!(
            snapshot.last_control_frame_signals_by_node.is_empty(),
            "explicit empty scheduled groups must suppress source-summary fallback just like worker-summary recovery: source_summary={source_summary:?} source={:?} scan={:?} control={:?}",
            snapshot.scheduled_source_groups_by_node,
            snapshot.scheduled_scan_groups_by_node,
            snapshot.last_control_frame_signals_by_node
        );
    }

    #[test]
    fn observability_snapshot_request_does_not_treat_historical_published_stats_as_active_recovery()
    {
        let cfg = SourceConfig::default();
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let published_stats = Arc::new(StdMutex::new(PublishedBatchStats {
            batch_count: 12,
            event_count: 345,
            control_event_count: 6,
            data_event_count: 339,
            last_published_at_us: Some(123456),
            last_published_origins: vec!["node-a::nfs1=4".to_string()],
            published_origin_counts: std::collections::BTreeMap::from([(
                "node-a::nfs1".to_string(),
                345,
            )]),
            summarized_path_origin_counts: std::collections::BTreeMap::new(),
            published_path_origin_counts: std::collections::BTreeMap::new(),
        }));

        let snapshot = source_observability_snapshot(
            &source,
            &["tick unit=runtime.exec.source generation=11 groups=[\"nfs1\"]".to_string()],
            &published_stats,
        );

        assert!(
            snapshot.last_control_frame_signals_by_node.is_empty(),
            "historical published stats alone must not resurrect active control summary in source_server observability: current_stream_generation={:?} source={:?} scan={:?} control={:?} published_batches={:?}",
            snapshot.status.current_stream_generation,
            snapshot.scheduled_source_groups_by_node,
            snapshot.scheduled_scan_groups_by_node,
            snapshot.last_control_frame_signals_by_node,
            snapshot.published_batches_by_node
        );
        assert_eq!(
            snapshot.published_batches_by_node.get("node-a"),
            Some(&12),
            "source_server observability should still preserve historical published stats for diagnostics even when they do not prove active recovery"
        );
    }
}
