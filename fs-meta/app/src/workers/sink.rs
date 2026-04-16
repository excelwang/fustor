use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bytes::Bytes;
use capanix_app_sdk::runtime::{ControlEnvelope, NodeId, RecvOpts, RuntimeWorkerBinding};
use capanix_app_sdk::{CnxError, Event, Result};
use capanix_host_adapter_fs::{HostAdapter, exchange_host_adapter_from_channel_boundary};
use capanix_runtime_entry_sdk::advanced::boundary::ChannelIoSubset;
use capanix_runtime_entry_sdk::worker_runtime::{
    RuntimeWorkerClientFactory, TypedRuntimeWorkerClient, TypedWorkerClient, TypedWorkerInit,
};

use crate::query::models::{HealthStats, QueryNode};
use crate::query::path::root_file_name_bytes;
use crate::query::request::{InternalQueryRequest, MaterializedQueryPayload, QueryOp, QueryScope};
use crate::runtime::orchestration::{SinkControlSignal, sink_control_signals_from_envelopes};
use crate::runtime::routes::{METHOD_FIND, ROUTE_TOKEN_FS_META, default_route_bindings};
use crate::sink::{SinkFileMeta, SinkStatusSnapshot, VisibilityLagSample};
use crate::source::config::{GrantedMountRoot, SourceConfig};
use crate::workers::sink_ipc::{
    SinkWorkerInitConfig, SinkWorkerRequest, SinkWorkerResponse, decode_request, decode_response,
    encode_request, encode_response,
};

const SINK_WORKER_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(15);
const SINK_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(2);
const SINK_WORKER_CONTROL_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const SINK_WORKER_UPDATE_ROOTS_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const SINK_WORKER_FORCE_FIND_TIMEOUT: Duration = Duration::from_secs(60);
const SINK_WORKER_FORCE_FIND_REPLY_IDLE_GRACE: Duration = Duration::from_secs(5);
const SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT: Duration = Duration::from_secs(60);
const SINK_WORKER_CLOSE_DRAIN_TIMEOUT: Duration = SINK_WORKER_UPDATE_ROOTS_RPC_TIMEOUT;
const SINK_WORKER_CLOSE_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(25);
const SINK_WORKER_STATUS_NONBLOCKING_PROBE_BUDGET: Duration = Duration::from_millis(350);

fn can_retry_on_control_frame(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::PeerError(message) if message == "worker not initialized"
    ) || matches!(
        err,
        CnxError::TransportClosed(_) | CnxError::Timeout | CnxError::ChannelClosed
    ) || is_retryable_worker_bridge_peer_error(err)
        || matches!(
            err,
            CnxError::AccessDenied(message)
                | CnxError::PeerError(message)
                | CnxError::Internal(message)
                if message.contains("drained/fenced")
                    && message.contains("grant attachments")
                    || message.contains("invalid or revoked grant attachment token")
                    || message.contains("missing route state for channel_buffer")
        )
}

fn is_restart_deferred_retire_pending_deactivate_batch(envelopes: &[ControlEnvelope]) -> bool {
    let Ok(signals) = sink_control_signals_from_envelopes(envelopes) else {
        return false;
    };
    !signals.is_empty()
        && signals.iter().all(|signal| {
            matches!(
                signal,
                SinkControlSignal::Deactivate { envelope, .. }
                    if matches!(
                        capanix_runtime_entry_sdk::control::decode_runtime_exec_control(envelope),
                        Ok(Some(capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(deactivate)))
                            if deactivate.reason == "restart_deferred_retire_pending"
                    )
            )
        })
}

fn is_retryable_worker_bridge_transport_error_message(message: &str) -> bool {
    message.contains("transport closed")
        && (message.contains("Connection reset by peer")
            || message.contains("early eof")
            || message.contains("Broken pipe")
            || message.contains("bridge stopped"))
}

fn is_retryable_worker_bridge_peer_error(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::PeerError(message) | CnxError::Internal(message)
            if is_retryable_worker_bridge_transport_error_message(message)
    )
}

fn is_retryable_worker_bridge_reset(err: &CnxError) -> bool {
    matches!(err, CnxError::TransportClosed(_) | CnxError::ChannelClosed)
        || is_retryable_worker_bridge_peer_error(err)
}

fn is_missing_channel_buffer_route_state(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::AccessDenied(message)
            | CnxError::PeerError(message)
            | CnxError::Internal(message)
            if message.contains("missing route state for channel_buffer")
    )
}

fn debug_control_scope_capture_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
}

fn debug_sink_worker_pre_dispatch_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_SINK_WORKER_PRE_DISPATCH").is_some()
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

fn summarize_groups_by_node(
    groups: &std::collections::BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    groups
        .iter()
        .map(|(node_id, groups)| format!("{node_id}={}", groups.join("|")))
        .collect()
}

fn summarize_sink_status_snapshot(snapshot: &SinkStatusSnapshot) -> String {
    format!(
        "groups={} scheduled={:?} control={:?} received_batches={:?} received_events={:?} received_origins={:?} received_origin_counts={:?} stream_received_batches={:?} stream_received_events={:?} stream_received_origin_counts={:?} stream_ready_origin_counts={:?} stream_deferred_origin_counts={:?} stream_dropped_origin_counts={:?} stream_applied_batches={:?} stream_applied_events={:?} stream_applied_control_events={:?} stream_applied_data_events={:?} stream_applied_origin_counts={:?} stream_last_applied_at_us={:?}",
        snapshot.groups.len(),
        summarize_groups_by_node(&snapshot.scheduled_groups_by_node),
        summarize_groups_by_node(&snapshot.last_control_frame_signals_by_node),
        snapshot.received_batches_by_node,
        snapshot.received_events_by_node,
        summarize_groups_by_node(&snapshot.last_received_origins_by_node),
        summarize_groups_by_node(&snapshot.received_origin_counts_by_node),
        snapshot.stream_received_batches_by_node,
        snapshot.stream_received_events_by_node,
        summarize_groups_by_node(&snapshot.stream_received_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_ready_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_deferred_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_dropped_origin_counts_by_node),
        snapshot.stream_applied_batches_by_node,
        snapshot.stream_applied_events_by_node,
        snapshot.stream_applied_control_events_by_node,
        snapshot.stream_applied_data_events_by_node,
        summarize_groups_by_node(&snapshot.stream_applied_origin_counts_by_node),
        snapshot.stream_last_applied_at_us_by_node
    )
}

fn scheduled_groups_from_snapshot(
    snapshot: &SinkStatusSnapshot,
) -> std::collections::BTreeSet<String> {
    snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>()
}

fn ready_groups_from_snapshot(snapshot: &SinkStatusSnapshot) -> std::collections::BTreeSet<String> {
    snapshot
        .groups
        .iter()
        .filter(|group| {
            matches!(
                classify_sink_group_status_readiness(group),
                SinkGroupStatusReadiness::Ready
            ) && group.live_nodes > 0
                && group.total_nodes > 0
        })
        .map(|group| group.group_id.clone())
        .collect::<std::collections::BTreeSet<_>>()
}

fn ready_groups_cover_scheduled_groups(
    snapshot: &SinkStatusSnapshot,
    scheduled_groups: &std::collections::BTreeSet<String>,
) -> bool {
    !scheduled_groups.is_empty()
        && scheduled_groups.is_subset(&ready_groups_from_snapshot(snapshot))
}

fn sink_status_origin_entry_group_id(entry: &str) -> &str {
    let origin = entry
        .split_once('=')
        .map(|(origin, _)| origin)
        .unwrap_or(entry);
    let scoped = origin
        .rsplit_once("::")
        .map(|(_, group_id)| group_id)
        .unwrap_or(origin);
    scoped
        .split_once(':')
        .map(|(group_id, _)| group_id)
        .unwrap_or(scoped)
}

fn snapshot_has_stream_group_evidence(
    snapshot: &SinkStatusSnapshot,
    scheduled_groups: &std::collections::BTreeSet<String>,
) -> bool {
    [
        &snapshot.stream_ready_origin_counts_by_node,
        &snapshot.stream_applied_origin_counts_by_node,
    ]
    .into_iter()
    .flat_map(|groups_by_node| groups_by_node.values())
    .flat_map(|entries| entries.iter())
    .any(|entry| scheduled_groups.contains(sink_status_origin_entry_group_id(entry)))
}

fn snapshot_has_ready_scheduled_groups(snapshot: &SinkStatusSnapshot) -> bool {
    let scheduled_groups = scheduled_groups_from_snapshot(snapshot);
    ready_groups_cover_scheduled_groups(snapshot, &scheduled_groups)
}

fn snapshot_looks_scheduled_missing_group_rows_after_stream_evidence(
    snapshot: &SinkStatusSnapshot,
) -> bool {
    let scheduled_groups = scheduled_groups_from_snapshot(snapshot);
    if scheduled_groups.is_empty() {
        return false;
    }
    let groups_by_id = snapshot
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<std::collections::BTreeMap<_, _>>();
    scheduled_groups
        .iter()
        .any(|group_id| !groups_by_id.contains_key(group_id.as_str()))
        && snapshot_has_stream_group_evidence(snapshot, &scheduled_groups)
}

fn snapshot_looks_scheduled_waiting_for_materialized_root(snapshot: &SinkStatusSnapshot) -> bool {
    let scheduled_groups = scheduled_groups_from_snapshot(snapshot);
    if scheduled_groups.is_empty() {
        return false;
    }
    let groups_by_id = snapshot
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<std::collections::BTreeMap<_, _>>();
    scheduled_groups.iter().all(|group_id| {
        groups_by_id.get(group_id.as_str()).is_some_and(|group| {
            matches!(
                classify_sink_group_status_readiness(group),
                SinkGroupStatusReadiness::WaitingForMaterializedRoot
            )
        })
    })
}

fn snapshot_looks_scheduled_pending_audit_without_stream_receipts(
    snapshot: &SinkStatusSnapshot,
) -> bool {
    let scheduled_groups = scheduled_groups_from_snapshot(snapshot);
    if scheduled_groups.is_empty() {
        return false;
    }
    let groups_by_id = snapshot
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<std::collections::BTreeMap<_, _>>();
    if !scheduled_groups.iter().all(|group_id| {
        groups_by_id.get(group_id.as_str()).is_some_and(|group| {
            matches!(
                classify_sink_group_status_readiness(group),
                SinkGroupStatusReadiness::PendingAudit
            )
        })
    }) {
        return false;
    }
    snapshot.received_batches_by_node.is_empty()
        && snapshot.received_events_by_node.is_empty()
        && snapshot.received_control_events_by_node.is_empty()
        && snapshot.received_data_events_by_node.is_empty()
        && snapshot.last_received_at_us_by_node.is_empty()
        && snapshot.last_received_origins_by_node.is_empty()
        && snapshot.received_origin_counts_by_node.is_empty()
        && snapshot.stream_received_batches_by_node.is_empty()
        && snapshot.stream_received_events_by_node.is_empty()
        && snapshot.stream_received_origin_counts_by_node.is_empty()
        && snapshot
            .stream_received_path_origin_counts_by_node
            .is_empty()
        && snapshot.stream_ready_origin_counts_by_node.is_empty()
        && snapshot.stream_ready_path_origin_counts_by_node.is_empty()
        && snapshot.stream_deferred_origin_counts_by_node.is_empty()
        && snapshot.stream_dropped_origin_counts_by_node.is_empty()
        && snapshot.stream_applied_batches_by_node.is_empty()
        && snapshot.stream_applied_events_by_node.is_empty()
        && snapshot.stream_applied_control_events_by_node.is_empty()
        && snapshot.stream_applied_data_events_by_node.is_empty()
        && snapshot.stream_applied_origin_counts_by_node.is_empty()
        && snapshot
            .stream_applied_path_origin_counts_by_node
            .is_empty()
        && snapshot.stream_last_applied_at_us_by_node.is_empty()
}

fn snapshot_looks_stale_empty(snapshot: &SinkStatusSnapshot) -> bool {
    snapshot.scheduled_groups_by_node.is_empty()
        && snapshot
            .groups
            .iter()
            .all(|group| group.live_nodes == 0 && group.total_nodes == 0)
}

fn snapshot_looks_scheduled_mixed_ready_and_unready(snapshot: &SinkStatusSnapshot) -> bool {
    let scheduled_groups = snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    if scheduled_groups.is_empty() {
        return false;
    }
    let groups_by_id = snapshot
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut any_ready = false;
    let mut any_unready = false;
    for group_id in scheduled_groups {
        match groups_by_id.get(group_id.as_str()) {
            Some(group) => match classify_sink_group_status_readiness(group) {
                SinkGroupStatusReadiness::Ready => any_ready = true,
                SinkGroupStatusReadiness::WaitingForMaterializedRoot
                | SinkGroupStatusReadiness::PendingAudit => any_unready = true,
            },
            None => {}
        }
    }
    any_ready && any_unready
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkGroupStatusReadiness {
    WaitingForMaterializedRoot,
    PendingAudit,
    Ready,
}

fn classify_sink_group_status_readiness(
    group: &crate::sink::SinkGroupStatusSnapshot,
) -> SinkGroupStatusReadiness {
    if group.live_nodes == 0 && group.total_nodes == 0 {
        return if group.primary_object_ref == "unassigned"
            || group.primary_object_ref == group.group_id
        {
            SinkGroupStatusReadiness::WaitingForMaterializedRoot
        } else {
            SinkGroupStatusReadiness::PendingAudit
        };
    }
    match group.readiness {
        crate::sink::GroupReadinessState::WaitingForMaterializedRoot => {
            SinkGroupStatusReadiness::WaitingForMaterializedRoot
        }
        crate::sink::GroupReadinessState::PendingAudit => SinkGroupStatusReadiness::PendingAudit,
        crate::sink::GroupReadinessState::Ready => SinkGroupStatusReadiness::Ready,
    }
}

fn snapshot_waiting_for_materialized_root_uses_placeholder_primary_truth(
    snapshot: &SinkStatusSnapshot,
) -> bool {
    let scheduled_groups = scheduled_groups_from_snapshot(snapshot);
    if scheduled_groups.is_empty() {
        return false;
    }
    let groups_by_id = snapshot
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<std::collections::BTreeMap<_, _>>();
    scheduled_groups.iter().all(|group_id| {
        groups_by_id.get(group_id.as_str()).is_some_and(|group| {
            matches!(
                classify_sink_group_status_readiness(group),
                SinkGroupStatusReadiness::WaitingForMaterializedRoot
            ) && (group.primary_object_ref == "unassigned"
                || group.primary_object_ref == group.group_id)
        })
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusSnapshotIssue {
    StaleEmpty,
    ScheduledMissingGroupRowsAfterStreamEvidence,
    ScheduledPendingAuditWithoutStreamReceipts,
    ScheduledWaitingForMaterializedRoot,
    ScheduledMixedReadyAndUnready,
}

fn classify_sink_status_snapshot_issue(
    snapshot: &SinkStatusSnapshot,
) -> Option<SinkStatusSnapshotIssue> {
    if snapshot_looks_stale_empty(snapshot) {
        return Some(SinkStatusSnapshotIssue::StaleEmpty);
    }
    if snapshot_looks_scheduled_missing_group_rows_after_stream_evidence(snapshot) {
        return Some(SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence);
    }
    if snapshot_looks_scheduled_pending_audit_without_stream_receipts(snapshot) {
        return Some(SinkStatusSnapshotIssue::ScheduledPendingAuditWithoutStreamReceipts);
    }
    if snapshot_looks_scheduled_waiting_for_materialized_root(snapshot) {
        return Some(SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot);
    }
    if snapshot_looks_scheduled_mixed_ready_and_unready(snapshot) {
        return Some(SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready);
    }
    None
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusLiveFoldMode {
    Blocking,
    ControlInflight,
    Steady,
    SteadyAfterRetryReset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusLiveFoldOutcome {
    ReturnLive,
    ReturnCached,
    FailClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusCachedFoldMode {
    WorkerUnavailable,
    ControlInflightNoClient,
    NotStarted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusCachedFoldOutcome {
    ReturnCached,
    FailClosed,
    PropagateError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusAvailabilityDecision {
    ReturnLive,
    ReturnCached,
    FailClosed,
    PropagateError,
}

#[derive(Clone, Debug)]
struct SinkStatusSnapshotProbeOutcome {
    snapshot: SinkStatusSnapshot,
    recovered_after_retry_reset: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SinkStatusSnapshotEvaluation {
    decision: SinkStatusAvailabilityDecision,
    should_mark_replay_required: bool,
    should_republish_zero_row_summary: bool,
}

fn cached_ready_truth_covers_live_issue(
    live_issue: SinkStatusSnapshotIssue,
    live_snapshot: &SinkStatusSnapshot,
    cached_snapshot: &SinkStatusSnapshot,
) -> bool {
    match live_issue {
        SinkStatusSnapshotIssue::ScheduledPendingAuditWithoutStreamReceipts
        | SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot => {
            let scheduled_groups = scheduled_groups_from_snapshot(live_snapshot);
            ready_groups_cover_scheduled_groups(cached_snapshot, &scheduled_groups)
        }
        SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence
        | SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready => {
            snapshot_has_ready_scheduled_groups(cached_snapshot)
        }
        SinkStatusSnapshotIssue::StaleEmpty => false,
    }
}

fn fold_live_sink_status_snapshot(
    live_snapshot: &SinkStatusSnapshot,
    cached_snapshot: &SinkStatusSnapshot,
    cached_issue: Option<SinkStatusSnapshotIssue>,
    mode: SinkStatusLiveFoldMode,
) -> SinkStatusLiveFoldOutcome {
    let Some(live_issue) = classify_sink_status_snapshot_issue(live_snapshot)
    else {
        return SinkStatusLiveFoldOutcome::ReturnLive;
    };
    match mode {
        SinkStatusLiveFoldMode::Blocking => match live_issue {
            SinkStatusSnapshotIssue::ScheduledPendingAuditWithoutStreamReceipts
            | SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot => {
                SinkStatusLiveFoldOutcome::FailClosed
            }
            SinkStatusSnapshotIssue::StaleEmpty
            | SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence
            | SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready => {
                SinkStatusLiveFoldOutcome::ReturnLive
            }
        },
        SinkStatusLiveFoldMode::ControlInflight => match live_issue {
            SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence
            | SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot
            | SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready => {
                if cached_ready_truth_covers_live_issue(live_issue, live_snapshot, cached_snapshot)
                {
                    SinkStatusLiveFoldOutcome::ReturnCached
                } else {
                    SinkStatusLiveFoldOutcome::FailClosed
                }
            }
            SinkStatusSnapshotIssue::StaleEmpty
            | SinkStatusSnapshotIssue::ScheduledPendingAuditWithoutStreamReceipts => {
                SinkStatusLiveFoldOutcome::ReturnLive
            }
        },
        SinkStatusLiveFoldMode::Steady => match live_issue {
            SinkStatusSnapshotIssue::StaleEmpty
                if matches!(
                    cached_issue,
                    Some(SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence)
                ) =>
            {
                SinkStatusLiveFoldOutcome::ReturnCached
            }
            SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence
            | SinkStatusSnapshotIssue::ScheduledPendingAuditWithoutStreamReceipts
            | SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot
            | SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready => {
                if cached_ready_truth_covers_live_issue(live_issue, live_snapshot, cached_snapshot)
                {
                    SinkStatusLiveFoldOutcome::ReturnCached
                } else {
                    SinkStatusLiveFoldOutcome::FailClosed
                }
            }
            SinkStatusSnapshotIssue::StaleEmpty => SinkStatusLiveFoldOutcome::FailClosed,
        },
        SinkStatusLiveFoldMode::SteadyAfterRetryReset => match live_issue {
            SinkStatusSnapshotIssue::StaleEmpty
                if matches!(
                    cached_issue,
                    Some(SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence)
                ) =>
            {
                SinkStatusLiveFoldOutcome::ReturnCached
            }
            SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot => {
                SinkStatusLiveFoldOutcome::ReturnLive
            }
            SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence
            | SinkStatusSnapshotIssue::ScheduledPendingAuditWithoutStreamReceipts
            | SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready => {
                if cached_ready_truth_covers_live_issue(live_issue, live_snapshot, cached_snapshot)
                {
                    SinkStatusLiveFoldOutcome::ReturnCached
                } else {
                    SinkStatusLiveFoldOutcome::FailClosed
                }
            }
            SinkStatusSnapshotIssue::StaleEmpty => SinkStatusLiveFoldOutcome::FailClosed,
        },
    }
}

fn fold_cached_sink_status_snapshot(
    cached_snapshot: &SinkStatusSnapshot,
    mode: SinkStatusCachedFoldMode,
) -> SinkStatusCachedFoldOutcome {
    let Some(cached_issue) = classify_sink_status_snapshot_issue(cached_snapshot)
    else {
        return SinkStatusCachedFoldOutcome::ReturnCached;
    };
    match mode {
        SinkStatusCachedFoldMode::WorkerUnavailable => match cached_issue {
            SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence => {
                SinkStatusCachedFoldOutcome::ReturnCached
            }
            SinkStatusSnapshotIssue::StaleEmpty
            | SinkStatusSnapshotIssue::ScheduledPendingAuditWithoutStreamReceipts
            | SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot
            | SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready => {
                SinkStatusCachedFoldOutcome::PropagateError
            }
        },
        SinkStatusCachedFoldMode::ControlInflightNoClient
        | SinkStatusCachedFoldMode::NotStarted => match cached_issue {
            SinkStatusSnapshotIssue::StaleEmpty
            | SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence
            | SinkStatusSnapshotIssue::ScheduledPendingAuditWithoutStreamReceipts
            | SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot
            | SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready => {
                SinkStatusCachedFoldOutcome::FailClosed
            }
        },
    }
}

fn cached_snapshot_needs_zero_row_republish(issue: Option<SinkStatusSnapshotIssue>) -> bool {
    matches!(issue, Some(SinkStatusSnapshotIssue::StaleEmpty))
}

fn evaluate_live_sink_status_snapshot(
    live_snapshot: &SinkStatusSnapshot,
    cached_snapshot: &SinkStatusSnapshot,
    cached_issue: Option<SinkStatusSnapshotIssue>,
    mode: SinkStatusLiveFoldMode,
) -> SinkStatusSnapshotEvaluation {
    let live_issue = classify_sink_status_snapshot_issue(live_snapshot);
    let decision = match fold_live_sink_status_snapshot(
        live_snapshot,
        cached_snapshot,
        cached_issue,
        mode,
    ) {
        SinkStatusLiveFoldOutcome::ReturnLive => SinkStatusAvailabilityDecision::ReturnLive,
        SinkStatusLiveFoldOutcome::ReturnCached => SinkStatusAvailabilityDecision::ReturnCached,
        SinkStatusLiveFoldOutcome::FailClosed => SinkStatusAvailabilityDecision::FailClosed,
    };
    SinkStatusSnapshotEvaluation {
        decision,
        should_mark_replay_required: matches!(
            live_issue,
            Some(SinkStatusSnapshotIssue::ScheduledPendingAuditWithoutStreamReceipts)
        ),
        should_republish_zero_row_summary: false,
    }
}

fn evaluate_cached_sink_status_snapshot(
    cached_snapshot: &SinkStatusSnapshot,
    mode: SinkStatusCachedFoldMode,
) -> SinkStatusSnapshotEvaluation {
    let cached_issue = classify_sink_status_snapshot_issue(cached_snapshot);
    let decision = match fold_cached_sink_status_snapshot(cached_snapshot, mode) {
        SinkStatusCachedFoldOutcome::ReturnCached => SinkStatusAvailabilityDecision::ReturnCached,
        SinkStatusCachedFoldOutcome::FailClosed => SinkStatusAvailabilityDecision::FailClosed,
        SinkStatusCachedFoldOutcome::PropagateError => SinkStatusAvailabilityDecision::PropagateError,
    };
    SinkStatusSnapshotEvaluation {
        decision,
        should_mark_replay_required: false,
        should_republish_zero_row_summary: cached_snapshot_needs_zero_row_republish(cached_issue),
    }
}

fn republish_scheduled_groups_into_zero_row_summary(
    snapshot: &mut SinkStatusSnapshot,
    node_id: &NodeId,
    groups: &std::collections::BTreeSet<String>,
) {
    if groups.is_empty() || !snapshot.scheduled_groups_by_node.is_empty() {
        return;
    }
    let zero_rows_only = !snapshot.groups.is_empty()
        && snapshot
            .groups
            .iter()
            .all(|group| group.live_nodes == 0 && group.total_nodes == 0);
    if !snapshot.groups.is_empty() && !zero_rows_only {
        return;
    }
    snapshot.scheduled_groups_by_node =
        std::collections::BTreeMap::from([(node_id.0.clone(), groups.iter().cloned().collect())]);
}

fn host_ref_matches_node_id(host_ref: &str, node_id: &NodeId) -> bool {
    host_ref == node_id.0
        || node_id
            .0
            .strip_prefix(host_ref)
            .is_some_and(|suffix| suffix.starts_with('-'))
}

fn stable_host_ref_for_node_id(node_id: &NodeId, grants: &[GrantedMountRoot]) -> String {
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

fn normalize_node_groups_key(
    groups_by_node: &mut std::collections::BTreeMap<String, Vec<String>>,
    from_node_id: &str,
    stable_host_ref: &str,
) {
    if from_node_id == stable_host_ref {
        return;
    }
    let Some(groups) = groups_by_node.remove(from_node_id) else {
        return;
    };
    let entry = groups_by_node
        .entry(stable_host_ref.to_string())
        .or_default();
    entry.extend(groups);
    entry.sort();
    entry.dedup();
}

fn normalize_sink_status_snapshot_node_keys(
    snapshot: &mut SinkStatusSnapshot,
    node_id: &NodeId,
    grants: &[GrantedMountRoot],
) {
    let stable_host_ref = stable_host_ref_for_node_id(node_id, grants);
    normalize_node_groups_key(
        &mut snapshot.scheduled_groups_by_node,
        &node_id.0,
        &stable_host_ref,
    );
    normalize_node_groups_key(
        &mut snapshot.last_control_frame_signals_by_node,
        &node_id.0,
        &stable_host_ref,
    );
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
    _shared: Arc<SharedSinkWorkerHandleState>,
    node_id: NodeId,
    worker_factory: RuntimeWorkerClientFactory,
    worker_binding: RuntimeWorkerBinding,
    worker: Arc<tokio::sync::Mutex<SharedSinkWorkerClient>>,
    config: Arc<Mutex<SourceConfig>>,
    logical_roots_cache: Arc<Mutex<Vec<crate::source::config::RootSpec>>>,
    status_cache: Arc<Mutex<SinkStatusSnapshot>>,
    scheduled_groups_cache: Arc<Mutex<Option<std::collections::BTreeSet<String>>>>,
    retained_control_state: Arc<tokio::sync::Mutex<RetainedSinkWorkerControlState>>,
    control_state_replay_required: Arc<AtomicUsize>,
    control_ops_inflight: Arc<AtomicUsize>,
}

struct SharedSinkWorkerHandleState {
    worker: Arc<tokio::sync::Mutex<SharedSinkWorkerClient>>,
    config: Arc<Mutex<SourceConfig>>,
    logical_roots_cache: Arc<Mutex<Vec<crate::source::config::RootSpec>>>,
    status_cache: Arc<Mutex<SinkStatusSnapshot>>,
    scheduled_groups_cache: Arc<Mutex<Option<std::collections::BTreeSet<String>>>>,
    retained_control_state: Arc<tokio::sync::Mutex<RetainedSinkWorkerControlState>>,
    control_state_replay_required: Arc<AtomicUsize>,
    control_ops_inflight: Arc<AtomicUsize>,
}

struct SharedSinkWorkerClient {
    instance_id: u64,
    client: Arc<TypedRuntimeWorkerClient<SinkWorkerRpc, SourceConfig>>,
}

#[derive(Default, Clone)]
struct RetainedSinkWorkerControlState {
    latest_host_grant_change: Option<SinkControlSignal>,
    active_by_route: std::collections::BTreeMap<(String, String), SinkControlSignal>,
}

fn retained_scheduled_group_ids(
    retained: &RetainedSinkWorkerControlState,
) -> Option<std::collections::BTreeSet<String>> {
    let groups = retained
        .active_by_route
        .values()
        .filter_map(|signal| match signal {
            SinkControlSignal::Activate { bound_scopes, .. } => Some(bound_scopes.as_slice()),
            _ => None,
        })
        .flat_map(|bound_scopes| bound_scopes.iter())
        .map(|scope| scope.scope_id.trim())
        .filter(|scope_id| !scope_id.is_empty())
        .map(|scope_id| scope_id.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    (!groups.is_empty()).then_some(groups)
}

struct InflightControlOpGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for InflightControlOpGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

fn next_shared_sink_worker_instance_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn sink_worker_handle_registry_key(
    node_id: &NodeId,
    worker_binding: &RuntimeWorkerBinding,
    worker_factory: &RuntimeWorkerClientFactory,
) -> String {
    let runtime_boundary_id = {
        let io_boundary = worker_factory.io_boundary();
        Arc::as_ptr(&io_boundary) as *const () as usize
    };
    format!(
        "{}|{}|{:?}|{:?}|{}|{}|{}",
        node_id.0,
        worker_binding.role_id,
        worker_binding.mode,
        worker_binding.launcher_kind,
        runtime_boundary_id,
        worker_binding
            .module_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
        worker_binding
            .socket_dir
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default()
    )
}

fn sink_worker_handle_registry()
-> &'static Mutex<std::collections::BTreeMap<String, Weak<SharedSinkWorkerHandleState>>> {
    static REGISTRY: std::sync::OnceLock<
        Mutex<std::collections::BTreeMap<String, Weak<SharedSinkWorkerHandleState>>>,
    > = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(std::collections::BTreeMap::new()))
}

fn lock_sink_worker_handle_registry() -> std::sync::MutexGuard<
    'static,
    std::collections::BTreeMap<String, Weak<SharedSinkWorkerHandleState>>,
> {
    match sink_worker_handle_registry().lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            log::warn!("sink worker handle registry lock poisoned; recovering shared handle state");
            poisoned.into_inner()
        }
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SinkWorkerCloseHook {
    pub entered: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SinkWorkerUpdateRootsHook {
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
pub(crate) struct SinkWorkerControlFrameErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
pub(crate) struct SinkWorkerControlFrameErrorQueueHook {
    pub errs: std::collections::VecDeque<CnxError>,
    pub sticky_worker_instance_id: Option<u64>,
    pub sticky_peer_err: Option<String>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SinkWorkerControlFramePauseHook {
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
pub(crate) struct SinkWorkerStatusErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SinkWorkerStatusTimeoutObserveHook {
    pub observed_timeouts: Arc<Mutex<Vec<Duration>>>,
}

#[cfg(test)]
pub(crate) struct SinkWorkerStatusSnapshotHook {
    pub snapshot: SinkStatusSnapshot,
}

#[cfg(test)]
pub(crate) struct SinkWorkerStatusResponseQueueHook {
    pub replies: std::collections::VecDeque<Result<SinkWorkerResponse>>,
}

#[cfg(test)]
pub(crate) struct SinkWorkerScheduledGroupsErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
pub(crate) struct SinkWorkerStatusNonblockingCacheFallbackHook;

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SinkWorkerRetryResetHook {
    pub reset_count: Arc<AtomicUsize>,
}

#[cfg(test)]
fn sink_worker_close_hook_cell() -> &'static Mutex<Option<SinkWorkerCloseHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerCloseHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_update_roots_hook_cell() -> &'static Mutex<Option<SinkWorkerUpdateRootsHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerUpdateRootsHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_control_frame_error_hook_cell()
-> &'static Mutex<Option<SinkWorkerControlFrameErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerControlFrameErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_control_frame_error_queue_hook_cell()
-> &'static Mutex<Option<SinkWorkerControlFrameErrorQueueHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerControlFrameErrorQueueHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_control_frame_pause_hook_cell()
-> &'static Mutex<Option<SinkWorkerControlFramePauseHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerControlFramePauseHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_status_error_hook_cell() -> &'static Mutex<Option<SinkWorkerStatusErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerStatusErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_status_snapshot_hook_cell() -> &'static Mutex<Option<SinkWorkerStatusSnapshotHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerStatusSnapshotHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_status_timeout_observe_hook_cell()
-> &'static Mutex<Option<SinkWorkerStatusTimeoutObserveHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerStatusTimeoutObserveHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_status_response_queue_hook_cell()
-> &'static Mutex<Option<SinkWorkerStatusResponseQueueHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerStatusResponseQueueHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_scheduled_groups_error_hook_cell()
-> &'static Mutex<Option<SinkWorkerScheduledGroupsErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerScheduledGroupsErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_status_nonblocking_cache_fallback_hook_cell()
-> &'static Mutex<Option<SinkWorkerStatusNonblockingCacheFallbackHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerStatusNonblockingCacheFallbackHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_retry_reset_hook_cell() -> &'static Mutex<Option<SinkWorkerRetryResetHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerRetryResetHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub(crate) fn install_sink_worker_close_hook(hook: SinkWorkerCloseHook) {
    let mut guard = match sink_worker_close_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_update_roots_hook(hook: SinkWorkerUpdateRootsHook) {
    let mut guard = match sink_worker_update_roots_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_close_hook() {
    let mut guard = match sink_worker_close_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_update_roots_hook() {
    let mut guard = match sink_worker_update_roots_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn install_sink_worker_control_frame_error_hook(hook: SinkWorkerControlFrameErrorHook) {
    let mut guard = match sink_worker_control_frame_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_control_frame_error_queue_hook(
    hook: SinkWorkerControlFrameErrorQueueHook,
) {
    let mut guard = match sink_worker_control_frame_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_control_frame_pause_hook(hook: SinkWorkerControlFramePauseHook) {
    let mut guard = match sink_worker_control_frame_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_error_hook(hook: SinkWorkerStatusErrorHook) {
    let mut guard = match sink_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_snapshot_hook(hook: SinkWorkerStatusSnapshotHook) {
    let mut guard = match sink_worker_status_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_timeout_observe_hook(
    hook: SinkWorkerStatusTimeoutObserveHook,
) {
    let mut guard = match sink_worker_status_timeout_observe_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_response_queue_hook(
    hook: SinkWorkerStatusResponseQueueHook,
) {
    let mut guard = match sink_worker_status_response_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_scheduled_groups_error_hook(
    hook: SinkWorkerScheduledGroupsErrorHook,
) {
    let mut guard = match sink_worker_scheduled_groups_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_nonblocking_cache_fallback_hook(
    hook: SinkWorkerStatusNonblockingCacheFallbackHook,
) {
    let mut guard = match sink_worker_status_nonblocking_cache_fallback_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_retry_reset_hook(hook: SinkWorkerRetryResetHook) {
    let mut guard = match sink_worker_retry_reset_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_control_frame_error_hook() {
    let mut guard = match sink_worker_control_frame_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
    drop(guard);
    let mut queued = match sink_worker_control_frame_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *queued = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_scheduled_groups_error_hook() {
    let mut guard = match sink_worker_scheduled_groups_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_status_nonblocking_cache_fallback_hook() {
    let mut guard = match sink_worker_status_nonblocking_cache_fallback_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_retry_reset_hook() {
    let mut guard = match sink_worker_retry_reset_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_control_frame_pause_hook() {
    let mut guard = match sink_worker_control_frame_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_status_error_hook() {
    let mut guard = match sink_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_status_snapshot_hook() {
    let mut guard = match sink_worker_status_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_status_timeout_observe_hook() {
    let mut guard = match sink_worker_status_timeout_observe_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_status_response_queue_hook() {
    let mut guard = match sink_worker_status_response_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn take_sink_worker_status_error_hook() -> Option<CnxError> {
    let mut guard = match sink_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
fn take_sink_worker_status_snapshot_hook() -> Option<SinkStatusSnapshot> {
    let mut guard = match sink_worker_status_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.snapshot)
}

#[cfg(test)]
fn record_sink_worker_status_timeout_probe(timeout: Duration) {
    let hook = {
        let guard = match sink_worker_status_timeout_observe_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        let mut observed = match hook.observed_timeouts.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        observed.push(timeout);
    }
}

#[cfg(test)]
fn take_sink_worker_status_response_queue_hook() -> Option<Result<SinkWorkerResponse>> {
    let mut guard = match sink_worker_status_response_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    let reply = hook.replies.pop_front();
    if hook.replies.is_empty() {
        *guard = None;
    }
    reply
}

#[cfg(test)]
fn take_sink_worker_scheduled_groups_error_hook() -> Option<CnxError> {
    let mut guard = match sink_worker_scheduled_groups_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
fn take_sink_worker_status_nonblocking_cache_fallback_hook() -> bool {
    let mut guard = match sink_worker_status_nonblocking_cache_fallback_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().is_some()
}

#[cfg(test)]
fn take_sink_worker_control_frame_error_hook(current_worker_instance_id: u64) -> Option<CnxError> {
    {
        let mut guard = match sink_worker_control_frame_error_queue_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(hook) = guard.as_mut() {
            if hook.sticky_worker_instance_id == Some(current_worker_instance_id)
                && let Some(err) = hook.sticky_peer_err.clone()
            {
                return Some(CnxError::PeerError(err));
            }
            if let Some(err) = hook.errs.pop_front() {
                if hook.errs.is_empty() && hook.sticky_peer_err.is_none() {
                    *guard = None;
                }
                return Some(err);
            }
            if hook.errs.is_empty() && hook.sticky_peer_err.is_none() {
                *guard = None;
            }
        }
    }
    let mut guard = match sink_worker_control_frame_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
async fn maybe_pause_before_on_control_frame_rpc() {
    let hook = {
        let mut guard = match sink_worker_control_frame_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.take()
    };
    if let Some(hook) = hook {
        let mut release = std::pin::pin!(hook.release.notified());
        std::future::poll_fn(|cx| {
            let _ = std::future::Future::poll(release.as_mut(), cx);
            std::task::Poll::Ready(())
        })
        .await;
        hook.entered.notify_waiters();
        release.await;
    }
}

#[cfg(test)]
fn notify_sink_worker_close_started() {
    let hook = {
        let guard = match sink_worker_close_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        hook.entered.notify_waiters();
    }
}

#[cfg(test)]
fn notify_sink_worker_retry_reset() {
    let hook = {
        let guard = match sink_worker_retry_reset_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        hook.reset_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
async fn maybe_pause_before_update_logical_roots_rpc() {
    let hook = {
        let mut guard = match sink_worker_update_roots_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.take()
    };
    if let Some(hook) = hook {
        let mut release = std::pin::pin!(hook.release.notified());
        std::future::poll_fn(|cx| {
            let _ = std::future::Future::poll(release.as_mut(), cx);
            std::task::Poll::Ready(())
        })
        .await;
        hook.entered.notify_waiters();
        release.await;
    }
}

impl SinkWorkerClientHandle {
    pub(crate) fn new(
        node_id: NodeId,
        config: SourceConfig,
        worker_binding: RuntimeWorkerBinding,
        worker_factory: RuntimeWorkerClientFactory,
    ) -> Result<Self> {
        let key = sink_worker_handle_registry_key(&node_id, &worker_binding, &worker_factory);
        let shared = {
            let mut registry = lock_sink_worker_handle_registry();
            if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
                existing
            } else {
                let shared = Arc::new(SharedSinkWorkerHandleState {
                    worker: Arc::new(tokio::sync::Mutex::new(SharedSinkWorkerClient {
                        instance_id: next_shared_sink_worker_instance_id(),
                        client: Arc::new(worker_factory.connect(
                            node_id.clone(),
                            config.clone(),
                            worker_binding.clone(),
                        )?),
                    })),
                    config: Arc::new(Mutex::new(config.clone())),
                    logical_roots_cache: Arc::new(Mutex::new(config.roots.clone())),
                    status_cache: Arc::new(Mutex::new(SinkStatusSnapshot::default())),
                    scheduled_groups_cache: Arc::new(Mutex::new(None)),
                    retained_control_state: Arc::new(tokio::sync::Mutex::new(
                        RetainedSinkWorkerControlState::default(),
                    )),
                    control_state_replay_required: Arc::new(AtomicUsize::new(0)),
                    control_ops_inflight: Arc::new(AtomicUsize::new(0)),
                });
                registry.insert(key, Arc::downgrade(&shared));
                shared
            }
        };
        Ok(Self {
            _shared: shared.clone(),
            node_id,
            worker_factory,
            worker_binding,
            worker: shared.worker.clone(),
            config: shared.config.clone(),
            logical_roots_cache: shared.logical_roots_cache.clone(),
            status_cache: shared.status_cache.clone(),
            scheduled_groups_cache: shared.scheduled_groups_cache.clone(),
            retained_control_state: shared.retained_control_state.clone(),
            control_state_replay_required: shared.control_state_replay_required.clone(),
            control_ops_inflight: shared.control_ops_inflight.clone(),
        })
    }

    fn begin_control_op(&self) -> InflightControlOpGuard {
        self.control_ops_inflight.fetch_add(1, Ordering::Relaxed);
        InflightControlOpGuard {
            counter: self.control_ops_inflight.clone(),
        }
    }

    fn control_op_inflight(&self) -> bool {
        self.control_ops_inflight.load(Ordering::Relaxed) > 0
    }

    pub(crate) async fn wait_for_control_ops_to_drain(&self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        while self.control_op_inflight() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(SINK_WORKER_CLOSE_DRAIN_POLL_INTERVAL).await;
        }
    }

    async fn shared_worker(
        &self,
    ) -> (
        u64,
        Arc<TypedRuntimeWorkerClient<SinkWorkerRpc, SourceConfig>>,
    ) {
        let guard = self.worker.lock().await;
        (guard.instance_id, guard.client.clone())
    }

    async fn worker_client(&self) -> Arc<TypedRuntimeWorkerClient<SinkWorkerRpc, SourceConfig>> {
        self.shared_worker().await.1
    }

    async fn existing_client(&self) -> Result<Option<TypedWorkerClient<SinkWorkerRpc>>> {
        self.worker_client().await.existing_client().await
    }

    async fn current_generation_tick_fast_path_eligible(&self) -> bool {
        self.control_state_replay_required.load(Ordering::Acquire) == 0
            && self.existing_client().await.ok().flatten().is_some()
    }

    fn current_config(&self) -> Result<SourceConfig> {
        self.config
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| CnxError::Internal("sink worker config lock poisoned".into()))
    }

    fn update_cached_runtime_config(
        &self,
        roots: &[crate::source::config::RootSpec],
        host_object_grants: &[GrantedMountRoot],
    ) -> Result<()> {
        let mut guard = self
            .config
            .lock()
            .map_err(|_| CnxError::Internal("sink worker config lock poisoned".into()))?;
        guard.roots = roots.to_vec();
        guard.host_object_grants = host_object_grants.to_vec();
        Ok(())
    }

    #[cfg(test)]
    async fn worker_instance_id_for_tests(&self) -> u64 {
        self.shared_worker().await.0
    }

    #[cfg(test)]
    async fn shared_worker_identity_for_tests(&self) -> usize {
        Arc::as_ptr(&self.worker_client().await) as *const () as usize
    }

    #[cfg(test)]
    async fn shared_worker_existing_client_for_tests(
        &self,
    ) -> Result<Option<TypedWorkerClient<SinkWorkerRpc>>> {
        self.worker_client().await.existing_client().await
    }

    #[cfg(test)]
    async fn shutdown_shared_worker_for_tests(&self, timeout: Duration) -> Result<()> {
        self.worker_client().await.shutdown(timeout).await
    }

    fn cached_logical_roots(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        self.logical_roots_cache
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| CnxError::Internal("sink worker logical roots cache lock poisoned".into()))
    }

    fn update_cached_logical_roots(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
    ) -> Result<()> {
        let mut guard = self.logical_roots_cache.lock().map_err(|_| {
            CnxError::Internal("sink worker logical roots cache lock poisoned".into())
        })?;
        *guard = roots;
        Ok(())
    }

    pub(crate) fn cached_status_snapshot(&self) -> Result<SinkStatusSnapshot> {
        self.status_cache
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| CnxError::Internal("sink worker status cache lock poisoned".into()))
    }

    fn update_cached_status_snapshot(&self, snapshot: SinkStatusSnapshot) -> Result<()> {
        let mut guard = self
            .status_cache
            .lock()
            .map_err(|_| CnxError::Internal("sink worker status cache lock poisoned".into()))?;
        *guard = snapshot;
        Ok(())
    }

    fn cached_scheduled_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.scheduled_groups_cache
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| {
                CnxError::Internal("sink worker scheduled groups cache lock poisoned".into())
            })
    }

    fn replace_cached_scheduled_group_ids(
        &self,
        groups: Option<std::collections::BTreeSet<String>>,
    ) -> Result<()> {
        let mut guard = self.scheduled_groups_cache.lock().map_err(|_| {
            CnxError::Internal("sink worker scheduled groups cache lock poisoned".into())
        })?;
        *guard = groups;
        Ok(())
    }

    fn update_cached_scheduled_group_ids(
        &self,
        groups: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        self.replace_cached_scheduled_group_ids(Some(groups.clone()))
    }

    fn republish_cached_scheduled_groups_into_empty_status_summary(&self) -> Result<()> {
        let Some(groups) = self.cached_scheduled_group_ids()? else {
            return Ok(());
        };
        let mut guard = self
            .status_cache
            .lock()
            .map_err(|_| CnxError::Internal("sink worker status cache lock poisoned".into()))?;
        republish_scheduled_groups_into_zero_row_summary(&mut guard, &self.node_id, &groups);
        Ok(())
    }

    fn retain_cached_status_for_surviving_roots(
        &self,
        roots: &[crate::source::config::RootSpec],
    ) -> Result<()> {
        let surviving_groups = roots
            .iter()
            .map(|root| root.id.clone())
            .collect::<std::collections::BTreeSet<_>>();

        {
            let mut guard = self.scheduled_groups_cache.lock().map_err(|_| {
                CnxError::Internal("sink worker scheduled groups cache lock poisoned".into())
            })?;
            *guard = (!surviving_groups.is_empty()).then_some(surviving_groups.clone());
        }

        let filter_group_entries =
            |entries_by_node: &mut std::collections::BTreeMap<String, Vec<String>>| {
                entries_by_node.retain(|_, entries| {
                    entries.retain(|entry| {
                        surviving_groups.contains(sink_status_origin_entry_group_id(entry))
                    });
                    !entries.is_empty()
                });
            };

        let mut guard = self
            .status_cache
            .lock()
            .map_err(|_| CnxError::Internal("sink worker status cache lock poisoned".into()))?;
        guard
            .groups
            .retain(|group| surviving_groups.contains(&group.group_id));
        guard.scheduled_groups_by_node.retain(|_, groups| {
            groups.retain(|group_id| surviving_groups.contains(group_id));
            !groups.is_empty()
        });
        filter_group_entries(&mut guard.last_received_origins_by_node);
        filter_group_entries(&mut guard.received_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_received_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_received_path_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_ready_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_ready_path_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_deferred_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_dropped_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_applied_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_applied_path_origin_counts_by_node);
        guard.live_nodes = guard.groups.iter().map(|group| group.live_nodes).sum();
        guard.tombstoned_count = guard
            .groups
            .iter()
            .map(|group| group.tombstoned_count)
            .sum();
        guard.attested_count = guard.groups.iter().map(|group| group.attested_count).sum();
        guard.suspect_count = guard.groups.iter().map(|group| group.suspect_count).sum();
        guard.blind_spot_count = guard
            .groups
            .iter()
            .map(|group| group.blind_spot_count)
            .sum();
        guard.shadow_time_us = guard
            .groups
            .iter()
            .map(|group| group.shadow_time_us)
            .max()
            .unwrap_or(0);
        guard.estimated_heap_bytes = guard
            .groups
            .iter()
            .map(|group| group.estimated_heap_bytes)
            .sum();
        Ok(())
    }

    async fn with_started_retry<T, F, Fut>(&self, op: F) -> Result<T>
    where
        F: Fn(TypedWorkerClient<SinkWorkerRpc>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let closure_entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker = self.worker_client().await;
        let existing_client = worker.existing_client().await?.is_some();
        if debug_sink_worker_pre_dispatch_enabled() {
            eprintln!(
                "fs_meta_sink_worker_client: pre_dispatch with_started_retry begin node={} existing_client={}",
                self.node_id.0, existing_client
            );
        }
        let result = worker
            .with_started_retry(|client| {
                closure_entered.store(true, std::sync::atomic::Ordering::Relaxed);
                op(client)
            })
            .await;
        let closure_entered = closure_entered.load(std::sync::atomic::Ordering::Relaxed);
        if debug_sink_worker_pre_dispatch_enabled() {
            match &result {
                Ok(_) => eprintln!(
                    "fs_meta_sink_worker_client: pre_dispatch with_started_retry done node={} ok=true closure_entered={}",
                    self.node_id.0, closure_entered
                ),
                Err(err) => eprintln!(
                    "fs_meta_sink_worker_client: pre_dispatch with_started_retry done node={} ok=false closure_entered={} err={}",
                    self.node_id.0, closure_entered, err
                ),
            }
        }
        result
    }

    async fn replace_shared_worker_client(
        &self,
    ) -> Result<Arc<TypedRuntimeWorkerClient<SinkWorkerRpc, SourceConfig>>> {
        #[cfg(test)]
        notify_sink_worker_retry_reset();
        let replacement = SharedSinkWorkerClient {
            instance_id: next_shared_sink_worker_instance_id(),
            client: Arc::new(self.worker_factory.connect(
                self.node_id.clone(),
                self.current_config()?,
                self.worker_binding.clone(),
            )?),
        };
        let stale_client = {
            let mut guard = self.worker.lock().await;
            let stale = guard.client.clone();
            *guard = replacement;
            stale
        };
        self.control_state_replay_required
            .store(1, Ordering::Release);
        Ok(stale_client)
    }

    async fn reconnect_shared_worker_client_detached(&self) -> Result<()> {
        let stale_client = self.replace_shared_worker_client().await?;
        tokio::spawn(async move {
            let _ = stale_client.shutdown(Duration::from_millis(250)).await;
        });
        Ok(())
    }

    async fn reset_shared_worker_client_for_retry(&self) -> Result<()> {
        self.reconnect_shared_worker_client_detached().await
    }

    async fn retain_control_signals(&self, signals: &[SinkControlSignal]) -> Result<()> {
        let scheduled_groups = {
            let mut retained = self.retained_control_state.lock().await;
            for signal in signals {
                match signal {
                    SinkControlSignal::RuntimeHostGrantChange { .. } => {
                        retained.latest_host_grant_change = Some(signal.clone());
                    }
                    SinkControlSignal::Activate {
                        unit, route_key, ..
                    } => {
                        retained.active_by_route.insert(
                            (unit.unit_id().to_string(), route_key.clone()),
                            signal.clone(),
                        );
                    }
                    SinkControlSignal::Deactivate {
                        unit, route_key, ..
                    } => {
                        retained
                            .active_by_route
                            .remove(&(unit.unit_id().to_string(), route_key.clone()));
                    }
                    SinkControlSignal::Tick { .. } | SinkControlSignal::Passthrough(_) => {}
                }
            }
            retained_scheduled_group_ids(&retained)
        };
        self.replace_cached_scheduled_group_ids(scheduled_groups)
    }

    async fn replay_retained_control_state_if_needed_with_timeouts(
        &self,
        total_timeout: Duration,
        rpc_timeout: Duration,
    ) -> Result<()> {
        if self
            .control_state_replay_required
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let envelopes = {
            let retained = self.retained_control_state.lock().await;
            let mut envelopes = Vec::new();
            if let Some(host_grant_change) = retained.latest_host_grant_change.as_ref() {
                envelopes.push(host_grant_change.envelope());
            }
            envelopes.extend(
                retained
                    .active_by_route
                    .values()
                    .map(SinkControlSignal::envelope),
            );
            envelopes
        };
        if envelopes.is_empty() {
            return Ok(());
        }
        if let Err(err) = self
            .on_control_frame_with_timeouts(envelopes, total_timeout, rpc_timeout)
            .await
        {
            self.control_state_replay_required
                .store(1, Ordering::Release);
            return Err(err);
        }
        Ok(())
    }

    async fn replay_retained_control_state_if_needed(&self) -> Result<()> {
        self.replay_retained_control_state_if_needed_with_timeouts(
            SINK_WORKER_CONTROL_TOTAL_TIMEOUT,
            SINK_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
    }

    async fn client(&self) -> Result<TypedWorkerClient<SinkWorkerRpc>> {
        self.worker_client().await.client().await
    }

    async fn call_worker(
        client: &TypedWorkerClient<SinkWorkerRpc>,
        request: SinkWorkerRequest,
        timeout: Duration,
    ) -> Result<SinkWorkerResponse> {
        client.call_with_timeout(request, timeout).await
    }

    pub async fn ensure_started(&self) -> Result<()> {
        eprintln!(
            "fs_meta_sink_worker_client: ensure_started begin node={}",
            self.node_id.0
        );
        self.worker_client().await.ensure_started().await.map(|_| {
            eprintln!(
                "fs_meta_sink_worker_client: ensure_started ok node={}",
                self.node_id.0
            );
        })
    }

    pub async fn update_logical_roots(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
        host_object_grants: Vec<GrantedMountRoot>,
    ) -> Result<()> {
        let _inflight = self.begin_control_op();
        eprintln!(
            "fs_meta_sink_worker_client: update_logical_roots begin node={} roots={} grants={}",
            self.node_id.0,
            roots.len(),
            host_object_grants.len()
        );
        self.with_started_retry(|client| {
            let roots = roots.clone();
            let host_object_grants = host_object_grants.clone();
            async move {
                #[cfg(test)]
                maybe_pause_before_update_logical_roots_rpc().await;
                match Self::call_worker(
                    &client,
                    SinkWorkerRequest::UpdateLogicalRoots {
                        roots: roots.clone(),
                        host_object_grants: host_object_grants.clone(),
                    },
                    SINK_WORKER_UPDATE_ROOTS_RPC_TIMEOUT,
                )
                .await?
                {
                    SinkWorkerResponse::Ack => {
                        eprintln!(
                            "fs_meta_sink_worker_client: update_logical_roots ok node={} roots={} grants={}",
                            self.node_id.0,
                            roots.len(),
                            host_object_grants.len()
                        );
                        Ok(())
                    }
                    other => Err(CnxError::ProtocolViolation(format!(
                        "unexpected sink worker response for update roots: {:?}",
                        other
                    ))),
                }
            }
        })
        .await?;
        self.update_cached_logical_roots(roots.clone())?;
        self.update_cached_runtime_config(&roots, &host_object_grants)?;
        self.retain_cached_status_for_surviving_roots(&roots)
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        self.cached_logical_roots()
    }

    pub async fn logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        let roots = match Self::call_worker(
            &self.client().await?,
            SinkWorkerRequest::LogicalRootsSnapshot,
            Duration::from_secs(5),
        )
        .await?
        {
            SinkWorkerResponse::LogicalRoots(roots) => roots,
            other => {
                return Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker response for logical roots: {:?}",
                    other
                )));
            }
        };
        self.update_cached_logical_roots(roots.clone())?;
        Ok(roots)
    }

    pub async fn health(&self) -> Result<HealthStats> {
        match Self::call_worker(
            &self.client().await?,
            SinkWorkerRequest::Health,
            Duration::from_secs(5),
        )
        .await?
        {
            SinkWorkerResponse::Health(health) => Ok(health),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for health: {:?}",
                other
            ))),
        }
    }

    pub async fn status_snapshot(&self) -> Result<SinkStatusSnapshot> {
        let replay_required = self.control_state_replay_required.load(Ordering::Acquire) > 0;
        self.replay_retained_control_state_if_needed().await?;
        let mut snapshot = self
            .status_snapshot_with_timeout(Duration::from_secs(5))
            .await?;
        let grants = self
            .config
            .lock()
            .map_err(|_| CnxError::Internal("sink worker config lock poisoned".into()))?
            .host_object_grants
            .clone();
        normalize_sink_status_snapshot_node_keys(&mut snapshot, &self.node_id, &grants);
        if let Some(groups) = self.cached_scheduled_group_ids()? {
            republish_scheduled_groups_into_zero_row_summary(&mut snapshot, &self.node_id, &groups);
        }
        if debug_control_scope_capture_enabled() {
            eprintln!(
                "fs_meta_sink_worker_client: status_snapshot reply node={} {}",
                self.node_id.0,
                summarize_sink_status_snapshot(&snapshot)
            );
        }
        if snapshot_looks_scheduled_pending_audit_without_stream_receipts(&snapshot) {
            if debug_control_scope_capture_enabled() {
                eprintln!(
                    "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason=scheduled_pending_audit_without_stream_receipts replay_required={} {}",
                    self.node_id.0,
                    replay_required,
                    summarize_sink_status_snapshot(&snapshot)
                );
            }
            return Err(CnxError::Timeout);
        }
        if snapshot_looks_scheduled_waiting_for_materialized_root(&snapshot) {
            if debug_control_scope_capture_enabled() {
                eprintln!(
                    "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason=scheduled_waiting_for_materialized_root replay_required={} {}",
                    self.node_id.0,
                    replay_required,
                    summarize_sink_status_snapshot(&snapshot)
                );
            }
            return Err(CnxError::Timeout);
        }
        self.update_cached_status_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }

    pub async fn status_snapshot_nonblocking(&self) -> Result<SinkStatusSnapshot> {
        #[cfg(test)]
        if take_sink_worker_status_nonblocking_cache_fallback_hook() {
            let snapshot = self.cached_status_snapshot()?;
            if debug_control_scope_capture_enabled() {
                eprintln!(
                    "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason=test_hook {}",
                    self.node_id.0,
                    summarize_sink_status_snapshot(&snapshot)
                );
            }
            return Ok(snapshot);
        }
        if self.control_op_inflight() {
            let snapshot = self.cached_status_snapshot()?;
            if self.existing_client().await?.is_some() {
                match self
                    .status_snapshot_with_timeout(SINK_WORKER_STATUS_NONBLOCKING_PROBE_BUDGET)
                    .await
                {
                    Ok(mut live_snapshot) => {
                        let grants = self
                            .config
                            .lock()
                            .map_err(|_| {
                                CnxError::Internal("sink worker config lock poisoned".into())
                            })?
                            .host_object_grants
                            .clone();
                        normalize_sink_status_snapshot_node_keys(
                            &mut live_snapshot,
                            &self.node_id,
                            &grants,
                        );
                        let cached_snapshot = self.cached_status_snapshot()?;
                        let missing_group_rows_after_stream_evidence =
                            snapshot_looks_scheduled_missing_group_rows_after_stream_evidence(
                                &live_snapshot,
                            );
                        let waiting_for_materialized_root =
                            snapshot_looks_scheduled_waiting_for_materialized_root(&live_snapshot);
                        let mixed_ready_and_unready =
                            snapshot_looks_scheduled_mixed_ready_and_unready(&live_snapshot);
                        if missing_group_rows_after_stream_evidence
                            || waiting_for_materialized_root
                            || mixed_ready_and_unready
                        {
                            if snapshot_has_ready_scheduled_groups(&cached_snapshot) {
                                if debug_control_scope_capture_enabled() {
                                    eprintln!(
                                        "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason=control_inflight live={} cached={}",
                                        self.node_id.0,
                                        summarize_sink_status_snapshot(&live_snapshot),
                                        summarize_sink_status_snapshot(&cached_snapshot)
                                    );
                                }
                                return Ok(cached_snapshot);
                            }
                            let reason = if missing_group_rows_after_stream_evidence {
                                "control_inflight_missing_scheduled_group_rows_after_stream_evidence"
                            } else if waiting_for_materialized_root {
                                "control_inflight_scheduled_waiting_for_materialized_root"
                            } else {
                                "control_inflight_scheduled_mixed_ready_and_unready"
                            };
                            if debug_control_scope_capture_enabled() {
                                eprintln!(
                                    "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason={} {}",
                                    self.node_id.0,
                                    reason,
                                    summarize_sink_status_snapshot(&live_snapshot)
                                );
                            }
                            return Err(CnxError::Timeout);
                        }
                        self.update_cached_status_snapshot(live_snapshot.clone())?;
                        if debug_control_scope_capture_enabled() {
                            eprintln!(
                                "fs_meta_sink_worker_client: status_snapshot short_probe node={} reason=control_inflight {}",
                                self.node_id.0,
                                summarize_sink_status_snapshot(&live_snapshot)
                            );
                        }
                        return Ok(live_snapshot);
                    }
                    Err(err) => {
                        if snapshot_looks_stale_empty(&snapshot) {
                            self.republish_cached_scheduled_groups_into_empty_status_summary()?;
                        }
                        if snapshot_looks_scheduled_waiting_for_materialized_root(&snapshot) {
                            if debug_control_scope_capture_enabled() {
                                eprintln!(
                                    "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason=control_inflight_scheduled_waiting_for_materialized_root_cached_snapshot {}",
                                    self.node_id.0,
                                    summarize_sink_status_snapshot(&snapshot)
                                );
                            }
                            return Err(CnxError::Timeout);
                        }
                        if snapshot_looks_scheduled_missing_group_rows_after_stream_evidence(
                            &snapshot,
                        ) {
                            if debug_control_scope_capture_enabled() {
                                eprintln!(
                                    "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason=control_inflight_missing_scheduled_group_rows_cached_snapshot {}",
                                    self.node_id.0,
                                    summarize_sink_status_snapshot(&snapshot)
                                );
                            }
                            return Err(CnxError::Timeout);
                        }
                        if snapshot_looks_scheduled_mixed_ready_and_unready(&snapshot) {
                            if debug_control_scope_capture_enabled() {
                                eprintln!(
                                    "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason=control_inflight_scheduled_mixed_ready_and_unready_cached_snapshot {}",
                                    self.node_id.0,
                                    summarize_sink_status_snapshot(&snapshot)
                                );
                            }
                            return Err(CnxError::Timeout);
                        }
                        return Err(err);
                    }
                }
            }
            if snapshot_looks_stale_empty(&snapshot) {
                self.republish_cached_scheduled_groups_into_empty_status_summary()?;
            }
            if snapshot_looks_scheduled_waiting_for_materialized_root(&snapshot) {
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason=control_inflight_scheduled_waiting_for_materialized_root_cached_snapshot {}",
                        self.node_id.0,
                        summarize_sink_status_snapshot(&snapshot)
                    );
                }
                return Err(CnxError::Timeout);
            }
            if snapshot_looks_scheduled_missing_group_rows_after_stream_evidence(&snapshot) {
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason=control_inflight_missing_scheduled_group_rows_cached_snapshot {}",
                        self.node_id.0,
                        summarize_sink_status_snapshot(&snapshot)
                    );
                }
                return Err(CnxError::Timeout);
            }
            if snapshot_looks_scheduled_mixed_ready_and_unready(&snapshot) {
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason=control_inflight_scheduled_mixed_ready_and_unready_cached_snapshot {}",
                        self.node_id.0,
                        summarize_sink_status_snapshot(&snapshot)
                    );
                }
                return Err(CnxError::Timeout);
            }
            if debug_control_scope_capture_enabled() {
                eprintln!(
                    "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason=control_inflight {}",
                    self.node_id.0,
                    summarize_sink_status_snapshot(&snapshot)
                );
            }
            return Ok(snapshot);
        }
        match self.existing_client().await? {
            Some(_) => {
                let cached_snapshot = self.cached_status_snapshot()?;
                if snapshot_looks_stale_empty(&cached_snapshot) {
                    self.republish_cached_scheduled_groups_into_empty_status_summary()?;
                }
                match self
                    .status_snapshot_with_timeout_outcome(
                        SINK_WORKER_STATUS_NONBLOCKING_PROBE_BUDGET,
                    )
                    .await
                {
                    Ok(probe_outcome) => {
                        let mut snapshot = probe_outcome.snapshot;
                        let cached_snapshot = self.cached_status_snapshot()?;
                        let grants = self
                            .config
                            .lock()
                            .map_err(|_| {
                                CnxError::Internal("sink worker config lock poisoned".into())
                            })?
                            .host_object_grants
                            .clone();
                        normalize_sink_status_snapshot_node_keys(
                            &mut snapshot,
                            &self.node_id,
                            &grants,
                        );
                        if debug_control_scope_capture_enabled() {
                            eprintln!(
                                "fs_meta_sink_worker_client: status_snapshot reply node={} {}",
                                self.node_id.0,
                                summarize_sink_status_snapshot(&snapshot)
                            );
                        }
                        let cached_issue = classify_sink_status_snapshot_issue(&cached_snapshot);
                        let fold_mode = if probe_outcome.recovered_after_retry_reset {
                            SinkStatusLiveFoldMode::SteadyAfterRetryReset
                        } else {
                            SinkStatusLiveFoldMode::Steady
                        };
                        let evaluation = evaluate_live_sink_status_snapshot(
                            &snapshot,
                            &cached_snapshot,
                            cached_issue,
                            fold_mode,
                        );
                        let live_issue = classify_sink_status_snapshot_issue(&snapshot);
                        if evaluation.should_mark_replay_required {
                            self.control_state_replay_required
                                .store(1, Ordering::Release);
                        }
                        match evaluation.decision {
                            SinkStatusAvailabilityDecision::ReturnLive => {
                                if debug_control_scope_capture_enabled()
                                    && probe_outcome.recovered_after_retry_reset
                                    && matches!(
                                        live_issue,
                                        Some(
                                            SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot
                                        )
                                    )
                                {
                                    eprintln!(
                                        "fs_meta_sink_worker_client: status_snapshot return_live node={} reason=recovered_after_retry_reset_waiting_for_materialized_root {}",
                                        self.node_id.0,
                                        summarize_sink_status_snapshot(&snapshot)
                                    );
                                }
                            }
                            SinkStatusAvailabilityDecision::ReturnCached => {
                                if debug_control_scope_capture_enabled() {
                                    let reason = match live_issue {
                                        Some(SinkStatusSnapshotIssue::StaleEmpty) => {
                                            "stale_empty_after_retry_reset"
                                        }
                                        Some(
                                            SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence,
                                        ) => "missing_scheduled_group_rows_after_stream_evidence",
                                        Some(
                                            SinkStatusSnapshotIssue::ScheduledPendingAuditWithoutStreamReceipts,
                                        ) => "replay_required_not_ready_without_stream_evidence",
                                        Some(
                                            SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot,
                                        ) => "scheduled_waiting_for_materialized_root",
                                        Some(
                                            SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready,
                                        ) => "scheduled_mixed_ready_and_unready",
                                        None => "steady",
                                    };
                                    eprintln!(
                                        "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason={} live={} cached={}",
                                        self.node_id.0,
                                        reason,
                                        summarize_sink_status_snapshot(&snapshot),
                                        summarize_sink_status_snapshot(&cached_snapshot)
                                    );
                                }
                                return Ok(cached_snapshot);
                            }
                            SinkStatusAvailabilityDecision::FailClosed => {
                                if debug_control_scope_capture_enabled() {
                                    let reason = match live_issue {
                                        Some(SinkStatusSnapshotIssue::StaleEmpty) => "stale_empty",
                                        Some(
                                            SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence,
                                        ) => "missing_scheduled_group_rows_after_stream_evidence",
                                        Some(
                                            SinkStatusSnapshotIssue::ScheduledPendingAuditWithoutStreamReceipts,
                                        ) => "replay_required_not_ready_without_stream_evidence",
                                        Some(
                                            SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot,
                                        ) => "scheduled_waiting_for_materialized_root",
                                        Some(
                                            SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready,
                                        ) => "scheduled_mixed_ready_and_unready",
                                        None => "steady",
                                    };
                                    eprintln!(
                                        "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason={} {}",
                                        self.node_id.0,
                                        reason,
                                        summarize_sink_status_snapshot(&snapshot)
                                    );
                                }
                                return Err(CnxError::Timeout);
                            }
                            SinkStatusAvailabilityDecision::PropagateError => {}
                        }
                        self.update_cached_status_snapshot(snapshot.clone())?;
                        Ok(snapshot)
                    }
                    Err(err) => {
                        let snapshot = self.cached_status_snapshot()?;
                        if snapshot_looks_scheduled_missing_group_rows_after_stream_evidence(
                            &snapshot,
                        ) {
                            if debug_control_scope_capture_enabled() {
                                eprintln!(
                                    "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason=worker_unavailable_missing_scheduled_group_rows_after_stream_evidence err={} {}",
                                    self.node_id.0,
                                    err,
                                    summarize_sink_status_snapshot(&snapshot)
                                );
                            }
                            Ok(snapshot)
                        } else if snapshot_looks_stale_empty(&snapshot)
                            || snapshot_looks_scheduled_waiting_for_materialized_root(&snapshot)
                            || snapshot_looks_scheduled_mixed_ready_and_unready(&snapshot)
                        {
                            if snapshot_looks_stale_empty(&snapshot) {
                                self.republish_cached_scheduled_groups_into_empty_status_summary()?;
                            }
                            if debug_control_scope_capture_enabled() {
                                eprintln!(
                                    "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason=worker_unavailable_stale_cache err={} {}",
                                    self.node_id.0,
                                    err,
                                    summarize_sink_status_snapshot(&snapshot)
                                );
                            }
                            Err(err)
                        } else {
                            self.republish_cached_scheduled_groups_into_empty_status_summary()?;
                            if debug_control_scope_capture_enabled() {
                                eprintln!(
                                    "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason=worker_unavailable err={} {}",
                                    self.node_id.0,
                                    err,
                                    summarize_sink_status_snapshot(&snapshot)
                                );
                            }
                            Ok(snapshot)
                        }
                    }
                }
            }
            None => {
                let snapshot = self.cached_status_snapshot()?;
                if snapshot_looks_stale_empty(&snapshot)
                    || snapshot_looks_scheduled_waiting_for_materialized_root(&snapshot)
                    || snapshot_looks_scheduled_missing_group_rows_after_stream_evidence(&snapshot)
                    || snapshot_looks_scheduled_mixed_ready_and_unready(&snapshot)
                {
                    if snapshot_looks_stale_empty(&snapshot) {
                        self.republish_cached_scheduled_groups_into_empty_status_summary()?;
                    }
                    if debug_control_scope_capture_enabled() {
                        eprintln!(
                            "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason=not_started_stale_cache {}",
                            self.node_id.0,
                            summarize_sink_status_snapshot(&snapshot)
                        );
                    }
                    Err(CnxError::Timeout)
                } else {
                    if debug_control_scope_capture_enabled() {
                        eprintln!(
                            "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason=not_started {}",
                            self.node_id.0,
                            summarize_sink_status_snapshot(&snapshot)
                        );
                    }
                    Ok(snapshot)
                }
            }
        }
    }

    async fn status_snapshot_probe_once_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<SinkStatusSnapshot> {
        self.replay_retained_control_state_if_needed().await?;
        let client = self.client().await?;
        #[cfg(test)]
        if let Some(reply) = take_sink_worker_status_response_queue_hook() {
            return match reply? {
                SinkWorkerResponse::StatusSnapshot(snapshot) => Ok(snapshot),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker response for status snapshot: {:?}",
                    other
                ))),
            };
        }
        #[cfg(test)]
        if let Some(snapshot) = take_sink_worker_status_snapshot_hook() {
            return Ok(snapshot);
        }
        #[cfg(test)]
        if let Some(err) = take_sink_worker_status_error_hook() {
            return Err(err);
        }
        match Self::call_worker(&client, SinkWorkerRequest::StatusSnapshot, timeout).await? {
            SinkWorkerResponse::StatusSnapshot(snapshot) => Ok(snapshot),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for status snapshot: {:?}",
                other
            ))),
        }
    }

    async fn status_snapshot_with_timeout_outcome(
        &self,
        timeout: Duration,
    ) -> Result<SinkStatusSnapshotProbeOutcome> {
        let deadline = std::time::Instant::now() + timeout;
        let mut recovered_after_retry_reset = false;
        let response = loop {
            let now = std::time::Instant::now();
            let remaining_before_replay = deadline.saturating_duration_since(now);
            if remaining_before_replay.is_zero() {
                return Err(CnxError::Timeout);
            }
            let replay_required_for_attempt =
                self.control_state_replay_required.load(Ordering::Acquire) > 0;
            self.replay_retained_control_state_if_needed_with_timeouts(
                remaining_before_replay,
                remaining_before_replay.min(SINK_WORKER_CONTROL_RPC_TIMEOUT),
            )
            .await?;
            let now = std::time::Instant::now();
            let attempt_timeout = deadline.saturating_duration_since(now);
            if attempt_timeout.is_zero() {
                return Err(CnxError::Timeout);
            }
            #[cfg(test)]
            record_sink_worker_status_timeout_probe(attempt_timeout);
            let rpc_result = self
                .with_started_retry(|client| async move {
                    #[cfg(test)]
                    if let Some(reply) = take_sink_worker_status_response_queue_hook() {
                        return reply;
                    }
                    #[cfg(test)]
                    if let Some(snapshot) = take_sink_worker_status_snapshot_hook() {
                        return Ok(SinkWorkerResponse::StatusSnapshot(snapshot));
                    }
                    #[cfg(test)]
                    if let Some(err) = take_sink_worker_status_error_hook() {
                        return Err(err);
                    }
                    Self::call_worker(&client, SinkWorkerRequest::StatusSnapshot, attempt_timeout)
                        .await
                })
                .await;
            match rpc_result {
                Ok(SinkWorkerResponse::Ack)
                    if replay_required_for_attempt && std::time::Instant::now() < deadline =>
                {
                    recovered_after_retry_reset = true;
                    self.control_state_replay_required
                        .store(1, Ordering::Release);
                    self.reset_shared_worker_client_for_retry().await?;
                }
                Ok(response) => break response,
                Err(err)
                    if can_retry_on_control_frame(&err) && std::time::Instant::now() < deadline =>
                {
                    recovered_after_retry_reset = true;
                    self.control_state_replay_required
                        .store(1, Ordering::Release);
                    self.reset_shared_worker_client_for_retry().await?;
                }
                Err(err) => return Err(err),
            }
        };
        match response {
            SinkWorkerResponse::StatusSnapshot(snapshot) => Ok(SinkStatusSnapshotProbeOutcome {
                snapshot,
                recovered_after_retry_reset,
            }),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for status snapshot: {:?}",
                other
            ))),
        }
    }

    async fn status_snapshot_with_timeout(&self, timeout: Duration) -> Result<SinkStatusSnapshot> {
        Ok(self.status_snapshot_with_timeout_outcome(timeout).await?.snapshot)
    }

    pub async fn scheduled_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.replay_retained_control_state_if_needed().await?;
        match self.scheduled_group_ids_with_timeout().await? {
            SinkWorkerResponse::ScheduledGroupIds(groups) => {
                let groups = groups.map(|groups| {
                    groups
                        .into_iter()
                        .collect::<std::collections::BTreeSet<_>>()
                });
                if let Some(groups) = groups.as_ref().filter(|groups| !groups.is_empty()) {
                    self.update_cached_scheduled_group_ids(groups)?;
                    return Ok(Some(groups.clone()));
                }
                self.cached_scheduled_group_ids()
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for scheduled groups: {:?}",
                other
            ))),
        }
    }

    async fn scheduled_group_ids_with_timeout(&self) -> Result<SinkWorkerResponse> {
        let deadline = std::time::Instant::now() + SINK_WORKER_CONTROL_TOTAL_TIMEOUT;
        loop {
            let now = std::time::Instant::now();
            let attempt_timeout =
                Duration::from_secs(5).min(deadline.saturating_duration_since(now));
            if attempt_timeout.is_zero() {
                return Err(CnxError::Timeout);
            }
            let rpc_result = self
                .with_started_retry(|client| async move {
                    #[cfg(test)]
                    if let Some(err) = take_sink_worker_scheduled_groups_error_hook() {
                        return Err(err);
                    }
                    Self::call_worker(
                        &client,
                        SinkWorkerRequest::ScheduledGroupIds,
                        attempt_timeout,
                    )
                    .await
                })
                .await;
            match rpc_result {
                Ok(response) => return Ok(response),
                Err(err)
                    if can_retry_on_control_frame(&err) && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn visibility_lag_samples_since(
        &self,
        since_us: u64,
    ) -> Result<Vec<VisibilityLagSample>> {
        match Self::call_worker(
            &self.client().await?,
            SinkWorkerRequest::VisibilityLagSamplesSince { since_us },
            Duration::from_secs(5),
        )
        .await?
        {
            SinkWorkerResponse::VisibilityLagSamples(samples) => Ok(samples),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for visibility lag samples: {:?}",
                other
            ))),
        }
    }

    pub async fn query_node(&self, path: Vec<u8>) -> Result<Option<QueryNode>> {
        self.with_started_retry(|client| {
            let path = path.clone();
            async move {
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
                decode_exact_query_node(
                    match Self::call_worker(
                        &client,
                        SinkWorkerRequest::MaterializedQuery { request },
                        SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
                    )
                    .await?
                    {
                        SinkWorkerResponse::Events(events) => events,
                        other => {
                            return Err(CnxError::ProtocolViolation(format!(
                                "unexpected sink worker response for materialized query: {:?}",
                                other
                            )));
                        }
                    },
                    &path,
                )
            }
        })
        .await
    }

    pub async fn materialized_query(&self, request: InternalQueryRequest) -> Result<Vec<Event>> {
        self.replay_retained_control_state_if_needed().await?;
        self.with_started_retry(|client| {
            let request = request.clone();
            async move {
                match Self::call_worker(
                    &client,
                    SinkWorkerRequest::MaterializedQuery {
                        request: request.clone(),
                    },
                    SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
                )
                .await?
                {
                    SinkWorkerResponse::Events(events) => Ok(events),
                    other => Err(CnxError::ProtocolViolation(format!(
                        "unexpected sink worker response for materialized query: {:?}",
                        other
                    ))),
                }
            }
        })
        .await
    }

    pub async fn materialized_query_nonblocking(
        &self,
        request: InternalQueryRequest,
    ) -> Result<Vec<Event>> {
        if self.control_op_inflight() {
            return Ok(Vec::new());
        }
        match self.existing_client().await? {
            Some(client) => {
                self.replay_retained_control_state_if_needed().await?;
                match Self::call_worker(
                    &client,
                    SinkWorkerRequest::MaterializedQuery { request },
                    SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
                )
                .await?
                {
                    SinkWorkerResponse::Events(events) => Ok(events),
                    other => Err(CnxError::ProtocolViolation(format!(
                        "unexpected sink worker response for materialized query: {:?}",
                        other
                    ))),
                }
            }
            None => Ok(Vec::new()),
        }
    }

    pub async fn subtree_stats(&self, path: Vec<u8>) -> Result<Vec<Event>> {
        self.with_started_retry(|client| {
            let path = path.clone();
            async move {
                match Self::call_worker(
                    &client,
                    SinkWorkerRequest::MaterializedQuery {
                        request: InternalQueryRequest::materialized(
                            QueryOp::Stats,
                            QueryScope {
                                path: path.clone(),
                                recursive: true,
                                max_depth: None,
                                selected_group: None,
                            },
                            None,
                        ),
                    },
                    SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
                )
                .await?
                {
                    SinkWorkerResponse::Events(events) => Ok(events),
                    other => Err(CnxError::ProtocolViolation(format!(
                        "unexpected sink worker response for materialized query: {:?}",
                        other
                    ))),
                }
            }
        })
        .await
    }

    pub async fn force_find_proxy(&self, request: InternalQueryRequest) -> Result<Vec<Event>> {
        self.ensure_started().await?;
        eprintln!(
            "fs-meta sink worker proxy: force_find start node={} path={:?} recursive={}",
            self.node_id.0, request.scope.path, request.scope.recursive
        );
        let adapter = exchange_host_adapter_from_channel_boundary(
            self.worker_factory.io_boundary(),
            self.node_id.clone(),
            default_route_bindings(),
        );
        let payload = rmp_serde::to_vec(&request).map_err(|err| {
            CnxError::Internal(format!("sink worker force-find encode failed: {err}"))
        })?;
        let result = adapter
            .call_collect(
                ROUTE_TOKEN_FS_META,
                METHOD_FIND,
                Bytes::from(payload),
                SINK_WORKER_FORCE_FIND_TIMEOUT,
                SINK_WORKER_FORCE_FIND_REPLY_IDLE_GRACE,
            )
            .await;
        eprintln!(
            "fs-meta sink worker proxy: force_find end node={} result={:?}",
            self.node_id.0,
            result
                .as_ref()
                .map(|events| events.len())
                .map_err(|err| err.to_string())
        );
        result
    }

    pub async fn send(&self, events: Vec<Event>) -> Result<()> {
        self.with_started_retry(|client| {
            let events = events.clone();
            async move {
                match Self::call_worker(
                    &client,
                    SinkWorkerRequest::Send {
                        events: events.clone(),
                    },
                    Duration::from_secs(5),
                )
                .await?
                {
                    SinkWorkerResponse::Ack => Ok(()),
                    other => Err(CnxError::ProtocolViolation(format!(
                        "unexpected sink worker response for send: {:?}",
                        other
                    ))),
                }
            }
        })
        .await
    }

    pub async fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        let timeout_ms = opts.timeout.map(|d| d.as_millis() as u64);
        let limit = opts.limit;
        self.with_started_retry(|client| async move {
            match Self::call_worker(
                &client,
                SinkWorkerRequest::Recv { timeout_ms, limit },
                Duration::from_secs(5),
            )
            .await?
            {
                SinkWorkerResponse::Events(events) => Ok(events),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected sink worker response for recv: {:?}",
                    other
                ))),
            }
        })
        .await
    }

    pub async fn on_control_frame(&self, envelopes: Vec<ControlEnvelope>) -> Result<()> {
        self.on_control_frame_with_timeouts(
            envelopes,
            SINK_WORKER_CONTROL_TOTAL_TIMEOUT,
            SINK_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
    }

    async fn on_control_frame_with_timeouts(
        &self,
        envelopes: Vec<ControlEnvelope>,
        total_timeout: Duration,
        rpc_timeout: Duration,
    ) -> Result<()> {
        let _inflight = self.begin_control_op();
        let decoded_signals = sink_control_signals_from_envelopes(&envelopes).ok();
        eprintln!(
            "fs_meta_sink_worker_client: on_control_frame begin node={} envelopes={}",
            self.node_id.0,
            envelopes.len()
        );
        if debug_control_scope_capture_enabled() {
            match decoded_signals.as_ref() {
                Some(signals) => eprintln!(
                    "fs_meta_sink_worker_client: on_control_frame summary node={} signals={:?}",
                    self.node_id.0,
                    summarize_sink_control_signals(signals)
                ),
                None => eprintln!(
                    "fs_meta_sink_worker_client: on_control_frame summary node={} decode_err={}",
                    self.node_id.0, "decode failed"
                ),
            }
        }
        let deadline = std::time::Instant::now() + total_timeout;
        let mut saw_missing_channel_buffer_route_state = false;
        loop {
            let now = std::time::Instant::now();
            let attempt_timeout =
                std::cmp::min(rpc_timeout, deadline.saturating_duration_since(now));
            if attempt_timeout.is_zero() {
                return Err(CnxError::Timeout);
            }
            let current_worker_instance_id = self.shared_worker().await.0;
            let rpc_result = match tokio::time::timeout(
                attempt_timeout,
                self.with_started_retry(|client| {
                    let envelopes = envelopes.clone();
                    async move {
                        #[cfg(test)]
                        maybe_pause_before_on_control_frame_rpc().await;
                        #[cfg(test)]
                        if let Some(err) =
                            take_sink_worker_control_frame_error_hook(current_worker_instance_id)
                        {
                            return Err(err);
                        }
                        Self::call_worker(
                            &client,
                            SinkWorkerRequest::OnControlFrame {
                                envelopes: envelopes.clone(),
                            },
                            attempt_timeout,
                        )
                        .await
                    }
                }),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(CnxError::Timeout),
            };
            match rpc_result {
                Ok(SinkWorkerResponse::Ack) => {
                    if let Some(signals) = decoded_signals.as_ref() {
                        self.retain_control_signals(signals).await?;
                    }
                    eprintln!(
                        "fs_meta_sink_worker_client: on_control_frame done node={} ok=true",
                        self.node_id.0
                    );
                    return Ok(());
                }
                Ok(other) => {
                    eprintln!(
                        "fs_meta_sink_worker_client: on_control_frame done node={} ok=false err=unexpected_response:{:?}",
                        self.node_id.0, other
                    );
                    return Err(CnxError::ProtocolViolation(format!(
                        "unexpected sink worker response for on_control_frame: {:?}",
                        other
                    )));
                }
                Err(err)
                    if can_retry_on_control_frame(&err) && std::time::Instant::now() < deadline =>
                {
                    if is_restart_deferred_retire_pending_deactivate_batch(&envelopes) {
                        self.reset_shared_worker_client_for_retry().await?;
                        eprintln!(
                            "fs_meta_sink_worker_client: on_control_frame fail-fast node={} err={} lane=restart_deferred_retire_pending_events_deactivate",
                            self.node_id.0, err
                        );
                        return Err(err);
                    }
                    if is_missing_channel_buffer_route_state(&err) {
                        saw_missing_channel_buffer_route_state = true;
                        self.reset_shared_worker_client_for_retry().await?;
                        continue;
                    }
                    if saw_missing_channel_buffer_route_state
                        && is_retryable_worker_bridge_reset(&err)
                    {
                        self.reconnect_shared_worker_client_detached().await?;
                        eprintln!(
                            "fs_meta_sink_worker_client: on_control_frame fail-fast node={} err={} lane=missing_channel_buffer_route_state_then_bridge_reset",
                            self.node_id.0, err
                        );
                        return Err(err);
                    }
                    if is_retryable_worker_bridge_reset(&err) {
                        self.reconnect_shared_worker_client_detached().await?;
                    } else {
                        self.reset_shared_worker_client_for_retry().await?;
                    }
                }
                Err(err) => {
                    eprintln!(
                        "fs_meta_sink_worker_client: on_control_frame done node={} ok=false err={}",
                        self.node_id.0, err
                    );
                    return Err(err);
                }
            }
        }
    }

    #[cfg(test)]
    async fn on_control_frame_with_timeouts_for_tests(
        &self,
        envelopes: Vec<ControlEnvelope>,
        total_timeout: Duration,
        rpc_timeout: Duration,
    ) -> Result<()> {
        self.on_control_frame_with_timeouts(envelopes, total_timeout, rpc_timeout)
            .await
    }

    pub async fn close(&self) -> Result<()> {
        #[cfg(test)]
        notify_sink_worker_close_started();
        self.wait_for_control_ops_to_drain(SINK_WORKER_CLOSE_DRAIN_TIMEOUT)
            .await;
        if Arc::strong_count(&self._shared) > 1 {
            return Ok(());
        }
        self.worker_client()
            .await
            .shutdown(Duration::from_secs(2))
            .await
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

    pub async fn ensure_started(&self) -> Result<()> {
        match self {
            Self::Local(_) => Ok(()),
            Self::Worker(client) => client.ensure_started().await,
        }
    }

    pub fn start_stream_endpoint(
        &self,
        boundary: Arc<dyn ChannelIoSubset>,
        node_id: NodeId,
    ) -> Result<()> {
        match self {
            Self::Local(sink) => sink.start_stream_endpoint(boundary, node_id),
            Self::Worker(_) => Ok(()),
        }
    }

    pub async fn update_logical_roots(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
        host_object_grants: &[GrantedMountRoot],
    ) -> Result<()> {
        match self {
            Self::Local(sink) => sink.update_logical_roots(roots, host_object_grants),
            Self::Worker(client) => {
                client
                    .update_logical_roots(roots, host_object_grants.to_vec())
                    .await
            }
        }
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        match self {
            Self::Local(sink) => sink.logical_roots_snapshot(),
            Self::Worker(client) => client.cached_logical_roots_snapshot(),
        }
    }

    pub async fn logical_roots_snapshot(&self) -> Result<Vec<crate::source::config::RootSpec>> {
        match self {
            Self::Local(sink) => sink.logical_roots_snapshot(),
            Self::Worker(client) => client.logical_roots_snapshot().await,
        }
    }

    pub async fn health(&self) -> Result<HealthStats> {
        match self {
            Self::Local(sink) => sink.health(),
            Self::Worker(client) => client.health().await,
        }
    }

    pub async fn status_snapshot(&self) -> Result<SinkStatusSnapshot> {
        match self {
            Self::Local(sink) => sink.status_snapshot(),
            Self::Worker(client) => client.status_snapshot().await,
        }
    }

    pub async fn status_snapshot_nonblocking(&self) -> Result<SinkStatusSnapshot> {
        match self {
            Self::Local(sink) => sink.status_snapshot(),
            Self::Worker(client) => client.status_snapshot_nonblocking().await,
        }
    }

    pub fn cached_status_snapshot(&self) -> Result<SinkStatusSnapshot> {
        match self {
            Self::Local(sink) => sink.status_snapshot(),
            Self::Worker(client) => client.cached_status_snapshot(),
        }
    }

    pub async fn scheduled_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self {
            Self::Local(sink) => sink.scheduled_group_ids_snapshot(),
            Self::Worker(client) => client.scheduled_group_ids().await,
        }
    }

    pub async fn shadow_time_us(&self) -> Result<u64> {
        match self {
            Self::Local(sink) => sink.shadow_time_us(),
            Self::Worker(client) => client.health().await.map(|stats| stats.shadow_time_us),
        }
    }

    pub async fn visibility_lag_samples_since(&self, since_us: u64) -> Vec<VisibilityLagSample> {
        match self {
            Self::Local(sink) => sink.visibility_lag_samples_since(since_us),
            Self::Worker(client) => client
                .visibility_lag_samples_since(since_us)
                .await
                .unwrap_or_default(),
        }
    }

    pub async fn query_node(&self, path: &[u8]) -> Result<Option<QueryNode>> {
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
            Self::Worker(client) => client.query_node(path.to_vec()).await,
        }
    }

    pub async fn materialized_query(&self, request: &InternalQueryRequest) -> Result<Vec<Event>> {
        match self {
            Self::Local(sink) => sink.materialized_query(request),
            Self::Worker(client) => client.materialized_query(request.clone()).await,
        }
    }

    pub async fn materialized_query_nonblocking(
        &self,
        request: &InternalQueryRequest,
    ) -> Result<Vec<Event>> {
        match self {
            Self::Local(sink) => sink.materialized_query(request),
            Self::Worker(client) => client.materialized_query_nonblocking(request.clone()).await,
        }
    }

    pub async fn subtree_stats(&self, path: &[u8]) -> Result<Vec<Event>> {
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
            Self::Worker(client) => client.subtree_stats(path.to_vec()).await,
        }
    }

    pub async fn force_find_proxy(&self, request: &InternalQueryRequest) -> Result<Vec<Event>> {
        match self {
            Self::Local(_) => Err(CnxError::InvalidInput(
                "force-find proxy requires sink worker execution mode".into(),
            )),
            Self::Worker(client) => client.force_find_proxy(request.clone()).await,
        }
    }

    pub async fn send(&self, events: &[Event]) -> Result<()> {
        match self {
            Self::Local(sink) => sink.send(events).await,
            Self::Worker(client) => client.send(events.to_vec()).await,
        }
    }

    pub async fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        match self {
            Self::Local(sink) => sink.recv(opts).await,
            Self::Worker(client) => client.recv(opts).await,
        }
    }

    pub async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        let signals = sink_control_signals_from_envelopes(envelopes)?;
        self.apply_orchestration_signals(&signals).await
    }

    pub(crate) async fn current_generation_tick_fast_path_eligible(&self) -> bool {
        match self {
            Self::Local(_) => true,
            Self::Worker(client) => client.current_generation_tick_fast_path_eligible().await,
        }
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
                client.on_control_frame(envelopes).await
            }
        }
    }

    pub(crate) async fn apply_orchestration_signals_with_total_timeout(
        &self,
        signals: &[SinkControlSignal],
        total_timeout: Duration,
    ) -> Result<()> {
        if total_timeout.is_zero() {
            return Err(CnxError::Timeout);
        }
        // runtime_app owns the outer recovery loop for mixed source/sink recovery.
        // Keep each nested sink-client control attempt short so retryable resets
        // return to the caller instead of burning the full nested client budget.
        let single_attempt_total_timeout = std::cmp::min(
            total_timeout,
            SINK_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT,
        );
        match self {
            Self::Local(sink) => {
                match tokio::time::timeout(
                    single_attempt_total_timeout,
                    sink.apply_orchestration_signals(signals),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(CnxError::Timeout),
                }
            }
            Self::Worker(client) => {
                let envelopes = signals
                    .iter()
                    .map(SinkControlSignal::envelope)
                    .collect::<Vec<_>>();
                let rpc_timeout = std::cmp::min(
                    SINK_WORKER_CONTROL_RPC_TIMEOUT,
                    single_attempt_total_timeout,
                );
                client
                    .on_control_frame_with_timeouts(
                        envelopes,
                        single_attempt_total_timeout,
                        rpc_timeout,
                    )
                    .await
            }
        }
    }

    pub async fn close(&self) -> Result<()> {
        match self {
            Self::Local(sink) => sink.close().await,
            Self::Worker(client) => client.close().await,
        }
    }

    pub(crate) async fn wait_for_control_ops_to_drain_for_handoff(&self) {
        if let Self::Worker(client) = self {
            client
                .wait_for_control_ops_to_drain(SINK_WORKER_CLOSE_DRAIN_TIMEOUT)
                .await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn shutdown_shared_worker_for_tests(&self, timeout: Duration) -> Result<()> {
        match self {
            Self::Local(_) => Err(CnxError::InvalidInput(
                "shutdown_shared_worker_for_tests requires worker-backed sink facade".into(),
            )),
            Self::Worker(client) => client.shutdown_shared_worker_for_tests(timeout).await,
        }
    }
}

capanix_runtime_entry_sdk::worker_runtime::define_typed_worker_rpc! {
    pub struct SinkWorkerRpc {
        request: SinkWorkerRequest,
        response: SinkWorkerResponse,
        encode_request: encode_request,
        decode_request: decode_request,
        encode_response: encode_response,
        decode_response: decode_response,
        invalid_input: SinkWorkerResponse::InvalidInput,
        error: SinkWorkerResponse::Error,
        unavailable: "sink worker unavailable",
    }
}

impl TypedWorkerInit<SourceConfig> for SinkWorkerRpc {
    type InitPayload = SinkWorkerInitConfig;

    fn init_payload(_node_id: &NodeId, config: &SourceConfig) -> Result<Self::InitPayload> {
        Ok(SinkWorkerInitConfig {
            roots: config.roots.clone(),
            host_object_grants: config.host_object_grants.clone(),
            sink_tombstone_ttl_ms: config.sink_tombstone_ttl.as_millis() as u64,
            sink_tombstone_tolerance_us: config.sink_tombstone_tolerance_us,
        })
    }
}

#[cfg(test)]
mod tests;
