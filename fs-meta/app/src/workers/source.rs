use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use capanix_app_sdk::runtime::{ControlEnvelope, NodeId, RuntimeWorkerBinding};
use capanix_app_sdk::{CnxError, Event, Result};
use capanix_runtime_entry_sdk::advanced::boundary::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelSendRequest,
};
use capanix_runtime_entry_sdk::control::{RuntimeBoundScope, RuntimeHostGrantState};
use capanix_runtime_entry_sdk::worker_runtime::{
    RuntimeWorkerClientFactory, TypedRuntimeWorkerClient, TypedWorkerClient, TypedWorkerInit,
};
use futures_util::StreamExt;
use tokio::task::JoinHandle;

use crate::query::request::InternalQueryRequest;
use crate::runtime::orchestration::{
    SourceControlSignal, SourceRuntimeUnit, source_control_signals_from_envelopes,
};
use crate::runtime::routes::ROUTE_KEY_EVENTS;
use crate::source::config::{GrantedMountRoot, RootSpec, SourceConfig};
use crate::source::{FSMetaSource, SourceStatusSnapshot};
use crate::state::cell::SignalCell;
use crate::workers::sink::SinkFacade;
use crate::workers::source_ipc::{
    SourceWorkerRequest, SourceWorkerResponse, decode_request, decode_response, encode_request,
    encode_response,
};

const SOURCE_WORKER_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(15);
const SOURCE_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_WORKER_START_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const SOURCE_WORKER_FORCE_FIND_TIMEOUT: Duration = Duration::from_secs(60);
const SOURCE_WORKER_UPDATE_ROOTS_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT: Duration = Duration::from_secs(90);
const SOURCE_WORKER_CLOSE_DRAIN_TIMEOUT: Duration = SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT;
const SOURCE_WORKER_CLOSE_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(25);
const SOURCE_WORKER_SCHEDULE_REFRESH_TOTAL_TIMEOUT: Duration = Duration::from_secs(2);
const SOURCE_WORKER_OBSERVABILITY_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const SOURCE_WORKER_RETRY_BACKOFF: Duration = Duration::from_millis(100);
const SOURCE_WORKER_NONBLOCKING_OBSERVABILITY_CACHE_TTL: Duration = Duration::from_secs(1);
const SOURCE_WORKER_DEGRADED_STATE: &str = "degraded_worker_unreachable";
const SOURCE_WORKER_DEGRADED_ROOT_KEY: &str = "source-worker";

fn clip_retry_deadline(deadline: std::time::Instant, budget: Duration) -> std::time::Instant {
    std::cmp::min(deadline, std::time::Instant::now() + budget)
}

fn can_use_cached_host_object_grants_snapshot(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::PeerError(message) if message == "worker not initialized"
    ) || matches!(err, CnxError::TransportClosed(_) | CnxError::Timeout)
        || matches!(
            err,
            CnxError::AccessDenied(message) | CnxError::PeerError(message)
                if message.contains("drained/fenced")
                    && message.contains("grant attachments")
        )
}

fn can_use_cached_logical_roots_snapshot(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::PeerError(message) if message == "worker not initialized"
    ) || matches!(err, CnxError::TransportClosed(_) | CnxError::Timeout)
        || matches!(
            err,
            CnxError::AccessDenied(message) | CnxError::PeerError(message)
                if message.contains("drained/fenced")
                    && message.contains("grant attachments")
        )
}

fn is_retryable_worker_bridge_peer_error(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::PeerError(message)
            if message.contains("transport closed")
                && (message.contains("Connection reset by peer")
                    || message.contains("early eof")
                    || message.contains("Broken pipe")
                    || message.contains("bridge stopped"))
    )
}

fn is_timeout_like_worker_control_reset(err: &CnxError) -> bool {
    matches!(err, CnxError::Timeout)
        || matches!(
            err,
            CnxError::PeerError(message) | CnxError::Internal(message)
                if message.contains("operation timed out")
        )
}

fn can_retry_update_logical_roots(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::PeerError(message) if message == "worker not initialized"
    ) || matches!(err, CnxError::TransportClosed(_) | CnxError::ChannelClosed)
        || is_timeout_like_worker_control_reset(err)
        || is_retryable_worker_bridge_peer_error(err)
        || matches!(
            err,
            CnxError::ProtocolViolation(message)
                if message.contains("unexpected correlation_id in reply batch")
        )
        || matches!(
            err,
            CnxError::PeerError(message)
                if message.contains("unexpected correlation_id in reply batch")
        )
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

fn can_retry_on_control_frame(err: &CnxError) -> bool {
    can_retry_update_logical_roots(err)
}

fn post_ack_schedule_refresh_exhaustion_error(err: &CnxError) -> Option<CnxError> {
    let reason = match err {
        _ if is_timeout_like_worker_control_reset(err) => "timeout",
        CnxError::TransportClosed(_) | CnxError::ChannelClosed => "transport_closed",
        CnxError::Internal(message)
        | CnxError::PeerError(message)
        | CnxError::AccessDenied(message)
            if message.contains("missing route state for channel_buffer") =>
        {
            "missing_route_state"
        }
        other if can_retry_on_control_frame(other) => "retryable_reset",
        _ => return None,
    };
    Some(CnxError::Internal(format!(
        "source worker post-ack scheduled-groups refresh exhausted before scheduled groups converged ({reason})"
    )))
}

fn can_retry_force_find(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::ProtocolViolation(message) | CnxError::PeerError(message)
            if message.contains("unexpected correlation_id in reply batch")
    )
}

fn is_restart_deferred_retire_pending_deactivate_batch(envelopes: &[ControlEnvelope]) -> bool {
    let Ok(signals) = source_control_signals_from_envelopes(envelopes) else {
        return false;
    };
    !signals.is_empty()
        && signals.iter().all(|signal| {
            matches!(
                signal,
                SourceControlSignal::Deactivate { envelope, .. }
                    if matches!(
                        capanix_runtime_entry_sdk::control::decode_runtime_exec_control(envelope),
                        Ok(Some(
                            capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                                deactivate
                            )
                        )) if deactivate.reason == "restart_deferred_retire_pending"
                    )
            )
        })
}

fn source_control_signals_prefer_short_existing_client_attempt(
    signals: &[SourceControlSignal],
) -> bool {
    signals.iter().any(|signal| {
        matches!(
            signal,
            SourceControlSignal::Activate { .. }
                | SourceControlSignal::Deactivate { .. }
                | SourceControlSignal::Tick { .. }
                | SourceControlSignal::ManualRescan { .. }
                | SourceControlSignal::Passthrough(_)
        )
    })
}

fn source_control_signals_require_post_ack_schedule_refresh(
    signals: &[SourceControlSignal],
) -> bool {
    signals.iter().any(|signal| {
        matches!(
            signal,
            SourceControlSignal::Activate { bound_scopes, .. } if !bound_scopes.is_empty()
        )
    })
}

fn source_control_signals_are_generation_one_activate_only(
    signals: &[SourceControlSignal],
) -> bool {
    let mut saw_activate = false;
    for signal in signals {
        match signal {
            SourceControlSignal::Activate { generation, .. } if *generation == 1 => {
                saw_activate = true;
            }
            _ => return false,
        }
    }
    saw_activate
}

fn debug_control_scope_capture_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
}

fn debug_force_find_route_capture_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_FORCE_FIND_ROUTE_CAPTURE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
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

fn host_ref_from_resource_id(resource_id: &str) -> Option<&str> {
    resource_id.split_once("::").map(|(host_ref, _)| host_ref)
}

fn runtime_scope_resource_matches_logical_root(resource_id: &str, logical_root_id: &str) -> bool {
    resource_id == logical_root_id
        || resource_id
            .rsplit_once("::")
            .is_some_and(|(_, tail)| tail == logical_root_id)
}

fn bound_scope_matches_logical_root(
    bound_scope: &RuntimeBoundScope,
    logical_root_id: &str,
) -> bool {
    bound_scope.scope_id == logical_root_id
        || bound_scope.resource_ids.iter().any(|resource_id| {
            runtime_scope_resource_matches_logical_root(resource_id, logical_root_id)
        })
}

fn bound_scope_has_explicit_local_resource_id(
    bound_scope: &RuntimeBoundScope,
    logical_root_id: &str,
    node_id: &NodeId,
) -> bool {
    bound_scope.resource_ids.iter().any(|resource_id| {
        runtime_scope_resource_matches_logical_root(resource_id, logical_root_id)
            && host_ref_from_resource_id(resource_id)
                .is_some_and(|host_ref| host_ref_matches_node_id(host_ref, node_id))
    })
}

fn bound_scope_has_bare_logical_root_id(
    bound_scope: &RuntimeBoundScope,
    logical_root_id: &str,
) -> bool {
    bound_scope.scope_id == logical_root_id
        || bound_scope
            .resource_ids
            .iter()
            .any(|resource_id| resource_id == logical_root_id)
}

fn root_has_any_matching_grant(root: &RootSpec, grants: &[GrantedMountRoot]) -> bool {
    grants.iter().any(|grant| root.selector.matches(grant))
}

fn root_has_local_matching_grant(
    root: &RootSpec,
    node_id: &NodeId,
    grants: &[GrantedMountRoot],
) -> bool {
    grants.iter().any(|grant| {
        host_ref_matches_node_id(&grant.host_ref, node_id) && root.selector.matches(grant)
    })
}

fn bound_scope_applies_locally(
    bound_scope: &RuntimeBoundScope,
    root: &RootSpec,
    node_id: &NodeId,
    grants: &[GrantedMountRoot],
) -> bool {
    if !bound_scope_matches_logical_root(bound_scope, &root.id) {
        return false;
    }
    if root_has_local_matching_grant(root, node_id, grants) {
        return true;
    }
    if bound_scope_has_explicit_local_resource_id(bound_scope, &root.id, node_id) {
        return true;
    }
    bound_scope_has_bare_logical_root_id(bound_scope, &root.id)
        && !root_has_any_matching_grant(root, grants)
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
    if groups.is_empty() || entry.is_empty() {
        entry.clear();
        return;
    }
    entry.extend(groups);
    entry.sort();
    entry.dedup();
}

fn normalize_observability_snapshot_scheduled_group_keys(
    snapshot: &mut SourceObservabilitySnapshot,
    node_id: &NodeId,
    cached_grants: Option<&Vec<GrantedMountRoot>>,
) {
    let mut stable_host_ref = stable_host_ref_for_node_id(node_id, &snapshot.grants);
    if stable_host_ref == node_id.0
        && let Some(cached_grants) = cached_grants
    {
        let cached_host_ref = stable_host_ref_for_node_id(node_id, cached_grants);
        if cached_host_ref != node_id.0 {
            stable_host_ref = cached_host_ref;
        }
    }
    normalize_node_groups_key(
        &mut snapshot.scheduled_source_groups_by_node,
        &node_id.0,
        &stable_host_ref,
    );
    normalize_node_groups_key(
        &mut snapshot.scheduled_scan_groups_by_node,
        &node_id.0,
        &stable_host_ref,
    );
}

fn merge_non_empty_cached_groups(
    current: &mut std::collections::BTreeMap<String, Vec<String>>,
    cached: &Option<std::collections::BTreeMap<String, Vec<String>>>,
) {
    let Some(cached) = cached else {
        return;
    };
    if current.is_empty() {
        if !cached.is_empty() {
            *current = cached.clone();
        }
        return;
    }
    for (node_id, groups) in cached {
        match current.get_mut(node_id) {
            Some(existing) if existing.is_empty() => {
                let _ = groups;
            }
            None => {
                current.insert(node_id.clone(), groups.clone());
            }
            _ => {}
        }
    }
}

fn union_cached_groups(
    current: &mut std::collections::BTreeMap<String, Vec<String>>,
    cached: &Option<std::collections::BTreeMap<String, Vec<String>>>,
) {
    let Some(cached) = cached else {
        return;
    };
    for (node_id, groups) in cached {
        if groups.is_empty() {
            continue;
        }
        match current.get_mut(node_id) {
            Some(existing) if existing.is_empty() => {}
            Some(existing) => {
                existing.extend(groups.iter().cloned());
                existing.sort();
                existing.dedup();
            }
            None => {
                current.insert(node_id.clone(), groups.clone());
            }
        }
    }
}

fn update_cached_scheduled_groups_from_refresh(
    cache: &mut SourceWorkerSnapshotCache,
    mut scheduled_source: std::collections::BTreeMap<String, Vec<String>>,
    mut scheduled_scan: std::collections::BTreeMap<String, Vec<String>>,
) {
    union_cached_groups(
        &mut scheduled_source,
        &cache.scheduled_source_groups_by_node,
    );
    union_cached_groups(&mut scheduled_scan, &cache.scheduled_scan_groups_by_node);
    cache.scheduled_source_groups_by_node = Some(scheduled_source);
    cache.scheduled_scan_groups_by_node = Some(scheduled_scan);
}

fn stable_host_ref_from_cached_scheduled_groups(
    node_id: &NodeId,
    cache: &SourceWorkerSnapshotCache,
) -> Option<String> {
    let host_refs = cache
        .scheduled_source_groups_by_node
        .iter()
        .chain(cache.scheduled_scan_groups_by_node.iter())
        .flat_map(|groups| groups.keys())
        .filter(|host_ref| host_ref_matches_node_id(host_ref, node_id))
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    match host_refs.len() {
        1 => host_refs.into_iter().next(),
        _ => None,
    }
}

fn cached_local_scheduled_groups(
    node_id: &NodeId,
    groups_by_node: &Option<std::collections::BTreeMap<String, Vec<String>>>,
) -> Option<std::collections::BTreeSet<String>> {
    let groups = groups_by_node
        .iter()
        .flat_map(|groups_by_node| groups_by_node.iter())
        .filter(|(host_ref, _)| host_ref_matches_node_id(host_ref, node_id))
        .flat_map(|(_, groups)| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    if groups.is_empty() {
        None
    } else {
        Some(groups)
    }
}

fn merge_cached_local_scheduled_groups(
    node_id: &NodeId,
    live_groups: Option<std::collections::BTreeSet<String>>,
    groups_by_node: &Option<std::collections::BTreeMap<String, Vec<String>>>,
) -> Option<std::collections::BTreeSet<String>> {
    if live_groups.as_ref().is_some_and(|groups| groups.is_empty()) {
        return Some(std::collections::BTreeSet::new());
    }
    let mut groups = live_groups.unwrap_or_default();
    if let Some(cached) = cached_local_scheduled_groups(node_id, groups_by_node) {
        groups.extend(cached);
    }
    if groups.is_empty() {
        None
    } else {
        Some(groups)
    }
}

#[derive(Default, Clone, Copy)]
struct PrimedLocalScheduleSummary {
    saw_activate_with_bound_scopes: bool,
    has_local_runnable_groups: bool,
}

fn prime_cached_schedule_from_control_signals(
    cache: &mut SourceWorkerSnapshotCache,
    node_id: &NodeId,
    signals: &[SourceControlSignal],
    fallback_roots: &[RootSpec],
    fallback_grants: &[GrantedMountRoot],
) -> PrimedLocalScheduleSummary {
    let changed_grants = signals.iter().rev().find_map(|signal| match signal {
        SourceControlSignal::RuntimeHostGrantChange { changed, .. } => Some((
            changed.version,
            changed
                .grants
                .iter()
                .filter(|row| std::path::Path::new(&row.object.mount_point).is_absolute())
                .map(|row| GrantedMountRoot {
                    object_ref: row.object_ref.clone(),
                    host_ref: row.host.host_ref.clone(),
                    host_ip: row.host.host_ip.clone(),
                    host_name: row.host.host_name.clone(),
                    site: row.host.site.clone(),
                    zone: row.host.zone.clone(),
                    host_labels: row.host.host_labels.clone(),
                    mount_point: row.object.mount_point.clone().into(),
                    fs_source: row.object.fs_source.clone(),
                    fs_type: row.object.fs_type.clone(),
                    mount_options: row.object.mount_options.clone(),
                    interfaces: row.interfaces.clone(),
                    active: matches!(row.grant_state, RuntimeHostGrantState::Active),
                })
                .collect::<Vec<_>>(),
        )),
        _ => None,
    });
    if let Some((version, grants)) = changed_grants {
        cache.host_object_grants_version = Some(version);
        cache.grants = Some(grants);
    }
    let grants = cache
        .grants
        .as_ref()
        .cloned()
        .unwrap_or_else(|| fallback_grants.to_vec());
    let roots = cache
        .logical_roots
        .as_ref()
        .cloned()
        .unwrap_or_else(|| fallback_roots.to_vec());
    let stable_host_ref = {
        let stable = stable_host_ref_for_node_id(node_id, &grants);
        if stable != node_id.0 {
            stable
        } else {
            let local_scope_hosts = signals
                .iter()
                .filter_map(|signal| match signal {
                    SourceControlSignal::Activate { bound_scopes, .. } => Some(bound_scopes),
                    _ => None,
                })
                .flat_map(|bound_scopes| bound_scopes.iter())
                .flat_map(|scope| scope.resource_ids.iter())
                .filter_map(|resource_id| host_ref_from_resource_id(resource_id))
                .filter(|host_ref| host_ref_matches_node_id(host_ref, node_id))
                .map(str::to_string)
                .collect::<std::collections::BTreeSet<_>>();
            match local_scope_hosts.len() {
                1 => local_scope_hosts
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| node_id.0.clone()),
                _ => stable,
            }
        }
    };
    let mut scheduled_source = std::collections::BTreeSet::new();
    let mut scheduled_scan = std::collections::BTreeSet::new();
    let mut saw_source_activate = false;
    let mut saw_scan_activate = false;
    for signal in signals {
        let (unit, bound_scopes) = match signal {
            SourceControlSignal::Activate {
                unit, bound_scopes, ..
            } => (Some(*unit), Some(bound_scopes.as_slice())),
            _ => (None, None),
        };
        let Some(unit) = unit else { continue };
        let Some(bound_scopes) = bound_scopes else {
            continue;
        };
        match unit {
            SourceRuntimeUnit::Source => saw_source_activate = true,
            SourceRuntimeUnit::Scan => saw_scan_activate = true,
        }
        for scope in bound_scopes {
            let applies_locally = roots
                .iter()
                .any(|root| bound_scope_applies_locally(scope, root, node_id, &grants));
            if !applies_locally {
                continue;
            }
            match unit {
                SourceRuntimeUnit::Source => {
                    scheduled_source.insert(scope.scope_id.clone());
                }
                SourceRuntimeUnit::Scan => {
                    scheduled_scan.insert(scope.scope_id.clone());
                }
            }
        }
    }
    let has_local_runnable_groups = !(scheduled_source.is_empty() && scheduled_scan.is_empty());
    if saw_source_activate && scheduled_source.is_empty() {
        cache.scheduled_source_groups_by_node = None;
    } else if !scheduled_source.is_empty() {
        cache.scheduled_source_groups_by_node = Some(std::collections::BTreeMap::from([(
            stable_host_ref.clone(),
            scheduled_source.into_iter().collect::<Vec<_>>(),
        )]));
    }
    if saw_scan_activate && scheduled_scan.is_empty() {
        cache.scheduled_scan_groups_by_node = None;
    } else if !scheduled_scan.is_empty() {
        cache.scheduled_scan_groups_by_node = Some(std::collections::BTreeMap::from([(
            stable_host_ref,
            scheduled_scan.into_iter().collect::<Vec<_>>(),
        )]));
    }
    PrimedLocalScheduleSummary {
        saw_activate_with_bound_scopes: saw_source_activate || saw_scan_activate,
        has_local_runnable_groups,
    }
}

fn prime_cached_control_summary_from_control_signals(
    cache: &mut SourceWorkerSnapshotCache,
    node_id: &NodeId,
    signals: &[SourceControlSignal],
    fallback_grants: &[GrantedMountRoot],
) {
    let summary = summarize_source_control_signals(signals);
    if summary.is_empty() {
        return;
    }
    let grants = cache
        .grants
        .as_ref()
        .cloned()
        .unwrap_or_else(|| fallback_grants.to_vec());
    let stable_host_ref = stable_host_ref_for_node_id(node_id, &grants);
    cache.last_control_frame_signals_by_node = Some(std::collections::BTreeMap::from([(
        stable_host_ref,
        summary,
    )]));
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

fn summarize_groups_by_node(
    groups: &std::collections::BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    groups
        .iter()
        .map(|(node_id, groups)| format!("{node_id}={}", groups.join("|")))
        .collect()
}

fn summarize_source_observability_snapshot(snapshot: &SourceObservabilitySnapshot) -> String {
    format!(
        "lifecycle={} primaries={} runners={} source={:?} scan={:?} control={:?} published_batches={:?} published_events={:?} published_origins={:?} published_origin_counts={:?} published_path_target={:?} enqueued_path_counts={:?} pending_path_counts={:?} yielded_path_counts={:?} summarized_path_counts={:?} published_path_counts={:?} degraded={:?}",
        snapshot.lifecycle_state,
        snapshot.source_primary_by_group.len(),
        snapshot.last_force_find_runner_by_group.len(),
        summarize_groups_by_node(&snapshot.scheduled_source_groups_by_node),
        summarize_groups_by_node(&snapshot.scheduled_scan_groups_by_node),
        summarize_groups_by_node(&snapshot.last_control_frame_signals_by_node),
        snapshot.published_batches_by_node,
        snapshot.published_events_by_node,
        summarize_groups_by_node(&snapshot.last_published_origins_by_node),
        summarize_groups_by_node(&snapshot.published_origin_counts_by_node),
        snapshot.published_path_capture_target,
        summarize_groups_by_node(&snapshot.enqueued_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.pending_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.yielded_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.summarized_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.published_path_origin_counts_by_node),
        snapshot.status.degraded_roots
    )
}

fn source_observability_snapshot_debug_maps_absent(
    snapshot: &SourceObservabilitySnapshot,
) -> bool {
    snapshot.scheduled_source_groups_by_node.is_empty()
        && snapshot.scheduled_scan_groups_by_node.is_empty()
        && snapshot.last_control_frame_signals_by_node.is_empty()
        && snapshot.published_batches_by_node.values().all(|count| *count == 0)
        && snapshot.published_events_by_node.values().all(|count| *count == 0)
        && snapshot
            .published_control_events_by_node
            .values()
            .all(|count| *count == 0)
        && snapshot
            .published_data_events_by_node
            .values()
            .all(|count| *count == 0)
        && snapshot.last_published_at_us_by_node.is_empty()
        && snapshot.last_published_origins_by_node.is_empty()
        && snapshot.published_origin_counts_by_node.is_empty()
        && snapshot.published_path_capture_target.is_none()
        && snapshot.enqueued_path_origin_counts_by_node.is_empty()
        && snapshot.pending_path_origin_counts_by_node.is_empty()
        && snapshot.yielded_path_origin_counts_by_node.is_empty()
        && snapshot.summarized_path_origin_counts_by_node.is_empty()
        && snapshot.published_path_origin_counts_by_node.is_empty()
}

fn source_observability_snapshot_has_active_state(
    snapshot: &SourceObservabilitySnapshot,
) -> bool {
    snapshot.status.current_stream_generation.is_some()
        || snapshot
            .status
            .logical_roots
            .iter()
            .any(|entry| entry.active_members > 0 || entry.status.eq_ignore_ascii_case("ready"))
        || snapshot.status.concrete_roots.iter().any(|entry| {
            entry.active
                || entry.current_stream_generation.is_some()
                || entry.emitted_batch_count > 0
                || entry.forwarded_batch_count > 0
                || entry.emitted_event_count > 0
                || entry.forwarded_event_count > 0
        })
        || !snapshot.scheduled_source_groups_by_node.is_empty()
        || !snapshot.scheduled_scan_groups_by_node.is_empty()
        || !snapshot.last_control_frame_signals_by_node.is_empty()
}

fn should_preserve_cached_observability_map<K: Ord, V>(
    can_preserve: bool,
    current: &std::collections::BTreeMap<K, V>,
    cached: Option<&std::collections::BTreeMap<K, V>>,
) -> bool {
    can_preserve && current.is_empty() && cached.is_some_and(|entries| !entries.is_empty())
}

fn should_preserve_cached_observability_option<T>(
    can_preserve: bool,
    current: &Option<T>,
    cached: &Option<T>,
) -> bool {
    can_preserve && current.is_none() && cached.is_some()
}

fn explicit_zero_published_counter_nodes(
    snapshot: &SourceObservabilitySnapshot,
) -> std::collections::BTreeSet<String> {
    let mut nodes = std::collections::BTreeSet::new();
    for node_id in snapshot.published_batches_by_node.keys() {
        nodes.insert(node_id.clone());
    }
    for node_id in snapshot.published_events_by_node.keys() {
        nodes.insert(node_id.clone());
    }
    for node_id in snapshot.published_control_events_by_node.keys() {
        nodes.insert(node_id.clone());
    }
    for node_id in snapshot.published_data_events_by_node.keys() {
        nodes.insert(node_id.clone());
    }
    nodes.retain(|node_id| {
        let mut saw_counter = false;
        for counter in [
            snapshot.published_batches_by_node.get(node_id),
            snapshot.published_events_by_node.get(node_id),
            snapshot.published_control_events_by_node.get(node_id),
            snapshot.published_data_events_by_node.get(node_id),
        ]
        .into_iter()
        .flatten()
        {
            saw_counter = true;
            if *counter != 0 {
                return false;
            }
        }
        saw_counter
    });
    nodes
}

fn merge_cached_observability_node_map<V: Clone>(
    can_preserve: bool,
    current: &std::collections::BTreeMap<String, V>,
    cached: Option<&std::collections::BTreeMap<String, V>>,
    explicit_clear_nodes: &std::collections::BTreeSet<String>,
) -> std::collections::BTreeMap<String, V> {
    let mut merged = if should_preserve_cached_observability_map(can_preserve, current, cached) {
        cached.cloned().unwrap_or_default()
    } else {
        current.clone()
    };
    for node_id in explicit_clear_nodes {
        merged.remove(node_id);
    }
    merged
}

fn merge_cached_observability_option<T: Clone>(
    can_preserve: bool,
    current: &Option<T>,
    cached: &Option<T>,
    explicit_clear: bool,
) -> Option<T> {
    if explicit_clear {
        None
    } else if should_preserve_cached_observability_option(can_preserve, current, cached) {
        cached.clone()
    } else {
        current.clone()
    }
}

fn recent_cached_source_observability_snapshot_is_incomplete(
    snapshot: &SourceObservabilitySnapshot,
) -> bool {
    source_observability_snapshot_has_active_state(snapshot)
        && source_observability_snapshot_debug_maps_absent(snapshot)
}

fn fail_close_incomplete_active_source_observability_snapshot(
    snapshot: &mut SourceObservabilitySnapshot,
) {
    if !recent_cached_source_observability_snapshot_is_incomplete(snapshot) {
        return;
    }
    snapshot.status.current_stream_generation = None;
    snapshot.status.logical_roots.clear();
    snapshot.status.concrete_roots.clear();
}

fn control_signals_by_node(
    node_key: &str,
    signals: &[String],
) -> std::collections::BTreeMap<String, Vec<String>> {
    if signals.is_empty() {
        return std::collections::BTreeMap::new();
    }
    std::collections::BTreeMap::from([(node_key.to_string(), signals.to_vec())])
}

fn debug_source_status_lifecycle_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_SOURCE_STATUS_LIFECYCLE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn next_source_status_trace_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

struct SourceStatusTraceGuard {
    node_id: String,
    trace_id: u64,
    phase: &'static str,
    completed: bool,
}

impl SourceStatusTraceGuard {
    fn new(node_id: String, trace_id: u64, phase: &'static str) -> Self {
        Self {
            node_id,
            trace_id,
            phase,
            completed: false,
        }
    }

    fn phase(&mut self, phase: &'static str) {
        self.phase = phase;
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for SourceStatusTraceGuard {
    fn drop(&mut self) {
        if debug_source_status_lifecycle_enabled() && !self.completed {
            eprintln!(
                "fs_meta_source_worker_client: observability_snapshot dropped node={} trace_id={} phase={}",
                self.node_id, self.trace_id, self.phase
            );
        }
    }
}

#[derive(Debug, Clone, Default)]
struct SourceWorkerSnapshotCache {
    lifecycle_state: Option<String>,
    last_live_observability_snapshot_at: Option<Instant>,
    host_object_grants_version: Option<u64>,
    grants: Option<Vec<GrantedMountRoot>>,
    logical_roots: Option<Vec<RootSpec>>,
    status: Option<SourceStatusSnapshot>,
    source_primary_by_group: Option<std::collections::BTreeMap<String, String>>,
    last_force_find_runner_by_group: Option<std::collections::BTreeMap<String, String>>,
    force_find_inflight_groups: Option<Vec<String>>,
    scheduled_source_groups_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    scheduled_scan_groups_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    last_control_frame_signals_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    published_batches_by_node: Option<std::collections::BTreeMap<String, u64>>,
    published_events_by_node: Option<std::collections::BTreeMap<String, u64>>,
    published_control_events_by_node: Option<std::collections::BTreeMap<String, u64>>,
    published_data_events_by_node: Option<std::collections::BTreeMap<String, u64>>,
    last_published_at_us_by_node: Option<std::collections::BTreeMap<String, u64>>,
    last_published_origins_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    published_origin_counts_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    published_path_capture_target: Option<String>,
    enqueued_path_origin_counts_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    pending_path_origin_counts_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    yielded_path_origin_counts_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    summarized_path_origin_counts_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    published_path_origin_counts_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
}

#[derive(Clone)]
pub struct SourceWorkerClientHandle {
    node_id: NodeId,
    config: SourceConfig,
    worker_factory: RuntimeWorkerClientFactory,
    worker_binding: RuntimeWorkerBinding,
    _shared: Arc<SharedSourceWorkerHandleState>,
    manual_rescan_signal: SignalCell,
    cache: Arc<Mutex<SourceWorkerSnapshotCache>>,
    retained_control_state: Arc<tokio::sync::Mutex<RetainedSourceWorkerControlState>>,
    control_state_replay_required: Arc<std::sync::atomic::AtomicBool>,
    start_serial: Arc<tokio::sync::Mutex<()>>,
    control_ops_inflight: Arc<AtomicUsize>,
    control_ops_serial: Arc<tokio::sync::Mutex<()>>,
}

struct InflightControlOpGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for InflightControlOpGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

struct SharedSourceWorkerHandleState {
    worker: tokio::sync::Mutex<SharedSourceWorkerClient>,
    cache: Arc<Mutex<SourceWorkerSnapshotCache>>,
    retained_control_state: Arc<tokio::sync::Mutex<RetainedSourceWorkerControlState>>,
    control_state_replay_required: Arc<std::sync::atomic::AtomicBool>,
    start_serial: Arc<tokio::sync::Mutex<()>>,
    control_ops_inflight: Arc<AtomicUsize>,
    control_ops_serial: Arc<tokio::sync::Mutex<()>>,
}

struct ScheduledGroupsRefreshFailure {
    err: CnxError,
    recovered_live_worker_during_refresh: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetainedTickOnlyWaveDisposition {
    FastPath,
    ReplayInPlace,
    ReconnectAndReplay,
}

struct SharedSourceWorkerClient {
    instance_id: u64,
    client: Arc<TypedRuntimeWorkerClient<SourceWorkerRpc, SourceConfig>>,
}

#[derive(Default, Clone)]
struct RetainedSourceWorkerControlState {
    latest_host_grant_change: Option<SourceControlSignal>,
    active_by_route: std::collections::BTreeMap<(String, String), SourceControlSignal>,
}

fn next_shared_source_worker_instance_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn source_worker_handle_registry_key(
    node_id: &NodeId,
    worker_binding: &RuntimeWorkerBinding,
    worker_factory: &RuntimeWorkerClientFactory,
) -> String {
    let runtime_boundary_id = {
        let io_boundary = worker_factory.io_boundary();
        Arc::as_ptr(&io_boundary) as *const () as usize
    };
    format!(
        "{}|{}|{:?}|{:?}|{}",
        node_id.0,
        worker_binding.role_id,
        worker_binding.mode,
        worker_binding.launcher_kind,
        runtime_boundary_id
    )
}

fn source_worker_handle_registry()
-> &'static Mutex<std::collections::BTreeMap<String, Weak<SharedSourceWorkerHandleState>>> {
    static REGISTRY: std::sync::OnceLock<
        Mutex<std::collections::BTreeMap<String, Weak<SharedSourceWorkerHandleState>>>,
    > = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(std::collections::BTreeMap::new()))
}

fn lock_source_worker_handle_registry() -> std::sync::MutexGuard<
    'static,
    std::collections::BTreeMap<String, Weak<SharedSourceWorkerHandleState>>,
> {
    match source_worker_handle_registry().lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            log::warn!(
                "source worker handle registry lock poisoned; recovering shared handle state"
            );
            poisoned.into_inner()
        }
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerCloseHook {
    pub entered: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerUpdateRootsHook {
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerUpdateRootsErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerLogicalRootsSnapshotHook {
    pub roots: Option<Vec<RootSpec>>,
    pub entered: Option<Arc<tokio::sync::Notify>>,
    pub release: Option<Arc<tokio::sync::Notify>>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerControlFrameErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
pub(crate) struct SourceWorkerControlFrameErrorQueueHook {
    pub errs: std::collections::VecDeque<CnxError>,
    pub sticky_worker_instance_id: Option<u64>,
    pub sticky_peer_err: Option<String>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerStartErrorQueueHook {
    pub errs: std::collections::VecDeque<CnxError>,
    pub sticky_worker_instance_id: Option<u64>,
    pub sticky_peer_err: Option<String>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerStatusErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
pub(crate) struct SourceWorkerObservabilityErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerObservabilityCallCountHook {
    pub count: Arc<AtomicUsize>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerTriggerRescanWhenReadyCallCountHook {
    pub count: Arc<AtomicUsize>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerScheduledGroupsErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerControlFrameHook {
    pub entered: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerControlFramePauseHook {
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerStartPauseHook {
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerStartDelayHook {
    pub delays: std::collections::VecDeque<Duration>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerScheduledGroupsRefreshQueueHook {
    pub replies: std::collections::VecDeque<(
        Option<std::collections::BTreeSet<String>>,
        Option<std::collections::BTreeSet<String>>,
    )>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerScheduledGroupsRefreshErrorQueueHook {
    pub errs: std::collections::VecDeque<CnxError>,
    pub sticky_worker_instance_id: Option<u64>,
    pub sticky_peer_err: Option<String>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerForceFindErrorQueueHook {
    pub errs: std::collections::VecDeque<CnxError>,
    pub sticky_worker_instance_id: Option<u64>,
    pub sticky_peer_err: Option<String>,
}

#[cfg(test)]
fn source_worker_close_hook_cell() -> &'static Mutex<Option<SourceWorkerCloseHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerCloseHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_update_roots_hook_cell() -> &'static Mutex<Option<SourceWorkerUpdateRootsHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerUpdateRootsHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_control_frame_hook_cell() -> &'static Mutex<Option<SourceWorkerControlFrameHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerControlFrameHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_control_frame_pause_hook_cell()
-> &'static Mutex<Option<SourceWorkerControlFramePauseHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerControlFramePauseHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_update_roots_error_hook_cell()
-> &'static Mutex<Option<SourceWorkerUpdateRootsErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerUpdateRootsErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_logical_roots_snapshot_hook_cell()
-> &'static Mutex<Option<SourceWorkerLogicalRootsSnapshotHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerLogicalRootsSnapshotHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_control_frame_error_hook_cell()
-> &'static Mutex<Option<SourceWorkerControlFrameErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerControlFrameErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_control_frame_error_queue_hook_cell()
-> &'static Mutex<Option<SourceWorkerControlFrameErrorQueueHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerControlFrameErrorQueueHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_status_error_hook_cell() -> &'static Mutex<Option<SourceWorkerStatusErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerStatusErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_observability_error_hook_cell()
-> &'static Mutex<Option<SourceWorkerObservabilityErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerObservabilityErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_observability_call_count_hook_cell()
-> &'static Mutex<Option<SourceWorkerObservabilityCallCountHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerObservabilityCallCountHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_trigger_rescan_when_ready_call_count_hook_cell()
-> &'static Mutex<Option<SourceWorkerTriggerRescanWhenReadyCallCountHook>> {
    static CELL: std::sync::OnceLock<
        Mutex<Option<SourceWorkerTriggerRescanWhenReadyCallCountHook>>,
    > = std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_scheduled_groups_error_hook_cell()
-> &'static Mutex<Option<SourceWorkerScheduledGroupsErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerScheduledGroupsErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_start_error_queue_hook_cell()
-> &'static Mutex<Option<SourceWorkerStartErrorQueueHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerStartErrorQueueHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_start_pause_hook_cell() -> &'static Mutex<Option<SourceWorkerStartPauseHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerStartPauseHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_start_delay_hook_cell() -> &'static Mutex<Option<SourceWorkerStartDelayHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerStartDelayHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_scheduled_groups_refresh_queue_hook_cell()
-> &'static Mutex<Option<SourceWorkerScheduledGroupsRefreshQueueHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerScheduledGroupsRefreshQueueHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_scheduled_groups_refresh_error_queue_hook_cell()
-> &'static Mutex<Option<SourceWorkerScheduledGroupsRefreshErrorQueueHook>> {
    static CELL: std::sync::OnceLock<
        Mutex<Option<SourceWorkerScheduledGroupsRefreshErrorQueueHook>>,
    > = std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_force_find_error_queue_hook_cell()
-> &'static Mutex<Option<SourceWorkerForceFindErrorQueueHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerForceFindErrorQueueHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub(crate) fn install_source_worker_close_hook(hook: SourceWorkerCloseHook) {
    let mut guard = match source_worker_close_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_control_frame_hook(hook: SourceWorkerControlFrameHook) {
    let mut guard = match source_worker_control_frame_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_control_frame_pause_hook(
    hook: SourceWorkerControlFramePauseHook,
) {
    let mut guard = match source_worker_control_frame_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_start_pause_hook(hook: SourceWorkerStartPauseHook) {
    let mut guard = match source_worker_start_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_start_delay_hook(hook: SourceWorkerStartDelayHook) {
    let mut guard = match source_worker_start_delay_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_scheduled_groups_refresh_queue_hook(
    hook: SourceWorkerScheduledGroupsRefreshQueueHook,
) {
    let mut guard = match source_worker_scheduled_groups_refresh_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_scheduled_groups_refresh_error_queue_hook(
    hook: SourceWorkerScheduledGroupsRefreshErrorQueueHook,
) {
    let mut guard = match source_worker_scheduled_groups_refresh_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_force_find_error_queue_hook(
    hook: SourceWorkerForceFindErrorQueueHook,
) {
    let mut guard = match source_worker_force_find_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn clear_source_worker_control_frame_hook() {
    let mut guard = match source_worker_control_frame_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_control_frame_pause_hook() {
    let mut guard = match source_worker_control_frame_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_control_frame_error_queue_hook() {
    let mut guard = match source_worker_control_frame_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_start_pause_hook() {
    let mut guard = match source_worker_start_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_start_delay_hook() {
    let mut guard = match source_worker_start_delay_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_scheduled_groups_refresh_queue_hook() {
    let mut guard = match source_worker_scheduled_groups_refresh_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_scheduled_groups_refresh_error_queue_hook() {
    let mut guard = match source_worker_scheduled_groups_refresh_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_force_find_error_queue_hook() {
    let mut guard = match source_worker_force_find_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_close_hook() {
    let mut guard = match source_worker_close_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn install_source_worker_update_roots_hook(hook: SourceWorkerUpdateRootsHook) {
    let mut guard = match source_worker_update_roots_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn clear_source_worker_update_roots_hook() {
    let mut guard = match source_worker_update_roots_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn install_source_worker_update_roots_error_hook(
    hook: SourceWorkerUpdateRootsErrorHook,
) {
    let mut guard = match source_worker_update_roots_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_logical_roots_snapshot_hook(
    hook: SourceWorkerLogicalRootsSnapshotHook,
) {
    let mut guard = match source_worker_logical_roots_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_control_frame_error_hook(
    hook: SourceWorkerControlFrameErrorHook,
) {
    let mut guard = match source_worker_control_frame_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_control_frame_error_queue_hook(
    hook: SourceWorkerControlFrameErrorQueueHook,
) {
    let mut guard = match source_worker_control_frame_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_status_error_hook(hook: SourceWorkerStatusErrorHook) {
    let mut guard = match source_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_observability_error_hook(
    hook: SourceWorkerObservabilityErrorHook,
) {
    let mut guard = match source_worker_observability_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_observability_call_count_hook(
    hook: SourceWorkerObservabilityCallCountHook,
) {
    let mut guard = match source_worker_observability_call_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_trigger_rescan_when_ready_call_count_hook(
    hook: SourceWorkerTriggerRescanWhenReadyCallCountHook,
) {
    let mut guard = match source_worker_trigger_rescan_when_ready_call_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_scheduled_groups_error_hook(
    hook: SourceWorkerScheduledGroupsErrorHook,
) {
    let mut guard = match source_worker_scheduled_groups_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_start_error_queue_hook(hook: SourceWorkerStartErrorQueueHook) {
    let mut guard = match source_worker_start_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn clear_source_worker_update_roots_error_hook() {
    let mut guard = match source_worker_update_roots_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_logical_roots_snapshot_hook() {
    let mut guard = match source_worker_logical_roots_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_control_frame_error_hook() {
    let mut guard = match source_worker_control_frame_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
    drop(guard);
    let mut queued = match source_worker_control_frame_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *queued = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_status_error_hook() {
    let mut guard = match source_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_observability_error_hook() {
    let mut guard = match source_worker_observability_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_observability_call_count_hook() {
    let mut guard = match source_worker_observability_call_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_trigger_rescan_when_ready_call_count_hook() {
    let mut guard = match source_worker_trigger_rescan_when_ready_call_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_scheduled_groups_error_hook() {
    let mut guard = match source_worker_scheduled_groups_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_start_error_queue_hook() {
    let mut guard = match source_worker_start_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn take_source_worker_status_error_hook() -> Option<CnxError> {
    let mut guard = match source_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
fn source_worker_logical_roots_snapshot_hook() -> Option<SourceWorkerLogicalRootsSnapshotHook> {
    let guard = match source_worker_logical_roots_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.as_ref().cloned()
}

#[cfg(test)]
async fn maybe_pause_before_logical_roots_snapshot_rpc() -> Option<Vec<RootSpec>> {
    let hook = source_worker_logical_roots_snapshot_hook()?;
    if let Some(entered) = hook.entered.as_ref() {
        entered.notify_waiters();
    }
    if let Some(release) = hook.release.as_ref() {
        release.notified().await;
    }
    hook.roots
}

#[cfg(test)]
fn take_source_worker_observability_error_hook() -> Option<CnxError> {
    let mut guard = match source_worker_observability_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
fn record_source_worker_observability_rpc_attempt() {
    let guard = match source_worker_observability_call_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(hook) = guard.as_ref() {
        hook.count.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
fn record_source_worker_trigger_rescan_when_ready_attempt() {
    let guard = match source_worker_trigger_rescan_when_ready_call_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(hook) = guard.as_ref() {
        hook.count.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
fn take_source_worker_scheduled_groups_error_hook() -> Option<CnxError> {
    let mut guard = match source_worker_scheduled_groups_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
fn take_source_worker_start_error_queue_hook(current_worker_instance_id: u64) -> Option<CnxError> {
    let mut guard = match source_worker_start_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    if hook.sticky_worker_instance_id == Some(current_worker_instance_id)
        && let Some(err) = hook.sticky_peer_err.clone()
    {
        return Some(CnxError::PeerError(err));
    }
    let err = hook.errs.pop_front();
    if hook.errs.is_empty() && hook.sticky_peer_err.is_none() {
        *guard = None;
    }
    err
}

#[cfg(test)]
fn take_source_worker_start_delay_hook() -> Option<Duration> {
    let mut guard = match source_worker_start_delay_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    let delay = hook.delays.pop_front();
    if hook.delays.is_empty() {
        *guard = None;
    }
    delay
}

#[cfg(test)]
fn take_source_worker_scheduled_groups_refresh_queue_hook() -> Option<(
    Option<std::collections::BTreeSet<String>>,
    Option<std::collections::BTreeSet<String>>,
)> {
    let mut guard = match source_worker_scheduled_groups_refresh_queue_hook_cell().lock() {
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
fn take_source_worker_scheduled_groups_refresh_error_queue_hook(
    current_worker_instance_id: u64,
) -> Option<CnxError> {
    let mut guard = match source_worker_scheduled_groups_refresh_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    if let Some(sticky_worker_instance_id) = hook.sticky_worker_instance_id {
        if sticky_worker_instance_id != current_worker_instance_id {
            return None;
        }
        if let Some(err) = hook.sticky_peer_err.clone() {
            return Some(CnxError::PeerError(err));
        }
    }
    let err = hook.errs.pop_front();
    if hook.errs.is_empty() && hook.sticky_peer_err.is_none() {
        *guard = None;
    }
    err
}

#[cfg(test)]
fn take_source_worker_force_find_error_queue_hook(
    current_worker_instance_id: u64,
) -> Option<CnxError> {
    let mut guard = match source_worker_force_find_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    if let Some(sticky_worker_instance_id) = hook.sticky_worker_instance_id {
        if sticky_worker_instance_id != current_worker_instance_id {
            return None;
        }
        if let Some(err) = hook.sticky_peer_err.clone() {
            return Some(CnxError::PeerError(err));
        }
    }
    let err = hook.errs.pop_front();
    if hook.errs.is_empty() && hook.sticky_peer_err.is_none() {
        *guard = None;
    }
    err
}

#[cfg(test)]
fn notify_source_worker_control_frame_started() {
    let hook = {
        let guard = match source_worker_control_frame_hook_cell().lock() {
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
async fn maybe_pause_before_on_control_frame_rpc() {
    let hook = {
        let mut guard = match source_worker_control_frame_pause_hook_cell().lock() {
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
async fn maybe_pause_before_ensure_started() {
    let hook = {
        let mut guard = match source_worker_start_pause_hook_cell().lock() {
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
fn notify_source_worker_close_started() {
    let hook = {
        let guard = match source_worker_close_hook_cell().lock() {
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
async fn maybe_pause_before_update_logical_roots_rpc() {
    let hook = {
        let mut guard = match source_worker_update_roots_hook_cell().lock() {
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
fn take_update_logical_roots_error_hook() -> Option<CnxError> {
    let mut guard = match source_worker_update_roots_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
fn take_on_control_frame_error_hook(current_worker_instance_id: u64) -> Option<CnxError> {
    {
        let mut guard = match source_worker_control_frame_error_queue_hook_cell().lock() {
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
    let mut guard = match source_worker_control_frame_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

impl SourceWorkerClientHandle {
    pub(crate) fn new(
        node_id: NodeId,
        config: SourceConfig,
        worker_binding: RuntimeWorkerBinding,
        worker_factory: RuntimeWorkerClientFactory,
    ) -> Result<Self> {
        let key = source_worker_handle_registry_key(&node_id, &worker_binding, &worker_factory);
        let shared = {
            let mut registry = lock_source_worker_handle_registry();
            if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
                existing
            } else {
                let shared = Arc::new(SharedSourceWorkerHandleState {
                    worker: tokio::sync::Mutex::new(SharedSourceWorkerClient {
                        instance_id: next_shared_source_worker_instance_id(),
                        client: Arc::new(worker_factory.connect(
                            node_id.clone(),
                            config.clone(),
                            worker_binding.clone(),
                        )?),
                    }),
                    cache: Arc::new(Mutex::new(SourceWorkerSnapshotCache {
                        grants: Some(config.host_object_grants.clone()),
                        logical_roots: Some(config.roots.clone()),
                        ..SourceWorkerSnapshotCache::default()
                    })),
                    retained_control_state: Arc::new(tokio::sync::Mutex::new(
                        RetainedSourceWorkerControlState::default(),
                    )),
                    control_state_replay_required: Arc::new(std::sync::atomic::AtomicBool::new(
                        false,
                    )),
                    start_serial: Arc::new(tokio::sync::Mutex::new(())),
                    control_ops_inflight: Arc::new(AtomicUsize::new(0)),
                    control_ops_serial: Arc::new(tokio::sync::Mutex::new(())),
                });
                registry.insert(key, Arc::downgrade(&shared));
                shared
            }
        };
        let manual_rescan_signal = SignalCell::from_state_boundary(
            crate::runtime::execution_units::SOURCE_RUNTIME_UNIT_ID,
            "manual_rescan",
            worker_factory.state_boundary(),
        )
        .map_err(|err| {
            CnxError::InvalidInput(format!("source manual-rescan signal init failed: {err}"))
        })?;
        Ok(Self {
            _shared: shared.clone(),
            node_id,
            worker_factory,
            worker_binding,
            manual_rescan_signal,
            cache: shared.cache.clone(),
            retained_control_state: shared.retained_control_state.clone(),
            control_state_replay_required: shared.control_state_replay_required.clone(),
            start_serial: shared.start_serial.clone(),
            control_ops_inflight: shared.control_ops_inflight.clone(),
            control_ops_serial: shared.control_ops_serial.clone(),
            config,
        })
    }

    async fn worker_client(&self) -> Arc<TypedRuntimeWorkerClient<SourceWorkerRpc, SourceConfig>> {
        self.shared_worker().await.1
    }

    async fn shared_worker(
        &self,
    ) -> (
        u64,
        Arc<TypedRuntimeWorkerClient<SourceWorkerRpc, SourceConfig>>,
    ) {
        let guard = self._shared.worker.lock().await;
        (guard.instance_id, guard.client.clone())
    }

    #[cfg(test)]
    pub(crate) async fn worker_instance_id_for_tests(&self) -> u64 {
        self.shared_worker().await.0
    }

    async fn replace_shared_worker_client(
        &self,
    ) -> Result<Arc<TypedRuntimeWorkerClient<SourceWorkerRpc, SourceConfig>>> {
        let replacement = SharedSourceWorkerClient {
            instance_id: next_shared_source_worker_instance_id(),
            client: Arc::new(self.worker_factory.connect(
                self.node_id.clone(),
                self.config.clone(),
                self.worker_binding.clone(),
            )?),
        };
        let stale_client = {
            let mut guard = self._shared.worker.lock().await;
            let stale = guard.client.clone();
            *guard = replacement;
            stale
        };
        self.control_state_replay_required
            .store(true, Ordering::Release);
        Ok(stale_client)
    }

    async fn reconnect_shared_worker_client(&self) -> Result<()> {
        let stale_client = self.replace_shared_worker_client().await?;
        let _ = stale_client.shutdown(Duration::from_millis(250)).await;
        Ok(())
    }

    async fn reconnect_shared_worker_client_detached(&self) -> Result<()> {
        let stale_client = self.replace_shared_worker_client().await?;
        tokio::spawn(async move {
            let _ = stale_client.shutdown(Duration::from_millis(250)).await;
        });
        Ok(())
    }

    pub(crate) async fn reconnect_after_fail_closed_control_error(&self) -> Result<()> {
        let stale_client = self.replace_shared_worker_client().await?;
        let _ = stale_client.shutdown(Duration::from_millis(250)).await;
        Ok(())
    }

    pub(crate) async fn reconnect_after_retryable_control_reset(&self) -> Result<()> {
        self.reconnect_after_fail_closed_control_error().await
    }

    async fn retain_control_signals(&self, signals: &[SourceControlSignal]) {
        let mut retained = self.retained_control_state.lock().await;
        for signal in signals {
            match signal {
                SourceControlSignal::RuntimeHostGrantChange { .. } => {
                    retained.latest_host_grant_change = Some(signal.clone());
                }
                SourceControlSignal::Activate {
                    unit, route_key, ..
                } => {
                    retained.active_by_route.insert(
                        (unit.unit_id().to_string(), route_key.clone()),
                        signal.clone(),
                    );
                }
                SourceControlSignal::Deactivate {
                    unit, route_key, ..
                } => {
                    retained
                        .active_by_route
                        .remove(&(unit.unit_id().to_string(), route_key.clone()));
                }
                SourceControlSignal::Tick { .. }
                | SourceControlSignal::ManualRescan { .. }
                | SourceControlSignal::Passthrough(_) => {}
            }
        }
    }

    async fn generation_one_activate_wave_matches_retained_state(
        &self,
        signals: &[SourceControlSignal],
    ) -> bool {
        if !source_control_signals_are_generation_one_activate_only(signals) {
            return false;
        }
        let retained = self.retained_control_state.lock().await;
        !retained.active_by_route.is_empty()
            && signals.iter().all(|signal| match signal {
                SourceControlSignal::Activate {
                    unit,
                    route_key,
                    bound_scopes,
                    ..
                } => retained
                    .active_by_route
                    .get(&(unit.unit_id().to_string(), route_key.clone()))
                    .is_some_and(|retained_signal| match retained_signal {
                        SourceControlSignal::Activate {
                            generation,
                            bound_scopes: retained_bound_scopes,
                            ..
                        } => *generation == 1 && retained_bound_scopes == bound_scopes,
                        _ => false,
                    }),
                _ => false,
            })
    }

    async fn classify_tick_only_wave_against_retained_state(
        &self,
        signals: &[SourceControlSignal],
    ) -> Option<RetainedTickOnlyWaveDisposition> {
        if signals.is_empty()
            || !signals
                .iter()
                .all(|signal| matches!(signal, SourceControlSignal::Tick { .. }))
        {
            return None;
        }
        let replay_required = self.control_state_replay_required.load(Ordering::Acquire);
        let retained = self.retained_control_state.lock().await;
        if retained.active_by_route.is_empty() {
            return None;
        }
        let mut saw_generation_mismatch = false;
        for signal in signals {
            let SourceControlSignal::Tick {
                unit,
                route_key,
                generation,
                ..
            } = signal
            else {
                return None;
            };
            let Some(retained_signal) = retained
                .active_by_route
                .get(&(unit.unit_id().to_string(), route_key.clone()))
            else {
                return None;
            };
            let SourceControlSignal::Activate {
                generation: retained_generation,
                ..
            } = retained_signal
            else {
                return None;
            };
            if retained_generation != generation {
                saw_generation_mismatch = true;
            }
        }
        if saw_generation_mismatch {
            Some(RetainedTickOnlyWaveDisposition::ReconnectAndReplay)
        } else if !replay_required {
            Some(RetainedTickOnlyWaveDisposition::FastPath)
        } else {
            Some(RetainedTickOnlyWaveDisposition::ReplayInPlace)
        }
    }

    async fn replay_retained_control_state_if_needed_for_refresh_until(
        &self,
        deadline: std::time::Instant,
    ) -> Result<()> {
        if self
            .control_state_replay_required
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
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
                    .map(SourceControlSignal::envelope),
            );
            envelopes
        };
        if envelopes.is_empty() {
            return Ok(());
        }

        let deadline = clip_retry_deadline(deadline, SOURCE_WORKER_SCHEDULE_REFRESH_TOTAL_TIMEOUT);
        loop {
            let attempt_timeout = match Self::schedule_refresh_call_timeout(deadline) {
                Ok(timeout) => timeout,
                Err(err) => {
                    self.control_state_replay_required
                        .store(true, Ordering::Release);
                    return Err(err);
                }
            };
            let worker = self.worker_client().await;
            let rpc_result = match tokio::time::timeout(
                attempt_timeout,
                worker.with_started_retry(|client| {
                    let envelopes = envelopes.clone();
                    async move {
                        Self::call_worker(
                            &client,
                            SourceWorkerRequest::OnControlFrame {
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
                Ok(SourceWorkerResponse::Ack) => {
                    self.control_state_replay_required
                        .store(false, Ordering::Release);
                    return Ok(());
                }
                Ok(other) => {
                    self.control_state_replay_required
                        .store(true, Ordering::Release);
                    return Err(CnxError::ProtocolViolation(format!(
                        "unexpected source worker response while replaying retained control state: {:?}",
                        other
                    )));
                }
                Err(err)
                    if can_retry_on_control_frame(&err) && std::time::Instant::now() < deadline =>
                {
                    self.reconnect_shared_worker_client().await?;
                    Self::sleep_retry_backoff_with_deadline(deadline).await?;
                }
                Err(err) => {
                    self.control_state_replay_required
                        .store(true, Ordering::Release);
                    return Err(err);
                }
            }
        }
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
            tokio::time::sleep(SOURCE_WORKER_CLOSE_DRAIN_POLL_INTERVAL).await;
        }
    }

    fn with_cache_mut<T>(&self, f: impl FnOnce(&mut SourceWorkerSnapshotCache) -> T) -> T {
        let mut guard = match self.cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                log::warn!("source worker cache lock poisoned; recovering cached snapshot state");
                poisoned.into_inner()
            }
        };
        f(&mut guard)
    }

    fn degraded_observability_snapshot_from_cache(
        &self,
        reason: impl Into<String>,
    ) -> SourceObservabilitySnapshot {
        let guard = match self.cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                log::warn!("source worker cache lock poisoned; recovering cached snapshot state");
                poisoned.into_inner()
            }
        };
        build_degraded_worker_observability_snapshot(&guard, reason)
    }

    fn update_cached_observability_snapshot(&self, snapshot: &SourceObservabilitySnapshot) {
        self.with_cache_mut(|cache| {
            let can_preserve_omitted_observability =
                source_observability_snapshot_has_active_state(snapshot)
                    && !source_observability_snapshot_debug_maps_absent(snapshot);
            let explicit_zero_published_nodes = explicit_zero_published_counter_nodes(snapshot);
            let preserve_last_scheduled_source_groups = should_preserve_cached_observability_map(
                can_preserve_omitted_observability,
                &snapshot.scheduled_source_groups_by_node,
                cache.scheduled_source_groups_by_node.as_ref(),
            );
            let preserve_last_scheduled_scan_groups = should_preserve_cached_observability_map(
                can_preserve_omitted_observability,
                &snapshot.scheduled_scan_groups_by_node,
                cache.scheduled_scan_groups_by_node.as_ref(),
            );
            let preserve_last_control_summary = should_preserve_cached_observability_map(
                can_preserve_omitted_observability,
                &snapshot.last_control_frame_signals_by_node,
                cache.last_control_frame_signals_by_node.as_ref(),
            );
            let preserve_last_published_batches = should_preserve_cached_observability_map(
                can_preserve_omitted_observability,
                &snapshot.published_batches_by_node,
                cache.published_batches_by_node.as_ref(),
            );
            let preserve_last_published_events = should_preserve_cached_observability_map(
                can_preserve_omitted_observability,
                &snapshot.published_events_by_node,
                cache.published_events_by_node.as_ref(),
            );
            let preserve_last_published_control_events = should_preserve_cached_observability_map(
                can_preserve_omitted_observability,
                &snapshot.published_control_events_by_node,
                cache.published_control_events_by_node.as_ref(),
            );
            let preserve_last_published_data_events = should_preserve_cached_observability_map(
                can_preserve_omitted_observability,
                &snapshot.published_data_events_by_node,
                cache.published_data_events_by_node.as_ref(),
            );
            cache.lifecycle_state = Some(snapshot.lifecycle_state.clone());
            cache.last_live_observability_snapshot_at = Some(Instant::now());
            cache.host_object_grants_version = Some(snapshot.host_object_grants_version);
            cache.grants = Some(snapshot.grants.clone());
            cache.logical_roots = Some(snapshot.logical_roots.clone());
            cache.status = Some(snapshot.status.clone());
            cache.source_primary_by_group = Some(snapshot.source_primary_by_group.clone());
            cache.last_force_find_runner_by_group =
                Some(snapshot.last_force_find_runner_by_group.clone());
            cache.force_find_inflight_groups = Some(snapshot.force_find_inflight_groups.clone());
            if !preserve_last_scheduled_source_groups {
                cache.scheduled_source_groups_by_node =
                    Some(snapshot.scheduled_source_groups_by_node.clone());
            }
            if !preserve_last_scheduled_scan_groups {
                cache.scheduled_scan_groups_by_node =
                    Some(snapshot.scheduled_scan_groups_by_node.clone());
            }
            if !preserve_last_control_summary {
                cache.last_control_frame_signals_by_node =
                    Some(snapshot.last_control_frame_signals_by_node.clone());
            }
            if !preserve_last_published_batches {
                cache.published_batches_by_node = Some(snapshot.published_batches_by_node.clone());
            }
            if !preserve_last_published_events {
                cache.published_events_by_node = Some(snapshot.published_events_by_node.clone());
            }
            if !preserve_last_published_control_events {
                cache.published_control_events_by_node =
                    Some(snapshot.published_control_events_by_node.clone());
            }
            if !preserve_last_published_data_events {
                cache.published_data_events_by_node =
                    Some(snapshot.published_data_events_by_node.clone());
            }
            cache.last_published_at_us_by_node = Some(merge_cached_observability_node_map(
                can_preserve_omitted_observability,
                &snapshot.last_published_at_us_by_node,
                cache.last_published_at_us_by_node.as_ref(),
                &explicit_zero_published_nodes,
            ));
            cache.last_published_origins_by_node = Some(merge_cached_observability_node_map(
                can_preserve_omitted_observability,
                &snapshot.last_published_origins_by_node,
                cache.last_published_origins_by_node.as_ref(),
                &explicit_zero_published_nodes,
            ));
            cache.published_origin_counts_by_node = Some(merge_cached_observability_node_map(
                can_preserve_omitted_observability,
                &snapshot.published_origin_counts_by_node,
                cache.published_origin_counts_by_node.as_ref(),
                &explicit_zero_published_nodes,
            ));
            cache.published_path_capture_target = merge_cached_observability_option(
                can_preserve_omitted_observability,
                &snapshot.published_path_capture_target,
                &cache.published_path_capture_target,
                !explicit_zero_published_nodes.is_empty(),
            );
            cache.enqueued_path_origin_counts_by_node = Some(merge_cached_observability_node_map(
                can_preserve_omitted_observability,
                &snapshot.enqueued_path_origin_counts_by_node,
                cache.enqueued_path_origin_counts_by_node.as_ref(),
                &explicit_zero_published_nodes,
            ));
            cache.pending_path_origin_counts_by_node = Some(merge_cached_observability_node_map(
                can_preserve_omitted_observability,
                &snapshot.pending_path_origin_counts_by_node,
                cache.pending_path_origin_counts_by_node.as_ref(),
                &explicit_zero_published_nodes,
            ));
            cache.yielded_path_origin_counts_by_node = Some(merge_cached_observability_node_map(
                can_preserve_omitted_observability,
                &snapshot.yielded_path_origin_counts_by_node,
                cache.yielded_path_origin_counts_by_node.as_ref(),
                &explicit_zero_published_nodes,
            ));
            cache.summarized_path_origin_counts_by_node =
                Some(merge_cached_observability_node_map(
                    can_preserve_omitted_observability,
                    &snapshot.summarized_path_origin_counts_by_node,
                    cache.summarized_path_origin_counts_by_node.as_ref(),
                    &explicit_zero_published_nodes,
                ));
            cache.published_path_origin_counts_by_node =
                Some(merge_cached_observability_node_map(
                    can_preserve_omitted_observability,
                    &snapshot.published_path_origin_counts_by_node,
                    cache.published_path_origin_counts_by_node.as_ref(),
                    &explicit_zero_published_nodes,
                ));
        });
    }

    fn schedule_refresh_call_timeout(deadline: std::time::Instant) -> Result<Duration> {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            Err(CnxError::Timeout)
        } else {
            Ok(std::cmp::min(SOURCE_WORKER_CONTROL_RPC_TIMEOUT, remaining))
        }
    }

    async fn sleep_retry_backoff_with_deadline(deadline: std::time::Instant) -> Result<()> {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(CnxError::Timeout);
        }
        tokio::time::sleep(std::cmp::min(SOURCE_WORKER_RETRY_BACKOFF, remaining)).await;
        Ok(())
    }

    async fn refresh_cached_scheduled_groups_from_live_worker_until(
        &self,
        deadline: std::time::Instant,
    ) -> std::result::Result<(), ScheduledGroupsRefreshFailure> {
        eprintln!(
            "fs_meta_source_worker_client: refresh_cached_scheduled_groups begin node={}",
            self.node_id.0
        );
        let replay_required_recovery = self.control_state_replay_required.load(Ordering::Acquire);
        let mut recovered_live_worker_during_refresh = false;
        let deadline = clip_retry_deadline(deadline, SOURCE_WORKER_SCHEDULE_REFRESH_TOTAL_TIMEOUT);
        loop {
            let worker = self.worker_client().await;
            let ensure_started_result = match tokio::time::timeout(
                Self::schedule_refresh_call_timeout(deadline).map_err(|err| {
                    ScheduledGroupsRefreshFailure {
                        err,
                        recovered_live_worker_during_refresh,
                    }
                })?,
                worker.ensure_started(),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(CnxError::Timeout),
            };
            match ensure_started_result {
                Ok(()) => {}
                Err(err)
                    if can_retry_on_control_frame(&err) && std::time::Instant::now() < deadline =>
                {
                    eprintln!(
                        "fs_meta_source_worker_client: refresh_cached_scheduled_groups retry ensure_started node={} err={}",
                        self.node_id.0, err
                    );
                    self.reconnect_shared_worker_client().await.map_err(|err| {
                        ScheduledGroupsRefreshFailure {
                            err,
                            recovered_live_worker_during_refresh,
                        }
                    })?;
                    Self::sleep_retry_backoff_with_deadline(deadline)
                        .await
                        .map_err(|err| ScheduledGroupsRefreshFailure {
                            err,
                            recovered_live_worker_during_refresh,
                        })?;
                    continue;
                }
                Err(err) => {
                    eprintln!(
                        "fs_meta_source_worker_client: refresh_cached_scheduled_groups err ensure_started node={} err={}",
                        self.node_id.0, err
                    );
                    return Err(ScheduledGroupsRefreshFailure {
                        err,
                        recovered_live_worker_during_refresh,
                    });
                }
            }
            self.replay_retained_control_state_if_needed_for_refresh_until(deadline)
                .await
                .map_err(|err| ScheduledGroupsRefreshFailure {
                    err,
                    recovered_live_worker_during_refresh,
                })?;
            if replay_required_recovery {
                recovered_live_worker_during_refresh = true;
            }
            let worker = self.worker_client().await;
            let client = match worker.client().await {
                Ok(client) => client,
                Err(err)
                    if can_retry_on_control_frame(&err) && std::time::Instant::now() < deadline =>
                {
                    eprintln!(
                        "fs_meta_source_worker_client: refresh_cached_scheduled_groups retry client node={} err={}",
                        self.node_id.0, err
                    );
                    self.reconnect_shared_worker_client().await.map_err(|err| {
                        ScheduledGroupsRefreshFailure {
                            err,
                            recovered_live_worker_during_refresh,
                        }
                    })?;
                    Self::sleep_retry_backoff_with_deadline(deadline)
                        .await
                        .map_err(|err| ScheduledGroupsRefreshFailure {
                            err,
                            recovered_live_worker_during_refresh,
                        })?;
                    continue;
                }
                Err(err) => {
                    eprintln!(
                        "fs_meta_source_worker_client: refresh_cached_scheduled_groups err client node={} err={}",
                        self.node_id.0, err
                    );
                    return Err(ScheduledGroupsRefreshFailure {
                        err,
                        recovered_live_worker_during_refresh,
                    });
                }
            };
            let grants = match Self::call_worker(
                &client,
                SourceWorkerRequest::HostObjectGrantsSnapshot,
                Self::schedule_refresh_call_timeout(deadline).map_err(|err| {
                    ScheduledGroupsRefreshFailure {
                        err,
                        recovered_live_worker_during_refresh,
                    }
                })?,
            )
            .await
            {
                Ok(SourceWorkerResponse::HostObjectGrants(grants)) => {
                    self.with_cache_mut(|cache| {
                        cache.grants = Some(grants.clone());
                    });
                    grants
                }
                Ok(other) => {
                    return Err(ScheduledGroupsRefreshFailure {
                        err: CnxError::ProtocolViolation(format!(
                            "unexpected source worker response for scheduled groups host grants refresh: {:?}",
                            other
                        )),
                        recovered_live_worker_during_refresh,
                    });
                }
                Err(err) if can_use_cached_host_object_grants_snapshot(&err) => self
                    .cached_host_object_grants_snapshot()
                    .map_err(|err| ScheduledGroupsRefreshFailure {
                        err,
                        recovered_live_worker_during_refresh,
                    })?,
                Err(err)
                    if can_retry_on_control_frame(&err) && std::time::Instant::now() < deadline =>
                {
                    eprintln!(
                        "fs_meta_source_worker_client: refresh_cached_scheduled_groups retry grants node={} err={}",
                        self.node_id.0, err
                    );
                    self.reconnect_shared_worker_client().await.map_err(|err| {
                        ScheduledGroupsRefreshFailure {
                            err,
                            recovered_live_worker_during_refresh,
                        }
                    })?;
                    Self::sleep_retry_backoff_with_deadline(deadline)
                        .await
                        .map_err(|err| ScheduledGroupsRefreshFailure {
                            err,
                            recovered_live_worker_during_refresh,
                        })?;
                    continue;
                }
                Err(err) => {
                    eprintln!(
                        "fs_meta_source_worker_client: refresh_cached_scheduled_groups err grants node={} err={}",
                        self.node_id.0, err
                    );
                    return Err(ScheduledGroupsRefreshFailure {
                        err,
                        recovered_live_worker_during_refresh,
                    });
                }
            };
            let stable_host_ref = {
                let stable = stable_host_ref_for_node_id(&self.node_id, &grants);
                if stable != self.node_id.0 {
                    stable
                } else {
                    self.with_cache_mut(|cache| {
                        stable_host_ref_from_cached_scheduled_groups(&self.node_id, cache)
                            .unwrap_or(stable)
                    })
                }
            };
            #[cfg(test)]
            let refresh_attempt = {
                let injected = take_source_worker_scheduled_groups_refresh_error_queue_hook(
                    self.worker_instance_id_for_tests().await,
                );
                if let Some(err) = injected {
                    Err(err)
                } else {
                    self.fetch_scheduled_groups_refresh_attempt(&client, &stable_host_ref, deadline)
                        .await
                }
            };
            #[cfg(not(test))]
            let refresh_attempt = self
                .fetch_scheduled_groups_refresh_attempt(&client, &stable_host_ref, deadline)
                .await;
            let (scheduled_source, scheduled_scan) = match refresh_attempt {
                Ok(groups) => groups,
                Err(err)
                    if can_retry_on_control_frame(&err) && std::time::Instant::now() < deadline =>
                {
                    eprintln!(
                        "fs_meta_source_worker_client: refresh_cached_scheduled_groups retry schedule node={} err={}",
                        self.node_id.0, err
                    );
                    if replay_required_recovery {
                        // A replay-required refresh timeout means the freshly re-acquired worker
                        // still has not become live enough to answer scheduled-group refresh RPCs.
                        // Recover by rotating to another live worker candidate instead of
                        // exhausting the replay-required path on the same stalled instance.
                        self.reconnect_shared_worker_client().await.map_err(|err| {
                            ScheduledGroupsRefreshFailure {
                                err,
                                recovered_live_worker_during_refresh,
                            }
                        })?;
                    } else {
                        self.reconnect_shared_worker_client().await.map_err(|err| {
                            ScheduledGroupsRefreshFailure {
                                err,
                                recovered_live_worker_during_refresh,
                            }
                        })?;
                    }
                    Self::sleep_retry_backoff_with_deadline(deadline)
                        .await
                        .map_err(|err| ScheduledGroupsRefreshFailure {
                            err,
                            recovered_live_worker_during_refresh,
                        })?;
                    continue;
                }
                Err(err) => {
                    eprintln!(
                        "fs_meta_source_worker_client: refresh_cached_scheduled_groups err schedule node={} err={}",
                        self.node_id.0, err
                    );
                    return Err(ScheduledGroupsRefreshFailure {
                        err,
                        recovered_live_worker_during_refresh,
                    });
                }
            };
            let cache_empty = self.with_cache_mut(|cache| {
                cache
                    .scheduled_source_groups_by_node
                    .as_ref()
                    .is_none_or(|groups| groups.is_empty())
                    && cache
                        .scheduled_scan_groups_by_node
                        .as_ref()
                        .is_none_or(|groups| groups.is_empty())
            });
            if scheduled_source.values().all(|groups| groups.is_empty())
                && scheduled_scan.values().all(|groups| groups.is_empty())
                && cache_empty
                && std::time::Instant::now() < deadline
            {
                Self::sleep_retry_backoff_with_deadline(deadline)
                    .await
                    .map_err(|err| ScheduledGroupsRefreshFailure {
                        err,
                        recovered_live_worker_during_refresh,
                    })?;
                continue;
            }
            self.with_cache_mut(|cache| {
                update_cached_scheduled_groups_from_refresh(
                    cache,
                    scheduled_source,
                    scheduled_scan,
                );
            });
            eprintln!(
                "fs_meta_source_worker_client: refresh_cached_scheduled_groups ok node={}",
                self.node_id.0
            );
            return Ok(());
        }
    }

    async fn fetch_scheduled_groups_refresh_attempt(
        &self,
        client: &TypedWorkerClient<SourceWorkerRpc>,
        stable_host_ref: &str,
        deadline: std::time::Instant,
    ) -> Result<(
        std::collections::BTreeMap<String, Vec<String>>,
        std::collections::BTreeMap<String, Vec<String>>,
    )> {
        #[cfg(test)]
        if let Some((source_groups, scan_groups)) =
            take_source_worker_scheduled_groups_refresh_queue_hook()
        {
            let to_map = |groups: Option<std::collections::BTreeSet<String>>| {
                groups
                    .map(|groups| {
                        std::collections::BTreeMap::from([(
                            stable_host_ref.to_string(),
                            groups.into_iter().collect::<Vec<_>>(),
                        )])
                    })
                    .unwrap_or_default()
            };
            return Ok((to_map(source_groups), to_map(scan_groups)));
        }

        let scheduled_source = match Self::call_worker(
            client,
            SourceWorkerRequest::ScheduledSourceGroupIds,
            Self::schedule_refresh_call_timeout(deadline)?,
        )
        .await?
        {
            SourceWorkerResponse::ScheduledGroupIds(groups) => groups
                .map(|groups| {
                    std::collections::BTreeMap::from([(
                        stable_host_ref.to_string(),
                        groups.into_iter().collect::<Vec<_>>(),
                    )])
                })
                .unwrap_or_default(),
            other => {
                return Err(CnxError::ProtocolViolation(format!(
                    "unexpected source worker response for scheduled source groups cache refresh: {:?}",
                    other
                )));
            }
        };
        let scheduled_scan = match Self::call_worker(
            client,
            SourceWorkerRequest::ScheduledScanGroupIds,
            Self::schedule_refresh_call_timeout(deadline)?,
        )
        .await?
        {
            SourceWorkerResponse::ScheduledGroupIds(groups) => groups
                .map(|groups| {
                    std::collections::BTreeMap::from([(
                        stable_host_ref.to_string(),
                        groups.into_iter().collect::<Vec<_>>(),
                    )])
                })
                .unwrap_or_default(),
            other => {
                return Err(CnxError::ProtocolViolation(format!(
                    "unexpected source worker response for scheduled scan groups cache refresh: {:?}",
                    other
                )));
            }
        };
        Ok((scheduled_source, scheduled_scan))
    }

    async fn with_started_retry<T, F, Fut>(&self, op: F) -> Result<T>
    where
        F: Fn(TypedWorkerClient<SourceWorkerRpc>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.worker_client().await.with_started_retry(op).await
    }

    async fn client(&self) -> Result<TypedWorkerClient<SourceWorkerRpc>> {
        self.worker_client().await.client().await
    }

    async fn existing_client(&self) -> Result<Option<TypedWorkerClient<SourceWorkerRpc>>> {
        self.worker_client().await.existing_client().await
    }

    async fn call_worker(
        client: &TypedWorkerClient<SourceWorkerRpc>,
        request: SourceWorkerRequest,
        timeout: Duration,
    ) -> Result<SourceWorkerResponse> {
        client.call_with_timeout(request, timeout).await
    }

    async fn observability_snapshot_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<SourceObservabilitySnapshot> {
        let deadline = std::time::Instant::now() + timeout;
        let response = loop {
            let now = std::time::Instant::now();
            let attempt_timeout = timeout.min(deadline.saturating_duration_since(now));
            if attempt_timeout.is_zero() {
                return Err(CnxError::Timeout);
            }
            let rpc_result = self
                .with_started_retry(|client| async move {
                    #[cfg(test)]
                    record_source_worker_observability_rpc_attempt();
                    #[cfg(test)]
                    if let Some(err) = take_source_worker_observability_error_hook() {
                        return Err(err);
                    }
                    Self::call_worker(
                        &client,
                        SourceWorkerRequest::ObservabilitySnapshot,
                        attempt_timeout,
                    )
                    .await
                })
                .await;
            match rpc_result {
                Ok(response) => break response,
                Err(err)
                    if can_retry_update_logical_roots(&err)
                        && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(SOURCE_WORKER_RETRY_BACKOFF).await;
                }
                Err(err) => return Err(err),
            }
        };
        match response {
            SourceWorkerResponse::ObservabilitySnapshot(mut snapshot) => {
                self.with_cache_mut(|cache| {
                    normalize_observability_snapshot_scheduled_group_keys(
                        &mut snapshot,
                        &self.node_id,
                        cache.grants.as_ref(),
                    );
                    merge_non_empty_cached_groups(
                        &mut snapshot.scheduled_source_groups_by_node,
                        &cache.scheduled_source_groups_by_node,
                    );
                    merge_non_empty_cached_groups(
                        &mut snapshot.scheduled_scan_groups_by_node,
                        &cache.scheduled_scan_groups_by_node,
                    );
                });
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_source_worker_client: observability_snapshot reply node={} {}",
                        self.node_id.0,
                        summarize_source_observability_snapshot(&snapshot)
                    );
                }
                self.update_cached_observability_snapshot(&snapshot);
                Ok(snapshot)
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for observability snapshot: {:?}",
                other
            ))),
        }
    }

    pub async fn start(&self) -> Result<()> {
        let _start_serial = self.start_serial.lock().await;
        let deadline = std::time::Instant::now() + SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT;
        loop {
            let now = std::time::Instant::now();
            let attempt_timeout = std::cmp::min(
                SOURCE_WORKER_START_ATTEMPT_TIMEOUT,
                deadline.saturating_duration_since(now),
            );
            if attempt_timeout.is_zero() {
                return Err(CnxError::Timeout);
            }
            eprintln!(
                "fs_meta_source_worker_client: ensure_started begin node={}",
                self.node_id.0
            );
            #[cfg(test)]
            maybe_pause_before_ensure_started().await;
            #[cfg(test)]
            let injected = take_source_worker_start_error_queue_hook(
                self.worker_instance_id_for_tests().await,
            );
            #[cfg(not(test))]
            let injected = None::<CnxError>;
            #[cfg(test)]
            let injected_delay = take_source_worker_start_delay_hook();
            let worker = self.worker_client().await;
            let start_result = match injected {
                Some(err) => Err(err),
                None => match tokio::time::timeout(attempt_timeout, async {
                    #[cfg(test)]
                    if let Some(delay) = injected_delay {
                        tokio::time::sleep(delay).await;
                    }
                    worker.ensure_started().await
                })
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(CnxError::Timeout),
                },
            };
            match start_result {
                Ok(()) => {
                    eprintln!(
                        "fs_meta_source_worker_client: ensure_started ok node={}",
                        self.node_id.0
                    );
                    return Ok(());
                }
                Err(err)
                    if can_retry_on_control_frame(&err) && std::time::Instant::now() < deadline =>
                {
                    eprintln!(
                        "fs_meta_source_worker_client: on_control_frame retry node={} err={}",
                        self.node_id.0, err
                    );
                    self.reconnect_shared_worker_client().await?;
                    tokio::time::sleep(SOURCE_WORKER_RETRY_BACKOFF).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn update_logical_roots(&self, roots: Vec<RootSpec>) -> Result<()> {
        let _inflight = self.begin_control_op();
        let _serial = self.control_ops_serial.lock().await;
        eprintln!(
            "fs_meta_source_worker_client: update_logical_roots begin node={} roots={}",
            self.node_id.0,
            roots.len()
        );
        let deadline = std::time::Instant::now() + SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT;
        loop {
            let now = std::time::Instant::now();
            let attempt_timeout = std::cmp::min(
                SOURCE_WORKER_UPDATE_ROOTS_RPC_TIMEOUT,
                deadline.saturating_duration_since(now),
            );
            if attempt_timeout.is_zero() {
                return Err(CnxError::Timeout);
            }
            let rpc_result = self
                .with_started_retry(|client| {
                    let roots = roots.clone();
                    async move {
                        #[cfg(test)]
                        maybe_pause_before_update_logical_roots_rpc().await;
                        #[cfg(test)]
                        if let Some(err) = take_update_logical_roots_error_hook() {
                            return Err(err);
                        }
                        Self::call_worker(
                            &client,
                            SourceWorkerRequest::UpdateLogicalRoots {
                                roots: roots.clone(),
                            },
                            attempt_timeout,
                        )
                        .await
                    }
                })
                .await;
            match rpc_result {
                Ok(SourceWorkerResponse::Ack) => {
                    self.with_cache_mut(|cache| {
                        cache.logical_roots = Some(roots.clone());
                    });
                    eprintln!(
                        "fs_meta_source_worker_client: update_logical_roots ok node={} roots={}",
                        self.node_id.0,
                        roots.len()
                    );
                    return Ok(());
                }
                Err(CnxError::InvalidInput(message)) => {
                    return Err(CnxError::InvalidInput(message));
                }
                Ok(other) => {
                    return Err(CnxError::ProtocolViolation(format!(
                        "unexpected source worker response for update roots: {:?}",
                        other
                    )));
                }
                Err(err)
                    if can_retry_update_logical_roots(&err)
                        && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(SOURCE_WORKER_RETRY_BACKOFF).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        self.with_cache_mut(|cache| {
            Ok(cache
                .logical_roots
                .clone()
                .unwrap_or_else(|| self.config.roots.clone()))
        })
    }

    pub async fn logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        let (worker_instance_id, worker) = self.shared_worker().await;
        let Some(client) = worker.existing_client().await? else {
            return self.cached_logical_roots_snapshot();
        };
        #[cfg(test)]
        if let Some(roots) = maybe_pause_before_logical_roots_snapshot_rpc().await {
            return Ok(roots);
        }
        let result = Self::call_worker(
            &client,
            SourceWorkerRequest::LogicalRootsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await;
        if self.shared_worker().await.0 != worker_instance_id {
            return Err(CnxError::TransportClosed(
                "stale shared source worker client detached during logical_roots_snapshot".into(),
            ));
        }
        match result {
            Ok(SourceWorkerResponse::LogicalRoots(roots)) => {
                self.with_cache_mut(|cache| {
                    cache.logical_roots = Some(roots.clone());
                });
                Ok(roots)
            }
            Ok(other) => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for logical roots: {:?}",
                other
            ))),
            Err(err) if can_use_cached_logical_roots_snapshot(&err) => {
                self.cached_logical_roots_snapshot()
            }
            Err(err) => Err(err),
        }
    }

    pub fn cached_host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        self.with_cache_mut(|cache| {
            Ok(cache
                .grants
                .clone()
                .unwrap_or_else(|| self.config.host_object_grants.clone()))
        })
    }

    pub async fn host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        let Some(client) = self.existing_client().await? else {
            return self.cached_host_object_grants_snapshot();
        };
        let result = Self::call_worker(
            &client,
            SourceWorkerRequest::HostObjectGrantsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await;
        match result {
            Ok(SourceWorkerResponse::HostObjectGrants(grants)) => {
                self.with_cache_mut(|cache| {
                    cache.grants = Some(grants.clone());
                });
                Ok(grants)
            }
            Ok(other) => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for host object grants: {:?}",
                other
            ))),
            Err(err) if can_use_cached_host_object_grants_snapshot(&err) => {
                self.cached_host_object_grants_snapshot()
            }
            Err(err) => Err(err),
        }
    }

    pub async fn host_object_grants_version_snapshot(&self) -> Result<u64> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::HostObjectGrantsVersionSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        {
            Ok(SourceWorkerResponse::HostObjectGrantsVersion(version)) => {
                self.with_cache_mut(|cache| {
                    cache.host_object_grants_version = Some(version);
                });
                Ok(version)
            }
            Ok(other) => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for host object grants version: {:?}",
                other
            ))),
            Err(err) => Err(err),
        }
    }

    pub async fn status_snapshot(&self) -> Result<SourceStatusSnapshot> {
        self.with_started_retry(|client| async move {
            #[cfg(test)]
            if let Some(err) = take_source_worker_status_error_hook() {
                return Err(err);
            }
            Self::call_worker(
                &client,
                SourceWorkerRequest::StatusSnapshot,
                SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
            )
            .await
        })
        .await
        .and_then(|response| match response {
            SourceWorkerResponse::StatusSnapshot(snapshot) => {
                self.with_cache_mut(|cache| {
                    cache.status = Some(snapshot.clone());
                });
                Ok(snapshot)
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for status snapshot: {:?}",
                other
            ))),
        })
    }

    pub async fn lifecycle_state_label(&self) -> Result<String> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::LifecycleState,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        {
            Ok(SourceWorkerResponse::LifecycleState(state)) => {
                self.with_cache_mut(|cache| {
                    cache.lifecycle_state = Some(state.clone());
                });
                Ok(state)
            }
            Ok(other) => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for lifecycle state: {:?}",
                other
            ))),
            Err(err) => Err(err),
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

    pub async fn scheduled_source_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self
            .scheduled_group_ids_with_timeout(SourceWorkerRequest::ScheduledSourceGroupIds)
            .await?
        {
            SourceWorkerResponse::ScheduledGroupIds(groups) => Ok(self.with_cache_mut(|cache| {
                merge_cached_local_scheduled_groups(
                    &self.node_id,
                    groups.map(|groups| groups.into_iter().collect()),
                    &cache.scheduled_source_groups_by_node,
                )
            })),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for scheduled source groups: {:?}",
                other
            ))),
        }
    }

    pub async fn scheduled_scan_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self
            .scheduled_group_ids_with_timeout(SourceWorkerRequest::ScheduledScanGroupIds)
            .await?
        {
            SourceWorkerResponse::ScheduledGroupIds(groups) => Ok(self.with_cache_mut(|cache| {
                merge_cached_local_scheduled_groups(
                    &self.node_id,
                    groups.map(|groups| groups.into_iter().collect()),
                    &cache.scheduled_scan_groups_by_node,
                )
            })),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for scheduled scan groups: {:?}",
                other
            ))),
        }
    }

    async fn scheduled_group_ids_with_timeout(
        &self,
        request: SourceWorkerRequest,
    ) -> Result<SourceWorkerResponse> {
        let deadline = std::time::Instant::now() + SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT;
        loop {
            let now = std::time::Instant::now();
            let attempt_timeout =
                SOURCE_WORKER_CONTROL_RPC_TIMEOUT.min(deadline.saturating_duration_since(now));
            if attempt_timeout.is_zero() {
                return Err(CnxError::Timeout);
            }
            let rpc_result = self
                .with_started_retry(|client| {
                    let request = request.clone();
                    async move {
                        #[cfg(test)]
                        if let Some(err) = take_source_worker_scheduled_groups_error_hook() {
                            return Err(err);
                        }
                        Self::call_worker(&client, request, attempt_timeout).await
                    }
                })
                .await;
            match rpc_result {
                Ok(response) => return Ok(response),
                Err(err)
                    if can_retry_on_control_frame(&err) && std::time::Instant::now() < deadline =>
                {
                    self.reconnect_shared_worker_client().await?;
                    tokio::time::sleep(SOURCE_WORKER_RETRY_BACKOFF).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn source_primary_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::SourcePrimaryByGroupSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::SourcePrimaryByGroup(groups) => {
                self.with_cache_mut(|cache| {
                    cache.source_primary_by_group = Some(groups.clone());
                });
                Ok(groups)
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for primary groups: {:?}",
                other
            ))),
        }
    }

    pub async fn last_force_find_runner_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::LastForceFindRunnerByGroupSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::LastForceFindRunnerByGroup(groups) => {
                self.with_cache_mut(|cache| {
                    cache.last_force_find_runner_by_group = Some(groups.clone());
                });
                Ok(groups)
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for last force-find runner: {:?}",
                other
            ))),
        }
    }

    pub async fn force_find_inflight_groups_snapshot(&self) -> Result<Vec<String>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::ForceFindInflightGroupsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::ForceFindInflightGroups(groups) => {
                self.with_cache_mut(|cache| {
                    cache.force_find_inflight_groups = Some(groups.clone());
                });
                Ok(groups)
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for force-find inflight groups: {:?}",
                other
            ))),
        }
    }

    pub async fn resolve_group_id_for_object_ref(
        &self,
        object_ref: &str,
    ) -> Result<Option<String>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::ResolveGroupIdForObjectRef {
                object_ref: object_ref.to_string(),
            },
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::ResolveGroupIdForObjectRef(group) => Ok(group),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for resolve group: {:?}",
                other
            ))),
        }
    }

    pub async fn force_find(&self, params: InternalQueryRequest) -> Result<Vec<Event>> {
        let target_node = self.node_id.clone();
        let deadline = std::time::Instant::now() + SOURCE_WORKER_FORCE_FIND_TIMEOUT;
        loop {
            let now = std::time::Instant::now();
            let attempt_timeout =
                SOURCE_WORKER_FORCE_FIND_TIMEOUT.min(deadline.saturating_duration_since(now));
            if attempt_timeout.is_zero() {
                return Err(CnxError::Timeout);
            }
            #[cfg(test)]
            let worker_instance_id = self.worker_instance_id_for_tests().await;
            let rpc_result = self
                .with_started_retry(|client| {
                    let params = params.clone();
                    let target_node = target_node.clone();
                    async move {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_source_worker_client: force_find rpc begin target_node={} selected_group={:?} recursive={} max_depth={:?} path={}",
                                target_node.0,
                                params.scope.selected_group,
                                params.scope.recursive,
                                params.scope.max_depth,
                                String::from_utf8_lossy(&params.scope.path)
                            );
                        }
                        #[cfg(test)]
                        if let Some(err) =
                            take_source_worker_force_find_error_queue_hook(worker_instance_id)
                        {
                            return Err(err);
                        }
                        let response = Self::call_worker(
                            &client,
                            SourceWorkerRequest::ForceFind {
                                request: params.clone(),
                            },
                            attempt_timeout,
                        )
                        .await;
                        match response {
                            Err(err) => {
                                if debug_force_find_route_capture_enabled() {
                                    eprintln!(
                                        "fs_meta_source_worker_client: force_find rpc failed target_node={} err={}",
                                        target_node.0, err
                                    );
                                }
                                Err(err)
                            }
                            Ok(SourceWorkerResponse::Events(events)) => {
                                if debug_force_find_route_capture_enabled() {
                                    eprintln!(
                                        "fs_meta_source_worker_client: force_find rpc done target_node={} events={} origins={:?}",
                                        target_node.0,
                                        events.len(),
                                        summarize_event_counts_by_origin(&events)
                                    );
                                }
                                Ok(events)
                            }
                            Ok(other) => Err(CnxError::ProtocolViolation(format!(
                                "unexpected source worker response for force-find: {:?}",
                                other
                            ))),
                        }
                    }
                })
                .await;
            match rpc_result {
                Ok(events) => {
                    let _ = self.last_force_find_runner_by_group_snapshot().await;
                    return Ok(events);
                }
                Err(err) if can_retry_force_find(&err) && std::time::Instant::now() < deadline => {
                    tokio::time::sleep(SOURCE_WORKER_RETRY_BACKOFF).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn publish_manual_rescan_signal(&self) -> Result<()> {
        self.manual_rescan_signal
            .emit(&self.node_id.0)
            .await
            .map(|_| ())
            .map_err(|err| {
                CnxError::Internal(format!("publish manual rescan signal failed: {err}"))
            })
    }

    pub async fn on_control_frame(&self, envelopes: Vec<ControlEnvelope>) -> Result<()> {
        self.on_control_frame_with_timeouts(
            envelopes,
            SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
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
        let _serial = self.control_ops_serial.lock().await;
        #[cfg(test)]
        notify_source_worker_control_frame_started();
        let decoded_signals = source_control_signals_from_envelopes(&envelopes).ok();
        let fail_fast_generation_one_activate_replay =
            if let Some(signals) = decoded_signals.as_ref() {
                self.generation_one_activate_wave_matches_retained_state(signals)
                    .await
            } else {
                false
            };
        let generation_one_activate_only = decoded_signals.as_ref().is_some_and(|signals| {
            source_control_signals_are_generation_one_activate_only(signals)
        });
        let retained_tick_only_wave = if let Some(signals) = decoded_signals.as_ref() {
            self.classify_tick_only_wave_against_retained_state(signals)
                .await
        } else {
            None
        };
        let fail_fast_on_control_frame_bridge_reset = fail_fast_generation_one_activate_replay
            || is_restart_deferred_retire_pending_deactivate_batch(&envelopes);
        eprintln!(
            "fs_meta_source_worker_client: on_control_frame begin node={} envelopes={}",
            self.node_id.0,
            envelopes.len()
        );
        if debug_control_scope_capture_enabled() {
            match decoded_signals.as_ref() {
                Some(signals) => eprintln!(
                    "fs_meta_source_worker_client: on_control_frame summary node={} signals={:?}",
                    self.node_id.0,
                    summarize_source_control_signals(&signals)
                ),
                None => eprintln!(
                    "fs_meta_source_worker_client: on_control_frame summary node={} decode_err={}",
                    self.node_id.0, "decode failed"
                ),
            }
        }
        if let Some(retained_tick_disposition) = retained_tick_only_wave {
            if retained_tick_disposition == RetainedTickOnlyWaveDisposition::FastPath {
                eprintln!(
                    "fs_meta_source_worker_client: on_control_frame done node={} ok=true retained_tick_fast_path=true",
                    self.node_id.0
                );
                return Ok(());
            }
            if retained_tick_disposition == RetainedTickOnlyWaveDisposition::ReconnectAndReplay {
                self.reconnect_shared_worker_client().await?;
            }
            self.replay_retained_control_state_if_needed_for_refresh_until(
                std::time::Instant::now() + total_timeout,
            )
            .await?;
            eprintln!(
                "fs_meta_source_worker_client: on_control_frame done node={} ok=true retained_tick_replay=true",
                self.node_id.0
            );
            return Ok(());
        }
        let deadline = std::time::Instant::now() + total_timeout;
        let fail_fast_short_caller_budget_bridge_reset =
            total_timeout <= SOURCE_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT;
        let mut saw_timeout_like_generation_one_reset = false;
        loop {
            let now = std::time::Instant::now();
            let worker = self.worker_client().await;
            let prefer_short_existing_client_attempt =
                decoded_signals.as_ref().is_some_and(|signals| {
                    source_control_signals_prefer_short_existing_client_attempt(signals)
                });
            let rpc_attempt_budget = if prefer_short_existing_client_attempt
                && worker.existing_client().await?.is_some()
            {
                SOURCE_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT
            } else {
                rpc_timeout
            };
            let attempt_timeout =
                std::cmp::min(rpc_attempt_budget, deadline.saturating_duration_since(now));
            if attempt_timeout.is_zero() {
                return Err(CnxError::Timeout);
            }
            #[cfg(test)]
            let rpc_result = {
                let worker_instance_id = self.worker_instance_id_for_tests().await;
                match tokio::time::timeout(
                    attempt_timeout,
                    worker.with_started_retry(|client| {
                        let envelopes = envelopes.clone();
                        async move {
                            #[cfg(test)]
                            maybe_pause_before_on_control_frame_rpc().await;
                            #[cfg(test)]
                            if let Some(err) = take_on_control_frame_error_hook(worker_instance_id)
                            {
                                return Err(err);
                            }
                            Self::call_worker(
                                &client,
                                SourceWorkerRequest::OnControlFrame {
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
                }
            };
            #[cfg(not(test))]
            let rpc_result = {
                match tokio::time::timeout(
                    attempt_timeout,
                    worker.with_started_retry(|client| {
                        let envelopes = envelopes.clone();
                        async move {
                            #[cfg(test)]
                            maybe_pause_before_on_control_frame_rpc().await;
                            Self::call_worker(
                                &client,
                                SourceWorkerRequest::OnControlFrame {
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
                }
            };
            match rpc_result {
                Ok(SourceWorkerResponse::Ack) => {
                    if let Some(signals) = decoded_signals.as_ref() {
                        self.retain_control_signals(signals).await;
                    }
                    let should_refresh_cached_schedule =
                        if let Some(signals) = decoded_signals.as_ref() {
                            let primed_local_schedule = self.with_cache_mut(|cache| {
                                let primed_local_schedule =
                                    prime_cached_schedule_from_control_signals(
                                    cache,
                                    &self.node_id,
                                    signals,
                                    &self.config.roots,
                                    &self.config.host_object_grants,
                                );
                                prime_cached_control_summary_from_control_signals(
                                    cache,
                                    &self.node_id,
                                    signals,
                                    &self.config.host_object_grants,
                                );
                                primed_local_schedule
                            });
                            source_control_signals_require_post_ack_schedule_refresh(signals)
                                && !(primed_local_schedule.saw_activate_with_bound_scopes
                                    && !primed_local_schedule.has_local_runnable_groups)
                        } else {
                            true
                        };
                    if should_refresh_cached_schedule {
                        let replay_required_before_refresh =
                            self.control_state_replay_required.load(Ordering::Acquire);
                        if let Err(failure) = self
                            .refresh_cached_scheduled_groups_from_live_worker_until(deadline)
                            .await
                        {
                            let ScheduledGroupsRefreshFailure {
                                err,
                                recovered_live_worker_during_refresh,
                            } = failure;
                            if replay_required_before_refresh
                                && matches!(err, CnxError::Timeout)
                                && !recovered_live_worker_during_refresh
                            {
                                eprintln!(
                                    "fs_meta_source_worker_client: refresh_cached_scheduled_groups replay-required fail-closed node={} err={}",
                                    self.node_id.0, err
                                );
                                return Err(CnxError::Internal(
                                    "source worker replay-required scheduled-groups refresh exhausted before a live worker recovered"
                                        .to_string(),
                                ));
                            } else if let Some(sharp_err) =
                                post_ack_schedule_refresh_exhaustion_error(&err)
                            {
                                eprintln!(
                                    "fs_meta_source_worker_client: refresh_cached_scheduled_groups fail-closed node={} err={}",
                                    self.node_id.0, sharp_err
                                );
                                return Err(sharp_err);
                            } else {
                                return Err(err);
                            }
                        }
                    }
                    eprintln!(
                        "fs_meta_source_worker_client: on_control_frame done node={} ok=true",
                        self.node_id.0
                    );
                    return Ok(());
                }
                Ok(other) => {
                    eprintln!(
                        "fs_meta_source_worker_client: on_control_frame done node={} ok=false err=unexpected_response:{:?}",
                        self.node_id.0, other
                    );
                    return Err(CnxError::ProtocolViolation(format!(
                        "unexpected source worker response for on_control_frame: {:?}",
                        other
                    )));
                }
                Err(err)
                    if (fail_fast_on_control_frame_bridge_reset
                        || (generation_one_activate_only
                            && saw_timeout_like_generation_one_reset))
                        && (matches!(
                            err,
                            CnxError::TransportClosed(_) | CnxError::ChannelClosed
                        ) || is_retryable_worker_bridge_peer_error(&err)) =>
                {
                    self.reconnect_shared_worker_client_detached().await?;
                    eprintln!(
                        "fs_meta_source_worker_client: on_control_frame done node={} ok=false err={} fail_fast_on_control_frame_bridge_reset=true",
                        self.node_id.0, err
                    );
                    return Err(err);
                }
                Err(err)
                    if fail_fast_short_caller_budget_bridge_reset
                        && (matches!(
                            err,
                            CnxError::TransportClosed(_) | CnxError::ChannelClosed
                        ) || is_retryable_worker_bridge_peer_error(&err)) =>
                {
                    eprintln!(
                        "fs_meta_source_worker_client: on_control_frame done node={} ok=false err={} fail_fast_short_caller_budget_bridge_reset=true",
                        self.node_id.0, err
                    );
                    return Err(err);
                }
                Err(err)
                    if can_retry_on_control_frame(&err) && std::time::Instant::now() < deadline =>
                {
                    if generation_one_activate_only && is_timeout_like_worker_control_reset(&err) {
                        saw_timeout_like_generation_one_reset = true;
                        self.reconnect_shared_worker_client_detached().await?;
                        continue;
                    }
                    self.reconnect_shared_worker_client().await?;
                    tokio::time::sleep(SOURCE_WORKER_RETRY_BACKOFF).await;
                }
                Err(err) => {
                    eprintln!(
                        "fs_meta_source_worker_client: on_control_frame done node={} ok=false err={}",
                        self.node_id.0, err
                    );
                    return Err(err);
                }
            }
        }
    }

    pub async fn trigger_rescan_when_ready(&self) -> Result<()> {
        self.with_started_retry(|client| async move {
            match Self::call_worker(
                &client,
                SourceWorkerRequest::TriggerRescanWhenReady,
                SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
            )
            .await?
            {
                SourceWorkerResponse::Ack => Ok(()),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected source worker response for trigger_rescan_when_ready: {:?}",
                    other
                ))),
            }
        })
        .await
    }

    pub async fn close(&self) -> Result<()> {
        #[cfg(test)]
        notify_source_worker_close_started();
        self.wait_for_control_ops_to_drain(SOURCE_WORKER_CLOSE_DRAIN_TIMEOUT)
            .await;
        if Arc::strong_count(&self._shared) > 1 {
            return Ok(());
        }
        self.worker_client()
            .await
            .shutdown(Duration::from_secs(2))
            .await?;
        self.with_cache_mut(|cache| {
            cache.lifecycle_state = Some("closed".to_string());
        });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn shutdown_shared_worker_for_tests(&self) -> Result<()> {
        self.worker_client()
            .await
            .shutdown(Duration::from_secs(2))
            .await
    }

    async fn try_observability_snapshot_nonblocking(&self) -> Result<SourceObservabilitySnapshot> {
        if self.control_op_inflight() {
            let snapshot =
                self.degraded_observability_snapshot_from_cache("source worker control in flight");
            if debug_control_scope_capture_enabled() {
                eprintln!(
                    "fs_meta_source_worker_client: observability_snapshot cache_fallback node={} reason=control_inflight {}",
                    self.node_id.0,
                    summarize_source_observability_snapshot(&snapshot)
                );
            }
            return Ok(snapshot);
        }
        let Some(_client) = self.existing_client().await? else {
            let snapshot =
                self.degraded_observability_snapshot_from_cache("source worker status not started");
            if debug_control_scope_capture_enabled() {
                eprintln!(
                    "fs_meta_source_worker_client: observability_snapshot cache_fallback node={} reason=not_started {}",
                    self.node_id.0,
                    summarize_source_observability_snapshot(&snapshot)
                );
            }
            return Ok(snapshot);
        };
        if let Some(snapshot) = self.with_cache_mut(|cache| {
            cache
                .last_live_observability_snapshot_at
                .filter(|last| last.elapsed() < SOURCE_WORKER_NONBLOCKING_OBSERVABILITY_CACHE_TTL)
                .and_then(|_| build_cached_worker_observability_snapshot(cache))
        }) {
            if !recent_cached_source_observability_snapshot_is_incomplete(&snapshot) {
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_source_worker_client: observability_snapshot cache_fallback node={} reason=recent_live_cache {}",
                        self.node_id.0,
                        summarize_source_observability_snapshot(&snapshot)
                    );
                }
                return Ok(snapshot);
            }
        }
        let trace_id = next_source_status_trace_id();
        let mut trace_guard =
            SourceStatusTraceGuard::new(self.node_id.0.clone(), trace_id, "before_rpc_await");
        eprintln!(
            "fs_meta_source_worker_client: observability_snapshot begin node={} timeout_ms={} trace_id={}",
            self.node_id.0,
            SOURCE_WORKER_OBSERVABILITY_RPC_TIMEOUT.as_millis(),
            trace_id
        );
        let result = self
            .observability_snapshot_with_timeout(SOURCE_WORKER_OBSERVABILITY_RPC_TIMEOUT)
            .await;
        trace_guard.phase("after_rpc_await");
        eprintln!(
            "fs_meta_source_worker_client: observability_snapshot done node={} ok={} trace_id={}",
            self.node_id.0,
            result.is_ok(),
            trace_id
        );
        trace_guard.complete();
        result
    }

    pub(crate) async fn observability_snapshot_nonblocking(&self) -> SourceObservabilitySnapshot {
        match self.try_observability_snapshot_nonblocking().await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                let snapshot = self.degraded_observability_snapshot_from_cache(format!(
                    "source worker unavailable: {err}"
                ));
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_source_worker_client: observability_snapshot cache_fallback node={} reason=worker_unavailable err={} {}",
                        self.node_id.0,
                        err,
                        summarize_source_observability_snapshot(&snapshot)
                    );
                }
                snapshot
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceObservabilitySnapshot {
    pub lifecycle_state: String,
    pub host_object_grants_version: u64,
    pub grants: Vec<GrantedMountRoot>,
    pub logical_roots: Vec<RootSpec>,
    pub status: SourceStatusSnapshot,
    pub source_primary_by_group: std::collections::BTreeMap<String, String>,
    pub last_force_find_runner_by_group: std::collections::BTreeMap<String, String>,
    pub force_find_inflight_groups: Vec<String>,
    pub scheduled_source_groups_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub scheduled_scan_groups_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub last_control_frame_signals_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_batches_by_node: std::collections::BTreeMap<String, u64>,
    pub published_events_by_node: std::collections::BTreeMap<String, u64>,
    pub published_control_events_by_node: std::collections::BTreeMap<String, u64>,
    pub published_data_events_by_node: std::collections::BTreeMap<String, u64>,
    pub last_published_at_us_by_node: std::collections::BTreeMap<String, u64>,
    pub last_published_origins_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_path_capture_target: Option<String>,
    pub enqueued_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub pending_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub yielded_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub summarized_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
}

#[derive(Default)]
struct RecoveredPublishedObservability {
    published_batches_by_node: std::collections::BTreeMap<String, u64>,
    published_events_by_node: std::collections::BTreeMap<String, u64>,
    published_control_events_by_node: std::collections::BTreeMap<String, u64>,
    published_data_events_by_node: std::collections::BTreeMap<String, u64>,
    last_published_at_us_by_node: std::collections::BTreeMap<String, u64>,
    last_published_origins_by_node: std::collections::BTreeMap<String, Vec<String>>,
    published_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    published_path_capture_target: Option<String>,
    summarized_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    published_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
}

fn scheduled_groups_by_node(
    node_id: &NodeId,
    grants: &[GrantedMountRoot],
    groups: Result<Option<std::collections::BTreeSet<String>>>,
) -> std::collections::BTreeMap<String, Vec<String>> {
    let Ok(Some(groups)) = groups else {
        return std::collections::BTreeMap::new();
    };
    let stable_host_ref = stable_host_ref_for_node_id(node_id, grants);
    std::collections::BTreeMap::from([(stable_host_ref, groups.into_iter().collect::<Vec<_>>())])
}

pub(crate) fn source_status_entry_looks_active_for_local_observability(
    entry: &crate::source::SourceConcreteRootHealthSnapshot,
) -> bool {
    entry.active
        || entry.current_stream_generation.is_some()
        || entry.emitted_batch_count > 0
        || entry.forwarded_batch_count > 0
        || entry.emitted_event_count > 0
        || entry.forwarded_event_count > 0
}

pub(crate) fn recovered_scheduled_groups_by_node_from_active_status(
    stable_host_ref: &str,
    status: &SourceStatusSnapshot,
    select_group: impl Fn(&crate::source::SourceConcreteRootHealthSnapshot) -> bool,
) -> std::collections::BTreeMap<String, Vec<String>> {
    let groups = status
        .concrete_roots
        .iter()
        .filter(|entry| source_status_entry_looks_active_for_local_observability(entry))
        .filter(|entry| select_group(entry))
        .map(|entry| entry.logical_root_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    if groups.is_empty() {
        return std::collections::BTreeMap::new();
    }
    std::collections::BTreeMap::from([(
        stable_host_ref.to_string(),
        groups.into_iter().collect::<Vec<_>>(),
    )])
}

fn recovered_control_signals_by_node_from_active_status(
    stable_host_ref: &str,
    status: &SourceStatusSnapshot,
    scheduled_source_groups_by_node: &std::collections::BTreeMap<String, Vec<String>>,
    scheduled_scan_groups_by_node: &std::collections::BTreeMap<String, Vec<String>>,
) -> std::collections::BTreeMap<String, Vec<String>> {
    if !source_observability_snapshot_has_active_state(&SourceObservabilitySnapshot {
        lifecycle_state: String::new(),
        host_object_grants_version: 0,
        grants: Vec::new(),
        logical_roots: Vec::new(),
        status: status.clone(),
        source_primary_by_group: std::collections::BTreeMap::new(),
        last_force_find_runner_by_group: std::collections::BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: std::collections::BTreeMap::new(),
        scheduled_scan_groups_by_node: std::collections::BTreeMap::new(),
        last_control_frame_signals_by_node: std::collections::BTreeMap::new(),
        published_batches_by_node: std::collections::BTreeMap::new(),
        published_events_by_node: std::collections::BTreeMap::new(),
        published_control_events_by_node: std::collections::BTreeMap::new(),
        published_data_events_by_node: std::collections::BTreeMap::new(),
        last_published_at_us_by_node: std::collections::BTreeMap::new(),
        last_published_origins_by_node: std::collections::BTreeMap::new(),
        published_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        pending_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        yielded_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        summarized_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_origin_counts_by_node: std::collections::BTreeMap::new(),
    }) {
        return std::collections::BTreeMap::new();
    }
    let recovered_generation = status.current_stream_generation.unwrap_or_else(|| {
        status
            .concrete_roots
            .iter()
            .filter_map(|entry| entry.current_stream_generation)
            .max()
            .unwrap_or_default()
    });
    let mut signals = Vec::<String>::new();
    if let Some(groups) = scheduled_source_groups_by_node.get(stable_host_ref) {
        if !groups.is_empty() {
            signals.push(format!(
                "recovered_active_state unit=runtime.exec.source generation={} groups={:?}",
                recovered_generation, groups
            ));
        }
    }
    if let Some(groups) = scheduled_scan_groups_by_node.get(stable_host_ref) {
        if !groups.is_empty() {
            signals.push(format!(
                "recovered_active_state unit=runtime.exec.scan generation={} groups={:?}",
                recovered_generation, groups
            ));
        }
    }
    if signals.is_empty() {
        std::collections::BTreeMap::new()
    } else {
        std::collections::BTreeMap::from([(stable_host_ref.to_string(), signals)])
    }
}

fn recovered_published_observability_from_active_status(
    stable_host_ref: &str,
    status: &SourceStatusSnapshot,
) -> RecoveredPublishedObservability {
    let mut published_batch_count = 0u64;
    let mut published_event_count = 0u64;
    let mut published_control_event_count = 0u64;
    let mut published_data_event_count = 0u64;
    let mut last_published_at_us = None::<u64>;
    let mut last_published_origins = std::collections::BTreeSet::<String>::new();
    let mut published_origin_counts = std::collections::BTreeMap::<String, u64>::new();
    let mut path_origin_counts = std::collections::BTreeMap::<String, u64>::new();
    let mut published_path_capture_target = None::<String>;

    for entry in status
        .concrete_roots
        .iter()
        .filter(|entry| source_status_entry_looks_active_for_local_observability(entry))
    {
        published_batch_count =
            published_batch_count.saturating_add(entry.forwarded_batch_count);
        published_event_count =
            published_event_count.saturating_add(entry.forwarded_event_count);
        published_control_event_count = published_control_event_count
            .saturating_add(entry.emitted_control_event_count);
        published_data_event_count =
            published_data_event_count.saturating_add(entry.emitted_data_event_count);
        last_published_at_us = last_published_at_us.max(
            entry
                .last_forwarded_at_us
                .or(entry.last_emitted_at_us),
        );
        for origin in entry
            .last_forwarded_origins
            .iter()
            .chain(entry.last_emitted_origins.iter())
        {
            last_published_origins.insert(origin.clone());
        }
        if entry.forwarded_event_count > 0 {
            *published_origin_counts
                .entry(entry.object_ref.clone())
                .or_default() += entry.forwarded_event_count;
        }
        if entry.forwarded_path_event_count > 0 {
            *path_origin_counts
                .entry(entry.object_ref.clone())
                .or_default() += entry.forwarded_path_event_count;
        }
        if published_path_capture_target.is_none() {
            published_path_capture_target = entry.emitted_path_capture_target.clone();
        }
    }

    let published_origin_counts = (!published_origin_counts.is_empty())
        .then(|| {
            std::collections::BTreeMap::from([(
                stable_host_ref.to_string(),
                published_origin_counts
                    .iter()
                    .map(|(origin, count)| format!("{origin}={count}"))
                    .collect::<Vec<_>>(),
            )])
        })
        .unwrap_or_default();
    let path_origin_counts = (!path_origin_counts.is_empty())
        .then(|| {
            std::collections::BTreeMap::from([(
                stable_host_ref.to_string(),
                path_origin_counts
                    .iter()
                    .map(|(origin, count)| format!("{origin}={count}"))
                    .collect::<Vec<_>>(),
            )])
        })
        .unwrap_or_default();

    RecoveredPublishedObservability {
        published_batches_by_node: (published_batch_count > 0)
            .then(|| {
                std::collections::BTreeMap::from([(
                    stable_host_ref.to_string(),
                    published_batch_count,
                )])
            })
            .unwrap_or_default(),
        published_events_by_node: (published_event_count > 0)
            .then(|| {
                std::collections::BTreeMap::from([(
                    stable_host_ref.to_string(),
                    published_event_count,
                )])
            })
            .unwrap_or_default(),
        published_control_events_by_node: (published_control_event_count > 0)
            .then(|| {
                std::collections::BTreeMap::from([(
                    stable_host_ref.to_string(),
                    published_control_event_count,
                )])
            })
            .unwrap_or_default(),
        published_data_events_by_node: (published_data_event_count > 0)
            .then(|| {
                std::collections::BTreeMap::from([(
                    stable_host_ref.to_string(),
                    published_data_event_count,
                )])
            })
            .unwrap_or_default(),
        last_published_at_us_by_node: last_published_at_us
            .map(|ts| std::collections::BTreeMap::from([(stable_host_ref.to_string(), ts)]))
            .unwrap_or_default(),
        last_published_origins_by_node: (!last_published_origins.is_empty())
            .then(|| {
                std::collections::BTreeMap::from([(
                    stable_host_ref.to_string(),
                    last_published_origins.into_iter().collect::<Vec<_>>(),
                )])
            })
            .unwrap_or_default(),
        published_origin_counts_by_node: published_origin_counts,
        published_path_capture_target,
        summarized_path_origin_counts_by_node: path_origin_counts.clone(),
        published_path_origin_counts_by_node: path_origin_counts,
    }
}

fn build_local_source_observability_snapshot(source: &FSMetaSource) -> SourceObservabilitySnapshot {
    let node_id = source.node_id();
    let grants = source.host_object_grants_snapshot();
    let stable_host_ref = stable_host_ref_for_node_id(&node_id, &grants);
    let status = source.status_snapshot();
    let scheduled_source_groups_by_node = {
        let explicit =
            scheduled_groups_by_node(&node_id, &grants, source.scheduled_source_group_ids());
        if explicit.is_empty() {
            recovered_scheduled_groups_by_node_from_active_status(
                &stable_host_ref,
                &status,
                |entry| entry.watch_enabled,
            )
        } else {
            explicit
        }
    };
    let scheduled_scan_groups_by_node = {
        let explicit =
            scheduled_groups_by_node(&node_id, &grants, source.scheduled_scan_group_ids());
        if explicit.is_empty() {
            recovered_scheduled_groups_by_node_from_active_status(
                &stable_host_ref,
                &status,
                |entry| entry.scan_enabled,
            )
        } else {
            explicit
        }
    };
    let last_control_frame_signals_by_node = {
        let explicit = control_signals_by_node(
            &stable_host_ref,
            &source.last_control_frame_signals_snapshot(),
        );
        if explicit.is_empty() {
            recovered_control_signals_by_node_from_active_status(
                &stable_host_ref,
                &status,
                &scheduled_source_groups_by_node,
                &scheduled_scan_groups_by_node,
            )
        } else {
            explicit
        }
    };
    let published = recovered_published_observability_from_active_status(&stable_host_ref, &status);

    SourceObservabilitySnapshot {
        lifecycle_state: format!("{:?}", source.state()).to_ascii_lowercase(),
        host_object_grants_version: source.host_object_grants_version_snapshot(),
        grants,
        logical_roots: source.logical_roots_snapshot(),
        status,
        source_primary_by_group: source.source_primary_by_group_snapshot(),
        last_force_find_runner_by_group: source.last_force_find_runner_by_group_snapshot(),
        force_find_inflight_groups: source.force_find_inflight_groups_snapshot(),
        scheduled_source_groups_by_node,
        scheduled_scan_groups_by_node,
        last_control_frame_signals_by_node,
        published_batches_by_node: published.published_batches_by_node,
        published_events_by_node: published.published_events_by_node,
        published_control_events_by_node: published.published_control_events_by_node,
        published_data_events_by_node: published.published_data_events_by_node,
        last_published_at_us_by_node: published.last_published_at_us_by_node,
        last_published_origins_by_node: published.last_published_origins_by_node,
        published_origin_counts_by_node: published.published_origin_counts_by_node,
        published_path_capture_target: published.published_path_capture_target,
        enqueued_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        pending_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        yielded_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        summarized_path_origin_counts_by_node: published.summarized_path_origin_counts_by_node,
        published_path_origin_counts_by_node: published.published_path_origin_counts_by_node,
    }
}

fn build_cached_worker_observability_snapshot(
    cache: &SourceWorkerSnapshotCache,
) -> Option<SourceObservabilitySnapshot> {
    Some(SourceObservabilitySnapshot {
        lifecycle_state: cache.lifecycle_state.clone()?,
        host_object_grants_version: cache.host_object_grants_version.unwrap_or_default(),
        grants: cache.grants.clone().unwrap_or_default(),
        logical_roots: cache.logical_roots.clone().unwrap_or_default(),
        status: cache.status.clone().unwrap_or_default(),
        source_primary_by_group: cache.source_primary_by_group.clone().unwrap_or_default(),
        last_force_find_runner_by_group: cache
            .last_force_find_runner_by_group
            .clone()
            .unwrap_or_default(),
        force_find_inflight_groups: cache.force_find_inflight_groups.clone().unwrap_or_default(),
        scheduled_source_groups_by_node: cache
            .scheduled_source_groups_by_node
            .clone()
            .unwrap_or_default(),
        scheduled_scan_groups_by_node: cache
            .scheduled_scan_groups_by_node
            .clone()
            .unwrap_or_default(),
        last_control_frame_signals_by_node: cache
            .last_control_frame_signals_by_node
            .clone()
            .unwrap_or_default(),
        published_batches_by_node: cache.published_batches_by_node.clone().unwrap_or_default(),
        published_events_by_node: cache.published_events_by_node.clone().unwrap_or_default(),
        published_control_events_by_node: cache
            .published_control_events_by_node
            .clone()
            .unwrap_or_default(),
        published_data_events_by_node: cache
            .published_data_events_by_node
            .clone()
            .unwrap_or_default(),
        last_published_at_us_by_node: cache
            .last_published_at_us_by_node
            .clone()
            .unwrap_or_default(),
        last_published_origins_by_node: cache
            .last_published_origins_by_node
            .clone()
            .unwrap_or_default(),
        published_origin_counts_by_node: cache
            .published_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        published_path_capture_target: cache.published_path_capture_target.clone(),
        enqueued_path_origin_counts_by_node: cache
            .enqueued_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        pending_path_origin_counts_by_node: cache
            .pending_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        yielded_path_origin_counts_by_node: cache
            .yielded_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        summarized_path_origin_counts_by_node: cache
            .summarized_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        published_path_origin_counts_by_node: cache
            .published_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
    })
}

fn build_degraded_worker_observability_snapshot(
    cache: &SourceWorkerSnapshotCache,
    reason: impl Into<String>,
) -> SourceObservabilitySnapshot {
    let reason = reason.into();
    let mut status = cache.status.clone().unwrap_or_default();
    status
        .degraded_roots
        .push((SOURCE_WORKER_DEGRADED_ROOT_KEY.to_string(), reason));
    let mut snapshot = SourceObservabilitySnapshot {
        lifecycle_state: SOURCE_WORKER_DEGRADED_STATE.to_string(),
        host_object_grants_version: cache.host_object_grants_version.unwrap_or_default(),
        grants: cache.grants.clone().unwrap_or_default(),
        logical_roots: cache.logical_roots.clone().unwrap_or_default(),
        status,
        source_primary_by_group: cache.source_primary_by_group.clone().unwrap_or_default(),
        last_force_find_runner_by_group: cache
            .last_force_find_runner_by_group
            .clone()
            .unwrap_or_default(),
        force_find_inflight_groups: cache.force_find_inflight_groups.clone().unwrap_or_default(),
        scheduled_source_groups_by_node: cache
            .scheduled_source_groups_by_node
            .clone()
            .unwrap_or_default(),
        scheduled_scan_groups_by_node: cache
            .scheduled_scan_groups_by_node
            .clone()
            .unwrap_or_default(),
        last_control_frame_signals_by_node: cache
            .last_control_frame_signals_by_node
            .clone()
            .unwrap_or_default(),
        published_batches_by_node: cache.published_batches_by_node.clone().unwrap_or_default(),
        published_events_by_node: cache.published_events_by_node.clone().unwrap_or_default(),
        published_control_events_by_node: cache
            .published_control_events_by_node
            .clone()
            .unwrap_or_default(),
        published_data_events_by_node: cache
            .published_data_events_by_node
            .clone()
            .unwrap_or_default(),
        last_published_at_us_by_node: cache
            .last_published_at_us_by_node
            .clone()
            .unwrap_or_default(),
        last_published_origins_by_node: cache
            .last_published_origins_by_node
            .clone()
            .unwrap_or_default(),
        published_origin_counts_by_node: cache
            .published_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        published_path_capture_target: cache.published_path_capture_target.clone(),
        enqueued_path_origin_counts_by_node: cache
            .enqueued_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        pending_path_origin_counts_by_node: cache
            .pending_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        yielded_path_origin_counts_by_node: cache
            .yielded_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        summarized_path_origin_counts_by_node: cache
            .summarized_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        published_path_origin_counts_by_node: cache
            .published_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
    };
    fail_close_incomplete_active_source_observability_snapshot(&mut snapshot);
    snapshot
}

#[derive(Clone)]
pub enum SourceFacade {
    Local(Arc<FSMetaSource>),
    Worker(Arc<SourceWorkerClientHandle>),
}

impl SourceFacade {
    pub fn local(source: Arc<FSMetaSource>) -> Self {
        Self::Local(source)
    }

    pub fn worker(client: Arc<SourceWorkerClientHandle>) -> Self {
        Self::Worker(client)
    }

    pub fn is_worker(&self) -> bool {
        matches!(self, Self::Worker(_))
    }

    pub async fn start(
        &self,
        sink: Arc<SinkFacade>,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> Result<Option<JoinHandle<()>>> {
        match self {
            Self::Local(source) => {
                let stream = source.pub_().await?;
                Ok(Some(spawn_local_source_pump(stream, sink, boundary)))
            }
            Self::Worker(client) => {
                client.start().await?;
                Ok(None)
            }
        }
    }

    pub(crate) async fn apply_orchestration_signals(
        &self,
        signals: &[SourceControlSignal],
    ) -> Result<()> {
        match self {
            Self::Local(source) => source.apply_orchestration_signals(signals).await,
            Self::Worker(client) => {
                let envelopes = signals
                    .iter()
                    .map(SourceControlSignal::envelope)
                    .collect::<Vec<_>>();
                client.on_control_frame(envelopes).await
            }
        }
    }

    pub(crate) async fn apply_orchestration_signals_with_total_timeout(
        &self,
        signals: &[SourceControlSignal],
        total_timeout: Duration,
    ) -> Result<()> {
        if total_timeout.is_zero() {
            return Err(CnxError::Timeout);
        }
        // runtime_app owns the outer recovery loop for local source recovery.
        // Keep each nested source-client control attempt short so retryable resets
        // return to the caller instead of burning the full nested client budget.
        let single_attempt_total_timeout = std::cmp::min(
            total_timeout,
            SOURCE_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT,
        );
        match self {
            Self::Local(source) => {
                match tokio::time::timeout(
                    single_attempt_total_timeout,
                    source.apply_orchestration_signals(signals),
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
                    .map(SourceControlSignal::envelope)
                    .collect::<Vec<_>>();
                let rpc_timeout = std::cmp::min(
                    SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
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

    pub async fn update_logical_roots(&self, roots: Vec<RootSpec>) -> Result<()> {
        match self {
            Self::Local(source) => source.update_logical_roots(roots).await,
            Self::Worker(client) => client.update_logical_roots(roots).await,
        }
    }

    pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        match self {
            Self::Local(source) => Ok(source.logical_roots_snapshot()),
            Self::Worker(client) => client.cached_logical_roots_snapshot(),
        }
    }

    pub async fn logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        match self {
            Self::Local(source) => Ok(source.logical_roots_snapshot()),
            Self::Worker(client) => client.logical_roots_snapshot().await,
        }
    }

    pub fn cached_host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        match self {
            Self::Local(source) => Ok(source.host_object_grants_snapshot()),
            Self::Worker(client) => client.cached_host_object_grants_snapshot(),
        }
    }

    pub async fn host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        match self {
            Self::Local(source) => Ok(source.host_object_grants_snapshot()),
            Self::Worker(client) => client.host_object_grants_snapshot().await,
        }
    }

    pub async fn host_object_grants_version_snapshot(&self) -> Result<u64> {
        match self {
            Self::Local(source) => Ok(source.host_object_grants_version_snapshot()),
            Self::Worker(client) => client.host_object_grants_version_snapshot().await,
        }
    }

    pub async fn status_snapshot(&self) -> Result<SourceStatusSnapshot> {
        match self {
            Self::Local(source) => Ok(source.status_snapshot()),
            Self::Worker(client) => client.status_snapshot().await,
        }
    }

    pub async fn lifecycle_state_label(&self) -> Result<String> {
        match self {
            Self::Local(source) => Ok(format!("{:?}", source.state()).to_ascii_lowercase()),
            Self::Worker(client) => client.lifecycle_state_label().await,
        }
    }

    pub async fn scheduled_source_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self {
            Self::Local(source) => source.scheduled_source_group_ids(),
            Self::Worker(client) => client.scheduled_source_group_ids().await,
        }
    }

    pub async fn scheduled_scan_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        match self {
            Self::Local(source) => source.scheduled_scan_group_ids(),
            Self::Worker(client) => client.scheduled_scan_group_ids().await,
        }
    }

    pub async fn source_primary_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        match self {
            Self::Local(source) => Ok(source.source_primary_by_group_snapshot()),
            Self::Worker(client) => client.source_primary_by_group_snapshot().await,
        }
    }

    pub async fn last_force_find_runner_by_group_snapshot(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        match self {
            Self::Local(source) => Ok(source.last_force_find_runner_by_group_snapshot()),
            Self::Worker(client) => client.last_force_find_runner_by_group_snapshot().await,
        }
    }

    pub async fn force_find_inflight_groups_snapshot(&self) -> Result<Vec<String>> {
        match self {
            Self::Local(source) => Ok(source.force_find_inflight_groups_snapshot()),
            Self::Worker(client) => client.force_find_inflight_groups_snapshot().await,
        }
    }

    pub async fn resolve_group_id_for_object_ref(
        &self,
        object_ref: &str,
    ) -> Result<Option<String>> {
        match self {
            Self::Local(source) => Ok(source.resolve_group_id_for_object_ref(object_ref)),
            Self::Worker(client) => client.resolve_group_id_for_object_ref(object_ref).await,
        }
    }

    pub async fn force_find(&self, params: &InternalQueryRequest) -> Result<Vec<Event>> {
        match self {
            Self::Local(source) => source.force_find(params),
            Self::Worker(client) => client.force_find(params.clone()).await,
        }
    }

    pub async fn force_find_via_node(
        &self,
        node_id: &NodeId,
        params: &InternalQueryRequest,
    ) -> Result<Vec<Event>> {
        match self {
            Self::Local(source) => source.force_find(params),
            Self::Worker(client) => {
                if client.node_id == *node_id {
                    client.force_find(params.clone()).await
                } else {
                    let remote = SourceWorkerClientHandle::new(
                        node_id.clone(),
                        client.config.clone(),
                        client.worker_binding.clone(),
                        client.worker_factory.clone(),
                    )?;
                    remote.force_find(params.clone()).await
                }
            }
        }
    }

    pub async fn publish_manual_rescan_signal(&self) -> Result<()> {
        match self {
            Self::Local(source) => source.publish_manual_rescan_signal().await,
            Self::Worker(client) => client.publish_manual_rescan_signal().await,
        }
    }

    pub async fn trigger_rescan_when_ready(&self) -> Result<()> {
        #[cfg(test)]
        record_source_worker_trigger_rescan_when_ready_attempt();
        match self {
            Self::Local(source) => {
                source.trigger_rescan_when_ready().await;
                Ok(())
            }
            Self::Worker(client) => client.trigger_rescan_when_ready().await,
        }
    }

    pub(crate) async fn reconnect_after_fail_closed_control_error(&self) -> Result<()> {
        match self {
            Self::Local(_) => Ok(()),
            Self::Worker(client) => client.reconnect_after_fail_closed_control_error().await,
        }
    }

    pub(crate) async fn reconnect_after_retryable_control_reset(&self) -> Result<()> {
        match self {
            Self::Local(_) => Ok(()),
            Self::Worker(client) => client.reconnect_after_retryable_control_reset().await,
        }
    }

    pub async fn close(&self) -> Result<()> {
        match self {
            Self::Local(source) => source.close().await,
            Self::Worker(client) => client.close().await,
        }
    }

    pub(crate) async fn wait_for_control_ops_to_drain_for_handoff(&self) {
        if let Self::Worker(client) = self {
            client
                .wait_for_control_ops_to_drain(SOURCE_WORKER_CLOSE_DRAIN_TIMEOUT)
                .await;
        }
    }

    pub(crate) async fn observability_snapshot(&self) -> Result<SourceObservabilitySnapshot> {
        match self {
            Self::Local(source) => Ok(build_local_source_observability_snapshot(source)),
            Self::Worker(client) => {
                let worker_client = client.client().await?;
                client
                    .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                    .await
            }
        }
    }

    pub(crate) async fn observability_snapshot_nonblocking(&self) -> SourceObservabilitySnapshot {
        match self {
            Self::Local(source) => build_local_source_observability_snapshot(source),
            Self::Worker(client) => client.observability_snapshot_nonblocking().await,
        }
    }
}

fn spawn_local_source_pump(
    stream: std::pin::Pin<Box<dyn futures_core::Stream<Item = Vec<Event>> + Send>>,
    sink: Arc<SinkFacade>,
    boundary: Option<Arc<dyn ChannelIoSubset>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Some(boundary) = boundary {
            tokio::pin!(stream);
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
                        "fs-meta app pump failed to publish source batch on stream route origin={}: {:?}",
                        origin,
                        err
                    );
                    break;
                }
            }
        } else {
            tokio::pin!(stream);
            while let Some(batch) = stream.next().await {
                if let Err(err) = sink.send(&batch).await {
                    log::error!("fs-meta app pump failed to apply batch: {:?}", err);
                }
            }
        }
    })
}

capanix_runtime_entry_sdk::worker_runtime::define_typed_worker_rpc! {
    pub struct SourceWorkerRpc {
        request: SourceWorkerRequest,
        response: SourceWorkerResponse,
        encode_request: encode_request,
        decode_request: decode_request,
        encode_response: encode_response,
        decode_response: decode_response,
        invalid_input: SourceWorkerResponse::InvalidInput,
        error: SourceWorkerResponse::Error,
        unavailable: "source worker unavailable",
    }
}

impl TypedWorkerInit<SourceConfig> for SourceWorkerRpc {
    type InitPayload = SourceConfig;

    fn init_payload(_node_id: &NodeId, config: &SourceConfig) -> Result<Self::InitPayload> {
        Ok(config.clone())
    }
}

#[cfg(test)]
mod tests;
