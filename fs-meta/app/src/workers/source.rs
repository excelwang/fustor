use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use capanix_app_sdk::runtime::{ControlEnvelope, NodeId, RuntimeWorkerBinding};
use capanix_app_sdk::{CnxError, Event, Result};
use capanix_runtime_entry_sdk::advanced::boundary::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelSendRequest,
};
use capanix_runtime_entry_sdk::control::RuntimeHostGrantState;
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
use crate::workers::sink::SinkFacade;
use crate::workers::source_ipc::{
    SourceWorkerRequest, SourceWorkerResponse, decode_request, decode_response, encode_request,
    encode_response,
};

const SOURCE_WORKER_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(15);
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
const SOURCE_WORKER_DEGRADED_STATE: &str = "degraded_worker_unreachable";
const SOURCE_WORKER_DEGRADED_ROOT_KEY: &str = "source-worker";

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

fn can_retry_update_logical_roots(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::PeerError(message) if message == "worker not initialized"
    ) || matches!(err, CnxError::TransportClosed(_) | CnxError::Timeout)
        || is_retryable_worker_bridge_peer_error(err)
        || matches!(
            err,
            CnxError::AccessDenied(message) | CnxError::PeerError(message)
                if message.contains("drained/fenced")
                    && message.contains("grant attachments")
        )
}

fn can_retry_on_control_frame(err: &CnxError) -> bool {
    can_retry_update_logical_roots(err)
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
        1 => host_refs.into_iter().next().unwrap_or_else(|| node_id.0.clone()),
        _ => node_id.0.clone(),
    }
}

fn host_ref_from_resource_id(resource_id: &str) -> Option<&str> {
    resource_id.split_once("::").map(|(host_ref, _)| host_ref)
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
            Some(existing) if existing.is_empty() && !groups.is_empty() => {
                *existing = groups.clone();
            }
            None => {
                current.insert(node_id.clone(), groups.clone());
            }
            _ => {}
        }
    }
}

fn update_cached_scheduled_groups_from_refresh(
    cache: &mut SourceWorkerSnapshotCache,
    mut scheduled_source: std::collections::BTreeMap<String, Vec<String>>,
    mut scheduled_scan: std::collections::BTreeMap<String, Vec<String>>,
) {
    merge_non_empty_cached_groups(&mut scheduled_source, &cache.scheduled_source_groups_by_node);
    merge_non_empty_cached_groups(&mut scheduled_scan, &cache.scheduled_scan_groups_by_node);
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

fn prime_cached_schedule_from_control_signals(
    cache: &mut SourceWorkerSnapshotCache,
    node_id: &NodeId,
    signals: &[SourceControlSignal],
    fallback_grants: &[GrantedMountRoot],
) {
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
    for signal in signals {
        let (unit, bound_scopes) = match signal {
            SourceControlSignal::Activate {
                unit,
                bound_scopes,
                ..
            } => (Some(*unit), Some(bound_scopes.as_slice())),
            _ => (None, None),
        };
        let Some(unit) = unit else { continue };
        let Some(bound_scopes) = bound_scopes else {
            continue;
        };
        for scope in bound_scopes {
            let applies_locally = scope.resource_ids.iter().any(|resource_id| {
                grants.iter().any(|grant| {
                    host_ref_matches_node_id(&grant.host_ref, node_id)
                        && grant.object_ref == *resource_id
                }) || host_ref_from_resource_id(resource_id)
                    .is_some_and(|host_ref| host_ref_matches_node_id(host_ref, node_id))
            });
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
    if !scheduled_source.is_empty() {
        cache.scheduled_source_groups_by_node = Some(std::collections::BTreeMap::from([(
            stable_host_ref.clone(),
            scheduled_source.into_iter().collect::<Vec<_>>(),
        )]));
    }
    if !scheduled_scan.is_empty() {
        cache.scheduled_scan_groups_by_node = Some(std::collections::BTreeMap::from([(
            stable_host_ref,
            scheduled_scan.into_iter().collect::<Vec<_>>(),
        )]));
    }
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
    cache: Arc<Mutex<SourceWorkerSnapshotCache>>,
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
    start_serial: Arc<tokio::sync::Mutex<()>>,
    control_ops_inflight: Arc<AtomicUsize>,
    control_ops_serial: Arc<tokio::sync::Mutex<()>>,
}

struct SharedSourceWorkerClient {
    instance_id: u64,
    client: Arc<TypedRuntimeWorkerClient<SourceWorkerRpc, SourceConfig>>,
}

fn next_shared_source_worker_instance_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn source_worker_handle_registry_key(
    node_id: &NodeId,
    worker_binding: &RuntimeWorkerBinding,
) -> String {
    format!(
        "{}|{}|{:?}|{:?}",
        node_id.0,
        worker_binding.role_id,
        worker_binding.mode,
        worker_binding.launcher_kind
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
pub(crate) struct SourceWorkerControlFrameErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
pub(crate) struct SourceWorkerControlFrameErrorQueueHook {
    pub errs: std::collections::VecDeque<CnxError>,
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
fn source_worker_control_frame_pause_hook_cell(
) -> &'static Mutex<Option<SourceWorkerControlFramePauseHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerControlFramePauseHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_update_roots_error_hook_cell(
) -> &'static Mutex<Option<SourceWorkerUpdateRootsErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerUpdateRootsErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_control_frame_error_hook_cell(
) -> &'static Mutex<Option<SourceWorkerControlFrameErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerControlFrameErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_control_frame_error_queue_hook_cell(
) -> &'static Mutex<Option<SourceWorkerControlFrameErrorQueueHook>> {
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
fn source_worker_observability_error_hook_cell(
) -> &'static Mutex<Option<SourceWorkerObservabilityErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerObservabilityErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_start_error_queue_hook_cell(
) -> &'static Mutex<Option<SourceWorkerStartErrorQueueHook>> {
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
fn source_worker_scheduled_groups_refresh_queue_hook_cell(
) -> &'static Mutex<Option<SourceWorkerScheduledGroupsRefreshQueueHook>> {
    static CELL: std::sync::OnceLock<
        Mutex<Option<SourceWorkerScheduledGroupsRefreshQueueHook>>,
    > = std::sync::OnceLock::new();
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
fn take_source_worker_observability_error_hook() -> Option<CnxError> {
    let mut guard = match source_worker_observability_error_hook_cell().lock() {
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
fn take_source_worker_scheduled_groups_refresh_queue_hook(
) -> Option<(
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
        hook.entered.notify_waiters();
        hook.release.notified().await;
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
        hook.entered.notify_waiters();
        hook.release.notified().await;
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
        hook.entered.notify_waiters();
        hook.release.notified().await;
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
fn take_on_control_frame_error_hook() -> Option<CnxError> {
    {
        let mut guard = match source_worker_control_frame_error_queue_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(hook) = guard.as_mut()
            && let Some(err) = hook.errs.pop_front()
        {
            if hook.errs.is_empty() {
                *guard = None;
            }
            return Some(err);
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
        let key = source_worker_handle_registry_key(&node_id, &worker_binding);
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
                    start_serial: Arc::new(tokio::sync::Mutex::new(())),
                    control_ops_inflight: Arc::new(AtomicUsize::new(0)),
                    control_ops_serial: Arc::new(tokio::sync::Mutex::new(())),
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
            cache: shared.cache.clone(),
            start_serial: shared.start_serial.clone(),
            control_ops_inflight: shared.control_ops_inflight.clone(),
            control_ops_serial: shared.control_ops_serial.clone(),
            config,
        })
    }

    async fn worker_client(&self) -> Arc<TypedRuntimeWorkerClient<SourceWorkerRpc, SourceConfig>> {
        self._shared.worker.lock().await.client.clone()
    }

    #[cfg(test)]
    async fn worker_instance_id_for_tests(&self) -> u64 {
        self._shared.worker.lock().await.instance_id
    }

    async fn reconnect_shared_worker_client(&self) -> Result<()> {
        let replacement = SharedSourceWorkerClient {
            instance_id: next_shared_source_worker_instance_id(),
            client: Arc::new(self.worker_factory.connect(
                self.node_id.clone(),
                self.config.clone(),
                self.worker_binding.clone(),
            )?),
        };
        *self._shared.worker.lock().await = replacement;
        Ok(())
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
            cache.lifecycle_state = Some(snapshot.lifecycle_state.clone());
            cache.host_object_grants_version = Some(snapshot.host_object_grants_version);
            cache.grants = Some(snapshot.grants.clone());
            cache.logical_roots = Some(snapshot.logical_roots.clone());
            cache.status = Some(snapshot.status.clone());
            cache.source_primary_by_group = Some(snapshot.source_primary_by_group.clone());
            cache.last_force_find_runner_by_group =
                Some(snapshot.last_force_find_runner_by_group.clone());
            cache.force_find_inflight_groups = Some(snapshot.force_find_inflight_groups.clone());
            cache.scheduled_source_groups_by_node =
                Some(snapshot.scheduled_source_groups_by_node.clone());
            cache.scheduled_scan_groups_by_node =
                Some(snapshot.scheduled_scan_groups_by_node.clone());
            cache.last_control_frame_signals_by_node =
                Some(snapshot.last_control_frame_signals_by_node.clone());
            cache.published_batches_by_node = Some(snapshot.published_batches_by_node.clone());
            cache.published_events_by_node = Some(snapshot.published_events_by_node.clone());
            cache.published_control_events_by_node =
                Some(snapshot.published_control_events_by_node.clone());
            cache.published_data_events_by_node =
                Some(snapshot.published_data_events_by_node.clone());
            cache.last_published_at_us_by_node =
                Some(snapshot.last_published_at_us_by_node.clone());
            cache.last_published_origins_by_node =
                Some(snapshot.last_published_origins_by_node.clone());
            cache.published_origin_counts_by_node =
                Some(snapshot.published_origin_counts_by_node.clone());
            cache.published_path_capture_target = snapshot.published_path_capture_target.clone();
            cache.enqueued_path_origin_counts_by_node =
                Some(snapshot.enqueued_path_origin_counts_by_node.clone());
            cache.pending_path_origin_counts_by_node =
                Some(snapshot.pending_path_origin_counts_by_node.clone());
            cache.yielded_path_origin_counts_by_node =
                Some(snapshot.yielded_path_origin_counts_by_node.clone());
            cache.summarized_path_origin_counts_by_node =
                Some(snapshot.summarized_path_origin_counts_by_node.clone());
            cache.published_path_origin_counts_by_node =
                Some(snapshot.published_path_origin_counts_by_node.clone());
        });
    }

    async fn refresh_cached_scheduled_groups_from_live_worker(&self) -> Result<()> {
        let client = self.client().await?;
        let grants = match Self::call_worker(
            &client,
            SourceWorkerRequest::HostObjectGrantsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
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
                return Err(CnxError::ProtocolViolation(format!(
                    "unexpected source worker response for scheduled groups host grants refresh: {:?}",
                    other
                )))
            }
            Err(err) if can_use_cached_host_object_grants_snapshot(&err) => {
                self.cached_host_object_grants_snapshot()?
            }
            Err(err) => return Err(err),
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
        let deadline = std::time::Instant::now() + SOURCE_WORKER_SCHEDULE_REFRESH_TOTAL_TIMEOUT;
        loop {
            let (scheduled_source, scheduled_scan) = self
                .fetch_scheduled_groups_refresh_attempt(&client, &stable_host_ref)
                .await?;
            let cache_empty = self.with_cache_mut(|cache| {
                cache.scheduled_source_groups_by_node
                    .as_ref()
                    .is_none_or(|groups| groups.is_empty())
                    && cache
                        .scheduled_scan_groups_by_node
                        .as_ref()
                        .is_none_or(|groups| groups.is_empty())
            });
            if scheduled_source
                .values()
                .all(|groups| groups.is_empty())
                && scheduled_scan.values().all(|groups| groups.is_empty())
                && cache_empty
                && std::time::Instant::now() < deadline
            {
                tokio::time::sleep(SOURCE_WORKER_RETRY_BACKOFF).await;
                continue;
            }
            self.with_cache_mut(|cache| {
                update_cached_scheduled_groups_from_refresh(
                    cache,
                    scheduled_source,
                    scheduled_scan,
                );
            });
            return Ok(());
        }
    }

    async fn fetch_scheduled_groups_refresh_attempt(
        &self,
        client: &TypedWorkerClient<SourceWorkerRpc>,
        stable_host_ref: &str,
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
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
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
                )))
            }
        };
        let scheduled_scan = match Self::call_worker(
            client,
            SourceWorkerRequest::ScheduledScanGroupIds,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
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
                )))
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
            let injected =
                take_source_worker_start_error_queue_hook(self.worker_instance_id_for_tests().await);
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
                    if can_retry_on_control_frame(&err)
                        && std::time::Instant::now() < deadline =>
                {
                    let _ = worker.shutdown(Duration::from_secs(2)).await;
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
        let Some(client) = self.existing_client().await? else {
            return self.cached_logical_roots_snapshot();
        };
        let result = Self::call_worker(
            &client,
            SourceWorkerRequest::LogicalRootsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await;
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

    pub async fn scheduled_source_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::ScheduledSourceGroupIds,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::ScheduledGroupIds(groups) => {
                Ok(groups.map(|groups| groups.into_iter().collect()))
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for scheduled source groups: {:?}",
                other
            ))),
        }
    }

    pub async fn scheduled_scan_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        match Self::call_worker(
            &self.client().await?,
            SourceWorkerRequest::ScheduledScanGroupIds,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::ScheduledGroupIds(groups) => {
                Ok(groups.map(|groups| groups.into_iter().collect()))
            }
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected source worker response for scheduled scan groups: {:?}",
                other
            ))),
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
        self.with_started_retry(|client| {
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
                let response = Self::call_worker(
                    &client,
                    SourceWorkerRequest::ForceFind {
                        request: params.clone(),
                    },
                    SOURCE_WORKER_FORCE_FIND_TIMEOUT,
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
        .await
    }

    pub async fn publish_manual_rescan_signal(&self) -> Result<()> {
        let _inflight = self.begin_control_op();
        self.with_started_retry(|client| async move {
            match Self::call_worker(
                &client,
                SourceWorkerRequest::PublishManualRescanSignal,
                SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
            )
            .await?
            {
                SourceWorkerResponse::Ack => Ok(()),
                other => Err(CnxError::ProtocolViolation(format!(
                    "unexpected source worker response for publish_manual_rescan_signal: {:?}",
                    other
                ))),
            }
        })
        .await
    }

    pub async fn on_control_frame(&self, envelopes: Vec<ControlEnvelope>) -> Result<()> {
        let _inflight = self.begin_control_op();
        let _serial = self.control_ops_serial.lock().await;
        #[cfg(test)]
        notify_source_worker_control_frame_started();
        let decoded_signals = source_control_signals_from_envelopes(&envelopes).ok();
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
        let deadline = std::time::Instant::now() + SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT;
        loop {
            let now = std::time::Instant::now();
            let attempt_timeout = std::cmp::min(
                SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
                deadline.saturating_duration_since(now),
            );
            if attempt_timeout.is_zero() {
                return Err(CnxError::Timeout);
            }
            let rpc_result = self
                .with_started_retry(|client| {
                    let envelopes = envelopes.clone();
                    async move {
                        #[cfg(test)]
                        maybe_pause_before_on_control_frame_rpc().await;
                        #[cfg(test)]
                        if let Some(err) = take_on_control_frame_error_hook() {
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
                })
                .await;
            match rpc_result {
                Ok(SourceWorkerResponse::Ack) => {
                    if let Some(signals) = decoded_signals.as_ref() {
                        self.with_cache_mut(|cache| {
                            prime_cached_schedule_from_control_signals(
                                cache,
                                &self.node_id,
                                signals,
                                &self.config.host_object_grants,
                            );
                        });
                    }
                    self.refresh_cached_scheduled_groups_from_live_worker().await?;
                    eprintln!(
                        "fs_meta_source_worker_client: on_control_frame done node={} ok=true",
                        self.node_id.0
                    );
                    return Ok(());
                }
                Ok(other) => {
                    eprintln!(
                        "fs_meta_source_worker_client: on_control_frame done node={} ok=false",
                        self.node_id.0
                    );
                    return Err(CnxError::ProtocolViolation(format!(
                        "unexpected source worker response for on_control_frame: {:?}",
                        other
                    )));
                }
                Err(err)
                    if can_retry_on_control_frame(&err)
                        && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(SOURCE_WORKER_RETRY_BACKOFF).await;
                }
                Err(err) => {
                    eprintln!(
                        "fs_meta_source_worker_client: on_control_frame done node={} ok=false",
                        self.node_id.0
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

fn scheduled_groups_by_node(
    node_id: &NodeId,
    grants: &[GrantedMountRoot],
    groups: Result<Option<std::collections::BTreeSet<String>>>,
) -> std::collections::BTreeMap<String, Vec<String>> {
    let Ok(Some(groups)) = groups else {
        return std::collections::BTreeMap::new();
    };
    if groups.is_empty() {
        return std::collections::BTreeMap::new();
    }
    let stable_host_ref = stable_host_ref_for_node_id(node_id, grants);
    std::collections::BTreeMap::from([(stable_host_ref, groups.into_iter().collect::<Vec<_>>())])
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
    SourceObservabilitySnapshot {
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
    }
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
        match self {
            Self::Local(source) => {
                source.trigger_rescan_when_ready().await;
                Ok(())
            }
            Self::Worker(client) => client.trigger_rescan_when_ready().await,
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
            Self::Local(source) => Ok(SourceObservabilitySnapshot {
                lifecycle_state: format!("{:?}", source.state()).to_ascii_lowercase(),
                host_object_grants_version: source.host_object_grants_version_snapshot(),
                grants: source.host_object_grants_snapshot(),
                logical_roots: source.logical_roots_snapshot(),
                status: source.status_snapshot(),
                source_primary_by_group: source.source_primary_by_group_snapshot(),
                last_force_find_runner_by_group: source.last_force_find_runner_by_group_snapshot(),
                force_find_inflight_groups: source.force_find_inflight_groups_snapshot(),
                scheduled_source_groups_by_node: scheduled_groups_by_node(
                    &source.node_id(),
                    &source.host_object_grants_snapshot(),
                    source.scheduled_source_group_ids(),
                ),
                scheduled_scan_groups_by_node: scheduled_groups_by_node(
                    &source.node_id(),
                    &source.host_object_grants_snapshot(),
                    source.scheduled_scan_group_ids(),
                ),
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
            }),
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
            Self::Local(source) => SourceObservabilitySnapshot {
                lifecycle_state: format!("{:?}", source.state()).to_ascii_lowercase(),
                host_object_grants_version: source.host_object_grants_version_snapshot(),
                grants: source.host_object_grants_snapshot(),
                logical_roots: source.logical_roots_snapshot(),
                status: source.status_snapshot(),
                source_primary_by_group: source.source_primary_by_group_snapshot(),
                last_force_find_runner_by_group: source.last_force_find_runner_by_group_snapshot(),
                force_find_inflight_groups: source.force_find_inflight_groups_snapshot(),
                scheduled_source_groups_by_node: scheduled_groups_by_node(
                    &source.node_id(),
                    &source.host_object_grants_snapshot(),
                    source.scheduled_source_group_ids(),
                ),
                scheduled_scan_groups_by_node: scheduled_groups_by_node(
                    &source.node_id(),
                    &source.host_object_grants_snapshot(),
                    source.scheduled_scan_group_ids(),
                ),
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
            },
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
mod tests {
    use super::*;
    use async_trait::async_trait;
    use capanix_app_sdk::raw::{
        BoundaryContext, ChannelBoundary, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
        StateBoundary,
    };
    use capanix_app_sdk::runtime::{
        ControlFrame, LogLevel, RuntimeWorkerBinding, RuntimeWorkerLauncherKind,
        in_memory_state_boundary,
    };
    use capanix_app_sdk::worker::WorkerMode;
    use capanix_runtime_entry_sdk::control::{
        RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeHostDescriptor,
        RuntimeHostGrant, RuntimeHostGrantChange, RuntimeHostGrantState, RuntimeHostObjectType,
        RuntimeObjectDescriptor, encode_runtime_exec_control, encode_runtime_host_grant_change,
    };
    use capanix_runtime_entry_sdk::worker_runtime::{
        RuntimeWorkerClientFactory, TypedWorkerInit, TypedWorkerRpc,
    };
    use std::collections::{HashMap, HashSet};
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex as StdMutex, OnceLock};
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::{Mutex as AsyncMutex, Notify};

    use crate::ControlEvent;
    use crate::FileMetaRecord;
    use crate::query::models::QueryNode;
    use crate::query::path::is_under_query_path;
    use crate::query::path::root_file_name_bytes;
    use crate::query::request::{
        InternalQueryRequest, MaterializedQueryPayload, QueryOp, QueryScope,
    };
    use crate::runtime::execution_units::{
        SINK_RUNTIME_UNIT_ID, SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID,
    };
    use crate::runtime::routes::{
        ROUTE_KEY_QUERY, ROUTE_KEY_SOURCE_RESCAN_CONTROL, ROUTE_KEY_SOURCE_RESCAN_INTERNAL,
        ROUTE_KEY_SOURCE_ROOTS_CONTROL,
    };
    use crate::workers::sink::SinkWorkerClientHandle;
    #[cfg(target_os = "linux")]
    mod real_nfs_lab {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/runtime_app_real_nfs_lab.rs"
        ));
    }

    #[derive(Default)]
    struct LoopbackWorkerBoundary {
        channels: AsyncMutex<HashMap<String, Vec<Event>>>,
        closed: StdMutex<HashSet<String>>,
        changed: Notify,
    }

    struct SourceWorkerUpdateRootsHookReset;

    impl Drop for SourceWorkerUpdateRootsHookReset {
        fn drop(&mut self) {
            clear_source_worker_update_roots_hook();
        }
    }

    struct SourceWorkerUpdateRootsErrorHookReset;

    impl Drop for SourceWorkerUpdateRootsErrorHookReset {
        fn drop(&mut self) {
            clear_source_worker_update_roots_error_hook();
        }
    }

    struct SourceWorkerControlFrameErrorHookReset;

    impl Drop for SourceWorkerControlFrameErrorHookReset {
        fn drop(&mut self) {
            clear_source_worker_control_frame_error_hook();
        }
    }

    struct SourceWorkerStatusErrorHookReset;

    impl Drop for SourceWorkerStatusErrorHookReset {
        fn drop(&mut self) {
            clear_source_worker_status_error_hook();
        }
    }

    struct SourceWorkerObservabilityErrorHookReset;

    impl Drop for SourceWorkerObservabilityErrorHookReset {
        fn drop(&mut self) {
            clear_source_worker_observability_error_hook();
        }
    }

    struct SourceWorkerControlFramePauseHookReset;

    impl Drop for SourceWorkerControlFramePauseHookReset {
        fn drop(&mut self) {
            clear_source_worker_control_frame_pause_hook();
        }
    }

    struct SourceWorkerStartPauseHookReset;

    impl Drop for SourceWorkerStartPauseHookReset {
        fn drop(&mut self) {
            clear_source_worker_start_pause_hook();
        }
    }

    struct SourceWorkerStartErrorQueueHookReset;

    impl Drop for SourceWorkerStartErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_start_error_queue_hook();
        }
    }

    struct SourceWorkerStartDelayHookReset;

    impl Drop for SourceWorkerStartDelayHookReset {
        fn drop(&mut self) {
            clear_source_worker_start_delay_hook();
        }
    }

    #[async_trait]
    impl ChannelIoSubset for LoopbackWorkerBoundary {
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

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: ChannelRecvRequest,
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
                {
                    let closed = self.closed.lock().expect("loopback closed lock");
                    if closed.contains(&request.channel_key.0) {
                        return Err(CnxError::ChannelClosed);
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

        fn channel_close(&self, _ctx: BoundaryContext, channel: ChannelKey) -> Result<()> {
            self.closed
                .lock()
                .expect("loopback closed lock")
                .insert(channel.0);
            self.changed.notify_waiters();
            Ok(())
        }
    }

    impl ChannelBoundary for LoopbackWorkerBoundary {
        fn log(&self, _ctx: BoundaryContext, _level: LogLevel, _msg: &str) {}
    }

    impl StateBoundary for LoopbackWorkerBoundary {}

    const WORKER_BOOTSTRAP_CONTROL_FRAME_KIND: &str = "capanix.worker.bootstrap:v1";

    #[derive(Debug, Clone, serde::Serialize)]
    enum TestWorkerBootstrapEnvelope<P> {
        Init { node_id: String, payload: P },
    }

    fn bootstrap_envelope<P: serde::Serialize>(
        message: &TestWorkerBootstrapEnvelope<P>,
    ) -> ControlEnvelope {
        ControlEnvelope::Frame(ControlFrame {
            kind: WORKER_BOOTSTRAP_CONTROL_FRAME_KIND.to_string(),
            payload: rmp_serde::to_vec_named(message).expect("encode bootstrap envelope"),
        })
    }

    fn fs_meta_runtime_lib_filename() -> &'static str {
        #[cfg(target_os = "macos")]
        {
            "libfs_meta_runtime.dylib"
        }
        #[cfg(target_os = "windows")]
        {
            "fs_meta_runtime.dll"
        }
        #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
        {
            "libfs_meta_runtime.so"
        }
    }

    fn fs_meta_runtime_workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root")
            .to_path_buf()
    }

    fn fs_meta_worker_module_path() -> PathBuf {
        static BIN: OnceLock<PathBuf> = OnceLock::new();
        BIN.get_or_init(|| {
            for name in ["CAPANIX_FS_META_APP_BINARY", "DATANIX_FS_META_APP_SO"] {
                if let Ok(path) = std::env::var(name) {
                    let resolved = PathBuf::from(path);
                    if resolved.exists() {
                        return resolved;
                    }
                }
            }
            let root = fs_meta_runtime_workspace_root();
            let lib_name = fs_meta_runtime_lib_filename();
            for candidate in [
                root.join("target/debug").join(lib_name),
                root.join("target/debug/deps").join(lib_name),
                root.join(".target/debug").join(lib_name),
                root.join(".target/debug/deps").join(lib_name),
            ] {
                if candidate.exists() {
                    return candidate;
                }
            }
            panic!("fs-meta worker module not found; set CAPANIX_FS_META_APP_BINARY");
        })
        .clone()
    }

    fn external_source_worker_binding(socket_dir: &Path) -> RuntimeWorkerBinding {
        external_source_worker_binding_with_module_path(socket_dir, &fs_meta_worker_module_path())
    }

    fn external_source_worker_binding_with_module_path(
        socket_dir: &Path,
        module_path: &Path,
    ) -> RuntimeWorkerBinding {
        RuntimeWorkerBinding {
            role_id: "source".to_string(),
            mode: WorkerMode::External,
            launcher_kind: RuntimeWorkerLauncherKind::WorkerHost,
            module_path: Some(module_path.to_path_buf()),
            socket_dir: Some(socket_dir.to_path_buf()),
        }
    }

    fn external_sink_worker_binding(socket_dir: &Path) -> RuntimeWorkerBinding {
        RuntimeWorkerBinding {
            role_id: "sink".to_string(),
            mode: WorkerMode::External,
            launcher_kind: RuntimeWorkerLauncherKind::WorkerHost,
            module_path: Some(fs_meta_worker_module_path()),
            socket_dir: Some(socket_dir.to_path_buf()),
        }
    }

    fn worker_source_root(id: &str, path: &Path) -> RootSpec {
        let mut root = RootSpec::new(id, path);
        root.watch = false;
        root.scan = true;
        root
    }

    fn worker_watch_scan_root(id: &str, path: &Path) -> RootSpec {
        RootSpec::new(id, path)
    }

    fn worker_source_export(
        object_ref: &str,
        host_ref: &str,
        host_ip: &str,
        mount_point: PathBuf,
    ) -> GrantedMountRoot {
        GrantedMountRoot {
            object_ref: object_ref.to_string(),
            host_ref: host_ref.to_string(),
            host_ip: host_ip.to_string(),
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

    fn worker_route_export(
        object_ref: &str,
        host_ref: &str,
        host_ip: &str,
        mount_point: &Path,
    ) -> RuntimeHostGrant {
        RuntimeHostGrant {
            object_ref: object_ref.to_string(),
            object_type: RuntimeHostObjectType::MountRoot,
            interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
            host: RuntimeHostDescriptor {
                host_ref: host_ref.to_string(),
                host_ip: host_ip.to_string(),
                host_name: Some(host_ref.to_string()),
                site: None,
                zone: None,
                host_labels: Default::default(),
            },
            object: RuntimeObjectDescriptor {
                mount_point: mount_point.display().to_string(),
                fs_source: mount_point.display().to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
            },
            grant_state: RuntimeHostGrantState::Active,
        }
    }

    fn bound_scope_with_resources(scope_id: &str, resource_ids: &[&str]) -> RuntimeBoundScope {
        RuntimeBoundScope {
            scope_id: scope_id.to_string(),
            resource_ids: resource_ids.iter().map(|id| (*id).to_string()).collect(),
        }
    }

    fn selected_group_request(path: &[u8], group_id: &str) -> InternalQueryRequest {
        InternalQueryRequest::materialized(
            QueryOp::Tree,
            QueryScope {
                path: path.to_vec(),
                recursive: false,
                max_depth: Some(0),
                selected_group: Some(group_id.to_string()),
            },
            None,
        )
    }

    fn selected_group_force_find_request(group_id: &str) -> InternalQueryRequest {
        InternalQueryRequest::force_find(
            QueryOp::Tree,
            QueryScope {
                path: b"/".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some(group_id.to_string()),
            },
        )
    }

    fn decode_exact_query_node(events: Vec<Event>, path: &[u8]) -> Result<Option<QueryNode>> {
        let mut selected = None::<QueryNode>;
        for event in &events {
            let payload = rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
                .map_err(|e| CnxError::Internal(format!("decode query response failed: {e}")))?;
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
                    path: entry.path.clone(),
                    file_name: root_file_name_bytes(&entry.path),
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

    async fn recv_loopback_events(
        boundary: &LoopbackWorkerBoundary,
        timeout_ms: u64,
    ) -> Result<Vec<Event>> {
        boundary
            .channel_recv(
                BoundaryContext::default(),
                ChannelRecvRequest {
                    channel_key: ChannelKey("fs-meta.events:v1.stream".to_string()),
                    timeout_ms: Some(timeout_ms),
                },
            )
            .await
    }

    fn record_control_and_data_counts(
        control_counts: &mut std::collections::BTreeMap<String, usize>,
        data_counts: &mut std::collections::BTreeMap<String, usize>,
        batch: Vec<Event>,
    ) {
        for event in batch {
            let origin = event.metadata().origin_id.0.clone();
            if rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok() {
                *control_counts.entry(origin).or_insert(0) += 1;
            } else {
                *data_counts.entry(origin).or_insert(0) += 1;
            }
        }
    }

    fn record_path_data_counts(
        path_counts: &mut std::collections::BTreeMap<String, usize>,
        batch: &[Event],
        target: &[u8],
    ) {
        for event in batch {
            let Ok(record) = rmp_serde::from_slice::<FileMetaRecord>(event.payload_bytes()) else {
                continue;
            };
            if is_under_query_path(&record.path, target) {
                *path_counts
                    .entry(event.metadata().origin_id.0.clone())
                    .or_insert(0) += 1;
            }
        }
    }

    #[test]
    fn source_worker_rpc_preserves_invalid_input_response_category() {
        let err = SourceWorkerRpc::into_result(SourceWorkerResponse::InvalidInput(
            "duplicate source root id 'dup'".to_string(),
        ))
        .expect_err("invalid input response must become an error");
        match err {
            CnxError::InvalidInput(message) => assert!(message.contains("duplicate")),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn degraded_worker_observability_uses_cached_snapshot() {
        let cache = SourceWorkerSnapshotCache {
            lifecycle_state: Some("ready".to_string()),
            host_object_grants_version: Some(7),
            grants: Some(vec![GrantedMountRoot {
                object_ref: "obj-a".to_string(),
                host_ref: "host-a".to_string(),
                host_ip: "10.0.0.1".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: Default::default(),
                mount_point: PathBuf::from("/mnt/nfs-a"),
                fs_source: "nfs://server/export".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: vec![],
                interfaces: vec![],
                active: true,
            }]),
            logical_roots: Some(vec![RootSpec::new("root-a", "/mnt/nfs-a")]),
            status: Some(SourceStatusSnapshot {
                current_stream_generation: None,
                logical_roots: vec![],
                concrete_roots: vec![],
                degraded_roots: vec![("existing-root".to_string(), "already degraded".to_string())],
            }),
            source_primary_by_group: Some(std::collections::BTreeMap::from([(
                "group-a".to_string(),
                "obj-a".to_string(),
            )])),
            last_force_find_runner_by_group: Some(std::collections::BTreeMap::from([(
                "group-a".to_string(),
                "obj-a".to_string(),
            )])),
            force_find_inflight_groups: Some(vec!["group-a".to_string()]),
            scheduled_source_groups_by_node: Some(std::collections::BTreeMap::from([(
                "node-a".to_string(),
                vec!["group-a".to_string()],
            )])),
            scheduled_scan_groups_by_node: Some(std::collections::BTreeMap::from([(
                "node-a".to_string(),
                vec!["group-a".to_string()],
            )])),
            last_control_frame_signals_by_node: Some(std::collections::BTreeMap::new()),
            published_batches_by_node: Some(std::collections::BTreeMap::new()),
            published_events_by_node: Some(std::collections::BTreeMap::new()),
            published_control_events_by_node: Some(std::collections::BTreeMap::new()),
            published_data_events_by_node: Some(std::collections::BTreeMap::new()),
            last_published_at_us_by_node: Some(std::collections::BTreeMap::new()),
            last_published_origins_by_node: Some(std::collections::BTreeMap::new()),
            published_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
            published_path_capture_target: None,
            enqueued_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
            pending_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
            yielded_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
            summarized_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
            published_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
        };

        let snapshot =
            build_degraded_worker_observability_snapshot(&cache, "source worker unavailable");

        assert_eq!(snapshot.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE);
        assert_eq!(snapshot.host_object_grants_version, 7);
        assert_eq!(snapshot.grants.len(), 1);
        assert_eq!(snapshot.logical_roots.len(), 1);
        assert_eq!(
            snapshot.source_primary_by_group.get("group-a"),
            Some(&"obj-a".to_string())
        );
        assert_eq!(
            snapshot.last_force_find_runner_by_group.get("group-a"),
            Some(&"obj-a".to_string())
        );
        assert_eq!(
            snapshot.force_find_inflight_groups,
            vec!["group-a".to_string()]
        );
        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-a"),
            Some(&vec!["group-a".to_string()])
        );
        assert_eq!(
            snapshot.scheduled_scan_groups_by_node.get("node-a"),
            Some(&vec!["group-a".to_string()])
        );
        assert_eq!(snapshot.status.degraded_roots.len(), 2);
        assert_eq!(
            snapshot.status.degraded_roots[1],
            (
                SOURCE_WORKER_DEGRADED_ROOT_KEY.to_string(),
                "source worker unavailable".to_string()
            )
        );
    }

    #[test]
    fn successful_refresh_does_not_clear_cached_scheduled_groups_when_latest_publication_is_empty()
    {
        let mut cache = SourceWorkerSnapshotCache {
            scheduled_source_groups_by_node: Some(std::collections::BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )])),
            scheduled_scan_groups_by_node: Some(std::collections::BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )])),
            ..SourceWorkerSnapshotCache::default()
        };

        update_cached_scheduled_groups_from_refresh(
            &mut cache,
            std::collections::BTreeMap::new(),
            std::collections::BTreeMap::new(),
        );

        assert_eq!(
            cache.scheduled_source_groups_by_node,
            Some(std::collections::BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )])),
            "a successful control refresh must not erase the last known non-empty scheduled source publication when the immediate refresh snapshot is transiently empty",
        );
        assert_eq!(
            cache.scheduled_scan_groups_by_node,
            Some(std::collections::BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )])),
            "a successful control refresh must not erase the last known non-empty scheduled scan publication when the immediate refresh snapshot is transiently empty",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_source_worker_emits_initial_and_manual_rescan_batches_for_each_primary_root()
    {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: {
                let mut root1 = RootSpec::new("nfs1", &nfs1);
                root1.watch = true;
                root1.scan = true;
                let mut root2 = RootSpec::new("nfs2", &nfs2);
                root2.watch = true;
                root2.scan = true;
                vec![root1, root2]
            },
            host_object_grants: vec![
                worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = SourceWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let mut control_counts = std::collections::BTreeMap::<String, usize>::new();
        let mut data_counts = std::collections::BTreeMap::<String, usize>::new();
        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < initial_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => {
                    record_control_and_data_counts(&mut control_counts, &mut data_counts, batch)
                }
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("initial batch recv failed: {err}"),
            }
            let complete = ["node-a::nfs1", "node-a::nfs2"].iter().all(|origin| {
                control_counts.get(*origin).copied() == Some(2)
                    && data_counts.get(*origin).copied().unwrap_or(0) > 0
            });
            if complete {
                break;
            }
        }

        let nfs1_initial_data = data_counts.get("node-a::nfs1").copied().unwrap_or(0);
        let nfs2_initial_data = data_counts.get("node-a::nfs2").copied().unwrap_or(0);
        assert_eq!(control_counts.get("node-a::nfs1").copied(), Some(2));
        assert_eq!(control_counts.get("node-a::nfs2").copied(), Some(2));
        assert!(nfs1_initial_data > 0, "nfs1 should emit initial data");
        assert!(nfs2_initial_data > 0, "nfs2 should emit initial data");

        std::fs::write(
            nfs1.join("force-find-stress").join("after-rescan.txt"),
            b"aa",
        )
        .expect("append nfs1 file");
        std::fs::write(
            nfs2.join("force-find-stress").join("after-rescan.txt"),
            b"bb",
        )
        .expect("append nfs2 file");
        tokio::time::timeout(
            Duration::from_secs(8),
            client.publish_manual_rescan_signal(),
        )
        .await
        .expect("manual rescan publish timed out")
        .expect("publish manual rescan");

        let rescan_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < rescan_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => {
                    record_control_and_data_counts(&mut control_counts, &mut data_counts, batch)
                }
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("manual rescan batch recv failed: {err}"),
            }
            let complete = control_counts.get("node-a::nfs1").copied().unwrap_or(0) >= 4
                && control_counts.get("node-a::nfs2").copied().unwrap_or(0) >= 4
                && data_counts.get("node-a::nfs1").copied().unwrap_or(0) > nfs1_initial_data
                && data_counts.get("node-a::nfs2").copied().unwrap_or(0) > nfs2_initial_data;
            if complete {
                break;
            }
        }

        assert!(
            control_counts.get("node-a::nfs1").copied().unwrap_or(0) >= 4,
            "nfs1 should emit a second epoch after manual rescan: {control_counts:?}"
        );
        assert!(
            control_counts.get("node-a::nfs2").copied().unwrap_or(0) >= 4,
            "nfs2 should emit a second epoch after manual rescan: {control_counts:?}"
        );
        assert!(
            data_counts.get("node-a::nfs1").copied().unwrap_or(0) > nfs1_initial_data,
            "nfs1 should emit additional data after manual rescan: {data_counts:?}"
        );
        assert!(
            data_counts.get("node-a::nfs2").copied().unwrap_or(0) > nfs2_initial_data,
            "nfs2 should emit additional data after manual rescan: {data_counts:?}"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_source_worker_force_find_updates_last_runner_snapshot_and_observability() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros();
        let root_dir = std::env::temp_dir().join(format!("fs-meta-worker-force-find-{unique}"));
        std::fs::create_dir_all(&root_dir).expect("create root dir");
        std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &root_dir)],
            host_object_grants: vec![
                worker_source_export("node-a::exp1", "node-a", "10.0.0.11", root_dir.clone()),
                worker_source_export("node-a::exp2", "node-a", "10.0.0.12", root_dir.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = SourceWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let params = selected_group_force_find_request("nfs1");
        let first = client
            .force_find(params.clone())
            .await
            .expect("first force-find over worker");
        assert!(
            !first.is_empty(),
            "first worker force-find should return at least one response event"
        );
        let first_runner = client
            .last_force_find_runner_by_group_snapshot()
            .await
            .expect("first last-runner snapshot");
        assert_eq!(
            first_runner.get("nfs1").map(String::as_str),
            Some("node-a::exp1")
        );

        let second = client
            .force_find(params.clone())
            .await
            .expect("second force-find over worker");
        assert!(
            !second.is_empty(),
            "second worker force-find should return at least one response event"
        );
        let second_runner = client
            .last_force_find_runner_by_group_snapshot()
            .await
            .expect("second last-runner snapshot");
        assert_eq!(
            second_runner.get("nfs1").map(String::as_str),
            Some("node-a::exp2")
        );

        let third = client
            .force_find(params)
            .await
            .expect("third force-find over worker");
        assert!(
            !third.is_empty(),
            "third worker force-find should return at least one response event"
        );
        let third_runner = client
            .last_force_find_runner_by_group_snapshot()
            .await
            .expect("third last-runner snapshot");
        assert_eq!(
            third_runner.get("nfs1").map(String::as_str),
            Some("node-a::exp1")
        );

        let observability = SourceFacade::Worker(client.clone().into())
            .observability_snapshot()
            .await
            .expect("worker observability snapshot after force-find");
        assert_eq!(
            observability
                .last_force_find_runner_by_group
                .get("nfs1")
                .map(String::as_str),
            Some("node-a::exp1")
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn logical_roots_snapshot_uses_cached_roots_when_worker_resets_mid_handoff() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![worker_source_export(
                "node-d::nfs1",
                "node-d",
                "10.0.0.41",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let initial_roots = client
            .logical_roots_snapshot()
            .await
            .expect("initial logical roots snapshot");
        assert_eq!(
            initial_roots
                .iter()
                .map(|root| root.id.as_str())
                .collect::<Vec<_>>(),
            vec!["nfs1"],
            "initial worker snapshot should expose the configured root"
        );

        client.close().await.expect("close source worker");

        let cached_roots = client
            .logical_roots_snapshot()
            .await
            .expect("cached logical roots snapshot after worker reset");
        assert_eq!(
            cached_roots
                .iter()
                .map(|root| root.id.as_str())
                .collect::<Vec<_>>(),
            vec!["nfs1"],
            "logical_roots_snapshot should fall back to cached roots during restart handoff"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn host_object_grants_snapshot_uses_cached_grants_when_worker_resets_mid_handoff() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![worker_source_export(
                "node-d::nfs1",
                "node-d",
                "10.0.0.41",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let initial_grants = client
            .host_object_grants_snapshot()
            .await
            .expect("initial host-object grants snapshot");
        assert_eq!(
            initial_grants
                .iter()
                .map(|grant| grant.object_ref.as_str())
                .collect::<Vec<_>>(),
            vec!["node-d::nfs1"],
            "initial worker snapshot should expose the configured grant"
        );

        client.close().await.expect("close source worker");

        let cached_grants = client
            .host_object_grants_snapshot()
            .await
            .expect("cached host-object grants snapshot after worker reset");
        assert_eq!(
            cached_grants
                .iter()
                .map(|grant| grant.object_ref.as_str())
                .collect::<Vec<_>>(),
            vec!["node-d::nfs1"],
            "host_object_grants_snapshot should fall back to cached grants during restart handoff"
        );
    }

    #[test]
    fn cached_host_object_grants_snapshot_is_used_for_stale_drained_pid_errors() {
        let err = CnxError::AccessDenied(
            "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                .to_string(),
        );
        assert!(
            can_use_cached_host_object_grants_snapshot(&err),
            "host_object_grants_snapshot should fall back to cached grants when a stale drained/fenced worker pid rejects new grant attachments"
        );
    }

    #[test]
    fn cached_logical_roots_snapshot_is_used_for_stale_drained_pid_errors() {
        let err = CnxError::AccessDenied(
            "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                .to_string(),
        );
        assert!(
            can_use_cached_logical_roots_snapshot(&err),
            "logical_roots_snapshot should fall back to cached roots when a stale drained/fenced worker pid rejects new grant attachments"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn status_snapshot_retries_stale_drained_fenced_pid_errors() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![worker_source_export(
                "node-d::nfs1",
                "node-d",
                "10.0.0.41",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let _reset = SourceWorkerStatusErrorHookReset;
        install_source_worker_status_error_hook(SourceWorkerStatusErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

        let snapshot = client
            .status_snapshot()
            .await
            .expect("status_snapshot should retry a stale drained/fenced pid error and reach the live worker");

        assert_eq!(
            snapshot.logical_roots.len(),
            1,
            "status snapshot should come back from the rebound live worker with the configured root"
        );
        assert!(
            snapshot.degraded_roots.is_empty(),
            "fresh live source worker snapshot should still decode after stale-pid retry"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn observability_snapshot_nonblocking_retries_stale_drained_fenced_pid_errors() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source-scan activate"),
            ])
            .await
            .expect("apply source+scan control");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let expected_groups = std::collections::BTreeSet::from([
            "nfs1".to_string(),
            "nfs2".to_string(),
        ]);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for initial source/scan schedule before stale observability retry: source={source_groups:?} scan={scan_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let _reset = SourceWorkerObservabilityErrorHookReset;
        install_source_worker_observability_error_hook(SourceWorkerObservabilityErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

        let snapshot = client.observability_snapshot_nonblocking().await;
        let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
        assert_ne!(
            snapshot.lifecycle_state,
            SOURCE_WORKER_DEGRADED_STATE,
            "observability_snapshot_nonblocking should retry a stale drained/fenced pid error and reach the live worker"
        );
        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-d"),
            Some(&expected),
            "live observability snapshot should preserve scheduled source groups after stale-pid retry: {:?}",
            snapshot.scheduled_source_groups_by_node
        );
        assert_eq!(
            snapshot.scheduled_scan_groups_by_node.get("node-d"),
            Some(&expected),
            "live observability snapshot should preserve scheduled scan groups after stale-pid retry: {:?}",
            snapshot.scheduled_scan_groups_by_node
        );
        assert!(
            snapshot.status.degraded_roots.is_empty(),
            "live observability snapshot after stale-pid retry should not degrade: {:?}",
            snapshot.status.degraded_roots
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn observability_snapshot_nonblocking_retries_peer_bridge_stopped_errors_after_begin() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source-scan activate"),
            ])
            .await
            .expect("apply source+scan control");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let expected_groups = std::collections::BTreeSet::from([
            "nfs1".to_string(),
            "nfs2".to_string(),
        ]);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for initial source/scan schedule before bridge-stop observability retry: source={source_groups:?} scan={scan_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let _reset = SourceWorkerObservabilityErrorHookReset;
        install_source_worker_observability_error_hook(SourceWorkerObservabilityErrorHook {
            err: CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
        });

        let snapshot = client.observability_snapshot_nonblocking().await;
        let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
        assert_ne!(
            snapshot.lifecycle_state,
            SOURCE_WORKER_DEGRADED_STATE,
            "observability_snapshot_nonblocking should retry a peer bridge-stopped error after begin and reach the live worker"
        );
        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-d"),
            Some(&expected),
            "live observability snapshot should preserve scheduled source groups after bridge-stopped retry: {:?}",
            snapshot.scheduled_source_groups_by_node
        );
        assert_eq!(
            snapshot.scheduled_scan_groups_by_node.get("node-d"),
            Some(&expected),
            "live observability snapshot should preserve scheduled scan groups after bridge-stopped retry: {:?}",
            snapshot.scheduled_scan_groups_by_node
        );
        assert!(
            snapshot.status.degraded_roots.is_empty(),
            "live observability snapshot after bridge-stopped retry should not degrade: {:?}",
            snapshot.status.degraded_roots
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn close_waits_for_inflight_update_logical_roots_control_op_before_shutting_down_worker()
    {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![worker_source_export(
                "node-d::nfs1",
                "node-d",
                "10.0.0.41",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        // update_logical_roots holds this same client-side control-op guard
        // while the worker RPC is in flight; close must not tear down the worker
        // bridge until that guard has drained.
        let inflight = client.begin_control_op();
        let close_task = tokio::spawn({
            let client = client.clone();
            async move { client.close().await }
        });
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert!(
            !close_task.is_finished(),
            "source worker close must wait for in-flight update_logical_roots before tearing down the worker bridge"
        );

        drop(inflight);
        close_task
            .await
            .expect("join close task")
            .expect("close source worker after update");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn update_logical_roots_replays_start_after_runtime_reverts_to_initialized_without_worker_receipt(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let primed_worker = client.client().await.expect("connect source worker");
        primed_worker
            .control_frames(
                &[bootstrap_envelope(&TestWorkerBootstrapEnvelope::Init {
                    node_id: client.node_id.0.clone(),
                    payload: SourceWorkerRpc::init_payload(&client.node_id, &client.config)
                        .expect("source worker init payload"),
                })],
                Duration::from_secs(5),
            )
            .await
            .expect("reinitialize worker without replaying Start");

        let not_ready = SourceWorkerClientHandle::call_worker(
            &primed_worker,
            SourceWorkerRequest::LogicalRootsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        .expect_err("raw worker should require Start after direct Init replay");
        assert!(
            matches!(&not_ready, CnxError::PeerError(message) if message == "worker not initialized"),
            "direct bootstrap Init should leave the worker runtime unstarted until Start is replayed: {not_ready:?}"
        );

        tokio::time::timeout(
            Duration::from_secs(5),
            client.update_logical_roots(vec![
                worker_source_root("nfs1", &nfs1),
                worker_source_root("nfs2", &nfs2),
            ]),
        )
        .await
        .expect("update_logical_roots should not hang after raw Init replay")
        .expect("update_logical_roots should replay Start and reach the live worker");

        let roots = client
            .logical_roots_snapshot()
            .await
            .expect("logical roots snapshot after bootstrap replay");
        assert_eq!(
            roots.iter().map(|root| root.id.as_str()).collect::<Vec<_>>(),
            vec!["nfs1", "nfs2"],
            "logical roots should reflect the post-bootstrap-replay update"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn update_logical_roots_reacquires_worker_client_after_transport_closes_mid_handoff() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _reset = SourceWorkerUpdateRootsHookReset;
        install_source_worker_update_roots_hook(SourceWorkerUpdateRootsHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let update_task = tokio::spawn({
            let client = client.clone();
            let nfs1 = nfs1.clone();
            let nfs2 = nfs2.clone();
            async move {
                client
                    .update_logical_roots(vec![
                        worker_source_root("nfs1", &nfs1),
                        worker_source_root("nfs2", &nfs2),
                    ])
                    .await
            }
        });

        entered.notified().await;
        client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown stale worker bridge");
        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker restart timed out")
            .expect("restart source worker");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(4), update_task)
            .await
            .expect("update_logical_roots should reacquire a live worker client after handoff")
            .expect("join update_logical_roots task")
            .expect("update_logical_roots after worker restart");

        let roots = client
            .logical_roots_snapshot()
            .await
            .expect("logical roots snapshot after update");
        assert_eq!(
            roots.iter().map(|root| root.id.as_str()).collect::<Vec<_>>(),
            vec!["nfs1", "nfs2"],
            "logical roots should reflect the post-handoff update"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_reacquires_worker_client_after_transport_closes_mid_handoff() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _reset = SourceWorkerControlFramePauseHookReset;
        install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let control_task = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .on_control_frame(vec![
                        encode_runtime_exec_control(&RuntimeExecControl::Activate(
                            RuntimeExecActivate {
                                route_key: ROUTE_KEY_QUERY.to_string(),
                                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                                lease: None,
                                generation: 2,
                                expires_at_ms: 1,
                                bound_scopes: vec![
                                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                                ],
                            },
                        ))
                        .expect("encode source activate"),
                        encode_runtime_exec_control(&RuntimeExecControl::Activate(
                            RuntimeExecActivate {
                                route_key: ROUTE_KEY_QUERY.to_string(),
                                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                                lease: None,
                                generation: 2,
                                expires_at_ms: 1,
                                bound_scopes: vec![
                                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                                ],
                            },
                        ))
                        .expect("encode source-scan activate"),
                    ])
                    .await
            }
        });

        entered.notified().await;
        client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown stale source worker bridge");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(8), control_task)
            .await
            .expect("on_control_frame should reacquire a live source worker client after handoff")
            .expect("join on_control_frame task")
            .expect("source on_control_frame after worker restart");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let scheduled_source = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            let scheduled_scan = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups")
                .unwrap_or_default();
            if scheduled_source == expected_groups && scheduled_scan == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < schedule_deadline,
                "timed out waiting for scheduled groups after source handoff retry: source={scheduled_source:?} scan={scheduled_scan:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn second_on_control_frame_reacquires_worker_client_after_first_wave_succeeded_and_transport_closes_mid_handoff(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source-scan activate"),
            ])
            .await
            .expect("first source control wave should succeed");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _reset = SourceWorkerControlFramePauseHookReset;
        install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let second_wave = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .on_control_frame(vec![
                        encode_runtime_exec_control(&RuntimeExecControl::Activate(
                            RuntimeExecActivate {
                                route_key: ROUTE_KEY_QUERY.to_string(),
                                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                                lease: None,
                                generation: 2,
                                expires_at_ms: 1,
                                bound_scopes: vec![
                                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                                ],
                            },
                        ))
                        .expect("encode second-wave source activate"),
                    ])
                    .await
            }
        });

        entered.notified().await;
        client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown stale source worker bridge after first wave");
        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker restart timed out")
            .expect("restart source worker after first-wave success");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(8), second_wave)
            .await
            .expect("second source control wave should reacquire a live worker client after reset")
            .expect("join second source control wave")
            .expect("second source control wave after worker restart");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            if source_groups == std::collections::BTreeSet::from([
                "nfs1".to_string(),
                "nfs2".to_string(),
            ]) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "scheduled source groups should remain converged after second-wave retry: source={source_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn third_on_control_frame_reacquires_worker_client_after_first_and_second_waves_succeeded(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let full_wave = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ];
        let source_only_wave = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-only activate"),
        ];

        client
            .on_control_frame(full_wave.clone())
            .await
            .expect("first source control wave should succeed");
        client
            .on_control_frame(source_only_wave.clone())
            .await
            .expect("second source-only control wave should succeed");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _reset = SourceWorkerControlFramePauseHookReset;
        install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let third_wave = tokio::spawn({
            let client = client.clone();
            async move { client.on_control_frame(source_only_wave).await }
        });

        entered.notified().await;
        client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown stale source worker bridge after two successful waves");
        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker restart timed out")
            .expect("restart source worker after two successful waves");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(8), third_wave)
            .await
            .expect("third source control wave should reacquire a live worker client after reset")
            .expect("join third source control wave")
            .expect("third source control wave after worker restart");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            if source_groups
                == std::collections::BTreeSet::from([
                    "nfs1".to_string(),
                    "nfs2".to_string(),
                ])
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "scheduled source groups should remain converged after third-wave retry: source={source_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn start_retries_retryable_bridge_errors_after_failed_followup_wave() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
            ])
            .await
            .expect("first source control wave should succeed");

        let _reset = SourceWorkerStartErrorQueueHookReset;
        install_source_worker_start_error_queue_hook(SourceWorkerStartErrorQueueHook {
            errs: std::collections::VecDeque::from(vec![CnxError::PeerError(
                "transport closed: sidecar control bridge stopped".to_string(),
            )]),
            sticky_worker_instance_id: None,
            sticky_peer_err: None,
        });

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker retry start timed out")
            .expect(
                "source worker start should recover from one retryable bridge reset after a follow-up failure",
            );

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode retried source activate"),
            ])
            .await
            .expect("source control should still converge after retryable start recovery");

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn start_retries_after_hung_ensure_started_attempt_from_failed_followup_wave() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
            ])
            .await
            .expect("first source control wave should succeed");

        let _reset = SourceWorkerStartDelayHookReset;
        install_source_worker_start_delay_hook(SourceWorkerStartDelayHook {
            delays: std::collections::VecDeque::from([Duration::from_secs(10)]),
        });

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect(
                "source worker start should recover after one hung ensure_started attempt from a follow-up failure",
            )
            .expect("source worker start should recover after one hung ensure_started attempt");

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode retried source activate"),
            ])
            .await
            .expect("source control should still converge after hung start recovery");

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn start_recreates_shared_worker_client_when_retryable_start_resets_never_converge() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![worker_source_export(
                "node-d::nfs1",
                "node-d",
                "10.0.0.41",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let failing_instance_id = client.worker_instance_id_for_tests().await;
        let _reset = SourceWorkerStartErrorQueueHookReset;
        install_source_worker_start_error_queue_hook(SourceWorkerStartErrorQueueHook {
            errs: std::collections::VecDeque::new(),
            sticky_worker_instance_id: Some(failing_instance_id),
            sticky_peer_err: Some("transport closed: sidecar control bridge stopped".to_string()),
        });

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start should swap off a permanently resetting shared worker client")
            .expect("source worker start should recover by recreating the shared worker client");

        assert_ne!(
            client.worker_instance_id_for_tests().await,
            failing_instance_id,
            "post-failure worker-ready recovery must recreate the shared worker client instead of retrying forever on the same permanently resetting instance"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn distinct_worker_bindings_share_started_source_worker_client_on_same_node() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let module_path = fs_meta_worker_module_path();
        let module_link = tmp.path().join("worker-module-link");
        std::os::unix::fs::symlink(&module_path, &module_link)
            .expect("create worker module symlink");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![worker_source_export(
                "node-d::nfs1",
                "node-d",
                "10.0.0.41",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let predecessor_socket_dir = tempdir().expect("create predecessor worker socket dir");
        let successor_socket_dir = tempdir().expect("create successor worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let predecessor = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg.clone(),
                external_source_worker_binding(predecessor_socket_dir.path()),
                factory.clone(),
            )
            .expect("construct predecessor source worker client"),
        );
        let successor = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding_with_module_path(
                    successor_socket_dir.path(),
                    &module_link,
                ),
                factory,
            )
            .expect("construct successor source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), predecessor.start())
            .await
            .expect("predecessor source worker start timed out")
            .expect("start predecessor source worker");

        assert!(
            successor
                .existing_client()
                .await
                .expect("successor existing client after predecessor start")
                .is_some(),
            "same-node successor handle should reuse the already-started source worker client even when socket_dir/module_path differ across runtime instances"
        );

        let roots = successor
            .logical_roots_snapshot()
            .await
            .expect("successor logical_roots_snapshot through shared started worker");
        assert_eq!(
            roots.iter().map(|root| root.id.as_str()).collect::<Vec<_>>(),
            vec!["nfs1"],
            "successor handle should see the shared started worker state after predecessor start"
        );

        predecessor.close().await.expect("close predecessor source worker");
        successor.close().await.expect("close successor source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_recovery_starts_on_same_shared_source_handle_serialize_worker_ready_recovery(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![worker_source_export(
                "node-d::nfs1",
                "node-d",
                "10.0.0.41",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let binding = external_source_worker_binding(worker_socket_dir.path());
        let predecessor = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg.clone(),
                binding.clone(),
                factory.clone(),
            )
            .expect("construct predecessor source worker client"),
        );
        let successor = Arc::new(
            SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
                .expect("construct successor source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), predecessor.start())
            .await
            .expect("source worker start timed out")
            .expect("start shared source worker");

        predecessor
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown shared source worker before recovery");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _reset = SourceWorkerStartPauseHookReset;
        install_source_worker_start_pause_hook(SourceWorkerStartPauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let predecessor_start = tokio::spawn({
            let predecessor = predecessor.clone();
            async move { predecessor.start().await }
        });

        entered.notified().await;

        let mut successor_start = tokio::spawn({
            let successor = successor.clone();
            async move { successor.start().await }
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(600), &mut successor_start)
                .await
                .is_err(),
            "same-node concurrent recovery start must wait while the shared source worker is still mid worker-ready recovery"
        );

        release.notify_waiters();

        predecessor_start
            .await
            .expect("join predecessor recovery start")
            .expect("predecessor recovery start");
        successor_start
            .await
            .expect("join successor recovery start")
            .expect("successor recovery start");

        let roots = successor
            .logical_roots_snapshot()
            .await
            .expect("logical roots snapshot after serialized recovery start");
        assert_eq!(
            roots.iter().map(|root| root.id.as_str()).collect::<Vec<_>>(),
            vec!["nfs1"],
            "shared source worker should still converge after serialized same-node recovery starts"
        );

        predecessor.close().await.expect("close predecessor source worker");
        successor.close().await.expect("close successor source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn update_logical_roots_waits_for_shared_control_frame_handoff_before_dispatching_to_worker(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let binding = external_source_worker_binding(worker_socket_dir.path());
        let control_client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg.clone(),
                binding.clone(),
                factory.clone(),
            )
            .expect("construct control source worker client"),
        );
        let update_client = Arc::new(
            SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
                .expect("construct update source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), control_client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let control_entered = Arc::new(Notify::new());
        let control_release = Arc::new(Notify::new());
        let _control_reset = SourceWorkerControlFramePauseHookReset;
        install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
            entered: control_entered.clone(),
            release: control_release.clone(),
        });

        let update_entered = Arc::new(Notify::new());
        let update_release = Arc::new(Notify::new());
        let _update_reset = SourceWorkerUpdateRootsHookReset;
        install_source_worker_update_roots_hook(SourceWorkerUpdateRootsHook {
            entered: update_entered.clone(),
            release: update_release.clone(),
        });

        let control_task = tokio::spawn({
            let control_client = control_client.clone();
            async move {
                control_client
                    .on_control_frame(vec![
                        encode_runtime_exec_control(&RuntimeExecControl::Activate(
                            RuntimeExecActivate {
                                route_key: ROUTE_KEY_QUERY.to_string(),
                                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                                lease: None,
                                generation: 2,
                                expires_at_ms: 1,
                                bound_scopes: vec![
                                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                                ],
                            },
                        ))
                        .expect("encode source activate"),
                        encode_runtime_exec_control(&RuntimeExecControl::Activate(
                            RuntimeExecActivate {
                                route_key: ROUTE_KEY_QUERY.to_string(),
                                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                                lease: None,
                                generation: 2,
                                expires_at_ms: 1,
                                bound_scopes: vec![
                                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                                ],
                            },
                        ))
                        .expect("encode source-scan activate"),
                    ])
                    .await
            }
        });

        control_entered.notified().await;

        let update_task = tokio::spawn({
            let update_client = update_client.clone();
            let nfs1 = nfs1.clone();
            let nfs2 = nfs2.clone();
            async move {
                update_client
                    .update_logical_roots(vec![
                        worker_source_root("nfs1", &nfs1),
                        worker_source_root("nfs2", &nfs2),
                    ])
                    .await
            }
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(600), update_entered.notified())
                .await
                .is_err(),
            "shared source update_logical_roots must not start dispatch while another handle is still mid-control-frame handoff on the same worker"
        );

        control_release.notify_waiters();
        control_task
            .await
            .expect("join control task")
            .expect("apply shared source control frame");

        update_entered.notified().await;
        update_release.notify_waiters();
        update_task
            .await
            .expect("join update task")
            .expect("update logical roots after shared control handoff");

        let roots = update_client
            .logical_roots_snapshot()
            .await
            .expect("logical roots snapshot after shared handoff");
        assert_eq!(
            roots.iter().map(|root| root.id.as_str()).collect::<Vec<_>>(),
            vec!["nfs1", "nfs2"],
            "logical roots should reflect the post-handoff update"
        );

        update_client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn close_keeps_shared_source_worker_client_alive_when_another_handle_still_exists() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![worker_source_export(
                "node-d::nfs1",
                "node-d",
                "10.0.0.41",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let binding = external_source_worker_binding(worker_socket_dir.path());
        let predecessor = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg.clone(),
                binding.clone(),
                factory.clone(),
            )
            .expect("construct predecessor source worker client"),
        );
        let successor = Arc::new(
            SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
                .expect("construct successor source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), successor.start())
            .await
            .expect("source worker start timed out")
            .expect("start shared source worker");

        assert!(
            successor
                .existing_client()
                .await
                .expect("existing client before predecessor close")
                .is_some(),
            "shared source worker must have a live client before predecessor close"
        );

        predecessor
            .close()
            .await
            .expect("close predecessor source worker handle");

        assert!(
            successor
                .existing_client()
                .await
                .expect("existing client after predecessor close")
                .is_some(),
            "closing one source handle must not tear down the shared worker client while a successor handle still exists"
        );

        let roots = successor
            .logical_roots_snapshot()
            .await
            .expect("successor source logical_roots_snapshot after predecessor close");
        assert_eq!(
            roots.iter().map(|root| root.id.as_str()).collect::<Vec<_>>(),
            vec!["nfs1"],
            "shared source worker should stay usable for the successor after predecessor close"
        );

        successor.close().await.expect("close successor source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn update_logical_roots_retries_stale_drained_fenced_pid_errors() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let _reset = SourceWorkerUpdateRootsErrorHookReset;
        install_source_worker_update_roots_error_hook(SourceWorkerUpdateRootsErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

        client
            .update_logical_roots(vec![
                worker_source_root("nfs1", &nfs1),
                worker_source_root("nfs2", &nfs2),
            ])
            .await
            .expect(
                "update_logical_roots should retry a stale drained/fenced pid error and reach the live worker",
            );

        let roots = client
            .logical_roots_snapshot()
            .await
            .expect("logical roots snapshot after stale-pid retry");
        assert_eq!(
            roots.iter().map(|root| root.id.as_str()).collect::<Vec<_>>(),
            vec!["nfs1", "nfs2"],
            "logical roots should reflect the post-retry update"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_stale_drained_fenced_pid_errors() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let _reset = SourceWorkerControlFrameErrorHookReset;
        install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source-scan activate"),
            ])
            .await
            .expect(
                "on_control_frame should retry a stale drained/fenced pid error and reach the live worker",
            );

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups")
                .unwrap_or_default();
            if source_groups == std::collections::BTreeSet::from([
                "nfs1".to_string(),
                "nfs2".to_string(),
            ]) && scan_groups == std::collections::BTreeSet::from([
                "nfs1".to_string(),
                "nfs2".to_string(),
            ]) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "scheduled groups should reflect the post-retry activation: source={:?} scan={:?}",
                source_groups,
                scan_groups
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        client.close().await.expect("close source worker");
    }

    async fn assert_on_control_frame_retries_bridge_reset_peer_error(
        err: CnxError,
        label: &str,
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source-scan activate"),
            ])
            .await
            .expect("first source control wave should succeed");

        let _reset = SourceWorkerControlFrameErrorHookReset;
        install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook { err });

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode second-wave source activate"),
            ])
            .await
            .unwrap_or_else(|err| panic!("{label}: {err}"));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            if source_groups
                == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: scheduled source groups should remain converged after retry: source={source_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_peer_connection_reset_errors_after_first_wave_succeeded() {
        assert_on_control_frame_retries_bridge_reset_peer_error(
            CnxError::PeerError(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: Connection reset by peer (os error 104)"
                    .to_string(),
            ),
            "on_control_frame should retry a peer connection-reset bridge error and reach the live worker",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_peer_early_eof_errors_after_first_wave_succeeded() {
        assert_on_control_frame_retries_bridge_reset_peer_error(
            CnxError::PeerError(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
            "on_control_frame should retry a peer early-eof bridge error and reach the live worker",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_peer_bridge_stopped_errors_after_first_wave_succeeded() {
        assert_on_control_frame_retries_bridge_reset_peer_error(
            CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
            "on_control_frame should retry a peer bridge-stopped error and reach the live worker",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn force_find_via_node_shares_runner_state_with_existing_target_worker_handle() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros();
        let root_dir =
            std::env::temp_dir().join(format!("fs-meta-worker-force-find-share-{unique}"));
        std::fs::create_dir_all(&root_dir).expect("create root dir");
        std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &root_dir)],
            host_object_grants: vec![
                worker_source_export("node-a::exp1", "node-a", "10.0.0.11", root_dir.clone()),
                worker_source_export("node-a::exp2", "node-a", "10.0.0.12", root_dir.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let binding = external_source_worker_binding(worker_socket_dir.path());
        let target_client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-a".to_string()),
                cfg.clone(),
                binding.clone(),
                factory.clone(),
            )
            .expect("construct target source worker client"),
        );
        let routing_client = Arc::new(
            SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
                .expect("construct routing source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), target_client.start())
            .await
            .expect("target source worker start timed out")
            .expect("start target source worker");

        let routed = SourceFacade::worker(routing_client);
        let params = selected_group_force_find_request("nfs1");
        let first = routed
            .force_find_via_node(&NodeId("node-a".to_string()), &params)
            .await
            .expect("routed force-find via node");
        assert!(
            !first.is_empty(),
            "routed force-find via target node should return at least one response event"
        );

        let shared_runner = target_client
            .last_force_find_runner_by_group_snapshot()
            .await
            .expect("target handle last-runner snapshot");
        assert_eq!(
            shared_runner.get("nfs1").map(String::as_str),
            Some("node-a::exp1"),
            "force_find_via_node should update runner state visible through the already-started target worker handle"
        );

        target_client
            .close()
            .await
            .expect("close target source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_source_worker_stream_batches_reach_sink_worker_for_each_scheduled_primary_root()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let source_factory = RuntimeWorkerClientFactory::new(
            boundary.clone(),
            boundary.clone(),
            in_memory_state_boundary(),
        );
        let sink_factory = RuntimeWorkerClientFactory::new(
            boundary.clone(),
            boundary.clone(),
            in_memory_state_boundary(),
        );
        let worker_socket_root = tempdir().expect("create worker socket dir");
        let source_socket_dir = worker_socket_root.path().join("source");
        let sink_socket_dir = worker_socket_root.path().join("sink");
        std::fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
        std::fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");
        let source = SourceWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg.clone(),
            external_source_worker_binding(&source_socket_dir),
            source_factory,
        )
        .expect("construct source worker client");
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_sink_worker_binding(&sink_socket_dir),
            sink_factory,
        )
        .expect("construct sink worker client");

        source
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 1,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 1,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source-scan activate"),
            ])
            .await
            .expect("activate source worker");
        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("activate sink worker");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduling_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = source
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            let scan_groups = source
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups")
                .unwrap_or_default();
            let sink_groups = sink
                .status_snapshot()
                .await
                .expect("sink status")
                .scheduled_groups_by_node
                .get("node-a")
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>();
            if source_groups == expected_groups
                && scan_groups == expected_groups
                && sink_groups == expected_groups
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduling_deadline,
                "timed out waiting for source/sink schedule convergence: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let mut control_counts = std::collections::BTreeMap::<String, usize>::new();
        let mut data_counts = std::collections::BTreeMap::<String, usize>::new();
        let selected_file = b"/force-find-stress/seed.txt";
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => {
                    let mut control = 0usize;
                    for event in &batch {
                        let origin = event.metadata().origin_id.0.clone();
                        if rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok() {
                            *control_counts.entry(origin).or_insert(0) += 1;
                            control += 1;
                        } else {
                            *data_counts.entry(origin).or_insert(0) += 1;
                        }
                    }
                    if control < batch.len() {
                        sink.send(batch)
                            .await
                            .expect("forward source batch to sink");
                    }
                }
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("stream batch recv failed: {err}"),
            }
            let complete = ["node-a::nfs1", "node-a::nfs2"].iter().all(|origin| {
                control_counts.get(*origin).copied().unwrap_or(0) > 0
                    && data_counts.get(*origin).copied().unwrap_or(0) > 0
            });
            let nfs1_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs1"))
                    .await
                    .expect("query nfs1"),
                selected_file,
            )
            .expect("decode nfs1")
            .is_some();
            let nfs2_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                    .await
                    .expect("query nfs2"),
                selected_file,
            )
            .expect("decode nfs2")
            .is_some();
            if complete && nfs1_ready && nfs2_ready {
                break;
            }
        }

        assert!(
            control_counts.get("node-a::nfs1").copied().unwrap_or(0) > 0,
            "nfs1 should publish at least one control event on the external source stream: {control_counts:?}"
        );
        assert!(
            control_counts.get("node-a::nfs2").copied().unwrap_or(0) > 0,
            "nfs2 should publish at least one control event on the external source stream: {control_counts:?}"
        );
        assert!(
            data_counts.get("node-a::nfs1").copied().unwrap_or(0) > 0,
            "nfs1 should produce at least one data batch on the external source stream: {data_counts:?}"
        );
        assert!(
            data_counts.get("node-a::nfs2").copied().unwrap_or(0) > 0,
            "nfs2 should produce at least one data batch on the external source stream: {data_counts:?}"
        );
        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs1"))
                    .await
                    .expect("query nfs1 final"),
                selected_file,
            )
            .expect("decode nfs1 final")
            .is_some(),
            "nfs1 should materialize after forwarding external source batches into sink.send(...)",
        );
        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                    .await
                    .expect("query nfs2 final"),
                selected_file,
            )
            .expect("decode nfs2 final")
            .is_some(),
            "nfs2 should materialize after forwarding external source batches into sink.send(...)",
        );

        source.close().await.expect("close source worker");
        sink.close().await.expect("close sink worker");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
    async fn external_source_worker_real_nfs_manual_rescan_publishes_newly_seeded_subtree_alongside_baseline_path()
     {
        let preflight = real_nfs_lab::RealNfsPreflight::detect();
        if !preflight.enabled {
            eprintln!(
                "skip real-nfs external source worker publish-path test: {}",
                preflight
                    .reason
                    .unwrap_or_else(|| "real-nfs preflight failed".to_string())
            );
            return;
        }

        let mut lab = real_nfs_lab::NfsLab::start().expect("start NFS lab");
        let nfs1 = lab
            .mount_export("node-a", "nfs1")
            .expect("mount node-a nfs1 export");
        let nfs2 = lab
            .mount_export("node-a", "nfs2")
            .expect("mount node-a nfs2 export");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = SourceWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let baseline_target = b"/data";
        let force_find_target = b"/force-find-stress";
        let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
        let mut force_find_counts = std::collections::BTreeMap::<String, usize>::new();
        let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < baseline_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => {
                    record_path_data_counts(&mut baseline_counts, &batch, baseline_target);
                    record_path_data_counts(&mut force_find_counts, &batch, force_find_target);
                }
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("initial publish recv failed: {err}"),
            }
            let baseline_ready = ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0);
            if baseline_ready {
                break;
            }
        }

        assert!(
            ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0),
            "baseline /data should publish for both local primary roots over the external source stream: {baseline_counts:?}"
        );
        assert!(
            force_find_counts.is_empty(),
            "new subtree target should stay absent before it is seeded and manually rescanned: {force_find_counts:?}"
        );

        lab.mkdir("nfs1", "force-find-stress")
            .expect("create nfs1 force-find dir");
        lab.mkdir("nfs2", "force-find-stress")
            .expect("create nfs2 force-find dir");
        lab.write_file("nfs1", "force-find-stress/seed.txt", "a\n")
            .expect("seed nfs1 force-find subtree");
        lab.write_file("nfs2", "force-find-stress/seed.txt", "b\n")
            .expect("seed nfs2 force-find subtree");

        tokio::time::timeout(
            Duration::from_secs(8),
            client.publish_manual_rescan_signal(),
        )
        .await
        .expect("manual rescan publish timed out")
        .expect("publish manual rescan");

        let force_find_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < force_find_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => {
                    record_path_data_counts(&mut force_find_counts, &batch, force_find_target);
                }
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("manual rescan publish recv failed: {err}"),
            }
            let subtree_ready = ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| force_find_counts.get(*origin).copied().unwrap_or(0) > 0);
            if subtree_ready {
                break;
            }
        }

        assert!(
            ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| force_find_counts.get(*origin).copied().unwrap_or(0) > 0),
            "newly seeded /force-find-stress subtree should publish for both local primary roots after manual rescan over the external source stream: {force_find_counts:?}"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_source_worker_nonblocking_observability_can_serve_stale_published_path_counts_during_control_inflight()
     {
        let previous = std::env::var("FSMETA_DEBUG_STREAM_PATH_CAPTURE").ok();
        unsafe {
            std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", "/force-find-stress");
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = SourceWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let baseline_target = b"/data";
        let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
        while tokio::time::Instant::now() < baseline_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => record_path_data_counts(&mut baseline_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("initial publish recv failed: {err}"),
            }
            if ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0)
            {
                break;
            }
        }
        assert!(
            ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0),
            "baseline /data should publish for both local primary roots: {baseline_counts:?}"
        );

        let primed_worker = client.client().await.expect("connect source worker");
        let primed = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("prime cached observability snapshot");
        assert_eq!(
            primed.published_path_capture_target.as_deref(),
            Some("/force-find-stress"),
            "primed snapshot should carry the configured path target"
        );
        assert!(
            primed
                .published_path_origin_counts_by_node
                .get("node-a")
                .is_none_or(Vec::is_empty),
            "before seeding subtree, cached path counts should be empty: {:?}",
            primed.published_path_origin_counts_by_node
        );

        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 subtree");
        std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 subtree");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"aa")
            .expect("seed nfs1 subtree");
        std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"bb")
            .expect("seed nfs2 subtree");

        tokio::time::timeout(
            Duration::from_secs(8),
            client.publish_manual_rescan_signal(),
        )
        .await
        .expect("manual rescan publish timed out")
        .expect("publish manual rescan");

        let force_find_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let force_find_target = b"/force-find-stress";
        let mut force_find_counts = std::collections::BTreeMap::<String, usize>::new();
        while tokio::time::Instant::now() < force_find_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => {
                    record_path_data_counts(&mut force_find_counts, &batch, force_find_target)
                }
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("manual rescan publish recv failed: {err}"),
            }
            if ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| force_find_counts.get(*origin).copied().unwrap_or(0) > 0)
            {
                break;
            }
        }
        assert!(
            ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| force_find_counts.get(*origin).copied().unwrap_or(0) > 0),
            "manual rescan should publish /force-find-stress for both roots before stale-observability check: {force_find_counts:?}"
        );

        let inflight = client.begin_control_op();
        let stale = client.observability_snapshot_nonblocking().await;
        drop(inflight);
        assert_eq!(
            stale.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
            "control-inflight nonblocking snapshot should use cached fallback"
        );
        assert!(
            stale
                .published_path_origin_counts_by_node
                .get("node-a")
                .is_none_or(Vec::is_empty),
            "cached fallback should still reflect stale pre-rescan path counts: {:?}",
            stale.published_path_origin_counts_by_node
        );

        let live_worker = client.client().await.expect("reconnect source worker");
        let live = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("fetch live post-rescan observability snapshot");
        let live_counts = live
            .published_path_origin_counts_by_node
            .get("node-a")
            .cloned()
            .unwrap_or_default();
        assert!(
            live_counts
                .iter()
                .any(|entry| entry.starts_with("node-a::nfs1=")),
            "live worker snapshot should include nfs1 /force-find-stress path counts: {live_counts:?}"
        );
        assert!(
            live_counts
                .iter()
                .any(|entry| entry.starts_with("node-a::nfs2=")),
            "live worker snapshot should include nfs2 /force-find-stress path counts after rescan: {live_counts:?}"
        );

        client.close().await.expect("close source worker");
        match previous {
            Some(value) => unsafe { std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", value) },
            None => unsafe { std::env::remove_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE") },
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_source_worker_nonblocking_observability_preserves_scheduled_groups_after_successful_control_before_next_inflight(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = SourceWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let primed_worker = client.client().await.expect("connect source worker");
        let primed = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("prime cached observability snapshot");
        assert!(
            primed.scheduled_source_groups_by_node.is_empty()
                && primed.scheduled_scan_groups_by_node.is_empty(),
            "baseline cached observability should start empty: source={:?} scan={:?}",
            primed.scheduled_source_groups_by_node,
            primed.scheduled_scan_groups_by_node
        );

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ])
            .await
            .expect("apply source+scan control");

        let expected = std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]);
        let local_source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("scheduled source groups")
            .unwrap_or_default();
        let local_scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scheduled scan groups")
            .unwrap_or_default();
        assert_eq!(
            local_source_groups,
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
        );
        assert_eq!(
            local_scan_groups,
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
        );

        let inflight = client.begin_control_op();
        let stale = client.observability_snapshot_nonblocking().await;
        drop(inflight);
        assert_eq!(
            stale.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
            "control-inflight nonblocking snapshot should still use cached fallback"
        );
        assert_eq!(
            stale.scheduled_source_groups_by_node, expected,
            "cached fallback should preserve latest scheduled source groups after successful control"
        );
        assert_eq!(
            stale.scheduled_scan_groups_by_node, expected,
            "cached fallback should preserve latest scheduled scan groups after successful control"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_refresh_retries_transient_empty_scheduled_groups_before_publishing_cache(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-b::nfs1", "node-b", "10.0.0.21", nfs1.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = SourceWorkerClientHandle::new(
            NodeId("node-b-29776102761141088687226881".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        install_source_worker_scheduled_groups_refresh_queue_hook(
            SourceWorkerScheduledGroupsRefreshQueueHook {
                replies: std::collections::VecDeque::from([(
                    Some(std::collections::BTreeSet::new()),
                    Some(std::collections::BTreeSet::new()),
                )]),
            },
        );

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
                }))
                .expect("encode scan activate"),
            ])
            .await
            .expect("apply control wave");

        let inflight = client.begin_control_op();
        let stale = client.observability_snapshot_nonblocking().await;
        drop(inflight);

        assert_eq!(
            stale.scheduled_source_groups_by_node,
            std::collections::BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string()],
            )]),
            "on_control_frame must retry transient empty scheduled-source refreshes before publishing the next degraded cache snapshot"
        );
        assert_eq!(
            stale.scheduled_scan_groups_by_node,
            std::collections::BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string()],
            )]),
            "on_control_frame must retry transient empty scheduled-scan refreshes before publishing the next degraded cache snapshot"
        );

        clear_source_worker_scheduled_groups_refresh_queue_hook();
        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn control_inflight_nonblocking_observability_primes_runtime_managed_groups_from_latest_activate_signals(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = SourceWorkerClientHandle::new(
            NodeId("node-c-29776225407437800789245953".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        install_source_worker_scheduled_groups_refresh_queue_hook(
            SourceWorkerScheduledGroupsRefreshQueueHook {
                replies: std::iter::repeat((
                    Some(std::collections::BTreeSet::new()),
                    Some(std::collections::BTreeSet::new()),
                ))
                .take(32)
                .collect(),
            },
        );

        let source_wave = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ];
        let control = std::iter::once(
            encode_runtime_host_grant_change(&RuntimeHostGrantChange {
                version: 1,
                grants: vec![
                    worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                    worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
                ],
            })
            .expect("encode runtime host grants changed"),
        )
        .chain(source_wave)
        .collect::<Vec<_>>();

        client
            .on_control_frame(control)
            .await
            .expect("apply runtime-managed multi-root control wave");

        let inflight = client.begin_control_op();
        let degraded = client.observability_snapshot_nonblocking().await;
        drop(inflight);

        let expected = std::collections::BTreeMap::from([(
            "node-c".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]);
        assert_eq!(
            degraded.scheduled_source_groups_by_node, expected,
            "control-inflight nonblocking observability must publish the latest runtime-managed source groups from accepted activate scopes even before live refresh converges"
        );
        assert_eq!(
            degraded.scheduled_scan_groups_by_node, expected,
            "control-inflight nonblocking observability must publish the latest runtime-managed scan groups from accepted activate scopes even before live refresh converges"
        );

        clear_source_worker_scheduled_groups_refresh_queue_hook();
        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn control_inflight_nonblocking_observability_primes_runtime_managed_groups_from_local_resource_ids_without_grant_change(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = SourceWorkerClientHandle::new(
            NodeId("node-b-29776249969860401661214721".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        install_source_worker_scheduled_groups_refresh_queue_hook(
            SourceWorkerScheduledGroupsRefreshQueueHook {
                replies: std::iter::repeat((
                    Some(std::collections::BTreeSet::new()),
                    Some(std::collections::BTreeSet::new()),
                ))
                .take(32)
                .collect(),
            },
        );

        let control = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ];

        client
            .on_control_frame(control)
            .await
            .expect("apply runtime-managed multi-root control wave without grant change");

        let inflight = client.begin_control_op();
        let degraded = client.observability_snapshot_nonblocking().await;
        drop(inflight);

        let expected = std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]);
        assert_eq!(
            degraded.scheduled_source_groups_by_node, expected,
            "control-inflight nonblocking observability must derive latest runtime-managed source groups from local resource ids even when the accepted activate wave carries no fresh grant-change payload"
        );
        assert_eq!(
            degraded.scheduled_scan_groups_by_node, expected,
            "control-inflight nonblocking observability must derive latest runtime-managed scan groups from local resource ids even when the accepted activate wave carries no fresh grant-change payload"
        );

        clear_source_worker_scheduled_groups_refresh_queue_hook();
        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_source_worker_observability_normalizes_instance_suffixed_node_id_to_host_ref_for_scheduled_groups(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let node_id = NodeId("node-a-29775285406139598021591041".to_string());
        let client = SourceWorkerClientHandle::new(
            node_id.clone(),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ])
            .await
            .expect("apply source+scan control");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for scheduled groups: source={source_groups:?} scan={scan_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let live_worker = client.client().await.expect("connect source worker");
        let snapshot = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("fetch live snapshot");
        let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-a"),
            Some(&expected),
            "scheduled source groups should be keyed by stable host_ref rather than instance-suffixed node id"
        );
        assert_eq!(
            snapshot.scheduled_scan_groups_by_node.get("node-a"),
            Some(&expected),
            "scheduled scan groups should be keyed by stable host_ref rather than instance-suffixed node id"
        );
        assert!(
            !snapshot.scheduled_source_groups_by_node.contains_key(&node_id.0),
            "instance-suffixed node id should not leak into scheduled source groups: {:?}",
            snapshot.scheduled_source_groups_by_node
        );
        assert!(
            !snapshot.scheduled_scan_groups_by_node.contains_key(&node_id.0),
            "instance-suffixed node id should not leak into scheduled scan groups: {:?}",
            snapshot.scheduled_scan_groups_by_node
        );

        client.close().await.expect("close source worker");
    }

    #[test]
    fn observability_snapshot_normalization_uses_cached_grants_when_live_reply_grants_are_empty() {
        let node_id = NodeId("node-d-29776112502313141518991361".to_string());
        let cached_grants = vec![GrantedMountRoot {
            object_ref: "node-d::nfs2".to_string(),
            host_ref: "node-d".to_string(),
            host_ip: "10.0.0.41".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: Default::default(),
            mount_point: PathBuf::from("/mnt/nfs2"),
            fs_source: "nfs://server/export2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: vec![],
            interfaces: vec![],
            active: true,
        }];
        let mut snapshot = SourceObservabilitySnapshot {
            lifecycle_state: "ready".to_string(),
            host_object_grants_version: 2,
            grants: Vec::new(),
            logical_roots: Vec::new(),
            status: SourceStatusSnapshot::default(),
            source_primary_by_group: std::collections::BTreeMap::new(),
            last_force_find_runner_by_group: std::collections::BTreeMap::new(),
            force_find_inflight_groups: Vec::new(),
            scheduled_source_groups_by_node: std::collections::BTreeMap::from([(
                node_id.0.clone(),
                vec!["nfs2".to_string()],
            )]),
            scheduled_scan_groups_by_node: std::collections::BTreeMap::from([(
                node_id.0.clone(),
                vec!["nfs2".to_string()],
            )]),
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
        };

        normalize_observability_snapshot_scheduled_group_keys(
            &mut snapshot,
            &node_id,
            Some(&cached_grants),
        );

        assert_eq!(
            snapshot.scheduled_source_groups_by_node,
            std::collections::BTreeMap::from([(
                "node-d".to_string(),
                vec!["nfs2".to_string()],
            )]),
            "live observability normalization must fall back to cached grants when the reply omitted grants but still published instance-suffixed scheduled source groups",
        );
        assert_eq!(
            snapshot.scheduled_scan_groups_by_node,
            std::collections::BTreeMap::from([(
                "node-d".to_string(),
                vec!["nfs2".to_string()],
            )]),
            "live observability normalization must fall back to cached grants when the reply omitted grants but still published instance-suffixed scheduled scan groups",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_source_observability_normalizes_instance_suffixed_node_id_to_host_ref_for_scheduled_groups(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_watch_scan_root("nfs2", &nfs2)],
            host_object_grants: vec![worker_source_export(
                "node-d::nfs2",
                "node-d",
                "10.0.0.41",
                nfs2.clone(),
            )],
            ..SourceConfig::default()
        };
        let node_id = NodeId("node-d-29775443922859927994892289".to_string());
        let source = Arc::new(
            FSMetaSource::with_boundaries(
                cfg,
                node_id.clone(),
                Some(Arc::new(LoopbackWorkerBoundary::default())),
            )
            .expect("init source"),
        );

        source
            .on_control_frame(&[
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![bound_scope_with_resources("nfs2", &["node-d::nfs2"])],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![bound_scope_with_resources("nfs2", &["node-d::nfs2"])],
                }))
                .expect("encode scan activate"),
            ])
            .await
            .expect("apply source+scan control");

        let snapshot = SourceFacade::Local(source)
            .observability_snapshot()
            .await
            .expect("fetch local observability");
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
            !snapshot.scheduled_source_groups_by_node.contains_key(&node_id.0),
            "instance-suffixed node id should not leak into local scheduled source groups: {:?}",
            snapshot.scheduled_source_groups_by_node
        );
        assert!(
            !snapshot.scheduled_scan_groups_by_node.contains_key(&node_id.0),
            "instance-suffixed node id should not leak into local scheduled scan groups: {:?}",
            snapshot.scheduled_scan_groups_by_node
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn restarted_instance_suffixed_source_worker_recovers_schedule_from_real_source_route_wave(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-a-29775285406139598021591041".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let real_source_wave = |generation| {
            vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        async fn assert_scheduled_groups(
            client: &Arc<SourceWorkerClientHandle>,
            expected_groups: &std::collections::BTreeSet<String>,
            label: &str,
            worker_socket_dir: &Path,
        ) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let source_groups = client
                    .scheduled_source_group_ids()
                    .await
                    .expect("scheduled source groups")
                    .unwrap_or_default();
                let scan_groups = client
                    .scheduled_scan_group_ids()
                    .await
                    .expect("scheduled scan groups")
                    .unwrap_or_default();
                if &source_groups == expected_groups && &scan_groups == expected_groups {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{label}: timed out waiting for scheduled groups after real source route wave: source={source_groups:?} scan={scan_groups:?} logical_roots={:?} stderr={}",
                    client.logical_roots_snapshot().await.unwrap_or_default(),
                    worker_stderr_excerpt(worker_socket_dir),
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        client
            .on_control_frame(real_source_wave(2))
            .await
            .expect("initial real source route wave should succeed");
        assert_scheduled_groups(
            &client,
            &expected_groups,
            "initial generation",
            worker_socket_dir.path(),
        )
        .await;

        client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown source worker for restart");
        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("restarted source worker start timed out")
            .expect("restart source worker");

        client
            .on_control_frame(real_source_wave(3))
            .await
            .expect("restarted real source route wave should succeed");
        assert_scheduled_groups(
            &client,
            &expected_groups,
            "restarted generation",
            worker_socket_dir.path(),
        )
        .await;

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn restarted_external_source_worker_preserves_runtime_host_grants_for_real_source_route_wave(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-c-29775384077525007841886209".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let source_wave = |generation| {
            vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };
        let initial_control = std::iter::once(
            encode_runtime_host_grant_change(&RuntimeHostGrantChange {
                version: 1,
                grants: vec![
                    worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                    worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
                ],
            })
            .expect("encode runtime host grants changed"),
        )
        .chain(source_wave(2))
        .collect::<Vec<_>>();

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        async fn assert_scheduled_groups(
            client: &Arc<SourceWorkerClientHandle>,
            expected_groups: &std::collections::BTreeSet<String>,
            label: &str,
            worker_socket_dir: &Path,
        ) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let source_groups = client
                    .scheduled_source_group_ids()
                    .await
                    .expect("scheduled source groups")
                    .unwrap_or_default();
                let scan_groups = client
                    .scheduled_scan_group_ids()
                    .await
                    .expect("scheduled scan groups")
                    .unwrap_or_default();
                if &source_groups == expected_groups && &scan_groups == expected_groups {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{label}: timed out waiting for scheduled groups after restart-preserved host grants: source={source_groups:?} scan={scan_groups:?} logical_roots={:?} grants_version={} grants={:?} stderr={}",
                    client.logical_roots_snapshot().await.unwrap_or_default(),
                    client.host_object_grants_version_snapshot().await.unwrap_or_default(),
                    client.host_object_grants_snapshot().await.unwrap_or_default(),
                    worker_stderr_excerpt(worker_socket_dir),
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        client
            .on_control_frame(initial_control)
            .await
            .expect("initial control wave should succeed");
        assert_scheduled_groups(
            &client,
            &expected_groups,
            "initial generation",
            worker_socket_dir.path(),
        )
        .await;

        client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown source worker for restart");
        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("restarted source worker start timed out")
            .expect("restart source worker");

        client
            .on_control_frame(source_wave(3))
            .await
            .expect("restarted control wave without new host grants should succeed");
        assert_scheduled_groups(
            &client,
            &expected_groups,
            "restarted generation",
            worker_socket_dir.path(),
        )
        .await;

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn restarted_external_source_worker_preserves_multi_root_observability_after_runtime_managed_upgrade_recovery(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-c-29776120697300046443446273".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let source_wave = |generation| {
            vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };

        let initial_control = std::iter::once(
            encode_runtime_host_grant_change(&RuntimeHostGrantChange {
                version: 1,
                grants: vec![
                    worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                    worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
                ],
            })
            .expect("encode runtime host grants changed"),
        )
        .chain(source_wave(2))
        .collect::<Vec<_>>();

        let expected_snapshot = std::collections::BTreeMap::from([(
            "node-c".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]);

        async fn assert_observability(
            client: &Arc<SourceWorkerClientHandle>,
            expected_snapshot: &std::collections::BTreeMap<String, Vec<String>>,
            label: &str,
            worker_socket_dir: &Path,
        ) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let worker = client.client().await.expect("connect source worker");
                let snapshot = client
                    .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                    .await
                    .expect("observability snapshot");
                if &snapshot.scheduled_source_groups_by_node == expected_snapshot
                    && &snapshot.scheduled_scan_groups_by_node == expected_snapshot
                {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{label}: timed out waiting for multi-root observability after runtime-managed restart recovery: snapshot_source={:?} snapshot_scan={:?} grants={:?} stderr={}",
                    snapshot.scheduled_source_groups_by_node,
                    snapshot.scheduled_scan_groups_by_node,
                    client.host_object_grants_snapshot().await.unwrap_or_default(),
                    worker_stderr_excerpt(worker_socket_dir),
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        client
            .on_control_frame(initial_control)
            .await
            .expect("initial control wave should succeed");
        assert_observability(
            &client,
            &expected_snapshot,
            "initial generation",
            worker_socket_dir.path(),
        )
        .await;

        client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown source worker for restart");
        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("restarted source worker start timed out")
            .expect("restart source worker");

        client
            .on_control_frame(source_wave(3))
            .await
            .expect("restarted control wave without new host grants should succeed");
        assert_observability(
            &client,
            &expected_snapshot,
            "restarted generation",
            worker_socket_dir.path(),
        )
        .await;

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn restarted_external_source_worker_preserves_single_root_observability_after_runtime_managed_upgrade_recovery(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-b-29775497172756365788053505".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let source_wave = |generation| {
            vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
                }))
                .expect("encode source roots activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
                }))
                .expect("encode source rescan-control activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
                }))
                .expect("encode source rescan activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
                }))
                .expect("encode source scan activate"),
            ]
        };
        let initial_control = std::iter::once(
            encode_runtime_host_grant_change(&RuntimeHostGrantChange {
                version: 1,
                grants: vec![worker_route_export("node-b::nfs1", "node-b", "10.0.0.21", &nfs1)],
            })
            .expect("encode runtime host grants changed"),
        )
        .chain(source_wave(2))
        .collect::<Vec<_>>();

        let expected_groups = std::collections::BTreeSet::from(["nfs1".to_string()]);
        async fn assert_schedule_and_observability(
            client: &Arc<SourceWorkerClientHandle>,
            expected_groups: &std::collections::BTreeSet<String>,
            expected_snapshot: &std::collections::BTreeMap<String, Vec<String>>,
            label: &str,
            worker_socket_dir: &Path,
        ) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let source_groups = client
                    .scheduled_source_group_ids()
                    .await
                    .expect("scheduled source groups")
                    .unwrap_or_default();
                let scan_groups = client
                    .scheduled_scan_group_ids()
                    .await
                    .expect("scheduled scan groups")
                    .unwrap_or_default();
                let worker = client.client().await.expect("connect source worker");
                let snapshot = client
                    .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                    .await
                    .expect("observability snapshot");
                if &source_groups == expected_groups
                    && &scan_groups == expected_groups
                    && &snapshot.scheduled_source_groups_by_node == expected_snapshot
                    && &snapshot.scheduled_scan_groups_by_node == expected_snapshot
                {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{label}: timed out waiting for schedule+observability convergence after runtime-managed restart recovery: source={source_groups:?} scan={scan_groups:?} snapshot_source={:?} snapshot_scan={:?} grants={:?} stderr={}",
                    snapshot.scheduled_source_groups_by_node,
                    snapshot.scheduled_scan_groups_by_node,
                    client.host_object_grants_snapshot().await.unwrap_or_default(),
                    worker_stderr_excerpt(worker_socket_dir),
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        client
            .on_control_frame(initial_control)
            .await
            .expect("initial control wave should succeed");
        assert_schedule_and_observability(
            &client,
            &expected_groups,
            &std::collections::BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string()],
            )]),
            "initial generation",
            worker_socket_dir.path(),
        )
        .await;

        client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown source worker for restart");
        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("restarted source worker start timed out")
            .expect("restart source worker");

        client
            .on_control_frame(source_wave(3))
            .await
            .expect("restarted control wave without new host grants should succeed");
        assert_schedule_and_observability(
            &client,
            &expected_groups,
            &std::collections::BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string()],
            )]),
            "restarted generation",
            worker_socket_dir.path(),
        )
        .await;

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn restarted_external_source_worker_cache_fallback_preserves_stable_host_ref_after_runtime_managed_upgrade_recovery(
    ) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let node_id = NodeId("node-b-29775497172756365788053505".to_string());
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                node_id.clone(),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let source_wave = |generation| {
            vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
                }))
                .expect("encode source roots activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
                }))
                .expect("encode source rescan-control activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
                }))
                .expect("encode source rescan activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
                }))
                .expect("encode source scan activate"),
            ]
        };
        let initial_control = std::iter::once(
            encode_runtime_host_grant_change(&RuntimeHostGrantChange {
                version: 1,
                grants: vec![worker_route_export("node-b::nfs1", "node-b", "10.0.0.21", &nfs1)],
            })
            .expect("encode runtime host grants changed"),
        )
        .chain(source_wave(2))
        .collect::<Vec<_>>();

        client
            .on_control_frame(initial_control)
            .await
            .expect("initial control wave should succeed");

        client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown source worker for restart");
        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("restarted source worker start timed out")
            .expect("restart source worker");

        client
            .on_control_frame(source_wave(3))
            .await
            .expect("restarted control wave without new host grants should succeed");

        let inflight = client.begin_control_op();
        let degraded = client.observability_snapshot_nonblocking().await;
        drop(inflight);

        let expected = vec!["nfs1".to_string()];
        assert_eq!(
            degraded.scheduled_source_groups_by_node.get("node-b"),
            Some(&expected),
            "cache fallback should preserve stable host_ref-keyed source schedule after runtime-managed upgrade recovery: {:?}",
            degraded.scheduled_source_groups_by_node
        );
        assert_eq!(
            degraded.scheduled_scan_groups_by_node.get("node-b"),
            Some(&expected),
            "cache fallback should preserve stable host_ref-keyed scan schedule after runtime-managed upgrade recovery: {:?}",
            degraded.scheduled_scan_groups_by_node
        );
        assert!(
            !degraded
                .scheduled_source_groups_by_node
                .contains_key(&node_id.0),
            "instance-suffixed node id should not leak into cached degraded source schedule: {:?}",
            degraded.scheduled_source_groups_by_node
        );
        assert!(
            !degraded
                .scheduled_scan_groups_by_node
                .contains_key(&node_id.0),
            "instance-suffixed node id should not leak into cached degraded scan schedule: {:?}",
            degraded.scheduled_scan_groups_by_node
        );

        client.close().await.expect("close source worker");
    }

    fn worker_stderr_excerpt(socket_dir: &Path) -> String {
        let mut excerpts = std::fs::read_dir(socket_dir)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok().map(|row| row.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".stderr.log"))
            })
            .filter_map(|path| {
                std::fs::read_to_string(&path)
                    .ok()
                    .map(|text| format!("{}:\n{}", path.display(), text))
            })
            .collect::<Vec<_>>();
        excerpts.sort();
        excerpts.join("\n---\n")
    }
}
