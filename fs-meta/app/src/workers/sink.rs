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
const SINK_WORKER_CONTROL_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const SINK_WORKER_UPDATE_ROOTS_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const SINK_WORKER_FORCE_FIND_TIMEOUT: Duration = Duration::from_secs(60);
const SINK_WORKER_FORCE_FIND_REPLY_IDLE_GRACE: Duration = Duration::from_secs(5);
const SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT: Duration = Duration::from_secs(60);
const SINK_WORKER_CLOSE_DRAIN_TIMEOUT: Duration = SINK_WORKER_UPDATE_ROOTS_RPC_TIMEOUT;
const SINK_WORKER_CLOSE_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(25);

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
            CnxError::AccessDenied(message) | CnxError::PeerError(message)
                if message.contains("drained/fenced")
                    && message.contains("grant attachments")
                    || message.contains("invalid or revoked grant attachment token")
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
fn take_sink_worker_status_error_hook() -> Option<CnxError> {
    let mut guard = match sink_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
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

    fn update_cached_scheduled_group_ids(
        &self,
        groups: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        let mut guard = self.scheduled_groups_cache.lock().map_err(|_| {
            CnxError::Internal("sink worker scheduled groups cache lock poisoned".into())
        })?;
        *guard = Some(groups.clone());
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

    async fn reconnect_shared_worker_client(&self) -> Result<()> {
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
        tokio::spawn(async move {
            let _ = stale_client.shutdown(Duration::from_millis(250)).await;
        });
        Ok(())
    }

    async fn reset_shared_worker_client_for_retry(&self) -> Result<()> {
        #[cfg(test)]
        notify_sink_worker_retry_reset();
        self.reconnect_shared_worker_client().await
    }

    async fn retain_control_signals(&self, signals: &[SinkControlSignal]) {
        let mut retained = self.retained_control_state.lock().await;
        for signal in signals {
            match signal {
                SinkControlSignal::RuntimeHostGrantChange { .. } => {
                    retained.latest_host_grant_change = Some(signal.clone());
                }
                SinkControlSignal::Activate {
                    unit, route_key, ..
                } => {
                    retained
                        .active_by_route
                        .insert((unit.unit_id().to_string(), route_key.clone()), signal.clone());
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
    }

    async fn replay_retained_control_state_if_needed(&self) -> Result<()> {
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
            envelopes.extend(retained.active_by_route.values().map(SinkControlSignal::envelope));
            envelopes
        };
        if envelopes.is_empty() {
            return Ok(());
        }
        if let Err(err) = self
            .on_control_frame_with_timeouts(
                envelopes,
                SINK_WORKER_CONTROL_TOTAL_TIMEOUT,
                SINK_WORKER_CONTROL_RPC_TIMEOUT,
            )
            .await
        {
            self.control_state_replay_required
                .store(1, Ordering::Release);
            return Err(err);
        }
        Ok(())
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
        self.update_cached_status_snapshot(SinkStatusSnapshot::default())
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
        if debug_control_scope_capture_enabled() {
            eprintln!(
                "fs_meta_sink_worker_client: status_snapshot reply node={} {}",
                self.node_id.0,
                summarize_sink_status_snapshot(&snapshot)
            );
        }
        self.update_cached_status_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }

    pub async fn status_snapshot_nonblocking(&self) -> Result<SinkStatusSnapshot> {
        fn snapshot_looks_stale_empty(snapshot: &SinkStatusSnapshot) -> bool {
            snapshot.scheduled_groups_by_node.is_empty()
                && snapshot.groups.iter().all(|group| {
                    !group.initial_audit_completed
                        && group.live_nodes == 0
                        && group.total_nodes == 0
                })
        }

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
            if snapshot_looks_stale_empty(&snapshot) {
                if self.existing_client().await?.is_some() {
                    if let Ok(mut live_snapshot) = self
                        .status_snapshot_with_timeout(Duration::from_millis(250))
                        .await
                    {
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
                }
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
                self.replay_retained_control_state_if_needed().await?;
                match self
                    .status_snapshot_with_timeout(Duration::from_secs(5))
                    .await
                {
                Ok(mut snapshot) => {
                    let grants = self
                        .config
                        .lock()
                        .map_err(|_| CnxError::Internal("sink worker config lock poisoned".into()))?
                        .host_object_grants
                        .clone();
                    normalize_sink_status_snapshot_node_keys(&mut snapshot, &self.node_id, &grants);
                    if debug_control_scope_capture_enabled() {
                        eprintln!(
                            "fs_meta_sink_worker_client: status_snapshot reply node={} {}",
                            self.node_id.0,
                            summarize_sink_status_snapshot(&snapshot)
                        );
                    }
                    self.update_cached_status_snapshot(snapshot.clone())?;
                    Ok(snapshot)
                }
                    Err(err) => {
                    let snapshot = self.cached_status_snapshot()?;
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
            None => {
                let snapshot = self.cached_status_snapshot()?;
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

    async fn status_snapshot_with_timeout(&self, timeout: Duration) -> Result<SinkStatusSnapshot> {
        let deadline = std::time::Instant::now() + timeout;
        let response = loop {
            let now = std::time::Instant::now();
            let attempt_timeout = timeout.min(deadline.saturating_duration_since(now));
            if attempt_timeout.is_zero() {
                return Err(CnxError::Timeout);
            }
            self.replay_retained_control_state_if_needed().await?;
            let rpc_result = self
                .with_started_retry(|client| async move {
                    #[cfg(test)]
                    if let Some(err) = take_sink_worker_status_error_hook() {
                        return Err(err);
                    }
                    Self::call_worker(&client, SinkWorkerRequest::StatusSnapshot, attempt_timeout)
                        .await
                })
                .await;
            match rpc_result {
                Ok(response) => break response,
                Err(err)
                    if can_retry_on_control_frame(&err) && std::time::Instant::now() < deadline =>
                {
                    self.reset_shared_worker_client_for_retry().await?;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(err) => return Err(err),
            }
        };
        match response {
            SinkWorkerResponse::StatusSnapshot(snapshot) => Ok(snapshot),
            other => Err(CnxError::ProtocolViolation(format!(
                "unexpected sink worker response for status snapshot: {:?}",
                other
            ))),
        }
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
                        self.retain_control_signals(signals).await;
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
                    self.reset_shared_worker_client_for_retry().await?;
                    tokio::time::sleep(Duration::from_millis(100)).await;
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
mod tests {
    use super::*;
    use async_trait::async_trait;
    use capanix_app_sdk::raw::{
        BoundaryContext, ChannelBoundary, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
        StateBoundary,
    };
    use capanix_app_sdk::runtime::{
        LogLevel, RuntimeWorkerBinding, RuntimeWorkerLauncherKind, in_memory_state_boundary,
    };
    use capanix_app_sdk::worker::WorkerMode;
    use capanix_runtime_entry_sdk::control::{
        RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeExecDeactivate,
        encode_runtime_exec_control,
    };
    use futures_util::StreamExt;
    use std::collections::{HashMap, HashSet};
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex as StdMutex, OnceLock};
    use tempfile::tempdir;
    use tokio::sync::{Mutex as AsyncMutex, Notify};
    use tokio::time::Duration;

    use crate::runtime::routes::{
        METHOD_SINK_QUERY, ROUTE_KEY_EVENTS, ROUTE_KEY_QUERY, ROUTE_TOKEN_FS_META_INTERNAL,
        default_route_bindings,
    };
    use crate::source::FSMetaSource;
    use crate::source::config::RootSpec;

    #[derive(Default)]
    struct LoopbackWorkerBoundary {
        channels: AsyncMutex<HashMap<String, Vec<Event>>>,
        closed: StdMutex<HashSet<String>>,
        send_batches_by_channel: StdMutex<HashMap<String, usize>>,
        recv_batches_by_channel: StdMutex<HashMap<String, usize>>,
        changed: Notify,
    }

    impl LoopbackWorkerBoundary {
        fn send_batch_count(&self, channel: &str) -> usize {
            *self
                .send_batches_by_channel
                .lock()
                .expect("loopback send batches lock")
                .get(channel)
                .unwrap_or(&0)
        }

        fn recv_batch_count(&self, channel: &str) -> usize {
            *self
                .recv_batches_by_channel
                .lock()
                .expect("loopback recv batches lock")
                .get(channel)
                .unwrap_or(&0)
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
                let mut send_batches = self
                    .send_batches_by_channel
                    .lock()
                    .expect("loopback send batches lock");
                *send_batches
                    .entry(request.channel_key.0.clone())
                    .or_default() += 1;
            }
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
                        let mut recv_batches = self
                            .recv_batches_by_channel
                            .lock()
                            .expect("loopback recv batches lock");
                        *recv_batches
                            .entry(request.channel_key.0.clone())
                            .or_default() += 1;
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

    fn external_sink_worker_binding(socket_dir: &Path) -> RuntimeWorkerBinding {
        RuntimeWorkerBinding {
            role_id: "sink".to_string(),
            mode: WorkerMode::External,
            launcher_kind: RuntimeWorkerLauncherKind::WorkerHost,
            module_path: Some(fs_meta_worker_module_path()),
            socket_dir: Some(socket_dir.to_path_buf()),
        }
    }

    fn sink_worker_root(id: &str, path: &Path) -> RootSpec {
        let mut root = RootSpec::new(id, path);
        root.watch = false;
        root.scan = true;
        root
    }

    fn sink_worker_export(
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

    fn test_worker_control_route_key_for(role_id: &str, node_id: &str) -> String {
        let normalize = |raw: &str| -> String {
            raw.chars()
                .map(|ch| {
                    if ch.is_ascii_alphanumeric() {
                        ch.to_ascii_lowercase()
                    } else {
                        '_'
                    }
                })
                .collect()
        };
        format!(
            "capanix.worker.{}.{}.rpc:v1",
            normalize(role_id),
            normalize(node_id)
        )
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

    fn bound_scope_with_resources(scope_id: &str, resource_ids: &[&str]) -> RuntimeBoundScope {
        RuntimeBoundScope {
            scope_id: scope_id.to_string(),
            resource_ids: resource_ids.iter().map(|id| (*id).to_string()).collect(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overlapping_external_sink_worker_handles_for_same_binding_keep_one_worker_lane() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                sink_worker_root("nfs1", &nfs1),
                sink_worker_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                sink_worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                sink_worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let binding = external_sink_worker_binding(worker_socket_dir.path());
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let first = Arc::new(
            SinkWorkerClientHandle::new(
                NodeId("node-a".to_string()),
                cfg.clone(),
                binding.clone(),
                factory.clone(),
            )
            .expect("construct first sink worker client"),
        );
        let second = Arc::new(
            SinkWorkerClientHandle::new(NodeId("node-a".to_string()), cfg, binding, factory)
                .expect("construct second sink worker client"),
        );
        let envelopes = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode sink activate"),
        ];

        let first_task = {
            let first = first.clone();
            let envelopes = envelopes.clone();
            tokio::spawn(async move { first.on_control_frame(envelopes).await })
        };
        let second_task = {
            let second = second.clone();
            let envelopes = envelopes.clone();
            tokio::spawn(async move { second.on_control_frame(envelopes).await })
        };

        tokio::time::timeout(Duration::from_secs(20), async {
            first_task.await.expect("join first activation task")
        })
        .await
        .expect("first activation timed out")
        .expect("first activation should succeed without spawning a conflicting worker");
        tokio::time::timeout(Duration::from_secs(20), async {
            second_task.await.expect("join second activation task")
        })
        .await
        .expect("second activation timed out")
        .expect("second activation should share the same worker lane");

        let first_status = first.status_snapshot().await.expect("first status");
        let second_status = second.status_snapshot().await.expect("second status");
        assert_eq!(
            first_status.scheduled_groups_by_node, second_status.scheduled_groups_by_node,
            "same-binding sink worker handles should observe one shared external worker state",
        );

        first.close().await.expect("close first sink worker handle");
        second
            .close()
            .await
            .expect("close second sink worker handle");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_sink_worker_materializes_each_local_primary_root_from_source_batches() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                sink_worker_root("nfs1", &nfs1),
                sink_worker_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                sink_worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                sink_worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
            .expect("init source");
        let mut stream = source.pub_().await.expect("start source pub stream");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < initial_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => sink.send(batch).await.expect("apply initial batch"),
                Ok(None) => break,
                Err(_) => continue,
            }
            let nfs1_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1"),
                b"/force-find-stress",
            )
            .expect("decode nfs1")
            .is_some();
            let nfs2_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                    .await
                    .expect("query nfs2"),
                b"/force-find-stress",
            )
            .expect("decode nfs2")
            .is_some();
            if nfs1_ready && nfs2_ready {
                break;
            }
        }

        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1 after initial"),
                b"/force-find-stress",
            )
            .expect("decode nfs1 after initial")
            .is_some(),
            "nfs1 initial materialization should exist",
        );
        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                    .await
                    .expect("query nfs2 after initial"),
                b"/force-find-stress",
            )
            .expect("decode nfs2 after initial")
            .is_some(),
            "nfs2 initial materialization should exist",
        );

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
        source
            .publish_manual_rescan_signal()
            .await
            .expect("publish manual rescan");

        let rescan_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < rescan_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => sink.send(batch).await.expect("apply rescan batch"),
                Ok(None) => break,
                Err(_) => continue,
            }
            let nfs1_done = decode_exact_query_node(
                sink.materialized_query(selected_group_request(
                    b"/force-find-stress/after-rescan.txt",
                    "nfs1",
                ))
                .await
                .expect("query nfs1 rescan"),
                b"/force-find-stress/after-rescan.txt",
            )
            .expect("decode nfs1 rescan")
            .is_some();
            let nfs2_done = decode_exact_query_node(
                sink.materialized_query(selected_group_request(
                    b"/force-find-stress/after-rescan.txt",
                    "nfs2",
                ))
                .await
                .expect("query nfs2 rescan"),
                b"/force-find-stress/after-rescan.txt",
            )
            .expect("decode nfs2 rescan")
            .is_some();
            if nfs1_done && nfs2_done {
                break;
            }
        }

        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(
                    b"/force-find-stress/after-rescan.txt",
                    "nfs1",
                ))
                .await
                .expect("query nfs1 final"),
                b"/force-find-stress/after-rescan.txt",
            )
            .expect("decode nfs1 final")
            .is_some(),
            "nfs1 should materialize its post-rescan file",
        );
        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(
                    b"/force-find-stress/after-rescan.txt",
                    "nfs2",
                ))
                .await
                .expect("query nfs2 final"),
                b"/force-find-stress/after-rescan.txt",
            )
            .expect("decode nfs2 final")
            .is_some(),
            "nfs2 should materialize its post-rescan file",
        );

        source.close().await.expect("close source");
        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_sink_worker_schedules_group_before_it_materializes_until_batches_arrive() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                sink_worker_root("nfs1", &nfs1),
                sink_worker_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                sink_worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                sink_worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
            .expect("init source");
        let mut stream = source.pub_().await.expect("start source pub stream");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");
        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
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
        .expect("activate sink groups");

        let selected_file = b"/force-find-stress/seed.txt";
        let mut deferred_nfs2_batches = Vec::<Vec<Event>>::new();
        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < initial_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => {
                    let mut nfs1_batch = Vec::new();
                    let mut nfs2_batch = Vec::new();
                    for event in batch {
                        match event.metadata().origin_id.0.as_str() {
                            "node-a::nfs1" => nfs1_batch.push(event),
                            "node-a::nfs2" => nfs2_batch.push(event),
                            _ => {}
                        }
                    }
                    if !nfs2_batch.is_empty() {
                        deferred_nfs2_batches.push(nfs2_batch);
                    }
                    if !nfs1_batch.is_empty() {
                        sink.send(nfs1_batch).await.expect("apply nfs1-only batch");
                    }
                }
                Ok(None) => break,
                Err(_) => continue,
            }
            let nfs1_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs1"))
                    .await
                    .expect("query nfs1 while nfs2 withheld"),
                selected_file,
            )
            .expect("decode nfs1 while nfs2 withheld")
            .is_some();
            if nfs1_ready {
                break;
            }
        }

        let status = sink
            .status_snapshot()
            .await
            .expect("status snapshot after nfs1-only send");
        assert_eq!(
            status
                .scheduled_groups_by_node
                .get("node-a")
                .cloned()
                .unwrap_or_default(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
            "scheduled group coverage should reflect both roots before nfs2 materializes",
        );
        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs1"))
                    .await
                    .expect("query nfs1 after nfs1-only send"),
                selected_file,
            )
            .expect("decode nfs1 after nfs1-only send")
            .is_some(),
            "nfs1 should materialize once its batches are sent",
        );
        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                    .await
                    .expect("query nfs2 before batches arrive"),
                selected_file,
            )
            .expect("decode nfs2 before batches arrive")
            .is_none(),
            "nfs2 should stay empty until its own batches are sent",
        );

        for batch in deferred_nfs2_batches.drain(..) {
            sink.send(batch).await.expect("apply deferred nfs2 batch");
        }
        let nfs2_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < nfs2_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => {
                    let nfs2_batch = batch
                        .into_iter()
                        .filter(|event| event.metadata().origin_id.0 == "node-a::nfs2")
                        .collect::<Vec<_>>();
                    if !nfs2_batch.is_empty() {
                        sink.send(nfs2_batch)
                            .await
                            .expect("apply remaining nfs2 batch");
                    }
                }
                Ok(None) => {}
                Err(_) => continue,
            }
            let nfs2_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                    .await
                    .expect("query nfs2 after deferred batches"),
                selected_file,
            )
            .expect("decode nfs2 after deferred batches")
            .is_some();
            if nfs2_ready {
                break;
            }
        }

        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                    .await
                    .expect("query nfs2 final"),
                selected_file,
            )
            .expect("decode nfs2 final")
            .is_some(),
            "nfs2 should materialize on the same sink-worker seam once its batches arrive",
        );

        source.close().await.expect("close source");
        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_sink_worker_internal_materialized_route_delivers_sequential_same_owner_queries_twice()
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
                sink_worker_root("nfs1", &nfs1),
                sink_worker_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                sink_worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                sink_worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
            .expect("init source");
        let mut stream = source.pub_().await.expect("start source pub stream");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");
        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
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
        .expect("activate sink groups");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let scheduled = sink
                .scheduled_group_ids()
                .await
                .expect("scheduled groups")
                .unwrap_or_default();
            if scheduled == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < schedule_deadline,
                "timed out waiting for scheduled groups: scheduled={scheduled:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let selected_dir = b"/force-find-stress";
        let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        while tokio::time::Instant::now() < materialized_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
                Ok(None) => break,
                Err(_) => continue,
            }
            let nfs1_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_dir, "nfs1"))
                    .await
                    .expect("query nfs1"),
                selected_dir,
            )
            .expect("decode nfs1")
            .is_some();
            let nfs2_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_dir, "nfs2"))
                    .await
                    .expect("query nfs2"),
                selected_dir,
            )
            .expect("decode nfs2")
            .is_some();
            if nfs1_ready && nfs2_ready {
                break;
            }
        }

        let adapter = exchange_host_adapter_from_channel_boundary(
            boundary.clone(),
            NodeId("node-d".to_string()),
            default_route_bindings(),
        );
        let route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY)
            .expect("resolve internal sink query route");
        let reply_route = format!("{}:reply", route.0);
        let first_events = adapter
            .call_collect(
                ROUTE_TOKEN_FS_META_INTERNAL,
                METHOD_SINK_QUERY,
                Bytes::from(
                    rmp_serde::to_vec(&selected_group_request(selected_dir, "nfs1"))
                        .expect("encode nfs1 internal query"),
                ),
                Duration::from_secs(5),
                Duration::from_millis(250),
            )
            .await;
        let second_events = adapter
            .call_collect(
                ROUTE_TOKEN_FS_META_INTERNAL,
                METHOD_SINK_QUERY,
                Bytes::from(
                    rmp_serde::to_vec(&selected_group_request(selected_dir, "nfs2"))
                        .expect("encode nfs2 internal query"),
                ),
                Duration::from_secs(5),
                Duration::from_millis(250),
            )
            .await;

        assert!(
            first_events.is_ok(),
            "first direct internal sink-query route should complete; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={} err={:?}",
            boundary.send_batch_count(&route.0),
            boundary.recv_batch_count(&route.0),
            boundary.send_batch_count(&reply_route),
            boundary.recv_batch_count(&reply_route),
            first_events.as_ref().err(),
        );
        assert!(
            second_events.is_ok(),
            "second direct internal sink-query route should complete; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={} err={:?}",
            boundary.send_batch_count(&route.0),
            boundary.recv_batch_count(&route.0),
            boundary.send_batch_count(&reply_route),
            boundary.recv_batch_count(&reply_route),
            second_events.as_ref().err(),
        );

        let first_node = decode_exact_query_node(
            first_events.expect("first direct internal query result"),
            selected_dir,
        )
        .expect("decode first direct internal query");
        let second_node = decode_exact_query_node(
            second_events.expect("second direct internal query result"),
            selected_dir,
        )
        .expect("decode second direct internal query");

        assert!(
            first_node.is_some(),
            "first direct internal sink-query route should return the owner materialized subtree"
        );
        assert!(
            second_node.is_some(),
            "second direct internal sink-query route should return the owner materialized subtree"
        );
        assert_eq!(
            boundary.send_batch_count(&route.0),
            2,
            "both sequential internal sink-query calls should send one request batch each"
        );
        assert_eq!(
            boundary.recv_batch_count(&route.0),
            2,
            "owner runtime endpoint should receive both internal sink-query request batches"
        );
        assert_eq!(
            boundary.send_batch_count(&reply_route),
            2,
            "owner runtime endpoint should send one reply batch for each internal sink-query call"
        );
        assert_eq!(
            boundary.recv_batch_count(&reply_route),
            2,
            "caller should receive one reply batch for each internal sink-query call"
        );

        source.close().await.expect("close source");
        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_sink_worker_internal_materialized_route_serves_local_owner_payload_while_control_inflight()
    {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![sink_worker_export(
                "node-a::nfs1",
                "node-a",
                "10.0.0.11",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
            .expect("init source");
        let mut stream = source.pub_().await.expect("start source pub stream");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");
        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-a::nfs1"])],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("activate sink group");

        let selected_dir = b"/force-find-stress";
        let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        while tokio::time::Instant::now() < materialized_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
                Ok(None) => break,
                Err(_) => continue,
            }
            let ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(selected_dir, "nfs1"))
                    .await
                    .expect("query nfs1"),
                selected_dir,
            )
            .expect("decode nfs1")
            .is_some();
            if ready {
                break;
            }
        }

        let adapter = exchange_host_adapter_from_channel_boundary(
            boundary.clone(),
            NodeId("node-d".to_string()),
            default_route_bindings(),
        );
        let route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY)
            .expect("resolve internal sink query route");
        let reply_route = format!("{}:reply", route.0);
        let baseline = adapter
            .call_collect(
                ROUTE_TOKEN_FS_META_INTERNAL,
                METHOD_SINK_QUERY,
                Bytes::from(
                    rmp_serde::to_vec(&selected_group_request(selected_dir, "nfs1"))
                        .expect("encode baseline internal query"),
                ),
                Duration::from_secs(5),
                Duration::from_millis(250),
            )
            .await
            .expect("baseline direct internal sink-query route should complete");
        assert!(
            decode_exact_query_node(baseline, selected_dir)
                .expect("decode baseline internal query")
                .is_some(),
            "baseline direct internal sink-query route should return the owner materialized subtree"
        );

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _reset = SinkWorkerControlFramePauseHookReset;
        install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let inflight_control = tokio::spawn({
            let sink = sink.clone();
            async move {
                sink.on_control_frame_with_timeouts_for_tests(
                    vec![
                        encode_runtime_exec_control(&RuntimeExecControl::Activate(
                            RuntimeExecActivate {
                                route_key: ROUTE_KEY_QUERY.to_string(),
                                unit_id: "runtime.exec.sink".to_string(),
                                lease: None,
                                generation: 2,
                                expires_at_ms: 1,
                                bound_scopes: vec![bound_scope_with_resources(
                                    "nfs1",
                                    &["node-a::nfs1"],
                                )],
                            },
                        ))
                        .expect("encode second-wave sink activate"),
                    ],
                    Duration::from_secs(2),
                    Duration::from_secs(2),
                )
                .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("sink control should reach pause point");

        let inflight_events = tokio::time::timeout(
            Duration::from_millis(800),
            adapter.call_collect(
                ROUTE_TOKEN_FS_META_INTERNAL,
                METHOD_SINK_QUERY,
                Bytes::from(
                    rmp_serde::to_vec(&selected_group_request(selected_dir, "nfs1"))
                        .expect("encode inflight internal query"),
                ),
                Duration::from_secs(5),
                Duration::from_millis(250),
            ),
        )
        .await
        .expect("direct internal sink-query route should still settle while sink control is in flight")
        .expect("inflight direct internal sink-query route");

        assert!(
            decode_exact_query_node(inflight_events, selected_dir)
                .expect("decode inflight internal query")
                .is_some(),
            "direct internal sink-query route during sink control inflight must still return the owner materialized subtree; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={}",
            boundary.send_batch_count(&route.0),
            boundary.recv_batch_count(&route.0),
            boundary.send_batch_count(&reply_route),
            boundary.recv_batch_count(&reply_route),
        );

        release.notify_waiters();
        let _ = inflight_control.await.expect("join inflight control");

        source.close().await.expect("close source");
        sink.close().await.expect("close sink worker");
    }

    struct SinkWorkerUpdateRootsHookReset;

    impl Drop for SinkWorkerUpdateRootsHookReset {
        fn drop(&mut self) {
            clear_sink_worker_update_roots_hook();
        }
    }

    struct SinkWorkerControlFrameErrorHookReset;

    impl Drop for SinkWorkerControlFrameErrorHookReset {
        fn drop(&mut self) {
            clear_sink_worker_control_frame_error_hook();
        }
    }

    struct SinkWorkerControlFramePauseHookReset;

    impl Drop for SinkWorkerControlFramePauseHookReset {
        fn drop(&mut self) {
            clear_sink_worker_control_frame_pause_hook();
        }
    }

    struct SinkWorkerStatusErrorHookReset;

    impl Drop for SinkWorkerStatusErrorHookReset {
        fn drop(&mut self) {
            clear_sink_worker_status_error_hook();
        }
    }

    struct SinkWorkerScheduledGroupsErrorHookReset;

    impl Drop for SinkWorkerScheduledGroupsErrorHookReset {
        fn drop(&mut self) {
            clear_sink_worker_scheduled_groups_error_hook();
        }
    }

    struct SinkWorkerRetryResetHookReset;

    impl Drop for SinkWorkerRetryResetHookReset {
        fn drop(&mut self) {
            clear_sink_worker_retry_reset_hook();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn materialized_query_nonblocking_does_not_dispatch_worker_rpc_while_control_inflight() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![sink_worker_export(
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
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-d::nfs1"])],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("activate sink query route");

        let worker_route = test_worker_control_route_key_for("sink", "node-d");
        let baseline_send_batches = boundary.send_batch_count(&worker_route);
        let _inflight = sink.begin_control_op();

        let events = sink
            .materialized_query_nonblocking(selected_group_request(b"/", "nfs1"))
            .await
            .expect(
                "materialized_query_nonblocking should fail closed from the local nonblocking path while sink worker control is already in flight",
            );
        assert!(
            events.is_empty(),
            "nonblocking materialized query during control inflight should fail closed with an empty result instead of dispatching another worker rpc: {events:?}"
        );
        assert_eq!(
            boundary.send_batch_count(&worker_route),
            baseline_send_batches,
            "materialized_query_nonblocking must not dispatch a worker rpc while sink worker control is already in flight"
        );

        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn status_snapshot_nonblocking_does_not_return_stale_empty_cache_after_materialization_when_control_is_marked_inflight()
    {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![sink_worker_export(
                "node-d::nfs1",
                "node-d",
                "10.0.0.41",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
            .expect("init source");
        let mut stream = source.pub_().await.expect("start source pub stream");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        let primed = sink
            .status_snapshot()
            .await
            .expect("prime initial empty cached status snapshot");
        assert!(
            primed.scheduled_groups_by_node.is_empty(),
            "precondition: initial cached snapshot should start empty"
        );

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-d::nfs1"])],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("activate sink query route");

        let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < materialized_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
                Ok(None) => break,
                Err(_) => continue,
            }
            let ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1"),
                b"/force-find-stress",
            )
            .expect("decode nfs1")
            .is_some();
            if ready {
                break;
            }
        }

        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1 after materialization"),
                b"/force-find-stress",
            )
            .expect("decode nfs1 after materialization")
            .is_some(),
            "precondition: owner-local materialized query should already see the ready subtree"
        );

        let _inflight = sink.begin_control_op();
        let snapshot = sink
            .status_snapshot_nonblocking()
            .await
            .expect("status_snapshot_nonblocking during synthetic control inflight");

        assert_eq!(
            snapshot.scheduled_groups_by_node.get("node-d"),
            Some(&vec!["nfs1".to_string()]),
            "status_snapshot_nonblocking must not regress to the stale empty pre-activate cache after the sink already materialized nfs1"
        );
        assert!(
            snapshot
                .groups
                .iter()
                .find(|group| group.group_id == "nfs1")
                .is_some_and(|group| group.initial_audit_completed && group.live_nodes > 0),
            "status_snapshot_nonblocking must keep the ready materialized group visible even while control is merely marked inflight: {snapshot:?}"
        );

        source.close().await.expect("close source");
        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn materialized_query_still_reads_local_payload_while_control_inflight() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![sink_worker_export(
                "node-d::nfs1",
                "node-d",
                "10.0.0.41",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
            .expect("init source");
        let mut stream = source.pub_().await.expect("start source pub stream");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-d::nfs1"])],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("activate sink query route");

        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < initial_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => sink.send(batch).await.expect("apply initial batch"),
                Ok(None) => break,
                Err(_) => continue,
            }
            let ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1"),
                b"/force-find-stress",
            )
            .expect("decode nfs1")
            .is_some();
            if ready {
                break;
            }
        }

        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1 after initial"),
                b"/force-find-stress",
            )
            .expect("decode nfs1 after initial")
            .is_some(),
            "initial materialization should exist before sink control pauses"
        );

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _reset = SinkWorkerControlFramePauseHookReset;
        install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let inflight_control = tokio::spawn({
            let sink = sink.clone();
            async move {
                sink.on_control_frame_with_timeouts_for_tests(
                    vec![
                        encode_runtime_exec_control(&RuntimeExecControl::Activate(
                            RuntimeExecActivate {
                                route_key: ROUTE_KEY_QUERY.to_string(),
                                unit_id: "runtime.exec.sink".to_string(),
                                lease: None,
                                generation: 3,
                                expires_at_ms: 1,
                                bound_scopes: vec![bound_scope_with_resources(
                                    "nfs1",
                                    &["node-d::nfs1"],
                                )],
                            },
                        ))
                        .expect("encode second-wave sink activate"),
                    ],
                    Duration::from_secs(2),
                    Duration::from_secs(2),
                )
                .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("sink control should reach pause point");

        let query = tokio::time::timeout(
            Duration::from_millis(800),
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1")),
        )
        .await;

        release.notify_waiters();
        let _ = inflight_control.await.expect("join inflight control");

        let events = query
            .expect(
                "blocking materialized_query should still settle while sink control is in flight",
            )
            .expect("blocking materialized_query during control inflight");
        assert!(
            decode_exact_query_node(events, b"/force-find-stress")
                .expect("decode query during inflight")
                .is_some(),
            "blocking materialized_query during sink control inflight must still return the last local materialized payload"
        );

        source.close().await.expect("close source");
        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_stale_drained_fenced_pid_errors() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        let _reset = SinkWorkerControlFrameErrorHookReset;
        install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect(
            "on_control_frame should retry a stale drained/fenced pid error and reach the live sink worker",
        );

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let scheduled = sink
                .scheduled_group_ids()
                .await
                .expect("scheduled groups")
                .unwrap_or_default();
            if scheduled == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < schedule_deadline,
                "timed out waiting for scheduled groups after retry: scheduled={scheduled:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_stale_drained_fenced_pid_errors_after_first_wave_succeeded() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 force-find dir");
        std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 force-find dir");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a")
            .expect("seed nfs1");
        std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b")
            .expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                sink_worker_root("nfs1", &nfs1),
                sink_worker_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
            .expect("init source");
        let mut stream = source.pub_().await.expect("start source pub stream");
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("first sink control wave should succeed");

        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < initial_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => sink.send(batch).await.expect("apply initial batch"),
                Ok(None) => break,
                Err(_) => continue,
            }
            let nfs1_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1"),
                b"/force-find-stress",
            )
            .expect("decode nfs1")
            .is_some();
            let nfs2_ready = decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                    .await
                    .expect("query nfs2"),
                b"/force-find-stress",
            )
            .expect("decode nfs2")
            .is_some();
            if nfs1_ready && nfs2_ready {
                break;
            }
        }

        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1 after initial"),
                b"/force-find-stress",
            )
            .expect("decode nfs1 after initial")
            .is_some(),
            "precondition: nfs1 initial materialization should exist before stale drained/fenced retry"
        );
        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                    .await
                    .expect("query nfs2 after initial"),
                b"/force-find-stress",
            )
            .expect("decode nfs2 after initial")
            .is_some(),
            "precondition: nfs2 initial materialization should exist before stale drained/fenced retry"
        );

        let _reset = SinkWorkerControlFrameErrorHookReset;
        install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
                route_key: ROUTE_KEY_EVENTS.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 3,
                reason: "test deactivate".to_string(),
            }))
            .expect("encode sink deactivate"),
        ])
        .await
        .expect(
            "on_control_frame should retry a stale drained/fenced pid error after the first sink wave already succeeded",
        );

        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1 after drained/fenced retry"),
                b"/force-find-stress",
            )
            .expect("decode nfs1 after drained/fenced retry")
            .is_some(),
            "stale drained/fenced retry must preserve nfs1 materialized payload"
        );
        assert!(
            decode_exact_query_node(
                sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                    .await
                    .expect("query nfs2 after drained/fenced retry"),
                b"/force-find-stress",
            )
            .expect("decode nfs2 after drained/fenced retry")
            .is_some(),
            "stale drained/fenced retry must preserve nfs2 materialized payload"
        );

        source.close().await.expect("close source");
        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_invalid_or_revoked_grant_attachment_tokens_after_first_wave_succeeded()
     {
        assert_on_control_frame_retries_bridge_reset_error(
            CnxError::AccessDenied("invalid or revoked grant attachment token".to_string()),
            "on_control_frame should retry an invalid or revoked grant attachment token after the first sink wave already succeeded",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_channel_closed_after_first_wave_succeeded() {
        assert_on_control_frame_retries_bridge_reset_error(
            CnxError::ChannelClosed,
            "on_control_frame should retry channel-closed continuity gaps after the first sink wave already succeeded",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_channel_closed_followup_resets_shared_client_before_retry() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("first sink control wave should succeed");

        let previous_worker_identity = sink.shared_worker_identity_for_tests().await;

        let reset_count = Arc::new(AtomicUsize::new(0));
        let _reset_hook = SinkWorkerRetryResetHookReset;
        install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
            reset_count: reset_count.clone(),
        });

        let _error_reset = SinkWorkerControlFrameErrorHookReset;
        install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook {
            err: CnxError::ChannelClosed,
        });

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("follow-up sink control wave should recover after channel-closed error");

        let next_worker_identity = sink.shared_worker_identity_for_tests().await;

        assert!(
            reset_count.load(Ordering::SeqCst) >= 1,
            "follow-up channel-closed recovery must reset the shared sink worker client before retry"
        );
        assert_ne!(
            next_worker_identity, previous_worker_identity,
            "follow-up channel-closed recovery must replace the shared sink worker handle before retry so later waves cannot inherit the stale bridge session"
        );

        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_enforces_total_timeout_when_worker_call_stalls_after_first_wave() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = Arc::new(
            SinkWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_sink_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct sink worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode first-wave sink activate"),
        ])
        .await
        .expect("first sink control wave should succeed");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _reset = SinkWorkerControlFramePauseHookReset;
        install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let control_task = tokio::spawn({
            let sink = sink.clone();
            async move {
                sink.on_control_frame_with_timeouts_for_tests(
                    vec![
                        encode_runtime_exec_control(&RuntimeExecControl::Activate(
                            RuntimeExecActivate {
                                route_key: ROUTE_KEY_QUERY.to_string(),
                                unit_id: "runtime.exec.sink".to_string(),
                                lease: None,
                                generation: 2,
                                expires_at_ms: 1,
                                bound_scopes: vec![
                                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                                ],
                            },
                        ))
                        .expect("encode second-wave sink activate"),
                    ],
                    Duration::from_millis(150),
                    Duration::from_millis(150),
                )
                .await
            }
        });

        entered.notified().await;
        let err = tokio::time::timeout(Duration::from_secs(1), control_task)
            .await
            .expect("stalled sink on_control_frame should resolve within the local total timeout budget")
            .expect("join stalled sink on_control_frame task")
            .expect_err("stalled sink on_control_frame should fail once its local timeout budget is exhausted");
        assert!(matches!(err, CnxError::Timeout), "err={err:?}");

        release.notify_waiters();
        sink.close().await.expect("close sink worker");
    }

    async fn assert_on_control_frame_retries_bridge_reset_error(err: CnxError, label: &str) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("first sink control wave should succeed");

        let _reset = SinkWorkerControlFrameErrorHookReset;
        install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook { err });

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode second-wave sink activate"),
        ])
        .await
        .unwrap_or_else(|err| panic!("{label}: {err}"));

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let scheduled = sink
                .scheduled_group_ids()
                .await
                .expect("scheduled groups")
                .unwrap_or_default();
            if scheduled == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < schedule_deadline,
                "{label}: scheduled groups should remain converged after retry: scheduled={scheduled:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_internal_peer_early_eof_errors_after_first_wave_succeeded() {
        assert_on_control_frame_retries_bridge_reset_error(
            CnxError::Internal(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
            "on_control_frame should retry an internal early-eof bridge error and reach the live sink worker",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_restart_deferred_retire_pending_events_deactivate_fails_fast_after_repeated_bridge_reset_errors()
     {
        struct SinkWorkerControlFrameErrorQueueHookReset;

        impl Drop for SinkWorkerControlFrameErrorQueueHookReset {
            fn drop(&mut self) {
                clear_sink_worker_control_frame_error_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode initial sink events activate"),
        ])
        .await
        .expect("initial sink events wave should succeed");

        let _reset = SinkWorkerControlFrameErrorQueueHookReset;
        install_sink_worker_control_frame_error_queue_hook(
            SinkWorkerControlFrameErrorQueueHook {
                errs: std::collections::VecDeque::from(vec![
                    CnxError::PeerError(
                        "transport closed: sidecar control bridge stopped".to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                ]),
                sticky_worker_instance_id: None,
                sticky_peer_err: Some(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
            },
        );

        let started = std::time::Instant::now();
        let err = sink
            .on_control_frame_with_timeouts_for_tests(
                vec![
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode sink events deactivate"),
                ],
                Duration::from_millis(250),
                Duration::from_millis(50),
            )
            .await
            .expect_err(
                "single restart_deferred_retire_pending events deactivate should fail fast once bridge resets keep repeating",
            );

        assert!(
            !matches!(err, CnxError::Timeout),
            "bridge-reset fail-close lane should return the bridge transport error instead of exhausting the whole local timeout budget: {err:?}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "bridge-reset fail-close lane should fail fast instead of spending the full local timeout budget: elapsed={:?} err={err:?}",
            started.elapsed()
        );

        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn status_snapshot_retries_stale_drained_fenced_pid_errors() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![sink_worker_export(
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
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        let _reset = SinkWorkerStatusErrorHookReset;
        install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
            err: CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

        let snapshot = sink
            .status_snapshot()
            .await
            .expect("status_snapshot should retry a stale drained/fenced pid error and reach the live sink worker");

        assert_eq!(
            snapshot.groups.len(),
            1,
            "fresh live sink worker snapshot should still decode after stale-pid retry"
        );

        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scheduled_group_ids_retry_stale_drained_fenced_pid_errors() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("apply sink control");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let scheduled = sink
                .scheduled_group_ids()
                .await
                .expect("initial scheduled groups")
                .unwrap_or_default();
            if scheduled == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for scheduled groups before stale-pid retry test: scheduled={scheduled:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let _reset = SinkWorkerScheduledGroupsErrorHookReset;
        install_sink_worker_scheduled_groups_error_hook(SinkWorkerScheduledGroupsErrorHook {
            err: CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled_group_ids should retry stale drained/fenced pid errors and reach the live sink worker");

        assert_eq!(scheduled, Some(expected_groups));

        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_restart_deferred_retire_pending_cleanup_batch_fails_fast_after_repeated_bridge_reset_errors()
     {
        struct SinkWorkerControlFrameErrorQueueHookReset;

        impl Drop for SinkWorkerControlFrameErrorQueueHookReset {
            fn drop(&mut self) {
                clear_sink_worker_control_frame_error_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode initial sink events activate"),
        ])
        .await
        .expect("initial sink events wave should succeed");

        let _reset = SinkWorkerControlFrameErrorQueueHookReset;
        install_sink_worker_control_frame_error_queue_hook(
            SinkWorkerControlFrameErrorQueueHook {
                errs: std::collections::VecDeque::from(vec![
                    CnxError::PeerError(
                        "transport closed: sidecar control bridge stopped".to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                ]),
                sticky_worker_instance_id: None,
                sticky_peer_err: Some(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
            },
        );

        let started = std::time::Instant::now();
        let err = sink
            .on_control_frame_with_timeouts_for_tests(
                vec![
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "materialized-find.node_c:v1.req".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode node-c query deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "materialized-find.node_d:v1.req".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode node-d query deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "materialized-find:v1.req".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode aggregate query deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "on-demand-force-find:v1.on-demand-force-find.req".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode force-find query deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "sink-logical-roots-control:v1.stream".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode sink roots control deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode events deactivate"),
                ],
                Duration::from_millis(250),
                Duration::from_millis(50),
            )
            .await
            .expect_err(
                "multi-envelope restart_deferred_retire_pending cleanup batch should fail fast once bridge resets keep repeating",
            );

        assert!(
            !matches!(err, CnxError::Timeout),
            "bridge-reset retained cleanup batch should return the bridge transport error instead of exhausting the whole local timeout budget: {err:?}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "bridge-reset retained cleanup batch should fail fast instead of spending the full local timeout budget: elapsed={:?} err={err:?}",
            started.elapsed()
        );

        sink.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn status_snapshot_nonblocking_retries_peer_bridge_stopped_errors_after_begin() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![sink_worker_export(
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
        let sink = SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        let first_snapshot = sink
            .status_snapshot()
            .await
            .expect("prime cached status snapshot");
        assert!(
            first_snapshot.scheduled_groups_by_node.is_empty(),
            "primed cached sink status should start without scheduled groups: {:?}",
            first_snapshot.scheduled_groups_by_node
        );

        sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-d::nfs1"])],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("apply sink control before nonblocking status");

        let _reset = SinkWorkerStatusErrorHookReset;
        install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
            err: CnxError::PeerError(
                "transport closed: sidecar control bridge stopped".to_string(),
            ),
        });

        let snapshot = sink
            .status_snapshot_nonblocking()
            .await
            .expect("status_snapshot_nonblocking should still return a snapshot");

        assert_eq!(
            snapshot.scheduled_groups_by_node.get("node-d"),
            Some(&vec!["nfs1".to_string()]),
            "status_snapshot_nonblocking should retry a peer bridge-stopped error and reach the live sink worker instead of returning the stale cached pre-activate snapshot: {:?}",
            snapshot.scheduled_groups_by_node
        );

        sink.close().await.expect("close sink worker");
    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn update_logical_roots_reacquires_worker_client_after_transport_closes_mid_handoff() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SinkWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_sink_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct sink worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let _reset = SinkWorkerUpdateRootsHookReset;
        install_sink_worker_update_roots_hook(SinkWorkerUpdateRootsHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let update_task = tokio::spawn({
            let client = client.clone();
            let nfs1 = nfs1.clone();
            let nfs2 = nfs2.clone();
            async move {
                client
                    .update_logical_roots(
                        vec![
                            sink_worker_root("nfs1", &nfs1),
                            sink_worker_root("nfs2", &nfs2),
                        ],
                        vec![
                            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
                        ],
                    )
                    .await
            }
        });

        entered.notified().await;
        client
            .shutdown_shared_worker_for_tests(Duration::from_secs(2))
            .await
            .expect("shutdown stale worker bridge");
        tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
            .await
            .expect("sink worker restart timed out")
            .expect("restart sink worker");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(4), update_task)
            .await
            .expect("update_logical_roots should reacquire a live sink worker client after handoff")
            .expect("join update_logical_roots task")
            .expect("update_logical_roots after worker restart");

        let roots = client
            .logical_roots_snapshot()
            .await
            .expect("logical roots snapshot after update");
        assert_eq!(
            roots
                .iter()
                .map(|root| root.id.as_str())
                .collect::<Vec<_>>(),
            vec!["nfs1", "nfs2"],
            "logical roots should reflect the post-handoff sink update"
        );

        client.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_reacquires_worker_client_after_transport_closes_mid_handoff() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SinkWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_sink_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct sink worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let _reset = SinkWorkerControlFramePauseHookReset;
        install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
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
                                unit_id: "runtime.exec.sink".to_string(),
                                lease: None,
                                generation: 2,
                                expires_at_ms: 1,
                                bound_scopes: vec![
                                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                                ],
                            },
                        ))
                        .expect("encode sink activate"),
                    ])
                    .await
            }
        });

        entered.notified().await;
        client
            .shutdown_shared_worker_for_tests(Duration::from_secs(2))
            .await
            .expect("shutdown stale sink worker bridge");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(8), control_task)
            .await
            .expect("on_control_frame should reacquire a live sink worker client after handoff")
            .expect("join on_control_frame task")
            .expect("sink on_control_frame after worker restart");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let scheduled = client
                .scheduled_group_ids()
                .await
                .expect("scheduled groups")
                .unwrap_or_default();
            if scheduled == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < schedule_deadline,
                "timed out waiting for scheduled groups after sink handoff retry: scheduled={scheduled:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        client.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn second_on_control_frame_reacquires_worker_client_after_first_wave_succeeded_and_transport_closes_mid_handoff()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SinkWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_sink_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct sink worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode first-wave sink activate"),
            ])
            .await
            .expect("first sink control wave should succeed");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _reset = SinkWorkerControlFramePauseHookReset;
        install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
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
                                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                    .to_string(),
                                lease: None,
                                generation: 2,
                                expires_at_ms: 1,
                                bound_scopes: vec![
                                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                                ],
                            },
                        ))
                        .expect("encode second-wave sink activate"),
                    ])
                    .await
            }
        });

        entered.notified().await;
        client
            .shutdown_shared_worker_for_tests(Duration::from_secs(2))
            .await
            .expect("shutdown stale sink worker bridge after first wave");
        tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
            .await
            .expect("sink worker restart timed out")
            .expect("restart sink worker after first-wave success");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(8), second_wave)
            .await
            .expect("second sink control wave should reacquire a live worker client after reset")
            .expect("join second sink control wave")
            .expect("second sink control wave after worker restart");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let scheduled = client
                .scheduled_group_ids()
                .await
                .expect("scheduled groups")
                .unwrap_or_default();
            if scheduled
                == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "scheduled sink groups should remain converged after second-wave retry: scheduled={scheduled:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        client.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn second_exact_shaped_sink_nine_wave_reacquires_worker_client_after_first_wave_succeeded_without_external_ensure_started()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
        std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 force-find dir");
        std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 force-find dir");
        std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a")
            .expect("seed nfs1");
        std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b")
            .expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                sink_worker_root("nfs1", &nfs1),
                sink_worker_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
            .expect("init source");
        let mut stream = source.pub_().await.expect("start source pub stream");
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SinkWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_sink_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct sink worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        let sink_wave = |generation| {
            let mut signals = vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode sink events activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!(
                        "{}.stream",
                        crate::runtime::routes::ROUTE_KEY_SINK_ROOTS_CONTROL
                    ),
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode sink roots-control activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", crate::runtime::routes::ROUTE_KEY_FORCE_FIND),
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode sink force-find activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: "materialized-find:v1.req".to_string(),
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode materialized-find activate"),
            ];
            for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
                signals.push(
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: crate::runtime::routes::sink_query_request_route_for(
                                node_id,
                            )
                            .0,
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode per-node materialized-find activate"),
                );
            }
            signals
        };

        client
            .on_control_frame(sink_wave(2))
            .await
            .expect("first exact-shaped sink nine-wave should succeed");

        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < initial_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => client.send(batch).await.expect("apply initial batch"),
                Ok(None) => break,
                Err(_) => continue,
            }
            let nfs1_ready = decode_exact_query_node(
                client
                    .materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1"),
                b"/force-find-stress",
            )
            .expect("decode nfs1")
            .is_some();
            let nfs2_ready = decode_exact_query_node(
                client
                    .materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                    .await
                    .expect("query nfs2"),
                b"/force-find-stress",
            )
            .expect("decode nfs2")
            .is_some();
            if nfs1_ready && nfs2_ready {
                break;
            }
        }

        assert!(
            decode_exact_query_node(
                client
                    .materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1 after initial"),
                b"/force-find-stress",
            )
            .expect("decode nfs1 after initial")
            .is_some(),
            "precondition: nfs1 initial materialization should exist before worker restart"
        );
        assert!(
            decode_exact_query_node(
                client
                    .materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                    .await
                    .expect("query nfs2 after initial"),
                b"/force-find-stress",
            )
            .expect("decode nfs2 after initial")
            .is_some(),
            "precondition: nfs2 initial materialization should exist before worker restart"
        );

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _reset = SinkWorkerControlFramePauseHookReset;
        install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let second_wave = tokio::spawn({
            let client = client.clone();
            async move { client.on_control_frame(sink_wave(3)).await }
        });

        entered.notified().await;
        client
            .shutdown_shared_worker_for_tests(Duration::from_secs(2))
            .await
            .expect("shutdown stale sink worker bridge after first exact-shaped sink nine-wave");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(8), second_wave)
            .await
            .expect("second exact-shaped sink nine-wave should restart the sink worker client without external ensure_started")
            .expect("join second exact-shaped sink nine-wave")
            .expect("second exact-shaped sink nine-wave after worker restart");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let scheduled = client
                .scheduled_group_ids()
                .await
                .expect("scheduled groups")
                .unwrap_or_default();
            if scheduled
                == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "scheduled sink groups should remain converged after automatic second nine-wave restart: scheduled={scheduled:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(
            decode_exact_query_node(
                client
                    .materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                    .await
                    .expect("query nfs1 after second wave restart"),
                b"/force-find-stress",
            )
            .expect("decode nfs1 after second wave restart")
            .is_some(),
            "second exact-shaped sink nine-wave restart must preserve nfs1 materialized payload"
        );
        assert!(
            decode_exact_query_node(
                client
                    .materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                    .await
                    .expect("query nfs2 after second wave restart"),
                b"/force-find-stress",
            )
            .expect("decode nfs2 after second wave restart")
            .is_some(),
            "second exact-shaped sink nine-wave restart must preserve nfs2 materialized payload"
        );

        source.close().await.expect("close source");
        client.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn second_on_control_frame_reacquires_worker_client_after_first_wave_succeeded_without_external_ensure_started()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SinkWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_sink_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct sink worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode first-wave sink activate"),
            ])
            .await
            .expect("first sink control wave should succeed");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _reset = SinkWorkerControlFramePauseHookReset;
        install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
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
                                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                    .to_string(),
                                lease: None,
                                generation: 2,
                                expires_at_ms: 1,
                                bound_scopes: vec![
                                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                                ],
                            },
                        ))
                        .expect("encode second-wave sink activate"),
                    ])
                    .await
            }
        });

        entered.notified().await;
        client
            .shutdown_shared_worker_for_tests(Duration::from_secs(2))
            .await
            .expect("shutdown stale sink worker bridge after first wave");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(8), second_wave)
            .await
            .expect("second sink control wave should restart the sink worker client without external ensure_started")
            .expect("join second sink control wave")
            .expect("second sink control wave after worker restart");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let scheduled = client
                .scheduled_group_ids()
                .await
                .expect("scheduled groups")
                .unwrap_or_default();
            if scheduled
                == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "scheduled sink groups should remain converged after automatic second-wave restart: scheduled={scheduled:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        client.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_sink_handle_later_nine_wave_survives_predecessor_fail_closed_cleanup_and_intermediate_success()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let binding = external_sink_worker_binding(worker_socket_dir.path());
        let predecessor = Arc::new(
            SinkWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg.clone(),
                binding.clone(),
                factory.clone(),
            )
            .expect("construct predecessor sink worker client"),
        );
        let successor = Arc::new(
            SinkWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
                .expect("construct successor sink worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), successor.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start shared sink worker");

        let sink_wave = |generation| {
            let mut signals = vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode sink events activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!(
                        "{}.stream",
                        crate::runtime::routes::ROUTE_KEY_SINK_ROOTS_CONTROL
                    ),
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode sink roots-control activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", crate::runtime::routes::ROUTE_KEY_FORCE_FIND),
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode sink force-find activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: "materialized-find:v1.req".to_string(),
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode materialized-find activate"),
            ];
            for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
                signals.push(
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: crate::runtime::routes::sink_query_request_route_for(
                                node_id,
                            )
                            .0,
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode per-node materialized-find activate"),
                );
            }
            signals
        };

        predecessor
            .on_control_frame(sink_wave(2))
            .await
            .expect("predecessor initial exact-shaped sink nine-wave should succeed");

        let _deactivate_pause_reset = SinkWorkerControlFramePauseHookReset;
        let deactivate_entered = Arc::new(Notify::new());
        let deactivate_release = Arc::new(Notify::new());
        install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
            entered: deactivate_entered.clone(),
            release: deactivate_release.clone(),
        });

        let fail_closed_deactivate = tokio::spawn({
            let predecessor = predecessor.clone();
            async move {
                predecessor
                    .on_control_frame(vec![
                        encode_runtime_exec_control(
                            &capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                                RuntimeExecDeactivate {
                                    route_key: format!(
                                        "{}.stream",
                                        crate::runtime::routes::ROUTE_KEY_EVENTS
                                    ),
                                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                        .to_string(),
                                    lease: None,
                                    generation: 3,
                                    reason: "restart_deferred_retire_pending".to_string(),
                                },
                            ),
                        )
                        .expect("encode sink events deactivate"),
                    ])
                    .await
            }
        });

        deactivate_entered.notified().await;
        predecessor
            .shutdown_shared_worker_for_tests(Duration::from_secs(2))
            .await
            .expect("shutdown shared sink worker during predecessor fail-closed cleanup");
        deactivate_release.notify_waiters();
        let _ = fail_closed_deactivate
            .await
            .expect("join predecessor fail-closed deactivate");

        tokio::time::timeout(
            Duration::from_secs(8),
            successor.on_control_frame(sink_wave(4)),
        )
        .await
        .expect("intermediate successor exact-shaped sink nine-wave should settle")
        .expect("intermediate successor exact-shaped sink nine-wave should recover");

        predecessor
            .close()
            .await
            .expect("close predecessor shared sink handle after intermediate success");

        tokio::time::timeout(Duration::from_secs(8), successor.on_control_frame(sink_wave(5)))
            .await
            .expect("later successor exact-shaped sink nine-wave should settle promptly after predecessor fail-closed cleanup plus intermediate success")
            .expect("later successor exact-shaped sink nine-wave should still succeed after predecessor fail-closed cleanup plus intermediate success");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let scheduled = successor
                .scheduled_group_ids()
                .await
                .expect("scheduled groups")
                .unwrap_or_default();
            if scheduled
                == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "scheduled sink groups should remain converged after later shared-handle nine-wave recovery: scheduled={scheduled:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        successor
            .close()
            .await
            .expect("close successor sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn close_keeps_shared_sink_worker_client_alive_when_another_handle_still_exists() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![sink_worker_export(
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
        let binding = external_sink_worker_binding(worker_socket_dir.path());
        let predecessor = Arc::new(
            SinkWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg.clone(),
                binding.clone(),
                factory.clone(),
            )
            .expect("construct predecessor sink worker client"),
        );
        let successor = Arc::new(
            SinkWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
                .expect("construct successor sink worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), successor.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start shared sink worker");

        assert!(
            successor
                .shared_worker_existing_client_for_tests()
                .await
                .expect("existing sink client before predecessor close")
                .is_some(),
            "shared sink worker must have a live client before predecessor close"
        );

        predecessor
            .close()
            .await
            .expect("close predecessor sink worker handle");

        assert!(
            successor
                .shared_worker_existing_client_for_tests()
                .await
                .expect("existing sink client after predecessor close")
                .is_some(),
            "closing one sink handle must not tear down the shared worker client while a successor handle still exists"
        );

        let roots = successor
            .logical_roots_snapshot()
            .await
            .expect("successor sink logical_roots_snapshot after predecessor close");
        assert_eq!(
            roots
                .iter()
                .map(|root| root.id.as_str())
                .collect::<Vec<_>>(),
            vec!["nfs1"],
            "shared sink worker should stay usable for the successor after predecessor close"
        );

        successor
            .close()
            .await
            .expect("close successor sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn close_waits_for_inflight_update_logical_roots_control_op_before_shutting_down_worker()
    {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SinkWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                external_sink_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct sink worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");

        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let _reset = SinkWorkerUpdateRootsHookReset;
        install_sink_worker_update_roots_hook(SinkWorkerUpdateRootsHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let update_task = tokio::spawn({
            let client = client.clone();
            let nfs1 = nfs1.clone();
            let nfs2 = nfs2.clone();
            async move {
                client
                    .update_logical_roots(
                        vec![
                            sink_worker_root("nfs1", &nfs1),
                            sink_worker_root("nfs2", &nfs2),
                        ],
                        vec![
                            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
                        ],
                    )
                    .await
            }
        });

        entered.notified().await;
        let close_task = tokio::spawn({
            let client = client.clone();
            async move { client.close().await }
        });
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert!(
            !close_task.is_finished(),
            "sink worker close must wait for in-flight update_logical_roots before tearing down the worker bridge"
        );

        release.notify_waiters();

        update_task
            .await
            .expect("join sink update task")
            .expect("sink update_logical_roots after close wait");
        close_task
            .await
            .expect("join sink close task")
            .expect("close sink worker after update");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn distinct_runtime_factories_do_not_share_started_sink_worker_client_on_same_node() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![sink_worker_root("nfs1", &nfs1)],
            host_object_grants: vec![sink_worker_export(
                "node-d::nfs1",
                "node-d",
                "10.0.0.41",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let state_boundary_a = in_memory_state_boundary();
        let state_boundary_b = in_memory_state_boundary();
        let boundary_a = Arc::new(LoopbackWorkerBoundary::default());
        let boundary_b = Arc::new(LoopbackWorkerBoundary::default());
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let binding = external_sink_worker_binding(worker_socket_dir.path());
        let predecessor = Arc::new(
            SinkWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg.clone(),
                binding.clone(),
                RuntimeWorkerClientFactory::new(
                    boundary_a.clone(),
                    boundary_a.clone(),
                    state_boundary_a,
                ),
            )
            .expect("construct predecessor sink worker client"),
        );
        let successor = Arc::new(
            SinkWorkerClientHandle::new(
                NodeId("node-d".to_string()),
                cfg,
                binding,
                RuntimeWorkerClientFactory::new(
                    boundary_b.clone(),
                    boundary_b.clone(),
                    state_boundary_b,
                ),
            )
            .expect("construct successor sink worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), predecessor.ensure_started())
            .await
            .expect("predecessor sink worker start timed out")
            .expect("start predecessor sink worker");

        assert!(
            predecessor
                .shared_worker_existing_client_for_tests()
                .await
                .expect("predecessor existing sink client")
                .is_some(),
            "predecessor should have a started sink worker client"
        );
        assert!(
            successor
                .shared_worker_existing_client_for_tests()
                .await
                .expect("successor existing sink client after predecessor start")
                .is_none(),
            "a successor created through a distinct runtime worker factory must not inherit the predecessor's started sink worker client"
        );
        assert!(
            !Arc::ptr_eq(&predecessor._shared, &successor._shared),
            "distinct runtime worker factories must not share one sink worker handle state"
        );

        tokio::time::timeout(Duration::from_secs(8), successor.ensure_started())
            .await
            .expect("successor sink worker start timed out")
            .expect("start successor sink worker");

        assert_ne!(
            predecessor.shared_worker_identity_for_tests().await,
            successor.shared_worker_identity_for_tests().await,
            "distinct runtime worker factories must not share one sink runtime worker client wrapper"
        );
        assert_eq!(
            predecessor
                .logical_roots_snapshot()
                .await
                .expect("predecessor logical_roots_snapshot after successor start")
                .iter()
                .map(|root| root.id.as_str())
                .collect::<Vec<_>>(),
            vec!["nfs1"],
            "predecessor sink worker should remain independently usable"
        );
        assert_eq!(
            successor
                .logical_roots_snapshot()
                .await
                .expect("successor logical_roots_snapshot after successor start")
                .iter()
                .map(|root| root.id.as_str())
                .collect::<Vec<_>>(),
            vec!["nfs1"],
            "successor sink worker should be independently usable"
        );

        predecessor
            .close()
            .await
            .expect("close predecessor sink worker");
        successor
            .close()
            .await
            .expect("close successor sink worker");
    }
}
