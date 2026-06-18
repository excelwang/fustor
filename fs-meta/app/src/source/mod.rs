//! File metadata source app for Capanix.
//!
//! Monitors a directory tree via host watch backends with LRU watch scheduling,
//! emits drift-compensated file metadata events.

pub mod config;
pub(crate) mod drift;
pub(crate) mod scanner;
pub(crate) mod sentinel;
pub(crate) mod watcher;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::io;
use std::pin::Pin;
#[cfg(test)]
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_stream::stream;
use bytes::Bytes;

use futures_core::Stream;
use futures_util::StreamExt;
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use capanix_app_sdk::runtime::{
    ControlEnvelope, EventMetadata, NodeId, RecvOpts, RouteKey, in_memory_state_boundary,
};
use capanix_app_sdk::{CnxError, Event, Result};
use capanix_host_adapter_fs::{HostFs, HostFsFacade};
use capanix_runtime_entry_sdk::advanced::boundary::{ChannelIoSubset, StateBoundary};
use capanix_runtime_entry_sdk::control::{
    RuntimeBoundScope, RuntimeHostGrantChange, RuntimeHostGrantState,
};

use crate::LogicalClock;
use crate::query::path::{is_under_query_path, path_buf_from_bytes, path_to_bytes};
use crate::query::request::{
    ForceFindQueryPayload, InternalQueryRequest, LiveScanRequest, QueryOp, QueryTransport,
};
use crate::query::result_ops::{
    RawQueryResult, merge_query_responses, raw_query_results_by_origin_from_source_events,
    subtree_stats_from_query_response, tree_group_payload_from_query_response,
};
use crate::runtime::endpoint::ManagedEndpointTask;
use crate::runtime::seam::resolve_host_fs_facade;

use crate::runtime::execution_units::{SOURCE_RUNTIME_UNIT_ID, SOURCE_RUNTIME_UNITS};
use crate::runtime::orchestration::{
    MANUAL_RESCAN_CONTROL_FRAME_KIND, ManualRescanControlPayload, SourceControlSignal,
    SourceRuntimeUnit, decode_logical_roots_control_payload,
    manual_rescan_scoped_target_acceptance_timeout_from_payload,
    source_control_signals_from_envelopes,
};
use crate::runtime::routes::{
    METHOD_SOURCE_RESCAN, METHOD_SOURCE_RESCAN_CONTROL, METHOD_SOURCE_ROOTS_CONTROL,
    METHOD_SOURCE_STATUS, ROUTE_KEY_SOURCE_RESCAN_INTERNAL, ROUTE_KEY_SOURCE_STATUS_INTERNAL,
    ROUTE_TOKEN_FS_META_INTERNAL, default_route_bindings, source_find_route_bindings_for,
    source_rescan_request_route_for, source_roots_control_stream_route_for,
    source_status_request_route_for,
};
use crate::runtime::unit_gate::RuntimeUnitGate;
use crate::source::config::{GrantedMountRoot, RootSpec, SourceConfig};
use crate::source::drift::DriftEstimator;
use crate::source::scanner::{AuditScanResult, ParallelScanner};
use crate::source::sentinel::{HealthSignal, Sentinel, SentinelAction, SentinelConfig};
use crate::source::watcher::WatchManager;
use crate::state::cell::{AuthorityJournal, HostObjectGrantsCell, LogicalRootsCell, SignalCell};
use crate::state::commit_boundary::CommitBoundary;
use crate::workers::source::SourceControlState;
use crate::{ControlEvent, EpochType, FileMetaRecord};

#[cfg(test)]
use crate::runtime::routes::ROUTE_KEY_QUERY;

const MANUAL_RESCAN_SEEN_REQUEST_IDS_LIMIT: usize = 256;

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_micros() as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
static SOURCE_STATUS_MANUAL_RESCAN_REARM_HOOK: OnceLock<Mutex<Option<Arc<AtomicUsize>>>> =
    OnceLock::new();

#[cfg(test)]
pub(crate) struct SourceStatusManualRescanRearmHookReset;

#[cfg(test)]
impl Drop for SourceStatusManualRescanRearmHookReset {
    fn drop(&mut self) {
        if let Ok(mut guard) = SOURCE_STATUS_MANUAL_RESCAN_REARM_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
        {
            *guard = None;
        }
    }
}

#[cfg(test)]
pub(crate) fn install_source_status_manual_rescan_rearm_hook(
    counter: Arc<AtomicUsize>,
) -> SourceStatusManualRescanRearmHookReset {
    let mut guard = SOURCE_STATUS_MANUAL_RESCAN_REARM_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("source status manual-rescan rearm hook lock");
    *guard = Some(counter);
    SourceStatusManualRescanRearmHookReset
}

#[cfg(test)]
fn record_source_status_manual_rescan_rearm() {
    if let Some(counter) = SOURCE_STATUS_MANUAL_RESCAN_REARM_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("source status manual-rescan rearm hook lock")
        .as_ref()
        .cloned()
    {
        counter.fetch_add(1, Ordering::SeqCst);
    }
}

const EPOCH0_SCAN_DELAY_ENV: &str = "FS_META_EPOCH0_SCAN_DELAY_MS";
const EPOCH0_SCAN_DELAY_DEFAULT_MS: u64 = 10_000;
const EPOCH0_SCAN_DELAY_MAX_MS: u64 = 300_000;
const RESCAN_READY_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const RESCAN_READY_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MANUAL_RESCAN_SIGNAL_NAME: &str = "manual_rescan";
const MANUAL_RESCAN_WATCH_POLL_INTERVAL: Duration = Duration::from_millis(250);
const SOURCE_PUB_STREAM_PENDING_BATCH_LIMIT: usize = 128;

fn epoch0_scan_delay_ms_from_raw(raw: Option<&str>, default_ms: u64) -> u64 {
    raw.and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default_ms)
        .clamp(0, EPOCH0_SCAN_DELAY_MAX_MS)
}

fn epoch0_scan_delay() -> Duration {
    // Tests keep default as zero, but the same code path still executes.
    let default_ms = if cfg!(test) {
        0
    } else {
        EPOCH0_SCAN_DELAY_DEFAULT_MS
    };
    let env_raw = std::env::var(EPOCH0_SCAN_DELAY_ENV).ok();
    Duration::from_millis(epoch0_scan_delay_ms_from_raw(
        env_raw.as_deref(),
        default_ms,
    ))
}

fn lock_or_recover<'a, T>(m: &'a Mutex<T>, context: &str) -> MutexGuard<'a, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            log::warn!("{context}: mutex poisoned; recovering inner state");
            poisoned.into_inner()
        }
    }
}

fn debug_control_scope_capture_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
}

fn debug_source_root_task_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_SOURCE_ROOT_TASK").is_some()
}

fn debug_stream_path_capture_target() -> Option<Vec<u8>> {
    match std::env::var("FSMETA_DEBUG_STREAM_PATH_CAPTURE") {
        Ok(value) if value.trim().is_empty() => Some(b"/force-find-stress".to_vec()),
        Ok(value) => Some(value.into_bytes()),
        Err(_) => None,
    }
}

fn count_events_under_query_path(events: &[Event], query_path: &[u8]) -> u64 {
    events
        .iter()
        .filter_map(|event| rmp_serde::from_slice::<FileMetaRecord>(event.payload_bytes()).ok())
        .filter(|record| is_under_query_path(&record.path, query_path))
        .count() as u64
}

fn root_specs_signature(
    roots: &[RootSpec],
) -> Vec<(
    String,
    Option<std::path::PathBuf>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    std::path::PathBuf,
    bool,
    bool,
    Option<u64>,
)> {
    roots
        .iter()
        .map(|root| {
            (
                root.id.clone(),
                root.selector.mount_point.clone(),
                root.selector.fs_source.clone(),
                root.selector.fs_type.clone(),
                root.selector.host_ip.clone(),
                root.selector.host_ref.clone(),
                root.subpath_scope.clone(),
                root.watch,
                root.scan,
                root.audit_interval_ms,
            )
        })
        .collect()
}

fn summarize_bound_scopes(bound_scopes: &[RuntimeBoundScope]) -> Vec<String> {
    bound_scopes
        .iter()
        .map(|scope| format!("{}=>{}", scope.scope_id, scope.resource_ids.join("|")))
        .collect()
}

fn encode_force_find_grouped_events(
    source_events: &[Event],
    query_path: &[u8],
    recursive: bool,
    max_depth: Option<usize>,
    op: QueryOp,
    object_to_group: &HashMap<String, String>,
) -> Result<Vec<Event>> {
    let grouped = raw_query_results_by_origin_from_source_events(source_events, query_path)?;
    let mut grouped_by_group = BTreeMap::<String, Vec<RawQueryResult>>::new();
    for (origin, query) in grouped {
        let group_id = object_to_group
            .get(&origin)
            .cloned()
            .unwrap_or(origin.clone());
        grouped_by_group.entry(group_id).or_default().push(query);
    }
    let all_group_ids = object_to_group
        .values()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();

    let mut out = Vec::new();
    for group_id in all_group_ids {
        let query = grouped_by_group
            .remove(&group_id)
            .map(merge_query_responses)
            .unwrap_or(RawQueryResult {
                nodes: Vec::new(),
                reliable: true,
                unreliable_reason: None,
            });
        let payload = match op {
            QueryOp::Tree => {
                let payload = tree_group_payload_from_query_response(
                    &query, query_path, recursive, max_depth,
                );
                rmp_serde::to_vec_named(&ForceFindQueryPayload::Tree(payload)).map_err(|err| {
                    CnxError::Internal(format!("serialize force-find tree payload failed: {err}"))
                })?
            }
            QueryOp::Stats => {
                let stats = subtree_stats_from_query_response(&query);
                rmp_serde::to_vec_named(&ForceFindQueryPayload::Stats(stats)).map_err(|err| {
                    CnxError::Internal(format!("serialize force-find stats payload failed: {err}"))
                })?
            }
        };
        let timestamp_us = source_events
            .iter()
            .filter(|event| {
                object_to_group
                    .get(&event.metadata().origin_id.0)
                    .is_some_and(|group| group == &group_id)
            })
            .map(|event| event.metadata().timestamp_us)
            .max()
            .unwrap_or(0);
        out.push(Event::new(
            EventMetadata {
                origin_id: NodeId(group_id),
                timestamp_us,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            bytes::Bytes::from(payload),
        ));
    }
    Ok(out)
}

fn coverage_mode_for_root(
    watch_enabled: bool,
    scan_enabled: bool,
    watch_lru_capacity: usize,
) -> &'static str {
    match (
        watch_enabled && watch_lru_capacity > 0,
        scan_enabled,
        watch_enabled,
    ) {
        (true, true, _) => "realtime_hotset_plus_audit",
        (_, true, _) => "audit_with_metadata",
        (false, false, true) => "watch_degraded",
        _ => "watch_degraded",
    }
}

fn host_ref_matches_node_id(host_ref: &str, node_id: &NodeId) -> bool {
    host_ref == node_id.0
        || node_id
            .0
            .strip_prefix(host_ref)
            .is_some_and(|suffix| suffix.starts_with('-'))
        || node_id.0.strip_prefix("cluster-").is_some_and(|scoped| {
            scoped == host_ref
                || scoped
                    .strip_prefix(host_ref)
                    .is_some_and(|suffix| suffix.starts_with('-'))
        })
}

fn runtime_scope_resource_matches_logical_root(resource_id: &str, logical_root_id: &str) -> bool {
    resource_id == logical_root_id
        || resource_id
            .rsplit_once("::")
            .is_some_and(|(_, tail)| tail == logical_root_id)
}

fn runtime_scope_row_matches_logical_root(row: &RuntimeBoundScope, logical_root_id: &str) -> bool {
    row.scope_id == logical_root_id
        || row.resource_ids.iter().any(|resource_id| {
            runtime_scope_resource_matches_logical_root(resource_id, logical_root_id)
        })
}

fn runtime_scope_row_has_explicit_local_resource_id(
    row: &RuntimeBoundScope,
    logical_root_id: &str,
    node_id: &NodeId,
) -> bool {
    row.resource_ids.iter().any(|resource_id| {
        runtime_scope_resource_matches_logical_root(resource_id, logical_root_id)
            && resource_id
                .rsplit_once("::")
                .is_some_and(|(host_ref, _)| host_ref_matches_node_id(host_ref, node_id))
    })
}

fn runtime_scope_row_has_bare_logical_root_id(
    row: &RuntimeBoundScope,
    logical_root_id: &str,
) -> bool {
    (row.scope_id == logical_root_id && row.resource_ids.is_empty())
        || row
            .resource_ids
            .iter()
            .any(|resource_id| resource_id == logical_root_id)
}

fn root_has_any_matching_grant(root: &RootSpec, host_object_grants: &[GrantedMountRoot]) -> bool {
    host_object_grants
        .iter()
        .any(|grant| root.selector.matches(grant))
}

fn root_has_local_matching_grant(
    root: &RootSpec,
    node_id: &NodeId,
    host_object_grants: &[GrantedMountRoot],
) -> bool {
    host_object_grants.iter().any(|grant| {
        host_ref_matches_node_id(&grant.host_ref, node_id) && root.selector.matches(grant)
    })
}

fn runtime_scope_rows_make_root_runnable_locally(
    root: &RootSpec,
    node_id: &NodeId,
    host_object_grants: &[GrantedMountRoot],
    active_rows: &[RuntimeBoundScope],
) -> bool {
    if root_has_local_matching_grant(root, node_id, host_object_grants) {
        return true;
    }

    let mut saw_bare_logical_root_id = false;
    for row in active_rows {
        if !runtime_scope_row_matches_logical_root(row, &root.id) {
            continue;
        }
        if runtime_scope_row_has_explicit_local_resource_id(row, &root.id, node_id) {
            return true;
        }
        if runtime_scope_row_has_bare_logical_root_id(row, &root.id) {
            saw_bare_logical_root_id = true;
        }
    }

    saw_bare_logical_root_id && !root_has_any_matching_grant(root, host_object_grants)
}

fn runtime_managed_local_resource_ids_for_root(
    root: &RootSpec,
    node_id: &NodeId,
    active_rows: &[RuntimeBoundScope],
) -> BTreeSet<String> {
    let mut object_refs = BTreeSet::new();
    for row in active_rows {
        if !runtime_scope_row_matches_logical_root(row, &root.id) {
            continue;
        }
        for resource_id in &row.resource_ids {
            if let Some((host_ref, _)) = resource_id.rsplit_once("::") {
                if host_ref_matches_node_id(host_ref, node_id)
                    && (runtime_scope_resource_matches_logical_root(resource_id, &root.id)
                        || row.scope_id == root.id)
                {
                    object_refs.insert(resource_id.clone());
                }
                continue;
            }
            if resource_id == &root.id {
                object_refs.insert(resource_id.clone());
            }
        }
    }
    object_refs
}

fn synthesize_runtime_managed_local_grants(
    roots: &[RootSpec],
    node_id: &NodeId,
    host_object_grants: &[GrantedMountRoot],
    source_rows: &[RuntimeBoundScope],
    scan_rows: &[RuntimeBoundScope],
) -> Vec<GrantedMountRoot> {
    let mut grants = Vec::new();
    for root in roots {
        if host_object_grants.iter().any(|grant| {
            host_ref_matches_node_id(&grant.host_ref, node_id) && root.selector.matches(grant)
        }) {
            continue;
        }
        let Some(mount_point) = root.selected_mount_point() else {
            continue;
        };
        let mut object_refs =
            runtime_managed_local_resource_ids_for_root(root, node_id, source_rows);
        object_refs.extend(runtime_managed_local_resource_ids_for_root(
            root, node_id, scan_rows,
        ));
        for object_ref in object_refs {
            grants.push(GrantedMountRoot {
                object_ref,
                host_ref: node_id.0.clone(),
                host_ip: node_id.0.clone(),
                host_name: Some(node_id.0.clone()),
                site: None,
                zone: None,
                host_labels: Default::default(),
                mount_point: mount_point.to_path_buf(),
                fs_source: mount_point.display().to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                active: true,
            });
        }
    }
    grants
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceLogicalRootHealthSnapshot {
    pub root_id: String,
    pub status: String,
    pub matched_grants: usize,
    pub active_members: usize,
    pub coverage_mode: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceConcreteRootHealthSnapshot {
    pub root_key: String,
    pub logical_root_id: String,
    pub object_ref: String,
    pub status: String,
    pub coverage_mode: String,
    pub watch_enabled: bool,
    pub scan_enabled: bool,
    pub is_group_primary: bool,
    pub active: bool,
    pub watch_lru_capacity: usize,
    pub audit_interval_ms: u64,
    pub overflow_count: u64,
    pub overflow_pending: bool,
    pub rescan_pending: bool,
    #[serde(default)]
    pub last_rescan_requested_at_us: Option<u64>,
    pub last_rescan_reason: Option<String>,
    pub last_error: Option<String>,
    pub last_audit_started_at_us: Option<u64>,
    pub last_audit_completed_at_us: Option<u64>,
    pub last_audit_duration_ms: Option<u64>,
    pub emitted_batch_count: u64,
    pub emitted_event_count: u64,
    pub emitted_control_event_count: u64,
    pub emitted_data_event_count: u64,
    pub emitted_path_capture_target: Option<String>,
    pub emitted_path_event_count: u64,
    pub last_emitted_at_us: Option<u64>,
    pub last_emitted_origins: Vec<String>,
    pub forwarded_batch_count: u64,
    pub forwarded_event_count: u64,
    pub forwarded_path_event_count: u64,
    pub last_forwarded_at_us: Option<u64>,
    pub last_forwarded_origins: Vec<String>,
    pub current_revision: Option<u64>,
    pub current_stream_generation: Option<u64>,
    pub candidate_revision: Option<u64>,
    pub candidate_stream_generation: Option<u64>,
    pub candidate_status: Option<String>,
    pub draining_revision: Option<u64>,
    pub draining_stream_generation: Option<u64>,
    pub draining_status: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SourceStatusSnapshot {
    pub current_stream_generation: Option<u64>,
    pub logical_roots: Vec<SourceLogicalRootHealthSnapshot>,
    pub concrete_roots: Vec<SourceConcreteRootHealthSnapshot>,
    pub degraded_roots: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SourceTargetedRescanDeliveryAcceptance {
    Accepted,
    NotLocalScanRoot,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SourceProgressSnapshot {
    pub(crate) rescan_observed_epoch: u64,
    pub(crate) scheduled_source_groups: BTreeSet<String>,
    pub(crate) scheduled_scan_groups: BTreeSet<String>,
    pub(crate) published_group_epoch: BTreeMap<String, u64>,
}

impl SourceProgressSnapshot {
    pub(crate) fn published_expected_groups_since(
        &self,
        request_epoch: u64,
        expected_groups: &BTreeSet<String>,
    ) -> bool {
        !expected_groups.is_empty()
            && expected_groups.iter().all(|group_id| {
                self.published_group_epoch
                    .get(group_id)
                    .is_some_and(|epoch| *epoch >= request_epoch)
            })
    }
}

impl SourceStatusSnapshot {
    #[cfg(test)]
    pub(crate) fn published_group_ids(&self) -> BTreeSet<String> {
        self.concrete_roots
            .iter()
            .filter(|entry| entry.active)
            .filter(|entry| {
                entry.forwarded_batch_count > 0
                    || entry.forwarded_event_count > 0
                    || entry.forwarded_path_event_count > 0
                    || entry.last_forwarded_at_us.is_some()
                    || entry.emitted_batch_count > 0
                    || entry.emitted_event_count > 0
                    || entry.emitted_path_event_count > 0
                    || entry.last_emitted_at_us.is_some()
                    || entry.last_audit_completed_at_us.is_some()
            })
            .map(|entry| entry.logical_root_id.clone())
            .collect()
    }
}

fn source_status_publication_marker(status: &SourceStatusSnapshot) -> (u64, u64) {
    let published_batches = status
        .concrete_roots
        .iter()
        .map(|entry| entry.forwarded_batch_count)
        .sum::<u64>();
    let last_published_at_us = status
        .concrete_roots
        .iter()
        .filter_map(|entry| entry.last_forwarded_at_us)
        .max()
        .unwrap_or_default();
    (published_batches, last_published_at_us)
}

fn source_status_rescan_completion_marker(status: &SourceStatusSnapshot) -> u64 {
    status
        .concrete_roots
        .iter()
        .filter_map(|entry| entry.last_audit_completed_at_us)
        .max()
        .unwrap_or_default()
}

#[derive(Debug, Clone, Default)]
struct CurrentStreamPathFrontierStats {
    generation: Option<u64>,
    enqueued_path_origin_counts: BTreeMap<String, u64>,
}

fn map_target_path_to_root(
    target: &[u8],
    root_path: &std::path::Path,
    logical_fallback: bool,
) -> Option<Vec<u8>> {
    let target_path = path_buf_from_bytes(target);
    if target_path == std::path::Path::new("/") {
        return Some(b"/".to_vec());
    }
    if target_path.starts_with(root_path) {
        let rel = target_path.strip_prefix(root_path).ok()?;
        if rel.as_os_str().is_empty() {
            Some(b"/".to_vec())
        } else {
            let mut out = vec![b'/'];
            out.extend_from_slice(&path_to_bytes(rel));
            Some(out)
        }
    } else {
        if logical_fallback {
            if target_path.is_absolute() {
                Some(target.to_vec())
            } else {
                let mut out = vec![b'/'];
                out.extend_from_slice(target);
                Some(out)
            }
        } else {
            None
        }
    }
}

fn target_matches_any_object_monitor_prefix(target: &[u8], roots: &[RootRuntime]) -> bool {
    let target_path = path_buf_from_bytes(target);
    if target_path == std::path::Path::new("/") {
        return true;
    }
    roots
        .iter()
        .any(|root| target_path.starts_with(&root.monitor_path))
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RescanReason {
    Overflow,
    Manual,
    Periodic,
}

fn should_process_rescan_reason(current_is_group_primary: bool, reason: RescanReason) -> bool {
    match reason {
        RescanReason::Manual => true,
        RescanReason::Periodic | RescanReason::Overflow => current_is_group_primary,
    }
}

fn summarize_event_path_origins(events: &[Event], query_path: &[u8]) -> BTreeMap<String, u64> {
    events
        .iter()
        .filter_map(|event| {
            let record = rmp_serde::from_slice::<FileMetaRecord>(event.payload_bytes()).ok()?;
            is_under_query_path(&record.path, query_path)
                .then(|| event.metadata().origin_id.0.clone())
        })
        .fold(BTreeMap::<String, u64>::new(), |mut acc, origin| {
            *acc.entry(origin).or_default() += 1;
            acc
        })
}

/// Lifecycle state of the source app.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleState {
    Scanning,
    Ready,
    Closed,
}

/// File metadata source app.
#[derive(Clone)]
pub struct FSMetaSource {
    config: SourceConfig,
    node_id: NodeId,
    boundary: Option<Arc<dyn ChannelIoSubset>>,
    drift_estimator: Arc<Mutex<DriftEstimator>>,
    logical_clock: Arc<LogicalClock>,
    state: Arc<Mutex<LifecycleState>>,
    shutdown: CancellationToken,
    /// Source runtime mutable-state carrier boundary.
    state_cell: SourceStateCell,
    /// Cluster-wide manual-rescan request carrier.
    manual_rescan_signal: SignalCell,
    /// Background watcher that fans cluster manual-rescan requests into local primary roots.
    manual_rescan_watch_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// Stateful wakeup for durable manual-rescan intents that may outlive a best-effort signal.
    manual_rescan_intent_wake: watch::Sender<u64>,
    /// Monotonic runtime host-object-grants version guard.
    host_object_grants_version: Arc<AtomicU64>,
    /// Runtime control gating snapshot keyed by unit id.
    unit_control: Arc<RuntimeUnitGate>,
    /// Managed endpoint tasks (runtime-boundary lifecycle).
    endpoint_tasks: Arc<Mutex<Vec<ManagedEndpointTask>>>,
    /// Sentinel: health monitoring and self-healing (4th index lifecycle mechanism).
    sentinel: Arc<Sentinel>,
    /// Per-group round-robin cursor for force-find execution.
    force_find_rr: Arc<Mutex<HashMap<String, usize>>>,
    /// Per-group in-flight guard so only one force-find runs at a time.
    force_find_inflight: Arc<Mutex<HashSet<String>>>,
    /// Last successful force-find runner per group for diagnostics/e2e verification.
    force_find_last_runner: Arc<Mutex<BTreeMap<String, String>>>,
    /// Last accepted runtime control summary for local observability snapshots.
    last_control_frame_signals: Arc<Mutex<Vec<String>>>,
    /// Highest manual-rescan control request timestamp accepted from runtime control streams.
    manual_rescan_control_high_watermark_us: Arc<AtomicU64>,
    /// Recently accepted manual-rescan request identities.
    manual_rescan_control_seen_request_ids: Arc<Mutex<VecDeque<String>>>,
    /// Canonical retained source control state for local/runtime replay.
    control_state: Arc<tokio::sync::Mutex<SourceControlState>>,
    /// Coalesced runtime-topology refresh trigger.
    runtime_refresh_dirty: Arc<AtomicBool>,
    /// Pending rescan request coupled to topology refresh.
    runtime_refresh_rescan: Arc<AtomicBool>,
    /// Single-flight guard for background runtime refresh.
    runtime_refresh_running: Arc<AtomicBool>,
    /// Live path-target counts observed at the `pub_()` yield seam.
    yielded_path_origin_counts: Arc<Mutex<BTreeMap<String, u64>>>,
    /// Current shared-stream path-target counts successfully queued before dequeue.
    enqueued_path_origin_counts: Arc<Mutex<CurrentStreamPathFrontierStats>>,
    /// Runtime ownership model for externally addressed source endpoints.
    endpoint_runtime: SourceEndpointRuntime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceEndpointRuntime {
    Local,
    Worker,
}

impl SourceEndpointRuntime {
    fn owns_external_scoped_rescan_routes(self) -> bool {
        matches!(self, Self::Local | Self::Worker)
    }

    fn owns_source_status_route(self, route_key: &str, local_node_id: &str) -> bool {
        match self {
            Self::Local => true,
            Self::Worker => route_key == source_status_request_route_for(local_node_id).0,
        }
    }
}

#[derive(Clone)]
struct RootRuntime {
    logical_root_id: String,
    spec: RootSpec,
    object_ref: String,
    active: bool,
    is_group_primary: bool,
    monitor_path: std::path::PathBuf,
    host_fs: Arc<HostFsFacade>,
    emit_prefix: std::path::PathBuf,
    scanner: Arc<ParallelScanner>,
    mtime_cache: Arc<Mutex<HashMap<std::path::PathBuf, f64>>>,
    epoch_counter: Arc<Mutex<u64>>,
    rescan_tx: tokio::sync::broadcast::Sender<RescanReason>,
}

type SharedWatchManager = Arc<Mutex<WatchManager>>;
type WatchManagerSlot = Arc<Mutex<Option<SharedWatchManager>>>;

#[derive(Debug, Clone, Copy, Default)]
struct ManualRescanIntent {
    requested: u64,
    completed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManualRescanSignalIntent {
    Direct,
    Pending(u64),
    AlreadyCompleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootAuditKind {
    Initial,
    Rescan,
}

#[derive(Debug)]
struct RootAuditCompletion {
    kind: RootAuditKind,
    started_at_us: u64,
    reason_label: &'static str,
    manual_requested_target: Option<u64>,
    result: std::result::Result<AuditScanResult, String>,
}

struct RootTaskHandle {
    cancel: CancellationToken,
    join: Option<std::thread::JoinHandle<()>>,
    ready_rx: tokio::sync::watch::Receiver<bool>,
    rescan_tx: tokio::sync::broadcast::Sender<RescanReason>,
}

impl RootTaskHandle {
    fn is_finished(&self) -> bool {
        self.join.as_ref().is_some_and(|join| join.is_finished())
    }

    fn cancel(&self) {
        self.cancel.cancel();
    }

    async fn cancel_and_join(mut self, timeout: Duration) {
        self.cancel.cancel();
        let Some(join) = self.join.take() else {
            return;
        };
        let deadline = tokio::time::Instant::now() + timeout;
        while !join.is_finished() {
            if tokio::time::Instant::now() >= deadline {
                log::warn!("source root task did not exit within {:?}", timeout);
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = join.join();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RootTaskSignature {
    logical_root_id: String,
    object_ref: String,
    monitor_path: std::path::PathBuf,
    emit_prefix: std::path::PathBuf,
    is_group_primary: bool,
    watch: bool,
    scan: bool,
    audit_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootTaskRole {
    Active,
    Candidate,
}

struct RootTaskSlot {
    revision: u64,
    stream_generation: u64,
    signature: RootTaskSignature,
    handle: RootTaskHandle,
}

struct RootTaskEntry {
    active: RootTaskSlot,
    candidate: Option<RootTaskSlot>,
}

#[derive(Clone)]
struct SourceStreamBinding {
    generation: u64,
    tx: mpsc::Sender<Vec<Event>>,
    rx: Arc<AsyncMutex<mpsc::Receiver<Vec<Event>>>>,
    realtime_tx: mpsc::Sender<Vec<Event>>,
    realtime_rx: Arc<AsyncMutex<mpsc::Receiver<Vec<Event>>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceStreamLane {
    Scan,
    Realtime,
}

struct RootTaskReplacementFallback {
    key: String,
    root: RootRuntime,
    expected_signature: RootTaskSignature,
}

#[derive(Debug, Clone, Default)]
struct LogicalRootHealthEntry {
    status: String,
    matched_grants: usize,
    active_members: usize,
    coverage_mode: String,
}

#[derive(Debug, Clone, Default)]
struct ConcreteRootHealthEntry {
    logical_root_id: String,
    object_ref: String,
    status: String,
    coverage_mode: String,
    watch_enabled: bool,
    scan_enabled: bool,
    is_group_primary: bool,
    active: bool,
    watch_lru_capacity: usize,
    audit_interval_ms: u64,
    overflow_count: u64,
    overflow_pending: bool,
    rescan_pending: bool,
    last_rescan_requested_at_us: Option<u64>,
    last_rescan_reason: Option<String>,
    last_error: Option<String>,
    last_audit_started_at_us: Option<u64>,
    last_audit_completed_at_us: Option<u64>,
    last_audit_duration_ms: Option<u64>,
    emitted_batch_count: u64,
    emitted_event_count: u64,
    emitted_control_event_count: u64,
    emitted_data_event_count: u64,
    emitted_path_event_count: u64,
    last_emitted_at_us: Option<u64>,
    last_emitted_origins: Vec<String>,
    forwarded_batch_count: u64,
    forwarded_event_count: u64,
    forwarded_path_event_count: u64,
    last_forwarded_at_us: Option<u64>,
    last_forwarded_origins: Vec<String>,
    current_revision: Option<u64>,
    current_stream_generation: Option<u64>,
    candidate_revision: Option<u64>,
    candidate_stream_generation: Option<u64>,
    candidate_status: Option<String>,
    draining_revision: Option<u64>,
    draining_stream_generation: Option<u64>,
    draining_status: Option<String>,
}

impl ConcreteRootHealthEntry {
    fn from_root(root: &RootRuntime, config: &SourceConfig) -> Self {
        Self {
            logical_root_id: root.logical_root_id.clone(),
            object_ref: root.object_ref.clone(),
            status: "planned".to_string(),
            coverage_mode: coverage_mode_for_root(
                root.spec.watch,
                root.spec.scan,
                config.lru_capacity,
            )
            .to_string(),
            watch_enabled: root.spec.watch,
            scan_enabled: root.spec.scan,
            is_group_primary: root.is_group_primary,
            active: root.active,
            watch_lru_capacity: if root.spec.watch {
                config.lru_capacity
            } else {
                0
            },
            audit_interval_ms: root
                .spec
                .audit_interval_ms
                .unwrap_or(config.audit_interval.as_millis() as u64),
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: false,
            last_rescan_requested_at_us: None,
            last_rescan_reason: None,
            last_error: None,
            last_audit_started_at_us: None,
            last_audit_completed_at_us: None,
            last_audit_duration_ms: None,
            emitted_batch_count: 0,
            emitted_event_count: 0,
            emitted_control_event_count: 0,
            emitted_data_event_count: 0,
            emitted_path_event_count: 0,
            last_emitted_at_us: None,
            last_emitted_origins: Vec::new(),
            forwarded_batch_count: 0,
            forwarded_event_count: 0,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: None,
            last_forwarded_origins: Vec::new(),
            current_revision: None,
            current_stream_generation: None,
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }
    }

    fn from_task_signature(
        signature: &RootTaskSignature,
        config: &SourceConfig,
        active: bool,
    ) -> Self {
        Self {
            logical_root_id: signature.logical_root_id.clone(),
            object_ref: signature.object_ref.clone(),
            status: "planned".to_string(),
            coverage_mode: coverage_mode_for_root(
                signature.watch,
                signature.scan,
                config.lru_capacity,
            )
            .to_string(),
            watch_enabled: signature.watch,
            scan_enabled: signature.scan,
            is_group_primary: signature.is_group_primary,
            active,
            watch_lru_capacity: if signature.watch {
                config.lru_capacity
            } else {
                0
            },
            audit_interval_ms: signature
                .audit_interval_ms
                .unwrap_or(config.audit_interval.as_millis() as u64),
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: false,
            last_rescan_requested_at_us: None,
            last_rescan_reason: None,
            last_error: None,
            last_audit_started_at_us: None,
            last_audit_completed_at_us: None,
            last_audit_duration_ms: None,
            emitted_batch_count: 0,
            emitted_event_count: 0,
            emitted_control_event_count: 0,
            emitted_data_event_count: 0,
            emitted_path_event_count: 0,
            last_emitted_at_us: None,
            last_emitted_origins: Vec::new(),
            forwarded_batch_count: 0,
            forwarded_event_count: 0,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: None,
            last_forwarded_origins: Vec::new(),
            current_revision: None,
            current_stream_generation: None,
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }
    }

    fn refresh_identity_from_task_signature(
        &mut self,
        signature: &RootTaskSignature,
        config: &SourceConfig,
        active: bool,
    ) {
        self.logical_root_id = signature.logical_root_id.clone();
        self.object_ref = signature.object_ref.clone();
        self.coverage_mode =
            coverage_mode_for_root(signature.watch, signature.scan, config.lru_capacity)
                .to_string();
        self.watch_enabled = signature.watch;
        self.scan_enabled = signature.scan;
        self.is_group_primary = signature.is_group_primary;
        self.active = active;
        self.watch_lru_capacity = if signature.watch {
            config.lru_capacity
        } else {
            0
        };
        self.audit_interval_ms = signature
            .audit_interval_ms
            .unwrap_or(config.audit_interval.as_millis() as u64);
    }
}

#[derive(Default)]
struct FanoutHealthState {
    logical_root: HashMap<String, String>,
    object_ref: HashMap<String, String>,
    logical_root_detail: HashMap<String, LogicalRootHealthEntry>,
    object_ref_detail: HashMap<String, ConcreteRootHealthEntry>,
}

/// In-memory state carrier for source runtime mutable states.
///
/// This boundary makes source state hosting explicit and creates a stable
/// replacement point for future externalized StateCell backends.
#[derive(Clone)]
struct SourceStateCell {
    logical_roots: Arc<Mutex<Vec<RootSpec>>>,
    logical_roots_cell: LogicalRootsCell,
    host_object_grants_cell: HostObjectGrantsCell,
    roots: Arc<Mutex<Vec<RootRuntime>>>,
    root_tasks: Arc<Mutex<HashMap<String, RootTaskEntry>>>,
    stream_binding: Arc<Mutex<Option<SourceStreamBinding>>>,
    manual_rescan_intents: Arc<Mutex<HashMap<String, ManualRescanIntent>>>,
    logical_root_fanout: Arc<Mutex<HashMap<String, Vec<GrantedMountRoot>>>>,
    fanout_health: Arc<Mutex<FanoutHealthState>>,
    host_object_grants: Arc<Mutex<Vec<GrantedMountRoot>>>,
    root_task_revision: Arc<AtomicU64>,
    stream_generation: Arc<AtomicU64>,
    logical_roots_control_generation: Arc<AtomicU64>,
    rescan_request_epoch: Arc<AtomicU64>,
    rescan_request_published_batches: Arc<AtomicU64>,
    rescan_request_last_published_at_us: Arc<AtomicU64>,
    rescan_request_last_audit_completed_at_us: Arc<AtomicU64>,
    rescan_observed_epoch: Arc<AtomicU64>,
    published_group_epoch: Arc<Mutex<BTreeMap<String, u64>>>,
    scan_audit_admission_open: Arc<AtomicBool>,
    commit_boundary: CommitBoundary,
}

impl SourceStateCell {
    fn new(
        logical_roots: Vec<RootSpec>,
        logical_roots_cell: LogicalRootsCell,
        host_object_grants_cell: HostObjectGrantsCell,
        roots: Vec<RootRuntime>,
        logical_root_fanout: HashMap<String, Vec<GrantedMountRoot>>,
        host_object_grants: Vec<GrantedMountRoot>,
        fanout_health: Arc<Mutex<FanoutHealthState>>,
        commit_boundary: CommitBoundary,
    ) -> Self {
        let cell = Self {
            logical_roots: Arc::new(Mutex::new(logical_roots)),
            logical_roots_cell,
            host_object_grants_cell,
            roots: Arc::new(Mutex::new(roots)),
            root_tasks: Arc::new(Mutex::new(HashMap::new())),
            stream_binding: Arc::new(Mutex::new(None)),
            manual_rescan_intents: Arc::new(Mutex::new(HashMap::new())),
            logical_root_fanout: Arc::new(Mutex::new(logical_root_fanout)),
            fanout_health,
            host_object_grants: Arc::new(Mutex::new(host_object_grants)),
            root_task_revision: Arc::new(AtomicU64::new(1)),
            stream_generation: Arc::new(AtomicU64::new(1)),
            logical_roots_control_generation: Arc::new(AtomicU64::new(0)),
            rescan_request_epoch: Arc::new(AtomicU64::new(0)),
            rescan_request_published_batches: Arc::new(AtomicU64::new(0)),
            rescan_request_last_published_at_us: Arc::new(AtomicU64::new(0)),
            rescan_request_last_audit_completed_at_us: Arc::new(AtomicU64::new(0)),
            rescan_observed_epoch: Arc::new(AtomicU64::new(0)),
            published_group_epoch: Arc::new(Mutex::new(BTreeMap::new())),
            scan_audit_admission_open: Arc::new(AtomicBool::new(true)),
            commit_boundary,
        };
        let root_count = lock_or_recover(&cell.logical_roots, "source.state.bootstrap.roots").len();
        let grant_count = lock_or_recover(
            &cell.host_object_grants,
            "source.state.bootstrap.host_object_grants",
        )
        .len();
        cell.record_authoritative_commit(
            "source.bootstrap",
            format!("roots={} host_object_grants={}", root_count, grant_count),
        );
        cell
    }

    fn roots_handle(&self) -> Arc<Mutex<Vec<RootRuntime>>> {
        self.roots.clone()
    }

    fn fanout_health_handle(&self) -> Arc<Mutex<FanoutHealthState>> {
        self.fanout_health.clone()
    }

    fn manual_rescan_intents_handle(&self) -> Arc<Mutex<HashMap<String, ManualRescanIntent>>> {
        self.manual_rescan_intents.clone()
    }

    fn record_authoritative_commit(&self, op: &str, detail: String) {
        self.commit_boundary.record(op, detail);
    }

    fn next_root_task_revision(&self) -> u64 {
        self.root_task_revision.fetch_add(1, Ordering::Relaxed)
    }

    fn next_stream_generation(&self) -> u64 {
        self.stream_generation.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn current_logical_roots_generation(&self) -> u64 {
        let _ = self.logical_roots_cell.refresh_from_boundary_blocking();
        self.logical_roots_cell.current_seq()
    }

    fn logical_roots_control_generation_is_stale(&self, generation: u64) -> bool {
        generation == 0
            || generation < self.current_logical_roots_generation()
            || generation
                <= self
                    .logical_roots_control_generation
                    .load(Ordering::Acquire)
    }

    fn mark_logical_roots_control_generation(&self, generation: u64) {
        if generation == 0 {
            return;
        }
        let mut current = self
            .logical_roots_control_generation
            .load(Ordering::Acquire);
        while generation > current {
            match self.logical_roots_control_generation.compare_exchange(
                current,
                generation,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }

    fn begin_rescan_request_epoch(
        &self,
        published_batches: u64,
        last_published_at_us: u64,
        last_audit_completed_at_us: u64,
    ) -> u64 {
        self.rescan_request_published_batches
            .store(published_batches, Ordering::Release);
        self.rescan_request_last_published_at_us
            .store(last_published_at_us, Ordering::Release);
        self.rescan_request_last_audit_completed_at_us
            .store(last_audit_completed_at_us, Ordering::Release);
        self.rescan_request_epoch
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    fn current_rescan_request_epoch(&self) -> u64 {
        self.rescan_request_epoch.load(Ordering::Acquire)
    }

    fn current_rescan_observed_epoch(&self) -> u64 {
        self.rescan_observed_epoch.load(Ordering::Acquire)
    }

    fn mark_rescan_request_observed(&self, request_epoch: u64) {
        let mut observed = self.rescan_observed_epoch.load(Ordering::Acquire);
        while observed < request_epoch {
            match self.rescan_observed_epoch.compare_exchange_weak(
                observed,
                request_epoch,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => observed = actual,
            }
        }
    }

    fn published_group_epoch_snapshot(&self) -> BTreeMap<String, u64> {
        lock_or_recover(
            &self.published_group_epoch,
            "source.published_group_epoch.snapshot",
        )
        .clone()
    }

    fn mark_group_published(&self, logical_root_id: &str) {
        let request_epoch = self.current_rescan_request_epoch();
        if request_epoch > self.current_rescan_observed_epoch() {
            self.rescan_observed_epoch
                .store(request_epoch, Ordering::Release);
        }
        let mut published_group_epoch = lock_or_recover(
            &self.published_group_epoch,
            "source.published_group_epoch.mark",
        );
        published_group_epoch
            .entry(logical_root_id.to_string())
            .and_modify(|epoch| *epoch = (*epoch).max(request_epoch))
            .or_insert(request_epoch);
    }

    fn mark_rescan_observed_if_publication_advanced(
        &self,
        published_batches: u64,
        last_published_at_us: u64,
        last_audit_completed_at_us: u64,
    ) {
        let request_epoch = self.current_rescan_request_epoch();
        if request_epoch <= self.current_rescan_observed_epoch() {
            return;
        }
        let baseline_published_batches = self
            .rescan_request_published_batches
            .load(Ordering::Acquire);
        let baseline_last_published_at_us = self
            .rescan_request_last_published_at_us
            .load(Ordering::Acquire);
        let baseline_last_audit_completed_at_us = self
            .rescan_request_last_audit_completed_at_us
            .load(Ordering::Acquire);
        if published_batches > baseline_published_batches
            || last_published_at_us > baseline_last_published_at_us
            || last_audit_completed_at_us > baseline_last_audit_completed_at_us
        {
            self.rescan_observed_epoch
                .store(request_epoch, Ordering::Release);
        }
    }

    fn close_scan_audit_admission(&self) {
        self.scan_audit_admission_open
            .store(false, Ordering::Release);
    }

    fn open_scan_audit_admission_if_closed(&self) -> bool {
        self.scan_audit_admission_open
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    #[cfg(test)]
    fn authority_log_len(&self) -> usize {
        self.commit_boundary.len()
    }
}

impl FSMetaSource {
    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    pub(crate) fn mark_publication_output_closed(&self, reason: impl Into<String>) {
        let reason = reason.into();
        *lock_or_recover(
            &self.state_cell.stream_binding,
            "source.publication_output_closed.stream_binding",
        ) = None;
        let root_keys = {
            let tasks = lock_or_recover(
                &self.state_cell.root_tasks,
                "source.publication_output_closed.root_tasks",
            );
            for entry in tasks.values() {
                entry.active.handle.cancel();
                if let Some(candidate) = entry.candidate.as_ref() {
                    candidate.handle.cancel();
                }
            }
            tasks.keys().cloned().collect::<Vec<_>>()
        };
        for root_key in root_keys {
            Self::update_object_health(&self.state_cell.fanout_health, &root_key, "output_closed");
            Self::set_object_last_error(&self.state_cell.fanout_health, &root_key, reason.clone());
        }
    }

    pub fn channel_boundary(&self) -> Option<Arc<dyn ChannelIoSubset>> {
        self.boundary.clone()
    }

    fn should_receive_control_stream_route(&self, route_key: &str) -> bool {
        let generation = self
            .unit_control
            .route_generation(SOURCE_RUNTIME_UNIT_ID, route_key)
            .ok()
            .flatten();
        if std::env::var_os("FSMETA_DEBUG_ROOTS_CONTROL_GATE").is_some()
            && route_key.contains("logical-roots-control")
        {
            eprintln!(
                "fs_meta_source: roots_control gate node={} route={} generation={:?}",
                self.node_id.0, route_key, generation
            );
        }
        let Some(generation) = generation else {
            return false;
        };
        let accepted = self
            .unit_control
            .accept_tick(SOURCE_RUNTIME_UNIT_ID, route_key, generation)
            .unwrap_or(false);
        if std::env::var_os("FSMETA_DEBUG_ROOTS_CONTROL_GATE").is_some()
            && route_key.contains("logical-roots-control")
        {
            eprintln!(
                "fs_meta_source: roots_control gate accept node={} route={} generation={} accepted={}",
                self.node_id.0, route_key, generation, accepted
            );
        }
        accepted
    }

    fn apply_activate_signal(
        &self,
        unit: SourceRuntimeUnit,
        route_key: &str,
        generation: u64,
        bound_scopes: &[RuntimeBoundScope],
    ) -> Result<()> {
        let unit_id = unit.unit_id();
        let accepted =
            self.unit_control
                .apply_activate(unit_id, route_key, generation, bound_scopes)?;
        if !accepted {
            log::debug!(
                "source-fs-meta: ignore stale activate unit={} generation={}",
                unit_id,
                generation
            );
        }
        Ok(())
    }

    fn apply_deactivate_signal(
        &self,
        unit: SourceRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> Result<()> {
        let unit_id = unit.unit_id();
        let accepted = self
            .unit_control
            .apply_deactivate(unit_id, route_key, generation)?;
        if !accepted {
            log::debug!(
                "source-fs-meta: ignore stale deactivate unit={} generation={}",
                unit_id,
                generation
            );
        }
        Ok(())
    }

    fn accept_tick_signal(
        &self,
        unit: SourceRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> Result<()> {
        let unit_id = unit.unit_id();
        let accepted = self
            .unit_control
            .accept_tick(unit_id, route_key, generation)?;
        if !accepted {
            log::debug!(
                "source-fs-meta: ignore stale/inactive unit tick unit={} generation={}",
                unit_id,
                generation
            );
        }
        Ok(())
    }

    async fn apply_host_object_grants_change(
        &self,
        changed: RuntimeHostGrantChange,
    ) -> Result<bool> {
        let current = self.host_object_grants_version.load(Ordering::Relaxed);
        if changed.version <= current {
            return Ok(false);
        }
        let grants = changed
            .grants
            .into_iter()
            .filter(|row| std::path::Path::new(&row.object.mount_point).is_absolute())
            .map(|row| GrantedMountRoot {
                object_ref: row.object_ref,
                host_ref: row.host.host_ref,
                host_ip: row.host.host_ip,
                host_name: row.host.host_name,
                site: row.host.site,
                zone: row.host.zone,
                host_labels: row.host.host_labels,
                mount_point: row.object.mount_point.into(),
                fs_source: row.object.fs_source,
                fs_type: row.object.fs_type,
                mount_options: row.object.mount_options,
                interfaces: row.interfaces,
                active: matches!(row.grant_state, RuntimeHostGrantState::Active),
            })
            .collect::<Vec<_>>();
        self.state_cell
            .host_object_grants_cell
            .replace(changed.version, grants.clone())
            .await?;
        self.host_object_grants_version
            .store(changed.version, Ordering::Relaxed);
        if changed.version > current.saturating_add(1) {
            log::warn!(
                "host object grants version jump detected: current={}, incoming={}; rebuilding full fanout",
                current,
                changed.version
            );
        }
        let root_specs = self.logical_roots_snapshot();
        let fanout = Self::compute_logical_root_fanout(&root_specs, &grants);
        let root_count = root_specs.len();
        let grant_count = grants.len();
        let version = changed.version;
        *lock_or_recover(
            &self.state_cell.host_object_grants,
            "source.control.host_object_grants",
        ) = grants;
        Self::set_logical_root_health(
            &self.state_cell.fanout_health,
            &root_specs,
            &fanout,
            self.config.lru_capacity,
        );
        *lock_or_recover(
            &self.state_cell.logical_root_fanout,
            "source.control.logical_root_fanout",
        ) = fanout;
        self.state_cell.record_authoritative_commit(
            "runtime.host_object_grants.changed",
            format!(
                "version={} roots={} host_object_grants={}",
                version, root_count, grant_count
            ),
        );
        Ok(true)
    }

    pub(crate) async fn perform_apply_orchestration_signals(
        &self,
        signals: &[SourceControlSignal],
    ) -> Result<()> {
        let mut refresh_runtime_topology = false;
        let mut applied_host_grant_changes = 0usize;
        for changed in signals.iter().filter_map(|signal| match signal {
            SourceControlSignal::RuntimeHostGrantChange { changed, .. } => Some(changed),
            _ => None,
        }) {
            if self
                .apply_host_object_grants_change(changed.clone())
                .await?
            {
                applied_host_grant_changes += 1;
                refresh_runtime_topology = true;
            }
        }
        eprintln!(
            "fs_meta_source: apply_orchestration_signals count={} has_runtime_state={}",
            signals.len(),
            self.unit_control.has_runtime_state()
        );
        for signal in signals {
            match signal {
                SourceControlSignal::Activate {
                    unit,
                    route_key,
                    generation,
                    bound_scopes,
                    ..
                } => {
                    if debug_control_scope_capture_enabled() {
                        eprintln!(
                            "fs_meta_source: control_scope_capture node={} unit={} route={} generation={} scopes={:?}",
                            self.node_id.0,
                            unit.unit_id(),
                            route_key,
                            generation,
                            summarize_bound_scopes(bound_scopes)
                        );
                    }
                    self.apply_activate_signal(*unit, route_key, *generation, bound_scopes)?;
                    refresh_runtime_topology = true;
                }
                SourceControlSignal::Deactivate {
                    unit,
                    route_key,
                    generation,
                    ..
                } => {
                    self.apply_deactivate_signal(*unit, route_key, *generation)?;
                    refresh_runtime_topology = true;
                }
                SourceControlSignal::Tick {
                    unit,
                    route_key,
                    generation,
                    ..
                } => {
                    // `RuntimeUnitTick` stays explicit at the source boundary even
                    // though runtime::orchestration pre-decodes the envelope into
                    // `SourceControlSignal::Tick`.
                    // Unit tick is runtime-owned scheduling signal.
                    // Source accepts and validates the envelope kind while keeping
                    // business data path independent.
                    self.accept_tick_signal(*unit, route_key, *generation)?;
                }
                SourceControlSignal::RuntimeHostGrantChange { changed, .. } => {
                    let _ = changed;
                }
                SourceControlSignal::ManualRescan { envelope } => {
                    if !self.accept_manual_rescan_control_envelope(envelope)? {
                        continue;
                    }
                    let roots_snapshot = lock_or_recover(
                        &self.state_cell.roots,
                        "source.control.manual_rescan.roots",
                    )
                    .clone();
                    FSMetaSource::request_rescan_on_primary_roots(
                        &roots_snapshot,
                        Some(&self.state_cell.root_tasks),
                        Some(&self.state_cell.fanout_health),
                        Some(&self.state_cell.manual_rescan_intents),
                        Some(&self.manual_rescan_intent_wake),
                        "manual",
                    );
                }
                SourceControlSignal::Passthrough(_) => {
                    return Err(CnxError::NotSupported(
                        "source-fs-meta: unsupported control envelope".into(),
                    ));
                }
            }
        }

        eprintln!(
            "fs_meta_source: orchestration accepted actions={} refresh_runtime_topology={} source_groups={:?} scan_groups={:?}",
            applied_host_grant_changes,
            refresh_runtime_topology,
            self.scheduled_source_group_ids()?,
            self.scheduled_scan_group_ids()?
        );
        if refresh_runtime_topology {
            self.schedule_runtime_refresh(true);
        }
        Ok(())
    }

    pub(crate) async fn apply_orchestration_signals(
        &self,
        signals: &[SourceControlSignal],
    ) -> Result<()> {
        self.perform_apply_orchestration_signals(signals).await?;
        self.control_state
            .lock()
            .await
            .mark_route_state_applied(signals);
        Ok(())
    }

    pub(crate) async fn control_signals_with_replay(
        &self,
        signals: &[SourceControlSignal],
    ) -> Vec<SourceControlSignal> {
        let mut desired = self.control_state.lock().await.clone();
        desired.retain_signals(signals);
        desired.replay_signals()
    }

    pub(crate) async fn record_retained_control_signals(&self, signals: &[SourceControlSignal]) {
        self.control_state.lock().await.retain_signals(signals);
        if !signals.is_empty() {
            let mut summary = lock_or_recover(
                &self.last_control_frame_signals,
                "source.retained_control.last_control_frame_signals",
            );
            apply_retained_control_summary(&mut summary, signals);
        }
    }

    pub(crate) async fn retained_control_state_snapshot(&self) -> SourceControlState {
        self.control_state.lock().await.clone()
    }

    #[cfg(test)]
    pub(crate) async fn retained_control_state_for_tests(&self) -> SourceControlState {
        self.retained_control_state_snapshot().await
    }

    fn schedule_runtime_refresh(&self, trigger_rescan: bool) {
        self.runtime_refresh_dirty.store(true, Ordering::Release);
        if trigger_rescan {
            self.runtime_refresh_rescan.store(true, Ordering::Release);
        }
        if self
            .runtime_refresh_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let source = self.clone();
        let _ = std::thread::Builder::new()
            .name(format!("fs-meta-source-runtime-refresh-{}", self.node_id.0))
            .spawn(move || {
                crate::runtime_app::shared_tokio_runtime().block_on(source.runtime_refresh_loop());
            });
    }

    async fn runtime_refresh_loop(self) {
        loop {
            let trigger_rescan = self.runtime_refresh_rescan.swap(false, Ordering::AcqRel);
            self.runtime_refresh_dirty.store(false, Ordering::Release);
            if let Err(err) = self.refresh_runtime_roots(trigger_rescan).await {
                if !self.shutdown.is_cancelled() {
                    log::error!("source runtime refresh failed: {err}");
                }
            }
            if self.shutdown.is_cancelled() {
                break;
            }
            if !self.runtime_refresh_dirty.load(Ordering::Acquire)
                && !self.runtime_refresh_rescan.load(Ordering::Acquire)
            {
                break;
            }
        }

        self.runtime_refresh_running.store(false, Ordering::Release);
        if self.shutdown.is_cancelled() {
            return;
        }
        if (self.runtime_refresh_dirty.load(Ordering::Acquire)
            || self.runtime_refresh_rescan.load(Ordering::Acquire))
            && self
                .runtime_refresh_running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            let source = self.clone();
            let _ = std::thread::Builder::new()
                .name(format!("fs-meta-source-runtime-refresh-{}", self.node_id.0))
                .spawn(move || {
                    crate::runtime_app::shared_tokio_runtime()
                        .block_on(source.runtime_refresh_loop());
                });
        }
    }

    fn scheduled_group_ids(&self, unit: SourceRuntimeUnit) -> Result<Option<BTreeSet<String>>> {
        if !self.unit_control.has_runtime_state() {
            return Ok(None);
        }
        if self.boundary.is_none()
            && self
                .unit_control
                .active_route_keys(unit.unit_id())?
                .is_empty()
        {
            return Ok(None);
        }
        let groups = match self.unit_control.unit_state(unit.unit_id())? {
            Some((true, rows)) => {
                let logical_roots = self.logical_roots_snapshot();
                let logical_root_ids = logical_roots
                    .iter()
                    .map(|root| root.id.as_str())
                    .collect::<BTreeSet<_>>();
                let active_root_groups = rows
                    .iter()
                    .filter(|row| logical_root_ids.contains(row.scope_id.as_str()))
                    .map(|row| row.scope_id.clone())
                    .collect::<BTreeSet<_>>();
                if !rows.is_empty() && active_root_groups.is_empty() {
                    return Ok(None);
                }
                let host_object_grants = self.host_object_grants_snapshot();
                let runnable_local_groups = logical_roots
                    .iter()
                    .filter(|root| match unit {
                        SourceRuntimeUnit::Source => root.watch || root.scan,
                        SourceRuntimeUnit::Scan => root.scan,
                    })
                    .filter(|root| {
                        runtime_scope_rows_make_root_runnable_locally(
                            root,
                            &self.node_id,
                            &host_object_grants,
                            &rows,
                        )
                    })
                    .map(|root| root.id.clone())
                    .collect::<BTreeSet<_>>();
                active_root_groups
                    .intersection(&runnable_local_groups)
                    .cloned()
                    .collect::<BTreeSet<_>>()
            }
            Some((false, _)) | None => BTreeSet::new(),
        };
        Ok(Some(groups))
    }

    fn runtime_route_group_ids(
        &self,
        unit: SourceRuntimeUnit,
        route_key: &str,
    ) -> Result<Option<BTreeSet<String>>> {
        if !self.unit_control.has_runtime_state() {
            return Ok(None);
        }
        let Some((_, bound_scopes)) = self
            .unit_control
            .active_route_state(unit.unit_id(), route_key)?
        else {
            return Ok(Some(BTreeSet::new()));
        };
        Ok(Some(
            bound_scopes
                .into_iter()
                .map(|scope| scope.scope_id)
                .filter(|scope_id| !scope_id.trim().is_empty())
                .collect(),
        ))
    }

    fn runtime_scope_rows_local_group_ids(
        &self,
        unit: SourceRuntimeUnit,
        rows: &[RuntimeBoundScope],
    ) -> Option<BTreeSet<String>> {
        let logical_roots = self.logical_roots_snapshot();
        let logical_root_ids = logical_roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<BTreeSet<_>>();
        let active_root_groups = rows
            .iter()
            .filter(|row| logical_root_ids.contains(row.scope_id.as_str()))
            .map(|row| row.scope_id.clone())
            .collect::<BTreeSet<_>>();
        if !rows.is_empty() && active_root_groups.is_empty() {
            return None;
        }
        let host_object_grants = self.host_object_grants_snapshot();
        let runnable_local_groups = logical_roots
            .iter()
            .filter(|root| match unit {
                SourceRuntimeUnit::Source => root.watch || root.scan,
                SourceRuntimeUnit::Scan => root.scan,
            })
            .filter(|root| {
                runtime_scope_rows_make_root_runnable_locally(
                    root,
                    &self.node_id,
                    &host_object_grants,
                    rows,
                )
            })
            .map(|root| root.id.clone())
            .collect::<BTreeSet<_>>();
        Some(
            active_root_groups
                .intersection(&runnable_local_groups)
                .cloned()
                .collect::<BTreeSet<_>>(),
        )
    }

    fn runtime_route_local_group_ids(
        &self,
        unit: SourceRuntimeUnit,
        route_key: &str,
    ) -> Result<Option<BTreeSet<String>>> {
        if !self.unit_control.has_runtime_state() {
            return Ok(None);
        }
        let Some((_, bound_scopes)) = self
            .unit_control
            .active_route_state(unit.unit_id(), route_key)?
        else {
            return Ok(Some(BTreeSet::new()));
        };
        Ok(self.runtime_scope_rows_local_group_ids(unit, &bound_scopes))
    }

    fn runtime_route_accepts_status_request(
        &self,
        route_key: &str,
        manual_rescan_delivery_evidence: bool,
    ) -> Result<bool> {
        if self
            .runtime_route_group_ids(SourceRuntimeUnit::Source, route_key)?
            .is_none_or(|groups| !groups.is_empty())
        {
            return Ok(true);
        }
        if manual_rescan_delivery_evidence
            && route_key == source_status_request_route_for(&self.node_id.0).0
        {
            return Ok(self
                .runtime_route_group_ids(
                    SourceRuntimeUnit::Source,
                    &source_rescan_request_route_for(&self.node_id.0).0,
                )?
                .is_none_or(|groups| !groups.is_empty()));
        }
        Ok(false)
    }

    fn runtime_status_route_targets_local_manual_rescan_delivery(
        &self,
        route_key: &str,
    ) -> Result<bool> {
        let delivery_route_key = if route_key == source_status_request_route_for(&self.node_id.0).0
        {
            source_rescan_request_route_for(&self.node_id.0).0
        } else {
            route_key.to_string()
        };
        Ok(self
            .runtime_route_local_group_ids(SourceRuntimeUnit::Source, &delivery_route_key)?
            .is_none_or(|groups| !groups.is_empty()))
    }

    fn should_start_source_status_endpoint_for_route(&self, route_key: &str) -> bool {
        if !self
            .endpoint_runtime
            .owns_source_status_route(route_key, &self.node_id.0)
        {
            return false;
        }
        if self
            .unit_control
            .route_active(SOURCE_RUNTIME_UNIT_ID, route_key)
            .unwrap_or(false)
        {
            return true;
        }
        if route_key == source_status_request_route_for(&self.node_id.0).0
            && self
                .unit_control
                .route_active(
                    SOURCE_RUNTIME_UNIT_ID,
                    &source_rescan_request_route_for(&self.node_id.0).0,
                )
                .unwrap_or(false)
        {
            return true;
        }
        matches!(self.endpoint_runtime, SourceEndpointRuntime::Local)
            && !self.unit_control.has_runtime_state()
    }

    pub(crate) fn snapshot_scheduled_source_group_ids(&self) -> Result<Option<BTreeSet<String>>> {
        self.scheduled_group_ids(SourceRuntimeUnit::Source)
    }

    pub(crate) fn snapshot_scheduled_scan_group_ids(&self) -> Result<Option<BTreeSet<String>>> {
        self.scheduled_group_ids(SourceRuntimeUnit::Scan)
    }

    pub(crate) fn scheduled_source_group_ids_snapshot(&self) -> Result<Option<BTreeSet<String>>> {
        self.snapshot_scheduled_source_group_ids()
    }

    pub(crate) fn scheduled_scan_group_ids_snapshot(&self) -> Result<Option<BTreeSet<String>>> {
        self.snapshot_scheduled_scan_group_ids()
    }

    pub fn scheduled_source_group_ids(&self) -> Result<Option<BTreeSet<String>>> {
        self.scheduled_source_group_ids_snapshot()
    }

    pub fn scheduled_scan_group_ids(&self) -> Result<Option<BTreeSet<String>>> {
        self.scheduled_scan_group_ids_snapshot()
    }

    pub(crate) async fn refresh_runtime_roots(&self, trigger_rescan: bool) -> Result<()> {
        let root_specs = self.logical_roots_snapshot();
        let host_object_grants = self.host_object_grants_snapshot();
        let source_rows = match self
            .unit_control
            .unit_state(SourceRuntimeUnit::Source.unit_id())?
        {
            Some((true, rows)) => rows,
            Some((false, _)) | None => Vec::new(),
        };
        let scan_rows = match self
            .unit_control
            .unit_state(SourceRuntimeUnit::Scan.unit_id())?
        {
            Some((true, rows)) => rows,
            Some((false, _)) | None => Vec::new(),
        };
        let mut effective_host_object_grants = host_object_grants.clone();
        effective_host_object_grants.extend(synthesize_runtime_managed_local_grants(
            &root_specs,
            &self.node_id,
            &host_object_grants,
            &source_rows,
            &scan_rows,
        ));
        let source_groups = self.scheduled_group_ids(SourceRuntimeUnit::Source)?;
        let scan_groups = self.scheduled_group_ids(SourceRuntimeUnit::Scan)?;
        let effective_scan_groups = if self.unit_control.has_runtime_state()
            && source_groups.is_some()
            && scan_groups.is_none()
            && scan_rows.is_empty()
        {
            source_groups.clone()
        } else {
            scan_groups.clone()
        };
        let desired = Self::build_root_runtimes(
            &self.config,
            &self.node_id,
            self.boundary.clone(),
            &root_specs,
            &effective_host_object_grants,
            source_groups.as_ref(),
            effective_scan_groups.as_ref(),
        );
        let current = lock_or_recover(
            &self.state_cell.roots,
            "source.refresh_runtime_roots.current",
        )
        .clone();
        let desired = Self::preserve_existing_root_runtime_state(&current, desired);
        let current_signature = Self::runtime_topology_signature(&current);
        let desired_signature = Self::runtime_topology_signature(&desired);
        let topology_changed = desired_signature != current_signature;
        let root_tasks_need_reconcile = self.root_tasks_need_reconcile(&desired);
        Self::sync_object_runtime_health(&self.state_cell.fanout_health, &desired, &self.config);
        if topology_changed {
            *lock_or_recover(&self.state_cell.roots, "source.refresh_runtime_roots") =
                desired.clone();
        }
        if topology_changed || root_tasks_need_reconcile {
            let desired_roots = if topology_changed {
                lock_or_recover(&self.state_cell.roots, "source.refresh_runtime_roots.read").clone()
            } else {
                desired
            };
            self.reconcile_root_tasks(&desired_roots).await;
        }
        if trigger_rescan && (topology_changed || root_tasks_need_reconcile) {
            self.trigger_topology_rescan_when_ready().await;
        }
        Ok(())
    }

    async fn trigger_topology_rescan_when_ready(&self) {
        if !self.wait_for_group_primary_scan_roots_ready().await {
            log::debug!(
                "source-fs-meta: queue topology rescan before primary scan roots report running"
            );
        }
        let roots = lock_or_recover(&self.state_cell.roots, "source.topology_rescan.roots").clone();
        Self::request_rescan_on_primary_roots(
            &roots,
            Some(&self.state_cell.root_tasks),
            Some(&self.state_cell.fanout_health),
            None,
            None,
            "topology",
        );
    }

    fn build_root_runtimes(
        config: &SourceConfig,
        node_id: &NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        roots: &[RootSpec],
        host_object_grants: &[GrantedMountRoot],
        source_groups: Option<&BTreeSet<String>>,
        scan_groups: Option<&BTreeSet<String>>,
    ) -> Vec<RootRuntime> {
        let group_primary = if let Some(scan_groups) = scan_groups {
            Self::compute_local_runtime_group_primary_by_logical_root(
                roots,
                host_object_grants,
                node_id,
                scan_groups,
            )
        } else {
            Self::compute_group_primary_by_logical_root(roots, host_object_grants)
        };
        let mut runtimes = Vec::new();
        for root in roots {
            let source_scheduled = source_groups.is_none_or(|groups| groups.contains(&root.id));
            let scan_scheduled = scan_groups.is_none_or(|groups| groups.contains(&root.id));
            if !source_scheduled && !scan_scheduled {
                continue;
            }
            let members = host_object_grants
                .iter()
                // Source/watch/scan runtimes are always programmed against the
                // locally bound host. Remote hosts participate through their own
                // source realizations plus routed query fan-in, not by spawning
                // local source tasks for foreign grants.
                .filter(|grant| {
                    host_ref_matches_node_id(&grant.host_ref, node_id)
                        && root.selector.matches(grant)
                })
                .cloned()
                .collect::<Vec<_>>();
            if members.is_empty() {
                continue;
            }
            for member in members {
                let monitor_path = match root.monitor_path_for(&member) {
                    Ok(path) => path,
                    Err(err) => {
                        log::warn!(
                            "logical root '{}' matched object '{}' but monitor path resolve failed: {}",
                            root.id,
                            member.object_ref,
                            err
                        );
                        continue;
                    }
                };
                let mut concrete = root.clone();
                concrete.id = format!("{}@{}", root.id, member.object_ref);
                concrete.watch = root.watch && source_scheduled;
                concrete.scan = root.scan && scan_scheduled;
                let emit_prefix = std::path::PathBuf::from("/");
                let host_fs = match resolve_host_fs_facade(
                    member.mount_point.clone(),
                    monitor_path.clone(),
                    boundary.clone(),
                    node_id,
                    &member.host_ref,
                    &member.object_ref,
                    &member.fs_type,
                    &member.fs_source,
                ) {
                    Ok(host_fs) => Arc::new(host_fs),
                    Err(err) => {
                        log::warn!(
                            "logical root '{}' matched member '{}' but host-fs backend resolve failed: {}",
                            root.id,
                            member.object_ref,
                            err
                        );
                        continue;
                    }
                };
                let scanner_host_fs: Arc<dyn HostFs> = host_fs.clone();
                let scanner = Arc::new(ParallelScanner::new(
                    monitor_path.clone(),
                    emit_prefix.clone(),
                    config.scan_workers,
                    config.batch_size,
                    config.max_scan_events,
                    NodeId(member.object_ref.clone()),
                    scanner_host_fs,
                ));
                let (rescan_tx, _) = tokio::sync::broadcast::channel(16);
                runtimes.push(RootRuntime {
                    logical_root_id: root.id.clone(),
                    spec: concrete,
                    object_ref: member.object_ref.clone(),
                    active: member.active,
                    is_group_primary: group_primary
                        .get(&root.id)
                        .is_some_and(|primary| primary == &member.object_ref),
                    monitor_path,
                    host_fs,
                    emit_prefix,
                    scanner,
                    mtime_cache: Arc::new(Mutex::new(HashMap::new())),
                    epoch_counter: Arc::new(Mutex::new(0)),
                    rescan_tx,
                });
            }
        }
        runtimes
    }

    fn compute_group_primary_by_logical_root(
        roots: &[RootSpec],
        host_object_grants: &[GrantedMountRoot],
    ) -> HashMap<String, String> {
        let mut primary = HashMap::new();
        for root in roots {
            let mut active_member_ids = host_object_grants
                .iter()
                .filter(|grant| root.selector.matches(grant) && grant.active)
                .map(|grant| grant.object_ref.clone())
                .collect::<Vec<_>>();
            active_member_ids.sort();
            active_member_ids.dedup();
            let mut member_ids = host_object_grants
                .iter()
                .filter(|grant| root.selector.matches(grant))
                .map(|grant| grant.object_ref.clone())
                .collect::<Vec<_>>();
            member_ids.sort();
            member_ids.dedup();
            let selected = active_member_ids
                .first()
                .cloned()
                .or_else(|| member_ids.first().cloned());
            if let Some(primary_member) = selected {
                primary.insert(root.id.clone(), primary_member);
            }
        }
        primary
    }

    fn compute_local_runtime_group_primary_by_logical_root(
        roots: &[RootSpec],
        host_object_grants: &[GrantedMountRoot],
        node_id: &NodeId,
        scan_groups: &BTreeSet<String>,
    ) -> HashMap<String, String> {
        let mut primary = HashMap::new();
        for root in roots {
            if !scan_groups.contains(&root.id) {
                continue;
            }
            let mut active_member_ids = host_object_grants
                .iter()
                .filter(|grant| {
                    root.selector.matches(grant)
                        && grant.active
                        && host_ref_matches_node_id(&grant.host_ref, node_id)
                })
                .map(|grant| grant.object_ref.clone())
                .collect::<Vec<_>>();
            active_member_ids.sort();
            active_member_ids.dedup();
            let mut member_ids = host_object_grants
                .iter()
                .filter(|grant| {
                    root.selector.matches(grant)
                        && host_ref_matches_node_id(&grant.host_ref, node_id)
                })
                .map(|grant| grant.object_ref.clone())
                .collect::<Vec<_>>();
            member_ids.sort();
            member_ids.dedup();
            let selected = active_member_ids
                .first()
                .cloned()
                .or_else(|| member_ids.first().cloned());
            if let Some(primary_member) = selected {
                primary.insert(root.id.clone(), primary_member);
            }
        }
        primary
    }

    fn compute_logical_root_fanout(
        roots: &[RootSpec],
        host_object_grants: &[GrantedMountRoot],
    ) -> HashMap<String, Vec<GrantedMountRoot>> {
        let mut by_root = HashMap::new();
        for root in roots {
            let members = host_object_grants
                .iter()
                .filter(|grant| root.selector.matches(grant))
                .cloned()
                .collect::<Vec<_>>();
            by_root.insert(root.id.clone(), members);
        }
        by_root
    }

    fn root_runtime_key(root: &RootRuntime) -> String {
        format!(
            "{}@{}@{}",
            root.logical_root_id,
            root.object_ref,
            root.monitor_path.display()
        )
    }

    fn root_task_signature(root: &RootRuntime) -> RootTaskSignature {
        RootTaskSignature {
            logical_root_id: root.logical_root_id.clone(),
            object_ref: root.object_ref.clone(),
            monitor_path: root.monitor_path.clone(),
            emit_prefix: root.emit_prefix.clone(),
            is_group_primary: root.is_group_primary,
            watch: root.spec.watch,
            scan: root.spec.scan,
            audit_interval_ms: root.spec.audit_interval_ms,
        }
    }

    fn preserve_existing_root_runtime_state(
        current_roots: &[RootRuntime],
        mut desired_roots: Vec<RootRuntime>,
    ) -> Vec<RootRuntime> {
        let current_by_key = current_roots
            .iter()
            .map(|root| (Self::root_runtime_key(root), root))
            .collect::<HashMap<_, _>>();
        for desired in &mut desired_roots {
            let key = Self::root_runtime_key(desired);
            let Some(current) = current_by_key.get(&key) else {
                continue;
            };
            if Self::root_task_signature(current) != Self::root_task_signature(desired) {
                continue;
            }
            desired.host_fs = current.host_fs.clone();
            desired.scanner = current.scanner.clone();
            desired.mtime_cache = current.mtime_cache.clone();
            desired.epoch_counter = current.epoch_counter.clone();
            desired.rescan_tx = current.rescan_tx.clone();
        }
        desired_roots
    }

    fn runtime_topology_signature(roots: &[RootRuntime]) -> Vec<String> {
        let mut out = roots
            .iter()
            .map(|root| {
                format!(
                    "{}|active={}|primary={}|watch={}|scan={}|audit={:?}",
                    Self::root_runtime_key(root),
                    root.active,
                    root.is_group_primary,
                    root.spec.watch,
                    root.spec.scan,
                    root.spec.audit_interval_ms,
                )
            })
            .collect::<Vec<_>>();
        out.sort();
        out
    }

    fn root_tasks_need_reconcile(&self, desired_roots: &[RootRuntime]) -> bool {
        let stream_binding = lock_or_recover(
            &self.state_cell.stream_binding,
            "source.root_tasks_need_reconcile.stream_binding",
        )
        .clone();
        let Some(stream_binding) = stream_binding else {
            return false;
        };

        let desired = desired_roots
            .iter()
            .filter(|root| root.active)
            .map(|root| {
                (
                    Self::root_runtime_key(root),
                    Self::root_task_signature(root),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let tasks = lock_or_recover(
            &self.state_cell.root_tasks,
            "source.root_tasks_need_reconcile.root_tasks",
        );
        if tasks.len() != desired.len() {
            return true;
        }
        for (key, signature) in desired {
            let Some(entry) = tasks.get(&key) else {
                return true;
            };
            if entry.active.signature != signature
                || entry.active.handle.is_finished()
                || entry.active.stream_generation != stream_binding.generation
            {
                return true;
            }
            if entry.candidate.as_ref().is_some_and(|candidate| {
                candidate.signature != signature
                    || candidate.handle.is_finished()
                    || candidate.stream_generation != stream_binding.generation
            }) {
                return true;
            }
        }
        false
    }

    fn root_current_is_group_primary(
        roots_handle: &Arc<Mutex<Vec<RootRuntime>>>,
        root_key: &str,
    ) -> bool {
        lock_or_recover(roots_handle, "source.root.current_primary")
            .iter()
            .find(|root| Self::root_runtime_key(root) == root_key)
            .is_some_and(|root| root.active && root.is_group_primary)
    }

    fn root_current_is_active(roots_handle: &Arc<Mutex<Vec<RootRuntime>>>, root_key: &str) -> bool {
        lock_or_recover(roots_handle, "source.root.current_active")
            .iter()
            .find(|root| Self::root_runtime_key(root) == root_key)
            .is_some_and(|root| root.active)
    }

    fn root_current_is_group_primary_scan_enabled(
        roots_handle: &Arc<Mutex<Vec<RootRuntime>>>,
        root_key: &str,
    ) -> bool {
        lock_or_recover(roots_handle, "source.root.current_primary_scan")
            .iter()
            .find(|root| Self::root_runtime_key(root) == root_key)
            .is_some_and(|root| root.active && root.is_group_primary && root.spec.scan)
    }

    async fn wait_for_task_handles_ready(
        ready_receivers: &mut [tokio::sync::watch::Receiver<bool>],
        timeout: Duration,
    ) -> bool {
        if ready_receivers.is_empty() {
            return true;
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if ready_receivers.iter().all(|rx| *rx.borrow()) {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn cancel_root_task_slot(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        slot: RootTaskSlot,
        config: &SourceConfig,
        active: bool,
        retired: bool,
    ) {
        Self::mark_root_task_draining(
            fanout_health,
            root_key,
            slot.revision,
            slot.stream_generation,
            &slot.signature,
            config,
            active,
            "draining",
        );
        slot.handle.cancel_and_join(Duration::from_secs(2)).await;
        Self::finish_root_task_draining(
            fanout_health,
            root_key,
            slot.revision,
            slot.stream_generation,
            retired,
        );
    }

    fn close_rescan_channels(rescan_channel_open: &mut bool, periodic_channel_open: &mut bool) {
        *rescan_channel_open = false;
        *periodic_channel_open = false;
    }

    fn set_logical_root_health(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        roots: &[RootSpec],
        fanout: &HashMap<String, Vec<GrantedMountRoot>>,
        watch_lru_capacity: usize,
    ) {
        let mut state = lock_or_recover(fanout_health, "source.logical_root_health");
        state.logical_root.clear();
        state.logical_root_detail.clear();
        for root in roots {
            let matched_grants = fanout.get(&root.id).map_or(0, Vec::len);
            let active_members = fanout
                .get(&root.id)
                .map(|members| members.iter().filter(|member| member.active).count())
                .unwrap_or(0);
            let status = if matched_grants > 0 {
                "ready"
            } else {
                "no_visible_export_match"
            };
            state
                .logical_root
                .insert(root.id.clone(), status.to_string());
            state.logical_root_detail.insert(
                root.id.clone(),
                LogicalRootHealthEntry {
                    status: status.to_string(),
                    matched_grants,
                    active_members,
                    coverage_mode: coverage_mode_for_root(
                        root.watch,
                        root.scan,
                        watch_lru_capacity,
                    )
                    .to_string(),
                },
            );
        }
    }

    fn sync_object_runtime_health(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        roots: &[RootRuntime],
        config: &SourceConfig,
    ) {
        let mut state = lock_or_recover(fanout_health, "source.object_health.sync");
        let previous_status = state.object_ref.clone();
        let previous_detail = state.object_ref_detail.clone();
        state.object_ref.clear();
        state.object_ref_detail.clear();
        for root in roots {
            let root_key = Self::root_runtime_key(root);
            let status = previous_status
                .get(&root_key)
                .cloned()
                .unwrap_or_else(|| "planned".to_string());
            let mut detail = previous_detail
                .get(&root_key)
                .cloned()
                .unwrap_or_else(|| ConcreteRootHealthEntry::from_root(root, config));
            detail.logical_root_id = root.logical_root_id.clone();
            detail.object_ref = root.object_ref.clone();
            detail.status = status.clone();
            detail.coverage_mode =
                coverage_mode_for_root(root.spec.watch, root.spec.scan, config.lru_capacity)
                    .to_string();
            detail.watch_enabled = root.spec.watch;
            detail.scan_enabled = root.spec.scan;
            detail.is_group_primary = root.is_group_primary;
            detail.active = root.active;
            detail.watch_lru_capacity = if root.spec.watch {
                config.lru_capacity
            } else {
                0
            };
            detail.audit_interval_ms = root
                .spec
                .audit_interval_ms
                .unwrap_or(config.audit_interval.as_millis() as u64);
            state.object_ref.insert(root_key.clone(), status);
            state.object_ref_detail.insert(root_key, detail);
        }
    }

    fn update_object_health(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        status: impl Into<String>,
    ) {
        let status = status.into();
        let mut health = lock_or_recover(fanout_health, "source.object_health");
        health
            .object_ref
            .insert(root_key.to_string(), status.clone());
        if let Some(detail) = health.object_ref_detail.get_mut(root_key) {
            detail.status = status.clone();
            if status == "running" {
                detail.last_error = None;
            } else if let Some(reason) = status.split_once(": ").map(|(_, reason)| reason) {
                detail.last_error = Some(reason.to_string());
            }
        }
    }

    fn update_root_task_slot_health(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        revision: u64,
        stream_generation: u64,
        role: RootTaskRole,
        signature: &RootTaskSignature,
        config: &SourceConfig,
        active: bool,
        status: impl Into<String>,
    ) {
        let status = status.into();
        let mut health = lock_or_recover(fanout_health, "source.object_health.slot");
        match role {
            RootTaskRole::Active => {
                let keep_output_closed =
                    health
                        .object_ref_detail
                        .get(root_key)
                        .is_some_and(|detail| {
                            detail.status == "output_closed"
                                && status != "output_closed"
                                && detail
                                    .current_stream_generation
                                    .is_none_or(|current| stream_generation <= current)
                        });
                if keep_output_closed {
                    return;
                }
                health
                    .object_ref
                    .insert(root_key.to_string(), status.clone());
                let detail = health
                    .object_ref_detail
                    .entry(root_key.to_string())
                    .or_insert_with(|| {
                        ConcreteRootHealthEntry::from_task_signature(signature, config, active)
                    });
                detail.refresh_identity_from_task_signature(signature, config, active);
                detail.status = status.clone();
                detail.current_revision = Some(revision);
                detail.current_stream_generation = Some(stream_generation);
                if status == "running" {
                    detail.last_error = None;
                } else if let Some(reason) = status.split_once(": ").map(|(_, reason)| reason) {
                    detail.last_error = Some(reason.to_string());
                }
            }
            RootTaskRole::Candidate => {
                let detail = health
                    .object_ref_detail
                    .entry(root_key.to_string())
                    .or_insert_with(|| {
                        ConcreteRootHealthEntry::from_task_signature(signature, config, active)
                    });
                detail.refresh_identity_from_task_signature(signature, config, active);
                detail.candidate_revision = Some(revision);
                detail.candidate_stream_generation = Some(stream_generation);
                detail.candidate_status = Some(status.clone());
                if status == "running" {
                    detail.last_error = None;
                }
            }
        }
    }

    fn mark_root_task_draining(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        revision: u64,
        stream_generation: u64,
        signature: &RootTaskSignature,
        config: &SourceConfig,
        active: bool,
        status: impl Into<String>,
    ) {
        let status = status.into();
        let mut health = lock_or_recover(fanout_health, "source.object_health.draining");
        let had_detail = health.object_ref_detail.contains_key(root_key);
        let current_matches = health
            .object_ref_detail
            .get(root_key)
            .is_some_and(|detail| detail.current_revision == Some(revision));
        if current_matches || !had_detail {
            health
                .object_ref
                .insert(root_key.to_string(), status.clone());
        }
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_insert_with(|| {
                ConcreteRootHealthEntry::from_task_signature(signature, config, active)
            });
        detail.refresh_identity_from_task_signature(signature, config, active);
        detail.draining_revision = Some(revision);
        detail.draining_stream_generation = Some(stream_generation);
        detail.draining_status = Some(status.clone());
        if current_matches || !had_detail {
            detail.status = status;
        }
    }

    fn clear_root_task_candidate_health(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
    ) {
        let mut health = lock_or_recover(fanout_health, "source.object_health.candidate_clear");
        if let Some(detail) = health.object_ref_detail.get_mut(root_key) {
            detail.candidate_revision = None;
            detail.candidate_stream_generation = None;
            detail.candidate_status = None;
        }
    }

    fn fail_root_task_candidate_health(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        revision: u64,
        stream_generation: u64,
        signature: &RootTaskSignature,
        config: &SourceConfig,
        active: bool,
        status: impl Into<String>,
    ) {
        let status = status.into();
        let mut health = lock_or_recover(fanout_health, "source.object_health.candidate_fail");
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_insert_with(|| {
                ConcreteRootHealthEntry::from_task_signature(signature, config, active)
            });
        detail.refresh_identity_from_task_signature(signature, config, active);
        detail.candidate_revision = Some(revision);
        detail.candidate_stream_generation = Some(stream_generation);
        detail.candidate_status = Some(status.clone());
        if let Some(reason) = status.split_once(": ").map(|(_, reason)| reason) {
            detail.last_error = Some(reason.to_string());
        }
    }

    fn promote_root_task_candidate_health(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        revision: u64,
        stream_generation: u64,
        signature: &RootTaskSignature,
        config: &SourceConfig,
        active: bool,
    ) {
        let mut health = lock_or_recover(fanout_health, "source.object_health.promote");
        let promoted_status = health
            .object_ref_detail
            .get(root_key)
            .and_then(|detail| detail.candidate_status.clone())
            .clone()
            .unwrap_or_else(|| "running".to_string());
        health
            .object_ref
            .insert(root_key.to_string(), promoted_status.clone());
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_insert_with(|| {
                ConcreteRootHealthEntry::from_task_signature(signature, config, active)
            });
        detail.refresh_identity_from_task_signature(signature, config, active);
        detail.current_revision = Some(revision);
        detail.current_stream_generation = Some(stream_generation);
        detail.candidate_revision = None;
        detail.candidate_stream_generation = None;
        detail.candidate_status = None;
        detail.status = promoted_status;
        detail.last_error = None;
    }

    fn finish_root_task_draining(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        revision: u64,
        stream_generation: u64,
        retired: bool,
    ) {
        let mut health = lock_or_recover(fanout_health, "source.object_health.draining_finish");
        if let Some(detail) = health.object_ref_detail.get_mut(root_key) {
            detail.draining_revision = Some(revision);
            detail.draining_stream_generation = Some(stream_generation);
            detail.draining_status = Some(if retired {
                "retired".to_string()
            } else {
                "stopped".to_string()
            });
        }
    }

    fn remove_object_health(fanout_health: &Arc<Mutex<FanoutHealthState>>, root_key: &str) {
        let mut health = lock_or_recover(fanout_health, "source.object_health");
        health.object_ref.remove(root_key);
        health.object_ref_detail.remove(root_key);
    }

    fn reset_current_stream_path_frontier_stats(&self, generation: u64) {
        *lock_or_recover(
            &self.enqueued_path_origin_counts,
            "source.current_stream_path_frontier.reset",
        ) = CurrentStreamPathFrontierStats {
            generation: Some(generation),
            enqueued_path_origin_counts: BTreeMap::new(),
        };
        lock_or_recover(
            &self.yielded_path_origin_counts,
            "source.current_stream_path_frontier.reset.yielded",
        )
        .clear();
    }

    fn record_current_stream_enqueued_path_counts(
        frontier: &Arc<Mutex<CurrentStreamPathFrontierStats>>,
        generation: u64,
        path_origin_counts: &BTreeMap<String, u64>,
    ) {
        if path_origin_counts.is_empty() {
            return;
        }
        let mut stats = lock_or_recover(frontier, "source.current_stream_path_frontier.enqueue");
        if stats.generation != Some(generation) {
            return;
        }
        for (origin, count) in path_origin_counts {
            *stats
                .enqueued_path_origin_counts
                .entry(origin.clone())
                .or_default() += *count;
        }
    }

    fn record_current_stream_yielded_path_counts(
        yielded_path_origin_counts: &Arc<Mutex<BTreeMap<String, u64>>>,
        events: &[Event],
    ) {
        if let Some(target) = debug_stream_path_capture_target().as_deref() {
            let matching = count_events_under_query_path(events, target);
            let path_origin_counts = summarize_event_path_origins(events, target);
            if !path_origin_counts.is_empty() {
                let mut yielded = lock_or_recover(
                    yielded_path_origin_counts,
                    "source.pub.yielded_path_origin_counts",
                );
                for (origin, count) in &path_origin_counts {
                    *yielded.entry(origin.clone()).or_default() += *count;
                }
            }
            if matching > 0 {
                let path_origins = path_origin_counts
                    .into_iter()
                    .map(|(origin, count)| format!("{origin}={count}"))
                    .collect::<Vec<_>>();
                eprintln!(
                    "fs_meta_source: pub_stream_path_capture target={} matching_events={} batch_events={} origins={:?}",
                    String::from_utf8_lossy(target),
                    matching,
                    events.len(),
                    path_origins
                );
            }
        }
    }

    fn record_current_stream_downstream_accepted_batch(
        state_cell: &SourceStateCell,
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        roots_handle: &Arc<Mutex<Vec<RootRuntime>>>,
        events: &[Event],
    ) {
        if events.is_empty() {
            return;
        }

        let roots = lock_or_recover(roots_handle, "source.pub.yielded.roots").clone();
        let mut roots_by_object_ref = BTreeMap::<String, Vec<(String, String)>>::new();
        for root in roots.iter().filter(|root| root.active) {
            roots_by_object_ref
                .entry(root.object_ref.clone())
                .or_default()
                .push((root.logical_root_id.clone(), Self::root_runtime_key(root)));
        }

        let capture_target = debug_stream_path_capture_target();
        let mut counts_by_origin = BTreeMap::<String, (u64, u64, Option<u64>)>::new();
        let mut audit_epoch_end_origins = BTreeSet::<String>::new();
        for event in events {
            let origin = event.metadata().origin_id.0.clone();
            let entry = counts_by_origin.entry(origin).or_default();
            entry.0 = entry.0.saturating_add(1);
            if rmp_serde::from_slice::<ControlEvent>(event.payload_bytes())
                .ok()
                .is_some_and(|control| {
                    matches!(
                        control,
                        ControlEvent::EpochEnd {
                            epoch_type: EpochType::Audit,
                            ..
                        }
                    )
                })
            {
                audit_epoch_end_origins.insert(event.metadata().origin_id.0.clone());
            }
            if let Some(target) = capture_target.as_deref()
                && rmp_serde::from_slice::<FileMetaRecord>(event.payload_bytes())
                    .ok()
                    .is_some_and(|record| is_under_query_path(&record.path, target))
            {
                entry.1 = entry.1.saturating_add(1);
            }
            entry.2 = Some(
                entry
                    .2
                    .map(|current| current.max(event.metadata().timestamp_us))
                    .unwrap_or(event.metadata().timestamp_us),
            );
        }

        for (origin, (event_count, path_event_count, last_forwarded_at_us)) in counts_by_origin {
            let Some(root_rows) = roots_by_object_ref.get(&origin) else {
                continue;
            };
            let last_forwarded_origins = vec![format!("{origin}={event_count}")];
            for (logical_root_id, root_key) in root_rows {
                if audit_epoch_end_origins.contains(&origin) {
                    state_cell.mark_group_published(logical_root_id);
                }
                Self::mark_root_forwarded_batch(
                    fanout_health,
                    root_key,
                    event_count,
                    path_event_count,
                    last_forwarded_at_us,
                    last_forwarded_origins.clone(),
                );
            }
        }
    }

    pub(crate) fn record_batch_downstream_accepted(&self, events: &[Event]) {
        let fanout_health = self.state_cell.fanout_health_handle();
        let roots_handle = self.state_cell.roots_handle();
        Self::record_current_stream_downstream_accepted_batch(
            &self.state_cell,
            &fanout_health,
            &roots_handle,
            events,
        );
    }

    fn set_object_last_error(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        error: impl Into<String>,
    ) {
        lock_or_recover(fanout_health, "source.object_health.last_error")
            .object_ref_detail
            .entry(root_key.to_string())
            .or_default()
            .last_error = Some(error.into());
    }

    fn mark_root_rescan_requested(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        reason: &str,
    ) {
        let mut health = lock_or_recover(fanout_health, "source.object_health.rescan_requested");
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_default();
        detail.rescan_pending = true;
        detail.last_rescan_requested_at_us = Some(now_us());
        detail.last_rescan_reason = Some(reason.to_string());
        if Self::audit_reason_invalidates_previous_completion(reason) {
            detail.last_audit_started_at_us = None;
            detail.last_audit_completed_at_us = None;
            detail.last_audit_duration_ms = None;
        }
    }

    fn mark_root_rescan_requested_if_not_pending(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        reason: &str,
    ) {
        let mut health = lock_or_recover(fanout_health, "source.object_health.rescan_requested");
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_default();
        let audit_inflight = Self::root_audit_inflight(detail);
        if detail.rescan_pending || audit_inflight {
            return;
        }
        detail.rescan_pending = true;
        detail.last_rescan_requested_at_us = Some(now_us());
        detail.last_rescan_reason = Some(reason.to_string());
        if Self::audit_reason_invalidates_previous_completion(reason) {
            detail.last_audit_started_at_us = None;
            detail.last_audit_completed_at_us = None;
            detail.last_audit_duration_ms = None;
        }
    }

    fn audit_reason_invalidates_previous_completion(reason: &str) -> bool {
        reason != "periodic"
    }

    fn root_audit_inflight(detail: &ConcreteRootHealthEntry) -> bool {
        match (
            detail.last_audit_started_at_us,
            detail.last_audit_completed_at_us,
        ) {
            (Some(started_at_us), Some(completed_at_us)) => started_at_us > completed_at_us,
            (Some(_), None) => true,
            _ => false,
        }
    }

    fn mark_root_overflow_observed(fanout_health: &Arc<Mutex<FanoutHealthState>>, root_key: &str) {
        let mut health = lock_or_recover(fanout_health, "source.object_health.overflow");
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_default();
        detail.overflow_count = detail.overflow_count.saturating_add(1);
        detail.overflow_pending = true;
        detail.last_rescan_reason = Some("overflow".to_string());
    }

    fn mark_root_audit_start(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        reason: &str,
        started_at_us: u64,
    ) {
        let mut health = lock_or_recover(fanout_health, "source.object_health.audit_start");
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_default();
        detail.rescan_pending = false;
        detail.last_rescan_reason = Some(reason.to_string());
        detail.last_audit_started_at_us = Some(started_at_us);
        if Self::audit_reason_invalidates_previous_completion(reason) {
            detail.last_audit_completed_at_us = None;
            detail.last_audit_duration_ms = None;
        }
    }

    fn mark_root_audit_completed(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        started_at_us: u64,
        completed_at_us: u64,
    ) {
        let mut health = lock_or_recover(fanout_health, "source.object_health.audit_end");
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_default();
        detail.rescan_pending = false;
        detail.overflow_pending = false;
        detail.last_audit_started_at_us = Some(started_at_us);
        detail.last_audit_completed_at_us = Some(completed_at_us);
        detail.last_audit_duration_ms = Some(completed_at_us.saturating_sub(started_at_us) / 1_000);
    }

    fn summarize_emitted_origins(events: &[Event]) -> Vec<String> {
        let mut counts = BTreeMap::<String, usize>::new();
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

    fn current_stream_queue_key(events: &[Event]) -> Option<String> {
        let origins = events
            .iter()
            .map(|event| event.metadata().origin_id.0.clone())
            .collect::<BTreeSet<_>>();
        match origins.len() {
            0 => None,
            1 => origins.into_iter().next(),
            _ => Some(origins.into_iter().collect::<Vec<_>>().join("|")),
        }
    }

    fn enqueue_current_stream_batch(
        pending_by_queue: &mut BTreeMap<String, VecDeque<Vec<Event>>>,
        ready_queues: &mut VecDeque<String>,
        events: Vec<Event>,
    ) {
        if events.is_empty() {
            return;
        }
        let queue_key = Self::current_stream_queue_key(&events)
            .unwrap_or_else(|| "__empty_current_stream_batch__".to_string());
        let queue = pending_by_queue.entry(queue_key.clone()).or_default();
        if queue.is_empty() {
            ready_queues.push_back(queue_key);
        }
        queue.push_back(events);
    }

    fn dequeue_current_stream_batch(
        pending_by_queue: &mut BTreeMap<String, VecDeque<Vec<Event>>>,
        ready_queues: &mut VecDeque<String>,
    ) -> Option<Vec<Event>> {
        while let Some(queue_key) = ready_queues.pop_front() {
            let mut remove_queue = false;
            let batch = {
                let Some(queue) = pending_by_queue.get_mut(&queue_key) else {
                    continue;
                };
                let batch = queue.pop_front();
                if queue.is_empty() {
                    remove_queue = true;
                } else {
                    ready_queues.push_back(queue_key.clone());
                }
                batch
            };
            if remove_queue {
                pending_by_queue.remove(&queue_key);
            }
            if batch.is_some() {
                return batch;
            }
        }
        None
    }

    fn current_stream_pending_batch_count(
        pending_by_queue: &BTreeMap<String, VecDeque<Vec<Event>>>,
    ) -> usize {
        pending_by_queue.values().map(VecDeque::len).sum()
    }

    fn source_stream_lane_for_batch(events: &[Event]) -> SourceStreamLane {
        for event in events {
            if let Ok(control) = rmp_serde::from_slice::<crate::ControlEvent>(event.payload_bytes())
            {
                if matches!(control, crate::ControlEvent::WatchOverflow) {
                    return SourceStreamLane::Realtime;
                }
                continue;
            }
            if let Ok(record) = rmp_serde::from_slice::<FileMetaRecord>(event.payload_bytes())
                && matches!(record.source, crate::SyncTrack::Realtime)
            {
                return SourceStreamLane::Realtime;
            }
        }
        SourceStreamLane::Scan
    }

    fn mark_root_emitted_batch(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        batch: &[Event],
    ) {
        let mut control = 0u64;
        let mut data = 0u64;
        let mut last_emitted_at_us = None::<u64>;
        for event in batch {
            last_emitted_at_us = Some(
                last_emitted_at_us
                    .map(|current| current.max(event.metadata().timestamp_us))
                    .unwrap_or(event.metadata().timestamp_us),
            );
            if rmp_serde::from_slice::<crate::ControlEvent>(event.payload_bytes()).is_ok() {
                control += 1;
            } else {
                data += 1;
            }
        }
        let mut health = lock_or_recover(fanout_health, "source.object_health.emitted_batch");
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_default();
        detail.emitted_batch_count = detail.emitted_batch_count.saturating_add(1);
        detail.emitted_event_count = detail
            .emitted_event_count
            .saturating_add(batch.len() as u64);
        detail.emitted_control_event_count =
            detail.emitted_control_event_count.saturating_add(control);
        detail.emitted_data_event_count = detail.emitted_data_event_count.saturating_add(data);
        let capture_target = debug_stream_path_capture_target();
        if let Some(target) = capture_target.as_deref() {
            detail.emitted_path_event_count = detail
                .emitted_path_event_count
                .saturating_add(count_events_under_query_path(batch, target));
        }
        detail.last_emitted_at_us = last_emitted_at_us;
        detail.last_emitted_origins = Self::summarize_emitted_origins(batch);
        if let Some(target) = capture_target {
            let matching = count_events_under_query_path(batch, &target);
            eprintln!(
                "fs_meta_source: emitted_path_capture root_key={} target={} matching_events={} batch_events={} origins={:?}",
                root_key,
                String::from_utf8_lossy(&target),
                matching,
                batch.len(),
                detail.last_emitted_origins,
            );
        }
    }

    fn mark_root_forwarded_batch(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        event_count: u64,
        path_event_count: u64,
        last_forwarded_at_us: Option<u64>,
        last_forwarded_origins: Vec<String>,
    ) {
        let mut health = lock_or_recover(fanout_health, "source.object_health.forwarded_batch");
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_default();
        detail.forwarded_batch_count = detail.forwarded_batch_count.saturating_add(1);
        detail.forwarded_event_count = detail.forwarded_event_count.saturating_add(event_count);
        detail.forwarded_path_event_count = detail
            .forwarded_path_event_count
            .saturating_add(path_event_count);
        detail.last_forwarded_at_us = last_forwarded_at_us;
        detail.last_forwarded_origins = last_forwarded_origins;
    }

    fn request_rescan_on_primary_roots(
        roots: &[RootRuntime],
        root_tasks: Option<&Arc<Mutex<HashMap<String, RootTaskEntry>>>>,
        fanout_health: Option<&Arc<Mutex<FanoutHealthState>>>,
        manual_rescan_intents: Option<&Arc<Mutex<HashMap<String, ManualRescanIntent>>>>,
        manual_rescan_intent_wake: Option<&watch::Sender<u64>>,
        reason: &str,
    ) {
        let signal_root_keys = Self::record_rescan_intent_on_primary_roots(
            roots,
            fanout_health,
            manual_rescan_intents,
            reason,
        );
        if reason == "manual" && !signal_root_keys.is_empty() {
            if let Some(wake) = manual_rescan_intent_wake {
                Self::wake_manual_rescan_intents(wake);
            }
        }
        Self::signal_rescan_on_root_keys(roots, root_tasks, &signal_root_keys);
    }

    fn wake_manual_rescan_intents(wake: &watch::Sender<u64>) {
        let next = wake.borrow().saturating_add(1);
        let _ = wake.send(next);
    }

    fn targeted_rescan_scan_root_keys(roots: &[RootRuntime]) -> Vec<String> {
        roots
            .iter()
            .filter(|root| root.active && root.is_group_primary && root.spec.scan)
            .map(Self::root_runtime_key)
            .collect::<Vec<_>>()
    }

    async fn wait_for_targeted_rescan_scan_roots(
        roots: Arc<Mutex<Vec<RootRuntime>>>,
        timeout: Option<Duration>,
    ) -> Vec<RootRuntime> {
        let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);
        loop {
            let snapshot =
                lock_or_recover(&roots, "source.rescan.scoped_endpoint.roots.expected").clone();
            if !Self::targeted_rescan_scan_root_keys(&snapshot).is_empty() {
                return snapshot;
            }
            let Some(deadline) = deadline else {
                return snapshot;
            };
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return snapshot;
            }
            tokio::time::sleep(remaining.min(Duration::from_millis(25))).await;
        }
    }

    fn scoped_rescan_acceptance_timeout(requests: &[Event]) -> Option<Duration> {
        requests.first().and_then(|request| {
            manual_rescan_scoped_target_acceptance_timeout_from_payload(request.payload_bytes())
        })
    }

    fn record_rescan_intent_on_primary_roots(
        roots: &[RootRuntime],
        fanout_health: Option<&Arc<Mutex<FanoutHealthState>>>,
        manual_rescan_intents: Option<&Arc<Mutex<HashMap<String, ManualRescanIntent>>>>,
        reason: &str,
    ) -> std::collections::BTreeSet<String> {
        let mut signal_root_keys = std::collections::BTreeSet::new();
        for root in roots {
            if !root.is_group_primary {
                continue;
            }
            let root_key = Self::root_runtime_key(root);
            if reason == "manual"
                && let Some(intents) = manual_rescan_intents
            {
                let mut intents = lock_or_recover(intents, "source.manual_rescan_intents.queue");
                let entry = intents.entry(root_key.clone()).or_default();
                entry.requested = entry.requested.saturating_add(1);
                signal_root_keys.insert(Self::root_runtime_key(root));
                if let Some(health) = fanout_health {
                    Self::mark_root_rescan_requested_if_not_pending(health, &root_key, reason);
                }
            } else {
                if let Some(health) = fanout_health {
                    Self::mark_root_rescan_requested(health, &root_key, reason);
                }
                signal_root_keys.insert(Self::root_runtime_key(root));
            }
        }
        signal_root_keys
    }

    fn record_rescan_intent_on_targeted_scan_roots(
        roots: &[RootRuntime],
        fanout_health: Option<&Arc<Mutex<FanoutHealthState>>>,
        manual_rescan_intents: Option<&Arc<Mutex<HashMap<String, ManualRescanIntent>>>>,
        reason: &str,
    ) -> std::collections::BTreeSet<String> {
        let mut signal_root_keys = std::collections::BTreeSet::new();
        for root in roots {
            if !root.active || !root.is_group_primary || !root.spec.scan {
                continue;
            }
            let root_key = Self::root_runtime_key(root);
            if reason == "manual"
                && let Some(intents) = manual_rescan_intents
            {
                let mut intents = lock_or_recover(intents, "source.manual_rescan_intents.queue");
                let entry = intents.entry(root_key.clone()).or_default();
                entry.requested = entry.requested.saturating_add(1);
                signal_root_keys.insert(Self::root_runtime_key(root));
                if let Some(health) = fanout_health {
                    Self::mark_root_rescan_requested_if_not_pending(health, &root_key, reason);
                }
            } else {
                if let Some(health) = fanout_health {
                    Self::mark_root_rescan_requested(health, &root_key, reason);
                }
                signal_root_keys.insert(Self::root_runtime_key(root));
            }
        }
        signal_root_keys
    }

    fn request_rescan_on_targeted_scan_roots(
        roots: &[RootRuntime],
        root_tasks: Option<&Arc<Mutex<HashMap<String, RootTaskEntry>>>>,
        fanout_health: Option<&Arc<Mutex<FanoutHealthState>>>,
        manual_rescan_intents: Option<&Arc<Mutex<HashMap<String, ManualRescanIntent>>>>,
        manual_rescan_intent_wake: Option<&watch::Sender<u64>>,
        reason: &str,
    ) {
        let signal_root_keys = Self::record_rescan_intent_on_targeted_scan_roots(
            roots,
            fanout_health,
            manual_rescan_intents,
            reason,
        );
        if reason == "manual" && !signal_root_keys.is_empty() {
            if let Some(wake) = manual_rescan_intent_wake {
                Self::wake_manual_rescan_intents(wake);
            }
        }
        Self::signal_rescan_on_primary_root_keys(roots, root_tasks, &signal_root_keys);
    }

    fn signal_rescan_on_root_keys(
        roots: &[RootRuntime],
        root_tasks: Option<&Arc<Mutex<HashMap<String, RootTaskEntry>>>>,
        root_keys: &std::collections::BTreeSet<String>,
    ) {
        if root_keys.is_empty() {
            return;
        }
        let mut signaled = std::collections::BTreeSet::new();
        if let Some(root_tasks) = root_tasks {
            let tasks = lock_or_recover(root_tasks, "source.rescan.signal.root_tasks");
            for root_key in root_keys {
                let Some(entry) = tasks.get(root_key) else {
                    continue;
                };
                if entry
                    .active
                    .handle
                    .rescan_tx
                    .send(RescanReason::Manual)
                    .is_ok()
                {
                    signaled.insert(root_key.clone());
                }
            }
        }
        for root in roots {
            let root_key = Self::root_runtime_key(root);
            if root_keys.contains(&root_key) && !signaled.contains(&root_key) {
                let _ = root.rescan_tx.send(RescanReason::Manual);
            }
        }
    }

    fn signal_rescan_on_primary_root_keys(
        roots: &[RootRuntime],
        root_tasks: Option<&Arc<Mutex<HashMap<String, RootTaskEntry>>>>,
        root_keys: &std::collections::BTreeSet<String>,
    ) {
        let primary_root_keys = root_keys
            .iter()
            .filter(|root_key| {
                roots
                    .iter()
                    .any(|root| root.is_group_primary && Self::root_runtime_key(root) == **root_key)
            })
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        Self::signal_rescan_on_root_keys(roots, root_tasks, &primary_root_keys);
    }

    fn manual_rescan_signal_intent_for_root(
        manual_rescan_intents: &Arc<Mutex<HashMap<String, ManualRescanIntent>>>,
        root_key: &str,
    ) -> ManualRescanSignalIntent {
        let intents = lock_or_recover(
            manual_rescan_intents,
            "source.root.manual_rescan_intents.signal_intent",
        );
        match intents.get(root_key) {
            Some(intent) if intent.requested > intent.completed => {
                ManualRescanSignalIntent::Pending(intent.requested)
            }
            Some(_) => ManualRescanSignalIntent::AlreadyCompleted,
            None => ManualRescanSignalIntent::Direct,
        }
    }

    fn pending_manual_rescan_target_for_root(
        manual_rescan_intents: &Arc<Mutex<HashMap<String, ManualRescanIntent>>>,
        root_key: &str,
    ) -> Option<u64> {
        let intents = lock_or_recover(
            manual_rescan_intents,
            "source.root.manual_rescan_intents.pending_target",
        );
        intents
            .get(root_key)
            .and_then(|intent| (intent.requested > intent.completed).then_some(intent.requested))
    }

    fn mark_manual_rescan_intent_completed_up_to(
        manual_rescan_intents: &Arc<Mutex<HashMap<String, ManualRescanIntent>>>,
        root_key: &str,
        completed_target: u64,
    ) {
        let mut intents = lock_or_recover(
            manual_rescan_intents,
            "source.root.manual_rescan_intents.mark_completed_up_to",
        );
        let entry = intents.entry(root_key.to_string()).or_default();
        entry.completed = entry.completed.max(completed_target).min(entry.requested);
    }

    /// Create a new source app with the given configuration.
    #[allow(dead_code)]
    pub fn new(config: SourceConfig, node_id: NodeId) -> Result<Self> {
        Self::with_boundaries(config, node_id, None)
    }

    /// Create a new source app, optionally attaching a boundary endpoint for source force-find queries.
    pub fn with_boundaries(
        config: SourceConfig,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> Result<Self> {
        Self::with_boundaries_and_state(config, node_id, boundary, in_memory_state_boundary())
    }

    /// Create a new source app, optionally attaching channel/state boundaries.
    pub fn with_boundaries_and_state(
        config: SourceConfig,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        state_boundary: Arc<dyn StateBoundary>,
    ) -> Result<Self> {
        Self::with_boundaries_and_state_internal(config, node_id, boundary, state_boundary, false)
    }

    pub(crate) fn with_boundaries_and_state_internal(
        config: SourceConfig,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        state_boundary: Arc<dyn StateBoundary>,
        defer_authority_read: bool,
    ) -> Result<Self> {
        Self::with_boundaries_and_state_inner(
            config,
            node_id,
            boundary,
            state_boundary,
            defer_authority_read,
            SourceEndpointRuntime::Local,
        )
    }

    pub(crate) fn with_worker_runtime_boundaries_and_state_internal(
        config: SourceConfig,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        state_boundary: Arc<dyn StateBoundary>,
        defer_authority_read: bool,
    ) -> Result<Self> {
        Self::with_boundaries_and_state_inner(
            config,
            node_id,
            boundary,
            state_boundary,
            defer_authority_read,
            SourceEndpointRuntime::Worker,
        )
    }

    fn with_boundaries_and_state_inner(
        config: SourceConfig,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        state_boundary: Arc<dyn StateBoundary>,
        _defer_authority_read: bool,
        endpoint_runtime: SourceEndpointRuntime,
    ) -> Result<Self> {
        let configured_host_object_grants = config.host_object_grants.clone();
        let configured_root_specs = config.effective_roots().map_err(CnxError::InvalidInput)?;
        let authority =
            AuthorityJournal::from_state_boundary(SOURCE_RUNTIME_UNIT_ID, state_boundary.clone())
                .map_err(|err| {
                CnxError::InvalidInput(format!("source statecell authority init failed: {err}"))
            })?;
        let logical_roots_cell = LogicalRootsCell::from_state_boundary(
            SOURCE_RUNTIME_UNIT_ID,
            configured_root_specs,
            state_boundary.clone(),
        )
        .map_err(|err| {
            CnxError::InvalidInput(format!("source logical-roots state init failed: {err}"))
        })?;
        let root_specs = logical_roots_cell.snapshot();
        let host_object_grants_cell = HostObjectGrantsCell::from_state_boundary(
            SOURCE_RUNTIME_UNIT_ID,
            configured_host_object_grants,
            state_boundary.clone(),
        )
        .map_err(|err| {
            CnxError::InvalidInput(format!(
                "source host-object-grants state init failed: {err}"
            ))
        })?;
        let (initial_host_object_grants_version, initial_host_object_grants) =
            host_object_grants_cell.snapshot();
        let drift_estimator = Arc::new(Mutex::new(DriftEstimator::new(
            config.drift_window_size,
            config.drift_graduation_threshold,
            config.drift_max_jump_us,
        )));
        let logical_clock = Arc::new(LogicalClock::new());
        let roots: Vec<RootRuntime> = Self::build_root_runtimes(
            &config,
            &node_id,
            boundary.clone(),
            &root_specs,
            &initial_host_object_grants,
            None,
            None,
        );
        let logical_root_fanout =
            Self::compute_logical_root_fanout(&root_specs, &initial_host_object_grants);
        let fanout_health = Arc::new(Mutex::new(FanoutHealthState::default()));
        let manual_rescan_signal = SignalCell::from_state_boundary(
            SOURCE_RUNTIME_UNIT_ID,
            MANUAL_RESCAN_SIGNAL_NAME,
            state_boundary,
        )
        .map_err(|err| {
            CnxError::InvalidInput(format!("source manual-rescan signal init failed: {err}"))
        })?;
        let commit_boundary = CommitBoundary::new(authority);
        Self::set_logical_root_health(
            &fanout_health,
            &root_specs,
            &logical_root_fanout,
            config.lru_capacity,
        );
        Self::sync_object_runtime_health(&fanout_health, &roots, &config);
        let sentinel = Arc::new(Sentinel::new(SentinelConfig::default()));

        let unit_control = Arc::new(if boundary.is_some() {
            RuntimeUnitGate::new_runtime_managed("source-fs-meta", SOURCE_RUNTIME_UNITS)
        } else {
            RuntimeUnitGate::new("source-fs-meta", SOURCE_RUNTIME_UNITS)
        });

        let source = Self {
            config,
            node_id: node_id.clone(),
            boundary: boundary.clone(),
            drift_estimator: drift_estimator.clone(),
            logical_clock: logical_clock.clone(),
            state: Arc::new(Mutex::new(LifecycleState::Scanning)),
            shutdown: CancellationToken::new(),
            state_cell: SourceStateCell::new(
                root_specs,
                logical_roots_cell,
                host_object_grants_cell,
                roots,
                logical_root_fanout,
                initial_host_object_grants,
                fanout_health,
                commit_boundary,
            ),
            manual_rescan_signal,
            manual_rescan_watch_task: Arc::new(Mutex::new(None)),
            manual_rescan_intent_wake: watch::channel(0).0,
            host_object_grants_version: Arc::new(AtomicU64::new(
                initial_host_object_grants_version,
            )),
            unit_control,
            endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
            sentinel,
            force_find_rr: Arc::new(Mutex::new(HashMap::new())),
            force_find_inflight: Arc::new(Mutex::new(HashSet::new())),
            force_find_last_runner: Arc::new(Mutex::new(BTreeMap::new())),
            last_control_frame_signals: Arc::new(Mutex::new(Vec::new())),
            manual_rescan_control_high_watermark_us: Arc::new(AtomicU64::new(0)),
            manual_rescan_control_seen_request_ids: Arc::new(Mutex::new(VecDeque::new())),
            control_state: Arc::new(tokio::sync::Mutex::new(SourceControlState::default())),
            runtime_refresh_dirty: Arc::new(AtomicBool::new(false)),
            runtime_refresh_rescan: Arc::new(AtomicBool::new(false)),
            runtime_refresh_running: Arc::new(AtomicBool::new(false)),
            yielded_path_origin_counts: Arc::new(Mutex::new(BTreeMap::new())),
            enqueued_path_origin_counts: Arc::new(Mutex::new(
                CurrentStreamPathFrontierStats::default(),
            )),
            endpoint_runtime,
        };

        if let Some(sys) = boundary {
            source.start_source_rescan_request_endpoints_on_boundary(sys)?;
        }

        Ok(source)
    }

    fn prune_finished_endpoint_tasks(&self, context: &str) {
        let mut tasks = lock_or_recover(&self.endpoint_tasks, context);
        tasks.retain(|task| {
            if !task.is_finished() {
                return true;
            }
            eprintln!(
                "fs_meta_source: pruning finished endpoint route={} reason={}",
                task.route_key(),
                task.finish_reason()
                    .unwrap_or_else(|| "unclassified_finish".to_string())
            );
            false
        });
    }

    fn endpoint_task_route_present(&self, route_key: &str, context: &str) -> bool {
        lock_or_recover(&self.endpoint_tasks, context)
            .iter()
            .any(|task| {
                task.route_key() == route_key
                    && !task.is_finished()
                    && task.finish_reason().is_none()
                    && !task.is_shutdown_requested()
            })
    }

    fn all_endpoint_task_routes_present_on_boundary(
        &self,
        route_keys: &BTreeSet<String>,
        boundary: &Arc<dyn ChannelIoSubset>,
        context: &str,
    ) -> bool {
        if route_keys.is_empty() {
            return true;
        }
        let tasks = lock_or_recover(&self.endpoint_tasks, context);
        route_keys.iter().all(|route_key| {
            tasks.iter().any(|task| {
                task.route_key() == route_key
                    && task.belongs_to_boundary(boundary)
                    && !task.is_finished()
                    && task.finish_reason().is_none()
                    && !task.is_shutdown_requested()
            })
        })
    }

    fn expected_runtime_endpoint_routes(&self) -> Result<BTreeSet<String>> {
        let routes = source_find_route_bindings_for(&self.node_id.0);
        let mut expected = BTreeSet::new();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            && self.should_start_source_status_endpoint_for_route(&route.0)
        {
            expected.insert(route.0);
        }
        let scoped_status_route = source_status_request_route_for(&self.node_id.0);
        if self.should_start_source_status_endpoint_for_route(&scoped_status_route.0) {
            expected.insert(scoped_status_route.0);
        }
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN) {
            expected.insert(route.0);
        }
        if let Ok(route) =
            routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN_CONTROL)
        {
            expected.insert(route.0);
        }
        if let Ok(route) = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_ROOTS_CONTROL)
        {
            expected.insert(route.0);
        }
        expected.insert(source_roots_control_stream_route_for(&self.node_id.0).0);
        for route_key in self.active_source_rescan_request_routes()? {
            expected.insert(route_key);
        }
        Ok(expected)
    }

    fn runtime_endpoints_current_on_boundary(
        &self,
        boundary: &Arc<dyn ChannelIoSubset>,
    ) -> Result<bool> {
        let expected = self.expected_runtime_endpoint_routes()?;
        Ok(self.all_endpoint_task_routes_present_on_boundary(
            &expected,
            boundary,
            "source.start_runtime_endpoints.fast_path",
        ))
    }

    fn retire_endpoint_tasks_for_different_boundary(
        &self,
        boundary: &Arc<dyn ChannelIoSubset>,
        context: &str,
    ) {
        let mut tasks = lock_or_recover(&self.endpoint_tasks, context);
        tasks.retain(|task| {
            let retire = !task.is_finished()
                && task.finish_reason().is_none()
                && !task.is_shutdown_requested()
                && !task.belongs_to_boundary(boundary);
            if retire {
                eprintln!(
                    "fs_meta_source: retiring endpoint from stale runtime boundary route={}",
                    task.route_key()
                );
                task.request_shutdown_and_close();
            }
            true
        });
    }

    fn source_rescan_request_route_from_control_summary(signal: &str) -> Option<String> {
        let (_, route_suffix) = signal.split_once("route=")?;
        let route = route_suffix
            .chars()
            .take_while(|ch| !ch.is_whitespace())
            .collect::<String>();
        (!route.is_empty()).then_some(route)
    }

    fn is_scoped_source_rescan_request_route(route_key: &str) -> bool {
        let Some(request_route) = route_key.strip_suffix(".req") else {
            return false;
        };
        let Some((stem, version)) = ROUTE_KEY_SOURCE_RESCAN_INTERNAL.rsplit_once(':') else {
            return request_route.starts_with(&format!("{ROUTE_KEY_SOURCE_RESCAN_INTERNAL}."));
        };
        let Some(route_stem) = request_route.strip_suffix(&format!(":{version}")) else {
            return false;
        };
        route_stem.starts_with(&format!("{stem}."))
    }

    fn active_source_rescan_request_routes(&self) -> Result<BTreeSet<String>> {
        let mut routes = BTreeSet::new();
        if !self.endpoint_runtime.owns_external_scoped_rescan_routes() {
            return Ok(routes);
        }
        let local_route = source_rescan_request_route_for(&self.node_id.0).0;
        routes.insert(local_route.clone());
        let signals = lock_or_recover(
            &self.last_control_frame_signals,
            "source.active_source_rescan_request_routes.last_control_frame_signals",
        );
        for signal in signals.iter() {
            let Some(route_key) = Self::source_rescan_request_route_from_control_summary(signal)
            else {
                continue;
            };
            if !signal.contains("unit=runtime.exec.source")
                || !Self::is_scoped_source_rescan_request_route(&route_key)
            {
                continue;
            }
            if route_key != local_route {
                continue;
            }
            if signal.contains("activate ") {
                routes.insert(route_key);
            } else if signal.contains("deactivate ") {
                routes.remove(&route_key);
            }
        }
        Ok(routes)
    }

    fn source_control_signal_generation(signal: &str) -> Option<u64> {
        let (_, suffix) = signal.split_once("generation=")?;
        let digits = suffix
            .chars()
            .take_while(|ch| ch.is_ascii_digit())
            .collect::<String>();
        (!digits.is_empty())
            .then(|| digits.parse::<u64>().ok())
            .flatten()
    }

    fn source_rescan_route_activation_generation_from_signals(
        signals: &[String],
        route: &str,
    ) -> Option<u64> {
        signals
            .iter()
            .filter(|signal| {
                signal.contains("activate ")
                    && signal.contains("unit=runtime.exec.source")
                    && signal.contains(&format!("route={route}"))
            })
            .filter_map(|signal| Self::source_control_signal_generation(signal))
            .max()
    }

    fn source_rescan_route_ready_generation_from_signals(
        signals: &[String],
        route: &str,
    ) -> Option<u64> {
        signals
            .iter()
            .filter(|signal| {
                signal.contains("ready ")
                    && signal.contains("unit=runtime.exec.source")
                    && signal.contains(&format!("route={route}"))
            })
            .filter_map(|signal| Self::source_control_signal_generation(signal))
            .max()
    }

    fn source_rescan_route_ready_matches_current_activation(&self, route: &str) -> bool {
        let signals = lock_or_recover(
            &self.last_control_frame_signals,
            "source.rescan.receivable.route_current_signals",
        );
        match Self::source_rescan_route_activation_generation_from_signals(&signals, route) {
            Some(activation_generation) => {
                Self::source_rescan_route_ready_generation_from_signals(&signals, route)
                    == Some(activation_generation)
            }
            None => true,
        }
    }

    fn retire_unproven_source_rescan_endpoint_tasks_for_manual_status(
        &self,
        boundary: &Arc<dyn ChannelIoSubset>,
    ) {
        let route = source_rescan_request_route_for(&self.node_id.0).0;
        let ready_matches_current_activation =
            self.source_rescan_route_ready_matches_current_activation(&route);
        let mut tasks = lock_or_recover(
            &self.endpoint_tasks,
            "source.rearm_source_rescan_manual_status.retire_unproven",
        );
        let has_receive_armed_endpoint = tasks.iter().any(|task| {
            task.route_key() == route
                && !task.is_finished()
                && task.finish_reason().is_none()
                && !task.is_shutdown_requested()
                && task.belongs_to_boundary(boundary)
                && task.is_receive_armed()
        });
        if ready_matches_current_activation && has_receive_armed_endpoint {
            return;
        }
        tasks.retain(|task| {
            let retire = task.route_key() == route
                && !task.is_finished()
                && task.finish_reason().is_none()
                && !task.is_shutdown_requested()
                && (!task.belongs_to_boundary(boundary) || !task.is_receive_armed());
            if retire {
                eprintln!(
                    "fs_meta_source: retiring unproven scoped rescan endpoint route={} before receive-armed ready refresh",
                    task.route_key()
                );
                if task.belongs_to_boundary(boundary) {
                    task.request_shutdown_and_close_on(boundary);
                } else {
                    task.request_shutdown_and_close();
                }
            }
            true
        });
    }

    fn local_targeted_rescan_scan_group_ids(&self) -> BTreeSet<String> {
        lock_or_recover(&self.state_cell.roots, "source.rescan.receivable.roots")
            .iter()
            .filter(|root| root.active && root.is_group_primary && root.spec.scan)
            .map(|root| root.logical_root_id.clone())
            .collect()
    }

    fn manual_rescan_ready_scopes(groups: &BTreeSet<String>) -> Option<String> {
        if groups.is_empty() {
            return None;
        }
        Some(format!(
            " scopes=[{}]",
            groups
                .iter()
                .map(|group| format!("\"{group}=>{group}\""))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }

    fn record_source_rescan_route_receivable(&self) {
        if !self.endpoint_runtime.owns_external_scoped_rescan_routes() {
            return;
        }
        let Some(scopes) =
            Self::manual_rescan_ready_scopes(&self.local_targeted_rescan_scan_group_ids())
        else {
            return;
        };
        let route = source_rescan_request_route_for(&self.node_id.0).0;
        let mut signals = lock_or_recover(
            &self.last_control_frame_signals,
            "source.rescan.receivable.last_control_frame_signals",
        );
        let generation =
            Self::source_rescan_route_activation_generation_from_signals(&signals, &route);
        let ready_signal = match generation {
            Some(generation) => {
                format!(
                    "ready unit=runtime.exec.source route={route} generation={generation}{scopes}"
                )
            }
            None => format!("ready unit=runtime.exec.source route={route}{scopes}"),
        };
        signals.retain(|signal| {
            !(signal.contains("ready ")
                && signal.contains("unit=runtime.exec.source")
                && signal.contains(&format!("route={route}")))
        });
        if !signals.iter().any(|signal| signal == &ready_signal) {
            signals.push(ready_signal);
        }
    }

    fn start_source_rescan_request_endpoints_on_boundary(
        &self,
        boundary: Arc<dyn ChannelIoSubset>,
    ) -> Result<()> {
        let rescan_roots = self.state_cell.roots_handle();
        let rescan_root_tasks = self.state_cell.root_tasks.clone();
        let rescan_fanout_health = self.state_cell.fanout_health_handle();
        let rescan_manual_intents = self.state_cell.manual_rescan_intents_handle();
        let rescan_manual_wake = self.manual_rescan_intent_wake.clone();
        let node_id_rescan = self.node_id.clone();
        let routes = source_find_route_bindings_for(&self.node_id.0);

        match routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN) {
            Ok(route)
                if !self.endpoint_task_route_present(
                    &route.0,
                    "source.start_rescan_endpoints.route_present.rescan",
                ) =>
            {
                log::info!(
                    "bound route listening on {}.{} for deferred source {}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_RESCAN,
                    node_id_rescan.0
                );
                let rescan_roots_for_handler = rescan_roots.clone();
                let rescan_root_tasks_for_handler = rescan_root_tasks.clone();
                let rescan_fanout_health_for_handler = rescan_fanout_health.clone();
                let rescan_manual_intents_for_handler = rescan_manual_intents.clone();
                let rescan_manual_wake_for_handler = rescan_manual_wake.clone();
                let endpoint_task = ManagedEndpointTask::spawn_with_unit(
                    boundary.clone(),
                    route,
                    format!(
                        "source:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN
                    ),
                    SOURCE_RUNTIME_UNIT_ID,
                    self.shutdown.clone(),
                    move |requests| {
                        let node_id_rescan = node_id_rescan.clone();
                        let rescan_roots = rescan_roots_for_handler.clone();
                        let rescan_root_tasks = rescan_root_tasks_for_handler.clone();
                        let rescan_fanout_health = rescan_fanout_health_for_handler.clone();
                        let rescan_manual_intents = rescan_manual_intents_for_handler.clone();
                        let rescan_manual_wake = rescan_manual_wake_for_handler.clone();
                        async move {
                            eprintln!(
                                "fs_meta_source: source.rescan endpoint start node={} requests={}",
                                node_id_rescan.0,
                                requests.len()
                            );
                            let expected = lock_or_recover(
                                &rescan_roots,
                                "source.rescan.endpoint.roots.expected",
                            )
                            .iter()
                            .filter(|root| root.is_group_primary && root.spec.scan)
                            .map(FSMetaSource::root_runtime_key)
                            .collect::<Vec<_>>();
                            let roots_snapshot = lock_or_recover(
                                &rescan_roots,
                                "source.rescan.endpoint.roots.trigger",
                            )
                            .clone();
                            FSMetaSource::request_rescan_on_primary_roots(
                                &roots_snapshot,
                                Some(&rescan_root_tasks),
                                Some(&rescan_fanout_health),
                                Some(&rescan_manual_intents),
                                Some(&rescan_manual_wake),
                                "manual",
                            );
                            eprintln!(
                                "fs_meta_source: source.rescan endpoint triggered node={} expected_roots={}",
                                node_id_rescan.0,
                                expected.len()
                            );

                            requests
                                .into_iter()
                                .map(|req| {
                                    let mut meta = EventMetadata {
                                        origin_id: node_id_rescan.clone(),
                                        timestamp_us: now_us(),
                                        logical_ts: None,
                                        correlation_id: None,
                                        ingress_auth: None,
                                        trace: None,
                                    };
                                    meta.correlation_id = req.metadata().correlation_id;
                                    Event::new(meta, bytes::Bytes::from_static(b"accepted"))
                                })
                                .collect()
                        }
                    },
                );
                lock_or_recover(
                    &self.endpoint_tasks,
                    "source.start_rescan_endpoints.rescan_tasks",
                )
                .push(endpoint_task);
            }
            Err(err) => {
                log::error!(
                    "failed to resolve deferred source bound route {}.{}: {:?}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_RESCAN,
                    err
                );
            }
            Ok(_) => {}
        }

        let scoped_route = source_rescan_request_route_for(&self.node_id.0);
        if self.endpoint_runtime.owns_external_scoped_rescan_routes()
            && !self.endpoint_task_route_present(
                &scoped_route.0,
                "source.start_rescan_endpoints.route_present.scoped_rescan",
            )
        {
            let node_id_rescan_scoped = self.node_id.clone();
            let rescan_roots_scoped = self.state_cell.roots_handle();
            let rescan_root_tasks_scoped = rescan_root_tasks.clone();
            let rescan_fanout_health_scoped = rescan_fanout_health.clone();
            let rescan_manual_intents_scoped = rescan_manual_intents.clone();
            let rescan_manual_wake_scoped = rescan_manual_wake.clone();
            log::info!(
                "bound route listening on {} for deferred source {}",
                scoped_route.0,
                node_id_rescan_scoped.0
            );
            let endpoint_task = ManagedEndpointTask::spawn_with_unit_wait_receivable(
                boundary,
                scoped_route.clone(),
                format!("source:{}", scoped_route.0),
                SOURCE_RUNTIME_UNIT_ID,
                self.shutdown.clone(),
                move |requests| {
                    let node_id_rescan_scoped = node_id_rescan_scoped.clone();
                    let rescan_roots_scoped = rescan_roots_scoped.clone();
                    let rescan_root_tasks_scoped = rescan_root_tasks_scoped.clone();
                    let rescan_fanout_health_scoped = rescan_fanout_health_scoped.clone();
                    let rescan_manual_intents_scoped = rescan_manual_intents_scoped.clone();
                    let rescan_manual_wake_scoped = rescan_manual_wake_scoped.clone();
                    async move {
                        eprintln!(
                            "fs_meta_source: source.rescan scoped endpoint start node={} requests={}",
                            node_id_rescan_scoped.0,
                            requests.len()
                        );
                        let acceptance_timeout =
                            FSMetaSource::scoped_rescan_acceptance_timeout(&requests);
                        let roots_snapshot = FSMetaSource::wait_for_targeted_rescan_scan_roots(
                            rescan_roots_scoped.clone(),
                            acceptance_timeout,
                        )
                        .await;
                        let expected =
                            FSMetaSource::targeted_rescan_scan_root_keys(&roots_snapshot);
                        let payload = if expected.is_empty() {
                            eprintln!(
                                "fs_meta_source: source.rescan scoped endpoint rejected node={} reason=no-local-scheduled-scan-root",
                                node_id_rescan_scoped.0
                            );
                            bytes::Bytes::from_static(
                                b"scoped source-rescan target has no local scheduled scan root (source-primary required)",
                            )
                        } else {
                            FSMetaSource::request_rescan_on_targeted_scan_roots(
                                &roots_snapshot,
                                Some(&rescan_root_tasks_scoped),
                                Some(&rescan_fanout_health_scoped),
                                Some(&rescan_manual_intents_scoped),
                                Some(&rescan_manual_wake_scoped),
                                "manual",
                            );
                            eprintln!(
                                "fs_meta_source: source.rescan scoped endpoint accepted and enqueued node={} expected_roots={}",
                                node_id_rescan_scoped.0,
                                expected.len()
                            );
                            bytes::Bytes::from_static(b"accepted")
                        };

                        requests
                            .into_iter()
                            .map(|req| {
                                let mut meta = EventMetadata {
                                    origin_id: node_id_rescan_scoped.clone(),
                                    timestamp_us: now_us(),
                                    logical_ts: None,
                                    correlation_id: None,
                                    ingress_auth: None,
                                    trace: None,
                                };
                                meta.correlation_id = req.metadata().correlation_id;
                                Event::new(meta, payload.clone())
                            })
                            .collect()
                    }
                },
            );
            lock_or_recover(
                &self.endpoint_tasks,
                "source.start_rescan_endpoints.scoped_rescan_tasks",
            )
            .push(endpoint_task);
        }

        Ok(())
    }

    pub(crate) async fn rearm_source_rescan_request_endpoints_for_manual_status_on_boundary(
        &self,
        boundary: Arc<dyn ChannelIoSubset>,
    ) -> Result<()> {
        if !self.endpoint_runtime.owns_external_scoped_rescan_routes() {
            return Ok(());
        }
        self.prune_finished_endpoint_tasks("source.rearm_source_rescan_manual_status.prune");
        self.retire_endpoint_tasks_for_different_boundary(
            &boundary,
            "source.rearm_source_rescan_manual_status.retire_stale_boundary",
        );
        self.retire_unproven_source_rescan_endpoint_tasks_for_manual_status(&boundary);
        self.start_source_rescan_request_endpoints_on_boundary(boundary)?;
        if matches!(
            self.targeted_rescan_delivery_acceptance(),
            SourceTargetedRescanDeliveryAcceptance::Accepted
        ) {
            self.record_source_rescan_route_receivable();
        }
        Ok(())
    }

    fn start_source_status_endpoint_on_route(
        &self,
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        route_present_context: &str,
    ) {
        if self.endpoint_task_route_present(&route.0, route_present_context) {
            return;
        }
        let status_source = self.clone();
        let status_node_id = self.node_id.clone();
        let status_boundary = boundary.clone();
        let status_route_key = route.0.clone();
        let endpoint_task = ManagedEndpointTask::spawn_with_unit(
            boundary,
            route,
            format!(
                "source:{}:{}",
                ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS
            ),
            SOURCE_RUNTIME_UNIT_ID,
            self.shutdown.clone(),
            move |requests| {
                let status_source = status_source.clone();
                let status_node_id = status_node_id.clone();
                let status_boundary = status_boundary.clone();
                let status_route_key = status_route_key.clone();
                async move {
                    let manual_rescan_delivery_evidence = requests.iter().any(|req| {
                        crate::query::api::source_status_request_requires_manual_rescan_delivery_evidence(
                            req.payload_bytes(),
                        )
                    });
                    let scan_audit_admission_release = requests.iter().any(|req| {
                        crate::query::api::source_status_request_requires_scan_audit_admission_release(
                            req.payload_bytes(),
                        )
                    });
                    let generic_source_status_route =
                        format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
                    let generic_non_target_status = match status_source
                        .runtime_route_accepts_status_request(
                            &status_route_key,
                            manual_rescan_delivery_evidence,
                        ) {
                        Ok(true) => false,
                        Ok(false) if status_route_key == generic_source_status_route => true,
                        Ok(false) => return Vec::new(),
                        Err(err) => {
                            log::warn!(
                                "source status route scope check failed route={}: {:?}",
                                status_route_key,
                                err
                            );
                            return Vec::new();
                        }
                    };
                    let manual_rescan_targets_local_source = if manual_rescan_delivery_evidence {
                        match status_source
                            .runtime_status_route_targets_local_manual_rescan_delivery(
                                &status_route_key,
                            ) {
                            Ok(targets_local) => targets_local,
                            Err(err) => {
                                log::warn!(
                                    "source status route manual-rescan scope check failed route={}: {:?}",
                                    status_route_key,
                                    err
                                );
                                false
                            }
                        }
                    } else {
                        false
                    };
                    let current_owner_health_evidence = requests.iter().any(|req| {
                        crate::query::api::source_status_request_requires_current_owner_health_evidence(
                            req.payload_bytes(),
                        )
                    });
                    let source_status_live_probe_timeout = requests
                        .iter()
                        .filter_map(|req| {
                            crate::query::api::source_status_request_live_probe_timeout(
                                req.payload_bytes(),
                            )
                        })
                        .min();
                    let mut snapshot = if generic_non_target_status {
                        let (snapshot, used_cached_fallback) =
                            status_source.observability_snapshot_nonblocking_for_status_route();
                        if used_cached_fallback {
                            eprintln!(
                                "fs_meta_source: source-status generic non-target using cached/degraded snapshot node={}",
                                status_node_id.0
                            );
                        }
                        snapshot
                    } else if scan_audit_admission_release {
                        let epoch = if status_source.open_scan_audit_admission_if_closed() {
                            status_source.submit_rescan_request_epoch()
                        } else {
                            0
                        };
                        eprintln!(
                            "fs_meta_source: release_source_scan_audit_after_sink_scope_ready_if_needed source-status ok node={} route={} epoch={}",
                            status_node_id.0, status_route_key, epoch
                        );
                        let (snapshot, used_cached_fallback) =
                            status_source.observability_snapshot_nonblocking_for_status_route();
                        if used_cached_fallback {
                            eprintln!(
                                "fs_meta_source: source-status release-scan-audit using cached/degraded snapshot node={}",
                                status_node_id.0
                            );
                        }
                        snapshot
                    } else if current_owner_health_evidence {
                        let (snapshot, used_cached_fallback) = status_source
                            .current_owner_health_observability_snapshot_for_status_route_with_timeout(
                                source_status_live_probe_timeout,
                            )
                            .await;
                        if used_cached_fallback {
                            eprintln!(
                                "fs_meta_source: source-status current-owner-health using cached/degraded snapshot node={}",
                                status_node_id.0
                            );
                        }
                        snapshot
                    } else if manual_rescan_delivery_evidence && manual_rescan_targets_local_source
                    {
                        if let Err(err) = status_source
                            .rearm_source_rescan_request_endpoints_for_manual_status_on_boundary(
                                status_boundary,
                            )
                            .await
                        {
                            eprintln!(
                                "fs_meta_source: source-status manual-rescan rearm failed node={} err={}",
                                status_node_id.0, err
                            );
                            return Vec::new();
                        }
                        #[cfg(test)]
                        record_source_status_manual_rescan_rearm();
                        let (snapshot, used_cached_fallback) =
                            status_source.observability_snapshot_nonblocking_for_status_route();
                        if used_cached_fallback {
                            eprintln!(
                                "fs_meta_source: source-status manual-rescan using cached delivery snapshot node={}",
                                status_node_id.0
                            );
                        }
                        snapshot
                    } else if manual_rescan_delivery_evidence {
                        let (snapshot, used_cached_fallback) =
                            status_source.observability_snapshot_nonblocking_for_status_route();
                        if used_cached_fallback {
                            eprintln!(
                                "fs_meta_source: source-status manual-rescan non-target using cached/degraded snapshot node={}",
                                status_node_id.0
                            );
                        }
                        snapshot
                    } else {
                        let (snapshot, used_cached_fallback) =
                            status_source.observability_snapshot_nonblocking_for_status_route();
                        if used_cached_fallback {
                            eprintln!(
                                "fs_meta_source: source-status endpoint using cached/degraded snapshot node={}",
                                status_node_id.0
                            );
                        }
                        snapshot
                    };
                    if !generic_non_target_status
                        && manual_rescan_delivery_evidence
                        && manual_rescan_targets_local_source
                        && status_source
                            .endpoint_runtime
                            .owns_external_scoped_rescan_routes()
                        && matches!(
                            status_source.targeted_rescan_delivery_acceptance(),
                            SourceTargetedRescanDeliveryAcceptance::Accepted
                        )
                    {
                        crate::workers::source::annotate_manual_rescan_route_receivable_evidence(
                            &mut snapshot,
                            &status_node_id,
                        );
                    }
                    let mut responses = Vec::with_capacity(requests.len());
                    for req in requests {
                        match rmp_serde::to_vec_named(&snapshot) {
                            Ok(payload) => responses.push(Event::new(
                                EventMetadata {
                                    origin_id: status_node_id.clone(),
                                    timestamp_us: now_us(),
                                    logical_ts: None,
                                    correlation_id: req.metadata().correlation_id,
                                    ingress_auth: None,
                                    trace: None,
                                },
                                Bytes::from(payload),
                            )),
                            Err(err) => eprintln!(
                                "fs_meta_source: source-status encode failed node={} err={}",
                                status_node_id.0, err
                            ),
                        }
                    }
                    responses
                }
            },
        );
        lock_or_recover(
            &self.endpoint_tasks,
            "source.start_runtime_endpoints.status_tasks",
        )
        .push(endpoint_task);
    }

    pub(crate) async fn start_runtime_endpoints_on_boundary(
        &self,
        boundary: Arc<dyn ChannelIoSubset>,
    ) -> Result<()> {
        self.start_manual_rescan_watch().await?;
        if self.runtime_endpoints_current_on_boundary(&boundary)? {
            return Ok(());
        }
        self.prune_finished_endpoint_tasks("source.start_runtime_endpoints.prune");
        self.retire_endpoint_tasks_for_different_boundary(
            &boundary,
            "source.start_runtime_endpoints.retire_stale_boundary",
        );

        let rescan_roots = self.state_cell.roots_handle();
        let rescan_root_tasks = self.state_cell.root_tasks.clone();
        let rescan_fanout_health = self.state_cell.fanout_health_handle();
        let rescan_manual_intents = self.state_cell.manual_rescan_intents_handle();
        let rescan_manual_wake = self.manual_rescan_intent_wake.clone();
        let node_id_rescan = self.node_id.clone();
        let node_id_rescan_scoped = self.node_id.clone();
        let rescan_roots_scoped = rescan_roots.clone();
        let routes = source_find_route_bindings_for(&self.node_id.0);

        match routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS) {
            Ok(route) => {
                if self.should_start_source_status_endpoint_for_route(&route.0) {
                    self.start_source_status_endpoint_on_route(
                        boundary.clone(),
                        route,
                        "source.start_runtime_endpoints.route_present.status",
                    );
                }
            }
            Err(err) => {
                log::error!(
                    "failed to resolve source status route {}.{}: {:?}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_STATUS,
                    err
                );
            }
        }
        let scoped_status_route = source_status_request_route_for(&self.node_id.0);
        if self.should_start_source_status_endpoint_for_route(&scoped_status_route.0) {
            self.start_source_status_endpoint_on_route(
                boundary.clone(),
                scoped_status_route,
                "source.start_runtime_endpoints.route_present.scoped_status",
            );
        }

        match routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN) {
            Ok(route)
                if !self.endpoint_task_route_present(
                    &route.0,
                    "source.start_runtime_endpoints.route_present.rescan",
                ) =>
            {
                log::info!(
                    "bound route listening on {}.{} for deferred source {}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_RESCAN,
                    node_id_rescan.0
                );
                let rescan_roots_for_handler = rescan_roots.clone();
                let rescan_root_tasks_for_handler = rescan_root_tasks.clone();
                let rescan_fanout_health_for_handler = rescan_fanout_health.clone();
                let rescan_manual_intents_for_handler = rescan_manual_intents.clone();
                let rescan_manual_wake_for_handler = rescan_manual_wake.clone();
                let endpoint_task = ManagedEndpointTask::spawn_with_unit(
                    boundary.clone(),
                    route,
                    format!(
                        "source:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN
                    ),
                    SOURCE_RUNTIME_UNIT_ID,
                    self.shutdown.clone(),
                    move |requests| {
                        let node_id_rescan = node_id_rescan.clone();
                        let rescan_roots = rescan_roots_for_handler.clone();
                        let rescan_root_tasks = rescan_root_tasks_for_handler.clone();
                        let rescan_fanout_health = rescan_fanout_health_for_handler.clone();
                        let rescan_manual_intents = rescan_manual_intents_for_handler.clone();
                        let rescan_manual_wake = rescan_manual_wake_for_handler.clone();
                        async move {
                            eprintln!(
                                "fs_meta_source: source.rescan endpoint start node={} requests={}",
                                node_id_rescan.0,
                                requests.len()
                            );
                            let expected = lock_or_recover(
                                &rescan_roots,
                                "source.rescan.endpoint.roots.expected",
                            )
                            .iter()
                            .filter(|root| root.is_group_primary && root.spec.scan)
                            .map(FSMetaSource::root_runtime_key)
                            .collect::<Vec<_>>();
                            let roots_snapshot = lock_or_recover(
                                &rescan_roots,
                                "source.rescan.endpoint.roots.trigger",
                            )
                            .clone();
                            FSMetaSource::request_rescan_on_primary_roots(
                                &roots_snapshot,
                                Some(&rescan_root_tasks),
                                Some(&rescan_fanout_health),
                                Some(&rescan_manual_intents),
                                Some(&rescan_manual_wake),
                                "manual",
                            );
                            eprintln!(
                                "fs_meta_source: source.rescan endpoint triggered node={} expected_roots={}",
                                node_id_rescan.0,
                                expected.len()
                            );

                            requests
                                .into_iter()
                                .map(|req| {
                                    let mut meta = EventMetadata {
                                        origin_id: node_id_rescan.clone(),
                                        timestamp_us: now_us(),
                                        logical_ts: None,
                                        correlation_id: None,
                                        ingress_auth: None,
                                        trace: None,
                                    };
                                    meta.correlation_id = req.metadata().correlation_id;
                                    Event::new(meta, bytes::Bytes::from_static(b"accepted"))
                                })
                                .collect()
                        }
                    },
                );
                lock_or_recover(&self.endpoint_tasks, "source.start_runtime_endpoints.tasks")
                    .push(endpoint_task);
            }
            Err(err) => {
                log::error!(
                    "failed to resolve deferred source bound route {}.{}: {:?}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_RESCAN,
                    err
                );
            }
            Ok(_) => {}
        }

        match routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN_CONTROL) {
            Ok(route)
                if !self.endpoint_task_route_present(
                    &route.0,
                    "source.start_runtime_endpoints.route_present.rescan_control",
                ) =>
            {
                log::info!(
                    "bound stream listening on {}.{} for deferred source {}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_RESCAN_CONTROL,
                    self.node_id.0
                );
                let control_route_key = route.0.clone();
                let control_ready = self.clone();
                let endpoint_task = ManagedEndpointTask::spawn_stream(
                    boundary.clone(),
                    route,
                    format!(
                        "source:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN_CONTROL
                    ),
                    SOURCE_RUNTIME_UNIT_ID,
                    self.shutdown.clone(),
                    move || control_ready.should_receive_control_stream_route(&control_route_key),
                    move |events| async move {
                        let _ = events;
                    },
                );
                lock_or_recover(
                    &self.endpoint_tasks,
                    "source.start_runtime_endpoints.control_stream_tasks",
                )
                .push(endpoint_task);
            }
            Err(err) => {
                log::error!(
                    "failed to resolve deferred source stream route {}.{}: {:?}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_RESCAN_CONTROL,
                    err
                );
            }
            Ok(_) => {}
        }

        let spawn_roots_control_stream =
            |route: capanix_app_sdk::runtime::RouteKey, route_label: String| {
                log::info!(
                    "bound stream listening on {} for deferred source {}",
                    route_label,
                    self.node_id.0
                );
                let source = self.clone();
                let control_node_id = self.node_id.clone();
                let control_route_key = route.0.clone();
                let control_ready = self.clone();
                let endpoint_task = ManagedEndpointTask::spawn_stream(
                    boundary.clone(),
                    route,
                    format!("source:{route_label}"),
                    SOURCE_RUNTIME_UNIT_ID,
                    self.shutdown.clone(),
                    move || control_ready.should_receive_control_stream_route(&control_route_key),
                    move |events| {
                        let source = source.clone();
                        let control_node_id = control_node_id.clone();
                        async move {
                            eprintln!(
                                "fs_meta_source: source.roots control stream received node={} events={}",
                                control_node_id.0,
                                events.len()
                            );
                            for event in events {
                                let payload = match decode_logical_roots_control_payload(
                                    event.payload_bytes(),
                                ) {
                                    Ok(payload) => payload,
                                    Err(err) => {
                                        log::warn!(
                                            "source logical-roots control decode failed on node {}: {:?}",
                                            control_node_id.0,
                                            err
                                        );
                                        continue;
                                    }
                                };
                                if source
                                    .logical_roots_control_generation_is_stale(payload.generation)
                                {
                                    log::warn!(
                                        "source logical-roots control stale on node {}: payload_generation={}",
                                        control_node_id.0,
                                        payload.generation,
                                    );
                                    continue;
                                }
                                let apply_result = if payload.defer_scan_audit {
                                    source
                                        .apply_logical_roots_update_defer_scan_audit(payload.roots)
                                        .await
                                } else {
                                    source
                                        .apply_logical_roots_snapshot(
                                            payload.roots,
                                            true,
                                            "source.roots_control_stream",
                                        )
                                        .await
                                };
                                match apply_result {
                                    Ok(()) => source
                                        .mark_logical_roots_control_generation(payload.generation),
                                    Err(err) => {
                                        log::warn!(
                                            "source logical-roots control apply failed on node {}: {:?}",
                                            control_node_id.0,
                                            err
                                        );
                                    }
                                }
                            }
                        }
                    },
                );
                lock_or_recover(
                    &self.endpoint_tasks,
                    "source.start_runtime_endpoints.roots_control_stream_tasks",
                )
                .push(endpoint_task);
            };

        match default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_ROOTS_CONTROL)
        {
            Ok(route)
                if !self.endpoint_task_route_present(
                    &route.0,
                    "source.start_runtime_endpoints.route_present.roots_control_default",
                ) =>
            {
                spawn_roots_control_stream(
                    route,
                    format!(
                        "{}.{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_ROOTS_CONTROL
                    ),
                )
            }
            Err(err) => {
                log::error!(
                    "failed to resolve deferred source stream route {}.{}: {:?}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_ROOTS_CONTROL,
                    err
                );
            }
            Ok(_) => {}
        }

        let scoped_roots_route = source_roots_control_stream_route_for(&self.node_id.0);
        if !self.endpoint_task_route_present(
            &scoped_roots_route.0,
            "source.start_runtime_endpoints.route_present.roots_control_scoped",
        ) {
            spawn_roots_control_stream(scoped_roots_route.clone(), scoped_roots_route.0);
        }

        for route_key in self.active_source_rescan_request_routes()? {
            let route = RouteKey(route_key.clone());
            if !self.endpoint_task_route_present(
                &route.0,
                "source.start_runtime_endpoints.route_present.scoped_rescan",
            ) {
                log::info!(
                    "bound route listening on {} for deferred source {}",
                    route.0,
                    node_id_rescan_scoped.0
                );
                let target_route_key = source_rescan_request_route_for(&node_id_rescan_scoped.0).0;
                let route_key_for_handler = route.0.clone();
                let scoped_node_id_for_handler = node_id_rescan_scoped.clone();
                let scoped_roots_for_handler = rescan_roots_scoped.clone();
                let scoped_root_tasks_for_handler = rescan_root_tasks.clone();
                let scoped_fanout_health_for_handler = rescan_fanout_health.clone();
                let scoped_manual_intents_for_handler = rescan_manual_intents.clone();
                let scoped_manual_wake_for_handler = rescan_manual_wake.clone();
                let endpoint_task = ManagedEndpointTask::spawn_with_unit(
                    boundary.clone(),
                    route.clone(),
                    format!("source:{}", route.0),
                    SOURCE_RUNTIME_UNIT_ID,
                    self.shutdown.clone(),
                    move |requests| {
                        let node_id_rescan_scoped = scoped_node_id_for_handler.clone();
                        let rescan_roots_scoped = scoped_roots_for_handler.clone();
                        let rescan_root_tasks = scoped_root_tasks_for_handler.clone();
                        let rescan_fanout_health = scoped_fanout_health_for_handler.clone();
                        let rescan_manual_intents = scoped_manual_intents_for_handler.clone();
                        let rescan_manual_wake = scoped_manual_wake_for_handler.clone();
                        let route_key_for_handler = route_key_for_handler.clone();
                        let target_route_key = target_route_key.clone();
                        async move {
                            eprintln!(
                                "fs_meta_source: source.rescan scoped endpoint start node={} route={} requests={}",
                                node_id_rescan_scoped.0,
                                route_key_for_handler,
                                requests.len()
                            );
                            if route_key_for_handler != target_route_key {
                                return requests
                                    .into_iter()
                                    .map(|req| {
                                        let mut meta = EventMetadata {
                                            origin_id: node_id_rescan_scoped.clone(),
                                            timestamp_us: now_us(),
                                            logical_ts: None,
                                            correlation_id: None,
                                            ingress_auth: None,
                                            trace: None,
                                        };
                                        meta.correlation_id = req.metadata().correlation_id;
                                        Event::new(meta, Bytes::from_static(b"not-target"))
                                    })
                                    .collect();
                            }
                            let acceptance_timeout =
                                FSMetaSource::scoped_rescan_acceptance_timeout(&requests);
                            let roots_snapshot = FSMetaSource::wait_for_targeted_rescan_scan_roots(
                                rescan_roots_scoped.clone(),
                                acceptance_timeout,
                            )
                            .await;
                            let expected =
                                FSMetaSource::targeted_rescan_scan_root_keys(&roots_snapshot);
                            let payload = if expected.is_empty() {
                                eprintln!(
                                    "fs_meta_source: source.rescan scoped endpoint rejected node={} route={} reason=no-local-scheduled-scan-root",
                                    node_id_rescan_scoped.0, route_key_for_handler,
                                );
                                Bytes::from_static(
                                    b"scoped source-rescan target has no local scheduled scan root (source-primary required)",
                                )
                            } else {
                                FSMetaSource::request_rescan_on_targeted_scan_roots(
                                    &roots_snapshot,
                                    Some(&rescan_root_tasks),
                                    Some(&rescan_fanout_health),
                                    Some(&rescan_manual_intents),
                                    Some(&rescan_manual_wake),
                                    "manual",
                                );
                                eprintln!(
                                    "fs_meta_source: source.rescan scoped endpoint accepted and enqueued node={} route={} expected_roots={}",
                                    node_id_rescan_scoped.0,
                                    route_key_for_handler,
                                    expected.len()
                                );
                                Bytes::from_static(b"accepted")
                            };

                            requests
                                .into_iter()
                                .map(|req| {
                                    let mut meta = EventMetadata {
                                        origin_id: node_id_rescan_scoped.clone(),
                                        timestamp_us: now_us(),
                                        logical_ts: None,
                                        correlation_id: None,
                                        ingress_auth: None,
                                        trace: None,
                                    };
                                    meta.correlation_id = req.metadata().correlation_id;
                                    Event::new(meta, payload.clone())
                                })
                                .collect()
                        }
                    },
                );
                lock_or_recover(
                    &self.endpoint_tasks,
                    "source.start_runtime_endpoints.scoped_tasks",
                )
                .push(endpoint_task);
            }
        }

        Ok(())
    }

    pub async fn start_runtime_endpoints(&self, boundary: Arc<dyn ChannelIoSubset>) -> Result<()> {
        self.start_runtime_endpoints_on_boundary(boundary).await
    }

    /// Current lifecycle state.
    #[allow(dead_code)]
    pub fn state(&self) -> LifecycleState {
        *lock_or_recover(&self.state, "source.state")
    }

    /// Trigger a manual rescan (e.g., simulating Q_OVERFLOW or for testing).
    pub fn trigger_rescan(&self) {
        let roots = lock_or_recover(&self.state_cell.roots, "source.trigger_rescan.roots").clone();
        Self::request_rescan_on_primary_roots(
            &roots,
            Some(&self.state_cell.root_tasks),
            Some(&self.state_cell.fanout_health),
            Some(&self.state_cell.manual_rescan_intents),
            Some(&self.manual_rescan_intent_wake),
            "manual",
        );
    }

    pub(crate) fn signal_recorded_manual_rescan_intents(
        &self,
        root_keys: &std::collections::BTreeSet<String>,
    ) {
        let roots = lock_or_recover(
            &self.state_cell.roots,
            "source.signal_recorded_manual_rescan_intents.roots",
        )
        .clone();
        Self::signal_rescan_on_root_keys(&roots, Some(&self.state_cell.root_tasks), root_keys);
        if !root_keys.is_empty() {
            Self::wake_manual_rescan_intents(&self.manual_rescan_intent_wake);
        }
    }

    pub async fn trigger_rescan_when_ready(&self) {
        if !self.wait_for_group_primary_scan_roots_ready().await {
            log::debug!(
                "source-fs-meta: queue deferred manual rescan before primary scan roots report running"
            );
        }
        self.trigger_rescan();
    }

    pub(crate) async fn perform_trigger_rescan_when_ready_epoch(&self) -> u64 {
        let has_local_source_primary_scan_roots = lock_or_recover(
            &self.state_cell.roots,
            "source.trigger_rescan_when_ready_epoch.roots",
        )
        .iter()
        .any(|root| root.is_group_primary && root.spec.scan);
        let status = self.status_snapshot();
        let (published_batches, last_published_at_us) = source_status_publication_marker(&status);
        let last_audit_completed_at_us = source_status_rescan_completion_marker(&status);
        let epoch = self.state_cell.begin_rescan_request_epoch(
            published_batches,
            last_published_at_us,
            last_audit_completed_at_us,
        );
        if has_local_source_primary_scan_roots {
            self.trigger_rescan_when_ready().await;
        } else {
            self.state_cell.mark_rescan_request_observed(epoch);
        }
        epoch
    }

    fn local_targeted_rescan_scan_root_keys(&self, context: &'static str) -> Vec<String> {
        lock_or_recover(&self.state_cell.roots, context)
            .iter()
            .filter(|root| root.active && root.is_group_primary && root.spec.scan)
            .map(Self::root_runtime_key)
            .collect()
    }

    pub(crate) fn targeted_rescan_delivery_acceptance(
        &self,
    ) -> SourceTargetedRescanDeliveryAcceptance {
        let target_roots =
            self.local_targeted_rescan_scan_root_keys("source.targeted_rescan_delivery.roots");
        if target_roots.is_empty() {
            SourceTargetedRescanDeliveryAcceptance::NotLocalScanRoot
        } else {
            SourceTargetedRescanDeliveryAcceptance::Accepted
        }
    }

    pub(crate) fn accept_targeted_rescan_delivery_with_failure(&self) -> Result<()> {
        match self.targeted_rescan_delivery_acceptance() {
            SourceTargetedRescanDeliveryAcceptance::Accepted => Ok(()),
            SourceTargetedRescanDeliveryAcceptance::NotLocalScanRoot => {
                Err(CnxError::InvalidInput(
                    "scoped source-rescan target has no local scheduled scan root (source-primary required)".into(),
                ))
            }
        }
    }

    pub(crate) fn submit_targeted_rescan_request_epoch_with_failure(&self) -> Result<u64> {
        let (epoch, signal_root_keys) = self.begin_targeted_rescan_request_epoch_with_failure()?;
        self.signal_recorded_manual_rescan_intents(&signal_root_keys);
        Ok(epoch)
    }

    pub(crate) fn begin_targeted_rescan_request_epoch_with_failure(
        &self,
    ) -> Result<(u64, std::collections::BTreeSet<String>)> {
        self.accept_targeted_rescan_delivery_with_failure()?;
        let status = self.status_snapshot();
        let (published_batches, last_published_at_us) = source_status_publication_marker(&status);
        let last_audit_completed_at_us = source_status_rescan_completion_marker(&status);
        let epoch = self.state_cell.begin_rescan_request_epoch(
            published_batches,
            last_published_at_us,
            last_audit_completed_at_us,
        );
        let roots = lock_or_recover(
            &self.state_cell.roots,
            "source.begin_targeted_rescan_request_epoch.roots",
        )
        .clone();
        let signal_root_keys = Self::record_rescan_intent_on_targeted_scan_roots(
            &roots,
            Some(&self.state_cell.fanout_health),
            Some(&self.state_cell.manual_rescan_intents),
            "manual",
        );
        Ok((epoch, signal_root_keys))
    }

    pub(crate) fn submit_rescan_request_epoch(&self) -> u64 {
        let (epoch, signal_root_keys) = self.begin_rescan_request_epoch();
        self.signal_recorded_manual_rescan_intents(&signal_root_keys);
        epoch
    }

    pub(crate) fn begin_rescan_request_epoch(&self) -> (u64, std::collections::BTreeSet<String>) {
        let has_local_source_primary_scan_roots = lock_or_recover(
            &self.state_cell.roots,
            "source.begin_rescan_request_epoch.roots.check",
        )
        .iter()
        .any(|root| root.is_group_primary && root.spec.scan);
        let status = self.status_snapshot();
        let (published_batches, last_published_at_us) = source_status_publication_marker(&status);
        let last_audit_completed_at_us = source_status_rescan_completion_marker(&status);
        let epoch = self.state_cell.begin_rescan_request_epoch(
            published_batches,
            last_published_at_us,
            last_audit_completed_at_us,
        );
        if has_local_source_primary_scan_roots {
            let roots = lock_or_recover(
                &self.state_cell.roots,
                "source.begin_rescan_request_epoch.roots.record",
            )
            .clone();
            let signal_root_keys = Self::record_rescan_intent_on_primary_roots(
                &roots,
                Some(&self.state_cell.fanout_health),
                Some(&self.state_cell.manual_rescan_intents),
                "manual",
            );
            (epoch, signal_root_keys)
        } else {
            self.state_cell.mark_rescan_request_observed(epoch);
            (epoch, std::collections::BTreeSet::new())
        }
    }

    #[cfg(test)]
    pub(crate) async fn trigger_rescan_when_ready_epoch(&self) -> u64 {
        self.perform_trigger_rescan_when_ready_epoch().await
    }

    /// Trigger an overflow diagnostic signal. Intended for tests simulating IN_Q_OVERFLOW recovery-path.
    #[allow(dead_code)]
    pub fn trigger_overflow_rescan(&self) {
        for root in lock_or_recover(&self.state_cell.roots, "source.trigger_overflow.roots").iter()
        {
            let _ = root.rescan_tx.send(RescanReason::Overflow);
        }
    }

    pub(crate) fn snapshot_logical_roots(&self) -> Vec<RootSpec> {
        lock_or_recover(
            &self.state_cell.logical_roots,
            "source.logical_roots.snapshot",
        )
        .clone()
    }

    pub(crate) fn snapshot_cached_logical_roots(&self) -> Vec<RootSpec> {
        self.snapshot_logical_roots()
    }

    /// Snapshot currently configured logical roots.
    pub fn logical_roots_snapshot(&self) -> Vec<RootSpec> {
        self.snapshot_logical_roots()
    }

    pub(crate) fn snapshot_host_object_grants(&self) -> Vec<GrantedMountRoot> {
        lock_or_recover(
            &self.state_cell.host_object_grants,
            "source.host_object_grants.snapshot",
        )
        .clone()
    }

    pub(crate) fn snapshot_cached_host_object_grants(&self) -> Vec<GrantedMountRoot> {
        self.snapshot_host_object_grants()
    }

    /// Snapshot current granted mount roots.
    pub fn host_object_grants_snapshot(&self) -> Vec<GrantedMountRoot> {
        self.snapshot_host_object_grants()
    }

    pub(crate) fn snapshot_host_object_grants_version(&self) -> u64 {
        self.host_object_grants_version.load(Ordering::Relaxed)
    }

    /// Snapshot current host object grants version.
    pub fn host_object_grants_version_snapshot(&self) -> u64 {
        self.snapshot_host_object_grants_version()
    }

    pub(crate) fn snapshot_lifecycle_state_label(&self) -> String {
        format!("{:?}", self.state()).to_ascii_lowercase()
    }

    pub(crate) async fn apply_logical_roots_snapshot(
        &self,
        roots: Vec<RootSpec>,
        persist_authoritative: bool,
        commit_label: &'static str,
    ) -> Result<()> {
        let mut cfg = self.config.clone();
        cfg.roots = roots;
        let root_specs = cfg.effective_roots().map_err(CnxError::InvalidInput)?;
        let host_object_grants = self.host_object_grants_snapshot();
        let fanout = Self::compute_logical_root_fanout(&root_specs, &host_object_grants);
        let root_count = root_specs.len();
        let grant_count = host_object_grants.len();
        let bound_scopes = root_specs
            .iter()
            .map(|root| RuntimeBoundScope {
                scope_id: root.id.clone(),
                resource_ids: Vec::new(),
            })
            .collect::<Vec<_>>();
        self.unit_control
            .sync_active_scopes(SourceRuntimeUnit::Source.unit_id(), &bound_scopes)?;
        self.unit_control
            .sync_active_scopes(SourceRuntimeUnit::Scan.unit_id(), &bound_scopes)?;

        *lock_or_recover(
            &self.state_cell.logical_roots,
            "source.apply.logical_roots_snapshot",
        ) = root_specs.clone();
        Self::set_logical_root_health(
            &self.state_cell.fanout_health,
            &root_specs,
            &fanout,
            self.config.lru_capacity,
        );
        *lock_or_recover(
            &self.state_cell.logical_root_fanout,
            "source.apply.logical_root_fanout",
        ) = fanout;
        if persist_authoritative {
            self.state_cell
                .logical_roots_cell
                .replace(root_specs.clone())
                .await?;
        }
        self.refresh_runtime_roots(false).await?;
        self.trigger_rescan();
        self.state_cell.record_authoritative_commit(
            commit_label,
            format!("roots={} host_object_grants={}", root_count, grant_count),
        );
        Ok(())
    }

    pub(crate) async fn sync_logical_roots_from_authoritative_cell_if_changed(
        &self,
    ) -> Result<bool> {
        let Some(authoritative_roots) = self.authoritative_logical_roots_if_changed()? else {
            return Ok(false);
        };
        eprintln!(
            "fs_meta_source: authoritative logical-roots sync begin node={} roots={}",
            self.node_id.0,
            authoritative_roots.len()
        );
        self.apply_logical_roots_snapshot(
            authoritative_roots.clone(),
            false,
            "source.sync_logical_roots_from_authority",
        )
        .await?;
        eprintln!(
            "fs_meta_source: authoritative logical-roots sync ok node={} roots={}",
            self.node_id.0,
            authoritative_roots.len()
        );
        Ok(true)
    }

    pub(crate) fn authoritative_logical_roots_if_changed(&self) -> Result<Option<Vec<RootSpec>>> {
        let authoritative_roots = self
            .state_cell
            .logical_roots_cell
            .refresh_from_boundary_blocking()?;
        if root_specs_signature(&authoritative_roots)
            == root_specs_signature(&self.logical_roots_snapshot())
        {
            return Ok(None);
        }
        Ok(Some(authoritative_roots))
    }

    /// Snapshot logical-root fanout mapping.
    pub fn logical_root_fanout_snapshot(&self) -> HashMap<String, Vec<GrantedMountRoot>> {
        lock_or_recover(
            &self.state_cell.logical_root_fanout,
            "source.logical_root_fanout.snapshot",
        )
        .clone()
    }

    pub(crate) fn snapshot_group_id_for_object_ref(&self, object_ref: &str) -> Option<String> {
        let fanout = lock_or_recover(
            &self.state_cell.logical_root_fanout,
            "source.logical_root_fanout.lookup",
        );
        fanout.iter().find_map(|(group_id, members)| {
            members
                .iter()
                .any(|member| member.object_ref == object_ref)
                .then(|| group_id.clone())
        })
    }

    pub(crate) fn current_logical_roots_generation(&self) -> u64 {
        self.state_cell.current_logical_roots_generation()
    }

    fn logical_roots_control_generation_is_stale(&self, generation: u64) -> bool {
        self.state_cell
            .logical_roots_control_generation_is_stale(generation)
    }

    fn mark_logical_roots_control_generation(&self, generation: u64) {
        self.state_cell
            .mark_logical_roots_control_generation(generation);
    }

    pub fn resolve_group_id_for_object_ref(&self, object_ref: &str) -> Option<String> {
        self.snapshot_group_id_for_object_ref(object_ref)
    }

    /// Snapshot fanout health projection.
    pub fn fanout_health_snapshot(
        &self,
    ) -> (
        std::collections::BTreeMap<String, String>,
        std::collections::BTreeMap<String, String>,
    ) {
        let state = lock_or_recover(
            &self.state_cell.fanout_health,
            "source.fanout_health.snapshot",
        );
        (
            state
                .logical_root
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            state
                .object_ref
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        )
    }

    /// Snapshot degraded roots tracked by sentinel.
    pub fn degraded_roots_snapshot(&self) -> Vec<(String, String)> {
        self.sentinel.degraded_roots()
    }

    pub(crate) fn build_status_snapshot(&self) -> SourceStatusSnapshot {
        let current_stream_generation = lock_or_recover(
            &self.state_cell.stream_binding,
            "source.status.snapshot.stream_binding",
        )
        .as_ref()
        .map(|binding| binding.generation);
        let health = lock_or_recover(&self.state_cell.fanout_health, "source.status.snapshot");
        let mut logical_roots = health
            .logical_root_detail
            .iter()
            .map(|(root_id, entry)| SourceLogicalRootHealthSnapshot {
                root_id: root_id.clone(),
                status: entry.status.clone(),
                matched_grants: entry.matched_grants,
                active_members: entry.active_members,
                coverage_mode: entry.coverage_mode.clone(),
            })
            .collect::<Vec<_>>();
        logical_roots.sort_by(|a, b| a.root_id.cmp(&b.root_id));

        let mut concrete_roots = health
            .object_ref_detail
            .iter()
            .map(|(root_key, entry)| SourceConcreteRootHealthSnapshot {
                root_key: root_key.clone(),
                logical_root_id: entry.logical_root_id.clone(),
                object_ref: entry.object_ref.clone(),
                status: entry.status.clone(),
                coverage_mode: entry.coverage_mode.clone(),
                watch_enabled: entry.watch_enabled,
                scan_enabled: entry.scan_enabled,
                is_group_primary: entry.is_group_primary,
                active: entry.active,
                watch_lru_capacity: entry.watch_lru_capacity,
                audit_interval_ms: entry.audit_interval_ms,
                overflow_count: entry.overflow_count,
                overflow_pending: entry.overflow_pending,
                rescan_pending: entry.rescan_pending,
                last_rescan_requested_at_us: entry.last_rescan_requested_at_us,
                last_rescan_reason: entry.last_rescan_reason.clone(),
                last_error: entry.last_error.clone(),
                last_audit_started_at_us: entry.last_audit_started_at_us,
                last_audit_completed_at_us: entry.last_audit_completed_at_us,
                last_audit_duration_ms: entry.last_audit_duration_ms,
                emitted_batch_count: entry.emitted_batch_count,
                emitted_event_count: entry.emitted_event_count,
                emitted_control_event_count: entry.emitted_control_event_count,
                emitted_data_event_count: entry.emitted_data_event_count,
                emitted_path_capture_target: debug_stream_path_capture_target()
                    .map(|target| String::from_utf8_lossy(&target).into_owned()),
                emitted_path_event_count: entry.emitted_path_event_count,
                last_emitted_at_us: entry.last_emitted_at_us,
                last_emitted_origins: entry.last_emitted_origins.clone(),
                forwarded_batch_count: entry.forwarded_batch_count,
                forwarded_event_count: entry.forwarded_event_count,
                forwarded_path_event_count: entry.forwarded_path_event_count,
                last_forwarded_at_us: entry.last_forwarded_at_us,
                last_forwarded_origins: entry.last_forwarded_origins.clone(),
                current_revision: entry.current_revision,
                current_stream_generation: entry.current_stream_generation,
                candidate_revision: entry.candidate_revision,
                candidate_stream_generation: entry.candidate_stream_generation,
                candidate_status: entry.candidate_status.clone(),
                draining_revision: entry.draining_revision,
                draining_stream_generation: entry.draining_stream_generation,
                draining_status: entry.draining_status.clone(),
            })
            .collect::<Vec<_>>();
        concrete_roots.sort_by(|a, b| a.root_key.cmp(&b.root_key));

        SourceStatusSnapshot {
            current_stream_generation,
            logical_roots,
            concrete_roots,
            degraded_roots: self.sentinel.degraded_roots(),
        }
    }

    pub fn status_snapshot(&self) -> SourceStatusSnapshot {
        self.build_status_snapshot()
    }

    pub fn yielded_path_origin_counts_snapshot(&self) -> BTreeMap<String, u64> {
        lock_or_recover(
            &self.yielded_path_origin_counts,
            "source.yielded_path_origin_counts.snapshot",
        )
        .clone()
    }

    pub fn enqueued_path_origin_counts_snapshot(&self) -> BTreeMap<String, u64> {
        lock_or_recover(
            &self.enqueued_path_origin_counts,
            "source.enqueued_path_origin_counts.snapshot",
        )
        .enqueued_path_origin_counts
        .clone()
    }

    pub fn pending_path_origin_counts_snapshot(&self) -> BTreeMap<String, u64> {
        let enqueued = self.enqueued_path_origin_counts_snapshot();
        let yielded = self.yielded_path_origin_counts_snapshot();
        let mut pending = BTreeMap::new();
        for (origin, count) in enqueued {
            let remaining = count.saturating_sub(yielded.get(&origin).copied().unwrap_or(0));
            if remaining > 0 {
                pending.insert(origin, remaining);
            }
        }
        pending
    }

    pub(crate) fn snapshot_source_primary_by_group(&self) -> BTreeMap<String, String> {
        let local_primary = lock_or_recover(
            &self.state_cell.roots,
            "source.snapshot_source_primary_by_group.roots",
        )
        .iter()
        .filter(|root| root.active && root.is_group_primary && root.spec.scan)
        .map(|root| (root.logical_root_id.clone(), root.object_ref.clone()))
        .collect::<BTreeMap<_, _>>();
        if !local_primary.is_empty() {
            return local_primary;
        }
        if self.unit_control.has_runtime_state() {
            return BTreeMap::new();
        }
        Self::compute_group_primary_by_logical_root(
            &self.snapshot_logical_roots(),
            &self.snapshot_host_object_grants(),
        )
        .into_iter()
        .collect()
    }

    pub fn source_primary_by_group_snapshot(&self) -> BTreeMap<String, String> {
        self.snapshot_source_primary_by_group()
    }

    pub(crate) async fn emit_manual_rescan_signal(&self) -> Result<()> {
        self.manual_rescan_signal
            .emit(&self.node_id.0)
            .await
            .map(|_| ())
            .map_err(|err| {
                CnxError::Internal(format!("publish manual rescan signal failed: {err}"))
            })
    }

    pub async fn publish_manual_rescan_signal(&self) -> Result<()> {
        self.emit_manual_rescan_signal().await
    }

    pub(crate) fn current_rescan_observed_epoch(&self) -> u64 {
        self.state_cell.current_rescan_observed_epoch()
    }

    pub(crate) fn materialized_read_cache_epoch(&self) -> u64 {
        self.state_cell
            .current_rescan_request_epoch()
            .max(self.state_cell.current_rescan_observed_epoch())
    }

    pub(crate) fn build_progress_snapshot(&self) -> SourceProgressSnapshot {
        SourceProgressSnapshot {
            rescan_observed_epoch: self.current_rescan_observed_epoch(),
            scheduled_source_groups: self
                .scheduled_source_group_ids()
                .ok()
                .flatten()
                .unwrap_or_default(),
            scheduled_scan_groups: self
                .scheduled_scan_group_ids()
                .ok()
                .flatten()
                .unwrap_or_default(),
            published_group_epoch: self.state_cell.published_group_epoch_snapshot(),
        }
    }

    #[cfg(test)]
    pub(crate) fn progress_snapshot(&self) -> SourceProgressSnapshot {
        self.build_progress_snapshot()
    }

    pub(crate) fn maybe_mark_rescan_observed_if_publication_advanced(
        &self,
        published_batches: u64,
        last_published_at_us: u64,
        last_audit_completed_at_us: u64,
    ) {
        self.state_cell
            .mark_rescan_observed_if_publication_advanced(
                published_batches,
                last_published_at_us,
                last_audit_completed_at_us,
            );
    }

    pub async fn start_manual_rescan_watch(&self) -> Result<()> {
        let mut task_slot = lock_or_recover(
            &self.manual_rescan_watch_task,
            "source.manual_rescan_watch.start",
        );
        if task_slot.as_ref().is_some_and(|task| !task.is_finished()) {
            return Ok(());
        }

        let source = self.clone();
        let shutdown = self.shutdown.clone();
        let initial_seq = self.manual_rescan_signal.current_seq();
        *task_slot = Some(tokio::spawn(async move {
            let mut offset = 0_u64;
            let mut last_seq = initial_seq;
            loop {
                match source.manual_rescan_signal.watch_since(offset).await {
                    Ok((next_offset, updates)) => {
                        offset = next_offset;
                        for update in updates {
                            if update.seq <= last_seq {
                                continue;
                            }
                            last_seq = update.seq;
                            eprintln!(
                                "fs_meta_source: observed cluster manual rescan signal seq={} requested_by={}",
                                update.seq, update.requested_by
                            );
                            let _ = source.perform_trigger_rescan_when_ready_epoch().await;
                        }
                    }
                    Err(err) => {
                        log::warn!("source manual rescan watch failed: {err}");
                    }
                }

                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(MANUAL_RESCAN_WATCH_POLL_INTERVAL) => {}
                }
            }
        }));
        Ok(())
    }

    pub(crate) fn snapshot_force_find_inflight_groups(&self) -> Vec<String> {
        let mut groups = lock_or_recover(
            &self.force_find_inflight,
            "source.force_find.inflight.snapshot",
        )
        .iter()
        .cloned()
        .collect::<Vec<_>>();
        groups.sort();
        groups
    }

    pub fn force_find_inflight_groups_snapshot(&self) -> Vec<String> {
        self.snapshot_force_find_inflight_groups()
    }

    pub(crate) fn snapshot_last_force_find_runner_by_group(&self) -> BTreeMap<String, String> {
        lock_or_recover(
            &self.force_find_last_runner,
            "source.force_find.last_runner.snapshot",
        )
        .clone()
    }

    pub fn last_force_find_runner_by_group_snapshot(&self) -> BTreeMap<String, String> {
        self.snapshot_last_force_find_runner_by_group()
    }

    pub(crate) fn snapshot_last_control_frame_signals(&self) -> Vec<String> {
        lock_or_recover(
            &self.last_control_frame_signals,
            "source.last_control_frame_signals.snapshot",
        )
        .clone()
    }

    pub fn last_control_frame_signals_snapshot(&self) -> Vec<String> {
        self.snapshot_last_control_frame_signals()
    }

    fn accept_manual_rescan_control_envelope(&self, envelope: &ControlEnvelope) -> Result<bool> {
        let payload = match envelope {
            ControlEnvelope::Frame(frame) if frame.kind == MANUAL_RESCAN_CONTROL_FRAME_KIND => {
                rmp_serde::from_slice::<ManualRescanControlPayload>(&frame.payload).map_err(
                    |err| {
                        CnxError::InvalidInput(format!(
                            "decode manual rescan control payload failed: {err}"
                        ))
                    },
                )?
            }
            _ => {
                return Err(CnxError::InvalidInput(
                    "manual rescan signal missing manual rescan control frame".into(),
                ));
            }
        };
        let request_id = payload.request_id.trim();
        if !request_id.is_empty() {
            let mut seen = lock_or_recover(
                &self.manual_rescan_control_seen_request_ids,
                "source.manual_rescan_control.seen_request_ids",
            );
            if seen.iter().any(|seen_id| seen_id == request_id) {
                return Ok(false);
            }
            seen.push_back(request_id.to_string());
            while seen.len() > MANUAL_RESCAN_SEEN_REQUEST_IDS_LIMIT {
                seen.pop_front();
            }
            return Ok(true);
        }
        let requested_at_us = payload.requested_at_us;
        let mut observed = self
            .manual_rescan_control_high_watermark_us
            .load(Ordering::Acquire);
        loop {
            if requested_at_us <= observed {
                return Ok(false);
            }
            match self
                .manual_rescan_control_high_watermark_us
                .compare_exchange(
                    observed,
                    requested_at_us,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                Ok(_) => return Ok(true),
                Err(actual) => observed = actual,
            }
        }
    }

    async fn wait_for_group_primary_scan_roots_ready(&self) -> bool {
        let expected = lock_or_recover(
            &self.state_cell.roots,
            "source.wait_for_group_primary_scan_roots_ready.roots",
        )
        .iter()
        .filter(|root| root.is_group_primary && root.spec.scan)
        .map(Self::root_runtime_key)
        .collect::<Vec<_>>();
        if expected.is_empty() {
            return true;
        }

        let deadline = tokio::time::Instant::now() + RESCAN_READY_WAIT_TIMEOUT;
        loop {
            if self.shutdown.is_cancelled() {
                return false;
            }

            let ready = {
                let health = lock_or_recover(
                    &self.state_cell.fanout_health,
                    "source.wait_for_group_primary_scan_roots_ready.health",
                );
                expected.iter().all(|root_key| {
                    health
                        .object_ref
                        .get(root_key)
                        .is_some_and(|status| status == "running")
                })
            };
            if ready {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                log::debug!(
                    "source-fs-meta: timed out waiting for primary scan roots before rescan: {:?}",
                    expected
                );
                return false;
            }
            tokio::time::sleep(RESCAN_READY_POLL_INTERVAL).await;
        }
    }

    pub(crate) async fn apply_logical_roots_update(&self, roots: Vec<RootSpec>) -> Result<()> {
        eprintln!(
            "fs_meta_source: update_logical_roots begin node={} roots={}",
            self.node_id.0,
            roots.len()
        );
        let root_count = roots.len();
        self.apply_logical_roots_snapshot(roots, true, "source.update_logical_roots")
            .await?;
        self.mark_logical_roots_control_generation(self.current_logical_roots_generation());
        eprintln!(
            "fs_meta_source: update_logical_roots ok node={} roots={} grants={}",
            self.node_id.0,
            root_count,
            self.host_object_grants_snapshot().len()
        );
        Ok(())
    }

    pub(crate) async fn apply_logical_roots_update_defer_scan_audit(
        &self,
        roots: Vec<RootSpec>,
    ) -> Result<()> {
        self.state_cell.close_scan_audit_admission();
        self.apply_logical_roots_update(roots).await
    }

    pub(crate) fn open_scan_audit_admission_if_closed(&self) -> bool {
        self.state_cell.open_scan_audit_admission_if_closed()
    }

    /// Update logical roots online and reconcile concrete-root tasks without restart.
    pub async fn update_logical_roots(&self, roots: Vec<RootSpec>) -> Result<()> {
        self.apply_logical_roots_update(roots).await
    }
}

impl FSMetaSource {
    /// Execute sentinel actions and bridge them to the existing rescan path.
    fn execute_sentinel_actions(
        actions: &[SentinelAction],
        root_key: &str,
        rescan_tx: Option<&tokio::sync::broadcast::Sender<RescanReason>>,
        fanout_health: Option<&Arc<Mutex<FanoutHealthState>>>,
    ) {
        for action in actions {
            match action {
                SentinelAction::TriggerRescan {
                    root_key: action_root,
                } => {
                    if action_root != root_key {
                        continue;
                    }
                    log::warn!("sentinel: action=trigger_rescan root={}", root_key,);
                    if let Some(tx) = rescan_tx {
                        if let Some(health) = fanout_health {
                            Self::mark_root_rescan_requested(health, root_key, "sentinel");
                        }
                        // Use Manual so sentinel-triggered rescans do not count as another overflow signal.
                        let _ = tx.send(RescanReason::Manual);
                    }
                }
                SentinelAction::ReportDegraded {
                    root_key: action_root,
                    reason,
                } => {
                    if action_root != root_key {
                        continue;
                    }
                    log::warn!(
                        "sentinel: action=degraded root={} reason={}",
                        root_key,
                        reason,
                    );
                    if let Some(health) = fanout_health {
                        Self::update_object_health(health, root_key, format!("degraded: {reason}"));
                    }
                }
                SentinelAction::ReportRecovered {
                    root_key: action_root,
                } => {
                    if action_root != root_key {
                        continue;
                    }
                    log::info!("sentinel: action=recovered root={}", root_key,);
                    if let Some(health) = fanout_health {
                        Self::update_object_health(health, root_key, "running");
                    }
                }
            }
        }
    }

    /// Access the sentinel for diagnostics.
    #[allow(dead_code)]
    pub(crate) fn sentinel(&self) -> &Sentinel {
        &self.sentinel
    }

    fn spawn_root_task(
        &self,
        root: RootRuntime,
        out_tx: mpsc::Sender<Vec<Event>>,
        realtime_out_tx: mpsc::Sender<Vec<Event>>,
        stream_generation: u64,
        role: RootTaskRole,
        revision: u64,
    ) -> RootTaskHandle {
        let root_key = Self::root_runtime_key(&root);
        let task_signature = Self::root_task_signature(&root);
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let (ready_tx, ready_rx) = tokio::sync::watch::channel(false);
        let global_shutdown = self.shutdown.clone();
        let config = self.config.clone();
        let drift_estimator = self.drift_estimator.clone();
        let logical_clock = self.logical_clock.clone();
        let fanout_health = self.state_cell.fanout_health_handle();
        let roots_handle = self.state_cell.roots_handle();
        let manual_rescan_intents = self.state_cell.manual_rescan_intents_handle();
        let manual_rescan_intent_wake = self.manual_rescan_intent_wake.subscribe();
        let enqueued_path_origin_counts = self.enqueued_path_origin_counts.clone();
        let scan_audit_admission_open = self.state_cell.scan_audit_admission_open.clone();
        let sentinel = self.sentinel.clone();
        let apply_startup_delay = self.boundary.is_some();
        let root_rescan_tx = root.rescan_tx.clone();
        let join = std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(err) => {
                    let status =
                        format!("init_error: build fs-meta source root task runtime: {err}");
                    log::error!("{status}");
                    Self::update_root_task_slot_health(
                        &fanout_health,
                        &root_key,
                        revision,
                        stream_generation,
                        role,
                        &task_signature,
                        &config,
                        Self::root_current_is_active(&roots_handle, &root_key),
                        status,
                    );
                    let _ = ready_tx.send(false);
                    return;
                }
            };
            runtime.block_on(async move {
                    let mut backoff = Duration::from_secs(1);
                    let max_backoff = Duration::from_secs(60);
                    loop {
                if global_shutdown.is_cancelled() || task_cancel.is_cancelled() {
                    break;
                }
                Self::update_root_task_slot_health(
                    &fanout_health,
                    &root_key,
                    revision,
                    stream_generation,
                    role,
                    &task_signature,
                    &config,
                    Self::root_current_is_active(&roots_handle, &root_key),
                    "warming",
                );
                let stream = Self::root_stream(
                    config.clone(),
                    drift_estimator.clone(),
                    logical_clock.clone(),
                    global_shutdown.clone(),
                    task_cancel.clone(),
                    root.clone(),
                    apply_startup_delay,
                    sentinel.clone(),
                    fanout_health.clone(),
                    roots_handle.clone(),
                    manual_rescan_intents.clone(),
                    manual_rescan_intent_wake.clone(),
                    root_key.clone(),
                    Some(realtime_out_tx.clone()),
                    scan_audit_admission_open.clone(),
                )
                .await;
                match stream {
                    Ok(stream) => {
                        eprintln!(
                            "fs_meta_source: root task stream ready root_key={} role={:?} revision={} stream_generation={}",
                            root_key, role, revision, stream_generation
                        );
                        let _ = ready_tx.send(true);
                        Self::update_root_task_slot_health(
                            &fanout_health,
                            &root_key,
                            revision,
                            stream_generation,
                            role,
                            &task_signature,
                            &config,
                            Self::root_current_is_active(&roots_handle, &root_key),
                            "running",
                        );
                        if Self::root_current_is_group_primary(&roots_handle, &root_key) {
                            // Group-primary owns audit/sentinel periodic loop authority.
                            let actions = sentinel.handle_signal(HealthSignal::PipelineRecovered {
                                root_key: root_key.clone(),
                            });
                            Self::execute_sentinel_actions(
                                &actions,
                                &root_key,
                                Some(&root.rescan_tx),
                                Some(&fanout_health),
                            );
                        }
                        tokio::pin!(stream);
                        while let Some(batch) = stream.next().await {
                            if task_cancel.is_cancelled() || global_shutdown.is_cancelled() {
                                break;
                            }
                            let path_capture_target = debug_stream_path_capture_target();
                            let path_matching = path_capture_target
                                .as_deref()
                                .map(|target| count_events_under_query_path(&batch, target))
                                .unwrap_or_default();
                            let path_origin_counts = path_capture_target
                                .as_deref()
                                .filter(|_| path_matching > 0)
                                .map(|target| {
                                    batch
                                        .iter()
                                        .filter_map(|event| {
                                            let record = rmp_serde::from_slice::<FileMetaRecord>(
                                                event.payload_bytes(),
                                            )
                                            .ok()?;
                                            is_under_query_path(&record.path, target)
                                                .then(|| event.metadata().origin_id.0.clone())
                                        })
                                        .fold(BTreeMap::<String, u64>::new(), |mut acc, origin| {
                                            *acc.entry(origin).or_default() += 1;
                                            acc
                                        })
                                })
                                .unwrap_or_default();
                            let path_origins = path_origin_counts
                                .iter()
                                .map(|(origin, count)| format!("{origin}={count}"))
                                .collect::<Vec<_>>();
                            Self::mark_root_emitted_batch(&fanout_health, &root_key, &batch);
                            if let Some(target) = path_capture_target.as_deref()
                                && path_matching > 0
                            {
                                eprintln!(
                                    "fs_meta_source: outflow_path_capture root_key={} target={} matching_events={} batch_events={} origins={:?}",
                                    root_key,
                                    String::from_utf8_lossy(target),
                                    path_matching,
                                    batch.len(),
                                    path_origins
                                );
                            }
                            let stream_lane = Self::source_stream_lane_for_batch(&batch);
                            if matches!(stream_lane, SourceStreamLane::Realtime) {
                                match realtime_out_tx.try_send(batch) {
                                    Ok(()) => {
                                        Self::record_current_stream_enqueued_path_counts(
                                            &enqueued_path_origin_counts,
                                            stream_generation,
                                            &path_origin_counts,
                                        );
                                        continue;
                                    }
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        Self::mark_root_overflow_observed(
                                            &fanout_health,
                                            &root_key,
                                        );
                                        let overflow_actions =
                                            sentinel.handle_signal(HealthSignal::WatchOverflow {
                                                root_key: root_key.clone(),
                                            });
                                        Self::execute_sentinel_actions(
                                            &overflow_actions,
                                            &root_key,
                                            Some(&root.rescan_tx),
                                            Some(&fanout_health),
                                        );
                                        let backpressure_actions = sentinel.handle_signal(
                                            HealthSignal::HostFsBackpressure {
                                                root_key: root_key.clone(),
                                                op: "realtime source output queue full"
                                                    .to_string(),
                                            },
                                        );
                                        Self::execute_sentinel_actions(
                                            &backpressure_actions,
                                            &root_key,
                                            Some(&root.rescan_tx),
                                            Some(&fanout_health),
                                        );
                                        let _ = root.rescan_tx.send(RescanReason::Overflow);
                                        continue;
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        let reason = "source realtime output channel closed";
                                        Self::update_root_task_slot_health(
                                            &fanout_health,
                                            &root_key,
                                            revision,
                                            stream_generation,
                                            role,
                                            &task_signature,
                                            &config,
                                            Self::root_current_is_active(&roots_handle, &root_key),
                                            "output_closed",
                                        );
                                        Self::set_object_last_error(&fanout_health, &root_key, reason);
                                        return;
                                    }
                                }
                            }
                            let send_result = tokio::select! {
                                _ = global_shutdown.cancelled() => break,
                                _ = task_cancel.cancelled() => break,
                                result = out_tx.send(batch) => result,
                            };
                            if send_result.is_ok() {
                                Self::record_current_stream_enqueued_path_counts(
                                    &enqueued_path_origin_counts,
                                    stream_generation,
                                    &path_origin_counts,
                                );
                            } else {
                                let reason = "source output channel closed";
                                Self::update_root_task_slot_health(
                                    &fanout_health,
                                    &root_key,
                                    revision,
                                    stream_generation,
                                    role,
                                    &task_signature,
                                    &config,
                                    Self::root_current_is_active(&roots_handle, &root_key),
                                    "output_closed",
                                );
                                Self::set_object_last_error(&fanout_health, &root_key, reason);
                                return;
                            }
                        }
                        backoff = Duration::from_secs(1);
                    }
                    Err(CnxError::ChannelClosed) => break,
                    Err(err) => {
                        log::warn!(
                            "host object {} pipeline failed: {:?}; retry in {}s",
                            root_key,
                            err,
                            backoff.as_secs()
                        );
                        Self::update_root_task_slot_health(
                            &fanout_health,
                            &root_key,
                            revision,
                            stream_generation,
                            role,
                            &task_signature,
                            &config,
                            Self::root_current_is_active(&roots_handle, &root_key),
                            format!("error: {err}"),
                        );
                        Self::set_object_last_error(&fanout_health, &root_key, err.to_string());
                        if Self::root_current_is_group_primary(&roots_handle, &root_key) {
                            // Group-primary owns audit/sentinel periodic loop authority.
                            let actions = sentinel.handle_signal(HealthSignal::PipelineError {
                                root_key: root_key.clone(),
                                error: format!("{err}"),
                            });
                            Self::execute_sentinel_actions(
                                &actions,
                                &root_key,
                                Some(&root.rescan_tx),
                                Some(&fanout_health),
                            );
                        }
                        tokio::select! {
                            _ = global_shutdown.cancelled() => break,
                            _ = task_cancel.cancelled() => break,
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
                    }
                }
            }
                    Self::update_root_task_slot_health(
                        &fanout_health,
                        &root_key,
                        revision,
                        stream_generation,
                        role,
                        &task_signature,
                        &config,
                        Self::root_current_is_active(&roots_handle, &root_key),
                        "stopped",
                    );
                })
        });
        RootTaskHandle {
            cancel,
            join: Some(join),
            ready_rx,
            rescan_tx: root_rescan_tx,
        }
    }

    async fn reconcile_root_tasks(&self, desired_roots: &[RootRuntime]) {
        let stream_binding = lock_or_recover(
            &self.state_cell.stream_binding,
            "source.reconcile.stream_binding",
        )
        .clone();
        let Some(stream_binding) = stream_binding else {
            return;
        };
        let out_tx = stream_binding.tx.clone();
        let realtime_out_tx = stream_binding.realtime_tx.clone();
        let stream_generation = stream_binding.generation;

        let desired_root_keys = desired_roots
            .iter()
            .map(Self::root_runtime_key)
            .collect::<BTreeSet<_>>();
        let desired_active_keys = desired_roots
            .iter()
            .filter(|root| root.active)
            .map(Self::root_runtime_key)
            .collect::<BTreeSet<_>>();
        let mut desired = HashMap::<String, (RootRuntime, RootTaskSignature)>::new();
        for root in desired_roots {
            if !root.active {
                continue;
            }
            desired.insert(
                Self::root_runtime_key(root),
                (root.clone(), Self::root_task_signature(root)),
            );
        }

        let mut removed_absent = Vec::<(String, RootTaskEntry)>::new();
        let mut removed_finished = Vec::<(String, RootTaskEntry)>::new();
        let mut stale_candidates = Vec::<(String, RootTaskSlot)>::new();
        let mut watch_disable_replacements = Vec::<RootTaskReplacementFallback>::new();
        let mut replacement_waits = Vec::<(
            String,
            RootTaskSignature,
            tokio::sync::watch::Receiver<bool>,
        )>::new();
        let mut added_ready = Vec::<tokio::sync::watch::Receiver<bool>>::new();
        {
            let mut tasks =
                lock_or_recover(&self.state_cell.root_tasks, "source.reconcile.root_tasks");
            let existing = tasks.keys().cloned().collect::<Vec<_>>();
            for key in existing {
                if !desired_active_keys.contains(&key) {
                    if let Some(entry) = tasks.remove(&key) {
                        removed_absent.push((key, entry));
                    }
                } else if tasks
                    .get(&key)
                    .is_some_and(|entry| entry.active.stream_generation != stream_generation)
                {
                    if let Some(entry) = tasks.remove(&key) {
                        removed_finished.push((key, entry));
                    }
                } else if tasks
                    .get(&key)
                    .is_some_and(|entry| entry.active.handle.is_finished())
                {
                    if let Some(entry) = tasks.remove(&key) {
                        removed_finished.push((key, entry));
                    }
                }
            }
            for (key, (root, signature)) in &desired {
                match tasks.get_mut(key) {
                    Some(existing_entry) if existing_entry.active.signature == *signature => {
                        if let Some(candidate) = existing_entry.candidate.as_ref() {
                            if candidate.handle.is_finished() {
                                if let Some(stale) = existing_entry.candidate.take() {
                                    stale_candidates.push((key.clone(), stale));
                                    Self::clear_root_task_candidate_health(
                                        &self.state_cell.fanout_health,
                                        key,
                                    );
                                }
                            } else if candidate.signature == *signature {
                                replacement_waits.push((
                                    key.clone(),
                                    signature.clone(),
                                    candidate.handle.ready_rx.clone(),
                                ));
                            } else if let Some(stale) = existing_entry.candidate.take() {
                                stale_candidates.push((key.clone(), stale));
                                Self::clear_root_task_candidate_health(
                                    &self.state_cell.fanout_health,
                                    key,
                                );
                            }
                        }
                    }
                    Some(existing_entry) => {
                        if existing_entry.active.signature.watch && !signature.watch {
                            if let Some(stale) = existing_entry.candidate.take() {
                                stale_candidates.push((key.clone(), stale));
                                Self::clear_root_task_candidate_health(
                                    &self.state_cell.fanout_health,
                                    key,
                                );
                            }
                            watch_disable_replacements.push(RootTaskReplacementFallback {
                                key: key.clone(),
                                root: root.clone(),
                                expected_signature: signature.clone(),
                            });
                            continue;
                        }
                        let mut candidate_ready = None;
                        if let Some(candidate) = existing_entry.candidate.as_ref() {
                            if candidate.handle.is_finished() {
                                if let Some(stale) = existing_entry.candidate.take() {
                                    stale_candidates.push((key.clone(), stale));
                                    Self::clear_root_task_candidate_health(
                                        &self.state_cell.fanout_health,
                                        key,
                                    );
                                }
                            } else if candidate.signature == *signature {
                                candidate_ready = Some(candidate.handle.ready_rx.clone());
                            }
                        }
                        if let Some(ready_rx) = candidate_ready {
                            replacement_waits.push((key.clone(), signature.clone(), ready_rx));
                            continue;
                        }
                        if let Some(stale) = existing_entry.candidate.take() {
                            stale_candidates.push((key.clone(), stale));
                            Self::clear_root_task_candidate_health(
                                &self.state_cell.fanout_health,
                                key,
                            );
                        }
                        let revision = self.state_cell.next_root_task_revision();
                        let handle = self.spawn_root_task(
                            root.clone(),
                            out_tx.clone(),
                            realtime_out_tx.clone(),
                            stream_generation,
                            RootTaskRole::Candidate,
                            revision,
                        );
                        replacement_waits.push((
                            key.clone(),
                            signature.clone(),
                            handle.ready_rx.clone(),
                        ));
                        existing_entry.candidate = Some(RootTaskSlot {
                            revision,
                            stream_generation,
                            signature: signature.clone(),
                            handle,
                        });
                    }
                    None => {
                        let revision = self.state_cell.next_root_task_revision();
                        let handle = self.spawn_root_task(
                            root.clone(),
                            out_tx.clone(),
                            realtime_out_tx.clone(),
                            stream_generation,
                            RootTaskRole::Active,
                            revision,
                        );
                        added_ready.push(handle.ready_rx.clone());
                        tasks.insert(
                            key.clone(),
                            RootTaskEntry {
                                active: RootTaskSlot {
                                    revision,
                                    stream_generation,
                                    signature: signature.clone(),
                                    handle,
                                },
                                candidate: None,
                            },
                        );
                    }
                }
            }
        }

        if !removed_finished.is_empty() {
            for (key, entry) in removed_finished {
                Self::cancel_root_task_slot(
                    &self.state_cell.fanout_health,
                    &key,
                    entry.active,
                    &self.config,
                    true,
                    true,
                )
                .await;
                if let Some(candidate) = entry.candidate {
                    candidate
                        .handle
                        .cancel_and_join(Duration::from_secs(2))
                        .await;
                    Self::clear_root_task_candidate_health(&self.state_cell.fanout_health, &key);
                }
            }
        }

        for (key, stale) in stale_candidates {
            Self::mark_root_task_draining(
                &self.state_cell.fanout_health,
                &key,
                stale.revision,
                stale.stream_generation,
                &stale.signature,
                &self.config,
                false,
                "draining",
            );
            stale.handle.cancel_and_join(Duration::from_secs(2)).await;
            Self::finish_root_task_draining(
                &self.state_cell.fanout_health,
                &key,
                stale.revision,
                stale.stream_generation,
                true,
            );
            Self::clear_root_task_candidate_health(&self.state_cell.fanout_health, &key);
        }

        let mut forced_ready = Vec::<tokio::sync::watch::Receiver<bool>>::new();
        let mut extracted_forced = Vec::<(RootTaskReplacementFallback, RootTaskEntry)>::new();
        {
            let mut tasks = lock_or_recover(
                &self.state_cell.root_tasks,
                "source.reconcile.root_tasks.watch_disable",
            );
            for replacement in watch_disable_replacements {
                if let Some(entry) = tasks.remove(&replacement.key) {
                    extracted_forced.push((replacement, entry));
                }
            }
        }

        for (replacement, entry) in extracted_forced {
            Self::cancel_root_task_slot(
                &self.state_cell.fanout_health,
                &replacement.key,
                entry.active,
                &self.config,
                true,
                true,
            )
            .await;
            if let Some(candidate) = entry.candidate {
                candidate
                    .handle
                    .cancel_and_join(Duration::from_secs(2))
                    .await;
                Self::clear_root_task_candidate_health(
                    &self.state_cell.fanout_health,
                    &replacement.key,
                );
            }
            let revision = self.state_cell.next_root_task_revision();
            let handle = self.spawn_root_task(
                replacement.root.clone(),
                out_tx.clone(),
                realtime_out_tx.clone(),
                stream_generation,
                RootTaskRole::Active,
                revision,
            );
            forced_ready.push(handle.ready_rx.clone());
            lock_or_recover(
                &self.state_cell.root_tasks,
                "source.reconcile.root_tasks.watch_disable.replace",
            )
            .insert(
                replacement.key.clone(),
                RootTaskEntry {
                    active: RootTaskSlot {
                        revision,
                        stream_generation,
                        signature: replacement.expected_signature,
                        handle,
                    },
                    candidate: None,
                },
            );
        }

        if !forced_ready.is_empty() {
            let _ =
                Self::wait_for_task_handles_ready(&mut forced_ready, Duration::from_secs(2)).await;
        }

        if !removed_absent.is_empty() && !added_ready.is_empty() {
            let _ =
                Self::wait_for_task_handles_ready(&mut added_ready, Duration::from_secs(2)).await;
        }

        let mut replacement_ready_receivers = replacement_waits
            .iter()
            .map(|(_, _, rx)| rx.clone())
            .collect::<Vec<_>>();
        if !replacement_ready_receivers.is_empty() {
            let _ = Self::wait_for_task_handles_ready(
                &mut replacement_ready_receivers,
                Duration::from_secs(2),
            )
            .await;
        }

        let mut fallback_replacements = Vec::<RootTaskReplacementFallback>::new();
        let mut promoted_old = Vec::<(String, RootTaskSlot)>::new();
        {
            let mut tasks = lock_or_recover(
                &self.state_cell.root_tasks,
                "source.reconcile.root_tasks.promote",
            );
            for (key, (desired_root, expected_signature)) in &desired {
                let Some(entry) = tasks.get_mut(key) else {
                    continue;
                };
                let candidate_ready = entry.candidate.as_ref().is_some_and(|slot| {
                    slot.signature == *expected_signature && *slot.handle.ready_rx.borrow()
                });
                if !candidate_ready {
                    if entry.active.signature != *expected_signature
                        && entry
                            .candidate
                            .as_ref()
                            .is_some_and(|slot| slot.signature == *expected_signature)
                    {
                        fallback_replacements.push(RootTaskReplacementFallback {
                            key: key.clone(),
                            root: desired_root.clone(),
                            expected_signature: expected_signature.clone(),
                        });
                    }
                    continue;
                }
                let candidate = entry.candidate.take().expect("candidate ready");
                Self::promote_root_task_candidate_health(
                    &self.state_cell.fanout_health,
                    key,
                    candidate.revision,
                    candidate.stream_generation,
                    &candidate.signature,
                    &self.config,
                    true,
                );
                let old_active = std::mem::replace(&mut entry.active, candidate);
                Self::mark_root_task_draining(
                    &self.state_cell.fanout_health,
                    key,
                    old_active.revision,
                    old_active.stream_generation,
                    &old_active.signature,
                    &self.config,
                    true,
                    "draining",
                );
                promoted_old.push((key.clone(), old_active));
            }
        }

        for (key, old_active) in promoted_old {
            Self::cancel_root_task_slot(
                &self.state_cell.fanout_health,
                &key,
                old_active,
                &self.config,
                true,
                true,
            )
            .await;
        }

        let mut fallback_ready = Vec::<tokio::sync::watch::Receiver<bool>>::new();
        let mut extracted_fallbacks =
            Vec::<(RootTaskReplacementFallback, RootTaskEntry, RootTaskSlot)>::new();
        {
            let mut tasks = lock_or_recover(
                &self.state_cell.root_tasks,
                "source.reconcile.root_tasks.fallback",
            );
            for fallback in fallback_replacements {
                let Some(entry) = tasks.remove(&fallback.key) else {
                    continue;
                };
                let Some(candidate) = entry.candidate.as_ref() else {
                    tasks.insert(fallback.key.clone(), entry);
                    continue;
                };
                if candidate.signature != fallback.expected_signature {
                    tasks.insert(fallback.key.clone(), entry);
                    continue;
                }
                let candidate_revision = candidate.revision;
                let mut entry = entry;
                let candidate_slot = entry
                    .candidate
                    .take()
                    .expect("candidate extracted for fallback");
                debug_assert_eq!(candidate_slot.revision, candidate_revision);
                extracted_fallbacks.push((fallback, entry, candidate_slot));
            }
        }

        for (fallback, entry, candidate) in extracted_fallbacks {
            Self::fail_root_task_candidate_health(
                &self.state_cell.fanout_health,
                &fallback.key,
                candidate.revision,
                candidate.stream_generation,
                &candidate.signature,
                &self.config,
                false,
                "timed_out: overlap candidate did not become ready",
            );
            candidate
                .handle
                .cancel_and_join(Duration::from_secs(2))
                .await;
            Self::clear_root_task_candidate_health(&self.state_cell.fanout_health, &fallback.key);
            Self::cancel_root_task_slot(
                &self.state_cell.fanout_health,
                &fallback.key,
                entry.active,
                &self.config,
                true,
                true,
            )
            .await;

            let revision = self.state_cell.next_root_task_revision();
            let handle = self.spawn_root_task(
                fallback.root.clone(),
                out_tx.clone(),
                realtime_out_tx.clone(),
                stream_generation,
                RootTaskRole::Active,
                revision,
            );
            fallback_ready.push(handle.ready_rx.clone());
            lock_or_recover(
                &self.state_cell.root_tasks,
                "source.reconcile.root_tasks.fallback.replace",
            )
            .insert(
                fallback.key.clone(),
                RootTaskEntry {
                    active: RootTaskSlot {
                        revision,
                        stream_generation,
                        signature: fallback.expected_signature,
                        handle,
                    },
                    candidate: None,
                },
            );
        }

        if !fallback_ready.is_empty() {
            let _ = Self::wait_for_task_handles_ready(&mut fallback_ready, Duration::from_secs(2))
                .await;
        }

        if !removed_absent.is_empty() {
            for (key, entry) in removed_absent {
                Self::cancel_root_task_slot(
                    &self.state_cell.fanout_health,
                    &key,
                    entry.active,
                    &self.config,
                    false,
                    true,
                )
                .await;
                if let Some(candidate) = entry.candidate {
                    candidate
                        .handle
                        .cancel_and_join(Duration::from_secs(2))
                        .await;
                    Self::clear_root_task_candidate_health(&self.state_cell.fanout_health, &key);
                }
                if desired_root_keys.contains(&key) {
                    Self::update_object_health(&self.state_cell.fanout_health, &key, "retired");
                } else {
                    Self::remove_object_health(&self.state_cell.fanout_health, &key);
                }
            }
        }
    }

    /// Domain-specific streaming producer: unified scan (Epoch 0) + real-time watch loop.
    ///
    /// This is a domain helper, not part of `RuntimeBoundaryApp`. Runtime orchestrators
    /// use this for the continuous event pipeline.
    pub(crate) async fn build_pub_stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Vec<Event>> + Send>>> {
        if cfg!(not(target_os = "linux")) {
            return Err(CnxError::NotSupported(
                "fs-meta realtime watch requires linux host".into(),
            ));
        }
        self.start_manual_rescan_watch().await?;
        let (out_rx, realtime_rx, stream_generation, new_stream_binding) = {
            let mut binding =
                lock_or_recover(&self.state_cell.stream_binding, "source.pub.stream_binding");
            match binding.as_ref() {
                Some(existing) => (
                    existing.rx.clone(),
                    existing.realtime_rx.clone(),
                    existing.generation,
                    false,
                ),
                None => {
                    let stream_generation = self.state_cell.next_stream_generation();
                    let (tx, rx) = mpsc::channel::<Vec<Event>>(1024);
                    let rx = Arc::new(AsyncMutex::new(rx));
                    let (realtime_tx, realtime_rx) = mpsc::channel::<Vec<Event>>(1024);
                    let realtime_rx = Arc::new(AsyncMutex::new(realtime_rx));
                    *binding = Some(SourceStreamBinding {
                        generation: stream_generation,
                        tx: tx.clone(),
                        rx: rx.clone(),
                        realtime_tx: realtime_tx.clone(),
                        realtime_rx: realtime_rx.clone(),
                    });
                    (rx, realtime_rx, stream_generation, true)
                }
            }
        };
        if new_stream_binding {
            self.reset_current_stream_path_frontier_stats(stream_generation);
        }
        *lock_or_recover(&self.state, "source.pub.state") = LifecycleState::Scanning;
        let roots_snapshot = lock_or_recover(&self.state_cell.roots, "source.pub.roots").clone();
        self.reconcile_root_tasks(&roots_snapshot).await;
        *lock_or_recover(&self.state, "source.pub.state.ready") = LifecycleState::Ready;

        let shutdown = self.shutdown.clone();
        let yielded_path_origin_counts = self.yielded_path_origin_counts.clone();
        let output_stream = stream! {
            let mut pending_by_queue = BTreeMap::<String, VecDeque<Vec<Event>>>::new();
            let mut ready_queues = VecDeque::<String>::new();
            let mut scan_input_closed = false;
            let mut realtime_input_closed = false;
            loop {
                if !realtime_input_closed {
                    let realtime_result = {
                        let mut realtime_rx = realtime_rx.lock().await;
                        realtime_rx.try_recv()
                    };
                    match realtime_result {
                        Ok(events) => {
                            Self::record_current_stream_yielded_path_counts(
                                &yielded_path_origin_counts,
                                &events,
                            );
                            yield events;
                            continue;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {}
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            realtime_input_closed = true;
                        }
                    }
                }

                while !scan_input_closed
                    && Self::current_stream_pending_batch_count(&pending_by_queue)
                        < SOURCE_PUB_STREAM_PENDING_BATCH_LIMIT
                {
                    let recv_result = {
                        let mut out_rx = out_rx.lock().await;
                        out_rx.try_recv()
                    };
                    match recv_result {
                        Ok(events) => {
                            Self::enqueue_current_stream_batch(
                                &mut pending_by_queue,
                                &mut ready_queues,
                                events,
                            );
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            scan_input_closed = true;
                            break;
                        }
                    }
                }

                if !realtime_input_closed {
                    let realtime_result = {
                        let mut realtime_rx = realtime_rx.lock().await;
                        realtime_rx.try_recv()
                    };
                    match realtime_result {
                        Ok(events) => {
                            Self::record_current_stream_yielded_path_counts(
                                &yielded_path_origin_counts,
                                &events,
                            );
                            yield events;
                            continue;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {}
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            realtime_input_closed = true;
                        }
                    }
                }

                if let Some(events) = Self::dequeue_current_stream_batch(
                    &mut pending_by_queue,
                    &mut ready_queues,
                ) {
                    Self::record_current_stream_yielded_path_counts(
                        &yielded_path_origin_counts,
                        &events,
                    );
                    yield events;
                    continue;
                }
                if scan_input_closed && realtime_input_closed {
                    break;
                }
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    batch = async {
                        let mut realtime_rx = realtime_rx.lock().await;
                        realtime_rx.recv().await
                    }, if !realtime_input_closed => match batch {
                        Some(events) => {
                            Self::record_current_stream_yielded_path_counts(
                                &yielded_path_origin_counts,
                                &events,
                            );
                            yield events;
                        }
                        None => realtime_input_closed = true,
                    },
                    batch = async {
                        let mut out_rx = out_rx.lock().await;
                        out_rx.recv().await
                    }, if !scan_input_closed => match batch {
                        Some(events) => {
                            Self::enqueue_current_stream_batch(
                                &mut pending_by_queue,
                                &mut ready_queues,
                                events,
                            );
                        }
                        None => scan_input_closed = true,
                    }
                }
            }
        };
        Ok(Box::pin(output_stream))
    }

    pub async fn pub_(&self) -> Result<Pin<Box<dyn Stream<Item = Vec<Event>> + Send>>> {
        self.build_pub_stream().await
    }

    fn current_watch_manager(slot: &WatchManagerSlot) -> Option<SharedWatchManager> {
        lock_or_recover(slot, "source.root_stream.watch_manager_slot").clone()
    }

    async fn root_stream(
        config: SourceConfig,
        drift_estimator: Arc<Mutex<DriftEstimator>>,
        logical_clock: Arc<LogicalClock>,
        global_shutdown: CancellationToken,
        task_shutdown: CancellationToken,
        root: RootRuntime,
        apply_startup_delay: bool,
        sentinel: Arc<Sentinel>,
        fanout_health: Arc<Mutex<FanoutHealthState>>,
        roots_handle: Arc<Mutex<Vec<RootRuntime>>>,
        manual_rescan_intents: Arc<Mutex<HashMap<String, ManualRescanIntent>>>,
        mut manual_rescan_intent_wake: watch::Receiver<u64>,
        root_key: String,
        realtime_out_tx: Option<mpsc::Sender<Vec<Event>>>,
        scan_audit_admission_open: Arc<AtomicBool>,
    ) -> Result<Pin<Box<dyn Stream<Item = Vec<Event>> + Send>>> {
        let debug_root_task = debug_source_root_task_enabled();
        let root_path = root.monitor_path.clone();
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(300);

        loop {
            if global_shutdown.is_cancelled() || task_shutdown.is_cancelled() {
                return Err(CnxError::ChannelClosed);
            }
            if debug_root_task {
                eprintln!(
                    "fs_meta_source: probing root availability root_key={} path={}",
                    root_key,
                    root_path.display()
                );
            }
            let probe_host_fs = Arc::clone(&root.host_fs);
            let probe_path = root_path.clone();
            let probe_result = tokio::time::timeout(
                Duration::from_secs(15),
                tokio::task::spawn_blocking(move || probe_host_fs.metadata(&probe_path)),
            )
            .await;
            match probe_result {
                Ok(Ok(Ok(_))) => {
                    if debug_root_task {
                        eprintln!(
                            "fs_meta_source: root availability ok root_key={} path={}",
                            root_key,
                            root_path.display()
                        );
                    }
                    break;
                }
                Ok(Ok(Err(e))) => {
                    if debug_root_task {
                        eprintln!(
                            "fs_meta_source: root unavailable root_key={} path={} err={}",
                            root_key,
                            root_path.display(),
                            e
                        );
                    }
                    Self::update_object_health(
                        &fanout_health,
                        &root_key,
                        format!("waiting_for_root: {e}"),
                    );
                    log::warn!(
                        "Root {} unavailable ({}), retrying in {}s...",
                        root.spec.id,
                        e,
                        backoff.as_secs()
                    );
                    tokio::select! {
                        _ = global_shutdown.cancelled() => return Err(CnxError::ChannelClosed),
                        _ = task_shutdown.cancelled() => return Err(CnxError::ChannelClosed),
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }
                Ok(Err(err)) => {
                    let e = io::Error::other(format!("root availability probe join failed: {err}"));
                    eprintln!(
                        "fs_meta_source: root availability probe join failed root_key={} path={} err={}",
                        root_key,
                        root_path.display(),
                        e
                    );
                    Self::update_object_health(
                        &fanout_health,
                        &root_key,
                        format!("waiting_for_root: {e}"),
                    );
                    tokio::select! {
                        _ = global_shutdown.cancelled() => return Err(CnxError::ChannelClosed),
                        _ = task_shutdown.cancelled() => return Err(CnxError::ChannelClosed),
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }
                Err(_) => {
                    eprintln!(
                        "fs_meta_source: root availability probe timeout root_key={} path={} timeout_ms=15000",
                        root_key,
                        root_path.display()
                    );
                    let actions = sentinel.handle_signal(HealthSignal::HostFsTimeout {
                        root_key: root_key.clone(),
                        op: "root availability metadata probe".to_string(),
                    });
                    Self::execute_sentinel_actions(
                        &actions,
                        &root_key,
                        Some(&root.rescan_tx),
                        Some(&fanout_health),
                    );
                    Self::update_object_health(
                        &fanout_health,
                        &root_key,
                        "degraded: HOST_FS_TIMEOUT",
                    );
                    tokio::select! {
                        _ = global_shutdown.cancelled() => return Err(CnxError::ChannelClosed),
                        _ = task_shutdown.cancelled() => return Err(CnxError::ChannelClosed),
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }
            }
        }

        let watch_manager_slot: WatchManagerSlot = Arc::new(Mutex::new(None));

        let (scan_tx, mut scan_rx) = mpsc::channel::<Vec<Event>>(256);
        let rescan_scan_tx = scan_tx.clone();
        let initial_scan_tx = scan_tx.clone();
        let (audit_done_tx, mut audit_done_rx) = mpsc::channel::<RootAuditCompletion>(8);

        let (watch_tx, mut watch_rx) = mpsc::channel::<Vec<Event>>(256);
        let watch_events_bypass_scan = realtime_out_tx.is_some();
        let mut rescan_rx = root.rescan_tx.subscribe();
        let watch_handle = if root.spec.watch {
            let setup_config = config.clone();
            let setup_root_path = root_path.clone();
            let setup_emit_prefix = root.emit_prefix.clone();
            let setup_host_fs = root.host_fs.clone();
            let setup_root_id = root.spec.id.clone();
            let setup_root_key = root_key.clone();
            let setup_fanout_health = fanout_health.clone();
            let setup_sentinel = sentinel.clone();
            let setup_rescan_tx = root.rescan_tx.clone();
            let setup_watch_slot = watch_manager_slot.clone();
            let setup_shutdown = global_shutdown.clone();
            let watch_output_tx = realtime_out_tx.clone().unwrap_or_else(|| watch_tx.clone());
            let watch_backpressure_reporter = realtime_out_tx.as_ref().map(|_| {
                let report_sentinel = sentinel.clone();
                let report_fanout_health = fanout_health.clone();
                let report_root_key = root_key.clone();
                let report_rescan_tx = root.rescan_tx.clone();
                Arc::new(move |op: &'static str| {
                    Self::mark_root_overflow_observed(&report_fanout_health, &report_root_key);
                    let overflow_actions =
                        report_sentinel.handle_signal(HealthSignal::WatchOverflow {
                            root_key: report_root_key.clone(),
                        });
                    Self::execute_sentinel_actions(
                        &overflow_actions,
                        &report_root_key,
                        Some(&report_rescan_tx),
                        Some(&report_fanout_health),
                    );
                    let backpressure_actions =
                        report_sentinel.handle_signal(HealthSignal::HostFsBackpressure {
                            root_key: report_root_key.clone(),
                            op: op.to_string(),
                        });
                    Self::execute_sentinel_actions(
                        &backpressure_actions,
                        &report_root_key,
                        Some(&report_rescan_tx),
                        Some(&report_fanout_health),
                    );
                    let _ = report_rescan_tx.send(RescanReason::Overflow);
                }) as watcher::WatchBackpressureReporter
            });
            let watch_loop_drift = Arc::clone(&drift_estimator);
            let watch_loop_node_id = NodeId(root.object_ref.clone());
            let watch_loop_clock = Arc::clone(&logical_clock);
            let watch_loop_host_fs = root.host_fs.clone();
            let watch_loop_throttle = config.throttle_interval;
            Some(tokio::spawn(async move {
                let setup_result = tokio::task::spawn_blocking(move || {
                    let backend = setup_host_fs
                        .watch_session_open()
                        .map_err(|err| format!("failed to initialize host watch backend: {err}"))?;
                    let manager = WatchManager::new(
                        &setup_config,
                        setup_root_path.clone(),
                        setup_emit_prefix,
                        Box::new(backend),
                    )
                    .map_err(|err| format!("root watch manager init failed: {err}"))?;
                    let manager = Arc::new(Mutex::new(manager));
                    {
                        let mut mgr = lock_or_recover(&manager, "source.root_stream.root_schedule");
                        mgr.schedule(&setup_root_path)
                            .map_err(|err| format!("root watch schedule failed: {err}"))?;
                    }
                    Ok::<_, String>(manager)
                })
                .await;
                let manager = match setup_result {
                    Ok(Ok(manager)) => manager,
                    Ok(Err(err)) => {
                        let actions = setup_sentinel.handle_signal(HealthSignal::WatchInitFailed {
                            root_key: setup_root_key.clone(),
                            error: err.clone(),
                        });
                        Self::execute_sentinel_actions(
                            &actions,
                            &setup_root_key,
                            Some(&setup_rescan_tx),
                            Some(&setup_fanout_health),
                        );
                        log::warn!(
                            "root {} watch setup failed ({}); continuing with scan/audit",
                            setup_root_id,
                            err
                        );
                        Self::set_object_last_error(&setup_fanout_health, &setup_root_key, err);
                        return;
                    }
                    Err(err) => {
                        let message = format!("root watch setup task failed: {err}");
                        let actions = setup_sentinel.handle_signal(HealthSignal::WatchInitFailed {
                            root_key: setup_root_key.clone(),
                            error: message.clone(),
                        });
                        Self::execute_sentinel_actions(
                            &actions,
                            &setup_root_key,
                            Some(&setup_rescan_tx),
                            Some(&setup_fanout_health),
                        );
                        log::warn!(
                            "root {} watch setup failed ({}); continuing with scan/audit",
                            setup_root_id,
                            message
                        );
                        Self::set_object_last_error(&setup_fanout_health, &setup_root_key, message);
                        return;
                    }
                };
                if setup_shutdown.is_cancelled() {
                    return;
                }
                *lock_or_recover(
                    &setup_watch_slot,
                    "source.root_stream.watch_manager_slot.install",
                ) = Some(manager.clone());
                let watch_loop = watcher::start_watch_loop(
                    manager,
                    watch_loop_drift,
                    watch_output_tx,
                    setup_shutdown,
                    watch_loop_node_id,
                    watch_loop_clock,
                    watch_loop_host_fs,
                    watch_loop_throttle,
                    Some(setup_rescan_tx),
                    watch_backpressure_reporter,
                );
                let _ = watch_loop.await;
            }))
        } else {
            drop(watch_tx);
            None
        };

        let rescan_scanner = Arc::clone(&root.scanner);
        let rescan_drift = Arc::clone(&drift_estimator);
        let rescan_clock = Arc::clone(&logical_clock);
        let rescan_mtime_cache = Arc::clone(&root.mtime_cache);
        let rescan_epoch = Arc::clone(&root.epoch_counter);
        let rescan_watch_slot = watch_manager_slot.clone();
        let initial_scanner = Arc::clone(&root.scanner);
        let initial_drift = Arc::clone(&drift_estimator);
        let initial_clock = Arc::clone(&logical_clock);
        let initial_mtime_cache = Arc::clone(&root.mtime_cache);
        let initial_epoch = Arc::clone(&root.epoch_counter);
        let initial_watch_slot = watch_manager_slot.clone();
        let audit_interval = root
            .spec
            .audit_interval_ms
            .map(Duration::from_millis)
            .unwrap_or(config.audit_interval);

        let output_stream = stream! {
            // CONTRACTS.CLUSTER.COLD_START: Delay initial Epoch 0 scan by default.
            // The scan remains owned by this root stream, not a detached side task, so
            // roots/control churn cannot leave a serving primary with no audit owner.
            let startup_delay = if apply_startup_delay {
                epoch0_scan_delay()
            } else {
                Duration::ZERO
            };
            if debug_root_task {
                eprintln!(
                    "fs_meta_source: root stream entered root_key={} startup_delay_ms={} watch={} scan={}",
                    root_key,
                    startup_delay.as_millis(),
                    root.spec.watch,
                    root.spec.scan
                );
            }
            let mut initial_scan_pending = true;
            let mut initial_scan_inflight = false;
            let mut audit_inflight = false;
            let mut initial_scan_delay_elapsed = startup_delay.is_zero();
            let mut initial_scan_sleep = Box::pin(tokio::time::sleep(startup_delay));
            let mut initial_scan_wait_logged = false;
            let mut audit_tick = tokio::time::interval(audit_interval);
            let mut scan_channel_open = true;
            let mut audit_completion_open = true;
            let mut periodic_channel_open = root.spec.scan && !audit_interval.is_zero();
            let mut periodic_first_tick_discarded = !periodic_channel_open;
            let mut watch_channel_open = watch_handle.is_some() && !watch_events_bypass_scan;
            let mut last_manual_dispatch_target = 0_u64;
            let mut initial_scan_satisfies_manual_target = None::<u64>;
            let mut first_loop_state_logged = false;
            if periodic_channel_open {
                audit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            }
            let mut rescan_channel_open = root.spec.scan;
            let dispatch_pending_manual = |last_manual_dispatch_target: &mut u64| {
                let requested_target = {
                    let intents = lock_or_recover(
                        &manual_rescan_intents,
                        "source.root.manual_rescan_intents.dispatch_check",
                    );
                    intents
                        .get(&root_key)
                        .and_then(|intent| {
                            (intent.requested > intent.completed).then_some(intent.requested)
                        })
                        .unwrap_or(0)
                };
                if requested_target > *last_manual_dispatch_target {
                    if root.rescan_tx.send(RescanReason::Manual).is_ok() {
                        *last_manual_dispatch_target = requested_target;
                    }
                }
            };
            if rescan_channel_open {
                dispatch_pending_manual(&mut last_manual_dispatch_target);
                if debug_root_task {
                    eprintln!(
                        "fs_meta_source: root stream initial manual dispatch checked root_key={}",
                        root_key
                    );
                }
            }
            loop {
                dispatch_pending_manual(&mut last_manual_dispatch_target);
                let current_is_group_primary =
                    Self::root_current_is_group_primary(&roots_handle, &root_key);
                let current_is_group_primary_scan_enabled =
                    Self::root_current_is_group_primary_scan_enabled(&roots_handle, &root_key);
                let scan_audit_admitted = scan_audit_admission_open.load(Ordering::Acquire);
                let rescan_open = scan_audit_admitted && rescan_channel_open && !audit_inflight;
                let periodic_open = scan_audit_admitted
                    && periodic_channel_open
                    && current_is_group_primary
                    && !audit_inflight;
                let initial_delay_open = initial_scan_pending && !initial_scan_delay_elapsed;
                let initial_ready_open = initial_scan_pending
                    && scan_audit_admitted
                    && !initial_scan_inflight
                    && !audit_inflight
                    && initial_scan_delay_elapsed
                    && current_is_group_primary_scan_enabled;
                let initial_wait_open = initial_scan_pending
                    && scan_audit_admitted
                    && !initial_scan_inflight
                    && initial_scan_delay_elapsed
                    && !current_is_group_primary_scan_enabled;
                let scan_audit_gate_wait_open = !scan_audit_admitted
                    && (initial_scan_pending || rescan_channel_open || periodic_channel_open);
                if debug_root_task && !first_loop_state_logged {
                    eprintln!(
                        "fs_meta_source: root stream first select root_key={} primary={} scan_primary={} scan_audit_admitted={} initial_delay_open={} initial_ready_open={} rescan_open={} periodic_open={} watch_open={} scan_open={}",
                        root_key,
                        current_is_group_primary,
                        current_is_group_primary_scan_enabled,
                        scan_audit_admitted,
                        initial_delay_open,
                        initial_ready_open,
                        rescan_open,
                        periodic_open,
                        watch_channel_open,
                        scan_channel_open
                    );
                    first_loop_state_logged = true;
                }
                if debug_root_task && initial_wait_open && !initial_scan_wait_logged {
                    eprintln!(
                        "fs_meta_source: initial scan waiting for scan-enabled group-primary ownership root_key={}",
                        root_key
                    );
                    initial_scan_wait_logged = true;
                }
                tokio::select! {
                    _ = global_shutdown.cancelled() => break,
                    _ = task_shutdown.cancelled() => break,
                    _ = tokio::time::sleep(RESCAN_READY_POLL_INTERVAL), if scan_audit_gate_wait_open => {}
                    changed = manual_rescan_intent_wake.changed(), if rescan_channel_open => {
                        if changed.is_err() {
                            Self::close_rescan_channels(
                                &mut rescan_channel_open,
                                &mut periodic_channel_open,
                            );
                        }
                    }
                    _ = &mut initial_scan_sleep, if initial_delay_open => {
                        initial_scan_delay_elapsed = true;
                        if debug_root_task {
                            eprintln!(
                                "fs_meta_source: initial scan delay elapsed root_key={}",
                                root_key
                            );
                        }
                    }
                    _ = tokio::time::sleep(RESCAN_READY_POLL_INTERVAL), if initial_wait_open => {}
                    _ = async {}, if initial_ready_open => {
                        if debug_root_task {
                            eprintln!(
                                "fs_meta_source: initial scan starting root_key={}",
                                root_key
                            );
                        }
                        let audit_started_at_us = now_us();
                        Self::mark_root_audit_start(
                            &fanout_health,
                            &root_key,
                            "initial_scan",
                            audit_started_at_us,
                        );
                        let scanner = Arc::clone(&initial_scanner);
                        let de = Arc::clone(&initial_drift);
                        let clk = Arc::clone(&initial_clock);
                        let cache = Arc::clone(&initial_mtime_cache);
                        let epoch = Arc::clone(&initial_epoch);
                        let wm = Self::current_watch_manager(&initial_watch_slot);
                        let scan_sender = initial_scan_tx.clone();
                        let done_tx = audit_done_tx.clone();
                        let pending_manual_target =
                            Self::pending_manual_rescan_target_for_root(
                                &manual_rescan_intents,
                                &root_key,
                            );
                        initial_scan_satisfies_manual_target = pending_manual_target;
                        tokio::spawn(async move {
                            let scan_result = tokio::task::spawn_blocking(move || {
                                let mut c = lock_or_recover(&cache, "source.root.scan_audit.mtime_cache");
                                let mut ep = lock_or_recover(&epoch, "source.root.scan_audit.epoch_counter");
                                let current_epoch = *ep;
                                *ep += 1;
                                scanner.scan_audit_streaming(current_epoch, &mut c, &de, &clk, wm, |batch| {
                                    scan_sender.blocking_send(batch).is_ok()
                                })
                            }).await;
                            let result = scan_result.map_err(|err| format!("initial scan join failed: {err}"));
                            let _ = done_tx.send(RootAuditCompletion {
                                kind: RootAuditKind::Initial,
                                started_at_us: audit_started_at_us,
                                reason_label: "initial_scan",
                                manual_requested_target: None,
                                result,
                            }).await;
                        });
                        initial_scan_inflight = true;
                        audit_inflight = true;
                    }
                    batch = scan_rx.recv(), if scan_channel_open => match batch {
                        Some(batch) => yield batch,
                        None => {
                            scan_channel_open = false;
                        }
                    },
                    completion = audit_done_rx.recv(), if audit_completion_open => match completion {
                        Some(completion) => {
                            audit_inflight = false;
                            match completion.result {
                                Ok(audit) if audit.completed => {
                                    Self::mark_root_audit_completed(
                                        &fanout_health,
                                        &root_key,
                                        completion.started_at_us,
                                        now_us(),
                                    );
                                    if debug_root_task || matches!(completion.kind, RootAuditKind::Rescan) {
                                        eprintln!(
                                            "fs_meta_source: {} completed batches={} root_key={} records={}",
                                            completion.reason_label,
                                            audit.batch_count,
                                            root_key,
                                            audit.record_count
                                        );
                                    }
                                    if matches!(completion.kind, RootAuditKind::Initial) {
                                        initial_scan_pending = false;
                                        initial_scan_inflight = false;
                                        if let Some(target) = initial_scan_satisfies_manual_target.take() {
                                            Self::mark_manual_rescan_intent_completed_up_to(
                                                &manual_rescan_intents,
                                                &root_key,
                                                target,
                                            );
                                            if target >= last_manual_dispatch_target {
                                                last_manual_dispatch_target = target;
                                            }
                                        }
                                    }
                                    if let Some(target) = completion.manual_requested_target {
                                        let mut intents = lock_or_recover(
                                            &manual_rescan_intents,
                                            "source.root.manual_rescan_intents.mark_completed",
                                        );
                                        let entry = intents.entry(root_key.clone()).or_default();
                                        entry.completed = entry.completed.max(target);
                                        if entry.completed >= last_manual_dispatch_target {
                                            last_manual_dispatch_target = entry.completed;
                                        }
                                    }
                                }
                                Ok(audit) => {
                                    let message = format!(
                                        "{} ended before full audit delivery (batches={} records={})",
                                        completion.reason_label,
                                        audit.batch_count,
                                        audit.record_count
                                    );
                                    Self::set_object_last_error(&fanout_health, &root_key, message);
                                    if matches!(completion.kind, RootAuditKind::Initial) {
                                        initial_scan_inflight = false;
                                    }
                                    if completion.manual_requested_target.is_some() {
                                        last_manual_dispatch_target = 0;
                                    }
                                }
                                Err(err) => {
                                    Self::set_object_last_error(&fanout_health, &root_key, err);
                                    if matches!(completion.kind, RootAuditKind::Initial) {
                                        initial_scan_inflight = false;
                                    }
                                    if completion.manual_requested_target.is_some() {
                                        last_manual_dispatch_target = 0;
                                    }
                                }
                            }
                        }
                        None => {
                            audit_completion_open = false;
                        }
                    },
                    batch = watch_rx.recv(), if watch_channel_open => match batch {
                        Some(events) if !events.is_empty() => yield events,
                        Some(_) => {},
                        None => {
                            watch_channel_open = false;
                        },
                    },
                    _ = audit_tick.tick(), if periodic_open => {
                        if !periodic_first_tick_discarded {
                            periodic_first_tick_discarded = true;
                            continue;
                        }
                        Self::mark_root_rescan_requested(
                            &fanout_health,
                            &root_key,
                            "periodic",
                        );
                        let _ = root.rescan_tx.send(RescanReason::Periodic);
                    }
                    rescan = rescan_rx.recv(), if rescan_open => match rescan {
                        Ok(reason) => {
                            if !should_process_rescan_reason(current_is_group_primary, reason) {
                                continue;
                            }
                            if matches!(reason, RescanReason::Overflow) {
                                Self::mark_root_overflow_observed(&fanout_health, &root_key);
                                let actions = sentinel.handle_signal(HealthSignal::WatchOverflow {
                                    root_key: root_key.clone(),
                                });
                                Self::execute_sentinel_actions(
                                    &actions,
                                    &root_key,
                                    Some(&root.rescan_tx),
                                    Some(&fanout_health),
                                );
                                continue;
                            }

                            let reason_label = match reason {
                                RescanReason::Manual => "manual",
                                RescanReason::Periodic => "periodic",
                                RescanReason::Overflow => "overflow",
                            };
                            let manual_signal_intent = if matches!(reason, RescanReason::Manual) {
                                Some(Self::manual_rescan_signal_intent_for_root(
                                    &manual_rescan_intents,
                                    &root_key,
                                ))
                            } else {
                                None
                            };
                            if matches!(
                                manual_signal_intent,
                                Some(ManualRescanSignalIntent::AlreadyCompleted)
                            ) {
                                continue;
                            }
                            let manual_requested_target = match manual_signal_intent {
                                Some(ManualRescanSignalIntent::Pending(target)) => Some(target),
                                _ => None,
                            };
                            let audit_started_at_us = now_us();
                            Self::mark_root_audit_start(
                                &fanout_health,
                                &root_key,
                                reason_label,
                                audit_started_at_us,
                            );
                            let scanner = Arc::clone(&rescan_scanner);
                            let de = Arc::clone(&rescan_drift);
                            let clk = Arc::clone(&rescan_clock);
                            let cache = Arc::clone(&rescan_mtime_cache);
                            let epoch = Arc::clone(&rescan_epoch);
                            let wm = Self::current_watch_manager(&rescan_watch_slot);
                            let manual_full_audit = matches!(reason, RescanReason::Manual);
                            let scan_sender = rescan_scan_tx.clone();
                            let done_tx = audit_done_tx.clone();
                            tokio::spawn(async move {
                                let scan_result = tokio::task::spawn_blocking(move || {
                                    let mut c = lock_or_recover(&cache, "source.root.rescan.mtime_cache");
                                    if manual_full_audit {
                                        c.clear();
                                        scanner.reset_audit_caches_for_manual_rescan();
                                    }
                                    let mut ep = lock_or_recover(&epoch, "source.root.rescan.epoch_counter");
                                    let current_epoch = *ep;
                                    *ep += 1;
                                    scanner.scan_audit_streaming(current_epoch, &mut c, &de, &clk, wm, |batch| {
                                        scan_sender.blocking_send(batch).is_ok()
                                    })
                                }).await;
                                let result = scan_result.map_err(|err| format!("rescan join failed: {err}"));
                                let _ = done_tx.send(RootAuditCompletion {
                                    kind: RootAuditKind::Rescan,
                                    started_at_us: audit_started_at_us,
                                    reason_label,
                                    manual_requested_target,
                                    result,
                                }).await;
                            });
                            audit_inflight = true;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            Self::close_rescan_channels(
                                &mut rescan_channel_open,
                                &mut periodic_channel_open,
                            );
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    }
                }
                if !scan_channel_open && !watch_channel_open && !rescan_channel_open {
                    break;
                }
            }
            if let Some(handle) = watch_handle {
                drop(handle);
            }
        };
        Ok(Box::pin(output_stream))
    }

    /// Domain-specific targeted scan query.
    ///
    /// This is a domain helper, not part of `RuntimeBoundaryApp`. Tests and
    /// RPC request handlers use this to query the source.
    pub fn query_scan(&self, request: &LiveScanRequest) -> Result<Vec<Event>> {
        let path = if request.path.is_empty() {
            b"/".to_vec()
        } else {
            request.path.clone()
        };
        let roots = lock_or_recover(&self.state_cell.roots, "source.req.roots");
        let logical_fallback = !target_matches_any_object_monitor_prefix(&path, &roots);
        let mut merged = Vec::new();
        for root in roots.iter() {
            if !root.active {
                continue;
            }
            let path_for_root =
                map_target_path_to_root(&path, &root.monitor_path, logical_fallback);
            let Some(path_for_root) = path_for_root else {
                continue;
            };
            let mut events = root.scanner.scan_targeted(
                &path_for_root,
                request.recursive,
                request.max_depth,
                &self.drift_estimator,
                &self.logical_clock,
            )?;
            merged.append(&mut events);
        }
        Ok(merged)
    }

    pub(crate) fn perform_force_find(&self, params: &InternalQueryRequest) -> Result<Vec<Event>> {
        if params.transport != QueryTransport::ForceFind {
            return Err(CnxError::InvalidInput(
                "force_find requires force-find transport".into(),
            ));
        }
        let mut attempted_roots = 0usize;
        let mut failed_roots = Vec::<(String, String)>::new();
        let roots_snapshot =
            lock_or_recover(&self.state_cell.roots, "source.force_find.roots").clone();
        let logical_fallback =
            !target_matches_any_object_monitor_prefix(&params.scope.path, &roots_snapshot);
        let mut grouped = BTreeMap::<String, Vec<(RootRuntime, Vec<u8>)>>::new();
        for root in roots_snapshot.into_iter() {
            if let Some(selected_group) = &params.scope.selected_group
                && selected_group != &root.logical_root_id
            {
                continue;
            }
            let path_for_root =
                map_target_path_to_root(&params.scope.path, &root.monitor_path, logical_fallback);
            let Some(path_for_root) = path_for_root else {
                continue;
            };
            if !root.active {
                attempted_roots += 1;
                failed_roots.push((
                    root.object_ref.clone(),
                    "target object is inactive".to_string(),
                ));
                continue;
            }
            grouped
                .entry(root.logical_root_id.clone())
                .or_default()
                .push((root, path_for_root));
        }
        if attempted_roots == 0 && grouped.is_empty() {
            if let Some(target) = &params.scope.selected_group {
                return Err(CnxError::PeerError(format!(
                    "force-find selected_group matched no group: {target}"
                )));
            }
            return Err(CnxError::PeerError(
                "force-find matched no groups".to_string(),
            ));
        }

        let reserved_groups = grouped.keys().cloned().collect::<Vec<_>>();
        {
            let mut inflight = lock_or_recover(
                &self.force_find_inflight,
                "source.force_find.inflight.acquire",
            );
            if let Some(group_id) = reserved_groups
                .iter()
                .find(|group_id| inflight.contains(*group_id))
            {
                return Err(CnxError::NotReady(format!(
                    "force-find already running for group: {group_id}"
                )));
            }
            for group_id in &reserved_groups {
                inflight.insert(group_id.clone());
            }
        }

        let result = (|| {
            let mut merged = Vec::new();
            let mut object_to_group = HashMap::<String, String>::new();
            let mut rr = lock_or_recover(&self.force_find_rr, "source.force_find.rr");
            let mut last_runner = lock_or_recover(
                &self.force_find_last_runner,
                "source.force_find.last_runner",
            );
            for (group_id, candidates) in &grouped {
                let len = candidates.len();
                if len == 0 {
                    continue;
                }
                for (root, _) in candidates {
                    object_to_group.insert(root.object_ref.clone(), group_id.clone());
                }
                let start = rr.get(group_id).copied().unwrap_or(0) % len;
                let mut winner: Option<usize> = None;
                for offset in 0..len {
                    let idx = (start + offset) % len;
                    let (root, path_for_root) = &candidates[idx];
                    attempted_roots += 1;
                    match root.scanner.scan_targeted(
                        path_for_root,
                        params.scope.recursive,
                        params.scope.max_depth,
                        &self.drift_estimator,
                        &self.logical_clock,
                    ) {
                        Ok(mut events) => {
                            merged.append(&mut events);
                            winner = Some(idx);
                            break;
                        }
                        Err(err) => {
                            log::warn!(
                                "force-find scan failed for object {}: {:?}",
                                root.object_ref,
                                err
                            );
                            failed_roots.push((root.object_ref.clone(), err.to_string()));
                        }
                    }
                }
                if let Some(idx) = winner {
                    rr.insert(group_id.clone(), (idx + 1) % len);
                    eprintln!(
                        "fs_meta_source: force-find winner group={} runner={} next_index={}",
                        group_id,
                        candidates[idx].0.object_ref,
                        (idx + 1) % len
                    );
                    last_runner.insert(group_id.clone(), candidates[idx].0.object_ref.clone());
                }
            }
            if merged.is_empty() && attempted_roots > 0 && !failed_roots.is_empty() {
                let details = failed_roots
                    .iter()
                    .take(4)
                    .map(|(root, err)| format!("{root}: {err}"))
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(CnxError::PeerError(format!(
                    "force-find failed on all targeted roots ({attempted_roots}): {details}"
                )));
            }
            encode_force_find_grouped_events(
                &merged,
                &params.scope.path,
                params.scope.recursive,
                params.scope.max_depth,
                params.op,
                &object_to_group,
            )
        })();

        lock_or_recover(
            &self.force_find_inflight,
            "source.force_find.inflight.release",
        )
        .retain(|group_id| !reserved_groups.iter().any(|reserved| reserved == group_id));

        result
    }

    /// Force-find query path used by projection API.
    ///
    /// Unlike `req`, this path supports per-object targeting and degrades
    /// gracefully when one object scan fails.
    pub fn force_find(&self, params: &InternalQueryRequest) -> Result<Vec<Event>> {
        self.perform_force_find(params)
    }
}

impl FSMetaSource {
    pub async fn send(&self, _events: &[Event]) -> Result<()> {
        Err(CnxError::NotSupported(
            "source-fs-meta: send not supported".into(),
        ))
    }

    pub async fn recv(&self, _opts: RecvOpts) -> Result<Vec<Event>> {
        self.query_scan(&crate::query::request::LiveScanRequest::default())
    }

    pub(crate) async fn apply_control_frame_signals(
        &self,
        signals: &[SourceControlSignal],
    ) -> Result<()> {
        self.apply_orchestration_signals(signals).await?;
        self.record_retained_control_signals(signals).await;
        *lock_or_recover(
            &self.last_control_frame_signals,
            "source.on_control_frame.last_control_frame_signals",
        ) = summarize_source_control_signals(signals);
        Ok(())
    }

    pub async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        let signals = source_control_signals_from_envelopes(envelopes)?;
        self.apply_control_frame_signals(&signals).await
    }

    pub(crate) async fn perform_close(&self) -> Result<()> {
        self.shutdown.cancel();
        let mut tasks = std::mem::take(&mut *lock_or_recover(
            &self.state_cell.root_tasks,
            "source.close.root_tasks",
        ));
        for (_key, entry) in tasks.drain() {
            entry
                .active
                .handle
                .cancel_and_join(Duration::from_secs(2))
                .await;
            if let Some(candidate) = entry.candidate {
                candidate
                    .handle
                    .cancel_and_join(Duration::from_secs(2))
                    .await;
            }
        }
        *lock_or_recover(
            &self.state_cell.stream_binding,
            "source.close.stream_binding",
        ) = None;
        let mut endpoint_tasks = std::mem::take(&mut *lock_or_recover(
            &self.endpoint_tasks,
            "source.close.endpoints",
        ));
        for task in &mut endpoint_tasks {
            task.shutdown(Duration::from_secs(2)).await;
        }
        if let Some(task) = lock_or_recover(
            &self.manual_rescan_watch_task,
            "source.close.manual_rescan_watch",
        )
        .take()
        {
            task.abort();
        }
        *lock_or_recover(&self.state, "source.close.state") = LifecycleState::Closed;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.perform_close().await
    }
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

fn source_control_signal_route_identity(signal: &SourceControlSignal) -> Option<(&str, &str)> {
    match signal {
        SourceControlSignal::Activate {
            unit, route_key, ..
        }
        | SourceControlSignal::Deactivate {
            unit, route_key, ..
        }
        | SourceControlSignal::Tick {
            unit, route_key, ..
        } => Some((unit.unit_id(), route_key.as_str())),
        SourceControlSignal::RuntimeHostGrantChange { .. }
        | SourceControlSignal::ManualRescan { .. }
        | SourceControlSignal::Passthrough(_) => None,
    }
}

fn source_control_summary_entry_matches_route(entry: &str, unit_id: &str, route_key: &str) -> bool {
    entry.contains(&format!("unit={unit_id}")) && entry.contains(&format!("route={route_key}"))
}

fn apply_retained_control_summary(summary: &mut Vec<String>, signals: &[SourceControlSignal]) {
    for signal in signals {
        let Some((unit_id, route_key)) = source_control_signal_route_identity(signal) else {
            continue;
        };
        summary
            .retain(|entry| !source_control_summary_entry_matches_route(entry, unit_id, route_key));
    }
    for entry in summarize_source_control_signals(signals) {
        if !summary.iter().any(|existing| existing == &entry) {
            summary.push(entry);
        }
    }
}

#[cfg(test)]
mod tests;
