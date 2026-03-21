//! File metadata source app for Capanix.
//!
//! Monitors a directory tree via host watch backends with LRU watch scheduling,
//! emits drift-compensated file metadata events.

pub mod config;
pub(crate) mod drift;
pub(crate) mod scanner;
pub(crate) mod sentinel;
pub(crate) mod watcher;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_stream::stream;

use futures_core::Stream;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use capanix_app_sdk::runtime::{
    ControlEnvelope, EventMetadata, NodeId, RecvOpts, in_memory_state_boundary,
};
use capanix_app_sdk::{CnxError, Event, Result};
use capanix_host_adapter_fs::{HostFs, HostFsFacade};
use capanix_runtime_host_sdk::boundary::{ChannelIoSubset, StateBoundary};
use capanix_runtime_host_sdk::control::{
    BoundScope, HostObjectGrantState, RuntimeHostObjectGrantsChanged,
};

use crate::LogicalClock;
use crate::query::path::{path_buf_from_bytes, path_to_bytes};
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
    SourceControlSignal, SourceRuntimeUnit, decode_logical_roots_control_payload,
    source_control_signals_from_envelopes,
};
use crate::runtime::routes::{
    METHOD_SOURCE_FIND, METHOD_SOURCE_RESCAN, METHOD_SOURCE_RESCAN_CONTROL,
    METHOD_SOURCE_ROOTS_CONTROL, ROUTE_TOKEN_FS_META_INTERNAL, default_route_bindings,
    source_rescan_route_key_for,
};
use crate::runtime::unit_gate::RuntimeUnitGate;
use crate::source::config::{GrantedMountRoot, RootSpec, SourceConfig};
use crate::source::drift::DriftEstimator;
use crate::source::scanner::ParallelScanner;
use crate::source::sentinel::{HealthSignal, Sentinel, SentinelAction, SentinelConfig};
use crate::source::watcher::WatchManager;
use crate::state::cell::{AuthorityJournal, SignalCell};
use crate::state::commit_boundary::CommitBoundary;

#[cfg(test)]
use crate::runtime::routes::ROUTE_KEY_QUERY;

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_micros() as u64,
        Err(_) => 0,
    }
}

const EPOCH0_SCAN_DELAY_ENV: &str = "FS_META_EPOCH0_SCAN_DELAY_MS";
const EPOCH0_SCAN_DELAY_DEFAULT_MS: u64 = 10_000;
const EPOCH0_SCAN_DELAY_MAX_MS: u64 = 300_000;
const RESCAN_READY_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const RESCAN_READY_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MANUAL_RESCAN_SIGNAL_NAME: &str = "manual_rescan";
const MANUAL_RESCAN_WATCH_POLL_INTERVAL: Duration = Duration::from_millis(250);

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

fn build_error_marker_event(node_id: &NodeId, correlation_id: Option<u64>, message: &str) -> Event {
    Event::new(
        EventMetadata {
            origin_id: node_id.clone(),
            timestamp_us: now_us(),
            logical_ts: None,
            correlation_id,
            ingress_auth: None,
            trace: None,
        },
        bytes::Bytes::copy_from_slice(message.as_bytes()),
    )
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
    match (watch_enabled && watch_lru_capacity > 0, scan_enabled) {
        (true, true) => "realtime_hotset_plus_audit",
        (true, false) => "realtime_hotset_only",
        (false, true) => "audit_only",
        (false, false) if watch_enabled => "watch_requested_without_capacity",
        (false, false) => "inactive",
    }
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
    pub last_rescan_reason: Option<String>,
    pub last_error: Option<String>,
    pub last_audit_started_at_us: Option<u64>,
    pub last_audit_completed_at_us: Option<u64>,
    pub last_audit_duration_ms: Option<u64>,
    pub current_revision: Option<u64>,
    pub candidate_revision: Option<u64>,
    pub candidate_status: Option<String>,
    pub draining_revision: Option<u64>,
    pub draining_status: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SourceStatusSnapshot {
    pub logical_roots: Vec<SourceLogicalRootHealthSnapshot>,
    pub concrete_roots: Vec<SourceConcreteRootHealthSnapshot>,
    pub degraded_roots: Vec<(String, String)>,
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
    /// Coalesced runtime-topology refresh trigger.
    runtime_refresh_dirty: Arc<AtomicBool>,
    /// Pending rescan request coupled to topology refresh.
    runtime_refresh_rescan: Arc<AtomicBool>,
    /// Single-flight guard for background runtime refresh.
    runtime_refresh_running: Arc<AtomicBool>,
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

#[derive(Debug, Clone, Copy, Default)]
struct ManualRescanIntent {
    requested: u64,
    completed: u64,
}

struct RootTaskHandle {
    cancel: CancellationToken,
    join: JoinHandle<()>,
    ready_rx: tokio::sync::watch::Receiver<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RootTaskSignature {
    logical_root_id: String,
    object_ref: String,
    monitor_path: std::path::PathBuf,
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
    signature: RootTaskSignature,
    handle: RootTaskHandle,
}

struct RootTaskEntry {
    active: RootTaskSlot,
    candidate: Option<RootTaskSlot>,
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
    last_rescan_reason: Option<String>,
    last_error: Option<String>,
    last_audit_started_at_us: Option<u64>,
    last_audit_completed_at_us: Option<u64>,
    last_audit_duration_ms: Option<u64>,
    current_revision: Option<u64>,
    candidate_revision: Option<u64>,
    candidate_status: Option<String>,
    draining_revision: Option<u64>,
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
            last_rescan_reason: None,
            last_error: None,
            last_audit_started_at_us: None,
            last_audit_completed_at_us: None,
            last_audit_duration_ms: None,
            current_revision: None,
            candidate_revision: None,
            candidate_status: None,
            draining_revision: None,
            draining_status: None,
        }
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
    roots: Arc<Mutex<Vec<RootRuntime>>>,
    root_tasks: Arc<Mutex<HashMap<String, RootTaskEntry>>>,
    stream_tx: Arc<Mutex<Option<mpsc::UnboundedSender<Vec<Event>>>>>,
    manual_rescan_intents: Arc<Mutex<HashMap<String, ManualRescanIntent>>>,
    logical_root_fanout: Arc<Mutex<HashMap<String, Vec<GrantedMountRoot>>>>,
    fanout_health: Arc<Mutex<FanoutHealthState>>,
    host_object_grants: Arc<Mutex<Vec<GrantedMountRoot>>>,
    root_task_revision: Arc<AtomicU64>,
    commit_boundary: CommitBoundary,
}

impl SourceStateCell {
    fn new(
        logical_roots: Vec<RootSpec>,
        roots: Vec<RootRuntime>,
        logical_root_fanout: HashMap<String, Vec<GrantedMountRoot>>,
        host_object_grants: Vec<GrantedMountRoot>,
        fanout_health: Arc<Mutex<FanoutHealthState>>,
        commit_boundary: CommitBoundary,
    ) -> Self {
        let cell = Self {
            logical_roots: Arc::new(Mutex::new(logical_roots)),
            roots: Arc::new(Mutex::new(roots)),
            root_tasks: Arc::new(Mutex::new(HashMap::new())),
            stream_tx: Arc::new(Mutex::new(None)),
            manual_rescan_intents: Arc::new(Mutex::new(HashMap::new())),
            logical_root_fanout: Arc::new(Mutex::new(logical_root_fanout)),
            fanout_health,
            host_object_grants: Arc::new(Mutex::new(host_object_grants)),
            root_task_revision: Arc::new(AtomicU64::new(1)),
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

    #[cfg(test)]
    fn authority_log_len(&self) -> usize {
        self.commit_boundary.len()
    }
}

impl FSMetaSource {
    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    pub fn channel_boundary(&self) -> Option<Arc<dyn ChannelIoSubset>> {
        self.boundary.clone()
    }

    fn apply_activate_signal(
        &self,
        unit: SourceRuntimeUnit,
        route_key: &str,
        generation: u64,
        bound_scopes: &[BoundScope],
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

    pub(crate) async fn apply_orchestration_signals(
        &self,
        signals: &[SourceControlSignal],
    ) -> Result<()> {
        enum PendingAction {
            UpdateHostObjectGrants(RuntimeHostObjectGrantsChanged),
        }
        let mut actions = Vec::new();
        let mut refresh_runtime_topology = false;
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
                    // Unit tick is runtime-owned scheduling signal.
                    // Source accepts and validates the envelope kind while keeping
                    // business data path independent.
                    self.accept_tick_signal(*unit, route_key, *generation)?;
                }
                SourceControlSignal::RuntimeHostObjectGrantsChanged { changed, .. } => {
                    actions.push(PendingAction::UpdateHostObjectGrants(changed.clone()));
                }
                SourceControlSignal::ManualRescan { .. } => {
                    let roots_snapshot = lock_or_recover(
                        &self.state_cell.roots,
                        "source.control.manual_rescan.roots",
                    )
                    .clone();
                    FSMetaSource::request_rescan_on_primary_roots(
                        &roots_snapshot,
                        Some(&self.state_cell.fanout_health),
                        Some(&self.state_cell.manual_rescan_intents),
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
            actions.len(),
            refresh_runtime_topology,
            self.scheduled_source_group_ids()?,
            self.scheduled_scan_group_ids()?
        );
        for action in actions {
            match action {
                PendingAction::UpdateHostObjectGrants(changed) => {
                    let current = self.host_object_grants_version.load(Ordering::Relaxed);
                    if changed.version <= current {
                        continue;
                    }
                    self.host_object_grants_version
                        .store(changed.version, Ordering::Relaxed);
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
                            active: matches!(row.grant_state, HostObjectGrantState::Active),
                        })
                        .collect::<Vec<_>>();
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
                    refresh_runtime_topology = true;
                }
            }
        }
        if refresh_runtime_topology {
            self.schedule_runtime_refresh(true);
        }
        Ok(())
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
        let groups = match self.unit_control.unit_state(unit.unit_id())? {
            Some((true, rows)) => rows
                .into_iter()
                .map(|row| row.scope_id)
                .collect::<BTreeSet<_>>(),
            Some((false, _)) | None => BTreeSet::new(),
        };
        Ok(Some(groups))
    }

    pub fn scheduled_source_group_ids(&self) -> Result<Option<BTreeSet<String>>> {
        self.scheduled_group_ids(SourceRuntimeUnit::Source)
    }

    pub fn scheduled_scan_group_ids(&self) -> Result<Option<BTreeSet<String>>> {
        self.scheduled_group_ids(SourceRuntimeUnit::Scan)
    }

    async fn refresh_runtime_roots(&self, trigger_rescan: bool) -> Result<()> {
        let root_specs = self.logical_roots_snapshot();
        let host_object_grants = self.host_object_grants_snapshot();
        let source_groups = self.scheduled_group_ids(SourceRuntimeUnit::Source)?;
        let scan_groups = self.scheduled_group_ids(SourceRuntimeUnit::Scan)?;
        let desired = Self::build_root_runtimes(
            &self.config,
            &self.node_id,
            self.boundary.clone(),
            &root_specs,
            &host_object_grants,
            source_groups.as_ref(),
            scan_groups.as_ref(),
        );
        let current_signature = {
            let current = lock_or_recover(
                &self.state_cell.roots,
                "source.refresh_runtime_roots.current",
            )
            .clone();
            Self::runtime_topology_signature(&current)
        };
        let desired_signature = Self::runtime_topology_signature(&desired);
        let topology_changed = desired_signature != current_signature;
        Self::sync_object_runtime_health(&self.state_cell.fanout_health, &desired, &self.config);
        *lock_or_recover(&self.state_cell.roots, "source.refresh_runtime_roots") = desired;
        if topology_changed {
            let desired_roots =
                lock_or_recover(&self.state_cell.roots, "source.refresh_runtime_roots.read")
                    .clone();
            self.reconcile_root_tasks(&desired_roots).await;
        }
        if trigger_rescan && topology_changed {
            self.trigger_rescan_when_ready().await;
        }
        Ok(())
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
        let group_primary =
            Self::compute_group_primary_by_logical_root(roots, host_object_grants, scan_groups);
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
                .filter(|grant| grant.host_ref == node_id.0 && root.selector.matches(grant))
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
                let emit_prefix = root.subpath_scope.clone();
                let host_fs = match resolve_host_fs_facade(
                    monitor_path.clone(),
                    boundary.clone(),
                    node_id,
                    &member.host_ref,
                    &member.object_ref,
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
        scan_groups: Option<&BTreeSet<String>>,
    ) -> HashMap<String, String> {
        let mut primary = HashMap::new();
        for root in roots {
            if scan_groups.is_some_and(|groups| !groups.contains(&root.id)) {
                continue;
            }
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
            watch: root.spec.watch,
            scan: root.spec.scan,
            audit_interval_ms: root.spec.audit_interval_ms,
        }
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

    fn root_current_is_group_primary(
        roots_handle: &Arc<Mutex<Vec<RootRuntime>>>,
        root_key: &str,
    ) -> bool {
        lock_or_recover(roots_handle, "source.root.current_primary")
            .iter()
            .find(|root| Self::root_runtime_key(root) == root_key)
            .is_some_and(|root| root.active && root.is_group_primary)
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
        retired: bool,
    ) {
        Self::mark_root_task_draining(fanout_health, root_key, slot.revision, "draining");
        slot.handle.cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), slot.handle.join).await;
        Self::finish_root_task_draining(fanout_health, root_key, slot.revision, retired);
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
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_default();
        detail.status = status.clone();
        if status == "running" {
            detail.last_error = None;
        } else if let Some(reason) = status.split_once(": ").map(|(_, reason)| reason) {
            detail.last_error = Some(reason.to_string());
        }
    }

    fn update_root_task_slot_health(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        revision: u64,
        role: RootTaskRole,
        status: impl Into<String>,
    ) {
        let status = status.into();
        let mut health = lock_or_recover(fanout_health, "source.object_health.slot");
        match role {
            RootTaskRole::Active => {
                health
                    .object_ref
                    .insert(root_key.to_string(), status.clone());
                let detail = health
                    .object_ref_detail
                    .entry(root_key.to_string())
                    .or_default();
                detail.status = status.clone();
                detail.current_revision = Some(revision);
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
                    .or_default();
                detail.candidate_revision = Some(revision);
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
        status: impl Into<String>,
    ) {
        let status = status.into();
        let mut health = lock_or_recover(fanout_health, "source.object_health.draining");
        let current_matches = health
            .object_ref_detail
            .get(root_key)
            .is_some_and(|detail| detail.current_revision == Some(revision));
        if current_matches {
            health
                .object_ref
                .insert(root_key.to_string(), status.clone());
        }
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_default();
        detail.draining_revision = Some(revision);
        detail.draining_status = Some(status.clone());
        if current_matches {
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
            detail.candidate_status = None;
        }
    }

    fn fail_root_task_candidate_health(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        revision: u64,
        status: impl Into<String>,
    ) {
        let status = status.into();
        let mut health = lock_or_recover(fanout_health, "source.object_health.candidate_fail");
        let detail = health
            .object_ref_detail
            .entry(root_key.to_string())
            .or_default();
        detail.candidate_revision = Some(revision);
        detail.candidate_status = Some(status.clone());
        if let Some(reason) = status.split_once(": ").map(|(_, reason)| reason) {
            detail.last_error = Some(reason.to_string());
        }
    }

    fn promote_root_task_candidate_health(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        revision: u64,
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
            .or_default();
        detail.current_revision = Some(revision);
        detail.candidate_revision = None;
        detail.candidate_status = None;
        detail.status = promoted_status;
        detail.last_error = None;
    }

    fn finish_root_task_draining(
        fanout_health: &Arc<Mutex<FanoutHealthState>>,
        root_key: &str,
        revision: u64,
        retired: bool,
    ) {
        let mut health = lock_or_recover(fanout_health, "source.object_health.draining_finish");
        if let Some(detail) = health.object_ref_detail.get_mut(root_key) {
            detail.draining_revision = Some(revision);
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
        detail.last_rescan_reason = Some(reason.to_string());
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

    fn request_rescan_on_primary_roots(
        roots: &[RootRuntime],
        fanout_health: Option<&Arc<Mutex<FanoutHealthState>>>,
        manual_rescan_intents: Option<&Arc<Mutex<HashMap<String, ManualRescanIntent>>>>,
        reason: &str,
    ) {
        for root in roots {
            if !root.is_group_primary {
                continue;
            }
            let root_key = Self::root_runtime_key(root);
            if let Some(health) = fanout_health {
                Self::mark_root_rescan_requested(health, &root_key, reason);
            }
            if reason == "manual"
                && let Some(intents) = manual_rescan_intents
            {
                let mut intents = lock_or_recover(intents, "source.manual_rescan_intents.queue");
                let entry = intents.entry(root_key).or_default();
                let should_signal = entry.requested <= entry.completed;
                entry.requested = entry.requested.saturating_add(1);
                if should_signal {
                    let _ = root.rescan_tx.send(RescanReason::Manual);
                }
            } else {
                let _ = root.rescan_tx.send(RescanReason::Manual);
            }
        }
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
        Self::with_boundaries_and_state_inner(config, node_id, boundary, state_boundary, false)
    }

    fn with_boundaries_and_state_inner(
        config: SourceConfig,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        state_boundary: Arc<dyn StateBoundary>,
        _defer_authority_read: bool,
    ) -> Result<Self> {
        let initial_host_object_grants = config.host_object_grants.clone();
        let root_specs = config.effective_roots().map_err(CnxError::InvalidInput)?;
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
        let authority =
            AuthorityJournal::from_state_boundary(SOURCE_RUNTIME_UNIT_ID, state_boundary.clone())
                .map_err(|err| {
                CnxError::InvalidInput(format!("source statecell authority init failed: {err}"))
            })?;
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
                roots,
                logical_root_fanout,
                initial_host_object_grants,
                fanout_health,
                commit_boundary,
            ),
            manual_rescan_signal,
            manual_rescan_watch_task: Arc::new(Mutex::new(None)),
            host_object_grants_version: Arc::new(AtomicU64::new(0)),
            unit_control,
            endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
            sentinel,
            force_find_rr: Arc::new(Mutex::new(HashMap::new())),
            force_find_inflight: Arc::new(Mutex::new(HashSet::new())),
            force_find_last_runner: Arc::new(Mutex::new(BTreeMap::new())),
            runtime_refresh_dirty: Arc::new(AtomicBool::new(false)),
            runtime_refresh_rescan: Arc::new(AtomicBool::new(false)),
            runtime_refresh_running: Arc::new(AtomicBool::new(false)),
        };

        if let Some(sys) = boundary {
            let rescan_roots = source.state_cell.roots_handle();
            let rescan_fanout_health = source.state_cell.fanout_health_handle();
            let rescan_manual_intents = source.state_cell.manual_rescan_intents_handle();
            let node_id_cloned = node_id.clone();
            let node_id_rescan = node_id.clone();
            let node_id_rescan_scoped = node_id.clone();
            let rescan_roots_scoped = rescan_roots.clone();
            let rescan_fanout_health_scoped = rescan_fanout_health.clone();
            let rescan_manual_intents_scoped = rescan_manual_intents.clone();
            let source_for_find = source.clone();
            let routes = default_route_bindings();
            match routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND) {
                Ok(route) => {
                    log::info!(
                        "bound route listening on {}.{} for source {}",
                        ROUTE_TOKEN_FS_META_INTERNAL,
                        METHOD_SOURCE_FIND,
                        node_id_cloned.0
                    );
                    let endpoint_task = ManagedEndpointTask::spawn(
                        sys.clone(),
                        route,
                        format!(
                            "source:{}:{}",
                            ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND
                        ),
                        source.shutdown.clone(),
                        move |requests| {
                            let mut responses = Vec::new();
                            for req in requests {
                                if let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                    req.payload_bytes(),
                                ) {
                                    if params.transport != QueryTransport::ForceFind {
                                        log::warn!(
                                            "boundary endpoint rejected non-force-find transport"
                                        );
                                        responses.push(build_error_marker_event(
                                            &node_id_cloned,
                                            req.metadata().correlation_id,
                                            "force-find source invalid request transport",
                                        ));
                                        continue;
                                    }
                                    eprintln!(
                                        "fs_meta_source: route force-find request correlation={:?} selected_group={:?} path={:?} recursive={}",
                                        req.metadata().correlation_id,
                                        params.scope.selected_group,
                                        params.scope.path,
                                        params.scope.recursive
                                    );
                                    match source_for_find.force_find(&params) {
                                        Ok(grouped_events) => {
                                            eprintln!(
                                                "fs_meta_source: route force-find response correlation={:?} groups_events={}",
                                                req.metadata().correlation_id,
                                                grouped_events.len()
                                            );
                                            for event in grouped_events {
                                                let mut meta = event.metadata().clone();
                                                meta.correlation_id = req.metadata().correlation_id;
                                                responses.push(Event::new(
                                                    meta,
                                                    bytes::Bytes::copy_from_slice(
                                                        event.payload_bytes(),
                                                    ),
                                                ));
                                            }
                                        }
                                        Err(err) => {
                                            log::error!(
                                                "boundary endpoint force-find failed: {:?}",
                                                err
                                            );
                                            responses.push(build_error_marker_event(
                                                &node_id_cloned,
                                                req.metadata().correlation_id,
                                                &err.to_string(),
                                            ));
                                        }
                                    }
                                } else {
                                    log::warn!(
                                        "boundary endpoint failed to parse InternalQueryRequest"
                                    );
                                    responses.push(build_error_marker_event(
                                        &node_id_cloned,
                                        req.metadata().correlation_id,
                                        "force-find source invalid request payload",
                                    ));
                                }
                            }
                            responses
                        },
                    );
                    lock_or_recover(
                        &source.endpoint_tasks,
                        "source.with_boundaries.endpoint_tasks",
                    )
                    .push(endpoint_task);
                }
                Err(err) => {
                    log::error!(
                        "failed to resolve source bound route {}.{}: {:?}",
                        ROUTE_TOKEN_FS_META_INTERNAL,
                        METHOD_SOURCE_FIND,
                        err
                    );
                }
            }

            match routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN) {
                Ok(route) => {
                    log::info!(
                        "bound route listening on {}.{} for source {}",
                        ROUTE_TOKEN_FS_META_INTERNAL,
                        METHOD_SOURCE_RESCAN,
                        node_id_rescan.0
                    );
                    let endpoint_task = ManagedEndpointTask::spawn(
                        sys.clone(),
                        route,
                        format!(
                            "source:{}:{}",
                            ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN
                        ),
                        source.shutdown.clone(),
                        move |requests| {
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
                            if !expected.is_empty() {
                                let deadline =
                                    std::time::Instant::now() + RESCAN_READY_WAIT_TIMEOUT;
                                loop {
                                    let ready = {
                                        let health = lock_or_recover(
                                            &rescan_fanout_health,
                                            "source.rescan.endpoint.health",
                                        );
                                        expected.iter().all(|root_key| {
                                            health
                                                .object_ref
                                                .get(root_key)
                                                .is_some_and(|status| status == "running")
                                        })
                                    };
                                    if ready || std::time::Instant::now() >= deadline {
                                        break;
                                    }
                                    std::thread::sleep(RESCAN_READY_POLL_INTERVAL);
                                }
                            }

                            let roots_snapshot = lock_or_recover(
                                &rescan_roots,
                                "source.rescan.endpoint.roots.trigger",
                            )
                            .clone();
                            FSMetaSource::request_rescan_on_primary_roots(
                                &roots_snapshot,
                                Some(&rescan_fanout_health),
                                Some(&rescan_manual_intents),
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
                                    Event::new(
                                        EventMetadata {
                                            origin_id: node_id_rescan.clone(),
                                            timestamp_us: now_us(),
                                            logical_ts: None,
                                            correlation_id: req.metadata().correlation_id,
                                            ingress_auth: None,
                                            trace: None,
                                        },
                                        bytes::Bytes::from_static(b"accepted"),
                                    )
                                })
                                .collect()
                        },
                    );
                    lock_or_recover(
                        &source.endpoint_tasks,
                        "source.with_boundaries.endpoint_tasks",
                    )
                    .push(endpoint_task);
                }
                Err(err) => {
                    log::error!(
                        "failed to resolve source bound route {}.{}: {:?}",
                        ROUTE_TOKEN_FS_META_INTERNAL,
                        METHOD_SOURCE_RESCAN,
                        err
                    );
                }
            }

            {
                let route = capanix_app_sdk::runtime::RouteKey(source_rescan_route_key_for(
                    &node_id_rescan_scoped.0,
                ));
                log::info!(
                    "bound route listening on {} for source {}",
                    route.0,
                    node_id_rescan_scoped.0
                );
                let endpoint_task = ManagedEndpointTask::spawn(
                    sys,
                    route.clone(),
                    format!("source:{}", route.0),
                    source.shutdown.clone(),
                    move |requests| {
                        eprintln!(
                            "fs_meta_source: source.rescan scoped endpoint start node={} requests={}",
                            node_id_rescan_scoped.0,
                            requests.len()
                        );
                        let expected = lock_or_recover(
                            &rescan_roots_scoped,
                            "source.rescan.scoped_endpoint.roots.expected",
                        )
                        .iter()
                        .filter(|root| root.is_group_primary && root.spec.scan)
                        .map(FSMetaSource::root_runtime_key)
                        .collect::<Vec<_>>();
                        if !expected.is_empty() {
                            let deadline = std::time::Instant::now() + RESCAN_READY_WAIT_TIMEOUT;
                            loop {
                                let ready = {
                                    let health = lock_or_recover(
                                        &rescan_fanout_health_scoped,
                                        "source.rescan.scoped_endpoint.health",
                                    );
                                    expected.iter().all(|root_key| {
                                        health
                                            .object_ref
                                            .get(root_key)
                                            .is_some_and(|status| status == "running")
                                    })
                                };
                                if ready || std::time::Instant::now() >= deadline {
                                    break;
                                }
                                std::thread::sleep(RESCAN_READY_POLL_INTERVAL);
                            }
                        }

                        let roots_snapshot = lock_or_recover(
                            &rescan_roots_scoped,
                            "source.rescan.scoped_endpoint.roots.trigger",
                        )
                        .clone();
                        FSMetaSource::request_rescan_on_primary_roots(
                            &roots_snapshot,
                            Some(&rescan_fanout_health_scoped),
                            Some(&rescan_manual_intents_scoped),
                            "manual",
                        );
                        eprintln!(
                            "fs_meta_source: source.rescan scoped endpoint triggered node={} expected_roots={}",
                            node_id_rescan_scoped.0,
                            expected.len()
                        );

                        requests
                            .into_iter()
                            .map(|req| {
                                Event::new(
                                    EventMetadata {
                                        origin_id: node_id_rescan_scoped.clone(),
                                        timestamp_us: now_us(),
                                        logical_ts: None,
                                        correlation_id: req.metadata().correlation_id,
                                        ingress_auth: None,
                                        trace: None,
                                    },
                                    bytes::Bytes::from_static(b"accepted"),
                                )
                            })
                            .collect()
                    },
                );
                lock_or_recover(
                    &source.endpoint_tasks,
                    "source.with_boundaries.endpoint_tasks",
                )
                .push(endpoint_task);
            }
        }

        Ok(source)
    }

    pub fn start_runtime_endpoints(&self, boundary: Arc<dyn ChannelIoSubset>) -> Result<()> {
        if !lock_or_recover(&self.endpoint_tasks, "source.start_runtime_endpoints").is_empty() {
            return Ok(());
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(self.start_manual_rescan_watch()))?;
        }

        let rescan_roots = self.state_cell.roots_handle();
        let rescan_fanout_health = self.state_cell.fanout_health_handle();
        let rescan_manual_intents = self.state_cell.manual_rescan_intents_handle();
        let node_id_cloned = self.node_id.clone();
        let node_id_rescan = self.node_id.clone();
        let node_id_rescan_scoped = self.node_id.clone();
        let rescan_roots_scoped = rescan_roots.clone();
        let rescan_fanout_health_scoped = rescan_fanout_health.clone();
        let rescan_manual_intents_scoped = rescan_manual_intents.clone();
        let source_for_find = self.clone();
        let routes = default_route_bindings();

        match routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND) {
            Ok(route) => {
                log::info!(
                    "bound route listening on {}.{} for deferred source {}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_FIND,
                    node_id_cloned.0
                );
                let endpoint_task = ManagedEndpointTask::spawn(
                    boundary.clone(),
                    route,
                    format!(
                        "source:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND
                    ),
                    self.shutdown.clone(),
                    move |requests| {
                        let mut responses = Vec::new();
                        for req in requests {
                            if let Ok(params) =
                                rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                            {
                                if params.transport != QueryTransport::ForceFind {
                                    log::warn!(
                                        "boundary endpoint rejected non-force-find transport"
                                    );
                                    responses.push(build_error_marker_event(
                                        &node_id_cloned,
                                        req.metadata().correlation_id,
                                        "force-find source invalid request transport",
                                    ));
                                    continue;
                                }
                                eprintln!(
                                    "fs_meta_source: route force-find request correlation={:?} selected_group={:?} path={:?} recursive={}",
                                    req.metadata().correlation_id,
                                    params.scope.selected_group,
                                    params.scope.path,
                                    params.scope.recursive
                                );
                                match source_for_find.force_find(&params) {
                                    Ok(grouped_events) => {
                                        eprintln!(
                                            "fs_meta_source: route force-find response correlation={:?} groups_events={}",
                                            req.metadata().correlation_id,
                                            grouped_events.len()
                                        );
                                        for event in grouped_events {
                                            let mut meta = event.metadata().clone();
                                            meta.correlation_id = req.metadata().correlation_id;
                                            responses.push(Event::new(
                                                meta,
                                                bytes::Bytes::copy_from_slice(
                                                    event.payload_bytes(),
                                                ),
                                            ));
                                        }
                                    }
                                    Err(err) => {
                                        log::error!(
                                            "boundary endpoint force-find failed: {:?}",
                                            err
                                        );
                                        responses.push(build_error_marker_event(
                                            &node_id_cloned,
                                            req.metadata().correlation_id,
                                            &err.to_string(),
                                        ));
                                    }
                                }
                            } else {
                                log::warn!(
                                    "boundary endpoint failed to parse InternalQueryRequest"
                                );
                                responses.push(build_error_marker_event(
                                    &node_id_cloned,
                                    req.metadata().correlation_id,
                                    "force-find source invalid request payload",
                                ));
                            }
                        }
                        responses
                    },
                );
                lock_or_recover(&self.endpoint_tasks, "source.start_runtime_endpoints.tasks")
                    .push(endpoint_task);
            }
            Err(err) => {
                log::error!(
                    "failed to resolve deferred source bound route {}.{}: {:?}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_FIND,
                    err
                );
            }
        }

        match routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN) {
            Ok(route) => {
                log::info!(
                    "bound route listening on {}.{} for deferred source {}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_RESCAN,
                    node_id_rescan.0
                );
                let endpoint_task = ManagedEndpointTask::spawn(
                    boundary.clone(),
                    route,
                    format!(
                        "source:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN
                    ),
                    self.shutdown.clone(),
                    move |requests| {
                        eprintln!(
                            "fs_meta_source: source.rescan endpoint start node={} requests={}",
                            node_id_rescan.0,
                            requests.len()
                        );
                        let expected =
                            lock_or_recover(&rescan_roots, "source.rescan.endpoint.roots.expected")
                                .iter()
                                .filter(|root| root.is_group_primary && root.spec.scan)
                                .map(FSMetaSource::root_runtime_key)
                                .collect::<Vec<_>>();
                        if !expected.is_empty() {
                            let deadline = std::time::Instant::now() + RESCAN_READY_WAIT_TIMEOUT;
                            loop {
                                let ready = {
                                    let health = lock_or_recover(
                                        &rescan_fanout_health,
                                        "source.rescan.endpoint.health",
                                    );
                                    expected.iter().all(|root_key| {
                                        health
                                            .object_ref
                                            .get(root_key)
                                            .is_some_and(|status| status == "running")
                                    })
                                };
                                if ready || std::time::Instant::now() >= deadline {
                                    break;
                                }
                                std::thread::sleep(RESCAN_READY_POLL_INTERVAL);
                            }
                        }

                        let roots_snapshot =
                            lock_or_recover(&rescan_roots, "source.rescan.endpoint.roots.trigger")
                                .clone();
                        FSMetaSource::request_rescan_on_primary_roots(
                            &roots_snapshot,
                            Some(&rescan_fanout_health),
                            Some(&rescan_manual_intents),
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
                                Event::new(
                                    EventMetadata {
                                        origin_id: node_id_rescan.clone(),
                                        timestamp_us: now_us(),
                                        logical_ts: None,
                                        correlation_id: req.metadata().correlation_id,
                                        ingress_auth: None,
                                        trace: None,
                                    },
                                    bytes::Bytes::from_static(b"accepted"),
                                )
                            })
                            .collect()
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
        }

        match routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN_CONTROL) {
            Ok(route) => {
                log::info!(
                    "bound stream listening on {}.{} for deferred source {}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_RESCAN_CONTROL,
                    self.node_id.0
                );
                let control_roots = self.state_cell.roots_handle();
                let control_fanout_health = self.state_cell.fanout_health_handle();
                let control_manual_rescan_intents = self.state_cell.manual_rescan_intents_handle();
                let endpoint_task = ManagedEndpointTask::spawn_stream(
                    boundary.clone(),
                    route,
                    format!(
                        "source:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN_CONTROL
                    ),
                    self.shutdown.clone(),
                    move || true,
                    move |events| {
                        let roots_snapshot = lock_or_recover(
                            &control_roots,
                            "source.rescan.control_stream.roots.trigger",
                        )
                        .clone();
                        for _ in 0..events.len() {
                            FSMetaSource::request_rescan_on_primary_roots(
                                &roots_snapshot,
                                Some(&control_fanout_health),
                                Some(&control_manual_rescan_intents),
                                "manual",
                            );
                        }
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
        }

        match routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_ROOTS_CONTROL) {
            Ok(route) => {
                log::info!(
                    "bound stream listening on {}.{} for deferred source {}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_ROOTS_CONTROL,
                    self.node_id.0
                );
                let source = self.clone();
                let control_node_id = self.node_id.clone();
                let endpoint_task = ManagedEndpointTask::spawn_stream(
                    boundary.clone(),
                    route,
                    format!(
                        "source:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_ROOTS_CONTROL
                    ),
                    self.shutdown.clone(),
                    move || true,
                    move |events| {
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
                            if let Err(err) = crate::runtime_app::shared_tokio_runtime()
                                .block_on(source.update_logical_roots(payload.roots))
                            {
                                log::warn!(
                                    "source logical-roots control apply failed on node {}: {:?}",
                                    control_node_id.0,
                                    err
                                );
                            }
                        }
                    },
                );
                lock_or_recover(
                    &self.endpoint_tasks,
                    "source.start_runtime_endpoints.roots_control_stream_tasks",
                )
                .push(endpoint_task);
            }
            Err(err) => {
                log::error!(
                    "failed to resolve deferred source stream route {}.{}: {:?}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_ROOTS_CONTROL,
                    err
                );
            }
        }

        {
            let route = capanix_app_sdk::runtime::RouteKey(source_rescan_route_key_for(
                &node_id_rescan_scoped.0,
            ));
            log::info!(
                "bound route listening on {} for deferred source {}",
                route.0,
                node_id_rescan_scoped.0
            );
            let endpoint_task = ManagedEndpointTask::spawn(
                boundary,
                route.clone(),
                format!("source:{}", route.0),
                self.shutdown.clone(),
                move |requests| {
                    eprintln!(
                        "fs_meta_source: source.rescan scoped endpoint start node={} requests={}",
                        node_id_rescan_scoped.0,
                        requests.len()
                    );
                    let expected = lock_or_recover(
                        &rescan_roots_scoped,
                        "source.rescan.scoped_endpoint.roots.expected",
                    )
                    .iter()
                    .filter(|root| root.is_group_primary && root.spec.scan)
                    .map(FSMetaSource::root_runtime_key)
                    .collect::<Vec<_>>();
                    if !expected.is_empty() {
                        let deadline = std::time::Instant::now() + RESCAN_READY_WAIT_TIMEOUT;
                        loop {
                            let ready = {
                                let health = lock_or_recover(
                                    &rescan_fanout_health_scoped,
                                    "source.rescan.scoped_endpoint.health",
                                );
                                expected.iter().all(|root_key| {
                                    health
                                        .object_ref
                                        .get(root_key)
                                        .is_some_and(|status| status == "running")
                                })
                            };
                            if ready || std::time::Instant::now() >= deadline {
                                break;
                            }
                            std::thread::sleep(RESCAN_READY_POLL_INTERVAL);
                        }
                    }

                    let roots_snapshot = lock_or_recover(
                        &rescan_roots_scoped,
                        "source.rescan.scoped_endpoint.roots.trigger",
                    )
                    .clone();
                    FSMetaSource::request_rescan_on_primary_roots(
                        &roots_snapshot,
                        Some(&rescan_fanout_health_scoped),
                        Some(&rescan_manual_intents_scoped),
                        "manual",
                    );
                    eprintln!(
                        "fs_meta_source: source.rescan scoped endpoint triggered node={} expected_roots={}",
                        node_id_rescan_scoped.0,
                        expected.len()
                    );

                    requests
                        .into_iter()
                        .map(|req| {
                            Event::new(
                                EventMetadata {
                                    origin_id: node_id_rescan_scoped.clone(),
                                    timestamp_us: now_us(),
                                    logical_ts: None,
                                    correlation_id: req.metadata().correlation_id,
                                    ingress_auth: None,
                                    trace: None,
                                },
                                bytes::Bytes::from_static(b"accepted"),
                            )
                        })
                        .collect()
                },
            );
            lock_or_recover(
                &self.endpoint_tasks,
                "source.start_runtime_endpoints.scoped_tasks",
            )
            .push(endpoint_task);
        }

        Ok(())
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
            Some(&self.state_cell.fanout_health),
            Some(&self.state_cell.manual_rescan_intents),
            "manual",
        );
    }

    pub async fn trigger_rescan_when_ready(&self) {
        self.wait_for_group_primary_scan_roots_ready().await;
        self.trigger_rescan();
    }

    /// Trigger an overflow diagnostic signal. Intended for tests simulating IN_Q_OVERFLOW recovery-path.
    #[allow(dead_code)]
    pub fn trigger_overflow_rescan(&self) {
        for root in lock_or_recover(&self.state_cell.roots, "source.trigger_overflow.roots").iter()
        {
            let _ = root.rescan_tx.send(RescanReason::Overflow);
        }
    }

    /// Snapshot currently configured logical roots.
    pub fn logical_roots_snapshot(&self) -> Vec<RootSpec> {
        lock_or_recover(
            &self.state_cell.logical_roots,
            "source.logical_roots.snapshot",
        )
        .clone()
    }

    /// Snapshot current granted mount roots.
    pub fn host_object_grants_snapshot(&self) -> Vec<GrantedMountRoot> {
        lock_or_recover(
            &self.state_cell.host_object_grants,
            "source.host_object_grants.snapshot",
        )
        .clone()
    }

    /// Snapshot current host object grants version.
    pub fn host_object_grants_version_snapshot(&self) -> u64 {
        self.host_object_grants_version.load(Ordering::Relaxed)
    }

    /// Snapshot logical-root fanout mapping.
    pub fn logical_root_fanout_snapshot(&self) -> HashMap<String, Vec<GrantedMountRoot>> {
        lock_or_recover(
            &self.state_cell.logical_root_fanout,
            "source.logical_root_fanout.snapshot",
        )
        .clone()
    }

    pub fn resolve_group_id_for_object_ref(&self, object_ref: &str) -> Option<String> {
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

    pub fn status_snapshot(&self) -> SourceStatusSnapshot {
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
                last_rescan_reason: entry.last_rescan_reason.clone(),
                last_error: entry.last_error.clone(),
                last_audit_started_at_us: entry.last_audit_started_at_us,
                last_audit_completed_at_us: entry.last_audit_completed_at_us,
                last_audit_duration_ms: entry.last_audit_duration_ms,
                current_revision: entry.current_revision,
                candidate_revision: entry.candidate_revision,
                candidate_status: entry.candidate_status.clone(),
                draining_revision: entry.draining_revision,
                draining_status: entry.draining_status.clone(),
            })
            .collect::<Vec<_>>();
        concrete_roots.sort_by(|a, b| a.root_key.cmp(&b.root_key));

        SourceStatusSnapshot {
            logical_roots,
            concrete_roots,
            degraded_roots: self.sentinel.degraded_roots(),
        }
    }

    pub fn source_primary_by_group_snapshot(&self) -> BTreeMap<String, String> {
        Self::compute_group_primary_by_logical_root(
            &self.logical_roots_snapshot(),
            &self.host_object_grants_snapshot(),
            None,
        )
        .into_iter()
        .collect()
    }

    pub fn publish_manual_rescan_signal(&self) -> Result<()> {
        self.manual_rescan_signal
            .emit(&self.node_id.0)
            .map(|_| ())
            .map_err(|err| {
                CnxError::Internal(format!("publish manual rescan signal failed: {err}"))
            })
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
                match source.manual_rescan_signal.watch_since(offset) {
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
                            source.trigger_rescan_when_ready().await;
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

    pub fn force_find_inflight_groups_snapshot(&self) -> Vec<String> {
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

    pub fn last_force_find_runner_by_group_snapshot(&self) -> BTreeMap<String, String> {
        lock_or_recover(
            &self.force_find_last_runner,
            "source.force_find.last_runner.snapshot",
        )
        .clone()
    }

    async fn wait_for_group_primary_scan_roots_ready(&self) {
        let expected = lock_or_recover(
            &self.state_cell.roots,
            "source.wait_for_group_primary_scan_roots_ready.roots",
        )
        .iter()
        .filter(|root| root.is_group_primary && root.spec.scan)
        .map(Self::root_runtime_key)
        .collect::<Vec<_>>();
        if expected.is_empty() {
            return;
        }

        let deadline = tokio::time::Instant::now() + RESCAN_READY_WAIT_TIMEOUT;
        loop {
            if self.shutdown.is_cancelled() {
                return;
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
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                log::debug!(
                    "source-fs-meta: timed out waiting for primary scan roots before rescan: {:?}",
                    expected
                );
                return;
            }
            tokio::time::sleep(RESCAN_READY_POLL_INTERVAL).await;
        }
    }

    /// Update logical roots online and reconcile concrete-root tasks without restart.
    pub async fn update_logical_roots(&self, roots: Vec<RootSpec>) -> Result<()> {
        let mut cfg = self.config.clone();
        cfg.roots = roots;
        let root_specs = cfg.effective_roots().map_err(CnxError::InvalidInput)?;
        let host_object_grants = self.host_object_grants_snapshot();
        let fanout = Self::compute_logical_root_fanout(&root_specs, &host_object_grants);
        let root_count = root_specs.len();
        let grant_count = host_object_grants.len();
        let bound_scopes = root_specs
            .iter()
            .map(|root| BoundScope {
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
            "source.update.logical_roots",
        ) = root_specs.clone();
        Self::set_logical_root_health(
            &self.state_cell.fanout_health,
            &root_specs,
            &fanout,
            self.config.lru_capacity,
        );
        *lock_or_recover(
            &self.state_cell.logical_root_fanout,
            "source.update.logical_root_fanout",
        ) = fanout;
        self.refresh_runtime_roots(true).await?;
        self.state_cell.record_authoritative_commit(
            "source.update_logical_roots",
            format!("roots={} host_object_grants={}", root_count, grant_count),
        );
        Ok(())
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
        out_tx: mpsc::UnboundedSender<Vec<Event>>,
        role: RootTaskRole,
        revision: u64,
    ) -> RootTaskHandle {
        let root_key = Self::root_runtime_key(&root);
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
        let sentinel = self.sentinel.clone();
        let apply_startup_delay = self.boundary.is_some();
        let join = tokio::spawn(async move {
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
                    role,
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
                    root_key.clone(),
                )
                .await;
                match stream {
                    Ok(stream) => {
                        let _ = ready_tx.send(true);
                        Self::update_root_task_slot_health(
                            &fanout_health,
                            &root_key,
                            revision,
                            role,
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
                            if out_tx.send(batch).is_err() {
                                Self::update_root_task_slot_health(
                                    &fanout_health,
                                    &root_key,
                                    revision,
                                    role,
                                    "output_closed",
                                );
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
                            role,
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
                role,
                "stopped",
            );
        });
        RootTaskHandle {
            cancel,
            join,
            ready_rx,
        }
    }

    async fn reconcile_root_tasks(&self, desired_roots: &[RootRuntime]) {
        let out_tx =
            lock_or_recover(&self.state_cell.stream_tx, "source.reconcile.stream_tx").clone();
        let Some(out_tx) = out_tx else {
            return;
        };

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
                }
            }
            for (key, (root, signature)) in &desired {
                match tasks.get_mut(key) {
                    Some(existing_entry) if existing_entry.active.signature == *signature => {
                        if let Some(candidate) = existing_entry.candidate.as_ref() {
                            if candidate.signature == *signature {
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
                            if candidate.signature == *signature {
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
                            signature: signature.clone(),
                            handle,
                        });
                    }
                    None => {
                        let revision = self.state_cell.next_root_task_revision();
                        let handle = self.spawn_root_task(
                            root.clone(),
                            out_tx.clone(),
                            RootTaskRole::Active,
                            revision,
                        );
                        added_ready.push(handle.ready_rx.clone());
                        tasks.insert(
                            key.clone(),
                            RootTaskEntry {
                                active: RootTaskSlot {
                                    revision,
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

        for (key, stale) in stale_candidates {
            Self::mark_root_task_draining(
                &self.state_cell.fanout_health,
                &key,
                stale.revision,
                "draining",
            );
            stale.handle.cancel.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(2), stale.handle.join).await;
            Self::finish_root_task_draining(
                &self.state_cell.fanout_health,
                &key,
                stale.revision,
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
                true,
            )
            .await;
            if let Some(candidate) = entry.candidate {
                candidate.handle.cancel.cancel();
                let _ = tokio::time::timeout(Duration::from_secs(2), candidate.handle.join).await;
                Self::clear_root_task_candidate_health(
                    &self.state_cell.fanout_health,
                    &replacement.key,
                );
            }
            let revision = self.state_cell.next_root_task_revision();
            let handle = self.spawn_root_task(
                replacement.root.clone(),
                out_tx.clone(),
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
                    &key,
                    candidate.revision,
                );
                let old_active = std::mem::replace(&mut entry.active, candidate);
                Self::mark_root_task_draining(
                    &self.state_cell.fanout_health,
                    &key,
                    old_active.revision,
                    "draining",
                );
                promoted_old.push((key.clone(), old_active));
            }
        }

        for (key, old_active) in promoted_old {
            Self::cancel_root_task_slot(&self.state_cell.fanout_health, &key, old_active, true)
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
                let candidate = entry.candidate.as_ref().expect("candidate exists").revision;
                let mut entry = entry;
                let candidate_slot = entry
                    .candidate
                    .take()
                    .expect("candidate extracted for fallback");
                debug_assert_eq!(candidate_slot.revision, candidate);
                extracted_fallbacks.push((fallback, entry, candidate_slot));
            }
        }

        for (fallback, entry, candidate) in extracted_fallbacks {
            Self::fail_root_task_candidate_health(
                &self.state_cell.fanout_health,
                &fallback.key,
                candidate.revision,
                "timed_out: overlap candidate did not become ready",
            );
            candidate.handle.cancel.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(2), candidate.handle.join).await;
            Self::clear_root_task_candidate_health(&self.state_cell.fanout_health, &fallback.key);
            Self::cancel_root_task_slot(
                &self.state_cell.fanout_health,
                &fallback.key,
                entry.active,
                true,
            )
            .await;

            let revision = self.state_cell.next_root_task_revision();
            let handle = self.spawn_root_task(
                fallback.root.clone(),
                out_tx.clone(),
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
                    true,
                )
                .await;
                if let Some(candidate) = entry.candidate {
                    candidate.handle.cancel.cancel();
                    let _ =
                        tokio::time::timeout(Duration::from_secs(2), candidate.handle.join).await;
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
    pub async fn pub_(&self) -> Result<Pin<Box<dyn Stream<Item = Vec<Event>> + Send>>> {
        if cfg!(not(target_os = "linux")) {
            return Err(CnxError::NotSupported(
                "fs-meta realtime watch requires linux host".into(),
            ));
        }
        self.start_manual_rescan_watch().await?;
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Vec<Event>>();
        *lock_or_recover(&self.state_cell.stream_tx, "source.pub.stream_tx") = Some(out_tx.clone());
        *lock_or_recover(&self.state, "source.pub.state") = LifecycleState::Scanning;
        let roots_snapshot = lock_or_recover(&self.state_cell.roots, "source.pub.roots").clone();
        self.reconcile_root_tasks(&roots_snapshot).await;
        *lock_or_recover(&self.state, "source.pub.state.ready") = LifecycleState::Ready;

        let shutdown = self.shutdown.clone();
        let output_stream = stream! {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    batch = out_rx.recv() => match batch {
                        Some(events) if !events.is_empty() => yield events,
                        Some(_) => {},
                        None => break,
                    }
                }
            }
        };
        Ok(Box::pin(output_stream))
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
        root_key: String,
    ) -> Result<Pin<Box<dyn Stream<Item = Vec<Event>> + Send>>> {
        let root_path = root.monitor_path.clone();
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(300);

        loop {
            if global_shutdown.is_cancelled() || task_shutdown.is_cancelled() {
                return Err(CnxError::ChannelClosed);
            }
            eprintln!(
                "fs_meta_source: probing root availability root_key={} path={}",
                root_key,
                root_path.display()
            );
            match root.host_fs.metadata(&root_path) {
                Ok(_) => break,
                Err(e) => {
                    eprintln!(
                        "fs_meta_source: root unavailable root_key={} path={} err={}",
                        root_key,
                        root_path.display(),
                        e
                    );
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
            }
        }

        let watch_manager = if root.spec.watch {
            match root.host_fs.watch_session_open() {
                Ok(backend) => match WatchManager::new(
                    &config,
                    root_path.clone(),
                    root.emit_prefix.clone(),
                    Box::new(backend),
                ) {
                    Ok(manager) => {
                        let manager = Arc::new(Mutex::new(manager));
                        let schedule_result = {
                            let mut mgr =
                                lock_or_recover(&manager, "source.root_stream.root_schedule");
                            mgr.schedule(&root_path)
                        };
                        if let Err(err) = schedule_result {
                            log::warn!(
                                "root {} watch schedule failed ({}); continuing with scan/audit",
                                root.spec.id,
                                err
                            );
                            Self::set_object_last_error(
                                &fanout_health,
                                &root_key,
                                format!("root watch schedule failed: {err}"),
                            );
                        }
                        Some(manager)
                    }
                    Err(err) => {
                        log::warn!(
                            "root {} watch manager init failed ({}); degrading to scan-only",
                            root.spec.id,
                            err
                        );
                        Self::set_object_last_error(
                            &fanout_health,
                            &root_key,
                            format!("root watch manager init failed: {err}"),
                        );
                        None
                    }
                },
                Err(err) => {
                    let actions = sentinel.handle_signal(HealthSignal::WatchInitFailed {
                        root_key: root_key.clone(),
                        error: err.to_string(),
                    });
                    Self::execute_sentinel_actions(
                        &actions,
                        &root_key,
                        Some(&root.rescan_tx),
                        Some(&fanout_health),
                    );
                    log::warn!(
                        "root {} watch backend init failed ({}); degrading to scan-only",
                        root.spec.id,
                        err
                    );
                    Self::set_object_last_error(
                        &fanout_health,
                        &root_key,
                        format!("failed to initialize host watch backend: {err}"),
                    );
                    None
                }
            }
        } else {
            None
        };

        let (scan_tx, mut scan_rx) = mpsc::channel::<Vec<Event>>(256);
        let rescan_scan_tx = scan_tx.clone();
        if root.spec.scan && Self::root_current_is_group_primary(&roots_handle, &root_key) {
            let scanner = Arc::clone(&root.scanner);
            let drift_est = Arc::clone(&drift_estimator);
            let clock = Arc::clone(&logical_clock);
            let mtime_cache = Arc::clone(&root.mtime_cache);
            let epoch_counter = Arc::clone(&root.epoch_counter);
            let scan_global_shutdown = global_shutdown.clone();
            let scan_task_shutdown = task_shutdown.clone();
            let scan_watch_mgr = watch_manager.clone();
            let initial_scan_health = fanout_health.clone();
            let initial_scan_root_key = root_key.clone();
            tokio::spawn(async move {
                // CONTRACTS.CLUSTER.COLD_START: Delay initial Epoch 0 scan by 10 seconds.
                // This prevents a massive CPU storm during cluster convergence, where components
                // like gossip + routing need CPU to form the cluster cleanly.
                let startup_delay = if apply_startup_delay {
                    epoch0_scan_delay()
                } else {
                    Duration::ZERO
                };
                if !startup_delay.is_zero() {
                    tokio::time::sleep(startup_delay).await;
                }

                if scan_global_shutdown.is_cancelled() || scan_task_shutdown.is_cancelled() {
                    return;
                }

                let audit_started_at_us = now_us();
                Self::mark_root_audit_start(
                    &initial_scan_health,
                    &initial_scan_root_key,
                    "initial_scan",
                    audit_started_at_us,
                );
                let initial_scan_root_key_for_scan = initial_scan_root_key.clone();

                let scan_result = tokio::task::spawn_blocking(move || {
                    let mut cache =
                        lock_or_recover(&mtime_cache, "source.root.scan_audit.mtime_cache");
                    let epoch = {
                        let mut ec =
                            lock_or_recover(&epoch_counter, "source.root.scan_audit.epoch_counter");
                        let epoch = *ec;
                        *ec += 1;
                        epoch
                    };
                    let batches =
                        scanner.scan_audit(epoch, &mut cache, &drift_est, &clock, scan_watch_mgr);
                    eprintln!(
                        "fs_meta_source: initial scan produced batches={} root_key={}",
                        batches.len(),
                        initial_scan_root_key_for_scan
                    );
                    for (idx, batch) in batches.into_iter().enumerate() {
                        if scan_global_shutdown.is_cancelled() || scan_task_shutdown.is_cancelled()
                        {
                            return;
                        }
                        eprintln!(
                            "fs_meta_source: initial scan enqueue batch_index={} len={} root_key={}",
                            idx,
                            batch.len(),
                            initial_scan_root_key_for_scan
                        );
                        if scan_tx.blocking_send(batch).is_err() {
                            return;
                        }
                    }
                })
                .await;
                Self::mark_root_audit_completed(
                    &initial_scan_health,
                    &initial_scan_root_key,
                    audit_started_at_us,
                    now_us(),
                );
                if let Err(err) = scan_result {
                    Self::set_object_last_error(
                        &initial_scan_health,
                        &initial_scan_root_key,
                        format!("initial scan join failed: {err}"),
                    );
                }
            });
        } else {
            drop(scan_tx);
        }

        let (watch_tx, mut watch_rx) = mpsc::channel::<Vec<Event>>(256);
        let mut rescan_rx = root.rescan_tx.subscribe();
        let watch_handle = if let Some(manager) = watch_manager.clone() {
            Some(watcher::start_watch_loop(
                manager,
                Arc::clone(&drift_estimator),
                watch_tx,
                global_shutdown.clone(),
                NodeId(root.object_ref.clone()),
                Arc::clone(&logical_clock),
                root.host_fs.clone(),
                config.throttle_interval,
                Some(root.rescan_tx.clone()),
            ))
        } else {
            drop(watch_tx);
            None
        };

        let rescan_scanner = Arc::clone(&root.scanner);
        let rescan_drift = Arc::clone(&drift_estimator);
        let rescan_clock = Arc::clone(&logical_clock);
        let rescan_mtime_cache = Arc::clone(&root.mtime_cache);
        let rescan_epoch = Arc::clone(&root.epoch_counter);
        let rescan_watch = watch_manager;
        let audit_interval = root
            .spec
            .audit_interval_ms
            .map(Duration::from_millis)
            .unwrap_or(config.audit_interval);

        let output_stream = stream! {
            let mut audit_tick = tokio::time::interval(audit_interval);
            let mut scan_channel_open = true;
            let mut periodic_channel_open = root.spec.scan && !audit_interval.is_zero();
            let mut watch_channel_open = watch_handle.is_some();
            let mut last_manual_dispatch_target = 0_u64;
            if periodic_channel_open {
                audit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                // Discard immediate first tick so periodic scan starts after one full interval.
                audit_tick.tick().await;
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
            }
            loop {
                dispatch_pending_manual(&mut last_manual_dispatch_target);
                let current_is_group_primary =
                    Self::root_current_is_group_primary(&roots_handle, &root_key);
                let rescan_open = rescan_channel_open;
                let periodic_open = periodic_channel_open && current_is_group_primary;
                tokio::select! {
                    _ = global_shutdown.cancelled() => break,
                    _ = task_shutdown.cancelled() => break,
                    batch = scan_rx.recv(), if scan_channel_open => match batch {
                        Some(batch) => yield batch,
                        None => {
                            scan_channel_open = false;
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
                            let manual_requested_target = if matches!(reason, RescanReason::Manual) {
                                lock_or_recover(
                                    &manual_rescan_intents,
                                    "source.root.manual_rescan_intents.requested_target",
                                )
                                .get(&root_key)
                                .map(|intent| intent.requested)
                            } else {
                                None
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
                            let wm = rescan_watch.clone();
                            let manual_full_audit = matches!(reason, RescanReason::Manual);
                            let rescan_batches = tokio::task::spawn_blocking(move || {
                                let mut c = lock_or_recover(&cache, "source.root.rescan.mtime_cache");
                                if manual_full_audit {
                                    c.clear();
                                    scanner.reset_audit_caches_for_manual_rescan();
                                }
                                let mut ep = lock_or_recover(&epoch, "source.root.rescan.epoch_counter");
                                let current_epoch = *ep;
                                *ep += 1;
                                scanner.scan_audit(current_epoch, &mut c, &de, &clk, wm)
                            }).await;
                            Self::mark_root_audit_completed(
                                &fanout_health,
                                &root_key,
                                audit_started_at_us,
                                now_us(),
                            );
                            match rescan_batches {
                                Ok(batches) => {
                                    for batch in batches.into_iter() {
                                        if rescan_scan_tx.send(batch).await.is_err() {
                                            break;
                                        }
                                    }
                                    if let Some(target) = manual_requested_target {
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
                                Err(err) => {
                                    Self::set_object_last_error(
                                        &fanout_health,
                                        &root_key,
                                        format!("rescan join failed: {err}"),
                                    );
                                }
                            }
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

    /// Force-find query path used by projection API.
    ///
    /// Unlike `req`, this path supports per-object targeting and degrades
    /// gracefully when one object scan fails.
    pub fn force_find(&self, params: &InternalQueryRequest) -> Result<Vec<Event>> {
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

    pub async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        let signals = source_control_signals_from_envelopes(envelopes)?;
        self.apply_orchestration_signals(&signals).await
    }

    pub async fn close(&self) -> Result<()> {
        self.shutdown.cancel();
        let mut tasks = std::mem::take(&mut *lock_or_recover(
            &self.state_cell.root_tasks,
            "source.close.root_tasks",
        ));
        for (_key, entry) in tasks.drain() {
            entry.active.handle.cancel.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(2), entry.active.handle.join).await;
            if let Some(candidate) = entry.candidate {
                candidate.handle.cancel.cancel();
                let _ = tokio::time::timeout(Duration::from_secs(2), candidate.handle.join).await;
            }
        }
        *lock_or_recover(&self.state_cell.stream_tx, "source.close.stream_tx") = None;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use capanix_runtime_host_sdk::control::{
        ExecActivate, ExecControl, ExecDeactivate, HostDescriptor, HostObjectGrant,
        HostObjectGrantState, HostObjectType, ObjectDescriptor, RuntimeHostObjectGrantsChanged,
        encode_exec_control_envelope, encode_runtime_host_object_grants_changed_envelope,
        encode_unit_tick_envelope,
    };
    use capanix_app_sdk::route_proto::UnitTick;
    use std::collections::BTreeSet;

    fn root(id: &str, path: &str) -> RootSpec {
        RootSpec::new(id, std::path::PathBuf::from(path))
    }

    fn test_export(
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

    fn route_export(
        object_ref: &str,
        host_ref: &str,
        host_ip: &str,
        mount_point: &str,
        active: bool,
    ) -> HostObjectGrant {
        HostObjectGrant {
            object_ref: object_ref.to_string(),
            object_type: HostObjectType::MountRoot,
            interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
            host: HostDescriptor {
                host_ref: host_ref.to_string(),
                host_ip: host_ip.to_string(),
                host_name: Some(host_ref.to_string()),
                site: None,
                zone: None,
                host_labels: Default::default(),
            },
            object: ObjectDescriptor {
                mount_point: mount_point.to_string(),
                fs_source: mount_point.to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
            },
            grant_state: if active {
                HostObjectGrantState::Active
            } else {
                HostObjectGrantState::Revoked
            },
        }
    }

    fn build_source(initial_grants: Vec<GrantedMountRoot>) -> FSMetaSource {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![root("nfs1", "/mnt/nfs1"), root("nfs2", "/mnt/nfs2")];
        cfg.host_object_grants = initial_grants;
        FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("build source")
    }

    fn pending_root_task_handle() -> RootTaskHandle {
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let (_ready_tx, ready_rx) = tokio::sync::watch::channel(false);
        let join = tokio::spawn(async move {
            task_cancel.cancelled().await;
        });
        RootTaskHandle {
            cancel,
            join,
            ready_rx,
        }
    }

    fn force_find_params() -> crate::query::request::InternalQueryRequest {
        crate::query::request::InternalQueryRequest::force_find(
            crate::query::request::QueryOp::Tree,
            crate::query::request::QueryScope {
                path: b"/".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: None,
            },
        )
    }

    fn event_origin_ids(events: &[Event]) -> BTreeSet<String> {
        events
            .iter()
            .map(|event| event.metadata().origin_id.0.clone())
            .collect()
    }

    #[test]
    fn dropping_source_clone_does_not_cancel_shared_shutdown() {
        let source = build_source(vec![test_export(
            "node-a",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )]);
        let clone = source.clone();
        drop(clone);
        assert!(
            !source.shutdown.is_cancelled(),
            "dropping a clone must not cancel the shared source runtime shutdown token"
        );
    }

    #[tokio::test]
    async fn runtime_host_object_grants_changed_updates_fanout() {
        let source = build_source(vec![test_export(
            "node-a",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )]);

        let envelope =
            encode_runtime_host_object_grants_changed_envelope(&RuntimeHostObjectGrantsChanged {
                version: 2,
                grants: vec![
                    route_export("node-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
                    route_export("node-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
                    route_export("node-z", "node-z", "10.0.0.13", "relative/path", true),
                ],
            })
            .expect("encode runtime host object grants changed");

        source
            .on_control_frame(&[envelope])
            .await
            .expect("apply host object grants changed frame");

        assert_eq!(source.host_object_grants_version.load(Ordering::Relaxed), 2);
        let grants = lock_or_recover(
            &source.state_cell.host_object_grants,
            "test.host_object_grants",
        )
        .clone();
        assert_eq!(grants.len(), 2, "non-absolute mount path must be filtered");

        let fanout = lock_or_recover(
            &source.state_cell.logical_root_fanout,
            "test.logical_root_fanout",
        )
        .clone();
        assert_eq!(fanout.get("nfs1").map(|v| v.len()), Some(1));
        assert_eq!(fanout.get("nfs2").map(|v| v.len()), Some(1));

        let health = lock_or_recover(&source.state_cell.fanout_health, "test.fanout_health");
        assert_eq!(
            health.logical_root.get("nfs1").map(String::as_str),
            Some("ready")
        );
        assert_eq!(
            health.logical_root.get("nfs2").map(String::as_str),
            Some("ready")
        );
    }

    #[tokio::test]
    async fn source_state_authority_log_records_runtime_mutations() {
        let source = build_source(vec![test_export(
            "node-a",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )]);
        let before = source.state_cell.authority_log_len();
        assert!(before >= 1, "bootstrap should append authority record");

        source
            .update_logical_roots(vec![root("nfs1", "/mnt/nfs1"), root("nfs3", "/mnt/nfs3")])
            .await
            .expect("update logical roots");
        let after_update = source.state_cell.authority_log_len();
        assert!(
            after_update > before,
            "logical root update should append authority record (before={}, after={})",
            before,
            after_update
        );

        let envelope =
            encode_runtime_host_object_grants_changed_envelope(&RuntimeHostObjectGrantsChanged {
                version: 1,
                grants: vec![route_export(
                    "node-a",
                    "node-a",
                    "10.0.0.11",
                    "/mnt/nfs1",
                    true,
                )],
            })
            .expect("encode runtime host object grants changed");
        source
            .on_control_frame(&[envelope])
            .await
            .expect("apply runtime host object grants changed frame");
        let after_control = source.state_cell.authority_log_len();
        assert!(
            after_control > after_update,
            "runtime host object grants changed should append authority record (update={}, control={})",
            after_update,
            after_control
        );
    }

    #[tokio::test]
    async fn runtime_host_object_grants_changed_ignores_stale_versions() {
        let source = build_source(vec![test_export(
            "node-a",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )]);

        let first =
            encode_runtime_host_object_grants_changed_envelope(&RuntimeHostObjectGrantsChanged {
                version: 1,
                grants: vec![route_export(
                    "node-a",
                    "node-a",
                    "10.0.0.11",
                    "/mnt/nfs1",
                    true,
                )],
            })
            .expect("encode first frame");
        source
            .on_control_frame(&[first])
            .await
            .expect("apply first frame");

        let stale =
            encode_runtime_host_object_grants_changed_envelope(&RuntimeHostObjectGrantsChanged {
                version: 1,
                grants: vec![route_export(
                    "node-b",
                    "node-b",
                    "10.0.0.12",
                    "/mnt/nfs2",
                    true,
                )],
            })
            .expect("encode stale frame");
        source
            .on_control_frame(&[stale])
            .await
            .expect("stale frame should be ignored");

        assert_eq!(source.host_object_grants_version.load(Ordering::Relaxed), 1);
        let grants = lock_or_recover(
            &source.state_cell.host_object_grants,
            "test.host_object_grants",
        )
        .clone();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].object_ref, "node-a");
        assert_eq!(grants[0].mount_point, std::path::PathBuf::from("/mnt/nfs1"));
    }

    #[tokio::test]
    async fn unit_tick_control_frame_is_accepted() {
        let source = build_source(vec![]);
        let envelope = encode_unit_tick_envelope(&UnitTick {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.scan".to_string(),
            generation: 1,
            at_ms: 1,
        })
        .expect("encode unit tick");

        source
            .on_control_frame(&[envelope])
            .await
            .expect("source should accept unit tick control frame");
    }

    #[tokio::test]
    async fn unit_tick_with_unknown_unit_id_is_rejected() {
        let source = build_source(vec![]);
        let envelope = encode_unit_tick_envelope(&UnitTick {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.unknown".to_string(),
            generation: 1,
            at_ms: 1,
        })
        .expect("encode unit tick");

        let err = source
            .on_control_frame(&[envelope])
            .await
            .expect_err("unknown unit id must be rejected");
        assert!(matches!(err, CnxError::NotSupported(_)));
        assert!(err.to_string().contains("unsupported unit_id"));
    }

    #[tokio::test]
    async fn exec_activate_with_unknown_unit_id_is_rejected() {
        let source = build_source(vec![]);
        let envelope = encode_exec_control_envelope(&ExecControl::Activate(ExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.unknown".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: Vec::new(),
        }))
        .expect("encode exec activate");

        let err = source
            .on_control_frame(&[envelope])
            .await
            .expect_err("unknown unit id must be rejected");
        assert!(matches!(err, CnxError::NotSupported(_)));
        assert!(err.to_string().contains("unsupported unit_id"));
    }

    #[tokio::test]
    async fn stale_deactivate_generation_is_ignored() {
        let source = build_source(vec![]);
        let activate = encode_exec_control_envelope(&ExecControl::Activate(ExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.scan".to_string(),
            lease: None,
            generation: 5,
            expires_at_ms: 1,
            bound_scopes: Vec::new(),
        }))
        .expect("encode activate");
        source
            .on_control_frame(&[activate])
            .await
            .expect("activate should pass");

        let stale_deactivate =
            encode_exec_control_envelope(&ExecControl::Deactivate(ExecDeactivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.scan".to_string(),
                lease: None,
                generation: 4,
                reason: "test".to_string(),
            }))
            .expect("encode deactivate");
        source
            .on_control_frame(&[stale_deactivate])
            .await
            .expect("stale deactivate should be ignored");

        let state = source.unit_control.snapshot("runtime.exec.scan");
        assert_eq!(state, Some((5, true)));
    }

    #[tokio::test]
    async fn stale_activate_generation_is_ignored() {
        let source = build_source(vec![]);
        let activate = encode_exec_control_envelope(&ExecControl::Activate(ExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.scan".to_string(),
            lease: None,
            generation: 5,
            expires_at_ms: 1,
            bound_scopes: Vec::new(),
        }))
        .expect("encode activate");
        source
            .on_control_frame(&[activate])
            .await
            .expect("activate should pass");

        let deactivate = encode_exec_control_envelope(&ExecControl::Deactivate(ExecDeactivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.scan".to_string(),
            lease: None,
            generation: 5,
            reason: "test".to_string(),
        }))
        .expect("encode deactivate");
        source
            .on_control_frame(&[deactivate])
            .await
            .expect("deactivate should pass");

        let stale_activate = encode_exec_control_envelope(&ExecControl::Activate(ExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.scan".to_string(),
            lease: None,
            generation: 4,
            expires_at_ms: 1,
            bound_scopes: Vec::new(),
        }))
        .expect("encode stale activate");
        source
            .on_control_frame(&[stale_activate])
            .await
            .expect("stale activate should be ignored");

        let state = source.unit_control.snapshot("runtime.exec.scan");
        assert_eq!(state, Some((5, false)));
    }

    #[test]
    fn build_root_runtimes_treats_local_object_ref_as_local_host() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![test_export(
            "node-a::nfs1",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];

        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let runtimes =
            lock_or_recover(&source.state_cell.roots, "test.local_composite_member").clone();
        assert_eq!(runtimes.len(), 1);
        assert_eq!(runtimes[0].object_ref, "node-a::nfs1");
    }

    #[test]
    fn build_root_runtimes_marks_only_group_primary_for_periodic_loop() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![
            test_export("node-a::exp1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            test_export("node-a::exp2", "node-a", "10.0.0.12", "/mnt/nfs1", true),
        ];

        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let runtimes =
            lock_or_recover(&source.state_cell.roots, "test.group_primary_flags").clone();
        assert_eq!(runtimes.len(), 2);
        let primary = runtimes
            .iter()
            .find(|root| root.is_group_primary)
            .expect("one runtime must be selected as group primary");
        assert_eq!(primary.object_ref, "node-a::exp1");
        let non_primary = runtimes
            .iter()
            .find(|root| !root.is_group_primary)
            .expect("one runtime must be non-primary");
        assert_eq!(non_primary.object_ref, "node-a::exp2");
    }

    #[test]
    fn build_root_runtimes_only_instantiates_local_members() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![
            test_export("node-a::exp1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            test_export("node-b::exp2", "node-b", "10.0.0.12", "/mnt/nfs1", true),
        ];

        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let runtimes =
            lock_or_recover(&source.state_cell.roots, "test.local_member_filter").clone();
        assert_eq!(runtimes.len(), 1);
        assert_eq!(runtimes[0].object_ref, "node-a::exp1");
        assert!(runtimes[0].is_group_primary);
    }

    #[test]
    fn logical_root_fanout_can_group_by_host_ip_descriptor() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec {
                id: "east".to_string(),
                selector: crate::source::config::RootSelector {
                    mount_point: Some(std::path::PathBuf::from("/mnt/shared")),
                    host_ip: Some("10.0.0.11".to_string()),
                    ..Default::default()
                },
                subpath_scope: std::path::PathBuf::from("/"),
                watch: true,
                scan: true,
                audit_interval_ms: None,
            },
            RootSpec {
                id: "west".to_string(),
                selector: crate::source::config::RootSelector {
                    mount_point: Some(std::path::PathBuf::from("/mnt/shared")),
                    host_ip: Some("10.0.0.12".to_string()),
                    ..Default::default()
                },
                subpath_scope: std::path::PathBuf::from("/"),
                watch: true,
                scan: true,
                audit_interval_ms: None,
            },
        ];
        cfg.host_object_grants = vec![
            test_export("node-a::exp1", "node-a", "10.0.0.11", "/mnt/shared", true),
            test_export("node-b::exp2", "node-b", "10.0.0.12", "/mnt/shared", true),
        ];

        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let fanout = source.logical_root_fanout_snapshot();
        assert_eq!(fanout.get("east").map(|rows| rows.len()), Some(1));
        assert_eq!(fanout.get("west").map(|rows| rows.len()), Some(1));
        assert_eq!(
            fanout
                .get("east")
                .and_then(|rows| rows.first())
                .map(|row| row.object_ref.as_str()),
            Some("node-a::exp1")
        );
        assert_eq!(
            fanout
                .get("west")
                .and_then(|rows| rows.first())
                .map(|row| row.object_ref.as_str()),
            Some("node-b::exp2")
        );
    }

    #[test]
    fn trigger_rescan_targets_group_primary_only() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![
            test_export("node-a::exp1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            test_export("node-a::exp2", "node-a", "10.0.0.12", "/mnt/nfs1", true),
        ];
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let runtimes =
            lock_or_recover(&source.state_cell.roots, "test.trigger_rescan_primary_only").clone();
        let mut primary_rx = None;
        let mut non_primary_rx = None;
        for root in runtimes {
            let rx = root.rescan_tx.subscribe();
            if root.is_group_primary {
                primary_rx = Some(rx);
            } else {
                non_primary_rx = Some(rx);
            }
        }

        source.trigger_rescan();

        let primary_reason = primary_rx
            .expect("primary receiver")
            .try_recv()
            .expect("primary should receive manual rescan");
        assert!(matches!(primary_reason, RescanReason::Manual));
        assert!(matches!(
            non_primary_rx.expect("non-primary receiver").try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn manual_rescan_requests_are_coalesced_while_one_is_pending() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![test_export(
            "node-a::exp1",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let primary_root = lock_or_recover(
            &source.state_cell.roots,
            "test.manual_rescan_requests_are_coalesced.roots",
        )
        .iter()
        .find(|root| root.is_group_primary)
        .cloned()
        .expect("primary root exists");
        let root_key = FSMetaSource::root_runtime_key(&primary_root);
        let mut rx = primary_root.rescan_tx.subscribe();

        FSMetaSource::request_rescan_on_primary_roots(
            std::slice::from_ref(&primary_root),
            None,
            Some(&source.state_cell.manual_rescan_intents),
            "manual",
        );
        FSMetaSource::request_rescan_on_primary_roots(
            std::slice::from_ref(&primary_root),
            None,
            Some(&source.state_cell.manual_rescan_intents),
            "manual",
        );

        assert!(matches!(
            rx.try_recv().expect("first signal should be sent"),
            RescanReason::Manual
        ));
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        {
            let mut intents = lock_or_recover(
                &source.state_cell.manual_rescan_intents,
                "test.manual_rescan_requests_are_coalesced.intents",
            );
            let entry = intents.get_mut(&root_key).expect("intent exists");
            assert_eq!(entry.requested, 2);
            assert_eq!(entry.completed, 0);
            entry.completed = entry.requested;
        }

        FSMetaSource::request_rescan_on_primary_roots(
            std::slice::from_ref(&primary_root),
            None,
            Some(&source.state_cell.manual_rescan_intents),
            "manual",
        );
        assert!(matches!(
            rx.try_recv()
                .expect("next signal should be re-armed after completion"),
            RescanReason::Manual
        ));
    }

    #[test]
    fn source_primary_snapshot_reports_cluster_primary_assignment() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![
            test_export("node-a::exp1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            test_export("node-z::exp2", "node-z", "10.0.0.12", "/mnt/nfs1", true),
        ];
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-z".to_string()), None)
            .expect("init source");

        let local_roots = lock_or_recover(
            &source.state_cell.roots,
            "test.source_primary_snapshot.local_roots",
        )
        .clone();
        assert_eq!(
            local_roots.len(),
            1,
            "local node should only materialize local member"
        );
        assert!(
            !local_roots[0].is_group_primary,
            "local member is not the lexical cluster primary in this setup"
        );

        let snapshot = source.source_primary_by_group_snapshot();
        assert_eq!(
            snapshot.get("nfs1").map(String::as_str),
            Some("node-a::exp1"),
            "snapshot must report authoritative cluster primary, not local-only primary state"
        );
    }

    #[tokio::test]
    async fn trigger_rescan_when_ready_waits_for_primary_root_running() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![test_export(
            "node-a::exp1",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let primary_root = lock_or_recover(
            &source.state_cell.roots,
            "test.trigger_rescan_when_ready.roots",
        )
        .iter()
        .find(|root| root.is_group_primary)
        .cloned()
        .expect("primary root exists");
        let root_key = FSMetaSource::root_runtime_key(&primary_root);
        let mut rx = primary_root.rescan_tx.subscribe();
        let fanout_health = source.state_cell.fanout_health.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            lock_or_recover(&fanout_health, "test.trigger_rescan_when_ready.health")
                .object_ref
                .insert(root_key, "running".to_string());
        });

        let started = tokio::time::Instant::now();
        source.trigger_rescan_when_ready().await;
        assert!(started.elapsed() >= Duration::from_millis(75));
        let reason = rx.try_recv().expect("primary should receive manual rescan");
        assert!(matches!(reason, RescanReason::Manual));
    }

    #[test]
    fn root_task_signature_ignores_group_primary_flag_changes() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![test_export(
            "node-a::exp1",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];

        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let runtime = lock_or_recover(&source.state_cell.roots, "test.root_task_signature")
            .first()
            .cloned()
            .expect("runtime exists");
        let mut flipped = runtime.clone();
        flipped.is_group_primary = !runtime.is_group_primary;

        let before = FSMetaSource::root_task_signature(&runtime);
        let after = FSMetaSource::root_task_signature(&flipped);
        assert_eq!(before, after);
    }

    #[test]
    fn status_snapshot_tracks_same_key_candidate_handoff() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![test_export(
            "node-a::exp1",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let runtime = lock_or_recover(&source.state_cell.roots, "test.status_snapshot_handoff")
            .first()
            .cloned()
            .expect("runtime exists");
        let root_key = FSMetaSource::root_runtime_key(&runtime);
        let health = source.state_cell.fanout_health_handle();

        FSMetaSource::update_root_task_slot_health(
            &health,
            &root_key,
            1,
            RootTaskRole::Active,
            "running",
        );
        FSMetaSource::update_root_task_slot_health(
            &health,
            &root_key,
            2,
            RootTaskRole::Candidate,
            "warming",
        );

        let snapshot = source.status_snapshot();
        let entry = snapshot
            .concrete_roots
            .iter()
            .find(|entry| entry.root_key == root_key)
            .expect("concrete root health exists");
        assert_eq!(entry.current_revision, Some(1));
        assert_eq!(entry.candidate_revision, Some(2));
        assert_eq!(entry.candidate_status.as_deref(), Some("warming"));

        FSMetaSource::promote_root_task_candidate_health(&health, &root_key, 2);
        FSMetaSource::mark_root_task_draining(&health, &root_key, 1, "draining");
        FSMetaSource::finish_root_task_draining(&health, &root_key, 1, true);

        let snapshot = source.status_snapshot();
        let entry = snapshot
            .concrete_roots
            .iter()
            .find(|entry| entry.root_key == root_key)
            .expect("concrete root health exists after promote");
        assert_eq!(entry.current_revision, Some(2));
        assert_eq!(entry.candidate_revision, None);
        assert_eq!(entry.draining_revision, Some(1));
        assert_eq!(entry.draining_status.as_deref(), Some("retired"));
    }

    #[tokio::test]
    async fn wait_for_task_handles_ready_blocks_until_all_receivers_flip() {
        let (tx_a, rx_a) = tokio::sync::watch::channel(false);
        let (tx_b, rx_b) = tokio::sync::watch::channel(false);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            let _ = tx_a.send(true);
            tokio::time::sleep(Duration::from_millis(40)).await;
            let _ = tx_b.send(true);
        });
        let started = tokio::time::Instant::now();
        let ready =
            FSMetaSource::wait_for_task_handles_ready(&mut [rx_a, rx_b], Duration::from_secs(1))
                .await;
        assert!(ready);
        assert!(started.elapsed() >= Duration::from_millis(80));
    }

    #[test]
    fn close_rescan_channels_disables_periodic_and_rescan() {
        let mut rescan_open = true;
        let mut periodic_open = true;
        FSMetaSource::close_rescan_channels(&mut rescan_open, &mut periodic_open);
        assert!(!rescan_open);
        assert!(!periodic_open);
    }

    #[tokio::test]
    async fn reconcile_root_tasks_falls_back_to_controlled_replace_when_candidate_never_ready() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros();
        let root_dir = std::env::temp_dir().join(format!("fs-meta-fallback-replace-{unique}"));
        std::fs::create_dir_all(&root_dir).expect("create root dir");
        std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs1", root_dir.clone())];
        cfg.host_object_grants = vec![test_export(
            "node-a::exp1",
            "node-a",
            "10.0.0.11",
            root_dir,
            true,
        )];
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");

        let desired_root = lock_or_recover(&source.state_cell.roots, "test.fallback.roots")
            .first()
            .cloned()
            .expect("desired root exists");
        let root_key = FSMetaSource::root_runtime_key(&desired_root);
        let desired_signature = FSMetaSource::root_task_signature(&desired_root);
        let mut stale_signature = desired_signature.clone();
        stale_signature.watch = !desired_signature.watch;

        let (out_tx, out_rx) = mpsc::unbounded_channel::<Vec<Event>>();
        drop(out_rx);
        *lock_or_recover(&source.state_cell.stream_tx, "test.fallback.stream_tx") = Some(out_tx);
        source
            .state_cell
            .root_task_revision
            .store(100, Ordering::Relaxed);
        lock_or_recover(&source.state_cell.root_tasks, "test.fallback.root_tasks").insert(
            root_key.clone(),
            RootTaskEntry {
                active: RootTaskSlot {
                    revision: 1,
                    signature: stale_signature,
                    handle: pending_root_task_handle(),
                },
                candidate: Some(RootTaskSlot {
                    revision: 2,
                    signature: desired_signature.clone(),
                    handle: pending_root_task_handle(),
                }),
            },
        );
        FSMetaSource::update_root_task_slot_health(
            &source.state_cell.fanout_health_handle(),
            &root_key,
            1,
            RootTaskRole::Active,
            "running",
        );
        FSMetaSource::update_root_task_slot_health(
            &source.state_cell.fanout_health_handle(),
            &root_key,
            2,
            RootTaskRole::Candidate,
            "warming",
        );

        let started = tokio::time::Instant::now();
        source.reconcile_root_tasks(&[desired_root.clone()]).await;
        assert!(
            started.elapsed() >= Duration::from_secs(2),
            "candidate fallback must wait for bounded ready timeout"
        );

        let tasks = lock_or_recover(
            &source.state_cell.root_tasks,
            "test.fallback.root_tasks.after",
        );
        let entry = tasks.get(&root_key).expect("root task still present");
        assert_eq!(entry.active.signature, desired_signature);
        assert_eq!(entry.active.revision, 100);
        assert!(
            entry.candidate.is_none(),
            "timed out candidate must be removed"
        );
        drop(tasks);

        let health = lock_or_recover(&source.state_cell.fanout_health, "test.fallback.health");
        let detail = health
            .object_ref_detail
            .get(&root_key)
            .expect("concrete root detail exists");
        assert_eq!(detail.candidate_revision, None);
        assert_eq!(detail.draining_revision, Some(1));
        assert_eq!(detail.draining_status.as_deref(), Some("retired"));

        source.close().await.expect("close source");
    }

    #[tokio::test]
    async fn runtime_host_object_grants_changed_reassigns_group_primary_after_failover() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros();
        let root_dir = std::env::temp_dir().join(format!("fs-meta-primary-failover-{unique}"));
        std::fs::create_dir_all(&root_dir).expect("create root dir");
        std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs1", root_dir.clone())];
        cfg.host_object_grants = vec![
            test_export(
                "node-a::exp1",
                "node-a",
                "10.0.0.11",
                root_dir.clone(),
                true,
            ),
            test_export(
                "node-a::exp2",
                "node-a",
                "10.0.0.12",
                root_dir.clone(),
                true,
            ),
        ];

        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let initial = lock_or_recover(&source.state_cell.roots, "test.primary_before").clone();
        let initial_primary = initial
            .iter()
            .find(|root| root.is_group_primary)
            .expect("initial primary exists")
            .object_ref
            .clone();
        assert_eq!(initial_primary, "node-a::exp1");

        let envelope =
            encode_runtime_host_object_grants_changed_envelope(&RuntimeHostObjectGrantsChanged {
                version: 1,
                grants: vec![
                    route_export(
                        "node-a::exp1",
                        "node-a",
                        "10.0.0.11",
                        &root_dir.display().to_string(),
                        false,
                    ),
                    route_export(
                        "node-a::exp2",
                        "node-a",
                        "10.0.0.12",
                        &root_dir.display().to_string(),
                        true,
                    ),
                ],
            })
            .expect("encode grants changed");

        source
            .on_control_frame(&[envelope])
            .await
            .expect("apply grants changed frame");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let updated = loop {
            let snapshot = lock_or_recover(&source.state_cell.roots, "test.primary_after").clone();
            let current_primary = snapshot
                .iter()
                .find(|root| root.is_group_primary)
                .map(|root| root.object_ref.clone());
            if current_primary.as_deref() == Some("node-a::exp2") {
                break snapshot;
            }
            if tokio::time::Instant::now() >= deadline {
                let object_refs = snapshot
                    .iter()
                    .map(|root| {
                        format!(
                            "{}:active={}:primary={}",
                            root.object_ref, root.active, root.is_group_primary
                        )
                    })
                    .collect::<Vec<_>>();
                panic!(
                    "timed out waiting for primary failover; roots={}",
                    object_refs.join(", ")
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        let updated_primary = updated
            .iter()
            .find(|root| root.is_group_primary)
            .expect("updated primary exists")
            .object_ref
            .clone();
        assert_eq!(updated_primary, "node-a::exp2");
        assert!(updated.iter().any(|root| {
            root.object_ref == "node-a::exp1" && !root.is_group_primary && !root.active
        }));

        let mut primary_rx = None;
        let mut non_primary_rx = None;
        for root in updated {
            let rx = root.rescan_tx.subscribe();
            if root.is_group_primary {
                primary_rx = Some(rx);
            } else if root.object_ref == "node-a::exp1" {
                non_primary_rx = Some(rx);
            }
        }
        source.trigger_rescan();
        let primary_reason = primary_rx
            .expect("primary receiver")
            .try_recv()
            .expect("new primary should receive manual rescan");
        assert!(matches!(primary_reason, RescanReason::Manual));
        assert!(matches!(
            non_primary_rx.expect("non-primary receiver").try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn force_find_round_robins_across_group_sources() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros();
        let root_dir = std::env::temp_dir().join(format!("fs-meta-force-find-rr-{unique}"));
        std::fs::create_dir_all(&root_dir).expect("create root dir");
        std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs1", root_dir.clone())];
        cfg.host_object_grants = vec![
            test_export(
                "node-a::exp1",
                "node-a",
                "10.0.0.11",
                root_dir.clone(),
                true,
            ),
            test_export(
                "node-a::exp2",
                "node-a",
                "10.0.0.12",
                root_dir.clone(),
                true,
            ),
        ];
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");

        let first = source
            .force_find(&force_find_params())
            .expect("first force-find");
        let second = source
            .force_find(&force_find_params())
            .expect("second force-find");
        let third = source
            .force_find(&force_find_params())
            .expect("third force-find");

        assert_eq!(
            event_origin_ids(&first),
            BTreeSet::from(["nfs1".to_string()])
        );
        assert_eq!(
            event_origin_ids(&second),
            BTreeSet::from(["nfs1".to_string()])
        );
        assert_eq!(
            event_origin_ids(&third),
            BTreeSet::from(["nfs1".to_string()])
        );
        let last_runner = lock_or_recover(
            &source.force_find_last_runner,
            "test.force_find.last_runner.round_robin",
        );
        assert_eq!(
            last_runner.get("nfs1").map(String::as_str),
            Some("node-a::exp1")
        );
        drop(last_runner);
        let rr = lock_or_recover(&source.force_find_rr, "test.force_find.rr.round_robin");
        assert_eq!(rr.get("nfs1").copied(), Some(1));
    }

    #[test]
    fn force_find_rejects_when_group_is_already_inflight() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros();
        let root_dir = std::env::temp_dir().join(format!("fs-meta-force-find-lock-{unique}"));
        std::fs::create_dir_all(&root_dir).expect("create root dir");
        std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs1", root_dir.clone())];
        cfg.host_object_grants = vec![test_export(
            "node-a::exp1",
            "node-a",
            "10.0.0.11",
            root_dir,
            true,
        )];
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");

        lock_or_recover(&source.force_find_inflight, "test.force_find.inflight")
            .insert("nfs1".to_string());
        let err = source
            .force_find(&force_find_params())
            .expect_err("in-flight force-find must fail closed");
        assert!(matches!(err, CnxError::NotReady(_)));
        assert!(err.to_string().contains("already running for group: nfs1"));
    }

    #[test]
    fn force_find_returns_error_when_selected_group_matches_no_runtime() {
        let source = build_source(vec![test_export(
            "node-a::nfs1",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )]);

        let params = crate::query::request::InternalQueryRequest::force_find(
            crate::query::request::QueryOp::Tree,
            crate::query::request::QueryScope {
                selected_group: Some("node-z::nfs9".to_string()),
                ..Default::default()
            },
        );
        let err = source
            .force_find(&params)
            .expect_err("unknown target origin must fail");
        assert!(matches!(err, CnxError::PeerError(_)));
        assert!(err.to_string().contains("matched no group"));
    }

    #[test]
    fn req_skips_unreachable_roots() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros();
        let base = std::env::temp_dir().join(format!("fs-meta-req-reachable-{unique}"));
        let missing = std::env::temp_dir().join(format!("fs-meta-req-unreachable-{unique}"));
        std::fs::create_dir_all(&base).expect("create base root");
        std::fs::write(base.join("ok.txt"), b"ok").expect("seed base file");
        let _ = std::fs::remove_dir_all(&missing);

        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("nfs-ok", base.clone()),
            RootSpec::new("nfs-unreachable", missing.clone()),
        ];
        cfg.host_object_grants = vec![
            test_export("node-a::nfs-ok", "node-a", "node-a", base.clone(), true),
            test_export(
                "node-a::nfs-unreachable",
                "node-a",
                "node-a",
                missing.clone(),
                false,
            ),
        ];
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let events = source
            .query_scan(&crate::query::request::LiveScanRequest::default())
            .expect("reachable roots should still query successfully");
        assert!(!events.is_empty());
    }

    #[test]
    fn force_find_reports_unreachable_target_root() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros();
        let missing_root =
            std::env::temp_dir().join(format!("fs-meta-target-unreachable-{unique}"));
        let _ = std::fs::remove_dir_all(&missing_root);

        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs-unreachable", missing_root.clone())];
        cfg.host_object_grants = vec![test_export(
            "node-a::nfs-unreachable",
            "node-a",
            "node-a",
            missing_root,
            false,
        )];
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");
        let params = crate::query::request::InternalQueryRequest::force_find(
            crate::query::request::QueryOp::Tree,
            crate::query::request::QueryScope {
                selected_group: Some("nfs-unreachable".to_string()),
                ..Default::default()
            },
        );
        let err = source
            .force_find(&params)
            .expect_err("unreachable target root must fail explicitly");
        assert!(matches!(err, CnxError::PeerError(_)));
        assert!(err.to_string().contains("inactive"));
    }

    #[test]
    fn force_find_returns_error_when_all_targeted_roots_fail() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros();
        let missing_root = std::env::temp_dir().join(format!("fs-meta-missing-root-{unique}"));
        let _ = std::fs::remove_dir_all(&missing_root);

        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs-missing", missing_root.clone())];
        cfg.host_object_grants = vec![test_export(
            "node-a::nfs-missing",
            "node-a",
            "10.0.0.99",
            missing_root,
            true,
        )];
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");

        let err = source
            .force_find(&crate::query::request::InternalQueryRequest::force_find(
                crate::query::request::QueryOp::Tree,
                crate::query::request::QueryScope::default(),
            ))
            .expect_err("all-root failures must fail force-find");
        assert!(matches!(err, CnxError::PeerError(_)));
        assert!(err.to_string().contains("failed on all targeted roots"));
        assert!(err.to_string().contains("node-a::nfs-missing"));
    }

    #[test]
    fn force_find_missing_subpath_returns_empty_group_payload_instead_of_error() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_micros();
        let base =
            std::env::temp_dir().join(format!("fs-meta-force-find-missing-subpath-{unique}"));
        std::fs::create_dir_all(&base).expect("create base root");

        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs1", base.clone())];
        cfg.host_object_grants = vec![test_export(
            "node-a::nfs1",
            "node-a",
            "10.0.0.11",
            base,
            true,
        )];
        let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source");

        let events = source
            .force_find(&crate::query::request::InternalQueryRequest::force_find(
                crate::query::request::QueryOp::Tree,
                crate::query::request::QueryScope {
                    path: b"/missing-subpath".to_vec(),
                    ..Default::default()
                },
            ))
            .expect("missing subpath should degrade to empty payload");
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn sentinel_trigger_rescan_bridges_into_rescan_channel() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(2);
        FSMetaSource::execute_sentinel_actions(
            &[SentinelAction::TriggerRescan {
                root_key: "nfs1@node-a".to_string(),
            }],
            "nfs1@node-a",
            Some(&tx),
            None,
        );
        let reason = rx
            .try_recv()
            .expect("sentinel trigger_rescan must publish a rescan signal");
        assert!(matches!(reason, RescanReason::Manual));
    }

    #[test]
    fn manual_rescan_is_processed_even_when_primary_flag_is_false() {
        assert!(should_process_rescan_reason(false, RescanReason::Manual));
    }

    #[test]
    fn periodic_and_overflow_rescan_still_require_primary() {
        assert!(!should_process_rescan_reason(false, RescanReason::Periodic));
        assert!(!should_process_rescan_reason(false, RescanReason::Overflow));
        assert!(should_process_rescan_reason(true, RescanReason::Periodic));
        assert!(should_process_rescan_reason(true, RescanReason::Overflow));
    }

    #[test]
    fn sentinel_degraded_updates_concrete_health_projection() {
        let health = Arc::new(Mutex::new(FanoutHealthState::default()));
        FSMetaSource::execute_sentinel_actions(
            &[SentinelAction::ReportDegraded {
                root_key: "nfs1@node-a@/mnt/nfs1".to_string(),
                reason: "overflow".to_string(),
            }],
            "nfs1@node-a@/mnt/nfs1",
            None,
            Some(&health),
        );
        let guard = lock_or_recover(&health, "test.sentinel_degraded_health");
        assert_eq!(
            guard
                .object_ref
                .get("nfs1@node-a@/mnt/nfs1")
                .map(String::as_str),
            Some("degraded: overflow")
        );
    }

    #[test]
    fn sentinel_actions_ignore_mismatched_root_key() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(2);
        let health = Arc::new(Mutex::new(FanoutHealthState::default()));
        FSMetaSource::execute_sentinel_actions(
            &[SentinelAction::TriggerRescan {
                root_key: "nfs1@node-a@/mnt/nfs1".to_string(),
            }],
            "nfs2@node-b@/mnt/nfs2",
            Some(&tx),
            Some(&health),
        );
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn epoch0_scan_delay_parser_respects_defaults_and_bounds() {
        assert_eq!(epoch0_scan_delay_ms_from_raw(None, 0), 0);
        assert_eq!(epoch0_scan_delay_ms_from_raw(Some("250"), 0), 250);
        assert_eq!(epoch0_scan_delay_ms_from_raw(Some("bad"), 100), 100);
        assert_eq!(
            epoch0_scan_delay_ms_from_raw(Some("999999999"), 100),
            EPOCH0_SCAN_DELAY_MAX_MS
        );
    }
}
