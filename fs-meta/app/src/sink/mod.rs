//! File metadata sink app for Capanix.
//!
//! Materializes a file metadata view from source events. Implements the full
//! L3 CONSISTENCY.md spec: LWW arbitration, epoch management, MID,
//! and query API with reliability annotations.

pub(crate) mod arbitrator;
pub(crate) mod clock;
pub(crate) mod epoch;
pub(crate) mod query;
pub(crate) mod tree;

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;

use capanix_app_sdk::runtime::{
    ControlEnvelope, EventMetadata, NodeId, RecvOpts, in_memory_state_boundary,
};
use capanix_app_sdk::{CnxError, Event, Result};
use capanix_host_adapter_fs::HostAdapter;
use capanix_runtime_entry_sdk::advanced::boundary::{ChannelIoSubset, StateBoundary};
use capanix_runtime_entry_sdk::control::{RuntimeBoundScope, RuntimeHostGrantState};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use crate::EventKind;
use crate::query::models::{HealthStats, QueryNode};
use crate::query::request::{InternalQueryRequest, MaterializedQueryPayload, QueryOp};
#[cfg(test)]
use crate::query::result_ops::{
    RawQueryResult, merge_query_responses, raw_query_results_by_origin_from_source_events,
    subtree_stats_from_query_response,
};
#[cfg(test)]
use crate::query::tree::TreeGroupPayload;
use crate::runtime::endpoint::ManagedEndpointTask;
use crate::runtime::execution_units::{SINK_RUNTIME_UNIT_ID, SINK_RUNTIME_UNITS};
use crate::runtime::orchestration::{
    SinkControlSignal, SinkRuntimeUnit, decode_logical_roots_control_payload,
    sink_control_signals_from_envelopes,
};
use crate::runtime::routes::{
    METHOD_FIND, METHOD_QUERY, METHOD_SINK_QUERY, METHOD_SINK_ROOTS_CONTROL, METHOD_SINK_STATUS,
    METHOD_SOURCE_FIND, METHOD_STREAM, ROUTE_TOKEN_FS_META, ROUTE_TOKEN_FS_META_EVENTS,
    ROUTE_TOKEN_FS_META_INTERNAL, default_route_bindings,
};
use crate::runtime::seam::exchange_host_adapter;
use crate::runtime::unit_gate::RuntimeUnitGate;
use crate::sink::arbitrator::{ProcessOutcome, TombstonePolicy, process_event};
use crate::sink::clock::SinkClock;
use crate::sink::epoch::EpochManager;
use crate::sink::query::query_node_from_node;
use crate::sink::tree::MaterializedTree;
use crate::source::config::{GrantedMountRoot, RootSpec, SourceConfig};
use crate::state::cell::AuthorityJournal;
use crate::state::commit_boundary::CommitBoundary;
use crate::{ControlEvent, FileMetaRecord};

#[cfg(test)]
use crate::runtime::routes::ROUTE_KEY_QUERY;

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_micros() as u64,
        Err(_) => 0,
    }
}

fn lock_or_recover<'a, T>(m: &'a Mutex<T>, context: &str) -> std::sync::MutexGuard<'a, T> {
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
        Bytes::copy_from_slice(message.as_bytes()),
    )
}

fn accumulate_health_stats(total: &mut HealthStats, stats: &HealthStats) {
    total.live_nodes += stats.live_nodes;
    total.tombstoned_count += stats.tombstoned_count;
    total.attested_count += stats.attested_count;
    total.suspect_count += stats.suspect_count;
    total.blind_spot_count += stats.blind_spot_count;
    total.shadow_time_us = total.shadow_time_us.max(stats.shadow_time_us);
}

fn accumulate_status_snapshot(
    snapshot: &mut SinkStatusSnapshot,
    stats: &HealthStats,
    estimated_heap_bytes: u64,
) {
    snapshot.live_nodes += stats.live_nodes;
    snapshot.tombstoned_count += stats.tombstoned_count;
    snapshot.attested_count += stats.attested_count;
    snapshot.suspect_count += stats.suspect_count;
    snapshot.blind_spot_count += stats.blind_spot_count;
    snapshot.shadow_time_us = snapshot.shadow_time_us.max(stats.shadow_time_us);
    snapshot.estimated_heap_bytes = snapshot
        .estimated_heap_bytes
        .saturating_add(estimated_heap_bytes);
}

const VISIBILITY_LAG_RETENTION_US: u64 = 31 * 24 * 60 * 60 * 1_000_000;
const VISIBILITY_LAG_MAX_SAMPLES: usize = 200_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VisibilityLagOp {
    Create,
    Modify,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VisibilityLagSample {
    pub observed_at_us: u64,
    pub lag_ms: u64,
    pub group_id: String,
    pub nfs_id: String,
    pub op: VisibilityLagOp,
}

#[derive(Default)]
struct VisibilityLagTelemetry {
    samples: VecDeque<VisibilityLagSample>,
}

impl VisibilityLagTelemetry {
    fn record_batch(&mut self, mut batch: Vec<VisibilityLagSample>) {
        if batch.is_empty() {
            return;
        }
        let now = batch
            .last()
            .map(|s| s.observed_at_us)
            .unwrap_or_else(now_us);
        let min_allowed = now.saturating_sub(VISIBILITY_LAG_RETENTION_US);
        self.samples
            .retain(|sample| sample.observed_at_us >= min_allowed);
        self.samples.extend(batch.drain(..));
        while self.samples.len() > VISIBILITY_LAG_MAX_SAMPLES {
            self.samples.pop_front();
        }
    }

    fn recent_since(&self, since_us: u64) -> Vec<VisibilityLagSample> {
        self.samples
            .iter()
            .filter(|sample| sample.observed_at_us >= since_us)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SinkGroupStatusSnapshot {
    pub group_id: String,
    pub primary_object_ref: String,
    pub total_nodes: u64,
    pub live_nodes: u64,
    pub tombstoned_count: u64,
    pub attested_count: u64,
    pub suspect_count: u64,
    pub blind_spot_count: u64,
    pub shadow_time_us: u64,
    pub shadow_lag_us: u64,
    pub overflow_pending_audit: bool,
    pub initial_audit_completed: bool,
    pub estimated_heap_bytes: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SinkStatusSnapshot {
    pub live_nodes: u64,
    pub tombstoned_count: u64,
    pub attested_count: u64,
    pub suspect_count: u64,
    pub blind_spot_count: u64,
    pub shadow_time_us: u64,
    pub estimated_heap_bytes: u64,
    pub groups: Vec<SinkGroupStatusSnapshot>,
}

fn sample_visibility_lag(
    group_id: &str,
    object_ref: &str,
    record: &FileMetaRecord,
    outcome: ProcessOutcome,
    observed_at_us: u64,
) -> Option<VisibilityLagSample> {
    if record.unix_stat.is_dir {
        return None;
    }
    let op = match outcome {
        ProcessOutcome::UpsertCreated => VisibilityLagOp::Create,
        ProcessOutcome::UpsertModified => VisibilityLagOp::Modify,
        _ => return None,
    };
    let baseline_us = match op {
        VisibilityLagOp::Create => {
            if record.unix_stat.ctime_us > 0 {
                record.unix_stat.ctime_us
            } else {
                record.unix_stat.mtime_us
            }
        }
        VisibilityLagOp::Modify => record.unix_stat.mtime_us,
    };
    if baseline_us == 0 {
        return None;
    }
    let lag_ms = observed_at_us.saturating_sub(baseline_us) / 1_000;
    Some(VisibilityLagSample {
        observed_at_us,
        lag_ms,
        group_id: group_id.to_string(),
        nfs_id: object_ref.to_string(),
        op,
    })
}

/// Internal per-group state of the sink app.
pub(crate) struct GroupSinkState {
    pub(crate) tree: MaterializedTree,
    pub(crate) clock: SinkClock,
    pub(crate) epoch_manager: EpochManager,
    pub(crate) tombstone_policy: TombstonePolicy,
    pub(crate) primary_object_ref: String,
    pub(crate) overflow_pending_audit: bool,
    pub(crate) audit_epoch_completed: bool,
    pub(crate) initial_audit_completed: bool,
    pub(crate) last_coverage_recovered_at: Option<Instant>,
    pub(crate) materialized_revision: u64,
    pub(crate) sentinel_health: BTreeMap<String, String>,
}

impl GroupSinkState {
    fn new(primary_object_ref: String, tombstone_policy: TombstonePolicy) -> Self {
        Self {
            tree: MaterializedTree::new(),
            clock: SinkClock::new(),
            epoch_manager: EpochManager::new(),
            tombstone_policy,
            primary_object_ref,
            overflow_pending_audit: false,
            audit_epoch_completed: false,
            initial_audit_completed: false,
            last_coverage_recovered_at: Some(Instant::now()),
            materialized_revision: 1,
            sentinel_health: BTreeMap::new(),
        }
    }

    fn refresh_initial_audit_completed(&mut self) {
        self.initial_audit_completed = self.audit_epoch_completed && self.tree.node_count() > 0;
    }
}

pub(crate) struct SinkState {
    pub(crate) groups: BTreeMap<String, GroupSinkState>,
    pub(crate) group_by_object_ref: HashMap<String, String>,
    pub(crate) tombstone_policy: TombstonePolicy,
}

impl SinkState {
    pub(crate) fn new(source_cfg: &SourceConfig) -> Self {
        let tombstone_policy = TombstonePolicy {
            ttl: source_cfg.sink_tombstone_ttl,
            tolerance_us: source_cfg.sink_tombstone_tolerance_us,
        };
        let mut state = Self {
            groups: BTreeMap::new(),
            group_by_object_ref: HashMap::new(),
            tombstone_policy,
        };
        state.reconcile_host_object_grants(&source_cfg.roots, &source_cfg.host_object_grants, None);
        state
    }

    pub(crate) fn reconcile_host_object_grants(
        &mut self,
        roots: &[RootSpec],
        grants: &[GrantedMountRoot],
        allowed_groups: Option<&BTreeSet<String>>,
    ) {
        let tombstone_policy = self.tombstone_policy;
        let mut groups = BTreeMap::<String, GroupSinkState>::new();
        let mut group_by_object_ref = HashMap::<String, String>::new();

        let mut previous_groups = std::mem::take(&mut self.groups);
        for root in roots {
            if allowed_groups.is_some_and(|groups| !groups.contains(&root.id)) {
                continue;
            }
            let members = grants
                .iter()
                .filter(|grant| root.selector.matches(grant))
                .collect::<Vec<_>>();
            let mut member_ids = members
                .iter()
                .map(|grant| grant.object_ref.clone())
                .collect::<Vec<_>>();
            member_ids.sort();
            member_ids.dedup();
            let mut active_member_ids = members
                .iter()
                .filter(|grant| grant.active)
                .map(|grant| grant.object_ref.clone())
                .collect::<Vec<_>>();
            active_member_ids.sort();
            active_member_ids.dedup();
            let primary = active_member_ids
                .first()
                .cloned()
                .or_else(|| member_ids.first().cloned())
                .unwrap_or_else(|| "unassigned".to_string());
            let mut group = previous_groups
                .remove(&root.id)
                .unwrap_or_else(|| GroupSinkState::new(primary.clone(), tombstone_policy));
            group.primary_object_ref = primary;
            for member_id in &member_ids {
                group_by_object_ref.insert(member_id.clone(), root.id.clone());
            }
            groups.insert(root.id.clone(), group);
        }
        self.groups = groups;
        self.group_by_object_ref = group_by_object_ref;
    }

    fn ensure_group_state_mut(
        &mut self,
        object_ref: &str,
    ) -> Result<(String, bool, &mut GroupSinkState)> {
        let group_id = self
            .group_by_object_ref
            .get(object_ref)
            .cloned()
            .ok_or_else(|| {
                CnxError::InvalidInput(format!(
                    "sink received object_ref without configured group mapping: {object_ref}"
                ))
            })?;
        let group = self.groups.get_mut(&group_id).ok_or_else(|| {
            CnxError::InvalidInput(format!(
                "sink missing configured group state for object_ref '{object_ref}' -> group '{group_id}'"
            ))
        })?;
        let is_primary = group.primary_object_ref == object_ref;
        Ok((group_id, is_primary, group))
    }
}

/// In-memory state carrier for sink group trees/clocks/epochs.
///
/// The carrier boundary is explicit so runtime can later swap this with an
/// external StateCell backend without rewriting sink business logic paths.
#[derive(Clone)]
struct SinkStateCell {
    inner: Arc<RwLock<SinkState>>,
    commit_boundary: CommitBoundary,
}

impl SinkStateCell {
    fn new(source_cfg: &SourceConfig, commit_boundary: CommitBoundary) -> Self {
        let cell = Self {
            inner: Arc::new(RwLock::new(SinkState::new(source_cfg))),
            commit_boundary,
        };
        cell.record_authoritative_commit(
            "sink.bootstrap",
            format!(
                "roots={} exports={}",
                source_cfg.roots.len(),
                source_cfg.host_object_grants.len()
            ),
        );
        cell
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, SinkState>> {
        self.inner
            .read()
            .map_err(|_| CnxError::Internal("SinkState lock poisoned".into()))
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, SinkState>> {
        self.inner
            .write()
            .map_err(|_| CnxError::Internal("SinkState lock poisoned".into()))
    }

    fn record_authoritative_commit(&self, op: &str, detail: String) {
        self.commit_boundary.record(op, detail);
    }

    #[cfg(test)]
    fn authority_log_len(&self) -> usize {
        self.commit_boundary.len()
    }
}

/// File metadata sink app.
///
/// Concurrency model:
/// - `send()` takes write lock on SinkState (exclusive)
/// - `recv()` / `req()` + query handlers take read lock (shared)
#[derive(Clone)]
pub struct SinkFileMeta {
    state: SinkStateCell,
    root_specs: Arc<RwLock<Vec<RootSpec>>>,
    host_object_grants: Arc<RwLock<Vec<GrantedMountRoot>>>,
    visibility_lag: Arc<Mutex<VisibilityLagTelemetry>>,
    pending_stream_events: Arc<Mutex<VecDeque<Event>>>,
    unit_control: Arc<RuntimeUnitGate>,
    shutdown: CancellationToken,
    endpoint_tasks: Arc<Mutex<Vec<ManagedEndpointTask>>>,
}

impl SinkFileMeta {
    /// Create a new sink app.
    #[allow(dead_code)]
    pub fn new(node_id: NodeId) -> Result<Self> {
        Self::with_boundaries(node_id, None, SourceConfig::default())
    }

    /// Create a new sink app and optionally attach boundary endpoints for public query,
    /// internal materialized fan-in query, and sink proxy force-find.
    pub fn with_boundaries(
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        source_cfg: SourceConfig,
    ) -> Result<Self> {
        Self::with_boundaries_and_state(node_id, boundary, in_memory_state_boundary(), source_cfg)
    }

    /// Create a new sink app and optionally attach channel/state boundaries.
    pub fn with_boundaries_and_state(
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        state_boundary: Arc<dyn StateBoundary>,
        source_cfg: SourceConfig,
    ) -> Result<Self> {
        Self::with_boundaries_and_state_inner(node_id, boundary, state_boundary, source_cfg, false)
    }

    fn with_boundaries_and_state_inner(
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        state_boundary: Arc<dyn StateBoundary>,
        source_cfg: SourceConfig,
        _defer_authority_read: bool,
    ) -> Result<Self> {
        let authority = AuthorityJournal::from_state_boundary(SINK_RUNTIME_UNIT_ID, state_boundary)
            .map_err(|err| {
                CnxError::InvalidInput(format!("sink statecell authority init failed: {err}"))
            })?;
        let state = SinkStateCell::new(&source_cfg, CommitBoundary::new(authority));
        let root_specs = Arc::new(RwLock::new(source_cfg.roots.clone()));
        let host_object_grants = Arc::new(RwLock::new(source_cfg.host_object_grants.clone()));
        let visibility_lag = Arc::new(Mutex::new(VisibilityLagTelemetry::default()));
        let pending_stream_events = Arc::new(Mutex::new(VecDeque::new()));
        let unit_control = Arc::new(if boundary.is_some() {
            RuntimeUnitGate::new_runtime_managed("sink-file-meta", SINK_RUNTIME_UNITS)
        } else {
            RuntimeUnitGate::new("sink-file-meta", SINK_RUNTIME_UNITS)
        });
        let sink = Self {
            state: state.clone(),
            root_specs: root_specs.clone(),
            host_object_grants: host_object_grants.clone(),
            visibility_lag: visibility_lag.clone(),
            pending_stream_events: pending_stream_events.clone(),
            unit_control: unit_control.clone(),
            shutdown: CancellationToken::new(),
            endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
        };

        if boundary.is_some() {
            sink.reconcile_runtime_groups(&source_cfg.host_object_grants)?;
        }

        if let Some(sys) = boundary {
            let adapter =
                exchange_host_adapter(sys.clone(), node_id.clone(), default_route_bindings());
            let routes = default_route_bindings();

            // 1. Materialized query endpoints.
            let query_state = state.clone();
            let node_id_cloned = node_id.clone();
            let query_root_specs = root_specs.clone();
            let query_host_object_grants = host_object_grants.clone();
            let query_visibility_lag = visibility_lag.clone();
            let query_pending_stream_events = pending_stream_events.clone();
            let query_unit_control = unit_control.clone();
            if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META, METHOD_QUERY) {
                log::info!(
                    "bound route listening on {}.{} for sink {}",
                    ROUTE_TOKEN_FS_META,
                    METHOD_QUERY,
                    node_id_cloned.0
                );
                let endpoint = ManagedEndpointTask::spawn(
                    sys.clone(),
                    route,
                    format!("sink:{}:{}", ROUTE_TOKEN_FS_META, METHOD_QUERY),
                    sink.shutdown.clone(),
                    move |requests| {
                        let query_state = query_state.clone();
                        let query_root_specs = query_root_specs.clone();
                        let query_host_object_grants = query_host_object_grants.clone();
                        let query_visibility_lag = query_visibility_lag.clone();
                        let query_pending_stream_events = query_pending_stream_events.clone();
                        let query_unit_control = query_unit_control.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                if let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                    req.payload_bytes(),
                                ) {
                                    eprintln!(
                                        "fs_meta_sink: query endpoint request selected_group={:?} recursive={} path={}",
                                        params.scope.selected_group,
                                        params.scope.recursive,
                                        String::from_utf8_lossy(&params.scope.path)
                                    );
                                    let sink_impl = SinkFileMeta {
                                        state: query_state.clone(),
                                        root_specs: query_root_specs.clone(),
                                        host_object_grants: query_host_object_grants.clone(),
                                        visibility_lag: query_visibility_lag.clone(),
                                        pending_stream_events: query_pending_stream_events.clone(),
                                        unit_control: query_unit_control.clone(),
                                        shutdown: CancellationToken::new(),
                                        endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
                                    };
                                    let mut events =
                                        sink_impl.materialized_query(&params).unwrap_or_default();
                                    eprintln!(
                                        "fs_meta_sink: query endpoint response events={}",
                                        events.len()
                                    );
                                    for event in &mut events {
                                        let mut meta = event.metadata().clone();
                                        meta.correlation_id = req.metadata().correlation_id;
                                        responses.push(Event::new(
                                            meta,
                                            Bytes::copy_from_slice(event.payload_bytes()),
                                        ));
                                    }
                                } else {
                                    log::warn!("bound route failed to parse InternalQueryRequest");
                                }
                            }
                            responses
                        }
                    },
                );
                lock_or_recover(&sink.endpoint_tasks, "sink.with_boundaries.endpoint_tasks")
                    .push(endpoint);
            } else {
                log::error!(
                    "failed to resolve route lookup for {}.{}",
                    ROUTE_TOKEN_FS_META,
                    METHOD_QUERY
                );
            }

            let internal_query_state = state.clone();
            let internal_query_root_specs = root_specs.clone();
            let internal_query_host_object_grants = host_object_grants.clone();
            let internal_query_visibility_lag = visibility_lag.clone();
            let internal_query_pending_stream_events = pending_stream_events.clone();
            let internal_query_unit_control = unit_control.clone();
            if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY) {
                log::info!(
                    "bound route listening on {}.{} for sink {}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SINK_QUERY,
                    node_id_cloned.0
                );
                let endpoint = ManagedEndpointTask::spawn(
                    sys.clone(),
                    route,
                    format!(
                        "sink:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY
                    ),
                    sink.shutdown.clone(),
                    move |requests| {
                        let internal_query_state = internal_query_state.clone();
                        let internal_query_root_specs = internal_query_root_specs.clone();
                        let internal_query_host_object_grants =
                            internal_query_host_object_grants.clone();
                        let internal_query_visibility_lag = internal_query_visibility_lag.clone();
                        let internal_query_pending_stream_events =
                            internal_query_pending_stream_events.clone();
                        let internal_query_unit_control = internal_query_unit_control.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                if let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                    req.payload_bytes(),
                                ) {
                                    eprintln!(
                                        "fs_meta_sink: internal query endpoint request selected_group={:?} recursive={} path={}",
                                        params.scope.selected_group,
                                        params.scope.recursive,
                                        String::from_utf8_lossy(&params.scope.path)
                                    );
                                    let sink_impl = SinkFileMeta {
                                        state: internal_query_state.clone(),
                                        root_specs: internal_query_root_specs.clone(),
                                        host_object_grants: internal_query_host_object_grants
                                            .clone(),
                                        visibility_lag: internal_query_visibility_lag.clone(),
                                        pending_stream_events: internal_query_pending_stream_events
                                            .clone(),
                                        unit_control: internal_query_unit_control.clone(),
                                        shutdown: CancellationToken::new(),
                                        endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
                                    };
                                    let mut events =
                                        sink_impl.materialized_query(&params).unwrap_or_default();
                                    eprintln!(
                                        "fs_meta_sink: internal query endpoint response events={}",
                                        events.len()
                                    );
                                    for event in &mut events {
                                        let mut meta = event.metadata().clone();
                                        meta.correlation_id = req.metadata().correlation_id;
                                        responses.push(Event::new(
                                            meta,
                                            Bytes::copy_from_slice(event.payload_bytes()),
                                        ));
                                    }
                                } else {
                                    log::warn!("bound route failed to parse InternalQueryRequest");
                                }
                            }
                            responses
                        }
                    },
                );
                lock_or_recover(&sink.endpoint_tasks, "sink.with_boundaries.endpoint_tasks")
                    .push(endpoint);
            } else {
                log::error!(
                    "failed to resolve route lookup for {}.{}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SINK_QUERY
                );
            }

            let internal_status_state = state.clone();
            let internal_status_root_specs = root_specs.clone();
            let internal_status_host_object_grants = host_object_grants.clone();
            let internal_status_visibility_lag = visibility_lag.clone();
            let internal_status_pending_stream_events = pending_stream_events.clone();
            let internal_status_unit_control = unit_control.clone();
            if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS) {
                log::info!(
                    "bound route listening on {}.{} for sink {}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SINK_STATUS,
                    node_id_cloned.0
                );
                let endpoint = ManagedEndpointTask::spawn(
                    sys.clone(),
                    route,
                    format!(
                        "sink:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS
                    ),
                    sink.shutdown.clone(),
                    move |requests| {
                        let internal_status_state = internal_status_state.clone();
                        let internal_status_root_specs = internal_status_root_specs.clone();
                        let internal_status_host_object_grants =
                            internal_status_host_object_grants.clone();
                        let internal_status_visibility_lag = internal_status_visibility_lag.clone();
                        let internal_status_pending_stream_events =
                            internal_status_pending_stream_events.clone();
                        let internal_status_unit_control = internal_status_unit_control.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                let sink_impl = SinkFileMeta {
                                    state: internal_status_state.clone(),
                                    root_specs: internal_status_root_specs.clone(),
                                    host_object_grants: internal_status_host_object_grants.clone(),
                                    visibility_lag: internal_status_visibility_lag.clone(),
                                    pending_stream_events: internal_status_pending_stream_events
                                        .clone(),
                                    unit_control: internal_status_unit_control.clone(),
                                    shutdown: CancellationToken::new(),
                                    endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
                                };
                                if let Ok(snapshot) = sink_impl.status_snapshot()
                                    && let Ok(payload) = rmp_serde::to_vec_named(&snapshot)
                                {
                                    responses.push(Event::new(
                                        EventMetadata {
                                            origin_id: req.metadata().origin_id.clone(),
                                            timestamp_us: now_us(),
                                            logical_ts: None,
                                            correlation_id: req.metadata().correlation_id,
                                            ingress_auth: None,
                                            trace: None,
                                        },
                                        Bytes::from(payload),
                                    ));
                                }
                            }
                            responses
                        }
                    },
                );
                lock_or_recover(&sink.endpoint_tasks, "sink.with_boundaries.endpoint_tasks")
                    .push(endpoint);
            } else {
                log::error!(
                    "failed to resolve route lookup for {}.{}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SINK_STATUS
                );
            }

            // 2. Fresh find path: proxy to source live-scan endpoint through host-adapter SDK.
            //
            // This path must collect correlated reply batches from all matching source
            // executors. A single reply batch is not sufficient once `source` is
            // realized as `scope_members + all`, because any single source may have no
            // matching rows for the requested group/path while its peers do.
            const FORCE_FIND_SOURCE_REPLY_IDLE_GRACE_MS: u64 = 750;
            let node_id_proxy = node_id.clone();
            let proxy_adapter = adapter.clone();
            if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META, METHOD_FIND) {
                log::info!(
                    "bound route listening on {}.{} for sink {}",
                    ROUTE_TOKEN_FS_META,
                    METHOD_FIND,
                    node_id_proxy.0
                );
                let endpoint = ManagedEndpointTask::spawn(
                    sys.clone(),
                    route,
                    format!("sink:{}:{}", ROUTE_TOKEN_FS_META, METHOD_FIND),
                    sink.shutdown.clone(),
                    move |requests| {
                        let node_id_proxy = node_id_proxy.clone();
                        let proxy_adapter = proxy_adapter.clone();
                        async move {
                            let mut responses = Vec::new();

                            for req in requests {
                                let _params = match rmp_serde::from_slice::<InternalQueryRequest>(
                                    req.payload_bytes(),
                                ) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        log::warn!(
                                            "find proxy failed to parse InternalQueryRequest: {:?}",
                                            e
                                        );
                                        responses.push(build_error_marker_event(
                                            &node_id_proxy,
                                            req.metadata().correlation_id,
                                            "find proxy invalid request payload",
                                        ));
                                        continue;
                                    }
                                };

                                match proxy_adapter
                                    .call_collect(
                                        ROUTE_TOKEN_FS_META_INTERNAL,
                                        METHOD_SOURCE_FIND,
                                        Bytes::copy_from_slice(req.payload_bytes()),
                                        Duration::from_secs(60),
                                        Duration::from_millis(
                                            FORCE_FIND_SOURCE_REPLY_IDLE_GRACE_MS,
                                        ),
                                    )
                                    .await
                                {
                                    Ok(source_events) => {
                                        eprintln!(
                                            "fs_meta_sink: public find proxy ok node={} events={}",
                                            node_id_proxy.0,
                                            source_events.len()
                                        );
                                        for event in source_events {
                                            let mut meta = event.metadata().clone();
                                            meta.correlation_id = req.metadata().correlation_id;
                                            responses.push(Event::new(
                                                meta,
                                                Bytes::copy_from_slice(event.payload_bytes()),
                                            ));
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "fs_meta_sink: public find proxy err node={} err={}",
                                            node_id_proxy.0, e
                                        );
                                        log::error!(
                                            "find proxy upstream route call failed: {:?}",
                                            e
                                        );
                                        responses.push(build_error_marker_event(
                                            &node_id_proxy,
                                            req.metadata().correlation_id,
                                            "find proxy upstream request failed",
                                        ));
                                    }
                                }
                            }
                            responses
                        }
                    },
                );
                lock_or_recover(&sink.endpoint_tasks, "sink.with_boundaries.endpoint_tasks")
                    .push(endpoint);
            } else {
                log::error!(
                    "failed to resolve route lookup for {}.{}",
                    ROUTE_TOKEN_FS_META,
                    METHOD_FIND
                );
            }

            let stream_state = state.clone();
            let stream_root_specs = root_specs.clone();
            let stream_host_object_grants = host_object_grants.clone();
            let stream_visibility_lag = visibility_lag.clone();
            let stream_unit_control = unit_control.clone();
            if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM) {
                let stream_sink = Arc::new(SinkFileMeta {
                    state: stream_state,
                    root_specs: stream_root_specs,
                    host_object_grants: stream_host_object_grants,
                    visibility_lag: stream_visibility_lag,
                    pending_stream_events: pending_stream_events.clone(),
                    unit_control: stream_unit_control,
                    shutdown: CancellationToken::new(),
                    endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
                });
                let stream_sink_ready = stream_sink.clone();
                let stream_sink_apply = stream_sink.clone();
                let endpoint = ManagedEndpointTask::spawn_stream(
                    sys,
                    route,
                    format!("sink:{}:{}", ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM),
                    sink.shutdown.clone(),
                    move || stream_sink_ready.has_scheduled_stream_targets(),
                    move |events| {
                        let stream_sink_apply = stream_sink_apply.clone();
                        async move {
                            if let Err(err) = stream_sink_apply.ingest_stream_events(&events) {
                                log::error!("sink stream ingest failed: {:?}", err);
                            }
                        }
                    },
                );
                lock_or_recover(&sink.endpoint_tasks, "sink.with_boundaries.endpoint_tasks")
                    .push(endpoint);
            } else {
                log::error!(
                    "failed to resolve route lookup for {}.{}",
                    ROUTE_TOKEN_FS_META_EVENTS,
                    METHOD_STREAM
                );
            }
        }

        Ok(sink)
    }

    pub fn start_runtime_endpoints(
        &self,
        boundary: Arc<dyn ChannelIoSubset>,
        node_id: NodeId,
    ) -> Result<()> {
        if !lock_or_recover(&self.endpoint_tasks, "sink.start_runtime_endpoints").is_empty() {
            return Ok(());
        }

        let adapter =
            exchange_host_adapter(boundary.clone(), node_id.clone(), default_route_bindings());
        let routes = default_route_bindings();

        let query_state = self.state.clone();
        let node_id_cloned = node_id.clone();
        let query_root_specs = self.root_specs.clone();
        let query_host_object_grants = self.host_object_grants.clone();
        let query_visibility_lag = self.visibility_lag.clone();
        let query_pending_stream_events = self.pending_stream_events.clone();
        let query_unit_control = self.unit_control.clone();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META, METHOD_QUERY) {
            log::info!(
                "bound route listening on {}.{} for sink {}",
                ROUTE_TOKEN_FS_META,
                METHOD_QUERY,
                node_id_cloned.0
            );
            let endpoint = ManagedEndpointTask::spawn(
                boundary.clone(),
                route,
                format!("sink:{}:{}", ROUTE_TOKEN_FS_META, METHOD_QUERY),
                self.shutdown.clone(),
                move |requests| {
                    let query_state = query_state.clone();
                    let query_root_specs = query_root_specs.clone();
                    let query_host_object_grants = query_host_object_grants.clone();
                    let query_visibility_lag = query_visibility_lag.clone();
                    let query_pending_stream_events = query_pending_stream_events.clone();
                    let query_unit_control = query_unit_control.clone();
                    async move {
                        let mut responses = Vec::new();
                        for req in requests {
                            if let Ok(params) =
                                rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                            {
                                let sink_impl = SinkFileMeta {
                                    state: query_state.clone(),
                                    root_specs: query_root_specs.clone(),
                                    host_object_grants: query_host_object_grants.clone(),
                                    visibility_lag: query_visibility_lag.clone(),
                                    pending_stream_events: query_pending_stream_events.clone(),
                                    unit_control: query_unit_control.clone(),
                                    shutdown: CancellationToken::new(),
                                    endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
                                };
                                let mut events =
                                    sink_impl.materialized_query(&params).unwrap_or_default();
                                for event in &mut events {
                                    let mut meta = event.metadata().clone();
                                    meta.correlation_id = req.metadata().correlation_id;
                                    responses.push(Event::new(
                                        meta,
                                        Bytes::copy_from_slice(event.payload_bytes()),
                                    ));
                                }
                            } else {
                                log::warn!("bound route failed to parse InternalQueryRequest");
                            }
                        }
                        responses
                    }
                },
            );
            lock_or_recover(
                &self.endpoint_tasks,
                "sink.start_runtime_endpoints.query_tasks",
            )
            .push(endpoint);
        }

        let internal_query_state = self.state.clone();
        let internal_query_root_specs = self.root_specs.clone();
        let internal_query_host_object_grants = self.host_object_grants.clone();
        let internal_query_visibility_lag = self.visibility_lag.clone();
        let internal_query_pending_stream_events = self.pending_stream_events.clone();
        let internal_query_unit_control = self.unit_control.clone();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY) {
            log::info!(
                "bound route listening on {}.{} for sink {}",
                ROUTE_TOKEN_FS_META_INTERNAL,
                METHOD_SINK_QUERY,
                node_id_cloned.0
            );
            let endpoint = ManagedEndpointTask::spawn(
                boundary.clone(),
                route,
                format!(
                    "sink:{}:{}",
                    ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY
                ),
                self.shutdown.clone(),
                move |requests| {
                    let internal_query_state = internal_query_state.clone();
                    let internal_query_root_specs = internal_query_root_specs.clone();
                    let internal_query_host_object_grants =
                        internal_query_host_object_grants.clone();
                    let internal_query_visibility_lag = internal_query_visibility_lag.clone();
                    let internal_query_pending_stream_events =
                        internal_query_pending_stream_events.clone();
                    let internal_query_unit_control = internal_query_unit_control.clone();
                    async move {
                        let mut responses = Vec::new();
                        for req in requests {
                            if let Ok(params) =
                                rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                            {
                                let sink_impl = SinkFileMeta {
                                    state: internal_query_state.clone(),
                                    root_specs: internal_query_root_specs.clone(),
                                    host_object_grants: internal_query_host_object_grants.clone(),
                                    visibility_lag: internal_query_visibility_lag.clone(),
                                    pending_stream_events: internal_query_pending_stream_events
                                        .clone(),
                                    unit_control: internal_query_unit_control.clone(),
                                    shutdown: CancellationToken::new(),
                                    endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
                                };
                                let mut events =
                                    sink_impl.materialized_query(&params).unwrap_or_default();
                                for event in &mut events {
                                    let mut meta = event.metadata().clone();
                                    meta.correlation_id = req.metadata().correlation_id;
                                    responses.push(Event::new(
                                        meta,
                                        Bytes::copy_from_slice(event.payload_bytes()),
                                    ));
                                }
                            } else {
                                log::warn!("bound route failed to parse InternalQueryRequest");
                            }
                        }
                        responses
                    }
                },
            );
            lock_or_recover(
                &self.endpoint_tasks,
                "sink.start_runtime_endpoints.internal_query_tasks",
            )
            .push(endpoint);
        }

        let internal_status_state = self.state.clone();
        let internal_status_root_specs = self.root_specs.clone();
        let internal_status_host_object_grants = self.host_object_grants.clone();
        let internal_status_visibility_lag = self.visibility_lag.clone();
        let internal_status_pending_stream_events = self.pending_stream_events.clone();
        let internal_status_unit_control = self.unit_control.clone();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS) {
            log::info!(
                "bound route listening on {}.{} for sink {}",
                ROUTE_TOKEN_FS_META_INTERNAL,
                METHOD_SINK_STATUS,
                node_id_cloned.0
            );
            let endpoint = ManagedEndpointTask::spawn(
                boundary.clone(),
                route,
                format!(
                    "sink:{}:{}",
                    ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS
                ),
                self.shutdown.clone(),
                move |requests| {
                    let internal_status_state = internal_status_state.clone();
                    let internal_status_root_specs = internal_status_root_specs.clone();
                    let internal_status_host_object_grants =
                        internal_status_host_object_grants.clone();
                    let internal_status_visibility_lag = internal_status_visibility_lag.clone();
                    let internal_status_pending_stream_events =
                        internal_status_pending_stream_events.clone();
                    let internal_status_unit_control = internal_status_unit_control.clone();
                    async move {
                        let mut responses = Vec::new();
                        for req in requests {
                            let sink_impl = SinkFileMeta {
                                state: internal_status_state.clone(),
                                root_specs: internal_status_root_specs.clone(),
                                host_object_grants: internal_status_host_object_grants.clone(),
                                visibility_lag: internal_status_visibility_lag.clone(),
                                pending_stream_events: internal_status_pending_stream_events
                                    .clone(),
                                unit_control: internal_status_unit_control.clone(),
                                shutdown: CancellationToken::new(),
                                endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
                            };
                            if let Ok(snapshot) = sink_impl.status_snapshot()
                                && let Ok(payload) = rmp_serde::to_vec_named(&snapshot)
                            {
                                responses.push(Event::new(
                                    EventMetadata {
                                        origin_id: req.metadata().origin_id.clone(),
                                        timestamp_us: now_us(),
                                        logical_ts: None,
                                        correlation_id: req.metadata().correlation_id,
                                        ingress_auth: None,
                                        trace: None,
                                    },
                                    Bytes::from(payload),
                                ));
                            }
                        }
                        responses
                    }
                },
            );
            lock_or_recover(
                &self.endpoint_tasks,
                "sink.start_runtime_endpoints.internal_status_tasks",
            )
            .push(endpoint);
        }

        const FORCE_FIND_SOURCE_REPLY_IDLE_GRACE_MS: u64 = 750;
        let node_id_proxy = node_id.clone();
        let proxy_adapter = adapter.clone();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META, METHOD_FIND) {
            log::info!(
                "bound route listening on {}.{} for sink {}",
                ROUTE_TOKEN_FS_META,
                METHOD_FIND,
                node_id_proxy.0
            );
            let endpoint = ManagedEndpointTask::spawn(
                boundary.clone(),
                route,
                format!("sink:{}:{}", ROUTE_TOKEN_FS_META, METHOD_FIND),
                self.shutdown.clone(),
                move |requests| {
                    let node_id_proxy = node_id_proxy.clone();
                    let proxy_adapter = proxy_adapter.clone();
                    async move {
                        let mut responses = Vec::new();

                        for req in requests {
                            let _params = match rmp_serde::from_slice::<InternalQueryRequest>(
                                req.payload_bytes(),
                            ) {
                                Ok(p) => p,
                                Err(e) => {
                                    log::warn!(
                                        "find proxy failed to parse InternalQueryRequest: {:?}",
                                        e
                                    );
                                    responses.push(build_error_marker_event(
                                        &node_id_proxy,
                                        req.metadata().correlation_id,
                                        "find proxy invalid request payload",
                                    ));
                                    continue;
                                }
                            };

                            match proxy_adapter
                                .call_collect(
                                    ROUTE_TOKEN_FS_META_INTERNAL,
                                    METHOD_SOURCE_FIND,
                                    Bytes::copy_from_slice(req.payload_bytes()),
                                    Duration::from_secs(60),
                                    Duration::from_millis(FORCE_FIND_SOURCE_REPLY_IDLE_GRACE_MS),
                                )
                                .await
                            {
                                Ok(source_events) => {
                                    for event in source_events {
                                        let mut meta = event.metadata().clone();
                                        meta.correlation_id = req.metadata().correlation_id;
                                        responses.push(Event::new(
                                            meta,
                                            Bytes::copy_from_slice(event.payload_bytes()),
                                        ));
                                    }
                                }
                                Err(e) => {
                                    log::error!("find proxy upstream route call failed: {:?}", e);
                                    responses.push(build_error_marker_event(
                                        &node_id_proxy,
                                        req.metadata().correlation_id,
                                        "find proxy upstream request failed",
                                    ));
                                }
                            }
                        }
                        responses
                    }
                },
            );
            lock_or_recover(
                &self.endpoint_tasks,
                "sink.start_runtime_endpoints.find_tasks",
            )
            .push(endpoint);
        }

        let stream_state = self.state.clone();
        let stream_root_specs = self.root_specs.clone();
        let stream_host_object_grants = self.host_object_grants.clone();
        let stream_visibility_lag = self.visibility_lag.clone();
        let stream_unit_control = self.unit_control.clone();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM) {
            let stream_sink = Arc::new(SinkFileMeta {
                state: stream_state,
                root_specs: stream_root_specs,
                host_object_grants: stream_host_object_grants,
                visibility_lag: stream_visibility_lag,
                pending_stream_events: self.pending_stream_events.clone(),
                unit_control: stream_unit_control,
                shutdown: CancellationToken::new(),
                endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
            });
            let stream_sink_ready = stream_sink.clone();
            let stream_sink_apply = stream_sink.clone();
            let endpoint = ManagedEndpointTask::spawn_stream(
                boundary.clone(),
                route,
                format!("sink:{}:{}", ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM),
                self.shutdown.clone(),
                move || stream_sink_ready.has_scheduled_stream_targets(),
                move |events| {
                    let stream_sink_apply = stream_sink_apply.clone();
                    async move {
                        if let Err(err) = stream_sink_apply.ingest_stream_events(&events) {
                            log::error!("sink stream ingest failed: {:?}", err);
                        }
                    }
                },
            );
            lock_or_recover(
                &self.endpoint_tasks,
                "sink.start_runtime_endpoints.stream_tasks",
            )
            .push(endpoint);
        }

        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_ROOTS_CONTROL) {
            let sink = Arc::new(SinkFileMeta {
                state: self.state.clone(),
                root_specs: self.root_specs.clone(),
                host_object_grants: self.host_object_grants.clone(),
                visibility_lag: self.visibility_lag.clone(),
                pending_stream_events: self.pending_stream_events.clone(),
                unit_control: self.unit_control.clone(),
                shutdown: self.shutdown.clone(),
                endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
            });
            let endpoint = ManagedEndpointTask::spawn_stream(
                boundary,
                route,
                format!(
                    "sink:{}:{}",
                    ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_ROOTS_CONTROL
                ),
                self.shutdown.clone(),
                move || true,
                move |events| {
                    let sink = sink.clone();
                    async move {
                        for event in events {
                            let payload =
                                match decode_logical_roots_control_payload(event.payload_bytes()) {
                                    Ok(payload) => payload,
                                    Err(err) => {
                                        log::warn!(
                                            "sink logical-roots control decode failed: {:?}",
                                            err
                                        );
                                        continue;
                                    }
                                };
                            let grants = match sink.logical_grants_snapshot() {
                                Ok(grants) => grants,
                                Err(err) => {
                                    log::warn!(
                                        "sink logical-roots control grants read failed: {:?}",
                                        err
                                    );
                                    continue;
                                }
                            };
                            if let Err(err) = sink.update_logical_roots(payload.roots, &grants) {
                                log::warn!("sink logical-roots control apply failed: {:?}", err);
                            }
                        }
                    }
                },
            );
            lock_or_recover(
                &self.endpoint_tasks,
                "sink.start_runtime_endpoints.roots_control_tasks",
            )
            .push(endpoint);
        }

        Ok(())
    }

    pub fn start_stream_endpoint(
        &self,
        boundary: Arc<dyn ChannelIoSubset>,
        node_id: NodeId,
    ) -> Result<()> {
        eprintln!(
            "fs_meta_sink: start_stream_endpoint requested node={}",
            node_id.0
        );
        if !lock_or_recover(&self.endpoint_tasks, "sink.start_stream_endpoint").is_empty() {
            eprintln!(
                "fs_meta_sink: start_stream_endpoint skipped node={} reason=already-started",
                node_id.0
            );
            return Ok(());
        }

        let routes = default_route_bindings();
        let stream_state = self.state.clone();
        let stream_root_specs = self.root_specs.clone();
        let stream_host_object_grants = self.host_object_grants.clone();
        let stream_visibility_lag = self.visibility_lag.clone();
        let stream_unit_control = self.unit_control.clone();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM) {
            eprintln!(
                "fs_meta_sink: start_stream_endpoint binding route={} node={}",
                route.0, node_id.0
            );
            log::info!(
                "bound stream route listening on {}.{} for sink {}",
                ROUTE_TOKEN_FS_META_EVENTS,
                METHOD_STREAM,
                node_id.0
            );
            let stream_sink = Arc::new(SinkFileMeta {
                state: stream_state,
                root_specs: stream_root_specs,
                host_object_grants: stream_host_object_grants,
                visibility_lag: stream_visibility_lag,
                pending_stream_events: self.pending_stream_events.clone(),
                unit_control: stream_unit_control,
                shutdown: self.shutdown.clone(),
                endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
            });
            let stream_sink_ready = stream_sink.clone();
            let stream_sink_apply = stream_sink.clone();
            let endpoint = ManagedEndpointTask::spawn_stream(
                boundary,
                route,
                format!("sink:{}:{}", ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM),
                self.shutdown.clone(),
                move || stream_sink_ready.has_scheduled_stream_targets(),
                move |events| {
                    let stream_sink_apply = stream_sink_apply.clone();
                    async move {
                        if let Err(err) = stream_sink_apply.ingest_stream_events(&events) {
                            log::error!("sink stream ingest failed: {:?}", err);
                        }
                    }
                },
            );
            lock_or_recover(
                &self.endpoint_tasks,
                "sink.start_stream_endpoint.endpoint_tasks",
            )
            .push(endpoint);
        }

        Ok(())
    }

    fn apply_activate_signal(
        &self,
        unit: SinkRuntimeUnit,
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
                "sink-file-meta: ignore stale activate unit={} generation={}",
                unit_id,
                generation
            );
        }
        Ok(())
    }

    fn scheduled_bound_scopes(&self) -> Result<Option<Vec<RuntimeBoundScope>>> {
        if !self.unit_control.has_runtime_state() {
            return Ok(None);
        }
        let scopes = match self.unit_control.unit_state(SINK_RUNTIME_UNIT_ID)? {
            Some((true, rows)) => rows,
            Some((false, _)) | None => Vec::new(),
        };
        Ok(Some(scopes))
    }

    fn scheduled_group_ids(&self) -> Result<Option<BTreeSet<String>>> {
        let Some(bound_scopes) = self.scheduled_bound_scopes()? else {
            return Ok(None);
        };
        Ok(Some(
            bound_scopes
                .into_iter()
                .map(|scope| scope.scope_id)
                .filter(|scope_id| !scope_id.trim().is_empty())
                .collect(),
        ))
    }

    pub fn scheduled_group_ids_snapshot(&self) -> Result<Option<BTreeSet<String>>> {
        self.scheduled_group_ids()
    }

    fn scheduled_stream_object_refs(&self) -> Result<Option<BTreeSet<String>>> {
        let Some(bound_scopes) = self.scheduled_bound_scopes()? else {
            return Ok(None);
        };
        let allowed_groups = bound_scopes
            .iter()
            .map(|scope| scope.scope_id.trim())
            .filter(|scope_id| !scope_id.is_empty())
            .map(|scope_id| scope_id.to_string())
            .collect::<BTreeSet<_>>();
        if allowed_groups.is_empty() {
            return Ok(Some(BTreeSet::new()));
        }
        let roots = self
            .root_specs
            .read()
            .map_err(|_| CnxError::Internal("Sink root_specs lock poisoned".into()))?
            .clone();
        let grants = self.logical_grants_snapshot()?;
        let mut object_refs = BTreeSet::new();
        for scope in &bound_scopes {
            if !allowed_groups.contains(scope.scope_id.trim()) {
                continue;
            }
            for resource_id in &scope.resource_ids {
                let trimmed = resource_id.trim();
                if !trimmed.is_empty() {
                    object_refs.insert(trimmed.to_string());
                }
            }
        }
        for root in roots {
            if !allowed_groups.contains(&root.id) {
                continue;
            }
            for grant in &grants {
                if root.selector.matches(grant) {
                    object_refs.insert(grant.object_ref.clone());
                }
            }
        }
        Ok(Some(object_refs))
    }

    fn reconcile_runtime_groups(&self, host_object_grants: &[GrantedMountRoot]) -> Result<()> {
        let roots = self
            .root_specs
            .read()
            .map_err(|_| CnxError::Internal("Sink root_specs lock poisoned".into()))?
            .clone();
        let allowed_groups = self.scheduled_group_ids()?;
        let mut state = self.state.write()?;
        state.reconcile_host_object_grants(&roots, host_object_grants, allowed_groups.as_ref());
        Ok(())
    }

    fn apply_deactivate_signal(
        &self,
        unit: SinkRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> Result<()> {
        let unit_id = unit.unit_id();
        let accepted = self
            .unit_control
            .apply_deactivate(unit_id, route_key, generation)?;
        if !accepted {
            log::debug!(
                "sink-file-meta: ignore stale deactivate unit={} generation={}",
                unit_id,
                generation
            );
        }
        Ok(())
    }

    fn accept_tick_signal(
        &self,
        unit: SinkRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> Result<()> {
        let unit_id = unit.unit_id();
        let accepted = self
            .unit_control
            .accept_tick(unit_id, route_key, generation)?;
        if !accepted {
            log::debug!(
                "sink-file-meta: ignore stale/inactive unit tick unit={} generation={}",
                unit_id,
                generation
            );
        }
        Ok(())
    }

    pub(crate) async fn apply_orchestration_signals(
        &self,
        signals: &[SinkControlSignal],
    ) -> Result<()> {
        let mut validated = 0usize;
        let mut pending_host_object_grants: Option<Vec<GrantedMountRoot>> = None;
        let mut refresh_runtime_groups = false;
        eprintln!(
            "fs_meta_sink: apply_orchestration_signals count={} has_runtime_state={}",
            signals.len(),
            self.unit_control.has_runtime_state()
        );
        for signal in signals {
            match signal {
                SinkControlSignal::Activate {
                    unit,
                    route_key,
                    generation,
                    bound_scopes,
                    ..
                } => {
                    self.apply_activate_signal(*unit, route_key, *generation, bound_scopes)?;
                    validated += 1;
                    refresh_runtime_groups = true;
                }
                SinkControlSignal::Deactivate {
                    unit,
                    route_key,
                    generation,
                    ..
                } => {
                    self.apply_deactivate_signal(*unit, route_key, *generation)?;
                    validated += 1;
                    refresh_runtime_groups = true;
                }
                SinkControlSignal::Tick {
                    unit,
                    route_key,
                    generation,
                    ..
                } => {
                    // Unit tick is runtime-owned scheduling signal.
                    // Sink accepts and validates envelope kind while keeping
                    // data interfaces independent.
                    self.accept_tick_signal(*unit, route_key, *generation)?;
                    validated += 1;
                }
                SinkControlSignal::RuntimeHostGrantChange { changed, .. } => {
                    let host_object_grants = changed
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
                        .collect::<Vec<_>>();
                    pending_host_object_grants = Some(host_object_grants);
                    validated += 1;
                }
                SinkControlSignal::Passthrough(_) => {
                    return Err(CnxError::NotSupported(
                        "sink-file-meta: unsupported control envelope".into(),
                    ));
                }
            }
        }

        eprintln!(
            "fs_meta_sink: orchestration validated={} refresh_runtime_groups={} scheduled_groups={:?} scheduled_stream_targets={:?}",
            validated,
            refresh_runtime_groups,
            self.scheduled_group_ids()?,
            self.scheduled_stream_object_refs()?
        );
        if let Some(host_object_grants) = pending_host_object_grants.as_ref() {
            *self.host_object_grants.write().map_err(|_| {
                CnxError::Internal("Sink host_object_grants lock poisoned".into())
            })? = host_object_grants.clone();
            self.state.record_authoritative_commit(
                "runtime.exec.host_object_grants.changed",
                format!(
                    "roots={} grants={}",
                    self.root_specs
                        .read()
                        .map_err(|_| CnxError::Internal("Sink root_specs lock poisoned".into()))?
                        .len(),
                    host_object_grants.len()
                ),
            );
        }
        if pending_host_object_grants.is_some() || refresh_runtime_groups {
            let grants = self.logical_grants_snapshot()?;
            self.reconcile_runtime_groups(&grants)?;
            self.flush_buffered_stream_events()?;
        }
        log::debug!("sink-file-meta accepted {} control envelope(s)", validated);
        Ok(())
    }

    fn logical_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {
        self.host_object_grants
            .read()
            .map(|guard| guard.clone())
            .map_err(|_| CnxError::Internal("Sink host_object_grants lock poisoned".into()))
    }

    pub fn update_logical_roots(
        &self,
        roots: Vec<RootSpec>,
        host_object_grants: &[GrantedMountRoot],
    ) -> Result<()> {
        let root_count = roots.len();
        let grant_count = host_object_grants.len();
        let bound_scopes = roots
            .iter()
            .map(|root| RuntimeBoundScope {
                scope_id: root.id.clone(),
                resource_ids: Vec::new(),
            })
            .collect::<Vec<_>>();
        self.unit_control
            .sync_active_scopes(SINK_RUNTIME_UNIT_ID, &bound_scopes)?;
        let mut root_specs = self
            .root_specs
            .write()
            .map_err(|_| CnxError::Internal("Sink root_specs lock poisoned".into()))?;
        let allowed_groups = self.scheduled_group_ids()?;
        let mut state = self.state.write()?;
        state.reconcile_host_object_grants(&roots, host_object_grants, allowed_groups.as_ref());
        *root_specs = roots;
        drop(state);
        drop(root_specs);
        self.flush_buffered_stream_events()?;
        self.state.record_authoritative_commit(
            "sink.update_logical_roots",
            format!("roots={} grants={}", root_count, grant_count),
        );
        Ok(())
    }

    pub fn logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {
        self.root_specs
            .read()
            .map(|guard| guard.clone())
            .map_err(|_| CnxError::Internal("Sink root_specs lock poisoned".into()))
    }

    /// Get health statistics for the entire sink.
    ///
    /// Returns aggregate counts of live nodes, tombstones, attested nodes,
    /// suspect nodes, blind spots, and the current shadow time.
    #[allow(dead_code)]
    pub fn health(&self) -> Result<HealthStats> {
        let state = self.state.read()?;
        let mut out = HealthStats::default();
        for group in state.groups.values() {
            let stats = query::get_health_stats(&group.tree, &group.clock);
            accumulate_health_stats(&mut out, &stats);
        }
        Ok(out)
    }

    pub fn status_snapshot(&self) -> Result<SinkStatusSnapshot> {
        let state = self.state.read()?;
        let now = now_us();
        let mut snapshot = SinkStatusSnapshot::default();
        let mut groups = Vec::with_capacity(state.groups.len());
        for (group_id, group) in &state.groups {
            let stats = query::get_health_stats(&group.tree, &group.clock);
            let estimated_heap_bytes = group.tree.estimated_heap_bytes();
            accumulate_status_snapshot(&mut snapshot, &stats, estimated_heap_bytes);
            groups.push(SinkGroupStatusSnapshot {
                group_id: group_id.clone(),
                primary_object_ref: group.primary_object_ref.clone(),
                total_nodes: group.tree.node_count() as u64,
                live_nodes: stats.live_nodes,
                tombstoned_count: stats.tombstoned_count,
                attested_count: stats.attested_count,
                suspect_count: stats.suspect_count,
                blind_spot_count: stats.blind_spot_count,
                shadow_time_us: stats.shadow_time_us,
                shadow_lag_us: if stats.shadow_time_us == 0 {
                    0
                } else {
                    now.saturating_sub(stats.shadow_time_us)
                },
                overflow_pending_audit: group.overflow_pending_audit,
                initial_audit_completed: group.initial_audit_completed,
                estimated_heap_bytes,
            });
        }
        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        snapshot.groups = groups;
        Ok(snapshot)
    }

    pub fn visibility_lag_samples_since(&self, since_us: u64) -> Vec<VisibilityLagSample> {
        lock_or_recover(
            &self.visibility_lag,
            "sink.visibility_lag_samples_since.visibility_lag",
        )
        .recent_since(since_us)
    }

    /// Query a single node by exact path.
    ///
    /// Returns `None` if the node doesn't exist or is tombstoned.
    pub fn query_node(&self, path: &[u8]) -> Result<Option<QueryNode>> {
        let state = self.state.read()?;
        for group in state.groups.values() {
            let node = group.tree.get(path);
            if let Some(n) = node
                && !n.is_tombstoned
            {
                return Ok(Some(query_node_from_node(path, n)));
            }
        }
        Ok(None)
    }

    /// Get the current shadow clock value (microseconds).
    ///
    /// Reflects the high-water mark of all received event timestamps.
    #[allow(dead_code)]
    pub fn shadow_time_us(&self) -> Result<u64> {
        let state = self.state.read()?;
        let mut shadow = 0u64;
        for group in state.groups.values() {
            shadow = shadow.max(group.clock.now_us());
        }
        Ok(shadow)
    }

    fn configured_stream_object_refs(&self) -> Result<BTreeSet<String>> {
        self.state
            .read()
            .map(|state| state.group_by_object_ref.keys().cloned().collect())
    }

    fn has_scheduled_stream_targets(&self) -> bool {
        self.scheduled_stream_object_refs()
            .map(|targets| targets.is_none_or(|targets| !targets.is_empty()))
            .unwrap_or(false)
    }

    fn ingest_stream_events(&self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let control_count = events
            .iter()
            .filter(|event| rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok())
            .count();
        eprintln!(
            "fs_meta_sink: ingest_stream_events received={} control={} data={} first_origin={}",
            events.len(),
            control_count,
            events.len().saturating_sub(control_count),
            events
                .first()
                .map(|event| event.metadata().origin_id.0.as_str())
                .unwrap_or("<none>")
        );

        let configured = self.configured_stream_object_refs()?;
        let scheduled = self.scheduled_stream_object_refs()?.unwrap_or_default();
        let mut ready = Vec::new();
        let mut deferred = VecDeque::new();
        let mut dropped = 0usize;

        for event in events.iter().cloned() {
            let object_ref = event.metadata().origin_id.0.clone();
            if configured.contains(&object_ref) {
                ready.push(event);
            } else if scheduled.contains(&object_ref) {
                deferred.push_back(event);
            } else {
                dropped += 1;
            }
        }

        if !deferred.is_empty() {
            let deferred_len = deferred.len();
            let mut pending = lock_or_recover(
                &self.pending_stream_events,
                "sink.ingest_stream_events.pending_stream_events",
            );
            pending.extend(deferred);
            log::debug!(
                "sink-file-meta deferred {} scheduled stream event(s) awaiting scope/grant convergence",
                deferred_len
            );
        }
        if dropped > 0 {
            log::debug!(
                "sink-file-meta dropped {} foreign stream event(s) outside scheduled scopes",
                dropped
            );
        }
        if !ready.is_empty() {
            self.apply_events(&ready)?;
        }
        Ok(())
    }

    fn flush_buffered_stream_events(&self) -> Result<()> {
        let configured = self.configured_stream_object_refs()?;
        let scheduled = self.scheduled_stream_object_refs()?.unwrap_or_default();
        let mut ready = Vec::new();
        let mut retained = VecDeque::new();
        let mut dropped = 0usize;
        {
            let mut pending = lock_or_recover(
                &self.pending_stream_events,
                "sink.flush_buffered_stream_events.pending_stream_events",
            );
            if pending.is_empty() {
                return Ok(());
            }
            while let Some(event) = pending.pop_front() {
                let object_ref = event.metadata().origin_id.0.clone();
                if configured.contains(&object_ref) {
                    ready.push(event);
                } else if scheduled.contains(&object_ref) {
                    retained.push_back(event);
                } else {
                    dropped += 1;
                }
            }
            *pending = retained;
        }
        if dropped > 0 {
            log::debug!(
                "sink-file-meta discarded {} stale buffered stream event(s) after scope change",
                dropped
            );
        }
        if !ready.is_empty() {
            self.apply_events(&ready)?;
        }
        Ok(())
    }

    fn apply_events(&self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let runtime_scoped = self.unit_control.has_runtime_state();
        let mut skipped_events = 0usize;
        let mut pending_lag_samples = Vec::new();
        let mut control_events = 0usize;
        let mut data_events = 0usize;
        let mut state = self.state.write()?;
        let mut accepted = Vec::with_capacity(events.len());

        for event in events {
            let object_ref = event.metadata().origin_id.0.as_str();
            match state.ensure_group_state_mut(object_ref) {
                Ok(_) => accepted.push(event),
                Err(err) if runtime_scoped => {
                    skipped_events += 1;
                    log::debug!(
                        "sink-file-meta ignored out-of-scope source event from {}: {}",
                        object_ref,
                        err
                    );
                }
                Err(err) => return Err(err),
            }
        }

        for event in accepted {
            let object_ref = event.metadata().origin_id.0.clone();
            let (group_id, is_group_primary, group_state) =
                state.ensure_group_state_mut(&object_ref)?;
            group_state.clock.advance(event.metadata().timestamp_us);

            let payload = event.payload_bytes();

            if let Ok(control) = rmp_serde::from_slice::<ControlEvent>(payload) {
                control_events += 1;
                match &control {
                    ControlEvent::WatchOverflow => {
                        group_state.overflow_pending_audit = true;
                    }
                    _ => {
                        if is_group_primary {
                            if let Some((completed_epoch_id, audit_start_time)) =
                                group_state.epoch_manager.process_control_event(&control)
                            {
                                epoch::missing_item_detection(
                                    &mut group_state.tree,
                                    completed_epoch_id,
                                    audit_start_time,
                                    &group_state.epoch_manager,
                                );
                                group_state.epoch_manager.clear_completed_audit_skip_state();
                            }
                            if matches!(
                                control,
                                ControlEvent::EpochEnd {
                                    epoch_type: crate::EpochType::Audit,
                                    ..
                                }
                            ) {
                                group_state.audit_epoch_completed = true;
                                if group_state.overflow_pending_audit {
                                    group_state.last_coverage_recovered_at = Some(Instant::now());
                                    group_state.materialized_revision =
                                        group_state.materialized_revision.saturating_add(1);
                                }
                                group_state.overflow_pending_audit = false;
                            }
                            group_state.refresh_initial_audit_completed();
                        }
                    }
                }
                continue;
            }

            let record: crate::FileMetaRecord = rmp_serde::from_slice(payload)
                .map_err(|e| CnxError::InvalidInput(format!("invalid file-meta payload: {e}")))?;
            data_events += 1;
            if record.audit_skipped {
                group_state
                    .epoch_manager
                    .mark_audit_skipped(record.path.clone());
            }

            let current_epoch = group_state.epoch_manager.current_epoch();
            let clock_snapshot = SinkClock {
                shadow_time_high_us: group_state.clock.now_us(),
            };
            let previous_node = group_state.tree.get(&record.path).cloned();
            let outcome = process_event(
                &record,
                &mut group_state.tree,
                &clock_snapshot,
                group_state.tombstone_policy,
                current_epoch,
            );
            let write_significant = match outcome {
                ProcessOutcome::UpsertCreated | ProcessOutcome::DeleteApplied => true,
                ProcessOutcome::UpsertModified => {
                    if record.audit_skipped {
                        false
                    } else if let (Some(before), Some(after)) =
                        (previous_node.as_ref(), group_state.tree.get(&record.path))
                    {
                        before.size != after.size
                            || before.modified_time_us != after.modified_time_us
                            || before.is_dir != after.is_dir
                            || before.is_tombstoned != after.is_tombstoned
                    } else {
                        previous_node.is_none()
                    }
                }
                ProcessOutcome::Ignored => false,
            };
            if write_significant {
                group_state
                    .tree
                    .mark_write_significant_change(&record.path, Instant::now());
                group_state.materialized_revision =
                    group_state.materialized_revision.saturating_add(1);
            }
            if let Some(sample) =
                sample_visibility_lag(&group_id, &object_ref, &record, outcome, now_us())
            {
                pending_lag_samples.push(sample);
            }
            group_state.refresh_initial_audit_completed();
        }
        drop(state);
        self.state.record_authoritative_commit(
            "sink.apply_event_batch",
            format!(
                "total={} control={} data={}",
                events.len(),
                control_events,
                data_events
            ),
        );

        if !pending_lag_samples.is_empty() {
            lock_or_recover(
                &self.visibility_lag,
                "sink.send.visibility_lag.record_batch",
            )
            .record_batch(pending_lag_samples);
        }

        if skipped_events > 0 {
            log::debug!(
                "sink-file-meta skipped {} out-of-scope event(s) while applying stream batch",
                skipped_events
            );
        }

        Ok(())
    }

    /// Domain-specific materialized query used by projection and tests.
    pub fn materialized_query(&self, request: &InternalQueryRequest) -> Result<Vec<Event>> {
        if request.transport != crate::query::request::QueryTransport::Materialized {
            return Err(CnxError::InvalidInput(
                "materialized_query requires materialized transport".into(),
            ));
        }
        let state = self.state.read()?;

        let dir_path = if request.scope.path.is_empty() {
            b"/".to_vec()
        } else {
            request.scope.path.clone()
        };

        let selected_group = request.scope.selected_group.as_deref();
        let tree_options = request.tree_options.clone().unwrap_or_default();
        let mut out = Vec::new();
        for (group_id, group) in &state.groups {
            if selected_group.is_some_and(|selected| selected != group_id.as_str()) {
                continue;
            }
            let payload = match request.op {
                QueryOp::Tree => {
                    let response = query::get_materialized_tree_payload(
                        &group.tree,
                        &dir_path,
                        &group.clock,
                        group.overflow_pending_audit,
                        request.scope.recursive,
                        request.scope.max_depth,
                        tree_options.read_class,
                        group.last_coverage_recovered_at,
                    );
                    rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(response))
                }
                QueryOp::Stats => {
                    let stats = query::get_subtree_stats_for_query(&group.tree, &dir_path);
                    rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(stats))
                }
            }
            .map_err(|e| CnxError::Internal(format!("serialize query response failed: {e}")))?;
            out.push(Event::new(
                EventMetadata {
                    origin_id: NodeId(group_id.clone()),
                    timestamp_us: group.clock.now_us(),
                    logical_ts: None,
                    correlation_id: None,
                    ingress_auth: None,
                    trace: None,
                },
                Bytes::from(payload),
            ));
        }
        Ok(out)
    }

    /// Domain-specific query: get aggregate stats for a subtree.
    ///
    /// This is a domain helper, not part of the `RuntimeBoundaryApp` trait. Used by
    /// RPC request handlers for Forest `op:"stats"` requests.
    pub fn subtree_stats(&self, path: &[u8]) -> Result<Vec<Event>> {
        let state = self.state.read()?;

        let dir_path = if path.is_empty() { b"/" as &[u8] } else { path };

        let mut out = Vec::new();
        for (group_id, group) in &state.groups {
            let stats = query::get_subtree_stats_for_query(&group.tree, dir_path);
            let payload = rmp_serde::to_vec_named(&stats)
                .map_err(|e| CnxError::Internal(format!("serialize subtree stats failed: {e}")))?;
            out.push(Event::new(
                EventMetadata {
                    origin_id: NodeId(group_id.clone()),
                    timestamp_us: group.clock.now_us(),
                    logical_ts: None,
                    correlation_id: None,
                    ingress_auth: None,
                    trace: None,
                },
                Bytes::from(payload),
            ));
        }
        Ok(out)
    }
}

#[cfg(test)]
fn query_responses_by_origin_from_source_events(
    events: &[Event],
    query_path: &[u8],
) -> Result<BTreeMap<String, RawQueryResult>> {
    raw_query_results_by_origin_from_source_events(events, query_path)
}

#[cfg(test)]
fn query_response_from_source_events(
    events: &[Event],
    query_path: &[u8],
) -> Result<RawQueryResult> {
    let grouped = query_responses_by_origin_from_source_events(events, query_path)?;
    Ok(merge_query_responses(grouped.into_values().collect()))
}

impl SinkFileMeta {
    pub async fn send(&self, events: &[Event]) -> Result<()> {
        self.apply_events(events)
    }

    pub async fn recv(&self, _opts: RecvOpts) -> Result<Vec<Event>> {
        self.materialized_query(&InternalQueryRequest::default())
    }

    pub async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        let signals = sink_control_signals_from_envelopes(envelopes)?;
        self.apply_orchestration_signals(&signals).await.map(|_| ())
    }

    pub async fn close(&self) -> Result<()> {
        self.shutdown.cancel();
        let mut endpoint_tasks = std::mem::take(&mut *lock_or_recover(
            &self.endpoint_tasks,
            "sink.close.endpoints",
        ));
        for task in &mut endpoint_tasks {
            task.shutdown(Duration::from_secs(2)).await;
        }

        log::info!("sink-file-meta closing");
        if let Ok(state) = self.state.read() {
            let mut stats = HealthStats::default();
            let mut sentinel_markers = 0usize;
            for group in state.groups.values() {
                sentinel_markers += group.sentinel_health.len();
                let group_stats = query::get_health_stats(&group.tree, &group.clock);
                accumulate_health_stats(&mut stats, &group_stats);
            }
            log::info!(
                "Final health: {} live, {} tombstoned, {} attested, {} suspect, {} blind spots",
                stats.live_nodes,
                stats.tombstoned_count,
                stats.attested_count,
                stats.suspect_count,
                stats.blind_spot_count,
            );
            log::info!("Final group sentinel markers: {}", sentinel_markers);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EpochType;
    use crate::shared_types::query::UnreliableReason;
    use bytes::Bytes;
    use capanix_app_sdk::runtime::EventMetadata;
    use capanix_host_fs_types::UnixStat;
    use capanix_runtime_entry_sdk::control::{
        RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeExecDeactivate,
        RuntimeHostDescriptor, RuntimeHostGrant, RuntimeHostGrantChange, RuntimeHostGrantState,
        RuntimeHostObjectType, RuntimeObjectDescriptor, RuntimeUnitTick,
        encode_runtime_exec_control, encode_runtime_host_grant_change, encode_runtime_unit_tick,
    };
    fn default_materialized_request() -> InternalQueryRequest {
        InternalQueryRequest::default()
    }

    fn decode_tree_payload(event: &Event) -> TreeGroupPayload {
        match rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
            .expect("decode materialized query payload")
        {
            MaterializedQueryPayload::Tree(payload) => payload,
            MaterializedQueryPayload::Stats(_) => {
                panic!("unexpected stats payload while decoding tree response")
            }
        }
    }

    fn payload_contains_path(payload: &TreeGroupPayload, path: &[u8]) -> bool {
        payload.root.exists && payload.root.path == path
            || payload.entries.iter().any(|entry| entry.path == path)
    }

    fn mk_source_event(origin: &str, record: FileMetaRecord) -> Event {
        let payload = rmp_serde::to_vec_named(&record).expect("encode record");
        Event::new(
            EventMetadata {
                origin_id: NodeId(origin.to_string()),
                timestamp_us: record.unix_stat.mtime_us,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            Bytes::from(payload),
        )
    }

    fn mk_record(path: &[u8], file_name: &str, ts: u64, event_kind: EventKind) -> FileMetaRecord {
        FileMetaRecord::from_unix(
            path.to_vec(),
            file_name.as_bytes().to_vec(),
            UnixStat {
                is_dir: false,
                size: 1,
                mtime_us: ts,
                ctime_us: ts,
                dev: None,
                ino: None,
            },
            event_kind,
            true,
            crate::SyncTrack::Scan,
            b"/".to_vec(),
            ts,
            false,
        )
    }

    fn mk_control_event(origin: &str, control: ControlEvent, ts: u64) -> Event {
        let payload = rmp_serde::to_vec_named(&control).expect("encode control event");
        Event::new(
            EventMetadata {
                origin_id: NodeId(origin.to_string()),
                timestamp_us: ts,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            Bytes::from(payload),
        )
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

    fn build_single_group_sink() -> SinkFileMeta {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![granted_mount_root(
            "node-a::exp",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("build sink")
    }

    fn bound_scope(scope_id: &str) -> RuntimeBoundScope {
        RuntimeBoundScope {
            scope_id: scope_id.to_string(),
            resource_ids: Vec::new(),
        }
    }

    fn bound_scope_with_resources(scope_id: &str, resource_ids: &[&str]) -> RuntimeBoundScope {
        RuntimeBoundScope {
            scope_id: scope_id.to_string(),
            resource_ids: resource_ids.iter().map(|id| (*id).to_string()).collect(),
        }
    }

    fn host_object_grants_changed_envelope(
        version: u64,
        grants: &[GrantedMountRoot],
    ) -> ControlEnvelope {
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version,
            grants: grants
                .iter()
                .map(|grant| RuntimeHostGrant {
                    object_ref: grant.object_ref.clone(),
                    object_type: RuntimeHostObjectType::MountRoot,
                    interfaces: grant.interfaces.clone(),
                    host: RuntimeHostDescriptor {
                        host_ref: grant.host_ref.clone(),
                        host_ip: grant.host_ip.clone(),
                        host_name: grant.host_name.clone(),
                        site: grant.site.clone(),
                        zone: grant.zone.clone(),
                        host_labels: grant.host_labels.clone(),
                    },
                    object: RuntimeObjectDescriptor {
                        mount_point: grant.mount_point.display().to_string(),
                        fs_source: grant.fs_source.clone(),
                        fs_type: grant.fs_type.clone(),
                        mount_options: grant.mount_options.clone(),
                    },
                    grant_state: if grant.active {
                        RuntimeHostGrantState::Active
                    } else {
                        RuntimeHostGrantState::Revoked
                    },
                })
                .collect(),
        })
        .expect("encode runtime host object grants changed envelope")
    }

    #[tokio::test]
    async fn unit_tick_control_frame_is_accepted() {
        let sink = build_single_group_sink();
        let envelope = encode_runtime_unit_tick(&RuntimeUnitTick {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            generation: 1,
            at_ms: 1,
        })
        .expect("encode unit tick");

        sink.on_control_frame(&[envelope])
            .await
            .expect("sink should accept unit tick control frame");
    }

    #[tokio::test]
    async fn unit_tick_with_unknown_unit_id_is_rejected() {
        let sink = build_single_group_sink();
        let envelope = encode_runtime_unit_tick(&RuntimeUnitTick {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.unknown".to_string(),
            generation: 1,
            at_ms: 1,
        })
        .expect("encode unit tick");

        let err = sink
            .on_control_frame(&[envelope])
            .await
            .expect_err("unknown unit id must be rejected");
        assert!(matches!(err, CnxError::NotSupported(_)));
        assert!(err.to_string().contains("unsupported unit_id"));
    }

    #[tokio::test]
    async fn exec_activate_with_unknown_unit_id_is_rejected() {
        let sink = build_single_group_sink();
        let envelope =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.unknown".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: Vec::new(),
            }))
            .expect("encode exec activate");

        let err = sink
            .on_control_frame(&[envelope])
            .await
            .expect_err("unknown unit id must be rejected");
        assert!(matches!(err, CnxError::NotSupported(_)));
        assert!(err.to_string().contains("unsupported unit_id"));
    }

    #[tokio::test]
    async fn stale_deactivate_generation_is_ignored() {
        let sink = build_single_group_sink();
        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 5,
                expires_at_ms: 1,
                bound_scopes: Vec::new(),
            }))
            .expect("encode activate");
        sink.on_control_frame(&[activate])
            .await
            .expect("activate should pass");

        let stale_deactivate =
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 4,
                reason: "test".to_string(),
            }))
            .expect("encode deactivate");
        sink.on_control_frame(&[stale_deactivate])
            .await
            .expect("stale deactivate should be ignored");

        let state = sink.unit_control.snapshot("runtime.exec.sink");
        assert_eq!(state, Some((5, true)));
    }

    #[tokio::test]
    async fn stale_activate_generation_is_ignored() {
        let sink = build_single_group_sink();
        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 5,
                expires_at_ms: 1,
                bound_scopes: Vec::new(),
            }))
            .expect("encode activate");
        sink.on_control_frame(&[activate])
            .await
            .expect("activate should pass");

        let deactivate =
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 5,
                reason: "test".to_string(),
            }))
            .expect("encode deactivate");
        sink.on_control_frame(&[deactivate])
            .await
            .expect("deactivate should pass");

        let stale_activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 4,
                expires_at_ms: 1,
                bound_scopes: Vec::new(),
            }))
            .expect("encode stale activate");
        sink.on_control_frame(&[stale_activate])
            .await
            .expect("stale activate should be ignored");

        let state = sink.unit_control.snapshot("runtime.exec.sink");
        assert_eq!(state, Some((5, false)));
    }

    #[test]
    fn force_find_aggregation_delete_wins_on_same_mtime() {
        let update = mk_source_event("src", mk_record(b"/a.txt", "a.txt", 10, EventKind::Update));
        let delete = mk_source_event("src", mk_record(b"/a.txt", "a.txt", 10, EventKind::Delete));
        let query = query_response_from_source_events(&[update, delete], b"/")
            .expect("decode source events");
        assert!(query.nodes.is_empty());
    }

    #[test]
    fn force_find_stats_are_derived_from_aggregated_query() {
        let file = mk_source_event(
            "src",
            FileMetaRecord::scan_update(
                b"/a.txt".to_vec(),
                b"a.txt".to_vec(),
                UnixStat {
                    is_dir: false,
                    size: 42,
                    mtime_us: 10,
                    ctime_us: 10,
                    dev: None,
                    ino: None,
                },
                b"/".to_vec(),
                10,
                false,
            ),
        );
        let dir = mk_source_event(
            "src",
            FileMetaRecord::scan_update(
                b"/dir".to_vec(),
                b"dir".to_vec(),
                UnixStat {
                    is_dir: true,
                    size: 0,
                    mtime_us: 11,
                    ctime_us: 11,
                    dev: None,
                    ino: None,
                },
                b"/".to_vec(),
                11,
                false,
            ),
        );

        let query =
            query_response_from_source_events(&[file, dir], b"/").expect("decode source events");
        let stats = subtree_stats_from_query_response(&query);
        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.total_files, 1);
        assert_eq!(stats.total_dirs, 1);
        assert_eq!(stats.total_size, 42);
    }

    #[test]
    fn force_find_aggregation_rejects_invalid_source_payloads() {
        let bad = Event::new(
            EventMetadata {
                origin_id: NodeId("src".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            Bytes::from_static(b"not-msgpack-record"),
        );
        assert!(query_response_from_source_events(&[bad], b"/").is_err());
    }

    #[test]
    fn ensure_group_state_requires_explicit_group_mapping() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = Vec::new();
        let mut state = SinkState::new(&cfg);
        let err = match state.ensure_group_state_mut("node-a::nfs1") {
            Ok(_) => panic!("unknown object ref must fail closed"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("configured group mapping"));
    }

    #[test]
    fn group_primary_prefers_lowest_active_process_member() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![
            granted_mount_root("node-b::exp", "node-b", "10.0.0.12", "/mnt/nfs1", false),
            granted_mount_root("node-a::exp", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        ];

        let state = SinkState::new(&cfg);
        let group = state.groups.get("nfs1").expect("group must exist");
        assert_eq!(group.primary_object_ref, "node-a::exp");
    }

    #[test]
    fn update_logical_roots_repartitions_groups_with_current_exports() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("old-root", "/mnt/nfs1")];
        cfg.host_object_grants = vec![granted_mount_root(
            "node-a::exp",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];

        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg.clone())
            .expect("init sink");
        {
            let state = sink.state.read().expect("state lock");
            assert!(state.groups.contains_key("old-root"));
            assert!(!state.groups.contains_key("new-root"));
        }

        sink.update_logical_roots(
            vec![RootSpec::new("new-root", "/mnt/nfs1")],
            &cfg.host_object_grants,
        )
        .expect("update roots");

        let state = sink.state.read().expect("state lock");
        assert!(!state.groups.contains_key("old-root"));
        assert!(state.groups.contains_key("new-root"));
    }

    #[tokio::test]
    async fn activate_limits_sink_state_to_bound_scopes_only() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![
            granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
        ];
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope("root-a")],
            }))
            .expect("encode activate");

        sink.on_control_frame(&[activate])
            .await
            .expect("activate should pass");

        let state = sink.state.read().expect("state lock");
        assert!(state.groups.contains_key("root-a"));
        assert!(!state.groups.contains_key("root-b"));
        drop(state);

        let query_events = sink
            .materialized_query(&default_materialized_request())
            .expect("query state");
        let origins = query_events
            .iter()
            .map(|event| event.metadata().origin_id.0.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            origins,
            std::collections::BTreeSet::from(["root-a".to_string()])
        );
    }

    #[tokio::test]
    async fn sink_activate_can_expand_to_newly_scheduled_group() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![
            granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
        ];
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate_root_a =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope("root-a")],
            }))
            .expect("encode activate root-a");
        sink.on_control_frame(&[activate_root_a])
            .await
            .expect("activate root-a should pass");

        let activate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 2,
                bound_scopes: vec![bound_scope("root-a"), bound_scope("root-b")],
            }))
            .expect("encode activate both");
        sink.on_control_frame(&[activate_both])
            .await
            .expect("activate both should pass");

        let state = sink.state.read().expect("state lock");
        assert!(state.groups.contains_key("root-a"));
        assert!(state.groups.contains_key("root-b"));
        drop(state);

        let query_events = sink
            .materialized_query(&default_materialized_request())
            .expect("query state");
        let origins = query_events
            .iter()
            .map(|event| event.metadata().origin_id.0.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            origins,
            std::collections::BTreeSet::from(["root-a".to_string(), "root-b".to_string()])
        );
    }

    #[tokio::test]
    async fn stream_replays_buffered_events_after_grants_catch_up() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![granted_mount_root(
            "node-a::exp-a",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("root-a", &["node-a::exp-a"]),
                    bound_scope_with_resources("root-b", &["node-b::exp-b"]),
                ],
            }))
            .expect("encode activate");
        sink.on_control_frame(&[activate])
            .await
            .expect("activate should pass");

        sink.ingest_stream_events(&[
            mk_source_event(
                "node-a::exp-a",
                mk_record(b"/kept.txt", "kept.txt", 10, EventKind::Update),
            ),
            mk_source_event(
                "node-b::exp-b",
                mk_record(b"/delayed.txt", "delayed.txt", 11, EventKind::Update),
            ),
        ])
        .expect("stream ingest should defer only the scheduled-but-unmapped event");

        let before = sink
            .materialized_query(&default_materialized_request())
            .expect("query before grants catch up");
        let before_origins = before
            .iter()
            .map(|event| event.metadata().origin_id.0.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            before_origins,
            std::collections::BTreeSet::from(["root-a".to_string(), "root-b".to_string()])
        );
        let before_responses = before
            .iter()
            .map(|event| {
                (
                    event.metadata().origin_id.0.clone(),
                    decode_tree_payload(event),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let root_a_before = before_responses
            .get("root-a")
            .expect("root-a response should exist before grants catch up");
        assert!(payload_contains_path(root_a_before, b"/kept.txt"));
        let root_b_before = before_responses
            .get("root-b")
            .expect("root-b response should exist before grants catch up");
        assert!(!root_b_before.root.exists);
        assert!(root_b_before.entries.is_empty());

        let grants_changed = host_object_grants_changed_envelope(
            1,
            &[
                granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
                granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
            ],
        );
        sink.on_control_frame(&[grants_changed])
            .await
            .expect("grants change should flush buffered stream events");

        let query_events = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            loop {
                let query_events = sink
                    .materialized_query(&default_materialized_request())
                    .expect("query after grants catch up");
                let responses = query_events
                    .iter()
                    .map(decode_tree_payload)
                    .collect::<Vec<_>>();
                if responses
                    .iter()
                    .any(|response| payload_contains_path(response, b"/delayed.txt"))
                {
                    break query_events;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out waiting for buffered stream replay after grants catch up"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        };
        let origins = query_events
            .iter()
            .map(|event| event.metadata().origin_id.0.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            origins,
            std::collections::BTreeSet::from(["root-a".to_string(), "root-b".to_string()])
        );
        let responses = query_events
            .iter()
            .map(decode_tree_payload)
            .collect::<Vec<_>>();
        assert!(
            responses
                .iter()
                .any(|response| payload_contains_path(response, b"/kept.txt"))
        );
        assert!(
            responses
                .iter()
                .any(|response| payload_contains_path(response, b"/delayed.txt"))
        );
    }

    #[tokio::test]
    async fn runtime_scoped_send_ignores_out_of_scope_group_events() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![
            granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
        ];
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope("root-a")],
            }))
            .expect("encode activate root-a");
        sink.on_control_frame(&[activate])
            .await
            .expect("activate root-a should pass");

        sink.send(&[
            mk_source_event(
                "node-a::exp-a",
                mk_record(b"/kept.txt", "kept.txt", 10, EventKind::Update),
            ),
            mk_source_event(
                "node-b::exp-b",
                mk_record(b"/ignored.txt", "ignored.txt", 11, EventKind::Update),
            ),
        ])
        .await
        .expect("out-of-scope events should be ignored");

        let query_events = sink
            .materialized_query(&default_materialized_request())
            .expect("query state");
        assert_eq!(query_events.len(), 1, "only scheduled group should reply");
        let response = decode_tree_payload(&query_events[0]);
        assert!(payload_contains_path(&response, b"/kept.txt"));
        assert!(!payload_contains_path(&response, b"/ignored.txt"));
    }

    #[test]
    fn sink_state_authority_log_records_root_updates() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("root-1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![granted_mount_root(
            "node-a::exp",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];

        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg.clone())
            .expect("init sink");
        let before = sink.state.authority_log_len();
        assert!(before >= 1, "bootstrap should append authority record");

        sink.update_logical_roots(
            vec![RootSpec::new("root-2", "/mnt/nfs1")],
            &cfg.host_object_grants,
        )
        .expect("update roots");

        let after = sink.state.authority_log_len();
        assert!(
            after > before,
            "root update should append authority record (before={}, after={})",
            before,
            after
        );
    }

    #[tokio::test]
    async fn watch_overflow_marks_group_unreliable_until_audit_end() {
        let sink = build_single_group_sink();
        sink.send(&[mk_control_event(
            "node-a::exp",
            ControlEvent::WatchOverflow,
            1,
        )])
        .await
        .expect("apply overflow control event");

        let events = sink
            .materialized_query(&default_materialized_request())
            .expect("query response");
        let response = decode_tree_payload(&events[0]);
        assert!(!response.reliability.reliable);
        assert_eq!(
            response.reliability.unreliable_reason,
            Some(UnreliableReason::WatchOverflowPendingAudit)
        );

        sink.send(&[
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
        ])
        .await
        .expect("apply audit epoch boundaries");

        let events = sink
            .materialized_query(&default_materialized_request())
            .expect("query response");
        let response = decode_tree_payload(&events[0]);
        assert!(response.reliability.reliable);
        assert_eq!(response.reliability.unreliable_reason, None);
    }

    #[tokio::test]
    async fn watch_overflow_reason_has_higher_priority_than_node_level_reasons() {
        let sink = build_single_group_sink();
        sink.send(&[mk_source_event(
            "node-a::exp",
            mk_record(b"/a.txt", "a.txt", 10, EventKind::Update),
        )])
        .await
        .expect("apply scan record");

        sink.send(&[mk_control_event(
            "node-a::exp",
            ControlEvent::WatchOverflow,
            11,
        )])
        .await
        .expect("apply overflow control event");

        let events = sink
            .materialized_query(&default_materialized_request())
            .expect("query response");
        let response = decode_tree_payload(&events[0]);
        assert!(!response.reliability.reliable);
        assert_eq!(
            response.reliability.unreliable_reason,
            Some(UnreliableReason::WatchOverflowPendingAudit)
        );
    }

    #[tokio::test]
    async fn initial_audit_completion_waits_for_materialized_root() {
        let sink = build_single_group_sink();
        sink.send(&[
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                1,
            ),
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
        ])
        .await
        .expect("apply empty audit epoch");

        let snapshot = sink.status_snapshot().expect("sink status");
        assert_eq!(snapshot.groups.len(), 1);
        assert!(
            !snapshot.groups[0].initial_audit_completed,
            "audit epoch without a materialized root must stay not-ready"
        );

        sink.send(&[mk_source_event(
            "node-a::exp",
            mk_record(b"/ready.txt", "ready.txt", 3, EventKind::Update),
        )])
        .await
        .expect("materialize root path");

        let snapshot = sink
            .status_snapshot()
            .expect("sink status after root materializes");
        assert!(
            snapshot.groups[0].initial_audit_completed,
            "materialized root should unlock initial audit readiness after audit completion"
        );
    }
}
