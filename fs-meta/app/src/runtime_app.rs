use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use crate::api::facade_status::{
    FacadePendingReason, SharedFacadePendingStatus, SharedFacadePendingStatusCell,
    SharedFacadeServiceStateCell, shared_facade_pending_status_cell,
    shared_facade_service_state_cell,
};
use crate::api::{ApiControlGate, ApiRequestTracker};
use crate::domain_state::FacadeServiceState;
use crate::query::TreeGroupPayload;
use crate::query::observation::{
    ObservationTrustPolicy, candidate_group_observation_evidence, evaluate_observation_status,
};
use crate::query::reliability::GroupReliability;
use crate::query::tree::ObservationState;
use crate::query::tree::{TreePageRoot, TreeStability};
use crate::query::{InternalQueryRequest, MaterializedQueryPayload, QueryNode, SubtreeStats};
use crate::runtime::endpoint::ManagedEndpointTask;
use crate::runtime::execution_units;
use crate::runtime::orchestration::{
    FacadeControlSignal, FacadeRuntimeUnit, SinkControlSignal, SinkRuntimeUnit,
    SourceControlSignal, split_app_control_signals,
};
use crate::runtime::routes::{
    METHOD_QUERY, METHOD_SINK_QUERY, METHOD_SINK_QUERY_PROXY, METHOD_SINK_STATUS,
    METHOD_SOURCE_FIND, METHOD_SOURCE_STATUS, ROUTE_KEY_EVENTS, ROUTE_KEY_FACADE_CONTROL,
    ROUTE_KEY_FORCE_FIND, ROUTE_KEY_QUERY, ROUTE_KEY_SINK_QUERY_INTERNAL,
    ROUTE_KEY_SINK_QUERY_PROXY, ROUTE_KEY_SINK_ROOTS_CONTROL, ROUTE_KEY_SINK_STATUS_INTERNAL,
    ROUTE_KEY_SOURCE_FIND_INTERNAL, ROUTE_KEY_SOURCE_STATUS_INTERNAL, ROUTE_TOKEN_FS_META,
    ROUTE_TOKEN_FS_META_INTERNAL, default_route_bindings, sink_query_route_bindings_for,
};
use crate::runtime::unit_gate::RuntimeUnitGate;
use crate::workers::sink::{SinkFacade, SinkWorkerClientHandle};
use crate::workers::source::{SourceFacade, SourceWorkerClientHandle};
use crate::{FSMetaConfig, api, source};
use async_trait::async_trait;
#[cfg(test)]
use capanix_app_sdk::runtime::ConfigValue;
use capanix_app_sdk::runtime::{
    ControlEnvelope, EventMetadata, NodeId, RecvOpts, RuntimeWorkerBinding, RuntimeWorkerBindings,
    RuntimeWorkerLauncherKind, in_memory_state_boundary,
};
use capanix_app_sdk::worker::WorkerMode;
use capanix_app_sdk::{CnxError, Event, Result, RuntimeBoundary, RuntimeBoundaryApp};
use capanix_managed_state_sdk::{ManagedStateDeclaration, ManagedStateProfile};
use capanix_runtime_entry_sdk::advanced::boundary::{
    ChannelBoundary, ChannelIoSubset, StateBoundary, boundary_handles,
};
use capanix_runtime_entry_sdk::control::{
    RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeExecDeactivate,
    RuntimeUnitTick, decode_runtime_exec_control, encode_runtime_exec_control,
    encode_runtime_unit_tick,
};
use capanix_runtime_entry_sdk::worker_runtime::RuntimeWorkerClientFactory;
use capanix_runtime_entry_sdk::{RuntimeBootstrapContext, RuntimeLoadedServiceApp};
use capanix_service_sdk::AppBuilder;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::sink::SinkFileMeta;
#[cfg(test)]
use crate::sink::{SinkGroupStatusSnapshot, SinkStatusSnapshot};
#[cfg(test)]
use crate::source::config::SourceConfig;

// Top-level fs-meta runtime-entry/bootstrap glue lowers through
// `service-sdk -> runtime-entry-sdk -> app-sdk`; lower runtime mirror/control
// carriers stay behind the sanctioned helper layer.
const ACTIVE_FACADE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);
const FIXED_BIND_HANDOFF_LATE_REQUEST_GRACE: Duration = Duration::from_millis(25);
const SOURCE_CONTROL_RECOVERY_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_FRAME_LEASE_ACQUIRE_BUDGET: Duration = Duration::from_secs(1);
const CONTROL_FRAME_LEASE_RETRY_INTERVAL: Duration = Duration::from_millis(10);

struct FacadeActivation {
    route_key: String,
    generation: u64,
    resource_ids: Vec<String>,
    handle: api::ApiServerHandle,
}

#[derive(Clone)]
struct FacadeSpawnInProgress {
    route_key: String,
    resource_ids: Vec<String>,
}

impl FacadeSpawnInProgress {
    fn from_pending(pending: &PendingFacadeActivation) -> Self {
        Self {
            route_key: pending.route_key.clone(),
            resource_ids: pending.resource_ids.clone(),
        }
    }

    fn matches_pending(&self, pending: &PendingFacadeActivation) -> bool {
        self.route_key == pending.route_key && self.resource_ids == pending.resource_ids
    }
}

#[derive(Clone)]
struct ProcessFacadeClaim {
    owner_instance_id: u64,
    #[cfg_attr(not(test), allow(dead_code))]
    route_key: String,
    #[cfg_attr(not(test), allow(dead_code))]
    resource_ids: Vec<String>,
    bind_addr: String,
}

impl ProcessFacadeClaim {
    fn from_pending(owner_instance_id: u64, pending: &PendingFacadeActivation) -> Option<Self> {
        if facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr) {
            return None;
        }
        Some(Self {
            owner_instance_id,
            route_key: pending.route_key.clone(),
            resource_ids: pending.resource_ids.clone(),
            bind_addr: pending.resolved.bind_addr.clone(),
        })
    }

    fn matches_pending(&self, pending: &PendingFacadeActivation) -> bool {
        self.bind_addr == pending.resolved.bind_addr
    }
}

fn source_signal_generation(signal: &SourceControlSignal) -> u64 {
    match signal {
        SourceControlSignal::Activate { generation, .. }
        | SourceControlSignal::Deactivate { generation, .. }
        | SourceControlSignal::Tick { generation, .. } => *generation,
        SourceControlSignal::RuntimeHostGrantChange { .. }
        | SourceControlSignal::ManualRescan { .. }
        | SourceControlSignal::Passthrough(_) => 0,
    }
}

fn facade_signal_generation(signal: &FacadeControlSignal) -> u64 {
    match signal {
        FacadeControlSignal::Activate { generation, .. }
        | FacadeControlSignal::Deactivate { generation, .. }
        | FacadeControlSignal::Tick { generation, .. }
        | FacadeControlSignal::ExposureConfirmed { generation, .. } => *generation,
        FacadeControlSignal::RuntimeHostGrantChange { .. } | FacadeControlSignal::Passthrough => 0,
    }
}

fn facade_publication_signal_is_sink_status_activate(signal: &FacadeControlSignal) -> bool {
    matches!(
        signal,
        FacadeControlSignal::Activate { unit, route_key, .. }
            if matches!(unit, FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer)
                && route_key == &format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)
    )
}

fn sink_status_snapshot_has_ready_scheduled_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
) -> bool {
    let scheduled_groups = snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    if scheduled_groups.is_empty() {
        return false;
    }
    let ready_groups = snapshot
        .groups
        .iter()
        .filter(|group| group.initial_audit_completed)
        .map(|group| group.group_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    scheduled_groups.is_subset(&ready_groups)
}

fn sink_status_snapshot_ready_for_expected_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
    expected_groups: &std::collections::BTreeSet<String>,
) -> bool {
    if expected_groups.is_empty() {
        return false;
    }
    let scheduled_groups = snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    let ready_groups = snapshot
        .groups
        .iter()
        .filter(|group| group.initial_audit_completed)
        .map(|group| group.group_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    expected_groups.is_subset(&scheduled_groups) && expected_groups.is_subset(&ready_groups)
}

fn current_generation_sink_replay_tick(
    source_signals: &[SourceControlSignal],
    facade_signals: &[FacadeControlSignal],
) -> Option<SinkControlSignal> {
    let generation = source_signals
        .iter()
        .map(source_signal_generation)
        .chain(facade_signals.iter().map(facade_signal_generation))
        .find(|generation| *generation > 0)?;
    let route_key = format!("{}.stream", ROUTE_KEY_EVENTS);
    Some(SinkControlSignal::Tick {
        unit: SinkRuntimeUnit::Sink,
        route_key: route_key.clone(),
        generation,
        envelope: encode_runtime_unit_tick(&RuntimeUnitTick {
            route_key,
            unit_id: execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            generation,
            at_ms: 1,
        })
        .expect("encode synthetic sink replay tick"),
    })
}

#[derive(Clone)]
struct PendingFixedBindHandoffRegistrant {
    instance_id: u64,
    api_task: Arc<Mutex<Option<FacadeActivation>>>,
    pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
    pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
    facade_spawn_in_progress: Arc<Mutex<Option<FacadeSpawnInProgress>>>,
    facade_pending_status: SharedFacadePendingStatusCell,
    facade_service_state: SharedFacadeServiceStateCell,
    api_request_tracker: Arc<ApiRequestTracker>,
    api_control_gate: Arc<ApiControlGate>,
    node_id: NodeId,
    runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    source: Arc<SourceFacade>,
    sink: Arc<SinkFacade>,
    query_sink: Arc<SinkFacade>,
    query_runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
}

#[derive(Clone)]
struct ActiveFixedBindFacadeRegistrant {
    instance_id: u64,
    api_task: Arc<Mutex<Option<FacadeActivation>>>,
    api_request_tracker: Arc<ApiRequestTracker>,
    api_control_gate: Arc<ApiControlGate>,
}

#[derive(Clone)]
struct PendingFixedBindHandoffReady {
    instance_id: u64,
    registrant: PendingFixedBindHandoffRegistrant,
}

#[derive(Clone)]
#[cfg_attr(not(test), allow(dead_code))]
struct PendingFacadeActivation {
    route_key: String,
    generation: u64,
    resource_ids: Vec<String>,
    bound_scopes: Vec<RuntimeBoundScope>,
    group_ids: Vec<String>,
    runtime_managed: bool,
    runtime_exposure_confirmed: bool,
    resolved: api::config::ResolvedApiConfig,
}

fn default_runtime_worker_binding(
    role_id: &str,
    mode: WorkerMode,
    module_path: Option<&std::path::Path>,
) -> RuntimeWorkerBinding {
    RuntimeWorkerBinding {
        role_id: role_id.to_string(),
        mode,
        launcher_kind: match mode {
            WorkerMode::Embedded => RuntimeWorkerLauncherKind::Embedded,
            WorkerMode::External => RuntimeWorkerLauncherKind::WorkerHost,
        },
        module_path: match mode {
            WorkerMode::Embedded => None,
            WorkerMode::External => module_path.map(std::path::Path::to_path_buf),
        },
        socket_dir: None,
    }
}

fn local_runtime_worker_binding(role_id: &str) -> RuntimeWorkerBinding {
    default_runtime_worker_binding(role_id, WorkerMode::Embedded, None)
}

fn required_runtime_worker_binding(
    bindings: &RuntimeWorkerBindings,
    role_id: &str,
) -> Result<RuntimeWorkerBinding> {
    bindings.roles.get(role_id).cloned().ok_or_else(|| {
        CnxError::InvalidInput(format!(
            "compiled runtime worker bindings must declare role '{role_id}'"
        ))
    })
}

fn runtime_worker_client_bindings(
    bindings: &RuntimeWorkerBindings,
) -> Result<(RuntimeWorkerBinding, RuntimeWorkerBinding)> {
    let facade = required_runtime_worker_binding(bindings, "facade")?;
    if facade.mode != WorkerMode::Embedded {
        return Err(CnxError::InvalidInput(
            "runtime worker binding for 'facade' must remain embedded".into(),
        ));
    }
    let source = required_runtime_worker_binding(bindings, "source")?;
    let sink = required_runtime_worker_binding(bindings, "sink")?;
    Ok((source, sink))
}

pub(crate) fn shared_tokio_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        let worker_threads = std::thread::available_parallelism()
            .map(|n| n.get().clamp(1, 2))
            .unwrap_or(1);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("fs-meta-shared-runtime")
            .enable_all()
            .build()
            .expect("build shared fs-meta tokio runtime")
    })
}

pub(crate) fn block_on_shared_runtime<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T> + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(_) => std::thread::scope(|scope| {
            let join = scope.spawn(|| shared_tokio_runtime().block_on(fut));
            join.join()
                .expect("shared fs-meta runtime helper thread must not panic")
        }),
        Err(_) => shared_tokio_runtime().block_on(fut),
    }
}

fn debug_source_status_lifecycle_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_SOURCE_STATUS_LIFECYCLE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn debug_force_find_runner_capture_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_FORCE_FIND_RUNNER_CAPTURE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn debug_sink_query_route_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_SINK_QUERY_ROUTE_TRACE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn next_source_status_endpoint_trace_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

struct SourceStatusEndpointTraceGuard {
    route: String,
    correlation: Option<u64>,
    trace_id: u64,
    phase: &'static str,
    completed: bool,
}

impl SourceStatusEndpointTraceGuard {
    fn new(route: String, correlation: Option<u64>, trace_id: u64, phase: &'static str) -> Self {
        Self {
            route,
            correlation,
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

impl Drop for SourceStatusEndpointTraceGuard {
    fn drop(&mut self) {
        if debug_source_status_lifecycle_enabled() && !self.completed {
            eprintln!(
                "fs_meta_runtime_app: source status endpoint dropped route={} correlation={:?} trace_id={} phase={}",
                self.route, self.correlation, self.trace_id, self.phase
            );
        }
    }
}

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_micros() as u64,
        Err(_) => 0,
    }
}

fn summarize_groups_by_node(groups: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    groups
        .iter()
        .map(|(node_id, groups)| format!("{node_id}={}", groups.join("|")))
        .collect()
}

fn summarize_group_string_map(groups: &BTreeMap<String, String>) -> Vec<String> {
    groups
        .iter()
        .map(|(group_id, value)| format!("{group_id}={value}"))
        .collect()
}

fn summarize_counts_by_node(counts: &BTreeMap<String, u64>) -> Vec<String> {
    counts
        .iter()
        .map(|(node_id, count)| format!("{node_id}={count}"))
        .collect()
}

fn summarize_sink_groups(groups: &[crate::sink::SinkGroupStatusSnapshot]) -> Vec<String> {
    groups
        .iter()
        .map(|group| {
            format!(
                "{}:live={} total={} init={} rev={}",
                group.group_id,
                group.live_nodes,
                group.total_nodes,
                group.initial_audit_completed,
                group.materialized_revision
            )
        })
        .collect()
}

fn summarize_sink_status_endpoint(snapshot: &crate::sink::SinkStatusSnapshot) -> String {
    format!(
        "groups={} group_details={:?} scheduled={:?} received_batches={:?} received_events={:?} received_origin_counts={:?} stream_path_capture_target={:?} stream_received_batches={:?} stream_received_events={:?} stream_received_origin_counts={:?} stream_received_path_origin_counts={:?} stream_ready_origin_counts={:?} stream_ready_path_origin_counts={:?} stream_deferred_origin_counts={:?} stream_dropped_origin_counts={:?} stream_applied_batches={:?} stream_applied_events={:?} stream_applied_control_events={:?} stream_applied_data_events={:?} stream_applied_origin_counts={:?} stream_applied_path_origin_counts={:?} stream_last_applied_at_us={:?}",
        snapshot.groups.len(),
        summarize_sink_groups(&snapshot.groups),
        summarize_groups_by_node(&snapshot.scheduled_groups_by_node),
        summarize_counts_by_node(&snapshot.received_batches_by_node),
        summarize_counts_by_node(&snapshot.received_events_by_node),
        summarize_groups_by_node(&snapshot.received_origin_counts_by_node),
        snapshot.stream_path_capture_target,
        summarize_counts_by_node(&snapshot.stream_received_batches_by_node),
        summarize_counts_by_node(&snapshot.stream_received_events_by_node),
        summarize_groups_by_node(&snapshot.stream_received_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_received_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_ready_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_ready_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_deferred_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_dropped_origin_counts_by_node),
        summarize_counts_by_node(&snapshot.stream_applied_batches_by_node),
        summarize_counts_by_node(&snapshot.stream_applied_events_by_node),
        summarize_counts_by_node(&snapshot.stream_applied_control_events_by_node),
        summarize_counts_by_node(&snapshot.stream_applied_data_events_by_node),
        summarize_groups_by_node(&snapshot.stream_applied_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_applied_path_origin_counts_by_node),
        summarize_counts_by_node(&snapshot.stream_last_applied_at_us_by_node),
    )
}

fn summarize_source_observability_endpoint(
    snapshot: &crate::workers::source::SourceObservabilitySnapshot,
) -> String {
    format!(
        "scheduled_source_groups={:?} scheduled_scan_groups={:?} published_batches={:?} published_events={:?} published_control_events={:?} published_data_events={:?} last_published_at_us={:?} published_origin_counts={:?} published_path_origin_counts={:?}",
        summarize_groups_by_node(&snapshot.scheduled_source_groups_by_node),
        summarize_groups_by_node(&snapshot.scheduled_scan_groups_by_node),
        summarize_counts_by_node(&snapshot.published_batches_by_node),
        summarize_counts_by_node(&snapshot.published_events_by_node),
        summarize_counts_by_node(&snapshot.published_control_events_by_node),
        summarize_counts_by_node(&snapshot.published_data_events_by_node),
        summarize_counts_by_node(&snapshot.last_published_at_us_by_node),
        summarize_groups_by_node(&snapshot.published_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.published_path_origin_counts_by_node),
    )
}

fn should_emit_selected_group_empty_materialized_reply(
    _node_id: &NodeId,
    source_primary_by_group: &BTreeMap<String, String>,
    request: &InternalQueryRequest,
) -> bool {
    let Some(group_id) = request.scope.selected_group.as_deref() else {
        return false;
    };
    // Selected-group proxy requests must settle explicitly even on non-owner
    // peers; returning no reply here can leave fanout batches unresolved.
    source_primary_by_group.contains_key(group_id)
}

fn selected_group_empty_materialized_reply(
    request: &InternalQueryRequest,
    correlation_id: Option<u64>,
) -> Result<Option<Event>> {
    let Some(group_id) = request.scope.selected_group.as_ref() else {
        return Ok(None);
    };
    let payload = match request.op {
        crate::query::QueryOp::Tree => {
            rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(TreeGroupPayload {
                reliability: GroupReliability::from_reason(Some(
                    crate::shared_types::query::UnreliableReason::Unattested,
                )),
                stability: TreeStability::not_evaluated(),
                root: TreePageRoot {
                    path: request.scope.path.clone(),
                    size: 0,
                    modified_time_us: 0,
                    is_dir: true,
                    exists: false,
                    has_children: false,
                },
                entries: Vec::new(),
            }))
            .map_err(|err| CnxError::Internal(format!("encode empty tree payload failed: {err}")))?
        }
        crate::query::QueryOp::Stats => {
            rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats::default()))
                .map_err(|err| {
                    CnxError::Internal(format!("encode empty stats payload failed: {err}"))
                })?
        }
    };
    Ok(Some(Event::new(
        EventMetadata {
            origin_id: NodeId(group_id.clone()),
            timestamp_us: now_us(),
            logical_ts: None,
            correlation_id,
            ingress_auth: None,
            trace: None,
        },
        bytes::Bytes::from(payload),
    )))
}

fn explicit_empty_sink_status_reply(
    origin_id: &NodeId,
    correlation_id: Option<u64>,
) -> Result<Event> {
    let snapshot = crate::sink::SinkStatusSnapshot::default();
    sink_status_reply(&snapshot, origin_id, correlation_id)
}

fn sink_status_reply(
    snapshot: &crate::sink::SinkStatusSnapshot,
    origin_id: &NodeId,
    correlation_id: Option<u64>,
) -> Result<Event> {
    let payload = rmp_serde::to_vec_named(snapshot)
        .map_err(|err| CnxError::Internal(format!("encode sink-status payload failed: {err}")))?;
    Ok(Event::new(
        EventMetadata {
            origin_id: origin_id.clone(),
            timestamp_us: now_us(),
            logical_ts: None,
            correlation_id,
            ingress_auth: None,
            trace: None,
        },
        bytes::Bytes::from(payload),
    ))
}

fn selected_group_bridge_eligible_from_sink_status(
    request: &InternalQueryRequest,
    snapshot: &crate::sink::SinkStatusSnapshot,
) -> bool {
    let Some(selected_group) = request.scope.selected_group.as_ref() else {
        return true;
    };
    let trusted_root_tree_request = request.op == crate::query::QueryOp::Tree
        && request.scope.path.as_slice() == b"/"
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    snapshot.groups.iter().any(|group| {
        group.group_id == *selected_group
            && (group.live_nodes > 0
                || (trusted_root_tree_request
                    && group.initial_audit_completed
                    && group.total_nodes > 0))
    })
}

fn node_id_from_object_ref(object_ref: &str) -> Option<NodeId> {
    object_ref
        .split_once("::")
        .map(|(node_id, _)| NodeId(node_id.to_string()))
}

fn scheduled_sink_owner_node_for_group(
    snapshot: &crate::sink::SinkStatusSnapshot,
    group_id: &str,
) -> Option<NodeId> {
    let mut scheduled_nodes = snapshot
        .scheduled_groups_by_node
        .iter()
        .filter(|(_, groups)| groups.iter().any(|group| group == group_id))
        .map(|(node_id, _)| NodeId(node_id.clone()))
        .collect::<Vec<_>>();
    scheduled_nodes.sort_by(|a, b| a.0.cmp(&b.0));
    scheduled_nodes.dedup_by(|a, b| a.0 == b.0);

    if let Some(primary_node) = snapshot
        .groups
        .iter()
        .find(|group| group.group_id == group_id)
        .and_then(|group| node_id_from_object_ref(&group.primary_object_ref))
    {
        if scheduled_nodes.is_empty() || scheduled_nodes.iter().any(|node| node.0 == primary_node.0)
        {
            return Some(primary_node);
        }
    }

    scheduled_nodes.into_iter().next()
}

fn selected_group_bridge_readiness_rank(
    request: &InternalQueryRequest,
    snapshot: &crate::sink::SinkStatusSnapshot,
) -> u8 {
    let Some(selected_group) = request.scope.selected_group.as_deref() else {
        return 0;
    };
    let trusted_root_tree_request = request.op == crate::query::QueryOp::Tree
        && request.scope.path.as_slice() == b"/"
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    snapshot
        .groups
        .iter()
        .find(|group| group.group_id == selected_group)
        .map(|group| {
            if trusted_root_tree_request && group.initial_audit_completed && group.total_nodes > 0 {
                2
            } else if group.live_nodes > 0 {
                1
            } else {
                0
            }
        })
        .unwrap_or(0)
}

fn selected_group_sink_query_bridge_snapshot<'a>(
    request: &InternalQueryRequest,
    live_snapshot: Option<&'a crate::sink::SinkStatusSnapshot>,
    cached_snapshot: Option<&'a crate::sink::SinkStatusSnapshot>,
) -> Option<&'a crate::sink::SinkStatusSnapshot> {
    let live_eligible = live_snapshot
        .filter(|snapshot| selected_group_bridge_eligible_from_sink_status(request, snapshot));
    let cached_eligible = cached_snapshot
        .filter(|snapshot| selected_group_bridge_eligible_from_sink_status(request, snapshot));

    match (live_eligible, cached_eligible) {
        (Some(live), Some(cached)) if request.scope.selected_group.is_some() => {
            if selected_group_bridge_readiness_rank(request, cached)
                > selected_group_bridge_readiness_rank(request, live)
            {
                Some(cached)
            } else {
                Some(live)
            }
        }
        (Some(live), _) => Some(live),
        (_, Some(cached)) => Some(cached),
        _ => live_snapshot.or(cached_snapshot),
    }
}

fn selected_group_sink_query_bridge_bindings(
    request: &InternalQueryRequest,
    snapshot: Option<&crate::sink::SinkStatusSnapshot>,
) -> Arc<capanix_host_adapter_fs::PostBindDispatchTable> {
    let Some(selected_group) = request.scope.selected_group.as_deref() else {
        return default_route_bindings();
    };
    let Some(snapshot) = snapshot else {
        return default_route_bindings();
    };
    if !selected_group_bridge_eligible_from_sink_status(request, snapshot) {
        return default_route_bindings();
    }
    if let Some(owner_node) = scheduled_sink_owner_node_for_group(snapshot, selected_group) {
        return sink_query_route_bindings_for(&owner_node.0);
    }
    default_route_bindings()
}

fn selected_group_payload_has_materialized_data(
    request: &InternalQueryRequest,
    payload: &MaterializedQueryPayload,
) -> bool {
    let trusted_root_tree_request = request.op == crate::query::QueryOp::Tree
        && request.scope.path.as_slice() == b"/"
        && request.scope.recursive
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    match payload {
        MaterializedQueryPayload::Tree(payload) => {
            if trusted_root_tree_request {
                payload.root.exists && (payload.root.has_children || !payload.entries.is_empty())
            } else {
                payload.root.exists || payload.root.has_children || !payload.entries.is_empty()
            }
        }
        MaterializedQueryPayload::Stats(_) => false,
    }
}

fn should_bridge_selected_group_sink_query(
    request: &InternalQueryRequest,
    local_events: &[Event],
    local_selected_group_bridge_eligible: bool,
) -> bool {
    if request.op != crate::query::QueryOp::Tree || request.scope.selected_group.is_none() {
        return false;
    }
    let selected_group = request
        .scope
        .selected_group
        .as_deref()
        .expect("selected_group must be present for bridge decision");
    let trusted_tree_request = request.op == crate::query::QueryOp::Tree
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    let trusted_non_root_unbounded_tree_request = trusted_tree_request
        && request.scope.path.as_slice() != b"/"
        && request.scope.recursive
        && request.scope.max_depth.is_none();
    let selected_group_tree_has_materialized_data = local_events.iter().any(|event| {
        event.metadata().origin_id.0 == selected_group
            && matches!(
                rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
                Ok(payload) if selected_group_payload_has_materialized_data(request, &payload)
            )
    });
    if selected_group_tree_has_materialized_data {
        return false;
    }
    let has_selected_group_tree_payload = local_events.iter().any(|event| {
        event.metadata().origin_id.0 == selected_group
            && matches!(
                rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
                Ok(MaterializedQueryPayload::Tree(_))
            )
    });
    if has_selected_group_tree_payload && trusted_non_root_unbounded_tree_request {
        return false;
    }
    if has_selected_group_tree_payload && !trusted_tree_request {
        return false;
    }
    let has_materialized_tree_data = local_events.iter().any(|event| {
        matches!(
            rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
            Ok(payload) if selected_group_payload_has_materialized_data(request, &payload)
        )
    });
    if !local_selected_group_bridge_eligible {
        return trusted_tree_request
            && local_events.is_empty()
            && !selected_group_tree_has_materialized_data
            && !has_materialized_tree_data;
    }
    !has_materialized_tree_data
}

fn should_fail_closed_selected_group_empty_after_bridge_failure(
    request: &InternalQueryRequest,
    local_events: &[Event],
    local_selected_group_bridge_eligible: bool,
    err: &CnxError,
) -> bool {
    let trusted_tree_request = request.op == crate::query::QueryOp::Tree
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    let trusted_root_tree_request =
        trusted_tree_request && request.scope.path.as_slice() == b"/" && request.scope.recursive;
    let trusted_non_root_bounded_tree_request = trusted_tree_request
        && request.scope.path.as_slice() != b"/"
        && (!request.scope.recursive || request.scope.max_depth.is_some());
    let trusted_continuity_sensitive_tree_request =
        trusted_root_tree_request || trusted_non_root_bounded_tree_request;
    let continuity_gap = if trusted_tree_request
        && request.scope.path.as_slice() != b"/"
        && !request.scope.recursive
    {
        matches!(err, CnxError::ProtocolViolation(_))
    } else {
        matches!(
            err,
            CnxError::Timeout | CnxError::TransportClosed(_) | CnxError::ProtocolViolation(_)
        )
    };
    continuity_gap
        && trusted_continuity_sensitive_tree_request
        && should_bridge_selected_group_sink_query(
            request,
            local_events,
            local_selected_group_bridge_eligible,
        )
        && !local_events.iter().any(|event| {
            matches!(
                rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
                Ok(payload) if selected_group_payload_has_materialized_data(request, &payload)
            )
        })
}

fn selected_group_empty_materialized_reply_after_bridge_failure(
    request: &InternalQueryRequest,
    local_events: &[Event],
    local_selected_group_bridge_eligible: bool,
    err: &CnxError,
    correlation_id: Option<u64>,
) -> Result<Option<Event>> {
    let Some(selected_group) = request.scope.selected_group.as_deref() else {
        return Ok(None);
    };
    let trusted_non_root_bounded_tree_request = request.op == crate::query::QueryOp::Tree
        && request.scope.path.as_slice() != b"/"
        && request.scope.recursive
        && request.scope.max_depth.is_some()
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    if !trusted_non_root_bounded_tree_request
        || !should_fail_closed_selected_group_empty_after_bridge_failure(
            request,
            local_events,
            local_selected_group_bridge_eligible,
            err,
        )
    {
        return Ok(None);
    }
    let has_selected_group_explicit_empty_tree_payload = local_events.iter().any(|event| {
        event.metadata().origin_id.0 == selected_group
            && matches!(
                rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
                Ok(MaterializedQueryPayload::Tree(payload))
                    if payload.root.path == request.scope.path
                        && !payload.root.exists
                        && !payload.root.has_children
                        && payload.entries.is_empty()
            )
    });
    if !has_selected_group_explicit_empty_tree_payload {
        return Ok(None);
    }
    selected_group_empty_materialized_reply(request, correlation_id)
}

// Bridge from query-peer proxy to internal sink query must stay best-effort.
// Keep this timeout short so proxy requests are never pinned behind an
// unavailable internal sink-query route.
const SINK_QUERY_PROXY_BRIDGE_TIMEOUT: Duration = Duration::from_millis(750);
const SINK_QUERY_PROXY_BRIDGE_IDLE_GRACE: Duration = Duration::from_millis(150);

fn facade_route_key_matches(unit: FacadeRuntimeUnit, route_key: &str) -> bool {
    let sink_query_proxy_route = format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY);
    let sink_status_route = format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL);
    let source_status_route = format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
    let source_find_route = format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL);
    match unit {
        FacadeRuntimeUnit::Facade => route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL),
        FacadeRuntimeUnit::Query => {
            route_key == format!("{}.req", ROUTE_KEY_QUERY)
                || route_key == sink_query_proxy_route
                || route_key == sink_status_route
                || route_key == source_status_route
                || route_key == source_find_route
        }
        FacadeRuntimeUnit::QueryPeer => {
            route_key == sink_query_proxy_route
                || route_key == sink_status_route
                || route_key == source_status_route
                || route_key == source_find_route
        }
    }
}

fn preferred_internal_query_endpoint_units(
    query_active: bool,
    query_peer_active: bool,
    prefer_query_peer_first: bool,
) -> Vec<&'static str> {
    if prefer_query_peer_first && query_peer_active {
        vec![
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            execution_units::QUERY_RUNTIME_UNIT_ID,
        ]
    } else if query_active {
        vec![execution_units::QUERY_RUNTIME_UNIT_ID]
    } else if query_peer_active {
        vec![
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            execution_units::QUERY_RUNTIME_UNIT_ID,
        ]
    } else {
        Vec::new()
    }
}

fn is_dual_lane_internal_query_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)
        || route_key == format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL)
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL)
}

fn internal_query_route_still_active(facade_gate: &RuntimeUnitGate, route_key: &str) -> bool {
    let query_active = facade_gate
        .route_active(execution_units::QUERY_RUNTIME_UNIT_ID, route_key)
        .unwrap_or(false);
    let query_peer_active = facade_gate
        .route_active(execution_units::QUERY_PEER_RUNTIME_UNIT_ID, route_key)
        .unwrap_or(false);
    query_active || query_peer_active
}

fn is_internal_status_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL)
}

fn is_facade_dependent_query_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_QUERY)
        || route_key == format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)
        || route_key == format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL)
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL)
}

fn is_uninitialized_cleanup_query_route(route_key: &str) -> bool {
    is_internal_status_route(route_key)
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL)
}

fn is_per_peer_sink_query_request_route(route_key: &str) -> bool {
    let Some(request_route) = route_key.strip_suffix(".req") else {
        return false;
    };
    let Some((stem, version)) = ROUTE_KEY_SINK_QUERY_INTERNAL.rsplit_once(':') else {
        return request_route.starts_with(&format!("{ROUTE_KEY_SINK_QUERY_INTERNAL}."));
    };
    let Some(route_stem) = request_route.strip_suffix(&format!(":{version}")) else {
        return false;
    };
    route_stem.starts_with(&format!("{stem}."))
}

fn is_retryable_worker_control_reset(err: &CnxError) -> bool {
    matches!(err, CnxError::TransportClosed(_) | CnxError::Timeout)
        || matches!(
            err,
            CnxError::PeerError(message) | CnxError::Internal(message)
                if message.contains("operation timed out")
        )
        || matches!(
            err,
            CnxError::PeerError(message)
                if message.contains("transport closed")
                    && (message.contains("Connection reset by peer")
                        || message.contains("early eof")
                        || message.contains("Broken pipe")
                        || message.contains("bridge stopped"))
        )
        || matches!(
            err,
            CnxError::AccessDenied(message) | CnxError::PeerError(message)
                if message.contains("drained/fenced")
                    && message.contains("grant attachments")
        )
}

fn is_retryable_worker_transport_close_reset(err: &CnxError) -> bool {
    matches!(err, CnxError::TransportClosed(_) | CnxError::Timeout)
        || matches!(
            err,
            CnxError::PeerError(message) | CnxError::Internal(message)
                if message.contains("transport closed")
                    && (message.contains("Connection reset by peer")
                        || message.contains("early eof")
                        || message.contains("Broken pipe")
                        || message.contains("bridge stopped"))
        )
}

fn facade_bind_addr_is_ephemeral(bind_addr: &str) -> bool {
    bind_addr
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .is_some_and(|port| port == 0)
}

fn process_facade_claim_cell() -> &'static StdMutex<BTreeMap<String, ProcessFacadeClaim>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, ProcessFacadeClaim>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(BTreeMap::new()))
}

fn shared_api_request_tracker_for_config(config: &api::ApiConfig) -> Arc<ApiRequestTracker> {
    let mut fixed_bind_addrs = config
        .local_listener_resources
        .iter()
        .map(|resource| resource.bind_addr.clone())
        .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
        .collect::<Vec<_>>();
    fixed_bind_addrs.sort();
    fixed_bind_addrs.dedup();
    if fixed_bind_addrs.is_empty() {
        return Arc::new(ApiRequestTracker::default());
    }
    static CELL: OnceLock<StdMutex<BTreeMap<Vec<String>, Arc<ApiRequestTracker>>>> =
        OnceLock::new();
    let mut guard = match CELL.get_or_init(|| StdMutex::new(BTreeMap::new())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard
        .entry(fixed_bind_addrs)
        .or_insert_with(|| Arc::new(ApiRequestTracker::default()))
        .clone()
}

fn runtime_worker_binding_serial_key(binding: &RuntimeWorkerBinding) -> String {
    format!(
        "{}|{:?}|{:?}",
        binding.role_id, binding.mode, binding.launcher_kind
    )
}

fn shared_control_frame_serial_for_runtime(
    _node_id: &NodeId,
    source_worker_binding: &RuntimeWorkerBinding,
    sink_worker_binding: &RuntimeWorkerBinding,
) -> Arc<Mutex<()>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, Arc<Mutex<()>>>>> = OnceLock::new();
    let key = format!(
        "{}|{}",
        runtime_worker_binding_serial_key(source_worker_binding),
        runtime_worker_binding_serial_key(sink_worker_binding)
    );
    let mut guard = match CELL.get_or_init(|| StdMutex::new(BTreeMap::new())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn shared_control_frame_lease_path_for_runtime(
    node_id: &NodeId,
    source_worker_binding: &RuntimeWorkerBinding,
    sink_worker_binding: &RuntimeWorkerBinding,
) -> Option<std::path::PathBuf> {
    if source_worker_binding.mode != WorkerMode::External
        && sink_worker_binding.mode != WorkerMode::External
    {
        return None;
    }
    let lease_root = std::env::var("DATANIX_E2E_TMP_ROOT")
        .or_else(|_| std::env::var("CAPANIX_E2E_TMP_ROOT"))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir())
        .join("fs-meta-control-frame-leases");
    let node_token = node_id
        .0
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '-',
        })
        .collect::<String>();
    let serial_token = format!(
        "{}--{}",
        runtime_worker_binding_serial_key(source_worker_binding),
        runtime_worker_binding_serial_key(sink_worker_binding)
    )
    .chars()
    .map(|ch| match ch {
        'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
        _ => '-',
    })
    .collect::<String>();
    Some(lease_root.join(format!("control-frame-{node_token}-{serial_token}.lock")))
}

struct ControlFrameLeaseGuard {
    file: File,
}

impl ControlFrameLeaseGuard {
    fn flock(file: &File, op: libc::c_int) -> std::io::Result<()> {
        let rc = unsafe { libc::flock(std::os::fd::AsRawFd::as_raw_fd(file), op) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    fn try_acquire(path: &std::path::Path) -> std::io::Result<Option<Self>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        match Self::flock(&file, libc::LOCK_EX | libc::LOCK_NB) {
            Ok(()) => Ok(Some(Self { file })),
            Err(err)
                if matches!(
                    err.raw_os_error(),
                    Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN
                ) =>
            {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    async fn acquire(
        path: &std::path::Path,
        deadline: tokio::time::Instant,
        should_abort: impl Fn() -> bool,
    ) -> std::io::Result<Self> {
        loop {
            if should_abort() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    format!(
                        "control-frame lease acquisition aborted while waiting for {}",
                        path.display()
                    ),
                ));
            }
            if let Some(guard) = Self::try_acquire(path)? {
                return Ok(guard);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "control-frame lease acquisition exceeded {:?} for {}",
                        CONTROL_FRAME_LEASE_ACQUIRE_BUDGET,
                        path.display()
                    ),
                ));
            }
            tokio::time::sleep(std::cmp::min(
                CONTROL_FRAME_LEASE_RETRY_INTERVAL,
                deadline.saturating_duration_since(now),
            ))
            .await;
        }
    }
}

impl Drop for ControlFrameLeaseGuard {
    fn drop(&mut self) {
        let _ = Self::flock(&self.file, libc::LOCK_UN);
    }
}

type SharedSinkRouteClaims =
    std::collections::BTreeMap<(String, String), std::collections::BTreeSet<u64>>;
type SharedSourceRouteClaims =
    std::collections::BTreeMap<(String, String), std::collections::BTreeSet<u64>>;

fn shared_sink_route_claims_for_runtime(
    node_id: &NodeId,
    source_worker_binding: &RuntimeWorkerBinding,
    sink_worker_binding: &RuntimeWorkerBinding,
) -> Arc<Mutex<SharedSinkRouteClaims>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, Arc<Mutex<SharedSinkRouteClaims>>>>> =
        OnceLock::new();
    let key = format!(
        "{}|{}|{}",
        node_id.0,
        runtime_worker_binding_serial_key(source_worker_binding),
        runtime_worker_binding_serial_key(sink_worker_binding)
    );
    let mut guard = match CELL.get_or_init(|| StdMutex::new(BTreeMap::new())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(SharedSinkRouteClaims::default())))
        .clone()
}

fn shared_source_route_claims_for_runtime(
    node_id: &NodeId,
    source_worker_binding: &RuntimeWorkerBinding,
    sink_worker_binding: &RuntimeWorkerBinding,
) -> Arc<Mutex<SharedSourceRouteClaims>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, Arc<Mutex<SharedSourceRouteClaims>>>>> =
        OnceLock::new();
    let key = format!(
        "{}|{}|{}",
        node_id.0,
        runtime_worker_binding_serial_key(source_worker_binding),
        runtime_worker_binding_serial_key(sink_worker_binding)
    );
    let mut guard = match CELL.get_or_init(|| StdMutex::new(BTreeMap::new())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(SharedSourceRouteClaims::default())))
        .clone()
}

fn next_app_instance_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn clear_owned_process_facade_claim(instance_id: u64) {
    let mut guard = match process_facade_claim_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.retain(|_, claim| claim.owner_instance_id != instance_id);
}

#[cfg(test)]
fn clear_process_facade_claim_for_tests() {
    clear_owned_process_facade_claim(0);
    let mut guard = match process_facade_claim_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.clear();
    clear_pending_fixed_bind_handoff_ready_for_tests();
    let mut active_guard = match active_fixed_bind_facade_owner_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    active_guard.clear();
}

fn pending_fixed_bind_handoff_ready_cell()
-> &'static StdMutex<BTreeMap<String, PendingFixedBindHandoffReady>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, PendingFixedBindHandoffReady>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(BTreeMap::new()))
}

fn mark_pending_fixed_bind_handoff_ready(
    bind_addr: &str,
    registrant: PendingFixedBindHandoffRegistrant,
) {
    let mut guard = match pending_fixed_bind_handoff_ready_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.insert(
        bind_addr.to_string(),
        PendingFixedBindHandoffReady {
            instance_id: registrant.instance_id,
            registrant,
        },
    );
}

fn clear_pending_fixed_bind_handoff_ready(bind_addr: &str) {
    let mut guard = match pending_fixed_bind_handoff_ready_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.remove(bind_addr);
}

fn pending_fixed_bind_handoff_ready_for(bind_addr: &str, instance_id: u64) -> bool {
    let guard = match pending_fixed_bind_handoff_ready_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard
        .get(bind_addr)
        .is_some_and(|ready| ready.instance_id != instance_id)
}

fn pending_fixed_bind_handoff_registrant_for(
    bind_addr: &str,
    released_by_instance_id: u64,
) -> Option<PendingFixedBindHandoffRegistrant> {
    let guard = match pending_fixed_bind_handoff_ready_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.get(bind_addr).and_then(|ready| {
        (ready.instance_id != released_by_instance_id).then_some(ready.registrant.clone())
    })
}

fn active_fixed_bind_facade_owner_cell()
-> &'static StdMutex<BTreeMap<String, ActiveFixedBindFacadeRegistrant>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, ActiveFixedBindFacadeRegistrant>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(BTreeMap::new()))
}

fn mark_active_fixed_bind_facade_owner(
    bind_addr: &str,
    registrant: ActiveFixedBindFacadeRegistrant,
) {
    let mut guard = match active_fixed_bind_facade_owner_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.insert(bind_addr.to_string(), registrant);
}

fn clear_active_fixed_bind_facade_owner(bind_addr: &str, instance_id: u64) {
    let mut guard = match active_fixed_bind_facade_owner_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if guard
        .get(bind_addr)
        .is_some_and(|registrant| registrant.instance_id == instance_id)
    {
        guard.remove(bind_addr);
    }
}

fn active_fixed_bind_facade_owner_for(
    bind_addr: &str,
    requester_instance_id: u64,
) -> Option<ActiveFixedBindFacadeRegistrant> {
    let guard = match active_fixed_bind_facade_owner_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.get(bind_addr).and_then(|registrant| {
        (registrant.instance_id != requester_instance_id).then_some(registrant.clone())
    })
}

fn conflicting_process_facade_claim_for(
    bind_addr: &str,
    requester_instance_id: u64,
) -> Option<ProcessFacadeClaim> {
    let guard = match process_facade_claim_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.get(bind_addr).and_then(|claim| {
        (claim.owner_instance_id != requester_instance_id).then_some(claim.clone())
    })
}

fn clear_process_facade_claim_for_bind_addr(bind_addr: &str, owner_instance_id: u64) {
    let mut guard = match process_facade_claim_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if guard
        .get(bind_addr)
        .is_some_and(|claim| claim.owner_instance_id == owner_instance_id)
    {
        guard.remove(bind_addr);
    }
}

#[cfg(test)]
fn clear_pending_fixed_bind_handoff_ready_for_tests() {
    let mut guard = match pending_fixed_bind_handoff_ready_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.clear();
}

#[cfg(test)]
#[derive(Clone)]
struct FacadeShutdownStartHook {
    entered: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct RuntimeProxyRequestPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct RuntimeControlFrameStartHook {
    entered: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct FacadeDeactivatePauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct SourceApplyPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct SinkApplyPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
struct SourceApplyErrorQueueHook {
    errs: std::collections::VecDeque<CnxError>,
}

#[cfg(test)]
fn source_apply_entry_count_hook_cell() -> &'static StdMutex<Option<Arc<AtomicUsize>>> {
    static CELL: OnceLock<StdMutex<Option<Arc<AtomicUsize>>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_source_apply_entry_count_hook(count: Arc<AtomicUsize>) {
    let mut guard = match source_apply_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(count);
}

#[cfg(test)]
fn clear_source_apply_entry_count_hook() {
    let mut guard = match source_apply_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn note_source_apply_entry_for_tests() {
    let hook = {
        let guard = match source_apply_entry_count_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(count) = hook {
        count.fetch_add(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
fn sink_apply_entry_count_hook_cell() -> &'static StdMutex<Option<Arc<AtomicUsize>>> {
    static CELL: OnceLock<StdMutex<Option<Arc<AtomicUsize>>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_sink_apply_entry_count_hook(count: Arc<AtomicUsize>) {
    let mut guard = match sink_apply_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(count);
}

#[cfg(test)]
fn clear_sink_apply_entry_count_hook() {
    let mut guard = match sink_apply_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn note_sink_apply_entry_for_tests() {
    let hook = {
        let guard = match sink_apply_entry_count_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(count) = hook {
        count.fetch_add(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
fn local_sink_status_republish_helper_entry_count_hook_cell()
-> &'static StdMutex<Option<Arc<AtomicUsize>>> {
    static CELL: OnceLock<StdMutex<Option<Arc<AtomicUsize>>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_local_sink_status_republish_helper_entry_count_hook(count: Arc<AtomicUsize>) {
    let mut guard = match local_sink_status_republish_helper_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(count);
}

#[cfg(test)]
fn clear_local_sink_status_republish_helper_entry_count_hook() {
    let mut guard = match local_sink_status_republish_helper_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn note_local_sink_status_republish_helper_entry_for_tests() {
    let hook = {
        let guard = match local_sink_status_republish_helper_entry_count_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(count) = hook {
        count.fetch_add(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
#[derive(Clone)]
struct LocalSinkStatusRepublishProbePauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct LocalSinkStatusRepublishRetriggerPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
fn facade_shutdown_start_hook_cell() -> &'static StdMutex<Option<FacadeShutdownStartHook>> {
    static CELL: OnceLock<StdMutex<Option<FacadeShutdownStartHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_facade_shutdown_start_hook(hook: FacadeShutdownStartHook) {
    let mut guard = match facade_shutdown_start_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_facade_shutdown_start_hook() {
    let mut guard = match facade_shutdown_start_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn notify_facade_shutdown_started() {
    let hook = {
        let guard = match facade_shutdown_start_hook_cell().lock() {
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
fn runtime_proxy_request_pause_hook_cell()
-> &'static StdMutex<BTreeMap<&'static str, RuntimeProxyRequestPauseHook>> {
    static CELL: OnceLock<StdMutex<BTreeMap<&'static str, RuntimeProxyRequestPauseHook>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn install_runtime_proxy_request_pause_hook(
    label: &'static str,
    hook: RuntimeProxyRequestPauseHook,
) {
    let mut guard = match runtime_proxy_request_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.insert(label, hook);
}

#[cfg(test)]
fn clear_runtime_proxy_request_pause_hook(label: &'static str) {
    let mut guard = match runtime_proxy_request_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.remove(label);
}

#[cfg(test)]
async fn maybe_pause_runtime_proxy_request(label: &'static str) {
    let hook = {
        let guard = match runtime_proxy_request_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.get(label).cloned()
    };
    if let Some(hook) = hook {
        eprintln!(
            "fs_meta_runtime_app: runtime proxy pause hook hit label={}",
            label
        );
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
fn runtime_control_frame_start_hook_cell() -> &'static StdMutex<Option<RuntimeControlFrameStartHook>>
{
    static CELL: OnceLock<StdMutex<Option<RuntimeControlFrameStartHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_runtime_control_frame_start_hook(hook: RuntimeControlFrameStartHook) {
    let mut guard = match runtime_control_frame_start_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_runtime_control_frame_start_hook() {
    let mut guard = match runtime_control_frame_start_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn notify_runtime_control_frame_started() {
    let hook = {
        let guard = match runtime_control_frame_start_hook_cell().lock() {
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
fn facade_deactivate_pause_hook_cell()
-> &'static StdMutex<BTreeMap<(String, String), FacadeDeactivatePauseHook>> {
    static CELL: OnceLock<StdMutex<BTreeMap<(String, String), FacadeDeactivatePauseHook>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn install_facade_deactivate_pause_hook(
    unit_id: &str,
    route_key: &str,
    hook: FacadeDeactivatePauseHook,
) {
    let mut guard = match facade_deactivate_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.insert((unit_id.to_string(), route_key.to_string()), hook);
}

#[cfg(test)]
fn clear_facade_deactivate_pause_hook(unit_id: &str, route_key: &str) {
    let mut guard = match facade_deactivate_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.remove(&(unit_id.to_string(), route_key.to_string()));
}

#[cfg(test)]
async fn maybe_pause_facade_deactivate(unit_id: &str, route_key: &str) {
    let hook = {
        let guard = match facade_deactivate_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .get(&(unit_id.to_string(), route_key.to_string()))
            .cloned()
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
fn source_apply_pause_hook_cell() -> &'static StdMutex<Option<SourceApplyPauseHook>> {
    static CELL: OnceLock<StdMutex<Option<SourceApplyPauseHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_source_apply_pause_hook(hook: SourceApplyPauseHook) {
    let mut guard = match source_apply_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_source_apply_pause_hook() {
    let mut guard = match source_apply_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn source_apply_error_queue_hook_cell() -> &'static StdMutex<Option<SourceApplyErrorQueueHook>> {
    static CELL: OnceLock<StdMutex<Option<SourceApplyErrorQueueHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_source_apply_error_queue_hook(hook: SourceApplyErrorQueueHook) {
    let mut guard = match source_apply_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_source_apply_error_queue_hook() {
    let mut guard = match source_apply_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn take_source_apply_error_queue_hook() -> Option<CnxError> {
    let mut guard = match source_apply_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    let err = hook.errs.pop_front();
    if hook.errs.is_empty() {
        *guard = None;
    }
    err
}

#[cfg(test)]
async fn maybe_pause_before_source_apply() {
    let hook = {
        let guard = match source_apply_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
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
fn sink_apply_pause_hook_cell() -> &'static StdMutex<Option<SinkApplyPauseHook>> {
    static CELL: OnceLock<StdMutex<Option<SinkApplyPauseHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_sink_apply_pause_hook(hook: SinkApplyPauseHook) {
    let mut guard = match sink_apply_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_sink_apply_pause_hook() {
    let mut guard = match sink_apply_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
async fn maybe_pause_before_sink_apply() {
    let hook = {
        let guard = match sink_apply_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
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
fn local_sink_status_republish_probe_pause_hook_cell()
-> &'static StdMutex<Option<LocalSinkStatusRepublishProbePauseHook>> {
    static CELL: OnceLock<StdMutex<Option<LocalSinkStatusRepublishProbePauseHook>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_local_sink_status_republish_probe_pause_hook(
    hook: LocalSinkStatusRepublishProbePauseHook,
) {
    let mut guard = match local_sink_status_republish_probe_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_local_sink_status_republish_probe_pause_hook() {
    let mut guard = match local_sink_status_republish_probe_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
async fn maybe_pause_before_local_sink_status_republish_probe() {
    let hook = {
        let guard = match local_sink_status_republish_probe_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
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
fn local_sink_status_republish_retrigger_pause_hook_cell()
-> &'static StdMutex<Option<LocalSinkStatusRepublishRetriggerPauseHook>> {
    static CELL: OnceLock<StdMutex<Option<LocalSinkStatusRepublishRetriggerPauseHook>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_local_sink_status_republish_retrigger_pause_hook(
    hook: LocalSinkStatusRepublishRetriggerPauseHook,
) {
    let mut guard = match local_sink_status_republish_retrigger_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_local_sink_status_republish_retrigger_pause_hook() {
    let mut guard = match local_sink_status_republish_retrigger_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
async fn maybe_pause_after_local_sink_status_republish_retrigger() {
    let hook = {
        let guard = match local_sink_status_republish_retrigger_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        hook.entered.notify_waiters();
        let mut release = std::pin::pin!(hook.release.notified());
        std::future::poll_fn(|cx| {
            let _ = std::future::Future::poll(release.as_mut(), cx);
            std::task::Poll::Ready(())
        })
        .await;
        release.await;
    }
}

pub struct FSMetaApp {
    instance_id: u64,
    config: FSMetaConfig,
    node_id: NodeId,
    runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    source: Arc<SourceFacade>,
    sink: Arc<SinkFacade>,
    query_sink: Arc<SinkFacade>,
    pump_task: Mutex<Option<JoinHandle<()>>>,
    runtime_endpoint_tasks: Mutex<Vec<ManagedEndpointTask>>,
    runtime_endpoint_routes: Mutex<std::collections::BTreeSet<String>>,
    api_task: Arc<Mutex<Option<FacadeActivation>>>,
    pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
    facade_spawn_in_progress: Arc<Mutex<Option<FacadeSpawnInProgress>>>,
    retained_active_facade_continuity: AtomicBool,
    pending_fixed_bind_claim_release_followup: AtomicBool,
    pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
    api_request_tracker: Arc<ApiRequestTracker>,
    api_control_gate: Arc<ApiControlGate>,
    control_frame_serial: Arc<Mutex<()>>,
    shared_control_frame_serial: Arc<Mutex<()>>,
    facade_pending_status: SharedFacadePendingStatusCell,
    facade_service_state: SharedFacadeServiceStateCell,
    facade_gate: RuntimeUnitGate,
    mirrored_query_peer_routes: Arc<Mutex<std::collections::BTreeMap<String, u64>>>,
    control_initialized: AtomicBool,
    retained_source_control_state: Mutex<RetainedSourceControlState>,
    source_state_replay_required: AtomicBool,
    retained_sink_control_state: Mutex<RetainedSinkControlState>,
    sink_state_replay_required: AtomicBool,
    retained_suppressed_public_query_activates:
        Mutex<std::collections::BTreeMap<String, FacadeControlSignal>>,
    shared_source_route_claims: Arc<Mutex<SharedSourceRouteClaims>>,
    shared_sink_route_claims: Arc<Mutex<SharedSinkRouteClaims>>,
    control_frame_lease_path: Option<std::path::PathBuf>,
    control_init_lock: Mutex<()>,
    closing: AtomicBool,
}

#[derive(Default, Clone)]
struct RetainedSourceControlState {
    latest_host_grant_change: Option<SourceControlSignal>,
    active_by_route: std::collections::BTreeMap<(String, String), SourceControlSignal>,
}

#[derive(Default, Clone)]
struct RetainedSinkControlState {
    latest_host_grant_change: Option<SinkControlSignal>,
    active_by_route: std::collections::BTreeMap<(String, String), SinkControlSignal>,
}

impl FSMetaApp {
    fn sink_status_snapshot_summary_is_empty(snapshot: &crate::sink::SinkStatusSnapshot) -> bool {
        snapshot.groups.is_empty() && snapshot.scheduled_groups_by_node.is_empty()
    }

    fn sink_status_snapshot_has_convergence_evidence(
        snapshot: &crate::sink::SinkStatusSnapshot,
    ) -> bool {
        !snapshot.received_batches_by_node.is_empty()
            || !snapshot.received_events_by_node.is_empty()
            || !snapshot.received_origin_counts_by_node.is_empty()
            || !snapshot.stream_received_batches_by_node.is_empty()
            || !snapshot.stream_received_events_by_node.is_empty()
            || !snapshot.stream_received_origin_counts_by_node.is_empty()
            || !snapshot
                .stream_received_path_origin_counts_by_node
                .is_empty()
            || !snapshot.stream_ready_origin_counts_by_node.is_empty()
            || !snapshot.stream_ready_path_origin_counts_by_node.is_empty()
            || !snapshot.stream_deferred_origin_counts_by_node.is_empty()
            || !snapshot.stream_dropped_origin_counts_by_node.is_empty()
            || !snapshot.stream_applied_batches_by_node.is_empty()
            || !snapshot.stream_applied_events_by_node.is_empty()
            || !snapshot.stream_applied_control_events_by_node.is_empty()
            || !snapshot.stream_applied_data_events_by_node.is_empty()
            || !snapshot.stream_applied_origin_counts_by_node.is_empty()
            || !snapshot
                .stream_applied_path_origin_counts_by_node
                .is_empty()
            || !snapshot.stream_last_applied_at_us_by_node.is_empty()
    }

    async fn wait_for_sink_status_republish_readiness_after_recovery(
        &self,
        source_to_sink_convergence_pretriggered: bool,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        let summarize_cached_sink_status_after_scope_convergence_timeout = |app: &Self| {
            app.sink
                .cached_status_snapshot()
                .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                .unwrap_or_else(|err| format!("cached_sink_status_unavailable err={err}"))
        };
        let mut triggered_rescan_when_ready = source_to_sink_convergence_pretriggered;
        let mut post_replay_rescan_retry_pending = false;
        let mut retried_after_post_recovery_sink_timeout = false;
        let mut manual_rescan_requested_after_post_recovery_sink_timeout = false;
        loop {
            let scope_convergence_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let converged_groups = loop {
                if post_replay_rescan_retry_pending {
                    self.source.trigger_rescan_when_ready().await?;
                    triggered_rescan_when_ready = true;
                    post_replay_rescan_retry_pending = false;
                }
                let source_groups = self
                    .source
                    .scheduled_source_group_ids()
                    .await?
                    .unwrap_or_default();
                let scan_groups = self
                    .source
                    .scheduled_scan_group_ids()
                    .await?
                    .unwrap_or_default();
                let sink_groups = self.sink.scheduled_group_ids().await?.unwrap_or_default();
                if !source_groups.is_empty()
                    && source_groups == scan_groups
                    && source_groups == sink_groups
                {
                    break sink_groups;
                }
                if !triggered_rescan_when_ready {
                    self.source.trigger_rescan_when_ready().await?;
                    triggered_rescan_when_ready = true;
                }
                if tokio::time::Instant::now() >= scope_convergence_deadline {
                    return Err(CnxError::Internal(format!(
                        "runtime scope convergence not observed after retained replay: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
                    )));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            };

            let sink_readiness_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                match tokio::time::timeout(Duration::from_millis(250), self.sink.status_snapshot())
                    .await
                {
                    Ok(Ok(snapshot)) => {
                        let ready_groups = snapshot
                            .groups
                            .iter()
                            .filter(|group| group.initial_audit_completed)
                            .map(|group| group.group_id.clone())
                            .collect::<std::collections::BTreeSet<_>>();
                        if ready_groups == converged_groups {
                            return Ok(None);
                        }
                        if tokio::time::Instant::now() >= sink_readiness_deadline {
                            return Err(CnxError::Internal(format!(
                                "sink status readiness not restored after retained replay once runtime scope converged: {}",
                                summarize_sink_status_endpoint(&snapshot)
                            )));
                        }
                    }
                    Ok(Err(err))
                        if matches!(err, CnxError::Timeout)
                            && !retried_after_post_recovery_sink_timeout
                            && tokio::time::Instant::now() < sink_readiness_deadline =>
                    {
                        // A later source-only recovery can converge route scopes from primed
                        // cached groups before the post-recovery rescan has actually
                        // rematerialized baseline source publication. Request one explicit
                        // manual rescan before the final source->sink retry so sink readiness
                        // wait does not return on a scheduled-only zero-state sink.
                        if !manual_rescan_requested_after_post_recovery_sink_timeout {
                            self.source.publish_manual_rescan_signal().await?;
                            manual_rescan_requested_after_post_recovery_sink_timeout = true;
                        }
                        retried_after_post_recovery_sink_timeout = true;
                        post_replay_rescan_retry_pending = true;
                        break;
                    }
                    Ok(Err(_err)) if tokio::time::Instant::now() < sink_readiness_deadline => {}
                    Ok(Err(err)) if matches!(err, CnxError::Timeout) => {
                        if self
                            .sink
                            .cached_status_snapshot()
                            .ok()
                            .is_some_and(|snapshot| {
                                Self::sink_status_snapshot_summary_is_empty(&snapshot)
                            })
                        {
                            return Ok(Some(converged_groups));
                        }
                        return Err(CnxError::Internal(format!(
                            "sink status readiness not restored after retained replay once runtime scope converged: groups={:?} raw_timeout=true {}",
                            converged_groups,
                            summarize_cached_sink_status_after_scope_convergence_timeout(self)
                        )));
                    }
                    Ok(Err(err)) => return Err(err),
                    Err(_)
                        if !retried_after_post_recovery_sink_timeout
                            && tokio::time::Instant::now() < sink_readiness_deadline =>
                    {
                        if !manual_rescan_requested_after_post_recovery_sink_timeout {
                            self.source.publish_manual_rescan_signal().await?;
                            manual_rescan_requested_after_post_recovery_sink_timeout = true;
                        }
                        retried_after_post_recovery_sink_timeout = true;
                        post_replay_rescan_retry_pending = true;
                        break;
                    }
                    Err(_) if tokio::time::Instant::now() < sink_readiness_deadline => {}
                    Err(_) => {
                        if self
                            .sink
                            .cached_status_snapshot()
                            .ok()
                            .is_some_and(|snapshot| {
                                Self::sink_status_snapshot_summary_is_empty(&snapshot)
                            })
                        {
                            return Ok(Some(converged_groups));
                        }
                        return Err(CnxError::Internal(format!(
                            "sink status readiness not restored after retained replay once runtime scope converged: groups={:?} raw_timeout=true {}",
                            converged_groups,
                            summarize_cached_sink_status_after_scope_convergence_timeout(self)
                        )));
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    async fn wait_for_local_sink_status_republish_after_recovery_from_parts(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        expected_groups: &std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &[SinkControlSignal],
    ) -> Result<()> {
        #[cfg(test)]
        note_local_sink_status_republish_helper_entry_for_tests();
        let summarize_cached_sink_status = |sink: &Arc<SinkFacade>| {
            sink.cached_status_snapshot()
                .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                .unwrap_or_else(|err| format!("cached_sink_status_unavailable err={err}"))
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut post_return_source_to_sink_convergence_retrigger_pending = true;
        let mut post_return_source_to_sink_convergence_retrigger_count = 0usize;
        let mut post_return_manual_rescan_republished = false;
        let mut post_return_sink_replay_pending = false;
        let mut first_sink_probe_pending = true;
        loop {
            let source_groups = source
                .scheduled_source_group_ids()
                .await?
                .unwrap_or_default();
            let scan_groups = source.scheduled_scan_group_ids().await?.unwrap_or_default();
            let sink_groups = sink.scheduled_group_ids().await?.unwrap_or_default();
            let runtime_scope_converged = source_groups == *expected_groups
                && scan_groups == *expected_groups
                && sink_groups == *expected_groups;
            if !runtime_scope_converged {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            if let Ok(snapshot) = sink.cached_status_snapshot() {
                let ready_groups = snapshot
                    .groups
                    .iter()
                    .filter(|group| group.initial_audit_completed)
                    .map(|group| group.group_id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                if ready_groups == *expected_groups {
                    return Ok(());
                }
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(CnxError::Internal(format!(
                    "local sink status republish not restored after retained replay once runtime scope converged: expected_groups={expected_groups:?} {}",
                    summarize_cached_sink_status(&sink)
                )));
            }
            if post_return_source_to_sink_convergence_retrigger_pending {
                source.trigger_rescan_when_ready().await?;
                #[cfg(test)]
                maybe_pause_after_local_sink_status_republish_retrigger().await;
                post_return_source_to_sink_convergence_retrigger_pending = false;
                post_return_source_to_sink_convergence_retrigger_count += 1;
            }
            #[cfg(test)]
            if first_sink_probe_pending {
                maybe_pause_before_local_sink_status_republish_probe().await;
                first_sink_probe_pending = false;
            }
            if post_return_sink_replay_pending {
                sink.apply_orchestration_signals_with_total_timeout(
                    post_return_sink_replay_signals,
                    remaining,
                )
                .await?;
                post_return_sink_replay_pending = false;
            }
            match tokio::time::timeout(
                remaining.min(Duration::from_millis(350)),
                sink.status_snapshot_nonblocking(),
            )
            .await
            {
                Ok(Ok(snapshot)) => {
                    let ready_groups = snapshot
                        .groups
                        .iter()
                        .filter(|group| group.initial_audit_completed)
                        .map(|group| group.group_id.clone())
                        .collect::<std::collections::BTreeSet<_>>();
                    if ready_groups == *expected_groups {
                        return Ok(());
                    }
                }
                Ok(Err(_)) | Err(_) => {}
            }
            let remaining_after_nonblocking_probe =
                deadline.saturating_duration_since(tokio::time::Instant::now());
            if !remaining_after_nonblocking_probe.is_zero() {
                match tokio::time::timeout(
                    remaining_after_nonblocking_probe.min(Duration::from_millis(350)),
                    sink.status_snapshot(),
                )
                .await
                {
                    Ok(Ok(snapshot))
                        if sink_status_snapshot_ready_for_expected_groups(
                            &snapshot,
                            expected_groups,
                        ) =>
                    {
                        return Ok(());
                    }
                    Ok(Ok(_)) | Ok(Err(_)) | Err(_) => {}
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(CnxError::Internal(format!(
                    "local sink status republish not restored after retained replay once runtime scope converged: expected_groups={expected_groups:?} {}",
                    summarize_cached_sink_status(&sink)
                )));
            }
            if !post_return_manual_rescan_republished {
                // Retained sink replay can leave the local sink status lane effectively fresh
                // again. Force one baseline replay pass, then allow one more direct
                // source->sink retrigger so the helper waits on a real rematerialization
                // instead of timing out against an empty-but-scheduled local summary.
                source.publish_manual_rescan_signal().await?;
                post_return_manual_rescan_republished = true;
                post_return_sink_replay_pending = !post_return_sink_replay_signals.is_empty();
                if post_return_source_to_sink_convergence_retrigger_count < 2 {
                    source.trigger_rescan_when_ready().await?;
                    #[cfg(test)]
                    maybe_pause_after_local_sink_status_republish_retrigger().await;
                    post_return_source_to_sink_convergence_retrigger_count += 1;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_for_local_sink_status_republish_after_recovery(
        &self,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        let post_return_sink_replay_signals = self
            .current_generation_retained_sink_replay_signals_for_local_republish()
            .await;
        Self::wait_for_local_sink_status_republish_after_recovery_from_parts(
            self.source.clone(),
            self.sink.clone(),
            expected_groups,
            &post_return_sink_replay_signals,
        )
        .await
    }

    async fn current_generation_retained_sink_replay_signals_for_local_republish(
        &self,
    ) -> Vec<SinkControlSignal> {
        let generation = self
            .retained_sink_control_state
            .lock()
            .await
            .active_by_route
            .values()
            .filter_map(|signal| match signal {
                SinkControlSignal::Activate { generation, .. } => Some(*generation),
                _ => None,
            })
            .max();
        let Some(generation) = generation else {
            return Vec::new();
        };
        let route_key = format!("{}.stream", ROUTE_KEY_EVENTS);
        let tick = SinkControlSignal::Tick {
            unit: SinkRuntimeUnit::Sink,
            route_key: route_key.clone(),
            generation,
            envelope: encode_runtime_unit_tick(&RuntimeUnitTick {
                route_key,
                unit_id: execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                generation,
                at_ms: 1,
            })
            .expect("encode deferred local sink-status replay tick"),
        };
        self.sink_signals_with_replay(std::slice::from_ref(&tick))
            .await
    }

    async fn apply_deferred_sink_owned_query_peer_publication_signal_from_parts(
        facade_gate: RuntimeUnitGate,
        mirrored_query_peer_routes: Arc<Mutex<std::collections::BTreeMap<String, u64>>>,
        signal: FacadeControlSignal,
    ) -> Result<()> {
        let FacadeControlSignal::Activate {
            unit,
            route_key,
            generation,
            bound_scopes,
        } = signal
        else {
            return Ok(());
        };
        if !matches!(unit, FacadeRuntimeUnit::QueryPeer)
            || !facade_route_key_matches(unit, &route_key)
        {
            return Ok(());
        }
        let accepted =
            facade_gate.apply_activate(unit.unit_id(), &route_key, generation, &bound_scopes)?;
        if !accepted {
            return Ok(());
        }
        if is_dual_lane_internal_query_route(&route_key) {
            let query_active = facade_gate
                .unit_state(execution_units::QUERY_RUNTIME_UNIT_ID)?
                .map(|(active, _)| active)
                .unwrap_or(false);
            let mut mirrored = mirrored_query_peer_routes.lock().await;
            if mirrored.contains_key(&route_key) || !query_active {
                facade_gate.apply_activate(
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    &route_key,
                    generation,
                    &bound_scopes,
                )?;
                mirrored.insert(route_key, generation);
            }
        }
        Ok(())
    }

    async fn suppress_deferred_sink_owned_query_peer_publication_signals(
        &self,
        signals: &[FacadeControlSignal],
    ) -> Result<()> {
        for signal in signals {
            let FacadeControlSignal::Activate {
                unit, route_key, ..
            } = signal
            else {
                continue;
            };
            if !matches!(unit, FacadeRuntimeUnit::QueryPeer)
                || !Self::facade_publication_signal_is_sink_owned_query_peer_activate(signal)
            {
                continue;
            }
            self.facade_gate
                .clear_route(execution_units::QUERY_PEER_RUNTIME_UNIT_ID, route_key)?;
            if is_dual_lane_internal_query_route(route_key) {
                self.mirrored_query_peer_routes
                    .lock()
                    .await
                    .remove(route_key);
            }
        }
        Ok(())
    }

    fn spawn_deferred_sink_owned_query_peer_publication_after_local_sink_status_ready(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        expected_groups: std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: Vec<SinkControlSignal>,
        deferred_signals: Vec<FacadeControlSignal>,
        facade_gate: RuntimeUnitGate,
        mirrored_query_peer_routes: Arc<Mutex<std::collections::BTreeMap<String, u64>>>,
        api_control_gate: Arc<ApiControlGate>,
        control_ready_after_republish: bool,
    ) {
        tokio::spawn(async move {
            if let Err(err) = Self::wait_for_local_sink_status_republish_after_recovery_from_parts(
                source,
                sink,
                &expected_groups,
                &post_return_sink_replay_signals,
            )
            .await
            {
                eprintln!(
                    "fs_meta_runtime_app: deferred local sink-status republish wait failed err={}",
                    err
                );
                return;
            }
            for signal in deferred_signals {
                if let Err(err) =
                    Self::apply_deferred_sink_owned_query_peer_publication_signal_from_parts(
                        facade_gate.clone(),
                        mirrored_query_peer_routes.clone(),
                        signal,
                    )
                    .await
                {
                    eprintln!(
                        "fs_meta_runtime_app: deferred sink-owned peer publication failed err={}",
                        err
                    );
                    return;
                }
            }
            api_control_gate.set_ready(control_ready_after_republish);
        });
    }

    pub fn new<C>(config: C, node_id: NodeId) -> Result<Self>
    where
        C: Into<FSMetaConfig>,
    {
        Self::with_runtime_workers_and_state(
            config.into(),
            local_runtime_worker_binding("source"),
            local_runtime_worker_binding("sink"),
            node_id,
            None,
            None,
            in_memory_state_boundary(),
        )
    }

    pub fn with_boundaries<C>(
        config: C,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> Result<Self>
    where
        C: Into<FSMetaConfig>,
    {
        Self::with_runtime_workers_and_state(
            config.into(),
            local_runtime_worker_binding("source"),
            local_runtime_worker_binding("sink"),
            node_id,
            boundary,
            None,
            in_memory_state_boundary(),
        )
    }

    pub(crate) fn with_boundaries_and_state<C>(
        config: C,
        source_worker_binding: RuntimeWorkerBinding,
        sink_worker_binding: RuntimeWorkerBinding,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        ordinary_boundary: Option<Arc<dyn ChannelBoundary>>,
        state_boundary: Arc<dyn StateBoundary>,
    ) -> Result<Self>
    where
        C: Into<FSMetaConfig>,
    {
        Self::with_runtime_workers_and_state(
            config.into(),
            source_worker_binding,
            sink_worker_binding,
            node_id,
            boundary,
            ordinary_boundary,
            state_boundary,
        )
    }

    fn with_runtime_workers_and_state(
        config: FSMetaConfig,
        source_worker_binding: RuntimeWorkerBinding,
        sink_worker_binding: RuntimeWorkerBinding,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        ordinary_boundary: Option<Arc<dyn ChannelBoundary>>,
        state_boundary: Arc<dyn StateBoundary>,
    ) -> Result<Self> {
        let source_cfg = config.source.clone();
        let sink_source_cfg = config.source.clone();
        let api_request_tracker = shared_api_request_tracker_for_config(&config.api);
        let api_control_gate = Arc::new(ApiControlGate::new(false));
        let source = match source_worker_binding.mode {
            WorkerMode::Embedded => Arc::new(SourceFacade::local(Arc::new(
                source::FSMetaSource::with_boundaries_and_state(
                    source_cfg,
                    node_id.clone(),
                    boundary.clone(),
                    state_boundary.clone(),
                )?,
            ))),
            WorkerMode::External => match boundary.clone() {
                Some(channel_boundary) => {
                    let control_boundary = ordinary_boundary.clone().ok_or_else(|| {
                        CnxError::InvalidInput(
                            "source worker mode requires ordinary runtime-boundary injection"
                                .to_string(),
                        )
                    })?;
                    let worker_factory = RuntimeWorkerClientFactory::new(
                        control_boundary,
                        channel_boundary.clone(),
                        state_boundary.clone(),
                    );
                    Arc::new(SourceFacade::worker(Arc::new(
                        SourceWorkerClientHandle::new(
                            node_id.clone(),
                            config.source.clone(),
                            source_worker_binding.clone(),
                            worker_factory,
                        )?,
                    )))
                }
                None => {
                    return Err(CnxError::InvalidInput(
                        "source worker mode requires runtime-boundary injection".to_string(),
                    ));
                }
            },
        };
        let sink = match sink_worker_binding.mode {
            WorkerMode::Embedded => Arc::new(SinkFacade::local(Arc::new(
                SinkFileMeta::with_boundaries_and_state(
                    node_id.clone(),
                    boundary.clone(),
                    state_boundary.clone(),
                    sink_source_cfg.clone(),
                )?,
            ))),
            WorkerMode::External => match boundary.clone() {
                Some(channel_boundary) => {
                    let control_boundary = ordinary_boundary.clone().ok_or_else(|| {
                        CnxError::InvalidInput(
                            "sink worker mode requires ordinary runtime-boundary injection"
                                .to_string(),
                        )
                    })?;
                    let worker_factory = RuntimeWorkerClientFactory::new(
                        control_boundary,
                        channel_boundary.clone(),
                        state_boundary.clone(),
                    );
                    Arc::new(SinkFacade::worker(Arc::new(SinkWorkerClientHandle::new(
                        node_id.clone(),
                        sink_source_cfg.clone(),
                        sink_worker_binding.clone(),
                        worker_factory,
                    )?)))
                }
                None => {
                    return Err(CnxError::InvalidInput(
                        "sink worker mode requires runtime-boundary injection".to_string(),
                    ));
                }
            },
        };
        let query_sink = sink.clone();
        let shared_control_frame_serial = shared_control_frame_serial_for_runtime(
            &node_id,
            &source_worker_binding,
            &sink_worker_binding,
        );
        let shared_source_route_claims = shared_source_route_claims_for_runtime(
            &node_id,
            &source_worker_binding,
            &sink_worker_binding,
        );
        let shared_sink_route_claims = shared_sink_route_claims_for_runtime(
            &node_id,
            &source_worker_binding,
            &sink_worker_binding,
        );
        let control_frame_lease_path = shared_control_frame_lease_path_for_runtime(
            &node_id,
            &source_worker_binding,
            &sink_worker_binding,
        );
        Ok(Self {
            instance_id: next_app_instance_id(),
            config,
            node_id,
            runtime_boundary: boundary,
            source,
            sink,
            query_sink,
            pump_task: Mutex::new(None),
            runtime_endpoint_tasks: Mutex::new(Vec::new()),
            runtime_endpoint_routes: Mutex::new(std::collections::BTreeSet::new()),
            api_task: Arc::new(Mutex::new(None)),
            pending_facade: Arc::new(Mutex::new(None)),
            facade_spawn_in_progress: Arc::new(Mutex::new(None)),
            retained_active_facade_continuity: AtomicBool::new(false),
            pending_fixed_bind_claim_release_followup: AtomicBool::new(false),
            pending_fixed_bind_has_suppressed_dependent_routes: Arc::new(AtomicBool::new(false)),
            api_request_tracker,
            api_control_gate,
            control_frame_serial: Arc::new(Mutex::new(())),
            shared_control_frame_serial,
            facade_pending_status: shared_facade_pending_status_cell(),
            facade_service_state: shared_facade_service_state_cell(),
            facade_gate: RuntimeUnitGate::new(
                "fs-meta",
                &[
                    execution_units::FACADE_RUNTIME_UNIT_ID,
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                ],
            ),
            mirrored_query_peer_routes: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            control_initialized: AtomicBool::new(false),
            retained_source_control_state: Mutex::new(RetainedSourceControlState::default()),
            source_state_replay_required: AtomicBool::new(false),
            retained_sink_control_state: Mutex::new(RetainedSinkControlState::default()),
            sink_state_replay_required: AtomicBool::new(false),
            retained_suppressed_public_query_activates: Mutex::new(
                std::collections::BTreeMap::new(),
            ),
            shared_source_route_claims,
            shared_sink_route_claims,
            control_frame_lease_path,
            control_init_lock: Mutex::new(()),
            closing: AtomicBool::new(false),
        })
    }

    fn control_initialized(&self) -> bool {
        self.control_initialized.load(Ordering::Acquire)
    }

    fn current_facade_service_state_from_runtime_facts(
        control_ready: bool,
        publication_ready: bool,
        pending_facade_present: bool,
    ) -> FacadeServiceState {
        if pending_facade_present {
            FacadeServiceState::Pending
        } else if control_ready && publication_ready {
            FacadeServiceState::Serving
        } else {
            FacadeServiceState::Unavailable
        }
    }

    async fn current_facade_service_state(&self) -> FacadeServiceState {
        let pending_facade_present = self.pending_facade.lock().await.is_some()
            || self
                .facade_pending_status
                .read()
                .ok()
                .is_some_and(|status| status.is_some());
        let publication_ready = self.facade_publication_ready().await;
        let state = Self::current_facade_service_state_from_runtime_facts(
            self.api_control_gate.is_ready(),
            publication_ready,
            pending_facade_present,
        );
        *self
            .facade_service_state
            .write()
            .expect("write published facade service state") = state;
        state
    }
    fn should_initialize_from_control(
        source_signals: &[SourceControlSignal],
        sink_signals: &[SinkControlSignal],
        facade_signals: &[FacadeControlSignal],
    ) -> bool {
        source_signals
            .iter()
            .any(Self::source_signal_can_initialize)
            || sink_signals.iter().any(Self::sink_signal_can_initialize)
            || facade_signals
                .iter()
                .any(Self::facade_signal_can_initialize)
    }

    fn source_signal_can_initialize(signal: &SourceControlSignal) -> bool {
        matches!(
            signal,
            SourceControlSignal::Activate { .. }
                | SourceControlSignal::Deactivate { .. }
                | SourceControlSignal::Tick { .. }
        )
    }

    fn sink_signal_can_initialize(signal: &SinkControlSignal) -> bool {
        matches!(
            signal,
            SinkControlSignal::Activate { .. }
                | SinkControlSignal::Deactivate { .. }
                | SinkControlSignal::Tick { .. }
        )
    }

    fn sink_signal_is_cleanup_only(signal: &SinkControlSignal) -> bool {
        matches!(
            signal,
            SinkControlSignal::Deactivate { .. } | SinkControlSignal::Tick { .. }
        )
    }

    fn source_signal_is_restart_deferred_retire_pending(signal: &SourceControlSignal) -> bool {
        let SourceControlSignal::Deactivate { envelope, .. } = signal else {
            return false;
        };
        matches!(
            decode_runtime_exec_control(envelope),
            Ok(Some(RuntimeExecControl::Deactivate(deactivate)))
                if deactivate.reason == "restart_deferred_retire_pending"
                    && deactivate
                        .lease
                        .as_ref()
                        .and_then(|lease| lease.drain_started_at_ms)
                        .is_some()
        )
    }

    fn sink_signal_is_restart_deferred_retire_pending(signal: &SinkControlSignal) -> bool {
        let SinkControlSignal::Deactivate {
            route_key,
            envelope,
            ..
        } = signal
        else {
            return false;
        };
        if route_key != &format!("{}.stream", ROUTE_KEY_EVENTS)
            && !is_per_peer_sink_query_request_route(route_key)
        {
            return false;
        }
        matches!(
            decode_runtime_exec_control(envelope),
            Ok(Some(RuntimeExecControl::Deactivate(deactivate)))
                if deactivate.reason == "restart_deferred_retire_pending"
                    && deactivate
                        .lease
                        .as_ref()
                        .and_then(|lease| lease.drain_started_at_ms)
                        .is_some()
        )
    }

    fn sink_signal_requires_fail_closed_retry_after_generation_cutover(
        signal: &SinkControlSignal,
    ) -> bool {
        matches!(
            signal,
            SinkControlSignal::Activate { route_key, .. }
                | SinkControlSignal::Deactivate { route_key, .. }
                if route_key == &format!("{}.stream", ROUTE_KEY_EVENTS)
        ) || Self::sink_signal_is_restart_deferred_retire_pending(signal)
    }

    fn facade_signal_can_initialize(signal: &FacadeControlSignal) -> bool {
        matches!(signal, FacadeControlSignal::Activate { .. })
    }

    fn facade_signal_is_cleanup_only(signal: &FacadeControlSignal) -> bool {
        matches!(
            signal,
            FacadeControlSignal::Deactivate { .. }
                | FacadeControlSignal::Tick { .. }
                | FacadeControlSignal::ExposureConfirmed { .. }
        )
    }

    fn facade_signal_requires_shared_serial_while_uninitialized(
        signal: &FacadeControlSignal,
    ) -> bool {
        matches!(
            signal,
            FacadeControlSignal::Deactivate {
                unit: FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer,
                ..
            }
        )
    }

    fn not_ready_error() -> CnxError {
        CnxError::NotReady(
            "fs-meta request handling is unavailable until runtime control initializes the app"
                .into(),
        )
    }

    fn remaining_initialize_budget(deadline: Option<tokio::time::Instant>) -> Result<Duration> {
        match deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    Err(CnxError::Timeout)
                } else {
                    Ok(remaining)
                }
            }
            None => Ok(Duration::MAX),
        }
    }

    async fn initialize_from_control(
        &self,
        wait_for_source_worker_handoff: bool,
        wait_for_sink_worker_handoff: bool,
    ) -> Result<()> {
        self.initialize_from_control_with_deadline(
            wait_for_source_worker_handoff,
            wait_for_sink_worker_handoff,
            None,
        )
        .await
    }

    async fn initialize_from_control_with_deadline(
        &self,
        wait_for_source_worker_handoff: bool,
        wait_for_sink_worker_handoff: bool,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<()> {
        eprintln!(
            "fs_meta_runtime_app: initialize_from_control begin initialized={}",
            self.control_initialized()
        );
        if self.control_initialized() {
            return Ok(());
        }
        let _guard = self.control_init_lock.lock().await;
        if self.control_initialized() {
            return Ok(());
        }
        if !self.config.api.enabled {
            return Err(CnxError::InvalidInput(
                "api.enabled must be true; fs-meta management API boundary is mandatory".into(),
            ));
        }

        if self.sink.is_worker() {
            eprintln!("fs_meta_runtime_app: initialize_from_control sink.ensure_started begin");
            let ensure_started = self.sink.ensure_started();
            if let Some(deadline) = deadline {
                tokio::time::timeout(
                    Self::remaining_initialize_budget(Some(deadline))?,
                    ensure_started,
                )
                .await
                .map_err(|_| CnxError::Timeout)??;
            } else {
                ensure_started.await?;
            }
            eprintln!("fs_meta_runtime_app: initialize_from_control sink.ensure_started ok");
        }
        let mut guard = self.pump_task.lock().await;
        if guard.is_none() {
            if wait_for_source_worker_handoff && wait_for_sink_worker_handoff {
                if let Some(deadline) = deadline {
                    tokio::time::timeout(
                        Self::remaining_initialize_budget(Some(deadline))?,
                        self.wait_for_shared_worker_control_handoff(),
                    )
                    .await
                    .map_err(|_| CnxError::Timeout)?;
                } else {
                    self.wait_for_shared_worker_control_handoff().await;
                }
            } else if wait_for_source_worker_handoff {
                if let Some(deadline) = deadline {
                    tokio::time::timeout(
                        Self::remaining_initialize_budget(Some(deadline))?,
                        self.source.wait_for_control_ops_to_drain_for_handoff(),
                    )
                    .await
                    .map_err(|_| CnxError::Timeout)?;
                } else {
                    self.source
                        .wait_for_control_ops_to_drain_for_handoff()
                        .await;
                }
            } else if wait_for_sink_worker_handoff {
                if let Some(deadline) = deadline {
                    tokio::time::timeout(
                        Self::remaining_initialize_budget(Some(deadline))?,
                        self.sink.wait_for_control_ops_to_drain_for_handoff(),
                    )
                    .await
                    .map_err(|_| CnxError::Timeout)?;
                } else {
                    self.sink.wait_for_control_ops_to_drain_for_handoff().await;
                }
            }
            eprintln!("fs_meta_runtime_app: initialize_from_control source.start begin");
            let start = self
                .source
                .start(self.sink.clone(), self.runtime_boundary.clone());
            *guard = if let Some(deadline) = deadline {
                tokio::time::timeout(Self::remaining_initialize_budget(Some(deadline))?, start)
                    .await
                    .map_err(|_| CnxError::Timeout)??
            } else {
                start.await?
            };
            eprintln!("fs_meta_runtime_app: initialize_from_control source.start ok");
        }
        drop(guard);

        eprintln!("fs_meta_runtime_app: initialize_from_control endpoints begin");
        let ensure_endpoints = self.ensure_runtime_proxy_endpoints_started();
        if let Some(deadline) = deadline {
            tokio::time::timeout(
                Self::remaining_initialize_budget(Some(deadline))?,
                ensure_endpoints,
            )
            .await
            .map_err(|_| CnxError::Timeout)??;
        } else {
            ensure_endpoints.await?;
        }
        eprintln!("fs_meta_runtime_app: initialize_from_control endpoints ok");
        self.control_initialized.store(true, Ordering::Release);
        self.api_control_gate
            .set_ready(self.facade_publication_ready().await);
        let _ = self.current_facade_service_state().await;
        eprintln!("fs_meta_runtime_app: initialize_from_control done");

        Ok(())
    }

    pub async fn start(&self) -> Result<()> {
        self.initialize_from_control(true, true).await
    }

    pub async fn send(&self, events: &[Event]) -> Result<()> {
        self.service_send(events).await
    }

    pub async fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        self.service_recv(opts).await
    }

    async fn service_send(&self, events: &[Event]) -> Result<()> {
        if !self.control_initialized() {
            return Err(Self::not_ready_error());
        }
        self.sink.send(events).await
    }

    async fn service_recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        if !self.control_initialized() {
            return Err(Self::not_ready_error());
        }
        self.sink.recv(opts).await
    }

    async fn ensure_runtime_proxy_endpoints_started(&self) -> Result<()> {
        let Some(boundary) = self.runtime_boundary.clone() else {
            return Ok(());
        };
        let mut tasks = self.runtime_endpoint_tasks.lock().await;
        let mut spawned_routes = self.runtime_endpoint_routes.lock().await;
        tasks.retain(|task| {
            if !task.is_finished() {
                return true;
            }
            eprintln!(
                "fs_meta_runtime_app: pruning finished runtime endpoint route={} reason={}",
                task.route_key(),
                task.finish_reason()
                    .unwrap_or_else(|| "unclassified_finish".to_string())
            );
            false
        });
        spawned_routes.clear();
        for task in tasks.iter() {
            spawned_routes.insert(task.route_key().to_string());
        }
        let routes = default_route_bindings();
        let query_active = self
            .facade_gate
            .unit_state(execution_units::QUERY_RUNTIME_UNIT_ID)?
            .map(|(active, _)| active)
            .unwrap_or(false);
        let query_peer_active = self
            .facade_gate
            .unit_state(execution_units::QUERY_PEER_RUNTIME_UNIT_ID)?
            .map(|(active, _)| active)
            .unwrap_or(false);
        let internal_query_active = query_active || query_peer_active;
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META, METHOD_QUERY) {
            if !query_active || !spawned_routes.insert(route.0.clone()) {
                // Not currently selected as query ingress owner, or already running.
            } else {
                let boundary_for_calls = self.runtime_boundary.clone().ok_or_else(|| {
                    CnxError::InvalidInput("fs-meta public query requires runtime boundary".into())
                })?;
                let caller_node = self.node_id.clone();
                eprintln!(
                    "fs_meta_runtime_app: spawning public query endpoint route={}",
                    route.0
                );
                let endpoint = ManagedEndpointTask::spawn(
                    boundary.clone(),
                    route,
                    format!("app:{}:{}", ROUTE_TOKEN_FS_META, METHOD_QUERY),
                    tokio_util::sync::CancellationToken::new(),
                    move |requests| {
                        let boundary_for_calls = boundary_for_calls.clone();
                        let caller_node = caller_node.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                    req.payload_bytes(),
                                ) else {
                                    continue;
                                };
                                eprintln!(
                                    "fs_meta_runtime_app: public query request selected_group={:?} recursive={} path={}",
                                    params.scope.selected_group,
                                    params.scope.recursive,
                                    String::from_utf8_lossy(&params.scope.path)
                                );
                                let adapter = crate::runtime::seam::exchange_host_adapter(
                                    boundary_for_calls.clone(),
                                    caller_node.clone(),
                                    crate::runtime::routes::default_route_bindings(),
                                );
                                let result: Result<Vec<Event>> = async {
                                    let payload = rmp_serde::to_vec(&params).map_err(|err| {
                                        CnxError::Internal(format!(
                                            "encode public query request failed: {err}"
                                        ))
                                    })?;
                                    capanix_host_adapter_fs::HostAdapter::call_collect(
                                        &adapter,
                                        ROUTE_TOKEN_FS_META_INTERNAL,
                                        crate::runtime::routes::METHOD_SINK_QUERY_PROXY,
                                        bytes::Bytes::from(payload),
                                        Duration::from_secs(30),
                                        Duration::from_secs(5),
                                    )
                                    .await
                                }
                                .await;
                                match result {
                                    Ok(mut events) => {
                                        eprintln!(
                                            "fs_meta_runtime_app: public query response events={}",
                                            events.len()
                                        );
                                        for event in &mut events {
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
                                        eprintln!(
                                            "fs_meta_runtime_app: public query failed err={}",
                                            err
                                        );
                                    }
                                }
                            }
                            responses
                        }
                    },
                );
                tasks.push(endpoint);
            }
        }
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS) {
            if !internal_query_active || !spawned_routes.insert(route.0.clone()) {
                // Not currently selected as query/query-peer sink-status owner, or already running.
            } else {
                let prefer_query_peer_first = self
                    .mirrored_query_peer_routes
                    .lock()
                    .await
                    .contains_key(&route.0);
                let endpoint_unit_ids = preferred_internal_query_endpoint_units(
                    query_active,
                    query_peer_active,
                    prefer_query_peer_first,
                );
                assert!(
                    !endpoint_unit_ids.is_empty(),
                    "internal query endpoint unit must exist when route is active"
                );
                let api_control_gate = self.api_control_gate.clone();
                let api_task = self.api_task.clone();
                let pending_facade = self.pending_facade.clone();
                let facade_pending_status = self.facade_pending_status.clone();
                let facade_gate = self.facade_gate.clone();
                let sink = self.sink.clone();
                let route_key = route.0.clone();
                eprintln!(
                    "fs_meta_runtime_app: spawning sink status endpoint route={}",
                    route.0
                );
                let endpoint = ManagedEndpointTask::spawn_with_units(
                    boundary.clone(),
                    route,
                    format!(
                        "app:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS
                    ),
                    endpoint_unit_ids,
                    tokio_util::sync::CancellationToken::new(),
                    move |requests| {
                        let api_control_gate = api_control_gate.clone();
                        let api_task = api_task.clone();
                        let pending_facade = pending_facade.clone();
                        let facade_pending_status = facade_pending_status.clone();
                        let facade_gate = facade_gate.clone();
                        let sink = sink.clone();
                        let route_key = route_key.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                let mut continuity_window = false;
                                if !api_control_gate.is_ready() {
                                    continuity_window = api_task.lock().await.is_some()
                                        && (pending_facade.lock().await.is_some()
                                            || facade_pending_status
                                                .read()
                                                .expect("read facade pending status")
                                                .is_some());
                                    if !continuity_window {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink status endpoint unavailable reason=control_not_ready"
                                        );
                                        continue;
                                    }
                                }
                                #[cfg(test)]
                                maybe_pause_runtime_proxy_request("sink_status").await;
                                if !internal_query_route_still_active(&facade_gate, &route_key) {
                                    eprintln!(
                                        "fs_meta_runtime_app: sink status endpoint unavailable reason=route_deactivated_after_pause route={}",
                                        route_key
                                    );
                                    match explicit_empty_sink_status_reply(
                                        &req.metadata().origin_id,
                                        req.metadata().correlation_id,
                                    ) {
                                        Ok(event) => responses.push(event),
                                        Err(err) => eprintln!(
                                            "fs_meta_runtime_app: sink status endpoint deactivated empty reply encode failed: {err}"
                                        ),
                                    }
                                    continue;
                                }
                                if !api_control_gate.is_ready() && !continuity_window {
                                    eprintln!(
                                        "fs_meta_runtime_app: sink status endpoint unavailable reason=control_not_ready_after_pause"
                                    );
                                    continue;
                                }
                                match sink.status_snapshot_nonblocking().await {
                                    Ok(snapshot) => {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink status endpoint response {}",
                                            summarize_sink_status_endpoint(&snapshot)
                                        );
                                        match sink_status_reply(
                                            &snapshot,
                                            &req.metadata().origin_id,
                                            req.metadata().correlation_id,
                                        ) {
                                            Ok(event) => responses.push(event),
                                            Err(err) => eprintln!(
                                                "fs_meta_runtime_app: sink status endpoint reply encode failed: {err}"
                                            ),
                                        }
                                    }
                                    Err(err) => {
                                        if !internal_query_route_still_active(
                                            &facade_gate,
                                            &route_key,
                                        ) {
                                            eprintln!(
                                                "fs_meta_runtime_app: sink status endpoint fail-closed after route deactivate route={} err={}",
                                                route_key, err
                                            );
                                            match explicit_empty_sink_status_reply(
                                                &req.metadata().origin_id,
                                                req.metadata().correlation_id,
                                            ) {
                                                Ok(event) => responses.push(event),
                                                Err(reply_err) => eprintln!(
                                                    "fs_meta_runtime_app: sink status endpoint deactivated empty reply encode failed: {reply_err}"
                                                ),
                                            }
                                            continue;
                                        }
                                        if matches!(err, CnxError::Timeout)
                                            && let Ok(cached_snapshot) =
                                                sink.cached_status_snapshot()
                                            && sink_status_snapshot_has_ready_scheduled_groups(
                                                &cached_snapshot,
                                            )
                                        {
                                            eprintln!(
                                                "fs_meta_runtime_app: sink status endpoint cached fallback err={} {}",
                                                err,
                                                summarize_sink_status_endpoint(&cached_snapshot)
                                            );
                                            match sink_status_reply(
                                                &cached_snapshot,
                                                &req.metadata().origin_id,
                                                req.metadata().correlation_id,
                                            ) {
                                                Ok(event) => responses.push(event),
                                                Err(reply_err) => eprintln!(
                                                    "fs_meta_runtime_app: sink status endpoint cached fallback encode failed: {reply_err}"
                                                ),
                                            }
                                            continue;
                                        }
                                        eprintln!(
                                            "fs_meta_runtime_app: sink status endpoint failed err={}",
                                            err
                                        );
                                    }
                                }
                            }
                            responses
                        }
                    },
                );
                tasks.push(endpoint);
            }
        }
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS) {
            if !internal_query_active || !spawned_routes.insert(route.0.clone()) {
                // Not currently selected as query/query-peer source-status owner, or already running.
            } else {
                let prefer_query_peer_first = self
                    .mirrored_query_peer_routes
                    .lock()
                    .await
                    .contains_key(&route.0);
                let endpoint_unit_ids = preferred_internal_query_endpoint_units(
                    query_active,
                    query_peer_active,
                    prefer_query_peer_first,
                );
                assert!(
                    !endpoint_unit_ids.is_empty(),
                    "internal query endpoint unit must exist when route is active"
                );
                let api_control_gate = self.api_control_gate.clone();
                let api_task = self.api_task.clone();
                let pending_facade = self.pending_facade.clone();
                let facade_pending_status = self.facade_pending_status.clone();
                let source = self.source.clone();
                let node_id = self.node_id.clone();
                eprintln!(
                    "fs_meta_runtime_app: spawning source status endpoint route={}",
                    route.0
                );
                let endpoint = ManagedEndpointTask::spawn_with_units(
                    boundary.clone(),
                    route,
                    format!(
                        "app:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS
                    ),
                    endpoint_unit_ids,
                    tokio_util::sync::CancellationToken::new(),
                    move |requests| {
                        let api_control_gate = api_control_gate.clone();
                        let api_task = api_task.clone();
                        let pending_facade = pending_facade.clone();
                        let facade_pending_status = facade_pending_status.clone();
                        let source = source.clone();
                        let node_id = node_id.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                // Internal source-status should fail closed on gate transitions,
                                // but when an active facade is still serving while a successor facade
                                // replacement is pending, allow the in-flight public request to reuse
                                // the current source snapshot instead of stalling behind control_ready.
                                let mut ready_epoch = api_control_gate.epoch();
                                let mut continuity_window = false;
                                if !api_control_gate.is_ready() {
                                    continuity_window = api_task.lock().await.is_some()
                                        && (pending_facade.lock().await.is_some()
                                            || facade_pending_status
                                                .read()
                                                .expect("read facade pending status")
                                                .is_some());
                                    if !continuity_window {
                                        eprintln!(
                                            "fs_meta_runtime_app: source status endpoint unavailable reason=control_not_ready"
                                        );
                                        api_control_gate.wait_ready().await;
                                        ready_epoch = api_control_gate.epoch();
                                    }
                                }
                                let trace_id = next_source_status_endpoint_trace_id();
                                let route_name = format!(
                                    "{}:{}",
                                    ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS
                                );
                                let mut trace_guard = SourceStatusEndpointTraceGuard::new(
                                    route_name,
                                    req.metadata().correlation_id,
                                    trace_id,
                                    "before_source_snapshot_await",
                                );
                                if debug_source_status_lifecycle_enabled() {
                                    eprintln!(
                                        "fs_meta_runtime_app: source status endpoint begin correlation={:?} trace_id={}",
                                        req.metadata().correlation_id,
                                        trace_id
                                    );
                                }
                                #[cfg(test)]
                                maybe_pause_runtime_proxy_request("source_status").await;
                                if !api_control_gate.is_ready() && !continuity_window {
                                    eprintln!(
                                        "fs_meta_runtime_app: source status endpoint unavailable reason=control_not_ready_after_pause"
                                    );
                                    continue;
                                }
                                if !continuity_window && api_control_gate.epoch() != ready_epoch {
                                    eprintln!(
                                        "fs_meta_runtime_app: source status endpoint unavailable reason=control_epoch_advanced"
                                    );
                                }
                                let snapshot = source.observability_snapshot_nonblocking().await;
                                trace_guard.phase("after_source_snapshot_await");
                                eprintln!(
                                    "fs_meta_runtime_app: source status endpoint response node={} groups={} runners={} correlation={:?} trace_id={}",
                                    node_id.0,
                                    snapshot.source_primary_by_group.len(),
                                    snapshot.last_force_find_runner_by_group.len(),
                                    req.metadata().correlation_id,
                                    trace_id
                                );
                                if debug_force_find_runner_capture_enabled() {
                                    eprintln!(
                                        "fs_meta_runtime_app: source status endpoint runner_capture node={} correlation={:?} trace_id={} last_runner={:?} inflight={:?}",
                                        node_id.0,
                                        req.metadata().correlation_id,
                                        trace_id,
                                        summarize_group_string_map(
                                            &snapshot.last_force_find_runner_by_group
                                        ),
                                        snapshot.force_find_inflight_groups
                                    );
                                }
                                if let Ok(payload) = rmp_serde::to_vec_named(&snapshot) {
                                    responses.push(Event::new(
                                        EventMetadata {
                                            origin_id: req.metadata().origin_id.clone(),
                                            timestamp_us: now_us(),
                                            logical_ts: None,
                                            correlation_id: req.metadata().correlation_id,
                                            ingress_auth: None,
                                            trace: None,
                                        },
                                        bytes::Bytes::from(payload),
                                    ));
                                }
                                trace_guard.complete();
                            }
                            responses
                        }
                    },
                );
                tasks.push(endpoint);
            }
        }
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY) {
            if !internal_query_active || !spawned_routes.insert(route.0.clone()) {
                // Not currently selected as query/query-peer proxy owner, or already running.
            } else {
                let prefer_query_peer_first = self
                    .mirrored_query_peer_routes
                    .lock()
                    .await
                    .contains_key(&route.0);
                let endpoint_unit_ids = preferred_internal_query_endpoint_units(
                    query_active,
                    query_peer_active,
                    prefer_query_peer_first,
                );
                assert!(
                    !endpoint_unit_ids.is_empty(),
                    "internal query endpoint unit must exist when route is active"
                );
                eprintln!(
                    "fs_meta_runtime_app: spawning sink query proxy endpoint route={}",
                    route.0
                );
                let route_key = route.0.clone();
                let facade_gate = self.facade_gate.clone();
                let boundary_for_calls = boundary.clone();
                let sink = self.sink.clone();
                let source = self.source.clone();
                let node_id = self.node_id.clone();
                let endpoint = ManagedEndpointTask::spawn_with_units(
                    boundary.clone(),
                    route,
                    format!(
                        "app:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY
                    ),
                    endpoint_unit_ids,
                    tokio_util::sync::CancellationToken::new(),
                    move |requests| {
                        let route_key = route_key.clone();
                        let facade_gate = facade_gate.clone();
                        let boundary_for_calls = boundary_for_calls.clone();
                        let sink = sink.clone();
                        let source = source.clone();
                        let node_id = node_id.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                    req.payload_bytes(),
                                ) else {
                                    continue;
                                };
                                eprintln!(
                                    "fs_meta_runtime_app: sink query proxy request selected_group={:?} recursive={} path={}",
                                    params.scope.selected_group,
                                    params.scope.recursive,
                                    String::from_utf8_lossy(&params.scope.path)
                                );
                                #[cfg(test)]
                                maybe_pause_runtime_proxy_request("sink_query_proxy").await;
                                if !internal_query_route_still_active(&facade_gate, &route_key) {
                                    eprintln!(
                                        "fs_meta_runtime_app: sink query proxy unavailable reason=route_deactivated_after_pause route={}",
                                        route_key
                                    );
                                    if params.scope.selected_group.is_some() {
                                        // This request has already entered the selected-group
                                        // proxy route and then lost ownership during a later
                                        // deactivate. Fail closed immediately with an explicit
                                        // empty reply instead of spending the full query timeout
                                        // on a now-inactive route.
                                        match selected_group_empty_materialized_reply(
                                            &params,
                                            req.metadata().correlation_id,
                                        ) {
                                            Ok(Some(event)) => {
                                                responses.push(event);
                                            }
                                            Ok(None) => {}
                                            Err(err) => {
                                                eprintln!(
                                                    "fs_meta_runtime_app: sink query proxy deactivated empty reply encode failed: {err}"
                                                );
                                            }
                                        }
                                    }
                                    continue;
                                }
                                let result = sink.materialized_query_nonblocking(&params).await;
                                match result {
                                    Ok(mut events) => {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink query proxy response events={}",
                                            events.len()
                                        );
                                        for event in &events {
                                            match rmp_serde::from_slice::<MaterializedQueryPayload>(
                                                event.payload_bytes(),
                                            ) {
                                                Ok(MaterializedQueryPayload::Tree(payload)) => {
                                                    eprintln!(
                                                        "fs_meta_runtime_app: sink query proxy payload group={} root_exists={} entries={} has_children={}",
                                                        event.metadata().origin_id.0,
                                                        payload.root.exists,
                                                        payload.entries.len(),
                                                        payload.root.has_children
                                                    );
                                                }
                                                Ok(MaterializedQueryPayload::Stats(_)) => {
                                                    eprintln!(
                                                        "fs_meta_runtime_app: sink query proxy payload group={} stats",
                                                        event.metadata().origin_id.0
                                                    );
                                                }
                                                Err(err) => {
                                                    eprintln!(
                                                        "fs_meta_runtime_app: sink query proxy payload decode failed group={} err={}",
                                                        event.metadata().origin_id.0,
                                                        err
                                                    );
                                                }
                                            }
                                        }
                                        let live_local_sink_status_snapshot =
                                            sink.status_snapshot_nonblocking().await.ok();
                                        let cached_local_sink_status_snapshot =
                                            sink.cached_status_snapshot().ok();
                                        let bridge_sink_status_snapshot =
                                            selected_group_sink_query_bridge_snapshot(
                                                &params,
                                                live_local_sink_status_snapshot.as_ref(),
                                                cached_local_sink_status_snapshot.as_ref(),
                                            );
                                        let local_selected_group_bridge_eligible =
                                            bridge_sink_status_snapshot
                                                .map(|snapshot| {
                                                    selected_group_bridge_eligible_from_sink_status(
                                                        &params, snapshot,
                                                    )
                                                })
                                                .unwrap_or(true);
                                        let should_bridge = should_bridge_selected_group_sink_query(
                                            &params,
                                            &events,
                                            local_selected_group_bridge_eligible,
                                        );
                                        if debug_sink_query_route_trace_enabled() {
                                            eprintln!(
                                                "fs_meta_runtime_app: sink query proxy bridge_decision correlation={:?} selected_group={:?} local_events={} eligible={} should_bridge={}",
                                                req.metadata().correlation_id,
                                                params.scope.selected_group,
                                                events.len(),
                                                local_selected_group_bridge_eligible,
                                                should_bridge
                                            );
                                        }
                                        let mut fail_closed_after_bridge_gap = false;
                                        if should_bridge {
                                            match rmp_serde::to_vec(&params) {
                                                Ok(payload) => {
                                                    let bridge_adapter = crate::runtime::seam::exchange_host_adapter(
                                                        boundary_for_calls.clone(),
                                                        node_id.clone(),
                                                        selected_group_sink_query_bridge_bindings(
                                                            &params,
                                                            bridge_sink_status_snapshot,
                                                        ),
                                                    );
                                                    match capanix_host_adapter_fs::HostAdapter::call_collect(
                                                        &bridge_adapter,
                                                        ROUTE_TOKEN_FS_META_INTERNAL,
                                                        METHOD_SINK_QUERY,
                                                        bytes::Bytes::from(payload),
                                                        SINK_QUERY_PROXY_BRIDGE_TIMEOUT,
                                                        SINK_QUERY_PROXY_BRIDGE_IDLE_GRACE,
                                                    )
                                                    .await
                                                    {
                                                        Ok(mut bridged) => {
                                                            eprintln!(
                                                                "fs_meta_runtime_app: sink query proxy bridged internal sink query events={}",
                                                                bridged.len()
                                                            );
                                                            events.append(&mut bridged);
                                                        }
                                                        Err(err) => {
                                                            eprintln!(
                                                                "fs_meta_runtime_app: sink query proxy bridge failed err={}",
                                                                err
                                                            );
                                                            match selected_group_empty_materialized_reply_after_bridge_failure(
                                                                &params,
                                                                &events,
                                                                local_selected_group_bridge_eligible,
                                                                &err,
                                                                req.metadata().correlation_id,
                                                            ) {
                                                                Ok(Some(event)) => {
                                                                    responses.push(event);
                                                                    continue;
                                                                }
                                                                Ok(None) => {}
                                                                Err(reply_err) => {
                                                                    eprintln!(
                                                                        "fs_meta_runtime_app: sink query proxy bridge failure empty reply encode failed: {reply_err}"
                                                                    );
                                                                }
                                                            }
                                                            if should_fail_closed_selected_group_empty_after_bridge_failure(
                                                                &params,
                                                                &events,
                                                                local_selected_group_bridge_eligible,
                                                                &err,
                                                            ) {
                                                                events.clear();
                                                                fail_closed_after_bridge_gap = true;
            }
        }
    }
                                                }
                                                Err(err) => {
                                                    eprintln!(
                                                        "fs_meta_runtime_app: sink query proxy bridge encode failed err={}",
                                                        err
                                                    );
                                                }
                                            }
                                        }
                                        if events.is_empty() && !fail_closed_after_bridge_gap {
                                            let should_emit_empty = if params
                                                .scope
                                                .selected_group
                                                .is_some()
                                            {
                                                let snapshot = source
                                                    .observability_snapshot_nonblocking()
                                                    .await;
                                                should_emit_selected_group_empty_materialized_reply(
                                                    &node_id,
                                                    &snapshot.source_primary_by_group,
                                                    &params,
                                                )
                                            } else {
                                                false
                                            };
                                            if should_emit_empty {
                                                match selected_group_empty_materialized_reply(
                                                    &params,
                                                    req.metadata().correlation_id,
                                                ) {
                                                    Ok(Some(event)) => {
                                                        responses.push(event);
                                                        continue;
                                                    }
                                                    Ok(None) => {}
                                                    Err(err) => {
                                                        eprintln!(
                                                            "fs_meta_runtime_app: sink query proxy empty reply encode failed: {err}"
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        for event in &mut events {
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
                                        eprintln!(
                                            "fs_meta_runtime_app: sink query proxy failed err={}",
                                            err
                                        );
                                        responses.push(Event::new(
                                            EventMetadata {
                                                origin_id: NodeId(
                                                    params
                                                        .scope
                                                        .selected_group
                                                        .clone()
                                                        .unwrap_or_else(|| {
                                                            "sink-query-proxy".to_string()
                                                        }),
                                                ),
                                                timestamp_us: now_us(),
                                                logical_ts: None,
                                                correlation_id: req.metadata().correlation_id,
                                                ingress_auth: None,
                                                trace: None,
                                            },
                                            bytes::Bytes::from(err.to_string()),
                                        ));
                                    }
                                }
                            }
                            responses
                        }
                    },
                );
                tasks.push(endpoint);
            }
        }
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND) {
            if !internal_query_active || !spawned_routes.insert(route.0.clone()) {
                // Not currently selected as query/query-peer source-find owner, or already running.
            } else {
                let prefer_query_peer_first = self
                    .mirrored_query_peer_routes
                    .lock()
                    .await
                    .contains_key(&route.0);
                let endpoint_unit_ids = preferred_internal_query_endpoint_units(
                    query_active,
                    query_peer_active,
                    prefer_query_peer_first,
                );
                assert!(
                    !endpoint_unit_ids.is_empty(),
                    "internal query endpoint unit must exist when route is active"
                );
                eprintln!(
                    "fs_meta_runtime_app: spawning source find proxy endpoint route={}",
                    route.0
                );
                let source = self.source.clone();
                let node_id = self.node_id.clone();
                let endpoint = ManagedEndpointTask::spawn_with_units(
                    boundary.clone(),
                    route,
                    format!(
                        "app:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND
                    ),
                    endpoint_unit_ids,
                    tokio_util::sync::CancellationToken::new(),
                    move |requests| {
                        let source = source.clone();
                        let node_id = node_id.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                    req.payload_bytes(),
                                ) else {
                                    continue;
                                };
                                eprintln!(
                                    "fs_meta_runtime_app: source find proxy request selected_group={:?} recursive={} path={}",
                                    params.scope.selected_group,
                                    params.scope.recursive,
                                    String::from_utf8_lossy(&params.scope.path)
                                );
                                #[cfg(test)]
                                maybe_pause_runtime_proxy_request("source_find").await;
                                match source.force_find(&params).await {
                                    Ok(mut events) => {
                                        eprintln!(
                                            "fs_meta_runtime_app: source find proxy response events={}",
                                            events.len()
                                        );
                                        if debug_force_find_runner_capture_enabled() {
                                            let last_runner = source
                                                .last_force_find_runner_by_group_snapshot()
                                                .await
                                                .unwrap_or_default();
                                            let inflight = source
                                                .force_find_inflight_groups_snapshot()
                                                .await
                                                .unwrap_or_default();
                                            let response_origins = events
                                                .iter()
                                                .map(|event| event.metadata().origin_id.0.clone())
                                                .collect::<Vec<_>>();
                                            eprintln!(
                                                "fs_meta_runtime_app: source find proxy runner_capture node={} selected_group={:?} path={} response_events={} response_origins={:?} last_runner={:?} inflight={:?}",
                                                node_id.0,
                                                params.scope.selected_group,
                                                String::from_utf8_lossy(&params.scope.path),
                                                events.len(),
                                                response_origins,
                                                summarize_group_string_map(&last_runner),
                                                inflight
                                            );
                                        }
                                        for event in &mut events {
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
                                        eprintln!(
                                            "fs_meta_runtime_app: source find proxy failed err={}",
                                            err
                                        );
                                        if debug_force_find_runner_capture_enabled() {
                                            let last_runner = source
                                                .last_force_find_runner_by_group_snapshot()
                                                .await
                                                .unwrap_or_default();
                                            let inflight = source
                                                .force_find_inflight_groups_snapshot()
                                                .await
                                                .unwrap_or_default();
                                            eprintln!(
                                                "fs_meta_runtime_app: source find proxy runner_capture_failed node={} selected_group={:?} path={} err={} last_runner={:?} inflight={:?}",
                                                node_id.0,
                                                params.scope.selected_group,
                                                String::from_utf8_lossy(&params.scope.path),
                                                err,
                                                summarize_group_string_map(&last_runner),
                                                inflight
                                            );
                                        }
                                        responses.push(Event::new(
                                            EventMetadata {
                                                origin_id: NodeId(
                                                    params
                                                        .scope
                                                        .selected_group
                                                        .clone()
                                                        .unwrap_or_else(|| {
                                                            "source-find-proxy".to_string()
                                                        }),
                                                ),
                                                timestamp_us: now_us(),
                                                logical_ts: None,
                                                correlation_id: req.metadata().correlation_id,
                                                ingress_auth: None,
                                                trace: None,
                                            },
                                            bytes::Bytes::from(err.to_string()),
                                        ));
                                    }
                                }
                            }
                            responses
                        }
                    },
                );
                tasks.push(endpoint);
            }
        }
        Ok(())
    }

    fn facade_candidate_resource_ids(bound_scopes: &[RuntimeBoundScope]) -> Vec<String> {
        let mut ids = std::collections::BTreeSet::new();
        for scope in bound_scopes {
            for resource_id in &scope.resource_ids {
                let trimmed = resource_id.trim();
                if !trimmed.is_empty() {
                    ids.insert(trimmed.to_string());
                }
            }
        }
        ids.into_iter().collect()
    }

    async fn runtime_scoped_facade_group_ids(
        source: &SourceFacade,
        sink: &SinkFacade,
    ) -> Result<Vec<String>> {
        let mut source_groups = source
            .scheduled_source_group_ids()
            .await?
            .unwrap_or_default();
        let scan_groups = source.scheduled_scan_group_ids().await?.unwrap_or_default();
        source_groups.extend(scan_groups);
        let sink_groups = sink.scheduled_group_ids().await?.unwrap_or_default();
        if !source_groups.is_empty() && !sink_groups.is_empty() {
            return Ok(source_groups.intersection(&sink_groups).cloned().collect());
        }
        if !source_groups.is_empty() {
            return Ok(source_groups.into_iter().collect());
        }
        Ok(sink_groups.into_iter().collect())
    }

    async fn facade_candidate_group_ids(
        source: &SourceFacade,
        sink: &SinkFacade,
        bound_scopes: &[RuntimeBoundScope],
    ) -> Result<Vec<String>> {
        let logical_root_ids = source
            .logical_roots_snapshot()
            .await?
            .into_iter()
            .map(|root| root.id)
            .collect::<std::collections::BTreeSet<_>>();
        let mut ids = std::collections::BTreeSet::new();
        for scope in bound_scopes {
            let scope_id = scope.scope_id.trim();
            if !scope_id.is_empty() && logical_root_ids.contains(scope_id) {
                ids.insert(scope_id.to_string());
            }
            for resource_id in &scope.resource_ids {
                let trimmed = resource_id.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Some(group_id) = source.resolve_group_id_for_object_ref(trimmed).await? {
                    ids.insert(group_id);
                }
            }
        }
        if !ids.is_empty() {
            return Ok(ids.into_iter().collect());
        }
        Self::runtime_scoped_facade_group_ids(source, sink).await
    }

    async fn observation_candidate_group_ids(
        source: &SourceFacade,
        sink: &SinkFacade,
        pending: &PendingFacadeActivation,
    ) -> Result<std::collections::BTreeSet<String>> {
        if !pending.group_ids.is_empty() {
            return Ok(pending.group_ids.iter().cloned().collect());
        }
        Ok(
            Self::facade_candidate_group_ids(source, sink, &pending.bound_scopes)
                .await?
                .into_iter()
                .collect(),
        )
    }

    async fn observation_eligible_for(
        source: &SourceFacade,
        sink: &SinkFacade,
        pending: &PendingFacadeActivation,
    ) -> Result<bool> {
        let source_status = source.status_snapshot().await?;
        let sink_status = sink.status_snapshot().await?;
        let candidate_groups = Self::observation_candidate_group_ids(source, sink, pending).await?;
        let status = evaluate_observation_status(
            &candidate_group_observation_evidence(&source_status, &sink_status, &candidate_groups),
            ObservationTrustPolicy::candidate_generation(),
        );
        Ok(status.state == ObservationState::TrustedMaterialized)
    }

    async fn try_spawn_pending_facade(&self) -> Result<bool> {
        let spawn_result = Self::try_spawn_pending_facade_from_parts(
            self.instance_id,
            self.api_task.clone(),
            self.pending_facade.clone(),
            self.facade_spawn_in_progress.clone(),
            self.pending_fixed_bind_has_suppressed_dependent_routes
                .clone(),
            self.facade_pending_status.clone(),
            self.facade_service_state.clone(),
            self.api_request_tracker.clone(),
            self.api_control_gate.clone(),
            self.node_id.clone(),
            self.runtime_boundary.clone(),
            self.source.clone(),
            self.sink.clone(),
            self.query_sink.clone(),
            self.runtime_boundary.clone(),
        )
        .await;
        let pending = self.pending_facade.lock().await.clone();
        match (spawn_result, pending) {
            (Ok(spawned), Some(pending)) => {
                let wait_for_claim_release = self
                    .pending_fixed_bind_facade_claim_conflict(pending.generation)
                    .await;
                if wait_for_claim_release
                    && pending.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                    && !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
                {
                    mark_pending_fixed_bind_handoff_ready(
                        &pending.resolved.bind_addr,
                        self.pending_fixed_bind_handoff_registrant(),
                    );
                }
                self.pending_fixed_bind_claim_release_followup
                    .store(wait_for_claim_release, Ordering::Release);
                if spawned {
                    self.pending_fixed_bind_has_suppressed_dependent_routes
                        .store(false, Ordering::Release);
                }
                self.refresh_active_fixed_bind_facade_owner().await;
                Ok(spawned)
            }
            (Ok(spawned), None) => {
                self.pending_fixed_bind_claim_release_followup
                    .store(false, Ordering::Release);
                self.pending_fixed_bind_has_suppressed_dependent_routes
                    .store(false, Ordering::Release);
                self.refresh_active_fixed_bind_facade_owner().await;
                Ok(spawned)
            }
            (Err(err), Some(pending))
                if Self::facade_spawn_error_is_bind_addr_in_use(&err)
                    && pending.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                    && !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr) =>
            {
                Self::record_pending_facade_retry_error(
                    &self.facade_pending_status,
                    &pending,
                    &err,
                );
                mark_pending_fixed_bind_handoff_ready(
                    &pending.resolved.bind_addr,
                    self.pending_fixed_bind_handoff_registrant(),
                );
                self.pending_fixed_bind_claim_release_followup
                    .store(true, Ordering::Release);
                Ok(false)
            }
            (Err(err), _) => Err(err),
        }
    }

    async fn try_spawn_pending_facade_from_parts(
        instance_id: u64,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
        facade_spawn_in_progress: Arc<Mutex<Option<FacadeSpawnInProgress>>>,
        pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
        facade_pending_status: SharedFacadePendingStatusCell,
        facade_service_state: SharedFacadeServiceStateCell,
        api_request_tracker: Arc<ApiRequestTracker>,
        api_control_gate: Arc<ApiControlGate>,
        node_id: NodeId,
        runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        query_sink: Arc<SinkFacade>,
        query_runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> Result<bool> {
        Self::try_spawn_pending_facade_from_parts_with_spawn(
            instance_id,
            api_task,
            pending_facade,
            facade_spawn_in_progress,
            pending_fixed_bind_has_suppressed_dependent_routes,
            facade_pending_status,
            facade_service_state,
            api_request_tracker,
            api_control_gate,
            node_id,
            runtime_boundary,
            source,
            sink,
            query_sink,
            query_runtime_boundary,
            |resolved,
             node_id,
             runtime_boundary,
             source,
             sink,
             query_sink,
             query_runtime_boundary,
             facade_pending_status,
             facade_service_state,
             api_request_tracker,
             api_control_gate| async move {
                api::spawn(
                    resolved,
                    node_id,
                    runtime_boundary,
                    source,
                    sink,
                    query_sink,
                    query_runtime_boundary,
                    facade_pending_status,
                    facade_service_state,
                    api_request_tracker,
                    api_control_gate,
                )
                .await
            },
        )
        .await
    }

    async fn try_spawn_pending_facade_from_parts_with_spawn<Spawn, SpawnFut>(
        instance_id: u64,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
        facade_spawn_in_progress: Arc<Mutex<Option<FacadeSpawnInProgress>>>,
        pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
        facade_pending_status: SharedFacadePendingStatusCell,
        facade_service_state: SharedFacadeServiceStateCell,
        api_request_tracker: Arc<ApiRequestTracker>,
        api_control_gate: Arc<ApiControlGate>,
        node_id: NodeId,
        runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        query_sink: Arc<SinkFacade>,
        query_runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
        spawn_facade: Spawn,
    ) -> Result<bool>
    where
        Spawn: FnOnce(
            api::config::ResolvedApiConfig,
            NodeId,
            Option<Arc<dyn ChannelIoSubset>>,
            Arc<SourceFacade>,
            Arc<SinkFacade>,
            Arc<SinkFacade>,
            Option<Arc<dyn ChannelIoSubset>>,
            SharedFacadePendingStatusCell,
            SharedFacadeServiceStateCell,
            Arc<ApiRequestTracker>,
            Arc<ApiControlGate>,
        ) -> SpawnFut,
        SpawnFut: std::future::Future<Output = Result<api::ApiServerHandle>>,
    {
        let Some(pending) = pending_facade.lock().await.clone() else {
            return Ok(false);
        };
        if let Some(claim) = ProcessFacadeClaim::from_pending(instance_id, &pending) {
            let mut guard = match process_facade_claim_cell().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(existing) = guard.get(&claim.bind_addr)
                && existing.owner_instance_id != instance_id
                && existing.matches_pending(&pending)
            {
                eprintln!(
                    "fs_meta_runtime_app: facade process claim already owned instance_id={} generation={} route_key={} resources={:?} bind_addr={}",
                    existing.owner_instance_id,
                    pending.generation,
                    pending.route_key,
                    pending.resource_ids,
                    pending.resolved.bind_addr
                );
                let registrant = PendingFixedBindHandoffRegistrant {
                    instance_id,
                    api_task: api_task.clone(),
                    pending_facade: pending_facade.clone(),
                    pending_fixed_bind_has_suppressed_dependent_routes:
                        pending_fixed_bind_has_suppressed_dependent_routes.clone(),
                    facade_spawn_in_progress: facade_spawn_in_progress.clone(),
                    facade_pending_status: facade_pending_status.clone(),
                    facade_service_state: facade_service_state.clone(),
                    api_request_tracker: api_request_tracker.clone(),
                    api_control_gate: api_control_gate.clone(),
                    node_id: node_id.clone(),
                    runtime_boundary: runtime_boundary.clone(),
                    source: source.clone(),
                    sink: sink.clone(),
                    query_sink: query_sink.clone(),
                    query_runtime_boundary: query_runtime_boundary.clone(),
                };
                mark_pending_fixed_bind_handoff_ready(&pending.resolved.bind_addr, registrant);
                return Ok(false);
            }
            guard.insert(claim.bind_addr.clone(), claim);
        }
        let stale_active = {
            let mut api_task_guard = api_task.lock().await;
            if api_task_guard
                .as_ref()
                .is_some_and(|active| !active.handle.is_running())
            {
                api_task_guard.take()
            } else {
                None
            }
        };
        if let Some(stale) = stale_active {
            eprintln!(
                "fs_meta_runtime_app: dropping stale inactive facade handle generation={} route_key={}",
                stale.generation, stale.route_key
            );
            stale.handle.shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT).await;
            clear_owned_process_facade_claim(instance_id);
            if !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr) {
                clear_active_fixed_bind_facade_owner(&pending.resolved.bind_addr, instance_id);
            }
        }
        let mut same_resource_replacement = None::<(String, Vec<String>, u64)>;
        let replacing_existing = {
            let api_task_guard = api_task.lock().await;
            if let Some(current) = api_task_guard.as_ref()
                && current.route_key == pending.route_key
                && current.resource_ids == pending.resource_ids
                && current.generation == pending.generation
            {
                drop(api_task_guard);
                let mut pending_guard = pending_facade.lock().await;
                if pending_guard.as_ref().is_some_and(|candidate| {
                    candidate.route_key == pending.route_key
                        && candidate.resource_ids == pending.resource_ids
                        && candidate.generation == pending.generation
                }) {
                    pending_guard.take();
                }
                Self::clear_pending_facade_status(&facade_pending_status);
                return Ok(true);
            }
            if let Some(current) = api_task_guard.as_ref()
                && current.route_key == pending.route_key
                && current.resource_ids == pending.resource_ids
            {
                same_resource_replacement = Some((
                    current.route_key.clone(),
                    current.resource_ids.clone(),
                    current.generation,
                ));
            }
            api_task_guard.is_some()
        };
        if let Some(wait_reason) = Self::facade_replacement_wait_reason(
            source.as_ref(),
            sink.as_ref(),
            &pending,
            replacing_existing,
        )
        .await?
        {
            Self::set_pending_facade_status_waiting(&facade_pending_status, &pending, wait_reason);
            return Ok(false);
        }
        if let Some((route_key, resource_ids, generation)) = same_resource_replacement.take() {
            let mut api_task_guard = api_task.lock().await;
            if api_task_guard.as_ref().is_some_and(|active| {
                active.route_key == route_key
                    && active.resource_ids == resource_ids
                    && active.generation == generation
            }) {
                if let Some(active) = api_task_guard.as_mut() {
                    active.generation = pending.generation;
                }
                drop(api_task_guard);
                let mut pending_guard = pending_facade.lock().await;
                if pending_guard.as_ref().is_some_and(|candidate| {
                    candidate.route_key == pending.route_key
                        && candidate.resource_ids == pending.resource_ids
                        && candidate.generation == pending.generation
                }) {
                    pending_guard.take();
                }
                Self::clear_pending_facade_status(&facade_pending_status);
                return Ok(true);
            }
        }
        {
            let mut inflight_guard = facade_spawn_in_progress.lock().await;
            if let Some(inflight) = inflight_guard.as_ref() {
                eprintln!(
                    "fs_meta_runtime_app: facade spawn already in progress generation={} route_key={} resources={:?} same_resource={}",
                    pending.generation,
                    pending.route_key,
                    pending.resource_ids,
                    inflight.matches_pending(&pending)
                );
                return Ok(false);
            }
            *inflight_guard = Some(FacadeSpawnInProgress::from_pending(&pending));
        }
        // Cold start has no prior facade to retain, so runtime confirmation is
        // sufficient to bring up the hosting boundary. Replacement also proceeds once
        // runtime confirms external exposure; materialized `/tree` and `/stats`
        // readiness is enforced at the query surface so `/on-demand-force-find`
        // can become available earlier.

        eprintln!(
            "fs_meta_runtime_app: spawning facade api server generation={} route_key={} resources={:?}",
            pending.generation, pending.route_key, pending.resource_ids
        );
        let spawn_result = spawn_facade(
            pending.resolved.clone(),
            node_id,
            runtime_boundary,
            source,
            sink,
            query_sink,
            query_runtime_boundary,
            facade_pending_status.clone(),
            facade_service_state.clone(),
            api_request_tracker.clone(),
            api_control_gate.clone(),
        )
        .await;
        {
            let mut inflight_guard = facade_spawn_in_progress.lock().await;
            if inflight_guard
                .as_ref()
                .is_some_and(|inflight| inflight.matches_pending(&pending))
            {
                inflight_guard.take();
            }
        }
        let handle = match spawn_result {
            Ok(handle) => handle,
            Err(err) => {
                let mut guard = match process_facade_claim_cell().lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                if guard.get(&pending.resolved.bind_addr).is_some_and(|claim| {
                    claim.owner_instance_id == instance_id && claim.matches_pending(&pending)
                }) {
                    guard.remove(&pending.resolved.bind_addr);
                }
                return Err(err);
            }
        };
        eprintln!(
            "fs_meta_runtime_app: facade api::spawn returned generation={} route_key={}",
            pending.generation, pending.route_key
        );

        let adopted_generation = {
            let pending_guard = pending_facade.lock().await;
            pending_guard.as_ref().and_then(|candidate| {
                (candidate.route_key == pending.route_key
                    && candidate.resource_ids == pending.resource_ids)
                    .then_some(candidate.generation)
            })
        };
        let Some(adopted_generation) = adopted_generation else {
            eprintln!(
                "fs_meta_runtime_app: shutting down stale facade handle generation={} route_key={}",
                pending.generation, pending.route_key
            );
            handle.shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT).await;
            let mut guard = match process_facade_claim_cell().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if guard.get(&pending.resolved.bind_addr).is_some_and(|claim| {
                claim.owner_instance_id == instance_id && claim.matches_pending(&pending)
            }) {
                guard.remove(&pending.resolved.bind_addr);
            }
            return Ok(false);
        };

        let previous = api_task.lock().await.replace(FacadeActivation {
            route_key: pending.route_key.clone(),
            generation: adopted_generation,
            resource_ids: pending.resource_ids.clone(),
            handle,
        });
        clear_pending_fixed_bind_handoff_ready(&pending.resolved.bind_addr);
        eprintln!(
            "fs_meta_runtime_app: facade handle active generation={} route_key={} resources={:?}",
            adopted_generation, pending.route_key, pending.resource_ids
        );
        let mut pending_guard = pending_facade.lock().await;
        if pending_guard.as_ref().is_some_and(|candidate| {
            candidate.route_key == pending.route_key
                && candidate.resource_ids == pending.resource_ids
        }) {
            pending_guard.take();
        }
        Self::clear_pending_facade_status(&facade_pending_status);
        drop(pending_guard);
        if let Some(current) = previous {
            eprintln!(
                "fs_meta_runtime_app: shutting down previous active facade generation={}",
                current.generation
            );
            current
                .handle
                .shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT)
                .await;
        }
        Ok(true)
    }

    fn facade_retry_backoff(retry_attempts: u64) -> Duration {
        match retry_attempts {
            0 | 1 => Duration::ZERO,
            2 => Duration::from_secs(2),
            3 => Duration::from_secs(4),
            4 => Duration::from_secs(8),
            5 => Duration::from_secs(16),
            _ => Duration::from_secs(30),
        }
    }

    fn pending_status_matches(
        status: &SharedFacadePendingStatus,
        pending: &PendingFacadeActivation,
    ) -> bool {
        status.route_key == pending.route_key
            && status.generation == pending.generation
            && status.resource_ids == pending.resource_ids
    }

    fn clear_pending_facade_status(status_cell: &SharedFacadePendingStatusCell) {
        if let Ok(mut guard) = status_cell.write() {
            *guard = None;
        }
    }

    fn set_pending_facade_status_waiting(
        status_cell: &SharedFacadePendingStatusCell,
        pending: &PendingFacadeActivation,
        reason: FacadePendingReason,
    ) {
        let pending_since_us = status_cell
            .read()
            .ok()
            .and_then(|guard| {
                guard.as_ref().and_then(|status| {
                    Self::pending_status_matches(status, pending).then_some(status.pending_since_us)
                })
            })
            .unwrap_or_else(now_us);
        if let Ok(mut guard) = status_cell.write() {
            *guard = Some(SharedFacadePendingStatus {
                route_key: pending.route_key.clone(),
                generation: pending.generation,
                resource_ids: pending.resource_ids.clone(),
                runtime_managed: pending.runtime_managed,
                runtime_exposure_confirmed: pending.runtime_exposure_confirmed,
                reason,
                retry_attempts: 0,
                pending_since_us,
                last_error: None,
                last_attempt_at_us: None,
                last_error_at_us: None,
                retry_backoff_ms: None,
                next_retry_at_us: None,
            });
        }
    }

    async fn facade_replacement_wait_reason(
        source: &SourceFacade,
        sink: &SinkFacade,
        pending: &PendingFacadeActivation,
        replacing_existing: bool,
    ) -> Result<Option<FacadePendingReason>> {
        if !replacing_existing {
            return Ok(None);
        }
        if !pending.runtime_exposure_confirmed {
            return Ok(Some(FacadePendingReason::AwaitingRuntimeExposure));
        }
        if pending.runtime_managed && !Self::observation_eligible_for(source, sink, pending).await?
        {
            return Ok(Some(FacadePendingReason::AwaitingObservationEligibility));
        }
        Ok(None)
    }

    fn record_pending_facade_retry_error(
        status_cell: &SharedFacadePendingStatusCell,
        pending: &PendingFacadeActivation,
        err: &CnxError,
    ) {
        let now = now_us();
        let (retry_attempts, pending_since_us) = status_cell
            .read()
            .ok()
            .and_then(|guard| {
                guard.as_ref().and_then(|status| {
                    Self::pending_status_matches(status, pending).then_some((
                        status.retry_attempts.saturating_add(1),
                        status.pending_since_us,
                    ))
                })
            })
            .unwrap_or((1, now));
        let backoff = Self::facade_retry_backoff(retry_attempts);
        let next_retry_at_us = now.saturating_add(backoff.as_micros() as u64);
        if let Ok(mut guard) = status_cell.write() {
            *guard = Some(SharedFacadePendingStatus {
                route_key: pending.route_key.clone(),
                generation: pending.generation,
                resource_ids: pending.resource_ids.clone(),
                runtime_managed: pending.runtime_managed,
                runtime_exposure_confirmed: pending.runtime_exposure_confirmed,
                reason: FacadePendingReason::RetryingAfterError,
                retry_attempts,
                pending_since_us,
                last_error: Some(err.to_string()),
                last_attempt_at_us: Some(now),
                last_error_at_us: Some(now),
                retry_backoff_ms: Some(backoff.as_millis() as u64),
                next_retry_at_us: Some(next_retry_at_us),
            });
        }
    }

    fn pending_facade_retry_due(
        status_cell: &SharedFacadePendingStatusCell,
        pending: &PendingFacadeActivation,
    ) -> bool {
        let now = now_us();
        status_cell
            .read()
            .ok()
            .and_then(|guard| {
                guard.as_ref().and_then(|status| {
                    Self::pending_status_matches(status, pending).then_some(
                        status
                            .next_retry_at_us
                            .map_or(true, |deadline| deadline <= now),
                    )
                })
            })
            .unwrap_or(true)
    }

    async fn pending_facade_snapshot_for(
        &self,
        route_key: &str,
        generation: u64,
    ) -> Option<PendingFacadeActivation> {
        self.pending_facade
            .lock()
            .await
            .as_ref()
            .and_then(|pending| {
                (pending.route_key == route_key && pending.generation == generation)
                    .then_some(pending.clone())
            })
    }

    async fn retry_pending_facade(
        &self,
        route_key: &str,
        generation: u64,
        from_tick: bool,
    ) -> Result<()> {
        let Some(pending) = self
            .pending_facade_snapshot_for(route_key, generation)
            .await
        else {
            return Ok(());
        };
        let has_active = self.api_task.lock().await.is_some();
        if let Some(wait_reason) = Self::facade_replacement_wait_reason(
            self.source.as_ref(),
            self.sink.as_ref(),
            &pending,
            has_active,
        )
        .await?
        {
            Self::set_pending_facade_status_waiting(
                &self.facade_pending_status,
                &pending,
                wait_reason,
            );
            return Ok(());
        }
        if from_tick && !Self::pending_facade_retry_due(&self.facade_pending_status, &pending) {
            return Ok(());
        }
        match self.try_spawn_pending_facade().await {
            Ok(_) => Ok(()),
            Err(err) => {
                Self::record_pending_facade_retry_error(
                    &self.facade_pending_status,
                    &pending,
                    &err,
                );
                log::warn!("fs-meta facade pending activation retry failed: {err}");
                Ok(())
            }
        }
    }

    fn facade_signal_apply_priority(signal: &FacadeControlSignal) -> u8 {
        match signal {
            FacadeControlSignal::Activate {
                unit: FacadeRuntimeUnit::Facade,
                ..
            } => 0,
            _ => 1,
        }
    }

    fn facade_signal_updates_facade_claim(signal: &FacadeControlSignal) -> bool {
        matches!(
            signal,
            FacadeControlSignal::Activate {
                unit: FacadeRuntimeUnit::Facade,
                ..
            } | FacadeControlSignal::Deactivate {
                unit: FacadeRuntimeUnit::Facade,
                ..
            } | FacadeControlSignal::Tick {
                unit: FacadeRuntimeUnit::Facade,
                ..
            } | FacadeControlSignal::ExposureConfirmed {
                unit: FacadeRuntimeUnit::Facade,
                ..
            }
        )
    }

    fn facade_publication_signal_is_query_activate(signal: &FacadeControlSignal) -> bool {
        matches!(
            signal,
            FacadeControlSignal::Activate {
                unit: FacadeRuntimeUnit::Query,
                route_key,
                ..
            } if route_key == &format!("{}.req", ROUTE_KEY_QUERY)
        )
    }

    fn facade_publication_signal_is_sink_owned_query_peer_activate(
        signal: &FacadeControlSignal,
    ) -> bool {
        matches!(
            signal,
            FacadeControlSignal::Activate {
                unit: FacadeRuntimeUnit::QueryPeer,
                route_key,
                ..
            } if route_key == &format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)
                || route_key == &format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)
                || is_per_peer_sink_query_request_route(route_key)
        )
    }

    async fn record_suppressed_public_query_activate(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
        bound_scopes: &[RuntimeBoundScope],
    ) {
        if !matches!(unit, FacadeRuntimeUnit::Query)
            || route_key != format!("{}.req", ROUTE_KEY_QUERY)
        {
            return;
        }
        self.retained_suppressed_public_query_activates
            .lock()
            .await
            .insert(
                route_key.to_string(),
                FacadeControlSignal::Activate {
                    unit,
                    route_key: route_key.to_string(),
                    generation,
                    bound_scopes: bound_scopes.to_vec(),
                },
            );
    }

    async fn replay_suppressed_public_query_activates_after_publication(&self) -> Result<()> {
        let facade_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
        let publication_complete = self.pending_facade.lock().await.is_none()
            && self
                .api_task
                .lock()
                .await
                .as_ref()
                .is_some_and(|active| active.route_key == facade_route_key);
        if !publication_complete {
            return Ok(());
        }
        self.pending_fixed_bind_has_suppressed_dependent_routes
            .store(false, Ordering::Release);
        let signals = {
            let mut retained = self.retained_suppressed_public_query_activates.lock().await;
            let drained = std::mem::take(&mut *retained);
            drained.into_values().collect::<Vec<_>>()
        };
        for signal in signals {
            self.apply_facade_signal(signal).await?;
        }
        Ok(())
    }

    fn facade_spawn_error_is_bind_addr_in_use(err: &CnxError) -> bool {
        let message = err.to_string().to_ascii_lowercase();
        message.contains("fs-meta api bind failed") && message.contains("address already in use")
    }

    async fn pending_fixed_bind_facade_claim_conflict(&self, generation: u64) -> bool {
        let pending = self.pending_facade.lock().await.clone();
        let Some(pending) = pending else {
            return false;
        };
        let _ = generation;
        if pending.route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL) {
            return false;
        }
        if facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr) {
            return false;
        }
        if self.api_task.lock().await.as_ref().is_some_and(|active| {
            active.route_key == pending.route_key && active.resource_ids == pending.resource_ids
        }) {
            return false;
        }
        let guard = match process_facade_claim_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.get(&pending.resolved.bind_addr).is_some_and(|claim| {
            claim.owner_instance_id != self.instance_id && claim.matches_pending(&pending)
        })
    }

    async fn pending_fixed_bind_facade_publication_incomplete(
        &self,
        generation: u64,
    ) -> Option<PendingFacadeActivation> {
        let pending = self.pending_facade.lock().await.clone();
        let Some(pending) = pending else {
            return None;
        };
        let _ = generation;
        if pending.route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL) {
            return None;
        }
        if facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr) {
            return None;
        }
        if self.api_task.lock().await.as_ref().is_some_and(|active| {
            active.route_key == pending.route_key && active.resource_ids == pending.resource_ids
        }) {
            return None;
        }
        Some(pending)
    }

    async fn retry_pending_fixed_bind_facade_after_claim_release_without_query_followup(
        &self,
        query_followup_present: bool,
    ) -> Result<()> {
        if query_followup_present
            || !self
                .pending_fixed_bind_claim_release_followup
                .load(Ordering::Acquire)
        {
            return Ok(());
        }
        let Some(pending) = self.pending_facade.lock().await.clone() else {
            self.pending_fixed_bind_claim_release_followup
                .store(false, Ordering::Release);
            return Ok(());
        };
        if pending.route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
            || facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
        {
            self.pending_fixed_bind_claim_release_followup
                .store(false, Ordering::Release);
            return Ok(());
        }
        if self
            .pending_fixed_bind_facade_claim_conflict(pending.generation)
            .await
        {
            return Ok(());
        }
        self.retry_pending_facade(&pending.route_key, pending.generation, false)
            .await?;
        if self
            .pending_fixed_bind_facade_publication_incomplete(pending.generation)
            .await
            .is_none()
        {
            self.pending_fixed_bind_claim_release_followup
                .store(false, Ordering::Release);
        }
        Ok(())
    }

    async fn retained_active_facade_continuity_keeps_internal_status_routes(&self) -> bool {
        if !self
            .retained_active_facade_continuity
            .load(Ordering::Acquire)
        {
            return false;
        }
        let facade_control_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
        self.api_task
            .lock()
            .await
            .as_ref()
            .is_some_and(|active| active.route_key == facade_control_route_key)
    }

    async fn query_route_activate_blocked_by_pending_facade_claim_conflict(
        &self,
        generation: u64,
    ) -> bool {
        self.pending_fixed_bind_facade_claim_conflict(generation)
            .await
    }

    fn sink_route_is_facade_dependent_query_request(route_key: &str) -> bool {
        route_key == format!("{}.req", ROUTE_KEY_QUERY)
            || route_key == format!("{}.req", ROUTE_KEY_FORCE_FIND)
            || is_per_peer_sink_query_request_route(route_key)
    }

    async fn sink_route_activate_blocked_by_pending_fixed_bind_claim(
        &self,
        route_key: &str,
        generation: u64,
    ) -> bool {
        if !Self::sink_route_is_facade_dependent_query_request(route_key) {
            return false;
        }
        if self
            .pending_fixed_bind_facade_claim_conflict(generation)
            .await
        {
            return true;
        }
        self.pending_fixed_bind_claim_release_followup
            .load(Ordering::Acquire)
            && self
                .pending_fixed_bind_facade_publication_incomplete(generation)
                .await
                .is_some()
    }

    async fn filter_sink_facade_dependent_route_activates_during_pending_fixed_bind_claim(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> Vec<SinkControlSignal> {
        let mut filtered = Vec::with_capacity(sink_signals.len());
        for signal in sink_signals {
            match signal {
                SinkControlSignal::Activate {
                    route_key,
                    generation,
                    ..
                } if self
                    .sink_route_activate_blocked_by_pending_fixed_bind_claim(route_key, *generation)
                    .await =>
                {
                    self.pending_fixed_bind_has_suppressed_dependent_routes
                        .store(true, Ordering::Release);
                    eprintln!(
                        "fs_meta_runtime_app: suppress sink-owned facade-dependent route route_key={} generation={} while pending fixed-bind facade publication is incomplete",
                        route_key, generation
                    );
                }
                _ => filtered.push(signal.clone()),
            }
        }
        filtered
    }

    async fn apply_facade_activate(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
        bound_scopes: &[RuntimeBoundScope],
    ) -> Result<()> {
        eprintln!(
            "fs_meta_runtime_app: apply_facade_activate unit={} route_key={} generation={} scopes={}",
            unit.unit_id(),
            route_key,
            generation,
            bound_scopes.len()
        );
        if !facade_route_key_matches(unit, route_key) {
            return Ok(());
        }
        if matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        ) && self
            .query_route_activate_blocked_by_pending_facade_claim_conflict(generation)
            .await
        {
            self.pending_fixed_bind_has_suppressed_dependent_routes
                .store(true, Ordering::Release);
            self.record_suppressed_public_query_activate(unit, route_key, generation, bound_scopes)
                .await;
            eprintln!(
                "fs_meta_runtime_app: suppress facade-dependent route unit={} route_key={} generation={} while pending facade fixed-bind claim is still owned by another instance",
                unit.unit_id(),
                route_key,
                generation
            );
            return Ok(());
        }
        if matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        ) && self
            .pending_fixed_bind_claim_release_followup
            .load(Ordering::Acquire)
        {
            if let Some(pending) = self
                .pending_fixed_bind_facade_publication_incomplete(generation)
                .await
            {
                self.retry_pending_facade(&pending.route_key, pending.generation, false)
                    .await?;
                if self
                    .pending_fixed_bind_facade_publication_incomplete(generation)
                    .await
                    .is_some()
                {
                    self.pending_fixed_bind_has_suppressed_dependent_routes
                        .store(true, Ordering::Release);
                    self.record_suppressed_public_query_activate(
                        unit,
                        route_key,
                        generation,
                        bound_scopes,
                    )
                    .await;
                    eprintln!(
                        "fs_meta_runtime_app: suppress facade-dependent route unit={} route_key={} generation={} while pending fixed-bind facade publication is still incomplete after predecessor claim release",
                        unit.unit_id(),
                        route_key,
                        generation
                    );
                    return Ok(());
                }
                self.pending_fixed_bind_claim_release_followup
                    .store(false, Ordering::Release);
            }
        }
        // Facade activation is a runtime-owned generation/bind/run handoff carrier.
        // Trusted external observation remains subordinate to package-local
        // observation_eligible and projection catch-up; activation alone does
        // not imply current authoritative truth is already reflected outside.
        let unit_id = unit.unit_id();
        let accepted =
            self.facade_gate
                .apply_activate(unit_id, route_key, generation, bound_scopes)?;
        if !accepted {
            log::info!(
                "fs-meta facade: ignore stale activate unit={} generation={}",
                unit_id,
                generation
            );
            return Ok(());
        }
        if matches!(unit, FacadeRuntimeUnit::Query) {
            self.mirrored_query_peer_routes
                .lock()
                .await
                .remove(route_key);
        } else if matches!(unit, FacadeRuntimeUnit::QueryPeer)
            && is_dual_lane_internal_query_route(route_key)
        {
            let query_active = self
                .facade_gate
                .unit_state(execution_units::QUERY_RUNTIME_UNIT_ID)?
                .map(|(active, _)| active)
                .unwrap_or(false);
            let mut mirrored = self.mirrored_query_peer_routes.lock().await;
            if mirrored.contains_key(route_key) || !query_active {
                self.facade_gate.apply_activate(
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    route_key,
                    generation,
                    bound_scopes,
                )?;
                mirrored.insert(route_key.to_string(), generation);
            }
        }
        let facade_control_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
        if matches!(unit, FacadeRuntimeUnit::Query) || route_key != facade_control_route_key {
            return Ok(());
        }
        self.retained_active_facade_continuity
            .store(false, Ordering::Release);
        let candidate_resource_ids = Self::facade_candidate_resource_ids(bound_scopes);
        let runtime_managed = self.runtime_boundary.is_some();
        let resolved = self
            .config
            .api
            .resolve_for_candidate_ids(&candidate_resource_ids)
            .ok_or_else(|| {
                CnxError::InvalidInput(format!(
                    "fs-meta facade activation requires locally announced facade resource among {:?}",
                    candidate_resource_ids
                ))
            })?;
        let stale_active = {
            let mut api_task = self.api_task.lock().await;
            if api_task.as_ref().is_some_and(|active| {
                active.route_key == route_key
                    && active.resource_ids == candidate_resource_ids
                    && !active.handle.is_running()
            }) {
                api_task.take()
            } else {
                None
            }
        };
        if let Some(stale) = stale_active {
            eprintln!(
                "fs_meta_runtime_app: prune dead active facade handle before same-resource activate route_key={} generation={}",
                route_key, generation
            );
            stale.handle.shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT).await;
            clear_owned_process_facade_claim(self.instance_id);
            if let Some(bind_addr) = self
                .config
                .api
                .resolve_for_candidate_ids(&stale.resource_ids)
                .map(|resolved| resolved.bind_addr)
                .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
            {
                clear_active_fixed_bind_facade_owner(&bind_addr, self.instance_id);
            }
        }
        {
            let mut api_task = self.api_task.lock().await;
            if let Some(current) = api_task.as_mut()
                && current.route_key == route_key
                && current.resource_ids == candidate_resource_ids
            {
                current.generation = generation;
                drop(api_task);
                let mut pending = self.pending_facade.lock().await;
                if pending.as_ref().is_some_and(|candidate| {
                    candidate.route_key == route_key
                        && candidate.resource_ids == candidate_resource_ids
                }) {
                    pending.take();
                }
                Self::clear_pending_facade_status(&self.facade_pending_status);
                let _ = self.current_facade_service_state().await;
                return Ok(());
            }
        }
        let runtime_exposure_confirmed = !runtime_managed;
        let pending = PendingFacadeActivation {
            route_key: route_key.to_string(),
            generation,
            resource_ids: candidate_resource_ids,
            bound_scopes: bound_scopes.to_vec(),
            group_ids: Vec::new(),
            runtime_managed,
            runtime_exposure_confirmed,
            resolved,
        };
        *self.pending_facade.lock().await = Some(pending.clone());
        let _ = self.current_facade_service_state().await;
        if !self.try_spawn_pending_facade().await? {
            eprintln!(
                "fs_meta_runtime_app: pending facade generation={} route_key={} runtime_exposure_confirmed={}",
                pending.generation, pending.route_key, pending.runtime_exposure_confirmed
            );
        }
        Ok(())
    }

    async fn shutdown_active_facade(&self) {
        self.api_request_tracker.wait_for_drain().await;
        self.api_control_gate.wait_for_facade_request_drain().await;
        #[cfg(test)]
        notify_facade_shutdown_started();
        eprintln!("fs_meta_runtime_app: shutdown_active_facade");
        let released_bind_addr = {
            let api_task = self.api_task.lock().await;
            api_task
                .as_ref()
                .and_then(|current| {
                    (current.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL))
                        .then(|| {
                            self.config
                                .api
                                .resolve_for_candidate_ids(&current.resource_ids)
                        })
                        .flatten()
                })
                .map(|resolved| resolved.bind_addr)
                .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
        };
        if let Some(current) = self.api_task.lock().await.take() {
            eprintln!(
                "fs_meta_runtime_app: shutting down previous active facade generation={}",
                current.generation
            );
            current
                .handle
                .shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT)
                .await;
        }
        clear_owned_process_facade_claim(self.instance_id);
        if let Some(bind_addr) = released_bind_addr {
            clear_active_fixed_bind_facade_owner(&bind_addr, self.instance_id);
            self.complete_pending_fixed_bind_handoff_after_release(&bind_addr, self.instance_id)
                .await;
        }
        if let Some(pending) = self.pending_facade.lock().await.clone() {
            clear_pending_fixed_bind_handoff_ready(&pending.resolved.bind_addr);
        }
    }

    async fn withdraw_uninitialized_query_routes(&self) {
        let query_routes = [
            format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
        ];
        for route_key in &query_routes {
            let _ = self
                .facade_gate
                .clear_route(execution_units::QUERY_RUNTIME_UNIT_ID, route_key);
            let _ = self
                .facade_gate
                .clear_route(execution_units::QUERY_PEER_RUNTIME_UNIT_ID, route_key);
            self.mirrored_query_peer_routes
                .lock()
                .await
                .remove(route_key);
        }

        let mut endpoint_tasks = std::mem::take(&mut *self.runtime_endpoint_tasks.lock().await);
        let mut retained = Vec::with_capacity(endpoint_tasks.len());
        for mut task in endpoint_tasks.drain(..) {
            if is_uninitialized_cleanup_query_route(task.route_key()) {
                task.shutdown(Duration::from_secs(1)).await;
            } else {
                retained.push(task);
            }
        }
        *self.runtime_endpoint_tasks.lock().await = retained;

        let mut routes = self.runtime_endpoint_routes.lock().await;
        routes.retain(|route_key| !is_uninitialized_cleanup_query_route(route_key));
    }

    async fn wait_for_shared_worker_control_handoff(&self) {
        self.source
            .wait_for_control_ops_to_drain_for_handoff()
            .await;
        self.sink.wait_for_control_ops_to_drain_for_handoff().await;
    }

    async fn reinitialize_after_control_reset(&self) -> Result<()> {
        self.control_initialized.store(false, Ordering::Release);
        self.api_control_gate.set_ready(false);
        let _ = self.current_facade_service_state().await;
        self.source_state_replay_required
            .store(true, Ordering::Release);
        self.sink_state_replay_required
            .store(true, Ordering::Release);
        self.initialize_from_control(true, true).await
    }

    async fn reinitialize_after_control_reset_with_deadline(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        self.control_initialized.store(false, Ordering::Release);
        self.api_control_gate.set_ready(false);
        self.source_state_replay_required
            .store(true, Ordering::Release);
        self.sink_state_replay_required
            .store(true, Ordering::Release);
        self.initialize_from_control_with_deadline(true, true, Some(deadline))
            .await
    }

    async fn mark_control_uninitialized_after_failure(&self) {
        self.control_initialized.store(false, Ordering::Release);
        self.api_control_gate.set_ready(false);
        let _ = self.current_facade_service_state().await;
        self.retained_active_facade_continuity
            .store(false, Ordering::Release);
        self.source_state_replay_required
            .store(true, Ordering::Release);
        self.sink_state_replay_required
            .store(true, Ordering::Release);
        self.clear_shared_source_route_claims_for_instance().await;
        self.clear_shared_sink_route_claims_for_instance().await;
        self.withdraw_uninitialized_query_routes().await;
    }

    async fn source_signals_with_replay(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> Vec<SourceControlSignal> {
        if !self.source_state_replay_required.load(Ordering::Acquire) {
            return source_signals.to_vec();
        }

        if Self::source_signals_are_host_grant_change_only(source_signals) {
            return source_signals.to_vec();
        }

        let mut desired = self.retained_source_control_state.lock().await.clone();
        Self::apply_source_signals_to_state(&mut desired, source_signals);
        let mut replayed = Self::source_signals_from_state(&desired);
        replayed.extend(Self::source_transient_followup_signals(source_signals));
        replayed
    }

    async fn record_retained_source_control_state(&self, source_signals: &[SourceControlSignal]) {
        let mut retained = self.retained_source_control_state.lock().await;
        Self::apply_source_signals_to_state(&mut retained, source_signals);
    }

    fn apply_source_signals_to_state(
        state: &mut RetainedSourceControlState,
        source_signals: &[SourceControlSignal],
    ) {
        for signal in source_signals {
            match signal {
                SourceControlSignal::Activate {
                    unit, route_key, ..
                }
                | SourceControlSignal::Deactivate {
                    unit, route_key, ..
                } => {
                    state.active_by_route.insert(
                        (unit.unit_id().to_string(), route_key.clone()),
                        signal.clone(),
                    );
                }
                SourceControlSignal::RuntimeHostGrantChange { .. } => {
                    state.latest_host_grant_change = Some(signal.clone());
                }
                SourceControlSignal::Tick { .. }
                | SourceControlSignal::ManualRescan { .. }
                | SourceControlSignal::Passthrough(_) => {}
            }
        }
    }

    fn source_signals_from_state(state: &RetainedSourceControlState) -> Vec<SourceControlSignal> {
        let mut merged = Vec::new();
        if let Some(changed) = state.latest_host_grant_change.clone() {
            merged.push(changed);
        }
        merged.extend(state.active_by_route.values().cloned());
        merged
    }

    fn source_signals_are_host_grant_change_only(source_signals: &[SourceControlSignal]) -> bool {
        !source_signals.is_empty()
            && source_signals
                .iter()
                .all(|signal| matches!(signal, SourceControlSignal::RuntimeHostGrantChange { .. }))
    }

    fn source_signals_are_transient_followup_only(source_signals: &[SourceControlSignal]) -> bool {
        !source_signals.is_empty()
            && source_signals.iter().all(|signal| {
                matches!(
                    signal,
                    SourceControlSignal::Tick { .. }
                        | SourceControlSignal::ManualRescan { .. }
                        | SourceControlSignal::Passthrough(_)
                )
            })
    }

    fn source_transient_followup_signals(
        source_signals: &[SourceControlSignal],
    ) -> Vec<SourceControlSignal> {
        source_signals
            .iter()
            .filter(|signal| {
                matches!(
                    signal,
                    SourceControlSignal::Tick { .. }
                        | SourceControlSignal::ManualRescan { .. }
                        | SourceControlSignal::Passthrough(_)
                )
            })
            .cloned()
            .collect()
    }

    async fn filter_shared_source_route_deactivates(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> Vec<SourceControlSignal> {
        let claims = self.shared_source_route_claims.lock().await;
        source_signals
            .iter()
            .filter(|signal| match signal {
                SourceControlSignal::Deactivate {
                    unit, route_key, ..
                } => claims
                    .get(&(unit.unit_id().to_string(), route_key.clone()))
                    .is_none_or(|owners| owners.iter().all(|owner| *owner == self.instance_id)),
                _ => true,
            })
            .cloned()
            .collect()
    }

    async fn record_shared_source_route_claims(&self, source_signals: &[SourceControlSignal]) {
        let mut claims = self.shared_source_route_claims.lock().await;
        for signal in source_signals {
            match signal {
                SourceControlSignal::Activate {
                    unit, route_key, ..
                } => {
                    claims
                        .entry((unit.unit_id().to_string(), route_key.clone()))
                        .or_default()
                        .insert(self.instance_id);
                }
                SourceControlSignal::Deactivate {
                    unit, route_key, ..
                } => {
                    let route_id = (unit.unit_id().to_string(), route_key.clone());
                    if let Some(owners) = claims.get_mut(&route_id) {
                        owners.remove(&self.instance_id);
                        if owners.is_empty() {
                            claims.remove(&route_id);
                        }
                    }
                }
                SourceControlSignal::Tick { .. }
                | SourceControlSignal::RuntimeHostGrantChange { .. }
                | SourceControlSignal::ManualRescan { .. }
                | SourceControlSignal::Passthrough(_) => {}
            }
        }
    }

    async fn clear_shared_source_route_claims_for_instance(&self) {
        let mut claims = self.shared_source_route_claims.lock().await;
        claims.retain(|_, owners| {
            owners.remove(&self.instance_id);
            !owners.is_empty()
        });
    }

    async fn apply_source_signals_with_recovery(
        &self,
        source_signals: &[SourceControlSignal],
        control_initialized_at_entry: bool,
        fail_closed_in_generation_cutover_lane: bool,
    ) -> Result<()> {
        let filtered_source_signals = self
            .filter_shared_source_route_deactivates(source_signals)
            .await;
        if control_initialized_at_entry
            && self.control_initialized.load(Ordering::Acquire)
            && !self.source_state_replay_required.load(Ordering::Acquire)
            && !filtered_source_signals.is_empty()
            && filtered_source_signals
                .iter()
                .all(|signal| matches!(signal, SourceControlSignal::Tick { .. }))
        {
            return Ok(());
        }
        let fail_closed_restart_deferred_retire_pending = fail_closed_in_generation_cutover_lane
            && !filtered_source_signals.is_empty()
            && filtered_source_signals
                .iter()
                .all(Self::source_signal_is_restart_deferred_retire_pending);
        let defer_retained_state_until_success = fail_closed_restart_deferred_retire_pending;
        if !defer_retained_state_until_success {
            self.record_retained_source_control_state(&filtered_source_signals)
                .await;
        }
        let replay_followup_signals = if self.source_state_replay_required.load(Ordering::Acquire)
            && Self::source_signals_are_transient_followup_only(&filtered_source_signals)
        {
            let followups = Self::source_transient_followup_signals(&filtered_source_signals);
            (!followups.is_empty()).then_some(followups)
        } else {
            None
        };
        let mut replay_followup_pending = replay_followup_signals.clone();
        let deadline = tokio::time::Instant::now() + SOURCE_CONTROL_RECOVERY_TOTAL_TIMEOUT;
        loop {
            let replaying_retained_state_only =
                self.source_state_replay_required.load(Ordering::Acquire)
                    && replay_followup_pending.is_some();
            let effective_source_signals = if replaying_retained_state_only {
                self.source_signals_with_replay(&[]).await
            } else if let Some(followups) = replay_followup_pending.clone() {
                followups
            } else {
                self.source_signals_with_replay(&filtered_source_signals)
                    .await
            };
            if effective_source_signals.is_empty() {
                self.record_shared_source_route_claims(&effective_source_signals)
                    .await;
                self.source_state_replay_required
                    .store(false, Ordering::Release);
                if replaying_retained_state_only && replay_followup_pending.is_some() {
                    continue;
                }
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            #[cfg(test)]
            let source_apply_result = if let Some(err) = take_source_apply_error_queue_hook() {
                Err(err)
            } else {
                self.source
                    .apply_orchestration_signals_with_total_timeout(
                        &effective_source_signals,
                        remaining,
                    )
                    .await
            };
            #[cfg(not(test))]
            let source_apply_result = self
                .source
                .apply_orchestration_signals_with_total_timeout(
                    &effective_source_signals,
                    remaining,
                )
                .await;
            match source_apply_result {
                Ok(()) => {
                    if defer_retained_state_until_success {
                        self.record_retained_source_control_state(&filtered_source_signals)
                            .await;
                    }
                    self.record_shared_source_route_claims(&effective_source_signals)
                        .await;
                    self.source_state_replay_required
                        .store(false, Ordering::Release);
                    if replaying_retained_state_only {
                        continue;
                    }
                    replay_followup_pending = None;
                    return Ok(());
                }
                Err(err) if fail_closed_restart_deferred_retire_pending => {
                    eprintln!(
                        "fs_meta_runtime_app: source control fail-closed restart_deferred_retire_pending err={}",
                        err
                    );
                    let _ = self
                        .source
                        .reconnect_after_fail_closed_control_error()
                        .await;
                    self.mark_control_uninitialized_after_failure().await;
                    return Ok(());
                }
                Err(err)
                    if is_retryable_worker_control_reset(&err)
                        && tokio::time::Instant::now() < deadline =>
                {
                    eprintln!(
                        "fs_meta_runtime_app: source control replay after retryable reset err={}",
                        err
                    );
                    let _ = self.source.reconnect_after_retryable_control_reset().await;
                    self.reinitialize_after_control_reset_with_deadline(deadline)
                        .await?;
                }
                Err(err) => {
                    self.mark_control_uninitialized_after_failure().await;
                    return Err(err);
                }
            }
        }
    }

    async fn sink_signals_with_replay(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> Vec<SinkControlSignal> {
        let replay_retained = self.sink_state_replay_required.load(Ordering::Acquire)
            || (!sink_signals.is_empty()
                && sink_signals
                    .iter()
                    .all(|signal| matches!(signal, SinkControlSignal::Tick { .. })));
        if !replay_retained {
            return sink_signals.to_vec();
        }

        let mut desired = self.retained_sink_control_state.lock().await.clone();
        Self::apply_sink_signals_to_state(&mut desired, sink_signals);
        let replay_generation = sink_signals.iter().find_map(|signal| match signal {
            SinkControlSignal::Tick { generation, .. } => Some(*generation),
            _ => None,
        });
        let mut replayed = Self::sink_signals_from_state(&desired);
        if let Some(generation) = replay_generation {
            replayed = replayed
                .iter()
                .map(|signal| Self::rebase_sink_signal_generation(signal, generation))
                .collect();
        }
        replayed.extend(
            sink_signals
                .iter()
                .filter(|signal| matches!(signal, SinkControlSignal::Tick { .. }))
                .cloned(),
        );
        replayed
    }

    async fn record_retained_sink_control_state(&self, sink_signals: &[SinkControlSignal]) {
        let mut retained = self.retained_sink_control_state.lock().await;
        Self::apply_sink_signals_to_state(&mut retained, sink_signals);
    }

    fn apply_sink_signals_to_state(
        state: &mut RetainedSinkControlState,
        sink_signals: &[SinkControlSignal],
    ) {
        for signal in sink_signals {
            match signal {
                SinkControlSignal::Activate {
                    unit, route_key, ..
                } => {
                    state.active_by_route.insert(
                        (unit.unit_id().to_string(), route_key.clone()),
                        signal.clone(),
                    );
                }
                SinkControlSignal::Deactivate {
                    unit, route_key, ..
                } => {
                    state
                        .active_by_route
                        .remove(&(unit.unit_id().to_string(), route_key.clone()));
                }
                SinkControlSignal::RuntimeHostGrantChange { .. } => {
                    state.latest_host_grant_change = Some(signal.clone());
                }
                SinkControlSignal::Tick { .. } | SinkControlSignal::Passthrough(_) => {}
            }
        }
    }

    fn sink_signals_from_state(state: &RetainedSinkControlState) -> Vec<SinkControlSignal> {
        let mut merged = Vec::new();
        if let Some(changed) = state.latest_host_grant_change.clone() {
            merged.push(changed);
        }
        merged.extend(state.active_by_route.values().cloned());
        merged
    }

    fn rebase_sink_signal_generation(
        signal: &SinkControlSignal,
        generation: u64,
    ) -> SinkControlSignal {
        match signal {
            SinkControlSignal::Activate {
                unit,
                route_key,
                bound_scopes,
                ..
            } => SinkControlSignal::Activate {
                unit: *unit,
                route_key: route_key.clone(),
                generation,
                bound_scopes: bound_scopes.clone(),
                envelope: encode_runtime_exec_control(&RuntimeExecControl::Activate(
                    RuntimeExecActivate {
                        route_key: route_key.clone(),
                        unit_id: unit.unit_id().to_string(),
                        lease: None,
                        generation,
                        expires_at_ms: 1,
                        bound_scopes: bound_scopes.clone(),
                    },
                ))
                .expect("encode rebased sink activate"),
            },
            SinkControlSignal::Deactivate {
                unit,
                route_key,
                envelope,
                ..
            } => {
                let mut reason = "restart_deferred_retire_pending".to_string();
                let mut lease = None;
                if let Ok(Some(RuntimeExecControl::Deactivate(decoded))) =
                    decode_runtime_exec_control(envelope)
                {
                    reason = decoded.reason;
                    lease = decoded.lease;
                }
                SinkControlSignal::Deactivate {
                    unit: *unit,
                    route_key: route_key.clone(),
                    generation,
                    envelope: encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: route_key.clone(),
                            unit_id: unit.unit_id().to_string(),
                            lease,
                            generation,
                            reason,
                        },
                    ))
                    .expect("encode rebased sink deactivate"),
                }
            }
            SinkControlSignal::RuntimeHostGrantChange { .. }
            | SinkControlSignal::Tick { .. }
            | SinkControlSignal::Passthrough(_) => signal.clone(),
        }
    }

    fn sink_signal_is_cleanup_only_query_request_route(signal: &SinkControlSignal) -> bool {
        matches!(
            signal,
            SinkControlSignal::Deactivate { route_key, .. }
                if is_per_peer_sink_query_request_route(route_key)
                    || route_key == &format!("{}.req", ROUTE_KEY_QUERY)
                    || route_key == &format!("{}.req", ROUTE_KEY_SINK_QUERY_INTERNAL)
                    || route_key == &format!("{}.req", ROUTE_KEY_FORCE_FIND)
                    || route_key == &format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL)
        )
    }

    async fn filter_stale_sink_ticks(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> Vec<SinkControlSignal> {
        let retained = self.retained_sink_control_state.lock().await.clone();
        sink_signals
            .iter()
            .filter(|signal| match signal {
                SinkControlSignal::Tick {
                    unit,
                    route_key,
                    generation,
                    ..
                } => Self::sink_tick_matches_retained_active_generation(
                    &retained,
                    *unit,
                    route_key,
                    *generation,
                ),
                _ => true,
            })
            .cloned()
            .collect()
    }

    fn sink_tick_matches_retained_active_generation(
        retained: &RetainedSinkControlState,
        unit: crate::runtime::orchestration::SinkRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> bool {
        matches!(
            retained
                .active_by_route
                .get(&(unit.unit_id().to_string(), route_key.to_string())),
            Some(SinkControlSignal::Activate {
                generation: retained_generation,
                ..
            }) if generation >= *retained_generation
        )
    }

    async fn filter_shared_sink_route_deactivates(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> Vec<SinkControlSignal> {
        let claims = self.shared_sink_route_claims.lock().await;
        sink_signals
            .iter()
            .filter(|signal| match signal {
                SinkControlSignal::Deactivate {
                    unit, route_key, ..
                } => claims
                    .get(&(unit.unit_id().to_string(), route_key.clone()))
                    .is_none_or(|owners| owners.iter().all(|owner| *owner == self.instance_id)),
                _ => true,
            })
            .cloned()
            .collect()
    }

    async fn record_shared_sink_route_claims(&self, sink_signals: &[SinkControlSignal]) {
        let mut claims = self.shared_sink_route_claims.lock().await;
        for signal in sink_signals {
            match signal {
                SinkControlSignal::Activate {
                    unit, route_key, ..
                } => {
                    claims
                        .entry((unit.unit_id().to_string(), route_key.clone()))
                        .or_default()
                        .insert(self.instance_id);
                }
                SinkControlSignal::Deactivate {
                    unit, route_key, ..
                } => {
                    let route_id = (unit.unit_id().to_string(), route_key.clone());
                    if let Some(owners) = claims.get_mut(&route_id) {
                        owners.remove(&self.instance_id);
                        if owners.is_empty() {
                            claims.remove(&route_id);
                        }
                    }
                }
                SinkControlSignal::Tick { .. }
                | SinkControlSignal::RuntimeHostGrantChange { .. }
                | SinkControlSignal::Passthrough(_) => {}
            }
        }
    }

    async fn clear_shared_sink_route_claims_for_instance(&self) {
        let mut claims = self.shared_sink_route_claims.lock().await;
        claims.retain(|_, owners| {
            owners.remove(&self.instance_id);
            !owners.is_empty()
        });
    }

    async fn clear_shared_sink_query_route_claims_for_cleanup(&self) {
        let mut claims = self.shared_sink_route_claims.lock().await;
        claims.retain(|(unit_id, route_key), _| {
            !(unit_id == execution_units::SINK_RUNTIME_UNIT_ID
                && is_per_peer_sink_query_request_route(route_key))
        });
    }

    async fn sink_signals_are_in_shared_generation_cutover_lane(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> bool {
        if sink_signals.is_empty()
            || !sink_signals
                .iter()
                .all(Self::sink_signal_requires_fail_closed_retry_after_generation_cutover)
        {
            return false;
        }

        let claims = self.shared_sink_route_claims.lock().await;
        sink_signals.iter().any(|signal| {
            Self::sink_signal_is_restart_deferred_retire_pending(signal)
                || match signal {
                    SinkControlSignal::Activate {
                        unit, route_key, ..
                    }
                    | SinkControlSignal::Deactivate {
                        unit, route_key, ..
                    } => claims
                        .get(&(unit.unit_id().to_string(), route_key.clone()))
                        .is_some_and(|owners| {
                            owners.iter().any(|owner| *owner != self.instance_id)
                        }),
                    SinkControlSignal::Tick { .. }
                    | SinkControlSignal::RuntimeHostGrantChange { .. }
                    | SinkControlSignal::Passthrough(_) => false,
                }
        })
    }

    async fn apply_sink_signals_with_recovery(
        &self,
        sink_signals: &[SinkControlSignal],
        replay_retained_state: bool,
        fail_closed_in_generation_cutover_lane: bool,
    ) -> Result<()> {
        let filtered_sink_signals = self
            .filter_sink_facade_dependent_route_activates_during_pending_fixed_bind_claim(
                &self
                    .filter_shared_sink_route_deactivates(
                        &self.filter_stale_sink_ticks(sink_signals).await,
                    )
                    .await,
            )
            .await;
        if self.control_initialized.load(Ordering::Acquire)
            && !self.sink_state_replay_required.load(Ordering::Acquire)
            && self.sink.current_generation_tick_fast_path_eligible().await
            && !filtered_sink_signals.is_empty()
            && filtered_sink_signals
                .iter()
                .all(|signal| matches!(signal, SinkControlSignal::Tick { .. }))
        {
            return Ok(());
        }
        self.record_retained_sink_control_state(&filtered_sink_signals)
            .await;
        let deadline = tokio::time::Instant::now() + SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT;
        loop {
            let effective_sink_signals = if replay_retained_state {
                let replayed_sink_signals =
                    self.sink_signals_with_replay(&filtered_sink_signals).await;
                self.filter_shared_sink_route_deactivates(&replayed_sink_signals)
                    .await
            } else {
                filtered_sink_signals.clone()
            };
            if effective_sink_signals.is_empty() {
                self.record_shared_sink_route_claims(&effective_sink_signals)
                    .await;
                if replay_retained_state {
                    self.sink_state_replay_required
                        .store(false, Ordering::Release);
                }
                return Ok(());
            }
            self.record_shared_sink_route_claims(&effective_sink_signals)
                .await;
            let fail_closed_restart_deferred_retire_pending = fail_closed_in_generation_cutover_lane
                && effective_sink_signals
                    .iter()
                    .all(Self::sink_signal_is_restart_deferred_retire_pending);
            if effective_sink_signals.len() == 1 {
                eprintln!(
                    "fs_meta_runtime_app: apply_sink_signals_with_recovery effective_signals={:?} replay_retained={} fail_closed={}",
                    effective_sink_signals,
                    replay_retained_state,
                    fail_closed_in_generation_cutover_lane
                );
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self
                .sink
                .apply_orchestration_signals_with_total_timeout(&effective_sink_signals, remaining)
                .await
            {
                Ok(()) => {
                    if replay_retained_state {
                        self.record_retained_sink_control_state(&effective_sink_signals)
                            .await;
                    }
                    if replay_retained_state && self.control_initialized.load(Ordering::Acquire) {
                        self.sink_state_replay_required
                            .store(false, Ordering::Release);
                    }
                    return Ok(());
                }
                Err(err) if fail_closed_restart_deferred_retire_pending => {
                    eprintln!(
                        "fs_meta_runtime_app: sink control fail-closed restart_deferred_retire_pending err={}",
                        err
                    );
                    self.mark_control_uninitialized_after_failure().await;
                    return Ok(());
                }
                Err(err)
                    if is_retryable_worker_control_reset(&err)
                        && tokio::time::Instant::now() < deadline =>
                {
                    if fail_closed_in_generation_cutover_lane
                        && is_retryable_worker_transport_close_reset(&err)
                    {
                        self.mark_control_uninitialized_after_failure().await;
                        return Err(err);
                    }
                    eprintln!(
                        "fs_meta_runtime_app: sink control replay after retryable reset err={}",
                        err
                    );
                    if replay_retained_state {
                        self.reinitialize_after_control_reset_with_deadline(deadline)
                            .await?;
                    } else {
                        self.sink.ensure_started().await?;
                    }
                }
                Err(err) => {
                    self.mark_control_uninitialized_after_failure().await;
                    return Err(err);
                }
            }
        }
    }

    async fn apply_facade_deactivate(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> Result<()> {
        eprintln!(
            "fs_meta_runtime_app: apply_facade_deactivate unit={} route_key={} generation={}",
            unit.unit_id(),
            route_key,
            generation
        );
        if !facade_route_key_matches(unit, route_key) {
            return Ok(());
        }
        let query_lane = matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        );
        let sink_query_proxy_route = format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY);
        if matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        ) && is_internal_status_route(route_key)
            && self.control_initialized()
        {
            self.api_control_gate.wait_for_facade_request_drain().await;
        }
        if query_lane && route_key == sink_query_proxy_route {
            self.sink.wait_for_control_ops_to_drain_for_handoff().await;
        }
        if !self.control_initialized() && query_lane && route_key == sink_query_proxy_route {
            let current_generation = self
                .facade_gate
                .route_generation(unit.unit_id(), route_key)?
                .unwrap_or(0);
            // Only clear per-peer sink-query shared claims as part of the uninitialized
            // stale cleanup chain. Clearing these claims during a live proxy deactivate
            // can disrupt in-flight internal proxy requests.
            if generation < current_generation {
                self.clear_shared_sink_query_route_claims_for_cleanup()
                    .await;
            }
        }
        let unit_id = unit.unit_id();
        #[cfg(test)]
        maybe_pause_facade_deactivate(unit_id, route_key).await;
        if matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        ) && is_facade_dependent_query_route(route_key)
            && self
                .retained_active_facade_continuity_keeps_internal_status_routes()
                .await
        {
            eprintln!(
                "fs_meta_runtime_app: retain facade-dependent query route during continuity-preserving deactivate route_key={} generation={}",
                route_key, generation
            );
            return Ok(());
        }
        let accepted = self
            .facade_gate
            .apply_deactivate(unit_id, route_key, generation)?;
        if !accepted {
            log::info!(
                "fs-meta facade: ignore stale deactivate unit={} generation={}",
                unit_id,
                generation
            );
            return Ok(());
        }
        if matches!(unit, FacadeRuntimeUnit::Query) {
            self.mirrored_query_peer_routes
                .lock()
                .await
                .remove(route_key);
        } else if matches!(unit, FacadeRuntimeUnit::QueryPeer)
            && is_dual_lane_internal_query_route(route_key)
        {
            let mut mirrored = self.mirrored_query_peer_routes.lock().await;
            if mirrored
                .get(route_key)
                .is_some_and(|mirrored_generation| generation >= *mirrored_generation)
            {
                self.facade_gate.apply_deactivate(
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    route_key,
                    generation,
                )?;
                mirrored.remove(route_key);
            }
        }
        let facade_control_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
        if matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        ) {
            return Ok(());
        }
        if !matches!(unit, FacadeRuntimeUnit::Facade) || route_key != facade_control_route_key {
            return Ok(());
        }
        let retain_active_facade = {
            let active_guard = self.api_task.lock().await;
            active_guard.as_ref().is_some_and(|active| {
                active.route_key == route_key && generation >= active.generation
            })
        };
        let pending_fixed_bind_conflict = self
            .pending_fixed_bind_facade_claim_conflict(generation)
            .await;
        let release_for_fixed_bind_handoff =
            self.fixed_bind_handoff_ready_for_release(generation).await;
        if retain_active_facade && !pending_fixed_bind_conflict && !release_for_fixed_bind_handoff {
            self.retained_active_facade_continuity
                .store(true, Ordering::Release);
            eprintln!(
                "fs_meta_runtime_app: retain active facade during continuity-preserving deactivate route_key={} generation={}",
                route_key, generation
            );
            return Ok(());
        }
        if pending_fixed_bind_conflict {
            if retain_active_facade {
                self.retained_active_facade_continuity
                    .store(true, Ordering::Release);
                eprintln!(
                    "fs_meta_runtime_app: retain active facade while pending fixed-bind claim remains owned elsewhere route_key={} generation={}",
                    route_key, generation
                );
                return Ok(());
            } else {
                eprintln!(
                    "fs_meta_runtime_app: retain pending facade during fixed-bind continuity-preserving deactivate route_key={} generation={}",
                    route_key, generation
                );
                return Ok(());
            }
        }
        if release_for_fixed_bind_handoff {
            if retain_active_facade {
                eprintln!(
                    "fs_meta_runtime_app: release active facade for fixed-bind handoff route_key={} generation={}",
                    route_key, generation
                );
            } else {
                eprintln!(
                    "fs_meta_runtime_app: retain pending facade during fixed-bind continuity-preserving deactivate route_key={} generation={}",
                    route_key, generation
                );
                return Ok(());
            }
        }
        let retain_pending_spawn = {
            let pending = self.pending_facade.lock().await.clone();
            if let Some(pending) = pending {
                if pending.route_key != route_key {
                    false
                } else {
                    self.facade_spawn_in_progress
                        .lock()
                        .await
                        .as_ref()
                        .is_some_and(|inflight| inflight.matches_pending(&pending))
                }
            } else {
                false
            }
        };
        if retain_pending_spawn {
            eprintln!(
                "fs_meta_runtime_app: retain pending facade during in-flight spawn route_key={} generation={}",
                route_key, generation
            );
            return Ok(());
        }
        self.retained_active_facade_continuity
            .store(false, Ordering::Release);
        *self.pending_facade.lock().await = None;
        Self::clear_pending_facade_status(&self.facade_pending_status);
        let _ = self.current_facade_service_state().await;
        let _ = self.current_facade_service_state().await;
        self.wait_for_shared_worker_control_handoff().await;
        self.shutdown_active_facade().await;
        Ok(())
    }

    fn accept_facade_tick(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> Result<bool> {
        if !facade_route_key_matches(unit, route_key) {
            return Ok(false);
        }
        self.facade_gate
            .accept_tick(unit.unit_id(), route_key, generation)
    }

    async fn confirm_pending_facade_exposure(&self, route_key: &str, generation: u64) -> bool {
        let mut pending_guard = self.pending_facade.lock().await;
        let Some(pending) = pending_guard.as_mut() else {
            return false;
        };
        if pending.route_key != route_key || pending.generation != generation {
            return false;
        }
        pending.runtime_exposure_confirmed = true;
        true
    }

    async fn apply_facade_signal(&self, signal: FacadeControlSignal) -> Result<()> {
        match signal {
            FacadeControlSignal::Activate {
                unit,
                route_key,
                generation,
                bound_scopes,
            } => {
                self.apply_facade_activate(unit, &route_key, generation, &bound_scopes)
                    .await?;
            }
            FacadeControlSignal::Deactivate {
                unit,
                route_key,
                generation,
            } => {
                self.apply_facade_deactivate(unit, &route_key, generation)
                    .await?;
            }
            FacadeControlSignal::Tick {
                unit,
                route_key,
                generation,
            } => {
                let accepted = self.accept_facade_tick(unit, &route_key, generation)?;
                if !accepted {
                    log::info!(
                        "fs-meta facade: ignore stale/inactive tick unit={} generation={}",
                        unit.unit_id(),
                        generation
                    );
                } else if matches!(unit, FacadeRuntimeUnit::Facade)
                    && route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                {
                    self.retry_pending_facade(&route_key, generation, true)
                        .await?;
                }
            }
            FacadeControlSignal::ExposureConfirmed {
                unit,
                route_key,
                generation,
                confirmed_at_us: _confirmed_at_us,
            } => {
                let accepted = self.accept_facade_tick(unit, &route_key, generation)?;
                if !accepted {
                    log::info!(
                        "fs-meta facade: ignore stale/inactive exposure_confirmed unit={} generation={}",
                        unit.unit_id(),
                        generation
                    );
                } else if matches!(unit, FacadeRuntimeUnit::Facade)
                    && route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                    && self
                        .confirm_pending_facade_exposure(&route_key, generation)
                        .await
                {
                    self.retry_pending_facade(&route_key, generation, false)
                        .await?;
                }
            }
            FacadeControlSignal::RuntimeHostGrantChange { .. }
            | FacadeControlSignal::Passthrough => {}
        }
        Ok(())
    }

    async fn fixed_bind_handoff_ready_for_release(&self, generation: u64) -> bool {
        let _ = generation;
        let Some(active) = self
            .api_task
            .lock()
            .await
            .as_ref()
            .map(|active| (active.route_key.clone(), active.resource_ids.clone()))
        else {
            return false;
        };
        if active.0 != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL) {
            return false;
        }
        let bind_addr = self
            .config
            .api
            .resolve_for_candidate_ids(&active.1)
            .map(|resolved| resolved.bind_addr)
            .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr));
        let Some(bind_addr) = bind_addr else {
            return false;
        };
        let ready = {
            let guard = match pending_fixed_bind_handoff_ready_cell().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.get(&bind_addr).cloned()
        };
        let Some(ready) = ready else {
            return false;
        };
        if ready.instance_id == self.instance_id {
            return false;
        }
        let Some(pending) = ready.registrant.pending_facade.lock().await.clone() else {
            return false;
        };
        pending.runtime_exposure_confirmed
            || !ready
                .registrant
                .pending_fixed_bind_has_suppressed_dependent_routes
                .load(Ordering::Acquire)
    }

    fn pending_fixed_bind_handoff_registrant(&self) -> PendingFixedBindHandoffRegistrant {
        PendingFixedBindHandoffRegistrant {
            instance_id: self.instance_id,
            api_task: self.api_task.clone(),
            pending_facade: self.pending_facade.clone(),
            pending_fixed_bind_has_suppressed_dependent_routes: self
                .pending_fixed_bind_has_suppressed_dependent_routes
                .clone(),
            facade_spawn_in_progress: self.facade_spawn_in_progress.clone(),
            facade_pending_status: self.facade_pending_status.clone(),
            facade_service_state: self.facade_service_state.clone(),
            api_request_tracker: self.api_request_tracker.clone(),
            api_control_gate: self.api_control_gate.clone(),
            node_id: self.node_id.clone(),
            runtime_boundary: self.runtime_boundary.clone(),
            source: self.source.clone(),
            sink: self.sink.clone(),
            query_sink: self.query_sink.clone(),
            query_runtime_boundary: self.runtime_boundary.clone(),
        }
    }

    fn active_fixed_bind_facade_registrant(&self) -> ActiveFixedBindFacadeRegistrant {
        ActiveFixedBindFacadeRegistrant {
            instance_id: self.instance_id,
            api_task: self.api_task.clone(),
            api_request_tracker: self.api_request_tracker.clone(),
            api_control_gate: self.api_control_gate.clone(),
        }
    }

    async fn refresh_active_fixed_bind_facade_owner(&self) {
        let active = self
            .api_task
            .lock()
            .await
            .as_ref()
            .map(|active| (active.route_key.clone(), active.resource_ids.clone()));
        let Some((route_key, resource_ids)) = active else {
            return;
        };
        if route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL) {
            return;
        }
        let Some(bind_addr) = self
            .config
            .api
            .resolve_for_candidate_ids(&resource_ids)
            .map(|resolved| resolved.bind_addr)
            .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
        else {
            return;
        };
        mark_active_fixed_bind_facade_owner(&bind_addr, self.active_fixed_bind_facade_registrant());
    }

    async fn release_conflicting_active_fixed_bind_facade_for_handoff(&self, bind_addr: &str) {
        let Some(owner) = active_fixed_bind_facade_owner_for(bind_addr, self.instance_id) else {
            return;
        };
        let released_by_instance_id = owner.instance_id;
        Self::release_active_fixed_bind_facade_owner_for_handoff(bind_addr, owner).await;
        self.complete_pending_fixed_bind_handoff_after_release(bind_addr, released_by_instance_id)
            .await;
    }

    async fn release_active_fixed_bind_facade_owner_for_handoff(
        bind_addr: &str,
        owner: ActiveFixedBindFacadeRegistrant,
    ) {
        owner.api_request_tracker.wait_for_drain().await;
        owner.api_control_gate.wait_for_facade_request_drain().await;
        #[cfg(test)]
        notify_facade_shutdown_started();
        if let Some(current) = owner.api_task.lock().await.take() {
            tokio::time::sleep(FIXED_BIND_HANDOFF_LATE_REQUEST_GRACE).await;
            owner.api_request_tracker.wait_for_drain().await;
            owner.api_control_gate.wait_for_facade_request_drain().await;
            current
                .handle
                .shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT)
                .await;
        }
        clear_owned_process_facade_claim(owner.instance_id);
        clear_active_fixed_bind_facade_owner(bind_addr, owner.instance_id);
    }

    async fn complete_pending_fixed_bind_handoff_after_release(
        &self,
        bind_addr: &str,
        released_by_instance_id: u64,
    ) {
        let Some(registrant) =
            pending_fixed_bind_handoff_registrant_for(bind_addr, released_by_instance_id)
        else {
            return;
        };
        let api_task = registrant.api_task.clone();
        let pending_facade = registrant.pending_facade.clone();
        let api_control_gate = registrant.api_control_gate.clone();
        let api_request_tracker = registrant.api_request_tracker.clone();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            match Self::try_spawn_pending_facade_from_parts(
                registrant.instance_id,
                registrant.api_task.clone(),
                registrant.pending_facade.clone(),
                registrant.facade_spawn_in_progress.clone(),
                registrant
                    .pending_fixed_bind_has_suppressed_dependent_routes
                    .clone(),
                registrant.facade_pending_status.clone(),
                registrant.facade_service_state.clone(),
                registrant.api_request_tracker.clone(),
                registrant.api_control_gate.clone(),
                registrant.node_id.clone(),
                registrant.runtime_boundary.clone(),
                registrant.source.clone(),
                registrant.sink.clone(),
                registrant.query_sink.clone(),
                registrant.query_runtime_boundary.clone(),
            )
            .await
            {
                Ok(_) => {
                    if pending_facade.lock().await.is_none()
                        && api_task.lock().await.as_ref().is_some_and(|active| {
                            active.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                        })
                    {
                        mark_active_fixed_bind_facade_owner(
                            bind_addr,
                            ActiveFixedBindFacadeRegistrant {
                                instance_id: registrant.instance_id,
                                api_task: api_task.clone(),
                                api_request_tracker: api_request_tracker.clone(),
                                api_control_gate: api_control_gate.clone(),
                            },
                        );
                        api_control_gate.set_ready(true);
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        break;
                    }
                }
                Err(err) => {
                    if tokio::time::Instant::now() >= deadline {
                        eprintln!(
                            "fs_meta_runtime_app: fixed-bind handoff completion retry failed bind_addr={} err={}",
                            bind_addr, err
                        );
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn facade_publication_ready(&self) -> bool {
        let pending = self.pending_facade.lock().await.clone();
        let facade_only_pending_handoff = pending.as_ref().is_some_and(|pending| {
            pending.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                && !self
                    .pending_fixed_bind_has_suppressed_dependent_routes
                    .load(Ordering::Acquire)
        });
        let source_replay_required = self.source_state_replay_required.load(Ordering::Acquire);
        let sink_replay_required = self.sink_state_replay_required.load(Ordering::Acquire);
        if !self.control_initialized()
            && !(facade_only_pending_handoff && !source_replay_required && !sink_replay_required)
        {
            return false;
        }
        if source_replay_required || sink_replay_required {
            return false;
        }
        let Some(pending) = pending else {
            return true;
        };
        if pending.route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL) {
            return true;
        }
        let active_ready = self.api_task.lock().await.as_ref().is_some_and(|active| {
            active.route_key == pending.route_key && active.resource_ids == pending.resource_ids
        });
        if active_ready {
            clear_pending_fixed_bind_handoff_ready(&pending.resolved.bind_addr);
            return true;
        }
        if !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr) {
            let allow_fixed_bind_handoff = pending.runtime_exposure_confirmed;
            mark_pending_fixed_bind_handoff_ready(
                &pending.resolved.bind_addr,
                self.pending_fixed_bind_handoff_registrant(),
            );
            if self
                .pending_fixed_bind_facade_claim_conflict(pending.generation)
                .await
                && allow_fixed_bind_handoff
            {
                if let Some(owner) = active_fixed_bind_facade_owner_for(
                    &pending.resolved.bind_addr,
                    self.instance_id,
                ) {
                    self.release_conflicting_active_fixed_bind_facade_for_handoff(
                        &pending.resolved.bind_addr,
                    )
                    .await;
                    if self.api_task.lock().await.as_ref().is_some_and(|active| {
                        active.route_key == pending.route_key
                            && active.resource_ids == pending.resource_ids
                    }) {
                        clear_pending_fixed_bind_handoff_ready(&pending.resolved.bind_addr);
                        return true;
                    }
                    let _ = owner;
                } else if self
                    .pending_fixed_bind_claim_release_followup
                    .load(Ordering::Acquire)
                {
                    if let Some(claim) = conflicting_process_facade_claim_for(
                        &pending.resolved.bind_addr,
                        self.instance_id,
                    ) {
                        clear_process_facade_claim_for_bind_addr(
                            &pending.resolved.bind_addr,
                            claim.owner_instance_id,
                        );
                        self.complete_pending_fixed_bind_handoff_after_release(
                            &pending.resolved.bind_addr,
                            claim.owner_instance_id,
                        )
                        .await;
                        if self.api_task.lock().await.as_ref().is_some_and(|active| {
                            active.route_key == pending.route_key
                                && active.resource_ids == pending.resource_ids
                        }) {
                            clear_pending_fixed_bind_handoff_ready(&pending.resolved.bind_addr);
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    async fn service_on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        let (source_signals, sink_signals, mut facade_signals) =
            split_app_control_signals(envelopes)?;
        facade_signals.sort_by_key(Self::facade_signal_apply_priority);
        let requires_shared_serial = !source_signals.is_empty()
            || !sink_signals.is_empty()
            || self.control_initialized()
            || facade_signals
                .iter()
                .any(Self::facade_signal_requires_shared_serial_while_uninitialized)
            || (!self.source_state_replay_required.load(Ordering::Acquire)
                && !self.sink_state_replay_required.load(Ordering::Acquire));
        let _serial_guard = self.control_frame_serial.lock().await;
        let _shared_serial_guard = if requires_shared_serial {
            Some(self.shared_control_frame_serial.lock().await)
        } else {
            None
        };
        let _lease_guard = match (
            requires_shared_serial,
            self.control_frame_lease_path.as_deref(),
        ) {
            (true, Some(path)) => {
                let deadline = tokio::time::Instant::now() + CONTROL_FRAME_LEASE_ACQUIRE_BUDGET;
                Some(
                    ControlFrameLeaseGuard::acquire(path, deadline, || {
                        self.closing.load(Ordering::Acquire)
                    })
                    .await
                    .map_err(|err| match err.kind() {
                        std::io::ErrorKind::TimedOut | std::io::ErrorKind::Interrupted => {
                            CnxError::NotReady(format!(
                                "fs-meta control-frame lease acquisition failed closed at {}: {err}",
                                path.display()
                            ))
                        }
                        _ => CnxError::Internal(format!(
                            "fs-meta control-frame lease acquisition failed at {}: {err}",
                            path.display()
                        )),
                    })?,
                )
            }
            _ => None,
        };
        let control_initialized_at_entry = self.control_initialized();
        let retained_sink_state_present_at_entry = !self
            .retained_sink_control_state
            .lock()
            .await
            .active_by_route
            .is_empty();
        #[cfg(test)]
        notify_runtime_control_frame_started();
        let should_initialize_from_control =
            Self::should_initialize_from_control(&source_signals, &sink_signals, &facade_signals);
        let initialize_wait_for_source_worker_handoff = !source_signals.is_empty();
        let initialize_wait_for_sink_worker_handoff = !sink_signals.is_empty();
        let request_sensitive =
            !source_signals.is_empty() || !sink_signals.is_empty() || !facade_signals.is_empty();
        if request_sensitive && self.api_request_tracker.inflight() > 0 {
            let skip_request_drain_for_uninitialized_recovery =
                !self.control_initialized() && should_initialize_from_control;
            if !skip_request_drain_for_uninitialized_recovery {
                self.api_request_tracker.wait_for_drain().await;
            }
        }
        if request_sensitive {
            self.api_control_gate.set_ready(false);
        }
        eprintln!(
            "fs_meta_runtime_app: on_control_frame begin source_signals={} sink_signals={} facade_signals={} initialized={}",
            source_signals.len(),
            sink_signals.len(),
            facade_signals.len(),
            self.control_initialized()
        );
        if source_signals.is_empty() && facade_signals.is_empty() && sink_signals.len() == 1 {
            eprintln!(
                "fs_meta_runtime_app: on_control_frame sink_only_lane signals={:?}",
                sink_signals
            );
        }
        if std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
            && source_signals.len() <= 2
            && !source_signals.is_empty()
        {
            eprintln!(
                "fs_meta_runtime_app: on_control_frame source_lane signals={:?}",
                source_signals
            );
        }
        let facade_cleanup_only_while_uninitialized = !self.control_initialized()
            && source_signals.is_empty()
            && sink_signals.is_empty()
            && !facade_signals.is_empty()
            && facade_signals
                .iter()
                .all(Self::facade_signal_is_cleanup_only);
        let source_cleanup_only_while_uninitialized = !self.control_initialized()
            && sink_signals.is_empty()
            && facade_signals.is_empty()
            && !source_signals.is_empty()
            && source_signals
                .iter()
                .all(Self::source_signal_is_restart_deferred_retire_pending);
        let sink_cleanup_only_while_uninitialized = !self.control_initialized()
            && source_signals.is_empty()
            && facade_signals.is_empty()
            && !sink_signals.is_empty()
            && sink_signals.iter().all(Self::sink_signal_is_cleanup_only);
        let sink_query_cleanup_only_while_uninitialized = sink_cleanup_only_while_uninitialized
            && sink_signals
                .iter()
                .all(Self::sink_signal_is_cleanup_only_query_request_route);
        let fail_closed_in_source_generation_cutover_lane = control_initialized_at_entry
            && sink_signals.is_empty()
            && facade_signals.is_empty()
            && !source_signals.is_empty()
            && source_signals
                .iter()
                .all(Self::source_signal_is_restart_deferred_retire_pending);
        let fail_closed_in_generation_cutover_lane = control_initialized_at_entry
            && source_signals.is_empty()
            && facade_signals.is_empty()
            && sink_signals.len() == 1
            && self
                .sink_signals_are_in_shared_generation_cutover_lane(&sink_signals)
                .await;
        if should_initialize_from_control {
            if !source_cleanup_only_while_uninitialized && !sink_cleanup_only_while_uninitialized {
                self.initialize_from_control(
                    initialize_wait_for_source_worker_handoff,
                    initialize_wait_for_sink_worker_handoff,
                )
                .await?;
            }
        } else if !self.control_initialized()
            && !facade_cleanup_only_while_uninitialized
            && !source_cleanup_only_while_uninitialized
            && !sink_cleanup_only_while_uninitialized
        {
            return Err(Self::not_ready_error());
        }
        if !facade_cleanup_only_while_uninitialized
            && !source_cleanup_only_while_uninitialized
            && !sink_cleanup_only_while_uninitialized
        {
            self.ensure_runtime_proxy_endpoints_started().await?;
        }
        if source_cleanup_only_while_uninitialized {
            self.record_shared_source_route_claims(&source_signals)
                .await;
        }
        if sink_query_cleanup_only_while_uninitialized {
            self.withdraw_uninitialized_query_routes().await;
            self.clear_shared_sink_query_route_claims_for_cleanup()
                .await;
        }
        if !sink_signals.is_empty() {
            let staged_sink_signals = if sink_cleanup_only_while_uninitialized {
                sink_signals.clone()
            } else {
                self.filter_shared_sink_route_deactivates(&sink_signals)
                    .await
            };
            if !sink_query_cleanup_only_while_uninitialized {
                self.record_retained_sink_control_state(&staged_sink_signals)
                    .await;
            }
        }
        let (facade_claim_signals, mut facade_publication_signals): (Vec<_>, Vec<_>) =
            facade_signals
                .into_iter()
                .partition(Self::facade_signal_updates_facade_claim);
        let sink_status_publication_present = facade_publication_signals
            .iter()
            .any(facade_publication_signal_is_sink_status_activate);
        let facade_claim_signals_present = !facade_claim_signals.is_empty();
        let mut pretriggered_source_to_sink_convergence = false;
        let mut mixed_recovery_expected_sink_groups = None;
        for signal in facade_claim_signals {
            self.apply_facade_signal(signal).await?;
        }
        let ordinary_source_tick_only_steady_noop = control_initialized_at_entry
            && self.control_initialized()
            && !self.source_state_replay_required.load(Ordering::Acquire)
            && !source_signals.is_empty()
            && ((!facade_claim_signals_present && facade_publication_signals.is_empty())
                || !sink_signals.is_empty())
            && source_signals
                .iter()
                .all(|signal| matches!(signal, SourceControlSignal::Tick { .. }));
        let sink_tick_fast_path_eligible =
            self.sink.current_generation_tick_fast_path_eligible().await;
        let ordinary_sink_tick_only_steady_noop = control_initialized_at_entry
            && self.control_initialized()
            && !self.sink_state_replay_required.load(Ordering::Acquire)
            && sink_tick_fast_path_eligible
            && !sink_signals.is_empty()
            && sink_signals
                .iter()
                .all(|signal| matches!(signal, SinkControlSignal::Tick { .. }));
        let sink_tick_recovery_requires_status_republish = control_initialized_at_entry
            && retained_sink_state_present_at_entry
            && !sink_tick_fast_path_eligible
            && source_signals.is_empty()
            && !facade_claim_signals_present
            && facade_publication_signals.is_empty()
            && !sink_signals.is_empty()
            && sink_signals
                .iter()
                .all(|signal| matches!(signal, SinkControlSignal::Tick { .. }));
        if facade_cleanup_only_while_uninitialized {
            for signal in facade_publication_signals.drain(..) {
                self.apply_facade_signal(signal).await?;
            }
            eprintln!(
                "fs_meta_runtime_app: on_control_frame cleanup-only facade followup left runtime uninitialized"
            );
            return Ok(());
        }
        let apply_sink_before_initial_source_wave = !control_initialized_at_entry
            && !retained_sink_state_present_at_entry
            && !source_signals.is_empty()
            && !sink_signals.is_empty()
            && !source_cleanup_only_while_uninitialized
            && !sink_query_cleanup_only_while_uninitialized
            && !ordinary_source_tick_only_steady_noop
            && !ordinary_sink_tick_only_steady_noop;
        if apply_sink_before_initial_source_wave {
            #[cfg(test)]
            note_sink_apply_entry_for_tests();
            #[cfg(test)]
            maybe_pause_before_sink_apply().await;
            eprintln!(
                "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals begin"
            );
            if let Err(err) = self
                .apply_sink_signals_with_recovery(
                    &sink_signals,
                    !sink_cleanup_only_while_uninitialized,
                    fail_closed_in_generation_cutover_lane,
                )
                .await
            {
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals err={}",
                    err
                );
                return Err(err);
            }
            eprintln!("fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals ok");
            if sink_tick_recovery_requires_status_republish {
                if let Some(expected_groups) = self
                    .wait_for_sink_status_republish_readiness_after_recovery(false)
                    .await?
                {
                    self.wait_for_local_sink_status_republish_after_recovery(&expected_groups)
                        .await?;
                }
            }
        }
        if !source_signals.is_empty() {
            if source_cleanup_only_while_uninitialized {
                if !self
                    .retained_sink_control_state
                    .lock()
                    .await
                    .active_by_route
                    .is_empty()
                {
                    self.sink_state_replay_required
                        .store(true, Ordering::Release);
                }
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame source cleanup-only followup left runtime uninitialized"
                );
            } else if ordinary_source_tick_only_steady_noop {
            } else {
                #[cfg(test)]
                note_source_apply_entry_for_tests();
                #[cfg(test)]
                maybe_pause_before_source_apply().await;
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame source.apply_orchestration_signals begin"
                );
                if let Err(err) = self
                    .apply_source_signals_with_recovery(
                        &source_signals,
                        control_initialized_at_entry,
                        fail_closed_in_source_generation_cutover_lane,
                    )
                    .await
                {
                    eprintln!(
                        "fs_meta_runtime_app: on_control_frame source.apply_orchestration_signals err={}",
                        err
                    );
                    return Err(err);
                }
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame source.apply_orchestration_signals ok"
                );
                if !control_initialized_at_entry && sink_signals.is_empty() {
                    let retained_sink_routes_present = !self
                        .retained_sink_control_state
                        .lock()
                        .await
                        .active_by_route
                        .is_empty();
                    if retained_sink_routes_present {
                        if sink_status_publication_present {
                            self.source.trigger_rescan_when_ready().await?;
                            pretriggered_source_to_sink_convergence = true;
                        }
                        // A later source-only recovery can reopen the runtime before the sink
                        // worker has replayed its retained control state into the current
                        // generation. Keep the sink replay armed so peer status/query routes do
                        // not resume against a zero-state sink snapshot.
                        self.sink_state_replay_required
                            .store(true, Ordering::Release);
                    }
                }
            }
        } else if self.source_state_replay_required.load(Ordering::Acquire)
            && !source_cleanup_only_while_uninitialized
            && !sink_cleanup_only_while_uninitialized
        {
            eprintln!("fs_meta_runtime_app: on_control_frame source.replay_retained begin");
            self.apply_source_signals_with_recovery(&[], control_initialized_at_entry, false)
                .await?;
            eprintln!("fs_meta_runtime_app: on_control_frame source.replay_retained ok");
        }
        let mut replayed_sink_state_after_uninitialized_source_recovery = false;
        if !sink_signals.is_empty() && !apply_sink_before_initial_source_wave {
            if sink_query_cleanup_only_while_uninitialized {
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame sink cleanup-only query followup left runtime uninitialized"
                );
            } else if ordinary_sink_tick_only_steady_noop {
            } else {
                #[cfg(test)]
                note_sink_apply_entry_for_tests();
                #[cfg(test)]
                maybe_pause_before_sink_apply().await;
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals begin"
                );
                if let Err(err) = self
                    .apply_sink_signals_with_recovery(
                        &sink_signals,
                        !sink_cleanup_only_while_uninitialized,
                        fail_closed_in_generation_cutover_lane,
                    )
                    .await
                {
                    eprintln!(
                        "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals err={}",
                        err
                    );
                    return Err(err);
                }
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals ok"
                );
                if sink_tick_recovery_requires_status_republish {
                    if let Some(expected_groups) = self
                        .wait_for_sink_status_republish_readiness_after_recovery(false)
                        .await?
                    {
                        self.wait_for_local_sink_status_republish_after_recovery(&expected_groups)
                            .await?;
                    }
                }
                if !control_initialized_at_entry
                    && retained_sink_state_present_at_entry
                    && !source_signals.is_empty()
                {
                    let expected_groups =
                        Self::runtime_scoped_facade_group_ids(&self.source, &self.sink)
                            .await?
                            .into_iter()
                            .collect::<std::collections::BTreeSet<_>>();
                    if !expected_groups.is_empty() {
                        mixed_recovery_expected_sink_groups = Some(expected_groups);
                    }
                }
            }
        } else if self.sink_state_replay_required.load(Ordering::Acquire)
            && !source_cleanup_only_while_uninitialized
            && !sink_cleanup_only_while_uninitialized
        {
            eprintln!("fs_meta_runtime_app: on_control_frame sink.replay_retained begin");
            let replay_signals =
                current_generation_sink_replay_tick(&source_signals, &facade_publication_signals)
                    .into_iter()
                    .collect::<Vec<_>>();
            self.apply_sink_signals_with_recovery(&replay_signals, true, false)
                .await?;
            eprintln!("fs_meta_runtime_app: on_control_frame sink.replay_retained ok");
            replayed_sink_state_after_uninitialized_source_recovery = !control_initialized_at_entry;
        }
        let mut defer_api_control_gate_reopen_until_sink_status_ready = None;
        let deferred_local_sink_replay_signals =
            if replayed_sink_state_after_uninitialized_source_recovery
                && sink_status_publication_present
            {
                self.current_generation_retained_sink_replay_signals_for_local_republish()
                    .await
            } else {
                Vec::new()
            };
        if replayed_sink_state_after_uninitialized_source_recovery
            && sink_status_publication_present
        {
            let source_led_uninitialized_mixed_recovery =
                !source_signals.is_empty() && sink_signals.is_empty();
            if source_led_uninitialized_mixed_recovery {
                let expected_groups =
                    Self::runtime_scoped_facade_group_ids(&self.source, &self.sink)
                        .await?
                        .into_iter()
                        .collect::<std::collections::BTreeSet<_>>();
                if !expected_groups.is_empty() {
                    let cached_sink_status_ready = self
                        .sink
                        .cached_status_snapshot()
                        .ok()
                        .is_some_and(|snapshot| {
                            sink_status_snapshot_ready_for_expected_groups(
                                &snapshot,
                                &expected_groups,
                            )
                        });
                    if cached_sink_status_ready {
                        eprintln!(
                            "fs_meta_runtime_app: skipping deferred sink-status republish wait after source-led uninitialized mixed recovery groups={expected_groups:?}"
                        );
                    } else {
                        eprintln!(
                            "fs_meta_runtime_app: deferring sink-status republish wait after source-led uninitialized mixed recovery groups={expected_groups:?}"
                        );
                        defer_api_control_gate_reopen_until_sink_status_ready =
                            Some(expected_groups);
                    }
                } else {
                    defer_api_control_gate_reopen_until_sink_status_ready = self
                        .wait_for_sink_status_republish_readiness_after_recovery(
                            pretriggered_source_to_sink_convergence,
                        )
                        .await?;
                }
            } else {
                defer_api_control_gate_reopen_until_sink_status_ready = self
                    .wait_for_sink_status_republish_readiness_after_recovery(
                        pretriggered_source_to_sink_convergence,
                    )
                    .await?;
            }
        }
        let (deferred_sink_owned_query_peer_publication_signals, facade_publication_signals): (
            Vec<_>,
            Vec<_>,
        ) = if defer_api_control_gate_reopen_until_sink_status_ready.is_some() {
            facade_publication_signals
                .into_iter()
                .partition(Self::facade_publication_signal_is_sink_owned_query_peer_activate)
        } else {
            (Vec::new(), facade_publication_signals)
        };
        let query_publication_followup_present = facade_publication_signals
            .iter()
            .any(Self::facade_publication_signal_is_query_activate);
        if !source_cleanup_only_while_uninitialized && !sink_cleanup_only_while_uninitialized {
            self.retry_pending_fixed_bind_facade_after_claim_release_without_query_followup(
                query_publication_followup_present,
            )
            .await?;
        }
        for signal in facade_publication_signals {
            self.apply_facade_signal(signal).await?;
        }
        self.replay_suppressed_public_query_activates_after_publication()
            .await?;
        if !sink_cleanup_only_while_uninitialized {
            self.ensure_runtime_proxy_endpoints_started().await?;
        }
        if let Some(expected_groups) = mixed_recovery_expected_sink_groups.as_ref() {
            self.wait_for_local_sink_status_republish_after_recovery(expected_groups)
                .await?;
        }
        if request_sensitive && !sink_cleanup_only_while_uninitialized {
            if let Some(expected_groups) = defer_api_control_gate_reopen_until_sink_status_ready {
                self.suppress_deferred_sink_owned_query_peer_publication_signals(
                    &deferred_sink_owned_query_peer_publication_signals,
                )
                .await?;
                let control_ready_after_republish = self.facade_publication_ready().await;
                self.api_control_gate
                    .set_ready(control_ready_after_republish);
                let _ = self.current_facade_service_state().await;
                Self::spawn_deferred_sink_owned_query_peer_publication_after_local_sink_status_ready(
                    self.source.clone(),
                    self.sink.clone(),
                    expected_groups,
                    deferred_local_sink_replay_signals,
                    deferred_sink_owned_query_peer_publication_signals,
                    self.facade_gate.clone(),
                    self.mirrored_query_peer_routes.clone(),
                    self.api_control_gate.clone(),
                    control_ready_after_republish,
                );
            } else {
                self.api_control_gate
                    .set_ready(self.facade_publication_ready().await);
                let _ = self.current_facade_service_state().await;
            }
        }
        eprintln!("fs_meta_runtime_app: on_control_frame done");
        Ok(())
    }

    pub async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        self.service_on_control_frame(envelopes).await
    }

    async fn service_close(&self) -> Result<()> {
        self.closing.store(true, Ordering::Release);
        self.api_request_tracker.wait_for_drain().await;
        self.control_initialized.store(false, Ordering::Release);
        self.api_control_gate.set_ready(false);
        self.retained_active_facade_continuity
            .store(false, Ordering::Release);
        self.pending_fixed_bind_has_suppressed_dependent_routes
            .store(false, Ordering::Release);
        self.clear_shared_source_route_claims_for_instance().await;
        self.clear_shared_sink_route_claims_for_instance().await;
        let mut fixed_bind_addrs = std::collections::BTreeSet::new();
        if let Some(active) = self
            .api_task
            .lock()
            .await
            .as_ref()
            .map(|active| active.resource_ids.clone())
            && let Some(bind_addr) = self
                .config
                .api
                .resolve_for_candidate_ids(&active)
                .map(|resolved| resolved.bind_addr)
                .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
        {
            fixed_bind_addrs.insert(bind_addr);
        }
        if let Some(pending_bind_addr) = self
            .pending_facade
            .lock()
            .await
            .as_ref()
            .map(|pending| pending.resolved.bind_addr.clone())
            .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
        {
            fixed_bind_addrs.insert(pending_bind_addr);
        }
        *self.pending_facade.lock().await = None;
        *self.facade_spawn_in_progress.lock().await = None;
        Self::clear_pending_facade_status(&self.facade_pending_status);
        self.shutdown_active_facade().await;
        clear_owned_process_facade_claim(self.instance_id);
        for bind_addr in fixed_bind_addrs {
            clear_active_fixed_bind_facade_owner(&bind_addr, self.instance_id);
            clear_pending_fixed_bind_handoff_ready(&bind_addr);
        }
        self.source.close().await?;
        self.sink.close().await?;
        let mut endpoint_tasks = std::mem::take(&mut *self.runtime_endpoint_tasks.lock().await);
        for task in &mut endpoint_tasks {
            task.shutdown(Duration::from_secs(2)).await;
        }
        self.runtime_endpoint_routes.lock().await.clear();
        if let Some(handle) = self.pump_task.lock().await.take() {
            handle.abort();
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.service_close().await
    }

    pub async fn query_tree(
        &self,
        params: &InternalQueryRequest,
    ) -> Result<std::collections::BTreeMap<String, TreeGroupPayload>> {
        let events = self.sink.materialized_query(params).await?;
        let mut grouped = std::collections::BTreeMap::<String, TreeGroupPayload>::new();
        for event in &events {
            let payload = rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
                .map_err(|e| CnxError::Internal(format!("decode tree response failed: {e}")))?;
            let MaterializedQueryPayload::Tree(response) = payload else {
                return Err(CnxError::Internal(
                    "unexpected stats payload for query_tree".into(),
                ));
            };
            grouped.insert(event.metadata().origin_id.0.clone(), response);
        }
        Ok(grouped)
    }

    pub async fn query_stats(&self, path: &[u8]) -> Result<SubtreeStats> {
        let events = self.sink.subtree_stats(path).await?;
        let mut agg = SubtreeStats::default();
        for event in &events {
            let stats = rmp_serde::from_slice::<SubtreeStats>(event.payload_bytes())
                .map_err(|e| CnxError::Internal(format!("decode stats response failed: {e}")))?;
            agg.total_nodes += stats.total_nodes;
            agg.total_files += stats.total_files;
            agg.total_dirs += stats.total_dirs;
            agg.total_size += stats.total_size;
            agg.attested_count += stats.attested_count;
            agg.blind_spot_count += stats.blind_spot_count;
        }
        Ok(agg)
    }

    pub async fn source_status_snapshot(&self) -> Result<crate::source::SourceStatusSnapshot> {
        self.source.status_snapshot().await
    }

    pub async fn sink_status_snapshot(&self) -> Result<crate::sink::SinkStatusSnapshot> {
        self.sink.status_snapshot().await
    }

    pub async fn trigger_rescan_when_ready(&self) -> Result<()> {
        self.source.trigger_rescan_when_ready().await
    }

    pub async fn query_node(&self, path: &[u8]) -> Result<Option<QueryNode>> {
        self.sink.query_node(path).await
    }
}

impl ManagedStateProfile for FSMetaApp {
    fn managed_state_declaration(&self) -> ManagedStateDeclaration {
        ManagedStateDeclaration::new(
            "statecell authoritative journal",
            "materialized sink/query projection tree",
            "authoritative_revision",
            "observed_projection_revision",
            "shared observation evaluator drives trusted-materialized and observation_eligible",
            "generation high-water plus statecell stale-writer fencing",
        )
    }
}

pub struct FSMetaRuntimeApp {
    runtime: RuntimeLoadedServiceApp,
}

impl FSMetaRuntimeApp {
    fn init_error_message(err: CnxError) -> String {
        match err {
            CnxError::InvalidInput(msg) => msg,
            other => other.to_string(),
        }
    }

    #[cfg(test)]
    fn runtime_local_host_ref(
        cfg: &std::collections::HashMap<String, ConfigValue>,
    ) -> Option<NodeId> {
        let runtime = match cfg.get("__cnx_runtime") {
            Some(ConfigValue::Map(map)) => map,
            _ => return None,
        };
        let local_host_ref = match runtime.get("local_host_ref") {
            Some(ConfigValue::String(v)) => v.trim(),
            _ => return None,
        };
        if local_host_ref.is_empty() {
            None
        } else {
            Some(NodeId(local_host_ref.to_string()))
        }
    }

    #[cfg(test)]
    fn required_runtime_local_host_ref(
        cfg: &std::collections::HashMap<String, ConfigValue>,
    ) -> Result<NodeId> {
        Self::runtime_local_host_ref(cfg).ok_or_else(|| {
            CnxError::InvalidInput(
                "__cnx_runtime.local_host_ref is required for fs-meta local execution identity"
                    .to_string(),
            )
        })
    }

    fn runtime_worker_bindings_from_bootstrap(
        bootstrap: &RuntimeBootstrapContext,
    ) -> Result<(RuntimeWorkerBinding, RuntimeWorkerBinding)> {
        runtime_worker_client_bindings(&bootstrap.worker_bindings()?)
    }

    fn build_from_runtime_boundaries(
        runtime_boundary: Arc<dyn RuntimeBoundary>,
        data_boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> Self {
        let format_error = |err: CnxError| {
            let msg = Self::init_error_message(err);
            log::error!("fs-meta runtime init failed: {msg}");
            msg
        };
        let runtime = RuntimeLoadedServiceApp::from_runtime_config(
            runtime_boundary,
            data_boundary,
            FSMetaConfig::from_runtime_manifest_config,
            move |bootstrap, cfg| {
                let boundary_handles = boundary_handles(&bootstrap);
                let ordinary_boundary = boundary_handles.ordinary_boundary();
                let (source_worker_binding, sink_worker_binding) =
                    Self::runtime_worker_bindings_from_bootstrap(&bootstrap)?;
                let app = Arc::new(FSMetaApp::with_boundaries_and_state(
                    cfg,
                    source_worker_binding,
                    sink_worker_binding,
                    bootstrap.local_host_ref().clone(),
                    boundary_handles.data_boundary(),
                    Some(ordinary_boundary),
                    boundary_handles.state_boundary(),
                )?);
                let close_app = app.clone();
                let built = AppBuilder::new()
                    .register_role("facade")
                    .request({
                        let app = app.clone();
                        move |_context, events| {
                            let app = app.clone();
                            async move { app.service_send(&events).await }
                        }
                    })
                    .stream({
                        let app = app.clone();
                        move |_context, opts| {
                            let app = app.clone();
                            async move { app.service_recv(opts).await }
                        }
                    })
                    .control({
                        let app = app.clone();
                        move |_context, envelopes| {
                            let app = app.clone();
                            async move { app.service_on_control_frame(&envelopes).await }
                        }
                    })
                    .close(move |_context| {
                        let app = close_app.clone();
                        async move { app.service_close().await }
                    })
                    .build(bootstrap.service_context());
                Ok(built)
            },
            format_error,
        );
        Self { runtime }
    }

    pub fn new_without_io(boundary: Arc<dyn RuntimeBoundary>) -> Self {
        Self::build_from_runtime_boundaries(boundary, None)
    }

    pub fn new(
        boundary: Arc<dyn RuntimeBoundary>,
        data_boundary: Arc<dyn ChannelIoSubset>,
    ) -> Self {
        Self::build_from_runtime_boundaries(boundary, Some(data_boundary))
    }
}

#[async_trait]
impl RuntimeBoundaryApp for FSMetaRuntimeApp {
    async fn start(&self) -> Result<()> {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.runtime.start().await
        } else {
            shared_tokio_runtime().block_on(self.runtime.start())
        }
    }

    async fn send(&self, events: &[Event], timeout: Duration) -> Result<()> {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.runtime.send(events, timeout).await
        } else {
            shared_tokio_runtime().block_on(self.runtime.send(events, timeout))
        }
    }

    async fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.runtime.recv(opts).await
        } else {
            shared_tokio_runtime().block_on(self.runtime.recv(opts))
        }
    }

    async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.runtime.on_control_frame(envelopes).await
        } else {
            shared_tokio_runtime().block_on(self.runtime.on_control_frame(envelopes))
        }
    }

    async fn close(&self) -> Result<()> {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.runtime.close().await.or(Ok(()))
        } else {
            shared_tokio_runtime()
                .block_on(self.runtime.close())
                .or(Ok(()))
        }
    }
}

#[cfg(test)]
mod tests;
