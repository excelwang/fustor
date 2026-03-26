use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use crate::api::facade_status::{
    FacadePendingReason, SharedFacadePendingStatus, SharedFacadePendingStatusCell,
    shared_facade_pending_status_cell,
};
use crate::query::TreeGroupPayload;
#[cfg(test)]
use crate::query::observation::{
    ObservationTrustPolicy, candidate_group_observation_evidence, evaluate_observation_status,
};
use crate::query::reliability::GroupReliability;
#[cfg(test)]
use crate::query::tree::ObservationState;
use crate::query::tree::{TreePageRoot, TreeStability};
use crate::query::{InternalQueryRequest, MaterializedQueryPayload, QueryNode, SubtreeStats};
use crate::runtime::endpoint::ManagedEndpointTask;
use crate::runtime::execution_units;
use crate::runtime::orchestration::{
    FacadeControlSignal, FacadeRuntimeUnit, SinkControlSignal, SourceControlSignal,
    split_app_control_signals,
};
use crate::runtime::routes::{
    METHOD_QUERY, METHOD_SINK_QUERY, METHOD_SINK_QUERY_PROXY, METHOD_SINK_STATUS, METHOD_SOURCE_FIND,
    METHOD_SOURCE_STATUS, ROUTE_KEY_FACADE_CONTROL, ROUTE_KEY_QUERY, ROUTE_KEY_SINK_QUERY_PROXY,
    ROUTE_KEY_SINK_STATUS_INTERNAL, ROUTE_KEY_SOURCE_FIND_INTERNAL,
    ROUTE_KEY_SOURCE_STATUS_INTERNAL, ROUTE_TOKEN_FS_META, ROUTE_TOKEN_FS_META_INTERNAL,
    default_route_bindings,
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
use capanix_runtime_entry_sdk::control::RuntimeBoundScope;
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
        "groups={} group_details={:?} scheduled={:?} received_batches={:?} received_events={:?} received_origin_counts={:?} stream_received_batches={:?} stream_received_events={:?} stream_received_origin_counts={:?} stream_ready_origin_counts={:?} stream_deferred_origin_counts={:?} stream_dropped_origin_counts={:?} stream_applied_batches={:?} stream_applied_events={:?} stream_applied_control_events={:?} stream_applied_data_events={:?} stream_applied_origin_counts={:?} stream_last_applied_at_us={:?}",
        snapshot.groups.len(),
        summarize_sink_groups(&snapshot.groups),
        summarize_groups_by_node(&snapshot.scheduled_groups_by_node),
        summarize_counts_by_node(&snapshot.received_batches_by_node),
        summarize_counts_by_node(&snapshot.received_events_by_node),
        summarize_groups_by_node(&snapshot.received_origin_counts_by_node),
        summarize_counts_by_node(&snapshot.stream_received_batches_by_node),
        summarize_counts_by_node(&snapshot.stream_received_events_by_node),
        summarize_groups_by_node(&snapshot.stream_received_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_ready_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_deferred_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_dropped_origin_counts_by_node),
        summarize_counts_by_node(&snapshot.stream_applied_batches_by_node),
        summarize_counts_by_node(&snapshot.stream_applied_events_by_node),
        summarize_counts_by_node(&snapshot.stream_applied_control_events_by_node),
        summarize_counts_by_node(&snapshot.stream_applied_data_events_by_node),
        summarize_groups_by_node(&snapshot.stream_applied_origin_counts_by_node),
        summarize_counts_by_node(&snapshot.stream_last_applied_at_us_by_node),
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

fn selected_group_bridge_eligible_from_sink_status(
    request: &InternalQueryRequest,
    snapshot: &crate::sink::SinkStatusSnapshot,
) -> bool {
    let Some(selected_group) = request.scope.selected_group.as_ref() else {
        return true;
    };
    snapshot
        .groups
        .iter()
        .any(|group| group.group_id == *selected_group && group.live_nodes > 0)
}

fn should_bridge_selected_group_sink_query(
    request: &InternalQueryRequest,
    local_events: &[Event],
    local_selected_group_bridge_eligible: bool,
) -> bool {
    if request.op != crate::query::QueryOp::Tree || request.scope.selected_group.is_none() {
        return false;
    }
    if !local_selected_group_bridge_eligible {
        return false;
    }
    let selected_group = request
        .scope
        .selected_group
        .as_deref()
        .expect("selected_group must be present for bridge decision");
    let has_selected_group_tree_payload = local_events.iter().any(|event| {
        event.metadata().origin_id.0 == selected_group
            && matches!(
                rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
                Ok(MaterializedQueryPayload::Tree(_))
            )
    });
    if has_selected_group_tree_payload {
        return false;
    }
    let has_materialized_tree_data = local_events.iter().any(|event| {
        matches!(
            rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
            Ok(MaterializedQueryPayload::Tree(payload))
                if payload.root.exists || payload.root.has_children || !payload.entries.is_empty()
        )
    });
    !has_materialized_tree_data
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
    control_frame_serial: Mutex<()>,
    facade_pending_status: SharedFacadePendingStatusCell,
    facade_gate: RuntimeUnitGate,
    control_initialized: AtomicBool,
    control_init_lock: Mutex<()>,
}

impl FSMetaApp {
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
            control_frame_serial: Mutex::new(()),
            facade_pending_status: shared_facade_pending_status_cell(),
            facade_gate: RuntimeUnitGate::new(
                "fs-meta",
                &[
                    execution_units::FACADE_RUNTIME_UNIT_ID,
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                ],
            ),
            control_initialized: AtomicBool::new(false),
            control_init_lock: Mutex::new(()),
        })
    }

    fn control_initialized(&self) -> bool {
        self.control_initialized.load(Ordering::Acquire)
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

    fn facade_signal_can_initialize(signal: &FacadeControlSignal) -> bool {
        matches!(
            signal,
            FacadeControlSignal::Activate { .. }
                | FacadeControlSignal::Deactivate { .. }
                | FacadeControlSignal::Tick { .. }
                | FacadeControlSignal::ExposureConfirmed { .. }
        )
    }

    fn not_ready_error() -> CnxError {
        CnxError::NotReady(
            "fs-meta request handling is unavailable until runtime control initializes the app"
                .into(),
        )
    }

    async fn initialize_from_control(&self) -> Result<()> {
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

        if !self.sink.is_worker() {
            eprintln!("fs_meta_runtime_app: initialize_from_control sink.ensure_started begin");
            self.sink.ensure_started().await?;
            eprintln!("fs_meta_runtime_app: initialize_from_control sink.ensure_started ok");
        }
        let mut guard = self.pump_task.lock().await;
        if guard.is_none() {
            eprintln!("fs_meta_runtime_app: initialize_from_control source.start begin");
            *guard = self
                .source
                .start(self.sink.clone(), self.runtime_boundary.clone())
                .await?;
            eprintln!("fs_meta_runtime_app: initialize_from_control source.start ok");
        }
        drop(guard);

        eprintln!("fs_meta_runtime_app: initialize_from_control endpoints begin");
        self.ensure_runtime_proxy_endpoints_started().await?;
        eprintln!("fs_meta_runtime_app: initialize_from_control endpoints ok");
        self.control_initialized.store(true, Ordering::Release);
        eprintln!("fs_meta_runtime_app: initialize_from_control done");

        Ok(())
    }

    pub async fn start(&self) -> Result<()> {
        self.initialize_from_control().await
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
                let sink = self.sink.clone();
                eprintln!(
                    "fs_meta_runtime_app: spawning sink status endpoint route={}",
                    route.0
                );
                let endpoint = ManagedEndpointTask::spawn(
                    boundary.clone(),
                    route,
                    format!(
                        "app:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS
                    ),
                    tokio_util::sync::CancellationToken::new(),
                    move |requests| {
                        let sink = sink.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                match sink.status_snapshot_nonblocking().await {
                                    Ok(snapshot) => {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink status endpoint response {}",
                                            summarize_sink_status_endpoint(&snapshot)
                                        );
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
                                    }
                                    Err(err) => {
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
                let source = self.source.clone();
                eprintln!(
                    "fs_meta_runtime_app: spawning source status endpoint route={}",
                    route.0
                );
                let endpoint = ManagedEndpointTask::spawn(
                    boundary.clone(),
                    route,
                    format!(
                        "app:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS
                    ),
                    tokio_util::sync::CancellationToken::new(),
                    move |requests| {
                        let source = source.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
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
                                let snapshot = source.observability_snapshot_nonblocking().await;
                                trace_guard.phase("after_source_snapshot_await");
                                eprintln!(
                                    "fs_meta_runtime_app: source status endpoint response groups={} runners={} correlation={:?} trace_id={}",
                                    snapshot.source_primary_by_group.len(),
                                    snapshot.last_force_find_runner_by_group.len(),
                                    req.metadata().correlation_id,
                                    trace_id
                                );
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
                eprintln!(
                    "fs_meta_runtime_app: spawning sink query proxy endpoint route={}",
                    route.0
                );
                let boundary_for_calls = boundary.clone();
                let sink = self.sink.clone();
                let source = self.source.clone();
                let node_id = self.node_id.clone();
                let endpoint = ManagedEndpointTask::spawn(
                    boundary.clone(),
                    route,
                    format!(
                        "app:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY
                    ),
                    tokio_util::sync::CancellationToken::new(),
                    move |requests| {
                        let adapter = crate::runtime::seam::exchange_host_adapter(
                            boundary_for_calls.clone(),
                            node_id.clone(),
                            crate::runtime::routes::default_route_bindings(),
                        );
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
                                        let local_selected_group_bridge_eligible = sink
                                            .status_snapshot_nonblocking()
                                            .await
                                            .ok()
                                            .map(|snapshot| {
                                                selected_group_bridge_eligible_from_sink_status(
                                                    &params, &snapshot,
                                                )
                                            })
                                            .unwrap_or(true);
                                        if should_bridge_selected_group_sink_query(
                                            &params,
                                            &events,
                                            local_selected_group_bridge_eligible,
                                        ) {
                                            match rmp_serde::to_vec(&params) {
                                                Ok(payload) => {
                                                    match capanix_host_adapter_fs::HostAdapter::call_collect(
                                                        &adapter,
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
                                        if events.is_empty() {
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
                eprintln!(
                    "fs_meta_runtime_app: spawning source find proxy endpoint route={}",
                    route.0
                );
                let source = self.source.clone();
                let endpoint = ManagedEndpointTask::spawn(
                    boundary.clone(),
                    route,
                    format!(
                        "app:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND
                    ),
                    tokio_util::sync::CancellationToken::new(),
                    move |requests| {
                        let source = source.clone();
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
                                match source.force_find(&params).await {
                                    Ok(mut events) => {
                                        eprintln!(
                                            "fs_meta_runtime_app: source find proxy response events={}",
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
                                            "fs_meta_runtime_app: source find proxy failed err={}",
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

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
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
        Self::try_spawn_pending_facade_from_parts(
            self.instance_id,
            self.api_task.clone(),
            self.pending_facade.clone(),
            self.facade_spawn_in_progress.clone(),
            self.facade_pending_status.clone(),
            self.node_id.clone(),
            self.runtime_boundary.clone(),
            self.source.clone(),
            self.sink.clone(),
            self.query_sink.clone(),
            self.runtime_boundary.clone(),
        )
        .await
    }

    async fn try_spawn_pending_facade_from_parts(
        instance_id: u64,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
        facade_spawn_in_progress: Arc<Mutex<Option<FacadeSpawnInProgress>>>,
        facade_pending_status: SharedFacadePendingStatusCell,
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
            facade_pending_status,
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
             facade_pending_status| async move {
                api::spawn(
                    resolved,
                    node_id,
                    runtime_boundary,
                    source,
                    sink,
                    query_sink,
                    query_runtime_boundary,
                    facade_pending_status,
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
        facade_pending_status: SharedFacadePendingStatusCell,
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
                return Ok(false);
            }
            guard.insert(claim.bind_addr.clone(), claim);
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
        if replacing_existing && !pending.runtime_exposure_confirmed {
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
            handle.shutdown(Duration::from_secs(2)).await;
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
            current.handle.shutdown(Duration::from_secs(2)).await;
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
                reason: FacadePendingReason::AwaitingRuntimeExposure,
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
        if has_active && !pending.runtime_exposure_confirmed {
            Self::set_pending_facade_status_waiting(&self.facade_pending_status, &pending);
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
        let facade_control_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
        if matches!(unit, FacadeRuntimeUnit::Query) || route_key != facade_control_route_key {
            return Ok(());
        }
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
        if !self.try_spawn_pending_facade().await? {
            eprintln!(
                "fs_meta_runtime_app: pending facade generation={} route_key={} awaiting_runtime_exposure={}",
                pending.generation, pending.route_key, !pending.runtime_exposure_confirmed
            );
            Self::set_pending_facade_status_waiting(&self.facade_pending_status, &pending);
        }
        Ok(())
    }

    async fn shutdown_active_facade(&self) {
        eprintln!("fs_meta_runtime_app: shutdown_active_facade");
        if let Some(current) = self.api_task.lock().await.take() {
            eprintln!(
                "fs_meta_runtime_app: shutting down previous active facade generation={}",
                current.generation
            );
            current.handle.shutdown(Duration::from_secs(2)).await;
        }
        clear_owned_process_facade_claim(self.instance_id);
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
        let unit_id = unit.unit_id();
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
        *self.pending_facade.lock().await = None;
        Self::clear_pending_facade_status(&self.facade_pending_status);
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

    async fn service_on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        let _serial_guard = self.control_frame_serial.lock().await;
        let (source_signals, sink_signals, facade_signals) = split_app_control_signals(envelopes)?;
        eprintln!(
            "fs_meta_runtime_app: on_control_frame begin source_signals={} sink_signals={} facade_signals={} initialized={}",
            source_signals.len(),
            sink_signals.len(),
            facade_signals.len(),
            self.control_initialized()
        );
        if Self::should_initialize_from_control(&source_signals, &sink_signals, &facade_signals) {
            self.initialize_from_control().await?;
        } else if !self.control_initialized() {
            return Err(Self::not_ready_error());
        }
        self.ensure_runtime_proxy_endpoints_started().await?;
        for signal in facade_signals {
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
        }
        if !source_signals.is_empty() {
            eprintln!(
                "fs_meta_runtime_app: on_control_frame source.apply_orchestration_signals begin"
            );
            self.source
                .apply_orchestration_signals(&source_signals)
                .await?;
            eprintln!(
                "fs_meta_runtime_app: on_control_frame source.apply_orchestration_signals ok"
            );
        }
        if !sink_signals.is_empty() {
            eprintln!(
                "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals begin"
            );
            self.sink.apply_orchestration_signals(&sink_signals).await?;
            eprintln!("fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals ok");
        }
        self.ensure_runtime_proxy_endpoints_started().await?;
        eprintln!("fs_meta_runtime_app: on_control_frame done");
        Ok(())
    }

    pub async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        self.service_on_control_frame(envelopes).await
    }

    async fn service_close(&self) -> Result<()> {
        self.control_initialized.store(false, Ordering::Release);
        *self.pending_facade.lock().await = None;
        *self.facade_spawn_in_progress.lock().await = None;
        Self::clear_pending_facade_status(&self.facade_pending_status);
        self.shutdown_active_facade().await;
        clear_owned_process_facade_claim(self.instance_id);
        self.source.close().await?;
        self.sink.close().await?;
        let mut endpoint_tasks = std::mem::take(&mut *self.runtime_endpoint_tasks.lock().await);
        for task in &mut endpoint_tasks {
            task.shutdown(Duration::from_secs(2)).await;
        }
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
mod tests {
    use super::*;
    use crate::{ControlEvent, FileMetaRecord};
    use crate::{FSMetaConfig, FSMetaProductConfig, api, query, source};
    use bytes::Bytes;
    use capanix_app_sdk::raw::{
        BoundaryContext, ChannelBoundary, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
        StateBoundary,
    };
    use capanix_app_sdk::runtime::{
        EventMetadata, LogLevel, RuntimeWorkerBinding, RuntimeWorkerBindings,
        RuntimeWorkerLauncherKind, in_memory_state_boundary,
    };
    use capanix_host_fs_types::UnixStat;
    use capanix_runtime_entry_sdk::control::{
        RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeHostDescriptor,
        RuntimeHostGrant, RuntimeHostGrantChange, RuntimeHostGrantState, RuntimeHostObjectType,
        RuntimeObjectDescriptor, RuntimeUnitExposure, RuntimeUnitTick, encode_runtime_exec_control,
        encode_runtime_host_grant_change, encode_runtime_unit_exposure, encode_runtime_unit_tick,
    };
    use reqwest::Client;
    use serde_json::json;
    use std::collections::HashMap;
    use std::fs;
    use std::net::TcpListener;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Mutex as StdMutex, OnceLock};
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::{Mutex as AsyncMutex, Notify};

    #[cfg(target_os = "linux")]
    mod real_nfs_lab {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/runtime_app_real_nfs_lab.rs"
        ));
    }

    fn write_auth_files(dir: &tempfile::TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
        let passwd = dir.path().join("fs-meta.passwd");
        let shadow = dir.path().join("fs-meta.shadow");
        fs::write(
            &passwd,
            "admin:1000:1000:fsmeta_management:/home/admin:/bin/bash:0\n",
        )
        .expect("write passwd");
        fs::write(&shadow, "admin:plain$admin:0\n").expect("write shadow");
        (passwd, shadow)
    }

    fn local_source_config() -> SourceConfig {
        SourceConfig::default()
    }

    fn fs_meta_worker_module_path(path: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(path)
    }

    fn compiled_runtime_worker_binding(
        role_id: &str,
        mode: WorkerMode,
        module_path: Option<&str>,
    ) -> RuntimeWorkerBinding {
        RuntimeWorkerBinding {
            role_id: role_id.to_string(),
            mode,
            launcher_kind: match mode {
                WorkerMode::Embedded => RuntimeWorkerLauncherKind::Embedded,
                WorkerMode::External => RuntimeWorkerLauncherKind::WorkerHost,
            },
            module_path: module_path.map(std::path::PathBuf::from),
            socket_dir: None,
        }
    }

    fn compiled_runtime_worker_bindings(
        bindings: &[(&str, WorkerMode, Option<&str>)],
    ) -> RuntimeWorkerBindings {
        RuntimeWorkerBindings {
            roles: bindings
                .iter()
                .map(|(role_id, mode, module_path)| {
                    (
                        (*role_id).to_string(),
                        compiled_runtime_worker_binding(role_id, *mode, *module_path),
                    )
                })
                .collect(),
        }
    }

    struct NoopBoundary;

    impl ChannelIoSubset for NoopBoundary {}

    fn facade_control_stream_route() -> String {
        format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
    }

    fn activate_envelope(unit_id: &str) -> ControlEnvelope {
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: facade_control_stream_route(),
            unit_id: unit_id.to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 60_000,
            bound_scopes: Vec::new(),
        }))
        .expect("encode activate envelope")
    }

    fn activate_envelope_with_scopes(
        unit_id: &str,
        scope_id: &str,
        resource_ids: &[&str],
    ) -> ControlEnvelope {
        activate_envelope_with_generation_and_scopes(unit_id, scope_id, resource_ids, 1)
    }

    fn activate_envelope_with_generation_and_scopes(
        unit_id: &str,
        scope_id: &str,
        resource_ids: &[&str],
        generation: u64,
    ) -> ControlEnvelope {
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: facade_control_stream_route(),
            unit_id: unit_id.to_string(),
            lease: None,
            generation,
            expires_at_ms: 60_000,
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: scope_id.to_string(),
                resource_ids: resource_ids.iter().map(|id| (*id).to_string()).collect(),
            }],
        }))
        .expect("encode activate envelope with scopes")
    }

    fn activate_envelope_with_scope_rows(
        unit_id: &str,
        scopes: &[(&str, &[&str])],
        generation: u64,
    ) -> ControlEnvelope {
        activate_envelope_with_route_key_and_scope_rows(
            unit_id,
            facade_control_stream_route(),
            scopes,
            generation,
        )
    }

    fn activate_envelope_with_route_key_and_scope_rows(
        unit_id: &str,
        route_key: String,
        scopes: &[(&str, &[&str])],
        generation: u64,
    ) -> ControlEnvelope {
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key,
            unit_id: unit_id.to_string(),
            lease: None,
            generation,
            expires_at_ms: 60_000,
            bound_scopes: scopes
                .iter()
                .map(|(scope_id, resource_ids)| RuntimeBoundScope {
                    scope_id: (*scope_id).to_string(),
                    resource_ids: resource_ids
                        .iter()
                        .map(|id| (*id).to_string())
                        .collect(),
                })
                .collect(),
        }))
        .expect("encode activate envelope with scope rows")
    }

    fn host_object_grants_changed_envelope(version: u64, mount_point: &str) -> ControlEnvelope {
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version,
            grants: vec![RuntimeHostGrant {
                object_ref: "single-app-node::root-1".to_string(),
                object_type: RuntimeHostObjectType::MountRoot,
                interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                host: RuntimeHostDescriptor {
                    host_ref: "single-app-node".to_string(),
                    host_ip: "127.0.0.1".to_string(),
                    host_name: Some("single-app-node".to_string()),
                    site: None,
                    zone: None,
                    host_labels: Default::default(),
                },
                object: RuntimeObjectDescriptor {
                    mount_point: mount_point.to_string(),
                    fs_source: mount_point.to_string(),
                    fs_type: "nfs".to_string(),
                    mount_options: Vec::new(),
                },
                grant_state: RuntimeHostGrantState::Active,
            }],
        })
        .expect("encode runtime host object grants changed envelope")
    }

    fn host_object_grants_changed_rows_envelope(
        version: u64,
        rows: &[(&str, &str, &str, &str, &str, bool)],
    ) -> ControlEnvelope {
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version,
            grants: rows
                .iter()
                .map(
                    |(object_ref, host_ref, host_ip, mount_point, fs_source, active)| {
                        RuntimeHostGrant {
                            object_ref: (*object_ref).to_string(),
                            object_type: RuntimeHostObjectType::MountRoot,
                            interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                            host: RuntimeHostDescriptor {
                                host_ref: (*host_ref).to_string(),
                                host_ip: (*host_ip).to_string(),
                                host_name: Some((*host_ref).to_string()),
                                site: None,
                                zone: None,
                                host_labels: Default::default(),
                            },
                            object: RuntimeObjectDescriptor {
                                mount_point: (*mount_point).to_string(),
                                fs_source: (*fs_source).to_string(),
                                fs_type: "nfs".to_string(),
                                mount_options: Vec::new(),
                            },
                            grant_state: if *active {
                                RuntimeHostGrantState::Active
                            } else {
                                RuntimeHostGrantState::Revoked
                            },
                        }
                    },
                )
                .collect(),
        })
        .expect("encode runtime host object grants changed rows envelope")
    }

    #[allow(dead_code)]
    fn tick_envelope(unit_id: &str, generation: u64) -> ControlEnvelope {
        encode_runtime_unit_tick(&RuntimeUnitTick {
            route_key: facade_control_stream_route(),
            unit_id: unit_id.to_string(),
            generation,
            at_ms: 1,
        })
        .expect("encode unit tick envelope")
    }

    #[allow(dead_code)]
    fn trusted_exposure_confirmed_envelope(unit_id: &str, generation: u64) -> ControlEnvelope {
        encode_runtime_unit_exposure(&RuntimeUnitExposure {
            route_key: facade_control_stream_route(),
            unit_id: unit_id.to_string(),
            generation,
            confirmed_at_us: 1,
        })
        .expect("encode trusted exposure confirmed envelope")
    }

    fn granted_mount_root(
        object_ref: &str,
        mount_point: &std::path::Path,
    ) -> source::config::GrantedMountRoot {
        source::config::GrantedMountRoot {
            object_ref: object_ref.to_string(),
            host_ref: "single-app-node".to_string(),
            host_ip: "127.0.0.1".to_string(),
            host_name: Some("single-app-node".to_string()),
            site: None,
            zone: None,
            host_labels: Default::default(),
            mount_point: mount_point.to_path_buf(),
            fs_source: mount_point.display().to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
            active: true,
        }
    }

    fn worker_root(id: &str, path: &Path) -> source::config::RootSpec {
        let mut root = source::config::RootSpec::new(id, path);
        root.watch = false;
        root.scan = true;
        root
    }

    fn worker_fs_source_root(id: &str, fs_source: &str) -> source::config::RootSpec {
        source::config::RootSpec {
            id: id.to_string(),
            selector: source::config::RootSelector {
                fs_source: Some(fs_source.to_string()),
                ..Default::default()
            },
            subpath_scope: PathBuf::from("/"),
            watch: false,
            scan: true,
            audit_interval_ms: None,
        }
    }

    fn worker_export(
        object_ref: &str,
        host_ref: &str,
        host_ip: &str,
        mount_point: PathBuf,
    ) -> source::config::GrantedMountRoot {
        worker_export_with_fs_source(object_ref, host_ref, host_ip, object_ref, mount_point)
    }

    fn worker_export_with_fs_source(
        object_ref: &str,
        host_ref: &str,
        host_ip: &str,
        fs_source: &str,
        mount_point: PathBuf,
    ) -> source::config::GrantedMountRoot {
        source::config::GrantedMountRoot {
            object_ref: object_ref.to_string(),
            host_ref: host_ref.to_string(),
            host_ip: host_ip.to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: Default::default(),
            mount_point,
            fs_source: fs_source.to_string(),
            fs_type: "nfs".to_string(),
            mount_options: vec![],
            interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
            active: true,
        }
    }

    fn selected_group_file_request(path: &[u8], group_id: &str) -> InternalQueryRequest {
        InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: path.to_vec(),
                recursive: false,
                max_depth: Some(0),
                selected_group: Some(group_id.to_string()),
            },
            Some(query::TreeQueryOptions::default()),
        )
    }

    fn selected_group_dir_request(path: &[u8], group_id: &str) -> InternalQueryRequest {
        InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: path.to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some(group_id.to_string()),
            },
            Some(query::TreeQueryOptions::default()),
        )
    }

    fn decode_tree_group_payloads(
        events: Vec<Event>,
    ) -> Result<std::collections::BTreeMap<String, TreeGroupPayload>> {
        let mut grouped = std::collections::BTreeMap::new();
        for event in events {
            let payload = rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
                .map_err(|err| {
                    CnxError::Internal(format!(
                        "decode selected-group tree payload failed: {err}"
                    ))
                })?;
            let MaterializedQueryPayload::Tree(payload) = payload else {
                return Err(CnxError::Internal(
                    "unexpected stats payload for selected-group tree query".into(),
                ));
            };
            grouped.insert(event.metadata().origin_id.0.clone(), payload);
        }
        Ok(grouped)
    }

    async fn selected_group_proxy_tree(
        boundary: Arc<dyn ChannelIoSubset>,
        caller_node: NodeId,
        request: &InternalQueryRequest,
    ) -> Result<std::collections::BTreeMap<String, TreeGroupPayload>> {
        let adapter = crate::runtime::seam::exchange_host_adapter(
            boundary,
            caller_node,
            crate::runtime::routes::default_route_bindings(),
        );
        let payload = rmp_serde::to_vec(request).map_err(|err| {
            CnxError::Internal(format!("encode selected-group request failed: {err}"))
        })?;
        let events = capanix_host_adapter_fs::HostAdapter::call_collect(
            &adapter,
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SINK_QUERY_PROXY,
            Bytes::from(payload),
            Duration::from_secs(5),
            Duration::from_millis(250),
        )
        .await?;
        decode_tree_group_payloads(events)
    }

    fn reserve_bind_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind temp listener");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        addr.to_string()
    }

    #[derive(Default)]
    struct LoopbackWorkerBoundary {
        channels: AsyncMutex<HashMap<String, Vec<Event>>>,
        closed: StdMutex<std::collections::HashSet<String>>,
        changed: Notify,
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

    fn runtime_test_worker_module_path() -> PathBuf {
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
            let root = Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .and_then(Path::parent)
                .expect("workspace root")
                .to_path_buf();
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

    fn external_runtime_worker_binding(role_id: &str, socket_dir: &Path) -> RuntimeWorkerBinding {
        RuntimeWorkerBinding {
            role_id: role_id.to_string(),
            mode: WorkerMode::External,
            launcher_kind: RuntimeWorkerLauncherKind::WorkerHost,
            module_path: Some(runtime_test_worker_module_path()),
            socket_dir: Some(socket_dir.to_path_buf()),
        }
    }

    fn facade_pending_status(app: &FSMetaApp) -> SharedFacadePendingStatus {
        app.facade_pending_status
            .read()
            .expect("read facade pending status")
            .clone()
            .expect("facade pending status present")
    }

    fn mk_source_event(origin: &str, path: &[u8], file_name: &[u8], ts: u64) -> Event {
        let record = FileMetaRecord::scan_update(
            path.to_vec(),
            file_name.to_vec(),
            UnixStat {
                is_dir: false,
                size: 1,
                mtime_us: ts,
                ctime_us: ts,
                dev: None,
                ino: None,
            },
            b"/".to_vec(),
            ts,
            false,
        );
        let payload = rmp_serde::to_vec_named(&record).expect("encode record");
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

    fn selector_mount_point(path: &str) -> ConfigValue {
        ConfigValue::Map(std::collections::HashMap::from([(
            "mount_point".to_string(),
            ConfigValue::String(path.to_string()),
        )]))
    }

    fn root_entry(path: &str) -> ConfigValue {
        ConfigValue::Map(std::collections::HashMap::from([(
            "selector".to_string(),
            selector_mount_point(path),
        )]))
    }

    fn root_entry_with_id(id: &str, path: &str) -> ConfigValue {
        ConfigValue::Map(std::collections::HashMap::from([
            ("id".to_string(), ConfigValue::String(id.to_string())),
            ("selector".to_string(), selector_mount_point(path)),
        ]))
    }

    fn minimal_api_config() -> ConfigValue {
        ConfigValue::Map(std::collections::HashMap::from([(
            "auth".to_string(),
            ConfigValue::Map(std::collections::HashMap::new()),
        )]))
    }

    #[tokio::test]
    async fn start_accepts_missing_roots_config_as_valid_deployed_state() {
        let _app = FSMetaApp::new(
            FSMetaConfig {
                source: local_source_config(),
                ..FSMetaConfig::default()
            },
            NodeId("single-app-node".into()),
        )
        .expect("empty roots should be accepted as valid deployed state");
    }

    #[test]
    fn selected_group_empty_materialized_reply_emits_empty_tree_payload() {
        let request = query::InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: b"/retired".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some("nfs4".to_string()),
            },
            Some(query::TreeQueryOptions::default()),
        );

        let event = selected_group_empty_materialized_reply(&request, Some(42))
            .expect("build empty reply")
            .expect("selected-group reply");
        assert_eq!(event.metadata().origin_id.0, "nfs4");
        assert_eq!(event.metadata().correlation_id, Some(42));

        let payload =
            rmp_serde::from_slice::<query::MaterializedQueryPayload>(event.payload_bytes())
                .expect("decode empty tree payload");
        let query::MaterializedQueryPayload::Tree(tree) = payload else {
            panic!("expected tree payload");
        };
        assert!(!tree.reliability.reliable);
        assert_eq!(
            tree.reliability.unreliable_reason,
            Some(crate::shared_types::query::UnreliableReason::Unattested)
        );
        assert_eq!(tree.root.path, b"/retired".to_vec());
        assert!(!tree.root.exists);
        assert!(tree.entries.is_empty());
    }

    #[test]
    fn selected_group_empty_materialized_reply_emits_empty_stats_payload() {
        let request = query::InternalQueryRequest::materialized(
            query::QueryOp::Stats,
            query::QueryScope {
                path: b"/retired".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some("nfs4".to_string()),
            },
            None,
        );

        let event = selected_group_empty_materialized_reply(&request, Some(7))
            .expect("build empty reply")
            .expect("selected-group reply");
        assert_eq!(event.metadata().origin_id.0, "nfs4");
        assert_eq!(event.metadata().correlation_id, Some(7));

        let payload =
            rmp_serde::from_slice::<query::MaterializedQueryPayload>(event.payload_bytes())
                .expect("decode empty stats payload");
        let query::MaterializedQueryPayload::Stats(stats) = payload else {
            panic!("expected stats payload");
        };
        assert_eq!(stats.total_nodes, 0);
        assert_eq!(stats.total_files, 0);
        assert_eq!(stats.total_dirs, 0);
        assert_eq!(stats.total_size, 0);
    }

    #[test]
    fn selected_group_empty_materialized_reply_emits_on_non_owner_peers_when_group_known() {
        let request = query::InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: b"/force-find-stress".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some("nfs2".to_string()),
            },
            Some(query::TreeQueryOptions::default()),
        );
        let source_primary_by_group =
            BTreeMap::from([("nfs2".to_string(), "node-a::nfs2".to_string())]);

        assert!(should_emit_selected_group_empty_materialized_reply(
            &NodeId("node-a".into()),
            &source_primary_by_group,
            &request,
        ));
        assert!(should_emit_selected_group_empty_materialized_reply(
            &NodeId("node-c".into()),
            &source_primary_by_group,
            &request,
        ));
    }

    #[test]
    fn selected_group_empty_materialized_reply_skips_missing_or_unselected_groups() {
        let selected = query::InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: b"/force-find-stress".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some("nfs2".to_string()),
            },
            Some(query::TreeQueryOptions::default()),
        );
        let unselected = query::InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: b"/force-find-stress".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: None,
            },
            Some(query::TreeQueryOptions::default()),
        );

        assert!(!should_emit_selected_group_empty_materialized_reply(
            &NodeId("node-a".into()),
            &BTreeMap::new(),
            &selected,
        ));
        assert!(!should_emit_selected_group_empty_materialized_reply(
            &NodeId("node-a".into()),
            &BTreeMap::from([("nfs2".to_string(), "node-a::nfs2".to_string())]),
            &unselected,
        ));
    }

    #[test]
    fn selected_group_tree_missing_local_payload_requires_sink_query_bridge() {
        let request = query::InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: b"/force-find-stress".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some("nfs2".to_string()),
            },
            Some(query::TreeQueryOptions::default()),
        );
        assert!(should_bridge_selected_group_sink_query(
            &request,
            &[],
            true,
        ));
    }

    #[test]
    fn selected_group_tree_nonempty_local_payload_skips_sink_query_bridge() {
        let request = query::InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: b"/force-find-stress".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some("nfs2".to_string()),
            },
            Some(query::TreeQueryOptions::default()),
        );
        let payload = rmp_serde::to_vec_named(&query::MaterializedQueryPayload::Tree(
            query::TreeGroupPayload {
                reliability: query::GroupReliability {
                    reliable: true,
                    unreliable_reason: None,
                },
                stability: query::TreeStability::not_evaluated(),
                root: query::TreePageRoot {
                    path: b"/force-find-stress".to_vec(),
                    size: 0,
                    modified_time_us: 1,
                    is_dir: true,
                    exists: true,
                    has_children: true,
                },
                entries: Vec::new(),
            },
        ))
        .expect("encode non-empty tree payload");
        let local_event = Event::new(
            EventMetadata {
                origin_id: NodeId("nfs2".to_string()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: Some(11),
                ingress_auth: None,
                trace: None,
            },
            Bytes::from(payload),
        );

        assert!(!should_bridge_selected_group_sink_query(
            &request,
            &[local_event],
            true,
        ));
    }

    #[test]
    fn selected_group_tree_empty_payload_skips_sink_query_bridge_when_no_local_sink_groups() {
        let request = query::InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: b"/force-find-stress".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some("nfs2".to_string()),
            },
            Some(query::TreeQueryOptions::default()),
        );
        let empty_event = selected_group_empty_materialized_reply(&request, Some(11))
            .expect("build empty reply")
            .expect("selected-group reply");

        assert!(!should_bridge_selected_group_sink_query(
            &request,
            &[empty_event],
            false,
        ));
    }

    #[test]
    fn selected_group_tree_empty_payload_skips_sink_query_bridge_when_selected_group_missing_locally()
    {
        let request = query::InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: b"/force-find-stress".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some("nfs2".to_string()),
            },
            Some(query::TreeQueryOptions::default()),
        );
        let empty_event = selected_group_empty_materialized_reply(&request, Some(11))
            .expect("build empty reply")
            .expect("selected-group reply");

        assert!(!should_bridge_selected_group_sink_query(
            &request,
            &[empty_event],
            false,
        ));
    }

    #[test]
    fn selected_group_tree_empty_local_payload_skips_bridge_when_selected_group_payload_present() {
        let request = query::InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: b"/force-find-stress".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some("nfs2".to_string()),
            },
            Some(query::TreeQueryOptions::default()),
        );
        let empty_event = selected_group_empty_materialized_reply(&request, Some(11))
            .expect("build empty reply")
            .expect("selected-group reply");

        assert!(!should_bridge_selected_group_sink_query(
            &request,
            &[empty_event],
            true,
        ));
    }

    #[test]
    fn selected_group_bridge_eligibility_skips_group_with_zero_live_nodes() {
        let request = query::InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: b"/force-find-stress".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some("nfs2".to_string()),
            },
            Some(query::TreeQueryOptions::default()),
        );
        let snapshot = SinkStatusSnapshot {
            groups: vec![SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "root-b".to_string(),
                total_nodes: 0,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_audit: false,
                initial_audit_completed: false,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            }],
            ..SinkStatusSnapshot::default()
        };

        assert!(!selected_group_bridge_eligible_from_sink_status(
            &request, &snapshot
        ));
    }

    #[test]
    fn selected_group_bridge_eligibility_requires_live_nodes() {
        let request = query::InternalQueryRequest::materialized(
            query::QueryOp::Tree,
            query::QueryScope {
                path: b"/force-find-stress".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some("nfs2".to_string()),
            },
            Some(query::TreeQueryOptions::default()),
        );
        let snapshot = SinkStatusSnapshot {
            groups: vec![SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "root-b".to_string(),
                total_nodes: 1,
                live_nodes: 1,
                tombstoned_count: 0,
                attested_count: 1,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 1,
                shadow_lag_us: 0,
                overflow_pending_audit: false,
                initial_audit_completed: true,
                materialized_revision: 2,
                estimated_heap_bytes: 64,
            }],
            ..SinkStatusSnapshot::default()
        };

        assert!(selected_group_bridge_eligible_from_sink_status(
            &request, &snapshot
        ));
    }

    #[tokio::test]
    async fn request_and_stream_fail_closed_until_control_initializes_app() {
        let app = FSMetaApp::new(
            FSMetaConfig {
                source: local_source_config(),
                ..FSMetaConfig::default()
            },
            NodeId("single-app-node".into()),
        )
        .expect("init app");

        let send_err = app.send(&[]).await.expect_err("send must fail closed");
        assert!(matches!(send_err, CnxError::NotReady(_)));

        let recv_err = app
            .recv(RecvOpts::default())
            .await
            .expect_err("recv must fail closed");
        assert!(matches!(recv_err, CnxError::NotReady(_)));
    }

    #[tokio::test]
    async fn control_frames_initialize_app_once() {
        let tmp = tempdir().expect("create temp dir");
        let mount = tmp.path().join("root-a");
        fs::create_dir_all(&mount).expect("create mount");
        let app = FSMetaApp::new(
            FSMetaConfig {
                source: SourceConfig {
                    roots: vec![source::config::RootSpec::new("root-a", &mount)],
                    ..local_source_config()
                },
                ..FSMetaConfig::default()
            },
            NodeId("single-app-node".into()),
        )
        .expect("init app");

        assert!(!app.control_initialized());
        app.on_control_frame(&[activate_envelope("runtime.exec.source")])
            .await
            .expect("first control should initialize app");
        assert!(app.control_initialized());
        app.on_control_frame(&[tick_envelope("runtime.exec.source", 1)])
            .await
            .expect("second control should remain idempotent");
        assert!(app.control_initialized());
    }

    #[tokio::test]
    async fn passthrough_control_does_not_initialize_app() {
        let app = FSMetaApp::new(
            FSMetaConfig {
                source: local_source_config(),
                ..FSMetaConfig::default()
            },
            NodeId("single-app-node".into()),
        )
        .expect("init app");

        let err = app
            .on_control_frame(&[ControlEnvelope::Frame(
                capanix_app_sdk::runtime::ControlFrame {
                    kind: "noop".into(),
                    payload: Vec::new(),
                },
            )])
            .await
            .expect_err("passthrough control must not initialize app");
        assert!(matches!(err, CnxError::NotReady(_)));
        assert!(!app.control_initialized());
    }

    #[tokio::test]
    async fn grants_changed_control_does_not_initialize_app() {
        let tmp = tempdir().expect("create temp dir");
        let mount = tmp.path().join("root-a");
        fs::create_dir_all(&mount).expect("create mount");
        let app = FSMetaApp::new(
            FSMetaConfig {
                source: SourceConfig {
                    roots: vec![source::config::RootSpec::new("root-a", &mount)],
                    ..local_source_config()
                },
                ..FSMetaConfig::default()
            },
            NodeId("single-app-node".into()),
        )
        .expect("init app");

        let err = app
            .on_control_frame(&[host_object_grants_changed_envelope(
                1,
                mount.to_str().expect("mount path"),
            )])
            .await
            .expect_err("grant-change control must not initialize app");
        assert!(matches!(err, CnxError::NotReady(_)));
        assert!(!app.control_initialized());
    }

    #[tokio::test]
    async fn manual_rescan_control_does_not_initialize_app() {
        let app = FSMetaApp::new(
            FSMetaConfig {
                source: local_source_config(),
                ..FSMetaConfig::default()
            },
            NodeId("single-app-node".into()),
        )
        .expect("init app");

        let err = app
            .on_control_frame(&[
                crate::runtime::orchestration::encode_manual_rescan_envelope(now_us())
                    .expect("encode manual rescan envelope"),
            ])
            .await
            .expect_err("manual rescan control must not initialize app");
        assert!(matches!(err, CnxError::NotReady(_)));
        assert!(!app.control_initialized());
    }

    #[tokio::test]
    async fn start_rejects_removed_unit_authority_state_config() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/tmp")]),
            ),
            ("api".to_string(), minimal_api_config()),
            (
                "unit_authority_state_dir".to_string(),
                ConfigValue::String("/var/lib/capanix/fs-meta/statecell".to_string()),
            ),
        ]);
        let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
            .expect_err("removed unit authority fields must fail-closed");
        assert!(
            err.to_string()
                .contains("unit_authority_state_carrier/unit_authority_state_dir are removed")
        );
    }

    #[tokio::test]
    async fn on_control_frame_routes_source_sink_and_facade_units() {
        let tmp = tempdir().expect("create temp dir");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                ..local_source_config()
            },
            api: api::ApiConfig {
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
                ..api::ApiConfig::default()
            },
        };
        let app = FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app");
        match app
            .on_control_frame(&[
                activate_envelope("runtime.exec.source"),
                activate_envelope("runtime.exec.sink"),
                activate_envelope_with_scopes(
                    "runtime.exec.facade",
                    "single-app-listener",
                    &["single-app-listener"],
                ),
            ])
            .await
        {
            Ok(()) => {}
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => {
                panic!("source/sink/facade unit controls must route without cross-rejection: {err}")
            }
        }
        let pending = app.pending_facade.lock().await.is_some();
        let active = app.api_task.lock().await.is_some();
        assert!(
            pending || active,
            "facade activation should either remain pending or spawn immediately"
        );
    }

    #[tokio::test]
    async fn concurrent_on_control_frame_waits_for_serial_gate() {
        let tmp = tempdir().expect("create temp dir");
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                ..local_source_config()
            },
            api: api::ApiConfig::default(),
        };
        let app =
            Arc::new(FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app"));
        let serial_guard = app.control_frame_serial.lock().await;
        let task = tokio::spawn({
            let app = app.clone();
            async move {
                app.on_control_frame(&[activate_envelope("runtime.exec.source")])
                    .await
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !task.is_finished(),
            "concurrent control batch must wait for serialized control processing"
        );

        drop(serial_guard);
        task.await
            .expect("join serialized control task")
            .expect("run serialized control frame");
        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn standalone_facade_activation_uses_local_runtime_scope_fallback() {
        let tmp = tempdir().expect("create temp dir");
        let root_a = tmp.path().join("root-a");
        fs::create_dir_all(&root_a).expect("create root-a");
        fs::write(root_a.join("ready.txt"), "ready\n").expect("seed root-a");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("root-a", &root_a)],
                host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root_a)],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "listener-a".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "listener-a".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app");
        app.start().await.expect("start app");
        match app
            .on_control_frame(&[activate_envelope_with_scopes(
                "runtime.exec.facade",
                "listener-a",
                &["listener-a"],
            )])
            .await
        {
            Ok(()) => {}
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => {
                panic!("standalone facade activation should use local scope fallback: {err}")
            }
        }
        assert!(
            app.api_task.lock().await.is_some(),
            "standalone facade activation should spawn the API once local materialization is ready"
        );
        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn start_embedded_runtime_boundary_accepts_cold_start_before_initial_scan_materialization()
     {
        if cfg!(not(target_os = "linux")) {
            return;
        }

        let tmp = tempdir().expect("create temp dir");
        let root_a = tmp.path().join("root-a");
        fs::create_dir_all(&root_a).expect("create root-a");
        fs::write(root_a.join("ready.txt"), "ready\n").expect("seed root-a");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("root-a", &root_a)],
                host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root_a)],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");
        tokio::time::timeout(Duration::from_secs(1), app.start())
            .await
            .expect("embedded runtime cold start should not wait for initial materialization")
            .expect("start app");
        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn facade_cutover_replaces_previous_after_runtime_confirmation_even_before_tree_ready() {
        let tmp = tempdir().expect("create temp dir");
        let root = tmp.path().join("root-a");
        fs::create_dir_all(&root).expect("create root");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", &root)],
                host_object_grants: vec![granted_mount_root("single-app-node::root-1", &root)],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");
        let existing = match api::spawn(
            app.config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
            app.node_id.clone(),
            app.runtime_boundary.clone(),
            app.source.clone(),
            app.sink.clone(),
            app.query_sink.clone(),
            app.runtime_boundary.clone(),
            app.facade_pending_status.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *app.api_task.lock().await = Some(FacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 1,
            resource_ids: vec!["single-app-listener".to_string()],
            handle: existing,
        });
        let readiness_pending = PendingFacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 2,
            resource_ids: vec!["single-app-listener".to_string()],
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: "single-app-listener".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
            group_ids: Vec::new(),
            runtime_managed: true,
            runtime_exposure_confirmed: true,
            resolved: app
                .config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
        };
        assert!(
            !FSMetaApp::observation_eligible_for(
                app.source.as_ref(),
                app.sink.as_ref(),
                &readiness_pending,
            )
            .await
            .expect("tree readiness still gated on initial audit"),
            "replacement facade should not imply materialized tree readiness"
        );
        *app.pending_facade.lock().await = Some(readiness_pending);
        assert!(
            app.try_spawn_pending_facade()
                .await
                .expect("runtime-confirmed replacement facade spawn"),
            "runtime confirmation should be enough to replace the facade even before tree readiness"
        );
        assert_eq!(
            app.api_task
                .lock()
                .await
                .as_ref()
                .expect("replacement facade active")
                .generation,
            2
        );
        assert!(app.pending_facade.lock().await.is_none());
        app.close().await.expect("close fs-meta app");
    }

    #[tokio::test]
    async fn facade_activate_same_listener_resource_advances_generation_without_pending_cutover() {
        let tmp = tempdir().expect("create temp dir");
        let root = tmp.path().join("root-a");
        fs::create_dir_all(&root).expect("create root");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", &root)],
                host_object_grants: vec![granted_mount_root("single-app-node::root-1", &root)],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");
        let existing = match api::spawn(
            app.config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
            app.node_id.clone(),
            app.runtime_boundary.clone(),
            app.source.clone(),
            app.sink.clone(),
            app.query_sink.clone(),
            app.runtime_boundary.clone(),
            app.facade_pending_status.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *app.api_task.lock().await = Some(FacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 1,
            resource_ids: vec!["single-app-listener".to_string()],
            handle: existing,
        });

        app.apply_facade_activate(
            FacadeRuntimeUnit::Facade,
            &facade_control_stream_route(),
            2,
            &[RuntimeBoundScope {
                scope_id: "single-app-listener".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
        )
        .await
        .expect("apply same-resource facade activate");

        assert_eq!(
            app.api_task
                .lock()
                .await
                .as_ref()
                .expect("same-resource facade remains active")
                .generation,
            2
        );
        assert!(
            app.pending_facade.lock().await.is_none(),
            "same-resource generation refresh must not leave a pending cutover"
        );
        assert!(
            app.facade_pending_status
                .read()
                .expect("read pending status")
                .is_none(),
            "same-resource generation refresh must clear pending diagnostics"
        );
        app.close().await.expect("close fs-meta app");
    }

    #[tokio::test]
    async fn facade_spawn_singleflight_adopts_latest_same_resource_generation() {
        let tmp = tempdir().expect("create temp dir");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                host_object_grants: vec![granted_mount_root("single-app-node::root-1", tmp.path())],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");
        let pending_generation_1 = PendingFacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 1,
            resource_ids: vec!["single-app-listener".to_string()],
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: "single-app-listener".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
            group_ids: Vec::new(),
            runtime_managed: true,
            runtime_exposure_confirmed: true,
            resolved: app
                .config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
        };
        *app.pending_facade.lock().await = Some(pending_generation_1.clone());

        let spawn_started = Arc::new(Notify::new());
        let release_spawn = Arc::new(Notify::new());
        let spawn_calls = Arc::new(AtomicUsize::new(0));
        let first_attempt = tokio::spawn({
            let api_task = app.api_task.clone();
            let pending_facade = app.pending_facade.clone();
            let facade_spawn_in_progress = app.facade_spawn_in_progress.clone();
            let facade_pending_status = app.facade_pending_status.clone();
            let node_id = app.node_id.clone();
            let runtime_boundary = app.runtime_boundary.clone();
            let source = app.source.clone();
            let sink = app.sink.clone();
            let query_sink = app.query_sink.clone();
            let query_runtime_boundary = app.runtime_boundary.clone();
            let spawn_started = spawn_started.clone();
            let release_spawn = release_spawn.clone();
            let spawn_calls = spawn_calls.clone();
            async move {
                FSMetaApp::try_spawn_pending_facade_from_parts_with_spawn(
                    app.instance_id,
                    api_task,
                    pending_facade,
                    facade_spawn_in_progress,
                    facade_pending_status,
                    node_id,
                    runtime_boundary,
                    source,
                    sink,
                    query_sink,
                    query_runtime_boundary,
                    move |resolved,
                          node_id,
                          runtime_boundary,
                          source,
                          sink,
                          query_sink,
                          query_runtime_boundary,
                          facade_pending_status| {
                        let spawn_started = spawn_started.clone();
                        let release_spawn = release_spawn.clone();
                        let spawn_calls = spawn_calls.clone();
                        async move {
                            spawn_calls.fetch_add(1, AtomicOrdering::SeqCst);
                            spawn_started.notify_one();
                            release_spawn.notified().await;
                            api::spawn(
                                resolved,
                                node_id,
                                runtime_boundary,
                                source,
                                sink,
                                query_sink,
                                query_runtime_boundary,
                                facade_pending_status,
                            )
                            .await
                        }
                    },
                )
                .await
            }
        });

        spawn_started.notified().await;
        *app.pending_facade.lock().await = Some(PendingFacadeActivation {
            generation: 2,
            ..pending_generation_1.clone()
        });

        let second_attempt = FSMetaApp::try_spawn_pending_facade_from_parts_with_spawn(
            app.instance_id,
            app.api_task.clone(),
            app.pending_facade.clone(),
            app.facade_spawn_in_progress.clone(),
            app.facade_pending_status.clone(),
            app.node_id.clone(),
            app.runtime_boundary.clone(),
            app.source.clone(),
            app.sink.clone(),
            app.query_sink.clone(),
            app.runtime_boundary.clone(),
            move |_, _, _, _, _, _, _, _| async move {
                panic!("duplicate facade spawn must be suppressed while same-resource spawn is in progress");
                #[allow(unreachable_code)]
                Err(CnxError::Internal("unreachable".into()))
            },
        )
        .await
        .expect("second same-resource facade activate should not fail");
        assert!(
            !second_attempt,
            "same-resource spawn in progress should suppress duplicate facade spawn"
        );

        release_spawn.notify_one();

        match first_attempt.await.expect("join first facade spawn") {
            Ok(true) => {}
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                app.close().await.expect("close app after bind restriction");
                return;
            }
            Ok(false) => panic!("first facade spawn unexpectedly reported no activation"),
            Err(err) => panic!("first facade spawn failed: {err}"),
        }

        assert_eq!(
            spawn_calls.load(AtomicOrdering::SeqCst),
            1,
            "same-resource facade activation must remain single-flight"
        );
        assert_eq!(
            app.api_task
                .lock()
                .await
                .as_ref()
                .expect("active facade after single-flight spawn")
                .generation,
            2,
            "returned facade handle should adopt the latest same-resource generation"
        );
        assert!(app.pending_facade.lock().await.is_none());
        assert!(app.facade_spawn_in_progress.lock().await.is_none());
        app.close().await.expect("close fs-meta app");
    }

    #[tokio::test]
    async fn facade_spawn_dedupes_across_distinct_app_instances_with_same_fixed_bind() {
        clear_process_facade_claim_for_tests();
        let tmp = tempdir().expect("create temp dir");
        let bind_addr = reserve_bind_addr();
        let listener_resource = api::config::ApiListenerResource {
            resource_id: "shared-app-listener".to_string(),
            bind_addr: bind_addr.clone(),
        };
        let (passwd_path_1, shadow_path_1) = write_auth_files(&tmp);
        let cfg_1 = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root-1", tmp.path())],
                host_object_grants: vec![granted_mount_root("node-a::root-1", tmp.path())],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: listener_resource.resource_id.clone(),
                local_listener_resources: vec![listener_resource.clone()],
                auth: api::ApiAuthConfig {
                    passwd_path: passwd_path_1,
                    shadow_path: shadow_path_1,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app_1 = FSMetaApp::with_boundaries(
            cfg_1,
            NodeId("fixed-bind-node-a".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init first app");
        *app_1.pending_facade.lock().await = Some(PendingFacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 1,
            resource_ids: vec![listener_resource.resource_id.clone()],
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: listener_resource.resource_id.clone(),
                resource_ids: vec![listener_resource.resource_id.clone()],
            }],
            group_ids: Vec::new(),
            runtime_managed: true,
            runtime_exposure_confirmed: true,
            resolved: app_1
                .config
                .api
                .resolve_for_candidate_ids(std::slice::from_ref(&listener_resource.resource_id))
                .expect("resolve first facade config"),
        });
        assert!(
            app_1
                .try_spawn_pending_facade()
                .await
                .expect("spawn first facade"),
            "first app instance should claim and activate the fixed facade"
        );

        let (passwd_path_2, shadow_path_2) = write_auth_files(&tmp);
        let cfg_2 = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root-2", tmp.path())],
                host_object_grants: vec![granted_mount_root("node-b::root-1", tmp.path())],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: listener_resource.resource_id.clone(),
                local_listener_resources: vec![listener_resource.clone()],
                auth: api::ApiAuthConfig {
                    passwd_path: passwd_path_2,
                    shadow_path: shadow_path_2,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app_2 = FSMetaApp::with_boundaries(
            cfg_2,
            NodeId("fixed-bind-node-b".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init second app");
        *app_2.pending_facade.lock().await = Some(PendingFacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 2,
            resource_ids: vec![listener_resource.resource_id.clone()],
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: listener_resource.resource_id.clone(),
                resource_ids: vec![listener_resource.resource_id.clone()],
            }],
            group_ids: Vec::new(),
            runtime_managed: true,
            runtime_exposure_confirmed: true,
            resolved: app_2
                .config
                .api
                .resolve_for_candidate_ids(std::slice::from_ref(&listener_resource.resource_id))
                .expect("resolve second facade config"),
        });
        let second_spawn = app_2
            .try_spawn_pending_facade()
            .await
            .expect("second app facade spawn should not error");
        assert!(
            !second_spawn,
            "distinct app instance must not start a duplicate fixed-bind facade"
        );
        assert!(
            app_2.api_task.lock().await.is_none(),
            "second app instance must not activate a duplicate facade handle"
        );

        let claim = {
            let guard = match process_facade_claim_cell().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard
                .get(&bind_addr)
                .cloned()
                .expect("fixed-bind facade claim should remain owned")
        };
        assert_eq!(claim.owner_instance_id, app_1.instance_id);
        assert_eq!(claim.bind_addr, bind_addr);
        assert_eq!(
            claim.resource_ids,
            vec![listener_resource.resource_id.clone()]
        );

        app_2.close().await.expect("close second app");
        app_1.close().await.expect("close first app");
        clear_process_facade_claim_for_tests();
    }

    #[tokio::test]
    async fn facade_retry_waits_for_worker_tick_instead_of_background_polling() {
        let tmp = tempdir().expect("create temp dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                host_object_grants: vec![granted_mount_root("single-app-node::root-1", tmp.path())],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "listener-a".to_string(),
                local_listener_resources: vec![
                    api::config::ApiListenerResource {
                        resource_id: "listener-a".to_string(),
                        bind_addr: bind_addr.clone(),
                    },
                    api::config::ApiListenerResource {
                        resource_id: "listener-b".to_string(),
                        bind_addr: bind_addr.clone(),
                    },
                ],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");
        let existing = match api::spawn(
            app.config
                .api
                .resolve_for_candidate_ids(&["listener-a".to_string()])
                .expect("resolve facade config"),
            app.node_id.clone(),
            app.runtime_boundary.clone(),
            app.source.clone(),
            app.sink.clone(),
            app.query_sink.clone(),
            app.runtime_boundary.clone(),
            app.facade_pending_status.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *app.api_task.lock().await = Some(FacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 1,
            resource_ids: vec!["listener-a".to_string()],
            handle: existing,
        });

        app.apply_facade_activate(
            FacadeRuntimeUnit::Facade,
            &facade_control_stream_route(),
            2,
            &[RuntimeBoundScope {
                scope_id: "test-root".to_string(),
                resource_ids: vec!["listener-b".to_string()],
            }],
        )
        .await
        .expect("apply facade activate");

        let waiting = facade_pending_status(&app);
        assert_eq!(waiting.reason, FacadePendingReason::AwaitingRuntimeExposure);
        assert_eq!(waiting.retry_attempts, 0);

        app.on_control_frame(&[trusted_exposure_confirmed_envelope(
            execution_units::FACADE_RUNTIME_UNIT_ID,
            2,
        )])
        .await
        .expect("handle exposure confirmed");

        let after_confirm = facade_pending_status(&app);
        assert_eq!(
            after_confirm.reason,
            FacadePendingReason::RetryingAfterError
        );
        assert_eq!(after_confirm.retry_attempts, 1);
        assert!(
            after_confirm
                .last_error
                .as_deref()
                .is_some_and(|msg| msg.contains("fs-meta api bind failed")),
            "expected bind failure in pending status: {:?}",
            after_confirm.last_error
        );

        tokio::time::sleep(Duration::from_millis(350)).await;
        let without_tick = facade_pending_status(&app);
        assert_eq!(
            without_tick.retry_attempts, 1,
            "pending retry attempts must stay idle until another runtime pulse arrives"
        );

        app.on_control_frame(&[tick_envelope(execution_units::FACADE_RUNTIME_UNIT_ID, 2)])
            .await
            .expect("handle worker tick");

        let after_tick = facade_pending_status(&app);
        assert_eq!(after_tick.reason, FacadePendingReason::RetryingAfterError);
        assert_eq!(after_tick.retry_attempts, 2);
        app.close().await.expect("close fs-meta app");
    }

    #[tokio::test]
    async fn status_reports_facade_pending_retry_diagnostics() {
        let tmp = tempdir().expect("create temp dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                host_object_grants: vec![granted_mount_root("single-app-node::root-1", tmp.path())],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "listener-a".to_string(),
                local_listener_resources: vec![
                    api::config::ApiListenerResource {
                        resource_id: "listener-a".to_string(),
                        bind_addr: bind_addr.clone(),
                    },
                    api::config::ApiListenerResource {
                        resource_id: "listener-b".to_string(),
                        bind_addr: bind_addr.clone(),
                    },
                ],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");
        let existing = match api::spawn(
            app.config
                .api
                .resolve_for_candidate_ids(&["listener-a".to_string()])
                .expect("resolve facade config"),
            app.node_id.clone(),
            app.runtime_boundary.clone(),
            app.source.clone(),
            app.sink.clone(),
            app.query_sink.clone(),
            app.runtime_boundary.clone(),
            app.facade_pending_status.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *app.api_task.lock().await = Some(FacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 1,
            resource_ids: vec!["listener-a".to_string()],
            handle: existing,
        });

        app.apply_facade_activate(
            FacadeRuntimeUnit::Facade,
            &facade_control_stream_route(),
            2,
            &[RuntimeBoundScope {
                scope_id: "test-root".to_string(),
                resource_ids: vec!["listener-b".to_string()],
            }],
        )
        .await
        .expect("apply facade activate");
        app.on_control_frame(&[trusted_exposure_confirmed_envelope(
            execution_units::FACADE_RUNTIME_UNIT_ID,
            2,
        )])
        .await
        .expect("handle exposure confirmed");

        let client = Client::new();
        let login = client
            .post(format!("http://{bind_addr}/api/fs-meta/v1/session/login"))
            .json(&json!({"username":"admin","password":"admin"}))
            .send()
            .await
            .expect("login request");
        assert!(
            login.status().is_success(),
            "login failed: {}",
            login.status()
        );
        let login_body: serde_json::Value = login.json().await.expect("decode login");
        let token = login_body["token"].as_str().expect("token");
        let status = client
            .get(format!("http://{bind_addr}/api/fs-meta/v1/status"))
            .bearer_auth(token)
            .send()
            .await
            .expect("status request");
        assert!(
            status.status().is_success(),
            "status failed: {}",
            status.status()
        );
        let status_body: serde_json::Value = status.json().await.expect("decode status");
        assert_eq!(
            status_body["facade"]["pending"]["reason"],
            serde_json::Value::String("retrying_after_error".to_string())
        );
        assert_eq!(status_body["facade"]["pending"]["generation"], 2);
        assert_eq!(
            status_body["facade"]["pending"]["runtime_exposure_confirmed"],
            serde_json::Value::Bool(true)
        );
        assert!(
            status_body["facade"]["pending"]["retry_attempts"]
                .as_u64()
                .is_some_and(|attempts| attempts >= 1)
        );
        assert!(
            status_body["facade"]["pending"]["last_error"]
                .as_str()
                .is_some_and(|msg| msg.contains("fs-meta api bind failed"))
        );
        app.close().await.expect("close fs-meta app");
    }

    #[test]
    fn facade_route_key_matches_dual_owned_internal_query_routes_for_both_units() {
        let sink_query_proxy = format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY);
        let sink_status = format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL);
        let source_status = format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
        let source_find = format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL);

        for route in [
            sink_query_proxy.as_str(),
            sink_status.as_str(),
            source_status.as_str(),
            source_find.as_str(),
        ] {
            assert!(facade_route_key_matches(FacadeRuntimeUnit::Query, route));
            assert!(facade_route_key_matches(
                FacadeRuntimeUnit::QueryPeer,
                route
            ));
        }
        assert!(!facade_route_key_matches(
            FacadeRuntimeUnit::QueryPeer,
            &format!("{}.req", ROUTE_KEY_QUERY)
        ));
    }

    #[tokio::test]
    async fn runtime_managed_listener_only_facade_requires_generation_scoped_group_evidence() {
        let tmp = tempdir().expect("create temp dir");
        let root_a = tmp.path().join("root-a");
        fs::create_dir_all(&root_a).expect("create root-a");
        fs::write(root_a.join("ready.txt"), "ready\n").expect("seed root-a");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("root-a", &root_a)],
                host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root_a)],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");

        let pending = PendingFacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 2,
            resource_ids: vec!["single-app-listener".to_string()],
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: "single-app-listener".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
            group_ids: Vec::new(),
            runtime_managed: true,
            runtime_exposure_confirmed: false,
            resolved: app
                .config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
        };
        assert!(
            !FSMetaApp::observation_eligible_for(app.source.as_ref(), app.sink.as_ref(), &pending)
                .await
                .expect("evaluate observation eligibility"),
            "runtime-managed facade gating must fail-closed when current generation group evidence is missing"
        );
        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn runtime_managed_cold_start_facade_starts_before_generation_scoped_group_evidence() {
        let tmp = tempdir().expect("create temp dir");
        let root_a = tmp.path().join("root-a");
        fs::create_dir_all(&root_a).expect("create root-a");
        fs::write(root_a.join("ready.txt"), "ready\n").expect("seed root-a");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("root-a", &root_a)],
                host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root_a)],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");

        let pending = PendingFacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 2,
            resource_ids: vec!["single-app-listener".to_string()],
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: "single-app-listener".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
            group_ids: Vec::new(),
            runtime_managed: true,
            runtime_exposure_confirmed: true,
            resolved: app
                .config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
        };
        assert!(
            !FSMetaApp::observation_eligible_for(app.source.as_ref(), app.sink.as_ref(), &pending)
                .await
                .expect("evaluate observation eligibility"),
            "cold start fixture intentionally lacks generation-scoped observation evidence"
        );
        *app.pending_facade.lock().await = Some(pending);

        assert!(
            app.try_spawn_pending_facade()
                .await
                .expect("cold start facade spawn"),
            "cold-start runtime-managed facade should still come up"
        );
        assert!(
            app.api_task.lock().await.is_some(),
            "cold-start facade activation must own an HTTP task"
        );
        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn observation_eligibility_ignores_unbound_sink_overflow_for_listener_only_facade() {
        let tmp = tempdir().expect("create temp dir");
        let root_a = tmp.path().join("root-a");
        let root_b = tmp.path().join("root-b");
        fs::create_dir_all(&root_a).expect("create root-a");
        fs::create_dir_all(&root_b).expect("create root-b");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![
                    source::config::RootSpec::new("root-a", &root_a),
                    source::config::RootSpec::new("root-b", &root_b),
                ],
                host_object_grants: vec![
                    granted_mount_root("single-app-node::root-a-1", &root_a),
                    granted_mount_root("single-app-node::root-b-1", &root_b),
                ],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app");

        app.on_control_frame(&[activate_envelope_with_generation_and_scopes(
            "runtime.exec.source",
            "root-a",
            &[],
            2,
        )])
        .await
        .expect("activate scoped source generation");

        let source_groups = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            loop {
                let groups = app
                    .source
                    .status_snapshot()
                    .await
                    .expect("source status")
                    .concrete_roots
                    .into_iter()
                    .filter(|root| root.active)
                    .map(|root| root.logical_root_id)
                    .collect::<std::collections::BTreeSet<_>>();
                if groups == std::collections::BTreeSet::from(["root-a".to_string()]) {
                    break groups;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out waiting for source runtime scope convergence: {groups:?}"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        };
        assert_eq!(
            source_groups,
            std::collections::BTreeSet::from(["root-a".to_string()])
        );

        app.sink
            .send(&[
                mk_source_event("single-app-node::root-a-1", b"/ready.txt", b"ready.txt", 10),
                mk_control_event(
                    "single-app-node::root-a-1",
                    ControlEvent::EpochEnd {
                        epoch_id: 0,
                        epoch_type: crate::EpochType::Audit,
                    },
                    11,
                ),
                mk_control_event("single-app-node::root-b-1", ControlEvent::WatchOverflow, 11),
            ])
            .await
            .expect("seed sink state");

        let sink_status = app.sink.status_snapshot().await.expect("sink status");
        assert!(
            sink_status
                .groups
                .iter()
                .any(|group| group.group_id == "root-b" && group.overflow_pending_audit),
            "unbound group should retain overflow evidence in sink status"
        );

        let pending = PendingFacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 2,
            resource_ids: vec!["single-app-listener".to_string()],
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: "root-a".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
            group_ids: Vec::new(),
            runtime_managed: false,
            runtime_exposure_confirmed: true,
            resolved: app
                .config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
        };
        assert!(
            FSMetaApp::observation_eligible_for(app.source.as_ref(), app.sink.as_ref(), &pending)
                .await
                .expect("evaluate observation eligibility"),
            "listener-only facade gating should use runtime-scoped source/sink groups instead of unrelated overflow on unbound groups"
        );
    }

    #[tokio::test]
    async fn on_control_frame_rejects_unknown_unit() {
        let tmp = tempdir().expect("create temp dir");
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                ..local_source_config()
            },
            api: api::ApiConfig::default(),
        };
        let app = FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app");
        let err = app
            .on_control_frame(&[activate_envelope("runtime.exec.unknown")])
            .await
            .expect_err("unknown unit must be rejected");
        assert!(matches!(
            err,
            CnxError::NotSupported(msg) if msg.contains("unsupported unit_id")
        ));
    }

    #[tokio::test]
    async fn on_control_frame_rejects_batch_atomically_on_unknown_unit() {
        let tmp = tempdir().expect("create temp dir");
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                ..local_source_config()
            },
            api: api::ApiConfig::default(),
        };
        let app = FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app");
        assert_eq!(
            app.source
                .host_object_grants_version_snapshot()
                .await
                .expect("snapshot version"),
            0
        );

        let err = app
            .on_control_frame(&[
                host_object_grants_changed_envelope(1, tmp.path().to_string_lossy().as_ref()),
                activate_envelope("runtime.exec.unknown"),
            ])
            .await
            .expect_err("batch with unknown unit must fail atomically");
        assert!(matches!(
            err,
            CnxError::NotSupported(msg) if msg.contains("unsupported unit_id")
        ));
        assert_eq!(
            app.source
                .host_object_grants_version_snapshot()
                .await
                .expect("snapshot version"),
            0,
            "batch failure must not apply partial shared control effects"
        );
    }

    #[tokio::test]
    async fn single_app_initial_scan_materializes_tree() {
        let tmp = tempdir().expect("create temp dir");
        fs::write(tmp.path().join("hello.txt"), b"world").expect("write seed file");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);

        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                host_object_grants: vec![source::config::GrantedMountRoot {
                    object_ref: "single-app-node::root-1".to_string(),
                    host_ref: "single-app-node".to_string(),
                    host_ip: "127.0.0.1".to_string(),
                    host_name: Some("single-app-node".to_string()),
                    site: None,
                    zone: None,
                    host_labels: Default::default(),
                    mount_point: tmp.path().to_path_buf(),
                    fs_source: tmp.path().display().to_string(),
                    fs_type: "nfs".to_string(),
                    mount_options: Vec::new(),
                    interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                    active: true,
                }],
                scan_workers: 1,
                batch_size: 128,
                max_scan_events: 4096,
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };

        let app = FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app");
        if cfg!(target_os = "linux") {
            match app.start().await {
                Ok(()) => {}
                Err(CnxError::InvalidInput(msg))
                    if msg.contains("fs-meta api bind failed: Operation not permitted") =>
                {
                    return;
                }
                Err(err) => panic!("start app: {err}"),
            }
        } else {
            let err = app.start().await.expect_err("non-linux should fail fast");
            assert!(matches!(err, CnxError::NotSupported(_)));
            return;
        }

        tokio::time::sleep(Duration::from_millis(300)).await;

        let query = app
            .query_tree(&query::InternalQueryRequest::materialized(
                query::QueryOp::Tree,
                query::QueryScope::default(),
                Some(query::TreeQueryOptions::default()),
            ))
            .await
            .expect("query tree");
        let paths: Vec<Vec<u8>> = query
            .values()
            .flat_map(|group| group.entries.iter().map(|entry| entry.path.clone()))
            .collect();
        assert!(paths.iter().any(|path| path == b"/hello.txt"));

        app.close().await.expect("close app");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_runtime_app_selected_group_proxy_materializes_each_local_primary_root() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
        fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
        fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_root = tempdir().expect("create worker socket dir");
        let source_socket_dir = worker_socket_root.path().join("source");
        let sink_socket_dir = worker_socket_root.path().join("sink");
        fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
        fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");

        let app = FSMetaApp::with_boundaries_and_state(
            FSMetaConfig {
                source: SourceConfig {
                    roots: vec![worker_root("nfs1", &nfs1), worker_root("nfs2", &nfs2)],
                    host_object_grants: vec![
                        worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                        worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
                    ],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".to_string()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init external-worker runtime app");

        app.on_control_frame(&[
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
        ])
        .await
        .expect("apply runtime control");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduling_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let source_groups = app
                .source
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            let scan_groups = app
                .source
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups")
                .unwrap_or_default();
            let sink_groups = app
                .sink
                .scheduled_group_ids()
                .await
                .expect("sink groups")
                .unwrap_or_default();
            if source_groups == expected_groups
                && scan_groups == expected_groups
                && sink_groups == expected_groups
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduling_deadline,
                "timed out waiting for runtime scope convergence: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        for group_id in ["nfs1", "nfs2"] {
            let request = selected_group_file_request(b"/force-find-stress/seed.txt", group_id);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            let mut direct_ready = false;
            let mut proxy_ready = false;
            let mut last_proxy_keys = Vec::<String>::new();
            let mut last_proxy_err = None::<String>;
            while tokio::time::Instant::now() < deadline {
                match app.query_tree(&request).await {
                    Ok(grouped) => {
                        if grouped.get(group_id).is_some_and(|payload| payload.root.exists) {
                            direct_ready = true;
                        }
                    }
                    Err(_) => {}
                }
                match selected_group_proxy_tree(
                    boundary.clone(),
                    NodeId("node-a".to_string()),
                    &request,
                )
                .await
                {
                    Ok(grouped) => {
                        last_proxy_keys = grouped.keys().cloned().collect();
                        last_proxy_err = None;
                        if grouped.get(group_id).is_some_and(|payload| payload.root.exists) {
                            proxy_ready = true;
                        }
                    }
                    Err(err) => {
                        last_proxy_err = Some(err.to_string());
                    }
                }
                if direct_ready && proxy_ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(
                direct_ready,
                "direct sink query should materialize selected group {group_id}"
            );
            assert!(
                proxy_ready,
                "selected-group proxy query should materialize selected group {group_id}: last_proxy_keys={last_proxy_keys:?} last_proxy_err={last_proxy_err:?}"
            );
        }

        app.close().await.expect("close app");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
    async fn external_runtime_app_selected_group_proxy_materializes_each_local_primary_root_real_nfs(
    ) {
        let preflight = real_nfs_lab::RealNfsPreflight::detect();
        if !preflight.enabled {
            eprintln!(
                "skip real-nfs runtime-app selected-group proxy test: {}",
                preflight
                    .reason
                    .unwrap_or_else(|| "real-nfs preflight failed".to_string())
            );
            return;
        }

        let mut lab = real_nfs_lab::NfsLab::start().expect("start NFS lab");
        lab.mkdir("nfs1", "force-find-stress")
            .expect("create nfs1 force-find dir");
        lab.mkdir("nfs2", "force-find-stress")
            .expect("create nfs2 force-find dir");
        lab.write_file("nfs1", "force-find-stress/seed.txt", "a\n")
            .expect("seed nfs1");
        lab.write_file("nfs2", "force-find-stress/seed.txt", "b\n")
            .expect("seed nfs2");
        let nfs1 = lab
            .mount_export("node-a", "nfs1")
            .expect("mount node-a nfs1 export");
        let nfs2 = lab
            .mount_export("node-a", "nfs2")
            .expect("mount node-a nfs2 export");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_root = tempdir().expect("create worker socket dir");
        let source_socket_dir = worker_socket_root.path().join("source");
        let sink_socket_dir = worker_socket_root.path().join("sink");
        fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
        fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");

        let app = FSMetaApp::with_boundaries_and_state(
            FSMetaConfig {
                source: SourceConfig {
                    roots: vec![worker_root("nfs1", &nfs1), worker_root("nfs2", &nfs2)],
                    host_object_grants: vec![
                        worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                        worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
                    ],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".to_string()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init external-worker runtime app");

        app.on_control_frame(&[
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
        ])
        .await
        .expect("apply runtime control");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduling_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let source_groups = app
                .source
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            let scan_groups = app
                .source
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups")
                .unwrap_or_default();
            let sink_groups = app
                .sink
                .scheduled_group_ids()
                .await
                .expect("sink groups")
                .unwrap_or_default();
            if source_groups == expected_groups
                && scan_groups == expected_groups
                && sink_groups == expected_groups
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduling_deadline,
                "timed out waiting for runtime scope convergence: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        for group_id in ["nfs1", "nfs2"] {
            let request = selected_group_dir_request(b"/force-find-stress", group_id);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            let mut direct_ready = false;
            let mut proxy_ready = false;
            let mut last_direct_exists = None::<bool>;
            let mut last_proxy_exists = None::<bool>;
            let mut last_proxy_keys = Vec::<String>::new();
            let mut last_proxy_err = None::<String>;
            while tokio::time::Instant::now() < deadline {
                match app.query_tree(&request).await {
                    Ok(grouped) => {
                        let exists = grouped.get(group_id).map(|payload| payload.root.exists);
                        last_direct_exists = exists;
                        if exists == Some(true) {
                            direct_ready = true;
                        }
                    }
                    Err(_) => {}
                }
                match selected_group_proxy_tree(
                    boundary.clone(),
                    NodeId("node-a".to_string()),
                    &request,
                )
                .await
                {
                    Ok(grouped) => {
                        last_proxy_keys = grouped.keys().cloned().collect();
                        last_proxy_exists = grouped.get(group_id).map(|payload| payload.root.exists);
                        last_proxy_err = None;
                        if last_proxy_exists == Some(true) {
                            proxy_ready = true;
                        }
                    }
                    Err(err) => {
                        last_proxy_err = Some(err.to_string());
                    }
                }
                if direct_ready && proxy_ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(
                direct_ready,
                "real-nfs direct sink query should materialize selected group {group_id}: last_direct_exists={last_direct_exists:?}"
            );
            assert!(
                proxy_ready,
                "real-nfs selected-group proxy query should materialize selected group {group_id}: last_proxy_exists={last_proxy_exists:?} last_proxy_keys={last_proxy_keys:?} last_proxy_err={last_proxy_err:?}"
            );
        }

        app.close().await.expect("close app");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
    async fn external_runtime_app_selected_group_proxy_real_nfs_remote_same_group_grant_keeps_owner_host_materialized(
    ) {
        let preflight = real_nfs_lab::RealNfsPreflight::detect();
        if !preflight.enabled {
            eprintln!(
                "skip real-nfs runtime-app distributed-grant test: {}",
                preflight
                    .reason
                    .unwrap_or_else(|| "real-nfs preflight failed".to_string())
            );
            return;
        }

        let mut lab = real_nfs_lab::NfsLab::start().expect("start NFS lab");
        lab.mkdir("nfs1", "force-find-stress")
            .expect("create nfs1 force-find dir");
        lab.mkdir("nfs2", "force-find-stress")
            .expect("create nfs2 force-find dir");
        lab.write_file("nfs1", "force-find-stress/seed.txt", "a\n")
            .expect("seed nfs1");
        lab.write_file("nfs2", "force-find-stress/seed.txt", "b\n")
            .expect("seed nfs2");

        let nfs1_source = lab.export_source("nfs1");
        let nfs2_source = lab.export_source("nfs2");
        let nfs1 = lab
            .mount_export("node-a", "nfs1")
            .expect("mount node-a nfs1 export");
        let nfs2_owner = lab
            .mount_export("node-a", "nfs2")
            .expect("mount node-a nfs2 export");
        let nfs2_remote = lab
            .mount_export("node-c", "nfs2")
            .expect("mount node-c nfs2 export");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_root = tempdir().expect("create worker socket dir");
        let source_socket_dir = worker_socket_root.path().join("source");
        let sink_socket_dir = worker_socket_root.path().join("sink");
        fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
        fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");

        let app = FSMetaApp::with_boundaries_and_state(
            FSMetaConfig {
                source: SourceConfig {
                    roots: vec![
                        worker_fs_source_root("nfs1", &nfs1_source),
                        worker_fs_source_root("nfs2", &nfs2_source),
                    ],
                    host_object_grants: vec![
                        worker_export_with_fs_source(
                            "node-a::nfs1",
                            "node-a",
                            "10.0.0.11",
                            &nfs1_source,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-a::nfs2",
                            "node-a",
                            "10.0.0.12",
                            &nfs2_source,
                            nfs2_owner.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-c::nfs2",
                            "node-c",
                            "10.0.0.31",
                            &nfs2_source,
                            nfs2_remote.clone(),
                        ),
                    ],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".to_string()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init external-worker runtime app");

        app.on_control_frame(&[
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
        ])
        .await
        .expect("apply runtime control");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduling_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let source_groups = app
                .source
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            let scan_groups = app
                .source
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups")
                .unwrap_or_default();
            let sink_groups = app
                .sink
                .scheduled_group_ids()
                .await
                .expect("sink groups")
                .unwrap_or_default();
            if source_groups == expected_groups
                && scan_groups == expected_groups
                && sink_groups == expected_groups
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduling_deadline,
                "timed out waiting for runtime scope convergence: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let primary_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let primary = app
                .source
                .source_primary_by_group_snapshot()
                .await
                .expect("source primary snapshot");
            let matched_grants = app
                .source
                .status_snapshot()
                .await
                .expect("source status snapshot")
                .logical_roots
                .into_iter()
                .find(|root| root.root_id == "nfs2")
                .map(|root| root.matched_grants);
            if primary.get("nfs2").map(String::as_str) == Some("node-a::nfs2")
                && matched_grants == Some(2)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < primary_deadline,
                "timed out waiting for distributed grant snapshot: primary={primary:?} matched_grants={matched_grants:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        for group_id in ["nfs1", "nfs2"] {
            let request = selected_group_dir_request(b"/force-find-stress", group_id);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            let mut direct_ready = false;
            let mut proxy_ready = false;
            let mut last_direct_exists = None::<bool>;
            let mut last_proxy_exists = None::<bool>;
            let mut last_proxy_keys = Vec::<String>::new();
            let mut last_proxy_err = None::<String>;
            while tokio::time::Instant::now() < deadline {
                match app.query_tree(&request).await {
                    Ok(grouped) => {
                        let exists = grouped.get(group_id).map(|payload| payload.root.exists);
                        last_direct_exists = exists;
                        if exists == Some(true) {
                            direct_ready = true;
                        }
                    }
                    Err(_) => {}
                }
                match selected_group_proxy_tree(
                    boundary.clone(),
                    NodeId("node-a".to_string()),
                    &request,
                )
                .await
                {
                    Ok(grouped) => {
                        last_proxy_keys = grouped.keys().cloned().collect();
                        last_proxy_exists = grouped.get(group_id).map(|payload| payload.root.exists);
                        last_proxy_err = None;
                        if last_proxy_exists == Some(true) {
                            proxy_ready = true;
                        }
                    }
                    Err(err) => {
                        last_proxy_err = Some(err.to_string());
                    }
                }
                if direct_ready && proxy_ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(
                direct_ready,
                "direct sink query should materialize selected group {group_id} with remote same-group grant: last_direct_exists={last_direct_exists:?}"
            );
            assert!(
                proxy_ready,
                "selected-group proxy query should materialize selected group {group_id} with remote same-group grant: last_proxy_exists={last_proxy_exists:?} last_proxy_keys={last_proxy_keys:?} last_proxy_err={last_proxy_err:?}"
            );
        }

        app.close().await.expect("close app");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
    async fn external_runtime_app_selected_group_proxy_real_nfs_grant_change_after_activation_keeps_owner_host_materialized(
    ) {
        let preflight = real_nfs_lab::RealNfsPreflight::detect();
        if !preflight.enabled {
            eprintln!(
                "skip real-nfs runtime-app grant-change test: {}",
                preflight
                    .reason
                    .unwrap_or_else(|| "real-nfs preflight failed".to_string())
            );
            return;
        }

        let mut lab = real_nfs_lab::NfsLab::start().expect("start NFS lab");
        lab.mkdir("nfs1", "force-find-stress")
            .expect("create nfs1 force-find dir");
        lab.mkdir("nfs2", "force-find-stress")
            .expect("create nfs2 force-find dir");
        lab.write_file("nfs1", "force-find-stress/seed.txt", "a\n")
            .expect("seed nfs1");
        lab.write_file("nfs2", "force-find-stress/seed.txt", "b\n")
            .expect("seed nfs2");

        let nfs1_source = lab.export_source("nfs1");
        let nfs2_source = lab.export_source("nfs2");
        let nfs1 = lab
            .mount_export("node-a", "nfs1")
            .expect("mount node-a nfs1 export");
        let nfs2_owner = lab
            .mount_export("node-a", "nfs2")
            .expect("mount node-a nfs2 export");
        let nfs2_remote = lab
            .mount_export("node-c", "nfs2")
            .expect("mount node-c nfs2 export");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_root = tempdir().expect("create worker socket dir");
        let source_socket_dir = worker_socket_root.path().join("source");
        let sink_socket_dir = worker_socket_root.path().join("sink");
        fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
        fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");

        let app = FSMetaApp::with_boundaries_and_state(
            FSMetaConfig {
                source: SourceConfig {
                    roots: vec![
                        worker_fs_source_root("nfs1", &nfs1_source),
                        worker_fs_source_root("nfs2", &nfs2_source),
                    ],
                    host_object_grants: Vec::new(),
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".to_string()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init external-worker runtime app");

        app.on_control_frame(&[
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
        ])
        .await
        .expect("apply runtime control");

        app.on_control_frame(&[host_object_grants_changed_rows_envelope(
            1,
            &[
                (
                    "node-a::nfs1",
                    "node-a",
                    "10.0.0.11",
                    nfs1.to_string_lossy().as_ref(),
                    &nfs1_source,
                    true,
                ),
                (
                    "node-a::nfs2",
                    "node-a",
                    "10.0.0.12",
                    nfs2_owner.to_string_lossy().as_ref(),
                    &nfs2_source,
                    true,
                ),
                (
                    "node-c::nfs2",
                    "node-c",
                    "10.0.0.31",
                    nfs2_remote.to_string_lossy().as_ref(),
                    &nfs2_source,
                    true,
                ),
            ],
        )])
        .await
        .expect("apply runtime host grants changed");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let grants_version = app
                .source
                .host_object_grants_version_snapshot()
                .await
                .expect("host object grants version");
            let source_groups = app
                .source
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            let scan_groups = app
                .source
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups")
                .unwrap_or_default();
            let sink_groups = app
                .sink
                .scheduled_group_ids()
                .await
                .expect("sink groups")
                .unwrap_or_default();
            let source_status = app
                .source
                .status_snapshot()
                .await
                .expect("source status snapshot");
            let nfs2_grants = source_status
                .logical_roots
                .iter()
                .find(|root| root.root_id == "nfs2")
                .map(|root| root.matched_grants);
            if grants_version == 1
                && source_groups == expected_groups
                && scan_groups == expected_groups
                && sink_groups == expected_groups
                && nfs2_grants == Some(2)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for post-grant-change convergence: version={grants_version} source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?} nfs2_grants={nfs2_grants:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        for group_id in ["nfs1", "nfs2"] {
            let request = selected_group_dir_request(b"/force-find-stress", group_id);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            let mut direct_ready = false;
            let mut proxy_ready = false;
            let mut last_direct_exists = None::<bool>;
            let mut last_proxy_exists = None::<bool>;
            let mut last_proxy_keys = Vec::<String>::new();
            let mut last_proxy_err = None::<String>;
            while tokio::time::Instant::now() < deadline {
                match app.query_tree(&request).await {
                    Ok(grouped) => {
                        let exists = grouped.get(group_id).map(|payload| payload.root.exists);
                        last_direct_exists = exists;
                        if exists == Some(true) {
                            direct_ready = true;
                        }
                    }
                    Err(_) => {}
                }
                match selected_group_proxy_tree(
                    boundary.clone(),
                    NodeId("node-a".to_string()),
                    &request,
                )
                .await
                {
                    Ok(grouped) => {
                        last_proxy_keys = grouped.keys().cloned().collect();
                        last_proxy_exists = grouped.get(group_id).map(|payload| payload.root.exists);
                        last_proxy_err = None;
                        if last_proxy_exists == Some(true) {
                            proxy_ready = true;
                        }
                    }
                    Err(err) => {
                        last_proxy_err = Some(err.to_string());
                    }
                }
                if direct_ready && proxy_ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(
                direct_ready,
                "direct sink query should materialize selected group {group_id} after grant-change fanout: last_direct_exists={last_direct_exists:?}"
            );
            assert!(
                proxy_ready,
                "selected-group proxy query should materialize selected group {group_id} after grant-change fanout: last_proxy_exists={last_proxy_exists:?} last_proxy_keys={last_proxy_keys:?} last_proxy_err={last_proxy_err:?}"
            );
        }

        app.close().await.expect("close app");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
    async fn external_runtime_app_selected_group_proxy_real_nfs_peer_owned_source_scope_leaves_group_unmaterialized(
    ) {
        let preflight = real_nfs_lab::RealNfsPreflight::detect();
        if !preflight.enabled {
            eprintln!(
                "skip real-nfs runtime-app peer-scope test: {}",
                preflight
                    .reason
                    .unwrap_or_else(|| "real-nfs preflight failed".to_string())
            );
            return;
        }

        let mut lab = real_nfs_lab::NfsLab::start().expect("start NFS lab");
        lab.mkdir("nfs1", "force-find-stress")
            .expect("create nfs1 force-find dir");
        lab.mkdir("nfs2", "force-find-stress")
            .expect("create nfs2 force-find dir");
        lab.write_file("nfs1", "force-find-stress/seed.txt", "a\n")
            .expect("seed nfs1");
        lab.write_file("nfs2", "force-find-stress/seed.txt", "b\n")
            .expect("seed nfs2");

        let nfs1_source = lab.export_source("nfs1");
        let nfs2_source = lab.export_source("nfs2");
        let nfs1 = lab
            .mount_export("node-a", "nfs1")
            .expect("mount node-a nfs1 export");
        let nfs2_owner = lab
            .mount_export("node-a", "nfs2")
            .expect("mount node-a nfs2 export");
        let nfs2_remote = lab
            .mount_export("node-c", "nfs2")
            .expect("mount node-c nfs2 export");

        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_root = tempdir().expect("create worker socket dir");
        let source_socket_dir = worker_socket_root.path().join("source");
        let sink_socket_dir = worker_socket_root.path().join("sink");
        fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
        fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");

        let app = FSMetaApp::with_boundaries_and_state(
            FSMetaConfig {
                source: SourceConfig {
                    roots: vec![
                        worker_fs_source_root("nfs1", &nfs1_source),
                        worker_fs_source_root("nfs2", &nfs2_source),
                    ],
                    host_object_grants: Vec::new(),
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".to_string()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init external-worker runtime app");

        app.on_control_frame(&[
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"])],
                1,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"])],
                1,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                1,
            ),
        ])
        .await
        .expect("apply runtime control");

        app.on_control_frame(&[host_object_grants_changed_rows_envelope(
            1,
            &[
                (
                    "node-a::nfs1",
                    "node-a",
                    "10.0.0.11",
                    nfs1.to_string_lossy().as_ref(),
                    &nfs1_source,
                    true,
                ),
                (
                    "node-a::nfs2",
                    "node-a",
                    "10.0.0.12",
                    nfs2_owner.to_string_lossy().as_ref(),
                    &nfs2_source,
                    true,
                ),
                (
                    "node-c::nfs2",
                    "node-c",
                    "10.0.0.31",
                    nfs2_remote.to_string_lossy().as_ref(),
                    &nfs2_source,
                    true,
                ),
            ],
        )])
        .await
        .expect("apply runtime host grants changed");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let source_groups = app
                .source
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            let scan_groups = app
                .source
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups")
                .unwrap_or_default();
            let sink_groups = app
                .sink
                .scheduled_group_ids()
                .await
                .expect("sink groups")
                .unwrap_or_default();
            let source_status = app
                .source
                .status_snapshot()
                .await
                .expect("source status snapshot");
            let nfs2_grants = source_status
                .logical_roots
                .iter()
                .find(|root| root.root_id == "nfs2")
                .map(|root| root.matched_grants);
            if source_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
                && scan_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
                && sink_groups
                    == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
                && nfs2_grants == Some(2)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for peer-owned source-scope convergence: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?} nfs2_grants={nfs2_grants:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let nfs1_request = selected_group_dir_request(b"/force-find-stress", "nfs1");
        let nfs1_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut nfs1_direct_ready = false;
        let mut nfs1_proxy_ready = false;
        while tokio::time::Instant::now() < nfs1_deadline {
            if app
                .query_tree(&nfs1_request)
                .await
                .ok()
                .and_then(|grouped| grouped.get("nfs1").map(|payload| payload.root.exists))
                == Some(true)
            {
                nfs1_direct_ready = true;
            }
            if selected_group_proxy_tree(boundary.clone(), NodeId("node-a".to_string()), &nfs1_request)
                .await
                .ok()
                .and_then(|grouped| grouped.get("nfs1").map(|payload| payload.root.exists))
                == Some(true)
            {
                nfs1_proxy_ready = true;
            }
            if nfs1_direct_ready && nfs1_proxy_ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(nfs1_direct_ready, "nfs1 direct query should still materialize");
        assert!(nfs1_proxy_ready, "nfs1 selected-group proxy should still materialize");

        let nfs2_request = selected_group_dir_request(b"/force-find-stress", "nfs2");
        let nfs2_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut last_direct_exists = None::<bool>;
        let mut last_proxy_exists = None::<bool>;
        let mut last_proxy_keys = Vec::<String>::new();
        let mut last_proxy_err = None::<String>;
        while tokio::time::Instant::now() < nfs2_deadline {
            if let Ok(grouped) = app.query_tree(&nfs2_request).await {
                last_direct_exists = grouped.get("nfs2").map(|payload| payload.root.exists);
            }
            match selected_group_proxy_tree(boundary.clone(), NodeId("node-a".to_string()), &nfs2_request)
                .await
            {
                Ok(grouped) => {
                    last_proxy_keys = grouped.keys().cloned().collect();
                    last_proxy_exists = grouped.get("nfs2").map(|payload| payload.root.exists);
                    last_proxy_err = None;
                }
                Err(err) => {
                    last_proxy_err = Some(err.to_string());
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert_eq!(
            last_direct_exists,
            Some(false),
            "peer-owned local source scope should leave nfs2 direct query unmaterialized before force-find"
        );
        assert_eq!(
            last_proxy_exists,
            Some(false),
            "peer-owned local source scope should leave nfs2 selected-group proxy unmaterialized before force-find: last_proxy_keys={last_proxy_keys:?} last_proxy_err={last_proxy_err:?}"
        );

        app.close().await.expect("close app");
    }

    #[test]
    fn parses_manifest_config_multi_roots() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![
                    {
                        let mut row = match root_entry_with_id("nfs1-a", "/mnt/nfs1") {
                            ConfigValue::Map(map) => map,
                            _ => unreachable!(),
                        };
                        row.insert("watch".to_string(), ConfigValue::Bool(true));
                        row.insert("scan".to_string(), ConfigValue::Bool(true));
                        ConfigValue::Map(row)
                    },
                    root_entry_with_id("nfs2-b", "/mnt/nfs2"),
                ]),
            ),
            ("api".to_string(), minimal_api_config()),
        ]);
        let parsed =
            FSMetaProductConfig::from_product_manifest_config(&cfg).expect("parse roots config");
        assert_eq!(parsed.source.roots.len(), 2);
        assert_eq!(parsed.source.roots[0].id, "nfs1-a");
    }

    #[test]
    fn runtime_local_host_ref_from_runtime_metadata() {
        let cfg = std::collections::HashMap::from([(
            "__cnx_runtime".to_string(),
            ConfigValue::Map(std::collections::HashMap::from([(
                "local_host_ref".to_string(),
                ConfigValue::String("host://capanix-node-3".to_string()),
            )])),
        )]);
        let resolved = FSMetaRuntimeApp::runtime_local_host_ref(&cfg)
            .expect("local_host_ref should resolve from runtime metadata");
        assert_eq!(resolved.0, "host://capanix-node-3");
    }

    #[test]
    fn runtime_local_host_ref_rejects_empty_value() {
        let cfg = std::collections::HashMap::from([(
            "__cnx_runtime".to_string(),
            ConfigValue::Map(std::collections::HashMap::from([(
                "local_host_ref".to_string(),
                ConfigValue::String("   ".to_string()),
            )])),
        )]);
        assert!(
            FSMetaRuntimeApp::runtime_local_host_ref(&cfg).is_none(),
            "blank local_host_ref must not be accepted"
        );
    }

    #[test]
    fn required_runtime_local_host_ref_fails_closed_when_missing() {
        let cfg = std::collections::HashMap::from([(
            "__cnx_runtime".to_string(),
            ConfigValue::Map(std::collections::HashMap::new()),
        )]);
        let err = FSMetaRuntimeApp::required_runtime_local_host_ref(&cfg)
            .expect_err("missing local_host_ref must fail closed");
        assert!(
            err.to_string()
                .contains("__cnx_runtime.local_host_ref is required"),
            "error should explain required runtime local_host_ref"
        );
    }

    #[test]
    fn parses_source_tuning_fields_with_bounds() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            ("api".to_string(), minimal_api_config()),
            ("scan_workers".to_string(), ConfigValue::Int(999)),
            ("audit_interval_ms".to_string(), ConfigValue::Int(1)),
            ("throttle_interval_ms".to_string(), ConfigValue::Int(1)),
            ("sink_tombstone_ttl_ms".to_string(), ConfigValue::Int(1)),
            (
                "sink_tombstone_tolerance_us".to_string(),
                ConfigValue::Int(i64::MAX),
            ),
        ]);
        let parsed = FSMetaProductConfig::from_product_manifest_config(&cfg)
            .expect("parse source tuning fields");
        assert_eq!(parsed.source.scan_workers, 16);
        assert_eq!(parsed.source.audit_interval, Duration::from_millis(5_000));
        assert_eq!(parsed.source.throttle_interval, Duration::from_millis(50));
        assert_eq!(
            parsed.source.sink_tombstone_ttl,
            Duration::from_millis(1_000)
        );
        assert_eq!(parsed.source.sink_tombstone_tolerance_us, 10_000_000);
    }

    #[test]
    fn rejects_removed_unit_authority_state_dir() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            ("api".to_string(), minimal_api_config()),
            (
                "unit_authority_state_dir".to_string(),
                ConfigValue::String("relative/statecell".to_string()),
            ),
        ]);
        let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
            .expect_err("removed unit_authority_state_dir must fail");
        assert!(
            err.to_string()
                .contains("unit_authority_state_carrier/unit_authority_state_dir are removed")
        );
    }

    #[test]
    fn rejects_removed_unit_authority_state_carrier() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            ("api".to_string(), minimal_api_config()),
            (
                "unit_authority_state_carrier".to_string(),
                ConfigValue::String("external".to_string()),
            ),
        ]);
        let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
            .expect_err("removed unit_authority_state_carrier must fail");
        assert!(
            err.to_string()
                .contains("unit_authority_state_carrier/unit_authority_state_dir are removed")
        );
    }

    #[test]
    fn rejects_removed_sink_execution_mode_field() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            ("api".to_string(), minimal_api_config()),
            (
                "sink_execution_mode".to_string(),
                ConfigValue::String("external".to_string()),
            ),
        ]);
        let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
            .expect_err("removed sink execution mode must fail");
        assert!(err.to_string().contains(
            "sink_execution_mode has been removed; declare generic workers.<role>.mode/startup.path/socket_dir instead"
        ));
    }

    #[test]
    fn rejects_removed_source_execution_mode_field() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            ("api".to_string(), minimal_api_config()),
            (
                "source_execution_mode".to_string(),
                ConfigValue::String("external".to_string()),
            ),
        ]);
        let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
            .expect_err("removed source execution mode must fail");
        assert!(err.to_string().contains(
            "source_execution_mode has been removed; declare generic workers.<role>.mode/startup.path/socket_dir instead"
        ));
    }

    #[test]
    fn rejects_removed_sink_worker_bin_path_field() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            ("api".to_string(), minimal_api_config()),
            (
                "sink_worker_bin_path".to_string(),
                ConfigValue::String("/usr/local/bin/fs_meta_sink_worker".to_string()),
            ),
        ]);
        let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
            .expect_err("removed sink worker bin path must fail");
        assert!(
            err.to_string()
                .contains("sink_worker_bin_path has been removed")
        );
    }

    #[test]
    fn rejects_removed_source_worker_bin_path_field() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            ("api".to_string(), minimal_api_config()),
            (
                "source_worker_bin_path".to_string(),
                ConfigValue::String("/usr/local/bin/fs_meta_source_worker".to_string()),
            ),
        ]);
        let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
            .expect_err("removed source worker bin path must fail");
        assert!(
            err.to_string()
                .contains("source_worker_bin_path has been removed")
        );
    }

    #[test]
    fn parses_worker_oriented_mode_config_shape() {
        let (source, sink) = runtime_worker_client_bindings(&compiled_runtime_worker_bindings(&[
            ("facade", WorkerMode::Embedded, None),
            (
                "source",
                WorkerMode::External,
                Some("/usr/local/lib/libfs_meta_runtime.so"),
            ),
            ("sink", WorkerMode::Embedded, None),
        ]))
        .expect("parse worker-oriented config");
        assert_eq!(source.mode, WorkerMode::External);
        assert_eq!(sink.mode, WorkerMode::Embedded);
        assert_eq!(
            source.module_path,
            Some(fs_meta_worker_module_path(
                "/usr/local/lib/libfs_meta_runtime.so",
            ))
        );
    }

    #[test]
    fn rejects_missing_compiled_source_worker_binding() {
        let err = runtime_worker_client_bindings(&compiled_runtime_worker_bindings(&[
            ("facade", WorkerMode::Embedded, None),
            (
                "sink",
                WorkerMode::External,
                Some("/usr/local/lib/libfs_meta_runtime.so"),
            ),
        ]))
        .expect_err("missing source binding must fail closed");
        assert!(
            err.to_string()
                .contains("compiled runtime worker bindings must declare role 'source'")
        );
    }

    #[test]
    fn rejects_missing_compiled_sink_worker_binding() {
        let err = runtime_worker_client_bindings(&compiled_runtime_worker_bindings(&[
            ("facade", WorkerMode::Embedded, None),
            (
                "source",
                WorkerMode::External,
                Some("/usr/local/lib/libfs_meta_runtime.so"),
            ),
        ]))
        .expect_err("missing sink binding must fail closed");
        assert!(
            err.to_string()
                .contains("compiled runtime worker bindings must declare role 'sink'")
        );
    }

    #[test]
    fn rejects_external_facade_worker_mode() {
        let err = runtime_worker_client_bindings(&compiled_runtime_worker_bindings(&[(
            "facade",
            WorkerMode::External,
            Some("/tmp/lib.so"),
        )]))
        .expect_err("facade-worker cannot yet move to external mode");
        assert!(
            err.to_string()
                .contains("runtime worker binding for 'facade' must remain embedded")
        );
    }

    #[test]
    fn rejects_missing_compiled_facade_worker_binding() {
        let err = runtime_worker_client_bindings(&compiled_runtime_worker_bindings(&[
            (
                "source",
                WorkerMode::External,
                Some("/usr/local/lib/libfs_meta_runtime.so"),
            ),
            (
                "sink",
                WorkerMode::External,
                Some("/usr/local/lib/libfs_meta_runtime.so"),
            ),
        ]))
        .expect_err("missing facade binding must fail closed");
        assert!(
            err.to_string()
                .contains("compiled runtime worker bindings must declare role 'facade'")
        );
    }

    #[test]
    fn rejects_root_source_locator_field() {
        let cfg = std::collections::HashMap::from([(
            "roots".to_string(),
            ConfigValue::Array(vec![{
                let mut row = match root_entry_with_id("nfs1", "/mnt/nfs1") {
                    ConfigValue::Map(map) => map,
                    _ => unreachable!(),
                };
                row.insert(
                    "source_locator".to_string(),
                    ConfigValue::String("10.0.0.11".to_string()),
                );
                ConfigValue::Map(row)
            }]),
        )]);
        let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
            .expect_err("must reject source_locator");
        assert!(err.to_string().contains("source_locator is forbidden"));
    }

    #[test]
    fn derives_root_id_from_selector_mount_point_when_missing() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            ("api".to_string(), minimal_api_config()),
        ]);
        let parsed =
            FSMetaProductConfig::from_product_manifest_config(&cfg).expect("parse roots config");
        assert_eq!(parsed.source.roots[0].id, "mnt-nfs1");
    }

    #[test]
    fn rejects_root_path_field() {
        let cfg = std::collections::HashMap::from([(
            "root_path".to_string(),
            ConfigValue::String("/mnt/nfs1".to_string()),
        )]);
        let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
            .expect_err("must reject root_path");
        assert!(err.to_string().contains("root_path is forbidden"));
    }

    #[test]
    fn allows_missing_roots_field_as_empty_deployed_state() {
        let cfg = std::collections::HashMap::from([("api".to_string(), minimal_api_config())]);
        let parsed = FSMetaProductConfig::from_product_manifest_config(&cfg)
            .expect("missing roots should parse as empty deployed state");
        assert!(parsed.source.roots.is_empty());
    }

    #[test]
    fn parses_api_config_auth_fields() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "api".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([
                    ("enabled".to_string(), ConfigValue::Bool(true)),
                    (
                        "facade_resource_id".to_string(),
                        ConfigValue::String("fs-meta-tcp-listener".to_string()),
                    ),
                    (
                        "auth".to_string(),
                        ConfigValue::Map(std::collections::HashMap::from([
                            (
                                "passwd_path".to_string(),
                                ConfigValue::String("/tmp/fs-meta.passwd".to_string()),
                            ),
                            (
                                "shadow_path".to_string(),
                                ConfigValue::String("/tmp/fs-meta.shadow".to_string()),
                            ),
                            ("session_ttl_secs".to_string(), ConfigValue::Int(900)),
                            (
                                "query_keys_path".to_string(),
                                ConfigValue::String("/tmp/fs-meta.query-keys.json".to_string()),
                            ),
                            (
                                "management_group".to_string(),
                                ConfigValue::String("fsmeta_management".to_string()),
                            ),
                        ])),
                    ),
                ])),
            ),
        ]);
        let cfg = {
            let mut cfg = cfg;
            cfg.insert(
                "__cnx_runtime".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([
                    (
                        "local_host_ref".to_string(),
                        ConfigValue::String("host://capanix-node-3".to_string()),
                    ),
                    (
                        "announced_resources".to_string(),
                        ConfigValue::Array(vec![ConfigValue::Map(
                            std::collections::HashMap::from([
                                (
                                    "resource_id".to_string(),
                                    ConfigValue::String("fs-meta-tcp-listener".to_string()),
                                ),
                                (
                                    "node_id".to_string(),
                                    ConfigValue::String("host://capanix-node-3".to_string()),
                                ),
                                (
                                    "resource_kind".to_string(),
                                    ConfigValue::String("tcp_listener".to_string()),
                                ),
                                (
                                    "bind_addr".to_string(),
                                    ConfigValue::String("127.0.0.1:18080".to_string()),
                                ),
                            ]),
                        )]),
                    ),
                ])),
            );
            cfg
        };
        let parsed = FSMetaConfig::from_runtime_manifest_config(&cfg).expect("parse api config");
        assert!(parsed.api.enabled);
        assert_eq!(parsed.api.facade_resource_id, "fs-meta-tcp-listener");
        assert_eq!(parsed.api.local_listener_resources.len(), 1);
        assert_eq!(
            parsed.api.local_listener_resources[0].bind_addr,
            "127.0.0.1:18080"
        );
        assert_eq!(parsed.api.auth.session_ttl_secs, 900);
        assert_eq!(
            parsed.api.auth.passwd_path,
            std::path::PathBuf::from("/tmp/fs-meta.passwd")
        );
        assert_eq!(
            parsed.api.auth.query_keys_path,
            std::path::PathBuf::from("/tmp/fs-meta.query-keys.json")
        );
        assert_eq!(parsed.api.auth.management_group, "fsmeta_management");
    }

    #[test]
    fn rejects_api_disabled_in_manifest_config() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "api".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([(
                    "enabled".to_string(),
                    ConfigValue::Bool(false),
                )])),
            ),
        ]);
        let mut cfg = cfg;
        if let Some(ConfigValue::Map(api)) = cfg.get_mut("api") {
            api.insert(
                "auth".to_string(),
                ConfigValue::Map(std::collections::HashMap::new()),
            );
        }
        let err = FSMetaProductConfig::from_product_manifest_config(&cfg)
            .expect_err("api must be mandatory");
        assert!(
            err.to_string()
                .contains("api.enabled must be true; fs-meta management API boundary is mandatory")
        );
    }
}
