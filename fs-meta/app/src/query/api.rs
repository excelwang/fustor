use crate::query::models::SubtreeStats;
use crate::query::observation::{
    ObservationTrustPolicy, evaluate_observation_status, materialized_query_observation_evidence,
    trusted_materialized_not_ready_message,
};
use crate::query::path::bytes_to_display_string;
#[cfg(test)]
use crate::query::path::normalized_path_for_query;
use crate::query::reliability::GroupReliability;
use crate::query::request::{
    ForceFindQueryPayload, InternalQueryRequest, MaterializedQueryPayload, QueryOp, QueryScope,
    TreeQueryOptions,
};
#[cfg(test)]
use crate::query::result_ops::{
    raw_query_results_by_origin_from_source_events, tree_group_payload_from_query_response,
};
use crate::query::tree::{
    ObservationState, ObservationStatus, PageOrder, ReadClass, StabilityState, TreeGroupPayload,
    TreePageEntry, TreePageRoot, TreeStability,
};
use crate::runtime::routes::{
    METHOD_SINK_QUERY, METHOD_SINK_QUERY_PROXY, METHOD_SINK_STATUS, METHOD_SOURCE_FIND,
    METHOD_SOURCE_STATUS, ROUTE_TOKEN_FS_META_INTERNAL, default_route_bindings,
    sink_query_route_bindings_for,
};
use crate::runtime::seam::exchange_host_adapter;
use crate::sink::SinkStatusSnapshot;
use crate::source::SourceStatusSnapshot;
use crate::source::config::GrantedMountRoot;
use crate::workers::sink::SinkFacade;
use crate::workers::source::{SourceFacade, SourceObservabilitySnapshot};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD as B64URL},
};
use bytes::Bytes;
// bound_route_metrics_snapshot remains an app-sdk helper for transport
// diagnostics; ordinary app-facing imports in this module stay on app-sdk.
use capanix_app_sdk::runtime::NodeId;
use capanix_app_sdk::{CnxError, Event, bound_route_metrics_snapshot};
use capanix_host_adapter_fs::HostAdapter;
use capanix_runtime_entry_sdk::advanced::boundary::ChannelIoSubset;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const QUERY_TIMEOUT_MS_DEFAULT: u64 = 60_000;
const FORCE_FIND_TIMEOUT_MS_DEFAULT: u64 = 60_000;
const GROUP_PAGE_SIZE_DEFAULT: usize = 64;
const GROUP_PAGE_SIZE_MAX: usize = 1_000;
const ENTRY_PAGE_SIZE_DEFAULT: usize = 1_000;
const ENTRY_PAGE_SIZE_MAX: usize = 10_000;
const PIT_TTL_MS_DEFAULT: u64 = 900_000;
const PIT_MAX_SESSIONS: usize = 128;
const PIT_MAX_TOTAL_BYTES: usize = 64 * 1024 * 1024;
// Materialized fanout replies on the local real-NFS cluster arrive within
// milliseconds, while duplicate route re-deliveries can appear seconds later.
// Keep the materialized collection window short so a late duplicate batch does
// not pin `/tree` and `/stats` open indefinitely.
const MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE: Duration = Duration::from_millis(500);
// Source/sink status fanout drives the trusted-materialized readiness gate.
// Unlike tree payload collection, a partial early return here can falsely
// downgrade healthy groups into "initial audit incomplete". Give peer status
// replies a little more time to settle before closing the collect window.
const STATUS_ROUTE_COLLECT_IDLE_GRACE: Duration = Duration::from_secs(2);
// Force-find is intentionally a slower freshness path. Keep a larger collection
// window here so the request remains in-flight long enough for overlap guards
// and multi-runner aggregation scenarios.
const FORCE_FIND_ROUTE_COLLECT_IDLE_GRACE: Duration = Duration::from_secs(5);
const FORCE_FIND_MIN_INFLIGHT_HOLD: Duration = Duration::from_secs(2);
const FORCE_FIND_INFLIGHT_CONFLICT_PREFIX: &str = "force-find inflight conflict:";

fn debug_status_route_fanin_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_STATUS_ROUTE_FANIN")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn debug_force_find_route_capture_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_FORCE_FIND_ROUTE_CAPTURE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn debug_group_ranking_capture_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_GROUP_RANKING_CAPTURE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn debug_materialized_route_capture_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_MATERIALIZED_ROUTE_CAPTURE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn summarize_groups_by_node(groups: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    groups
        .iter()
        .map(|(node_id, groups)| format!("{node_id}={}", groups.join("|")))
        .collect()
}

fn summarize_sink_status_route_snapshot(snapshot: &SinkStatusSnapshot) -> String {
    format!(
        "groups={} scheduled={:?} control={:?} received_batches={:?} received_events={:?} received_origins={:?} received_origin_counts={:?}",
        snapshot.groups.len(),
        summarize_groups_by_node(&snapshot.scheduled_groups_by_node),
        summarize_groups_by_node(&snapshot.last_control_frame_signals_by_node),
        snapshot.received_batches_by_node,
        snapshot.received_events_by_node,
        summarize_groups_by_node(&snapshot.last_received_origins_by_node),
        summarize_groups_by_node(&snapshot.received_origin_counts_by_node)
    )
}

fn summarize_event_counts_by_origin(events: &[Event]) -> Vec<String> {
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

#[derive(Deserialize)]
pub struct ApiParams {
    pub path: Option<String>,
    pub path_b64: Option<String>,
    pub group: Option<String>,
    pub recursive: Option<bool>,
    pub max_depth: Option<usize>,
    pub pit_id: Option<String>,
    pub group_order: Option<GroupOrder>,
    pub group_page_size: Option<usize>,
    pub group_after: Option<String>,
    pub entry_page_size: Option<usize>,
    pub entry_after: Option<String>,
    pub read_class: Option<ReadClass>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum MemberGroupingStrategy {
    #[default]
    MountPoint,
    HostIp,
    ObjectRef,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum GroupOrder {
    #[default]
    GroupKey,
    FileCount,
    FileAge,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CursorQueryMode {
    Tree,
    ForceFind,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct GroupPageCursor {
    version: u8,
    mode: CursorQueryMode,
    pit_id: String,
    next_group_offset: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct TreeEntryCursor {
    version: u8,
    mode: CursorQueryMode,
    pit_id: String,
    group: String,
    next_entry_offset: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct ForceFindEntryCursor {
    version: u8,
    mode: CursorQueryMode,
    pit_id: String,
    group: String,
    next_entry_offset: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct ProjectionPolicy {
    #[serde(default)]
    pub member_grouping: MemberGroupingStrategy,
    #[serde(default)]
    pub mount_point_by_object_ref: HashMap<String, String>,
    #[serde(default)]
    pub object_ref_by_host_ip: HashMap<String, String>,
    #[serde(default)]
    pub host_ip_by_object_ref: HashMap<String, String>,
    #[serde(default)]
    pub query_timeout_ms: Option<u64>,
    #[serde(default)]
    pub force_find_timeout_ms: Option<u64>,
}

impl ProjectionPolicy {
    fn query_timeout(&self) -> Duration {
        Duration::from_millis(self.query_timeout_ms.unwrap_or(QUERY_TIMEOUT_MS_DEFAULT))
    }

    fn force_find_timeout(&self) -> Duration {
        Duration::from_millis(
            self.force_find_timeout_ms
                .unwrap_or(FORCE_FIND_TIMEOUT_MS_DEFAULT),
        )
    }

    fn group_key_for_object_ref(&self, object_ref: &str) -> String {
        match self.member_grouping {
            MemberGroupingStrategy::HostIp => self
                .host_ip_by_object_ref
                .get(object_ref)
                .cloned()
                .unwrap_or_else(|| object_ref.to_string()),
            MemberGroupingStrategy::ObjectRef => object_ref.to_string(),
            MemberGroupingStrategy::MountPoint => {
                if let Some(mount_point) = self.mount_point_by_object_ref.get(object_ref) {
                    // Keep logical group ids stable: if mapped path suffix is exactly the same
                    // id (e.g. id=nfs1, mount=/.../nfs1), return id instead of leaking host path.
                    if mount_point
                        .rsplit('/')
                        .next()
                        .is_some_and(|tail| tail == object_ref)
                    {
                        object_ref.to_string()
                    } else {
                        mount_point.clone()
                    }
                } else {
                    object_ref.to_string()
                }
            }
        }
    }
}

#[derive(Clone)]
enum QueryBackend {
    Local {
        sink: Arc<SinkFacade>,
        source: Arc<SourceFacade>,
    },
    Route {
        sink: Arc<SinkFacade>,
        boundary: Arc<dyn ChannelIoSubset>,
        origin_id: NodeId,
        source: Arc<SourceFacade>,
    },
}

#[derive(Clone)]
struct ApiState {
    backend: QueryBackend,
    policy: Arc<RwLock<ProjectionPolicy>>,
    pit_store: Arc<Mutex<QueryPitStore>>,
    force_find_inflight: Arc<Mutex<BTreeSet<String>>>,
    force_find_route_rr: Arc<Mutex<BTreeMap<String, usize>>>,
    readiness_source: Option<Arc<SourceFacade>>,
    readiness_sink: Option<Arc<SinkFacade>>,
    materialized_sink_status_cache: Arc<Mutex<Option<CachedSinkStatusSnapshot>>>,
}

#[derive(Clone, Debug)]
struct CachedSinkStatusSnapshot {
    groups: BTreeSet<String>,
    snapshot: SinkStatusSnapshot,
}

struct ForceFindInflightGuard {
    inflight: Arc<Mutex<std::collections::BTreeSet<String>>>,
    groups: Vec<String>,
}

impl Drop for ForceFindInflightGuard {
    fn drop(&mut self) {
        if let Ok(mut inflight) = self.inflight.lock() {
            for group in &self.groups {
                inflight.remove(group);
            }
            eprintln!(
                "fs_meta_query_api: force-find inflight release groups={:?} remaining={:?}",
                self.groups, inflight
            );
        }
    }
}

#[derive(Clone, Debug)]
struct TreePitScope {
    path: Vec<u8>,
    group: Option<String>,
    recursive: bool,
    max_depth: Option<usize>,
    group_order: GroupOrder,
    read_class: ReadClass,
}

#[derive(Clone, Debug)]
struct ForceFindPitScope {
    path: Vec<u8>,
    group: Option<String>,
    recursive: bool,
    max_depth: Option<usize>,
    group_order: GroupOrder,
}

#[derive(Clone, Debug)]
enum PitScope {
    Tree(TreePitScope),
    ForceFind(ForceFindPitScope),
}

#[derive(Clone, Debug)]
struct PitMetadata {
    read_class: ReadClass,
    metadata_available: bool,
    withheld_reason: Option<&'static str>,
}

#[derive(Clone, Debug)]
struct GroupPitSnapshot {
    group: String,
    status: &'static str,
    reliable: bool,
    unreliable_reason: Option<crate::shared_types::query::UnreliableReason>,
    stability: TreeStability,
    meta: PitMetadata,
    root: Option<TreePageRoot>,
    entries: Vec<TreePageEntry>,
    errors: Vec<String>,
}

#[derive(Clone, Debug)]
struct PitSession {
    mode: CursorQueryMode,
    scope: PitScope,
    read_class: ReadClass,
    observation_status: ObservationStatus,
    groups: Vec<GroupPitSnapshot>,
    expires_at_ms: u64,
    estimated_bytes: usize,
}

#[derive(Default)]
struct QueryPitStore {
    sessions: HashMap<String, Arc<PitSession>>,
    total_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
enum QueryMode {
    Find,
    ForceFind,
}

#[cfg(test)]
fn materialized_query_readiness_error(
    source_status: &SourceStatusSnapshot,
    sink_status: &SinkStatusSnapshot,
) -> Option<String> {
    let status = evaluate_observation_status(
        &materialized_query_observation_evidence(source_status, sink_status),
        ObservationTrustPolicy::materialized_query(),
    );
    if status.state == ObservationState::TrustedMaterialized {
        return None;
    }
    Some(trusted_materialized_not_ready_message(&status))
}

pub(crate) fn merge_sink_status_snapshots(
    mut snapshots: Vec<SinkStatusSnapshot>,
) -> SinkStatusSnapshot {
    if snapshots.is_empty() {
        return SinkStatusSnapshot::default();
    }
    if snapshots.len() == 1 {
        return snapshots.pop().unwrap_or_default();
    }

    let mut groups = BTreeMap::<String, crate::sink::SinkGroupStatusSnapshot>::new();
    let mut scheduled_groups_by_node = BTreeMap::<String, Vec<String>>::new();
    let mut last_control_frame_signals_by_node = BTreeMap::<String, Vec<String>>::new();
    let mut received_batches_by_node = BTreeMap::<String, u64>::new();
    let mut received_events_by_node = BTreeMap::<String, u64>::new();
    let mut received_control_events_by_node = BTreeMap::<String, u64>::new();
    let mut received_data_events_by_node = BTreeMap::<String, u64>::new();
    let mut last_received_at_us_by_node = BTreeMap::<String, u64>::new();
    let mut last_received_origins_by_node = BTreeMap::<String, Vec<String>>::new();
    let mut received_origin_counts_by_node = BTreeMap::<String, Vec<String>>::new();
    let mut stream_received_batches_by_node = BTreeMap::<String, u64>::new();
    let mut stream_received_events_by_node = BTreeMap::<String, u64>::new();
    let mut stream_received_origin_counts_by_node = BTreeMap::<String, Vec<String>>::new();
    let mut stream_ready_origin_counts_by_node = BTreeMap::<String, Vec<String>>::new();
    let mut stream_deferred_origin_counts_by_node = BTreeMap::<String, Vec<String>>::new();
    let mut stream_dropped_origin_counts_by_node = BTreeMap::<String, Vec<String>>::new();
    let mut stream_applied_batches_by_node = BTreeMap::<String, u64>::new();
    let mut stream_applied_events_by_node = BTreeMap::<String, u64>::new();
    let mut stream_applied_control_events_by_node = BTreeMap::<String, u64>::new();
    let mut stream_applied_data_events_by_node = BTreeMap::<String, u64>::new();
    let mut stream_applied_origin_counts_by_node = BTreeMap::<String, Vec<String>>::new();
    let mut stream_last_applied_at_us_by_node = BTreeMap::<String, u64>::new();
    for snapshot in snapshots {
        scheduled_groups_by_node.extend(snapshot.scheduled_groups_by_node);
        last_control_frame_signals_by_node.extend(snapshot.last_control_frame_signals_by_node);
        received_batches_by_node.extend(snapshot.received_batches_by_node);
        received_events_by_node.extend(snapshot.received_events_by_node);
        received_control_events_by_node.extend(snapshot.received_control_events_by_node);
        received_data_events_by_node.extend(snapshot.received_data_events_by_node);
        last_received_at_us_by_node.extend(snapshot.last_received_at_us_by_node);
        last_received_origins_by_node.extend(snapshot.last_received_origins_by_node);
        received_origin_counts_by_node.extend(snapshot.received_origin_counts_by_node);
        stream_received_batches_by_node.extend(snapshot.stream_received_batches_by_node);
        stream_received_events_by_node.extend(snapshot.stream_received_events_by_node);
        stream_received_origin_counts_by_node
            .extend(snapshot.stream_received_origin_counts_by_node);
        stream_ready_origin_counts_by_node.extend(snapshot.stream_ready_origin_counts_by_node);
        stream_deferred_origin_counts_by_node
            .extend(snapshot.stream_deferred_origin_counts_by_node);
        stream_dropped_origin_counts_by_node.extend(snapshot.stream_dropped_origin_counts_by_node);
        stream_applied_batches_by_node.extend(snapshot.stream_applied_batches_by_node);
        stream_applied_events_by_node.extend(snapshot.stream_applied_events_by_node);
        stream_applied_control_events_by_node
            .extend(snapshot.stream_applied_control_events_by_node);
        stream_applied_data_events_by_node.extend(snapshot.stream_applied_data_events_by_node);
        stream_applied_origin_counts_by_node.extend(snapshot.stream_applied_origin_counts_by_node);
        stream_last_applied_at_us_by_node.extend(snapshot.stream_last_applied_at_us_by_node);
        for group in snapshot.groups {
            groups
                .entry(group.group_id.clone())
                .and_modify(|current| {
                    let current_score = (
                        u8::from(current.initial_audit_completed),
                        current.total_nodes,
                        current.live_nodes,
                        current.shadow_time_us,
                    );
                    let incoming_score = (
                        u8::from(group.initial_audit_completed),
                        group.total_nodes,
                        group.live_nodes,
                        group.shadow_time_us,
                    );
                    if incoming_score > current_score {
                        *current = group.clone();
                    }
                })
                .or_insert(group);
        }
    }

    let mut merged = SinkStatusSnapshot::default();
    merged.groups = groups.into_values().collect();
    merged.scheduled_groups_by_node = scheduled_groups_by_node;
    merged.last_control_frame_signals_by_node = last_control_frame_signals_by_node;
    merged.received_batches_by_node = received_batches_by_node;
    merged.received_events_by_node = received_events_by_node;
    merged.received_control_events_by_node = received_control_events_by_node;
    merged.received_data_events_by_node = received_data_events_by_node;
    merged.last_received_at_us_by_node = last_received_at_us_by_node;
    merged.last_received_origins_by_node = last_received_origins_by_node;
    merged.received_origin_counts_by_node = received_origin_counts_by_node;
    merged.stream_received_batches_by_node = stream_received_batches_by_node;
    merged.stream_received_events_by_node = stream_received_events_by_node;
    merged.stream_received_origin_counts_by_node = stream_received_origin_counts_by_node;
    merged.stream_ready_origin_counts_by_node = stream_ready_origin_counts_by_node;
    merged.stream_deferred_origin_counts_by_node = stream_deferred_origin_counts_by_node;
    merged.stream_dropped_origin_counts_by_node = stream_dropped_origin_counts_by_node;
    merged.stream_applied_batches_by_node = stream_applied_batches_by_node;
    merged.stream_applied_events_by_node = stream_applied_events_by_node;
    merged.stream_applied_control_events_by_node = stream_applied_control_events_by_node;
    merged.stream_applied_data_events_by_node = stream_applied_data_events_by_node;
    merged.stream_applied_origin_counts_by_node = stream_applied_origin_counts_by_node;
    merged.stream_last_applied_at_us_by_node = stream_last_applied_at_us_by_node;
    merged.groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
    for group in &merged.groups {
        merged.live_nodes += group.live_nodes;
        merged.tombstoned_count += group.tombstoned_count;
        merged.attested_count += group.attested_count;
        merged.suspect_count += group.suspect_count;
        merged.blind_spot_count += group.blind_spot_count;
        merged.estimated_heap_bytes += group.estimated_heap_bytes;
        merged.shadow_time_us = merged.shadow_time_us.max(group.shadow_time_us);
    }
    merged
}

pub(crate) fn internal_status_request_payload() -> Bytes {
    // Keep status RPCs on a non-empty wire shape so request/reply lanes do not depend on
    // zero-length payload semantics.
    Bytes::from_static(&[0x80])
}

fn merge_source_status_snapshots(mut snapshots: Vec<SourceStatusSnapshot>) -> SourceStatusSnapshot {
    if snapshots.is_empty() {
        return SourceStatusSnapshot::default();
    }
    if snapshots.len() == 1 {
        return snapshots.pop().unwrap_or_default();
    }

    let mut logical_root_map = BTreeMap::new();
    let mut concrete_root_map = BTreeMap::new();
    let mut degraded_root_map = BTreeMap::new();
    let mut current_stream_generation = None;
    for snapshot in snapshots {
        current_stream_generation = std::cmp::max(
            current_stream_generation,
            snapshot.current_stream_generation,
        );
        for entry in snapshot.logical_roots {
            logical_root_map
                .entry(entry.root_id.clone())
                .or_insert(entry);
        }
        for entry in snapshot.concrete_roots {
            concrete_root_map
                .entry(entry.root_key.clone())
                .or_insert(entry);
        }
        for (root_key, reason) in snapshot.degraded_roots {
            degraded_root_map.entry(root_key).or_insert(reason);
        }
    }

    SourceStatusSnapshot {
        current_stream_generation,
        logical_roots: logical_root_map.into_values().collect(),
        concrete_roots: concrete_root_map.into_values().collect(),
        degraded_roots: degraded_root_map.into_iter().collect(),
    }
}

async fn route_source_status_snapshot(
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    timeout: Duration,
) -> Result<SourceStatusSnapshot, CnxError> {
    let adapter = exchange_host_adapter(boundary, origin_id, default_route_bindings());
    let events = adapter
        .call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SOURCE_STATUS,
            internal_status_request_payload(),
            timeout,
            STATUS_ROUTE_COLLECT_IDLE_GRACE,
        )
        .await?;
    let snapshots = events
        .into_iter()
        .map(|event| {
            rmp_serde::from_slice::<SourceObservabilitySnapshot>(event.payload_bytes())
                .map(|snapshot| snapshot.status)
                .map_err(|err| {
                    CnxError::Internal(format!("decode source status snapshot failed: {err}"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(merge_source_status_snapshots(snapshots))
}

pub(crate) async fn route_sink_status_snapshot(
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    timeout: Duration,
) -> Result<SinkStatusSnapshot, CnxError> {
    let adapter = exchange_host_adapter(boundary, origin_id, default_route_bindings());
    let events = adapter
        .call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SINK_STATUS,
            internal_status_request_payload(),
            timeout,
            STATUS_ROUTE_COLLECT_IDLE_GRACE,
        )
        .await?;
    let snapshots = events
        .into_iter()
        .map(|event| {
            rmp_serde::from_slice::<SinkStatusSnapshot>(event.payload_bytes()).map_err(|err| {
                CnxError::Internal(format!("decode sink status snapshot failed: {err}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if debug_status_route_fanin_enabled() {
        let summaries = snapshots
            .iter()
            .map(summarize_sink_status_route_snapshot)
            .collect::<Vec<_>>();
        eprintln!(
            "fs_meta_api_status: sink_route_collect events={} snapshots={:?}",
            snapshots.len(),
            summaries
        );
    }
    Ok(merge_sink_status_snapshots(snapshots))
}

async fn load_materialized_status_snapshots(
    state: &ApiState,
) -> Result<(SourceStatusSnapshot, SinkStatusSnapshot), CnxError> {
    let (Some(source), Some(sink)) = (&state.readiness_source, &state.readiness_sink) else {
        return Ok((
            SourceStatusSnapshot::default(),
            SinkStatusSnapshot::default(),
        ));
    };
    let source_status = match &state.backend {
        QueryBackend::Route {
            boundary,
            origin_id,
            ..
        } => match route_source_status_snapshot(
            boundary.clone(),
            origin_id.clone(),
            Duration::from_secs(30),
        )
        .await
        {
            Ok(snapshot) => snapshot,
            Err(err @ CnxError::Timeout)
            | Err(err @ CnxError::TransportClosed(_))
            | Err(err @ CnxError::ProtocolViolation(_)) => return Err(err),
            Err(err) => return Err(err),
        },
        QueryBackend::Local { .. } => source.status_snapshot().await?,
    };
    let readiness_groups = scan_enabled_readiness_groups(state)?;
    let source_status = filter_source_status_snapshot(source_status, &readiness_groups);
    let sink_status = match &state.backend {
        QueryBackend::Route {
            boundary,
            origin_id,
            ..
        } => match route_sink_status_snapshot(
            boundary.clone(),
            origin_id.clone(),
            Duration::from_secs(30),
        )
        .await
        {
            Ok(snapshot) => snapshot,
            Err(err @ CnxError::Timeout)
            | Err(err @ CnxError::TransportClosed(_))
            | Err(err @ CnxError::ProtocolViolation(_)) => return Err(err),
            Err(err) => return Err(err),
        },
        QueryBackend::Local { .. } => sink.status_snapshot().await?,
    };
    let sink_status = {
        let mut cache = state.materialized_sink_status_cache.lock().map_err(|_| {
            CnxError::Internal("materialized sink status cache lock poisoned".into())
        })?;
        let merged =
            merge_with_cached_sink_status_snapshot(cache.as_ref(), &readiness_groups, sink_status);
        *cache = Some(CachedSinkStatusSnapshot {
            groups: readiness_groups,
            snapshot: merged.clone(),
        });
        merged
    };
    Ok((source_status, sink_status))
}

fn materialized_observation_status(
    source_status: &SourceStatusSnapshot,
    sink_status: &SinkStatusSnapshot,
) -> ObservationStatus {
    evaluate_observation_status(
        &materialized_query_observation_evidence(source_status, sink_status),
        ObservationTrustPolicy::materialized_query(),
    )
}

fn scan_enabled_readiness_groups(state: &ApiState) -> Result<BTreeSet<String>, CnxError> {
    state
        .readiness_source
        .as_ref()
        .map(|source| {
            source.cached_logical_roots_snapshot().map(|roots| {
                roots
                    .into_iter()
                    .filter(|root| root.scan)
                    .map(|root| root.id)
                    .collect()
            })
        })
        .transpose()
        .map(|groups| groups.unwrap_or_default())
}

fn filter_sink_status_snapshot(
    snapshot: SinkStatusSnapshot,
    allowed_groups: &BTreeSet<String>,
) -> SinkStatusSnapshot {
    if allowed_groups.is_empty() {
        return snapshot;
    }
    let groups = snapshot
        .groups
        .into_iter()
        .filter(|group| allowed_groups.contains(&group.group_id))
        .collect::<Vec<_>>();
    merge_sink_status_snapshots(vec![SinkStatusSnapshot {
        groups,
        ..SinkStatusSnapshot::default()
    }])
}

fn filter_source_status_snapshot(
    snapshot: SourceStatusSnapshot,
    allowed_groups: &BTreeSet<String>,
) -> SourceStatusSnapshot {
    if allowed_groups.is_empty() {
        return snapshot;
    }
    SourceStatusSnapshot {
        current_stream_generation: snapshot.current_stream_generation,
        logical_roots: snapshot
            .logical_roots
            .into_iter()
            .filter(|root| allowed_groups.contains(&root.root_id))
            .collect(),
        concrete_roots: snapshot
            .concrete_roots
            .into_iter()
            .filter(|root| allowed_groups.contains(&root.logical_root_id))
            .collect(),
        degraded_roots: snapshot
            .degraded_roots
            .into_iter()
            .filter(|(root_id, _)| allowed_groups.contains(root_id))
            .collect(),
    }
}

fn merge_with_cached_sink_status_snapshot(
    cached: Option<&CachedSinkStatusSnapshot>,
    allowed_groups: &BTreeSet<String>,
    fresh: SinkStatusSnapshot,
) -> SinkStatusSnapshot {
    let fresh = filter_sink_status_snapshot(fresh, allowed_groups);
    match cached {
        Some(cached) if cached.groups == *allowed_groups => {
            merge_sink_status_snapshots(vec![cached.snapshot.clone(), fresh])
        }
        _ => fresh,
    }
}

fn build_projection_maps(
    grants: &[GrantedMountRoot],
) -> (
    HashMap<String, String>,
    HashMap<String, String>,
    HashMap<String, String>,
) {
    let mut mount_point_by_object_ref = HashMap::new();
    let mut host_ip_by_object_ref = HashMap::new();
    let mut object_ref_by_host_ip = HashMap::new();
    let mut sorted = grants.to_vec();
    sorted.sort_by(|a, b| a.object_ref.cmp(&b.object_ref));
    for grant in &sorted {
        let object_ref = grant.object_ref.clone();
        let host_ip = grant.host_ip.clone();
        mount_point_by_object_ref
            .insert(object_ref.clone(), grant.mount_point.display().to_string());
        host_ip_by_object_ref.insert(object_ref.clone(), host_ip.clone());
        object_ref_by_host_ip.entry(host_ip).or_insert(object_ref);
    }
    (
        mount_point_by_object_ref,
        object_ref_by_host_ip,
        host_ip_by_object_ref,
    )
}

pub fn projection_policy_from_host_object_grants(grants: &[GrantedMountRoot]) -> ProjectionPolicy {
    let (mount_point_by_object_ref, object_ref_by_host_ip, host_ip_by_object_ref) =
        build_projection_maps(grants);
    ProjectionPolicy {
        member_grouping: MemberGroupingStrategy::MountPoint,
        mount_point_by_object_ref,
        object_ref_by_host_ip,
        host_ip_by_object_ref,
        query_timeout_ms: None,
        force_find_timeout_ms: None,
    }
}

pub fn refresh_policy_from_host_object_grants(
    policy: &Arc<RwLock<ProjectionPolicy>>,
    grants: &[GrantedMountRoot],
) {
    let (mount_point_by_object_ref, object_ref_by_host_ip, host_ip_by_object_ref) =
        build_projection_maps(grants);
    let mut guard = match policy.write() {
        Ok(guard) => guard,
        Err(poisoned) => {
            log::warn!("projection policy lock poisoned; recovering write guard");
            poisoned.into_inner()
        }
    };
    guard.mount_point_by_object_ref = mount_point_by_object_ref;
    guard.object_ref_by_host_ip = object_ref_by_host_ip;
    guard.host_ip_by_object_ref = host_ip_by_object_ref;
}

fn snapshot_policy(policy: &Arc<RwLock<ProjectionPolicy>>) -> ProjectionPolicy {
    match policy.read() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => {
            log::warn!("projection policy lock poisoned; recovering read guard");
            poisoned.into_inner().clone()
        }
    }
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn encode_pit_id(seed: &str) -> String {
    let raw = format!("{seed}:{}", unix_now_ms());
    B64.encode(raw)
}

impl QueryPitStore {
    fn cleanup_expired(&mut self, now_ms: u64) {
        let expired = self
            .sessions
            .iter()
            .filter_map(|(pit_id, session)| {
                (session.expires_at_ms <= now_ms).then(|| (pit_id.clone(), session.estimated_bytes))
            })
            .collect::<Vec<_>>();
        for (pit_id, bytes) in expired {
            self.sessions.remove(&pit_id);
            self.total_bytes = self.total_bytes.saturating_sub(bytes);
        }
    }

    fn insert(&mut self, pit_id: String, session: Arc<PitSession>) -> Result<(), CnxError> {
        self.cleanup_expired(unix_now_ms());
        if self.sessions.len() >= PIT_MAX_SESSIONS
            || self.total_bytes.saturating_add(session.estimated_bytes) > PIT_MAX_TOTAL_BYTES
        {
            return Err(CnxError::NotReady(
                "pit capacity exceeded; retry after existing queries expire".into(),
            ));
        }
        self.total_bytes = self.total_bytes.saturating_add(session.estimated_bytes);
        self.sessions.insert(pit_id, session);
        Ok(())
    }

    fn get(&mut self, pit_id: &str) -> Result<Arc<PitSession>, CnxError> {
        let now_ms = unix_now_ms();
        self.cleanup_expired(now_ms);
        let session = self
            .sessions
            .get(pit_id)
            .cloned()
            .ok_or_else(|| CnxError::InvalidInput(format!("pit expired: {pit_id}")))?;
        if session.expires_at_ms <= now_ms {
            let bytes = session.estimated_bytes;
            self.sessions.remove(pit_id);
            self.total_bytes = self.total_bytes.saturating_sub(bytes);
            return Err(CnxError::InvalidInput(format!("pit expired: {pit_id}")));
        }
        Ok(session)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NormalizedApiParams {
    path: Vec<u8>,
    group: Option<String>,
    recursive: bool,
    max_depth: Option<usize>,
    pit_id: Option<String>,
    group_order: GroupOrder,
    group_page_size: Option<usize>,
    group_after: Option<String>,
    entry_page_size: Option<usize>,
    entry_after: Option<String>,
    read_class: Option<ReadClass>,
}

fn decode_path_param(path: Option<String>, path_b64: Option<String>) -> Result<Vec<u8>, CnxError> {
    match (path, path_b64) {
        (Some(_), Some(_)) => Err(CnxError::InvalidInput(
            "path and path_b64 are mutually exclusive".into(),
        )),
        (Some(path), None) => Ok(path.into_bytes()),
        (None, Some(raw)) => B64URL
            .decode(raw)
            .map_err(|err| CnxError::InvalidInput(format!("invalid path_b64: {err}"))),
        (None, None) => Ok(b"/".to_vec()),
    }
}

fn normalize_api_params(params: ApiParams) -> Result<NormalizedApiParams, CnxError> {
    Ok(NormalizedApiParams {
        path: decode_path_param(params.path, params.path_b64)?,
        group: params.group.filter(|group| !group.trim().is_empty()),
        recursive: params.recursive.unwrap_or(true),
        max_depth: params.max_depth,
        pit_id: params.pit_id,
        group_order: params.group_order.unwrap_or(GroupOrder::GroupKey),
        group_page_size: params.group_page_size,
        group_after: params.group_after,
        entry_page_size: params.entry_page_size,
        entry_after: params.entry_after,
        read_class: params.read_class,
    })
}

fn tree_read_class(params: &NormalizedApiParams) -> ReadClass {
    params.read_class.unwrap_or(ReadClass::TrustedMaterialized)
}

fn stats_read_class(params: &NormalizedApiParams) -> ReadClass {
    params.read_class.unwrap_or(ReadClass::TrustedMaterialized)
}

fn validate_tree_query_params(params: &NormalizedApiParams) -> std::result::Result<(), CnxError> {
    if params.pit_id.is_none() && (params.group_after.is_some() || params.entry_after.is_some()) {
        return Err(CnxError::InvalidInput(
            "pit_id is required when using group_after or entry_after".into(),
        ));
    }
    Ok(())
}

fn validate_force_find_params(params: &NormalizedApiParams) -> std::result::Result<(), CnxError> {
    if params
        .read_class
        .is_some_and(|read_class| read_class != ReadClass::Fresh)
    {
        return Err(CnxError::InvalidInput(
            "read_class must be fresh on /on-demand-force-find".into(),
        ));
    }
    if params.pit_id.is_none() && (params.group_after.is_some() || params.entry_after.is_some()) {
        return Err(CnxError::InvalidInput(
            "pit_id is required when using group_after or entry_after".into(),
        ));
    }
    Ok(())
}

fn normalize_group_page_size(params: &NormalizedApiParams) -> Result<usize, CnxError> {
    let value = params.group_page_size.unwrap_or(GROUP_PAGE_SIZE_DEFAULT);
    if value == 0 || value > GROUP_PAGE_SIZE_MAX {
        return Err(CnxError::InvalidInput(format!(
            "group_page_size must be between 1 and {GROUP_PAGE_SIZE_MAX}"
        )));
    }
    Ok(value)
}

fn normalize_entry_page_size(params: &NormalizedApiParams) -> Result<Option<usize>, CnxError> {
    let value = params.entry_page_size.unwrap_or(ENTRY_PAGE_SIZE_DEFAULT);
    if value == 0 || value > ENTRY_PAGE_SIZE_MAX {
        return Err(CnxError::InvalidInput(format!(
            "entry_page_size must be between 1 and {ENTRY_PAGE_SIZE_MAX}"
        )));
    }
    Ok(Some(value))
}

fn encode_group_page_cursor(cursor: &GroupPageCursor) -> Result<String, CnxError> {
    let bytes = serde_json::to_vec(cursor)
        .map_err(|err| CnxError::Internal(format!("serialize group page cursor failed: {err}")))?;
    Ok(B64.encode(bytes))
}

fn decode_group_page_cursor(raw: &str) -> Result<GroupPageCursor, CnxError> {
    let bytes = B64
        .decode(raw)
        .map_err(|err| CnxError::InvalidInput(format!("invalid group_after cursor: {err}")))?;
    let cursor = serde_json::from_slice::<GroupPageCursor>(&bytes).map_err(|err| {
        CnxError::InvalidInput(format!("invalid group_after cursor payload: {err}"))
    })?;
    if cursor.version != 1 {
        return Err(CnxError::InvalidInput(format!(
            "unsupported group_after cursor version: {}",
            cursor.version
        )));
    }
    Ok(cursor)
}

fn encode_tree_entry_cursor(cursor: &TreeEntryCursor) -> Result<String, CnxError> {
    let bytes = serde_json::to_vec(cursor)
        .map_err(|err| CnxError::Internal(format!("serialize tree entry cursor failed: {err}")))?;
    Ok(B64.encode(bytes))
}

fn decode_tree_entry_cursor(raw: &str) -> Result<TreeEntryCursor, CnxError> {
    let bytes = B64
        .decode(raw)
        .map_err(|err| CnxError::InvalidInput(format!("invalid entry_after cursor: {err}")))?;
    let cursor = serde_json::from_slice::<TreeEntryCursor>(&bytes).map_err(|err| {
        CnxError::InvalidInput(format!("invalid entry_after cursor payload: {err}"))
    })?;
    if cursor.version != 1 {
        return Err(CnxError::InvalidInput(format!(
            "unsupported entry_after cursor version: {}",
            cursor.version
        )));
    }
    Ok(cursor)
}

fn encode_force_find_entry_cursor(cursor: &ForceFindEntryCursor) -> Result<String, CnxError> {
    let bytes = serde_json::to_vec(cursor).map_err(|err| {
        CnxError::Internal(format!("serialize force-find entry cursor failed: {err}"))
    })?;
    Ok(B64.encode(bytes))
}

fn decode_force_find_entry_cursor(raw: &str) -> Result<ForceFindEntryCursor, CnxError> {
    let bytes = B64
        .decode(raw)
        .map_err(|err| CnxError::InvalidInput(format!("invalid entry_after cursor: {err}")))?;
    let cursor = serde_json::from_slice::<ForceFindEntryCursor>(&bytes).map_err(|err| {
        CnxError::InvalidInput(format!("invalid entry_after cursor payload: {err}"))
    })?;
    if cursor.version != 1 {
        return Err(CnxError::InvalidInput(format!(
            "unsupported entry_after cursor version: {}",
            cursor.version
        )));
    }
    Ok(cursor)
}

fn decode_entry_cursor_bundle(raw: &str) -> Result<BTreeMap<String, String>, CnxError> {
    let bytes = B64
        .decode(raw)
        .map_err(|err| CnxError::InvalidInput(format!("invalid entry_after bundle: {err}")))?;
    serde_json::from_slice::<BTreeMap<String, String>>(&bytes)
        .map_err(|err| CnxError::InvalidInput(format!("invalid entry_after bundle payload: {err}")))
}

fn validate_tree_group_cursor(
    params: &NormalizedApiParams,
    cursor: &GroupPageCursor,
) -> Result<(), CnxError> {
    if cursor.mode != CursorQueryMode::Tree
        || params
            .pit_id
            .as_deref()
            .is_none_or(|pit_id| pit_id != cursor.pit_id)
    {
        return Err(CnxError::InvalidInput(
            "group_after cursor does not match the requested tree scope".into(),
        ));
    }
    Ok(())
}

fn validate_force_find_group_cursor(
    params: &NormalizedApiParams,
    cursor: &GroupPageCursor,
) -> Result<(), CnxError> {
    if cursor.mode != CursorQueryMode::ForceFind
        || params
            .pit_id
            .as_deref()
            .is_none_or(|pit_id| pit_id != cursor.pit_id)
    {
        return Err(CnxError::InvalidInput(
            "group_after cursor does not match the requested force-find scope".into(),
        ));
    }
    Ok(())
}

fn validate_tree_entry_cursor(
    params: &NormalizedApiParams,
    group: &str,
    cursor: &TreeEntryCursor,
) -> Result<(), CnxError> {
    if cursor.group != group
        || cursor.mode != CursorQueryMode::Tree
        || params
            .pit_id
            .as_deref()
            .is_none_or(|pit_id| pit_id != cursor.pit_id)
    {
        return Err(CnxError::InvalidInput(
            "entry_after cursor does not match the requested tree scope".into(),
        ));
    }
    Ok(())
}

fn validate_force_find_entry_cursor(
    params: &NormalizedApiParams,
    group: &str,
    cursor: &ForceFindEntryCursor,
) -> Result<(), CnxError> {
    if cursor.group != group
        || cursor.mode != CursorQueryMode::ForceFind
        || params
            .pit_id
            .as_deref()
            .is_none_or(|pit_id| pit_id != cursor.pit_id)
    {
        return Err(CnxError::InvalidInput(
            "entry_after cursor does not match the requested force-find scope".into(),
        ));
    }
    Ok(())
}

fn encode_entry_cursor_bundle(
    cursors: &BTreeMap<String, String>,
) -> Result<Option<String>, CnxError> {
    if cursors.is_empty() {
        return Ok(None);
    }
    let bytes = serde_json::to_vec(cursors)
        .map_err(|err| CnxError::Internal(format!("serialize entry_after bundle failed: {err}")))?;
    Ok(Some(B64.encode(bytes)))
}

fn group_page_start_index(cursor: Option<&GroupPageCursor>) -> usize {
    cursor.map(|cursor| cursor.next_group_offset).unwrap_or(0)
}

fn tree_metadata_json(read_class: ReadClass) -> (bool, serde_json::Value) {
    (
        true,
        serde_json::json!({
            "read_class": read_class,
            "metadata_available": true,
            "withheld_reason": serde_json::Value::Null,
        }),
    )
}

fn decode_materialized_stats_payload(bytes: &[u8]) -> Result<SubtreeStats, String> {
    match rmp_serde::from_slice::<MaterializedQueryPayload>(bytes) {
        Ok(MaterializedQueryPayload::Stats(stats)) => Ok(stats),
        Ok(MaterializedQueryPayload::Tree(_)) => {
            Err("unexpected tree payload for stats query".into())
        }
        Err(err) => Err(err.to_string()),
    }
}

fn decode_force_find_stats_payload(bytes: &[u8]) -> Result<SubtreeStats, String> {
    match rmp_serde::from_slice::<ForceFindQueryPayload>(bytes) {
        Ok(ForceFindQueryPayload::Stats(stats)) => Ok(stats),
        Ok(ForceFindQueryPayload::Tree(_)) => {
            Err("unexpected tree payload for force-find stats query".into())
        }
        Err(err) => Err(err.to_string()),
    }
}

fn build_materialized_stats_request(
    path: &[u8],
    recursive: bool,
    selected_group: Option<String>,
) -> InternalQueryRequest {
    InternalQueryRequest::materialized(
        QueryOp::Stats,
        QueryScope {
            path: path.to_vec(),
            recursive,
            max_depth: None,
            selected_group,
        },
        None,
    )
}

fn build_materialized_tree_request(
    path: &[u8],
    recursive: bool,
    max_depth: Option<usize>,
    read_class: ReadClass,
    selected_group: Option<String>,
) -> InternalQueryRequest {
    InternalQueryRequest::materialized(
        QueryOp::Tree,
        QueryScope {
            path: path.to_vec(),
            recursive,
            max_depth,
            selected_group,
        },
        Some(TreeQueryOptions { read_class }),
    )
}

fn build_force_find_stats_request(
    path: &[u8],
    recursive: bool,
    selected_group: Option<String>,
) -> InternalQueryRequest {
    InternalQueryRequest::force_find(
        QueryOp::Stats,
        QueryScope {
            path: path.to_vec(),
            recursive,
            max_depth: None,
            selected_group,
        },
    )
}

fn build_force_find_tree_request(
    path: &[u8],
    recursive: bool,
    max_depth: Option<usize>,
    selected_group: Option<String>,
) -> InternalQueryRequest {
    InternalQueryRequest::force_find(
        QueryOp::Tree,
        QueryScope {
            path: path.to_vec(),
            recursive,
            max_depth,
            selected_group,
        },
    )
}

pub fn create_local_router(
    sink: Arc<SinkFacade>,
    source: Arc<SourceFacade>,
    runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    origin_id: NodeId,
    policy: Arc<RwLock<ProjectionPolicy>>,
    force_find_inflight: Arc<Mutex<BTreeSet<String>>>,
) -> Router {
    eprintln!(
        "fs_meta_query_api: create router backend={}",
        if runtime_boundary.is_some() {
            "route"
        } else {
            "local"
        }
    );
    let state = ApiState {
        backend: runtime_boundary.map_or_else(
            || QueryBackend::Local {
                sink: sink.clone(),
                source: source.clone(),
            },
            |boundary| QueryBackend::Route {
                sink: sink.clone(),
                boundary,
                origin_id,
                source: source.clone(),
            },
        ),
        policy,
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight,
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
    };
    base_router(state)
}

fn base_router(state: ApiState) -> Router {
    Router::new()
        .route("/stats", get(get_stats))
        .route("/tree", get(get_tree))
        .route("/on-demand-force-find", get(get_force_find))
        .route("/bound-route-metrics", get(get_bound_route_metrics))
        .with_state(state)
}

pub(crate) async fn run_timed_query<Fut>(
    fut: Fut,
    timeout: Duration,
) -> Result<Vec<Event>, CnxError>
where
    Fut: std::future::Future<Output = Result<Vec<Event>, CnxError>>,
{
    tokio::time::timeout(timeout, fut)
        .await
        .map_err(|_| CnxError::Timeout)?
}

async fn query_materialized_events(
    backend: QueryBackend,
    params: InternalQueryRequest,
    timeout: Duration,
) -> Result<Vec<Event>, CnxError> {
    eprintln!(
        "fs_meta_query_api: query_materialized_events start selected_group={:?} recursive={} path={}",
        params.scope.selected_group,
        params.scope.recursive,
        String::from_utf8_lossy(&params.scope.path)
    );
    match backend {
        QueryBackend::Local { sink, .. } => {
            let result = run_timed_query(sink.materialized_query(&params), timeout).await;
            eprintln!(
                "fs_meta_query_api: query_materialized_events local done ok={}",
                result.is_ok()
            );
            result
        }
        QueryBackend::Route {
            boundary,
            origin_id,
            sink,
            source,
        } => {
            let selected_group_sink_status = if params.scope.selected_group.is_some() {
                route_sink_status_snapshot(
                    boundary.clone(),
                    origin_id.clone(),
                    std::cmp::min(timeout, Duration::from_secs(5)),
                )
                .await
                .ok()
            } else {
                None
            };
            if let Some(group_id) = params.scope.selected_group.as_deref()
                && let Some(node_id) = materialized_owner_node_for_group(
                    source.as_ref(),
                    selected_group_sink_status.as_ref(),
                    group_id,
                )
                .await?
            {
                if node_id == origin_id {
                    let result = run_timed_query(sink.materialized_query(&params), timeout).await;
                    eprintln!(
                        "fs_meta_query_api: query_materialized_events selected-group route owner_node={} local_owner=true ok={}",
                        node_id.0,
                        result.is_ok()
                    );
                    return result;
                }
                let result =
                    route_materialized_events_via_node(boundary.clone(), node_id.clone(), params.clone(), timeout)
                        .await;
                eprintln!(
                    "fs_meta_query_api: query_materialized_events selected-group route owner_node={} local_owner=false ok={}",
                    node_id.0,
                    result.is_ok()
                );
                return result;
            }
            let adapter = exchange_host_adapter(boundary, origin_id, default_route_bindings());
            let payload = rmp_serde::to_vec(&params).map_err(|err| {
                CnxError::Internal(format!("encode materialized query failed: {err}"))
            })?;
            eprintln!("fs_meta_query_api: query_materialized_events route call_collect begin");
            let events = adapter
                .call_collect(
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SINK_QUERY_PROXY,
                    Bytes::from(payload),
                    timeout,
                    MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE,
                )
                .await
            .map_err(|err| {
                eprintln!(
                    "fs_meta_query_api: query_materialized_events route call_collect failed err={}",
                    err
                );
                err
            })?;
            eprintln!(
                "fs_meta_query_api: query_materialized_events route call_collect done events={}",
                events.len()
            );
            eprintln!(
                "fs_meta_query_api: route materialized query returned events={} origins={:?}",
                events.len(),
                events
                    .iter()
                    .map(|event| event.metadata().origin_id.0.clone())
                    .collect::<Vec<_>>()
            );
            Ok(events)
        }
    }
}

async fn route_materialized_events_via_node(
    boundary: Arc<dyn ChannelIoSubset>,
    node_id: NodeId,
    params: InternalQueryRequest,
    timeout: Duration,
) -> Result<Vec<Event>, CnxError> {
    let owner_node = node_id.0.clone();
    let adapter = exchange_host_adapter(
        boundary,
        node_id,
        sink_query_route_bindings_for(&owner_node),
    );
    if debug_materialized_route_capture_enabled() {
        eprintln!(
            "fs_meta_query_api: materialized_route_capture begin owner_node={} selected_group={:?} recursive={} path={} timeout_ms={}",
            owner_node,
            params.scope.selected_group,
            params.scope.recursive,
            String::from_utf8_lossy(&params.scope.path),
            timeout.as_millis()
        );
    }
    let payload = rmp_serde::to_vec(&params)
        .map_err(|err| CnxError::Internal(format!("encode materialized query failed: {err}")))?;
    let result = adapter
        .call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SINK_QUERY,
            Bytes::from(payload),
            timeout,
            MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE,
        )
        .await;
    if debug_materialized_route_capture_enabled() {
        match &result {
            Ok(events) => eprintln!(
                "fs_meta_query_api: materialized_route_capture done owner_node={} selected_group={:?} events={} origins={:?}",
                owner_node,
                params.scope.selected_group,
                events.len(),
                summarize_event_counts_by_origin(events)
            ),
            Err(err) => eprintln!(
                "fs_meta_query_api: materialized_route_capture failed owner_node={} selected_group={:?} err={}",
                owner_node,
                params.scope.selected_group,
                err
            ),
        }
    }
    result
}

async fn query_force_find_events(
    backend: QueryBackend,
    params: InternalQueryRequest,
    timeout: Duration,
) -> Result<Vec<Event>, CnxError> {
    eprintln!(
        "fs_meta_query_api: query_force_find_events start selected_group={:?} recursive={} path={}",
        params.scope.selected_group,
        params.scope.recursive,
        String::from_utf8_lossy(&params.scope.path)
    );
    match backend {
        QueryBackend::Local { source, .. } => {
            let result = run_timed_query(source.force_find(&params), timeout).await;
            eprintln!(
                "fs_meta_query_api: query_force_find_events local done ok={}",
                result.is_ok()
            );
            result
        }
        QueryBackend::Route {
            boundary,
            origin_id,
            ..
        } => {
            let adapter = exchange_host_adapter(boundary, origin_id, default_route_bindings());
            let payload = rmp_serde::to_vec_named(&params).map_err(|err| {
                CnxError::Internal(format!("encode force-find query failed: {err}"))
            })?;
            eprintln!("fs_meta_query_api: query_force_find_events route call_collect begin");
            let events = adapter
                .call_collect(
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SOURCE_FIND,
                    Bytes::from(payload),
                    timeout,
                    FORCE_FIND_ROUTE_COLLECT_IDLE_GRACE,
                )
                .await
            .map_err(|err| {
                eprintln!(
                    "fs_meta_query_api: query_force_find_events route call_collect failed err={}",
                    err
                );
                err
            })?;
            eprintln!(
                "fs_meta_query_api: query_force_find_events route call_collect done events={} origins={:?}",
                events.len(),
                events
                    .iter()
                    .map(|event| event.metadata().origin_id.0.clone())
                    .collect::<Vec<_>>()
            );
            Ok(events)
        }
    }
}

async fn route_force_find_events_via_node(
    boundary: Arc<dyn ChannelIoSubset>,
    node_id: NodeId,
    params: InternalQueryRequest,
    timeout: Duration,
) -> Result<Vec<Event>, CnxError> {
    let adapter = exchange_host_adapter(boundary, node_id, default_route_bindings());
    let payload = rmp_serde::to_vec_named(&params)
        .map_err(|err| CnxError::Internal(format!("encode force-find query failed: {err}")))?;
    adapter
        .call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SOURCE_FIND,
            Bytes::from(payload),
            timeout,
            FORCE_FIND_ROUTE_COLLECT_IDLE_GRACE,
        )
        .await
}

#[cfg(test)]
fn query_responses_by_origin_from_source_events(
    events: &[Event],
    query_path: &[u8],
) -> Result<BTreeMap<String, TreeGroupPayload>, CnxError> {
    raw_query_results_by_origin_from_source_events(events, query_path).map(|grouped| {
        grouped
            .into_iter()
            .map(|(origin, response)| {
                let payload =
                    tree_group_payload_from_query_response(&response, query_path, true, None);
                (origin, payload)
            })
            .collect()
    })
}

#[cfg(test)]
fn max_total_files_from_stats_events(events: &[Event]) -> Option<u64> {
    let mut max_files = None::<u64>;
    for event in events {
        let Ok(stats) = decode_materialized_stats_payload(event.payload_bytes()) else {
            continue;
        };
        max_files = Some(max_files.map_or(stats.total_files, |v| v.max(stats.total_files)));
    }
    max_files
}

#[cfg(test)]
fn latest_file_mtime_from_stats_events(events: &[Event]) -> Option<u64> {
    let mut latest = None::<u64>;
    for event in events {
        let Ok(stats) = decode_materialized_stats_payload(event.payload_bytes()) else {
            continue;
        };
        let Some(group_latest) = stats.latest_file_mtime_us else {
            continue;
        };
        latest = Some(latest.map_or(group_latest, |v| v.max(group_latest)));
    }
    latest
}

fn event_group_key(policy: &ProjectionPolicy, event: &Event) -> String {
    policy.group_key_for_object_ref(&event.metadata().origin_id.0)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GroupRank {
    group_key: String,
    rank_metric: Option<u64>,
}

async fn collect_group_rankings(
    state: &ApiState,
    policy: &ProjectionPolicy,
    mode: QueryMode,
    path: &[u8],
    recursive: bool,
    timeout: Duration,
    group_order: GroupOrder,
    target_groups: &[String],
    explicit_group: Option<&str>,
) -> Result<Vec<GroupRank>, CnxError> {
    let mut ordered_groups = target_groups.to_vec();
    ordered_groups.sort();
    ordered_groups.dedup();
    if group_order == GroupOrder::GroupKey {
        return Ok(ordered_groups
            .into_iter()
            .map(|group_key| GroupRank {
                group_key,
                rank_metric: None,
            })
            .collect());
    }

    let mut candidates = Vec::<GroupRank>::with_capacity(ordered_groups.len());
    for group_key in ordered_groups {
        let stats_events = match mode {
            QueryMode::Find => {
                let request =
                    build_materialized_stats_request(path, recursive, Some(group_key.clone()));
                match query_materialized_events(state.backend.clone(), request, timeout).await {
                    Ok(events) => events,
                    Err(CnxError::Timeout)
                    | Err(CnxError::TransportClosed(_))
                    | Err(CnxError::ProtocolViolation(_)) => Vec::new(),
                    Err(err) => return Err(err),
                }
            }
            QueryMode::ForceFind => {
                query_force_find_group_stats(
                    state,
                    path,
                    recursive,
                    timeout,
                    &group_key,
                    explicit_group == Some(group_key.as_str()),
                )
                .await?
            }
        };
        let mut best_total_files = None::<u64>;
        let mut best_latest_mtime = None::<u64>;
        for event in &stats_events {
            if event_group_key(policy, event) != group_key {
                continue;
            }
            let stats = match mode {
                QueryMode::Find => decode_materialized_stats_payload(event.payload_bytes()),
                QueryMode::ForceFind => decode_force_find_stats_payload(event.payload_bytes()),
            };
            let Ok(stats) = stats else {
                continue;
            };
            best_total_files = Some(
                best_total_files.map_or(stats.total_files, |value| value.max(stats.total_files)),
            );
            if let Some(latest_mtime) = stats.latest_file_mtime_us {
                best_latest_mtime =
                    Some(best_latest_mtime.map_or(latest_mtime, |value| value.max(latest_mtime)));
            }
        }
        let rank_metric = match group_order {
            GroupOrder::GroupKey => None,
            GroupOrder::FileCount => Some(best_total_files.unwrap_or(0)),
            GroupOrder::FileAge => Some(best_latest_mtime.unwrap_or(0)),
        };
        if debug_group_ranking_capture_enabled() {
            eprintln!(
                "fs_meta_query_api: ranking_capture mode={:?} order={:?} group={} path={} events={} origins={:?} best_total_files={:?} best_latest_mtime={:?} rank_metric={:?}",
                mode,
                group_order,
                group_key,
                String::from_utf8_lossy(path),
                stats_events.len(),
                summarize_event_counts_by_origin(&stats_events),
                best_total_files,
                best_latest_mtime,
                rank_metric
            );
        }
        candidates.push(GroupRank {
            group_key,
            rank_metric,
        });
    }
    match group_order {
        GroupOrder::GroupKey => {
            candidates.sort_by(|a, b| a.group_key.cmp(&b.group_key));
        }
        GroupOrder::FileCount | GroupOrder::FileAge => {
            candidates.sort_by(|a, b| {
                b.rank_metric
                    .cmp(&a.rank_metric)
                    .then_with(|| a.group_key.cmp(&b.group_key))
            });
        }
    }
    if debug_group_ranking_capture_enabled() {
        eprintln!(
            "fs_meta_query_api: ranking_capture ordered mode={:?} order={:?} groups={:?}",
            mode,
            group_order,
            candidates
                .iter()
                .map(|candidate| format!("{}={:?}", candidate.group_key, candidate.rank_metric))
                .collect::<Vec<_>>()
        );
    }
    Ok(candidates)
}

fn materialized_scheduled_group_ids(snapshot: &SinkStatusSnapshot) -> BTreeSet<String> {
    let mut groups = snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<BTreeSet<_>>();
    if groups.is_empty() {
        groups.extend(snapshot.groups.iter().map(|group| group.group_id.clone()));
    }
    groups
}

async fn materialized_target_groups(
    state: &ApiState,
    selected_group: Option<&str>,
    timeout: Duration,
) -> Result<Vec<String>, CnxError> {
    let source = state
        .readiness_source
        .clone()
        .or_else(|| match &state.backend {
            QueryBackend::Local { source, .. } | QueryBackend::Route { source, .. } => {
                Some(source.clone())
            }
        });
    let sink = state
        .readiness_sink
        .clone()
        .or_else(|| match &state.backend {
            QueryBackend::Local { sink, .. } | QueryBackend::Route { sink, .. } => {
                Some(sink.clone())
            }
        });
    let mut groups = if let Some(group) = selected_group {
        vec![group.to_string()]
    } else {
        source
            .as_ref()
            .map(|source| {
                source.cached_logical_roots_snapshot().map(|roots| {
                    roots
                        .into_iter()
                        .filter(|root| root.scan)
                        .map(|root| root.id)
                        .collect::<Vec<_>>()
                })
            })
            .transpose()?
            .unwrap_or_default()
    };
    groups.sort();
    groups.dedup();
    let scheduled_groups = match (&state.backend, sink.as_ref()) {
        (
            QueryBackend::Route {
                boundary,
                origin_id,
                ..
            },
            _,
        ) => route_sink_status_snapshot(
            boundary.clone(),
            origin_id.clone(),
            std::cmp::min(timeout, Duration::from_secs(5)),
        )
        .await
        .ok()
        .map(|snapshot| materialized_scheduled_group_ids(&snapshot)),
        (QueryBackend::Local { .. }, Some(sink)) => sink
            .status_snapshot()
            .await
            .ok()
            .map(|snapshot| materialized_scheduled_group_ids(&snapshot)),
        (QueryBackend::Local { .. }, None) => None,
    };
    if let Some(scheduled_groups) = scheduled_groups {
        groups.retain(|group| scheduled_groups.contains(group));
    }
    Ok(groups)
}

fn node_id_from_object_ref(object_ref: &str) -> Option<NodeId> {
    object_ref
        .split_once("::")
        .map(|(node_id, _)| NodeId(node_id.to_string()))
}

fn scheduled_sink_owner_node_for_group(
    sink_status: &SinkStatusSnapshot,
    group_id: &str,
) -> Option<NodeId> {
    let mut nodes = sink_status
        .scheduled_groups_by_node
        .iter()
        .filter(|(_, groups)| groups.iter().any(|group| group == group_id))
        .map(|(node_id, _)| NodeId(node_id.clone()))
        .collect::<Vec<_>>();
    nodes.sort_by(|a, b| a.0.cmp(&b.0));
    nodes.dedup_by(|a, b| a.0 == b.0);
    nodes.into_iter().next()
}

async fn materialized_owner_node_for_group(
    source: &SourceFacade,
    sink_status: Option<&SinkStatusSnapshot>,
    group_id: &str,
) -> Result<Option<NodeId>, CnxError> {
    if let Some(sink_status) = sink_status
        && let Some(node_id) = scheduled_sink_owner_node_for_group(sink_status, group_id)
    {
        return Ok(Some(node_id));
    }
    Ok(source
        .source_primary_by_group_snapshot()
        .await?
        .get(group_id)
        .and_then(|object_ref| node_id_from_object_ref(object_ref)))
}

async fn force_find_candidate_object_refs_for_group(
    source: &SourceFacade,
    group_id: &str,
) -> Result<Vec<String>, CnxError> {
    let roots = source.logical_roots_snapshot().await?;
    let grants = source.host_object_grants_snapshot().await?;
    let Some(root) = roots.into_iter().find(|root| root.id == group_id) else {
        return Ok(Vec::new());
    };
    let mut candidates = grants
        .into_iter()
        .filter(|grant| root.selector.matches(grant))
        .map(|grant| grant.object_ref)
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.dedup();
    Ok(candidates)
}

async fn select_force_find_runner_node_for_group(
    state: &ApiState,
    source: &SourceFacade,
    group_id: &str,
) -> Result<Option<NodeId>, CnxError> {
    let candidates = force_find_candidate_object_refs_for_group(source, group_id).await?;
    if candidates.is_empty() {
        return materialized_owner_node_for_group(source, None, group_id).await;
    }
    let len = candidates.len();
    let idx = {
        let mut rr = state
            .force_find_route_rr
            .lock()
            .map_err(|_| CnxError::Internal("force-find route rr lock poisoned".into()))?;
        let idx = rr.get(group_id).copied().unwrap_or(0) % len;
        rr.insert(group_id.to_string(), (idx + 1) % len);
        idx
    };
    Ok(node_id_from_object_ref(&candidates[idx]))
}

fn acquire_force_find_group(
    state: &ApiState,
    group: &str,
) -> Result<ForceFindInflightGuard, CnxError> {
    acquire_force_find_groups(state, vec![group.to_string()])
}

async fn query_force_find_group_stats(
    state: &ApiState,
    path: &[u8],
    recursive: bool,
    timeout: Duration,
    group_id: &str,
    strict_conflict: bool,
) -> Result<Vec<Event>, CnxError> {
    let _guard = match acquire_force_find_group(state, group_id) {
        Ok(guard) => guard,
        Err(_err) if !strict_conflict => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    let request = build_force_find_stats_request(path, recursive, Some(group_id.to_string()));
    let events = match &state.backend {
        QueryBackend::Route {
            boundary, source, ..
        } => {
            if let Some(node_id) =
                select_force_find_runner_node_for_group(state, source.as_ref(), group_id).await?
            {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find stats via_node begin group={} node={} recursive={} path={}",
                        group_id,
                        node_id.0,
                        recursive,
                        String::from_utf8_lossy(path)
                    );
                }
                let result = run_timed_query(
                    route_force_find_events_via_node(
                        boundary.clone(),
                        node_id.clone(),
                        request.clone(),
                        timeout,
                    ),
                    timeout,
                )
                .await;
                match result {
                    Ok(events) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: force-find stats via_node done group={} node={} events={} origins={:?}",
                                group_id,
                                node_id.0,
                                events.len(),
                                summarize_event_counts_by_origin(&events)
                            );
                        }
                        events
                    }
                    Err(err) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: force-find stats via_node failed group={} node={} err={}",
                                group_id, node_id.0, err
                            );
                        }
                        return Err(err);
                    }
                }
            } else {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find stats route_fallback begin group={} recursive={} path={}",
                        group_id,
                        recursive,
                        String::from_utf8_lossy(path)
                    );
                }
                let result = query_force_find_events(state.backend.clone(), request, timeout).await;
                match result {
                    Ok(events) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: force-find stats route_fallback done group={} events={} origins={:?}",
                                group_id,
                                events.len(),
                                summarize_event_counts_by_origin(&events)
                            );
                        }
                        events
                    }
                    Err(err) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: force-find stats route_fallback failed group={} err={}",
                                group_id, err
                            );
                        }
                        return Err(err);
                    }
                }
            }
        }
        _ => {
            let result = query_force_find_events(state.backend.clone(), request, timeout).await;
            match result {
                Ok(events) => {
                    if debug_force_find_route_capture_enabled() {
                        eprintln!(
                            "fs_meta_query_api: force-find stats local done group={} events={} origins={:?}",
                            group_id,
                            events.len(),
                            summarize_event_counts_by_origin(&events)
                        );
                    }
                    events
                }
                Err(err) => {
                    if debug_force_find_route_capture_enabled() {
                        eprintln!(
                            "fs_meta_query_api: force-find stats local failed group={} err={}",
                            group_id, err
                        );
                    }
                    return Err(err);
                }
            }
        }
    };
    if !_guard.groups.is_empty() {
        tokio::time::sleep(FORCE_FIND_MIN_INFLIGHT_HOLD).await;
    }
    Ok(events)
}

async fn query_force_find_group_tree(
    state: &ApiState,
    path: &[u8],
    recursive: bool,
    max_depth: Option<usize>,
    timeout: Duration,
    group_id: &str,
    strict_conflict: bool,
) -> Result<Vec<Event>, CnxError> {
    let _guard = match acquire_force_find_group(state, group_id) {
        Ok(guard) => guard,
        Err(_err) if !strict_conflict => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    let request =
        build_force_find_tree_request(path, recursive, max_depth, Some(group_id.to_string()));
    let events = match &state.backend {
        QueryBackend::Route {
            boundary, source, ..
        } => {
            if let Some(node_id) =
                select_force_find_runner_node_for_group(state, source.as_ref(), group_id).await?
            {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find tree via_node begin group={} node={} recursive={} max_depth={:?} strict_conflict={} path={}",
                        group_id,
                        node_id.0,
                        recursive,
                        max_depth,
                        strict_conflict,
                        String::from_utf8_lossy(path)
                    );
                }
                let result = run_timed_query(
                    route_force_find_events_via_node(
                        boundary.clone(),
                        node_id.clone(),
                        request.clone(),
                        timeout,
                    ),
                    timeout,
                )
                .await;
                match result {
                    Ok(events) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: force-find tree via_node done group={} node={} events={} origins={:?}",
                                group_id,
                                node_id.0,
                                events.len(),
                                summarize_event_counts_by_origin(&events)
                            );
                        }
                        events
                    }
                    Err(err) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: force-find tree via_node failed group={} node={} err={}",
                                group_id, node_id.0, err
                            );
                        }
                        return Err(err);
                    }
                }
            } else {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find tree route_fallback begin group={} recursive={} max_depth={:?} strict_conflict={} path={}",
                        group_id,
                        recursive,
                        max_depth,
                        strict_conflict,
                        String::from_utf8_lossy(path)
                    );
                }
                let result = query_force_find_events(state.backend.clone(), request, timeout).await;
                match result {
                    Ok(events) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: force-find tree route_fallback done group={} events={} origins={:?}",
                                group_id,
                                events.len(),
                                summarize_event_counts_by_origin(&events)
                            );
                        }
                        events
                    }
                    Err(err) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: force-find tree route_fallback failed group={} err={}",
                                group_id, err
                            );
                        }
                        return Err(err);
                    }
                }
            }
        }
        _ => {
            let result = query_force_find_events(state.backend.clone(), request, timeout).await;
            match result {
                Ok(events) => {
                    if debug_force_find_route_capture_enabled() {
                        eprintln!(
                            "fs_meta_query_api: force-find tree local done group={} events={} origins={:?}",
                            group_id,
                            events.len(),
                            summarize_event_counts_by_origin(&events)
                        );
                    }
                    events
                }
                Err(err) => {
                    if debug_force_find_route_capture_enabled() {
                        eprintln!(
                            "fs_meta_query_api: force-find tree local failed group={} err={}",
                            group_id, err
                        );
                    }
                    return Err(err);
                }
            }
        }
    };
    if !_guard.groups.is_empty() {
        tokio::time::sleep(FORCE_FIND_MIN_INFLIGHT_HOLD).await;
    }
    Ok(events)
}

fn decode_materialized_selected_group_response(
    events: &[Event],
    policy: &ProjectionPolicy,
    selected_group: &str,
    query_path: &[u8],
) -> Result<TreeGroupPayload, CnxError> {
    fn payload_score(payload: &TreeGroupPayload) -> (u8, usize, bool, u64) {
        (
            u8::from(payload.root.exists),
            payload.entries.len(),
            payload.root.has_children,
            payload.root.modified_time_us,
        )
    }

    let mut last_decode_error = None::<String>;
    let mut best = None::<TreeGroupPayload>;
    for event in events {
        if event_group_key(policy, event) != selected_group {
            continue;
        }
        match rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()) {
            Ok(MaterializedQueryPayload::Tree(payload)) => {
                eprintln!(
                    "fs_meta_query_api: decode group={} root_exists={} entries={} has_children={} modified_time_us={}",
                    selected_group,
                    payload.root.exists,
                    payload.entries.len(),
                    payload.root.has_children,
                    payload.root.modified_time_us
                );
                let replace = best
                    .as_ref()
                    .is_none_or(|current| payload_score(&payload) > payload_score(current));
                if replace {
                    best = Some(payload);
                }
            }
            Ok(MaterializedQueryPayload::Stats(_)) => {
                last_decode_error = Some("unexpected stats payload for tree query".into());
            }
            Err(err) => last_decode_error = Some(err.to_string()),
        }
    }
    if let Some(payload) = best {
        return Ok(payload);
    }
    if last_decode_error.is_none() {
        return Ok(TreeGroupPayload {
            reliability: GroupReliability::from_reason(Some(
                crate::shared_types::query::UnreliableReason::Unattested,
            )),
            stability: TreeStability::not_evaluated(),
            root: TreePageRoot {
                path: query_path.to_vec(),
                size: 0,
                modified_time_us: 0,
                is_dir: true,
                exists: false,
                has_children: false,
            },
            entries: Vec::new(),
        });
    }
    Err(CnxError::Internal(last_decode_error.unwrap_or_else(|| {
        format!("tree query returned no payload for requested group '{selected_group}'")
    })))
}

fn empty_materialized_tree_group_payload(query_path: &[u8]) -> TreeGroupPayload {
    TreeGroupPayload {
        reliability: GroupReliability {
            reliable: true,
            unreliable_reason: None,
        },
        stability: TreeStability::not_evaluated(),
        root: TreePageRoot {
            path: query_path.to_vec(),
            size: 0,
            modified_time_us: 0,
            is_dir: true,
            exists: false,
            has_children: false,
        },
        entries: Vec::new(),
    }
}

fn tree_root_json(root: &TreePageRoot) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "path".into(),
        serde_json::json!(path_to_string_lossy(&root.path)),
    );
    maybe_insert_b64(&mut obj, "path_b64", &root.path);
    obj.insert("size".into(), serde_json::json!(root.size));
    obj.insert(
        "modified_time_us".into(),
        serde_json::json!(root.modified_time_us),
    );
    obj.insert("is_dir".into(), serde_json::json!(root.is_dir));
    obj.insert("exists".into(), serde_json::json!(root.exists));
    obj.insert("has_children".into(), serde_json::json!(root.has_children));
    serde_json::Value::Object(obj)
}

fn tree_entry_json(entry: &TreePageEntry) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "path".into(),
        serde_json::json!(path_to_string_lossy(&entry.path)),
    );
    maybe_insert_b64(&mut obj, "path_b64", &entry.path);
    obj.insert("depth".into(), serde_json::json!(entry.depth));
    obj.insert("size".into(), serde_json::json!(entry.size));
    obj.insert(
        "modified_time_us".into(),
        serde_json::json!(entry.modified_time_us),
    );
    obj.insert("is_dir".into(), serde_json::json!(entry.is_dir));
    obj.insert("has_children".into(), serde_json::json!(entry.has_children));
    serde_json::Value::Object(obj)
}

fn maybe_insert_b64(obj: &mut serde_json::Map<String, serde_json::Value>, key: &str, bytes: &[u8]) {
    if std::str::from_utf8(bytes).is_err() {
        obj.insert(key.into(), serde_json::json!(B64URL.encode(bytes)));
    }
}

fn scope_matches_tree(params: &NormalizedApiParams, scope: &TreePitScope) -> bool {
    params.path == scope.path
        && params.group == scope.group
        && params.recursive == scope.recursive
        && params.max_depth == scope.max_depth
        && params.group_order == scope.group_order
        && tree_read_class(params) == scope.read_class
}

fn scope_matches_force_find(params: &NormalizedApiParams, scope: &ForceFindPitScope) -> bool {
    params.path == scope.path
        && params.group == scope.group
        && params.recursive == scope.recursive
        && params.max_depth == scope.max_depth
        && params.group_order == scope.group_order
}

fn estimated_group_bytes(group: &GroupPitSnapshot) -> usize {
    let root_bytes = group.root.as_ref().map(|root| root.path.len()).unwrap_or(0);
    let entry_bytes = group
        .entries
        .iter()
        .map(|entry| entry.path.len() + std::mem::size_of::<TreePageEntry>())
        .sum::<usize>();
    group.group.len()
        + root_bytes
        + entry_bytes
        + group.errors.iter().map(|err| err.len()).sum::<usize>()
        + std::mem::size_of::<GroupPitSnapshot>()
}

fn build_group_page_cursor(
    mode: CursorQueryMode,
    pit_id: &str,
    next_group_offset: usize,
) -> Result<String, CnxError> {
    encode_group_page_cursor(&GroupPageCursor {
        version: 1,
        mode,
        pit_id: pit_id.to_string(),
        next_group_offset,
    })
}

fn build_tree_entry_cursor(
    pit_id: &str,
    group: &str,
    next_entry_offset: usize,
) -> Result<String, CnxError> {
    encode_tree_entry_cursor(&TreeEntryCursor {
        version: 1,
        mode: CursorQueryMode::Tree,
        pit_id: pit_id.to_string(),
        group: group.to_string(),
        next_entry_offset,
    })
}

fn build_force_find_entry_cursor(
    pit_id: &str,
    group: &str,
    next_entry_offset: usize,
) -> Result<String, CnxError> {
    encode_force_find_entry_cursor(&ForceFindEntryCursor {
        version: 1,
        mode: CursorQueryMode::ForceFind,
        pit_id: pit_id.to_string(),
        group: group.to_string(),
        next_entry_offset,
    })
}

fn render_pit_group(
    mode: CursorQueryMode,
    pit_id: &str,
    snapshot: &GroupPitSnapshot,
    entry_offset: usize,
    entry_page_size: usize,
    next_entry_cursors: &mut BTreeMap<String, String>,
) -> Result<serde_json::Value, CnxError> {
    let mut body = serde_json::Map::new();
    body.insert("group".into(), serde_json::json!(snapshot.group));
    body.insert("status".into(), serde_json::json!(snapshot.status));
    body.insert("reliable".into(), serde_json::json!(snapshot.reliable));
    body.insert(
        "unreliable_reason".into(),
        serde_json::to_value(&snapshot.unreliable_reason).unwrap_or(serde_json::Value::Null),
    );
    body.insert("stability".into(), stability_json(&snapshot.stability));
    body.insert(
        "meta".into(),
        serde_json::json!({
            "read_class": snapshot.meta.read_class,
            "metadata_available": snapshot.meta.metadata_available,
            "withheld_reason": snapshot.meta.withheld_reason,
        }),
    );
    if !snapshot.errors.is_empty() {
        body.insert("errors".into(), serde_json::json!(snapshot.errors));
    }
    if snapshot.meta.metadata_available {
        if entry_offset > snapshot.entries.len() {
            return Err(CnxError::InvalidInput(format!(
                "entry_after cursor exceeds group '{}' page bounds",
                snapshot.group
            )));
        }
        let end = snapshot
            .entries
            .len()
            .min(entry_offset.saturating_add(entry_page_size));
        let entries = snapshot.entries[entry_offset..end].to_vec();
        let has_more_entries = end < snapshot.entries.len();
        let next_cursor = if has_more_entries {
            let encoded = match mode {
                CursorQueryMode::Tree => build_tree_entry_cursor(pit_id, &snapshot.group, end)?,
                CursorQueryMode::ForceFind => {
                    build_force_find_entry_cursor(pit_id, &snapshot.group, end)?
                }
            };
            next_entry_cursors.insert(snapshot.group.clone(), encoded.clone());
            Some(encoded)
        } else {
            None
        };
        if let Some(root) = snapshot.root.as_ref() {
            body.insert("root".into(), tree_root_json(root));
        }
        body.insert(
            "entries".into(),
            serde_json::Value::Array(entries.iter().map(tree_entry_json).collect::<Vec<_>>()),
        );
        body.insert(
            "entry_page".into(),
            serde_json::json!({
                "order": PageOrder::PathLex,
                "page_size": entry_page_size,
                "returned_entries": entries.len(),
                "has_more_entries": has_more_entries,
                "next_cursor": next_cursor,
            }),
        );
    }
    Ok(serde_json::Value::Object(body))
}

fn render_pit_response(
    session: &PitSession,
    pit_id: &str,
    group_page_size: usize,
    group_offset: usize,
    entry_offsets: &BTreeMap<String, usize>,
    entry_page_size: usize,
) -> Result<serde_json::Value, CnxError> {
    if group_offset > session.groups.len() {
        return Err(CnxError::InvalidInput(
            "group_after cursor exceeds available group page bounds".into(),
        ));
    }
    let selected = session
        .groups
        .iter()
        .skip(group_offset)
        .take(group_page_size)
        .collect::<Vec<_>>();
    let has_more_groups = group_offset + selected.len() < session.groups.len();
    let next_group_cursor = if has_more_groups {
        Some(build_group_page_cursor(
            session.mode,
            pit_id,
            group_offset + selected.len(),
        )?)
    } else {
        None
    };
    let mut next_entry_cursors = BTreeMap::<String, String>::new();
    let groups = selected
        .into_iter()
        .map(|snapshot| {
            let offset = entry_offsets.get(&snapshot.group).copied().unwrap_or(0);
            render_pit_group(
                session.mode,
                pit_id,
                snapshot,
                offset,
                entry_page_size,
                &mut next_entry_cursors,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let path = match &session.scope {
        PitScope::Tree(scope) => scope.path.clone(),
        PitScope::ForceFind(scope) => scope.path.clone(),
    };
    let group_order = match &session.scope {
        PitScope::Tree(scope) => scope.group_order,
        PitScope::ForceFind(scope) => scope.group_order,
    };
    let mut body = serde_json::Map::new();
    body.insert(
        "path".into(),
        serde_json::json!(path_to_string_lossy(&path)),
    );
    maybe_insert_b64(&mut body, "path_b64", &path);
    body.insert("status".into(), serde_json::json!("ok"));
    body.insert("read_class".into(), serde_json::json!(session.read_class));
    body.insert(
        "observation_status".into(),
        observation_status_json(&session.observation_status),
    );
    body.insert("group_order".into(), serde_json::json!(group_order));
    body.insert(
        "pit".into(),
        serde_json::json!({
            "id": pit_id,
            "expires_at_ms": session.expires_at_ms,
        }),
    );
    body.insert("groups".into(), serde_json::json!(groups));
    body.insert(
        "group_page".into(),
        serde_json::json!({
            "returned_groups": groups.len(),
            "has_more_groups": has_more_groups,
            "next_cursor": next_group_cursor,
            "next_entry_after": encode_entry_cursor_bundle(&next_entry_cursors)?,
        }),
    );
    Ok(serde_json::Value::Object(body))
}

async fn build_tree_pit_session(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    timeout: Duration,
    observation_status: ObservationStatus,
) -> Result<PitSession, CnxError> {
    eprintln!(
        "fs_meta_query_api: build_tree_pit_session start path={} recursive={}",
        String::from_utf8_lossy(&params.path),
        params.recursive
    );
    let target_groups = materialized_target_groups(state, params.group.as_deref(), timeout).await?;
    let rankings = collect_group_rankings(
        state,
        policy,
        QueryMode::Find,
        &params.path,
        params.recursive,
        timeout,
        params.group_order,
        &target_groups,
        None,
    )
    .await?;
    eprintln!(
        "fs_meta_query_api: build_tree_pit_session rankings groups={}",
        rankings.len()
    );
    let mut groups = Vec::with_capacity(rankings.len());
    let read_class = tree_read_class(params);
    for rank in rankings {
        let tree_params = build_materialized_tree_request(
            &params.path,
            params.recursive,
            params.max_depth,
            read_class,
            Some(rank.group_key.clone()),
        );
        let events =
            match query_materialized_events(state.backend.clone(), tree_params, timeout).await {
                Ok(events) => events,
                Err(CnxError::Timeout)
                | Err(CnxError::TransportClosed(_))
                | Err(CnxError::ProtocolViolation(_)) => {
                    let response = empty_materialized_tree_group_payload(&params.path);
                    groups.push(GroupPitSnapshot {
                        group: rank.group_key,
                        status: "ok",
                        reliable: response.reliability.reliable,
                        unreliable_reason: response.reliability.unreliable_reason,
                        stability: response.stability,
                        meta: PitMetadata {
                            read_class,
                            metadata_available: true,
                            withheld_reason: None,
                        },
                        root: Some(response.root),
                        entries: response.entries,
                        errors: Vec::new(),
                    });
                    continue;
                }
                Err(err) => return Err(err),
            };
        match decode_materialized_selected_group_response(
            &events,
            policy,
            &rank.group_key,
            &params.path,
        ) {
            Ok(response) => {
                let (metadata_available, _meta_json) = tree_metadata_json(read_class);
                groups.push(GroupPitSnapshot {
                    group: rank.group_key,
                    status: "ok",
                    reliable: response.reliability.reliable,
                    unreliable_reason: response.reliability.unreliable_reason,
                    stability: response.stability,
                    meta: PitMetadata {
                        read_class,
                        metadata_available,
                        withheld_reason: None,
                    },
                    root: metadata_available.then_some(response.root),
                    entries: if metadata_available {
                        response.entries
                    } else {
                        Vec::new()
                    },
                    errors: Vec::new(),
                });
            }
            Err(err) => groups.push(GroupPitSnapshot {
                group: rank.group_key,
                status: "error",
                reliable: false,
                unreliable_reason: None,
                stability: TreeStability {
                    state: StabilityState::Unknown,
                    ..TreeStability::not_evaluated()
                },
                meta: PitMetadata {
                    read_class,
                    metadata_available: false,
                    withheld_reason: Some("group-payload-missing"),
                },
                root: None,
                entries: Vec::new(),
                errors: vec![err.to_string()],
            }),
        }
    }
    let estimated_bytes =
        groups.iter().map(estimated_group_bytes).sum::<usize>() + std::mem::size_of::<PitSession>();
    Ok(PitSession {
        mode: CursorQueryMode::Tree,
        scope: PitScope::Tree(TreePitScope {
            path: params.path.clone(),
            group: params.group.clone(),
            recursive: params.recursive,
            max_depth: params.max_depth,
            group_order: params.group_order,
            read_class,
        }),
        read_class,
        observation_status,
        groups,
        expires_at_ms: unix_now_ms() + PIT_TTL_MS_DEFAULT,
        estimated_bytes,
    })
}

async fn build_force_find_pit_session(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    timeout: Duration,
) -> Result<PitSession, CnxError> {
    let target_groups = resolve_force_find_groups(state, params).await?;
    let rankings = if params.group_order == GroupOrder::GroupKey {
        let mut ordered = target_groups.clone();
        ordered.sort();
        ordered
            .into_iter()
            .map(|group_key| GroupRank {
                group_key,
                rank_metric: None,
            })
            .collect::<Vec<_>>()
    } else {
        collect_group_rankings(
            state,
            policy,
            QueryMode::ForceFind,
            &params.path,
            params.recursive,
            timeout,
            params.group_order,
            &target_groups,
            params.group.as_deref(),
        )
        .await?
    };
    let mut groups = Vec::with_capacity(rankings.len());
    for rank in rankings {
        let strict_conflict = params.group.as_deref() == Some(rank.group_key.as_str());
        let events = match query_force_find_group_tree(
            state,
            &params.path,
            params.recursive,
            params.max_depth,
            timeout,
            &rank.group_key,
            strict_conflict,
        )
        .await
        {
            Ok(events) => {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find group_tree result group={} events={} origins={:?}",
                        rank.group_key,
                        events.len(),
                        summarize_event_counts_by_origin(&events)
                    );
                }
                events
            }
            Err(err) => {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find group_tree error group={} strict_conflict={} err={}",
                        rank.group_key, strict_conflict, err
                    );
                }
                if strict_conflict {
                    return Err(err);
                }
                groups.push(GroupPitSnapshot {
                    group: rank.group_key,
                    status: "error",
                    reliable: false,
                    unreliable_reason: None,
                    stability: TreeStability::not_evaluated(),
                    meta: PitMetadata {
                        read_class: ReadClass::Fresh,
                        metadata_available: false,
                        withheld_reason: Some("group-payload-missing"),
                    },
                    root: None,
                    entries: Vec::new(),
                    errors: vec![err.to_string()],
                });
                continue;
            }
        };
        match decode_force_find_selected_group_response(
            &events,
            policy,
            &rank.group_key,
            &params.path,
        ) {
            Ok(payload) => {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find group_decode ok group={} root_exists={} has_children={} entries={} reliable={} errors=0",
                        rank.group_key,
                        payload.root.exists,
                        payload.root.has_children,
                        payload.entries.len(),
                        payload.reliability.reliable
                    );
                }
                groups.push(GroupPitSnapshot {
                    group: rank.group_key,
                    status: "ok",
                    reliable: payload.reliability.reliable,
                    unreliable_reason: payload.reliability.unreliable_reason,
                    stability: payload.stability,
                    meta: PitMetadata {
                        read_class: ReadClass::Fresh,
                        metadata_available: true,
                        withheld_reason: None,
                    },
                    root: Some(payload.root),
                    entries: payload.entries,
                    errors: Vec::new(),
                });
            }
            Err(err) => {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find group_decode failed group={} strict_conflict={} err={} events={} origins={:?}",
                        rank.group_key,
                        strict_conflict,
                        err,
                        events.len(),
                        summarize_event_counts_by_origin(&events)
                    );
                }
                if strict_conflict {
                    return Err(err);
                }
                groups.push(GroupPitSnapshot {
                    group: rank.group_key,
                    status: "error",
                    reliable: false,
                    unreliable_reason: None,
                    stability: TreeStability::not_evaluated(),
                    meta: PitMetadata {
                        read_class: ReadClass::Fresh,
                        metadata_available: false,
                        withheld_reason: Some("group-payload-missing"),
                    },
                    root: None,
                    entries: Vec::new(),
                    errors: vec![err.to_string()],
                });
            }
        }
    }
    let estimated_bytes =
        groups.iter().map(estimated_group_bytes).sum::<usize>() + std::mem::size_of::<PitSession>();
    Ok(PitSession {
        mode: CursorQueryMode::ForceFind,
        scope: PitScope::ForceFind(ForceFindPitScope {
            path: params.path.clone(),
            group: params.group.clone(),
            recursive: params.recursive,
            max_depth: params.max_depth,
            group_order: params.group_order,
        }),
        read_class: ReadClass::Fresh,
        observation_status: ObservationStatus::fresh_only(),
        groups,
        expires_at_ms: unix_now_ms() + PIT_TTL_MS_DEFAULT,
        estimated_bytes,
    })
}

async fn query_tree_page_response(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    timeout: Duration,
    observation_status: ObservationStatus,
) -> Result<serde_json::Value, CnxError> {
    let group_page_size = normalize_group_page_size(params)?;
    let entry_page_size = normalize_entry_page_size(params)?.unwrap_or(0);
    let entry_bundle = params
        .entry_after
        .as_deref()
        .map(decode_entry_cursor_bundle)
        .transpose()?
        .unwrap_or_default();
    let mut entry_offsets = BTreeMap::<String, usize>::new();
    for (group, raw) in entry_bundle {
        let cursor = decode_tree_entry_cursor(&raw)?;
        validate_tree_entry_cursor(params, &group, &cursor)?;
        entry_offsets.insert(group, cursor.next_entry_offset);
    }
    let group_cursor = params
        .group_after
        .as_deref()
        .map(decode_group_page_cursor)
        .transpose()?;
    if let Some(cursor) = group_cursor.as_ref() {
        validate_tree_group_cursor(params, cursor)?;
    }

    if let Some(pit_id) = params.pit_id.as_deref() {
        let session = {
            let mut guard = state
                .pit_store
                .lock()
                .map_err(|_| CnxError::Internal("pit store lock poisoned".into()))?;
            guard.get(pit_id)?
        };
        let PitScope::Tree(scope) = &session.scope else {
            return Err(CnxError::InvalidInput(
                "pit_id does not reference a tree query session".into(),
            ));
        };
        if !scope_matches_tree(params, scope) {
            return Err(CnxError::InvalidInput(
                "pit_id does not match the requested tree scope".into(),
            ));
        }
        return render_pit_response(
            &session,
            pit_id,
            group_page_size,
            group_page_start_index(group_cursor.as_ref()),
            &entry_offsets,
            entry_page_size,
        );
    }

    let session =
        Arc::new(build_tree_pit_session(state, policy, params, timeout, observation_status).await?);
    let pit_id = encode_pit_id("tree");
    {
        let mut guard = state
            .pit_store
            .lock()
            .map_err(|_| CnxError::Internal("pit store lock poisoned".into()))?;
        guard.insert(pit_id.clone(), session.clone())?;
    }
    render_pit_response(
        &session,
        &pit_id,
        group_page_size,
        0,
        &entry_offsets,
        entry_page_size,
    )
}

fn decode_force_find_selected_group_response(
    events: &[Event],
    policy: &ProjectionPolicy,
    selected_group: &str,
    query_path: &[u8],
) -> Result<TreeGroupPayload, CnxError> {
    let mut successes = Vec::<TreeGroupPayload>::new();
    let mut errors = Vec::<String>::new();
    for event in events {
        if event_group_key(policy, event) != selected_group {
            continue;
        }
        match rmp_serde::from_slice::<ForceFindQueryPayload>(event.payload_bytes()) {
            Ok(ForceFindQueryPayload::Tree(response)) => successes.push(response),
            Ok(ForceFindQueryPayload::Stats(_)) => {
                errors.push("unexpected stats payload for force-find tree query".into());
            }
            Err(_) => {
                let decode_msg = String::from_utf8(event.payload_bytes().to_vec())
                    .ok()
                    .filter(|msg| !msg.trim().is_empty())
                    .unwrap_or_else(|| "failed to decode force-find response".to_string());
                errors.push(decode_msg);
            }
        }
    }
    if successes.is_empty() {
        if errors.is_empty() {
            return Ok(TreeGroupPayload {
                reliability: GroupReliability::from_reason(None),
                stability: TreeStability::not_evaluated(),
                root: TreePageRoot {
                    path: query_path.to_vec(),
                    size: 0,
                    modified_time_us: 0,
                    is_dir: true,
                    exists: false,
                    has_children: false,
                },
                entries: Vec::new(),
            });
        }
        let detail = errors
            .first()
            .cloned()
            .unwrap_or_else(|| "force-find returned no payload for requested group".to_string());
        return Err(CnxError::Internal(detail));
    }
    Ok(successes
        .into_iter()
        .next()
        .expect("successes must contain one payload when non-empty"))
}

async fn query_force_find_page_response(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    timeout: Duration,
) -> Result<serde_json::Value, CnxError> {
    let group_page_size = normalize_group_page_size(params)?;
    let entry_page_size = normalize_entry_page_size(params)?.unwrap_or(ENTRY_PAGE_SIZE_DEFAULT);
    let entry_bundle = params
        .entry_after
        .as_deref()
        .map(decode_entry_cursor_bundle)
        .transpose()?
        .unwrap_or_default();
    let mut entry_offsets = BTreeMap::<String, usize>::new();
    for (group, raw) in entry_bundle {
        let cursor = decode_force_find_entry_cursor(&raw)?;
        validate_force_find_entry_cursor(params, &group, &cursor)?;
        entry_offsets.insert(group, cursor.next_entry_offset);
    }
    let group_cursor = params
        .group_after
        .as_deref()
        .map(decode_group_page_cursor)
        .transpose()?;
    if let Some(cursor) = group_cursor.as_ref() {
        validate_force_find_group_cursor(params, cursor)?;
    }

    if let Some(pit_id) = params.pit_id.as_deref() {
        let session = {
            let mut guard = state
                .pit_store
                .lock()
                .map_err(|_| CnxError::Internal("pit store lock poisoned".into()))?;
            guard.get(pit_id)?
        };
        let PitScope::ForceFind(scope) = &session.scope else {
            return Err(CnxError::InvalidInput(
                "pit_id does not reference a force-find query session".into(),
            ));
        };
        if !scope_matches_force_find(params, scope) {
            return Err(CnxError::InvalidInput(
                "pit_id does not match the requested force-find scope".into(),
            ));
        }
        return render_pit_response(
            &session,
            pit_id,
            group_page_size,
            group_page_start_index(group_cursor.as_ref()),
            &entry_offsets,
            entry_page_size,
        );
    }

    let session = Arc::new(build_force_find_pit_session(state, policy, params, timeout).await?);
    let pit_id = encode_pit_id("force-find");
    {
        let mut guard = state
            .pit_store
            .lock()
            .map_err(|_| CnxError::Internal("pit store lock poisoned".into()))?;
        guard.insert(pit_id.clone(), session.clone())?;
    }
    render_pit_response(
        &session,
        &pit_id,
        group_page_size,
        0,
        &entry_offsets,
        entry_page_size,
    )
}

fn error_status(err: &CnxError) -> StatusCode {
    if matches!(err, CnxError::InvalidInput(msg) if msg.starts_with("pit expired:")) {
        return StatusCode::GONE;
    }
    if matches!(err, CnxError::NotReady(msg) if msg.starts_with("pit capacity exceeded")) {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if matches!(err, CnxError::NotReady(msg) if msg.starts_with(FORCE_FIND_INFLIGHT_CONFLICT_PREFIX))
    {
        return StatusCode::TOO_MANY_REQUESTS;
    }
    match err {
        CnxError::InvalidInput(_) => StatusCode::BAD_REQUEST,
        CnxError::ProtocolViolation(_) => StatusCode::BAD_GATEWAY,
        CnxError::Timeout => StatusCode::GATEWAY_TIMEOUT,
        CnxError::TransportClosed(_) => StatusCode::SERVICE_UNAVAILABLE,
        CnxError::PeerError(_) => StatusCode::BAD_GATEWAY,
        CnxError::NotReady(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn error_code(err: &CnxError) -> &'static str {
    if matches!(err, CnxError::InvalidInput(msg) if msg.starts_with("pit expired:")) {
        return "PIT_EXPIRED";
    }
    if matches!(err, CnxError::NotReady(msg) if msg.starts_with("pit capacity exceeded")) {
        return "PIT_CAPACITY_EXCEEDED";
    }
    if matches!(err, CnxError::NotReady(msg) if msg.starts_with(FORCE_FIND_INFLIGHT_CONFLICT_PREFIX))
    {
        return "FORCE_FIND_INFLIGHT_CONFLICT";
    }
    match err {
        CnxError::InvalidInput(_) => "INVALID_INPUT",
        CnxError::ProtocolViolation(_) => "PROTOCOL_VIOLATION",
        CnxError::Timeout => "TIMEOUT",
        CnxError::TransportClosed(_) => "TRANSPORT_CLOSED",
        CnxError::PeerError(_) => "PEER_ERROR",
        CnxError::NotReady(_) => "NOT_READY",
        _ => "INTERNAL_ERROR",
    }
}

#[cfg(test)]
fn error_response(err: CnxError) -> Response {
    error_response_with_context(err, None)
}

fn error_response_with_context(err: CnxError, path: Option<&[u8]>) -> Response {
    let status = error_status(&err);
    let mut body = serde_json::Map::new();
    body.insert("error".into(), serde_json::json!(err.to_string()));
    body.insert("code".into(), serde_json::json!(error_code(&err)));
    body.insert(
        "path".into(),
        serde_json::json!(path.map(path_to_string_lossy)),
    );
    if let Some(path) = path {
        maybe_insert_b64(&mut body, "path_b64", path);
    }
    let body = serde_json::Value::Object(body);
    (status, Json(body)).into_response()
}

fn decode_stats_groups(
    events: Vec<Event>,
    policy: &ProjectionPolicy,
    selected_group: Option<&str>,
    read_class: ReadClass,
) -> serde_json::Map<String, serde_json::Value> {
    #[derive(Default)]
    struct GroupStatsAccumulator {
        ok: Option<SubtreeStats>,
        errors: Vec<String>,
    }

    let mut grouped = BTreeMap::<String, GroupStatsAccumulator>::new();
    for event in events {
        let group_key = event_group_key(policy, &event);
        if selected_group.is_some_and(|group| group != group_key) {
            continue;
        }
        let acc = grouped.entry(group_key).or_default();
        let stats = match read_class {
            ReadClass::Fresh => decode_force_find_stats_payload(event.payload_bytes()),
            ReadClass::Materialized | ReadClass::TrustedMaterialized => {
                decode_materialized_stats_payload(event.payload_bytes())
            }
        };
        match stats {
            Ok(stats) => {
                let current = acc.ok.get_or_insert_with(SubtreeStats::default);
                current.total_nodes += stats.total_nodes;
                current.total_files += stats.total_files;
                current.total_dirs += stats.total_dirs;
                current.total_size += stats.total_size;
                current.attested_count += stats.attested_count;
                current.blind_spot_count += stats.blind_spot_count;
                if let Some(latest) = stats.latest_file_mtime_us {
                    current.latest_file_mtime_us = Some(
                        current
                            .latest_file_mtime_us
                            .map_or(latest, |existing| existing.max(latest)),
                    );
                }
            }
            Err(_) => {
                let msg = String::from_utf8(event.payload_bytes().to_vec())
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| "failed to decode subtree stats".to_string());
                acc.errors.push(msg);
            }
        }
    }

    let mut out = serde_json::Map::<String, serde_json::Value>::new();
    for (group_key, acc) in grouped {
        if let Some(stats) = acc.ok {
            out.insert(
                group_key,
                serde_json::json!({
                    "status": "ok",
                    "data": {
                        "total_nodes": stats.total_nodes,
                        "total_files": stats.total_files,
                        "total_dirs": stats.total_dirs,
                        "total_size": stats.total_size,
                        "latest_file_mtime_us": stats.latest_file_mtime_us,
                        "attested_count": stats.attested_count,
                        "blind_spot_count": stats.blind_spot_count,
                    },
                    "partial_failure": !acc.errors.is_empty(),
                    "errors": acc.errors,
                }),
            );
        } else {
            out.insert(
                group_key,
                serde_json::json!({
                    "status": "error",
                    "message": "all group stats failed",
                    "errors": acc.errors,
                }),
            );
        }
    }
    out
}

fn zero_stats_group_json() -> serde_json::Value {
    serde_json::json!({
        "status": "ok",
        "data": {
            "total_nodes": 0u64,
            "total_files": 0u64,
            "total_dirs": 0u64,
            "total_size": 0u64,
            "latest_file_mtime_us": serde_json::Value::Null,
            "attested_count": 0u64,
            "blind_spot_count": 0u64,
        },
        "partial_failure": false,
        "errors": [],
    })
}

async fn collect_materialized_stats_groups(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    read_class: ReadClass,
) -> Result<serde_json::Map<String, serde_json::Value>, CnxError> {
    let mut groups = serde_json::Map::<String, serde_json::Value>::new();
    let target_groups = match read_class {
        ReadClass::Fresh => resolve_force_find_groups(state, params).await?,
        ReadClass::Materialized | ReadClass::TrustedMaterialized => {
            materialized_target_groups(state, params.group.as_deref(), policy.query_timeout())
                .await?
        }
    };
    for group_id in target_groups {
        let events = match read_class {
            ReadClass::Fresh => {
                let strict_conflict = params.group.as_deref() == Some(group_id.as_str());
                query_force_find_group_stats(
                    state,
                    &params.path,
                    params.recursive,
                    policy.force_find_timeout(),
                    &group_id,
                    strict_conflict,
                )
                .await?
            }
            ReadClass::Materialized | ReadClass::TrustedMaterialized => {
                let request = build_materialized_stats_request(
                    &params.path,
                    params.recursive,
                    Some(group_id.clone()),
                );
                query_materialized_events(state.backend.clone(), request, policy.query_timeout())
                    .await?
            }
        };
        let targeted = decode_stats_groups(events, policy, Some(&group_id), read_class);
        groups.insert(
            group_id.clone(),
            targeted
                .get(&group_id)
                .cloned()
                .unwrap_or_else(zero_stats_group_json),
        );
    }

    Ok(groups)
}

async fn get_stats(
    State(state): State<ApiState>,
    Query(params): Query<ApiParams>,
) -> impl IntoResponse {
    let policy = snapshot_policy(&state.policy);
    let params = match normalize_api_params(params) {
        Ok(params) => params,
        Err(err) => return error_response_with_context(err, None),
    };
    let read_class = stats_read_class(&params);
    let observation_status = match read_class {
        ReadClass::Fresh => ObservationStatus::fresh_only(),
        ReadClass::Materialized | ReadClass::TrustedMaterialized => {
            let (source_status, sink_status) =
                match load_materialized_status_snapshots(&state).await {
                    Ok(statuses) => statuses,
                    Err(err) => return error_response_with_context(err, Some(&params.path)),
                };
            let status = materialized_observation_status(&source_status, &sink_status);
            if read_class == ReadClass::TrustedMaterialized
                && status.state != ObservationState::TrustedMaterialized
            {
                return error_response_with_context(
                    CnxError::NotReady(trusted_materialized_not_ready_message(&status)),
                    Some(&params.path),
                );
            }
            status
        }
    };
    let groups = match collect_materialized_stats_groups(&state, &policy, &params, read_class).await
    {
        Ok(groups) => groups,
        Err(err) => return error_response_with_context(err, Some(&params.path)),
    };

    let mut body = serde_json::Map::new();
    body.insert(
        "path".into(),
        serde_json::json!(path_to_string_lossy(&params.path)),
    );
    maybe_insert_b64(&mut body, "path_b64", &params.path);
    body.insert("read_class".into(), serde_json::json!(read_class));
    body.insert(
        "observation_status".into(),
        observation_status_json(&observation_status),
    );
    body.insert("groups".into(), serde_json::json!(groups));
    Json(serde_json::Value::Object(body)).into_response()
}

async fn get_tree(
    State(state): State<ApiState>,
    Query(params): Query<ApiParams>,
) -> impl IntoResponse {
    let policy = snapshot_policy(&state.policy);
    let params = match normalize_api_params(params) {
        Ok(params) => params,
        Err(err) => return error_response_with_context(err, None),
    };
    let path_for_error = params.path.clone();
    if let Err(err) = validate_tree_query_params(&params) {
        return error_response_with_context(err, Some(&path_for_error));
    }
    let read_class = tree_read_class(&params);
    if read_class == ReadClass::Fresh {
        return match query_force_find_page_response(
            &state,
            &policy,
            &params,
            policy.force_find_timeout(),
        )
        .await
        {
            Ok(resp) => Json(resp).into_response(),
            Err(err) => error_response_with_context(err, Some(&path_for_error)),
        };
    }
    let (source_status, sink_status) = match load_materialized_status_snapshots(&state).await {
        Ok(statuses) => statuses,
        Err(err) => return error_response_with_context(err, Some(&path_for_error)),
    };
    let observation_status = materialized_observation_status(&source_status, &sink_status);
    if read_class == ReadClass::TrustedMaterialized
        && observation_status.state != ObservationState::TrustedMaterialized
    {
        return error_response_with_context(
            CnxError::NotReady(trusted_materialized_not_ready_message(&observation_status)),
            Some(&path_for_error),
        );
    }
    match query_tree_page_response(
        &state,
        &policy,
        &params,
        policy.query_timeout(),
        observation_status,
    )
    .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => error_response_with_context(err, Some(&path_for_error)),
    }
}

async fn get_force_find(
    State(state): State<ApiState>,
    Query(params): Query<ApiParams>,
) -> impl IntoResponse {
    let policy = snapshot_policy(&state.policy);
    let params = match normalize_api_params(params) {
        Ok(params) => params,
        Err(err) => return error_response_with_context(err, None),
    };
    let path_for_error = params.path.clone();
    if let Err(err) = validate_force_find_params(&params) {
        return error_response_with_context(err, Some(&path_for_error));
    }
    let response =
        query_force_find_page_response(&state, &policy, &params, policy.force_find_timeout()).await;
    match response {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => error_response_with_context(err, Some(&path_for_error)),
    }
}

async fn resolve_force_find_groups(
    state: &ApiState,
    params: &NormalizedApiParams,
) -> Result<Vec<String>, CnxError> {
    if let Some(group) = params.group.as_ref() {
        return Ok(vec![group.clone()]);
    }

    async fn fallback_force_find_groups(
        source: &Arc<SourceFacade>,
    ) -> Result<Vec<String>, CnxError> {
        let scan_enabled_groups = source
            .cached_logical_roots_snapshot()?
            .into_iter()
            .filter(|root| root.scan)
            .map(|root| root.id)
            .collect::<BTreeSet<_>>();
        if let Ok(snapshot) = source.observability_snapshot().await {
            let mut groups = snapshot
                .source_primary_by_group
                .into_keys()
                .filter(|group_id| scan_enabled_groups.contains(group_id))
                .collect::<Vec<_>>();
            if !groups.is_empty() {
                groups.sort();
                groups.dedup();
                return Ok(groups);
            }
        }
        let mut groups = scan_enabled_groups.into_iter().collect::<Vec<_>>();
        groups.sort();
        groups.dedup();
        Ok(groups)
    }

    match &state.backend {
        QueryBackend::Local { source, .. } | QueryBackend::Route { source, .. } => {
            fallback_force_find_groups(source).await
        }
    }
}

fn acquire_force_find_groups(
    state: &ApiState,
    groups: Vec<String>,
) -> Result<ForceFindInflightGuard, CnxError> {
    let mut inflight = state
        .force_find_inflight
        .lock()
        .map_err(|_| CnxError::Internal("force-find inflight lock poisoned".into()))?;
    if let Some(group) = groups.iter().find(|group| inflight.contains(*group)) {
        eprintln!(
            "fs_meta_query_api: force-find inflight conflict requested={:?} active={:?}",
            groups, inflight
        );
        return Err(CnxError::NotReady(format!(
            "{FORCE_FIND_INFLIGHT_CONFLICT_PREFIX} force-find already running for group: {group}"
        )));
    }
    for group in &groups {
        inflight.insert(group.clone());
    }
    eprintln!(
        "fs_meta_query_api: force-find inflight acquire groups={:?} active={:?}",
        groups, inflight
    );
    drop(inflight);
    Ok(ForceFindInflightGuard {
        inflight: state.force_find_inflight.clone(),
        groups,
    })
}

async fn get_bound_route_metrics() -> impl IntoResponse {
    let m = bound_route_metrics_snapshot();
    Json(serde_json::json!({
        "call_timeout_total": m.call_timeout_total,
        "correlation_mismatch_total": m.correlation_mismatch_total,
        "uncorrelated_reply_total": m.uncorrelated_reply_total,
        "recv_loop_iterations": m.recv_loop_iterations,
        "pending_calls": m.pending_calls,
    }))
    .into_response()
}

fn path_to_string_lossy(path: &[u8]) -> String {
    bytes_to_display_string(path)
}

fn observation_status_json(status: &ObservationStatus) -> serde_json::Value {
    serde_json::json!({
        "state": status.state,
        "reasons": status.reasons,
    })
}

fn stability_json(stability: &TreeStability) -> serde_json::Value {
    serde_json::json!({
        "mode": stability.mode,
        "state": stability.state,
        "quiet_window_ms": stability.quiet_window_ms,
        "observed_quiet_for_ms": stability.observed_quiet_for_ms,
        "remaining_ms": stability.remaining_ms,
        "blocked_reasons": stability.blocked_reasons,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileMetaRecord;
    use crate::runtime::endpoint::ManagedEndpointTask;
    use crate::runtime::routes::{
        METHOD_SINK_QUERY, METHOD_SOURCE_FIND, ROUTE_TOKEN_FS_META_INTERNAL,
        default_route_bindings, sink_query_request_route_for,
    };
    use crate::sink::SinkFileMeta;
    use crate::source::FSMetaSource;
    use crate::source::config::{GrantedMountRoot, RootSpec, SourceConfig};
    use crate::workers::sink::{SinkFacade, SinkWorkerClientHandle};
    use crate::workers::source::SourceFacade;
    use async_trait::async_trait;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use bytes::Bytes;
    use capanix_app_sdk::runtime::{
        EventMetadata, LogLevel, NodeId, RuntimeWorkerBinding, RuntimeWorkerLauncherKind,
        in_memory_state_boundary,
    };
    use capanix_app_sdk::worker::WorkerMode;
    use capanix_host_fs_types::UnixStat;
    use capanix_runtime_entry_sdk::advanced::boundary::{
        BoundaryContext, ChannelBoundary, ChannelIoSubset, ChannelKey, ChannelRecvRequest,
        ChannelSendRequest, StateBoundary,
    };
    use capanix_runtime_entry_sdk::worker_runtime::RuntimeWorkerClientFactory;
    use futures_util::StreamExt;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::OnceLock;
    use tempfile::{TempDir, tempdir};
    use tokio::sync::{Mutex as AsyncMutex, Notify};
    use tokio_util::sync::CancellationToken;
    use tower::util::ServiceExt;

    #[derive(Clone, Copy)]
    enum ForceFindFixtureScenario {
        Standard,
        FileAgeNoFiles,
    }

    struct ForceFindFixture {
        _tempdir: TempDir,
        app: Router,
    }

    #[derive(Default)]
    struct LoopbackRouteBoundary {
        channels: AsyncMutex<HashMap<String, Vec<Event>>>,
        closed: std::sync::Mutex<std::collections::HashSet<String>>,
        changed: Notify,
    }

    #[derive(Default)]
    struct ReusableObservedRouteBoundary {
        channels: AsyncMutex<HashMap<String, Vec<Event>>>,
        closed: std::sync::Mutex<std::collections::HashSet<String>>,
        send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
        recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
        changed: Notify,
    }

    struct MaterializedRouteRaceBoundary {
        state: std::sync::Mutex<MaterializedRouteRaceState>,
        proxy_call_channel: String,
        internal_call_channel: String,
        selected_group: String,
        path: Vec<u8>,
    }

    #[derive(Default)]
    struct MaterializedRouteRaceState {
        sent_call_channel: Option<String>,
        sent_correlation: Option<u64>,
        recv_counts_by_channel: HashMap<String, usize>,
    }

    #[async_trait]
    impl ChannelIoSubset for LoopbackRouteBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
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
        ) -> capanix_app_sdk::Result<Vec<Event>> {
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

        fn channel_close(
            &self,
            _ctx: BoundaryContext,
            channel: ChannelKey,
        ) -> capanix_app_sdk::Result<()> {
            self.closed
                .lock()
                .expect("loopback closed lock")
                .insert(channel.0);
            self.changed.notify_waiters();
            Ok(())
        }
    }

    impl MaterializedRouteRaceBoundary {
        fn new(
            proxy_call_channel: String,
            internal_call_channel: String,
            selected_group: impl Into<String>,
            path: impl Into<Vec<u8>>,
        ) -> Self {
            Self {
                state: std::sync::Mutex::new(MaterializedRouteRaceState::default()),
                proxy_call_channel,
                internal_call_channel,
                selected_group: selected_group.into(),
                path: path.into(),
            }
        }

        fn sent_call_channel(&self) -> Option<String> {
            self.state
                .lock()
                .expect("route race state lock")
                .sent_call_channel
                .clone()
        }
    }

    impl ReusableObservedRouteBoundary {
        fn send_batch_count(&self, channel: &str) -> usize {
            *self
                .send_batches_by_channel
                .lock()
                .expect("reusable route boundary send batches lock")
                .get(channel)
                .unwrap_or(&0)
        }

        fn recv_batch_count(&self, channel: &str) -> usize {
            *self
                .recv_batches_by_channel
                .lock()
                .expect("reusable route boundary recv batches lock")
                .get(channel)
                .unwrap_or(&0)
        }
    }

    impl ChannelBoundary for ReusableObservedRouteBoundary {
        fn log(&self, _ctx: BoundaryContext, _level: LogLevel, _msg: &str) {}
    }

    impl StateBoundary for ReusableObservedRouteBoundary {}

    #[async_trait]
    impl ChannelIoSubset for MaterializedRouteRaceBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            let correlation = request
                .events
                .first()
                .and_then(|event| event.metadata().correlation_id);
            let mut state = self.state.lock().expect("route race state lock");
            state.sent_call_channel = Some(request.channel_key.0);
            state.sent_correlation = correlation;
            Ok(())
        }

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            enum ReplyKind {
                Empty,
                Real,
            }

            let (delay, reply_kind, correlation) = {
                let mut state = self.state.lock().expect("route race state lock");
                let sent_call_channel = state
                    .sent_call_channel
                    .clone()
                    .ok_or_else(|| CnxError::Internal("missing sent call channel".into()))?;
                let correlation = state
                    .sent_correlation
                    .ok_or_else(|| CnxError::Internal("missing sent correlation".into()))?;
                let recv_count = state
                    .recv_counts_by_channel
                    .entry(request.channel_key.0.clone())
                    .or_default();
                let current_recv = *recv_count;
                *recv_count += 1;
                let proxy_reply_channel = format!("{}:reply", self.proxy_call_channel);
                let internal_reply_channel = format!("{}:reply", self.internal_call_channel);
                if request.channel_key.0 == proxy_reply_channel
                    && sent_call_channel == self.proxy_call_channel
                {
                    match current_recv {
                        0 => (Duration::ZERO, ReplyKind::Empty, correlation),
                        1 => (
                            MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE + Duration::from_millis(150),
                            ReplyKind::Real,
                            correlation,
                        ),
                        _ => return Err(CnxError::Timeout),
                    }
                } else if request.channel_key.0 == internal_reply_channel
                    && sent_call_channel == self.internal_call_channel
                {
                    match current_recv {
                        0 => (Duration::from_millis(50), ReplyKind::Real, correlation),
                        _ => return Err(CnxError::Timeout),
                    }
                } else {
                    return Err(CnxError::Timeout);
                }
            };

            tokio::time::sleep(delay).await;
            let payload = match reply_kind {
                ReplyKind::Empty => empty_materialized_tree_payload_for_test(&self.path),
                ReplyKind::Real => real_materialized_tree_payload_for_test(&self.path),
            };
            Ok(vec![mk_event_with_correlation(
                &self.selected_group,
                correlation,
                payload,
            )])
        }
    }

    #[async_trait]
    impl ChannelIoSubset for ReusableObservedRouteBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            {
                let mut send_batches = self
                    .send_batches_by_channel
                    .lock()
                    .expect("reusable route boundary send batches lock");
                *send_batches.entry(request.channel_key.0.clone()).or_default() += 1;
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
        ) -> capanix_app_sdk::Result<Vec<Event>> {
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
                            .expect("reusable route boundary recv batches lock");
                        *recv_batches.entry(request.channel_key.0.clone()).or_default() += 1;
                        return Ok(events);
                    }
                }
                {
                    let closed = self.closed.lock().expect("reusable route boundary closed lock");
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

        fn channel_close(
            &self,
            _ctx: BoundaryContext,
            channel: ChannelKey,
        ) -> capanix_app_sdk::Result<()> {
            self.closed
                .lock()
                .expect("reusable route boundary closed lock")
                .insert(channel.0);
            self.changed.notify_waiters();
            Ok(())
        }
    }

    impl ForceFindFixture {
        fn new(scenario: ForceFindFixtureScenario) -> Self {
            let tempdir = tempfile::tempdir().expect("create tempdir");
            let sink_a = tempdir.path().join("sink-a");
            let sink_b = tempdir.path().join("sink-b");
            fs::create_dir_all(&sink_a).expect("create sink-a dir");
            fs::create_dir_all(&sink_b).expect("create sink-b dir");

            match scenario {
                ForceFindFixtureScenario::Standard => {
                    fs::write(sink_b.join("winner-b"), b"b").expect("write winner-b");
                    fs::write(sink_b.join("extra-b-1"), b"b").expect("write extra-b-1");
                    fs::write(sink_b.join("extra-b-2"), b"b").expect("write extra-b-2");
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    fs::write(sink_a.join("winner-a"), b"a").expect("write winner-a");
                }
                ForceFindFixtureScenario::FileAgeNoFiles => {
                    fs::create_dir_all(sink_a.join("empty-a")).expect("create empty-a");
                    fs::create_dir_all(sink_b.join("empty-b")).expect("create empty-b");
                }
            }

            let grants = vec![
                granted_mount_root("sink-a", &sink_a),
                granted_mount_root("sink-b", &sink_b),
            ];
            let config = source_config_with_grants(&grants);
            let source = Arc::new(SourceFacade::local(Arc::new(
                FSMetaSource::with_boundaries(config.clone(), NodeId("source-node".into()), None)
                    .expect("build source"),
            )));
            let sink = Arc::new(SinkFacade::local(Arc::new(
                SinkFileMeta::with_boundaries(NodeId("sink-node".into()), None, config.clone())
                    .expect("build sink"),
            )));
            let policy = Arc::new(RwLock::new(projection_policy_from_host_object_grants(
                &grants,
            )));

            Self {
                _tempdir: tempdir,
                app: create_local_router(
                    sink,
                    source,
                    None,
                    NodeId("test-node".into()),
                    policy,
                    Arc::new(Mutex::new(BTreeSet::new())),
                ),
            }
        }
    }

    fn granted_mount_root(object_ref: &str, mount_point: &Path) -> GrantedMountRoot {
        GrantedMountRoot {
            object_ref: object_ref.to_string(),
            host_ref: "source-node".to_string(),
            host_ip: format!("10.0.0.{}", if object_ref == "sink-a" { 1 } else { 2 }),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: mount_point.to_path_buf(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        }
    }

    fn source_config_with_grants(grants: &[GrantedMountRoot]) -> SourceConfig {
        SourceConfig {
            roots: grants
                .iter()
                .map(|grant| RootSpec::new(grant.object_ref.clone(), grant.mount_point.clone()))
                .collect(),
            host_object_grants: grants.to_vec(),
            ..SourceConfig::default()
        }
    }

    fn mk_event(origin: &str, payload: Vec<u8>) -> Event {
        Event::new(
            EventMetadata {
                origin_id: NodeId(origin.to_string()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            Bytes::from(payload),
        )
    }

    fn mk_event_with_correlation(origin: &str, correlation: u64, payload: Vec<u8>) -> Event {
        Event::new(
            EventMetadata {
                origin_id: NodeId(origin.to_string()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: Some(correlation),
                ingress_auth: None,
                trace: None,
            },
            Bytes::from(payload),
        )
    }

    fn empty_materialized_tree_payload_for_test(path: &[u8]) -> Vec<u8> {
        rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(TreeGroupPayload {
            reliability: GroupReliability::from_reason(Some(
                crate::shared_types::query::UnreliableReason::Unattested,
            )),
            stability: TreeStability::not_evaluated(),
            root: TreePageRoot {
                path: path.to_vec(),
                size: 0,
                modified_time_us: 0,
                is_dir: true,
                exists: false,
                has_children: false,
            },
            entries: Vec::new(),
        }))
        .expect("encode empty materialized tree payload")
    }

    fn real_materialized_tree_payload_for_test(path: &[u8]) -> Vec<u8> {
        rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(TreeGroupPayload {
            reliability: GroupReliability {
                reliable: true,
                unreliable_reason: None,
            },
            stability: TreeStability::not_evaluated(),
            root: TreePageRoot {
                path: path.to_vec(),
                size: 0,
                modified_time_us: 1,
                is_dir: true,
                exists: true,
                has_children: true,
            },
            entries: Vec::new(),
        }))
        .expect("encode real materialized tree payload")
    }

    fn mk_source_record_event(origin: &str, path: &[u8], file_name: &[u8], ts: u64) -> Event {
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
        let payload = rmp_serde::to_vec_named(&record).expect("encode source record");
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

    fn origin_policy() -> ProjectionPolicy {
        ProjectionPolicy {
            member_grouping: MemberGroupingStrategy::ObjectRef,
            ..ProjectionPolicy::default()
        }
    }

    fn source_facade_with_group(group_id: &str, grants: &[GrantedMountRoot]) -> Arc<SourceFacade> {
        let mut root = RootSpec::new(group_id, "/unused");
        root.selector = crate::source::config::RootSelector {
            fs_type: Some("nfs".to_string()),
            ..crate::source::config::RootSelector::default()
        };
        root.subpath_scope = std::path::PathBuf::from("/");
        let config = SourceConfig {
            roots: vec![root],
            host_object_grants: grants.to_vec(),
            ..SourceConfig::default()
        };
        Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::with_boundaries(config, NodeId("source-node".into()), None)
                .expect("build source"),
        )))
    }

    fn source_facade_with_roots(
        roots: Vec<RootSpec>,
        grants: &[GrantedMountRoot],
    ) -> Arc<SourceFacade> {
        let config = SourceConfig {
            roots,
            host_object_grants: grants.to_vec(),
            ..SourceConfig::default()
        };
        Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::with_boundaries(config, NodeId("source-node".into()), None)
                .expect("build source"),
        )))
    }

    fn sink_facade_with_group(grants: &[GrantedMountRoot]) -> Arc<SinkFacade> {
        let mut root = RootSpec::new("nfs1", "/unused");
        root.selector = crate::source::config::RootSelector {
            fs_type: Some("nfs".to_string()),
            ..crate::source::config::RootSelector::default()
        };
        root.subpath_scope = std::path::PathBuf::from("/");
        let config = SourceConfig {
            roots: vec![root],
            host_object_grants: grants.to_vec(),
            ..SourceConfig::default()
        };
        Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("sink-node".into()), None, config)
                .expect("build sink"),
        )))
    }

    fn test_api_state_for_source(source: Arc<SourceFacade>, sink: Arc<SinkFacade>) -> ApiState {
        ApiState {
            backend: QueryBackend::Local { sink, source },
            policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
            force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
            force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
            readiness_source: None,
            readiness_sink: None,
            materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        }
    }

    fn test_api_state_for_route_source(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        boundary: Arc<dyn ChannelIoSubset>,
        origin_id: NodeId,
    ) -> ApiState {
        ApiState {
            backend: QueryBackend::Route {
                sink,
                boundary,
                origin_id,
                source,
            },
            policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
            force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
            force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
            readiness_source: None,
            readiness_sink: None,
            materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        }
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

    fn external_sink_worker_binding(socket_dir: &Path) -> RuntimeWorkerBinding {
        RuntimeWorkerBinding {
            role_id: "sink".to_string(),
            mode: WorkerMode::External,
            launcher_kind: RuntimeWorkerLauncherKind::WorkerHost,
            module_path: Some(fs_meta_worker_module_path()),
            socket_dir: Some(socket_dir.to_path_buf()),
        }
    }

    fn external_worker_root(id: &str, path: &Path) -> RootSpec {
        let mut root = RootSpec::new(id, path);
        root.watch = false;
        root.scan = true;
        root
    }

    fn sink_group_status(
        group_id: &str,
        initial_audit_completed: bool,
    ) -> crate::sink::SinkGroupStatusSnapshot {
        crate::sink::SinkGroupStatusSnapshot {
            group_id: group_id.to_string(),
            primary_object_ref: format!("{group_id}-owner"),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_audit: false,
            initial_audit_completed,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }
    }

    #[test]
    fn cached_sink_status_preserves_complete_snapshot_for_same_roots() {
        let groups = BTreeSet::from(["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()]);
        let cached = CachedSinkStatusSnapshot {
            groups: groups.clone(),
            snapshot: SinkStatusSnapshot {
                groups: vec![
                    sink_group_status("nfs1", true),
                    sink_group_status("nfs2", true),
                    sink_group_status("nfs3", true),
                ],
                ..SinkStatusSnapshot::default()
            },
        };
        let fresh = SinkStatusSnapshot {
            groups: vec![
                sink_group_status("nfs1", true),
                sink_group_status("nfs2", true),
            ],
            ..SinkStatusSnapshot::default()
        };

        let merged = merge_with_cached_sink_status_snapshot(Some(&cached), &groups, fresh);
        let merged_groups = merged
            .groups
            .iter()
            .map(|group| group.group_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(merged_groups, vec!["nfs1", "nfs2", "nfs3"]);
        assert!(
            merged
                .groups
                .iter()
                .all(|group| group.initial_audit_completed)
        );
    }

    #[test]
    fn cached_sink_status_drops_stale_groups_after_root_set_change() {
        let cached = CachedSinkStatusSnapshot {
            groups: BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
            snapshot: SinkStatusSnapshot {
                groups: vec![
                    sink_group_status("nfs1", true),
                    sink_group_status("nfs2", true),
                ],
                ..SinkStatusSnapshot::default()
            },
        };
        let fresh_groups = BTreeSet::from(["nfs1".to_string()]);
        let fresh = SinkStatusSnapshot {
            groups: vec![sink_group_status("nfs1", true)],
            ..SinkStatusSnapshot::default()
        };

        let merged = merge_with_cached_sink_status_snapshot(Some(&cached), &fresh_groups, fresh);
        let merged_groups = merged
            .groups
            .iter()
            .map(|group| group.group_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(merged_groups, vec!["nfs1"]);
    }

    #[test]
    fn merge_sink_status_snapshots_preserves_received_origin_counts_by_node() {
        let merged = merge_sink_status_snapshots(vec![
            SinkStatusSnapshot {
                scheduled_groups_by_node: BTreeMap::from([(
                    "node-a".to_string(),
                    vec!["nfs1".to_string()],
                )]),
                received_batches_by_node: BTreeMap::from([("node-a".to_string(), 11)]),
                received_events_by_node: BTreeMap::from([("node-a".to_string(), 111)]),
                received_control_events_by_node: BTreeMap::from([("node-a".to_string(), 2)]),
                received_data_events_by_node: BTreeMap::from([("node-a".to_string(), 109)]),
                last_received_at_us_by_node: BTreeMap::from([("node-a".to_string(), 123)]),
                last_received_origins_by_node: BTreeMap::from([(
                    "node-a".to_string(),
                    vec!["node-a::nfs1=2".to_string()],
                )]),
                received_origin_counts_by_node: BTreeMap::from([(
                    "node-a".to_string(),
                    vec!["node-a::nfs1=111".to_string()],
                )]),
                stream_applied_batches_by_node: BTreeMap::from([("node-a".to_string(), 5)]),
                stream_applied_events_by_node: BTreeMap::from([("node-a".to_string(), 50)]),
                stream_applied_control_events_by_node: BTreeMap::from([("node-a".to_string(), 4)]),
                stream_applied_data_events_by_node: BTreeMap::from([("node-a".to_string(), 46)]),
                stream_applied_origin_counts_by_node: BTreeMap::from([(
                    "node-a".to_string(),
                    vec!["node-a::nfs1=50".to_string()],
                )]),
                stream_last_applied_at_us_by_node: BTreeMap::from([("node-a".to_string(), 321)]),
                ..SinkStatusSnapshot::default()
            },
            SinkStatusSnapshot {
                scheduled_groups_by_node: BTreeMap::from([(
                    "node-b".to_string(),
                    vec!["nfs2".to_string()],
                )]),
                received_batches_by_node: BTreeMap::from([("node-b".to_string(), 22)]),
                received_events_by_node: BTreeMap::from([("node-b".to_string(), 222)]),
                received_control_events_by_node: BTreeMap::from([("node-b".to_string(), 3)]),
                received_data_events_by_node: BTreeMap::from([("node-b".to_string(), 219)]),
                last_received_at_us_by_node: BTreeMap::from([("node-b".to_string(), 456)]),
                last_received_origins_by_node: BTreeMap::from([(
                    "node-b".to_string(),
                    vec!["node-b::nfs2=1".to_string()],
                )]),
                received_origin_counts_by_node: BTreeMap::from([(
                    "node-b".to_string(),
                    vec!["node-b::nfs2=222".to_string()],
                )]),
                stream_applied_batches_by_node: BTreeMap::from([("node-b".to_string(), 6)]),
                stream_applied_events_by_node: BTreeMap::from([("node-b".to_string(), 60)]),
                stream_applied_control_events_by_node: BTreeMap::from([("node-b".to_string(), 5)]),
                stream_applied_data_events_by_node: BTreeMap::from([("node-b".to_string(), 55)]),
                stream_applied_origin_counts_by_node: BTreeMap::from([(
                    "node-b".to_string(),
                    vec!["node-b::nfs2=60".to_string()],
                )]),
                stream_last_applied_at_us_by_node: BTreeMap::from([("node-b".to_string(), 654)]),
                ..SinkStatusSnapshot::default()
            },
        ]);

        assert_eq!(merged.received_batches_by_node.get("node-a"), Some(&11));
        assert_eq!(merged.received_batches_by_node.get("node-b"), Some(&22));
        assert_eq!(merged.received_events_by_node.get("node-a"), Some(&111));
        assert_eq!(merged.received_events_by_node.get("node-b"), Some(&222));
        assert_eq!(
            merged.stream_applied_batches_by_node.get("node-a"),
            Some(&5)
        );
        assert_eq!(
            merged.stream_applied_batches_by_node.get("node-b"),
            Some(&6)
        );
        assert_eq!(
            merged.stream_applied_events_by_node.get("node-a"),
            Some(&50)
        );
        assert_eq!(
            merged.stream_applied_events_by_node.get("node-b"),
            Some(&60)
        );
        assert_eq!(
            merged.last_received_origins_by_node.get("node-a"),
            Some(&vec!["node-a::nfs1=2".to_string()])
        );
        assert_eq!(
            merged.last_received_origins_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=1".to_string()])
        );
        assert_eq!(
            merged.received_origin_counts_by_node.get("node-a"),
            Some(&vec!["node-a::nfs1=111".to_string()])
        );
        assert_eq!(
            merged.received_origin_counts_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=222".to_string()])
        );
    }

    #[test]
    fn mount_point_grouping_keeps_object_ref_opaque_outside_descriptor_match() {
        let policy = ProjectionPolicy {
            member_grouping: MemberGroupingStrategy::MountPoint,
            mount_point_by_object_ref: HashMap::from([
                ("nfs1".to_string(), "/tmp/workdir/nfs1".to_string()),
                ("nfs2".to_string(), "/mnt/storage/nfs2".to_string()),
                ("node-a::nfs2".to_string(), "/mnt/storage/nfs2".to_string()),
                (
                    "/tmp/workdir/nfs1".to_string(),
                    "/tmp/workdir/nfs1".to_string(),
                ),
            ]),
            ..ProjectionPolicy::default()
        };
        assert_eq!(policy.group_key_for_object_ref("nfs1"), "nfs1");
        assert_eq!(policy.group_key_for_object_ref("nfs2"), "nfs2");
        assert_eq!(
            policy.group_key_for_object_ref("/tmp/workdir/nfs1"),
            "/tmp/workdir/nfs1"
        );
        assert_eq!(
            policy.group_key_for_object_ref("node-a::nfs2"),
            "/mnt/storage/nfs2"
        );
        assert_eq!(
            policy.group_key_for_object_ref("/tmp/workdir/nfs3"),
            "/tmp/workdir/nfs3"
        );
    }

    #[test]
    fn object_ref_grouping_preserves_utf8_and_distinguishes_normalization_forms() {
        let composed = "café-👩🏽‍💻";
        let decomposed = "cafe\u{301}-👩🏽‍💻";
        assert_ne!(composed, decomposed);

        let policy = ProjectionPolicy {
            member_grouping: MemberGroupingStrategy::ObjectRef,
            ..ProjectionPolicy::default()
        };

        assert_eq!(policy.group_key_for_object_ref(composed), composed);
        assert_eq!(policy.group_key_for_object_ref(decomposed), decomposed);
        assert_ne!(
            policy.group_key_for_object_ref(composed),
            policy.group_key_for_object_ref(decomposed)
        );
    }

    #[test]
    fn normalized_record_path_for_query_rebases_prefixed_absolute_path() {
        let query_path = b"/qf-e2e-job";
        let record_path = b"/tmp/capanix/data/nfs1/qf-e2e-job/file-a.txt";
        let normalized =
            normalized_path_for_query(record_path, query_path).expect("normalize path");
        assert_eq!(normalized, b"/qf-e2e-job/file-a.txt".to_vec());
    }

    #[test]
    fn node_id_from_object_ref_extracts_node_prefix() {
        assert_eq!(
            node_id_from_object_ref("node-a::nfs1").map(|id| id.0),
            Some("node-a".to_string())
        );
        assert!(node_id_from_object_ref("nfs1").is_none());
    }

    #[test]
    fn force_find_runner_node_selection_rotates_across_group_members() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let root_a = tmp.path().join("node-a");
        let root_b = tmp.path().join("node-b");
        fs::create_dir_all(&root_a).expect("create node-a dir");
        fs::create_dir_all(&root_b).expect("create node-b dir");
        let grants = vec![
            GrantedMountRoot {
                object_ref: "node-a::nfs1".to_string(),
                host_ref: "node-a".to_string(),
                host_ip: "10.0.0.1".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: std::collections::BTreeMap::new(),
                mount_point: root_a,
                fs_source: "nfs".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: Vec::new(),
                active: true,
            },
            GrantedMountRoot {
                object_ref: "node-b::nfs1".to_string(),
                host_ref: "node-b".to_string(),
                host_ip: "10.0.0.2".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: std::collections::BTreeMap::new(),
                mount_point: root_b,
                fs_source: "nfs".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: Vec::new(),
                active: true,
            },
        ];
        let source = source_facade_with_group("nfs1", &grants);
        let sink = sink_facade_with_group(&grants);
        let state = test_api_state_for_source(source.clone(), sink);

        let first = crate::runtime_app::shared_tokio_runtime()
            .block_on(select_force_find_runner_node_for_group(
                &state,
                source.as_ref(),
                "nfs1",
            ))
            .expect("first selection")
            .expect("first node");
        let second = crate::runtime_app::shared_tokio_runtime()
            .block_on(select_force_find_runner_node_for_group(
                &state,
                source.as_ref(),
                "nfs1",
            ))
            .expect("second selection")
            .expect("second node");
        let third = crate::runtime_app::shared_tokio_runtime()
            .block_on(select_force_find_runner_node_for_group(
                &state,
                source.as_ref(),
                "nfs1",
            ))
            .expect("third selection")
            .expect("third node");

        assert_eq!(first.0, "node-a");
        assert_eq!(second.0, "node-b");
        assert_eq!(third.0, "node-a");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn selected_group_force_find_route_uses_source_find_endpoint_for_chosen_node() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let root = tmp.path().join("node-a");
        fs::create_dir_all(root.join("force-find-stress")).expect("create node-a dir");
        fs::write(root.join("force-find-stress").join("seed.txt"), b"a")
            .expect("seed node-a file");
        let grants = vec![GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        }];
        let source = source_facade_with_group("nfs1", &grants);
        let sink = sink_facade_with_group(&grants);
        let boundary = Arc::new(LoopbackRouteBoundary::default());
        let route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND)
            .expect("resolve source-find route");
        let routed_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let routed_calls_for_handler = routed_calls.clone();
        let mut endpoint = ManagedEndpointTask::spawn(
            boundary.clone(),
            route,
            "test-source-find-endpoint",
            CancellationToken::new(),
            move |requests| {
                let routed_calls = routed_calls_for_handler.clone();
                async move {
                    routed_calls.fetch_add(requests.len(), std::sync::atomic::Ordering::SeqCst);
                    vec![mk_event("node-a::routed", vec![1, 2, 3])]
                }
            },
        );
        let state = test_api_state_for_route_source(
            source,
            sink,
            boundary,
            NodeId("node-d".to_string()),
        );

        let result = query_force_find_group_tree(
            &state,
            b"/force-find-stress",
            true,
            None,
            Duration::from_secs(2),
            "nfs1",
            true,
        )
        .await
        .expect("selected-group force-find route query");

        assert_eq!(
            routed_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "selected-group routed force-find should use the internal source-find endpoint on the chosen node"
        );
        assert_eq!(
            result
                .iter()
                .map(|event| event.metadata().origin_id.0.as_str())
                .collect::<Vec<_>>(),
            vec!["node-a::routed"]
        );

        endpoint.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn selected_group_materialized_route_uses_sink_query_on_chosen_node() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let root = tmp.path().join("node-a");
        fs::create_dir_all(root.join("force-find-stress")).expect("create node-a dir");
        fs::write(root.join("force-find-stress").join("seed.txt"), b"a")
            .expect("seed node-a file");
        let grants = vec![GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        }];
        let source = source_facade_with_group("nfs1", &grants);
        let sink = sink_facade_with_group(&grants);
        let boundary = Arc::new(LoopbackRouteBoundary::default());
        let route = sink_query_request_route_for("node-a");
        let observed_request_nodes = Arc::new(Mutex::new(Vec::<String>::new()));
        let observed_request_nodes_for_handler = observed_request_nodes.clone();
        let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(
            TreeGroupPayload {
                reliability: GroupReliability {
                    reliable: true,
                    unreliable_reason: None,
                },
                stability: TreeStability::not_evaluated(),
                root: TreePageRoot {
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
        .expect("encode materialized tree payload");
        let mut endpoint = ManagedEndpointTask::spawn(
            boundary.clone(),
            route,
            "test-sink-query-endpoint",
            CancellationToken::new(),
            move |requests| {
                let observed_request_nodes = observed_request_nodes_for_handler.clone();
                let payload = payload.clone();
                async move {
                    let mut responses = Vec::new();
                    observed_request_nodes
                        .lock()
                        .expect("request nodes lock")
                        .extend(
                            requests
                                .iter()
                                .map(|req| req.metadata().origin_id.0.clone()),
                        );
                    for req in requests {
                        responses.push(mk_event_with_correlation(
                            "nfs1",
                            req.metadata()
                                .correlation_id
                                .expect("route request correlation"),
                            payload.to_vec(),
                        ));
                    }
                    responses
                }
            },
        );
        let state = test_api_state_for_route_source(
            source,
            sink,
            boundary,
            NodeId("node-d".to_string()),
        );

        let result = query_materialized_events(
            state.backend.clone(),
            build_materialized_tree_request(
                b"/force-find-stress",
                true,
                None,
                ReadClass::Materialized,
                Some("nfs1".to_string()),
            ),
            Duration::from_secs(2),
        )
        .await
        .expect("selected-group materialized route query");

        assert_eq!(
            observed_request_nodes.lock().expect("request nodes lock").as_slice(),
            &["node-a".to_string()],
            "selected-group routed materialized query should call the internal sink-query endpoint as the chosen owner node"
        );
        assert_eq!(
            result
                .iter()
                .map(|event| event.metadata().origin_id.0.as_str())
                .collect::<Vec<_>>(),
            vec!["nfs1"]
        );

        endpoint.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn selected_group_materialized_route_targets_owner_scoped_internal_route() {
        let generic_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY)
            .expect("resolve generic sink-query route");
        let owner_route = sink_query_request_route_for("node-a");
        let boundary = Arc::new(ReusableObservedRouteBoundary::default());
        let non_owner_groups = Arc::new(Mutex::new(Vec::<String>::new()));
        let owner_groups = Arc::new(Mutex::new(Vec::<String>::new()));
        let non_owner_groups_for_handler = non_owner_groups.clone();
        let owner_groups_for_handler = owner_groups.clone();

        let mut non_owner_endpoint = ManagedEndpointTask::spawn(
            boundary.clone(),
            generic_route.clone(),
            "test-nonowner-generic-sink-query-endpoint",
            CancellationToken::new(),
            move |requests| {
                let non_owner_groups = non_owner_groups_for_handler.clone();
                async move {
                    for req in requests {
                        let params = rmp_serde::from_slice::<InternalQueryRequest>(
                            req.payload_bytes(),
                        )
                        .expect("decode non-owner internal query request");
                        non_owner_groups
                            .lock()
                            .expect("non-owner groups lock")
                            .push(
                                params
                                    .scope
                                    .selected_group
                                    .clone()
                                    .expect("selected group for non-owner request"),
                            );
                    }
                    Vec::new()
                }
            },
        );
        let mut owner_endpoint = ManagedEndpointTask::spawn(
            boundary.clone(),
            owner_route.clone(),
            "test-owner-scoped-sink-query-endpoint",
            CancellationToken::new(),
            move |requests| {
                let owner_groups = owner_groups_for_handler.clone();
                async move {
                    let mut responses = Vec::new();
                    for req in requests {
                        let params = rmp_serde::from_slice::<InternalQueryRequest>(
                            req.payload_bytes(),
                        )
                        .expect("decode owner internal query request");
                        let group_id = params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for owner request");
                        owner_groups
                            .lock()
                            .expect("owner groups lock")
                            .push(group_id.clone());
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("route request correlation"),
                            real_materialized_tree_payload_for_test(&params.scope.path),
                        ));
                    }
                    responses
                }
            },
        );

        let result = route_materialized_events_via_node(
            boundary.clone(),
            NodeId("node-a".to_string()),
            build_materialized_tree_request(
                b"/force-find-stress",
                true,
                None,
                ReadClass::Materialized,
                Some("nfs2".to_string()),
            ),
            Duration::from_millis(250),
        )
        .await;

        assert!(
            result.is_ok(),
            "selected-group routed materialized query should use an owner-scoped internal route; generic_send_batches={} owner_send_batches={} non_owner_groups={:?} owner_groups={:?} err={:?}",
            boundary.send_batch_count(&generic_route.0),
            boundary.send_batch_count(&owner_route.0),
            non_owner_groups
                .lock()
                .expect("non-owner groups lock")
                .clone(),
            owner_groups.lock().expect("owner groups lock").clone(),
            result.as_ref().err(),
        );
        let payload = decode_materialized_selected_group_response(
            &result.expect("selected-group route result"),
            &ProjectionPolicy::default(),
            "nfs2",
            b"/force-find-stress",
        )
        .expect("decode selected-group response");
        assert!(payload.root.exists);
        assert!(
            non_owner_groups
                .lock()
                .expect("non-owner groups lock")
                .is_empty(),
            "generic non-owner internal sink-query route should not receive selected-group owner-targeted requests"
        );
        assert_eq!(
            owner_groups.lock().expect("owner groups lock").as_slice(),
            &["nfs2".to_string()],
            "owner-scoped internal sink-query route should receive the selected group"
        );

        non_owner_endpoint.shutdown(Duration::from_secs(2)).await;
        owner_endpoint.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn selected_group_materialized_route_waits_for_owner_payload_instead_of_settling_on_fast_empty_proxy_reply() {
        let proxy_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
            .expect("resolve sink-query-proxy route");
        let internal_route = sink_query_request_route_for("node-a");
        let boundary = Arc::new(MaterializedRouteRaceBoundary::new(
            proxy_route.0.clone(),
            internal_route.0.clone(),
            "nfs1",
            b"/force-find-stress".to_vec(),
        ));

        let events = route_materialized_events_via_node(
            boundary.clone(),
            NodeId("node-a".to_string()),
            build_materialized_tree_request(
                b"/force-find-stress",
                true,
                None,
                ReadClass::Materialized,
                Some("nfs1".to_string()),
            ),
            Duration::from_secs(2),
        )
        .await
        .expect("route selected-group materialized query");

        let payload = decode_materialized_selected_group_response(
            &events,
            &ProjectionPolicy::default(),
            "nfs1",
            b"/force-find-stress",
        )
        .expect("decode selected-group materialized response");

        assert_eq!(
            boundary.sent_call_channel().as_deref(),
            Some(internal_route.0.as_str()),
            "selected-group materialized routing must bypass the fan-in proxy and use the internal sink-query route for the chosen owner"
        );
        assert!(
            payload.root.exists,
            "selected-group materialized route must wait for the chosen owner's real payload instead of settling on an earlier empty reply"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sequential_selected_group_materialized_route_reaches_owner_twice_for_same_owner_node() {
        let route = sink_query_request_route_for("node-a");
        let reply_route = format!("{}:reply", route.0);
        let boundary = Arc::new(ReusableObservedRouteBoundary::default());
        let observed_groups = Arc::new(Mutex::new(Vec::<String>::new()));
        let observed_groups_for_handler = observed_groups.clone();
        let mut endpoint = ManagedEndpointTask::spawn(
            boundary.clone(),
            route.clone(),
            "test-sequential-sink-query-endpoint",
            CancellationToken::new(),
            move |requests| {
                let observed_groups = observed_groups_for_handler.clone();
                async move {
                    let mut responses = Vec::new();
                    for req in requests {
                        let params = rmp_serde::from_slice::<InternalQueryRequest>(
                            req.payload_bytes(),
                        )
                        .expect("decode internal query request");
                        let group_id = params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group");
                        observed_groups
                            .lock()
                            .expect("observed groups lock")
                            .push(group_id.clone());
                        let payload = real_materialized_tree_payload_for_test(&params.scope.path);
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("route request correlation"),
                            payload,
                        ));
                    }
                    responses
                }
            },
        );

        let first = route_materialized_events_via_node(
            boundary.clone(),
            NodeId("node-a".to_string()),
            build_materialized_tree_request(
                b"/force-find-stress",
                true,
                None,
                ReadClass::Materialized,
                Some("nfs1".to_string()),
            ),
            Duration::from_secs(2),
        )
        .await;
        let second = route_materialized_events_via_node(
            boundary.clone(),
            NodeId("node-a".to_string()),
            build_materialized_tree_request(
                b"/force-find-stress",
                true,
                None,
                ReadClass::Materialized,
                Some("nfs2".to_string()),
            ),
            Duration::from_secs(2),
        )
        .await;

        assert!(
            first.is_ok(),
            "first same-owner selected-group materialized route should complete; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={} owner_groups={:?} err={:?}",
            boundary.send_batch_count(&route.0),
            boundary.recv_batch_count(&route.0),
            boundary.send_batch_count(&reply_route),
            boundary.recv_batch_count(&reply_route),
            observed_groups.lock().expect("observed groups lock").clone(),
            first.as_ref().err(),
        );
        assert!(
            second.is_ok(),
            "second same-owner selected-group materialized route should complete; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={} owner_groups={:?} err={:?}",
            boundary.send_batch_count(&route.0),
            boundary.recv_batch_count(&route.0),
            boundary.send_batch_count(&reply_route),
            boundary.recv_batch_count(&reply_route),
            observed_groups.lock().expect("observed groups lock").clone(),
            second.as_ref().err(),
        );

        let first = first.expect("first route result");
        let second = second.expect("second route result");
        let first_payload = decode_materialized_selected_group_response(
            &first,
            &ProjectionPolicy::default(),
            "nfs1",
            b"/force-find-stress",
        )
        .expect("decode first selected-group materialized response");
        let second_payload = decode_materialized_selected_group_response(
            &second,
            &ProjectionPolicy::default(),
            "nfs2",
            b"/force-find-stress",
        )
        .expect("decode second selected-group materialized response");

        assert!(first_payload.root.exists);
        assert!(second_payload.root.exists);
        assert_eq!(
            observed_groups.lock().expect("observed groups lock").as_slice(),
            &["nfs1".to_string(), "nfs2".to_string()],
            "owner endpoint should receive both sequential same-owner selected-group materialized reads"
        );
        assert_eq!(
            boundary.send_batch_count(&route.0),
            2,
            "both sequential reads should send one internal sink-query request batch each"
        );
        assert_eq!(
            boundary.recv_batch_count(&route.0),
            2,
            "owner endpoint should receive both internal sink-query request batches"
        );
        assert_eq!(
            boundary.send_batch_count(&reply_route),
            2,
            "owner endpoint should send one reply batch for each sequential same-owner read"
        );
        assert_eq!(
            boundary.recv_batch_count(&reply_route),
            2,
            "query-side collection should receive one reply batch for each sequential same-owner read"
        );

        endpoint.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn selected_group_materialized_route_reaches_external_owner_worker_twice_across_nodes() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
        fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
        fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

        let roots = vec![
            external_worker_root("nfs1", &nfs1),
            external_worker_root("nfs2", &nfs2),
        ];
        let grants = vec![
            GrantedMountRoot {
                object_ref: "node-a::nfs1".to_string(),
                host_ref: "node-a".to_string(),
                host_ip: "10.0.0.11".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: std::collections::BTreeMap::new(),
                mount_point: nfs1.clone(),
                fs_source: "nfs".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: Vec::new(),
                active: true,
            },
            GrantedMountRoot {
                object_ref: "node-a::nfs2".to_string(),
                host_ref: "node-a".to_string(),
                host_ip: "10.0.0.12".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: std::collections::BTreeMap::new(),
                mount_point: nfs2.clone(),
                fs_source: "nfs".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: Vec::new(),
                active: true,
            },
        ];
        let cfg = SourceConfig {
            roots: roots.clone(),
            host_object_grants: grants.clone(),
            ..SourceConfig::default()
        };
        let source_runtime =
            FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
                .expect("init owner source runtime");
        let mut stream = source_runtime.pub_().await.expect("start owner source stream");

        let boundary = Arc::new(ReusableObservedRouteBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = tempdir().expect("create worker socket dir");
        let factory = RuntimeWorkerClientFactory::new(
            boundary.clone(),
            boundary.clone(),
            state_boundary,
        );
        let sink_worker = SinkWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client");

        tokio::time::timeout(Duration::from_secs(8), sink_worker.ensure_started())
            .await
            .expect("sink worker start timed out")
            .expect("start sink worker");
        sink_worker
            .on_control_frame(vec![
                capanix_runtime_entry_sdk::control::encode_runtime_exec_control(
                    &capanix_runtime_entry_sdk::control::RuntimeExecControl::Activate(
                        capanix_runtime_entry_sdk::control::RuntimeExecActivate {
                            route_key: crate::runtime::routes::ROUTE_KEY_QUERY.to_string(),
                            unit_id: "runtime.exec.sink".to_string(),
                            lease: None,
                            generation: 1,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                capanix_runtime_entry_sdk::control::RuntimeBoundScope {
                                    scope_id: "nfs1".to_string(),
                                    resource_ids: vec!["node-a::nfs1".to_string()],
                                },
                                capanix_runtime_entry_sdk::control::RuntimeBoundScope {
                                    scope_id: "nfs2".to_string(),
                                    resource_ids: vec!["node-a::nfs2".to_string()],
                                },
                            ],
                        },
                    ),
                )
                .expect("encode sink activate"),
            ])
            .await
            .expect("activate sink worker groups");

        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        while tokio::time::Instant::now() < ready_deadline {
            match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
                Ok(Some(batch)) => sink_worker.send(batch).await.expect("apply source batch"),
                Ok(None) => break,
                Err(_) => continue,
            }
            let nfs1_ready = decode_materialized_selected_group_response(
                &sink_worker
                    .materialized_query(build_materialized_tree_request(
                        b"/force-find-stress",
                        false,
                        Some(0),
                        ReadClass::Materialized,
                        Some("nfs1".to_string()),
                    ))
                    .await
                    .expect("query owner worker nfs1"),
                &ProjectionPolicy::default(),
                "nfs1",
                b"/force-find-stress",
            )
            .expect("decode owner worker nfs1")
            .root
            .exists;
            let nfs2_ready = decode_materialized_selected_group_response(
                &sink_worker
                    .materialized_query(build_materialized_tree_request(
                        b"/force-find-stress",
                        false,
                        Some(0),
                        ReadClass::Materialized,
                        Some("nfs2".to_string()),
                    ))
                    .await
                    .expect("query owner worker nfs2"),
                &ProjectionPolicy::default(),
                "nfs2",
                b"/force-find-stress",
            )
            .expect("decode owner worker nfs2")
            .root
            .exists;
            if nfs1_ready && nfs2_ready {
                break;
            }
        }

        let source = source_facade_with_roots(roots, &grants);
        let owner_map_before = source
            .source_primary_by_group_snapshot()
            .await
            .expect("source primary snapshot before");
        assert_eq!(
            owner_map_before.get("nfs1").map(String::as_str),
            Some("node-a::nfs1")
        );
        assert_eq!(
            owner_map_before.get("nfs2").map(String::as_str),
            Some("node-a::nfs2")
        );
        let state = test_api_state_for_route_source(
            source.clone(),
            sink_facade_with_group(&grants),
            boundary.clone(),
            NodeId("node-d".to_string()),
        );
        let route = sink_query_request_route_for("node-a");
        let reply_route = format!("{}:reply", route.0);

        let first = query_materialized_events(
            state.backend.clone(),
            build_materialized_tree_request(
                b"/force-find-stress",
                true,
                None,
                ReadClass::Materialized,
                Some("nfs1".to_string()),
            ),
            Duration::from_secs(5),
        )
        .await;
        let owner_map_after_first = source
            .source_primary_by_group_snapshot()
            .await
            .expect("source primary snapshot after first");
        let second = query_materialized_events(
            state.backend.clone(),
            build_materialized_tree_request(
                b"/force-find-stress",
                true,
                None,
                ReadClass::Materialized,
                Some("nfs2".to_string()),
            ),
            Duration::from_secs(5),
        )
        .await;

        assert_eq!(
            owner_map_after_first.get("nfs2").map(String::as_str),
            Some("node-a::nfs2"),
            "caller-side owner map should remain stable for the second same-owner selected-group read"
        );
        assert!(
            first.is_ok(),
            "first caller->owner selected-group route should complete; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={} err={:?}",
            boundary.send_batch_count(&route.0),
            boundary.recv_batch_count(&route.0),
            boundary.send_batch_count(&reply_route),
            boundary.recv_batch_count(&reply_route),
            first.as_ref().err(),
        );
        assert!(
            second.is_ok(),
            "second caller->owner selected-group route should complete; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={} err={:?}",
            boundary.send_batch_count(&route.0),
            boundary.recv_batch_count(&route.0),
            boundary.send_batch_count(&reply_route),
            boundary.recv_batch_count(&reply_route),
            second.as_ref().err(),
        );

        let first_payload = decode_materialized_selected_group_response(
            &first.expect("first selected-group route result"),
            &ProjectionPolicy::default(),
            "nfs1",
            b"/force-find-stress",
        )
        .expect("decode first selected-group response");
        let second_payload = decode_materialized_selected_group_response(
            &second.expect("second selected-group route result"),
            &ProjectionPolicy::default(),
            "nfs2",
            b"/force-find-stress",
        )
        .expect("decode second selected-group response");

        assert!(first_payload.root.exists);
        assert!(second_payload.root.exists);
        assert_eq!(
            boundary.send_batch_count(&route.0),
            2,
            "caller should send two direct internal sink-query request batches across nodes"
        );
        assert_eq!(
            boundary.recv_batch_count(&route.0),
            2,
            "owner worker runtime endpoint should receive both direct internal sink-query request batches"
        );
        assert_eq!(
            boundary.send_batch_count(&reply_route),
            2,
            "owner worker runtime endpoint should send one reply batch for each direct internal sink-query request"
        );
        assert_eq!(
            boundary.recv_batch_count(&reply_route),
            2,
            "caller should receive one reply batch for each direct internal sink-query request"
        );

        source_runtime.close().await.expect("close owner source runtime");
        sink_worker.close().await.expect("close sink worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn selected_group_materialized_route_prefers_sink_scheduled_owner_over_stale_source_primary(
    ) {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let node_a_root = tmp.path().join("node-a");
        fs::create_dir_all(node_a_root.join("retired-layout")).expect("create node-a dir");
        let grants = vec![GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        }];
        let source = source_facade_with_group("nfs4", &grants);
        let sink = sink_facade_with_group(&grants);
        let boundary = Arc::new(ReusableObservedRouteBoundary::default());
        let sink_status_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
            .expect("resolve sink-status route");
        let node_a_route = sink_query_request_route_for("node-a");
        let node_b_route = sink_query_request_route_for("node-b");
        let node_a_groups = Arc::new(Mutex::new(Vec::<String>::new()));
        let node_b_groups = Arc::new(Mutex::new(Vec::<String>::new()));
        let node_a_groups_for_handler = node_a_groups.clone();
        let node_b_groups_for_handler = node_b_groups.clone();
        let status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([
                (
                    "node-a".to_string(),
                    vec!["nfs1".to_string(), "nfs2".to_string()],
                ),
                ("node-b".to_string(), vec!["nfs4".to_string()]),
            ]),
            groups: vec![sink_group_status("nfs4", true)],
            ..SinkStatusSnapshot::default()
        })
        .expect("encode sink status snapshot");

        let mut sink_status_endpoint = ManagedEndpointTask::spawn(
            boundary.clone(),
            sink_status_route.clone(),
            "test-sink-status-endpoint",
            CancellationToken::new(),
            move |requests| {
                let status_payload = status_payload.clone();
                async move {
                    requests
                        .into_iter()
                        .map(|req| {
                            mk_event_with_correlation(
                                "node-b",
                                req.metadata()
                                    .correlation_id
                                    .expect("sink-status request correlation"),
                                status_payload.clone(),
                            )
                        })
                        .collect::<Vec<_>>()
                }
            },
        );
        let mut node_a_endpoint = ManagedEndpointTask::spawn(
            boundary.clone(),
            node_a_route.clone(),
            "test-stale-owner-sink-query-endpoint",
            CancellationToken::new(),
            move |requests| {
                let node_a_groups = node_a_groups_for_handler.clone();
                async move {
                    for req in requests {
                        let params = rmp_serde::from_slice::<InternalQueryRequest>(
                            req.payload_bytes(),
                        )
                        .expect("decode node-a internal query request");
                        node_a_groups
                            .lock()
                            .expect("node-a groups lock")
                            .push(
                                params
                                    .scope
                                    .selected_group
                                    .clone()
                                    .expect("selected group for node-a request"),
                            );
                    }
                    Vec::new()
                }
            },
        );
        let mut node_b_endpoint = ManagedEndpointTask::spawn(
            boundary.clone(),
            node_b_route.clone(),
            "test-scheduled-owner-sink-query-endpoint",
            CancellationToken::new(),
            move |requests| {
                let node_b_groups = node_b_groups_for_handler.clone();
                async move {
                    let mut responses = Vec::new();
                    for req in requests {
                        let params = rmp_serde::from_slice::<InternalQueryRequest>(
                            req.payload_bytes(),
                        )
                        .expect("decode node-b internal query request");
                        let group_id = params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for node-b request");
                        node_b_groups
                            .lock()
                            .expect("node-b groups lock")
                            .push(group_id.clone());
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("node-b request correlation"),
                            real_materialized_tree_payload_for_test(&params.scope.path),
                        ));
                    }
                    responses
                }
            },
        );

        let state = test_api_state_for_route_source(
            source,
            sink,
            boundary.clone(),
            NodeId("node-d".to_string()),
        );

        let result = query_materialized_events(
            state.backend.clone(),
            build_materialized_tree_request(
                b"/retired-layout",
                true,
                None,
                ReadClass::Materialized,
                Some("nfs4".to_string()),
            ),
            Duration::from_millis(250),
        )
        .await;

        assert!(
            result.is_ok(),
            "selected-group materialized route should prefer the sink-scheduled owner over stale source primary; sink_status_calls={} node_a_calls={} node_b_calls={} node_a_groups={:?} node_b_groups={:?} err={:?}",
            boundary.send_batch_count(&sink_status_route.0),
            boundary.send_batch_count(&node_a_route.0),
            boundary.send_batch_count(&node_b_route.0),
            node_a_groups.lock().expect("node-a groups lock").clone(),
            node_b_groups.lock().expect("node-b groups lock").clone(),
            result.as_ref().err(),
        );
        let payload = decode_materialized_selected_group_response(
            &result.expect("scheduled owner route result"),
            &ProjectionPolicy::default(),
            "nfs4",
            b"/retired-layout",
        )
        .expect("decode selected-group scheduled owner response");
        assert!(payload.root.exists);
        assert!(
            node_a_groups
                .lock()
                .expect("node-a groups lock")
                .is_empty(),
            "stale source-primary owner should not receive the selected-group materialized request once sink scheduling points elsewhere"
        );
        assert_eq!(
            node_b_groups.lock().expect("node-b groups lock").as_slice(),
            &["nfs4".to_string()],
            "scheduled sink owner should receive the selected-group materialized request"
        );

        sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
        node_a_endpoint.shutdown(Duration::from_secs(2)).await;
        node_b_endpoint.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn build_tree_pit_session_excludes_unscheduled_groups_after_root_transition() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let node_a_nfs2 = tmp.path().join("node-a-nfs2");
        let node_a_nfs4 = tmp.path().join("node-a-nfs4");
        fs::create_dir_all(node_a_nfs2.join("live-layout")).expect("create node-a nfs2 dir");
        fs::create_dir_all(node_a_nfs4.join("retired-layout")).expect("create node-a nfs4 dir");
        let grants = vec![
            GrantedMountRoot {
                object_ref: "node-a::nfs2".to_string(),
                host_ref: "node-a".to_string(),
                host_ip: "10.0.0.1".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: std::collections::BTreeMap::new(),
                mount_point: node_a_nfs2,
                fs_source: "nfs2".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: Vec::new(),
                active: true,
            },
            GrantedMountRoot {
                object_ref: "node-a::nfs4".to_string(),
                host_ref: "node-a".to_string(),
                host_ip: "10.0.0.1".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: std::collections::BTreeMap::new(),
                mount_point: node_a_nfs4,
                fs_source: "nfs4".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: Vec::new(),
                active: true,
            },
        ];
        let mut nfs2 = RootSpec::new("nfs2", "/unused");
        nfs2.selector = crate::source::config::RootSelector {
            fs_type: Some("nfs".to_string()),
            ..crate::source::config::RootSelector::default()
        };
        nfs2.subpath_scope = std::path::PathBuf::from("/");
        let mut nfs4 = RootSpec::new("nfs4", "/unused");
        nfs4.selector = crate::source::config::RootSelector {
            fs_type: Some("nfs".to_string()),
            ..crate::source::config::RootSelector::default()
        };
        nfs4.subpath_scope = std::path::PathBuf::from("/");
        let source = source_facade_with_roots(vec![nfs2, nfs4], &grants);
        let sink = sink_facade_with_group(&grants);
        let boundary = Arc::new(ReusableObservedRouteBoundary::default());
        let sink_status_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
            .expect("resolve sink-status route");
        let node_a_route = sink_query_request_route_for("node-a");
        let routed_groups = Arc::new(Mutex::new(Vec::<String>::new()));
        let routed_groups_for_handler = routed_groups.clone();
        let status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs2".to_string()],
            )]),
            groups: vec![sink_group_status("nfs2", true)],
            ..SinkStatusSnapshot::default()
        })
        .expect("encode sink status snapshot");

        let mut sink_status_endpoint = ManagedEndpointTask::spawn(
            boundary.clone(),
            sink_status_route.clone(),
            "test-sink-status-endpoint",
            CancellationToken::new(),
            move |requests| {
                let status_payload = status_payload.clone();
                async move {
                    requests
                        .into_iter()
                        .map(|req| {
                            mk_event_with_correlation(
                                "node-a",
                                req.metadata()
                                    .correlation_id
                                    .expect("sink-status request correlation"),
                                status_payload.clone(),
                            )
                        })
                        .collect::<Vec<_>>()
                }
            },
        );
        let mut node_a_endpoint = ManagedEndpointTask::spawn(
            boundary.clone(),
            node_a_route.clone(),
            "test-owner-sink-query-endpoint",
            CancellationToken::new(),
            move |requests| {
                let routed_groups = routed_groups_for_handler.clone();
                async move {
                    let mut responses = Vec::new();
                    for req in requests {
                        let params = rmp_serde::from_slice::<InternalQueryRequest>(
                            req.payload_bytes(),
                        )
                        .expect("decode node-a internal query request");
                        let group_id = params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for node-a request");
                        routed_groups
                            .lock()
                            .expect("routed groups lock")
                            .push(group_id.clone());
                        if group_id == "nfs2" {
                            responses.push(mk_event_with_correlation(
                                &group_id,
                                req.metadata()
                                    .correlation_id
                                    .expect("node-a request correlation"),
                                real_materialized_tree_payload_for_test(&params.scope.path),
                            ));
                        }
                    }
                    responses
                }
            },
        );

        let state = ApiState {
            backend: QueryBackend::Route {
                sink: sink.clone(),
                boundary: boundary.clone(),
                origin_id: NodeId("node-d".to_string()),
                source: source.clone(),
            },
            policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
            force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
            force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
            readiness_source: Some(source),
            readiness_sink: Some(sink),
            materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        };
        let params = NormalizedApiParams {
            path: b"/retired-layout".to_vec(),
            group: None,
            recursive: true,
            max_depth: None,
            pit_id: None,
            group_order: GroupOrder::GroupKey,
            group_page_size: Some(GROUP_PAGE_SIZE_DEFAULT),
            group_after: None,
            entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
            entry_after: None,
            read_class: Some(ReadClass::Materialized),
        };

        let session = build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(250),
            ObservationStatus::fresh_only(),
        )
        .await
        .expect("build tree pit session");

        assert_eq!(
            session
                .groups
                .iter()
                .map(|group| group.group.as_str())
                .collect::<Vec<_>>(),
            vec!["nfs2"],
            "tree PIT should exclude groups that current sink scheduling does not bind/serve; sink_status_calls={} routed_groups={:?}",
            boundary.send_batch_count(&sink_status_route.0),
            routed_groups.lock().expect("routed groups lock").clone(),
        );
        assert_eq!(
            routed_groups.lock().expect("routed groups lock").as_slice(),
            &["nfs2".to_string()],
            "materialized tree PIT should only route the currently scheduled group"
        );

        sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
        node_a_endpoint.shutdown(Duration::from_secs(2)).await;
    }

    #[test]
    fn materialized_owner_node_for_group_tracks_group_primary_by_group() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let root_a = tmp.path().join("node-a");
        let root_b = tmp.path().join("node-b");
        fs::create_dir_all(&root_a).expect("create node-a dir");
        fs::create_dir_all(&root_b).expect("create node-b dir");

        let grants = vec![
            GrantedMountRoot {
                object_ref: "node-a::nfs1".to_string(),
                host_ref: "node-a".to_string(),
                host_ip: "10.0.0.1".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: std::collections::BTreeMap::new(),
                mount_point: root_a.clone(),
                fs_source: "server:/nfs1".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: Vec::new(),
                active: true,
            },
            GrantedMountRoot {
                object_ref: "node-b::nfs2".to_string(),
                host_ref: "node-b".to_string(),
                host_ip: "10.0.0.2".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: std::collections::BTreeMap::new(),
                mount_point: root_b.clone(),
                fs_source: "server:/nfs2".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: Vec::new(),
                active: true,
            },
        ];
        let source = source_facade_with_roots(
            vec![
                RootSpec::new("nfs1", &root_a),
                RootSpec::new("nfs2", &root_b),
            ],
            &grants,
        );

        let nfs1 = crate::runtime_app::shared_tokio_runtime()
            .block_on(materialized_owner_node_for_group(source.as_ref(), None, "nfs1"))
            .expect("resolve nfs1 owner")
            .expect("nfs1 owner");
        let nfs2 = crate::runtime_app::shared_tokio_runtime()
            .block_on(materialized_owner_node_for_group(source.as_ref(), None, "nfs2"))
            .expect("resolve nfs2 owner")
            .expect("nfs2 owner");

        assert_eq!(nfs1.0, "node-a");
        assert_eq!(nfs2.0, "node-b");
    }

    #[test]
    fn resolve_force_find_groups_uses_local_source_snapshot_and_filters_scan_disabled_roots() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let root_a = tmp.path().join("nfs1");
        let root_b = tmp.path().join("nfs2");
        let root_c = tmp.path().join("nfs3");
        fs::create_dir_all(&root_a).expect("create nfs1 dir");
        fs::create_dir_all(&root_b).expect("create nfs2 dir");
        fs::create_dir_all(&root_c).expect("create nfs3 dir");

        let grants = vec![
            granted_mount_root("node-a::nfs1", &root_a),
            granted_mount_root("node-b::nfs2", &root_b),
            granted_mount_root("node-c::nfs3", &root_c),
        ];
        let mut nfs3 = RootSpec::new("nfs3", &root_c);
        nfs3.scan = false;
        let source = source_facade_with_roots(
            vec![
                RootSpec::new("nfs1", &root_a),
                RootSpec::new("nfs2", &root_b),
                nfs3,
            ],
            &grants,
        );
        let sink = sink_facade_with_group(&grants);
        let state = test_api_state_for_source(source, sink);
        let params = NormalizedApiParams {
            path: b"/".to_vec(),
            group: None,
            recursive: true,
            max_depth: None,
            pit_id: None,
            group_order: GroupOrder::default(),
            group_page_size: Some(GROUP_PAGE_SIZE_DEFAULT),
            group_after: None,
            entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
            entry_after: None,
            read_class: None,
        };

        let groups = crate::runtime_app::shared_tokio_runtime()
            .block_on(resolve_force_find_groups(&state, &params))
            .expect("resolve groups");

        assert_eq!(groups, vec!["nfs1".to_string(), "nfs2".to_string()]);
    }

    #[test]
    fn query_responses_from_source_events_keeps_prefixed_find_rows() {
        let query_path = b"/qf-e2e-job";
        let events = vec![
            mk_source_record_event(
                "node-a::nfs1",
                b"/tmp/capanix/data/nfs1/qf-e2e-job/file-a.txt",
                b"file-a.txt",
                10,
            ),
            mk_source_record_event(
                "node-a::nfs1",
                b"/tmp/capanix/data/nfs1/qf-e2e-job/file-b.txt",
                b"file-b.txt",
                11,
            ),
        ];

        let grouped = query_responses_by_origin_from_source_events(&events, query_path)
            .expect("build grouped query responses");
        let response = grouped.get("node-a::nfs1").expect("origin response exists");
        let mut paths = response
            .entries
            .iter()
            .map(|node| String::from_utf8_lossy(&node.path).to_string())
            .collect::<Vec<_>>();
        paths.sort();
        assert_eq!(
            paths,
            vec![
                "/qf-e2e-job/file-a.txt".to_string(),
                "/qf-e2e-job/file-b.txt".to_string()
            ]
        );
    }

    #[test]
    fn max_total_files_from_stats_events_uses_highest_total_files() {
        let mk_stats_event = |origin: &str, total_files: u64| -> Event {
            let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
                total_files,
                ..SubtreeStats::default()
            }))
            .expect("encode stats");
            mk_event(origin, payload)
        };

        let events = vec![mk_stats_event("n1", 3), mk_stats_event("n2", 8)];
        assert_eq!(max_total_files_from_stats_events(&events), Some(8));
    }

    #[test]
    fn latest_file_mtime_from_stats_events_uses_newest_file_mtime() {
        let mk_stats_event = |origin: &str, mtime: Option<u64>| -> Event {
            let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
                latest_file_mtime_us: mtime,
                ..SubtreeStats::default()
            }))
            .expect("encode stats");
            mk_event(origin, payload)
        };

        let events = vec![
            mk_stats_event("n1", Some(7)),
            mk_stats_event("n2", Some(11)),
            mk_stats_event("n3", None),
            mk_event("n3", b"bad-msgpack".to_vec()),
        ];
        assert_eq!(latest_file_mtime_from_stats_events(&events), Some(11));
    }

    #[test]
    fn error_response_maps_protocol_violation_to_bad_gateway() {
        let response = error_response(CnxError::ProtocolViolation("x".into()));
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn error_response_maps_timeout_to_gateway_timeout() {
        let response = error_response(CnxError::Timeout);
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    }

    #[test]
    fn error_response_maps_internal_to_internal_server_error() {
        let response = error_response(CnxError::Internal("x".into()));
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn error_response_maps_transport_closed_to_service_unavailable() {
        let response = error_response(CnxError::TransportClosed("x".into()));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn error_response_maps_peer_error_to_bad_gateway() {
        let response = error_response(CnxError::PeerError("x".into()));
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn error_response_maps_invalid_input_to_bad_request() {
        let response = error_response(CnxError::InvalidInput("x".into()));
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn error_response_maps_force_find_inflight_conflict_to_too_many_requests() {
        let response = error_response(CnxError::NotReady(format!(
            "{FORCE_FIND_INFLIGHT_CONFLICT_PREFIX} force-find already running for group: nfs1"
        )));
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn build_materialized_stats_request_keeps_shape() {
        let params = build_materialized_stats_request(b"/a", true, Some("sink-a".into()));
        assert_eq!(params.op, QueryOp::Stats);
        assert_eq!(params.scope.path, b"/a");
        assert!(params.scope.recursive);
        assert_eq!(params.scope.selected_group.as_deref(), Some("sink-a"));
    }

    #[test]
    fn build_materialized_tree_request_sets_selected_group() {
        let params = build_materialized_tree_request(
            b"/a",
            false,
            Some(2),
            ReadClass::TrustedMaterialized,
            Some("sink-a".to_string()),
        );
        assert_eq!(params.op, QueryOp::Tree);
        assert_eq!(params.scope.path, b"/a");
        assert!(!params.scope.recursive);
        assert_eq!(params.scope.max_depth, Some(2));
        let tree_options = params.tree_options.expect("tree options");
        assert_eq!(tree_options.read_class, ReadClass::TrustedMaterialized);
        assert_eq!(params.scope.selected_group.as_deref(), Some("sink-a"));
    }

    #[test]
    fn normalize_api_params_uses_defaults() {
        let params = ApiParams {
            path: None,
            path_b64: None,
            group: None,
            recursive: None,
            max_depth: None,
            pit_id: None,
            group_order: None,
            group_page_size: None,
            group_after: None,
            entry_page_size: None,
            entry_after: None,
            read_class: None,
        };
        let normalized = normalize_api_params(params).expect("normalize defaults");
        assert_eq!(normalized.path, b"/".to_vec());
        assert_eq!(normalized.group, None);
        assert!(normalized.recursive);
        assert_eq!(normalized.max_depth, None);
        assert_eq!(normalized.pit_id, None);
        assert_eq!(normalized.group_order, GroupOrder::GroupKey);
        assert_eq!(normalized.group_page_size, None);
        assert_eq!(normalized.group_after, None);
        assert_eq!(normalized.entry_page_size, None);
        assert_eq!(normalized.entry_after, None);
        assert_eq!(normalized.read_class, None);
    }

    #[test]
    fn normalize_api_params_keeps_group_pagination_shape() {
        let params = ApiParams {
            path: Some("/mnt".into()),
            path_b64: None,
            group: None,
            recursive: Some(false),
            max_depth: Some(3),
            pit_id: Some("pit-1".into()),
            group_order: Some(GroupOrder::FileAge),
            group_page_size: Some(25),
            group_after: Some("group-cursor-1".into()),
            entry_page_size: Some(100),
            entry_after: Some("entry-cursor-bundle-1".into()),
            read_class: Some(ReadClass::Materialized),
        };
        let normalized = normalize_api_params(params).expect("normalize explicit params");
        assert_eq!(normalized.path, b"/mnt".to_vec());
        assert_eq!(normalized.group, None);
        assert!(!normalized.recursive);
        assert_eq!(normalized.max_depth, Some(3));
        assert_eq!(normalized.pit_id.as_deref(), Some("pit-1"));
        assert_eq!(normalized.group_order, GroupOrder::FileAge);
        assert_eq!(normalized.group_page_size, Some(25));
        assert_eq!(normalized.group_after.as_deref(), Some("group-cursor-1"));
        assert_eq!(normalized.entry_page_size, Some(100));
        assert_eq!(
            normalized.entry_after.as_deref(),
            Some("entry-cursor-bundle-1")
        );
        assert_eq!(normalized.read_class, Some(ReadClass::Materialized));
    }

    #[test]
    fn normalize_api_params_decodes_path_b64() {
        let params = ApiParams {
            path: None,
            path_b64: Some(B64URL.encode(b"/bad/\xffname")),
            group: None,
            recursive: None,
            max_depth: None,
            pit_id: None,
            group_order: None,
            group_page_size: None,
            group_after: None,
            entry_page_size: None,
            entry_after: None,
            read_class: None,
        };
        let normalized = normalize_api_params(params).expect("normalize path_b64");
        assert_eq!(normalized.path, b"/bad/\xffname".to_vec());
    }

    #[test]
    fn normalize_api_params_rejects_path_and_path_b64_together() {
        let params = ApiParams {
            path: Some("/mnt".into()),
            path_b64: Some(B64URL.encode(b"/mnt")),
            group: None,
            recursive: None,
            max_depth: None,
            pit_id: None,
            group_order: None,
            group_page_size: None,
            group_after: None,
            entry_page_size: None,
            entry_after: None,
            read_class: None,
        };
        let err = normalize_api_params(params).expect_err("reject mixed path inputs");
        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn tree_json_includes_bytes_safe_fields() {
        let root = TreePageRoot {
            path: b"/bad/\xffname".to_vec(),
            size: 1,
            modified_time_us: 2,
            is_dir: false,
            exists: true,
            has_children: false,
        };
        let entry = TreePageEntry {
            path: b"/bad/\xffname".to_vec(),
            depth: 1,
            size: 1,
            modified_time_us: 2,
            is_dir: false,
            has_children: false,
        };

        let root_json = tree_root_json(&root);
        assert_eq!(
            root_json["path_b64"],
            serde_json::json!(B64URL.encode(b"/bad/\xffname"))
        );
        assert_eq!(
            root_json["path"],
            serde_json::json!(path_to_string_lossy(b"/bad/\xffname"))
        );

        let entry_json = tree_entry_json(&entry);
        assert_eq!(
            entry_json["path_b64"],
            serde_json::json!(B64URL.encode(b"/bad/\xffname"))
        );
    }

    #[test]
    fn tree_json_omits_bytes_safe_fields_for_utf8_names() {
        let root = TreePageRoot {
            path: b"/utf8/hello.txt".to_vec(),
            size: 1,
            modified_time_us: 2,
            is_dir: false,
            exists: true,
            has_children: false,
        };
        let entry = TreePageEntry {
            path: b"/utf8/hello.txt".to_vec(),
            depth: 1,
            size: 1,
            modified_time_us: 2,
            is_dir: false,
            has_children: false,
        };

        let root_json = tree_root_json(&root);
        assert!(root_json.get("path_b64").is_none());

        let entry_json = tree_entry_json(&entry);
        assert!(entry_json.get("path_b64").is_none());
    }

    #[test]
    fn decode_stats_groups_keeps_decode_error_group() {
        let ok_payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
            total_files: 1,
            ..SubtreeStats::default()
        }))
        .expect("encode stats");
        let events = vec![
            mk_event("n1", ok_payload),
            mk_event("n2", b"bad-msgpack".to_vec()),
        ];
        let groups = decode_stats_groups(events, &origin_policy(), None, ReadClass::Materialized);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups["n1"]["status"], "ok");
        assert_eq!(groups["n2"]["status"], "error");
    }

    #[test]
    fn decode_stats_groups_preserves_utf8_group_keys_exactly() {
        let composed = "café-👩🏽‍💻";
        let decomposed = "cafe\u{301}-👩🏽‍💻";
        let mk_stats_event = |origin: &str| -> Event {
            let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
                total_files: 1,
                ..SubtreeStats::default()
            }))
            .expect("encode stats");
            mk_event(origin, payload)
        };

        let groups = decode_stats_groups(
            vec![mk_stats_event(composed), mk_stats_event(decomposed)],
            &origin_policy(),
            None,
            ReadClass::Materialized,
        );

        assert!(groups.contains_key(composed));
        assert!(groups.contains_key(decomposed));
        assert_ne!(composed, decomposed);
        assert_eq!(groups[composed]["status"], "ok");
        assert_eq!(groups[decomposed]["status"], "ok");
    }

    #[test]
    fn materialized_query_readiness_waits_for_initial_audit_completion() {
        let source_status = SourceStatusSnapshot {
            current_stream_generation: None,
            logical_roots: Vec::new(),
            concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-a-key".into(),
                logical_root_id: "root-a".into(),
                object_ref: "obj-a".into(),
                status: "ok".into(),
                coverage_mode: "audit_only".into(),
                watch_enabled: false,
                scan_enabled: true,
                is_group_primary: true,
                active: true,
                watch_lru_capacity: 0,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: Some(10),
                last_audit_completed_at_us: Some(20),
                last_audit_duration_ms: Some(1),
                emitted_batch_count: 0,
                emitted_event_count: 0,
                emitted_control_event_count: 0,
                emitted_data_event_count: 0,
                emitted_path_capture_target: None,
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
            }],
            degraded_roots: Vec::new(),
        };
        let sink_status = SinkStatusSnapshot {
            live_nodes: 0,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            estimated_heap_bytes: 0,
            scheduled_groups_by_node: BTreeMap::new(),
            last_control_frame_signals_by_node: BTreeMap::new(),
            groups: vec![crate::sink::SinkGroupStatusSnapshot {
                group_id: "root-a".into(),
                primary_object_ref: "obj-a".into(),
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

        let err = materialized_query_readiness_error(&source_status, &sink_status)
            .expect("initial audit should gate materialized queries");
        assert!(err.contains("initial audit incomplete"));
        assert!(err.contains("root-a"));
    }

    #[test]
    fn materialized_query_readiness_fail_closed_when_sink_group_missing() {
        let source_status = SourceStatusSnapshot {
            current_stream_generation: None,
            logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "root-a".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "audit_only".into(),
            }],
            concrete_roots: Vec::new(),
            degraded_roots: Vec::new(),
        };
        let sink_status = SinkStatusSnapshot::default();

        let err = materialized_query_readiness_error(&source_status, &sink_status)
            .expect("missing sink group must gate materialized queries");
        assert!(err.contains("initial audit incomplete"));
        assert!(err.contains("root-a"));
    }

    #[test]
    fn materialized_query_readiness_ignores_inactive_or_non_scan_groups() {
        let source_status = SourceStatusSnapshot {
            current_stream_generation: None,
            logical_roots: Vec::new(),
            concrete_roots: vec![
                crate::source::SourceConcreteRootHealthSnapshot {
                    root_key: "root-a-key".into(),
                    logical_root_id: "root-a".into(),
                    object_ref: "obj-a".into(),
                    status: "ok".into(),
                    coverage_mode: "watch_only".into(),
                    watch_enabled: true,
                    scan_enabled: false,
                    is_group_primary: true,
                    active: true,
                    watch_lru_capacity: 128,
                    audit_interval_ms: 10_000,
                    overflow_count: 0,
                    overflow_pending: false,
                    rescan_pending: false,
                    last_rescan_reason: None,
                    last_error: None,
                    last_audit_started_at_us: None,
                    last_audit_completed_at_us: None,
                    last_audit_duration_ms: None,
                    emitted_batch_count: 0,
                    emitted_event_count: 0,
                    emitted_control_event_count: 0,
                    emitted_data_event_count: 0,
                    emitted_path_capture_target: None,
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
                },
                crate::source::SourceConcreteRootHealthSnapshot {
                    root_key: "root-b-key".into(),
                    logical_root_id: "root-b".into(),
                    object_ref: "obj-b".into(),
                    status: "ok".into(),
                    coverage_mode: "audit_only".into(),
                    watch_enabled: false,
                    scan_enabled: true,
                    is_group_primary: true,
                    active: false,
                    watch_lru_capacity: 0,
                    audit_interval_ms: 10_000,
                    overflow_count: 0,
                    overflow_pending: false,
                    rescan_pending: false,
                    last_rescan_reason: None,
                    last_error: None,
                    last_audit_started_at_us: None,
                    last_audit_completed_at_us: None,
                    last_audit_duration_ms: None,
                    emitted_batch_count: 0,
                    emitted_event_count: 0,
                    emitted_control_event_count: 0,
                    emitted_data_event_count: 0,
                    emitted_path_capture_target: None,
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
                },
            ],
            degraded_roots: Vec::new(),
        };
        let sink_status = SinkStatusSnapshot::default();
        assert!(materialized_query_readiness_error(&source_status, &sink_status).is_none());
    }

    #[test]
    fn filter_source_status_snapshot_drops_groups_outside_current_roots() {
        let snapshot = SourceStatusSnapshot {
            current_stream_generation: None,
            logical_roots: vec![
                crate::source::SourceLogicalRootHealthSnapshot {
                    root_id: "root-a".into(),
                    status: "ok".into(),
                    matched_grants: 1,
                    active_members: 1,
                    coverage_mode: "audit_only".into(),
                },
                crate::source::SourceLogicalRootHealthSnapshot {
                    root_id: "root-b".into(),
                    status: "ok".into(),
                    matched_grants: 1,
                    active_members: 1,
                    coverage_mode: "audit_only".into(),
                },
            ],
            concrete_roots: vec![
                crate::source::SourceConcreteRootHealthSnapshot {
                    root_key: "root-a-key".into(),
                    logical_root_id: "root-a".into(),
                    object_ref: "obj-a".into(),
                    status: "ok".into(),
                    coverage_mode: "audit_only".into(),
                    watch_enabled: false,
                    scan_enabled: true,
                    is_group_primary: true,
                    active: true,
                    watch_lru_capacity: 0,
                    audit_interval_ms: 10_000,
                    overflow_count: 0,
                    overflow_pending: false,
                    rescan_pending: false,
                    last_rescan_reason: None,
                    last_error: None,
                    last_audit_started_at_us: None,
                    last_audit_completed_at_us: None,
                    last_audit_duration_ms: None,
                    emitted_batch_count: 0,
                    emitted_event_count: 0,
                    emitted_control_event_count: 0,
                    emitted_data_event_count: 0,
                    emitted_path_capture_target: None,
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
                },
                crate::source::SourceConcreteRootHealthSnapshot {
                    root_key: "root-b-key".into(),
                    logical_root_id: "root-b".into(),
                    object_ref: "obj-b".into(),
                    status: "ok".into(),
                    coverage_mode: "audit_only".into(),
                    watch_enabled: false,
                    scan_enabled: true,
                    is_group_primary: true,
                    active: true,
                    watch_lru_capacity: 0,
                    audit_interval_ms: 10_000,
                    overflow_count: 0,
                    overflow_pending: false,
                    rescan_pending: false,
                    last_rescan_reason: None,
                    last_error: None,
                    last_audit_started_at_us: None,
                    last_audit_completed_at_us: None,
                    last_audit_duration_ms: None,
                    emitted_batch_count: 0,
                    emitted_event_count: 0,
                    emitted_control_event_count: 0,
                    emitted_data_event_count: 0,
                    emitted_path_capture_target: None,
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
                },
            ],
            degraded_roots: vec![
                ("root-a".into(), "degraded".into()),
                ("root-b".into(), "degraded".into()),
            ],
        };
        let filtered =
            filter_source_status_snapshot(snapshot, &BTreeSet::from(["root-b".to_string()]));
        assert_eq!(filtered.logical_roots.len(), 1);
        assert_eq!(filtered.logical_roots[0].root_id, "root-b");
        assert_eq!(filtered.concrete_roots.len(), 1);
        assert_eq!(filtered.concrete_roots[0].logical_root_id, "root-b");
        assert_eq!(
            filtered.degraded_roots,
            vec![("root-b".to_string(), "degraded".to_string())]
        );
    }

    #[test]
    fn materialized_query_readiness_ignores_stale_source_groups_outside_current_roots() {
        let source_status = SourceStatusSnapshot {
            current_stream_generation: None,
            logical_roots: vec![
                crate::source::SourceLogicalRootHealthSnapshot {
                    root_id: "root-a".into(),
                    status: "ok".into(),
                    matched_grants: 1,
                    active_members: 1,
                    coverage_mode: "audit_only".into(),
                },
                crate::source::SourceLogicalRootHealthSnapshot {
                    root_id: "root-b".into(),
                    status: "ok".into(),
                    matched_grants: 1,
                    active_members: 1,
                    coverage_mode: "audit_only".into(),
                },
            ],
            concrete_roots: vec![
                crate::source::SourceConcreteRootHealthSnapshot {
                    root_key: "root-a-key".into(),
                    logical_root_id: "root-a".into(),
                    object_ref: "obj-a".into(),
                    status: "ok".into(),
                    coverage_mode: "audit_only".into(),
                    watch_enabled: false,
                    scan_enabled: true,
                    is_group_primary: true,
                    active: true,
                    watch_lru_capacity: 0,
                    audit_interval_ms: 10_000,
                    overflow_count: 0,
                    overflow_pending: false,
                    rescan_pending: false,
                    last_rescan_reason: None,
                    last_error: None,
                    last_audit_started_at_us: None,
                    last_audit_completed_at_us: None,
                    last_audit_duration_ms: None,
                    emitted_batch_count: 0,
                    emitted_event_count: 0,
                    emitted_control_event_count: 0,
                    emitted_data_event_count: 0,
                    emitted_path_capture_target: None,
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
                },
                crate::source::SourceConcreteRootHealthSnapshot {
                    root_key: "root-b-key".into(),
                    logical_root_id: "root-b".into(),
                    object_ref: "obj-b".into(),
                    status: "ok".into(),
                    coverage_mode: "audit_only".into(),
                    watch_enabled: false,
                    scan_enabled: true,
                    is_group_primary: true,
                    active: true,
                    watch_lru_capacity: 0,
                    audit_interval_ms: 10_000,
                    overflow_count: 0,
                    overflow_pending: false,
                    rescan_pending: false,
                    last_rescan_reason: None,
                    last_error: None,
                    last_audit_started_at_us: None,
                    last_audit_completed_at_us: None,
                    last_audit_duration_ms: None,
                    emitted_batch_count: 0,
                    emitted_event_count: 0,
                    emitted_control_event_count: 0,
                    emitted_data_event_count: 0,
                    emitted_path_capture_target: None,
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
                },
            ],
            degraded_roots: Vec::new(),
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![crate::sink::SinkGroupStatusSnapshot {
                group_id: "root-b".into(),
                primary_object_ref: "obj-b".into(),
                total_nodes: 4,
                live_nodes: 4,
                tombstoned_count: 0,
                attested_count: 4,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 1,
                shadow_lag_us: 0,
                overflow_pending_audit: false,
                initial_audit_completed: true,
                materialized_revision: 1,
                estimated_heap_bytes: 1,
            }],
            ..SinkStatusSnapshot::default()
        };
        let filtered_source =
            filter_source_status_snapshot(source_status, &BTreeSet::from(["root-b".to_string()]));
        assert!(materialized_query_readiness_error(&filtered_source, &sink_status).is_none());
    }

    #[tokio::test]
    async fn rpc_metrics_endpoint_returns_ok() {
        let resp = get_bound_route_metrics().await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION", mode="system")
    // @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
    #[tokio::test]
    async fn force_find_group_order_file_count_top_bucket_local() {
        let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
        let req = Request::builder()
            .uri("/on-demand-force-find?path=/&group_order=file-count&group_page_size=1")
            .method("GET")
            .body(Body::empty())
            .expect("build request");

        let resp = fixture.app.oneshot(req).await.expect("serve request");
        assert_eq!(resp.status(), StatusCode::OK);

        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("response body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
        assert_eq!(json["group_order"], "file-count");
        assert_eq!(json["status"], "ok");
        assert_eq!(json["group_page"]["returned_groups"], 1);
        assert_eq!(json["groups"][0]["group"], "sink-b");
        assert!(
            json["groups"][0]["entries"]
                .as_array()
                .is_some_and(|entries| entries.iter().any(|entry| {
                    entry["path"]
                        .as_str()
                        .is_some_and(|path| path.ends_with("/winner-b"))
                }))
        );
        assert_eq!(json["groups"][0]["stability"]["state"], "not-evaluated");
    }

    // @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION", mode="system")
    // @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
    #[tokio::test]
    async fn force_find_group_order_file_age_top_bucket_local() {
        let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
        let req = Request::builder()
            .uri("/on-demand-force-find?path=/&group_order=file-age&group_page_size=1")
            .method("GET")
            .body(Body::empty())
            .expect("build request");

        let resp = fixture.app.oneshot(req).await.expect("serve request");
        assert_eq!(resp.status(), StatusCode::OK);

        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("response body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
        assert_eq!(json["group_order"], "file-age");
        assert_eq!(json["status"], "ok");
        assert_eq!(json["group_page"]["returned_groups"], 1);
        assert_eq!(json["groups"][0]["group"], "sink-a");
        assert!(
            json["groups"][0]["entries"]
                .as_array()
                .is_some_and(|entries| entries.iter().any(|entry| {
                    entry["path"]
                        .as_str()
                        .is_some_and(|path| path.ends_with("/winner-a"))
                }))
        );
    }

    // @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION", mode="system")
    // @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
    #[tokio::test]
    async fn force_find_group_order_file_age_keeps_empty_groups_eligible_local() {
        let fixture = ForceFindFixture::new(ForceFindFixtureScenario::FileAgeNoFiles);
        let req = Request::builder()
            .uri("/on-demand-force-find?path=/&group_order=file-age&group_page_size=1")
            .method("GET")
            .body(Body::empty())
            .expect("build request");

        let resp = fixture.app.oneshot(req).await.expect("serve request");
        assert_eq!(resp.status(), StatusCode::OK);

        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("response body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
        assert_eq!(json["group_order"], "file-age");
        assert_eq!(json["status"], "ok");
        assert_eq!(json["group_page"]["returned_groups"], 1);
        assert_eq!(json["groups"][0]["group"], "sink-a");
        assert!(
            json["groups"][0]["entries"]
                .as_array()
                .is_some_and(|entries| entries.iter().any(|entry| {
                    entry["path"]
                        .as_str()
                        .is_some_and(|path| path.ends_with("/empty-a"))
                }))
        );
    }

    // @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
    // @verify_spec("CONTRACTS.QUERY_OUTCOME.NO_CROSS_GROUP_ENTRY_MERGE", mode="system")
    #[tokio::test]
    async fn force_find_defaults_to_group_key_multi_group_response_local() {
        let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
        let req = Request::builder()
            .uri("/on-demand-force-find?path=/")
            .method("GET")
            .body(Body::empty())
            .expect("build request");

        let resp = fixture.app.oneshot(req).await.expect("serve request");
        assert_eq!(resp.status(), StatusCode::OK);

        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("response body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
        assert_eq!(json["group_order"], "group-key");
        assert!(
            json["groups"]
                .as_array()
                .is_some_and(|groups| !groups.is_empty())
        );
        assert!(
            json["group_page"]["returned_groups"]
                .as_u64()
                .unwrap_or_default()
                >= 1
        );
    }

    // @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
    #[tokio::test]
    async fn force_find_explicit_group_returns_only_that_group_local() {
        let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
        let req = Request::builder()
            .uri("/on-demand-force-find?path=/&group=sink-b")
            .method("GET")
            .body(Body::empty())
            .expect("build request");

        let resp = fixture.app.oneshot(req).await.expect("serve request");
        assert_eq!(resp.status(), StatusCode::OK);

        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("response body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
        assert_eq!(json["group_page"]["returned_groups"], 1);
        let groups = json["groups"].as_array().expect("groups array");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0]["group"], "sink-b");
    }

    // @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_HTTP_FACADE_DEFAULTS_AND_SHAPE", mode="system")
    #[tokio::test]
    async fn force_find_defaults_when_query_params_omitted_local() {
        let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
        let req = Request::builder()
            .uri("/on-demand-force-find")
            .method("GET")
            .body(Body::empty())
            .expect("build request");

        let resp = fixture.app.oneshot(req).await.expect("serve request");
        assert_eq!(resp.status(), StatusCode::OK);

        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("response body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
        assert_eq!(json["group_order"], "group-key");
        assert!(
            json["groups"]
                .as_array()
                .is_some_and(|groups| !groups.is_empty())
        );
        assert_eq!(
            json["group_page"]["returned_groups"],
            json["groups"]
                .as_array()
                .map(|groups| groups.len())
                .unwrap_or_default()
        );
    }

    // @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_BOUND_ROUTE_METRICS_DIAGNOSTICS_BOUNDARY", mode="system")
    #[tokio::test]
    async fn projection_rpc_metrics_endpoint_shape_local() {
        let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
        let req = Request::builder()
            .uri("/bound-route-metrics")
            .method("GET")
            .body(Body::empty())
            .expect("build request");

        let resp = fixture.app.oneshot(req).await.expect("serve request");
        assert_eq!(resp.status(), StatusCode::OK);

        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("response body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
        assert!(payload.get("call_timeout_total").is_some());
        assert!(payload.get("correlation_mismatch_total").is_some());
        assert!(payload.get("uncorrelated_reply_total").is_some());
        assert!(payload.get("recv_loop_iterations").is_some());
        assert!(payload.get("pending_calls").is_some());
    }

    // @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_PARAMETER_AXES_REMAIN_ORTHOGONAL", mode="system")
    // @verify_spec("CONTRACTS.API_BOUNDARY.QUERY_PATH_PARAMETERS_OWN_PAYLOAD_SHAPE", mode="system")
    #[tokio::test]
    async fn force_find_rejects_status_only_and_keeps_pagination_axis_local() {
        let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
        let req = Request::builder()
            .uri("/on-demand-force-find?path=/&group_order=group-key&read_class=trusted-materialized")
            .method("GET")
            .body(Body::empty())
            .expect("build request");

        let resp = fixture.app.oneshot(req).await.expect("serve request");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("response body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json error payload");
        let msg = payload
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        assert!(msg.contains("read_class must be fresh on /on-demand-force-find"));
        assert_eq!(
            payload.get("code").and_then(|v| v.as_str()),
            Some("INVALID_INPUT")
        );
        assert_eq!(payload.get("path"), Some(&serde_json::json!("/")));
    }

    // @verify_spec("CONTRACTS.API_BOUNDARY.API_NON_OWNERSHIP_OF_QUERY_FIND_CHANNEL_PATH", mode="system")
    #[tokio::test]
    async fn namespace_projection_endpoints_removed_local() {
        let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);

        let req_tree = Request::builder()
            .uri("/namespace-tree?path=/peers/sink-b/mnt/nfs1&best=false")
            .method("GET")
            .body(Body::empty())
            .expect("build request");
        let resp_tree = fixture
            .app
            .clone()
            .oneshot(req_tree)
            .await
            .expect("serve namespace tree request");
        assert_eq!(resp_tree.status(), StatusCode::NOT_FOUND);

        let req_force_find = Request::builder()
            .uri(
                "/namespace-on-demand-force-find?path=/peers/sink-b/mnt/nfs1&group_order=group-key",
            )
            .method("GET")
            .body(Body::empty())
            .expect("build request");
        let resp_force_find = fixture
            .app
            .oneshot(req_force_find)
            .await
            .expect("serve namespace force-find request");
        assert_eq!(resp_force_find.status(), StatusCode::NOT_FOUND);
    }
}
