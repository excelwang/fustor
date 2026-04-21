use crate::domain_state::QueryObservationState;
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
use capanix_app_sdk::runtime::{EventMetadata, NodeId};
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
const SELECTED_GROUP_OWNER_ROUTE_COLLECT_IDLE_GRACE: Duration = Duration::from_millis(100);
const SELECTED_GROUP_PROXY_ROUTE_COLLECT_IDLE_GRACE: Duration = Duration::from_millis(100);
const RANKING_QUERY_MIN_BUDGET: Duration = Duration::from_millis(1000);
const SELECTED_GROUP_PROXY_FALLBACK_MIN_BUDGET: Duration = Duration::from_millis(2000);
const TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET: Duration = Duration::from_millis(400);
const TRUSTED_READY_LATER_RANKED_NON_ROOT_RETRY_BUDGET: Duration = Duration::from_millis(100);
const UNREADY_SELECTED_GROUP_ROUTE_BUDGET: Duration = Duration::from_millis(1200);
const EXPLICIT_EMPTY_SINK_STATUS_RECOLLECT_BUDGET: Duration = Duration::from_millis(250);
const REQUEST_SCOPED_SINK_STATUS_LOAD_BUDGET: Duration = Duration::from_millis(500);
const FIRST_RANKED_TRUSTED_READY_GROUP_STAGE_BUDGET: Duration = Duration::from_millis(2500);
const LATER_RANKED_TRUSTED_GROUP_STAGE_BUDGET: Duration = Duration::from_millis(1500);
// Source/sink status fanout drives the trusted-materialized readiness gate.
// Unlike tree payload collection, a partial early return here can falsely
// downgrade healthy groups into "initial audit incomplete". Give peer status
// replies a little more time to settle before closing the collect window.
const STATUS_ROUTE_COLLECT_IDLE_GRACE: Duration = Duration::from_secs(2);
const LOAD_MATERIALIZED_SINK_STATUS_ROUTE_IDLE_GRACE: Duration = Duration::from_millis(250);
const STATUS_ROUTE_ATTEMPT_TIMEOUT_CAP: Duration = Duration::from_secs(5);
const STATUS_ROUTE_RETRY_BACKOFF: Duration = Duration::from_millis(30);
const SELECTED_GROUP_PROXY_ROUTE_RETRY_BACKOFF: Duration = Duration::from_millis(100);
// Force-find is intentionally a slower freshness path. Keep a larger collection
// window here so the request remains in-flight long enough for overlap guards
// and multi-runner aggregation scenarios.
const FORCE_FIND_ROUTE_COLLECT_IDLE_GRACE: Duration = Duration::from_secs(5);
const FORCE_FIND_MIN_INFLIGHT_HOLD: Duration = Duration::from_secs(2);
const FORCE_FIND_ROUTE_RETRY_BACKOFF: Duration = Duration::from_millis(100);
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
    tree_query_serial: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Clone, Debug)]
struct CachedSinkStatusSnapshot {
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

fn build_pit_session(
    mode: CursorQueryMode,
    scope: PitScope,
    read_class: ReadClass,
    observation_status: ObservationStatus,
    groups: Vec<GroupPitSnapshot>,
) -> PitSession {
    let estimated_bytes =
        groups.iter().map(estimated_group_bytes).sum::<usize>() + std::mem::size_of::<PitSession>();
    PitSession {
        mode,
        scope,
        read_class,
        observation_status,
        groups,
        expires_at_ms: unix_now_ms() + PIT_TTL_MS_DEFAULT,
        estimated_bytes,
    }
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
        for (node_id, groups_for_node) in snapshot.scheduled_groups_by_node {
            let entry = scheduled_groups_by_node.entry(node_id).or_default();
            entry.extend(groups_for_node);
            entry.sort();
            entry.dedup();
        }
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
                        u8::from(sink_group_readiness_reports_live_materialized_group(
                            current,
                        )),
                        current.total_nodes,
                        current.live_nodes,
                        current.shadow_time_us,
                    );
                    let incoming_score = (
                        u8::from(sink_group_readiness_reports_live_materialized_group(&group)),
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
                .and_modify(|current| {
                    if source_logical_root_merge_score(&entry)
                        > source_logical_root_merge_score(current)
                    {
                        *current = entry.clone();
                    }
                })
                .or_insert(entry);
        }
        for entry in snapshot.concrete_roots {
            concrete_root_map
                .entry(entry.root_key.clone())
                .and_modify(|current| {
                    if source_concrete_root_merge_score(&entry)
                        > source_concrete_root_merge_score(current)
                    {
                        *current = entry.clone();
                    }
                })
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

fn source_logical_root_merge_score(
    entry: &crate::source::SourceLogicalRootHealthSnapshot,
) -> (u8, usize, usize) {
    (
        u8::from(entry.active_members > 0),
        entry.active_members,
        entry.matched_grants,
    )
}

fn source_concrete_root_merge_score(
    entry: &crate::source::SourceConcreteRootHealthSnapshot,
) -> (u8, u8, u8, u8, u64, u64) {
    (
        u8::from(entry.active),
        u8::from(entry.is_group_primary),
        u8::from(entry.scan_enabled),
        u8::from(!entry.rescan_pending),
        entry.current_stream_generation.unwrap_or_default(),
        entry.current_revision.unwrap_or_default(),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StatusRoutePlan {
    route_timeout: Duration,
    attempt_timeout_cap: Duration,
    collect_idle_grace: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RouteTerminalError {
    AccessDenied(String),
    InvalidAuthContext(String),
    SignatureInvalid(String),
    ReplayDetected(String),
    ScopeDenied(String),
    NotSupported(String),
    InvalidManifest(String),
    Backpressure,
    ChannelClosed,
    ResourceExhausted(String),
    Timeout,
    NotReady(String),
    Internal(String),
    ProtocolViolation(String),
    LinkError(String),
    TransportClosed(String),
    PeerError(String),
    InvalidInput(String),
    TxInvalid(String),
    TxPrecondition(String),
    TxCommitFailed(String),
    TxBusy,
}

impl RouteTerminalError {
    fn from_error(err: CnxError) -> Self {
        match err {
            CnxError::AccessDenied(message) => Self::AccessDenied(message),
            CnxError::InvalidAuthContext(message) => Self::InvalidAuthContext(message),
            CnxError::SignatureInvalid(message) => Self::SignatureInvalid(message),
            CnxError::ReplayDetected(message) => Self::ReplayDetected(message),
            CnxError::ScopeDenied(message) => Self::ScopeDenied(message),
            CnxError::NotSupported(message) => Self::NotSupported(message),
            CnxError::InvalidManifest(message) => Self::InvalidManifest(message),
            CnxError::Backpressure => Self::Backpressure,
            CnxError::ChannelClosed => Self::ChannelClosed,
            CnxError::ResourceExhausted(message) => Self::ResourceExhausted(message),
            CnxError::Timeout => Self::Timeout,
            CnxError::NotReady(message) => Self::NotReady(message),
            CnxError::Internal(message) => Self::Internal(message),
            CnxError::ProtocolViolation(message) => Self::ProtocolViolation(message),
            CnxError::LinkError(message) => Self::LinkError(message),
            CnxError::TransportClosed(message) => Self::TransportClosed(message),
            CnxError::PeerError(message) => Self::PeerError(message),
            CnxError::InvalidInput(message) => Self::InvalidInput(message),
            CnxError::TxInvalid(message) => Self::TxInvalid(message),
            CnxError::TxPrecondition(message) => Self::TxPrecondition(message),
            CnxError::TxCommitFailed(message) => Self::TxCommitFailed(message),
            CnxError::TxBusy => Self::TxBusy,
        }
    }

    fn into_error(self) -> CnxError {
        match self {
            Self::AccessDenied(message) => CnxError::AccessDenied(message),
            Self::InvalidAuthContext(message) => CnxError::InvalidAuthContext(message),
            Self::SignatureInvalid(message) => CnxError::SignatureInvalid(message),
            Self::ReplayDetected(message) => CnxError::ReplayDetected(message),
            Self::ScopeDenied(message) => CnxError::ScopeDenied(message),
            Self::NotSupported(message) => CnxError::NotSupported(message),
            Self::InvalidManifest(message) => CnxError::InvalidManifest(message),
            Self::Backpressure => CnxError::Backpressure,
            Self::ChannelClosed => CnxError::ChannelClosed,
            Self::ResourceExhausted(message) => CnxError::ResourceExhausted(message),
            Self::Timeout => CnxError::Timeout,
            Self::NotReady(message) => CnxError::NotReady(message),
            Self::Internal(message) => CnxError::Internal(message),
            Self::ProtocolViolation(message) => CnxError::ProtocolViolation(message),
            Self::LinkError(message) => CnxError::LinkError(message),
            Self::TransportClosed(message) => CnxError::TransportClosed(message),
            Self::PeerError(message) => CnxError::PeerError(message),
            Self::InvalidInput(message) => CnxError::InvalidInput(message),
            Self::TxInvalid(message) => CnxError::TxInvalid(message),
            Self::TxPrecondition(message) => CnxError::TxPrecondition(message),
            Self::TxCommitFailed(message) => CnxError::TxCommitFailed(message),
            Self::TxBusy => CnxError::TxBusy,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StatusRouteMachine {
    plan: StatusRoutePlan,
    route_deadline: tokio::time::Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StatusRouteAttemptRuntime {
    attempt_timeout: Duration,
    collect_idle_grace: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StatusRouteMachinePhase {
    Attempt(StatusRouteAttemptRuntime),
    WaitUntil(tokio::time::Instant),
    Failed(RouteTerminalError),
}

impl StatusRoutePlan {
    pub(crate) fn new(route_timeout: Duration, collect_idle_grace: Duration) -> Self {
        Self {
            route_timeout,
            attempt_timeout_cap: std::cmp::min(STATUS_ROUTE_ATTEMPT_TIMEOUT_CAP, route_timeout),
            collect_idle_grace,
        }
    }

    fn caller_capped(route_timeout: Duration, collect_idle_grace: Duration) -> Self {
        Self::new(
            std::cmp::min(route_timeout, STATUS_ROUTE_ATTEMPT_TIMEOUT_CAP),
            collect_idle_grace,
        )
    }

    fn attempt_timeout(self, remaining: Duration) -> Duration {
        std::cmp::min(remaining, self.attempt_timeout_cap)
    }

    fn route_timeout(self) -> Duration {
        self.route_timeout
    }

    fn collect_idle_grace(self) -> Duration {
        self.collect_idle_grace
    }

    fn rebudget(self, route_timeout: Duration) -> Self {
        Self::new(route_timeout, self.collect_idle_grace)
    }

    fn machine(self) -> StatusRouteMachine {
        self.machine_from_now(tokio::time::Instant::now())
    }

    fn machine_from_now(self, now: tokio::time::Instant) -> StatusRouteMachine {
        StatusRouteMachine {
            plan: self,
            route_deadline: now + self.route_timeout,
        }
    }
}

impl StatusRouteMachine {
    fn remaining_at(self, now: tokio::time::Instant) -> Duration {
        self.route_deadline
            .checked_duration_since(now)
            .unwrap_or_default()
    }

    fn attempt_at(self, now: tokio::time::Instant) -> Option<StatusRouteAttemptRuntime> {
        let remaining = self.remaining_at(now);
        if remaining.is_zero() {
            None
        } else {
            Some(StatusRouteAttemptRuntime {
                attempt_timeout: self.plan.attempt_timeout(remaining),
                collect_idle_grace: self.plan.collect_idle_grace(),
            })
        }
    }

    fn entry_phase(self) -> StatusRouteMachinePhase {
        self.entry_phase_at(tokio::time::Instant::now())
    }

    fn entry_phase_at(self, now: tokio::time::Instant) -> StatusRouteMachinePhase {
        match self.attempt_at(now) {
            Some(attempt) => StatusRouteMachinePhase::Attempt(attempt),
            None => StatusRouteMachinePhase::Failed(RouteTerminalError::Timeout),
        }
    }

    fn followup_phase_at(
        self,
        now: tokio::time::Instant,
        err: CnxError,
        is_retryable_gap: fn(&CnxError) -> bool,
    ) -> StatusRouteMachinePhase {
        if !is_retryable_gap(&err) {
            return StatusRouteMachinePhase::Failed(RouteTerminalError::from_error(err));
        }
        let remaining = self.remaining_at(now);
        if remaining.is_zero() {
            StatusRouteMachinePhase::Failed(RouteTerminalError::from_error(err))
        } else {
            StatusRouteMachinePhase::WaitUntil(
                now + std::cmp::min(STATUS_ROUTE_RETRY_BACKOFF, remaining),
            )
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MaterializedStatusLoadPlan {
    source_route: StatusRoutePlan,
    sink_route: StatusRoutePlan,
    explicit_empty_sink_recollect: StatusRoutePlan,
}

impl Default for MaterializedStatusLoadPlan {
    fn default() -> Self {
        Self {
            source_route: StatusRoutePlan::new(
                Duration::from_secs(30),
                STATUS_ROUTE_COLLECT_IDLE_GRACE,
            ),
            sink_route: StatusRoutePlan::new(
                Duration::from_secs(30),
                LOAD_MATERIALIZED_SINK_STATUS_ROUTE_IDLE_GRACE,
            ),
            explicit_empty_sink_recollect: StatusRoutePlan::new(
                EXPLICIT_EMPTY_SINK_STATUS_RECOLLECT_BUDGET,
                LOAD_MATERIALIZED_SINK_STATUS_ROUTE_IDLE_GRACE,
            ),
        }
    }
}

impl MaterializedStatusLoadPlan {
    fn request_source_refresh_plan(self, caller_timeout: Duration) -> StatusRoutePlan {
        self.source_route.rebudget(
            StatusRoutePlan::caller_capped(caller_timeout, self.source_route.collect_idle_grace())
                .route_timeout(),
        )
    }
}

async fn route_source_status_snapshot(
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    plan: StatusRoutePlan,
) -> Result<SourceStatusSnapshot, CnxError> {
    let events = execute_status_route_collect(
        boundary,
        origin_id,
        plan.machine(),
        METHOD_SOURCE_STATUS,
        is_retryable_source_status_continuity_gap,
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

fn is_retryable_source_status_continuity_gap(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::Timeout | CnxError::TransportClosed(_) | CnxError::ProtocolViolation(_)
    ) || matches!(
        err,
        CnxError::Internal(message)
            | CnxError::PeerError(message)
            | CnxError::AccessDenied(message)
                if message.contains("missing route state for channel_buffer")
                    || message.contains("bound route return dispatcher stopped")
                    || (message.contains("drained/fenced")
                        && message.contains("grant attachments"))
    )
}

pub(crate) async fn route_sink_status_snapshot(
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    plan: StatusRoutePlan,
) -> Result<SinkStatusSnapshot, CnxError> {
    let events = execute_status_route_collect(
        boundary,
        origin_id,
        plan.machine(),
        METHOD_SINK_STATUS,
        is_retryable_sink_status_continuity_gap,
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

async fn execute_status_route_collect(
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    machine: StatusRouteMachine,
    method: &'static str,
    is_retryable_gap: fn(&CnxError) -> bool,
) -> Result<Vec<Event>, CnxError> {
    let mut phase = machine.entry_phase();
    loop {
        match phase {
            StatusRouteMachinePhase::Attempt(attempt) => {
                let adapter = exchange_host_adapter(
                    boundary.clone(),
                    origin_id.clone(),
                    default_route_bindings(),
                );
                match adapter
                    .call_collect(
                        ROUTE_TOKEN_FS_META_INTERNAL,
                        method,
                        internal_status_request_payload(),
                        attempt.attempt_timeout,
                        attempt.collect_idle_grace,
                    )
                    .await
                {
                    Ok(events) => return Ok(events),
                    Err(err) => {
                        phase = machine.followup_phase_at(
                            tokio::time::Instant::now(),
                            err,
                            is_retryable_gap,
                        );
                    }
                }
            }
            StatusRouteMachinePhase::WaitUntil(until) => {
                tokio::time::sleep_until(until).await;
                phase = machine.entry_phase();
            }
            StatusRouteMachinePhase::Failed(err) => {
                return Err(err.into_error());
            }
        }
    }
}

fn is_retryable_sink_status_continuity_gap(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::Timeout
            | CnxError::TransportClosed(_)
            | CnxError::ProtocolViolation(_)
            | CnxError::Internal(_)
    )
}

async fn load_materialized_status_snapshots(
    state: &ApiState,
) -> Result<(SourceStatusSnapshot, SinkStatusSnapshot), CnxError> {
    let status_load_plan = MaterializedStatusLoadPlan::default();
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
            status_load_plan.source_route,
        )
        .await
        {
            Ok(snapshot) => snapshot,
            Err(err) if is_retryable_source_status_continuity_gap(&err) => {
                source.status_snapshot().await.map_err(|_| err)?
            }
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
        } => {
            let mut snapshot = match route_sink_status_snapshot(
                boundary.clone(),
                origin_id.clone(),
                status_load_plan.sink_route,
            )
            .await
            {
                Ok(snapshot) => snapshot,
                Err(err @ CnxError::Timeout)
                | Err(err @ CnxError::TransportClosed(_))
                | Err(err @ CnxError::ProtocolViolation(_)) => {
                    sink.status_snapshot().await.map_err(|_| err)?
                }
                Err(err) => return Err(err),
            };
            if sink_status_snapshot_reports_explicit_empty_for_all_active_readiness_groups(
                &source_status,
                &snapshot,
                &readiness_groups,
            ) {
                if let Ok(retry_snapshot) = route_sink_status_snapshot(
                    boundary.clone(),
                    origin_id.clone(),
                    status_load_plan.explicit_empty_sink_recollect,
                )
                .await
                {
                    snapshot = retry_snapshot;
                }
            }
            snapshot
        }
        QueryBackend::Local { .. } => sink.status_snapshot().await?,
    };
    let route_explicit_empty_groups = sink_status
        .groups
        .iter()
        .filter(|group| fresh_sink_group_explicitly_empty(group))
        .map(|group| group.group_id.clone())
        .collect::<BTreeSet<_>>();
    let mut sink_status = {
        let mut cache = state.materialized_sink_status_cache.lock().map_err(|_| {
            CnxError::Internal("materialized sink status cache lock poisoned".into())
        })?;
        let cached_snapshot = cache.as_ref().map(|cached| cached.snapshot.clone());
        let merged =
            merge_with_cached_sink_status_snapshot(cache.as_ref(), &readiness_groups, sink_status);
        let preserved = preserve_cached_ready_groups_across_explicit_empty_root_transition(
            merged,
            cached_snapshot.as_ref(),
            &source_status,
            &route_explicit_empty_groups,
        );
        *cache = Some(CachedSinkStatusSnapshot {
            snapshot: preserved.clone(),
        });
        preserved
    };
    if matches!(&state.backend, QueryBackend::Route { .. })
        && let Some(local_sink) = &state.readiness_sink
    {
        if let Ok(local_sink_status) = local_sink.cached_status_snapshot() {
            let local_sink_status =
                filter_sink_status_snapshot(local_sink_status, &readiness_groups);
            let explicit_empty_groups_to_clear =
                local_sink_snapshot_route_preserved_explicit_empty_groups(
                    &sink_status,
                    &local_sink_status,
                    &route_explicit_empty_groups,
                );
            if sink_status_snapshot_has_better_groups(&sink_status, &local_sink_status)
                || !explicit_empty_groups_to_clear.is_empty()
            {
                sink_status = merge_with_local_sink_status_snapshot(
                    sink_status,
                    local_sink_status,
                    &explicit_empty_groups_to_clear,
                );
                let mut cache = state.materialized_sink_status_cache.lock().map_err(|_| {
                    CnxError::Internal("materialized sink status cache lock poisoned".into())
                })?;
                *cache = Some(CachedSinkStatusSnapshot {
                    snapshot: sink_status.clone(),
                });
            }
        }
    }
    Ok((source_status, sink_status))
}

async fn load_request_scoped_materialized_sink_status_snapshot(
    state: &ApiState,
    plan: TreePitRequestScopedSinkStatusPlan,
) -> Option<SinkStatusSnapshot> {
    let readiness_groups = scan_enabled_readiness_groups(state).ok()?;
    match &state.backend {
        QueryBackend::Route {
            boundary,
            origin_id,
            ..
        } => route_sink_status_snapshot(
            boundary.clone(),
            origin_id.clone(),
            StatusRoutePlan::new(plan.route_timeout(), STATUS_ROUTE_COLLECT_IDLE_GRACE),
        )
        .await
        .ok()
        .map(|snapshot| filter_sink_status_snapshot(snapshot, &readiness_groups)),
        QueryBackend::Local { sink, .. } => sink
            .status_snapshot()
            .await
            .ok()
            .map(|snapshot| filter_sink_status_snapshot(snapshot, &readiness_groups)),
    }
}

fn merge_request_scoped_materialized_sink_status_snapshot(
    loaded_sink_status: &SinkStatusSnapshot,
    request_scoped_sink_status: SinkStatusSnapshot,
) -> SinkStatusSnapshot {
    let mut allowed_groups = materialized_scheduled_group_ids(loaded_sink_status);
    allowed_groups.extend(materialized_scheduled_group_ids(
        &request_scoped_sink_status,
    ));
    let cached = CachedSinkStatusSnapshot {
        snapshot: loaded_sink_status.clone(),
    };
    let merged = merge_with_cached_sink_status_snapshot(
        Some(&cached),
        &allowed_groups,
        request_scoped_sink_status.clone(),
    );
    preserve_request_scoped_ready_owner_across_equal_score_ties(merged, &request_scoped_sink_status)
}

fn preserve_request_scoped_ready_owner_across_equal_score_ties(
    mut current: SinkStatusSnapshot,
    request_scoped_sink_status: &SinkStatusSnapshot,
) -> SinkStatusSnapshot {
    let current_by_group = current
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<BTreeMap<_, _>>();
    let replacement_groups = request_scoped_sink_status
        .groups
        .iter()
        .filter(|group| {
            sink_group_readiness_reports_live_materialized_group(group)
                && sink_status_snapshot_schedules_group(
                    Some(request_scoped_sink_status),
                    &group.group_id,
                )
                && current_by_group
                    .get(group.group_id.as_str())
                    .is_some_and(|current_group| {
                        sink_group_merge_score(group) == sink_group_merge_score(current_group)
                            && group.primary_object_ref != current_group.primary_object_ref
                    })
        })
        .cloned()
        .collect::<Vec<_>>();
    if replacement_groups.is_empty() {
        return current;
    }
    let replacement_group_ids = replacement_groups
        .iter()
        .map(|group| group.group_id.clone())
        .collect::<BTreeSet<_>>();
    current
        .groups
        .retain(|group| !replacement_group_ids.contains(&group.group_id));
    current.scheduled_groups_by_node =
        remove_groups_from_scheduled_map(current.scheduled_groups_by_node, &replacement_group_ids);
    let replacement_scheduled_groups_by_node = request_scoped_sink_status
        .scheduled_groups_by_node
        .iter()
        .filter_map(|(node_id, groups)| {
            let groups = groups
                .iter()
                .filter(|group_id| replacement_group_ids.contains(*group_id))
                .cloned()
                .collect::<Vec<_>>();
            (!groups.is_empty()).then_some((node_id.clone(), groups))
        })
        .collect::<BTreeMap<_, _>>();
    merge_sink_status_snapshots(vec![
        current,
        SinkStatusSnapshot {
            groups: replacement_groups,
            scheduled_groups_by_node: replacement_scheduled_groups_by_node,
            ..SinkStatusSnapshot::default()
        },
    ])
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestScopedLoadedReadyGroupPreservationLane {
    KeepCurrent,
    ExplicitEmptyDrift,
    MissingReadyGroupRow,
    PartialScheduleOmission,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RequestScopedLoadedReadyGroupPreservationDecisionInput {
    loaded_group_reports_live_materialized: bool,
    loaded_group_has_stronger_merge_score: bool,
    request_scoped_mentions_group: bool,
    request_scoped_explicit_empty: bool,
    request_scoped_schedules_group: bool,
    request_scoped_omits_all_groups_from_schedule: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RequestScopedLoadedReadyGroupPreservationDecision {
    lane: RequestScopedLoadedReadyGroupPreservationLane,
    should_restore_loaded_group: bool,
}

fn request_scoped_loaded_ready_group_preservation_decision(
    input: RequestScopedLoadedReadyGroupPreservationDecisionInput,
) -> RequestScopedLoadedReadyGroupPreservationDecision {
    if !input.loaded_group_reports_live_materialized || !input.loaded_group_has_stronger_merge_score
    {
        return RequestScopedLoadedReadyGroupPreservationDecision {
            lane: RequestScopedLoadedReadyGroupPreservationLane::KeepCurrent,
            should_restore_loaded_group: false,
        };
    }
    if input.request_scoped_explicit_empty {
        return RequestScopedLoadedReadyGroupPreservationDecision {
            lane: RequestScopedLoadedReadyGroupPreservationLane::ExplicitEmptyDrift,
            should_restore_loaded_group: true,
        };
    }
    if !input.request_scoped_omits_all_groups_from_schedule
        && !input.request_scoped_mentions_group
        && input.request_scoped_schedules_group
    {
        return RequestScopedLoadedReadyGroupPreservationDecision {
            lane: RequestScopedLoadedReadyGroupPreservationLane::MissingReadyGroupRow,
            should_restore_loaded_group: true,
        };
    }
    if !input.request_scoped_omits_all_groups_from_schedule
        && !input.request_scoped_mentions_group
        && !input.request_scoped_schedules_group
    {
        return RequestScopedLoadedReadyGroupPreservationDecision {
            lane: RequestScopedLoadedReadyGroupPreservationLane::PartialScheduleOmission,
            should_restore_loaded_group: true,
        };
    }
    RequestScopedLoadedReadyGroupPreservationDecision {
        lane: RequestScopedLoadedReadyGroupPreservationLane::KeepCurrent,
        should_restore_loaded_group: false,
    }
}

fn preserve_request_loaded_ready_groups_across_explicit_empty_request_scoped_drift(
    current: SinkStatusSnapshot,
    loaded_sink_status: &SinkStatusSnapshot,
    request_scoped_sink_status: &SinkStatusSnapshot,
) -> SinkStatusSnapshot {
    let explicit_empty_groups = request_scoped_sink_status
        .groups
        .iter()
        .filter(|group| fresh_sink_group_explicitly_empty(group))
        .map(|group| group.group_id.clone())
        .collect::<BTreeSet<_>>();
    if explicit_empty_groups.is_empty() {
        return current;
    }
    let current_by_group = current
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<BTreeMap<_, _>>();
    let request_scoped_scheduled_groups =
        materialized_scheduled_group_ids(request_scoped_sink_status);
    let request_scoped_omits_all_groups_from_schedule =
        sink_status_snapshot_omits_all_groups_from_schedule(request_scoped_sink_status);
    let restored_groups = loaded_sink_status
        .groups
        .iter()
        .filter(|group| {
            request_scoped_loaded_ready_group_preservation_decision(
                RequestScopedLoadedReadyGroupPreservationDecisionInput {
                    loaded_group_reports_live_materialized:
                        sink_group_readiness_reports_live_materialized_group(group),
                    loaded_group_has_stronger_merge_score: current_by_group
                        .get(group.group_id.as_str())
                        .map(|current| {
                            sink_group_merge_score(group) > sink_group_merge_score(current)
                        })
                        .unwrap_or(true),
                    request_scoped_mentions_group: sink_status_mentions_group(
                        request_scoped_sink_status,
                        &group.group_id,
                    ),
                    request_scoped_explicit_empty: explicit_empty_groups.contains(&group.group_id),
                    request_scoped_schedules_group: request_scoped_scheduled_groups
                        .contains(&group.group_id),
                    request_scoped_omits_all_groups_from_schedule,
                },
            )
            .should_restore_loaded_group
        })
        .cloned()
        .collect::<Vec<_>>();
    if restored_groups.is_empty() {
        return current;
    }
    let restored_group_ids = restored_groups
        .iter()
        .map(|group| group.group_id.clone())
        .collect::<BTreeSet<_>>();
    let restored_scheduled_groups_by_node = loaded_sink_status
        .scheduled_groups_by_node
        .iter()
        .filter_map(|(node_id, groups)| {
            let groups = groups
                .iter()
                .filter(|group_id| restored_group_ids.contains(*group_id))
                .cloned()
                .collect::<Vec<_>>();
            (!groups.is_empty()).then_some((node_id.clone(), groups))
        })
        .collect::<BTreeMap<_, _>>();
    merge_sink_status_snapshots(vec![
        current,
        SinkStatusSnapshot {
            groups: restored_groups,
            scheduled_groups_by_node: restored_scheduled_groups_by_node,
            ..SinkStatusSnapshot::default()
        },
    ])
}

fn preserve_request_loaded_ready_groups_across_partial_request_scoped_schedule_omission(
    current: SinkStatusSnapshot,
    loaded_sink_status: &SinkStatusSnapshot,
    request_scoped_sink_status: &SinkStatusSnapshot,
) -> SinkStatusSnapshot {
    let request_scoped_scheduled_groups =
        materialized_scheduled_group_ids(request_scoped_sink_status);
    let request_scoped_omits_all_groups_from_schedule =
        sink_status_snapshot_omits_all_groups_from_schedule(request_scoped_sink_status);
    if request_scoped_scheduled_groups.is_empty() {
        return current;
    }
    let current_by_group = current
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<BTreeMap<_, _>>();
    let restored_groups = loaded_sink_status
        .groups
        .iter()
        .filter(|group| {
            request_scoped_loaded_ready_group_preservation_decision(
                RequestScopedLoadedReadyGroupPreservationDecisionInput {
                    loaded_group_reports_live_materialized:
                        sink_group_readiness_reports_live_materialized_group(group),
                    loaded_group_has_stronger_merge_score: current_by_group
                        .get(group.group_id.as_str())
                        .map(|current| {
                            sink_group_merge_score(group) > sink_group_merge_score(current)
                        })
                        .unwrap_or(true),
                    request_scoped_mentions_group: sink_status_mentions_group(
                        request_scoped_sink_status,
                        &group.group_id,
                    ),
                    request_scoped_explicit_empty: false,
                    request_scoped_schedules_group: request_scoped_scheduled_groups
                        .contains(&group.group_id),
                    request_scoped_omits_all_groups_from_schedule,
                },
            )
            .should_restore_loaded_group
        })
        .cloned()
        .collect::<Vec<_>>();
    if restored_groups.is_empty() {
        return current;
    }
    let restored_group_ids = restored_groups
        .iter()
        .map(|group| group.group_id.clone())
        .collect::<BTreeSet<_>>();
    let restored_scheduled_groups_by_node = loaded_sink_status
        .scheduled_groups_by_node
        .iter()
        .filter_map(|(node_id, groups)| {
            let groups = groups
                .iter()
                .filter(|group_id| restored_group_ids.contains(*group_id))
                .cloned()
                .collect::<Vec<_>>();
            (!groups.is_empty()).then_some((node_id.clone(), groups))
        })
        .collect::<BTreeMap<_, _>>();
    merge_sink_status_snapshots(vec![
        current,
        SinkStatusSnapshot {
            groups: restored_groups,
            scheduled_groups_by_node: restored_scheduled_groups_by_node,
            ..SinkStatusSnapshot::default()
        },
    ])
}

fn request_scoped_schedule_omitted_ready_groups(
    loaded_sink_status: &SinkStatusSnapshot,
    request_scoped_sink_status: &SinkStatusSnapshot,
) -> BTreeSet<String> {
    loaded_sink_status
        .groups
        .iter()
        .filter(|group| {
            sink_group_readiness_reports_live_materialized_group(group)
                && request_scoped_sink_status
                    .groups
                    .iter()
                    .find(|request_scoped_group| request_scoped_group.group_id == group.group_id)
                    .map(fresh_sink_group_explicitly_empty)
                    .unwrap_or(true)
        })
        .map(|group| group.group_id.clone())
        .collect()
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
    let scheduled_groups_by_node = snapshot
        .scheduled_groups_by_node
        .into_iter()
        .filter_map(|(node_id, groups)| {
            let groups = groups
                .into_iter()
                .filter(|group| allowed_groups.contains(group))
                .collect::<Vec<_>>();
            (!groups.is_empty()).then_some((node_id, groups))
        })
        .collect::<BTreeMap<_, _>>();
    let last_control_frame_signals_by_node = snapshot
        .last_control_frame_signals_by_node
        .into_iter()
        .filter_map(|(node_id, groups)| {
            let groups = groups
                .into_iter()
                .filter(|group| allowed_groups.contains(group))
                .collect::<Vec<_>>();
            (!groups.is_empty()).then_some((node_id, groups))
        })
        .collect::<BTreeMap<_, _>>();
    merge_sink_status_snapshots(vec![SinkStatusSnapshot {
        groups,
        scheduled_groups_by_node,
        last_control_frame_signals_by_node,
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

fn fresh_sink_group_explicitly_empty(group: &crate::sink::SinkGroupStatusSnapshot) -> bool {
    !sink_group_readiness_reports_live_materialized_group(group)
        && group.total_nodes == 0
        && group.live_nodes == 0
        && group.shadow_time_us == 0
}

fn remove_groups_from_scheduled_map(
    scheduled_groups_by_node: BTreeMap<String, Vec<String>>,
    group_ids: &BTreeSet<String>,
) -> BTreeMap<String, Vec<String>> {
    scheduled_groups_by_node
        .into_iter()
        .filter_map(|(node_id, groups)| {
            let groups = groups
                .into_iter()
                .filter(|group_id| !group_ids.contains(group_id))
                .collect::<Vec<_>>();
            (!groups.is_empty()).then_some((node_id, groups))
        })
        .collect()
}

fn merge_with_cached_sink_status_snapshot(
    cached: Option<&CachedSinkStatusSnapshot>,
    allowed_groups: &BTreeSet<String>,
    fresh: SinkStatusSnapshot,
) -> SinkStatusSnapshot {
    let fresh = filter_sink_status_snapshot(fresh, allowed_groups);
    match cached {
        Some(cached) => {
            let explicit_empty_groups = fresh
                .groups
                .iter()
                .filter(|group| fresh_sink_group_explicitly_empty(group))
                .map(|group| group.group_id.clone())
                .collect::<BTreeSet<_>>();
            let mut cached = filter_sink_status_snapshot(cached.snapshot.clone(), allowed_groups);
            if !explicit_empty_groups.is_empty() {
                cached
                    .groups
                    .retain(|group| !explicit_empty_groups.contains(&group.group_id));
                cached.scheduled_groups_by_node = remove_groups_from_scheduled_map(
                    cached.scheduled_groups_by_node,
                    &explicit_empty_groups,
                );
            }
            merge_sink_status_snapshots(vec![cached, fresh])
        }
        _ => fresh,
    }
}

fn sink_group_merge_score(group: &crate::sink::SinkGroupStatusSnapshot) -> (u8, u64, u64, u64) {
    (
        u8::from(sink_group_readiness_reports_live_materialized_group(group)),
        group.total_nodes,
        group.live_nodes,
        group.shadow_time_us,
    )
}

fn source_status_group_still_active(source_status: &SourceStatusSnapshot, group_id: &str) -> bool {
    source_status
        .logical_roots
        .iter()
        .any(|root| root.root_id == group_id)
        || source_status
            .concrete_roots
            .iter()
            .any(|root| root.logical_root_id == group_id && root.active && root.scan_enabled)
}

fn preserve_cached_ready_groups_across_explicit_empty_root_transition(
    current: SinkStatusSnapshot,
    cached: Option<&SinkStatusSnapshot>,
    source_status: &SourceStatusSnapshot,
    explicit_empty_groups: &BTreeSet<String>,
) -> SinkStatusSnapshot {
    if explicit_empty_groups.is_empty() {
        return current;
    }
    let Some(cached) = cached else {
        return current;
    };
    let current_by_group = current
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<BTreeMap<_, _>>();
    let restored_groups = cached
        .groups
        .iter()
        .filter(|group| {
            explicit_empty_groups.contains(&group.group_id)
                && sink_group_readiness_reports_live_materialized_group(group)
                && source_status_group_still_active(source_status, &group.group_id)
                && current_by_group
                    .get(group.group_id.as_str())
                    .map(|current| sink_group_merge_score(group) > sink_group_merge_score(current))
                    .unwrap_or(true)
        })
        .cloned()
        .collect::<Vec<_>>();
    if restored_groups.is_empty() {
        return current;
    }
    let restored_group_ids = restored_groups
        .iter()
        .map(|group| group.group_id.clone())
        .collect::<BTreeSet<_>>();
    let restored_scheduled_groups_by_node = cached
        .scheduled_groups_by_node
        .iter()
        .filter_map(|(node_id, groups)| {
            let groups = groups
                .iter()
                .filter(|group_id| restored_group_ids.contains(*group_id))
                .cloned()
                .collect::<Vec<_>>();
            (!groups.is_empty()).then_some((node_id.clone(), groups))
        })
        .collect::<BTreeMap<_, _>>();
    merge_sink_status_snapshots(vec![
        current,
        SinkStatusSnapshot {
            groups: restored_groups,
            scheduled_groups_by_node: restored_scheduled_groups_by_node,
            ..SinkStatusSnapshot::default()
        },
    ])
}

fn sink_status_snapshot_reports_explicit_empty_for_all_active_readiness_groups(
    source_status: &SourceStatusSnapshot,
    sink_status: &SinkStatusSnapshot,
    readiness_groups: &BTreeSet<String>,
) -> bool {
    if !sink_status.scheduled_groups_by_node.is_empty() {
        return false;
    }
    let active_groups = if readiness_groups.is_empty() {
        let mut groups = source_status
            .logical_roots
            .iter()
            .map(|root| root.root_id.clone())
            .collect::<BTreeSet<_>>();
        groups.extend(
            source_status
                .concrete_roots
                .iter()
                .filter(|root| root.active && root.scan_enabled)
                .map(|root| root.logical_root_id.clone()),
        );
        groups
    } else {
        readiness_groups
            .iter()
            .filter(|group_id| source_status_group_still_active(source_status, group_id))
            .cloned()
            .collect::<BTreeSet<_>>()
    };
    if active_groups.is_empty() {
        return false;
    }
    let sink_groups = sink_status
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<BTreeMap<_, _>>();
    active_groups.iter().all(|group_id| {
        sink_groups
            .get(group_id.as_str())
            .is_some_and(|group| fresh_sink_group_explicitly_empty(group))
    })
}

fn sink_status_snapshot_has_better_groups(
    current: &SinkStatusSnapshot,
    candidate: &SinkStatusSnapshot,
) -> bool {
    let current_by_group = current
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<BTreeMap<_, _>>();
    candidate.groups.iter().any(|group| {
        current_by_group
            .get(group.group_id.as_str())
            .map(|current| sink_group_merge_score(group) > sink_group_merge_score(current))
            .unwrap_or(true)
    })
}

fn local_sink_snapshot_route_preserved_explicit_empty_groups(
    current: &SinkStatusSnapshot,
    candidate: &SinkStatusSnapshot,
    route_explicit_empty_groups: &BTreeSet<String>,
) -> BTreeSet<String> {
    if route_explicit_empty_groups.is_empty() {
        return BTreeSet::new();
    }
    let current_by_group = current
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<BTreeMap<_, _>>();
    candidate
        .groups
        .iter()
        .filter(|group| {
            route_explicit_empty_groups.contains(&group.group_id)
                && fresh_sink_group_explicitly_empty(group)
                && current_by_group
                    .get(group.group_id.as_str())
                    .is_some_and(|current| {
                        sink_group_merge_score(current) > sink_group_merge_score(group)
                    })
        })
        .map(|group| group.group_id.clone())
        .collect()
}

fn merge_with_local_sink_status_snapshot(
    current: SinkStatusSnapshot,
    candidate: SinkStatusSnapshot,
    explicit_empty_groups_to_clear: &BTreeSet<String>,
) -> SinkStatusSnapshot {
    if explicit_empty_groups_to_clear.is_empty() {
        return merge_sink_status_snapshots(vec![current, candidate]);
    }
    let mut current = current;
    current
        .groups
        .retain(|group| !explicit_empty_groups_to_clear.contains(&group.group_id));
    current.scheduled_groups_by_node = remove_groups_from_scheduled_map(
        current.scheduled_groups_by_node,
        explicit_empty_groups_to_clear,
    );
    merge_sink_status_snapshots(vec![current, candidate])
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
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
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
    query_materialized_events_with_sink_status_snapshot(backend, params, timeout, None).await
}

async fn query_materialized_events_with_sink_status_snapshot(
    backend: QueryBackend,
    params: InternalQueryRequest,
    timeout: Duration,
    selected_group_sink_status: Option<SinkStatusSnapshot>,
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
                if let Some(selected_group_sink_status) = selected_group_sink_status {
                    Some(selected_group_sink_status)
                } else {
                    route_sink_status_snapshot(
                        boundary.clone(),
                        origin_id.clone(),
                        StatusRoutePlan::caller_capped(timeout, STATUS_ROUTE_COLLECT_IDLE_GRACE),
                    )
                    .await
                    .ok()
                }
            } else {
                None
            };
            if let Some(group_id) = params.scope.selected_group.as_deref()
                && let Some(node_id) = materialized_owner_node_for_group(
                    source.as_ref(),
                    selected_group_sink_status.as_ref(),
                    group_id,
                    MaterializedOwnerOmissionPolicy::Authoritative,
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
                let result = route_materialized_events_via_node(
                    boundary.clone(),
                    node_id.clone(),
                    params.clone(),
                    SelectedGroupOwnerRoutePlan::new(timeout),
                )
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
    plan: SelectedGroupOwnerRoutePlan,
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
            plan.route_timeout().as_millis()
        );
    }
    let payload = rmp_serde::to_vec(&params)
        .map_err(|err| CnxError::Internal(format!("encode materialized query failed: {err}")))?;
    let result = adapter
        .call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SINK_QUERY,
            Bytes::from(payload),
            plan.route_timeout(),
            plan.collect_idle_grace(),
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
                owner_node, params.scope.selected_group, err
            ),
        }
    }
    result
}

fn selected_group_sink_status_reports_live_materialized_group(
    snapshot: Option<&SinkStatusSnapshot>,
    group_id: &str,
) -> bool {
    snapshot
        .and_then(|snapshot| {
            snapshot
                .groups
                .iter()
                .find(|group| group.group_id == group_id)
        })
        .is_some_and(sink_group_readiness_reports_live_materialized_group)
}

fn selected_group_sink_status_is_unready_empty(
    snapshot: Option<&SinkStatusSnapshot>,
    group_id: &str,
) -> bool {
    snapshot
        .and_then(|snapshot| {
            snapshot
                .groups
                .iter()
                .find(|group| group.group_id == group_id)
        })
        .is_some_and(sink_group_readiness_reports_unready_empty_group)
}

fn sink_group_readiness_reports_live_materialized_group(
    group: &crate::sink::SinkGroupStatusSnapshot,
) -> bool {
    matches!(group.readiness, crate::sink::GroupReadinessState::Ready)
        && group.live_nodes > 0
        && group.total_nodes > 0
}

fn sink_group_readiness_reports_unready_empty_group(
    group: &crate::sink::SinkGroupStatusSnapshot,
) -> bool {
    !matches!(group.readiness, crate::sink::GroupReadinessState::Ready)
        && group.live_nodes == 0
        && group.total_nodes == 0
}

fn sink_status_snapshot_schedules_group(
    snapshot: Option<&SinkStatusSnapshot>,
    group_id: &str,
) -> bool {
    snapshot.is_some_and(|snapshot| {
        snapshot
            .scheduled_groups_by_node
            .values()
            .any(|groups| groups.iter().any(|group| group == group_id))
    })
}

fn selected_group_sink_status_omits_group(
    snapshot: Option<&SinkStatusSnapshot>,
    group_id: &str,
) -> bool {
    snapshot.is_some_and(|snapshot| !sink_status_mentions_group(snapshot, group_id))
}

fn request_scoped_omitted_ready_root_group_should_settle_empty_tree(
    params: &InternalQueryRequest,
    snapshot: Option<&SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<&BTreeSet<String>>,
    group_id: &str,
) -> bool {
    matches!(params.op, QueryOp::Tree)
        && params
            .tree_options
            .as_ref()
            .is_some_and(|options| options.read_class == ReadClass::TrustedMaterialized)
        && (params.scope.path.is_empty() || params.scope.path == b"/")
        && selected_group_sink_status_omits_group(snapshot, group_id)
        && request_scoped_schedule_omitted_ready_groups
            .is_some_and(|groups| groups.contains(group_id))
}

fn sink_status_snapshot_omits_all_groups_from_schedule(snapshot: &SinkStatusSnapshot) -> bool {
    snapshot.groups.is_empty() && snapshot.scheduled_groups_by_node.is_empty()
}

fn selected_group_materialized_tree_payload_is_empty(
    policy: &ProjectionPolicy,
    events: &[Event],
    group_id: &str,
    query_path: &[u8],
) -> bool {
    let mut saw_same_path_payload = false;
    for event in events {
        if event_group_key(policy, event) != group_id {
            continue;
        }
        let Ok(MaterializedQueryPayload::Tree(payload)) =
            rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
        else {
            continue;
        };
        if payload.root.path != query_path {
            continue;
        }
        saw_same_path_payload = true;
        if payload.root.exists || !payload.entries.is_empty() || payload.root.has_children {
            return false;
        }
    }
    saw_same_path_payload
}

fn selected_group_materialized_tree_payload_omits_same_path(
    policy: &ProjectionPolicy,
    events: &[Event],
    group_id: &str,
    query_path: &[u8],
) -> bool {
    let mut saw_group_event = false;
    for event in events {
        if event_group_key(policy, event) != group_id {
            continue;
        }
        saw_group_event = true;
        let Ok(MaterializedQueryPayload::Tree(payload)) =
            rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
        else {
            continue;
        };
        if payload.root.path == query_path {
            return false;
        }
    }
    saw_group_event
}

fn synthesize_empty_selected_group_tree_events(
    events: &[Event],
    policy: &ProjectionPolicy,
    group_id: &str,
    query_path: &[u8],
) -> Vec<Event> {
    let mut metadata = EventMetadata {
        origin_id: NodeId(group_id.to_string()),
        timestamp_us: 0,
        logical_ts: None,
        correlation_id: None,
        ingress_auth: None,
        trace: None,
    };
    for event in events {
        if event_group_key(policy, event) == group_id {
            metadata = event.metadata().clone();
            break;
        }
    }
    let payload = rmp_serde::to_vec(&MaterializedQueryPayload::Tree(
        empty_materialized_tree_group_payload(query_path),
    ))
    .expect("encode synthetic empty selected-group tree payload");
    vec![Event::new(metadata, Bytes::from(payload))]
}

fn trusted_materialized_exact_file_lane_settles_empty_after_prior_decode(
    params: &InternalQueryRequest,
    selected_group_sink_status: Option<&SinkStatusSnapshot>,
    group_id: &str,
    prior_materialized_exact_file_decoded: bool,
) -> bool {
    matches!(params.op, QueryOp::Tree)
        && params
            .tree_options
            .as_ref()
            .is_some_and(|options| options.read_class == ReadClass::TrustedMaterialized)
        && prior_materialized_exact_file_decoded
        && !trusted_materialized_empty_group_root_requires_fail_closed(&params.scope.path)
        && selected_group_sink_status_reports_live_materialized_group(
            selected_group_sink_status,
            group_id,
        )
}

fn selected_group_materialized_tree_payload_is_structural_placeholder(
    policy: &ProjectionPolicy,
    events: &[Event],
    group_id: &str,
    query_path: &[u8],
) -> bool {
    for event in events {
        if event_group_key(policy, event) != group_id {
            continue;
        }
        let Ok(MaterializedQueryPayload::Tree(payload)) =
            rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
        else {
            continue;
        };
        if payload.root.path != query_path {
            continue;
        }
        // A same-path root that already attests child continuity is usable
        // trusted-materialized evidence, even if the current page is empty.
        if payload.root.exists && payload.entries.is_empty() && !payload.root.has_children {
            return true;
        }
    }
    false
}

fn selected_group_materialized_tree_payload_has_no_entries(
    policy: &ProjectionPolicy,
    events: &[Event],
    group_id: &str,
    query_path: &[u8],
) -> bool {
    for event in events {
        if event_group_key(policy, event) != group_id {
            continue;
        }
        let Ok(MaterializedQueryPayload::Tree(payload)) =
            rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
        else {
            continue;
        };
        if payload.root.path == query_path && payload.entries.is_empty() {
            return true;
        }
    }
    false
}

fn selected_group_ready_root_tree_requires_rescue(
    policy: &ProjectionPolicy,
    request_scoped_schedule_omitted_ready_groups: Option<&BTreeSet<String>>,
    events: &[Event],
    group_id: &str,
    query_path: &[u8],
) -> bool {
    let is_root_path = query_path.is_empty() || query_path == b"/";
    if is_root_path
        && request_scoped_schedule_omitted_ready_groups
            .is_some_and(|groups| groups.contains(group_id))
        && selected_group_materialized_tree_payload_has_no_entries(
            policy, events, group_id, query_path,
        )
    {
        return true;
    }
    selected_group_materialized_tree_payload_is_empty(policy, events, group_id, query_path)
        || (is_root_path
            && selected_group_materialized_tree_payload_is_structural_placeholder(
                policy, events, group_id, query_path,
            ))
}

fn selected_group_empty_ready_tree_requires_proxy_fallback(
    policy: &ProjectionPolicy,
    params: &InternalQueryRequest,
    selected_group_sink_status: Option<&SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<&BTreeSet<String>>,
    group_id: &str,
    events: &[Event],
) -> bool {
    let path = if params.scope.path.is_empty() {
        b"/".as_slice()
    } else {
        params.scope.path.as_slice()
    };
    matches!(params.op, QueryOp::Tree)
        && params
            .tree_options
            .as_ref()
            .is_some_and(|options| options.read_class == ReadClass::TrustedMaterialized)
        && path == b"/"
        && selected_group_sink_status_reports_live_materialized_group(
            selected_group_sink_status,
            group_id,
        )
        && selected_group_ready_root_tree_requires_rescue(
            policy,
            request_scoped_schedule_omitted_ready_groups,
            events,
            group_id,
            path,
        )
}

fn selected_group_empty_ready_tree_requires_primary_owner_reroute(
    policy: &ProjectionPolicy,
    params: &InternalQueryRequest,
    selected_group_sink_status: Option<&SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<&BTreeSet<String>>,
    group_id: &str,
    events: &[Event],
) -> bool {
    matches!(params.op, QueryOp::Tree)
        && params
            .tree_options
            .as_ref()
            .is_some_and(|options| options.read_class == ReadClass::TrustedMaterialized)
        && selected_group_sink_status_reports_live_materialized_group(
            selected_group_sink_status,
            group_id,
        )
        && selected_group_ready_root_tree_requires_rescue(
            policy,
            request_scoped_schedule_omitted_ready_groups,
            events,
            group_id,
            params.scope.path.as_slice(),
        )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectedGroupTreePitPlanInput {
    read_class: ReadClass,
    observation_state: ObservationState,
    selected_group_sink_reports_live_materialized: bool,
    prior_materialized_group_decoded: bool,
    is_last_ranked_group: bool,
    selected_group_sink_unready_empty: bool,
    empty_root_requires_fail_closed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectedGroupTreePitPlan {
    prior_materialized_group_decoded: bool,
    trusted_materialized_ready_group: bool,
    requires_rescue: bool,
    allow_empty_owner_retry: bool,
    should_resolve_selected_group_owner: bool,
    empty_root_requires_fail_closed: bool,
    stage_timeout_policy: TreePitStageTimeoutPolicy,
    reserve_proxy_budget: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EmptyTreeRescueLane {
    ReturnCurrent,
    OwnerRetryThenSettle,
    OwnerRetryThenProxyFallback,
    ProxyFallback,
    FailClosed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EmptyTreeRescueLaneFacts {
    allow_owner_retry: bool,
    settle_after_owner_retry: bool,
    attempt_proxy_fallback: bool,
    fail_closed_without_proxy_budget: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TrustedMaterializedEmptyResponseRescuePlan {
    lane: EmptyTreeRescueLane,
    owner_retry_timeout: Duration,
    fail_closed_on_proxy_error: bool,
    fail_closed_on_final_empty_response: bool,
    defer_first_ranked_empty_non_root_group: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TrustedMaterializedDeferredEmptyGroupDecisionInput {
    defer_first_ranked_empty_non_root_group: bool,
    rank_index: usize,
    response_is_empty: bool,
    deferred_group_present: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TrustedMaterializedDeferredEmptyGroupLane {
    KeepCurrent,
    DeferCurrentFirstRankedEmptyGroup,
    FailClosedPriorDeferredGroup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TrustedMaterializedDeferredEmptyGroupDecision {
    lane: TrustedMaterializedDeferredEmptyGroupLane,
    should_defer_current_group: bool,
    should_fail_closed_prior_deferred_group: bool,
}

fn decide_empty_tree_rescue_lane(facts: EmptyTreeRescueLaneFacts) -> EmptyTreeRescueLane {
    if facts.allow_owner_retry {
        if facts.settle_after_owner_retry || !facts.attempt_proxy_fallback {
            EmptyTreeRescueLane::OwnerRetryThenSettle
        } else {
            EmptyTreeRescueLane::OwnerRetryThenProxyFallback
        }
    } else if facts.attempt_proxy_fallback {
        EmptyTreeRescueLane::ProxyFallback
    } else if facts.fail_closed_without_proxy_budget {
        EmptyTreeRescueLane::FailClosed
    } else {
        EmptyTreeRescueLane::ReturnCurrent
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TreePitStageTimeoutPolicy {
    UseRemainingForLastNonTrustedAfterPriorDecode,
    FirstRankedTrustedReadyBudget,
    LaterRankedTrustedGroupBudget,
    UnreadySelectedGroupRouteBudget,
    DefaultCollectIdleGraceCap,
}

impl TreePitStageTimeoutPolicy {
    fn timeout(self, remaining: Duration, caller_timeout: Duration) -> Duration {
        match self {
            Self::UseRemainingForLastNonTrustedAfterPriorDecode => remaining,
            Self::FirstRankedTrustedReadyBudget => {
                std::cmp::min(remaining, FIRST_RANKED_TRUSTED_READY_GROUP_STAGE_BUDGET)
            }
            Self::LaterRankedTrustedGroupBudget => {
                std::cmp::min(remaining, LATER_RANKED_TRUSTED_GROUP_STAGE_BUDGET)
            }
            Self::UnreadySelectedGroupRouteBudget => {
                std::cmp::min(remaining, UNREADY_SELECTED_GROUP_ROUTE_BUDGET)
            }
            Self::DefaultCollectIdleGraceCap => std::cmp::min(
                caller_timeout + MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE,
                remaining,
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SelectedGroupOwnerRouteGapProxyPolicy {
    UseRemainingBudget,
    FailClosedAfterProxyGap,
}

impl SelectedGroupOwnerRouteGapProxyPolicy {
    fn proxy_timeout(self, remaining: Duration) -> Duration {
        match self {
            Self::UseRemainingBudget => remaining,
            Self::FailClosedAfterProxyGap => {
                std::cmp::min(remaining, TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET)
            }
        }
    }

    fn fail_closed_after_proxy_gap(self) -> bool {
        matches!(self, Self::FailClosedAfterProxyGap)
    }
}

impl SelectedGroupTreePitPlan {
    fn new(input: SelectedGroupTreePitPlanInput) -> Self {
        let trusted_materialized_ready_group = input.read_class == ReadClass::TrustedMaterialized
            && input.selected_group_sink_reports_live_materialized;
        let requires_rescue = trusted_materialized_ready_group
            || (input.read_class == ReadClass::TrustedMaterialized
                && input.observation_state == ObservationState::TrustedMaterialized
                && input.prior_materialized_group_decoded
                && !input.empty_root_requires_fail_closed);
        let stage_timeout_policy = if input.read_class != ReadClass::TrustedMaterialized
            && input.is_last_ranked_group
            && input.prior_materialized_group_decoded
        {
            TreePitStageTimeoutPolicy::UseRemainingForLastNonTrustedAfterPriorDecode
        } else if input.read_class == ReadClass::TrustedMaterialized
            && trusted_materialized_ready_group
            && !input.prior_materialized_group_decoded
        {
            TreePitStageTimeoutPolicy::FirstRankedTrustedReadyBudget
        } else if input.read_class == ReadClass::TrustedMaterialized && requires_rescue {
            TreePitStageTimeoutPolicy::LaterRankedTrustedGroupBudget
        } else if input.read_class != ReadClass::TrustedMaterialized
            && input.selected_group_sink_unready_empty
        {
            TreePitStageTimeoutPolicy::UnreadySelectedGroupRouteBudget
        } else {
            TreePitStageTimeoutPolicy::DefaultCollectIdleGraceCap
        };
        let reserve_proxy_budget = if input.read_class == ReadClass::TrustedMaterialized
            && trusted_materialized_ready_group
            && !input.prior_materialized_group_decoded
        {
            !input.empty_root_requires_fail_closed
        } else {
            !(input.read_class != ReadClass::TrustedMaterialized
                && input.is_last_ranked_group
                && input.prior_materialized_group_decoded)
        };

        Self {
            prior_materialized_group_decoded: input.prior_materialized_group_decoded,
            trusted_materialized_ready_group,
            requires_rescue,
            allow_empty_owner_retry: trusted_materialized_ready_group
                && !input.prior_materialized_group_decoded,
            should_resolve_selected_group_owner: requires_rescue,
            empty_root_requires_fail_closed: input.empty_root_requires_fail_closed,
            stage_timeout_policy,
            reserve_proxy_budget,
        }
    }

    fn stage_timeout(self, remaining: Duration, caller_timeout: Duration) -> Duration {
        self.stage_timeout_policy.timeout(remaining, caller_timeout)
    }

    fn owner_route_gap_proxy_policy(
        self,
        trusted_materialized_tree_query: bool,
    ) -> SelectedGroupOwnerRouteGapProxyPolicy {
        if self.reserve_proxy_budget
            && trusted_materialized_tree_query
            && !self.allow_empty_owner_retry
            && self.empty_root_requires_fail_closed
        {
            SelectedGroupOwnerRouteGapProxyPolicy::FailClosedAfterProxyGap
        } else {
            SelectedGroupOwnerRouteGapProxyPolicy::UseRemainingBudget
        }
    }

    fn stats_rescue_lane(
        self,
        root_path: bool,
        owner_stats_zeroish: bool,
        proxy_available: bool,
    ) -> SelectedGroupStatsRescueLane {
        if !self.trusted_materialized_ready_group || !owner_stats_zeroish {
            return SelectedGroupStatsRescueLane::ReturnCurrent;
        }

        if root_path {
            if proxy_available {
                SelectedGroupStatsRescueLane::ProxyFallbackThenFailClosed
            } else {
                SelectedGroupStatsRescueLane::FailClosed
            }
        } else if proxy_available {
            SelectedGroupStatsRescueLane::ProxyFallback
        } else {
            SelectedGroupStatsRescueLane::ReturnCurrent
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreePitSessionPlan {
    caller_timeout: Duration,
    total_ranked_groups: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreePitSessionTiming {
    session_deadline: tokio::time::Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreePitStageTiming {
    session_deadline: tokio::time::Instant,
    stage_deadline: tokio::time::Instant,
    remaining_session: Duration,
    stage_timeout: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TreePitStagePreparation {
    Ready(TreePitStageTiming),
    Unavailable(TreePitUnavailableStageLane),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TreePitUnavailableStageLane {
    PushEmptySnapshot,
    FailClosedUnavailable,
    FailClosedEmptyRoot,
}

#[derive(Debug)]
enum TreePitStageEntryAction {
    Query(TreePitStageTiming),
    PushEmptySnapshot,
    ReturnError(CnxError),
}

#[derive(Debug)]
enum TreePitStageQueryFailureAction {
    RetryableGap {
        gap: RetryableTreePitStageGap,
        err: CnxError,
    },
    Fatal(CnxError),
}

#[derive(Debug)]
enum TreePitStageDecodeErrorAction {
    PushEmptySnapshot,
    PushErrorSnapshot(CnxError),
    ReturnError(CnxError),
}

#[derive(Debug)]
enum TreePitStageExecutionAction {
    PushPayload {
        response: TreeGroupPayload,
        empty_response_plan: TrustedMaterializedEmptyResponseRescuePlan,
    },
    PushEmptySnapshot,
    PushErrorSnapshot(CnxError),
    ReturnError(CnxError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RetryableTreePitStageGap {
    group_plan: TreePitGroupPlan,
    session_deadline: tokio::time::Instant,
    stage_deadline: tokio::time::Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ForceFindSessionPlan {
    caller_timeout: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ForceFindRoutePlan {
    route_timeout: Duration,
    collect_idle_grace: Duration,
    retry_backoff: Duration,
    min_inflight_hold: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ForceFindRouteAttemptRuntime {
    route_plan: ForceFindRoutePlan,
    remaining: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ForceFindTreeRouteErrorLane {
    Fail,
    RetryCurrentRoute,
    FallbackThenRetry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ForceFindTreeRouteAction {
    ReturnError(RouteTerminalError),
    QueryViaSelectedRunner {
        node_id: NodeId,
        route_plan: ForceFindRoutePlan,
    },
    QueryGenericFallback {
        route_plan: ForceFindRoutePlan,
        after_runner_gap: bool,
    },
    WaitUntil(tokio::time::Instant),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ForceFindRouteMachinePhase {
    PlanSelectedRunner,
    AttemptSelectedRunner {
        node_id: NodeId,
        route_plan: ForceFindRoutePlan,
    },
    FallbackToGeneric {
        route_plan: ForceFindRoutePlan,
        after_runner_gap: bool,
    },
    WaitForForceFindReadiness(tokio::time::Instant),
    Failed(RouteTerminalError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ForceFindRouteMachine {
    plan: ForceFindRoutePlan,
    route_deadline: tokio::time::Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ForceFindInFlightHoldAction {
    ReturnNow,
    WaitUntil(tokio::time::Instant),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProxyRouteMachinePhase {
    Attempt(TreePitProxyAttemptRuntime),
    WaitUntil(tokio::time::Instant),
    Failed(RouteTerminalError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreePitRequestScopedSinkStatusPlan {
    route_timeout: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectedGroupOwnerRoutePlan {
    route_timeout: Duration,
    collect_idle_grace: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreePitProxyRoutePlan {
    route_timeout: Duration,
    collect_idle_grace: Duration,
    retry_backoff: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProxyRouteMachine {
    plan: TreePitProxyRoutePlan,
    route_deadline: tokio::time::Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreePitProxyAttemptRuntime {
    attempt_timeout: Duration,
    collect_idle_grace: Duration,
    retry_backoff: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreePitGroupPlanInput {
    read_class: ReadClass,
    observation_state: ObservationState,
    selected_group_sink_reports_live_materialized: bool,
    prior_materialized_group_decoded: bool,
    prior_materialized_exact_file_decoded: bool,
    rank_index: usize,
    is_last_ranked_group: bool,
    selected_group_sink_unready_empty: bool,
    empty_root_requires_fail_closed: bool,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreePitOwnerSnapshotPlanInput {
    trusted_materialized_tree_query: bool,
    reserve_proxy_budget: bool,
    allow_empty_owner_retry: bool,
    empty_root_requires_fail_closed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TreePitGroupPlan {
    read_class: ReadClass,
    caller_timeout: Duration,
    stage_plan: SelectedGroupTreePitPlan,
    prior_materialized_exact_file_decoded: bool,
    rank_index: usize,
    total_ranked_groups: usize,
}

impl TreePitSessionPlan {
    fn new(caller_timeout: Duration, total_ranked_groups: usize) -> Self {
        Self {
            caller_timeout,
            total_ranked_groups,
        }
    }

    fn request_scoped_sink_status_plan(self) -> TreePitRequestScopedSinkStatusPlan {
        TreePitRequestScopedSinkStatusPlan {
            route_timeout: std::cmp::min(
                self.caller_timeout,
                REQUEST_SCOPED_SINK_STATUS_LOAD_BUDGET,
            ),
        }
    }

    fn selected_group_stage_plan(self, input: TreePitGroupPlanInput) -> TreePitGroupPlan {
        TreePitGroupPlan {
            read_class: input.read_class,
            caller_timeout: self.caller_timeout,
            stage_plan: SelectedGroupTreePitPlan::new(SelectedGroupTreePitPlanInput {
                read_class: input.read_class,
                observation_state: input.observation_state,
                selected_group_sink_reports_live_materialized: input
                    .selected_group_sink_reports_live_materialized,
                prior_materialized_group_decoded: input.prior_materialized_group_decoded,
                is_last_ranked_group: input.is_last_ranked_group,
                selected_group_sink_unready_empty: input.selected_group_sink_unready_empty,
                empty_root_requires_fail_closed: input.empty_root_requires_fail_closed,
            }),
            prior_materialized_exact_file_decoded: input.prior_materialized_exact_file_decoded,
            rank_index: input.rank_index,
            total_ranked_groups: self.total_ranked_groups,
        }
    }

    fn session_timing(
        self,
        ranking_started_at: tokio::time::Instant,
        ranking_retryable_failure: bool,
    ) -> TreePitSessionTiming {
        self.session_timing_from_now(
            ranking_started_at,
            ranking_retryable_failure,
            tokio::time::Instant::now(),
        )
    }

    fn session_timing_from_now(
        self,
        ranking_started_at: tokio::time::Instant,
        ranking_retryable_failure: bool,
        now: tokio::time::Instant,
    ) -> TreePitSessionTiming {
        let session_deadline = if ranking_retryable_failure {
            ranking_started_at + self.caller_timeout
        } else {
            now + self.caller_timeout
        };
        TreePitSessionTiming { session_deadline }
    }

    #[cfg(test)]
    fn selected_group_owner_snapshot_plan(
        self,
        input: TreePitOwnerSnapshotPlanInput,
    ) -> TreePitGroupPlan {
        let base_plan = self.selected_group_stage_plan(TreePitGroupPlanInput {
            read_class: if input.trusted_materialized_tree_query {
                ReadClass::TrustedMaterialized
            } else {
                ReadClass::Materialized
            },
            observation_state: if input.trusted_materialized_tree_query {
                ObservationState::TrustedMaterialized
            } else {
                ObservationState::MaterializedUntrusted
            },
            selected_group_sink_reports_live_materialized: input.trusted_materialized_tree_query,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: input.empty_root_requires_fail_closed,
        });
        TreePitGroupPlan {
            stage_plan: SelectedGroupTreePitPlan {
                allow_empty_owner_retry: input.allow_empty_owner_retry,
                reserve_proxy_budget: input.reserve_proxy_budget,
                requires_rescue: input.trusted_materialized_tree_query
                    || input.allow_empty_owner_retry,
                should_resolve_selected_group_owner: input.trusted_materialized_tree_query
                    || input.allow_empty_owner_retry,
                ..base_plan.stage_plan
            },
            ..base_plan
        }
    }
}

impl TreePitSessionTiming {
    fn entry_action_for(
        self,
        group_plan: TreePitGroupPlan,
        group_key: &str,
    ) -> TreePitStageEntryAction {
        self.entry_action_for_at(group_plan, group_key, tokio::time::Instant::now())
    }

    fn entry_action_for_at(
        self,
        group_plan: TreePitGroupPlan,
        group_key: &str,
        now: tokio::time::Instant,
    ) -> TreePitStageEntryAction {
        match self.prepare_stage_for_at(group_plan, now) {
            TreePitStagePreparation::Ready(stage_timing) => {
                TreePitStageEntryAction::Query(stage_timing)
            }
            TreePitStagePreparation::Unavailable(
                TreePitUnavailableStageLane::PushEmptySnapshot,
            ) => TreePitStageEntryAction::PushEmptySnapshot,
            TreePitStagePreparation::Unavailable(
                TreePitUnavailableStageLane::FailClosedUnavailable,
            ) => TreePitStageEntryAction::ReturnError(
                trusted_materialized_unavailable_selected_group_tree_error(group_key),
            ),
            TreePitStagePreparation::Unavailable(
                TreePitUnavailableStageLane::FailClosedEmptyRoot,
            ) => TreePitStageEntryAction::ReturnError(
                trusted_materialized_empty_selected_group_tree_error(group_key),
            ),
        }
    }

    fn prepare_stage_for(self, group_plan: TreePitGroupPlan) -> TreePitStagePreparation {
        self.prepare_stage_for_at(group_plan, tokio::time::Instant::now())
    }

    fn prepare_stage_for_at(
        self,
        group_plan: TreePitGroupPlan,
        now: tokio::time::Instant,
    ) -> TreePitStagePreparation {
        self.stage_timing_for_at(group_plan, now)
            .map(TreePitStagePreparation::Ready)
            .unwrap_or_else(|| {
                TreePitStagePreparation::Unavailable(group_plan.unavailable_stage_lane())
            })
    }

    fn remaining_at(self, now: tokio::time::Instant) -> Duration {
        self.session_deadline
            .checked_duration_since(now)
            .unwrap_or_default()
    }

    fn stage_timing_for_at(
        self,
        group_plan: TreePitGroupPlan,
        now: tokio::time::Instant,
    ) -> Option<TreePitStageTiming> {
        let remaining_session = self.remaining_at(now);
        if remaining_session.is_zero() {
            return None;
        }
        let stage_timeout = group_plan.stage_timeout(remaining_session);
        Some(TreePitStageTiming {
            session_deadline: self.session_deadline,
            stage_deadline: now + stage_timeout,
            remaining_session,
            stage_timeout,
        })
    }
}

impl TreePitStageTiming {
    fn empty_response_rescue_plan(
        self,
        group_plan: TreePitGroupPlan,
        selected_group_owner_known: bool,
    ) -> TrustedMaterializedEmptyResponseRescuePlan {
        group_plan.empty_response_rescue_plan(selected_group_owner_known, self.remaining_session)
    }

    fn retryable_gap(self, group_plan: TreePitGroupPlan) -> RetryableTreePitStageGap {
        RetryableTreePitStageGap {
            group_plan,
            session_deadline: self.session_deadline,
            stage_deadline: self.stage_deadline,
        }
    }

    fn query_failure_action(
        self,
        group_plan: TreePitGroupPlan,
        err: CnxError,
    ) -> TreePitStageQueryFailureAction {
        match &err {
            CnxError::Timeout | CnxError::TransportClosed(_) | CnxError::ProtocolViolation(_) => {
                TreePitStageQueryFailureAction::RetryableGap {
                    gap: self.retryable_gap(group_plan),
                    err,
                }
            }
            _ => TreePitStageQueryFailureAction::Fatal(err),
        }
    }

    fn decode_failure_action(
        self,
        group_plan: TreePitGroupPlan,
        group_key: &str,
        query_path: &[u8],
        request_scoped_schedule_omitted_ready_groups: Option<&BTreeSet<String>>,
        err: CnxError,
    ) -> TreePitStageDecodeErrorAction {
        let missing_same_path_payload =
            decode_materialized_selected_group_response_missing_same_path_payload(&err);
        let root_path_request_scoped_omitted_ready_group = group_plan.read_class
            == ReadClass::TrustedMaterialized
            && (query_path.is_empty() || query_path == b"/")
            && request_scoped_schedule_omitted_ready_groups
                .is_some_and(|groups| groups.contains(group_key))
            && missing_same_path_payload;
        if root_path_request_scoped_omitted_ready_group {
            return TreePitStageDecodeErrorAction::PushEmptySnapshot;
        }

        if group_plan.read_class == ReadClass::TrustedMaterialized
            && group_plan.stage_plan.prior_materialized_group_decoded
            && group_plan.stage_plan.trusted_materialized_ready_group
        {
            if group_plan.stage_plan.empty_root_requires_fail_closed
                && matches!(&err, CnxError::Timeout)
            {
                return TreePitStageDecodeErrorAction::ReturnError(err);
            }
            if !group_plan.stage_plan.empty_root_requires_fail_closed && missing_same_path_payload {
                return TreePitStageDecodeErrorAction::PushEmptySnapshot;
            }
            return TreePitStageDecodeErrorAction::ReturnError(
                trusted_materialized_unavailable_selected_group_tree_error(group_key),
            );
        }

        if group_plan.read_class == ReadClass::TrustedMaterialized
            && group_plan.prior_materialized_exact_file_decoded
            && group_plan.stage_plan.trusted_materialized_ready_group
            && missing_same_path_payload
        {
            return TreePitStageDecodeErrorAction::PushEmptySnapshot;
        }

        TreePitStageDecodeErrorAction::PushErrorSnapshot(err)
    }
}

impl RetryableTreePitStageGap {
    fn remaining_session_at(self, now: tokio::time::Instant) -> Duration {
        self.session_deadline
            .checked_duration_since(now)
            .unwrap_or_default()
    }

    fn remaining_stage_at(self, now: tokio::time::Instant) -> Duration {
        self.stage_deadline
            .checked_duration_since(now)
            .unwrap_or_default()
    }

    fn owner_retry_timeout_at(self, now: tokio::time::Instant) -> Duration {
        self.group_plan
            .retryable_gap_owner_retry_timeout(self.remaining_session_at(now))
    }

    fn proxy_route_machine_at(self, now: tokio::time::Instant) -> Option<ProxyRouteMachine> {
        let plan = self.group_plan.retryable_gap_proxy_route_plan(
            self.remaining_session_at(now),
            self.remaining_stage_at(now),
        );
        if plan.route_timeout().is_zero() {
            None
        } else {
            Some(plan.machine_from_now(now))
        }
    }

    fn fail_closed_after_retryable_gap(self) -> bool {
        self.group_plan.fail_closed_after_retryable_gap()
    }
}

impl ForceFindSessionPlan {
    fn new(caller_timeout: Duration) -> Self {
        Self { caller_timeout }
    }

    fn route_plan(self) -> ForceFindRoutePlan {
        ForceFindRoutePlan::new(self.caller_timeout)
    }
}

impl ForceFindRoutePlan {
    fn new(route_timeout: Duration) -> Self {
        Self {
            route_timeout,
            collect_idle_grace: std::cmp::min(FORCE_FIND_ROUTE_COLLECT_IDLE_GRACE, route_timeout),
            retry_backoff: std::cmp::min(FORCE_FIND_ROUTE_RETRY_BACKOFF, route_timeout),
            min_inflight_hold: std::cmp::min(FORCE_FIND_MIN_INFLIGHT_HOLD, route_timeout),
        }
    }

    fn route_timeout(self) -> Duration {
        self.route_timeout
    }

    fn collect_idle_grace(self) -> Duration {
        self.collect_idle_grace
    }

    fn retry_backoff(self) -> Duration {
        self.retry_backoff
    }

    fn min_inflight_hold(self) -> Duration {
        self.min_inflight_hold
    }

    fn inflight_hold_action(self, inflight: bool) -> ForceFindInFlightHoldAction {
        self.inflight_hold_action_at(tokio::time::Instant::now(), inflight)
    }

    fn inflight_hold_action_at(
        self,
        now: tokio::time::Instant,
        inflight: bool,
    ) -> ForceFindInFlightHoldAction {
        if !inflight || self.min_inflight_hold().is_zero() {
            ForceFindInFlightHoldAction::ReturnNow
        } else {
            ForceFindInFlightHoldAction::WaitUntil(now + self.min_inflight_hold())
        }
    }

    fn rebudget(self, route_timeout: Duration) -> Self {
        Self::new(route_timeout)
    }

    fn machine(self) -> ForceFindRouteMachine {
        self.machine_from_now(tokio::time::Instant::now())
    }

    fn machine_from_now(self, now: tokio::time::Instant) -> ForceFindRouteMachine {
        ForceFindRouteMachine {
            plan: self,
            route_deadline: now + self.route_timeout,
        }
    }

    fn tree_route_error_lane(
        self,
        err: &CnxError,
        selected_runner_attempt: bool,
        remaining: Duration,
    ) -> ForceFindTreeRouteErrorLane {
        if remaining.is_zero() {
            return ForceFindTreeRouteErrorLane::Fail;
        }
        let retryable =
            is_retryable_force_find_route_error(err) || is_retryable_force_find_runner_gap(err);
        if !retryable {
            return ForceFindTreeRouteErrorLane::Fail;
        }
        if selected_runner_attempt {
            ForceFindTreeRouteErrorLane::FallbackThenRetry
        } else {
            ForceFindTreeRouteErrorLane::RetryCurrentRoute
        }
    }
}

impl ForceFindRouteMachine {
    async fn phase_for_selected_runner(
        &self,
        state: &ApiState,
        source: &SourceFacade,
        group_id: &str,
    ) -> Result<ForceFindRouteMachinePhase, CnxError> {
        Ok(self.phase_for_selected_runner_at(
            tokio::time::Instant::now(),
            select_force_find_runner_node_for_group(state, source, group_id).await?,
        ))
    }

    fn tree_entry_action_at(
        &self,
        now: tokio::time::Instant,
        selected_runner: Option<NodeId>,
    ) -> ForceFindTreeRouteAction {
        let Some(attempt_runtime) = self.attempt_at(now) else {
            return ForceFindTreeRouteAction::ReturnError(RouteTerminalError::Timeout);
        };
        let route_plan = attempt_runtime.route_plan();
        if let Some(node_id) = selected_runner {
            ForceFindTreeRouteAction::QueryViaSelectedRunner {
                node_id,
                route_plan,
            }
        } else {
            ForceFindTreeRouteAction::QueryGenericFallback {
                route_plan,
                after_runner_gap: false,
            }
        }
    }

    fn attempt_at(&self, now: tokio::time::Instant) -> Option<ForceFindRouteAttemptRuntime> {
        let remaining = self
            .route_deadline
            .checked_duration_since(now)
            .unwrap_or_default();
        if remaining.is_zero() {
            return None;
        }
        Some(ForceFindRouteAttemptRuntime {
            route_plan: self.plan.rebudget(remaining),
            remaining,
        })
    }

    fn tree_followup_action_at(
        &self,
        now: tokio::time::Instant,
        err: CnxError,
        selected_runner_attempt: bool,
    ) -> ForceFindTreeRouteAction {
        let Some(attempt_runtime) = self.attempt_at(now) else {
            return ForceFindTreeRouteAction::ReturnError(RouteTerminalError::Timeout);
        };
        match attempt_runtime.tree_route_error_lane(&err, selected_runner_attempt) {
            ForceFindTreeRouteErrorLane::Fail => {
                ForceFindTreeRouteAction::ReturnError(RouteTerminalError::from_error(err))
            }
            ForceFindTreeRouteErrorLane::RetryCurrentRoute => ForceFindTreeRouteAction::WaitUntil(
                now + attempt_runtime.route_plan().retry_backoff(),
            ),
            ForceFindTreeRouteErrorLane::FallbackThenRetry => {
                ForceFindTreeRouteAction::QueryGenericFallback {
                    route_plan: attempt_runtime.route_plan(),
                    after_runner_gap: true,
                }
            }
        }
    }

    fn entry_phase(&self) -> ForceFindRouteMachinePhase {
        ForceFindRouteMachinePhase::PlanSelectedRunner
    }

    fn phase_for_selected_runner_at(
        &self,
        now: tokio::time::Instant,
        selected_runner: Option<NodeId>,
    ) -> ForceFindRouteMachinePhase {
        self.phase_from_action(self.tree_entry_action_at(now, selected_runner))
    }

    fn followup_phase_at(
        &self,
        now: tokio::time::Instant,
        err: CnxError,
        selected_runner_attempt: bool,
    ) -> ForceFindRouteMachinePhase {
        self.phase_from_action(self.tree_followup_action_at(now, err, selected_runner_attempt))
    }

    fn phase_after_wait(&self) -> ForceFindRouteMachinePhase {
        ForceFindRouteMachinePhase::PlanSelectedRunner
    }

    fn phase_from_action(&self, action: ForceFindTreeRouteAction) -> ForceFindRouteMachinePhase {
        match action {
            ForceFindTreeRouteAction::ReturnError(err) => ForceFindRouteMachinePhase::Failed(err),
            ForceFindTreeRouteAction::QueryViaSelectedRunner {
                node_id,
                route_plan,
            } => ForceFindRouteMachinePhase::AttemptSelectedRunner {
                node_id,
                route_plan,
            },
            ForceFindTreeRouteAction::QueryGenericFallback {
                route_plan,
                after_runner_gap,
            } => ForceFindRouteMachinePhase::FallbackToGeneric {
                route_plan,
                after_runner_gap,
            },
            ForceFindTreeRouteAction::WaitUntil(until) => {
                ForceFindRouteMachinePhase::WaitForForceFindReadiness(until)
            }
        }
    }
}

impl ForceFindRouteAttemptRuntime {
    fn route_plan(self) -> ForceFindRoutePlan {
        self.route_plan
    }

    fn tree_route_error_lane(
        self,
        err: &CnxError,
        selected_runner_attempt: bool,
    ) -> ForceFindTreeRouteErrorLane {
        self.route_plan
            .tree_route_error_lane(err, selected_runner_attempt, self.remaining)
    }
}

impl TreePitRequestScopedSinkStatusPlan {
    fn route_timeout(self) -> Duration {
        self.route_timeout
    }
}

impl SelectedGroupOwnerRoutePlan {
    fn new(route_timeout: Duration) -> Self {
        Self {
            route_timeout,
            collect_idle_grace: std::cmp::min(
                SELECTED_GROUP_OWNER_ROUTE_COLLECT_IDLE_GRACE,
                route_timeout,
            ),
        }
    }

    fn route_timeout(self) -> Duration {
        self.route_timeout
    }

    fn collect_idle_grace(self) -> Duration {
        self.collect_idle_grace
    }
}

impl TreePitProxyRoutePlan {
    fn new(route_timeout: Duration) -> Self {
        Self {
            route_timeout,
            collect_idle_grace: std::cmp::min(
                SELECTED_GROUP_PROXY_ROUTE_COLLECT_IDLE_GRACE,
                route_timeout,
            ),
            retry_backoff: std::cmp::min(SELECTED_GROUP_PROXY_ROUTE_RETRY_BACKOFF, route_timeout),
        }
    }

    fn route_timeout(self) -> Duration {
        self.route_timeout
    }

    fn collect_idle_grace(self) -> Duration {
        self.collect_idle_grace
    }

    fn retry_backoff(self) -> Duration {
        self.retry_backoff
    }

    fn machine(self) -> ProxyRouteMachine {
        self.machine_from_now(tokio::time::Instant::now())
    }

    fn machine_from_now(self, now: tokio::time::Instant) -> ProxyRouteMachine {
        ProxyRouteMachine {
            plan: self,
            route_deadline: now + self.route_timeout,
        }
    }
}

impl ProxyRouteMachine {
    fn route_timeout(self) -> Duration {
        self.plan.route_timeout()
    }

    fn attempt_at(self, now: tokio::time::Instant) -> Option<TreePitProxyAttemptRuntime> {
        let attempt_timeout = self
            .route_deadline
            .checked_duration_since(now)
            .unwrap_or_default();
        if attempt_timeout.is_zero() {
            return None;
        }
        Some(TreePitProxyAttemptRuntime {
            attempt_timeout,
            collect_idle_grace: std::cmp::min(self.plan.collect_idle_grace(), attempt_timeout),
            retry_backoff: std::cmp::min(self.plan.retry_backoff(), attempt_timeout),
        })
    }

    fn entry_phase(self) -> ProxyRouteMachinePhase {
        self.entry_phase_at(tokio::time::Instant::now())
    }

    fn entry_phase_at(self, now: tokio::time::Instant) -> ProxyRouteMachinePhase {
        match self.attempt_at(now) {
            Some(attempt_runtime) => ProxyRouteMachinePhase::Attempt(attempt_runtime),
            None => ProxyRouteMachinePhase::Failed(RouteTerminalError::Timeout),
        }
    }

    fn followup_phase_at(self, now: tokio::time::Instant, err: CnxError) -> ProxyRouteMachinePhase {
        if !is_retryable_materialized_proxy_continuity_gap(&err) {
            ProxyRouteMachinePhase::Failed(RouteTerminalError::from_error(err))
        } else {
            match self.attempt_at(now) {
                Some(attempt_runtime) => {
                    ProxyRouteMachinePhase::WaitUntil(now + attempt_runtime.retry_backoff)
                }
                None => ProxyRouteMachinePhase::Failed(RouteTerminalError::from_error(err)),
            }
        }
    }
}

impl TreePitGroupPlan {
    fn unavailable_stage_lane(self) -> TreePitUnavailableStageLane {
        if self.stage_plan.trusted_materialized_ready_group {
            if self.stage_plan.empty_root_requires_fail_closed {
                TreePitUnavailableStageLane::FailClosedEmptyRoot
            } else {
                TreePitUnavailableStageLane::FailClosedUnavailable
            }
        } else {
            TreePitUnavailableStageLane::PushEmptySnapshot
        }
    }

    fn remaining_ranked_groups(self) -> usize {
        self.total_ranked_groups.saturating_sub(self.rank_index)
    }

    fn is_last_ranked_group(self) -> bool {
        self.rank_index + 1 == self.total_ranked_groups
    }

    fn stage_timeout(self, remaining_session: Duration) -> Duration {
        self.stage_plan
            .stage_timeout(remaining_session, self.caller_timeout)
    }

    fn owner_attempt_timeout(self, remaining_session: Duration) -> Duration {
        if remaining_session.is_zero() {
            return Duration::ZERO;
        }
        if !self.stage_plan.reserve_proxy_budget {
            return remaining_session;
        }

        let owner_min_budget = std::cmp::min(
            remaining_session,
            if self.stage_plan.empty_root_requires_fail_closed {
                TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET
            } else {
                TRUSTED_READY_LATER_RANKED_NON_ROOT_RETRY_BUDGET
            },
        );
        let proxy_reserve = std::cmp::min(
            remaining_session.checked_div(2).unwrap_or_default(),
            std::cmp::min(remaining_session, SELECTED_GROUP_PROXY_FALLBACK_MIN_BUDGET),
        );
        let capped_proxy_reserve = std::cmp::min(
            proxy_reserve,
            remaining_session.saturating_sub(owner_min_budget),
        );
        remaining_session.saturating_sub(capped_proxy_reserve)
    }

    fn owner_empty_tree_retry_timeout(self, remaining_session: Duration) -> Duration {
        std::cmp::min(remaining_session, TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET)
    }

    fn owner_route_plan(self, remaining_session: Duration) -> SelectedGroupOwnerRoutePlan {
        SelectedGroupOwnerRoutePlan::new(self.owner_attempt_timeout(remaining_session))
    }

    fn owner_empty_tree_rescue_plan(
        self,
        facts: SelectedGroupOwnerEmptyTreeRescueFacts,
    ) -> SelectedGroupOwnerEmptyTreeRescuePlan {
        let lane = decide_empty_tree_rescue_lane(EmptyTreeRescueLaneFacts {
            allow_owner_retry: facts.owner_response_is_empty
                && !facts.requires_proxy_fallback
                && self.stage_plan.allow_empty_owner_retry
                && facts.selected_group_sink_reports_live_materialized,
            // First-ranked trusted non-root lanes still need the generic proxy
            // after the bounded owner retry. The PIT layer can later settle an
            // empty proxy payload for legitimate missing-subtree/exact-file
            // cases without short-circuiting recovery here.
            settle_after_owner_retry: false,
            attempt_proxy_fallback: (facts.requires_proxy_fallback
                && (self.stage_plan.reserve_proxy_budget
                    || self.stage_plan.allow_empty_owner_retry))
                || (facts.owner_response_is_empty
                    && !facts.requires_proxy_fallback
                    && self.stage_plan.allow_empty_owner_retry
                    && facts.selected_group_sink_reports_live_materialized),
            fail_closed_without_proxy_budget: facts.requires_proxy_fallback
                && !self.stage_plan.reserve_proxy_budget
                && !self.stage_plan.allow_empty_owner_retry,
        });

        SelectedGroupOwnerEmptyTreeRescuePlan {
            lane,
            attempt_primary_owner_reroute: facts.requires_primary_owner_reroute,
        }
    }

    fn empty_response_rescue_plan(
        self,
        selected_group_owner_known: bool,
        remaining_session: Duration,
    ) -> TrustedMaterializedEmptyResponseRescuePlan {
        let settle_after_prior_exact_file_decode = self.stage_plan.trusted_materialized_ready_group
            && self.prior_materialized_exact_file_decoded
            && !self.stage_plan.empty_root_requires_fail_closed;
        let settle_after_initial_empty_retry_path = self.stage_plan.allow_empty_owner_retry
            && !self.stage_plan.empty_root_requires_fail_closed;
        let owner_retry_timeout = if settle_after_initial_empty_retry_path
            || settle_after_prior_exact_file_decode
            || !selected_group_owner_known
        {
            Duration::ZERO
        } else if self.stage_plan.empty_root_requires_fail_closed
            && self.stage_plan.prior_materialized_group_decoded
        {
            remaining_session
        } else if self.stage_plan.prior_materialized_group_decoded {
            std::cmp::min(
                remaining_session,
                TRUSTED_READY_LATER_RANKED_NON_ROOT_RETRY_BUDGET,
            )
        } else {
            std::cmp::min(remaining_session, TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET)
        };
        let attempt_generic_proxy_fallback = selected_group_owner_known
            && !self.stage_plan.empty_root_requires_fail_closed
            && !settle_after_initial_empty_retry_path
            && !settle_after_prior_exact_file_decode;
        let lane = decide_empty_tree_rescue_lane(EmptyTreeRescueLaneFacts {
            allow_owner_retry: !owner_retry_timeout.is_zero(),
            settle_after_owner_retry: !attempt_generic_proxy_fallback,
            attempt_proxy_fallback: attempt_generic_proxy_fallback,
            fail_closed_without_proxy_budget: self.stage_plan.empty_root_requires_fail_closed
                && self.stage_plan.trusted_materialized_ready_group,
        });

        TrustedMaterializedEmptyResponseRescuePlan {
            lane,
            owner_retry_timeout,
            fail_closed_on_proxy_error: self.stage_plan.trusted_materialized_ready_group
                && self.stage_plan.prior_materialized_group_decoded
                && attempt_generic_proxy_fallback,
            fail_closed_on_final_empty_response: self.stage_plan.trusted_materialized_ready_group
                && self.stage_plan.empty_root_requires_fail_closed,
            defer_first_ranked_empty_non_root_group: self
                .stage_plan
                .trusted_materialized_ready_group
                && !self.stage_plan.prior_materialized_group_decoded
                && !self.prior_materialized_exact_file_decoded
                && !self.stage_plan.empty_root_requires_fail_closed,
        }
    }

    fn empty_response_proxy_retry_timeout(self, remaining_session: Duration) -> Duration {
        if self.is_last_ranked_group() {
            std::cmp::min(remaining_session, SELECTED_GROUP_PROXY_FALLBACK_MIN_BUDGET)
        } else {
            let shared_budget_ceiling = remaining_session
                .checked_div(self.remaining_ranked_groups().max(1) as u32)
                .unwrap_or_default();
            std::cmp::min(
                std::cmp::min(remaining_session, SELECTED_GROUP_PROXY_FALLBACK_MIN_BUDGET),
                shared_budget_ceiling,
            )
        }
    }

    fn retryable_gap_owner_retry_timeout(self, remaining_session: Duration) -> Duration {
        if self.stage_plan.empty_root_requires_fail_closed
            && self.stage_plan.prior_materialized_group_decoded
        {
            std::cmp::min(remaining_session, LATER_RANKED_TRUSTED_GROUP_STAGE_BUDGET)
        } else {
            Duration::ZERO
        }
    }

    fn retryable_gap_proxy_timeout(
        self,
        remaining_session: Duration,
        remaining_stage: Duration,
    ) -> Duration {
        let remaining = if !self.stage_plan.empty_root_requires_fail_closed {
            remaining_session + MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE
        } else {
            remaining_stage
        };
        std::cmp::min(remaining, SELECTED_GROUP_PROXY_FALLBACK_MIN_BUDGET)
    }

    fn fail_closed_after_retryable_gap(self) -> bool {
        self.stage_plan.prior_materialized_group_decoded
            || self.stage_plan.empty_root_requires_fail_closed
    }

    fn stats_proxy_retry_timeout(self, remaining_session: Duration) -> Duration {
        self.empty_response_proxy_retry_timeout(remaining_session)
    }

    fn empty_response_proxy_route_plan(self, remaining_session: Duration) -> TreePitProxyRoutePlan {
        TreePitProxyRoutePlan::new(self.empty_response_proxy_retry_timeout(remaining_session))
    }

    fn retryable_gap_proxy_route_plan(
        self,
        remaining_session: Duration,
        remaining_stage: Duration,
    ) -> TreePitProxyRoutePlan {
        TreePitProxyRoutePlan::new(
            self.retryable_gap_proxy_timeout(remaining_session, remaining_stage),
        )
    }

    fn stats_proxy_route_plan(self, remaining_session: Duration) -> TreePitProxyRoutePlan {
        TreePitProxyRoutePlan::new(self.stats_proxy_retry_timeout(remaining_session))
    }

    fn owner_route_gap_proxy_route_plan(
        self,
        remaining_session: Duration,
        trusted_materialized_tree_query: bool,
    ) -> TreePitProxyRoutePlan {
        let policy = self
            .stage_plan
            .owner_route_gap_proxy_policy(trusted_materialized_tree_query);
        TreePitProxyRoutePlan::new(policy.proxy_timeout(remaining_session))
    }

    fn direct_proxy_route_plan(self, remaining_session: Duration) -> TreePitProxyRoutePlan {
        TreePitProxyRoutePlan::new(remaining_session)
    }
}

fn trusted_materialized_deferred_empty_group_decision(
    input: TrustedMaterializedDeferredEmptyGroupDecisionInput,
) -> TrustedMaterializedDeferredEmptyGroupDecision {
    if input.defer_first_ranked_empty_non_root_group
        && input.rank_index == 0
        && input.response_is_empty
    {
        return TrustedMaterializedDeferredEmptyGroupDecision {
            lane: TrustedMaterializedDeferredEmptyGroupLane::DeferCurrentFirstRankedEmptyGroup,
            should_defer_current_group: true,
            should_fail_closed_prior_deferred_group: false,
        };
    }

    if input.deferred_group_present && input.rank_index > 0 && !input.response_is_empty {
        return TrustedMaterializedDeferredEmptyGroupDecision {
            lane: TrustedMaterializedDeferredEmptyGroupLane::FailClosedPriorDeferredGroup,
            should_defer_current_group: false,
            should_fail_closed_prior_deferred_group: true,
        };
    }

    TrustedMaterializedDeferredEmptyGroupDecision {
        lane: TrustedMaterializedDeferredEmptyGroupLane::KeepCurrent,
        should_defer_current_group: false,
        should_fail_closed_prior_deferred_group: false,
    }
}

struct TrustedMaterializedEmptyTreeResponseRescueInput<'a> {
    state: &'a ApiState,
    policy: &'a ProjectionPolicy,
    tree_params: InternalQueryRequest,
    selected_group_sink_status: Option<&'a SinkStatusSnapshot>,
    group_plan: TreePitGroupPlan,
    empty_response_plan: TrustedMaterializedEmptyResponseRescuePlan,
    group_key: &'a str,
    query_path: &'a [u8],
    session_deadline: tokio::time::Instant,
}

async fn rescue_trusted_materialized_empty_tree_response(
    input: TrustedMaterializedEmptyTreeResponseRescueInput<'_>,
    response: &mut TreeGroupPayload,
) -> Result<(), CnxError> {
    if !input.empty_response_plan.owner_retry_timeout.is_zero() {
        match run_timed_query(
            query_materialized_events_via_selected_group_owner_direct(
                input.state,
                input.tree_params.clone(),
                input.empty_response_plan.owner_retry_timeout,
                input.selected_group_sink_status,
            ),
            input.empty_response_plan.owner_retry_timeout,
        )
        .await
        {
            Ok(retry_events) => {
                if let Ok(mut retry_response) = decode_materialized_selected_group_response(
                    &retry_events,
                    input.policy,
                    input.group_key,
                    input.query_path,
                ) {
                    maybe_promote_richer_same_path_materialized_tree_response(
                        &retry_events,
                        input.policy,
                        input.group_key,
                        input.query_path,
                        &mut retry_response,
                    );
                    if !trusted_materialized_tree_payload_is_empty(&retry_response) {
                        *response = retry_response;
                    }
                }
            }
            Err(CnxError::Timeout)
            | Err(CnxError::TransportClosed(_))
            | Err(CnxError::ProtocolViolation(_))
                if input.group_plan.stage_plan.empty_root_requires_fail_closed =>
            {
                return Err(trusted_materialized_empty_selected_group_tree_error(
                    input.group_key,
                ));
            }
            Err(CnxError::Timeout)
            | Err(CnxError::TransportClosed(_))
            | Err(CnxError::ProtocolViolation(_)) => {}
            Err(err) => return Err(err),
        }
    }

    if matches!(
        input.empty_response_plan.lane,
        EmptyTreeRescueLane::OwnerRetryThenProxyFallback | EmptyTreeRescueLane::ProxyFallback
    ) && trusted_materialized_tree_payload_is_empty(response)
        && let QueryBackend::Route {
            boundary,
            origin_id,
            ..
        } = &input.state.backend
    {
        let remaining = input
            .session_deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_default();
        if !remaining.is_zero() {
            let proxy_route_plan = input.group_plan.empty_response_proxy_route_plan(remaining);
            match query_materialized_events_via_generic_proxy(
                boundary.clone(),
                origin_id.clone(),
                input.tree_params.clone(),
                proxy_route_plan.machine(),
            )
            .await
            {
                Ok(proxy_events) => match decode_materialized_selected_group_response(
                    &proxy_events,
                    input.policy,
                    input.group_key,
                    input.query_path,
                ) {
                    Ok(mut proxy_response) => {
                        maybe_promote_richer_same_path_materialized_tree_response(
                            &proxy_events,
                            input.policy,
                            input.group_key,
                            input.query_path,
                            &mut proxy_response,
                        );
                        if !trusted_materialized_tree_payload_is_empty(&proxy_response) {
                            *response = proxy_response;
                        }
                    }
                    Err(err) if input.empty_response_plan.fail_closed_on_proxy_error => {
                        if debug_materialized_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: pit_group_stage proxy_fallback_decode_failed group={} path={} err={}",
                                input.group_key,
                                String::from_utf8_lossy(input.query_path),
                                err
                            );
                        }
                        return Err(trusted_materialized_unavailable_selected_group_tree_error(
                            input.group_key,
                        ));
                    }
                    Err(_) => {}
                },
                Err(CnxError::Timeout)
                | Err(CnxError::TransportClosed(_))
                | Err(CnxError::ProtocolViolation(_))
                    if input.empty_response_plan.fail_closed_on_proxy_error =>
                {
                    if debug_materialized_route_capture_enabled() {
                        eprintln!(
                            "fs_meta_query_api: pit_group_stage proxy_fallback_failed group={} path={} err=retryable-proxy-failure",
                            input.group_key,
                            String::from_utf8_lossy(input.query_path),
                        );
                    }
                    return Err(trusted_materialized_unavailable_selected_group_tree_error(
                        input.group_key,
                    ));
                }
                Err(_) => {}
            }
        }
    }

    Ok(())
}

struct TreePitStageExecutionInput<'a> {
    state: &'a ApiState,
    policy: &'a ProjectionPolicy,
    tree_params: InternalQueryRequest,
    events: &'a [Event],
    selected_group_sink_status: Option<&'a SinkStatusSnapshot>,
    group_plan: TreePitGroupPlan,
    stage_timing: TreePitStageTiming,
    selected_group_owner_known: bool,
    group_key: &'a str,
    query_path: &'a [u8],
    request_scoped_schedule_omitted_ready_groups: Option<&'a BTreeSet<String>>,
}

async fn resolve_tree_pit_stage_execution_action(
    input: TreePitStageExecutionInput<'_>,
) -> TreePitStageExecutionAction {
    match decode_materialized_selected_group_response(
        input.events,
        input.policy,
        input.group_key,
        input.query_path,
    ) {
        Ok(mut response) => {
            let empty_response_plan = input
                .stage_timing
                .empty_response_rescue_plan(input.group_plan, input.selected_group_owner_known);
            maybe_promote_richer_same_path_materialized_tree_response(
                input.events,
                input.policy,
                input.group_key,
                input.query_path,
                &mut response,
            );
            if input.group_plan.stage_plan.requires_rescue
                && trusted_materialized_tree_payload_is_empty(&response)
                && let Err(err) = rescue_trusted_materialized_empty_tree_response(
                    TrustedMaterializedEmptyTreeResponseRescueInput {
                        state: input.state,
                        policy: input.policy,
                        tree_params: input.tree_params,
                        selected_group_sink_status: input.selected_group_sink_status,
                        group_plan: input.group_plan,
                        empty_response_plan,
                        group_key: input.group_key,
                        query_path: input.query_path,
                        session_deadline: input.stage_timing.session_deadline,
                    },
                    &mut response,
                )
                .await
            {
                return TreePitStageExecutionAction::ReturnError(err);
            }
            TreePitStageExecutionAction::PushPayload {
                response,
                empty_response_plan,
            }
        }
        Err(err) => match input.stage_timing.decode_failure_action(
            input.group_plan,
            input.group_key,
            input.query_path,
            input.request_scoped_schedule_omitted_ready_groups,
            err,
        ) {
            TreePitStageDecodeErrorAction::PushEmptySnapshot => {
                TreePitStageExecutionAction::PushEmptySnapshot
            }
            TreePitStageDecodeErrorAction::PushErrorSnapshot(err) => {
                TreePitStageExecutionAction::PushErrorSnapshot(err)
            }
            TreePitStageDecodeErrorAction::ReturnError(err) => {
                TreePitStageExecutionAction::ReturnError(err)
            }
        },
    }
}

fn finalize_trusted_materialized_tree_group_response(
    groups: &mut Vec<GroupPitSnapshot>,
    group_rank_metrics: &mut BTreeMap<String, Option<u64>>,
    group_order: GroupOrder,
    read_class: ReadClass,
    group_key: String,
    response: TreeGroupPayload,
    empty_response_plan: TrustedMaterializedEmptyResponseRescuePlan,
    rank_index: usize,
    deferred_first_ranked_empty_trusted_non_root_group: &mut Option<String>,
) -> Result<(), CnxError> {
    if empty_response_plan.fail_closed_on_final_empty_response
        && trusted_materialized_tree_payload_is_empty(&response)
    {
        return Err(trusted_materialized_empty_selected_group_tree_error(
            &group_key,
        ));
    }

    let deferred_empty_group_decision = trusted_materialized_deferred_empty_group_decision(
        TrustedMaterializedDeferredEmptyGroupDecisionInput {
            defer_first_ranked_empty_non_root_group: empty_response_plan
                .defer_first_ranked_empty_non_root_group,
            rank_index,
            response_is_empty: trusted_materialized_tree_payload_is_empty(&response),
            deferred_group_present: deferred_first_ranked_empty_trusted_non_root_group.is_some(),
        },
    );
    if deferred_empty_group_decision.should_defer_current_group {
        *deferred_first_ranked_empty_trusted_non_root_group = Some(group_key.clone());
    } else if deferred_empty_group_decision.should_fail_closed_prior_deferred_group {
        let deferred_group = deferred_first_ranked_empty_trusted_non_root_group
            .as_ref()
            .expect("deferred empty group must exist when fail-closing prior deferred group");
        return Err(trusted_materialized_empty_selected_group_tree_error(
            deferred_group,
        ));
    }

    push_materialized_tree_group_payload(
        groups,
        group_rank_metrics,
        group_order,
        read_class,
        group_key,
        response,
    );
    Ok(())
}

fn apply_tree_pit_stage_execution_action(
    action: TreePitStageExecutionAction,
    groups: &mut Vec<GroupPitSnapshot>,
    group_rank_metrics: &mut BTreeMap<String, Option<u64>>,
    group_order: GroupOrder,
    read_class: ReadClass,
    group_key: String,
    query_path: &[u8],
    rank_index: usize,
    deferred_first_ranked_empty_trusted_non_root_group: &mut Option<String>,
) -> Result<(), CnxError> {
    match action {
        TreePitStageExecutionAction::PushPayload {
            response,
            empty_response_plan,
        } => finalize_trusted_materialized_tree_group_response(
            groups,
            group_rank_metrics,
            group_order,
            read_class,
            group_key,
            response,
            empty_response_plan,
            rank_index,
            deferred_first_ranked_empty_trusted_non_root_group,
        ),
        TreePitStageExecutionAction::PushEmptySnapshot => {
            push_empty_materialized_tree_group_snapshot(
                groups,
                group_rank_metrics,
                group_order,
                read_class,
                group_key,
                query_path,
            );
            Ok(())
        }
        TreePitStageExecutionAction::PushErrorSnapshot(err) => {
            push_materialized_tree_group_error_snapshot(groups, read_class, group_key, err);
            Ok(())
        }
        TreePitStageExecutionAction::ReturnError(err) => Err(err),
    }
}

struct RetryableTreePitStageGapInput<'a> {
    state: &'a ApiState,
    policy: &'a ProjectionPolicy,
    tree_params: InternalQueryRequest,
    selected_group_sink_status: Option<&'a SinkStatusSnapshot>,
    retryable_gap: RetryableTreePitStageGap,
    group_key: String,
    query_path: &'a [u8],
    read_class: ReadClass,
    group_order: GroupOrder,
}

async fn handle_retryable_tree_pit_stage_gap(
    input: RetryableTreePitStageGapInput<'_>,
    err: CnxError,
    groups: &mut Vec<GroupPitSnapshot>,
    group_rank_metrics: &mut BTreeMap<String, Option<u64>>,
) -> Result<(), CnxError> {
    let group_plan = input.retryable_gap.group_plan;
    if group_plan.stage_plan.requires_rescue {
        let now = tokio::time::Instant::now();
        let empty_root_requires_fail_closed = group_plan.stage_plan.empty_root_requires_fail_closed;
        let fail_closed_after_retryable_gap = input.retryable_gap.fail_closed_after_retryable_gap();
        let outer_owner_retry_timeout = input.retryable_gap.owner_retry_timeout_at(now);
        if !outer_owner_retry_timeout.is_zero() {
            if debug_materialized_route_capture_enabled() {
                eprintln!(
                    "fs_meta_query_api: pit_group_stage owner_retry_after_proxy_gap group={} path={} owner_retry_timeout_ms={}",
                    input.group_key,
                    String::from_utf8_lossy(input.query_path),
                    outer_owner_retry_timeout.as_millis()
                );
            }
            if let Ok(retry_events) = run_timed_query(
                query_materialized_events_via_selected_group_owner_direct(
                    input.state,
                    input.tree_params.clone(),
                    outer_owner_retry_timeout,
                    input.selected_group_sink_status,
                ),
                outer_owner_retry_timeout,
            )
            .await
                && let Ok(mut retry_response) = decode_materialized_selected_group_response(
                    &retry_events,
                    input.policy,
                    &input.group_key,
                    input.query_path,
                )
            {
                maybe_promote_richer_same_path_materialized_tree_response(
                    &retry_events,
                    input.policy,
                    &input.group_key,
                    input.query_path,
                    &mut retry_response,
                );
                if !trusted_materialized_tree_payload_is_empty(&retry_response) {
                    push_materialized_tree_group_payload(
                        groups,
                        group_rank_metrics,
                        input.group_order,
                        input.read_class,
                        input.group_key,
                        retry_response,
                    );
                    return Ok(());
                }
            }
        }
        if let QueryBackend::Route {
            boundary,
            origin_id,
            ..
        } = &input.state.backend
        {
            if let Some(proxy_retry_machine) = input.retryable_gap.proxy_route_machine_at(now) {
                if debug_materialized_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: pit_group_stage proxy_fallback_after_owner_timeout group={} path={} proxy_timeout_ms={} remaining_ms={}",
                        input.group_key,
                        String::from_utf8_lossy(input.query_path),
                        proxy_retry_machine.route_timeout().as_millis(),
                        proxy_retry_machine.route_timeout().as_millis(),
                    );
                }
                match query_materialized_events_via_generic_proxy(
                    boundary.clone(),
                    origin_id.clone(),
                    input.tree_params.clone(),
                    proxy_retry_machine,
                )
                .await
                {
                    Ok(proxy_events) => {
                        match decode_materialized_selected_group_response(
                            &proxy_events,
                            input.policy,
                            &input.group_key,
                            input.query_path,
                        ) {
                            Ok(mut proxy_response) => {
                                maybe_promote_richer_same_path_materialized_tree_response(
                                    &proxy_events,
                                    input.policy,
                                    &input.group_key,
                                    input.query_path,
                                    &mut proxy_response,
                                );
                                if debug_materialized_route_capture_enabled() {
                                    eprintln!(
                                        "fs_meta_query_api: pit_group_stage proxy_fallback_decode group={} root_exists={} entries={} has_children={}",
                                        input.group_key,
                                        proxy_response.root.exists,
                                        proxy_response.entries.len(),
                                        proxy_response.root.has_children
                                    );
                                }
                                if !empty_root_requires_fail_closed
                                    || !trusted_materialized_tree_payload_is_empty(&proxy_response)
                                {
                                    push_materialized_tree_group_payload(
                                        groups,
                                        group_rank_metrics,
                                        input.group_order,
                                        input.read_class,
                                        input.group_key,
                                        proxy_response,
                                    );
                                    return Ok(());
                                }
                            }
                            Err(err) if fail_closed_after_retryable_gap => {
                                if debug_materialized_route_capture_enabled() {
                                    eprintln!(
                                        "fs_meta_query_api: pit_group_stage proxy_fallback_decode_failed group={} path={} err={}",
                                        input.group_key,
                                        String::from_utf8_lossy(input.query_path),
                                        err
                                    );
                                }
                                return Err(
                                    trusted_materialized_unavailable_selected_group_tree_error(
                                        &input.group_key,
                                    ),
                                );
                            }
                            Err(_) => {}
                        }
                    }
                    Err(CnxError::Timeout)
                        if fail_closed_after_retryable_gap
                            && group_plan.stage_plan.trusted_materialized_ready_group
                            && trusted_materialized_empty_group_root_requires_fail_closed(
                                input.query_path,
                            ) =>
                    {
                        if debug_materialized_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: pit_group_stage proxy_fallback_failed group={} path={} err=timeout-preserved",
                                input.group_key,
                                String::from_utf8_lossy(input.query_path),
                            );
                        }
                        return Err(CnxError::Timeout);
                    }
                    Err(CnxError::Timeout)
                    | Err(CnxError::TransportClosed(_))
                    | Err(CnxError::ProtocolViolation(_))
                        if fail_closed_after_retryable_gap =>
                    {
                        if debug_materialized_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: pit_group_stage proxy_fallback_failed group={} path={} err=retryable-proxy-failure",
                                input.group_key,
                                String::from_utf8_lossy(input.query_path),
                            );
                        }
                        return Err(trusted_materialized_unavailable_selected_group_tree_error(
                            &input.group_key,
                        ));
                    }
                    Err(_) => {}
                }
            }
        }
        if fail_closed_after_retryable_gap {
            if debug_materialized_route_capture_enabled() {
                eprintln!(
                    "fs_meta_query_api: pit_group_stage fail_closed_retryable_gap group={} path={} err={}",
                    input.group_key,
                    String::from_utf8_lossy(input.query_path),
                    err
                );
            }
            if group_plan.stage_plan.trusted_materialized_ready_group
                && trusted_materialized_empty_group_root_requires_fail_closed(input.query_path)
                && matches!(err, CnxError::Timeout)
            {
                return Err(CnxError::Timeout);
            }
            return Err(trusted_materialized_unavailable_selected_group_tree_error(
                &input.group_key,
            ));
        }
        if debug_materialized_route_capture_enabled() {
            eprintln!(
                "fs_meta_query_api: pit_group_stage fail_closed group={} path={} err={}",
                input.group_key,
                String::from_utf8_lossy(input.query_path),
                err
            );
        }
        return Err(trusted_materialized_unavailable_selected_group_tree_error(
            &input.group_key,
        ));
    }

    push_empty_materialized_tree_group_snapshot(
        groups,
        group_rank_metrics,
        input.group_order,
        input.read_class,
        input.group_key,
        input.query_path,
    );
    Ok(())
}

struct TreePitRankedGroupExecutionInput<'a> {
    state: &'a ApiState,
    policy: &'a ProjectionPolicy,
    params: &'a NormalizedApiParams,
    request_sink_status: Option<&'a SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<&'a BTreeSet<String>>,
    observation_state: ObservationState,
    session_plan: TreePitSessionPlan,
    session_timing: TreePitSessionTiming,
    read_class: ReadClass,
    total_ranked_groups: usize,
}

fn sink_primary_owner_node_for_group(
    sink_status: Option<&SinkStatusSnapshot>,
    group_id: &str,
) -> Option<NodeId> {
    sink_status
        .and_then(|snapshot| {
            snapshot
                .groups
                .iter()
                .find(|group| group.group_id == group_id)
        })
        .and_then(|group| node_id_from_object_ref(&group.primary_object_ref))
}

fn trusted_materialized_empty_selected_group_tree_error(group_id: &str) -> CnxError {
    CnxError::NotReady(format!(
        "trusted-materialized selected-group tree remained empty despite ready sink status for group {group_id}"
    ))
}

fn trusted_materialized_unavailable_selected_group_tree_error(group_id: &str) -> CnxError {
    CnxError::NotReady(format!(
        "trusted-materialized selected-group tree remained unavailable despite ready sink status for group {group_id}"
    ))
}

fn trusted_materialized_unavailable_selected_group_stats_error(group_id: &str) -> CnxError {
    CnxError::NotReady(format!(
        "trusted-materialized selected-group stats remained unavailable despite ready sink status for group {group_id}"
    ))
}

#[test]
fn selected_group_sink_status_reports_live_materialized_group_requires_positive_total_nodes() {
    let snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs2".to_string(),
            primary_object_ref: "node-a::nfs2".to_string(),
            total_nodes: 0,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 9,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    assert!(
        !selected_group_sink_status_reports_live_materialized_group(Some(&snapshot), "nfs2"),
        "live materialized selected-group status must not treat total_nodes=0 as ready even when live_nodes>0 and initial_audit_completed=true: {snapshot:?}"
    );
}

#[test]
fn selected_group_sink_status_reports_live_materialized_group_prefers_exported_readiness_over_legacy_initial_audit_bool()
 {
    let snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs2".to_string(),
            primary_object_ref: "node-a::nfs2".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 9,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    assert!(
        selected_group_sink_status_reports_live_materialized_group(Some(&snapshot), "nfs2"),
        "selected-group readiness must trust exported sink readiness over stale initial_audit_completed=false: {snapshot:?}"
    );
}

#[test]
fn selected_group_sink_status_is_unready_empty_prefers_exported_readiness_over_legacy_initial_audit_bool()
 {
    let snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs2".to_string(),
            primary_object_ref: "node-a::nfs2".to_string(),
            total_nodes: 0,
            live_nodes: 0,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
            materialized_revision: 9,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    assert!(
        selected_group_sink_status_is_unready_empty(Some(&snapshot), "nfs2"),
        "selected-group unready-empty classification must trust exported sink readiness over stale initial_audit_completed=true: {snapshot:?}"
    );
}

#[test]
fn selected_group_tree_pit_plan_allows_empty_owner_retry_only_for_first_ready_group() {
    let plan = SelectedGroupTreePitPlan::new(SelectedGroupTreePitPlanInput {
        read_class: ReadClass::TrustedMaterialized,
        observation_state: ObservationState::TrustedMaterialized,
        selected_group_sink_reports_live_materialized: true,
        prior_materialized_group_decoded: false,
        is_last_ranked_group: false,
        selected_group_sink_unready_empty: false,
        empty_root_requires_fail_closed: false,
    });

    assert_eq!(
        plan,
        SelectedGroupTreePitPlan {
            prior_materialized_group_decoded: false,
            trusted_materialized_ready_group: true,
            requires_rescue: true,
            allow_empty_owner_retry: true,
            should_resolve_selected_group_owner: true,
            empty_root_requires_fail_closed: false,
            stage_timeout_policy: TreePitStageTimeoutPolicy::FirstRankedTrustedReadyBudget,
            reserve_proxy_budget: true,
        },
        "first-ranked trusted ready group should fold to a single rescue decision that keeps one empty-owner retry and selected-owner resolution"
    );
}

#[test]
fn selected_group_tree_pit_plan_keeps_owner_resolution_for_later_non_root_rescue_without_empty_owner_retry()
 {
    let plan = SelectedGroupTreePitPlan::new(SelectedGroupTreePitPlanInput {
        read_class: ReadClass::TrustedMaterialized,
        observation_state: ObservationState::TrustedMaterialized,
        selected_group_sink_reports_live_materialized: false,
        prior_materialized_group_decoded: true,
        is_last_ranked_group: false,
        selected_group_sink_unready_empty: false,
        empty_root_requires_fail_closed: false,
    });

    assert_eq!(
        plan,
        SelectedGroupTreePitPlan {
            prior_materialized_group_decoded: true,
            trusted_materialized_ready_group: false,
            requires_rescue: true,
            allow_empty_owner_retry: false,
            should_resolve_selected_group_owner: true,
            empty_root_requires_fail_closed: false,
            stage_timeout_policy: TreePitStageTimeoutPolicy::LaterRankedTrustedGroupBudget,
            reserve_proxy_budget: true,
        },
        "later-ranked trusted non-root rescue should still resolve the selected-group owner, but must not reopen the first-ranked empty-owner retry lane"
    );
}

#[test]
fn selected_group_tree_pit_plan_disables_non_root_rescue_when_empty_root_must_fail_closed() {
    let plan = SelectedGroupTreePitPlan::new(SelectedGroupTreePitPlanInput {
        read_class: ReadClass::TrustedMaterialized,
        observation_state: ObservationState::TrustedMaterialized,
        selected_group_sink_reports_live_materialized: false,
        prior_materialized_group_decoded: true,
        is_last_ranked_group: false,
        selected_group_sink_unready_empty: false,
        empty_root_requires_fail_closed: true,
    });

    assert_eq!(
        plan,
        SelectedGroupTreePitPlan {
            prior_materialized_group_decoded: true,
            trusted_materialized_ready_group: false,
            requires_rescue: false,
            allow_empty_owner_retry: false,
            should_resolve_selected_group_owner: false,
            empty_root_requires_fail_closed: true,
            stage_timeout_policy: TreePitStageTimeoutPolicy::DefaultCollectIdleGraceCap,
            reserve_proxy_budget: true,
        },
        "empty-root fail-closed paths must not inherit the later-ranked non-root rescue lane"
    );
}

#[test]
fn selected_group_tree_pit_plan_fail_closes_route_gap_only_for_trusted_root_without_empty_owner_retry()
 {
    let policy = SelectedGroupTreePitPlan::new(SelectedGroupTreePitPlanInput {
        read_class: ReadClass::TrustedMaterialized,
        observation_state: ObservationState::TrustedMaterialized,
        selected_group_sink_reports_live_materialized: true,
        prior_materialized_group_decoded: true,
        is_last_ranked_group: false,
        selected_group_sink_unready_empty: false,
        empty_root_requires_fail_closed: true,
    })
    .owner_route_gap_proxy_policy(true);

    assert_eq!(
        policy,
        SelectedGroupOwnerRouteGapProxyPolicy::FailClosedAfterProxyGap,
        "trusted-materialized root lanes that reserve proxy budget and disallow empty-owner retry must fail closed after an owner route continuity gap"
    );
    assert_eq!(
        policy.proxy_timeout(Duration::from_secs(5)),
        TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET,
        "fail-closed owner route gap lanes should cap the proxy attempt to the selected-group retry budget"
    );
}

#[test]
fn selected_group_tree_pit_plan_uses_remaining_budget_for_non_fail_closed_route_gap_paths() {
    let policy = SelectedGroupTreePitPlan::new(SelectedGroupTreePitPlanInput {
        read_class: ReadClass::TrustedMaterialized,
        observation_state: ObservationState::TrustedMaterialized,
        selected_group_sink_reports_live_materialized: true,
        prior_materialized_group_decoded: false,
        is_last_ranked_group: false,
        selected_group_sink_unready_empty: false,
        empty_root_requires_fail_closed: true,
    })
    .owner_route_gap_proxy_policy(true);

    assert_eq!(
        policy,
        SelectedGroupOwnerRouteGapProxyPolicy::UseRemainingBudget,
        "lanes that still allow empty-owner retry must keep the full remaining proxy budget after an owner route gap"
    );
    assert_eq!(
        policy.proxy_timeout(Duration::from_secs(5)),
        Duration::from_secs(5),
        "non-fail-closed owner route gap lanes should keep the remaining proxy budget intact"
    );
}

#[test]
fn tree_pit_group_plan_splits_proxy_budget_across_remaining_groups() {
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 3).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: false,
            prior_materialized_group_decoded: true,
            prior_materialized_exact_file_decoded: false,
            rank_index: 1,
            is_last_ranked_group: false,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert_eq!(
        plan.empty_response_proxy_retry_timeout(Duration::from_millis(1800)),
        Duration::from_millis(900),
        "middle-ranked PIT groups should share the remaining proxy budget instead of each reopening the full fallback lane"
    );
}

#[test]
fn tree_pit_group_plan_caps_retryable_gap_owner_retry_for_fail_closed_prior_decode() {
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 2).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: true,
            prior_materialized_exact_file_decoded: false,
            rank_index: 1,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: true,
        },
    );

    assert_eq!(
        plan.retryable_gap_owner_retry_timeout(Duration::from_secs(5)),
        LATER_RANKED_TRUSTED_GROUP_STAGE_BUDGET,
        "fail-closed later-ranked retry lanes should stay capped to the later-ranked trusted stage budget"
    );
}

#[test]
fn tree_pit_group_plan_extends_retryable_gap_proxy_budget_with_idle_grace_for_non_fail_closed_paths()
 {
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 2).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: false,
            prior_materialized_group_decoded: true,
            prior_materialized_exact_file_decoded: false,
            rank_index: 1,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert_eq!(
        plan.retryable_gap_proxy_timeout(Duration::from_millis(1100), Duration::from_millis(50)),
        Duration::from_millis(1600),
        "non-fail-closed retryable gaps should keep the session budget plus collect grace instead of collapsing to the expired stage window"
    );

    assert_eq!(
        plan.retryable_gap_proxy_route_plan(Duration::from_millis(1100), Duration::from_millis(50)),
        TreePitProxyRoutePlan {
            route_timeout: Duration::from_millis(1600),
            collect_idle_grace: SELECTED_GROUP_PROXY_ROUTE_COLLECT_IDLE_GRACE,
            retry_backoff: SELECTED_GROUP_PROXY_ROUTE_RETRY_BACKOFF,
        },
        "retryable-gap proxy lanes should keep proxy collect-idle-grace and retry-backoff inside the planner-owned route plan",
    );
}

#[test]
fn tree_pit_retryable_stage_error_runtime_owns_owner_retry_timeout_from_remaining_session() {
    let now = tokio::time::Instant::now();
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 2).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: true,
            prior_materialized_exact_file_decoded: false,
            rank_index: 1,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: true,
        },
    );
    let runtime = TreePitStageTiming {
        session_deadline: now + Duration::from_millis(600),
        stage_deadline: now + Duration::from_millis(200),
        remaining_session: Duration::from_millis(600),
        stage_timeout: Duration::from_millis(200),
    }
    .retryable_gap(plan);

    assert_eq!(
        runtime.owner_retry_timeout_at(now + Duration::from_millis(100)),
        Duration::from_millis(500),
        "retryable stage-error owner retries should come from runtime-owned remaining-session budget instead of each error helper recomputing deadline deltas inline",
    );
}

#[test]
fn tree_pit_retryable_stage_error_runtime_owns_proxy_route_machine_from_deadlines() {
    let now = tokio::time::Instant::now();
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 2).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: false,
            prior_materialized_group_decoded: true,
            prior_materialized_exact_file_decoded: false,
            rank_index: 1,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );
    let runtime = TreePitStageTiming {
        session_deadline: now + Duration::from_millis(1100),
        stage_deadline: now + Duration::from_millis(50),
        remaining_session: Duration::from_millis(1100),
        stage_timeout: Duration::from_millis(50),
    }
    .retryable_gap(plan);

    assert_eq!(
        runtime.proxy_route_machine_at(now + Duration::from_millis(300)),
        Some(ProxyRouteMachine {
            plan: TreePitProxyRoutePlan {
                route_timeout: Duration::from_millis(1300),
                collect_idle_grace: SELECTED_GROUP_PROXY_ROUTE_COLLECT_IDLE_GRACE,
                retry_backoff: SELECTED_GROUP_PROXY_ROUTE_RETRY_BACKOFF,
            },
            route_deadline: now + Duration::from_millis(1600),
        }),
        "retryable stage-error proxy fallback should use machine-owned session/stage deadlines instead of re-deriving remaining budgets inside the error helper",
    );
}

#[test]
fn tree_pit_group_plan_uses_stage_budget_for_fail_closed_retryable_gap_proxy_timeout() {
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 2).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: true,
            prior_materialized_exact_file_decoded: false,
            rank_index: 1,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: true,
        },
    );

    assert_eq!(
        plan.retryable_gap_proxy_timeout(Duration::from_secs(4), Duration::from_millis(300)),
        Duration::from_millis(300),
        "fail-closed retryable gaps must stay inside the current stage budget instead of borrowing from the outer session"
    );
}

#[test]
fn tree_pit_group_plan_stats_rescue_proxy_fallback_then_fail_closed_for_trusted_ready_root() {
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert_eq!(
        plan.stage_plan.stats_rescue_lane(true, true, true),
        SelectedGroupStatsRescueLane::ProxyFallbackThenFailClosed
    );
}

#[test]
fn tree_pit_group_plan_stats_rescue_proxy_fallback_for_trusted_ready_non_root() {
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert_eq!(
        plan.stage_plan.stats_rescue_lane(false, true, true),
        SelectedGroupStatsRescueLane::ProxyFallback
    );
}

#[test]
fn tree_pit_group_plan_stats_rescue_returns_current_without_trusted_ready_group() {
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: false,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert_eq!(
        plan.stage_plan.stats_rescue_lane(true, true, true),
        SelectedGroupStatsRescueLane::ReturnCurrent
    );
}

#[test]
fn tree_pit_group_plan_owner_snapshot_override_stays_inside_planner() {
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 1)
        .selected_group_owner_snapshot_plan(TreePitOwnerSnapshotPlanInput {
            trusted_materialized_tree_query: false,
            reserve_proxy_budget: false,
            allow_empty_owner_retry: true,
            empty_root_requires_fail_closed: false,
        });

    assert_eq!(
        plan.stage_plan,
        SelectedGroupTreePitPlan {
            prior_materialized_group_decoded: false,
            trusted_materialized_ready_group: false,
            requires_rescue: true,
            allow_empty_owner_retry: true,
            should_resolve_selected_group_owner: true,
            empty_root_requires_fail_closed: false,
            stage_timeout_policy: TreePitStageTimeoutPolicy::DefaultCollectIdleGraceCap,
            reserve_proxy_budget: false,
        },
        "owner-snapshot selected-group plans should fold owner-retry and proxy-reserve overrides inside the planner instead of mutating stage plan at the call site",
    );
}

#[test]
fn tree_pit_session_plan_caps_request_scoped_sink_status_load_budget() {
    assert_eq!(
        TreePitSessionPlan::new(Duration::from_secs(5), 1)
            .request_scoped_sink_status_plan()
            .route_timeout(),
        REQUEST_SCOPED_SINK_STATUS_LOAD_BUDGET,
        "request-scoped sink-status loads should stay inside the planner-owned load budget instead of re-capping query timeout at the helper call site",
    );
    assert_eq!(
        TreePitSessionPlan::new(Duration::from_millis(250), 1)
            .request_scoped_sink_status_plan()
            .route_timeout(),
        Duration::from_millis(250),
        "planner-owned request-scoped sink-status loads should preserve smaller caller budgets",
    );
}

#[test]
fn tree_pit_session_runtime_uses_ranking_started_budget_after_retryable_rankings() {
    let started_at = tokio::time::Instant::now();
    let now = started_at + Duration::from_millis(200);
    let runtime = TreePitSessionPlan::new(Duration::from_secs(1), 1)
        .session_timing_from_now(started_at, true, now);

    assert_eq!(
        runtime.remaining_at(now),
        Duration::from_millis(800),
        "retryable ranking failures must keep PIT session timing anchored to ranking start instead of reopening a fresh session deadline after rankings complete",
    );
}

#[test]
fn tree_pit_session_runtime_owns_stage_deadline_from_remaining_session_budget() {
    let now = tokio::time::Instant::now();
    let runtime =
        TreePitSessionPlan::new(Duration::from_secs(5), 1).session_timing_from_now(now, false, now);
    let group_plan = TreePitSessionPlan::new(Duration::from_secs(5), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert_eq!(
        runtime.stage_timing_for_at(group_plan, now + Duration::from_millis(300)),
        Some(TreePitStageTiming {
            session_deadline: now + Duration::from_secs(5),
            stage_deadline: now + Duration::from_millis(2800),
            remaining_session: Duration::from_millis(4700),
            stage_timeout: FIRST_RANKED_TRUSTED_READY_GROUP_STAGE_BUDGET,
        }),
        "tree PIT runtime should own remaining-session and stage-deadline calculation instead of leaving build_tree_pit_session to re-derive them inline",
    );
}

#[test]
fn tree_pit_session_runtime_prepares_unavailable_trusted_group_lane_from_group_plan() {
    let now = tokio::time::Instant::now();
    let runtime = TreePitSessionPlan::new(Duration::from_millis(10), 1)
        .session_timing_from_now(now, false, now);
    let unavailable_group = TreePitSessionPlan::new(Duration::from_millis(10), 1)
        .selected_group_stage_plan(TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        });
    let empty_root_group = TreePitSessionPlan::new(Duration::from_millis(10), 1)
        .selected_group_stage_plan(TreePitGroupPlanInput {
            empty_root_requires_fail_closed: true,
            ..TreePitGroupPlanInput {
                read_class: ReadClass::TrustedMaterialized,
                observation_state: ObservationState::TrustedMaterialized,
                selected_group_sink_reports_live_materialized: true,
                prior_materialized_group_decoded: false,
                prior_materialized_exact_file_decoded: false,
                rank_index: 0,
                is_last_ranked_group: true,
                selected_group_sink_unready_empty: false,
                empty_root_requires_fail_closed: false,
            }
        });

    assert_eq!(
        runtime.prepare_stage_for_at(unavailable_group, now + Duration::from_millis(20)),
        TreePitStagePreparation::Unavailable(TreePitUnavailableStageLane::FailClosedUnavailable),
        "tree PIT runtime should own the trusted-group unavailable lane instead of leaving build_tree_pit_session to branch on missing stage runtime inline",
    );
    assert_eq!(
        runtime.prepare_stage_for_at(empty_root_group, now + Duration::from_millis(20)),
        TreePitStagePreparation::Unavailable(TreePitUnavailableStageLane::FailClosedEmptyRoot),
        "tree PIT runtime should own the trusted empty-root fail-closed lane instead of leaving build_tree_pit_session to branch on helper-local empty-root booleans",
    );
}

#[test]
fn tree_pit_session_runtime_prepares_unavailable_untrusted_group_as_empty_snapshot() {
    let now = tokio::time::Instant::now();
    let runtime = TreePitSessionPlan::new(Duration::from_millis(10), 1)
        .session_timing_from_now(now, false, now);
    let group_plan = TreePitSessionPlan::new(Duration::from_millis(10), 1)
        .selected_group_stage_plan(TreePitGroupPlanInput {
            read_class: ReadClass::Materialized,
            observation_state: ObservationState::MaterializedUntrusted,
            selected_group_sink_reports_live_materialized: false,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        });

    assert_eq!(
        runtime.prepare_stage_for_at(group_plan, now + Duration::from_millis(20)),
        TreePitStagePreparation::Unavailable(TreePitUnavailableStageLane::PushEmptySnapshot),
        "tree PIT runtime should own the untrusted empty-snapshot lane instead of leaving the main loop to special-case a missing stage runtime",
    );
}

#[test]
fn tree_pit_session_runtime_promotes_stage_entry_action_without_outer_lane_branching() {
    let now = tokio::time::Instant::now();
    let runtime = TreePitSessionPlan::new(Duration::from_millis(10), 1)
        .session_timing_from_now(now, false, now);
    let unavailable_group = TreePitSessionPlan::new(Duration::from_millis(10), 1)
        .selected_group_stage_plan(TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        });
    let untrusted_group = TreePitSessionPlan::new(Duration::from_millis(10), 1)
        .selected_group_stage_plan(TreePitGroupPlanInput {
            read_class: ReadClass::Materialized,
            observation_state: ObservationState::MaterializedUntrusted,
            selected_group_sink_reports_live_materialized: false,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        });

    assert!(
        matches!(
            runtime.entry_action_for_at(
                unavailable_group,
                "group-a",
                now + Duration::from_millis(20),
            ),
            TreePitStageEntryAction::ReturnError(CnxError::NotReady(message))
                if message.contains("group-a")
        ),
        "tree PIT session runtime should convert unavailable trusted-group lanes into a single stage-entry action instead of leaving build_tree_pit_session to branch on raw unavailable lanes inline",
    );
    assert!(
        matches!(
            runtime.entry_action_for_at(
                untrusted_group,
                "group-b",
                now + Duration::from_millis(20),
            ),
            TreePitStageEntryAction::PushEmptySnapshot
        ),
        "tree PIT session runtime should also map untrusted unavailable groups into the canonical push-empty entry action",
    );
}

#[test]
fn tree_pit_stage_runtime_classifies_retryable_failures_through_runtime() {
    let now = tokio::time::Instant::now();
    let stage_runtime = TreePitStageTiming {
        session_deadline: now + Duration::from_secs(2),
        stage_deadline: now + Duration::from_millis(500),
        remaining_session: Duration::from_secs(2),
        stage_timeout: Duration::from_millis(500),
    };
    let group_plan = TreePitSessionPlan::new(Duration::from_secs(2), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert!(
        matches!(
            stage_runtime.query_failure_action(group_plan, CnxError::Timeout),
            TreePitStageQueryFailureAction::RetryableGap {
                gap,
                err: CnxError::Timeout,
            } if gap == stage_runtime.retryable_gap(group_plan)
        ),
        "tree PIT stage runtime should own retryable timeout classification instead of leaving the main loop to pattern-match raw CnxError variants inline",
    );
    assert!(
        matches!(
            stage_runtime.query_failure_action(group_plan, CnxError::AccessDenied("denied".into())),
            TreePitStageQueryFailureAction::Fatal(CnxError::AccessDenied(message)) if message == "denied"
        ),
        "tree PIT stage runtime should keep fatal-error classification in one place instead of leaving the main loop to special-case every non-retryable stage failure",
    );
}

#[test]
fn tree_pit_stage_runtime_routes_request_scoped_omitted_root_decode_gaps_to_empty_snapshot() {
    let stage_runtime = TreePitStageTiming {
        session_deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        stage_deadline: tokio::time::Instant::now() + Duration::from_millis(500),
        remaining_session: Duration::from_secs(2),
        stage_timeout: Duration::from_millis(500),
    };
    let group_plan = TreePitSessionPlan::new(Duration::from_secs(2), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: false,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );
    let omitted_ready_groups = BTreeSet::from([String::from("group-a")]);

    assert!(
        matches!(
            stage_runtime.decode_failure_action(
                group_plan,
                "group-a",
                b"/",
                Some(&omitted_ready_groups),
                CnxError::Internal("tree query returned no payload for requested group".into()),
            ),
            TreePitStageDecodeErrorAction::PushEmptySnapshot
        ),
        "tree PIT stage runtime should own the request-scoped omitted-root decode-gap lane instead of leaving the main loop to funnel raw booleans through a helper-local decode-error state machine",
    );
}

#[test]
fn tree_pit_stage_runtime_fail_closes_trusted_ready_nonempty_decode_error_after_prior_group_decode()
{
    let stage_runtime = TreePitStageTiming {
        session_deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        stage_deadline: tokio::time::Instant::now() + Duration::from_millis(500),
        remaining_session: Duration::from_secs(2),
        stage_timeout: Duration::from_millis(500),
    };
    let group_plan = TreePitSessionPlan::new(Duration::from_secs(2), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: true,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert!(
        matches!(
            stage_runtime.decode_failure_action(
                group_plan,
                "group-b",
                b"/subtree",
                None,
                CnxError::Internal("corrupt tree payload".into()),
            ),
            TreePitStageDecodeErrorAction::ReturnError(CnxError::NotReady(message))
                if message.contains("trusted-materialized selected-group tree remained unavailable")
                    && message.contains("group-b")
        ),
        "tree PIT stage runtime should own the trusted-ready decode-error fail-closed lane instead of leaving build_tree_pit_session to branch on helper-local prior-decode booleans",
    );
}

#[test]
fn tree_pit_stage_runtime_settles_exact_file_decode_gap_as_empty_snapshot() {
    let stage_runtime = TreePitStageTiming {
        session_deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        stage_deadline: tokio::time::Instant::now() + Duration::from_millis(500),
        remaining_session: Duration::from_secs(2),
        stage_timeout: Duration::from_millis(500),
    };
    let group_plan = TreePitSessionPlan::new(Duration::from_secs(2), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: true,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert!(
        matches!(
            stage_runtime.decode_failure_action(
                group_plan,
                "group-c",
                b"/exact-file",
                None,
                CnxError::Internal("tree query returned no payload for requested group".into()),
            ),
            TreePitStageDecodeErrorAction::PushEmptySnapshot
        ),
        "tree PIT stage runtime should own the exact-file decode-gap settle lane instead of leaving build_tree_pit_session to special-case prior exact-file decode flags through a helper-local decode handler",
    );
}

#[test]
fn status_route_plan_caps_attempt_timeout_to_canonical_cap() {
    let plan = StatusRoutePlan::new(Duration::from_secs(30), STATUS_ROUTE_COLLECT_IDLE_GRACE);
    let caller_capped =
        StatusRoutePlan::caller_capped(Duration::from_secs(30), STATUS_ROUTE_COLLECT_IDLE_GRACE);

    assert_eq!(
        plan.attempt_timeout(Duration::from_secs(30)),
        STATUS_ROUTE_ATTEMPT_TIMEOUT_CAP,
        "status-route attempts should stay capped to the canonical attempt timeout instead of each caller re-applying its own min(...) seam",
    );
    assert_eq!(
        plan.attempt_timeout(Duration::from_secs(3)),
        Duration::from_secs(3),
        "status-route planning should preserve smaller remaining budgets",
    );
    assert_eq!(
        caller_capped.route_timeout(),
        STATUS_ROUTE_ATTEMPT_TIMEOUT_CAP,
        "caller-capped routed status helpers should funnel through StatusRoutePlan instead of open-coding min(timeout, cap) at each call site",
    );
}

#[test]
fn status_route_machine_owns_attempt_budget_and_retry_followup() {
    let now = tokio::time::Instant::now();
    let machine = StatusRoutePlan::new(Duration::from_secs(30), STATUS_ROUTE_COLLECT_IDLE_GRACE)
        .machine_from_now(now);

    assert_eq!(
        machine.entry_phase_at(now),
        StatusRouteMachinePhase::Attempt(StatusRouteAttemptRuntime {
            attempt_timeout: STATUS_ROUTE_ATTEMPT_TIMEOUT_CAP,
            collect_idle_grace: STATUS_ROUTE_COLLECT_IDLE_GRACE,
        }),
        "routed status machine should own the canonical attempt budget instead of leaving helper loops to recompute remaining and attempt timeout inline",
    );
    assert_eq!(
        machine.entry_phase_at(now + Duration::from_secs(29) + Duration::from_millis(980)),
        StatusRouteMachinePhase::Attempt(StatusRouteAttemptRuntime {
            attempt_timeout: Duration::from_millis(20),
            collect_idle_grace: STATUS_ROUTE_COLLECT_IDLE_GRACE,
        }),
        "routed status machine should preserve the final remaining route budget for the last attempt instead of leaving helpers to open-code deadline math",
    );
    assert_eq!(
        machine.followup_phase_at(
            now,
            CnxError::Timeout,
            is_retryable_source_status_continuity_gap
        ),
        StatusRouteMachinePhase::WaitUntil(now + STATUS_ROUTE_RETRY_BACKOFF),
        "routed status machine should own the retry wait followup instead of leaving helper loops to hot-spin on retryable continuity gaps",
    );
    assert_eq!(
        machine.followup_phase_at(
            now + Duration::from_secs(29) + Duration::from_millis(995),
            CnxError::Timeout,
            is_retryable_source_status_continuity_gap,
        ),
        StatusRouteMachinePhase::WaitUntil(now + Duration::from_secs(30)),
        "routed status machine should cap the retry wait followup to the remaining route budget instead of letting helpers sleep past the deadline",
    );
    assert_eq!(
        machine.followup_phase_at(
            now + Duration::from_secs(30),
            CnxError::Timeout,
            is_retryable_source_status_continuity_gap,
        ),
        StatusRouteMachinePhase::Failed(RouteTerminalError::Timeout),
        "routed status machine should also own the retry followup timeout cut-off instead of leaving helper loops to branch on remaining.is_zero() after each retryable continuity gap",
    );
}

#[test]
fn materialized_status_load_plan_owns_canonical_route_timing() {
    let plan = MaterializedStatusLoadPlan::default();

    assert_eq!(
        plan.source_route,
        StatusRoutePlan::new(Duration::from_secs(30), STATUS_ROUTE_COLLECT_IDLE_GRACE),
        "materialized status-load planning should own the canonical routed source-status timing",
    );
    assert_eq!(
        plan.sink_route,
        StatusRoutePlan::new(
            Duration::from_secs(30),
            LOAD_MATERIALIZED_SINK_STATUS_ROUTE_IDLE_GRACE,
        ),
        "materialized status-load planning should own the canonical routed sink-status timing",
    );
    assert_eq!(
        plan.explicit_empty_sink_recollect,
        StatusRoutePlan::new(
            EXPLICIT_EMPTY_SINK_STATUS_RECOLLECT_BUDGET,
            LOAD_MATERIALIZED_SINK_STATUS_ROUTE_IDLE_GRACE,
        ),
        "explicit-empty sink-status recollect should stay inside the planner instead of helper-local timing constants",
    );
}

#[test]
fn status_route_machine_owns_non_retryable_followup_lane() {
    let now = tokio::time::Instant::now();
    let machine = StatusRoutePlan::new(Duration::from_secs(30), STATUS_ROUTE_COLLECT_IDLE_GRACE)
        .machine_from_now(now);

    assert_eq!(
        machine.followup_phase_at(
            now,
            CnxError::AccessDenied("non-retryable".into()),
            is_retryable_source_status_continuity_gap,
        ),
        StatusRouteMachinePhase::Failed(RouteTerminalError::AccessDenied("non-retryable".into(),)),
        "routed status machine should own the non-retryable followup lane instead of leaving the helper loop to branch directly on the raw error",
    );
}

#[test]
fn materialized_status_load_plan_request_source_refresh_caps_and_preserves_caller_budget() {
    let plan = MaterializedStatusLoadPlan::default();

    assert_eq!(
        plan.request_source_refresh_plan(Duration::from_secs(30)),
        StatusRoutePlan::new(
            STATUS_ROUTE_ATTEMPT_TIMEOUT_CAP,
            STATUS_ROUTE_COLLECT_IDLE_GRACE,
        ),
        "request-scoped source refresh should cap larger caller budgets inside the planner",
    );
    assert_eq!(
        plan.request_source_refresh_plan(Duration::from_millis(250)),
        StatusRoutePlan::new(Duration::from_millis(250), STATUS_ROUTE_COLLECT_IDLE_GRACE),
        "request-scoped source refresh should preserve smaller caller budgets inside the planner",
    );
}

#[test]
fn tree_pit_group_plan_empty_response_proxy_route_plan_caps_small_timeout() {
    let plan = TreePitSessionPlan::new(Duration::from_millis(80), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert_eq!(
        plan.empty_response_proxy_route_plan(Duration::from_millis(80)),
        TreePitProxyRoutePlan {
            route_timeout: Duration::from_millis(80),
            collect_idle_grace: Duration::from_millis(80),
            retry_backoff: Duration::from_millis(80),
        },
        "proxy route planning should cap collect-idle-grace and retry-backoff to the remaining proxy timeout instead of letting the helper own extra timing",
    );
}

#[test]
fn tree_pit_proxy_route_machine_owns_attempt_budget_and_retry_backoff() {
    let now = tokio::time::Instant::now();
    let machine = TreePitProxyRoutePlan::new(Duration::from_secs(3)).machine_from_now(now);

    assert_eq!(
        machine.entry_phase_at(now + Duration::from_millis(200)),
        ProxyRouteMachinePhase::Attempt(TreePitProxyAttemptRuntime {
            attempt_timeout: Duration::from_millis(2800),
            collect_idle_grace: SELECTED_GROUP_PROXY_ROUTE_COLLECT_IDLE_GRACE,
            retry_backoff: SELECTED_GROUP_PROXY_ROUTE_RETRY_BACKOFF,
        }),
        "generic-proxy route machine should own attempt timeout, collect-idle-grace, and retry-backoff from the remaining route budget instead of recomputing them inline inside the helper loop",
    );

    assert_eq!(
        machine.entry_phase_at(now + Duration::from_millis(2970)),
        ProxyRouteMachinePhase::Attempt(TreePitProxyAttemptRuntime {
            attempt_timeout: Duration::from_millis(30),
            collect_idle_grace: Duration::from_millis(30),
            retry_backoff: Duration::from_millis(30),
        }),
        "generic-proxy route machine should cap collect-idle-grace and retry-backoff to the final remaining route budget",
    );
}

#[test]
fn tree_pit_proxy_route_machine_owns_retry_wait_action() {
    let now = tokio::time::Instant::now();
    let machine = TreePitProxyRoutePlan::new(Duration::from_millis(30)).machine_from_now(now);

    assert_eq!(
        machine.followup_phase_at(now, CnxError::Timeout),
        ProxyRouteMachinePhase::WaitUntil(now + Duration::from_millis(30)),
        "generic-proxy machine should hand product code a canonical wait-until retry action instead of leaving the helper loop to sleep on a bare backoff duration",
    );
}

#[test]
fn tree_pit_proxy_route_machine_owns_non_retryable_followup_lane() {
    let now = tokio::time::Instant::now();
    let machine = TreePitProxyRoutePlan::new(Duration::from_millis(30)).machine_from_now(now);

    assert_eq!(
        machine.followup_phase_at(now, CnxError::ProtocolViolation("bad payload".into())),
        ProxyRouteMachinePhase::Failed(
            RouteTerminalError::ProtocolViolation("bad payload".into(),)
        ),
        "generic-proxy machine should own the non-retryable followup lane instead of leaving the helper loop to branch directly on the raw error",
    );
}

#[test]
fn tree_pit_group_plan_owner_attempt_timeout_reserves_proxy_budget_for_non_fail_closed_ready_group()
{
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert_eq!(
        plan.owner_attempt_timeout(Duration::from_millis(250)),
        Duration::from_millis(125),
        "selected-group owner attempts should keep proxy reserve inside the planner for non-fail-closed ready-root lanes instead of helper-local timeout math",
    );
}

#[test]
fn tree_pit_group_plan_owner_attempt_timeout_preserves_small_fail_closed_budget() {
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: true,
        },
    );

    assert_eq!(
        plan.owner_attempt_timeout(Duration::from_millis(250)),
        Duration::from_millis(250),
        "fail-closed ready-root lanes must keep the full remaining owner budget instead of reserving proxy time that cannot be spent",
    );
}

#[test]
fn tree_pit_group_plan_owner_route_plan_owns_owner_route_collect_idle_grace() {
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert_eq!(
        plan.owner_route_plan(Duration::from_millis(250)),
        SelectedGroupOwnerRoutePlan {
            route_timeout: Duration::from_millis(125),
            collect_idle_grace: SELECTED_GROUP_OWNER_ROUTE_COLLECT_IDLE_GRACE,
        },
        "selected-group owner route planning should own the owner attempt timeout and collect-idle-grace together instead of leaving the route helper to recalculate collect-idle-grace from a bare timeout",
    );

    assert_eq!(
        plan.owner_route_plan(Duration::from_millis(80)),
        SelectedGroupOwnerRoutePlan {
            route_timeout: Duration::from_millis(80),
            collect_idle_grace: Duration::from_millis(80),
        },
        "planner-owned owner route timing should preserve smaller remaining budgets when the capped owner attempt is below the canonical collect-idle-grace",
    );
}

#[test]
fn tree_pit_group_plan_owner_empty_tree_retry_timeout_caps_to_selected_group_budget() {
    let plan = TreePitSessionPlan::new(Duration::from_secs(5), 1).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: true,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: true,
            selected_group_sink_unready_empty: false,
            empty_root_requires_fail_closed: false,
        },
    );

    assert_eq!(
        plan.owner_empty_tree_retry_timeout(Duration::from_secs(5)),
        TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET,
        "owner empty-tree retries should use the planner-owned selected-group retry budget instead of helper-local caps",
    );
    assert_eq!(
        plan.owner_empty_tree_retry_timeout(Duration::from_millis(80)),
        Duration::from_millis(80),
        "planner-owned owner empty-tree retries should preserve smaller remaining budgets",
    );
}

#[test]
fn force_find_session_plan_caps_route_timing_to_caller_timeout() {
    assert_eq!(
        ForceFindSessionPlan::new(Duration::from_millis(80)).route_plan(),
        ForceFindRoutePlan {
            route_timeout: Duration::from_millis(80),
            collect_idle_grace: Duration::from_millis(80),
            retry_backoff: Duration::from_millis(80),
            min_inflight_hold: Duration::from_millis(80),
        },
        "force-find planner should cap collect-idle-grace, retry-backoff, and inflight-hold to the caller timeout instead of leaving helper-local timing seams",
    );
    assert_eq!(
        ForceFindSessionPlan::new(Duration::from_secs(10)).route_plan(),
        ForceFindRoutePlan {
            route_timeout: Duration::from_secs(10),
            collect_idle_grace: FORCE_FIND_ROUTE_COLLECT_IDLE_GRACE,
            retry_backoff: FORCE_FIND_ROUTE_RETRY_BACKOFF,
            min_inflight_hold: FORCE_FIND_MIN_INFLIGHT_HOLD,
        },
        "force-find planner should preserve the canonical route timing when the caller budget is large enough",
    );
}

#[test]
fn force_find_route_runtime_owns_remaining_budget_and_retry_lane() {
    let now = tokio::time::Instant::now();
    let machine = ForceFindSessionPlan::new(Duration::from_secs(3))
        .route_plan()
        .machine_from_now(now);

    assert_eq!(
        machine.attempt_at(now + Duration::from_millis(200)),
        Some(ForceFindRouteAttemptRuntime {
            route_plan: ForceFindRoutePlan {
                route_timeout: Duration::from_millis(2800),
                collect_idle_grace: Duration::from_millis(2800),
                retry_backoff: FORCE_FIND_ROUTE_RETRY_BACKOFF,
                min_inflight_hold: FORCE_FIND_MIN_INFLIGHT_HOLD,
            },
            remaining: Duration::from_millis(2800),
        }),
        "force-find route runtime should own remaining route budget instead of leaving the main loop to re-derive rebudgeted route timing inline",
    );

    let final_attempt = machine
        .attempt_at(now + Duration::from_millis(2970))
        .expect("final force-find attempt runtime");
    assert_eq!(
        final_attempt,
        ForceFindRouteAttemptRuntime {
            route_plan: ForceFindRoutePlan {
                route_timeout: Duration::from_millis(30),
                collect_idle_grace: Duration::from_millis(30),
                retry_backoff: Duration::from_millis(30),
                min_inflight_hold: Duration::from_millis(30),
            },
            remaining: Duration::from_millis(30),
        },
        "force-find route runtime should cap collect-idle-grace, retry-backoff, and inflight-hold to the final remaining route budget",
    );
    assert_eq!(
        final_attempt.tree_route_error_lane(&CnxError::Timeout, false),
        ForceFindTreeRouteErrorLane::RetryCurrentRoute,
        "force-find runtime should keep retry-lane ownership with the attempt runtime instead of helper-local remaining-budget math",
    );
}

#[test]
fn force_find_route_runtime_owns_tree_followup_action_and_backoff() {
    let now = tokio::time::Instant::now();
    let machine = ForceFindSessionPlan::new(Duration::from_secs(3))
        .route_plan()
        .machine_from_now(now);

    assert_eq!(
        machine.tree_followup_action_at(
            now,
            CnxError::PeerError("selected_group matched no group".into()),
            true,
        ),
        ForceFindTreeRouteAction::QueryGenericFallback {
            route_plan: ForceFindRoutePlan {
                route_timeout: Duration::from_secs(3),
                collect_idle_grace: Duration::from_secs(3),
                retry_backoff: FORCE_FIND_ROUTE_RETRY_BACKOFF,
                min_inflight_hold: FORCE_FIND_MIN_INFLIGHT_HOLD,
            },
            after_runner_gap: true,
        },
        "force-find runtime should hand product code a canonical fallback action instead of making the helper loop re-derive the runner-gap route pivot from remaining budget plus raw error",
    );

    let final_machine = ForceFindSessionPlan::new(Duration::from_millis(30))
        .route_plan()
        .machine_from_now(now);
    assert_eq!(
        final_machine.tree_followup_action_at(now, CnxError::Timeout, false),
        ForceFindTreeRouteAction::WaitUntil(now + Duration::from_millis(30)),
        "force-find runtime should own retry wait-until capping for the last route budget instead of leaving the helper loop to recompute sleep timing inline",
    );
}

#[test]
fn force_find_route_plan_owns_inflight_hold_wait_action() {
    let now = tokio::time::Instant::now();
    let plan = ForceFindSessionPlan::new(Duration::from_millis(30)).route_plan();

    assert_eq!(
        plan.inflight_hold_action_at(now, true),
        ForceFindInFlightHoldAction::WaitUntil(now + Duration::from_millis(30)),
        "force-find route planning should own the inflight hold wait action instead of leaving product code to sleep on a bare hold duration",
    );
    assert_eq!(
        plan.inflight_hold_action_at(now, false),
        ForceFindInFlightHoldAction::ReturnNow,
        "force-find route planning should skip inflight hold when no group lock is actually shared",
    );
}

#[test]
fn force_find_route_machine_replans_selected_runner_after_wait() {
    let now = tokio::time::Instant::now();
    let machine = ForceFindSessionPlan::new(Duration::from_secs(3))
        .route_plan()
        .machine_from_now(now);

    assert_eq!(
        machine.followup_phase_at(now, CnxError::Timeout, false),
        ForceFindRouteMachinePhase::WaitForForceFindReadiness(now + FORCE_FIND_ROUTE_RETRY_BACKOFF),
        "force-find route machine should own the fallback retry wait phase instead of leaving retry timing in the generic collect loop",
    );
    assert_eq!(
        machine.phase_after_wait(),
        ForceFindRouteMachinePhase::PlanSelectedRunner,
        "force-find route machine should return to planner-owned runner selection after a retry wait instead of resuming through a generic action loop",
    );
}

#[test]
fn force_find_route_machine_routes_selected_runner_gap_to_generic_fallback() {
    let now = tokio::time::Instant::now();
    let machine = ForceFindSessionPlan::new(Duration::from_secs(3))
        .route_plan()
        .machine_from_now(now);

    assert_eq!(
        machine.followup_phase_at(
            now,
            CnxError::PeerError("selected_group matched no group".into()),
            true,
        ),
        ForceFindRouteMachinePhase::FallbackToGeneric {
            route_plan: ForceFindRoutePlan {
                route_timeout: Duration::from_secs(3),
                collect_idle_grace: Duration::from_secs(3),
                retry_backoff: FORCE_FIND_ROUTE_RETRY_BACKOFF,
                min_inflight_hold: FORCE_FIND_MIN_INFLIGHT_HOLD,
            },
            after_runner_gap: true,
        },
        "force-find route machine should own the selected-runner-gap fallback phase instead of leaving reroute branching in the executor body",
    );
}

#[test]
fn force_find_route_machine_removes_external_selected_runner_planner_symbol() {
    let source = include_str!("api.rs");
    assert!(
        !source.contains(concat!("async fn ", "plan_force_find_group_route_phase")),
        "force-find route hard cut should not leave a standalone planner helper once selected-runner resolution is owned by the route machine",
    );
}

#[test]
fn query_pit_session_machine_removes_legacy_runtime_and_ranked_group_loop_symbols() {
    let source = include_str!("api.rs");
    assert!(
        !source.contains(concat!("struct ", "TreePitSessionRuntime")),
        "tree PIT session hard cut should not leave a standalone session runtime struct once timing is owned by the session machine",
    );
    assert!(
        !source.contains(concat!("struct ", "TreePitStageRuntime")),
        "tree PIT session hard cut should not leave a standalone stage runtime struct once stage timing is owned by the session machine",
    );
    assert!(
        !source.contains(concat!("struct ", "RetryableTreePitStageErrorRuntime")),
        "tree PIT session hard cut should not leave a standalone retryable stage-error runtime once retryable gaps are owned by the session machine",
    );
    assert!(
        !source.contains(concat!("async fn ", "execute_tree_pit_ranked_groups")),
        "tree PIT session hard cut should not leave a standalone ranked-group loop helper once ranked execution is owned by the session machine",
    );
    assert!(
        !source.contains(concat!("async fn ", "execute_force_find_ranked_groups")),
        "force-find PIT session hard cut should not leave a standalone ranked-group loop helper once ranked execution is owned by the session machine",
    );
    assert!(
        !source.contains(concat!("async fn ", "execute_selected_group_stats_rescue")),
        "stats query hard cut should not leave a standalone rescue helper once rescue ownership is held by the stats query machine",
    );
}

#[test]
fn force_find_route_runtime_owns_tree_entry_action() {
    let now = tokio::time::Instant::now();
    let machine = ForceFindSessionPlan::new(Duration::from_secs(3))
        .route_plan()
        .machine_from_now(now);

    assert_eq!(
        machine.tree_entry_action_at(now, Some(NodeId("node-a".into()))),
        ForceFindTreeRouteAction::QueryViaSelectedRunner {
            node_id: NodeId("node-a".into()),
            route_plan: ForceFindRoutePlan {
                route_timeout: Duration::from_secs(3),
                collect_idle_grace: Duration::from_secs(3),
                retry_backoff: FORCE_FIND_ROUTE_RETRY_BACKOFF,
                min_inflight_hold: FORCE_FIND_MIN_INFLIGHT_HOLD,
            },
        },
        "force-find runtime should own the selected-runner route entry plan instead of leaving the helper loop to re-derive route timeout from remaining budget inline",
    );
    assert_eq!(
        machine.tree_entry_action_at(now, None),
        ForceFindTreeRouteAction::QueryGenericFallback {
            route_plan: ForceFindRoutePlan {
                route_timeout: Duration::from_secs(3),
                collect_idle_grace: Duration::from_secs(3),
                retry_backoff: FORCE_FIND_ROUTE_RETRY_BACKOFF,
                min_inflight_hold: FORCE_FIND_MIN_INFLIGHT_HOLD,
            },
            after_runner_gap: false,
        },
        "force-find runtime should own the generic fallback entry plan instead of leaving the helper loop to branch on selected-runner presence and open-code the same route plan",
    );
}

#[test]
fn force_find_route_plan_tree_error_lane_routes_runner_gaps_through_canonical_fallback() {
    let plan = ForceFindSessionPlan::new(Duration::from_secs(5)).route_plan();

    assert_eq!(
        plan.tree_route_error_lane(
            &CnxError::PeerError("selected_group matched no group".into()),
            true,
            Duration::from_secs(2),
        ),
        ForceFindTreeRouteErrorLane::FallbackThenRetry,
        "force-find tree runner gaps should route through the planner-owned generic fallback lane instead of leaving the main loop to special-case runner-gap recovery",
    );
    assert_eq!(
        plan.tree_route_error_lane(&CnxError::Timeout, false, Duration::from_secs(2)),
        ForceFindTreeRouteErrorLane::RetryCurrentRoute,
        "force-find tree fallback retries should stay inside the planner-owned retry-current-route lane for retryable fallback errors",
    );
    assert_eq!(
        plan.tree_route_error_lane(
            &CnxError::AccessDenied("denied".into()),
            true,
            Duration::from_secs(2),
        ),
        ForceFindTreeRouteErrorLane::Fail,
        "non-retryable force-find tree route errors must fail directly instead of reopening fallback or retry lanes",
    );
}

#[test]
fn merge_request_scoped_materialized_sink_status_snapshot_prefers_later_ready_owner_on_equal_score()
{
    let loaded_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "node-a::nfs1".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 9,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 7,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };
    let request_scoped_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "node-b::nfs1".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 9,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 7,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let merged = merge_request_scoped_materialized_sink_status_snapshot(
        &loaded_sink_status,
        request_scoped_sink_status,
    );

    assert_eq!(
        merged.groups[0].primary_object_ref, "node-b::nfs1",
        "later request-scoped ready owner should replace equally-ready loaded owner instead of keeping stale primary_object_ref: {merged:?}"
    );
    assert_eq!(
        sink_primary_owner_node_for_group(Some(&merged), "nfs1"),
        Some(NodeId("node-b".to_string())),
        "selected-group owner resolution should follow the later request-scoped ready owner on equal score ties: {merged:?}"
    );
}

#[test]
fn merge_request_scoped_materialized_sink_status_snapshot_keeps_loaded_owner_when_later_ready_row_omits_group_from_schedule()
 {
    let loaded_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "node-a::nfs1".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 9,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 7,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };
    let request_scoped_sink_status = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "node-b::nfs1".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 9,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 7,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let merged = merge_request_scoped_materialized_sink_status_snapshot(
        &loaded_sink_status,
        request_scoped_sink_status,
    );

    assert_eq!(
        merged.groups[0].primary_object_ref, "node-a::nfs1",
        "later request-scoped ready rows must not replace the loaded owner on equal score ties when the request-scoped sink snapshot omits that group from schedule: {merged:?}"
    );
    assert_eq!(
        sink_primary_owner_node_for_group(Some(&merged), "nfs1"),
        Some(NodeId("node-a".to_string())),
        "selected-group owner resolution must keep the loaded owner when a later request-scoped sink snapshot omits the group from schedule: {merged:?}"
    );
}

#[test]
fn request_scoped_schedule_omitted_ready_groups_includes_loaded_ready_group_when_request_scope_regresses_to_explicit_empty()
 {
    let loaded_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-a::nfs4".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 9,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 7,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };
    let request_scoped_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-a::nfs4".to_string(),
            total_nodes: 0,
            live_nodes: 0,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::PendingMaterialization,
            materialized_revision: 7,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let omitted = request_scoped_schedule_omitted_ready_groups(
        &loaded_sink_status,
        &request_scoped_sink_status,
    );

    assert_eq!(
        omitted,
        BTreeSet::from(["nfs4".to_string()]),
        "request-scoped explicit-empty same-group drift should still mark a loaded ready root group as omitted for the trusted-materialized synthetic-empty settle lane"
    );
}

#[test]
fn materialized_owner_node_for_group_falls_back_to_source_primary_when_request_scoped_sink_status_is_fully_empty()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    std::fs::create_dir_all(&root_a).expect("create node-a dir");
    std::fs::create_dir_all(&root_b).expect("create node-b dir");

    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: BTreeMap::new(),
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
            host_labels: BTreeMap::new(),
            mount_point: root_b.clone(),
            fs_source: "server:/nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = Arc::new(SourceFacade::local(Arc::new(
        crate::source::FSMetaSource::with_boundaries(
            crate::source::config::SourceConfig {
                roots: vec![
                    crate::source::config::RootSpec::new("nfs1", &root_a),
                    crate::source::config::RootSpec::new("nfs2", &root_b),
                ],
                host_object_grants: grants,
                ..crate::source::config::SourceConfig::default()
            },
            NodeId("source-node".into()),
            None,
        )
        .expect("build source"),
    )));

    let owner = crate::runtime_app::shared_tokio_runtime()
        .block_on(materialized_owner_node_for_group(
            source.as_ref(),
            Some(&SinkStatusSnapshot::default()),
            "nfs1",
            MaterializedOwnerOmissionPolicy::TreatAsCollectionGap,
        ))
        .expect("resolve owner from empty request-scoped sink snapshot")
        .expect("empty request-scoped sink snapshot should fall back to source primary");

    assert_eq!(
        owner,
        NodeId("node-a".to_string()),
        "a fully empty later request-scoped sink-status snapshot must be treated as a collection gap and must not suppress source-primary owner routing for nfs1"
    );
}

#[test]
fn materialized_owner_node_for_group_falls_back_to_source_primary_when_request_scoped_sink_status_omits_group_but_keeps_other_groups()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    std::fs::create_dir_all(&root_a).expect("create node-a dir");
    std::fs::create_dir_all(&root_b).expect("create node-b dir");

    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: BTreeMap::new(),
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
            host_labels: BTreeMap::new(),
            mount_point: root_b.clone(),
            fs_source: "server:/nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = Arc::new(SourceFacade::local(Arc::new(
        crate::source::FSMetaSource::with_boundaries(
            crate::source::config::SourceConfig {
                roots: vec![
                    crate::source::config::RootSpec::new("nfs1", &root_a),
                    crate::source::config::RootSpec::new("nfs2", &root_b),
                ],
                host_object_grants: grants,
                ..crate::source::config::SourceConfig::default()
            },
            NodeId("source-node".into()),
            None,
        )
        .expect("build source"),
    )));
    let request_scoped_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs2".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs2".to_string(),
            primary_object_ref: "node-b::nfs2".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let owner = crate::runtime_app::shared_tokio_runtime()
        .block_on(materialized_owner_node_for_group(
            source.as_ref(),
            Some(&request_scoped_sink_status),
            "nfs1",
            MaterializedOwnerOmissionPolicy::TreatAsCollectionGap,
        ))
        .expect("resolve owner from omitted-group request-scoped sink snapshot")
        .expect(
            "request-scoped omission of nfs1 must still preserve source-primary owner fallback",
        );

    assert_eq!(
        owner,
        NodeId("node-a".to_string()),
        "a later request-scoped sink-status snapshot that only keeps other ready groups must not authoritatively suppress source-primary owner routing for omitted nfs1"
    );
}

fn group_counts_as_prior_materialized_tree_decode(group: &GroupPitSnapshot) -> bool {
    group.status == "ok"
        && group
            .root
            .as_ref()
            .is_some_and(|root| root.exists && (root.is_dir || root.has_children))
}

fn group_counts_as_prior_materialized_exact_file_decode(group: &GroupPitSnapshot) -> bool {
    group.status == "ok"
        && group
            .root
            .as_ref()
            .is_some_and(|root| root.exists && !root.is_dir && !root.has_children)
}

fn decode_materialized_selected_group_response_missing_same_path_payload(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::Internal(message)
            if message.contains("tree query returned no payload for requested group")
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectedGroupOwnerEmptyTreeRescuePlan {
    lane: EmptyTreeRescueLane,
    attempt_primary_owner_reroute: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectedGroupOwnerEmptyTreeRescueFacts {
    owner_response_is_empty: bool,
    requires_proxy_fallback: bool,
    requires_primary_owner_reroute: bool,
    selected_group_sink_reports_live_materialized: bool,
}

fn selected_group_events_require_proxy_fallback(
    policy: &ProjectionPolicy,
    params: &InternalQueryRequest,
    selected_group_sink_status: Option<&SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<&BTreeSet<String>>,
    group_id: &str,
    events: &[Event],
) -> bool {
    selected_group_empty_ready_tree_requires_proxy_fallback(
        policy,
        params,
        selected_group_sink_status,
        request_scoped_schedule_omitted_ready_groups,
        group_id,
        events,
    )
}

async fn maybe_reroute_selected_group_primary_owner(
    policy: &ProjectionPolicy,
    params: &InternalQueryRequest,
    deadline: tokio::time::Instant,
    boundary: Arc<dyn ChannelIoSubset>,
    selected_group_sink_status: Option<&SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<&BTreeSet<String>>,
    group_id: &str,
    owner_node_id: &NodeId,
    attempt_primary_owner_reroute: bool,
) -> Result<Option<Vec<Event>>, CnxError> {
    if !attempt_primary_owner_reroute {
        return Ok(None);
    }
    let Some(primary_node_id) =
        sink_primary_owner_node_for_group(selected_group_sink_status, group_id)
            .filter(|primary_node_id| primary_node_id != owner_node_id)
    else {
        return Ok(None);
    };
    let remaining = deadline
        .checked_duration_since(tokio::time::Instant::now())
        .unwrap_or_default();
    if remaining.is_zero() {
        return Ok(None);
    }
    let primary_events = route_materialized_events_via_node(
        boundary,
        primary_node_id,
        params.clone(),
        SelectedGroupOwnerRoutePlan::new(remaining),
    )
    .await?;
    if selected_group_events_require_proxy_fallback(
        policy,
        params,
        selected_group_sink_status,
        request_scoped_schedule_omitted_ready_groups,
        group_id,
        &primary_events,
    ) {
        return Ok(None);
    }
    Ok(Some(primary_events))
}

async fn proxy_selected_group_empty_tree_rescue(
    policy: &ProjectionPolicy,
    params: &InternalQueryRequest,
    deadline: tokio::time::Instant,
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    sink: &Arc<SinkFacade>,
    selected_group_sink_status: Option<&SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<&BTreeSet<String>>,
    group_id: &str,
    group_plan: TreePitGroupPlan,
    owner_attempt: &SelectedGroupOwnerAttempt,
    allow_empty_owner_retry: bool,
    return_none_when_proxy_budget_exhausted: bool,
) -> Result<Option<Vec<Event>>, CnxError> {
    let remaining = deadline
        .checked_duration_since(tokio::time::Instant::now())
        .unwrap_or_default();
    if remaining.is_zero() {
        return if return_none_when_proxy_budget_exhausted {
            Ok(None)
        } else {
            Err(trusted_materialized_empty_selected_group_tree_error(
                group_id,
            ))
        };
    }

    let proxy_route_plan = group_plan.empty_response_proxy_route_plan(remaining);
    let proxy_events = query_materialized_events_via_generic_proxy(
        boundary.clone(),
        origin_id.clone(),
        params.clone(),
        proxy_route_plan.machine(),
    )
    .await?;
    if !selected_group_events_require_proxy_fallback(
        policy,
        params,
        selected_group_sink_status,
        request_scoped_schedule_omitted_ready_groups,
        group_id,
        &proxy_events,
    ) {
        return Ok(Some(proxy_events));
    }
    if !allow_empty_owner_retry {
        return Ok(Some(proxy_events));
    }

    let remaining = deadline
        .checked_duration_since(tokio::time::Instant::now())
        .unwrap_or_default();
    if !remaining.is_zero() {
        let retry_owner_timeout = group_plan.owner_empty_tree_retry_timeout(remaining);
        if !retry_owner_timeout.is_zero() {
            let retry_events = query_materialized_events_via_selected_group_owner_attempt(
                owner_attempt,
                sink,
                boundary,
                params,
                retry_owner_timeout,
            )
            .await?;
            if !selected_group_events_require_proxy_fallback(
                policy,
                params,
                selected_group_sink_status,
                request_scoped_schedule_omitted_ready_groups,
                group_id,
                &retry_events,
            ) {
                return Ok(Some(retry_events));
            }
        }
    }

    Err(trusted_materialized_empty_selected_group_tree_error(
        group_id,
    ))
}

async fn execute_selected_group_owner_empty_tree_rescue(
    policy: &ProjectionPolicy,
    params: &InternalQueryRequest,
    deadline: tokio::time::Instant,
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    sink: &Arc<SinkFacade>,
    selected_group_sink_status: Option<&SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<&BTreeSet<String>>,
    group_id: &str,
    group_plan: TreePitGroupPlan,
    owner_attempt: &SelectedGroupOwnerAttempt,
    rescue_plan: SelectedGroupOwnerEmptyTreeRescuePlan,
    events: Vec<Event>,
    allow_empty_owner_retry: bool,
) -> Result<Vec<Event>, CnxError> {
    match rescue_plan.lane {
        EmptyTreeRescueLane::ReturnCurrent => Ok(events),
        EmptyTreeRescueLane::FailClosed => Err(
            trusted_materialized_empty_selected_group_tree_error(group_id),
        ),
        EmptyTreeRescueLane::OwnerRetryThenSettle
        | EmptyTreeRescueLane::OwnerRetryThenProxyFallback => {
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .unwrap_or_default();
            if remaining.is_zero() {
                return Ok(events);
            }
            let retry_owner_timeout = group_plan.owner_empty_tree_retry_timeout(remaining);
            if retry_owner_timeout.is_zero() {
                return Ok(events);
            }
            match query_materialized_events_via_selected_group_owner_attempt(
                owner_attempt,
                sink,
                boundary.clone(),
                params,
                retry_owner_timeout,
            )
            .await
            {
                Ok(retry_events) => {
                    if !selected_group_materialized_tree_payload_is_empty(
                        policy,
                        &retry_events,
                        group_id,
                        params.scope.path.as_slice(),
                    ) {
                        return Ok(retry_events);
                    }
                    if matches!(rescue_plan.lane, EmptyTreeRescueLane::OwnerRetryThenSettle) {
                        return Ok(retry_events);
                    }
                    match proxy_selected_group_empty_tree_rescue(
                        policy,
                        params,
                        deadline,
                        boundary,
                        origin_id,
                        sink,
                        selected_group_sink_status,
                        request_scoped_schedule_omitted_ready_groups,
                        group_id,
                        group_plan,
                        owner_attempt,
                        allow_empty_owner_retry,
                        true,
                    )
                    .await?
                    {
                        Some(proxy_or_retry_events) => Ok(proxy_or_retry_events),
                        None => Ok(events),
                    }
                }
                Err(CnxError::Timeout)
                | Err(CnxError::TransportClosed(_))
                | Err(CnxError::ProtocolViolation(_)) => {
                    if matches!(rescue_plan.lane, EmptyTreeRescueLane::OwnerRetryThenSettle) {
                        return Ok(events);
                    }
                    match proxy_selected_group_empty_tree_rescue(
                        policy,
                        params,
                        deadline,
                        boundary,
                        origin_id,
                        sink,
                        selected_group_sink_status,
                        request_scoped_schedule_omitted_ready_groups,
                        group_id,
                        group_plan,
                        owner_attempt,
                        allow_empty_owner_retry,
                        true,
                    )
                    .await?
                    {
                        Some(proxy_or_retry_events) => Ok(proxy_or_retry_events),
                        None => Ok(events),
                    }
                }
                Err(err) => Err(err),
            }
        }
        EmptyTreeRescueLane::ProxyFallback => Ok(proxy_selected_group_empty_tree_rescue(
            policy,
            params,
            deadline,
            boundary,
            origin_id,
            sink,
            selected_group_sink_status,
            request_scoped_schedule_omitted_ready_groups,
            group_id,
            group_plan,
            owner_attempt,
            allow_empty_owner_retry,
            false,
        )
        .await?
        .expect("direct proxy fallback either resolves or fails closed")),
    }
}

async fn query_materialized_events_via_generic_proxy(
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    params: InternalQueryRequest,
    machine: ProxyRouteMachine,
) -> Result<Vec<Event>, CnxError> {
    if debug_materialized_route_capture_enabled() {
        eprintln!(
            "fs_meta_query_api: materialized_proxy_capture begin selected_group={:?} recursive={} path={} timeout_ms={}",
            params.scope.selected_group,
            params.scope.recursive,
            String::from_utf8_lossy(&params.scope.path),
            machine.route_timeout().as_millis()
        );
    }
    let payload = rmp_serde::to_vec(&params)
        .map_err(|err| CnxError::Internal(format!("encode materialized query failed: {err}")))?;
    let result =
        execute_tree_pit_proxy_route_collect(boundary, origin_id, Bytes::from(payload), machine)
            .await;
    if debug_materialized_route_capture_enabled() {
        match &result {
            Ok(events) => {
                eprintln!(
                    "fs_meta_query_api: materialized_proxy_capture done selected_group={:?} events={} origins={:?}",
                    params.scope.selected_group,
                    events.len(),
                    summarize_event_counts_by_origin(events)
                );
            }
            Err(err) => {
                eprintln!(
                    "fs_meta_query_api: materialized_proxy_capture failed selected_group={:?} err={}",
                    params.scope.selected_group, err
                );
            }
        }
    }
    result
}

async fn execute_tree_pit_proxy_route_collect(
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    payload: Bytes,
    machine: ProxyRouteMachine,
) -> Result<Vec<Event>, CnxError> {
    let mut phase = machine.entry_phase();
    loop {
        match phase {
            ProxyRouteMachinePhase::Attempt(attempt_runtime) => {
                let adapter = exchange_host_adapter(
                    boundary.clone(),
                    origin_id.clone(),
                    default_route_bindings(),
                );
                match adapter
                    .call_collect(
                        ROUTE_TOKEN_FS_META_INTERNAL,
                        METHOD_SINK_QUERY_PROXY,
                        payload.clone(),
                        attempt_runtime.attempt_timeout,
                        attempt_runtime.collect_idle_grace,
                    )
                    .await
                {
                    Ok(events) => return Ok(events),
                    Err(err) => {
                        phase = machine.followup_phase_at(tokio::time::Instant::now(), err);
                    }
                }
            }
            ProxyRouteMachinePhase::WaitUntil(until) => {
                tokio::time::sleep_until(until).await;
                phase = machine.entry_phase();
            }
            ProxyRouteMachinePhase::Failed(err) => {
                return Err(err.into_error());
            }
        }
    }
}

fn is_retryable_materialized_proxy_continuity_gap(err: &CnxError) -> bool {
    matches!(err, CnxError::Timeout | CnxError::TransportClosed(_))
        || matches!(
            err,
            CnxError::Internal(message)
                | CnxError::PeerError(message)
                | CnxError::AccessDenied(message)
                    if message.contains("missing route state for channel_buffer")
                        || message.contains("bound route return dispatcher stopped")
                        || (message.contains("drained/fenced")
                            && message.contains("grant attachments"))
        )
}

#[cfg(test)]
async fn query_materialized_events_with_selected_group_owner_snapshot(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: InternalQueryRequest,
    timeout: Duration,
    selected_group_sink_status: Option<SinkStatusSnapshot>,
    reserve_proxy_budget: bool,
    allow_empty_owner_retry: bool,
) -> Result<Vec<Event>, CnxError> {
    let trusted_materialized_tree_query = matches!(params.op, QueryOp::Tree)
        && params
            .tree_options
            .as_ref()
            .is_some_and(|options| options.read_class == ReadClass::TrustedMaterialized);
    let group_plan = TreePitSessionPlan::new(timeout, 1).selected_group_owner_snapshot_plan(
        TreePitOwnerSnapshotPlanInput {
            trusted_materialized_tree_query,
            reserve_proxy_budget,
            allow_empty_owner_retry,
            empty_root_requires_fail_closed: trusted_materialized_tree_query
                && trusted_materialized_empty_group_root_requires_fail_closed(&params.scope.path),
        },
    );
    query_materialized_events_with_selected_group_owner_snapshot_and_request_scoped_omissions(
        state,
        policy,
        params,
        timeout,
        selected_group_sink_status,
        None,
        group_plan,
    )
    .await
}

#[derive(Clone, Debug)]
enum SelectedGroupOwnerAttempt {
    Local,
    Routed(NodeId),
}

impl SelectedGroupOwnerAttempt {
    fn owner_node_id(&self, origin_id: &NodeId) -> NodeId {
        match self {
            Self::Local => origin_id.clone(),
            Self::Routed(node_id) => node_id.clone(),
        }
    }
}

async fn query_materialized_events_via_selected_group_owner_attempt(
    owner_attempt: &SelectedGroupOwnerAttempt,
    sink: &Arc<SinkFacade>,
    boundary: Arc<dyn ChannelIoSubset>,
    params: &InternalQueryRequest,
    timeout: Duration,
) -> Result<Vec<Event>, CnxError> {
    match owner_attempt {
        SelectedGroupOwnerAttempt::Local => {
            run_timed_query(sink.materialized_query(params), timeout).await
        }
        SelectedGroupOwnerAttempt::Routed(node_id) => {
            route_materialized_events_via_node(
                boundary,
                node_id.clone(),
                params.clone(),
                SelectedGroupOwnerRoutePlan::new(timeout),
            )
            .await
        }
    }
}

async fn finalize_selected_group_owner_success(
    policy: &ProjectionPolicy,
    params: &InternalQueryRequest,
    deadline: tokio::time::Instant,
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    sink: &Arc<SinkFacade>,
    selected_group_sink_status: Option<&SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<&BTreeSet<String>>,
    group_id: &str,
    owner_attempt: &SelectedGroupOwnerAttempt,
    events: Vec<Event>,
    group_plan: TreePitGroupPlan,
) -> Result<Vec<Event>, CnxError> {
    if trusted_materialized_exact_file_lane_settles_empty_after_prior_decode(
        params,
        selected_group_sink_status,
        group_id,
        group_plan.prior_materialized_exact_file_decoded,
    ) {
        if selected_group_materialized_tree_payload_is_empty(
            policy,
            &events,
            group_id,
            params.scope.path.as_slice(),
        ) {
            return Ok(events);
        }
        if selected_group_materialized_tree_payload_omits_same_path(
            policy,
            &events,
            group_id,
            params.scope.path.as_slice(),
        ) {
            return Ok(synthesize_empty_selected_group_tree_events(
                &events,
                policy,
                group_id,
                params.scope.path.as_slice(),
            ));
        }
    }

    let requires_proxy_fallback = selected_group_empty_ready_tree_requires_proxy_fallback(
        policy,
        params,
        selected_group_sink_status,
        request_scoped_schedule_omitted_ready_groups,
        group_id,
        &events,
    );
    let owner_response_is_empty = selected_group_materialized_tree_payload_is_empty(
        policy,
        &events,
        group_id,
        params.scope.path.as_slice(),
    );
    let requires_primary_owner_reroute =
        selected_group_empty_ready_tree_requires_primary_owner_reroute(
            policy,
            params,
            selected_group_sink_status,
            request_scoped_schedule_omitted_ready_groups,
            group_id,
            &events,
        );
    let selected_group_sink_reports_live_materialized =
        selected_group_sink_status_reports_live_materialized_group(
            selected_group_sink_status,
            group_id,
        );
    let rescue_plan =
        group_plan.owner_empty_tree_rescue_plan(SelectedGroupOwnerEmptyTreeRescueFacts {
            owner_response_is_empty,
            requires_proxy_fallback,
            requires_primary_owner_reroute,
            selected_group_sink_reports_live_materialized,
        });
    let owner_node_id = owner_attempt.owner_node_id(&origin_id);

    if let Some(primary_events) = maybe_reroute_selected_group_primary_owner(
        policy,
        params,
        deadline,
        boundary.clone(),
        selected_group_sink_status,
        request_scoped_schedule_omitted_ready_groups,
        group_id,
        &owner_node_id,
        rescue_plan.attempt_primary_owner_reroute,
    )
    .await?
    {
        return Ok(primary_events);
    }

    execute_selected_group_owner_empty_tree_rescue(
        policy,
        params,
        deadline,
        boundary,
        origin_id,
        sink,
        selected_group_sink_status,
        request_scoped_schedule_omitted_ready_groups,
        group_id,
        group_plan,
        owner_attempt,
        rescue_plan,
        events,
        group_plan.stage_plan.allow_empty_owner_retry,
    )
    .await
}

async fn query_materialized_events_with_selected_group_owner_snapshot_and_request_scoped_omissions(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: InternalQueryRequest,
    timeout: Duration,
    selected_group_sink_status: Option<SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<&BTreeSet<String>>,
    group_plan: TreePitGroupPlan,
) -> Result<Vec<Event>, CnxError> {
    match &state.backend {
        QueryBackend::Route {
            boundary,
            origin_id,
            sink,
            source,
        } => {
            let deadline = tokio::time::Instant::now() + timeout;
            if let Some(group_id) = params.scope.selected_group.as_deref()
                && request_scoped_omitted_ready_root_group_should_settle_empty_tree(
                    &params,
                    selected_group_sink_status.as_ref(),
                    request_scoped_schedule_omitted_ready_groups,
                    group_id,
                )
            {
                return Ok(synthesize_empty_selected_group_tree_events(
                    &[],
                    policy,
                    group_id,
                    params.scope.path.as_slice(),
                ));
            }
            if let Some(group_id) = params.scope.selected_group.as_deref()
                && let Some(node_id) = materialized_owner_node_for_group(
                    source.as_ref(),
                    selected_group_sink_status.as_ref(),
                    group_id,
                    MaterializedOwnerOmissionPolicy::TreatAsCollectionGap,
                )
                .await?
            {
                if node_id == *origin_id {
                    let owner_attempt_timeout = group_plan.owner_attempt_timeout(
                        deadline
                            .checked_duration_since(tokio::time::Instant::now())
                            .unwrap_or_default(),
                    );
                    let events = query_materialized_events_via_selected_group_owner_attempt(
                        &SelectedGroupOwnerAttempt::Local,
                        sink,
                        boundary.clone(),
                        &params,
                        owner_attempt_timeout,
                    )
                    .await?;
                    return finalize_selected_group_owner_success(
                        policy,
                        &params,
                        deadline,
                        boundary.clone(),
                        origin_id.clone(),
                        sink,
                        selected_group_sink_status.as_ref(),
                        request_scoped_schedule_omitted_ready_groups,
                        group_id,
                        &SelectedGroupOwnerAttempt::Local,
                        events,
                        group_plan,
                    )
                    .await;
                }
                let owner_route_plan = group_plan.owner_route_plan(
                    deadline
                        .checked_duration_since(tokio::time::Instant::now())
                        .unwrap_or_default(),
                );
                match route_materialized_events_via_node(
                    boundary.clone(),
                    node_id.clone(),
                    params.clone(),
                    owner_route_plan,
                )
                .await
                {
                    Ok(events) => {
                        return finalize_selected_group_owner_success(
                            policy,
                            &params,
                            deadline,
                            boundary.clone(),
                            origin_id.clone(),
                            sink,
                            selected_group_sink_status.as_ref(),
                            request_scoped_schedule_omitted_ready_groups,
                            group_id,
                            &SelectedGroupOwnerAttempt::Routed(node_id.clone()),
                            events,
                            group_plan,
                        )
                        .await;
                    }
                    Err(CnxError::Timeout)
                    | Err(CnxError::TransportClosed(_))
                    | Err(CnxError::ProtocolViolation(_)) => {
                        let remaining = deadline
                            .checked_duration_since(tokio::time::Instant::now())
                            .unwrap_or_default();
                        if remaining.is_zero() {
                            return Err(CnxError::Timeout);
                        }
                        let trusted_materialized_tree_query = matches!(params.op, QueryOp::Tree)
                            && params.tree_options.as_ref().is_some_and(|options| {
                                options.read_class == ReadClass::TrustedMaterialized
                            });
                        let route_gap_proxy_policy = group_plan
                            .stage_plan
                            .owner_route_gap_proxy_policy(trusted_materialized_tree_query);
                        let proxy_timeout = group_plan.owner_route_gap_proxy_route_plan(
                            remaining,
                            trusted_materialized_tree_query,
                        );
                        if proxy_timeout.route_timeout().is_zero() {
                            return Err(CnxError::Timeout);
                        }
                        let proxy_events = match query_materialized_events_via_generic_proxy(
                            boundary.clone(),
                            origin_id.clone(),
                            params.clone(),
                            proxy_timeout.machine(),
                        )
                        .await
                        {
                            Ok(proxy_events) => proxy_events,
                            Err(CnxError::Timeout)
                                if route_gap_proxy_policy.fail_closed_after_proxy_gap() =>
                            {
                                return Err(CnxError::Timeout);
                            }
                            Err(CnxError::TransportClosed(_))
                            | Err(CnxError::ProtocolViolation(_))
                                if route_gap_proxy_policy.fail_closed_after_proxy_gap() =>
                            {
                                return Err(
                                    trusted_materialized_unavailable_selected_group_tree_error(
                                        group_id,
                                    ),
                                );
                            }
                            Err(err) => return Err(err),
                        };
                        if selected_group_empty_ready_tree_requires_primary_owner_reroute(
                            policy,
                            &params,
                            selected_group_sink_status.as_ref(),
                            request_scoped_schedule_omitted_ready_groups,
                            group_id,
                            &proxy_events,
                        ) && let Some(primary_node_id) = sink_primary_owner_node_for_group(
                            selected_group_sink_status.as_ref(),
                            group_id,
                        )
                        .filter(|primary_node_id| *primary_node_id != node_id)
                        {
                            let remaining = deadline
                                .checked_duration_since(tokio::time::Instant::now())
                                .unwrap_or_default();
                            if !remaining.is_zero() {
                                let primary_events = route_materialized_events_via_node(
                                    boundary.clone(),
                                    primary_node_id,
                                    params.clone(),
                                    SelectedGroupOwnerRoutePlan::new(remaining),
                                )
                                .await?;
                                if !selected_group_materialized_tree_payload_is_empty(
                                    policy,
                                    &primary_events,
                                    group_id,
                                    params.scope.path.as_slice(),
                                ) {
                                    return Ok(primary_events);
                                }
                            }
                        }
                        return Ok(proxy_events);
                    }
                    Err(err) => return Err(err),
                }
            }
            if params.scope.selected_group.is_some() {
                if let Some(group_id) = params.scope.selected_group.as_deref()
                    && selected_group_sink_status_omits_group(
                        selected_group_sink_status.as_ref(),
                        group_id,
                    )
                {
                    if request_scoped_omitted_ready_root_group_should_settle_empty_tree(
                        &params,
                        selected_group_sink_status.as_ref(),
                        request_scoped_schedule_omitted_ready_groups,
                        group_id,
                    ) {
                        return Ok(synthesize_empty_selected_group_tree_events(
                            &[],
                            policy,
                            group_id,
                            params.scope.path.as_slice(),
                        ));
                    }
                    return Ok(Vec::new());
                }
                let remaining = deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    return Err(CnxError::Timeout);
                }
                let proxy_route_plan = group_plan.direct_proxy_route_plan(remaining);
                return query_materialized_events_via_generic_proxy(
                    boundary.clone(),
                    origin_id.clone(),
                    params,
                    proxy_route_plan.machine(),
                )
                .await;
            }
            query_materialized_events(state.backend.clone(), params, timeout).await
        }
        QueryBackend::Local { .. } => {
            query_materialized_events(state.backend.clone(), params, timeout).await
        }
    }
}

async fn query_materialized_events_via_selected_group_owner_direct(
    state: &ApiState,
    params: InternalQueryRequest,
    timeout: Duration,
    selected_group_sink_status: Option<&SinkStatusSnapshot>,
) -> Result<Vec<Event>, CnxError> {
    match &state.backend {
        QueryBackend::Route {
            boundary,
            origin_id,
            sink,
            source,
        } => {
            if let Some(group_id) = params.scope.selected_group.as_deref()
                && let Some(node_id) = materialized_owner_node_for_group(
                    source.as_ref(),
                    selected_group_sink_status,
                    group_id,
                    MaterializedOwnerOmissionPolicy::Authoritative,
                )
                .await?
            {
                if node_id == *origin_id {
                    return run_timed_query(sink.materialized_query(&params), timeout).await;
                }
                return route_materialized_events_via_node(
                    boundary.clone(),
                    node_id,
                    params,
                    SelectedGroupOwnerRoutePlan::new(timeout),
                )
                .await;
            }
            query_materialized_events(state.backend.clone(), params, timeout).await
        }
        QueryBackend::Local { .. } => {
            query_materialized_events(state.backend.clone(), params, timeout).await
        }
    }
}

async fn query_force_find_events(
    backend: QueryBackend,
    params: InternalQueryRequest,
    route_plan: ForceFindRoutePlan,
) -> Result<Vec<Event>, CnxError> {
    eprintln!(
        "fs_meta_query_api: query_force_find_events start selected_group={:?} recursive={} path={}",
        params.scope.selected_group,
        params.scope.recursive,
        String::from_utf8_lossy(&params.scope.path)
    );
    match backend {
        QueryBackend::Local { source, .. } => {
            let result =
                run_timed_query(source.force_find(&params), route_plan.route_timeout()).await;
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
                    route_plan.route_timeout(),
                    route_plan.collect_idle_grace(),
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
    route_plan: ForceFindRoutePlan,
) -> Result<Vec<Event>, CnxError> {
    let adapter = exchange_host_adapter(boundary, node_id, default_route_bindings());
    let payload = rmp_serde::to_vec_named(&params)
        .map_err(|err| CnxError::Internal(format!("encode force-find query failed: {err}")))?;
    adapter
        .call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SOURCE_FIND,
            Bytes::from(payload),
            route_plan.route_timeout(),
            route_plan.collect_idle_grace(),
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
    deadline: Option<tokio::time::Instant>,
    request_sink_status: Option<&SinkStatusSnapshot>,
    group_order: GroupOrder,
    target_groups: &[String],
    explicit_group: Option<&str>,
) -> Result<(Vec<GroupRank>, bool), CnxError> {
    let mut ordered_groups = target_groups.to_vec();
    ordered_groups.sort();
    ordered_groups.dedup();
    if group_order == GroupOrder::GroupKey {
        return Ok((
            ordered_groups
                .into_iter()
                .map(|group_key| GroupRank {
                    group_key,
                    rank_metric: None,
                })
                .collect(),
            false,
        ));
    }

    let mut candidates = Vec::<GroupRank>::with_capacity(ordered_groups.len());
    let mut had_retryable_failure = false;
    for group_key in ordered_groups {
        let query_timeout = deadline
            .and_then(|deadline| deadline.checked_duration_since(tokio::time::Instant::now()))
            .unwrap_or_else(|| deadline.map(|_| Duration::ZERO).unwrap_or(timeout));
        let stats_events = match mode {
            QueryMode::Find => {
                if query_timeout.is_zero()
                    || deadline.is_some()
                        && had_retryable_failure
                        && query_timeout < RANKING_QUERY_MIN_BUDGET
                {
                    Vec::new()
                } else {
                    let request =
                        build_materialized_stats_request(path, recursive, Some(group_key.clone()));
                    let selected_group_sink_status = request_sink_status.map(|snapshot| {
                        filter_sink_status_snapshot(
                            snapshot.clone(),
                            &BTreeSet::from([group_key.clone()]),
                        )
                    });
                    match query_materialized_events_with_sink_status_snapshot(
                        state.backend.clone(),
                        request,
                        query_timeout,
                        selected_group_sink_status,
                    )
                    .await
                    {
                        Ok(events) => events,
                        Err(CnxError::Timeout)
                        | Err(CnxError::TransportClosed(_))
                        | Err(CnxError::ProtocolViolation(_)) => {
                            had_retryable_failure = true;
                            Vec::new()
                        }
                        Err(err) => return Err(err),
                    }
                }
            }
            QueryMode::ForceFind => {
                if query_timeout.is_zero() {
                    Vec::new()
                } else {
                    query_force_find_group_stats(
                        state,
                        path,
                        recursive,
                        ForceFindSessionPlan::new(query_timeout).route_plan(),
                        &group_key,
                        explicit_group == Some(group_key.as_str()),
                    )
                    .await?
                }
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
    Ok((candidates, had_retryable_failure))
}

fn materialized_scheduled_group_ids(snapshot: &SinkStatusSnapshot) -> BTreeSet<String> {
    let mut groups = snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<BTreeSet<_>>();
    groups.extend(snapshot.groups.iter().map(|group| group.group_id.clone()));
    groups
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MaterializedTargetGroupSelectionMode {
    Tree,
    Stats,
}

async fn materialized_target_groups(
    state: &ApiState,
    selected_group: Option<&str>,
    request_source_status: Option<&SourceStatusSnapshot>,
    request_sink_status: Option<&SinkStatusSnapshot>,
    timeout: Duration,
    preserve_unscheduled_groups: bool,
    selection_mode: MaterializedTargetGroupSelectionMode,
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
        let request_source_groups = preserve_unscheduled_groups.then(|| {
            request_source_status
                .map(|status| {
                    status
                        .logical_roots
                        .iter()
                        .map(|root| root.root_id.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        });
        if let Some(groups) = request_source_groups.filter(|groups| !groups.is_empty()) {
            groups
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
        }
    };
    groups.sort();
    groups.dedup();
    let allowed_groups = groups.iter().cloned().collect::<BTreeSet<_>>();
    let scheduled_groups = if let Some(snapshot) = request_sink_status {
        Some(materialized_scheduled_group_ids(
            &filter_sink_status_snapshot(snapshot.clone(), &allowed_groups),
        ))
    } else {
        match (&state.backend, sink.as_ref()) {
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
                StatusRoutePlan::caller_capped(timeout, STATUS_ROUTE_COLLECT_IDLE_GRACE),
            )
            .await
            .ok()
            .map(|snapshot| {
                let mut cache = state.materialized_sink_status_cache.lock().map_err(|_| {
                    CnxError::Internal("materialized sink status cache lock poisoned".into())
                })?;
                let merged = merge_with_cached_sink_status_snapshot(
                    cache.as_ref(),
                    &allowed_groups,
                    snapshot,
                );
                *cache = Some(CachedSinkStatusSnapshot {
                    snapshot: merged.clone(),
                });
                Ok::<_, CnxError>(materialized_scheduled_group_ids(&merged))
            })
            .transpose()?,
            (QueryBackend::Local { .. }, Some(sink)) => sink
                .status_snapshot()
                .await
                .ok()
                .map(|snapshot| {
                    let mut cache = state.materialized_sink_status_cache.lock().map_err(|_| {
                        CnxError::Internal("materialized sink status cache lock poisoned".into())
                    })?;
                    let merged = merge_with_cached_sink_status_snapshot(
                        cache.as_ref(),
                        &allowed_groups,
                        snapshot,
                    );
                    *cache = Some(CachedSinkStatusSnapshot {
                        snapshot: merged.clone(),
                    });
                    Ok::<_, CnxError>(materialized_scheduled_group_ids(&merged))
                })
                .transpose()?,
            (QueryBackend::Local { .. }, None) => None,
        }
    };
    let preserve_groups_when_first_routed_sink_status_is_empty =
        matches!(&state.backend, QueryBackend::Route { .. })
            && scheduled_groups
                .as_ref()
                .is_some_and(|groups| groups.is_empty());
    let preserve_unscheduled_groups_for_mode = match selection_mode {
        MaterializedTargetGroupSelectionMode::Tree => {
            preserve_unscheduled_groups
                && scheduled_groups
                    .as_ref()
                    .is_some_and(|groups| groups.is_empty())
        }
        MaterializedTargetGroupSelectionMode::Stats => preserve_unscheduled_groups,
    };
    if let Some(scheduled_groups) = scheduled_groups
        && !preserve_unscheduled_groups_for_mode
        && !preserve_groups_when_first_routed_sink_status_is_empty
    {
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
    let mut scheduled_nodes = sink_status
        .scheduled_groups_by_node
        .iter()
        .filter(|(_, groups)| groups.iter().any(|group| group == group_id))
        .map(|(node_id, _)| NodeId(node_id.clone()))
        .collect::<Vec<_>>();
    scheduled_nodes.sort_by(|a, b| a.0.cmp(&b.0));
    scheduled_nodes.dedup_by(|a, b| a.0 == b.0);

    if let Some(primary_node) = sink_status
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

fn sink_status_mentions_group(sink_status: &SinkStatusSnapshot, group_id: &str) -> bool {
    sink_status
        .groups
        .iter()
        .any(|group| group.group_id == group_id)
        || sink_status
            .scheduled_groups_by_node
            .values()
            .any(|groups| groups.iter().any(|group| group == group_id))
}

fn sink_status_authoritatively_omits_group(
    sink_status: &SinkStatusSnapshot,
    group_id: &str,
) -> bool {
    !sink_status_mentions_group(sink_status, group_id)
        && !sink_status_snapshot_omits_all_groups_from_schedule(sink_status)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MaterializedOwnerOmissionPolicy {
    Authoritative,
    TreatAsCollectionGap,
}

async fn materialized_owner_node_for_group(
    source: &SourceFacade,
    sink_status: Option<&SinkStatusSnapshot>,
    group_id: &str,
    omission_policy: MaterializedOwnerOmissionPolicy,
) -> Result<Option<NodeId>, CnxError> {
    if let Some(sink_status) = sink_status {
        if let Some(node_id) = scheduled_sink_owner_node_for_group(sink_status, group_id) {
            return Ok(Some(node_id));
        }
        if omission_policy == MaterializedOwnerOmissionPolicy::Authoritative
            && sink_status_authoritatively_omits_group(sink_status, group_id)
        {
            return Ok(None);
        }
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
        return materialized_owner_node_for_group(
            source,
            None,
            group_id,
            MaterializedOwnerOmissionPolicy::Authoritative,
        )
        .await;
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
    route_plan: ForceFindRoutePlan,
    group_id: &str,
    strict_conflict: bool,
) -> Result<Vec<Event>, CnxError> {
    let _guard = match acquire_force_find_group(state, group_id) {
        Ok(guard) => guard,
        Err(_err) if !strict_conflict => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    let request = build_force_find_stats_request(path, recursive, Some(group_id.to_string()));
    let events =
        execute_force_find_group_stats_collect(state, request, route_plan, group_id, recursive)
            .await?;
    apply_force_find_inflight_hold(route_plan, !_guard.groups.is_empty()).await;
    Ok(events)
}

async fn execute_force_find_group_stats_collect(
    state: &ApiState,
    request: InternalQueryRequest,
    route_plan: ForceFindRoutePlan,
    group_id: &str,
    recursive: bool,
) -> Result<Vec<Event>, CnxError> {
    match &state.backend {
        QueryBackend::Route {
            boundary, source, ..
        } => {
            execute_force_find_group_stats_route_collect(
                state,
                boundary.clone(),
                source.as_ref(),
                request,
                route_plan,
                group_id,
                recursive,
            )
            .await
        }
        _ => {
            let result = query_force_find_events(state.backend.clone(), request, route_plan).await;
            if debug_force_find_route_capture_enabled() {
                match &result {
                    Ok(events) => {
                        eprintln!(
                            "fs_meta_query_api: force-find stats local done group={} events={} origins={:?}",
                            group_id,
                            events.len(),
                            summarize_event_counts_by_origin(events)
                        );
                    }
                    Err(err) => {
                        eprintln!(
                            "fs_meta_query_api: force-find stats local failed group={} err={}",
                            group_id, err
                        );
                    }
                }
            }
            result
        }
    }
}

async fn execute_force_find_group_stats_route_collect(
    state: &ApiState,
    boundary: Arc<dyn ChannelIoSubset>,
    source: &SourceFacade,
    request: InternalQueryRequest,
    route_plan: ForceFindRoutePlan,
    group_id: &str,
    recursive: bool,
) -> Result<Vec<Event>, CnxError> {
    execute_force_find_group_route_collect(
        state,
        boundary,
        source,
        request,
        route_plan.machine(),
        group_id,
        ForceFindRouteCollectKind::Stats { recursive },
    )
    .await
}

fn is_retryable_force_find_route_error(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::Timeout | CnxError::TransportClosed(_) | CnxError::ProtocolViolation(_)
    )
}

fn is_retryable_force_find_runner_gap(err: &CnxError) -> bool {
    matches!(err, CnxError::PeerError(message) if message.contains("selected_group matched no group"))
}

async fn query_force_find_group_tree_on_route(
    state: &ApiState,
    boundary: Arc<dyn ChannelIoSubset>,
    source: &SourceFacade,
    request: InternalQueryRequest,
    route_plan: ForceFindRoutePlan,
    group_id: &str,
    recursive: bool,
    max_depth: Option<usize>,
    strict_conflict: bool,
) -> Result<Vec<Event>, CnxError> {
    execute_force_find_group_route_collect(
        state,
        boundary,
        source,
        request,
        route_plan.machine(),
        group_id,
        ForceFindRouteCollectKind::Tree {
            recursive,
            max_depth,
            strict_conflict,
        },
    )
    .await
}

#[derive(Clone, Copy, Debug)]
enum ForceFindRouteCollectKind {
    Tree {
        recursive: bool,
        max_depth: Option<usize>,
        strict_conflict: bool,
    },
    Stats {
        recursive: bool,
    },
}

impl ForceFindRouteCollectKind {
    fn label(self) -> &'static str {
        match self {
            ForceFindRouteCollectKind::Tree { .. } => "tree",
            ForceFindRouteCollectKind::Stats { .. } => "stats",
        }
    }

    fn recursive(self) -> bool {
        match self {
            ForceFindRouteCollectKind::Tree { recursive, .. }
            | ForceFindRouteCollectKind::Stats { recursive } => recursive,
        }
    }

    fn max_depth(self) -> Option<usize> {
        match self {
            ForceFindRouteCollectKind::Tree { max_depth, .. } => max_depth,
            ForceFindRouteCollectKind::Stats { .. } => None,
        }
    }

    fn strict_conflict(self) -> bool {
        match self {
            ForceFindRouteCollectKind::Tree {
                strict_conflict, ..
            } => strict_conflict,
            ForceFindRouteCollectKind::Stats { .. } => false,
        }
    }
}

async fn execute_force_find_group_route_collect(
    state: &ApiState,
    boundary: Arc<dyn ChannelIoSubset>,
    source: &SourceFacade,
    request: InternalQueryRequest,
    machine: ForceFindRouteMachine,
    group_id: &str,
    kind: ForceFindRouteCollectKind,
) -> Result<Vec<Event>, CnxError> {
    let mut phase = machine.entry_phase();
    loop {
        match phase {
            ForceFindRouteMachinePhase::PlanSelectedRunner => {
                phase = machine
                    .phase_for_selected_runner(state, source, group_id)
                    .await?;
            }
            ForceFindRouteMachinePhase::AttemptSelectedRunner {
                node_id,
                route_plan,
            } => {
                let remaining = route_plan.route_timeout();
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find {} via_node begin group={} node={} recursive={} max_depth={:?} strict_conflict={} path={} timeout_ms={}",
                        kind.label(),
                        group_id,
                        node_id.0,
                        kind.recursive(),
                        kind.max_depth(),
                        kind.strict_conflict(),
                        String::from_utf8_lossy(&request.scope.path),
                        remaining.as_millis(),
                    );
                }
                match run_timed_query(
                    route_force_find_events_via_node(
                        boundary.clone(),
                        node_id.clone(),
                        request.clone(),
                        route_plan,
                    ),
                    route_plan.route_timeout(),
                )
                .await
                {
                    Ok(events) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: force-find {} via_node done group={} node={} events={} origins={:?}",
                                kind.label(),
                                group_id,
                                node_id.0,
                                events.len(),
                                summarize_event_counts_by_origin(&events)
                            );
                        }
                        return Ok(events);
                    }
                    Err(err) => {
                        let runner_gap = is_retryable_force_find_runner_gap(&err);
                        let err_text = err.to_string();
                        let next_phase =
                            machine.followup_phase_at(tokio::time::Instant::now(), err, true);
                        if debug_force_find_route_capture_enabled() {
                            match &next_phase {
                                ForceFindRouteMachinePhase::FallbackToGeneric {
                                    route_plan: fallback_route_plan,
                                    after_runner_gap,
                                } => {
                                    let route_action = if runner_gap { "reroute" } else { "retry" };
                                    eprintln!(
                                        "fs_meta_query_api: force-find {} via_node {} group={} node={} err={}",
                                        kind.label(),
                                        route_action,
                                        group_id,
                                        node_id.0,
                                        err_text
                                    );
                                    eprintln!(
                                        "fs_meta_query_api: force-find {} route_fallback begin group={} recursive={} max_depth={:?} strict_conflict={} path={} timeout_ms={} after_runner_gap={}",
                                        kind.label(),
                                        group_id,
                                        kind.recursive(),
                                        kind.max_depth(),
                                        kind.strict_conflict(),
                                        String::from_utf8_lossy(&request.scope.path),
                                        fallback_route_plan.route_timeout().as_millis(),
                                        after_runner_gap,
                                    );
                                }
                                ForceFindRouteMachinePhase::Failed(_)
                                | ForceFindRouteMachinePhase::WaitForForceFindReadiness(_) => {
                                    eprintln!(
                                        "fs_meta_query_api: force-find {} via_node failed group={} node={} err={}",
                                        kind.label(),
                                        group_id,
                                        node_id.0,
                                        err_text
                                    );
                                }
                                ForceFindRouteMachinePhase::PlanSelectedRunner
                                | ForceFindRouteMachinePhase::AttemptSelectedRunner { .. } => {}
                            }
                        }
                        phase = next_phase;
                    }
                }
            }
            ForceFindRouteMachinePhase::FallbackToGeneric {
                route_plan,
                after_runner_gap,
            } => {
                let remaining = route_plan.route_timeout();
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find {} route_fallback begin group={} recursive={} max_depth={:?} strict_conflict={} path={} timeout_ms={} after_runner_gap={}",
                        kind.label(),
                        group_id,
                        kind.recursive(),
                        kind.max_depth(),
                        kind.strict_conflict(),
                        String::from_utf8_lossy(&request.scope.path),
                        remaining.as_millis(),
                        after_runner_gap,
                    );
                }
                match query_force_find_events(state.backend.clone(), request.clone(), route_plan)
                    .await
                {
                    Ok(events) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: force-find {} route_fallback done group={} events={} origins={:?} after_runner_gap={}",
                                kind.label(),
                                group_id,
                                events.len(),
                                summarize_event_counts_by_origin(&events),
                                after_runner_gap,
                            );
                        }
                        return Ok(events);
                    }
                    Err(err) => {
                        let runner_gap = is_retryable_force_find_runner_gap(&err);
                        let err_text = err.to_string();
                        let next_phase =
                            machine.followup_phase_at(tokio::time::Instant::now(), err, false);
                        if debug_force_find_route_capture_enabled() {
                            let route_action = if runner_gap { "reroute" } else { "retry" };
                            match &next_phase {
                                ForceFindRouteMachinePhase::WaitForForceFindReadiness(_) => {
                                    eprintln!(
                                        "fs_meta_query_api: force-find {} route_fallback {} group={} err={} after_runner_gap={}",
                                        kind.label(),
                                        route_action,
                                        group_id,
                                        err_text,
                                        after_runner_gap
                                    );
                                }
                                ForceFindRouteMachinePhase::Failed(_) => {
                                    eprintln!(
                                        "fs_meta_query_api: force-find {} route_fallback failed group={} err={} after_runner_gap={}",
                                        kind.label(),
                                        group_id,
                                        err_text,
                                        after_runner_gap
                                    );
                                }
                                ForceFindRouteMachinePhase::PlanSelectedRunner
                                | ForceFindRouteMachinePhase::AttemptSelectedRunner { .. }
                                | ForceFindRouteMachinePhase::FallbackToGeneric { .. } => {}
                            }
                        }
                        phase = next_phase;
                    }
                }
            }
            ForceFindRouteMachinePhase::WaitForForceFindReadiness(until) => {
                tokio::time::sleep_until(until).await;
                phase = machine.phase_after_wait();
            }
            ForceFindRouteMachinePhase::Failed(err) => {
                return Err(err.into_error());
            }
        }
    }
}

async fn query_force_find_group_tree(
    state: &ApiState,
    path: &[u8],
    recursive: bool,
    max_depth: Option<usize>,
    route_plan: ForceFindRoutePlan,
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
            query_force_find_group_tree_on_route(
                state,
                boundary.clone(),
                source.as_ref(),
                request.clone(),
                route_plan,
                group_id,
                recursive,
                max_depth,
                strict_conflict,
            )
            .await?
        }
        _ => {
            let result = query_force_find_events(state.backend.clone(), request, route_plan).await;
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
    apply_force_find_inflight_hold(route_plan, !_guard.groups.is_empty()).await;
    Ok(events)
}

async fn apply_force_find_inflight_hold(route_plan: ForceFindRoutePlan, inflight: bool) {
    match route_plan.inflight_hold_action(inflight) {
        ForceFindInFlightHoldAction::ReturnNow => {}
        ForceFindInFlightHoldAction::WaitUntil(until) => {
            tokio::time::sleep_until(until).await;
        }
    }
}

fn build_force_find_group_error_snapshot(group_key: String, err: CnxError) -> GroupPitSnapshot {
    GroupPitSnapshot {
        group: group_key,
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
    }
}

struct ForceFindRankedGroupExecutionInput<'a> {
    state: &'a ApiState,
    policy: &'a ProjectionPolicy,
    params: &'a NormalizedApiParams,
    route_plan: ForceFindRoutePlan,
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

    fn payload_precedence(
        event: &Event,
        payload: &TreeGroupPayload,
    ) -> (u64, u8, usize, bool, u64) {
        let score = payload_score(payload);
        (
            event.metadata().timestamp_us,
            score.0,
            score.1,
            score.2,
            score.3,
        )
    }

    let mut last_decode_error = None::<String>;
    let mut best = None::<TreeGroupPayload>;
    let mut best_precedence = None::<(u64, u8, usize, bool, u64)>;
    for event in events {
        if event_group_key(policy, event) != selected_group {
            continue;
        }
        match rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()) {
            Ok(MaterializedQueryPayload::Tree(payload)) => {
                if payload.root.path != query_path {
                    continue;
                }
                eprintln!(
                    "fs_meta_query_api: decode group={} root_exists={} entries={} has_children={} modified_time_us={}",
                    selected_group,
                    payload.root.exists,
                    payload.entries.len(),
                    payload.root.has_children,
                    payload.root.modified_time_us
                );
                let precedence = payload_precedence(event, &payload);
                let replace = best_precedence
                    .as_ref()
                    .is_none_or(|current| precedence > *current);
                if replace {
                    best_precedence = Some(precedence);
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
    Err(CnxError::Internal(last_decode_error.unwrap_or_else(|| {
        format!("tree query returned no payload for requested group '{selected_group}'")
    })))
}

fn decode_richer_same_path_materialized_selected_group_response(
    events: &[Event],
    policy: &ProjectionPolicy,
    selected_group: &str,
    query_path: &[u8],
) -> Option<TreeGroupPayload> {
    fn payload_score(payload: &TreeGroupPayload) -> (u8, usize, bool, u64) {
        (
            u8::from(payload.root.exists),
            payload.entries.len(),
            payload.root.has_children,
            payload.root.modified_time_us,
        )
    }

    let mut best = None::<TreeGroupPayload>;
    let mut best_precedence = None::<(u64, u8, usize, bool, u64)>;
    for event in events {
        if event_group_key(policy, event) != selected_group {
            continue;
        }
        let Ok(MaterializedQueryPayload::Tree(payload)) =
            rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
        else {
            continue;
        };
        if payload.root.path != query_path || trusted_materialized_tree_payload_is_empty(&payload) {
            continue;
        }
        let score = payload_score(&payload);
        let precedence = (
            event.metadata().timestamp_us,
            score.0,
            score.1,
            score.2,
            score.3,
        );
        let replace = best_precedence
            .as_ref()
            .is_none_or(|current| precedence > *current);
        if replace {
            best_precedence = Some(precedence);
            best = Some(payload);
        }
    }
    best
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

fn trusted_materialized_tree_payload_is_empty(payload: &TreeGroupPayload) -> bool {
    !payload.root.exists && payload.entries.is_empty() && !payload.root.has_children
}

fn maybe_promote_richer_same_path_materialized_tree_response(
    events: &[Event],
    policy: &ProjectionPolicy,
    selected_group: &str,
    query_path: &[u8],
    response: &mut TreeGroupPayload,
) {
    if trusted_materialized_tree_payload_is_empty(response)
        && let Some(richer_response) = decode_richer_same_path_materialized_selected_group_response(
            events,
            policy,
            selected_group,
            query_path,
        )
    {
        *response = richer_response;
    }
}

fn record_tree_group_snapshot_observed_rank(
    group_order: GroupOrder,
    group_rank_metrics: &mut BTreeMap<String, Option<u64>>,
    snapshot: &GroupPitSnapshot,
) {
    if let Some(observed_rank) = observed_rank_metric_from_tree_snapshot(group_order, snapshot) {
        let entry = group_rank_metrics
            .entry(snapshot.group.clone())
            .or_insert(None);
        *entry = Some(entry.unwrap_or(0).max(observed_rank));
    }
}

fn push_materialized_tree_group_payload(
    groups: &mut Vec<GroupPitSnapshot>,
    group_rank_metrics: &mut BTreeMap<String, Option<u64>>,
    group_order: GroupOrder,
    read_class: ReadClass,
    group_key: String,
    response: TreeGroupPayload,
) {
    let (metadata_available, _meta_json) = tree_metadata_json(read_class);
    groups.push(GroupPitSnapshot {
        group: group_key,
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
    if let Some(last) = groups.last() {
        record_tree_group_snapshot_observed_rank(group_order, group_rank_metrics, last);
    }
}

fn push_empty_materialized_tree_group_snapshot(
    groups: &mut Vec<GroupPitSnapshot>,
    group_rank_metrics: &mut BTreeMap<String, Option<u64>>,
    group_order: GroupOrder,
    read_class: ReadClass,
    group_key: String,
    query_path: &[u8],
) {
    push_materialized_tree_group_payload(
        groups,
        group_rank_metrics,
        group_order,
        read_class,
        group_key,
        empty_materialized_tree_group_payload(query_path),
    );
}

fn push_materialized_tree_group_error_snapshot(
    groups: &mut Vec<GroupPitSnapshot>,
    read_class: ReadClass,
    group_key: String,
    err: CnxError,
) {
    groups.push(GroupPitSnapshot {
        group: group_key,
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
    });
}

fn trusted_materialized_empty_group_root_requires_fail_closed(path: &[u8]) -> bool {
    path.is_empty() || path == b"/"
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

#[cfg(test)]
async fn build_tree_pit_session(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    timeout: Duration,
    observation_status: ObservationStatus,
    request_sink_status: Option<&SinkStatusSnapshot>,
) -> Result<PitSession, CnxError> {
    build_tree_pit_session_with_request_scoped_schedule_omitted_ready_groups(
        state,
        policy,
        params,
        timeout,
        observation_status,
        request_sink_status,
        None,
    )
    .await
}

async fn build_tree_pit_session_with_request_scoped_schedule_omitted_ready_groups(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    timeout: Duration,
    observation_status: ObservationStatus,
    request_sink_status: Option<&SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<&BTreeSet<String>>,
) -> Result<PitSession, CnxError> {
    prepare_tree_pit_session_machine(
        state,
        policy,
        params,
        timeout,
        observation_status,
        request_sink_status,
        request_scoped_schedule_omitted_ready_groups,
    )
    .await?
    .run()
    .await
}

struct TreePitSessionMachine<'a> {
    input: TreePitRankedGroupExecutionInput<'a>,
    rankings: Vec<GroupRank>,
    observation_status: ObservationStatus,
}

struct TreePitGroupMachine<'a, 'b> {
    input: &'b TreePitRankedGroupExecutionInput<'a>,
    rank_index: usize,
    group_key: String,
    state: &'b mut TreePitRankedGroupsState,
}

impl TreePitGroupMachine<'_, '_> {
    async fn run(self) -> Result<(), CnxError> {
        let Self {
            input,
            rank_index,
            group_key,
            state,
        } = self;
        let tree_params = build_materialized_tree_request(
            &input.params.path,
            input.params.recursive,
            input.params.max_depth,
            input.read_class,
            Some(group_key.clone()),
        );
        let selected_group_sink_status = if let Some(snapshot) = input.request_sink_status {
            Some(filter_sink_status_snapshot(
                snapshot.clone(),
                &BTreeSet::from([group_key.clone()]),
            ))
        } else {
            let allowed_groups = BTreeSet::from([group_key.clone()]);
            let cache = input
                .state
                .materialized_sink_status_cache
                .lock()
                .map_err(|_| {
                    CnxError::Internal("materialized sink status cache lock poisoned".into())
                })?;
            cache
                .as_ref()
                .map(|cached| filter_sink_status_snapshot(cached.snapshot.clone(), &allowed_groups))
        };
        let prior_materialized_group_decoded = state
            .groups
            .iter()
            .any(group_counts_as_prior_materialized_tree_decode);
        let prior_materialized_exact_file_decoded = state
            .groups
            .iter()
            .any(group_counts_as_prior_materialized_exact_file_decode);
        let is_last_ranked_group = rank_index + 1 == input.total_ranked_groups;
        let group_plan = input
            .session_plan
            .selected_group_stage_plan(TreePitGroupPlanInput {
                read_class: input.read_class,
                observation_state: input.observation_state,
                selected_group_sink_reports_live_materialized:
                    selected_group_sink_status_reports_live_materialized_group(
                        selected_group_sink_status.as_ref(),
                        &group_key,
                    ),
                prior_materialized_group_decoded,
                prior_materialized_exact_file_decoded,
                rank_index,
                is_last_ranked_group,
                selected_group_sink_unready_empty: selected_group_sink_status_is_unready_empty(
                    selected_group_sink_status.as_ref(),
                    &group_key,
                ),
                empty_root_requires_fail_closed:
                    trusted_materialized_empty_group_root_requires_fail_closed(&input.params.path),
            });
        let selected_group_owner_known =
            if group_plan.stage_plan.should_resolve_selected_group_owner {
                match &input.state.backend {
                    QueryBackend::Route { source, .. } => materialized_owner_node_for_group(
                        source.as_ref(),
                        selected_group_sink_status.as_ref(),
                        &group_key,
                        MaterializedOwnerOmissionPolicy::Authoritative,
                    )
                    .await?
                    .is_some(),
                    QueryBackend::Local { .. } => true,
                }
            } else {
                false
            };
        let stage_timing = match input
            .session_timing
            .entry_action_for(group_plan, &group_key)
        {
            TreePitStageEntryAction::Query(stage_timing) => stage_timing,
            TreePitStageEntryAction::PushEmptySnapshot => {
                push_empty_materialized_tree_group_snapshot(
                    &mut state.groups,
                    &mut state.group_rank_metrics,
                    input.params.group_order,
                    input.read_class,
                    group_key,
                    &input.params.path,
                );
                return Ok(());
            }
            TreePitStageEntryAction::ReturnError(err) => return Err(err),
        };
        let stage_timeout = stage_timing.stage_timeout;
        if debug_materialized_route_capture_enabled() {
            let sink_group_summary = selected_group_sink_status
                .as_ref()
                .and_then(|snapshot| snapshot.groups.first())
                .map(|group| {
                    format!(
                        "group={} init={} live={} total={} primary={}",
                        group.group_id,
                        group.is_ready(),
                        group.live_nodes,
                        group.total_nodes,
                        group.primary_object_ref
                    )
                })
                .unwrap_or_else(|| "group=<missing>".to_string());
            eprintln!(
                "fs_meta_query_api: pit_group_stage group={} path={} trusted_ready={} prior_decoded={} last_ranked={} stage_timeout_ms={} remaining_ms={} sink_status={}",
                group_key,
                String::from_utf8_lossy(&input.params.path),
                group_plan.stage_plan.trusted_materialized_ready_group,
                group_plan.stage_plan.prior_materialized_group_decoded,
                is_last_ranked_group,
                stage_timeout.as_millis(),
                stage_timing.remaining_session.as_millis(),
                sink_group_summary
            );
        }
        let events = match run_timed_query(
            query_materialized_events_with_selected_group_owner_snapshot_and_request_scoped_omissions(
                input.state,
                input.policy,
                tree_params.clone(),
                stage_timeout,
                selected_group_sink_status.clone(),
                input.request_scoped_schedule_omitted_ready_groups,
                group_plan,
            ),
            stage_timeout,
        )
        .await
        {
            Ok(events) => events,
            Err(err) => match stage_timing.query_failure_action(group_plan, err) {
                TreePitStageQueryFailureAction::RetryableGap { gap, err } => {
                    handle_retryable_tree_pit_stage_gap(
                        RetryableTreePitStageGapInput {
                            state: input.state,
                            policy: input.policy,
                            tree_params: tree_params.clone(),
                            selected_group_sink_status: selected_group_sink_status.as_ref(),
                            retryable_gap: gap,
                            group_key,
                            query_path: &input.params.path,
                            read_class: input.read_class,
                            group_order: input.params.group_order,
                        },
                        err,
                        &mut state.groups,
                        &mut state.group_rank_metrics,
                    )
                    .await?;
                    return Ok(());
                }
                TreePitStageQueryFailureAction::Fatal(err) => {
                    if debug_materialized_route_capture_enabled() {
                        eprintln!(
                            "fs_meta_query_api: pit_group_stage fatal group={} path={} err={}",
                            group_key,
                            String::from_utf8_lossy(&input.params.path),
                            err
                        );
                    }
                    return Err(err);
                }
            },
        };
        apply_tree_pit_stage_execution_action(
            resolve_tree_pit_stage_execution_action(TreePitStageExecutionInput {
                state: input.state,
                policy: input.policy,
                tree_params: tree_params.clone(),
                events: &events,
                selected_group_sink_status: selected_group_sink_status.as_ref(),
                group_plan,
                stage_timing,
                selected_group_owner_known,
                group_key: &group_key,
                query_path: &input.params.path,
                request_scoped_schedule_omitted_ready_groups: input
                    .request_scoped_schedule_omitted_ready_groups,
            })
            .await,
            &mut state.groups,
            &mut state.group_rank_metrics,
            input.params.group_order,
            input.read_class,
            group_key,
            &input.params.path,
            rank_index,
            &mut state.deferred_first_ranked_empty_trusted_non_root_group,
        )?;
        Ok(())
    }
}

impl TreePitSessionMachine<'_> {
    async fn run(self) -> Result<PitSession, CnxError> {
        let snapshots = self.run_ranked_groups().await?;
        let read_class = self.input.read_class;
        let scope = TreePitScope {
            path: self.input.params.path.clone(),
            group: self.input.params.group.clone(),
            recursive: self.input.params.recursive,
            max_depth: self.input.params.max_depth,
            group_order: self.input.params.group_order,
            read_class,
        };
        Ok(build_pit_session(
            CursorQueryMode::Tree,
            PitScope::Tree(scope),
            read_class,
            self.observation_status,
            snapshots,
        ))
    }

    async fn run_ranked_groups(&self) -> Result<Vec<GroupPitSnapshot>, CnxError> {
        let mut state = TreePitRankedGroupsState::new(&self.rankings);
        for (rank_index, rank) in self.rankings.iter().cloned().enumerate() {
            TreePitGroupMachine {
                input: &self.input,
                rank_index,
                group_key: rank.group_key,
                state: &mut state,
            }
            .run()
            .await?;
        }
        Ok(state.finalize(self.input.params.group_order))
    }
}

async fn prepare_tree_pit_session_machine<'a>(
    state: &'a ApiState,
    policy: &'a ProjectionPolicy,
    params: &'a NormalizedApiParams,
    timeout: Duration,
    observation_status: ObservationStatus,
    request_sink_status: Option<&'a SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<&'a BTreeSet<String>>,
) -> Result<TreePitSessionMachine<'a>, CnxError> {
    let ranking_started_at = tokio::time::Instant::now();
    let target_groups = materialized_target_groups(
        state,
        params.group.as_deref(),
        None,
        request_sink_status,
        timeout,
        params.group.is_none() && !params.path.is_empty() && params.path != b"/",
        MaterializedTargetGroupSelectionMode::Tree,
    )
    .await?;
    let (rankings, ranking_retryable_failure) = collect_group_rankings(
        state,
        policy,
        QueryMode::Find,
        &params.path,
        params.recursive,
        timeout,
        Some(ranking_started_at + timeout),
        request_sink_status,
        params.group_order,
        &target_groups,
        None,
    )
    .await?;
    let read_class = tree_read_class(params);
    let total_ranked_groups = rankings.len();
    let session_plan = TreePitSessionPlan::new(timeout, total_ranked_groups);
    let session_timing = session_plan.session_timing(ranking_started_at, ranking_retryable_failure);
    Ok(TreePitSessionMachine {
        input: TreePitRankedGroupExecutionInput {
            state,
            policy,
            params,
            request_sink_status,
            request_scoped_schedule_omitted_ready_groups,
            observation_state: observation_status.state,
            session_plan,
            session_timing,
            read_class,
            total_ranked_groups,
        },
        rankings,
        observation_status,
    })
}

async fn build_force_find_pit_session(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    timeout: Duration,
) -> Result<PitSession, CnxError> {
    prepare_force_find_pit_session_machine(state, policy, params, timeout)
        .await?
        .run()
        .await
}

struct ForceFindPitSessionMachine<'a> {
    input: ForceFindRankedGroupExecutionInput<'a>,
    rankings: Vec<GroupRank>,
}

struct ForceFindPitGroupMachine<'a, 'b> {
    input: &'b ForceFindRankedGroupExecutionInput<'a>,
    rank: GroupRank,
}

impl ForceFindPitGroupMachine<'_, '_> {
    async fn run(self) -> Result<GroupPitSnapshot, CnxError> {
        let Self { input, rank } = self;
        let strict_conflict = input.params.group.as_deref() == Some(rank.group_key.as_str());
        let group_key = rank.group_key;
        let events = match query_force_find_group_tree(
            input.state,
            &input.params.path,
            input.params.recursive,
            input.params.max_depth,
            input.route_plan,
            &group_key,
            strict_conflict,
        )
        .await
        {
            Ok(events) => {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find group_tree result group={} events={} origins={:?}",
                        group_key,
                        events.len(),
                        summarize_event_counts_by_origin(&events)
                    );
                }
                events
            }
            Err(err) => {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find group_tree failed group={} strict_conflict={} err={}",
                        group_key, strict_conflict, err
                    );
                }
                if strict_conflict {
                    return Err(err);
                }
                return Ok(build_force_find_group_error_snapshot(group_key, err));
            }
        };
        match decode_force_find_selected_group_response(
            &events,
            input.policy,
            &group_key,
            &input.params.path,
        ) {
            Ok(payload) => {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find group_decode ok group={} root_exists={} has_children={} entries={} reliable={} errors=0",
                        group_key,
                        payload.root.exists,
                        payload.root.has_children,
                        payload.entries.len(),
                        payload.reliability.reliable
                    );
                }
                Ok(GroupPitSnapshot {
                    group: group_key,
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
                })
            }
            Err(err) => {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find group_decode failed group={} strict_conflict={} err={} events={} origins={:?}",
                        group_key,
                        strict_conflict,
                        err,
                        events.len(),
                        summarize_event_counts_by_origin(&events)
                    );
                }
                if strict_conflict {
                    Err(err)
                } else {
                    Ok(build_force_find_group_error_snapshot(group_key, err))
                }
            }
        }
    }
}

impl ForceFindPitSessionMachine<'_> {
    async fn run(self) -> Result<PitSession, CnxError> {
        let scope = ForceFindPitScope {
            path: self.input.params.path.clone(),
            group: self.input.params.group.clone(),
            recursive: self.input.params.recursive,
            max_depth: self.input.params.max_depth,
            group_order: self.input.params.group_order,
        };
        Ok(build_pit_session(
            CursorQueryMode::ForceFind,
            PitScope::ForceFind(scope),
            ReadClass::Fresh,
            ObservationStatus::fresh_only(),
            self.run_ranked_groups().await?,
        ))
    }

    async fn run_ranked_groups(&self) -> Result<Vec<GroupPitSnapshot>, CnxError> {
        let mut groups = Vec::with_capacity(self.rankings.len());
        for rank in self.rankings.iter().cloned() {
            groups.push(
                ForceFindPitGroupMachine {
                    input: &self.input,
                    rank,
                }
                .run()
                .await?,
            );
        }
        Ok(groups)
    }
}

async fn prepare_force_find_pit_session_machine<'a>(
    state: &'a ApiState,
    policy: &'a ProjectionPolicy,
    params: &'a NormalizedApiParams,
    timeout: Duration,
) -> Result<ForceFindPitSessionMachine<'a>, CnxError> {
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
            None,
            None,
            params.group_order,
            &target_groups,
            params.group.as_deref(),
        )
        .await?
        .0
    };
    Ok(ForceFindPitSessionMachine {
        input: ForceFindRankedGroupExecutionInput {
            state,
            policy,
            params,
            route_plan: ForceFindSessionPlan::new(timeout).route_plan(),
        },
        rankings,
    })
}

struct TreePitRankedGroupsState {
    groups: Vec<GroupPitSnapshot>,
    group_rank_metrics: BTreeMap<String, Option<u64>>,
    deferred_first_ranked_empty_trusted_non_root_group: Option<String>,
}

impl TreePitRankedGroupsState {
    fn new(rankings: &[GroupRank]) -> Self {
        Self {
            groups: Vec::with_capacity(rankings.len()),
            group_rank_metrics: rankings
                .iter()
                .map(|rank| (rank.group_key.clone(), rank.rank_metric))
                .collect(),
            deferred_first_ranked_empty_trusted_non_root_group: None,
        }
    }

    fn finalize(mut self, group_order: GroupOrder) -> Vec<GroupPitSnapshot> {
        if matches!(group_order, GroupOrder::FileCount | GroupOrder::FileAge) {
            self.groups.sort_by(|left, right| {
                let left_rank = self
                    .group_rank_metrics
                    .get(&left.group)
                    .copied()
                    .flatten()
                    .or_else(|| observed_rank_metric_from_tree_snapshot(group_order, left))
                    .unwrap_or(0);
                let right_rank = self
                    .group_rank_metrics
                    .get(&right.group)
                    .copied()
                    .flatten()
                    .or_else(|| observed_rank_metric_from_tree_snapshot(group_order, right))
                    .unwrap_or(0);
                right_rank
                    .cmp(&left_rank)
                    .then_with(|| left.group.cmp(&right.group))
            });
        }
        self.groups
    }
}

async fn query_tree_page_response(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    timeout: Duration,
    observation_status: ObservationStatus,
    request_sink_status: Option<SinkStatusSnapshot>,
    request_scoped_schedule_omitted_ready_groups: Option<BTreeSet<String>>,
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

    let _tree_query_serial = state.tree_query_serial.lock().await;
    let session = Arc::new(
        build_tree_pit_session_with_request_scoped_schedule_omitted_ready_groups(
            state,
            policy,
            params,
            timeout,
            observation_status,
            request_sink_status.as_ref(),
            request_scoped_schedule_omitted_ready_groups.as_ref(),
        )
        .await?,
    );
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

fn observed_rank_metric_from_tree_snapshot(
    group_order: GroupOrder,
    group: &GroupPitSnapshot,
) -> Option<u64> {
    match group_order {
        GroupOrder::GroupKey => None,
        GroupOrder::FileCount => Some(group.entries.len() as u64),
        GroupOrder::FileAge => {
            let root_mtime = group
                .root
                .as_ref()
                .map(|root| root.modified_time_us)
                .unwrap_or(0);
            Some(
                group
                    .entries
                    .iter()
                    .fold(root_mtime, |best, entry| best.max(entry.modified_time_us)),
            )
        }
    }
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
                if matches!(read_class, ReadClass::Fresh) {
                    current.blind_spot_count += stats.blind_spot_count;
                }
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

fn stats_group_value_metrics(group: &serde_json::Value) -> Option<(u64, u64, u64, u64, u64)> {
    let data = group.get("data")?;
    let total_files = data.get("total_files")?.as_u64()?;
    let total_nodes = data.get("total_nodes")?.as_u64()?;
    let total_dirs = data.get("total_dirs")?.as_u64()?;
    let total_size = data.get("total_size")?.as_u64()?;
    let latest_mtime = data
        .get("latest_file_mtime_us")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    Some((
        total_files,
        total_nodes,
        total_dirs,
        total_size,
        latest_mtime,
    ))
}

fn stats_group_looks_zeroish(group: &serde_json::Value) -> bool {
    stats_group_value_metrics(group).is_some_and(
        |(total_files, total_nodes, total_dirs, total_size, latest_mtime)| {
            total_files == 0
                && total_size == 0
                && latest_mtime == 0
                && total_nodes <= 1
                && total_dirs <= 1
        },
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SelectedGroupStatsRescueLane {
    ReturnCurrent,
    ProxyFallback,
    ProxyFallbackThenFailClosed,
    FailClosed,
}
struct StatsQueryMachine<'a> {
    state: &'a ApiState,
    policy: &'a ProjectionPolicy,
    params: &'a NormalizedApiParams,
    read_class: ReadClass,
    request_source_status: Option<&'a SourceStatusSnapshot>,
    request_sink_status: Option<&'a SinkStatusSnapshot>,
}

struct StatsGroupMachine<'a, 'b> {
    query: &'b StatsQueryMachine<'a>,
    group_id: String,
}

impl StatsGroupMachine<'_, '_> {
    async fn run(&self) -> Result<serde_json::Value, CnxError> {
        let group_id = &self.group_id;
        let materialized_request = build_materialized_stats_request(
            &self.query.params.path,
            self.query.params.recursive,
            Some(group_id.clone()),
        );
        let group_plan = TreePitSessionPlan::new(self.query.policy.query_timeout(), 1)
            .selected_group_stage_plan(TreePitGroupPlanInput {
                read_class: self.query.read_class,
                observation_state: match self.query.read_class {
                    ReadClass::TrustedMaterialized => ObservationState::TrustedMaterialized,
                    ReadClass::Materialized => ObservationState::MaterializedUntrusted,
                    ReadClass::Fresh => {
                        unreachable!("fresh stats do not build materialized PIT group plans")
                    }
                },
                selected_group_sink_reports_live_materialized:
                    selected_group_sink_status_reports_live_materialized_group(
                        self.query.request_sink_status,
                        group_id,
                    ),
                prior_materialized_group_decoded: false,
                prior_materialized_exact_file_decoded: false,
                rank_index: 0,
                is_last_ranked_group: true,
                selected_group_sink_unready_empty: false,
                empty_root_requires_fail_closed: false,
            });
        let events = match self.query.read_class {
            ReadClass::Fresh => {
                let strict_conflict = self.query.params.group.as_deref() == Some(group_id.as_str());
                query_force_find_group_stats(
                    self.query.state,
                    &self.query.params.path,
                    self.query.params.recursive,
                    ForceFindSessionPlan::new(self.query.policy.force_find_timeout()).route_plan(),
                    group_id,
                    strict_conflict,
                )
                .await?
            }
            ReadClass::Materialized | ReadClass::TrustedMaterialized => {
                if let Some(snapshot) = self.query.request_sink_status {
                    query_materialized_events_with_selected_group_owner_snapshot_and_request_scoped_omissions(
                        self.query.state,
                        self.query.policy,
                        materialized_request.clone(),
                        self.query.policy.query_timeout(),
                        Some(snapshot.clone()),
                        None,
                        group_plan,
                    )
                    .await?
                } else {
                    query_materialized_events(
                        self.query.state.backend.clone(),
                        materialized_request.clone(),
                        self.query.policy.query_timeout(),
                    )
                    .await?
                }
            }
        };
        let mut targeted = decode_stats_groups(
            events,
            self.query.policy,
            Some(group_id),
            self.query.read_class,
        );
        targeted = self
            .rescue_targeted_group(
                &materialized_request,
                group_id,
                group_plan,
                tokio::time::Instant::now() + self.query.policy.query_timeout(),
                targeted,
            )
            .await?;
        Ok(targeted
            .get(group_id)
            .cloned()
            .unwrap_or_else(zero_stats_group_json))
    }

    async fn rescue_targeted_group(
        &self,
        materialized_request: &InternalQueryRequest,
        group_id: &str,
        group_plan: TreePitGroupPlan,
        deadline: tokio::time::Instant,
        mut targeted: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Map<String, serde_json::Value>, CnxError> {
        let rescue_lane = group_plan.stage_plan.stats_rescue_lane(
            materialized_request.scope.path.is_empty() || materialized_request.scope.path == b"/",
            targeted
                .get(group_id)
                .is_some_and(stats_group_looks_zeroish),
            matches!(&self.query.state.backend, QueryBackend::Route { .. }),
        );

        match rescue_lane {
            SelectedGroupStatsRescueLane::ReturnCurrent => Ok(targeted),
            SelectedGroupStatsRescueLane::FailClosed => Err(
                trusted_materialized_unavailable_selected_group_stats_error(group_id),
            ),
            SelectedGroupStatsRescueLane::ProxyFallback
            | SelectedGroupStatsRescueLane::ProxyFallbackThenFailClosed => {
                let QueryBackend::Route {
                    boundary,
                    origin_id,
                    ..
                } = &self.query.state.backend
                else {
                    return if matches!(
                        rescue_lane,
                        SelectedGroupStatsRescueLane::ProxyFallbackThenFailClosed
                    ) {
                        Err(trusted_materialized_unavailable_selected_group_stats_error(
                            group_id,
                        ))
                    } else {
                        Ok(targeted)
                    };
                };

                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return if matches!(
                        rescue_lane,
                        SelectedGroupStatsRescueLane::ProxyFallbackThenFailClosed
                    ) {
                        Err(trusted_materialized_unavailable_selected_group_stats_error(
                            group_id,
                        ))
                    } else {
                        Ok(targeted)
                    };
                }
                let proxy_timeout = group_plan.stats_proxy_route_plan(remaining);
                if proxy_timeout.route_timeout().is_zero() {
                    return if matches!(
                        rescue_lane,
                        SelectedGroupStatsRescueLane::ProxyFallbackThenFailClosed
                    ) {
                        Err(trusted_materialized_unavailable_selected_group_stats_error(
                            group_id,
                        ))
                    } else {
                        Ok(targeted)
                    };
                }

                match query_materialized_events_via_generic_proxy(
                    boundary.clone(),
                    origin_id.clone(),
                    materialized_request.clone(),
                    proxy_timeout.machine(),
                )
                .await
                {
                    Ok(proxy_events) => {
                        let proxy_targeted = decode_stats_groups(
                            proxy_events,
                            self.query.policy,
                            Some(group_id),
                            ReadClass::TrustedMaterialized,
                        );
                        let current_metrics = targeted
                            .get(group_id)
                            .and_then(stats_group_value_metrics)
                            .unwrap_or((0, 0, 0, 0, 0));
                        let proxy_metrics = proxy_targeted
                            .get(group_id)
                            .and_then(stats_group_value_metrics)
                            .unwrap_or((0, 0, 0, 0, 0));
                        if proxy_metrics > current_metrics {
                            targeted = proxy_targeted;
                        }
                    }
                    Err(CnxError::Timeout)
                    | Err(CnxError::TransportClosed(_))
                    | Err(CnxError::ProtocolViolation(_)) => {}
                    Err(err) => return Err(err),
                }

                if matches!(
                    rescue_lane,
                    SelectedGroupStatsRescueLane::ProxyFallbackThenFailClosed
                ) && targeted
                    .get(group_id)
                    .is_some_and(stats_group_looks_zeroish)
                {
                    return Err(trusted_materialized_unavailable_selected_group_stats_error(
                        group_id,
                    ));
                }

                Ok(targeted)
            }
        }
    }
}

impl StatsQueryMachine<'_> {
    async fn run(&self) -> Result<serde_json::Map<String, serde_json::Value>, CnxError> {
        let mut groups = serde_json::Map::<String, serde_json::Value>::new();
        let target_groups = match self.read_class {
            ReadClass::Fresh => resolve_force_find_groups(self.state, self.params).await?,
            ReadClass::Materialized | ReadClass::TrustedMaterialized => {
                materialized_target_groups(
                    self.state,
                    self.params.group.as_deref(),
                    self.request_source_status,
                    self.request_sink_status,
                    self.policy.query_timeout(),
                    self.params.group.is_none()
                        && !self.params.path.is_empty()
                        && self.params.path != b"/",
                    MaterializedTargetGroupSelectionMode::Stats,
                )
                .await?
            }
        };
        for group_id in target_groups {
            groups.insert(
                group_id.clone(),
                StatsGroupMachine {
                    query: self,
                    group_id,
                }
                .run()
                .await?,
            );
        }

        Ok(groups)
    }
}

async fn collect_materialized_stats_groups(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    read_class: ReadClass,
    request_source_status: Option<&SourceStatusSnapshot>,
    request_sink_status: Option<&SinkStatusSnapshot>,
) -> Result<serde_json::Map<String, serde_json::Value>, CnxError> {
    StatsQueryMachine {
        state,
        policy,
        params,
        read_class,
        request_source_status,
        request_sink_status,
    }
    .run()
    .await
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
    let mut request_source_status = None::<SourceStatusSnapshot>;
    let mut request_sink_status = None::<SinkStatusSnapshot>;
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
            request_source_status = Some(
                if params.group.is_none() && !params.path.is_empty() && params.path != b"/" {
                    let status_load_plan = MaterializedStatusLoadPlan::default();
                    match &state.backend {
                        QueryBackend::Route {
                            boundary,
                            origin_id,
                            ..
                        } => merge_source_status_snapshots(vec![
                            source_status.clone(),
                            route_source_status_snapshot(
                                boundary.clone(),
                                origin_id.clone(),
                                status_load_plan
                                    .request_source_refresh_plan(policy.query_timeout()),
                            )
                            .await
                            .unwrap_or_else(|_| source_status.clone()),
                        ]),
                        QueryBackend::Local { .. } => source_status.clone(),
                    }
                } else {
                    source_status.clone()
                },
            );
            request_sink_status = Some(sink_status.clone());
            status
        }
    };
    let groups = match collect_materialized_stats_groups(
        &state,
        &policy,
        &params,
        read_class,
        request_source_status.as_ref(),
        request_sink_status.as_ref(),
    )
    .await
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
        && trusted_materialized_empty_group_root_requires_fail_closed(&params.path)
    {
        return error_response_with_context(
            CnxError::NotReady(trusted_materialized_not_ready_message(&observation_status)),
            Some(&path_for_error),
        );
    }
    if read_class == ReadClass::TrustedMaterialized
        && observation_status.state != ObservationState::TrustedMaterialized
        && !trusted_materialized_empty_group_root_requires_fail_closed(&params.path)
    {
        eprintln!(
            "fs_meta_query_api: trusted non-root tree proceeding with request-scoped readiness path={} not_ready={}",
            String::from_utf8_lossy(&params.path),
            trusted_materialized_not_ready_message(&observation_status)
        );
    }
    let (request_sink_status, request_scoped_schedule_omitted_ready_groups) =
        if read_class == ReadClass::TrustedMaterialized {
            let request_scoped_plan = TreePitSessionPlan::new(policy.query_timeout(), 1)
                .request_scoped_sink_status_plan();
            let request_scoped =
                load_request_scoped_materialized_sink_status_snapshot(&state, request_scoped_plan)
                    .await
                    .unwrap_or_else(|| sink_status.clone());
            let request_scoped_schedule_omitted_ready_groups =
                request_scoped_schedule_omitted_ready_groups(&sink_status, &request_scoped);
            let merged = merge_request_scoped_materialized_sink_status_snapshot(
                &sink_status,
                request_scoped.clone(),
            );
            let preserved_omission =
            preserve_request_loaded_ready_groups_across_partial_request_scoped_schedule_omission(
                merged,
                &sink_status,
                &request_scoped,
            );
            let preserved =
                preserve_request_loaded_ready_groups_across_explicit_empty_request_scoped_drift(
                    preserved_omission,
                    &sink_status,
                    &request_scoped,
                );
            if trusted_materialized_empty_group_root_requires_fail_closed(&params.path)
                && sink_status_snapshot_omits_all_groups_from_schedule(&request_scoped)
            {
                (
                    request_scoped,
                    Some(request_scoped_schedule_omitted_ready_groups),
                )
            } else {
                (
                    preserved,
                    Some(request_scoped_schedule_omitted_ready_groups),
                )
            }
        } else {
            (sink_status.clone(), None)
        };
    match query_tree_page_response(
        &state,
        &policy,
        &params,
        policy.query_timeout(),
        observation_status,
        Some(request_sink_status),
        request_scoped_schedule_omitted_ready_groups,
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
        let mut groups = scan_enabled_groups.iter().cloned().collect::<Vec<_>>();
        if let Ok(snapshot) = source.observability_snapshot().await {
            groups.extend(
                snapshot
                    .source_primary_by_group
                    .into_keys()
                    .filter(|group_id| scan_enabled_groups.contains(group_id))
                    .collect::<Vec<_>>(),
            );
        }
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
    let state = QueryObservationState::from(status);
    serde_json::json!({
        "state": state.as_str(),
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
mod tests;
