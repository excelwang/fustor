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
const RANKING_QUERY_MIN_BUDGET: Duration = Duration::from_millis(1000);
const SELECTED_GROUP_PROXY_FALLBACK_MIN_BUDGET: Duration = Duration::from_millis(2000);
const TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET: Duration = Duration::from_millis(400);
const TRUSTED_READY_LATER_RANKED_NON_ROOT_RETRY_BUDGET: Duration = Duration::from_millis(100);
const UNREADY_SELECTED_GROUP_ROUTE_BUDGET: Duration = Duration::from_millis(1200);
const EXPLICIT_EMPTY_SINK_STATUS_RECOLLECT_BUDGET: Duration = Duration::from_millis(250);
const FIRST_RANKED_TRUSTED_READY_GROUP_STAGE_BUDGET: Duration = Duration::from_millis(2500);
const LATER_RANKED_TRUSTED_GROUP_STAGE_BUDGET: Duration = Duration::from_millis(1500);
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

async fn route_source_status_snapshot(
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    timeout: Duration,
) -> Result<SourceStatusSnapshot, CnxError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_err = None::<CnxError>;
    let events = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break Err(last_err.unwrap_or(CnxError::Timeout));
        }
        let attempt_timeout = remaining.min(Duration::from_secs(5));
        let adapter = exchange_host_adapter(
            boundary.clone(),
            origin_id.clone(),
            default_route_bindings(),
        );
        match adapter
            .call_collect(
                ROUTE_TOKEN_FS_META_INTERNAL,
                METHOD_SOURCE_STATUS,
                internal_status_request_payload(),
                attempt_timeout,
                STATUS_ROUTE_COLLECT_IDLE_GRACE,
            )
            .await
        {
            Ok(events) => break Ok(events),
            Err(err) if is_retryable_source_status_continuity_gap(&err) => {
                last_err = Some(err);
            }
            Err(err) => break Err(err),
        }
    }?;
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
    timeout: Duration,
) -> Result<SinkStatusSnapshot, CnxError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_err = None::<CnxError>;
    let events = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break Err(last_err.unwrap_or(CnxError::Timeout));
        }
        let attempt_timeout = remaining.min(Duration::from_secs(5));
        let adapter = exchange_host_adapter(
            boundary.clone(),
            origin_id.clone(),
            default_route_bindings(),
        );
        match adapter
            .call_collect(
                ROUTE_TOKEN_FS_META_INTERNAL,
                METHOD_SINK_STATUS,
                internal_status_request_payload(),
                attempt_timeout,
                STATUS_ROUTE_COLLECT_IDLE_GRACE,
            )
            .await
        {
            Ok(events) => break Ok(events),
            Err(err) if is_retryable_sink_status_continuity_gap(&err) => {
                last_err = Some(err);
            }
            Err(err) => break Err(err),
        }
    }?;
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
                Duration::from_secs(30),
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
                    EXPLICIT_EMPTY_SINK_STATUS_RECOLLECT_BUDGET,
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
    !group.initial_audit_completed
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
        u8::from(group.initial_audit_completed),
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
                && group.initial_audit_completed
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
                        std::cmp::min(timeout, Duration::from_secs(5)),
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
                    timeout,
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
        .is_some_and(|group| {
            group.initial_audit_completed && group.live_nodes > 0 && group.total_nodes > 0
        })
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
        .is_some_and(|group| {
            !group.initial_audit_completed && group.live_nodes == 0 && group.total_nodes == 0
        })
}

fn selected_group_materialized_tree_payload_is_empty(
    policy: &ProjectionPolicy,
    events: &[Event],
    group_id: &str,
) -> bool {
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

    let mut best = None::<TreeGroupPayload>;
    let mut best_precedence = None::<(u64, u8, usize, bool, u64)>;
    for event in events {
        if event_group_key(policy, event) != group_id {
            continue;
        }
        let Ok(MaterializedQueryPayload::Tree(payload)) =
            rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
        else {
            continue;
        };
        let precedence = payload_precedence(event, &payload);
        let replace = best_precedence
            .as_ref()
            .is_none_or(|current| precedence > *current);
        if replace {
            best_precedence = Some(precedence);
            best = Some(payload);
        }
    }

    best.is_some_and(|payload| {
        !payload.root.exists && payload.entries.is_empty() && !payload.root.has_children
    })
}

fn selected_group_empty_ready_tree_requires_proxy_fallback(
    policy: &ProjectionPolicy,
    params: &InternalQueryRequest,
    selected_group_sink_status: Option<&SinkStatusSnapshot>,
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
        && selected_group_materialized_tree_payload_is_empty(policy, events, group_id)
}

fn selected_group_empty_ready_tree_requires_primary_owner_reroute(
    policy: &ProjectionPolicy,
    params: &InternalQueryRequest,
    selected_group_sink_status: Option<&SinkStatusSnapshot>,
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
        && selected_group_materialized_tree_payload_is_empty(policy, events, group_id)
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
            overflow_pending_audit: false,
            initial_audit_completed: true,
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

fn selected_group_owner_attempt_timeout(
    deadline: tokio::time::Instant,
    prefer_proxy_budget: bool,
) -> Duration {
    let remaining = deadline
        .checked_duration_since(tokio::time::Instant::now())
        .unwrap_or_default();
    if remaining.is_zero() {
        return Duration::ZERO;
    }
    let owner_min_budget = std::cmp::min(
        remaining,
        if prefer_proxy_budget {
            TRUSTED_READY_LATER_RANKED_NON_ROOT_RETRY_BUDGET
        } else {
            TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET
        },
    );
    let proxy_reserve = std::cmp::max(
        remaining.checked_div(2).unwrap_or_default(),
        std::cmp::min(remaining, SELECTED_GROUP_PROXY_FALLBACK_MIN_BUDGET),
    );
    let capped_proxy_reserve =
        std::cmp::min(proxy_reserve, remaining.saturating_sub(owner_min_budget));
    remaining.saturating_sub(capped_proxy_reserve)
}

async fn query_materialized_events_via_generic_proxy(
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    params: InternalQueryRequest,
    timeout: Duration,
) -> Result<Vec<Event>, CnxError> {
    if debug_materialized_route_capture_enabled() {
        eprintln!(
            "fs_meta_query_api: materialized_proxy_capture begin selected_group={:?} recursive={} path={} timeout_ms={}",
            params.scope.selected_group,
            params.scope.recursive,
            String::from_utf8_lossy(&params.scope.path),
            timeout.as_millis()
        );
    }
    let payload = rmp_serde::to_vec(&params)
        .map_err(|err| CnxError::Internal(format!("encode materialized query failed: {err}")))?;
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_err = None::<CnxError>;
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            return Err(last_err.unwrap_or(CnxError::Timeout));
        }
        let adapter = exchange_host_adapter(
            boundary.clone(),
            origin_id.clone(),
            default_route_bindings(),
        );
        match adapter
            .call_collect(
                ROUTE_TOKEN_FS_META_INTERNAL,
                METHOD_SINK_QUERY_PROXY,
                Bytes::from(payload.clone()),
                remaining,
                MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE,
            )
            .await
        {
            Ok(events) => {
                if debug_materialized_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: materialized_proxy_capture done selected_group={:?} events={} origins={:?}",
                        params.scope.selected_group,
                        events.len(),
                        summarize_event_counts_by_origin(&events)
                    );
                }
                return Ok(events);
            }
            Err(err) if is_retryable_materialized_proxy_continuity_gap(&err) => {
                if debug_materialized_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: materialized_proxy_capture retry selected_group={:?} remaining_ms={} err={}",
                        params.scope.selected_group,
                        remaining.as_millis(),
                        err
                    );
                }
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(err) => {
                if debug_materialized_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: materialized_proxy_capture failed selected_group={:?} err={}",
                        params.scope.selected_group, err
                    );
                }
                return Err(err);
            }
        }
    }
}

fn is_retryable_materialized_proxy_continuity_gap(err: &CnxError) -> bool {
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

async fn query_materialized_events_with_selected_group_owner_snapshot(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: InternalQueryRequest,
    timeout: Duration,
    selected_group_sink_status: Option<SinkStatusSnapshot>,
    reserve_proxy_budget: bool,
    allow_empty_owner_retry: bool,
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
                && let Some(node_id) = materialized_owner_node_for_group(
                    source.as_ref(),
                    selected_group_sink_status.as_ref(),
                    group_id,
                )
                .await?
            {
                if node_id == *origin_id {
                    let owner_attempt_timeout = if reserve_proxy_budget {
                        selected_group_owner_attempt_timeout(
                            deadline,
                            !trusted_materialized_empty_group_root_requires_fail_closed(
                                &params.scope.path,
                            ),
                        )
                    } else {
                        deadline
                            .checked_duration_since(tokio::time::Instant::now())
                            .unwrap_or_default()
                    };
                    let events =
                        run_timed_query(sink.materialized_query(&params), owner_attempt_timeout)
                            .await?;
                    let requires_proxy_fallback =
                        selected_group_empty_ready_tree_requires_proxy_fallback(
                            policy,
                            &params,
                            selected_group_sink_status.as_ref(),
                            group_id,
                            &events,
                        );
                    let requires_primary_owner_reroute =
                        selected_group_empty_ready_tree_requires_primary_owner_reroute(
                            policy,
                            &params,
                            selected_group_sink_status.as_ref(),
                            group_id,
                            &events,
                        );
                    if !requires_proxy_fallback {
                        if requires_primary_owner_reroute
                            && let Some(primary_node_id) = sink_primary_owner_node_for_group(
                                selected_group_sink_status.as_ref(),
                                group_id,
                            )
                            .filter(|primary_node_id| *primary_node_id != *origin_id)
                        {
                            let remaining = deadline
                                .checked_duration_since(tokio::time::Instant::now())
                                .unwrap_or_default();
                            if !remaining.is_zero() {
                                let primary_events = route_materialized_events_via_node(
                                    boundary.clone(),
                                    primary_node_id,
                                    params.clone(),
                                    remaining,
                                )
                                .await?;
                                if !selected_group_materialized_tree_payload_is_empty(
                                    policy,
                                    &primary_events,
                                    group_id,
                                ) {
                                    return Ok(primary_events);
                                }
                            }
                        }
                        if allow_empty_owner_retry
                            && selected_group_sink_status_reports_live_materialized_group(
                                selected_group_sink_status.as_ref(),
                                group_id,
                            )
                            && selected_group_materialized_tree_payload_is_empty(
                                policy, &events, group_id,
                            )
                        {
                            let remaining = deadline
                                .checked_duration_since(tokio::time::Instant::now())
                                .unwrap_or_default();
                            if !remaining.is_zero() {
                                let retry_owner_timeout = std::cmp::min(
                                    remaining,
                                    TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET,
                                );
                                if !retry_owner_timeout.is_zero() {
                                    match run_timed_query(
                                        sink.materialized_query(&params),
                                        retry_owner_timeout,
                                    )
                                    .await
                                    {
                                        Ok(retry_events) => {
                                            if !selected_group_materialized_tree_payload_is_empty(
                                                policy,
                                                &retry_events,
                                                group_id,
                                            ) {
                                                return Ok(retry_events);
                                            }
                                            let remaining = deadline
                                                .checked_duration_since(tokio::time::Instant::now())
                                                .unwrap_or_default();
                                            if !remaining.is_zero() {
                                                let proxy_events =
                                                    query_materialized_events_via_generic_proxy(
                                                        boundary.clone(),
                                                        origin_id.clone(),
                                                        params.clone(),
                                                        remaining,
                                                    )
                                                    .await?;
                                                return Ok(proxy_events);
                                            }
                                        }
                                        Err(CnxError::Timeout)
                                        | Err(CnxError::TransportClosed(_))
                                        | Err(CnxError::ProtocolViolation(_)) => {
                                            let remaining = deadline
                                                .checked_duration_since(tokio::time::Instant::now())
                                                .unwrap_or_default();
                                            if !remaining.is_zero() {
                                                let proxy_events =
                                                    query_materialized_events_via_generic_proxy(
                                                        boundary.clone(),
                                                        origin_id.clone(),
                                                        params.clone(),
                                                        remaining,
                                                    )
                                                    .await?;
                                                return Ok(proxy_events);
                                            }
                                        }
                                        Err(err) => return Err(err),
                                    }
                                }
                            }
                        }
                        return Ok(events);
                    }
                    if !reserve_proxy_budget && !allow_empty_owner_retry {
                        return Err(trusted_materialized_empty_selected_group_tree_error(
                            group_id,
                        ));
                    }
                    let remaining = deadline
                        .checked_duration_since(tokio::time::Instant::now())
                        .unwrap_or_default();
                    if remaining.is_zero() {
                        return Err(trusted_materialized_empty_selected_group_tree_error(
                            group_id,
                        ));
                    }
                    if let Some(primary_node_id) = sink_primary_owner_node_for_group(
                        selected_group_sink_status.as_ref(),
                        group_id,
                    )
                    .filter(|primary_node_id| *primary_node_id != *origin_id)
                    {
                        let primary_events = route_materialized_events_via_node(
                            boundary.clone(),
                            primary_node_id,
                            params.clone(),
                            remaining,
                        )
                        .await?;
                        if !selected_group_empty_ready_tree_requires_proxy_fallback(
                            policy,
                            &params,
                            selected_group_sink_status.as_ref(),
                            group_id,
                            &primary_events,
                        ) {
                            return Ok(primary_events);
                        }
                    }
                    let events = query_materialized_events_via_generic_proxy(
                        boundary.clone(),
                        origin_id.clone(),
                        params.clone(),
                        remaining,
                    )
                    .await?;
                    if selected_group_empty_ready_tree_requires_proxy_fallback(
                        policy,
                        &params,
                        selected_group_sink_status.as_ref(),
                        group_id,
                        &events,
                    ) {
                        if !allow_empty_owner_retry {
                            return Ok(events);
                        }
                        let remaining = deadline
                            .checked_duration_since(tokio::time::Instant::now())
                            .unwrap_or_default();
                        if !remaining.is_zero() {
                            let retry_owner_timeout =
                                std::cmp::min(remaining, TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET);
                            if !retry_owner_timeout.is_zero() {
                                let retry_events = run_timed_query(
                                    sink.materialized_query(&params),
                                    retry_owner_timeout,
                                )
                                .await?;
                                if !selected_group_empty_ready_tree_requires_proxy_fallback(
                                    policy,
                                    &params,
                                    selected_group_sink_status.as_ref(),
                                    group_id,
                                    &retry_events,
                                ) {
                                    return Ok(retry_events);
                                }
                            }
                        }
                        return Err(trusted_materialized_empty_selected_group_tree_error(
                            group_id,
                        ));
                    }
                    return Ok(events);
                }
                let owner_attempt_timeout = if reserve_proxy_budget {
                    selected_group_owner_attempt_timeout(
                        deadline,
                        !trusted_materialized_empty_group_root_requires_fail_closed(
                            &params.scope.path,
                        ),
                    )
                } else {
                    deadline
                        .checked_duration_since(tokio::time::Instant::now())
                        .unwrap_or_default()
                };
                match route_materialized_events_via_node(
                    boundary.clone(),
                    node_id.clone(),
                    params.clone(),
                    owner_attempt_timeout,
                )
                .await
                {
                    Ok(events) => {
                        let requires_proxy_fallback =
                            selected_group_empty_ready_tree_requires_proxy_fallback(
                                policy,
                                &params,
                                selected_group_sink_status.as_ref(),
                                group_id,
                                &events,
                            );
                        let requires_primary_owner_reroute =
                            selected_group_empty_ready_tree_requires_primary_owner_reroute(
                                policy,
                                &params,
                                selected_group_sink_status.as_ref(),
                                group_id,
                                &events,
                            );
                        if !requires_proxy_fallback {
                            if requires_primary_owner_reroute
                                && let Some(primary_node_id) = sink_primary_owner_node_for_group(
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
                                        remaining,
                                    )
                                    .await?;
                                    if !selected_group_materialized_tree_payload_is_empty(
                                        policy,
                                        &primary_events,
                                        group_id,
                                    ) {
                                        return Ok(primary_events);
                                    }
                                }
                            }
                            if allow_empty_owner_retry
                                && selected_group_sink_status_reports_live_materialized_group(
                                    selected_group_sink_status.as_ref(),
                                    group_id,
                                )
                                && selected_group_materialized_tree_payload_is_empty(
                                    policy, &events, group_id,
                                )
                            {
                                let remaining = deadline
                                    .checked_duration_since(tokio::time::Instant::now())
                                    .unwrap_or_default();
                                if !remaining.is_zero() {
                                    let retry_owner_timeout = std::cmp::min(
                                        remaining,
                                        TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET,
                                    );
                                    if !retry_owner_timeout.is_zero() {
                                        match route_materialized_events_via_node(
                                            boundary.clone(),
                                            node_id.clone(),
                                            params.clone(),
                                            retry_owner_timeout,
                                        )
                                        .await
                                        {
                                            Ok(retry_events) => {
                                                if !selected_group_materialized_tree_payload_is_empty(
                                                    policy,
                                                    &retry_events,
                                                    group_id,
                                                ) {
                                                    return Ok(retry_events);
                                                }
                                                let remaining = deadline
                                                    .checked_duration_since(
                                                        tokio::time::Instant::now(),
                                                    )
                                                    .unwrap_or_default();
                                                if !remaining.is_zero() {
                                                    let proxy_events =
                                                        query_materialized_events_via_generic_proxy(
                                                            boundary.clone(),
                                                            origin_id.clone(),
                                                            params.clone(),
                                                            remaining,
                                                        )
                                                        .await?;
                                                    return Ok(proxy_events);
                                                }
                                            }
                                            Err(CnxError::Timeout)
                                            | Err(CnxError::TransportClosed(_))
                                            | Err(CnxError::ProtocolViolation(_)) => {
                                                let remaining = deadline
                                                    .checked_duration_since(
                                                        tokio::time::Instant::now(),
                                                    )
                                                    .unwrap_or_default();
                                                if !remaining.is_zero() {
                                                    let proxy_events =
                                                        query_materialized_events_via_generic_proxy(
                                                            boundary.clone(),
                                                            origin_id.clone(),
                                                            params.clone(),
                                                            remaining,
                                                        )
                                                        .await?;
                                                    return Ok(proxy_events);
                                                }
                                            }
                                            Err(err) => return Err(err),
                                        }
                                    }
                                }
                            }
                            return Ok(events);
                        }
                        if !reserve_proxy_budget && !allow_empty_owner_retry {
                            return Err(trusted_materialized_empty_selected_group_tree_error(
                                group_id,
                            ));
                        }
                        let remaining = deadline
                            .checked_duration_since(tokio::time::Instant::now())
                            .unwrap_or_default();
                        if remaining.is_zero() {
                            return Err(trusted_materialized_empty_selected_group_tree_error(
                                group_id,
                            ));
                        }
                        if let Some(primary_node_id) = sink_primary_owner_node_for_group(
                            selected_group_sink_status.as_ref(),
                            group_id,
                        )
                        .filter(|primary_node_id| *primary_node_id != node_id)
                        {
                            let primary_events = route_materialized_events_via_node(
                                boundary.clone(),
                                primary_node_id,
                                params.clone(),
                                remaining,
                            )
                            .await?;
                            if !selected_group_empty_ready_tree_requires_proxy_fallback(
                                policy,
                                &params,
                                selected_group_sink_status.as_ref(),
                                group_id,
                                &primary_events,
                            ) {
                                return Ok(primary_events);
                            }
                        }
                        let events = query_materialized_events_via_generic_proxy(
                            boundary.clone(),
                            origin_id.clone(),
                            params.clone(),
                            remaining,
                        )
                        .await?;
                        if selected_group_empty_ready_tree_requires_proxy_fallback(
                            policy,
                            &params,
                            selected_group_sink_status.as_ref(),
                            group_id,
                            &events,
                        ) {
                            if !allow_empty_owner_retry {
                                return Ok(events);
                            }
                            let remaining = deadline
                                .checked_duration_since(tokio::time::Instant::now())
                                .unwrap_or_default();
                            if !remaining.is_zero() {
                                let retry_owner_timeout = std::cmp::min(
                                    remaining,
                                    TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET,
                                );
                                if !retry_owner_timeout.is_zero() {
                                    let retry_events = route_materialized_events_via_node(
                                        boundary.clone(),
                                        node_id.clone(),
                                        params.clone(),
                                        retry_owner_timeout,
                                    )
                                    .await?;
                                    if !selected_group_empty_ready_tree_requires_proxy_fallback(
                                        policy,
                                        &params,
                                        selected_group_sink_status.as_ref(),
                                        group_id,
                                        &retry_events,
                                    ) {
                                        return Ok(retry_events);
                                    }
                                }
                            }
                            return Err(trusted_materialized_empty_selected_group_tree_error(
                                group_id,
                            ));
                        }
                        return Ok(events);
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
                        let fail_closed_after_proxy_gap = reserve_proxy_budget
                            && !allow_empty_owner_retry
                            && trusted_materialized_empty_group_root_requires_fail_closed(
                                &params.scope.path,
                            );
                        let proxy_timeout = if fail_closed_after_proxy_gap {
                            std::cmp::min(remaining, TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET)
                        } else {
                            remaining
                        };
                        if proxy_timeout.is_zero() {
                            return Err(CnxError::Timeout);
                        }
                        let proxy_events = match query_materialized_events_via_generic_proxy(
                            boundary.clone(),
                            origin_id.clone(),
                            params.clone(),
                            proxy_timeout,
                        )
                        .await
                        {
                            Ok(proxy_events) => proxy_events,
                            Err(CnxError::Timeout)
                            | Err(CnxError::TransportClosed(_))
                            | Err(CnxError::ProtocolViolation(_))
                                if fail_closed_after_proxy_gap =>
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
                                    remaining,
                                )
                                .await?;
                                if !selected_group_materialized_tree_payload_is_empty(
                                    policy,
                                    &primary_events,
                                    group_id,
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
                let remaining = deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    return Err(CnxError::Timeout);
                }
                return query_materialized_events_via_generic_proxy(
                    boundary.clone(),
                    origin_id.clone(),
                    params,
                    remaining,
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
                    timeout,
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
                        query_timeout,
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

async fn materialized_target_groups(
    state: &ApiState,
    selected_group: Option<&str>,
    request_sink_status: Option<&SinkStatusSnapshot>,
    timeout: Duration,
    preserve_unscheduled_groups: bool,
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
                std::cmp::min(timeout, Duration::from_secs(5)),
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
    if let Some(scheduled_groups) = scheduled_groups
        && !preserve_unscheduled_groups
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

fn is_retryable_force_find_route_error(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::Timeout | CnxError::TransportClosed(_) | CnxError::ProtocolViolation(_)
    )
}

fn is_retryable_force_find_runner_gap(err: &CnxError) -> bool {
    matches!(err, CnxError::PeerError(message) if message.contains("selected_group matched no group"))
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
    let deadline = tokio::time::Instant::now() + timeout;
    let events = match &state.backend {
        QueryBackend::Route {
            boundary, source, ..
        } => loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(CnxError::Timeout);
            }
            if let Some(node_id) =
                select_force_find_runner_node_for_group(state, source.as_ref(), group_id).await?
            {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: force-find tree via_node begin group={} node={} recursive={} max_depth={:?} strict_conflict={} path={} timeout_ms={}",
                        group_id,
                        node_id.0,
                        recursive,
                        max_depth,
                        strict_conflict,
                        String::from_utf8_lossy(path),
                        remaining.as_millis(),
                    );
                }
                let result = run_timed_query(
                    route_force_find_events_via_node(
                        boundary.clone(),
                        node_id.clone(),
                        request.clone(),
                        remaining,
                    ),
                    remaining,
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
                        break events;
                    }
                    Err(err)
                        if (is_retryable_force_find_route_error(&err)
                            || is_retryable_force_find_runner_gap(&err))
                            && tokio::time::Instant::now() < deadline =>
                    {
                        if debug_force_find_route_capture_enabled() {
                            let action = if is_retryable_force_find_runner_gap(&err) {
                                "reroute"
                            } else {
                                "retry"
                            };
                            eprintln!(
                                "fs_meta_query_api: force-find tree via_node {} group={} node={} err={}",
                                action, group_id, node_id.0, err
                            );
                        }
                        let fallback_remaining =
                            deadline.saturating_duration_since(tokio::time::Instant::now());
                        if fallback_remaining.is_zero() {
                            return Err(err);
                        }
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: force-find tree route_fallback begin group={} recursive={} max_depth={:?} strict_conflict={} path={} timeout_ms={} after_runner_gap=true",
                                group_id,
                                recursive,
                                max_depth,
                                strict_conflict,
                                String::from_utf8_lossy(path),
                                fallback_remaining.as_millis(),
                            );
                        }
                        match query_force_find_events(
                            state.backend.clone(),
                            request.clone(),
                            fallback_remaining,
                        )
                        .await
                        {
                            Ok(events) => {
                                if debug_force_find_route_capture_enabled() {
                                    eprintln!(
                                        "fs_meta_query_api: force-find tree route_fallback done group={} events={} origins={:?} after_runner_gap=true",
                                        group_id,
                                        events.len(),
                                        summarize_event_counts_by_origin(&events)
                                    );
                                }
                                break events;
                            }
                            Err(fallback_err)
                                if (is_retryable_force_find_route_error(&fallback_err)
                                    || is_retryable_force_find_runner_gap(&fallback_err))
                                    && tokio::time::Instant::now() < deadline =>
                            {
                                if debug_force_find_route_capture_enabled() {
                                    eprintln!(
                                        "fs_meta_query_api: force-find tree route_fallback retry group={} err={} after_runner_gap=true",
                                        group_id, fallback_err
                                    );
                                }
                            }
                            Err(fallback_err) => {
                                if debug_force_find_route_capture_enabled() {
                                    eprintln!(
                                        "fs_meta_query_api: force-find tree route_fallback failed group={} err={} after_runner_gap=true",
                                        group_id, fallback_err
                                    );
                                }
                                return Err(fallback_err);
                            }
                        }
                        tokio::time::sleep(FORCE_FIND_ROUTE_RETRY_BACKOFF).await;
                        continue;
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
                        "fs_meta_query_api: force-find tree route_fallback begin group={} recursive={} max_depth={:?} strict_conflict={} path={} timeout_ms={}",
                        group_id,
                        recursive,
                        max_depth,
                        strict_conflict,
                        String::from_utf8_lossy(path),
                        remaining.as_millis(),
                    );
                }
                let result =
                    query_force_find_events(state.backend.clone(), request.clone(), remaining)
                        .await;
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
                        break events;
                    }
                    Err(err)
                        if (is_retryable_force_find_route_error(&err)
                            || is_retryable_force_find_runner_gap(&err))
                            && tokio::time::Instant::now() < deadline =>
                    {
                        if debug_force_find_route_capture_enabled() {
                            let action = if is_retryable_force_find_runner_gap(&err) {
                                "reroute"
                            } else {
                                "retry"
                            };
                            eprintln!(
                                "fs_meta_query_api: force-find tree route_fallback {} group={} err={}",
                                action, group_id, err
                            );
                        }
                        tokio::time::sleep(FORCE_FIND_ROUTE_RETRY_BACKOFF).await;
                        continue;
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
        },
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

async fn build_tree_pit_session(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    timeout: Duration,
    observation_status: ObservationStatus,
    request_sink_status: Option<&SinkStatusSnapshot>,
) -> Result<PitSession, CnxError> {
    eprintln!(
        "fs_meta_query_api: build_tree_pit_session start path={} recursive={}",
        String::from_utf8_lossy(&params.path),
        params.recursive
    );
    let ranking_started_at = tokio::time::Instant::now();
    let target_groups = materialized_target_groups(
        state,
        params.group.as_deref(),
        request_sink_status,
        timeout,
        false,
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
    eprintln!(
        "fs_meta_query_api: build_tree_pit_session rankings groups={}",
        rankings.len()
    );
    let mut group_rank_metrics = rankings
        .iter()
        .map(|rank| (rank.group_key.clone(), rank.rank_metric))
        .collect::<BTreeMap<_, _>>();
    let mut groups = Vec::with_capacity(rankings.len());
    let read_class = tree_read_class(params);
    let session_deadline = if ranking_retryable_failure {
        ranking_started_at + timeout
    } else {
        tokio::time::Instant::now() + timeout
    };
    let total_ranked_groups = rankings.len();
    for (rank_index, rank) in rankings.into_iter().enumerate() {
        let group_key = rank.group_key.clone();
        let tree_params = build_materialized_tree_request(
            &params.path,
            params.recursive,
            params.max_depth,
            read_class,
            Some(group_key.clone()),
        );
        let selected_group_sink_status = if let Some(snapshot) = request_sink_status {
            Some(filter_sink_status_snapshot(
                snapshot.clone(),
                &BTreeSet::from([group_key.clone()]),
            ))
        } else {
            let allowed_groups = BTreeSet::from([group_key.clone()]);
            let cache = state.materialized_sink_status_cache.lock().map_err(|_| {
                CnxError::Internal("materialized sink status cache lock poisoned".into())
            })?;
            cache
                .as_ref()
                .map(|cached| filter_sink_status_snapshot(cached.snapshot.clone(), &allowed_groups))
        };
        let trusted_materialized_ready_group = read_class == ReadClass::TrustedMaterialized
            && selected_group_sink_status_reports_live_materialized_group(
                selected_group_sink_status.as_ref(),
                &group_key,
            );
        let prior_materialized_group_decoded = groups.iter().any(|group: &GroupPitSnapshot| {
            group.status == "ok"
                && group
                    .root
                    .as_ref()
                    .is_some_and(|root| root.exists || root.has_children)
        });
        let is_last_ranked_group = rank_index + 1 == total_ranked_groups;
        let allow_empty_owner_retry =
            trusted_materialized_ready_group && !prior_materialized_group_decoded;
        let selected_group_owner_known = if trusted_materialized_ready_group {
            match &state.backend {
                QueryBackend::Route { source, .. } => materialized_owner_node_for_group(
                    source.as_ref(),
                    selected_group_sink_status.as_ref(),
                    &group_key,
                )
                .await?
                .is_some(),
                QueryBackend::Local { .. } => true,
            }
        } else {
            false
        };
        let remaining = session_deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            if trusted_materialized_ready_group {
                if trusted_materialized_empty_group_root_requires_fail_closed(&params.path) {
                    return Err(trusted_materialized_empty_selected_group_tree_error(
                        &group_key,
                    ));
                }
                return Err(trusted_materialized_unavailable_selected_group_tree_error(
                    &group_key,
                ));
            }
            let response = empty_materialized_tree_group_payload(&params.path);
            groups.push(GroupPitSnapshot {
                group: group_key,
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
        let stage_timeout = if read_class != ReadClass::TrustedMaterialized
            && is_last_ranked_group
            && prior_materialized_group_decoded
        {
            remaining + MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE
        } else if read_class == ReadClass::TrustedMaterialized
            && trusted_materialized_ready_group
            && !prior_materialized_group_decoded
        {
            std::cmp::min(remaining, FIRST_RANKED_TRUSTED_READY_GROUP_STAGE_BUDGET)
        } else if read_class == ReadClass::TrustedMaterialized && prior_materialized_group_decoded {
            std::cmp::min(remaining, LATER_RANKED_TRUSTED_GROUP_STAGE_BUDGET)
        } else if read_class != ReadClass::TrustedMaterialized
            && selected_group_sink_status_is_unready_empty(
                selected_group_sink_status.as_ref(),
                &group_key,
            )
        {
            std::cmp::min(remaining, UNREADY_SELECTED_GROUP_ROUTE_BUDGET)
        } else {
            std::cmp::min(timeout + MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE, remaining)
        };
        if debug_materialized_route_capture_enabled() {
            let sink_group_summary = selected_group_sink_status
                .as_ref()
                .and_then(|snapshot| snapshot.groups.first())
                .map(|group| {
                    format!(
                        "group={} init={} live={} total={} primary={}",
                        group.group_id,
                        group.initial_audit_completed,
                        group.live_nodes,
                        group.total_nodes,
                        group.primary_object_ref
                    )
                })
                .unwrap_or_else(|| "group=<missing>".to_string());
            eprintln!(
                "fs_meta_query_api: pit_group_stage group={} path={} trusted_ready={} prior_decoded={} last_ranked={} stage_timeout_ms={} remaining_ms={} sink_status={}",
                group_key,
                String::from_utf8_lossy(&params.path),
                trusted_materialized_ready_group,
                prior_materialized_group_decoded,
                is_last_ranked_group,
                stage_timeout.as_millis(),
                remaining.as_millis(),
                sink_group_summary
            );
        }
        let stage_deadline = tokio::time::Instant::now() + stage_timeout;
        let events = match run_timed_query(
            query_materialized_events_with_selected_group_owner_snapshot(
                state,
                policy,
                tree_params.clone(),
                stage_timeout,
                selected_group_sink_status.clone(),
                !(read_class == ReadClass::TrustedMaterialized
                    && trusted_materialized_ready_group
                    && !prior_materialized_group_decoded),
                allow_empty_owner_retry,
            ),
            stage_timeout,
        )
        .await
        {
            Ok(events) => events,
            Err(err)
                if matches!(
                    &err,
                    CnxError::Timeout
                        | CnxError::TransportClosed(_)
                        | CnxError::ProtocolViolation(_)
                ) =>
            {
                if trusted_materialized_ready_group {
                    let empty_root_requires_fail_closed =
                        trusted_materialized_empty_group_root_requires_fail_closed(&params.path);
                    let fail_closed_after_retryable_gap =
                        prior_materialized_group_decoded || empty_root_requires_fail_closed;
                    if empty_root_requires_fail_closed
                        && prior_materialized_group_decoded
                        && let outer_owner_retry_timeout = session_deadline
                            .checked_duration_since(tokio::time::Instant::now())
                            .unwrap_or_default()
                            .min(LATER_RANKED_TRUSTED_GROUP_STAGE_BUDGET)
                        && !outer_owner_retry_timeout.is_zero()
                    {
                        if debug_materialized_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: pit_group_stage owner_retry_after_proxy_gap group={} path={} owner_retry_timeout_ms={}",
                                group_key,
                                String::from_utf8_lossy(&params.path),
                                outer_owner_retry_timeout.as_millis()
                            );
                        }
                        if let Ok(retry_events) = run_timed_query(
                            query_materialized_events_via_selected_group_owner_direct(
                                state,
                                tree_params.clone(),
                                outer_owner_retry_timeout,
                                selected_group_sink_status.as_ref(),
                            ),
                            outer_owner_retry_timeout,
                        )
                        .await
                            && let Ok(mut retry_response) =
                                decode_materialized_selected_group_response(
                                    &retry_events,
                                    policy,
                                    &group_key,
                                    &params.path,
                                )
                        {
                            if trusted_materialized_tree_payload_is_empty(&retry_response)
                                && let Some(richer_response) =
                                    decode_richer_same_path_materialized_selected_group_response(
                                        &retry_events,
                                        policy,
                                        &group_key,
                                        &params.path,
                                    )
                            {
                                retry_response = richer_response;
                            }
                            if !trusted_materialized_tree_payload_is_empty(&retry_response) {
                                let (metadata_available, _meta_json) =
                                    tree_metadata_json(read_class);
                                groups.push(GroupPitSnapshot {
                                    group: group_key,
                                    status: "ok",
                                    reliable: retry_response.reliability.reliable,
                                    unreliable_reason: retry_response.reliability.unreliable_reason,
                                    stability: retry_response.stability,
                                    meta: PitMetadata {
                                        read_class,
                                        metadata_available,
                                        withheld_reason: None,
                                    },
                                    root: metadata_available.then_some(retry_response.root),
                                    entries: if metadata_available {
                                        retry_response.entries
                                    } else {
                                        Vec::new()
                                    },
                                    errors: Vec::new(),
                                });
                                if let Some(last) = groups.last() {
                                    if let Some(observed_rank) =
                                        observed_rank_metric_from_tree_snapshot(
                                            params.group_order,
                                            last,
                                        )
                                    {
                                        let entry = group_rank_metrics
                                            .entry(last.group.clone())
                                            .or_insert(None);
                                        *entry = Some(entry.unwrap_or(0).max(observed_rank));
                                    }
                                }
                                continue;
                            }
                        }
                    }
                    if let QueryBackend::Route {
                        boundary,
                        origin_id,
                        ..
                    } = &state.backend
                    {
                        let remaining = if !empty_root_requires_fail_closed {
                            session_deadline
                                .checked_duration_since(tokio::time::Instant::now())
                                .unwrap_or_default()
                                + MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE
                        } else {
                            stage_deadline
                                .checked_duration_since(tokio::time::Instant::now())
                                .unwrap_or_default()
                        };
                        if !remaining.is_zero() {
                            let proxy_retry_timeout =
                                std::cmp::min(remaining, SELECTED_GROUP_PROXY_FALLBACK_MIN_BUDGET);
                            if debug_materialized_route_capture_enabled() {
                                eprintln!(
                                    "fs_meta_query_api: pit_group_stage proxy_fallback_after_owner_timeout group={} path={} proxy_timeout_ms={} remaining_ms={}",
                                    group_key,
                                    String::from_utf8_lossy(&params.path),
                                    proxy_retry_timeout.as_millis(),
                                    remaining.as_millis()
                                );
                            }
                            if !proxy_retry_timeout.is_zero() {
                                match run_timed_query(
                                    query_materialized_events_via_generic_proxy(
                                        boundary.clone(),
                                        origin_id.clone(),
                                        tree_params.clone(),
                                        proxy_retry_timeout,
                                    ),
                                    proxy_retry_timeout,
                                )
                                .await
                                {
                                    Ok(proxy_events) => {
                                        match decode_materialized_selected_group_response(
                                            &proxy_events,
                                            policy,
                                            &group_key,
                                            &params.path,
                                        ) {
                                            Ok(mut proxy_response) => {
                                                if trusted_materialized_tree_payload_is_empty(
                                                    &proxy_response,
                                                ) && let Some(richer_response) =
                                                    decode_richer_same_path_materialized_selected_group_response(
                                                        &proxy_events,
                                                        policy,
                                                        &group_key,
                                                        &params.path,
                                                    )
                                                {
                                                    proxy_response = richer_response;
                                                }
                                                if debug_materialized_route_capture_enabled() {
                                                    eprintln!(
                                                        "fs_meta_query_api: pit_group_stage proxy_fallback_decode group={} root_exists={} entries={} has_children={}",
                                                        group_key,
                                                        proxy_response.root.exists,
                                                        proxy_response.entries.len(),
                                                        proxy_response.root.has_children
                                                    );
                                                }
                                                if !empty_root_requires_fail_closed
                                                    || !trusted_materialized_tree_payload_is_empty(
                                                        &proxy_response,
                                                    )
                                                {
                                                    let (metadata_available, _meta_json) =
                                                        tree_metadata_json(read_class);
                                                    groups.push(GroupPitSnapshot {
                                                        group: group_key,
                                                        status: "ok",
                                                        reliable: proxy_response
                                                            .reliability
                                                            .reliable,
                                                        unreliable_reason: proxy_response
                                                            .reliability
                                                            .unreliable_reason,
                                                        stability: proxy_response.stability,
                                                        meta: PitMetadata {
                                                            read_class,
                                                            metadata_available,
                                                            withheld_reason: None,
                                                        },
                                                        root: metadata_available
                                                            .then_some(proxy_response.root),
                                                        entries: if metadata_available {
                                                            proxy_response.entries
                                                        } else {
                                                            Vec::new()
                                                        },
                                                        errors: Vec::new(),
                                                    });
                                                    if let Some(last) = groups.last() {
                                                        if let Some(observed_rank) =
                                                            observed_rank_metric_from_tree_snapshot(
                                                                params.group_order,
                                                                last,
                                                            )
                                                        {
                                                            let entry = group_rank_metrics
                                                                .entry(last.group.clone())
                                                                .or_insert(None);
                                                            *entry = Some(
                                                                entry
                                                                    .unwrap_or(0)
                                                                    .max(observed_rank),
                                                            );
                                                        }
                                                    }
                                                    continue;
                                                }
                                            }
                                            Err(err) if fail_closed_after_retryable_gap => {
                                                if debug_materialized_route_capture_enabled() {
                                                    eprintln!(
                                                        "fs_meta_query_api: pit_group_stage proxy_fallback_decode_failed group={} path={} err={}",
                                                        group_key,
                                                        String::from_utf8_lossy(&params.path),
                                                        err
                                                    );
                                                }
                                                return Err(
                                                    trusted_materialized_unavailable_selected_group_tree_error(
                                                        &group_key,
                                                    ),
                                                );
                                            }
                                            Err(_) => {}
                                        }
                                    }
                                    Err(CnxError::Timeout)
                                    | Err(CnxError::TransportClosed(_))
                                    | Err(CnxError::ProtocolViolation(_))
                                        if fail_closed_after_retryable_gap =>
                                    {
                                        if debug_materialized_route_capture_enabled() {
                                            eprintln!(
                                                "fs_meta_query_api: pit_group_stage proxy_fallback_failed group={} path={} err=retryable-proxy-failure",
                                                group_key,
                                                String::from_utf8_lossy(&params.path),
                                            );
                                        }
                                        return Err(
                                            trusted_materialized_unavailable_selected_group_tree_error(
                                                &group_key,
                                            ),
                                        );
                                    }
                                    Err(_) => {}
                                }
                            }
                        }
                    }
                    if fail_closed_after_retryable_gap {
                        if debug_materialized_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_query_api: pit_group_stage fail_closed_retryable_gap group={} path={} err={}",
                                group_key,
                                String::from_utf8_lossy(&params.path),
                                err
                            );
                        }
                        return Err(trusted_materialized_unavailable_selected_group_tree_error(
                            &group_key,
                        ));
                    }
                    if debug_materialized_route_capture_enabled() {
                        eprintln!(
                            "fs_meta_query_api: pit_group_stage fail_closed group={} path={} err={}",
                            group_key,
                            String::from_utf8_lossy(&params.path),
                            err
                        );
                    }
                    return Err(CnxError::Timeout);
                }
                let response = empty_materialized_tree_group_payload(&params.path);
                groups.push(GroupPitSnapshot {
                    group: group_key,
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
            Err(err) => {
                if debug_materialized_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_query_api: pit_group_stage fatal group={} path={} err={}",
                        group_key,
                        String::from_utf8_lossy(&params.path),
                        err
                    );
                }
                return Err(err);
            }
        };
        match decode_materialized_selected_group_response(&events, policy, &group_key, &params.path)
        {
            Ok(mut response) => {
                if trusted_materialized_ready_group
                    && trusted_materialized_tree_payload_is_empty(&response)
                    && let Some(richer_response) =
                        decode_richer_same_path_materialized_selected_group_response(
                            &events,
                            policy,
                            &group_key,
                            &params.path,
                        )
                {
                    response = richer_response;
                }
                if trusted_materialized_ready_group
                    && trusted_materialized_tree_payload_is_empty(&response)
                {
                    let empty_root_requires_fail_closed =
                        trusted_materialized_empty_group_root_requires_fail_closed(&params.path);
                    let settle_after_initial_empty_retry_path =
                        allow_empty_owner_retry && !empty_root_requires_fail_closed;
                    if !settle_after_initial_empty_retry_path {
                        let remaining = session_deadline
                            .checked_duration_since(tokio::time::Instant::now())
                            .unwrap_or_default();
                        if !remaining.is_zero() && selected_group_owner_known {
                            let owner_retry_timeout = if empty_root_requires_fail_closed
                                && prior_materialized_group_decoded
                            {
                                remaining
                            } else if prior_materialized_group_decoded {
                                std::cmp::min(
                                    TRUSTED_READY_LATER_RANKED_NON_ROOT_RETRY_BUDGET,
                                    remaining,
                                )
                            } else {
                                std::cmp::min(TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET, remaining)
                            };
                            if !owner_retry_timeout.is_zero() {
                                match run_timed_query(
                                    query_materialized_events_via_selected_group_owner_direct(
                                        state,
                                        tree_params.clone(),
                                        owner_retry_timeout,
                                        selected_group_sink_status.as_ref(),
                                    ),
                                    owner_retry_timeout,
                                )
                                .await
                                {
                                    Ok(retry_events) => {
                                        if let Ok(mut retry_response) =
                                            decode_materialized_selected_group_response(
                                                &retry_events,
                                                policy,
                                                &group_key,
                                                &params.path,
                                            )
                                        {
                                            if trusted_materialized_tree_payload_is_empty(
                                                &retry_response,
                                            ) && let Some(richer_response) =
                                                decode_richer_same_path_materialized_selected_group_response(
                                                    &retry_events,
                                                    policy,
                                                    &group_key,
                                                    &params.path,
                                                )
                                            {
                                                retry_response = richer_response;
                                            }
                                            if !trusted_materialized_tree_payload_is_empty(
                                                &retry_response,
                                            ) {
                                                response = retry_response;
                                            }
                                        }
                                    }
                                    Err(CnxError::Timeout)
                                    | Err(CnxError::TransportClosed(_))
                                    | Err(CnxError::ProtocolViolation(_))
                                        if empty_root_requires_fail_closed =>
                                    {
                                        return Err(
                                            trusted_materialized_empty_selected_group_tree_error(
                                                &group_key,
                                            ),
                                        );
                                    }
                                    Err(CnxError::Timeout)
                                    | Err(CnxError::TransportClosed(_))
                                    | Err(CnxError::ProtocolViolation(_)) => {}
                                    Err(err) => return Err(err),
                                }
                            }
                            if selected_group_owner_known
                                && !empty_root_requires_fail_closed
                                && trusted_materialized_tree_payload_is_empty(&response)
                                && let QueryBackend::Route {
                                    boundary,
                                    origin_id,
                                    ..
                                } = &state.backend
                            {
                                let remaining = session_deadline
                                    .checked_duration_since(tokio::time::Instant::now())
                                    .unwrap_or_default();
                                if !remaining.is_zero() {
                                    let proxy_retry_timeout = if is_last_ranked_group {
                                        std::cmp::min(
                                            remaining,
                                            SELECTED_GROUP_PROXY_FALLBACK_MIN_BUDGET,
                                        )
                                    } else {
                                        let shared_budget_ceiling = remaining
                                            .checked_div(
                                                total_ranked_groups.saturating_sub(rank_index)
                                                    as u32,
                                            )
                                            .unwrap_or_default();
                                        std::cmp::min(
                                            std::cmp::min(
                                                remaining,
                                                SELECTED_GROUP_PROXY_FALLBACK_MIN_BUDGET,
                                            ),
                                            shared_budget_ceiling,
                                        )
                                    };
                                    let fail_closed_after_proxy_error =
                                        trusted_materialized_ready_group
                                            && prior_materialized_group_decoded
                                            && trusted_materialized_tree_payload_is_empty(
                                                &response,
                                            );
                                    match run_timed_query(
                                        query_materialized_events_via_generic_proxy(
                                            boundary.clone(),
                                            origin_id.clone(),
                                            tree_params.clone(),
                                            proxy_retry_timeout,
                                        ),
                                        proxy_retry_timeout,
                                    )
                                    .await
                                    {
                                        Ok(proxy_events) => {
                                            match decode_materialized_selected_group_response(
                                                &proxy_events,
                                                policy,
                                                &group_key,
                                                &params.path,
                                            ) {
                                                Ok(mut proxy_response) => {
                                                    if trusted_materialized_tree_payload_is_empty(
                                                        &proxy_response,
                                                    ) && let Some(richer_response) =
                                                        decode_richer_same_path_materialized_selected_group_response(
                                                            &proxy_events,
                                                            policy,
                                                            &group_key,
                                                            &params.path,
                                                        )
                                                    {
                                                        proxy_response = richer_response;
                                                    }
                                                    if !trusted_materialized_tree_payload_is_empty(
                                                        &proxy_response,
                                                    ) {
                                                        response = proxy_response;
                                                    }
                                                }
                                                Err(err) if fail_closed_after_proxy_error => {
                                                    if debug_materialized_route_capture_enabled() {
                                                        eprintln!(
                                                            "fs_meta_query_api: pit_group_stage proxy_fallback_decode_failed group={} path={} err={}",
                                                            group_key,
                                                            String::from_utf8_lossy(&params.path),
                                                            err
                                                        );
                                                    }
                                                    return Err(
                                                        trusted_materialized_unavailable_selected_group_tree_error(
                                                            &group_key,
                                                        ),
                                                    );
                                                }
                                                Err(_) => {}
                                            }
                                        }
                                        Err(CnxError::Timeout)
                                        | Err(CnxError::TransportClosed(_))
                                        | Err(CnxError::ProtocolViolation(_))
                                            if fail_closed_after_proxy_error =>
                                        {
                                            if debug_materialized_route_capture_enabled() {
                                                eprintln!(
                                                    "fs_meta_query_api: pit_group_stage proxy_fallback_failed group={} path={} err=retryable-proxy-failure",
                                                    group_key,
                                                    String::from_utf8_lossy(&params.path),
                                                );
                                            }
                                            return Err(
                                                trusted_materialized_unavailable_selected_group_tree_error(
                                                    &group_key,
                                                ),
                                            );
                                        }
                                        Err(_) => {}
                                    }
                                }
                            }
                        }
                    }
                }
                if trusted_materialized_ready_group
                    && trusted_materialized_empty_group_root_requires_fail_closed(&params.path)
                    && trusted_materialized_tree_payload_is_empty(&response)
                {
                    return Err(trusted_materialized_empty_selected_group_tree_error(
                        &group_key,
                    ));
                }
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
                    if let Some(observed_rank) =
                        observed_rank_metric_from_tree_snapshot(params.group_order, last)
                    {
                        let entry = group_rank_metrics.entry(last.group.clone()).or_insert(None);
                        *entry = Some(entry.unwrap_or(0).max(observed_rank));
                    }
                }
            }
            Err(err) => groups.push(GroupPitSnapshot {
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
            }),
        }
    }
    if matches!(
        params.group_order,
        GroupOrder::FileCount | GroupOrder::FileAge
    ) {
        groups.sort_by(|left, right| {
            let left_rank = group_rank_metrics
                .get(&left.group)
                .copied()
                .flatten()
                .or_else(|| observed_rank_metric_from_tree_snapshot(params.group_order, left))
                .unwrap_or(0);
            let right_rank = group_rank_metrics
                .get(&right.group)
                .copied()
                .flatten()
                .or_else(|| observed_rank_metric_from_tree_snapshot(params.group_order, right))
                .unwrap_or(0);
            right_rank
                .cmp(&left_rank)
                .then_with(|| left.group.cmp(&right.group))
        });
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
            None,
            None,
            params.group_order,
            &target_groups,
            params.group.as_deref(),
        )
        .await?
        .0
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
    request_sink_status: Option<SinkStatusSnapshot>,
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
        build_tree_pit_session(
            state,
            policy,
            params,
            timeout,
            observation_status,
            request_sink_status.as_ref(),
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

async fn collect_materialized_stats_groups(
    state: &ApiState,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    read_class: ReadClass,
    request_sink_status: Option<&SinkStatusSnapshot>,
) -> Result<serde_json::Map<String, serde_json::Value>, CnxError> {
    let mut groups = serde_json::Map::<String, serde_json::Value>::new();
    let target_groups = match read_class {
        ReadClass::Fresh => resolve_force_find_groups(state, params).await?,
        ReadClass::Materialized | ReadClass::TrustedMaterialized => {
            materialized_target_groups(
                state,
                params.group.as_deref(),
                request_sink_status,
                policy.query_timeout(),
                params.group.is_none() && !params.path.is_empty() && params.path != b"/",
            )
            .await?
        }
    };
    for group_id in target_groups {
        let materialized_request = build_materialized_stats_request(
            &params.path,
            params.recursive,
            Some(group_id.clone()),
        );
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
                if let Some(snapshot) = request_sink_status {
                    query_materialized_events_with_selected_group_owner_snapshot(
                        state,
                        policy,
                        materialized_request.clone(),
                        policy.query_timeout(),
                        Some(snapshot.clone()),
                        true,
                        false,
                    )
                    .await?
                } else {
                    query_materialized_events(
                        state.backend.clone(),
                        materialized_request.clone(),
                        policy.query_timeout(),
                    )
                    .await?
                }
            }
        };
        let mut targeted = decode_stats_groups(events, policy, Some(&group_id), read_class);
        if read_class == ReadClass::TrustedMaterialized
            && selected_group_sink_status_reports_live_materialized_group(
                request_sink_status,
                &group_id,
            )
            && targeted
                .get(&group_id)
                .is_some_and(stats_group_looks_zeroish)
            && let QueryBackend::Route {
                boundary,
                origin_id,
                ..
            } = &state.backend
        {
            match query_materialized_events_via_generic_proxy(
                boundary.clone(),
                origin_id.clone(),
                materialized_request.clone(),
                policy.query_timeout(),
            )
            .await
            {
                Ok(proxy_events) => {
                    let proxy_targeted =
                        decode_stats_groups(proxy_events, policy, Some(&group_id), read_class);
                    let current_metrics = targeted
                        .get(&group_id)
                        .and_then(stats_group_value_metrics)
                        .unwrap_or((0, 0, 0, 0, 0));
                    let proxy_metrics = proxy_targeted
                        .get(&group_id)
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
        }
        if read_class == ReadClass::TrustedMaterialized
            && (params.path.is_empty() || params.path == b"/")
            && selected_group_sink_status_reports_live_materialized_group(
                request_sink_status,
                &group_id,
            )
            && targeted
                .get(&group_id)
                .and_then(|group| group.get("data"))
                .and_then(|data| data.get("total_nodes"))
                .and_then(|value| value.as_u64())
                == Some(0)
        {
            return Err(trusted_materialized_unavailable_selected_group_stats_error(
                &group_id,
            ));
        }
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
            request_sink_status = Some(sink_status.clone());
            status
        }
    };
    let groups = match collect_materialized_stats_groups(
        &state,
        &policy,
        &params,
        read_class,
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
        Some(sink_status),
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
