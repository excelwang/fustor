use crate::query::models::SubtreeStats;
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
    MetadataMode, PageOrder, StabilityState, TreeGroupPayload, TreePageEntry, TreePageRoot,
    TreeStability,
};
use crate::sink::SinkStatusSnapshot;
use crate::source::SourceStatusSnapshot;
use crate::source::config::GrantedMountRoot;
use crate::runtime::routes::{
    METHOD_SINK_QUERY_PROXY, METHOD_SINK_STATUS, METHOD_SOURCE_FIND, METHOD_SOURCE_STATUS,
    ROUTE_TOKEN_FS_META_INTERNAL, default_route_bindings,
};
use crate::runtime::seam::exchange_host_adapter;
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
use capanix_host_adapter_fs_meta::HostAdapter;
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_host_fs_types::query::StabilityMode;
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
// Force-find is intentionally a slower freshness path. Keep a larger collection
// window here so the request remains in-flight long enough for overlap guards
// and multi-runner aggregation scenarios.
const FORCE_FIND_ROUTE_COLLECT_IDLE_GRACE: Duration = Duration::from_secs(5);
const FORCE_FIND_MIN_INFLIGHT_HOLD: Duration = Duration::from_secs(2);

#[derive(Deserialize)]
pub struct ApiParams {
    pub path: Option<String>,
    pub path_b64: Option<String>,
    pub recursive: Option<bool>,
    pub max_depth: Option<usize>,
    pub pit_id: Option<String>,
    pub group_order: Option<GroupOrder>,
    pub group_page_size: Option<usize>,
    pub group_after: Option<String>,
    pub entry_page_size: Option<usize>,
    pub entry_after: Option<String>,
    pub stability_mode: Option<StabilityMode>,
    pub quiet_window_ms: Option<u64>,
    pub metadata_mode: Option<MetadataMode>,
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
    InProcess {
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
    readiness_source: Option<Arc<SourceFacade>>,
    readiness_sink: Option<Arc<SinkFacade>>,
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
                self.groups,
                inflight
            );
        }
    }
}

#[derive(Clone, Debug)]
struct TreePitScope {
    path: Vec<u8>,
    recursive: bool,
    max_depth: Option<usize>,
    group_order: GroupOrder,
    stability_mode: StabilityMode,
    quiet_window_ms: Option<u64>,
    metadata_mode: MetadataMode,
}

#[derive(Clone, Debug)]
struct ForceFindPitScope {
    path: Vec<u8>,
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
    metadata_mode: MetadataMode,
    metadata_available: bool,
    withheld_reason: Option<&'static str>,
}

#[derive(Clone, Debug)]
struct GroupPitSnapshot {
    group: String,
    status: &'static str,
    reliable: bool,
    unreliable_reason: Option<capanix_host_fs_types::query::UnreliableReason>,
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
    groups: Vec<GroupPitSnapshot>,
    expires_at_ms: u64,
    estimated_bytes: usize,
}

#[derive(Default)]
struct QueryPitStore {
    sessions: HashMap<String, Arc<PitSession>>,
    total_bytes: usize,
}

#[derive(Clone, Copy)]
enum QueryMode {
    Find,
    ForceFind,
}

fn materialized_query_readiness_error(
    source_status: &SourceStatusSnapshot,
    sink_status: &SinkStatusSnapshot,
) -> Option<String> {
    let degraded_groups = source_status
        .degraded_roots
        .iter()
        .map(|(root_id, _)| root_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let sink_groups = sink_status
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut pending_initial_audit = Vec::<String>::new();
    let mut overflow_pending = Vec::<String>::new();
    let mut degraded = Vec::<String>::new();

    for root in source_status
        .concrete_roots
        .iter()
        .filter(|root| root.active && root.is_group_primary && root.scan_enabled)
    {
        let group_id = root.logical_root_id.as_str();
        if degraded_groups.contains(group_id) {
            degraded.push(group_id.to_string());
            continue;
        }
        let Some(group) = sink_groups.get(group_id) else {
            pending_initial_audit.push(group_id.to_string());
            continue;
        };
        if !group.initial_audit_completed {
            pending_initial_audit.push(group_id.to_string());
        }
        if group.overflow_pending_audit {
            overflow_pending.push(group_id.to_string());
        }
    }

    if degraded.is_empty() && overflow_pending.is_empty() && pending_initial_audit.is_empty() {
        return None;
    }

    let mut reasons = Vec::new();
    if !pending_initial_audit.is_empty() {
        reasons.push(format!(
            "initial audit incomplete for groups [{}]",
            pending_initial_audit.join(", ")
        ));
    }
    if !overflow_pending.is_empty() {
        reasons.push(format!(
            "overflow audit pending for groups [{}]",
            overflow_pending.join(", ")
        ));
    }
    if !degraded.is_empty() {
        reasons.push(format!(
            "degraded coverage for groups [{}]",
            degraded.join(", ")
        ));
    }

    Some(format!(
        "materialized /tree and /stats remain unavailable until initial audit catch-up completes: {}",
        reasons.join("; ")
    ))
}

fn materialized_query_source_gate_error(source_status: &SourceStatusSnapshot) -> Option<String> {
    let degraded_groups = source_status
        .degraded_roots
        .iter()
        .map(|(root_id, _)| root_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let degraded_primary_scan_groups = source_status
        .concrete_roots
        .iter()
        .filter(|root| root.active && root.is_group_primary && root.scan_enabled)
        .filter_map(|root| {
            degraded_groups
                .contains(root.logical_root_id.as_str())
                .then(|| root.logical_root_id.clone())
        })
        .collect::<Vec<_>>();
    if degraded_primary_scan_groups.is_empty() {
        return None;
    }
    Some(format!(
        "materialized /tree and /stats remain unavailable while source coverage is degraded for groups [{}]",
        degraded_primary_scan_groups.join(", ")
    ))
}

fn merge_sink_status_snapshots(mut snapshots: Vec<SinkStatusSnapshot>) -> SinkStatusSnapshot {
    if snapshots.is_empty() {
        return SinkStatusSnapshot::default();
    }
    if snapshots.len() == 1 {
        return snapshots.pop().unwrap_or_default();
    }

    let mut groups = BTreeMap::<String, crate::sink::SinkGroupStatusSnapshot>::new();
    for snapshot in snapshots {
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

pub(crate) async fn route_sink_status_snapshot(
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    timeout: Duration,
) -> Result<SinkStatusSnapshot, CnxError> {
    let adapter = exchange_host_adapter(boundary, origin_id, default_route_bindings());
    let events = run_blocking_query(
        move || {
            adapter.call_collect(
                ROUTE_TOKEN_FS_META_INTERNAL,
                METHOD_SINK_STATUS,
                Bytes::new(),
                timeout,
                MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE,
            )
        },
        timeout,
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
    Ok(merge_sink_status_snapshots(snapshots))
}

async fn ensure_materialized_queries_ready(state: &ApiState) -> Result<(), CnxError> {
    let (Some(source), Some(sink)) = (&state.readiness_source, &state.readiness_sink) else {
        return Ok(());
    };
    eprintln!("fs_meta_query_api: ensure_materialized_queries_ready begin");
    let source_status = source.status_snapshot()?;
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
            Ok(snapshot) => {
                eprintln!(
                    "fs_meta_query_api: readiness route sink-status ok groups={}",
                    snapshot.groups.len()
                );
                Some(snapshot)
            }
            Err(CnxError::Timeout)
            | Err(CnxError::TransportClosed(_))
            | Err(CnxError::ProtocolViolation(_)) => {
                eprintln!(
                    "fs_meta_query_api: readiness route sink-status unavailable; falling back to source-only gate"
                );
                None
            }
            Err(err) => {
                eprintln!("fs_meta_query_api: readiness route sink-status failed err={err}");
                return Err(err);
            }
        },
        QueryBackend::InProcess { .. } => {
            let snapshot = sink.status_snapshot()?;
            eprintln!(
                "fs_meta_query_api: readiness inprocess sink-status ok groups={}",
                snapshot.groups.len()
            );
            Some(snapshot)
        }
    };
    match sink_status {
        Some(sink_status) => {
            if let Some(message) = materialized_query_readiness_error(&source_status, &sink_status)
            {
                eprintln!("fs_meta_query_api: readiness not ready message={message}");
                return Err(CnxError::NotReady(message));
            }
        }
        None => {
            if let Some(message) = materialized_query_source_gate_error(&source_status) {
                eprintln!(
                    "fs_meta_query_api: readiness source-only gate not ready message={message}"
                );
                return Err(CnxError::NotReady(message));
            }
        }
    }
    eprintln!("fs_meta_query_api: ensure_materialized_queries_ready ok");
    Ok(())
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
    recursive: bool,
    max_depth: Option<usize>,
    pit_id: Option<String>,
    group_order: GroupOrder,
    group_page_size: Option<usize>,
    group_after: Option<String>,
    entry_page_size: Option<usize>,
    entry_after: Option<String>,
    stability_mode: StabilityMode,
    quiet_window_ms: Option<u64>,
    metadata_mode: MetadataMode,
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
        recursive: params.recursive.unwrap_or(true),
        max_depth: params.max_depth,
        pit_id: params.pit_id,
        group_order: params.group_order.unwrap_or(GroupOrder::GroupKey),
        group_page_size: params.group_page_size,
        group_after: params.group_after,
        entry_page_size: params.entry_page_size,
        entry_after: params.entry_after,
        stability_mode: params.stability_mode.unwrap_or(StabilityMode::None),
        quiet_window_ms: params.quiet_window_ms,
        metadata_mode: params.metadata_mode.unwrap_or(MetadataMode::Full),
    })
}

fn validate_tree_query_params(params: &NormalizedApiParams) -> std::result::Result<(), CnxError> {
    match params.stability_mode {
        StabilityMode::None => {
            if params.quiet_window_ms.is_some() {
                return Err(CnxError::InvalidInput(
                    "quiet_window_ms is only valid when stability_mode=quiet-window".into(),
                ));
            }
        }
        StabilityMode::QuietWindow => {
            if params.quiet_window_ms.is_none() {
                return Err(CnxError::InvalidInput(
                    "quiet_window_ms is required when stability_mode=quiet-window".into(),
                ));
            }
        }
    }
    if params.metadata_mode == MetadataMode::StatusOnly {
        if params.entry_page_size.is_some() {
            return Err(CnxError::InvalidInput(
                "entry_page_size is invalid when metadata_mode=status-only".into(),
            ));
        }
        if params.entry_after.is_some() {
            return Err(CnxError::InvalidInput(
                "entry_after is invalid when metadata_mode=status-only".into(),
            ));
        }
    }
    if params.pit_id.is_none() && (params.group_after.is_some() || params.entry_after.is_some()) {
        return Err(CnxError::InvalidInput(
            "pit_id is required when using group_after or entry_after".into(),
        ));
    }
    Ok(())
}

fn validate_force_find_params(params: &NormalizedApiParams) -> std::result::Result<(), CnxError> {
    if params.stability_mode != StabilityMode::None {
        return Err(CnxError::InvalidInput(
            "stability_mode must be none on /on-demand-force-find".into(),
        ));
    }
    if params.quiet_window_ms.is_some() {
        return Err(CnxError::InvalidInput(
            "quiet_window_ms is invalid on /on-demand-force-find".into(),
        ));
    }
    if params.metadata_mode != MetadataMode::Full {
        return Err(CnxError::InvalidInput(
            "metadata_mode must be full on /on-demand-force-find".into(),
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
    if params.metadata_mode == MetadataMode::StatusOnly {
        return Ok(Some(0));
    }
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

fn tree_metadata_json(
    params: &NormalizedApiParams,
    stability: &TreeStability,
) -> (bool, serde_json::Value) {
    let metadata_available = match params.metadata_mode {
        MetadataMode::Full => true,
        MetadataMode::StatusOnly => false,
        MetadataMode::StableOnly => stability.state == StabilityState::Stable,
    };
    let meta = if metadata_available {
        serde_json::json!({
            "metadata_mode": params.metadata_mode,
            "metadata_available": true,
        })
    } else {
        serde_json::json!({
            "metadata_mode": params.metadata_mode,
            "metadata_available": false,
            "withheld_reason": match params.metadata_mode {
                MetadataMode::StatusOnly => "status-only",
                MetadataMode::StableOnly => "stability-not-stable",
                MetadataMode::Full => "metadata-unavailable",
            },
        })
    };
    (metadata_available, meta)
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
    stability_mode: StabilityMode,
    quiet_window_ms: Option<u64>,
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
        Some(TreeQueryOptions {
            stability_mode,
            quiet_window_ms,
        }),
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

pub fn create_inprocess_router(
    sink: Arc<SinkFacade>,
    source: Arc<SourceFacade>,
    runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    origin_id: NodeId,
    policy: Arc<RwLock<ProjectionPolicy>>,
    force_find_inflight: Arc<Mutex<BTreeSet<String>>>,
) -> Router {
    eprintln!(
        "fs_meta_query_api: create router backend={}",
        if runtime_boundary.is_some() { "route" } else { "inprocess" }
    );
    let state = ApiState {
        backend: runtime_boundary.map_or_else(
            || QueryBackend::InProcess {
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
        readiness_source: Some(source),
        readiness_sink: Some(sink),
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

async fn run_blocking_query<F>(f: F, timeout: Duration) -> Result<Vec<Event>, CnxError>
where
    F: FnOnce() -> Result<Vec<Event>, CnxError> + Send + 'static,
{
    let handle = tokio::task::spawn_blocking(f);
    let abort_handle = handle.abort_handle();
    match tokio::time::timeout(timeout, handle).await {
        Ok(joined) => {
            joined.map_err(|e| CnxError::Internal(format!("blocking query task failed: {e}")))?
        }
        Err(_) => {
            abort_handle.abort();
            Err(CnxError::Timeout)
        }
    }
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
        QueryBackend::InProcess { sink, .. } => {
            let result = run_blocking_query(move || sink.materialized_query(&params), timeout).await;
            eprintln!("fs_meta_query_api: query_materialized_events inprocess done ok={}", result.is_ok());
            result
        }
        QueryBackend::Route {
            boundary,
            origin_id,
            ..
        } => {
            let adapter = exchange_host_adapter(boundary, origin_id, default_route_bindings());
            let payload = rmp_serde::to_vec(&params).map_err(|err| {
                CnxError::Internal(format!("encode materialized query failed: {err}"))
            })?;
            eprintln!("fs_meta_query_api: query_materialized_events route call_collect begin");
            let events = run_blocking_query(
                move || {
                    adapter.call_collect(
                        ROUTE_TOKEN_FS_META_INTERNAL,
                        METHOD_SINK_QUERY_PROXY,
                        Bytes::from(payload),
                        timeout,
                        MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE,
                    )
                },
                timeout,
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
        QueryBackend::InProcess { source, .. } => {
            let result = run_blocking_query(move || source.force_find(&params), timeout).await;
            eprintln!(
                "fs_meta_query_api: query_force_find_events inprocess done ok={}",
                result.is_ok()
            );
            result
        }
        QueryBackend::Route { boundary, origin_id, .. } => {
            let adapter = exchange_host_adapter(boundary, origin_id, default_route_bindings());
            let payload = rmp_serde::to_vec_named(&params).map_err(|err| {
                CnxError::Internal(format!("encode force-find query failed: {err}"))
            })?;
            eprintln!("fs_meta_query_api: query_force_find_events route call_collect begin");
            let events = run_blocking_query(
                move || {
                    adapter.call_collect(
                        ROUTE_TOKEN_FS_META_INTERNAL,
                        METHOD_SOURCE_FIND,
                        Bytes::from(payload),
                        timeout,
                        FORCE_FIND_ROUTE_COLLECT_IDLE_GRACE,
                    )
                },
                timeout,
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

fn selected_group_matches(
    policy: &ProjectionPolicy,
    selected_group: Option<&str>,
    event: &Event,
) -> bool {
    selected_group.is_none_or(|selected| selected == event_group_key(policy, event))
}

async fn query_tree_events_for_all_groups(
    backend: QueryBackend,
    policy: &ProjectionPolicy,
    mode: QueryMode,
    path: &[u8],
    recursive: bool,
    max_depth: Option<usize>,
    stability_mode: StabilityMode,
    quiet_window_ms: Option<u64>,
    timeout: Duration,
    selected_group: Option<&str>,
) -> Result<Vec<Event>, CnxError> {
    let events = match mode {
        QueryMode::Find => {
            let request = build_materialized_tree_request(
                path,
                recursive,
                max_depth,
                stability_mode,
                quiet_window_ms,
                selected_group.map(str::to_string),
            );
            query_materialized_events(backend, request, timeout).await?
        }
        QueryMode::ForceFind => {
            let request = build_force_find_tree_request(
                path,
                recursive,
                max_depth,
                selected_group.map(str::to_string),
            );
            query_force_find_events(backend, request, timeout).await?
        }
    };
    Ok(events
        .into_iter()
        .filter(|event| selected_group_matches(policy, selected_group, event))
        .collect())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GroupRank {
    group_key: String,
    rank_metric: Option<u64>,
}

async fn collect_group_rankings(
    backend: QueryBackend,
    policy: &ProjectionPolicy,
    mode: QueryMode,
    path: &[u8],
    recursive: bool,
    timeout: Duration,
    group_order: GroupOrder,
    expected_groups: &[String],
) -> Result<Vec<GroupRank>, CnxError> {
    #[derive(Default)]
    struct GroupBestState {
        latest_file_mtime_us: Option<u64>,
        per_origin_total_files: BTreeMap<String, u64>,
        saw_success: bool,
    }

    let stats_events = match mode {
        QueryMode::Find => {
            let request = build_materialized_stats_request(path, recursive, None);
            query_materialized_events(backend.clone(), request, timeout).await?
        }
        QueryMode::ForceFind => {
            let request = build_force_find_stats_request(path, recursive, None);
            query_force_find_events(backend.clone(), request, timeout).await?
        }
    };
    let mut grouped = BTreeMap::<String, GroupBestState>::new();
    for event in stats_events {
        let stats = match mode {
            QueryMode::Find => decode_materialized_stats_payload(event.payload_bytes()),
            QueryMode::ForceFind => decode_force_find_stats_payload(event.payload_bytes()),
        };
        let Ok(stats) = stats else {
            continue;
        };
        let group_key = event_group_key(policy, &event);
        let origin = event.metadata().origin_id.0.clone();
        let state = grouped.entry(group_key).or_default();
        state.saw_success = true;
        state
            .per_origin_total_files
            .entry(origin.clone())
            .and_modify(|value| *value = (*value).max(stats.total_files))
            .or_insert(stats.total_files);
        if let Some(latest_mtime) = stats.latest_file_mtime_us {
            state.latest_file_mtime_us = Some(
                state
                    .latest_file_mtime_us
                    .map_or(latest_mtime, |value| value.max(latest_mtime)),
            );
        }
    }

    let mut candidates = Vec::<GroupRank>::new();
    for (group_key, state) in grouped {
        if !state.saw_success {
            continue;
        }
        match group_order {
            GroupOrder::GroupKey => {
                candidates.push(GroupRank {
                    group_key,
                    rank_metric: None,
                });
            }
            GroupOrder::FileCount => {
                if state.per_origin_total_files.is_empty() {
                    continue;
                }
                candidates.push(GroupRank {
                    group_key,
                    rank_metric: state.per_origin_total_files.values().copied().max(),
                });
            }
            GroupOrder::FileAge => {
                if state.per_origin_total_files.is_empty() {
                    continue;
                }
                candidates.push(GroupRank {
                    group_key,
                    rank_metric: Some(state.latest_file_mtime_us.unwrap_or(0)),
                });
            }
        }
    }

    let present = candidates
        .iter()
        .map(|candidate| candidate.group_key.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let missing = expected_groups
        .iter()
        .filter(|group| !present.contains(group.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    for group_key in missing {
        let stats_events = match mode {
            QueryMode::Find => {
                let request = build_materialized_stats_request(path, recursive, Some(group_key.clone()));
                query_materialized_events(backend.clone(), request, timeout).await?
            }
            QueryMode::ForceFind => {
                let request = build_force_find_stats_request(path, recursive, Some(group_key.clone()));
                query_force_find_events(backend.clone(), request, timeout).await?
            }
        };
        let mut best_total_files = None::<u64>;
        let mut best_latest_mtime = None::<u64>;
        for event in &stats_events {
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
                best_latest_mtime = Some(
                    best_latest_mtime.map_or(latest_mtime, |value| value.max(latest_mtime)),
                );
            }
        }
        let rank_metric = match group_order {
            GroupOrder::GroupKey => None,
            GroupOrder::FileCount => Some(best_total_files.unwrap_or(0)),
            GroupOrder::FileAge => Some(best_latest_mtime.unwrap_or(0)),
        };
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
    Ok(candidates)
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
                capanix_host_fs_types::query::UnreliableReason::Unattested,
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
        && params.recursive == scope.recursive
        && params.max_depth == scope.max_depth
        && params.group_order == scope.group_order
        && params.stability_mode == scope.stability_mode
        && params.quiet_window_ms == scope.quiet_window_ms
        && params.metadata_mode == scope.metadata_mode
}

fn scope_matches_force_find(params: &NormalizedApiParams, scope: &ForceFindPitScope) -> bool {
    params.path == scope.path
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
            "metadata_mode": snapshot.meta.metadata_mode,
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
    backend: QueryBackend,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    timeout: Duration,
) -> Result<PitSession, CnxError> {
    eprintln!(
        "fs_meta_query_api: build_tree_pit_session start path={} recursive={}",
        String::from_utf8_lossy(&params.path),
        params.recursive
    );
    let expected_groups = state
        .readiness_source
        .as_ref()
        .map(|source| {
            source
                .cached_logical_roots_snapshot()
                .map(|roots| {
                    roots.into_iter()
                        .filter(|root| root.scan)
                        .map(|root| root.id)
                        .collect::<Vec<_>>()
                })
        })
        .transpose()?
        .unwrap_or_default();
    let rankings = collect_group_rankings(
        backend.clone(),
        policy,
        QueryMode::Find,
        &params.path,
        params.recursive,
        timeout,
        params.group_order,
        &expected_groups,
    )
    .await?;
    eprintln!(
        "fs_meta_query_api: build_tree_pit_session rankings groups={}",
        rankings.len()
    );
    let mut groups = Vec::with_capacity(rankings.len());
    for rank in rankings {
        let tree_params = build_materialized_tree_request(
            &params.path,
            params.recursive,
            params.max_depth,
            params.stability_mode,
            params.quiet_window_ms,
            Some(rank.group_key.clone()),
        );
        let events = match query_materialized_events(backend.clone(), tree_params, timeout).await {
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
                        metadata_mode: params.metadata_mode,
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
                let (metadata_available, _meta_json) =
                    tree_metadata_json(params, &response.stability);
                let withheld_reason = (!metadata_available).then_some(match params.metadata_mode {
                    MetadataMode::StatusOnly => "status-only",
                    MetadataMode::StableOnly => "stability-not-stable",
                    MetadataMode::Full => "metadata-unavailable",
                });
                groups.push(GroupPitSnapshot {
                    group: rank.group_key,
                    status: "ok",
                    reliable: response.reliability.reliable,
                    unreliable_reason: response.reliability.unreliable_reason,
                    stability: response.stability,
                    meta: PitMetadata {
                        metadata_mode: params.metadata_mode,
                        metadata_available,
                        withheld_reason,
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
                    metadata_mode: params.metadata_mode,
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
            recursive: params.recursive,
            max_depth: params.max_depth,
            group_order: params.group_order,
            stability_mode: params.stability_mode,
            quiet_window_ms: params.quiet_window_ms,
            metadata_mode: params.metadata_mode,
        }),
        groups,
        expires_at_ms: unix_now_ms() + PIT_TTL_MS_DEFAULT,
        estimated_bytes,
    })
}

async fn build_force_find_pit_session(
    state: &ApiState,
    backend: QueryBackend,
    policy: &ProjectionPolicy,
    params: &NormalizedApiParams,
    timeout: Duration,
) -> Result<PitSession, CnxError> {
    let expected_groups = state
        .readiness_source
        .as_ref()
        .map(|source| {
            source
                .cached_logical_roots_snapshot()
                .map(|roots| {
                    roots.into_iter()
                        .filter(|root| root.scan)
                        .map(|root| root.id)
                        .collect::<Vec<_>>()
                })
        })
        .transpose()?
        .unwrap_or_default();
    let events = query_tree_events_for_all_groups(
        backend.clone(),
        policy,
        QueryMode::ForceFind,
        &params.path,
        params.recursive,
        params.max_depth,
        StabilityMode::None,
        None,
        timeout,
        None,
    )
    .await?;
    let rankings = if params.group_order == GroupOrder::GroupKey {
        let mut ordered = if expected_groups.is_empty() {
            events
                .iter()
                .map(|event| event_group_key(policy, event))
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            expected_groups.clone()
        };
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
            backend,
            policy,
            QueryMode::ForceFind,
            &params.path,
            params.recursive,
            timeout,
            params.group_order,
            &expected_groups,
        )
        .await?
    };
    let mut groups = Vec::with_capacity(rankings.len());
    for rank in rankings {
        match decode_force_find_selected_group_response(
            &events,
            policy,
            &rank.group_key,
            &params.path,
        ) {
            Ok(payload) => {
                groups.push(GroupPitSnapshot {
                    group: rank.group_key,
                    status: "ok",
                    reliable: payload.reliability.reliable,
                    unreliable_reason: payload.reliability.unreliable_reason,
                    stability: payload.stability,
                    meta: PitMetadata {
                        metadata_mode: MetadataMode::Full,
                        metadata_available: true,
                        withheld_reason: None,
                    },
                    root: Some(payload.root),
                    entries: payload.entries,
                    errors: Vec::new(),
                });
            }
            Err(err) => groups.push(GroupPitSnapshot {
                group: rank.group_key,
                status: "error",
                reliable: false,
                unreliable_reason: None,
                stability: TreeStability::not_evaluated(),
                meta: PitMetadata {
                    metadata_mode: MetadataMode::Full,
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
        mode: CursorQueryMode::ForceFind,
        scope: PitScope::ForceFind(ForceFindPitScope {
            path: params.path.clone(),
            recursive: params.recursive,
            max_depth: params.max_depth,
            group_order: params.group_order,
        }),
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

    let session = Arc::new(
        build_tree_pit_session(state, state.backend.clone(), policy, params, timeout).await?,
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

    let session = Arc::new(
        build_force_find_pit_session(state, state.backend.clone(), policy, params, timeout)
            .await?,
    );
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
        match decode_materialized_stats_payload(event.payload_bytes()) {
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
) -> Result<serde_json::Map<String, serde_json::Value>, CnxError> {
    let request = build_materialized_stats_request(&params.path, params.recursive, None);
    let events =
        query_materialized_events(state.backend.clone(), request, policy.query_timeout()).await?;
    let mut groups = decode_stats_groups(events, policy, None);

    if let Some(source) = state.readiness_source.as_ref()
        && let Ok(expected_roots) = source.cached_logical_roots_snapshot()
    {
        for group_id in expected_roots.into_iter().filter(|root| root.scan).map(|root| root.id) {
            if groups.contains_key(&group_id) {
                continue;
            }
            let request = build_materialized_stats_request(
                &params.path,
                params.recursive,
                Some(group_id.clone()),
            );
            let events = query_materialized_events(
                state.backend.clone(),
                request,
                policy.query_timeout(),
            )
            .await?;
            let targeted = decode_stats_groups(events, policy, Some(&group_id));
            groups.insert(
                group_id.clone(),
                targeted
                    .get(&group_id)
                    .cloned()
                    .unwrap_or_else(zero_stats_group_json),
            );
        }
    }

    Ok(groups)
}

async fn get_stats(
    State(state): State<ApiState>,
    Query(params): Query<ApiParams>,
) -> impl IntoResponse {
    eprintln!("fs_meta_query_api: get_stats handler entered");
    let policy = snapshot_policy(&state.policy);
    let params = match normalize_api_params(params) {
        Ok(params) => params,
        Err(err) => return error_response_with_context(err, None),
    };
    if let Err(err) = ensure_materialized_queries_ready(&state).await {
        return error_response_with_context(err, Some(&params.path));
    }
    let groups = match collect_materialized_stats_groups(&state, &policy, &params).await {
        Ok(groups) => groups,
        Err(err) => return error_response_with_context(err, Some(&params.path)),
    };

    let mut body = serde_json::Map::new();
    body.insert(
        "path".into(),
        serde_json::json!(path_to_string_lossy(&params.path)),
    );
    maybe_insert_b64(&mut body, "path_b64", &params.path);
    body.insert("groups".into(), serde_json::json!(groups));
    Json(serde_json::Value::Object(body)).into_response()
}

async fn get_tree(
    State(state): State<ApiState>,
    Query(params): Query<ApiParams>,
) -> impl IntoResponse {
    eprintln!("fs_meta_query_api: get_tree handler entered");
    let policy = snapshot_policy(&state.policy);
    let params = match normalize_api_params(params) {
        Ok(params) => params,
        Err(err) => return error_response_with_context(err, None),
    };
    let path_for_error = params.path.clone();
    if let Err(err) = validate_tree_query_params(&params) {
        return error_response_with_context(err, Some(&path_for_error));
    }
    if let Err(err) = ensure_materialized_queries_ready(&state).await {
        return error_response_with_context(err, Some(&path_for_error));
    }
    match query_tree_page_response(&state, &policy, &params, policy.query_timeout()).await {
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
    let force_find_groups = match resolve_force_find_groups(&state, &params) {
        Ok(groups) => groups,
        Err(err) => return error_response_with_context(err, Some(&path_for_error)),
    };
    let guard = match acquire_force_find_groups(&state, force_find_groups) {
        Ok(guard) => guard,
        Err(err) => return error_response_with_context(err, Some(&path_for_error)),
    };
    let response =
        query_force_find_page_response(&state, &policy, &params, policy.force_find_timeout())
            .await;
    if !guard.groups.is_empty() {
        tokio::time::sleep(FORCE_FIND_MIN_INFLIGHT_HOLD).await;
    }
    drop(guard);
    match response {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => error_response_with_context(err, Some(&path_for_error)),
    }
}

fn resolve_force_find_groups(
    state: &ApiState,
    _params: &NormalizedApiParams,
) -> Result<Vec<String>, CnxError> {
    fn fallback_force_find_groups(source: &Arc<SourceFacade>) -> Result<Vec<String>, CnxError> {
        if let Ok(snapshot) = source.observability_snapshot() {
            let mut groups = snapshot
                .source_primary_by_group
                .into_keys()
                .collect::<Vec<_>>();
            if !groups.is_empty() {
                groups.sort();
                groups.dedup();
                return Ok(groups);
            }
        }
        let mut groups = source
            .cached_logical_roots_snapshot()?
            .into_iter()
            .filter(|root| root.scan)
            .map(|root| root.id)
            .collect::<Vec<_>>();
        groups.sort();
        groups.dedup();
        Ok(groups)
    }

    match &state.backend {
        QueryBackend::InProcess { source, .. } => fallback_force_find_groups(source),
        QueryBackend::Route {
            boundary,
            origin_id,
            source,
            ..
        } => {
            let mut groups = route_source_group_ids(
                boundary.clone(),
                origin_id.clone(),
                Duration::from_secs(30),
            )
            .unwrap_or_default();
            if groups.is_empty() {
                groups = fallback_force_find_groups(source)?;
            }
            groups.sort();
            groups.dedup();
            Ok(groups)
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
            groups,
            inflight
        );
        return Err(CnxError::NotReady(format!(
            "force-find already running for group: {group}"
        )));
    }
    for group in &groups {
        inflight.insert(group.clone());
    }
    eprintln!(
        "fs_meta_query_api: force-find inflight acquire groups={:?} active={:?}",
        groups,
        inflight
    );
    drop(inflight);
    Ok(ForceFindInflightGuard {
        inflight: state.force_find_inflight.clone(),
        groups,
    })
}

fn route_source_group_ids(
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    timeout: Duration,
) -> Result<Vec<String>, CnxError> {
    let adapter = exchange_host_adapter(boundary, origin_id, default_route_bindings());
    let events = adapter.call_collect(
        ROUTE_TOKEN_FS_META_INTERNAL,
        METHOD_SOURCE_STATUS,
        Bytes::new(),
        timeout,
        MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE,
    )?;
    let mut groups = BTreeSet::new();
    for event in events {
        let snapshot: SourceObservabilitySnapshot =
            rmp_serde::from_slice(event.payload_bytes()).map_err(|err| {
                CnxError::Internal(format!("decode source observability failed: {err}"))
            })?;
        groups.extend(snapshot.source_primary_by_group.into_keys());
    }
    Ok(groups.into_iter().collect())
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
    use crate::sink::SinkFileMeta;
    use crate::source::FSMetaSource;
    use crate::source::config::{
        GrantedMountRoot, RootSpec, SinkExecutionMode, SourceConfig, SourceExecutionMode,
    };
    use crate::workers::sink::SinkFacade;
    use crate::workers::source::SourceFacade;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use bytes::Bytes;
    use capanix_app_sdk::runtime::{EventMetadata, NodeId};
    use capanix_host_fs_types::query::StabilityMode;
    use capanix_host_fs_types::{FileMetaRecord, UnixStat};
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;
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
                app: create_inprocess_router(
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
            source_execution_mode: SourceExecutionMode::InProcess,
            sink_execution_mode: SinkExecutionMode::InProcess,
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
            StabilityMode::QuietWindow,
            Some(5_000),
            Some("sink-a".to_string()),
        );
        assert_eq!(params.op, QueryOp::Tree);
        assert_eq!(params.scope.path, b"/a");
        assert!(!params.scope.recursive);
        assert_eq!(params.scope.max_depth, Some(2));
        let tree_options = params.tree_options.expect("tree options");
        assert_eq!(tree_options.stability_mode, StabilityMode::QuietWindow);
        assert_eq!(tree_options.quiet_window_ms, Some(5_000));
        assert_eq!(params.scope.selected_group.as_deref(), Some("sink-a"));
    }

    #[test]
    fn normalize_api_params_uses_defaults() {
        let params = ApiParams {
            path: None,
            path_b64: None,
            recursive: None,
            max_depth: None,
            pit_id: None,
            group_order: None,
            group_page_size: None,
            group_after: None,
            entry_page_size: None,
            entry_after: None,
            stability_mode: None,
            quiet_window_ms: None,
            metadata_mode: None,
        };
        let normalized = normalize_api_params(params).expect("normalize defaults");
        assert_eq!(normalized.path, b"/".to_vec());
        assert!(normalized.recursive);
        assert_eq!(normalized.max_depth, None);
        assert_eq!(normalized.pit_id, None);
        assert_eq!(normalized.group_order, GroupOrder::GroupKey);
        assert_eq!(normalized.group_page_size, None);
        assert_eq!(normalized.group_after, None);
        assert_eq!(normalized.entry_page_size, None);
        assert_eq!(normalized.entry_after, None);
        assert_eq!(normalized.stability_mode, StabilityMode::None);
        assert_eq!(normalized.metadata_mode, MetadataMode::Full);
    }

    #[test]
    fn normalize_api_params_keeps_group_pagination_shape() {
        let params = ApiParams {
            path: Some("/mnt".into()),
            path_b64: None,
            recursive: Some(false),
            max_depth: Some(3),
            pit_id: Some("pit-1".into()),
            group_order: Some(GroupOrder::FileAge),
            group_page_size: Some(25),
            group_after: Some("group-cursor-1".into()),
            entry_page_size: Some(100),
            entry_after: Some("entry-cursor-bundle-1".into()),
            stability_mode: Some(StabilityMode::QuietWindow),
            quiet_window_ms: Some(2_000),
            metadata_mode: Some(MetadataMode::StableOnly),
        };
        let normalized = normalize_api_params(params).expect("normalize explicit params");
        assert_eq!(normalized.path, b"/mnt".to_vec());
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
        assert_eq!(normalized.stability_mode, StabilityMode::QuietWindow);
        assert_eq!(normalized.quiet_window_ms, Some(2_000));
        assert_eq!(normalized.metadata_mode, MetadataMode::StableOnly);
    }

    #[test]
    fn normalize_api_params_decodes_path_b64() {
        let params = ApiParams {
            path: None,
            path_b64: Some(B64URL.encode(b"/bad/\xffname")),
            recursive: None,
            max_depth: None,
            pit_id: None,
            group_order: None,
            group_page_size: None,
            group_after: None,
            entry_page_size: None,
            entry_after: None,
            stability_mode: None,
            quiet_window_ms: None,
            metadata_mode: None,
        };
        let normalized = normalize_api_params(params).expect("normalize path_b64");
        assert_eq!(normalized.path, b"/bad/\xffname".to_vec());
    }

    #[test]
    fn normalize_api_params_rejects_path_and_path_b64_together() {
        let params = ApiParams {
            path: Some("/mnt".into()),
            path_b64: Some(B64URL.encode(b"/mnt")),
            recursive: None,
            max_depth: None,
            pit_id: None,
            group_order: None,
            group_page_size: None,
            group_after: None,
            entry_page_size: None,
            entry_after: None,
            stability_mode: None,
            quiet_window_ms: None,
            metadata_mode: None,
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
        let groups = decode_stats_groups(events, &origin_policy(), None);
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
                current_revision: None,
                candidate_revision: None,
                candidate_status: None,
                draining_revision: None,
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
                estimated_heap_bytes: 0,
            }],
        };

        let err = materialized_query_readiness_error(&source_status, &sink_status)
            .expect("initial audit should gate materialized queries");
        assert!(err.contains("initial audit incomplete"));
        assert!(err.contains("root-a"));
    }

    #[test]
    fn materialized_query_readiness_ignores_inactive_or_non_scan_groups() {
        let source_status = SourceStatusSnapshot {
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
                    current_revision: None,
                    candidate_revision: None,
                    candidate_status: None,
                    draining_revision: None,
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
                    current_revision: None,
                    candidate_revision: None,
                    candidate_status: None,
                    draining_revision: None,
                    draining_status: None,
                },
            ],
            degraded_roots: Vec::new(),
        };
        let sink_status = SinkStatusSnapshot::default();
        assert!(materialized_query_readiness_error(&source_status, &sink_status).is_none());
    }

    #[tokio::test]
    async fn rpc_metrics_endpoint_returns_ok() {
        let resp = get_bound_route_metrics().await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION", mode="system")
    // @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
    #[tokio::test]
    async fn force_find_group_order_file_count_top_bucket_inprocess() {
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
    async fn force_find_group_order_file_age_top_bucket_inprocess() {
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
    async fn force_find_group_order_file_age_keeps_empty_groups_eligible_inprocess() {
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
    async fn force_find_defaults_to_group_key_multi_group_response_inprocess() {
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

    // @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_HTTP_FACADE_DEFAULTS_AND_SHAPE", mode="system")
    #[tokio::test]
    async fn force_find_defaults_when_query_params_omitted_inprocess() {
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
    async fn projection_rpc_metrics_endpoint_shape_inprocess() {
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
    async fn force_find_rejects_status_only_and_keeps_pagination_axis_inprocess() {
        let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
        let req = Request::builder()
            .uri("/on-demand-force-find?path=/&group_order=group-key&metadata_mode=status-only")
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
        assert!(msg.contains("metadata_mode must be full on /on-demand-force-find"));
        assert_eq!(
            payload.get("code").and_then(|v| v.as_str()),
            Some("INVALID_INPUT")
        );
        assert_eq!(payload.get("path"), Some(&serde_json::json!("/")));
    }

    // @verify_spec("CONTRACTS.API_BOUNDARY.API_NON_OWNERSHIP_OF_QUERY_FIND_CHANNEL_PATH", mode="system")
    #[tokio::test]
    async fn namespace_projection_endpoints_removed_inprocess() {
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
