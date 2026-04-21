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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::FutureExt;
use serde::ser::SerializeStruct;

use capanix_app_sdk::runtime::{
    ControlEnvelope, EventMetadata, NodeId, RecvOpts, RouteKey, StateCellHandle,
    StateCellReadRequest, StateCellWriteRequest, StateClass, in_memory_state_boundary,
};
use capanix_app_sdk::{CnxError, Event, Result};
use capanix_host_adapter_fs::HostAdapter;
use capanix_runtime_entry_sdk::advanced::boundary::{
    BoundaryContext, ChannelIoSubset, StateBoundary,
};
use capanix_runtime_entry_sdk::control::{RuntimeBoundScope, RuntimeHostGrantState};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use crate::EventKind;
use crate::query::models::{HealthStats, QueryNode};
use crate::query::path::is_under_query_path;
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
    METHOD_FIND, METHOD_QUERY, METHOD_SINK_QUERY, METHOD_SINK_ROOTS_CONTROL, METHOD_SOURCE_FIND,
    METHOD_STREAM, ROUTE_KEY_EVENTS, ROUTE_TOKEN_FS_META, ROUTE_TOKEN_FS_META_EVENTS,
    ROUTE_TOKEN_FS_META_INTERNAL, default_route_bindings, sink_query_request_route_for,
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

fn debug_sink_query_route_trace_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_SINK_QUERY_ROUTE_TRACE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn is_per_peer_sink_query_request_route(route_key: &str) -> bool {
    let Some(request_route) = route_key.strip_suffix(".req") else {
        return false;
    };
    let Some((stem, version)) =
        crate::runtime::routes::ROUTE_KEY_SINK_QUERY_INTERNAL.rsplit_once(':')
    else {
        return request_route.starts_with(&format!(
            "{}.",
            crate::runtime::routes::ROUTE_KEY_SINK_QUERY_INTERNAL
        ));
    };
    let Some(route_stem) = request_route.strip_suffix(&format!(":{version}")) else {
        return false;
    };
    route_stem.starts_with(&format!("{stem}."))
}

fn matching_endpoint_task_states(tasks: &[ManagedEndpointTask], route_key: &str) -> Vec<String> {
    tasks
        .iter()
        .filter(|task| task.route_key() == route_key)
        .map(|task| {
            format!(
                "finished={} reason={}",
                task.is_finished(),
                task.finish_reason().unwrap_or_else(|| "none".to_string())
            )
        })
        .collect()
}

fn per_peer_sink_query_route_locality(node_id: &NodeId, route_key: &str) -> &'static str {
    if route_key == sink_query_request_route_for(&node_id.0).0 {
        "self"
    } else {
        "remote"
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

fn debug_control_scope_capture_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
}

fn debug_stream_delivery_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_STREAM_DELIVERY").is_some()
}

fn debug_stream_path_capture_target() -> Option<Vec<u8>> {
    static TARGET: std::sync::OnceLock<Option<Vec<u8>>> = std::sync::OnceLock::new();
    TARGET
        .get_or_init(|| match std::env::var("FSMETA_DEBUG_STREAM_PATH_CAPTURE") {
            Ok(value) if value.trim().is_empty() => Some(b"/force-find-stress".to_vec()),
            Ok(value) => Some(value.into_bytes()),
            Err(_) => None,
        })
        .clone()
}

fn summarize_bound_scopes(bound_scopes: &[RuntimeBoundScope]) -> Vec<String> {
    bound_scopes
        .iter()
        .map(|scope| format!("{}=>{}", scope.scope_id, scope.resource_ids.join("|")))
        .collect()
}

fn collect_event_origin_counts(events: &[Event]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::<String, u64>::new();
    for event in events {
        *counts
            .entry(event.metadata().origin_id.0.clone())
            .or_default() += 1;
    }
    counts
}

fn collect_event_origin_counts_for_query_path(
    events: &[Event],
    query_path: &[u8],
) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::<String, u64>::new();
    for event in events {
        let Ok(record) = rmp_serde::from_slice::<FileMetaRecord>(event.payload_bytes()) else {
            continue;
        };
        if is_under_query_path(&record.path, query_path) {
            *counts
                .entry(event.metadata().origin_id.0.clone())
                .or_default() += 1;
        }
    }
    counts
}

fn format_origin_counts(counts: &BTreeMap<String, u64>) -> Vec<String> {
    counts
        .iter()
        .map(|(origin, count)| format!("{origin}={count}"))
        .collect()
}

fn accumulate_origin_counts(target: &mut BTreeMap<String, u64>, incoming: &BTreeMap<String, u64>) {
    for (origin, count) in incoming {
        *target.entry(origin.clone()).or_default() += *count;
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
const SINK_STATE_SCHEMA_REV: u64 = 1;

fn sink_state_handle(scope: &str) -> StateCellHandle {
    StateCellHandle {
        cell_id: format!("fs-meta.sink-state.{scope}"),
        schema_rev: SINK_STATE_SCHEMA_REV,
        state_class: StateClass::Authoritative,
    }
}

fn sink_state_boundary_bridge(scope: &str) -> BoundaryContext {
    BoundaryContext::for_unit(scope)
}

fn is_statecell_not_found(err: &CnxError) -> bool {
    match err {
        CnxError::InvalidInput(msg) => msg.contains("statecell not found"),
        _ => false,
    }
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn encode_instant_age_ms(instant: Option<Instant>, now: Instant) -> Option<u64> {
    instant
        .and_then(|value| now.checked_duration_since(value))
        .map(duration_millis_u64)
}

fn decode_instant_age_ms(age_ms: Option<u64>) -> Option<Instant> {
    age_ms.map(|age| Instant::now() - Duration::from_millis(age))
}

fn encode_instant_remaining_ms(instant: Option<Instant>, now: Instant) -> Option<u64> {
    instant
        .and_then(|value| value.checked_duration_since(now))
        .map(duration_millis_u64)
}

fn decode_instant_remaining_ms(remaining_ms: Option<u64>) -> Option<Instant> {
    remaining_ms.map(|remaining| Instant::now() + Duration::from_millis(remaining))
}

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

#[derive(Debug, Clone)]
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
    pub overflow_pending_materialization: bool,
    pub readiness: GroupReadinessState,
    pub materialized_revision: u64,
    pub estimated_heap_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct GroupReadinessFacts<'a> {
    group_id: &'a str,
    primary_object_ref: &'a str,
    total_nodes: u64,
    live_nodes: u64,
    ready_materialization_evidence: bool,
}

impl<'a> GroupReadinessFacts<'a> {
    fn from_snapshot(
        group_id: &'a str,
        primary_object_ref: &'a str,
        total_nodes: u64,
        live_nodes: u64,
    ) -> Self {
        Self {
            group_id,
            primary_object_ref,
            total_nodes,
            live_nodes,
            ready_materialization_evidence: total_nodes > 0 || live_nodes > 0,
        }
    }

    fn from_live_group_state(
        primary_object_ref: &'a str,
        has_live_materialized_nodes: bool,
    ) -> Self {
        Self {
            group_id: "",
            primary_object_ref,
            total_nodes: 0,
            live_nodes: 0,
            ready_materialization_evidence: has_live_materialized_nodes,
        }
    }

    fn uses_placeholder_primary_truth(self) -> bool {
        self.primary_object_ref.is_empty()
            || self.primary_object_ref == "unassigned"
            || self.primary_object_ref == self.group_id
    }

    fn normalized_live_readiness(self, readiness: GroupReadinessState) -> GroupReadinessState {
        match readiness {
            GroupReadinessState::PendingMaterialization => {
                GroupReadinessState::PendingMaterialization
            }
            _ if self.ready_materialization_evidence => GroupReadinessState::Ready,
            _ => GroupReadinessState::WaitingForMaterializedRoot,
        }
    }

    fn normalize_snapshot_readiness(self, readiness: GroupReadinessState) -> GroupReadinessState {
        match readiness {
            GroupReadinessState::WaitingForMaterializedRoot if self.live_nodes > 0 => {
                GroupReadinessState::Ready
            }
            GroupReadinessState::Ready if self.total_nodes == 0 && self.live_nodes == 0 => {
                if self.uses_placeholder_primary_truth() {
                    GroupReadinessState::WaitingForMaterializedRoot
                } else {
                    GroupReadinessState::PendingMaterialization
                }
            }
            _ => readiness,
        }
    }

    fn normalized_snapshot_readiness(self, readiness: GroupReadinessState) -> GroupReadinessState {
        self.normalize_snapshot_readiness(readiness)
    }
}

impl SinkGroupStatusSnapshot {
    fn from_status_fields(
        group_id: String,
        primary_object_ref: String,
        total_nodes: u64,
        live_nodes: u64,
        tombstoned_count: u64,
        attested_count: u64,
        suspect_count: u64,
        blind_spot_count: u64,
        shadow_time_us: u64,
        shadow_lag_us: u64,
        overflow_pending_materialization: bool,
        readiness: GroupReadinessState,
        materialized_revision: u64,
        estimated_heap_bytes: u64,
    ) -> Self {
        let readiness = GroupReadinessFacts::from_snapshot(
            &group_id,
            &primary_object_ref,
            total_nodes,
            live_nodes,
        )
        .normalized_snapshot_readiness(readiness);
        Self {
            group_id,
            primary_object_ref,
            total_nodes,
            live_nodes,
            tombstoned_count,
            attested_count,
            suspect_count,
            blind_spot_count,
            shadow_time_us,
            shadow_lag_us,
            overflow_pending_materialization,
            readiness,
            materialized_revision,
            estimated_heap_bytes,
        }
    }

    pub(crate) fn normalized_readiness(&self) -> GroupReadinessState {
        GroupReadinessFacts::from_snapshot(
            &self.group_id,
            &self.primary_object_ref,
            self.total_nodes,
            self.live_nodes,
        )
        .normalized_snapshot_readiness(self.readiness)
    }

    pub fn is_ready(&self) -> bool {
        matches!(self.normalized_readiness(), GroupReadinessState::Ready)
    }
}

impl<'de> serde::Deserialize<'de> for SinkGroupStatusSnapshot {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct RawSinkGroupStatusSnapshot {
            group_id: String,
            primary_object_ref: String,
            total_nodes: u64,
            live_nodes: u64,
            tombstoned_count: u64,
            attested_count: u64,
            suspect_count: u64,
            blind_spot_count: u64,
            shadow_time_us: u64,
            shadow_lag_us: u64,
            overflow_pending_materialization: bool,
            readiness: GroupReadinessState,
            materialized_revision: u64,
            estimated_heap_bytes: u64,
        }

        let raw = RawSinkGroupStatusSnapshot::deserialize(deserializer)?;
        Ok(Self::from_status_fields(
            raw.group_id,
            raw.primary_object_ref,
            raw.total_nodes,
            raw.live_nodes,
            raw.tombstoned_count,
            raw.attested_count,
            raw.suspect_count,
            raw.blind_spot_count,
            raw.shadow_time_us,
            raw.shadow_lag_us,
            raw.overflow_pending_materialization,
            raw.readiness,
            raw.materialized_revision,
            raw.estimated_heap_bytes,
        ))
    }
}

impl serde::Serialize for SinkGroupStatusSnapshot {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let readiness = self.normalized_readiness();
        let mut state = serializer.serialize_struct("SinkGroupStatusSnapshot", 14)?;
        state.serialize_field("group_id", &self.group_id)?;
        state.serialize_field("primary_object_ref", &self.primary_object_ref)?;
        state.serialize_field("total_nodes", &self.total_nodes)?;
        state.serialize_field("live_nodes", &self.live_nodes)?;
        state.serialize_field("tombstoned_count", &self.tombstoned_count)?;
        state.serialize_field("attested_count", &self.attested_count)?;
        state.serialize_field("suspect_count", &self.suspect_count)?;
        state.serialize_field("blind_spot_count", &self.blind_spot_count)?;
        state.serialize_field("shadow_time_us", &self.shadow_time_us)?;
        state.serialize_field("shadow_lag_us", &self.shadow_lag_us)?;
        state.serialize_field(
            "overflow_pending_materialization",
            &self.overflow_pending_materialization,
        )?;
        state.serialize_field("readiness", &readiness)?;
        state.serialize_field("materialized_revision", &self.materialized_revision)?;
        state.serialize_field("estimated_heap_bytes", &self.estimated_heap_bytes)?;
        state.end()
    }
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
    pub scheduled_groups_by_node: BTreeMap<String, Vec<String>>,
    pub last_control_frame_signals_by_node: BTreeMap<String, Vec<String>>,
    pub received_batches_by_node: BTreeMap<String, u64>,
    pub received_events_by_node: BTreeMap<String, u64>,
    pub received_control_events_by_node: BTreeMap<String, u64>,
    pub received_data_events_by_node: BTreeMap<String, u64>,
    pub last_received_at_us_by_node: BTreeMap<String, u64>,
    pub last_received_origins_by_node: BTreeMap<String, Vec<String>>,
    pub received_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub stream_received_batches_by_node: BTreeMap<String, u64>,
    pub stream_received_events_by_node: BTreeMap<String, u64>,
    pub stream_received_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub stream_path_capture_target: Option<String>,
    pub stream_received_path_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub stream_ready_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub stream_ready_path_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub stream_deferred_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub stream_dropped_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub stream_applied_batches_by_node: BTreeMap<String, u64>,
    pub stream_applied_events_by_node: BTreeMap<String, u64>,
    pub stream_applied_control_events_by_node: BTreeMap<String, u64>,
    pub stream_applied_data_events_by_node: BTreeMap<String, u64>,
    pub stream_applied_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub stream_applied_path_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub stream_last_applied_at_us_by_node: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SinkStatusSnapshotReadinessSummary {
    pub(crate) scheduled_groups: BTreeSet<String>,
    pub(crate) ready_groups: BTreeSet<String>,
    pub(crate) waiting_for_materialized_root_groups: BTreeSet<String>,
    pub(crate) pending_materialization_groups: BTreeSet<String>,
    pub(crate) missing_scheduled_groups: BTreeSet<String>,
    pub(crate) has_stream_group_evidence: bool,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SinkStatusSnapshotIssue {
    StaleEmpty,
    ScheduledMissingGroupRowsAfterStreamEvidence,
    ScheduledPendingMaterializationWithoutStreamReceipts,
    ScheduledWaitingForMaterializedRoot,
    ScheduledMixedReadyAndUnready,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SinkStatusConcern {
    CoverageGap,
    ReplayPending,
    WaitingForMaterializedRoot,
    MixedReadiness,
    StaleEmpty,
}

#[derive(Clone, Debug)]
pub(crate) struct SinkStatusConcernProjection {
    pub(crate) summary: SinkStatusSnapshotReadinessSummary,
    pub(crate) concern: Option<SinkStatusConcern>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SinkProgressSnapshot {
    pub(crate) scheduled_groups: BTreeSet<String>,
    pub(crate) materialized_groups: BTreeSet<String>,
    pub(crate) ready_groups: BTreeSet<String>,
    pub(crate) empty_summary: bool,
}

impl SinkProgressSnapshot {
    pub(crate) fn has_ready_scheduled_groups(&self) -> bool {
        !self.scheduled_groups.is_empty() && self.scheduled_groups.is_subset(&self.ready_groups)
    }

    pub(crate) fn ready_for_expected_groups(&self, expected_groups: &BTreeSet<String>) -> bool {
        !expected_groups.is_empty()
            && expected_groups.is_subset(&self.scheduled_groups)
            && expected_groups.is_subset(&self.ready_groups)
    }
}

impl SinkStatusSnapshot {
    pub(crate) fn progress_snapshot(&self) -> SinkProgressSnapshot {
        let summary = self.readiness_summary();
        SinkProgressSnapshot {
            scheduled_groups: summary.scheduled_groups.clone(),
            materialized_groups: self
                .groups
                .iter()
                .filter(|group| {
                    !matches!(
                        group.normalized_readiness(),
                        GroupReadinessState::PendingMaterialization
                    )
                })
                .map(|group| group.group_id.clone())
                .collect(),
            ready_groups: summary.ready_groups,
            empty_summary: self.groups.is_empty() && summary.scheduled_groups.is_empty(),
        }
    }

    pub(crate) fn scheduled_groups(&self) -> BTreeSet<String> {
        self.scheduled_groups_by_node
            .values()
            .flat_map(|groups| groups.iter().cloned())
            .collect()
    }

    pub(crate) fn readiness_summary(&self) -> SinkStatusSnapshotReadinessSummary {
        let scheduled_groups = self.scheduled_groups();
        if scheduled_groups.is_empty() {
            return SinkStatusSnapshotReadinessSummary::default();
        }

        let groups_by_id = self
            .groups
            .iter()
            .map(|group| (group.group_id.as_str(), group))
            .collect::<BTreeMap<_, _>>();
        let mut summary = SinkStatusSnapshotReadinessSummary {
            has_stream_group_evidence: snapshot_has_stream_group_evidence(self, &scheduled_groups),
            scheduled_groups,
            ..SinkStatusSnapshotReadinessSummary::default()
        };

        for group_id in &summary.scheduled_groups {
            let Some(group) = groups_by_id.get(group_id.as_str()) else {
                summary.missing_scheduled_groups.insert(group_id.clone());
                continue;
            };
            match group.normalized_readiness() {
                GroupReadinessState::Ready if group.live_nodes > 0 && group.total_nodes > 0 => {
                    summary.ready_groups.insert(group_id.clone());
                }
                GroupReadinessState::WaitingForMaterializedRoot => {
                    summary
                        .waiting_for_materialized_root_groups
                        .insert(group_id.clone());
                }
                GroupReadinessState::PendingMaterialization => {
                    summary
                        .pending_materialization_groups
                        .insert(group_id.clone());
                }
                GroupReadinessState::Ready => {}
            }
        }

        summary
    }

    pub(crate) fn ready_groups(&self) -> BTreeSet<String> {
        self.readiness_summary().ready_groups
    }

    pub(crate) fn has_ready_scheduled_groups(&self) -> bool {
        self.progress_snapshot().has_ready_scheduled_groups()
    }

    pub(crate) fn looks_stale_empty(&self) -> bool {
        self.scheduled_groups_by_node.is_empty()
            && self
                .groups
                .iter()
                .all(|group| group.live_nodes == 0 && group.total_nodes == 0)
    }

    pub(crate) fn concern_projection(&self) -> SinkStatusConcernProjection {
        let summary = self.readiness_summary();
        let concern = if self.looks_stale_empty() {
            Some(SinkStatusConcern::StaleEmpty)
        } else if readiness_summary_looks_scheduled_missing_group_rows_after_stream_evidence(
            &summary,
        ) {
            Some(SinkStatusConcern::CoverageGap)
        } else if readiness_summary_looks_scheduled_pending_materialization_without_stream_receipts(
            &summary, self,
        ) {
            Some(SinkStatusConcern::ReplayPending)
        } else if readiness_summary_looks_scheduled_waiting_for_materialized_root(&summary) {
            Some(SinkStatusConcern::WaitingForMaterializedRoot)
        } else if readiness_summary_looks_scheduled_mixed_ready_and_unready(&summary) {
            Some(SinkStatusConcern::MixedReadiness)
        } else {
            None
        };

        SinkStatusConcernProjection { summary, concern }
    }

    #[cfg(test)]
    pub(crate) fn issue(&self) -> Option<SinkStatusSnapshotIssue> {
        self.concern_projection()
            .concern
            .map(|concern| match concern {
                SinkStatusConcern::CoverageGap => {
                    SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence
                }
                SinkStatusConcern::ReplayPending => {
                    SinkStatusSnapshotIssue::ScheduledPendingMaterializationWithoutStreamReceipts
                }
                SinkStatusConcern::WaitingForMaterializedRoot => {
                    SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot
                }
                SinkStatusConcern::MixedReadiness => {
                    SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready
                }
                SinkStatusConcern::StaleEmpty => SinkStatusSnapshotIssue::StaleEmpty,
            })
    }
}

pub(crate) fn sink_status_origin_entry_group_id(entry: &str) -> &str {
    let origin = entry
        .split_once('=')
        .map(|(origin, _)| origin)
        .unwrap_or(entry);
    let scoped = origin
        .rsplit_once("::")
        .map(|(_, group_id)| group_id)
        .unwrap_or(origin);
    scoped
        .split_once(':')
        .map(|(group_id, _)| group_id)
        .unwrap_or(scoped)
}

fn snapshot_has_stream_group_evidence(
    snapshot: &SinkStatusSnapshot,
    scheduled_groups: &BTreeSet<String>,
) -> bool {
    [
        &snapshot.stream_received_origin_counts_by_node,
        &snapshot.stream_received_path_origin_counts_by_node,
        &snapshot.stream_ready_origin_counts_by_node,
        &snapshot.stream_applied_origin_counts_by_node,
        &snapshot.stream_ready_path_origin_counts_by_node,
        &snapshot.stream_applied_path_origin_counts_by_node,
    ]
    .into_iter()
    .flat_map(|groups_by_node| groups_by_node.values())
    .flat_map(|entries| entries.iter())
    .any(|entry| scheduled_groups.contains(sink_status_origin_entry_group_id(entry)))
}

fn readiness_summary_looks_scheduled_missing_group_rows_after_stream_evidence(
    summary: &SinkStatusSnapshotReadinessSummary,
) -> bool {
    !summary.scheduled_groups.is_empty()
        && !summary.missing_scheduled_groups.is_empty()
        && summary.has_stream_group_evidence
}

fn readiness_summary_looks_scheduled_waiting_for_materialized_root(
    summary: &SinkStatusSnapshotReadinessSummary,
) -> bool {
    !summary.scheduled_groups.is_empty()
        && summary.missing_scheduled_groups.is_empty()
        && summary.waiting_for_materialized_root_groups.len() == summary.scheduled_groups.len()
}

fn readiness_summary_looks_scheduled_pending_materialization_without_stream_receipts(
    summary: &SinkStatusSnapshotReadinessSummary,
    snapshot: &SinkStatusSnapshot,
) -> bool {
    if summary.scheduled_groups.is_empty()
        || !summary.missing_scheduled_groups.is_empty()
        || summary.pending_materialization_groups.len() != summary.scheduled_groups.len()
    {
        return false;
    }
    snapshot.received_batches_by_node.is_empty()
        && snapshot.received_events_by_node.is_empty()
        && snapshot.received_control_events_by_node.is_empty()
        && snapshot.received_data_events_by_node.is_empty()
        && snapshot.last_received_at_us_by_node.is_empty()
        && snapshot.last_received_origins_by_node.is_empty()
        && snapshot.received_origin_counts_by_node.is_empty()
        && snapshot.stream_received_batches_by_node.is_empty()
        && snapshot.stream_received_events_by_node.is_empty()
        && snapshot.stream_received_origin_counts_by_node.is_empty()
        && snapshot
            .stream_received_path_origin_counts_by_node
            .is_empty()
        && snapshot.stream_ready_origin_counts_by_node.is_empty()
        && snapshot.stream_ready_path_origin_counts_by_node.is_empty()
        && snapshot.stream_deferred_origin_counts_by_node.is_empty()
        && snapshot.stream_dropped_origin_counts_by_node.is_empty()
        && snapshot.stream_applied_batches_by_node.is_empty()
        && snapshot.stream_applied_events_by_node.is_empty()
        && snapshot.stream_applied_control_events_by_node.is_empty()
        && snapshot.stream_applied_data_events_by_node.is_empty()
        && snapshot.stream_applied_origin_counts_by_node.is_empty()
        && snapshot
            .stream_applied_path_origin_counts_by_node
            .is_empty()
        && snapshot.stream_last_applied_at_us_by_node.is_empty()
}

fn readiness_summary_looks_scheduled_mixed_ready_and_unready(
    summary: &SinkStatusSnapshotReadinessSummary,
) -> bool {
    !summary.scheduled_groups.is_empty()
        && !summary.ready_groups.is_empty()
        && (!summary.waiting_for_materialized_root_groups.is_empty()
            || !summary.pending_materialization_groups.is_empty())
}

#[derive(Debug, Clone, Default)]
struct StreamDeliveryStats {
    received_batches: u64,
    received_events: u64,
    received_origin_counts: BTreeMap<String, u64>,
    received_path_origin_counts: BTreeMap<String, u64>,
    ready_origin_counts: BTreeMap<String, u64>,
    ready_path_origin_counts: BTreeMap<String, u64>,
    deferred_origin_counts: BTreeMap<String, u64>,
    dropped_origin_counts: BTreeMap<String, u64>,
    applied_batches: u64,
    applied_events: u64,
    applied_control_events: u64,
    applied_data_events: u64,
    applied_origin_counts: BTreeMap<String, u64>,
    applied_path_origin_counts: BTreeMap<String, u64>,
    last_applied_at_us: u64,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum GroupReadinessState {
    PendingMaterialization,
    WaitingForMaterializedRoot,
    Ready,
}

pub(crate) struct GroupSinkState {
    pub(crate) tree: MaterializedTree,
    pub(crate) clock: SinkClock,
    pub(crate) epoch_manager: EpochManager,
    pub(crate) tombstone_policy: TombstonePolicy,
    pub(crate) primary_object_ref: String,
    pub(crate) overflow_pending_materialization: bool,
    pub(crate) readiness_state: GroupReadinessState,
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
            overflow_pending_materialization: false,
            readiness_state: GroupReadinessState::PendingMaterialization,
            last_coverage_recovered_at: Some(Instant::now()),
            materialized_revision: 1,
            sentinel_health: BTreeMap::new(),
        }
    }

    fn has_live_materialized_nodes(&self) -> bool {
        self.tree
            .aggregate_at(b"/")
            .is_some_and(|aggregate| aggregate.total_nodes > 0)
    }

    fn group_readiness_projection(&self) -> GroupReadinessState {
        GroupReadinessFacts::from_live_group_state(
            &self.primary_object_ref,
            self.has_live_materialized_nodes(),
        )
        .normalized_live_readiness(self.readiness_state)
    }

    fn group_readiness_state(&self) -> GroupReadinessState {
        self.group_readiness_projection()
    }

    fn refresh_materialization_readiness(&mut self) {
        self.readiness_state = self.group_readiness_projection();
    }

    fn mark_materialization_ready(&mut self) {
        self.readiness_state = GroupReadinessFacts::from_live_group_state(
            &self.primary_object_ref,
            self.has_live_materialized_nodes(),
        )
        .normalized_live_readiness(GroupReadinessState::WaitingForMaterializedRoot);
    }

    fn preserves_materialized_state_for_omission_window(&self) -> bool {
        let readiness = self.group_readiness_state();
        matches!(
            readiness,
            GroupReadinessState::WaitingForMaterializedRoot | GroupReadinessState::Ready
        ) || self.tree.node_count() > 0
            || self.materialized_revision > 1
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedFileMetaNode {
    path: Vec<u8>,
    size: u64,
    modified_time_us: u64,
    created_time_us: u64,
    is_dir: bool,
    source: crate::SyncTrack,
    monitoring_attested: bool,
    last_confirmed_age_ms: Option<u64>,
    suspect_remaining_ms: Option<u64>,
    blind_spot: bool,
    is_tombstoned: bool,
    tombstone_remaining_ms: Option<u64>,
    last_seen_epoch: u64,
    subtree_last_write_significant_change_age_ms: Option<u64>,
}

impl PersistedFileMetaNode {
    fn from_live(path: &[u8], node: &tree::FileMetaNode, now: Instant) -> Self {
        Self {
            path: path.to_vec(),
            size: node.size,
            modified_time_us: node.modified_time_us,
            created_time_us: node.created_time_us,
            is_dir: node.is_dir,
            source: node.source.clone(),
            monitoring_attested: node.monitoring_attested,
            last_confirmed_age_ms: encode_instant_age_ms(node.last_confirmed_at, now),
            suspect_remaining_ms: encode_instant_remaining_ms(node.suspect_until, now),
            blind_spot: node.blind_spot,
            is_tombstoned: node.is_tombstoned,
            tombstone_remaining_ms: encode_instant_remaining_ms(node.tombstone_expires_at, now),
            last_seen_epoch: node.last_seen_epoch,
            subtree_last_write_significant_change_age_ms: encode_instant_age_ms(
                node.subtree_last_write_significant_change_at,
                now,
            ),
        }
    }

    fn into_live(self) -> tree::FileMetaNode {
        tree::FileMetaNode {
            size: self.size,
            modified_time_us: self.modified_time_us,
            created_time_us: self.created_time_us,
            is_dir: self.is_dir,
            source: self.source,
            monitoring_attested: self.monitoring_attested,
            last_confirmed_at: decode_instant_age_ms(self.last_confirmed_age_ms),
            suspect_until: decode_instant_remaining_ms(self.suspect_remaining_ms),
            blind_spot: self.blind_spot,
            is_tombstoned: self.is_tombstoned,
            tombstone_expires_at: decode_instant_remaining_ms(self.tombstone_remaining_ms),
            last_seen_epoch: self.last_seen_epoch,
            subtree_last_write_significant_change_at: decode_instant_age_ms(
                self.subtree_last_write_significant_change_age_ms,
            ),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedGroupSinkState {
    group_id: String,
    primary_object_ref: String,
    overflow_pending_materialization: bool,
    readiness_state: GroupReadinessState,
    materialized_revision: u64,
    sentinel_health: BTreeMap<String, String>,
    shadow_time_high_us: u64,
    epoch_state_active: Option<u64>,
    completed_epochs: u64,
    #[serde(alias = "audit_start_age_ms")]
    materialization_start_age_ms: Option<u64>,
    last_coverage_recovered_age_ms: Option<u64>,
    nodes: Vec<PersistedFileMetaNode>,
}

impl PersistedGroupSinkState {
    fn from_live(group_id: &str, group: &GroupSinkState) -> Self {
        let now = Instant::now();
        Self {
            group_id: group_id.to_string(),
            primary_object_ref: group.primary_object_ref.clone(),
            overflow_pending_materialization: group.overflow_pending_materialization,
            readiness_state: group.group_readiness_state(),
            materialized_revision: group.materialized_revision,
            sentinel_health: group.sentinel_health.clone(),
            shadow_time_high_us: group.clock.shadow_time_high_us,
            epoch_state_active: match group.epoch_manager.state {
                epoch::EpochState::Idle => None,
                epoch::EpochState::Active(epoch_id) => Some(epoch_id),
            },
            completed_epochs: group.epoch_manager.completed_epochs,
            materialization_start_age_ms: encode_instant_age_ms(
                group.epoch_manager.audit_start_time,
                now,
            ),
            last_coverage_recovered_age_ms: encode_instant_age_ms(
                group.last_coverage_recovered_at,
                now,
            ),
            nodes: group
                .tree
                .iter()
                .map(|(path, node)| PersistedFileMetaNode::from_live(path, node, now))
                .collect(),
        }
    }

    fn into_live(self, tombstone_policy: TombstonePolicy) -> GroupSinkState {
        let mut tree = MaterializedTree::new();
        for node in self.nodes {
            let path = node.path.clone();
            tree.insert(path, node.into_live());
        }
        let mut epoch_manager = EpochManager::new();
        epoch_manager.completed_epochs = self.completed_epochs;
        epoch_manager.state = self
            .epoch_state_active
            .map(epoch::EpochState::Active)
            .unwrap_or(epoch::EpochState::Idle);
        epoch_manager.audit_start_time = decode_instant_age_ms(self.materialization_start_age_ms);
        let mut restored = GroupSinkState {
            tree,
            clock: SinkClock {
                shadow_time_high_us: self.shadow_time_high_us,
            },
            epoch_manager,
            tombstone_policy,
            primary_object_ref: self.primary_object_ref,
            overflow_pending_materialization: self.overflow_pending_materialization,
            readiness_state: self.readiness_state,
            last_coverage_recovered_at: decode_instant_age_ms(self.last_coverage_recovered_age_ms),
            materialized_revision: self.materialized_revision,
            sentinel_health: self.sentinel_health,
        };
        restored.refresh_materialization_readiness();
        restored
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedSinkState {
    scope: String,
    persisted_at_us: u64,
    groups: Vec<PersistedGroupSinkState>,
    #[serde(default)]
    retained_groups: Vec<PersistedGroupSinkState>,
}

#[derive(Clone)]
struct SinkStateSnapshotCell {
    scope: Arc<str>,
    handle: StateCellHandle,
    state_boundary: Arc<dyn StateBoundary>,
}

impl SinkStateSnapshotCell {
    fn from_state_boundary(
        scope: &str,
        state_boundary: Arc<dyn StateBoundary>,
    ) -> std::io::Result<(Self, Option<PersistedSinkState>)> {
        let handle = sink_state_handle(scope);
        let loaded =
            match crate::runtime_app::block_on_shared_runtime(state_boundary.statecell_read(
                sink_state_boundary_bridge(scope),
                StateCellReadRequest {
                    handle: handle.clone(),
                },
            )) {
                Ok(resp) => {
                    if resp.status != "ok" {
                        return Err(std::io::Error::other(format!(
                            "statecell_read failed for sink state scope={scope}: status={}",
                            resp.status
                        )));
                    }
                    if resp.payload.is_empty() {
                        None
                    } else {
                        let decoded: PersistedSinkState = rmp_serde::from_slice(&resp.payload)
                            .map_err(|err| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("decode sink state snapshot failed: {err}"),
                                )
                            })?;
                        if decoded.scope != scope {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!(
                                    "sink state snapshot scope mismatch: expected {scope}, got {}",
                                    decoded.scope
                                ),
                            ));
                        }
                        Some(decoded)
                    }
                }
                Err(err) if is_statecell_not_found(&err) => None,
                Err(err) => {
                    return Err(std::io::Error::other(format!(
                        "statecell_read failed for sink state scope={scope}: {err}"
                    )));
                }
            };
        Ok((
            Self {
                scope: Arc::<str>::from(scope),
                handle,
                state_boundary,
            },
            loaded,
        ))
    }

    fn persist(&self, snapshot: &PersistedSinkState) -> Result<()> {
        let payload = rmp_serde::to_vec_named(snapshot).map_err(|err| {
            CnxError::Internal(format!("encode sink state snapshot failed: {err}"))
        })?;
        let response =
            crate::runtime_app::block_on_shared_runtime(self.state_boundary.statecell_write(
                sink_state_boundary_bridge(&self.scope),
                StateCellWriteRequest {
                    handle: self.handle.clone(),
                    payload,
                    lease_epoch: Some(snapshot.persisted_at_us.max(1)),
                },
            ))?;
        if response.status != "committed" && response.status != "ok" {
            return Err(CnxError::Internal(format!(
                "statecell_write returned non-committed status for sink state scope={}: {}",
                self.scope, response.status
            )));
        }
        Ok(())
    }
}

pub(crate) struct SinkState {
    pub(crate) groups: BTreeMap<String, GroupSinkState>,
    retained_groups: BTreeMap<String, GroupSinkState>,
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
            retained_groups: BTreeMap::new(),
            group_by_object_ref: HashMap::new(),
            tombstone_policy,
        };
        state.reconcile_host_object_grants(&source_cfg.roots, &source_cfg.host_object_grants, None);
        state
    }

    fn apply_persisted_snapshot(&mut self, snapshot: PersistedSinkState) {
        for persisted in snapshot.groups {
            let group_id = persisted.group_id.clone();
            if let Some(group) = self.groups.get_mut(&group_id) {
                *group = persisted.into_live(self.tombstone_policy);
            }
        }
        for persisted in snapshot.retained_groups {
            let group_id = persisted.group_id.clone();
            self.groups.remove(&group_id);
            self.retained_groups
                .insert(group_id, persisted.into_live(self.tombstone_policy));
        }
    }

    fn to_persisted_snapshot(&self, scope: &str) -> PersistedSinkState {
        PersistedSinkState {
            scope: scope.to_string(),
            persisted_at_us: now_us(),
            groups: self
                .groups
                .iter()
                .map(|(group_id, group)| PersistedGroupSinkState::from_live(group_id, group))
                .collect(),
            retained_groups: self
                .retained_groups
                .iter()
                .map(|(group_id, group)| PersistedGroupSinkState::from_live(group_id, group))
                .collect(),
        }
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
        let configured_root_ids = roots
            .iter()
            .map(|root| root.id.clone())
            .collect::<BTreeSet<_>>();

        let mut previous_groups = std::mem::take(&mut self.groups);
        let mut retained_groups = std::mem::take(&mut self.retained_groups);
        let previous_group_by_object_ref = std::mem::take(&mut self.group_by_object_ref);
        for root in roots {
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
                .unwrap_or_else(|| root.id.clone());
            let mut group = previous_groups
                .remove(&root.id)
                .or_else(|| retained_groups.remove(&root.id))
                .unwrap_or_else(|| GroupSinkState::new(primary.clone(), tombstone_policy));
            group.primary_object_ref = primary.clone();
            if allowed_groups.is_some_and(|groups| !groups.contains(&root.id)) {
                if group.preserves_materialized_state_for_omission_window() {
                    retained_groups.insert(root.id.clone(), group);
                }
                continue;
            }
            group_by_object_ref
                .entry(root.id.clone())
                .or_insert_with(|| root.id.clone());
            for member_id in member_ids {
                group_by_object_ref.insert(member_id, root.id.clone());
            }
            groups.insert(root.id.clone(), group);
        }
        let reassigned_previous_group_ids = previous_group_by_object_ref
            .iter()
            .filter_map(|(member_id, previous_group_id)| {
                group_by_object_ref
                    .contains_key(member_id)
                    .then_some(previous_group_id.clone())
            })
            .collect::<BTreeSet<_>>();
        for (group_id, group) in previous_groups {
            if !reassigned_previous_group_ids.contains(&group_id)
                && group.preserves_materialized_state_for_omission_window()
            {
                retained_groups.entry(group_id).or_insert(group);
            }
        }
        retained_groups.retain(|group_id, group| {
            group.preserves_materialized_state_for_omission_window()
                && (configured_root_ids.contains(group_id)
                    || !reassigned_previous_group_ids.contains(group_id))
        });
        self.groups = groups;
        self.retained_groups = retained_groups;
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
    snapshot_cell: Option<SinkStateSnapshotCell>,
}

impl SinkStateCell {
    fn new(
        source_cfg: &SourceConfig,
        commit_boundary: CommitBoundary,
        snapshot_cell: Option<SinkStateSnapshotCell>,
        persisted_snapshot: Option<PersistedSinkState>,
        record_bootstrap: bool,
    ) -> Self {
        let mut state = SinkState::new(source_cfg);
        if let Some(snapshot) = persisted_snapshot {
            state.apply_persisted_snapshot(snapshot);
        }
        let cell = Self {
            inner: Arc::new(RwLock::new(state)),
            commit_boundary,
            snapshot_cell,
        };
        if record_bootstrap {
            cell.record_authoritative_commit(
                "sink.bootstrap",
                format!(
                    "roots={} exports={}",
                    source_cfg.roots.len(),
                    source_cfg.host_object_grants.len()
                ),
            );
        }
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

    fn persist_snapshot(&self) -> Result<()> {
        let Some(snapshot_cell) = &self.snapshot_cell else {
            return Ok(());
        };
        let snapshot = self.read()?.to_persisted_snapshot(SINK_RUNTIME_UNIT_ID);
        snapshot_cell.persist(&snapshot)
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
#[derive(Default)]
struct StreamRecvObserver {
    observed_epoch: AtomicU64,
    notify: Notify,
}

impl StreamRecvObserver {
    fn observed_epoch(&self) -> u64 {
        self.observed_epoch.load(Ordering::Acquire)
    }

    fn mark_before_recv(&self) {
        self.observed_epoch.fetch_add(1, Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    async fn wait_for_epoch_after(&self, epoch: u64) {
        if self.observed_epoch() > epoch {
            return;
        }
        loop {
            let notified = self.notify.notified();
            if self.observed_epoch() > epoch {
                return;
            }
            notified.await;
            if self.observed_epoch() > epoch {
                return;
            }
        }
    }
}

#[derive(Clone)]
pub struct SinkFileMeta {
    node_id: NodeId,
    state: SinkStateCell,
    root_specs: Arc<RwLock<Vec<RootSpec>>>,
    host_object_grants: Arc<RwLock<Vec<GrantedMountRoot>>>,
    visibility_lag: Arc<Mutex<VisibilityLagTelemetry>>,
    stream_delivery_stats: Arc<Mutex<StreamDeliveryStats>>,
    pending_stream_events: Arc<Mutex<VecDeque<Event>>>,
    stream_receive_enabled: Arc<AtomicBool>,
    unit_control: Arc<RuntimeUnitGate>,
    stream_recv_observer: Option<Arc<StreamRecvObserver>>,
    stream_recv_ready_notify: Option<Arc<Notify>>,
    shutdown: CancellationToken,
    endpoint_tasks: Arc<Mutex<Vec<ManagedEndpointTask>>>,
}

impl SinkFileMeta {
    fn stream_recv_epoch(&self) -> u64 {
        self.stream_recv_observer
            .as_ref()
            .map(|observer| observer.observed_epoch())
            .unwrap_or(0)
    }

    fn mark_before_stream_recv(&self) {
        if let Some(observer) = self.stream_recv_observer.as_ref() {
            observer.mark_before_recv();
        }
    }

    fn notify_stream_recv_waiters(&self) {
        if let Some(notify) = self.stream_recv_ready_notify.as_ref() {
            notify.notify_waiters();
        }
    }

    async fn wait_until_stream_can_receive(&self) {
        let Some(notify) = self.stream_recv_ready_notify.clone() else {
            return;
        };
        if self.should_receive_stream_events() {
            return;
        }
        loop {
            let notified = notify.notified();
            if self.should_receive_stream_events() {
                return;
            }
            notified.await;
            if self.should_receive_stream_events() {
                return;
            }
        }
    }

    async fn wait_until_stream_recv_observed_after_activation(
        &self,
        was_receivable: bool,
        observed_epoch_before: u64,
    ) {
        if !self.unit_control.has_runtime_state()
            || lock_or_recover(
                &self.endpoint_tasks,
                "sink.wait_until_stream_recv_observed_after_activation.endpoint_tasks",
            )
            .is_empty()
        {
            return;
        }
        if was_receivable || !self.should_receive_stream_events() {
            return;
        }
        let Some(observer) = self.stream_recv_observer.clone() else {
            return;
        };
        observer.wait_for_epoch_after(observed_epoch_before).await;
    }

    pub(crate) fn debug_traced_route_state(&self, route_key: &str) -> Result<String> {
        let gate_generation = self
            .unit_control
            .route_generation(SINK_RUNTIME_UNIT_ID, route_key)?;
        let endpoint_tasks = lock_or_recover(&self.endpoint_tasks, "sink.debug_traced_route_state");
        Ok(format!(
            "route={} locality={} gate_generation={:?} endpoint_tasks={:?}",
            route_key,
            per_peer_sink_query_route_locality(&self.node_id, route_key),
            gate_generation,
            matching_endpoint_task_states(&endpoint_tasks, route_key)
        ))
    }

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

    pub(crate) fn with_boundaries_and_state_deferred_authority(
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        state_boundary: Arc<dyn StateBoundary>,
        source_cfg: SourceConfig,
    ) -> Result<Self> {
        Self::with_boundaries_and_state_inner(node_id, boundary, state_boundary, source_cfg, true)
    }

    fn with_boundaries_and_state_inner(
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        state_boundary: Arc<dyn StateBoundary>,
        source_cfg: SourceConfig,
        defer_authority_read: bool,
    ) -> Result<Self> {
        let (snapshot_cell, persisted_snapshot) = SinkStateSnapshotCell::from_state_boundary(
            SINK_RUNTIME_UNIT_ID,
            state_boundary.clone(),
        )
        .map_err(|err| {
            CnxError::InvalidInput(format!("sink statecell snapshot init failed: {err}"))
        })?;
        let authority = if defer_authority_read {
            AuthorityJournal::deferred(SINK_RUNTIME_UNIT_ID, state_boundary.clone())
        } else {
            AuthorityJournal::from_state_boundary(SINK_RUNTIME_UNIT_ID, state_boundary).map_err(
                |err| {
                    CnxError::InvalidInput(format!("sink statecell authority init failed: {err}"))
                },
            )?
        };
        let state = SinkStateCell::new(
            &source_cfg,
            CommitBoundary::new(authority),
            Some(snapshot_cell),
            persisted_snapshot,
            !defer_authority_read,
        );
        let root_specs = Arc::new(RwLock::new(source_cfg.roots.clone()));
        let host_object_grants = Arc::new(RwLock::new(source_cfg.host_object_grants.clone()));
        let visibility_lag = Arc::new(Mutex::new(VisibilityLagTelemetry::default()));
        let stream_delivery_stats = Arc::new(Mutex::new(StreamDeliveryStats::default()));
        let pending_stream_events = Arc::new(Mutex::new(VecDeque::new()));
        let stream_receive_enabled = Arc::new(AtomicBool::new(false));
        let stream_recv_observer = Arc::new(StreamRecvObserver::default());
        let stream_recv_ready_notify = Arc::new(Notify::new());
        let unit_control = Arc::new(if boundary.is_some() {
            RuntimeUnitGate::new_runtime_managed("sink-file-meta", SINK_RUNTIME_UNITS)
        } else {
            RuntimeUnitGate::new("sink-file-meta", SINK_RUNTIME_UNITS)
        });
        let sink = Self {
            node_id: node_id.clone(),
            state: state.clone(),
            root_specs: root_specs.clone(),
            host_object_grants: host_object_grants.clone(),
            visibility_lag: visibility_lag.clone(),
            stream_delivery_stats: stream_delivery_stats.clone(),
            pending_stream_events: pending_stream_events.clone(),
            stream_receive_enabled: stream_receive_enabled.clone(),
            unit_control: unit_control.clone(),
            stream_recv_observer: Some(stream_recv_observer.clone()),
            stream_recv_ready_notify: Some(stream_recv_ready_notify.clone()),
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
            let query_stream_delivery_stats = stream_delivery_stats.clone();
            let query_pending_stream_events = pending_stream_events.clone();
            let query_stream_receive_enabled = stream_receive_enabled.clone();
            let query_unit_control = unit_control.clone();
            if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META, METHOD_QUERY) {
                log::info!(
                    "bound route listening on {}.{} for sink {}",
                    ROUTE_TOKEN_FS_META,
                    METHOD_QUERY,
                    node_id_cloned.0
                );
                let endpoint = ManagedEndpointTask::spawn_with_unit(
                    sys.clone(),
                    route,
                    format!("sink:{}:{}", ROUTE_TOKEN_FS_META, METHOD_QUERY),
                    SINK_RUNTIME_UNIT_ID,
                    sink.shutdown.clone(),
                    {
                        let query_node_id = node_id_cloned.clone();
                        move |requests| {
                            let query_state = query_state.clone();
                            let query_root_specs = query_root_specs.clone();
                            let query_host_object_grants = query_host_object_grants.clone();
                            let query_visibility_lag = query_visibility_lag.clone();
                            let query_stream_delivery_stats = query_stream_delivery_stats.clone();
                            let query_pending_stream_events = query_pending_stream_events.clone();
                            let query_stream_receive_enabled = query_stream_receive_enabled.clone();
                            let query_unit_control = query_unit_control.clone();
                            let query_node_id = query_node_id.clone();
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
                                            node_id: query_node_id.clone(),
                                            state: query_state.clone(),
                                            root_specs: query_root_specs.clone(),
                                            host_object_grants: query_host_object_grants.clone(),
                                            visibility_lag: query_visibility_lag.clone(),
                                            stream_delivery_stats: query_stream_delivery_stats
                                                .clone(),
                                            pending_stream_events: query_pending_stream_events
                                                .clone(),
                                            stream_receive_enabled: query_stream_receive_enabled
                                                .clone(),
                                            unit_control: query_unit_control.clone(),
                                            stream_recv_observer: None,
                                            stream_recv_ready_notify: None,
                                            shutdown: CancellationToken::new(),
                                            endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
                                        };
                                        let mut events = sink_impl
                                            .materialized_query(&params)
                                            .unwrap_or_default();
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
                                        log::warn!(
                                            "bound route failed to parse InternalQueryRequest"
                                        );
                                    }
                                }
                                responses
                            }
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
            let internal_query_stream_delivery_stats = stream_delivery_stats.clone();
            let internal_query_pending_stream_events = pending_stream_events.clone();
            let internal_query_stream_receive_enabled = stream_receive_enabled.clone();
            let internal_query_unit_control = unit_control.clone();
            let mut internal_query_routes = Vec::<RouteKey>::new();
            if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY) {
                internal_query_routes.push(route);
            } else {
                log::error!(
                    "failed to resolve route lookup for {}.{}",
                    ROUTE_TOKEN_FS_META_INTERNAL,
                    METHOD_SINK_QUERY
                );
            }
            internal_query_routes.push(sink_query_request_route_for(&node_id_cloned.0));
            for route in internal_query_routes {
                let internal_query_state_for_route = internal_query_state.clone();
                let internal_query_root_specs_for_route = internal_query_root_specs.clone();
                let internal_query_host_object_grants_for_route =
                    internal_query_host_object_grants.clone();
                let internal_query_visibility_lag_for_route = internal_query_visibility_lag.clone();
                let internal_query_stream_delivery_stats_for_route =
                    internal_query_stream_delivery_stats.clone();
                let internal_query_pending_stream_events_for_route =
                    internal_query_pending_stream_events.clone();
                let internal_query_stream_receive_enabled_for_route =
                    internal_query_stream_receive_enabled.clone();
                let internal_query_unit_control_for_route = internal_query_unit_control.clone();
                let route_key_for_trace = route.0.clone();
                log::info!(
                    "bound route listening on {} for sink {}",
                    route.0,
                    node_id_cloned.0
                );
                let endpoint = ManagedEndpointTask::spawn_with_unit(
                    sys.clone(),
                    route,
                    format!(
                        "sink:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY
                    ),
                    SINK_RUNTIME_UNIT_ID,
                    sink.shutdown.clone(),
                    {
                        let internal_query_node_id = node_id_cloned.clone();
                        move |requests| {
                            let internal_query_state = internal_query_state_for_route.clone();
                            let internal_query_root_specs =
                                internal_query_root_specs_for_route.clone();
                            let internal_query_host_object_grants =
                                internal_query_host_object_grants_for_route.clone();
                            let internal_query_visibility_lag =
                                internal_query_visibility_lag_for_route.clone();
                            let internal_query_stream_delivery_stats =
                                internal_query_stream_delivery_stats_for_route.clone();
                            let internal_query_pending_stream_events =
                                internal_query_pending_stream_events_for_route.clone();
                            let internal_query_stream_receive_enabled =
                                internal_query_stream_receive_enabled_for_route.clone();
                            let internal_query_unit_control =
                                internal_query_unit_control_for_route.clone();
                            let internal_query_node_id = internal_query_node_id.clone();
                            let route_key_for_trace = route_key_for_trace.clone();
                            async move {
                                let mut responses = Vec::new();
                                for req in requests {
                                    if let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                        req.payload_bytes(),
                                    ) {
                                        if debug_sink_query_route_trace_enabled() {
                                            eprintln!(
                                                "fs_meta_sink: internal query route_request route={} correlation={:?} selected_group={:?} recursive={} path={}",
                                                route_key_for_trace,
                                                req.metadata().correlation_id,
                                                params.scope.selected_group,
                                                params.scope.recursive,
                                                String::from_utf8_lossy(&params.scope.path)
                                            );
                                        }
                                        eprintln!(
                                            "fs_meta_sink: internal query endpoint request selected_group={:?} recursive={} path={}",
                                            params.scope.selected_group,
                                            params.scope.recursive,
                                            String::from_utf8_lossy(&params.scope.path)
                                        );
                                        let sink_impl = SinkFileMeta {
                                            node_id: internal_query_node_id.clone(),
                                            state: internal_query_state.clone(),
                                            root_specs: internal_query_root_specs.clone(),
                                            host_object_grants: internal_query_host_object_grants
                                                .clone(),
                                            visibility_lag: internal_query_visibility_lag.clone(),
                                            stream_delivery_stats:
                                                internal_query_stream_delivery_stats.clone(),
                                            pending_stream_events:
                                                internal_query_pending_stream_events.clone(),
                                            stream_receive_enabled:
                                                internal_query_stream_receive_enabled.clone(),
                                            unit_control: internal_query_unit_control.clone(),
                                            stream_recv_observer: None,
                                            stream_recv_ready_notify: None,
                                            shutdown: CancellationToken::new(),
                                            endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
                                        };
                                        let mut events = sink_impl
                                            .materialized_query(&params)
                                            .unwrap_or_default();
                                        if debug_sink_query_route_trace_enabled() {
                                            eprintln!(
                                                "fs_meta_sink: internal query route_response route={} correlation={:?} events={}",
                                                route_key_for_trace,
                                                req.metadata().correlation_id,
                                                events.len()
                                            );
                                        }
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
                                        log::warn!(
                                            "bound route failed to parse InternalQueryRequest"
                                        );
                                    }
                                }
                                responses
                            }
                        }
                    },
                );
                lock_or_recover(&sink.endpoint_tasks, "sink.with_boundaries.endpoint_tasks")
                    .push(endpoint);
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
                let endpoint = ManagedEndpointTask::spawn_with_unit(
                    sys.clone(),
                    route,
                    format!("sink:{}:{}", ROUTE_TOKEN_FS_META, METHOD_FIND),
                    SINK_RUNTIME_UNIT_ID,
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
            let stream_delivery_stats = stream_delivery_stats.clone();
            let stream_unit_control = unit_control.clone();
            if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM) {
                let stream_sink = Arc::new(SinkFileMeta {
                    node_id: node_id.clone(),
                    state: stream_state,
                    root_specs: stream_root_specs,
                    host_object_grants: stream_host_object_grants,
                    visibility_lag: stream_visibility_lag,
                    stream_delivery_stats,
                    pending_stream_events: pending_stream_events.clone(),
                    stream_receive_enabled: stream_receive_enabled.clone(),
                    unit_control: stream_unit_control,
                    stream_recv_observer: Some(stream_recv_observer.clone()),
                    stream_recv_ready_notify: Some(stream_recv_ready_notify.clone()),
                    shutdown: CancellationToken::new(),
                    endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
                });
                let stream_sink_ready = stream_sink.clone();
                let stream_sink_wait = stream_sink.clone();
                let stream_sink_observe = stream_sink.clone();
                let stream_sink_apply = stream_sink.clone();
                let endpoint = ManagedEndpointTask::spawn_stream_with_before_recv_and_wait(
                    sys,
                    route,
                    format!("sink:{}:{}", ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM),
                    SINK_RUNTIME_UNIT_ID,
                    sink.shutdown.clone(),
                    move || stream_sink_ready.should_receive_stream_events(),
                    move || {
                        let stream_sink_wait = stream_sink_wait.clone();
                        async move { stream_sink_wait.wait_until_stream_can_receive().await }
                            .boxed()
                    },
                    move || stream_sink_observe.mark_before_stream_recv(),
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

            sink.enable_stream_receive();
        }

        Ok(sink)
    }

    fn prune_finished_endpoint_tasks(&self, context: &str) {
        let mut tasks = lock_or_recover(&self.endpoint_tasks, context);
        tasks.retain(|task| {
            if !task.is_finished() {
                return true;
            }
            eprintln!(
                "fs_meta_sink: pruning finished endpoint route={} reason={}",
                task.route_key(),
                task.finish_reason()
                    .unwrap_or_else(|| "unclassified_finish".to_string())
            );
            false
        });
    }

    pub fn start_runtime_endpoints(
        &self,
        boundary: Arc<dyn ChannelIoSubset>,
        node_id: NodeId,
    ) -> Result<()> {
        let start = Instant::now();
        eprintln!(
            "fs_meta_sink: start_runtime_endpoints begin node={}",
            node_id.0
        );
        self.prune_finished_endpoint_tasks("sink.start_runtime_endpoints.prune");
        if !lock_or_recover(&self.endpoint_tasks, "sink.start_runtime_endpoints").is_empty() {
            eprintln!(
                "fs_meta_sink: start_runtime_endpoints skip node={} reason=already-started elapsed_ms={}",
                node_id.0,
                start.elapsed().as_millis()
            );
            return Ok(());
        }

        self.disable_stream_receive();

        eprintln!(
            "fs_meta_sink: start_runtime_endpoints adapter begin node={} elapsed_ms={}",
            node_id.0,
            start.elapsed().as_millis()
        );
        let adapter =
            exchange_host_adapter(boundary.clone(), node_id.clone(), default_route_bindings());
        eprintln!(
            "fs_meta_sink: start_runtime_endpoints adapter ok node={} elapsed_ms={}",
            node_id.0,
            start.elapsed().as_millis()
        );
        let routes = default_route_bindings();

        let query_state = self.state.clone();
        let node_id_cloned = node_id.clone();
        let query_root_specs = self.root_specs.clone();
        let query_host_object_grants = self.host_object_grants.clone();
        let query_visibility_lag = self.visibility_lag.clone();
        let query_stream_delivery_stats = self.stream_delivery_stats.clone();
        let query_pending_stream_events = self.pending_stream_events.clone();
        let query_stream_receive_enabled = self.stream_receive_enabled.clone();
        let query_unit_control = self.unit_control.clone();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META, METHOD_QUERY) {
            eprintln!(
                "fs_meta_sink: start_runtime_endpoints query spawn begin node={} route={} elapsed_ms={}",
                node_id_cloned.0,
                route.0,
                start.elapsed().as_millis()
            );
            log::info!(
                "bound route listening on {}.{} for sink {}",
                ROUTE_TOKEN_FS_META,
                METHOD_QUERY,
                node_id_cloned.0
            );
            let endpoint = ManagedEndpointTask::spawn_with_unit(
                boundary.clone(),
                route,
                format!("sink:{}:{}", ROUTE_TOKEN_FS_META, METHOD_QUERY),
                SINK_RUNTIME_UNIT_ID,
                self.shutdown.clone(),
                {
                    let query_node_id = node_id_cloned.clone();
                    move |requests| {
                        let query_state = query_state.clone();
                        let query_root_specs = query_root_specs.clone();
                        let query_host_object_grants = query_host_object_grants.clone();
                        let query_visibility_lag = query_visibility_lag.clone();
                        let query_stream_delivery_stats = query_stream_delivery_stats.clone();
                        let query_pending_stream_events = query_pending_stream_events.clone();
                        let query_stream_receive_enabled = query_stream_receive_enabled.clone();
                        let query_unit_control = query_unit_control.clone();
                        let query_node_id = query_node_id.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                if let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                    req.payload_bytes(),
                                ) {
                                    let sink_impl = SinkFileMeta {
                                        node_id: query_node_id.clone(),
                                        state: query_state.clone(),
                                        root_specs: query_root_specs.clone(),
                                        host_object_grants: query_host_object_grants.clone(),
                                        visibility_lag: query_visibility_lag.clone(),
                                        stream_delivery_stats: query_stream_delivery_stats.clone(),
                                        pending_stream_events: query_pending_stream_events.clone(),
                                        stream_receive_enabled: query_stream_receive_enabled
                                            .clone(),
                                        unit_control: query_unit_control.clone(),
                                        stream_recv_observer: None,
                                        stream_recv_ready_notify: None,
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
                    }
                },
            );
            eprintln!(
                "fs_meta_sink: start_runtime_endpoints query spawn ok node={} elapsed_ms={}",
                node_id_cloned.0,
                start.elapsed().as_millis()
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
        let internal_query_stream_delivery_stats = self.stream_delivery_stats.clone();
        let internal_query_pending_stream_events = self.pending_stream_events.clone();
        let internal_query_stream_receive_enabled = self.stream_receive_enabled.clone();
        let internal_query_unit_control = self.unit_control.clone();
        let mut internal_query_routes = Vec::<RouteKey>::new();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY) {
            internal_query_routes.push(route);
        }
        internal_query_routes.push(sink_query_request_route_for(&node_id_cloned.0));
        for route in internal_query_routes {
            let internal_query_state_for_route = internal_query_state.clone();
            let internal_query_root_specs_for_route = internal_query_root_specs.clone();
            let internal_query_host_object_grants_for_route =
                internal_query_host_object_grants.clone();
            let internal_query_visibility_lag_for_route = internal_query_visibility_lag.clone();
            let internal_query_stream_delivery_stats_for_route =
                internal_query_stream_delivery_stats.clone();
            let internal_query_pending_stream_events_for_route =
                internal_query_pending_stream_events.clone();
            let internal_query_stream_receive_enabled_for_route =
                internal_query_stream_receive_enabled.clone();
            let internal_query_unit_control_for_route = internal_query_unit_control.clone();
            let route_key_for_trace = route.0.clone();
            eprintln!(
                "fs_meta_sink: start_runtime_endpoints internal_query spawn begin node={} route={} elapsed_ms={}",
                node_id_cloned.0,
                route.0,
                start.elapsed().as_millis()
            );
            log::info!(
                "bound route listening on {} for sink {}",
                route.0,
                node_id_cloned.0
            );
            let endpoint = ManagedEndpointTask::spawn_with_unit(
                boundary.clone(),
                route,
                format!(
                    "sink:{}:{}",
                    ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY
                ),
                SINK_RUNTIME_UNIT_ID,
                self.shutdown.clone(),
                {
                    let internal_query_node_id = node_id_cloned.clone();
                    move |requests| {
                        let internal_query_state = internal_query_state_for_route.clone();
                        let internal_query_root_specs = internal_query_root_specs_for_route.clone();
                        let internal_query_host_object_grants =
                            internal_query_host_object_grants_for_route.clone();
                        let internal_query_visibility_lag =
                            internal_query_visibility_lag_for_route.clone();
                        let internal_query_stream_delivery_stats =
                            internal_query_stream_delivery_stats_for_route.clone();
                        let internal_query_pending_stream_events =
                            internal_query_pending_stream_events_for_route.clone();
                        let internal_query_stream_receive_enabled =
                            internal_query_stream_receive_enabled_for_route.clone();
                        let internal_query_unit_control =
                            internal_query_unit_control_for_route.clone();
                        let internal_query_node_id = internal_query_node_id.clone();
                        let route_key_for_trace = route_key_for_trace.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                if let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                    req.payload_bytes(),
                                ) {
                                    if debug_sink_query_route_trace_enabled() {
                                        eprintln!(
                                            "fs_meta_sink: internal query route_request route={} correlation={:?} selected_group={:?} recursive={} path={}",
                                            route_key_for_trace,
                                            req.metadata().correlation_id,
                                            params.scope.selected_group,
                                            params.scope.recursive,
                                            String::from_utf8_lossy(&params.scope.path)
                                        );
                                    }
                                    let sink_impl = SinkFileMeta {
                                        node_id: internal_query_node_id.clone(),
                                        state: internal_query_state.clone(),
                                        root_specs: internal_query_root_specs.clone(),
                                        host_object_grants: internal_query_host_object_grants
                                            .clone(),
                                        visibility_lag: internal_query_visibility_lag.clone(),
                                        stream_delivery_stats: internal_query_stream_delivery_stats
                                            .clone(),
                                        pending_stream_events: internal_query_pending_stream_events
                                            .clone(),
                                        stream_receive_enabled:
                                            internal_query_stream_receive_enabled.clone(),
                                        unit_control: internal_query_unit_control.clone(),
                                        stream_recv_observer: None,
                                        stream_recv_ready_notify: None,
                                        shutdown: CancellationToken::new(),
                                        endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
                                    };
                                    let mut events =
                                        sink_impl.materialized_query(&params).unwrap_or_default();
                                    if debug_sink_query_route_trace_enabled() {
                                        eprintln!(
                                            "fs_meta_sink: internal query route_response route={} correlation={:?} events={}",
                                            route_key_for_trace,
                                            req.metadata().correlation_id,
                                            events.len()
                                        );
                                    }
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
                    }
                },
            );
            eprintln!(
                "fs_meta_sink: start_runtime_endpoints internal_query spawn ok node={} elapsed_ms={}",
                node_id_cloned.0,
                start.elapsed().as_millis()
            );
            lock_or_recover(
                &self.endpoint_tasks,
                "sink.start_runtime_endpoints.internal_query_tasks",
            )
            .push(endpoint);
        }

        const FORCE_FIND_SOURCE_REPLY_IDLE_GRACE_MS: u64 = 750;
        let node_id_proxy = node_id.clone();
        let node_id_proxy_log = node_id_proxy.clone();
        let proxy_adapter = adapter.clone();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META, METHOD_FIND) {
            eprintln!(
                "fs_meta_sink: start_runtime_endpoints find spawn begin node={} route={} elapsed_ms={}",
                node_id_proxy_log.0,
                route.0,
                start.elapsed().as_millis()
            );
            log::info!(
                "bound route listening on {}.{} for sink {}",
                ROUTE_TOKEN_FS_META,
                METHOD_FIND,
                node_id_proxy.0
            );
            let endpoint = ManagedEndpointTask::spawn_with_unit(
                boundary.clone(),
                route,
                format!("sink:{}:{}", ROUTE_TOKEN_FS_META, METHOD_FIND),
                SINK_RUNTIME_UNIT_ID,
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
            eprintln!(
                "fs_meta_sink: start_runtime_endpoints find spawn ok node={} elapsed_ms={}",
                node_id_proxy_log.0,
                start.elapsed().as_millis()
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
        let stream_delivery_stats = self.stream_delivery_stats.clone();
        let stream_receive_enabled = self.stream_receive_enabled.clone();
        let stream_unit_control = self.unit_control.clone();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM) {
            eprintln!(
                "fs_meta_sink: start_runtime_endpoints stream spawn begin node={} route={} elapsed_ms={}",
                node_id.0,
                route.0,
                start.elapsed().as_millis()
            );
            let stream_sink = Arc::new(SinkFileMeta {
                node_id: node_id.clone(),
                state: stream_state,
                root_specs: stream_root_specs,
                host_object_grants: stream_host_object_grants,
                visibility_lag: stream_visibility_lag,
                stream_delivery_stats,
                pending_stream_events: self.pending_stream_events.clone(),
                stream_receive_enabled: stream_receive_enabled,
                unit_control: stream_unit_control,
                stream_recv_observer: self.stream_recv_observer.clone(),
                stream_recv_ready_notify: self.stream_recv_ready_notify.clone(),
                shutdown: CancellationToken::new(),
                endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
            });
            let stream_sink_ready = stream_sink.clone();
            let stream_sink_wait = stream_sink.clone();
            let stream_sink_observe = stream_sink.clone();
            let stream_sink_apply = stream_sink.clone();
            let endpoint = ManagedEndpointTask::spawn_stream_with_before_recv_and_wait(
                boundary.clone(),
                route,
                format!("sink:{}:{}", ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM),
                SINK_RUNTIME_UNIT_ID,
                self.shutdown.clone(),
                move || stream_sink_ready.should_receive_stream_events(),
                move || {
                    let stream_sink_wait = stream_sink_wait.clone();
                    async move { stream_sink_wait.wait_until_stream_can_receive().await }.boxed()
                },
                move || stream_sink_observe.mark_before_stream_recv(),
                move |events| {
                    let stream_sink_apply = stream_sink_apply.clone();
                    async move {
                        if let Err(err) = stream_sink_apply.ingest_stream_events(&events) {
                            log::error!("sink stream ingest failed: {:?}", err);
                        }
                    }
                },
            );
            eprintln!(
                "fs_meta_sink: start_runtime_endpoints stream spawn ok node={} elapsed_ms={}",
                node_id.0,
                start.elapsed().as_millis()
            );
            lock_or_recover(
                &self.endpoint_tasks,
                "sink.start_runtime_endpoints.stream_tasks",
            )
            .push(endpoint);
        }

        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_ROOTS_CONTROL) {
            eprintln!(
                "fs_meta_sink: start_runtime_endpoints roots_control spawn begin node={} route={} elapsed_ms={}",
                node_id.0,
                route.0,
                start.elapsed().as_millis()
            );
            let roots_control_stream_receive_enabled = self.stream_receive_enabled.clone();
            let roots_control_route_key = route.0.clone();
            let sink = Arc::new(SinkFileMeta {
                node_id: self.node_id.clone(),
                state: self.state.clone(),
                root_specs: self.root_specs.clone(),
                host_object_grants: self.host_object_grants.clone(),
                visibility_lag: self.visibility_lag.clone(),
                stream_delivery_stats: self.stream_delivery_stats.clone(),
                pending_stream_events: self.pending_stream_events.clone(),
                stream_receive_enabled: roots_control_stream_receive_enabled,
                unit_control: self.unit_control.clone(),
                stream_recv_observer: None,
                stream_recv_ready_notify: None,
                shutdown: self.shutdown.clone(),
                endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
            });
            let roots_control_ready = sink.clone();
            let endpoint = ManagedEndpointTask::spawn_stream(
                boundary,
                route,
                format!(
                    "sink:{}:{}",
                    ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_ROOTS_CONTROL
                ),
                SINK_RUNTIME_UNIT_ID,
                self.shutdown.clone(),
                move || {
                    roots_control_ready
                        .should_receive_control_stream_route(&roots_control_route_key)
                },
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
            eprintln!(
                "fs_meta_sink: start_runtime_endpoints roots_control spawn ok node={} elapsed_ms={}",
                node_id.0,
                start.elapsed().as_millis()
            );
            lock_or_recover(
                &self.endpoint_tasks,
                "sink.start_runtime_endpoints.roots_control_tasks",
            )
            .push(endpoint);
        }

        eprintln!(
            "fs_meta_sink: start_runtime_endpoints ok node={} elapsed_ms={}",
            node_id.0,
            start.elapsed().as_millis()
        );
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
        self.prune_finished_endpoint_tasks("sink.start_stream_endpoint.prune");
        if !lock_or_recover(&self.endpoint_tasks, "sink.start_stream_endpoint").is_empty() {
            eprintln!(
                "fs_meta_sink: start_stream_endpoint skipped node={} reason=already-started",
                node_id.0
            );
            return Ok(());
        }

        self.disable_stream_receive();

        let routes = default_route_bindings();
        let stream_state = self.state.clone();
        let stream_root_specs = self.root_specs.clone();
        let stream_host_object_grants = self.host_object_grants.clone();
        let stream_visibility_lag = self.visibility_lag.clone();
        let stream_delivery_stats = self.stream_delivery_stats.clone();
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
                node_id: self.node_id.clone(),
                state: stream_state,
                root_specs: stream_root_specs,
                host_object_grants: stream_host_object_grants,
                visibility_lag: stream_visibility_lag,
                stream_delivery_stats,
                pending_stream_events: self.pending_stream_events.clone(),
                stream_receive_enabled: self.stream_receive_enabled.clone(),
                unit_control: stream_unit_control,
                stream_recv_observer: self.stream_recv_observer.clone(),
                stream_recv_ready_notify: self.stream_recv_ready_notify.clone(),
                shutdown: self.shutdown.clone(),
                endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
            });
            let stream_sink_ready = stream_sink.clone();
            let stream_sink_wait = stream_sink.clone();
            let stream_sink_observe = stream_sink.clone();
            let stream_sink_apply = stream_sink.clone();
            let endpoint = ManagedEndpointTask::spawn_stream_with_before_recv_and_wait(
                boundary,
                route,
                format!("sink:{}:{}", ROUTE_TOKEN_FS_META_EVENTS, METHOD_STREAM),
                SINK_RUNTIME_UNIT_ID,
                self.shutdown.clone(),
                move || stream_sink_ready.should_receive_stream_events(),
                move || {
                    let stream_sink_wait = stream_sink_wait.clone();
                    async move { stream_sink_wait.wait_until_stream_can_receive().await }.boxed()
                },
                move || stream_sink_observe.mark_before_stream_recv(),
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

        self.enable_stream_receive();

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
        } else if unit == SinkRuntimeUnit::Sink && !bound_scopes.is_empty() {
            // Sink holder routes should converge on one current scoped holder truth.
            // When a successor runtime.exec.sink activate arrives for the same scopes,
            // keep active routes aligned to the latest bound scope/resource mapping
            // instead of merging stale dead-holder refs into the exported state.
            self.unit_control
                .sync_active_scopes(unit_id, bound_scopes)?;
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
        {
            let mut state = self.state.write()?;
            state.reconcile_host_object_grants(&roots, host_object_grants, allowed_groups.as_ref());
        }
        self.state.persist_snapshot()?;
        Ok(())
    }

    fn apply_deactivate_signal(
        &self,
        unit: SinkRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> Result<()> {
        let unit_id = unit.unit_id();
        if debug_sink_query_route_trace_enabled()
            && unit_id == SINK_RUNTIME_UNIT_ID
            && is_per_peer_sink_query_request_route(route_key)
        {
            let before_generation = self.unit_control.route_generation(unit_id, route_key)?;
            let endpoint_tasks =
                lock_or_recover(&self.endpoint_tasks, "sink.trace_route_deactivate.before");
            eprintln!(
                "fs_meta_sink: traced_route_deactivate before route={} generation={} gate_generation={:?} endpoint_tasks={:?}",
                route_key,
                generation,
                before_generation,
                matching_endpoint_task_states(&endpoint_tasks, route_key)
            );
            eprintln!(
                "fs_meta_sink: traced_route_deactivate locality route={} locality={}",
                route_key,
                per_peer_sink_query_route_locality(&self.node_id, route_key)
            );
        }
        let accepted = self
            .unit_control
            .apply_deactivate(unit_id, route_key, generation)?;
        if debug_sink_query_route_trace_enabled()
            && unit_id == SINK_RUNTIME_UNIT_ID
            && is_per_peer_sink_query_request_route(route_key)
        {
            let after_generation = self.unit_control.route_generation(unit_id, route_key)?;
            let endpoint_tasks =
                lock_or_recover(&self.endpoint_tasks, "sink.trace_route_deactivate.after");
            eprintln!(
                "fs_meta_sink: traced_route_deactivate after route={} generation={} accepted={} gate_generation={:?} endpoint_tasks={:?}",
                route_key,
                generation,
                accepted,
                after_generation,
                matching_endpoint_task_states(&endpoint_tasks, route_key)
            );
        }
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
        let mut activated_events_stream_route = false;
        let stream_recv_epoch_before = self.stream_recv_epoch();
        let stream_receivable_before = self.should_receive_stream_events();
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
                    if debug_control_scope_capture_enabled() {
                        eprintln!(
                            "fs_meta_sink: control_scope_capture node={} unit={} route={} generation={} scopes={:?}",
                            self.node_id.0,
                            unit.unit_id(),
                            route_key,
                            generation,
                            summarize_bound_scopes(bound_scopes)
                        );
                    }
                    self.apply_activate_signal(*unit, route_key, *generation, bound_scopes)?;
                    if *unit == SinkRuntimeUnit::Sink
                        && route_key == &format!("{}.{}", ROUTE_KEY_EVENTS, METHOD_STREAM)
                    {
                        activated_events_stream_route = true;
                    }
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
                    // `RuntimeUnitTick` stays explicit at the sink boundary even
                    // though runtime::orchestration pre-decodes the envelope into
                    // `SinkControlSignal::Tick`.
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
        self.notify_stream_recv_waiters();
        if activated_events_stream_route {
            self.wait_until_stream_recv_observed_after_activation(
                stream_receivable_before,
                stream_recv_epoch_before,
            )
            .await;
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
        eprintln!(
            "fs_meta_sink: update_logical_roots begin roots={} grants={}",
            roots.len(),
            host_object_grants.len()
        );
        let root_count = roots.len();
        let grant_count = host_object_grants.len();
        let current_allowed_groups = self.scheduled_group_ids()?;
        if let Some(current_allowed_groups) = current_allowed_groups.as_ref() {
            let root_ids = roots
                .iter()
                .map(|root| root.id.clone())
                .collect::<BTreeSet<_>>();
            let restorable_group_ids = self
                .state
                .read()?
                .retained_groups
                .iter()
                .filter(|(_, group)| {
                    group.preserves_materialized_state_for_omission_window()
                        && !matches!(
                            group.group_readiness_state(),
                            GroupReadinessState::PendingMaterialization
                        )
                })
                .map(|(group_id, _)| group_id.clone())
                .collect::<BTreeSet<_>>();
            let preserve_current_subset =
                !current_allowed_groups.is_empty() && current_allowed_groups.is_subset(&root_ids);
            let next_allowed_groups = if preserve_current_subset {
                current_allowed_groups
                    .iter()
                    .chain(restorable_group_ids.iter())
                    .filter(|group_id| root_ids.contains(*group_id))
                    .cloned()
                    .collect::<BTreeSet<_>>()
            } else {
                root_ids
            };
            let bound_scopes = roots
                .iter()
                .filter(|root| next_allowed_groups.contains(&root.id))
                .map(|root| RuntimeBoundScope {
                    scope_id: root.id.clone(),
                    resource_ids: Vec::new(),
                })
                .collect::<Vec<_>>();
            self.unit_control
                .sync_active_scopes(SINK_RUNTIME_UNIT_ID, &bound_scopes)?;
        }
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
        eprintln!(
            "fs_meta_sink: update_logical_roots ok roots={} grants={}",
            root_count, grant_count
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
            groups.push(SinkGroupStatusSnapshot::from_status_fields(
                group_id.clone(),
                group.primary_object_ref.clone(),
                group.tree.node_count() as u64,
                stats.live_nodes,
                stats.tombstoned_count,
                stats.attested_count,
                stats.suspect_count,
                stats.blind_spot_count,
                stats.shadow_time_us,
                if stats.shadow_time_us == 0 {
                    0
                } else {
                    now.saturating_sub(stats.shadow_time_us)
                },
                group.overflow_pending_materialization,
                group.group_readiness_state(),
                group.materialized_revision,
                estimated_heap_bytes,
            ));
        }
        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        snapshot.groups = groups;
        if let Some(groups) = self.scheduled_group_ids()?
            && !groups.is_empty()
        {
            snapshot
                .scheduled_groups_by_node
                .insert(self.node_id.0.clone(), groups.into_iter().collect());
        }
        snapshot.stream_path_capture_target = debug_stream_path_capture_target()
            .map(|target| String::from_utf8_lossy(&target).into_owned());
        if let Ok(stats) = self.stream_delivery_stats.lock() {
            if stats.received_batches > 0 {
                snapshot
                    .stream_received_batches_by_node
                    .insert(self.node_id.0.clone(), stats.received_batches);
            }
            if stats.received_events > 0 {
                snapshot
                    .stream_received_events_by_node
                    .insert(self.node_id.0.clone(), stats.received_events);
            }
            if !stats.received_origin_counts.is_empty() {
                snapshot.stream_received_origin_counts_by_node.insert(
                    self.node_id.0.clone(),
                    format_origin_counts(&stats.received_origin_counts),
                );
            }
            if !stats.received_path_origin_counts.is_empty() {
                snapshot.stream_received_path_origin_counts_by_node.insert(
                    self.node_id.0.clone(),
                    format_origin_counts(&stats.received_path_origin_counts),
                );
            }
            if !stats.ready_origin_counts.is_empty() {
                snapshot.stream_ready_origin_counts_by_node.insert(
                    self.node_id.0.clone(),
                    format_origin_counts(&stats.ready_origin_counts),
                );
            }
            if !stats.ready_path_origin_counts.is_empty() {
                snapshot.stream_ready_path_origin_counts_by_node.insert(
                    self.node_id.0.clone(),
                    format_origin_counts(&stats.ready_path_origin_counts),
                );
            }
            if !stats.deferred_origin_counts.is_empty() {
                snapshot.stream_deferred_origin_counts_by_node.insert(
                    self.node_id.0.clone(),
                    format_origin_counts(&stats.deferred_origin_counts),
                );
            }
            if !stats.dropped_origin_counts.is_empty() {
                snapshot.stream_dropped_origin_counts_by_node.insert(
                    self.node_id.0.clone(),
                    format_origin_counts(&stats.dropped_origin_counts),
                );
            }
            if stats.applied_batches > 0 {
                snapshot
                    .stream_applied_batches_by_node
                    .insert(self.node_id.0.clone(), stats.applied_batches);
            }
            if stats.applied_events > 0 {
                snapshot
                    .stream_applied_events_by_node
                    .insert(self.node_id.0.clone(), stats.applied_events);
            }
            if stats.applied_control_events > 0 {
                snapshot
                    .stream_applied_control_events_by_node
                    .insert(self.node_id.0.clone(), stats.applied_control_events);
            }
            if stats.applied_data_events > 0 {
                snapshot
                    .stream_applied_data_events_by_node
                    .insert(self.node_id.0.clone(), stats.applied_data_events);
            }
            if !stats.applied_origin_counts.is_empty() {
                snapshot.stream_applied_origin_counts_by_node.insert(
                    self.node_id.0.clone(),
                    format_origin_counts(&stats.applied_origin_counts),
                );
            }
            if !stats.applied_path_origin_counts.is_empty() {
                snapshot.stream_applied_path_origin_counts_by_node.insert(
                    self.node_id.0.clone(),
                    format_origin_counts(&stats.applied_path_origin_counts),
                );
            }
            if stats.last_applied_at_us > 0 {
                snapshot
                    .stream_last_applied_at_us_by_node
                    .insert(self.node_id.0.clone(), stats.last_applied_at_us);
            }
        }
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

    fn should_receive_stream_events(&self) -> bool {
        let route_key = format!("{}.{}", ROUTE_KEY_EVENTS, METHOD_STREAM);
        self.stream_receive_enabled.load(Ordering::Acquire)
            && self.has_scheduled_stream_targets()
            && self.should_receive_control_stream_route(&route_key)
    }

    fn should_receive_control_stream_route(&self, route_key: &str) -> bool {
        let Ok(Some(generation)) = self
            .unit_control
            .route_generation(SINK_RUNTIME_UNIT_ID, route_key)
        else {
            return false;
        };
        self.unit_control
            .accept_tick(SINK_RUNTIME_UNIT_ID, route_key, generation)
            .unwrap_or(false)
    }

    pub(crate) fn enable_stream_receive(&self) {
        self.stream_receive_enabled.store(true, Ordering::Release);
        self.notify_stream_recv_waiters();
    }

    pub(crate) fn disable_stream_receive(&self) {
        self.stream_receive_enabled.store(false, Ordering::Release);
        self.notify_stream_recv_waiters();
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
        let incoming_counts = collect_event_origin_counts(events);
        let capture_target = debug_stream_path_capture_target();
        let incoming_path_counts = capture_target
            .as_deref()
            .map(|target| collect_event_origin_counts_for_query_path(events, target))
            .unwrap_or_default();
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

        let ready_counts = collect_event_origin_counts(&ready);
        let ready_path_counts = capture_target
            .as_deref()
            .map(|target| collect_event_origin_counts_for_query_path(&ready, target))
            .unwrap_or_default();
        let ready_control_count = ready
            .iter()
            .filter(|event| rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok())
            .count() as u64;
        let ready_data_count = ready.len() as u64 - ready_control_count;
        let deferred_events = deferred.iter().cloned().collect::<Vec<_>>();
        let deferred_counts = collect_event_origin_counts(&deferred_events);
        let dropped_counts = incoming_counts
            .iter()
            .map(|(origin, count)| {
                let ready = ready_counts.get(origin).copied().unwrap_or(0);
                let deferred = deferred_counts.get(origin).copied().unwrap_or(0);
                (origin.clone(), count.saturating_sub(ready + deferred))
            })
            .filter(|(_, count)| *count > 0)
            .collect::<BTreeMap<_, _>>();

        {
            let mut stats = lock_or_recover(
                &self.stream_delivery_stats,
                "sink.ingest_stream_events.stream_delivery_stats",
            );
            stats.received_batches = stats.received_batches.saturating_add(1);
            stats.received_events = stats.received_events.saturating_add(events.len() as u64);
            accumulate_origin_counts(&mut stats.received_origin_counts, &incoming_counts);
            accumulate_origin_counts(
                &mut stats.received_path_origin_counts,
                &incoming_path_counts,
            );
            accumulate_origin_counts(&mut stats.ready_origin_counts, &ready_counts);
            accumulate_origin_counts(&mut stats.ready_path_origin_counts, &ready_path_counts);
            accumulate_origin_counts(&mut stats.deferred_origin_counts, &deferred_counts);
            accumulate_origin_counts(&mut stats.dropped_origin_counts, &dropped_counts);
        }

        if debug_stream_delivery_enabled() {
            eprintln!(
                "fs_meta_sink: ingest_stream_events classify node={} configured={:?} scheduled={:?} incoming={:?} ready={:?} deferred={:?} dropped={}",
                self.node_id.0,
                configured,
                scheduled,
                format_origin_counts(&incoming_counts),
                format_origin_counts(&ready_counts),
                format_origin_counts(&deferred_counts),
                dropped
            );
            if let Some(target) = capture_target.as_deref() {
                eprintln!(
                    "fs_meta_sink: ingest_stream_events path_capture node={} target={} received={:?} ready={:?}",
                    self.node_id.0,
                    String::from_utf8_lossy(target),
                    format_origin_counts(&incoming_path_counts),
                    format_origin_counts(&ready_path_counts),
                );
            }
            eprintln!(
                "fs_meta_sink: ingest_stream_events counts node={} ready={} deferred={} dropped={}",
                self.node_id.0,
                ready.len(),
                deferred_events.len(),
                dropped
            );
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
            let mut stats = lock_or_recover(
                &self.stream_delivery_stats,
                "sink.ingest_stream_events.stream_delivery_stats.applied",
            );
            stats.applied_batches = stats.applied_batches.saturating_add(1);
            stats.applied_events = stats.applied_events.saturating_add(ready.len() as u64);
            stats.applied_control_events = stats
                .applied_control_events
                .saturating_add(ready_control_count);
            stats.applied_data_events = stats.applied_data_events.saturating_add(ready_data_count);
            accumulate_origin_counts(&mut stats.applied_origin_counts, &ready_counts);
            accumulate_origin_counts(&mut stats.applied_path_origin_counts, &ready_path_counts);
            stats.last_applied_at_us = now_us();
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
            let ready_counts = collect_event_origin_counts(&ready);
            let ready_path_counts = debug_stream_path_capture_target()
                .as_deref()
                .map(|target| collect_event_origin_counts_for_query_path(&ready, target))
                .unwrap_or_default();
            let ready_control_count = ready
                .iter()
                .filter(|event| {
                    rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok()
                })
                .count() as u64;
            let ready_data_count = ready.len() as u64 - ready_control_count;
            let mut stats = lock_or_recover(
                &self.stream_delivery_stats,
                "sink.flush_buffered_stream_events.stream_delivery_stats.applied",
            );
            stats.applied_batches = stats.applied_batches.saturating_add(1);
            stats.applied_events = stats.applied_events.saturating_add(ready.len() as u64);
            stats.applied_control_events = stats
                .applied_control_events
                .saturating_add(ready_control_count);
            stats.applied_data_events = stats.applied_data_events.saturating_add(ready_data_count);
            accumulate_origin_counts(&mut stats.applied_origin_counts, &ready_counts);
            accumulate_origin_counts(&mut stats.applied_path_origin_counts, &ready_path_counts);
            stats.last_applied_at_us = now_us();
        }
        Ok(())
    }

    fn apply_events(&self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let debug_apply_events = std::env::var_os("FSMETA_DEBUG_SINK_APPLY_EVENTS").is_some();
        if debug_apply_events {
            let mut incoming_by_origin = BTreeMap::<String, usize>::new();
            for event in events {
                *incoming_by_origin
                    .entry(event.metadata().origin_id.0.clone())
                    .or_insert(0) += 1;
            }
            eprintln!(
                "fs_meta_sink: apply_events incoming total={} origins={:?}",
                events.len(),
                incoming_by_origin
            );
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
                        group_state.overflow_pending_materialization = true;
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
                                group_state.refresh_materialization_readiness();
                            }
                            if matches!(
                                control,
                                ControlEvent::EpochEnd {
                                    epoch_type: crate::EpochType::Audit,
                                    ..
                                }
                            ) {
                                group_state.mark_materialization_ready();
                                if group_state.overflow_pending_materialization {
                                    group_state.last_coverage_recovered_at = Some(Instant::now());
                                    group_state.materialized_revision =
                                        group_state.materialized_revision.saturating_add(1);
                                }
                                group_state.overflow_pending_materialization = false;
                            }
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
            group_state.refresh_materialization_readiness();
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
        }

        if debug_apply_events {
            eprintln!(
                "fs_meta_sink: apply_events processed total={} control={} data={} skipped={}",
                events.len(),
                control_events,
                data_events,
                skipped_events
            );
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
        self.state.persist_snapshot()?;

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
        let debug_materialized_query =
            std::env::var_os("FSMETA_DEBUG_SINK_MATERIALIZED_QUERY").is_some();

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
                        group.overflow_pending_materialization,
                        request.scope.recursive,
                        request.scope.max_depth,
                        tree_options.read_class,
                        group.last_coverage_recovered_at,
                    );
                    if debug_materialized_query {
                        let live_nodes = group
                            .tree
                            .aggregate_at(b"/")
                            .map(|aggregate| aggregate.total_nodes)
                            .unwrap_or(0);
                        eprintln!(
                            "fs_meta_sink: materialized_query group={} selected={:?} path={} recursive={} node_count={} live_nodes={} readiness={:?} root_exists={} entries={} has_children={}",
                            group_id,
                            selected_group,
                            String::from_utf8_lossy(&dir_path),
                            request.scope.recursive,
                            group.tree.node_count(),
                            live_nodes,
                            group.group_readiness_state(),
                            response.root.exists,
                            response.entries.len(),
                            response.root.has_children
                        );
                    }
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
        self.disable_stream_receive();
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
        self.state.persist_snapshot()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
