use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, header},
};
use capanix_app_sdk::CnxError;
use capanix_app_sdk::Event;
use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_host_adapter_fs::HostAdapter;
use capanix_runtime_entry_sdk::advanced::boundary::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelSendRequest,
};
#[cfg(test)]
use tokio::sync::Notify;

use crate::domain_state::FacadeServiceState;
use crate::query::api::{
    internal_status_request_payload, merge_sink_status_snapshots,
    refresh_policy_from_host_object_grants, route_sink_status_snapshot,
};
use crate::query::observation::{
    ObservationTrustPolicy, candidate_group_observation_evidence, evaluate_observation_status,
};
use crate::runtime::execution_units;
use crate::runtime::orchestration::encode_logical_roots_control_payload;
use crate::runtime::orchestration::encode_manual_rescan_envelope;
use crate::runtime::routes::ROUTE_KEY_SOURCE_RESCAN_CONTROL;
#[cfg(test)]
use crate::runtime::routes::source_rescan_route_key_for;
use crate::runtime::routes::{
    METHOD_SOURCE_STATUS, ROUTE_KEY_SINK_ROOTS_CONTROL, ROUTE_KEY_SOURCE_ROOTS_CONTROL,
    ROUTE_TOKEN_FS_META_INTERNAL, default_route_bindings,
};
use crate::runtime::seam::exchange_host_adapter;
use crate::sink::SinkStatusSnapshot;
use crate::source::config::{GrantedMountRoot, RootSelector, RootSpec};
use crate::workers::source::SourceObservabilitySnapshot;

use super::auth::SessionPrincipal;
use super::errors::ApiError;
#[cfg(test)]
use super::facade_status::FacadePendingReason;
#[cfg(test)]
use super::facade_status::PublishedFacadeStatusReader;
#[cfg(test)]
use super::facade_status::SharedFacadePendingStatus;
use super::state::ApiState;
use super::types::{
    CreateQueryApiKeyRequest, CreateQueryApiKeyResponse, DegradedRoot, LoginRequest, LoginResponse,
    QueryApiKeysResponse, RescanResponse, RevokeQueryApiKeyResponse, RootEntry, RootPreviewItem,
    RootSelectorEntry, RootUpdateEntry, RootsPreviewResponse, RootsResponse, RootsUpdateRequest,
    RootsUpdateResponse, RuntimeGrantsResponse, StatusFacade, StatusFacadePending, StatusResponse,
    StatusSink, StatusSinkDebug, StatusSinkGroup, StatusSinkGroupReadiness, StatusSource,
    StatusSourceConcreteRoot,
    StatusSourceLogicalRoot,
};

fn debug_status_route_fanin_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_STATUS_ROUTE_FANIN")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn debug_force_find_runner_capture_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_FORCE_FIND_RUNNER_CAPTURE")
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

fn summarize_group_string_map(groups: &BTreeMap<String, String>) -> Vec<String> {
    groups
        .iter()
        .map(|(group_id, value)| format!("{group_id}={value}"))
        .collect()
}

fn summarize_source_status_route_snapshot(snapshot: &SourceObservabilitySnapshot) -> String {
    format!(
        "primaries={} runners={} last_runner={:?} inflight={:?} source={:?} scan={:?} control={:?} published_batches={:?} published_events={:?} published_origins={:?} published_origin_counts={:?} enqueued_path_counts={:?} pending_path_counts={:?} yielded_path_counts={:?} summarized_path_counts={:?} published_path_counts={:?}",
        snapshot.source_primary_by_group.len(),
        snapshot.last_force_find_runner_by_group.len(),
        summarize_group_string_map(&snapshot.last_force_find_runner_by_group),
        snapshot.force_find_inflight_groups,
        summarize_groups_by_node(&snapshot.scheduled_source_groups_by_node),
        summarize_groups_by_node(&snapshot.scheduled_scan_groups_by_node),
        summarize_groups_by_node(&snapshot.last_control_frame_signals_by_node),
        snapshot.published_batches_by_node,
        snapshot.published_events_by_node,
        summarize_groups_by_node(&snapshot.last_published_origins_by_node),
        summarize_groups_by_node(&snapshot.published_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.enqueued_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.pending_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.yielded_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.summarized_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.published_path_origin_counts_by_node),
    )
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct RootsPutPauseHook {
    pub entered: std::sync::Arc<Notify>,
    pub release: std::sync::Arc<Notify>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct RootsPutBeforeResponseHook {
    pub entered: std::sync::Arc<Notify>,
    pub release: std::sync::Arc<Notify>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct RescanPauseHook {
    pub entered: std::sync::Arc<Notify>,
    pub release: std::sync::Arc<Notify>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct StatusPauseHook {
    pub entered: std::sync::Arc<Notify>,
    pub release: std::sync::Arc<Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct StatusRouteTraceHook {
    events: std::sync::Arc<StdMutex<Vec<String>>>,
}

#[cfg(test)]
fn roots_put_pause_hook_cell() -> &'static StdMutex<Option<RootsPutPauseHook>> {
    static CELL: OnceLock<StdMutex<Option<RootsPutPauseHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn roots_put_before_response_hook_cell() -> &'static StdMutex<Option<RootsPutBeforeResponseHook>> {
    static CELL: OnceLock<StdMutex<Option<RootsPutBeforeResponseHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn rescan_pause_hook_cell() -> &'static StdMutex<Option<RescanPauseHook>> {
    static CELL: OnceLock<StdMutex<Option<RescanPauseHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn status_pause_hook_cell() -> &'static StdMutex<Option<StatusPauseHook>> {
    static CELL: OnceLock<StdMutex<Option<StatusPauseHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn status_route_trace_hook_cell() -> &'static StdMutex<Option<StatusRouteTraceHook>> {
    static CELL: OnceLock<StdMutex<Option<StatusRouteTraceHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
pub(crate) fn install_roots_put_pause_hook(hook: RootsPutPauseHook) {
    let mut guard = match roots_put_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_roots_put_before_response_hook(hook: RootsPutBeforeResponseHook) {
    let mut guard = match roots_put_before_response_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_rescan_pause_hook(hook: RescanPauseHook) {
    let mut guard = match rescan_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_status_pause_hook(hook: StatusPauseHook) {
    let mut guard = match status_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn install_status_route_trace_hook(hook: StatusRouteTraceHook) {
    let mut guard = match status_route_trace_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_status_route_trace_capture(events: std::sync::Arc<StdMutex<Vec<String>>>) {
    install_status_route_trace_hook(StatusRouteTraceHook { events });
}

#[cfg(test)]
pub(crate) fn clear_roots_put_pause_hook() {
    let mut guard = match roots_put_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_roots_put_before_response_hook() {
    let mut guard = match roots_put_before_response_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_rescan_pause_hook() {
    let mut guard = match rescan_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_status_pause_hook() {
    let mut guard = match status_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn clear_status_route_trace_hook() {
    let mut guard = match status_route_trace_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_status_route_trace_capture() {
    clear_status_route_trace_hook();
}

#[cfg(test)]
async fn maybe_pause_roots_put_after_previous_source_roots() {
    let hook = {
        let guard = match roots_put_pause_hook_cell().lock() {
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
async fn maybe_pause_roots_put_before_response() {
    let hook = {
        let guard = match roots_put_before_response_hook_cell().lock() {
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
async fn maybe_pause_rescan_before_return() {
    let hook = {
        let guard = match rescan_pause_hook_cell().lock() {
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
async fn maybe_pause_status_before_remote_collection() {
    let hook = {
        let guard = match status_pause_hook_cell().lock() {
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

fn next_status_trace_request_id() -> Option<u64> {
    #[cfg(test)]
    {
        static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
        Some(NEXT_REQUEST_ID.fetch_add(1, Ordering::SeqCst))
    }
    #[cfg(not(test))]
    {
        None
    }
}

fn record_status_route_trace(_trace_id: Option<u64>, _stage: impl Into<String>) {
    #[cfg(test)]
    if let Some(trace_id) = _trace_id {
        let hook = {
            let guard = match status_route_trace_hook_cell().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.clone()
        };
        if let Some(hook) = hook {
            let mut events = match hook.events.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            events.push(format!("{trace_id}:{}", _stage.into()));
        }
    }
}

fn summarize_sorted_set(groups: &BTreeSet<String>) -> String {
    if groups.is_empty() {
        return "-".to_string();
    }
    groups.iter().cloned().collect::<Vec<_>>().join("|")
}

fn summarize_candidate_sink_group_state(
    sink_status: &SinkStatusSnapshot,
    candidate_groups: &BTreeSet<String>,
) -> String {
    if candidate_groups.is_empty() {
        return "-".to_string();
    }
    let sink_groups = sink_status
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<BTreeMap<_, _>>();
    candidate_groups
        .iter()
        .map(|group_id| match sink_groups.get(group_id.as_str()) {
            Some(group) => format!(
                "{group_id}:initial_audit_completed={}:overflow_pending_audit={}",
                group.initial_audit_completed, group.overflow_pending_audit
            ),
            None => format!("{group_id}:missing"),
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub async fn login(
    State(state): State<ApiState>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, ApiError> {
    if req.username.trim().is_empty() || req.password.is_empty() {
        return Err(ApiError::bad_request("username/password must not be empty"));
    }
    let (token, expires_in_secs, user) = state.auth.login(&req.username, &req.password)?;
    Ok(Json(LoginResponse {
        token,
        expires_in_secs,
        user,
    }))
}

pub async fn status(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, ApiError> {
    let _ = authorize_management(&state, &headers)?;
    let started_at = Instant::now();
    let trace_id = next_status_trace_request_id();
    record_status_route_trace(trace_id, "status.enter");

    let local_sink = match state.sink.status_snapshot_nonblocking().await {
        Ok(snapshot) => snapshot,
        Err(err) if state.sink.is_worker() => {
            log::warn!("status falling back to default cached sink snapshot: {err}");
            Default::default()
        }
        Err(err) => {
            return Err(ApiError::internal(format!("sink status failed: {err}")));
        }
    };
    let local_source = state.source.observability_snapshot_nonblocking().await;
    if state.source.is_worker() || state.sink.is_worker() {
        let active_candidate_groups =
            snapshot_scoped_active_facade_candidate_groups(&local_source, &local_sink);
        record_status_route_trace(
            trace_id,
            format!(
                "status.local.gate candidates={} source_scan={} source_degraded={} initial_audit={} overflow_pending={} sink_groups={}{}",
                summarize_sorted_set(&active_candidate_groups),
                summarize_sorted_set(&active_candidate_groups),
                summarize_sorted_set(
                    &candidate_group_observation_evidence(
                        &local_source.status,
                        &local_sink,
                        &active_candidate_groups,
                    )
                    .degraded_groups
                ),
                summarize_sorted_set(
                    &candidate_group_observation_evidence(
                        &local_source.status,
                        &local_sink,
                        &active_candidate_groups,
                    )
                    .initial_audit_groups
                ),
                summarize_sorted_set(
                    &candidate_group_observation_evidence(
                        &local_source.status,
                        &local_sink,
                        &active_candidate_groups,
                    )
                    .overflow_pending_groups
                ),
                summarize_candidate_sink_group_state(&local_sink, &active_candidate_groups),
                if active_candidate_groups.is_empty() {
                    " state=SkippedNoCandidateGroups".to_string()
                } else {
                    let local_observation_status = evaluate_observation_status(
                        &candidate_group_observation_evidence(
                            &local_source.status,
                            &local_sink,
                            &active_candidate_groups,
                        ),
                        ObservationTrustPolicy::candidate_generation(),
                    );
                    format!(
                        " state={:?} reasons={}",
                        local_observation_status.state,
                        local_observation_status.reasons.join(" ; ")
                    )
                }
            ),
        );
        // Management /status remains available while active facade observation is still converging.
        // Facade readiness is reported via published facade state and pending diagnostics rather than
        // by fail-closing the management surface on observation trust.
    }
    let _facade_request_guard = state.request_tracker.begin();
    #[cfg(test)]
    maybe_pause_status_before_remote_collection().await;
    record_status_route_trace(trace_id, "status.remote.begin");
    let status_collect = |boundary, origin_id| {
        collect_remote_status_snapshots_on_shared_boundary(
            local_sink.clone(),
            local_source.clone(),
            boundary,
            origin_id,
            trace_id,
        )
    };
    // Management /status should prefer the management/runtime boundary when present.
    // The query boundary can churn independently during generation-two turnover,
    // but it is still a valid second chance when the preferred runtime boundary
    // fully transport-collapses before any remote status evidence arrives.
    let runtime_boundary = state.runtime_boundary.clone();
    let query_boundary = state.query_runtime_boundary.clone();
    let (sink_status, source, runner_sets, sink_outcome, source_outcome) = match runtime_boundary {
        Some(boundary) => {
            let initial = status_collect(boundary, state.node_id.clone()).await;
            let runtime_fully_unavailable =
                status_route_collection_incomplete(initial.3, initial.4);
            if runtime_fully_unavailable {
                if let Some(query_boundary) = query_boundary {
                    let different_boundary =
                        state
                            .runtime_boundary
                            .as_ref()
                            .is_none_or(|runtime_boundary| {
                                !std::sync::Arc::ptr_eq(runtime_boundary, &query_boundary)
                            });
                    if different_boundary {
                        status_collect(query_boundary, state.node_id.clone()).await
                    } else {
                        initial
                    }
                } else {
                    initial
                }
            } else {
                initial
            }
        }
        None => {
            if let Some(boundary) = query_boundary {
                status_collect(boundary, state.node_id.clone()).await
            } else {
                (
                    local_sink,
                    local_source.clone(),
                    local_runner_sets(&local_source),
                    StatusRouteOutcome::Skipped,
                    StatusRouteOutcome::Skipped,
                )
            }
        }
    };
    record_status_route_trace(
        trace_id,
        format!(
            "status.remote.done sink={} source={}",
            sink_outcome.as_str(),
            source_outcome.as_str()
        ),
    );
    let published_facade_status = state.published_facade_status.snapshot();
    let published_facade_state = published_facade_status.state;
    let live_source_nodes = active_source_node_count(&source);
    let gate_inflight = state
        .force_find_inflight
        .lock()
        .map(|guard| guard.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let mut merged_inflight = source.force_find_inflight_groups.clone();
    merged_inflight.extend(gate_inflight);
    merged_inflight.sort();
    merged_inflight.dedup();
    eprintln!(
        "fs_meta_api_status: sink_route={} source_route={} elapsed_ms={} sink_groups={} source_runners={:?} inflight={:?}",
        sink_outcome.as_str(),
        source_outcome.as_str(),
        started_at.elapsed().as_millis(),
        sink_status.groups.len(),
        runner_sets,
        merged_inflight
    );
    if should_fail_closed_status_route_collection(
        sink_outcome,
        source_outcome,
        published_facade_state,
    ) {
        return Err(ApiError::service_unavailable(format!(
            "status remote route collection incomplete: sink_route={} source_route={}",
            sink_outcome.as_str(),
            source_outcome.as_str()
        )));
    }
    if debug_force_find_runner_capture_enabled() {
        eprintln!(
            "fs_meta_api_status: runner_capture node={} source_route={} last_runner={:?} runner_sets={:?} inflight={:?}",
            state.node_id.0,
            source_outcome.as_str(),
            summarize_group_string_map(&source.last_force_find_runner_by_group),
            runner_sets,
            merged_inflight
        );
    }

    Ok(Json(StatusResponse {
        source: status_source_from_observability(source, runner_sets, merged_inflight),
        sink: StatusSink {
            live_nodes: status_sink_live_nodes(live_source_nodes, &sink_status),
            tombstoned_count: sink_status.tombstoned_count,
            attested_count: sink_status.attested_count,
            suspect_count: sink_status.suspect_count,
            blind_spot_count: sink_status.blind_spot_count,
            shadow_time_us: sink_status.shadow_time_us,
            estimated_heap_bytes: sink_status.estimated_heap_bytes,
            groups: sink_status
                .groups
                .into_iter()
                .map(|group| StatusSinkGroup {
                    group_id: group.group_id,
                    primary_object_ref: group.primary_object_ref,
                    total_nodes: group.total_nodes,
                    live_nodes: group.live_nodes,
                    tombstoned_count: group.tombstoned_count,
                    attested_count: group.attested_count,
                    suspect_count: group.suspect_count,
                    blind_spot_count: group.blind_spot_count,
                    shadow_time_us: group.shadow_time_us,
                    shadow_lag_us: group.shadow_lag_us,
                    overflow_pending_audit: group.overflow_pending_audit,
                    readiness: match group.readiness {
                        crate::sink::GroupReadinessState::PendingAudit => {
                            StatusSinkGroupReadiness::PendingAudit
                        }
                        crate::sink::GroupReadinessState::WaitingForMaterializedRoot => {
                            StatusSinkGroupReadiness::WaitingForMaterializedRoot
                        }
                        crate::sink::GroupReadinessState::Ready => {
                            StatusSinkGroupReadiness::Ready
                        }
                    },
                    initial_audit_completed: group.initial_audit_completed,
                    estimated_heap_bytes: group.estimated_heap_bytes,
                })
                .collect(),
            debug: StatusSinkDebug {
                scheduled_groups_by_node: sink_status.scheduled_groups_by_node,
                last_control_frame_signals_by_node: sink_status.last_control_frame_signals_by_node,
                received_batches_by_node: sink_status.received_batches_by_node,
                received_events_by_node: sink_status.received_events_by_node,
                received_control_events_by_node: sink_status.received_control_events_by_node,
                received_data_events_by_node: sink_status.received_data_events_by_node,
                last_received_at_us_by_node: sink_status.last_received_at_us_by_node,
                last_received_origins_by_node: sink_status.last_received_origins_by_node,
                received_origin_counts_by_node: sink_status.received_origin_counts_by_node,
            },
        },
        facade: status_facade_from_published_state(published_facade_status),
    }))
}

fn active_source_node_count(source: &SourceObservabilitySnapshot) -> u64 {
    source
        .grants
        .iter()
        .filter(|grant| grant.active)
        .map(|grant| grant.host_ip.clone())
        .collect::<BTreeSet<_>>()
        .len() as u64
}

fn status_sink_live_nodes(live_source_nodes: u64, sink_status: &SinkStatusSnapshot) -> u64 {
    let _ = live_source_nodes;
    sink_status.live_nodes
}

fn snapshot_scoped_active_facade_candidate_groups(
    local_source: &SourceObservabilitySnapshot,
    local_sink: &SinkStatusSnapshot,
) -> BTreeSet<String> {
    let mut source_groups = local_source
        .scheduled_source_groups_by_node
        .values()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>();
    source_groups.extend(
        local_source
            .scheduled_scan_groups_by_node
            .values()
            .flatten()
            .cloned(),
    );
    let sink_groups = local_sink
        .scheduled_groups_by_node
        .values()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>();
    if !source_groups.is_empty() && !sink_groups.is_empty() {
        return source_groups.intersection(&sink_groups).cloned().collect();
    }
    if !source_groups.is_empty() && sink_groups.is_empty() {
        return BTreeSet::new();
    }
    if !source_groups.is_empty() {
        return source_groups;
    }
    sink_groups
}

const STATUS_ROUTE_TIMEOUT: Duration = Duration::from_millis(350);
const STATUS_ROUTE_COLLECT_IDLE_GRACE: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatusRouteOutcome {
    Skipped,
    Ok,
    Timeout,
    Transport,
    Protocol,
    Internal,
}

impl StatusRouteOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Skipped => "skipped",
            Self::Ok => "ok",
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::Protocol => "protocol",
            Self::Internal => "internal",
        }
    }
}

fn local_runner_sets(local_source: &SourceObservabilitySnapshot) -> BTreeMap<String, Vec<String>> {
    local_source
        .last_force_find_runner_by_group
        .iter()
        .map(|(group, runner)| (group.clone(), vec![runner.clone()]))
        .collect()
}

fn merge_source_runner_sets(
    primary: BTreeMap<String, Vec<String>>,
    fallback: BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, Vec<String>> {
    let mut merged = primary
        .into_iter()
        .map(|(group, runners)| (group, runners.into_iter().collect::<BTreeSet<_>>()))
        .collect::<BTreeMap<_, _>>();
    for (group, runners) in fallback {
        merged.entry(group).or_default().extend(runners);
    }
    merged
        .into_iter()
        .map(|(group, runners)| (group, runners.into_iter().collect()))
        .collect()
}

async fn merge_remote_status_snapshots<SinkCollect, SinkFuture, SourceCollect, SourceFuture>(
    local_sink: SinkStatusSnapshot,
    local_source: SourceObservabilitySnapshot,
    query_runtime_boundary: Option<std::sync::Arc<dyn ChannelIoSubset>>,
    node_id: NodeId,
    sink_collect: SinkCollect,
    source_collect: SourceCollect,
) -> (
    SinkStatusSnapshot,
    SourceObservabilitySnapshot,
    BTreeMap<String, Vec<String>>,
    StatusRouteOutcome,
    StatusRouteOutcome,
)
where
    SinkCollect:
        Fn(std::sync::Arc<dyn ChannelIoSubset>, NodeId) -> SinkFuture + Send + Sync + 'static,
    SinkFuture: Future<Output = Result<SinkStatusSnapshot, CnxError>> + Send,
    SourceCollect:
        Fn(std::sync::Arc<dyn ChannelIoSubset>, NodeId) -> SourceFuture + Send + Sync + 'static,
    SourceFuture: Future<
            Output = Result<(SourceObservabilitySnapshot, BTreeMap<String, Vec<String>>), CnxError>,
        > + Send,
{
    let Some(boundary) = query_runtime_boundary else {
        return (
            local_sink,
            local_source.clone(),
            local_runner_sets(&local_source),
            StatusRouteOutcome::Skipped,
            StatusRouteOutcome::Skipped,
        );
    };

    let retry_boundary = boundary.clone();
    let retry_node_id = node_id.clone();
    let (mut sink_result, mut source_result) = tokio::join!(
        sink_collect(boundary.clone(), node_id.clone()),
        source_collect(boundary, node_id),
    );
    let both_internal = matches!(sink_result, Err(CnxError::Internal(_)))
        && matches!(source_result, Err(CnxError::Internal(_)));
    let both_transport = sink_result
        .as_ref()
        .err()
        .is_some_and(|err| classify_status_route_error(err) == StatusRouteOutcome::Transport)
        && source_result
            .as_ref()
            .err()
            .is_some_and(|err| classify_status_route_error(err) == StatusRouteOutcome::Transport);
    if both_internal || both_transport {
        sink_result = sink_collect(retry_boundary.clone(), retry_node_id.clone()).await;
        source_result = source_collect(retry_boundary, retry_node_id).await;
    }
    let local_source_runner_sets = local_runner_sets(&local_source);

    let (sink_status, sink_outcome) = match sink_result {
        Ok(snapshot) => (
            merge_sink_status_snapshots(vec![local_sink, snapshot]),
            StatusRouteOutcome::Ok,
        ),
        Err(err) => {
            log_status_route_fallback("sink", &err);
            (local_sink, classify_status_route_error(&err))
        }
    };
    let (source, runner_sets, source_outcome) = match source_result {
        Ok((snapshot, runner_sets)) => (
            merge_source_observability(local_source, snapshot),
            merge_source_runner_sets(runner_sets, local_source_runner_sets.clone()),
            StatusRouteOutcome::Ok,
        ),
        Err(err) => {
            log_status_route_fallback("source", &err);
            (
                local_source,
                local_source_runner_sets,
                classify_status_route_error(&err),
            )
        }
    };

    (
        sink_status,
        source,
        runner_sets,
        sink_outcome,
        source_outcome,
    )
}

async fn collect_remote_status_snapshots_on_shared_boundary(
    local_sink: SinkStatusSnapshot,
    local_source: SourceObservabilitySnapshot,
    boundary: std::sync::Arc<dyn ChannelIoSubset>,
    node_id: NodeId,
    trace_id: Option<u64>,
) -> (
    SinkStatusSnapshot,
    SourceObservabilitySnapshot,
    BTreeMap<String, Vec<String>>,
    StatusRouteOutcome,
    StatusRouteOutcome,
) {
    record_status_route_trace(trace_id, "sink.collect.begin");
    let sink_result =
        route_sink_status_snapshot(boundary.clone(), node_id.clone(), STATUS_ROUTE_TIMEOUT).await;
    record_status_route_trace(trace_id, "source.collect.begin");
    let source_result = route_source_observability_snapshot(
        boundary,
        node_id,
        STATUS_ROUTE_TIMEOUT,
        STATUS_ROUTE_COLLECT_IDLE_GRACE,
    )
    .await;
    let local_source_runner_sets = local_runner_sets(&local_source);

    let (sink_status, sink_outcome) = match sink_result {
        Ok(snapshot) => (
            merge_sink_status_snapshots(vec![local_sink, snapshot]),
            StatusRouteOutcome::Ok,
        ),
        Err(err) => {
            log_status_route_fallback("sink", &err);
            (local_sink, classify_status_route_error(&err))
        }
    };
    record_status_route_trace(
        trace_id,
        format!("sink.collect.outcome={}", sink_outcome.as_str()),
    );
    let (source, runner_sets, source_outcome) = match source_result {
        Ok((snapshot, runner_sets)) => (
            merge_source_observability(local_source, snapshot),
            merge_source_runner_sets(runner_sets, local_source_runner_sets.clone()),
            StatusRouteOutcome::Ok,
        ),
        Err(err) => {
            log_status_route_fallback("source", &err);
            (
                local_source,
                local_source_runner_sets,
                classify_status_route_error(&err),
            )
        }
    };
    record_status_route_trace(
        trace_id,
        format!("source.collect.outcome={}", source_outcome.as_str()),
    );

    (
        sink_status,
        source,
        runner_sets,
        sink_outcome,
        source_outcome,
    )
}

fn classify_status_route_error(err: &CnxError) -> StatusRouteOutcome {
    match err {
        CnxError::Timeout => StatusRouteOutcome::Timeout,
        CnxError::ChannelClosed => StatusRouteOutcome::Transport,
        CnxError::TransportClosed(_) => StatusRouteOutcome::Transport,
        CnxError::ProtocolViolation(_) => StatusRouteOutcome::Protocol,
        CnxError::AccessDenied(message)
            if message.contains("drained/fenced") && message.contains("grant attachments")
                || message.contains("invalid or revoked grant attachment token") =>
        {
            StatusRouteOutcome::Transport
        }
        CnxError::PeerError(message)
            if message.contains("transport closed")
                || message.contains("bound route is closed")
                || message.contains("sidecar control bridge")
                || message.contains("sidecar data bridge")
                || message.contains("ipc read len")
                || message.contains("ipc write len")
                || message.contains("Connection reset by peer")
                || message.contains("Broken pipe")
                || message.contains("early eof") =>
        {
            StatusRouteOutcome::Transport
        }
        CnxError::Internal(message)
            if message.contains("transport closed")
                || message.contains("bound route is closed")
                || message.contains("sidecar control bridge")
                || message.contains("sidecar data bridge")
                || message.contains("ipc read len")
                || message.contains("ipc write len")
                || message.contains("Connection reset by peer")
                || message.contains("Broken pipe")
                || message.contains("early eof") =>
        {
            StatusRouteOutcome::Transport
        }
        _ => StatusRouteOutcome::Internal,
    }
}

fn should_fail_closed_status_route_collection(
    sink_outcome: StatusRouteOutcome,
    source_outcome: StatusRouteOutcome,
    published_facade_state: FacadeServiceState,
) -> bool {
    if matches!(
        published_facade_state,
        FacadeServiceState::Pending
            | FacadeServiceState::Serving
            | FacadeServiceState::Degraded
    ) {
        return false;
    }
    status_route_collection_incomplete(sink_outcome, source_outcome)
}

fn status_route_collection_incomplete(
    sink_outcome: StatusRouteOutcome,
    source_outcome: StatusRouteOutcome,
) -> bool {
    !matches!(
        sink_outcome,
        StatusRouteOutcome::Skipped | StatusRouteOutcome::Ok
    ) && !matches!(
        source_outcome,
        StatusRouteOutcome::Skipped | StatusRouteOutcome::Ok
    )
}

fn log_status_route_fallback(label: &str, err: &CnxError) {
    match classify_status_route_error(err) {
        StatusRouteOutcome::Timeout
        | StatusRouteOutcome::Transport
        | StatusRouteOutcome::Protocol => {
            log::warn!("status falling back to local {label} snapshot after route error: {err}");
        }
        StatusRouteOutcome::Internal => {
            log::warn!("status falling back to local {label} snapshot: {err}");
        }
        StatusRouteOutcome::Skipped | StatusRouteOutcome::Ok => {}
    }
}

pub async fn runtime_grants(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<RuntimeGrantsResponse>, ApiError> {
    let _ = authorize_management(&state, &headers)?;
    Ok(Json(RuntimeGrantsResponse {
        grants: state
            .source
            .host_object_grants_snapshot()
            .await
            .map_err(|err| ApiError::internal(format!("source grants snapshot failed: {err}")))?,
    }))
}

pub async fn roots_get(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<RootsResponse>, ApiError> {
    let _ = authorize_management(&state, &headers)?;
    let roots = state
        .source
        .logical_roots_snapshot()
        .await
        .map_err(|err| ApiError::internal(format!("source logical roots snapshot failed: {err}")))?
        .into_iter()
        .map(root_entry_from_spec)
        .collect();
    Ok(Json(RootsResponse { roots }))
}

pub async fn roots_preview(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(req): Json<RootsUpdateRequest>,
) -> Result<Json<RootsPreviewResponse>, ApiError> {
    let _ = authorize_management(&state, &headers)?;
    eprintln!(
        "fs_meta_api: roots_preview request roots={}",
        req.roots.len()
    );
    let mut roots = Vec::with_capacity(req.roots.len());
    for entry in req.roots {
        match root_spec_from_update(entry) {
            Ok(root) => roots.push(root),
            Err(err) => {
                eprintln!("fs_meta_api: roots_preview invalid input: {}", err.message);
                return Err(err);
            }
        }
    }
    let grants = state
        .source
        .host_object_grants_snapshot()
        .await
        .map_err(|err| {
            eprintln!("fs_meta_api: roots_preview grants snapshot failed: {err}");
            ApiError::internal(format!("source grants snapshot failed: {err}"))
        })?;
    Ok(Json(preview_roots(&roots, &grants)?))
}

pub async fn roots_put(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(req): Json<RootsUpdateRequest>,
) -> Result<Json<RootsUpdateResponse>, ApiError> {
    let _ = authorize_management(&state, &headers)?;
    let roots = req
        .roots
        .into_iter()
        .map(root_spec_from_update)
        .collect::<Result<Vec<_>, _>>()?;
    validate_unique_root_ids(&roots)?;
    eprintln!("fs_meta_api: roots_put request roots={}", roots.len());

    let grants = state
        .source
        .host_object_grants_snapshot()
        .await
        .map_err(|err| ApiError::internal(format!("source grants snapshot failed: {err}")))?;
    eprintln!(
        "fs_meta_api: roots_put grants snapshot ok grants={}",
        grants.len()
    );
    let preview = preview_roots(&roots, &grants)?;
    if !preview.unmatched_roots.is_empty() {
        return Err(ApiError::bad_request(format!(
            "roots update rejected: unmatched runtime grants for roots [{}]",
            preview.unmatched_roots.join(", ")
        )));
    }
    eprintln!("fs_meta_api: roots_put preview ok roots={}", roots.len());

    let previous_source_roots = state.source.logical_roots_snapshot().await.map_err(|err| {
        ApiError::internal(format!("source logical roots snapshot failed: {err}"))
    })?;
    eprintln!(
        "fs_meta_api: roots_put previous source roots ok roots={}",
        previous_source_roots.len()
    );
    #[cfg(test)]
    maybe_pause_roots_put_after_previous_source_roots().await;
    let previous_grants = grants.clone();
    // roots apply updates app-owned authoritative root/group definitions and
    // refreshes source/sink state against current runtime grants and bound
    // scopes. It does not make the API the owner of runtime bind/run
    // realization.
    state
        .source
        .update_logical_roots(roots.clone())
        .await
        .map_err(|err| {
            eprintln!("fs_meta_api: roots_put source update failed: {err}");
            err
        })?;
    eprintln!(
        "fs_meta_api: roots_put source update ok roots={}",
        roots.len()
    );
    let previous_sink_roots = state.sink.cached_logical_roots_snapshot()?;
    eprintln!(
        "fs_meta_api: roots_put previous sink roots ok roots={}",
        previous_sink_roots.len()
    );
    if let Err(err) = state
        .sink
        .update_logical_roots(roots.clone(), &grants)
        .await
    {
        eprintln!("fs_meta_api: roots_put sink sync failed: {err}");
        let sink_rollback = state
            .sink
            .update_logical_roots(previous_sink_roots, &previous_grants)
            .await;
        let source_rollback = state
            .source
            .update_logical_roots(previous_source_roots)
            .await;
        return match (sink_rollback, source_rollback) {
            (Ok(()), Ok(())) => Err(ApiError::internal(format!(
                "roots update aborted: sink sync failed: {err}"
            ))),
            (Err(sink_rollback_err), Ok(())) => Err(ApiError::internal(format!(
                "roots update diverged after sink sync failure: sink={err}; sink_rollback={sink_rollback_err}"
            ))),
            (Ok(()), Err(source_rollback_err)) => Err(ApiError::internal(format!(
                "roots update diverged after sink sync failure: sink={err}; source_rollback={source_rollback_err}"
            ))),
            (Err(sink_rollback_err), Err(source_rollback_err)) => Err(ApiError::internal(format!(
                "roots update diverged after sink sync failure: sink={err}; sink_rollback={sink_rollback_err}; source_rollback={source_rollback_err}"
            ))),
        };
    }
    eprintln!(
        "fs_meta_api: roots_put sink update ok roots={}",
        roots.len()
    );
    if let Some(boundary) = state.runtime_boundary.as_ref() {
        let payload = encode_logical_roots_control_payload(&roots).map_err(|err| {
            ApiError::internal(format!(
                "logical roots control payload encode failed: {err}"
            ))
        })?;
        for route_key in [ROUTE_KEY_SOURCE_ROOTS_CONTROL, ROUTE_KEY_SINK_ROOTS_CONTROL] {
            eprintln!(
                "fs_meta_api: roots_put control send begin route={} roots={}",
                route_key,
                roots.len()
            );
            match boundary
                .channel_send(
                    BoundaryContext::default(),
                    ChannelSendRequest {
                        channel_key: ChannelKey(format!("{route_key}.stream")),
                        events: vec![Event::new(
                            EventMetadata {
                                origin_id: state.node_id.clone(),
                                timestamp_us: SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .map(|d| d.as_micros().min(u128::from(u64::MAX)) as u64)
                                    .unwrap_or(0),
                                logical_ts: None,
                                correlation_id: None,
                                ingress_auth: None,
                                trace: None,
                            },
                            bytes::Bytes::from(payload.clone()),
                        )],
                        timeout_ms: Some(Duration::from_secs(5).as_millis() as u64),
                    },
                )
                .await
            {
                Ok(()) => {
                    eprintln!(
                        "fs_meta_api: roots_put control send ok route={} roots={}",
                        route_key,
                        roots.len()
                    );
                }
                Err(err) if is_stale_drained_pid_control_send_error(&err) => {
                    eprintln!(
                        "fs_meta_api: roots_put control send tolerated stale drained pid route={} err={}",
                        route_key, err
                    );
                }
                Err(err) => {
                    return Err(ApiError::internal(format!(
                        "logical roots control send failed route={route_key}: {err}"
                    )));
                }
            }
        }
    }
    refresh_policy_from_host_object_grants(&state.projection_policy, &grants);
    eprintln!("fs_meta_api: roots_put policy refresh ok");
    #[cfg(test)]
    maybe_pause_roots_put_before_response().await;

    Ok(Json(RootsUpdateResponse {
        roots_count: roots.len(),
    }))
}

pub async fn rescan(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<RescanResponse>, ApiError> {
    let _ = authorize_management(&state, &headers)?;
    let _facade_request_guard = state.request_tracker.begin();
    if let Some(boundary) = state.runtime_boundary.as_ref() {
        eprintln!(
            "fs_meta_api: rescan via runtime_boundary node={}",
            state.node_id.0
        );
        let requested_at_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0);
        let envelope = encode_manual_rescan_envelope(requested_at_us).map_err(|err| {
            ApiError::internal(format!("manual rescan envelope encode failed: {err}"))
        })?;
        let payload = rmp_serde::to_vec_named(&envelope).map_err(|err| {
            ApiError::internal(format!("manual rescan envelope serialize failed: {err}"))
        })?;
        match boundary
            .channel_send(
                BoundaryContext::default(),
                ChannelSendRequest {
                    channel_key: ChannelKey(format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL)),
                    events: vec![Event::new(
                        EventMetadata {
                            origin_id: state.node_id.clone(),
                            timestamp_us: 0,
                            logical_ts: None,
                            correlation_id: None,
                            ingress_auth: None,
                            trace: None,
                        },
                        bytes::Bytes::from(payload),
                    )],
                    timeout_ms: Some(Duration::from_secs(5).as_millis() as u64),
                },
            )
            .await
        {
            Ok(()) => {}
            Err(err) if is_stale_drained_pid_control_send_error(&err) => {
                eprintln!(
                    "fs_meta_api: rescan control send tolerated stale drained pid route={} err={}",
                    ROUTE_KEY_SOURCE_RESCAN_CONTROL, err
                );
                state.source.publish_manual_rescan_signal().await.map_err(|signal_err| {
                    ApiError::internal(format!(
                        "manual rescan signal publish failed after stale drained control pid: {signal_err}"
                    ))
                })?;
            }
            Err(err) => {
                return Err(ApiError::internal(format!(
                    "manual rescan control send failed: {err}"
                )));
            }
        }
    } else {
        eprintln!(
            "fs_meta_api: rescan via local source node={}",
            state.node_id.0
        );
        state.source.trigger_rescan_when_ready().await?;
    }
    #[cfg(test)]
    maybe_pause_rescan_before_return().await;
    Ok(Json(RescanResponse { accepted: true }))
}

pub async fn query_api_keys_list(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<QueryApiKeysResponse>, ApiError> {
    let _ = authorize_management(&state, &headers)?;
    Ok(Json(QueryApiKeysResponse {
        keys: state.auth.list_query_api_keys()?,
    }))
}

pub async fn query_api_keys_create(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(req): Json<CreateQueryApiKeyRequest>,
) -> Result<Json<CreateQueryApiKeyResponse>, ApiError> {
    let _ = authorize_management(&state, &headers)?;
    let (api_key, key) = state.auth.create_query_api_key(&req.label)?;
    Ok(Json(CreateQueryApiKeyResponse { api_key, key }))
}

pub async fn query_api_keys_revoke(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(key_id): Path<String>,
) -> Result<Json<RevokeQueryApiKeyResponse>, ApiError> {
    let _ = authorize_management(&state, &headers)?;
    let revoked = state.auth.revoke_query_api_key(&key_id)?;
    Ok(Json(RevokeQueryApiKeyResponse { revoked }))
}

fn preview_roots(
    roots: &[RootSpec],
    grants: &[GrantedMountRoot],
) -> Result<RootsPreviewResponse, ApiError> {
    let mut preview = Vec::with_capacity(roots.len());
    let mut unmatched_roots = Vec::new();
    for root in roots {
        let matched_grants = grants
            .iter()
            .filter(|grant| root.selector.matches(grant))
            .cloned()
            .collect::<Vec<_>>();
        if matched_grants.is_empty() {
            unmatched_roots.push(root.id.clone());
        }
        let mut monitor_paths = Vec::with_capacity(matched_grants.len());
        for grant in &matched_grants {
            let monitor_path = root
                .monitor_path_for(grant)
                .map_err(ApiError::bad_request)?;
            monitor_paths.push(monitor_path.display().to_string());
        }
        preview.push(RootPreviewItem {
            root_id: root.id.clone(),
            matched_grants,
            monitor_paths,
        });
    }
    Ok(RootsPreviewResponse {
        preview,
        unmatched_roots,
    })
}

fn validate_unique_root_ids(roots: &[RootSpec]) -> Result<(), ApiError> {
    let mut ids = BTreeSet::new();
    for root in roots {
        if !ids.insert(root.id.clone()) {
            return Err(ApiError::bad_request(format!(
                "duplicate root id '{}'",
                root.id
            )));
        }
    }
    Ok(())
}

fn status_source_from_observability(
    source: SourceObservabilitySnapshot,
    runner_sets: BTreeMap<String, Vec<String>>,
    force_find_inflight_groups: Vec<String>,
) -> StatusSource {
    let SourceObservabilitySnapshot {
        lifecycle_state,
        host_object_grants_version,
        grants,
        logical_roots,
        status,
        source_primary_by_group,
        last_force_find_runner_by_group,
        force_find_inflight_groups: _source_force_find_inflight_groups,
        scheduled_source_groups_by_node,
        scheduled_scan_groups_by_node,
        last_control_frame_signals_by_node,
        published_batches_by_node,
        published_events_by_node,
        published_control_events_by_node,
        published_data_events_by_node,
        last_published_at_us_by_node,
        last_published_origins_by_node,
        published_origin_counts_by_node,
        published_path_capture_target,
        enqueued_path_origin_counts_by_node,
        pending_path_origin_counts_by_node,
        yielded_path_origin_counts_by_node,
        summarized_path_origin_counts_by_node,
        published_path_origin_counts_by_node,
    } = source;
    let crate::source::SourceStatusSnapshot {
        current_stream_generation,
        logical_roots: status_logical_roots,
        concrete_roots,
        degraded_roots,
    } = status;
    let degraded_roots = degraded_roots
        .into_iter()
        .map(|(root_key, reason)| DegradedRoot { root_key, reason })
        .collect::<Vec<_>>();
    StatusSource {
        lifecycle_state,
        host_object_grants_version,
        grants_count: grants.len(),
        roots_count: logical_roots.len(),
        degraded_roots,
        logical_roots: status_logical_roots
            .into_iter()
            .map(|entry| StatusSourceLogicalRoot {
                root_id: entry.root_id,
                status: entry.status,
                matched_grants: entry.matched_grants,
                active_members: entry.active_members,
                coverage_mode: entry.coverage_mode,
            })
            .collect(),
        concrete_roots: concrete_roots
            .into_iter()
            .map(|entry| StatusSourceConcreteRoot {
                root_key: entry.root_key,
                logical_root_id: entry.logical_root_id,
                object_ref: entry.object_ref,
                status: entry.status,
                coverage_mode: entry.coverage_mode,
                watch_enabled: entry.watch_enabled,
                scan_enabled: entry.scan_enabled,
                is_group_primary: entry.is_group_primary,
                active: entry.active,
                watch_lru_capacity: entry.watch_lru_capacity,
                audit_interval_ms: entry.audit_interval_ms,
                overflow_count: entry.overflow_count,
                overflow_pending: entry.overflow_pending,
                rescan_pending: entry.rescan_pending,
                last_rescan_reason: entry.last_rescan_reason,
                last_error: entry.last_error,
                last_audit_started_at_us: entry.last_audit_started_at_us,
                last_audit_completed_at_us: entry.last_audit_completed_at_us,
                last_audit_duration_ms: entry.last_audit_duration_ms,
                emitted_batch_count: entry.emitted_batch_count,
                emitted_event_count: entry.emitted_event_count,
                emitted_control_event_count: entry.emitted_control_event_count,
                emitted_data_event_count: entry.emitted_data_event_count,
                emitted_path_capture_target: entry.emitted_path_capture_target,
                emitted_path_event_count: entry.emitted_path_event_count,
                last_emitted_at_us: entry.last_emitted_at_us,
                last_emitted_origins: entry.last_emitted_origins,
                forwarded_batch_count: entry.forwarded_batch_count,
                forwarded_event_count: entry.forwarded_event_count,
                forwarded_path_event_count: entry.forwarded_path_event_count,
                last_forwarded_at_us: entry.last_forwarded_at_us,
                last_forwarded_origins: entry.last_forwarded_origins,
                current_revision: entry.current_revision,
                current_stream_generation: entry.current_stream_generation,
                candidate_revision: entry.candidate_revision,
                candidate_stream_generation: entry.candidate_stream_generation,
                candidate_status: entry.candidate_status,
                draining_revision: entry.draining_revision,
                draining_stream_generation: entry.draining_stream_generation,
                draining_status: entry.draining_status,
            })
            .collect(),
        debug: super::types::StatusSourceDebug {
            current_stream_generation,
            source_primary_by_group,
            last_force_find_runner_by_group,
            last_force_find_runners_by_group: runner_sets,
            force_find_inflight_groups,
            scheduled_source_groups_by_node,
            scheduled_scan_groups_by_node,
            last_control_frame_signals_by_node,
            published_batches_by_node,
            published_events_by_node,
            published_control_events_by_node,
            published_data_events_by_node,
            last_published_at_us_by_node,
            last_published_origins_by_node,
            published_origin_counts_by_node,
            published_path_capture_target,
            enqueued_path_origin_counts_by_node,
            pending_path_origin_counts_by_node,
            yielded_path_origin_counts_by_node,
            summarized_path_origin_counts_by_node,
            published_path_origin_counts_by_node,
        },
    }
}

async fn route_source_observability_snapshot(
    boundary: std::sync::Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    timeout: Duration,
    idle_after_first: Duration,
) -> Result<(SourceObservabilitySnapshot, BTreeMap<String, Vec<String>>), CnxError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_err = None::<CnxError>;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(last_err.unwrap_or(CnxError::Timeout));
        }
        let attempt_timeout = remaining.min(Duration::from_secs(5));
        let events = match collect_internal_source_status_events(
            boundary.clone(),
            origin_id.clone(),
            attempt_timeout,
            idle_after_first,
            &[
                execution_units::QUERY_RUNTIME_UNIT_ID,
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            ],
        )
        .await
        {
            Ok(events) => events,
            Err(err @ CnxError::Timeout)
            | Err(err @ CnxError::TransportClosed(_))
            | Err(err @ CnxError::ProtocolViolation(_))
            | Err(err @ CnxError::Internal(_)) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(err) => return Err(err),
        };
        let snapshots = events
            .into_iter()
            .map(|event| {
                rmp_serde::from_slice::<SourceObservabilitySnapshot>(event.payload_bytes())
                    .map_err(|err| {
                        CnxError::Internal(format!("decode source observability failed: {err}"))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if source_observability_snapshots_need_retry(&snapshots)
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        if debug_status_route_fanin_enabled() {
            let summaries = snapshots
                .iter()
                .map(summarize_source_status_route_snapshot)
                .collect::<Vec<_>>();
            eprintln!(
                "fs_meta_api_status: source_route_collect events={} snapshots={:?}",
                snapshots.len(),
                summaries
            );
        }
        let runner_sets = source_runner_sets(&snapshots);
        if debug_force_find_runner_capture_enabled() {
            let last_runners = snapshots
                .iter()
                .map(|snapshot| summarize_group_string_map(&snapshot.last_force_find_runner_by_group))
                .collect::<Vec<_>>();
            eprintln!(
                "fs_meta_api_status: source_route_runner_capture snapshots={} last_runners={:?} runner_sets={:?}",
                snapshots.len(),
                last_runners,
                runner_sets
            );
        }
        return Ok((merge_source_observability_snapshots(snapshots), runner_sets));
    }
}

fn source_observability_snapshot_debug_maps_absent(
    snapshot: &SourceObservabilitySnapshot,
) -> bool {
    snapshot.scheduled_source_groups_by_node.is_empty()
        && snapshot.scheduled_scan_groups_by_node.is_empty()
        && snapshot.last_control_frame_signals_by_node.is_empty()
        && snapshot
            .published_batches_by_node
            .values()
            .all(|count| *count == 0)
        && snapshot
            .published_events_by_node
            .values()
            .all(|count| *count == 0)
        && snapshot
            .published_control_events_by_node
            .values()
            .all(|count| *count == 0)
        && snapshot
            .published_data_events_by_node
            .values()
            .all(|count| *count == 0)
        && snapshot.last_published_at_us_by_node.is_empty()
        && snapshot.last_published_origins_by_node.is_empty()
        && snapshot.published_origin_counts_by_node.is_empty()
        && snapshot.enqueued_path_origin_counts_by_node.is_empty()
        && snapshot.pending_path_origin_counts_by_node.is_empty()
        && snapshot.yielded_path_origin_counts_by_node.is_empty()
        && snapshot.summarized_path_origin_counts_by_node.is_empty()
        && snapshot.published_path_origin_counts_by_node.is_empty()
}

fn source_observability_snapshot_has_active_state(
    snapshot: &SourceObservabilitySnapshot,
) -> bool {
    snapshot.status.current_stream_generation.is_some()
        || snapshot
            .status
            .logical_roots
            .iter()
            .any(|entry| entry.active_members > 0 || entry.status.eq_ignore_ascii_case("ready"))
        || snapshot.status.concrete_roots.iter().any(|entry| {
            entry.active
                || entry.current_stream_generation.is_some()
                || entry.emitted_batch_count > 0
                || entry.forwarded_batch_count > 0
                || entry.emitted_event_count > 0
                || entry.forwarded_event_count > 0
        })
}

fn source_observability_snapshots_need_retry(
    snapshots: &[SourceObservabilitySnapshot],
) -> bool {
    snapshots.iter().any(|snapshot| {
        source_observability_snapshot_has_active_state(snapshot)
            && source_observability_snapshot_debug_maps_absent(snapshot)
    })
}

async fn collect_internal_source_status_events(
    boundary: std::sync::Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    timeout: Duration,
    idle_after_first: Duration,
    _unit_ids: &[&str],
) -> Result<Vec<Event>, CnxError> {
    let adapter = exchange_host_adapter(boundary, origin_id, default_route_bindings());
    adapter
        .call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SOURCE_STATUS,
            internal_status_request_payload(),
            timeout,
            idle_after_first,
        )
        .await
}

fn merge_source_observability(
    local: SourceObservabilitySnapshot,
    aggregated: SourceObservabilitySnapshot,
) -> SourceObservabilitySnapshot {
    const SOURCE_WORKER_DEGRADED_ROOT_KEY: &str = "source-worker";
    const SOURCE_WORKER_CACHE_REASON: &str = "source worker status served from cache";

    fn has_live_data(snapshot: &SourceObservabilitySnapshot) -> bool {
        !snapshot.grants.is_empty()
            || !snapshot.logical_roots.is_empty()
            || !snapshot.status.logical_roots.is_empty()
            || !snapshot.status.concrete_roots.is_empty()
            || !snapshot.source_primary_by_group.is_empty()
            || !snapshot.last_force_find_runner_by_group.is_empty()
            || !snapshot.force_find_inflight_groups.is_empty()
            || !snapshot.scheduled_source_groups_by_node.is_empty()
            || !snapshot.scheduled_scan_groups_by_node.is_empty()
            || !snapshot.last_control_frame_signals_by_node.is_empty()
            || !snapshot.published_batches_by_node.is_empty()
            || !snapshot.published_events_by_node.is_empty()
            || !snapshot.published_control_events_by_node.is_empty()
            || !snapshot.published_data_events_by_node.is_empty()
            || !snapshot.last_published_at_us_by_node.is_empty()
            || !snapshot.last_published_origins_by_node.is_empty()
            || !snapshot.published_origin_counts_by_node.is_empty()
            || snapshot.published_path_capture_target.is_some()
            || !snapshot.enqueued_path_origin_counts_by_node.is_empty()
            || !snapshot.pending_path_origin_counts_by_node.is_empty()
            || !snapshot.yielded_path_origin_counts_by_node.is_empty()
            || !snapshot.summarized_path_origin_counts_by_node.is_empty()
            || !snapshot.published_path_origin_counts_by_node.is_empty()
    }

    let (mut merged, fallback) = if has_live_data(&aggregated) {
        (aggregated, local)
    } else {
        (local, aggregated)
    };

    merged.host_object_grants_version = merged
        .host_object_grants_version
        .max(fallback.host_object_grants_version);

    let mut grant_map = merged
        .grants
        .into_iter()
        .map(|grant| (grant.object_ref.clone(), grant))
        .collect::<BTreeMap<_, _>>();
    for grant in fallback.grants {
        grant_map.entry(grant.object_ref.clone()).or_insert(grant);
    }
    merged.grants = grant_map.into_values().collect();

    let mut root_map = merged
        .logical_roots
        .into_iter()
        .map(|root| (root.id.clone(), root))
        .collect::<BTreeMap<_, _>>();
    for root in fallback.logical_roots {
        root_map.entry(root.id.clone()).or_insert(root);
    }
    merged.logical_roots = root_map.into_values().collect();

    let mut logical_root_map = merged
        .status
        .logical_roots
        .into_iter()
        .map(|entry| (entry.root_id.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    for entry in fallback.status.logical_roots {
        logical_root_map
            .entry(entry.root_id.clone())
            .or_insert(entry);
    }
    merged.status.logical_roots = logical_root_map.into_values().collect();

    let mut concrete_root_map = merged
        .status
        .concrete_roots
        .into_iter()
        .map(|entry| (entry.root_key.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    for entry in fallback.status.concrete_roots {
        concrete_root_map
            .entry(entry.root_key.clone())
            .or_insert(entry);
    }
    merged.status.concrete_roots = concrete_root_map.into_values().collect();

    let merged_has_live_data = has_live_data(&merged);
    let mut degraded_root_map = merged
        .status
        .degraded_roots
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    for (root_key, reason) in fallback.status.degraded_roots {
        if merged_has_live_data
            && root_key == SOURCE_WORKER_DEGRADED_ROOT_KEY
            && reason == SOURCE_WORKER_CACHE_REASON
        {
            continue;
        }
        degraded_root_map.entry(root_key).or_insert(reason);
    }
    merged.status.degraded_roots = degraded_root_map.into_iter().collect();

    for (group, object_ref) in fallback.source_primary_by_group {
        merged
            .source_primary_by_group
            .entry(group)
            .or_insert(object_ref);
    }
    for (group, runner) in fallback.last_force_find_runner_by_group {
        merged
            .last_force_find_runner_by_group
            .entry(group)
            .or_insert(runner);
    }
    for (node_id, groups) in fallback.scheduled_source_groups_by_node {
        match merged.scheduled_source_groups_by_node.get_mut(&node_id) {
            Some(existing) if existing.is_empty() && !groups.is_empty() => *existing = groups,
            None => {
                merged
                    .scheduled_source_groups_by_node
                    .insert(node_id, groups);
            }
            _ => {}
        }
    }
    for (node_id, groups) in fallback.scheduled_scan_groups_by_node {
        match merged.scheduled_scan_groups_by_node.get_mut(&node_id) {
            Some(existing) if existing.is_empty() && !groups.is_empty() => *existing = groups,
            None => {
                merged.scheduled_scan_groups_by_node.insert(node_id, groups);
            }
            _ => {}
        }
    }
    for (node_id, signals) in fallback.last_control_frame_signals_by_node {
        merged
            .last_control_frame_signals_by_node
            .entry(node_id)
            .or_insert(signals);
    }
    for (node_id, count) in fallback.published_batches_by_node {
        merged
            .published_batches_by_node
            .entry(node_id)
            .or_insert(count);
    }
    for (node_id, count) in fallback.published_events_by_node {
        merged
            .published_events_by_node
            .entry(node_id)
            .or_insert(count);
    }
    for (node_id, count) in fallback.published_control_events_by_node {
        merged
            .published_control_events_by_node
            .entry(node_id)
            .or_insert(count);
    }
    for (node_id, count) in fallback.published_data_events_by_node {
        merged
            .published_data_events_by_node
            .entry(node_id)
            .or_insert(count);
    }
    for (node_id, ts) in fallback.last_published_at_us_by_node {
        merged
            .last_published_at_us_by_node
            .entry(node_id)
            .or_insert(ts);
    }
    for (node_id, origins) in fallback.last_published_origins_by_node {
        merged
            .last_published_origins_by_node
            .entry(node_id)
            .or_insert(origins);
    }
    for (node_id, origins) in fallback.published_origin_counts_by_node {
        merged
            .published_origin_counts_by_node
            .entry(node_id)
            .or_insert(origins);
    }
    if merged.published_path_capture_target.is_none() {
        merged.published_path_capture_target = fallback.published_path_capture_target;
    }
    for (node_id, origins) in fallback.enqueued_path_origin_counts_by_node {
        merged
            .enqueued_path_origin_counts_by_node
            .entry(node_id)
            .or_insert(origins);
    }
    for (node_id, origins) in fallback.pending_path_origin_counts_by_node {
        merged
            .pending_path_origin_counts_by_node
            .entry(node_id)
            .or_insert(origins);
    }
    for (node_id, origins) in fallback.yielded_path_origin_counts_by_node {
        merged
            .yielded_path_origin_counts_by_node
            .entry(node_id)
            .or_insert(origins);
    }
    for (node_id, origins) in fallback.summarized_path_origin_counts_by_node {
        merged
            .summarized_path_origin_counts_by_node
            .entry(node_id)
            .or_insert(origins);
    }
    for (node_id, origins) in fallback.published_path_origin_counts_by_node {
        merged
            .published_path_origin_counts_by_node
            .entry(node_id)
            .or_insert(origins);
    }

    let mut inflight = merged
        .force_find_inflight_groups
        .into_iter()
        .collect::<BTreeSet<_>>();
    inflight.extend(fallback.force_find_inflight_groups);
    merged.force_find_inflight_groups = inflight.into_iter().collect();
    merged
}

fn merge_source_observability_snapshots(
    snapshots: Vec<SourceObservabilitySnapshot>,
) -> SourceObservabilitySnapshot {
    let mut iter = snapshots.into_iter();
    let Some(mut merged) = iter.next() else {
        return SourceObservabilitySnapshot {
            lifecycle_state: "unknown".to_string(),
            host_object_grants_version: 0,
            grants: Vec::new(),
            logical_roots: Vec::new(),
            status: Default::default(),
            source_primary_by_group: BTreeMap::new(),
            last_force_find_runner_by_group: BTreeMap::new(),
            force_find_inflight_groups: Vec::new(),
            scheduled_source_groups_by_node: BTreeMap::new(),
            scheduled_scan_groups_by_node: BTreeMap::new(),
            last_control_frame_signals_by_node: BTreeMap::new(),
            published_batches_by_node: BTreeMap::new(),
            published_events_by_node: BTreeMap::new(),
            published_control_events_by_node: BTreeMap::new(),
            published_data_events_by_node: BTreeMap::new(),
            last_published_at_us_by_node: BTreeMap::new(),
            last_published_origins_by_node: BTreeMap::new(),
            published_origin_counts_by_node: BTreeMap::new(),
            published_path_capture_target: None,
            enqueued_path_origin_counts_by_node: BTreeMap::new(),
            pending_path_origin_counts_by_node: BTreeMap::new(),
            yielded_path_origin_counts_by_node: BTreeMap::new(),
            summarized_path_origin_counts_by_node: BTreeMap::new(),
            published_path_origin_counts_by_node: BTreeMap::new(),
        };
    };

    let mut logical_root_map = merged
        .status
        .logical_roots
        .iter()
        .cloned()
        .map(|entry| (entry.root_id.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut concrete_root_map = merged
        .status
        .concrete_roots
        .iter()
        .cloned()
        .map(|entry| (entry.root_key.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut degraded_root_map = merged
        .status
        .degraded_roots
        .iter()
        .cloned()
        .collect::<BTreeMap<_, _>>();
    let mut grant_map = merged
        .grants
        .iter()
        .cloned()
        .map(|entry| (entry.object_ref.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut root_map = merged
        .logical_roots
        .iter()
        .cloned()
        .map(|entry| (entry.id.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut inflight = merged
        .force_find_inflight_groups
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();

    for snapshot in iter {
        merged.host_object_grants_version = merged
            .host_object_grants_version
            .max(snapshot.host_object_grants_version);
        merged
            .source_primary_by_group
            .extend(snapshot.source_primary_by_group);
        merged
            .last_force_find_runner_by_group
            .extend(snapshot.last_force_find_runner_by_group);
        merged
            .scheduled_source_groups_by_node
            .extend(snapshot.scheduled_source_groups_by_node);
        merged
            .scheduled_scan_groups_by_node
            .extend(snapshot.scheduled_scan_groups_by_node);
        merged
            .last_control_frame_signals_by_node
            .extend(snapshot.last_control_frame_signals_by_node);
        merged
            .published_batches_by_node
            .extend(snapshot.published_batches_by_node);
        merged
            .published_events_by_node
            .extend(snapshot.published_events_by_node);
        merged
            .published_control_events_by_node
            .extend(snapshot.published_control_events_by_node);
        merged
            .published_data_events_by_node
            .extend(snapshot.published_data_events_by_node);
        merged
            .last_published_at_us_by_node
            .extend(snapshot.last_published_at_us_by_node);
        merged
            .last_published_origins_by_node
            .extend(snapshot.last_published_origins_by_node);
        merged
            .published_origin_counts_by_node
            .extend(snapshot.published_origin_counts_by_node);
        if merged.published_path_capture_target.is_none() {
            merged.published_path_capture_target = snapshot.published_path_capture_target;
        }
        merged
            .enqueued_path_origin_counts_by_node
            .extend(snapshot.enqueued_path_origin_counts_by_node);
        merged
            .pending_path_origin_counts_by_node
            .extend(snapshot.pending_path_origin_counts_by_node);
        merged
            .yielded_path_origin_counts_by_node
            .extend(snapshot.yielded_path_origin_counts_by_node);
        merged
            .summarized_path_origin_counts_by_node
            .extend(snapshot.summarized_path_origin_counts_by_node);
        merged
            .published_path_origin_counts_by_node
            .extend(snapshot.published_path_origin_counts_by_node);
        inflight.extend(snapshot.force_find_inflight_groups);
        for grant in snapshot.grants {
            grant_map.entry(grant.object_ref.clone()).or_insert(grant);
        }
        for root in snapshot.logical_roots {
            root_map.entry(root.id.clone()).or_insert(root);
        }
        for entry in snapshot.status.logical_roots {
            logical_root_map
                .entry(entry.root_id.clone())
                .or_insert(entry);
        }
        for entry in snapshot.status.concrete_roots {
            concrete_root_map
                .entry(entry.root_key.clone())
                .or_insert(entry);
        }
        for (root_key, reason) in snapshot.status.degraded_roots {
            degraded_root_map.entry(root_key).or_insert(reason);
        }
    }

    merged.grants = grant_map.into_values().collect();
    merged.logical_roots = root_map.into_values().collect();
    merged.status.logical_roots = logical_root_map.into_values().collect();
    merged.status.concrete_roots = concrete_root_map.into_values().collect();
    merged.status.degraded_roots = degraded_root_map.into_iter().collect();
    merged.force_find_inflight_groups = inflight.into_iter().collect();
    merged
}

fn source_runner_sets(snapshots: &[SourceObservabilitySnapshot]) -> BTreeMap<String, Vec<String>> {
    let mut by_group = BTreeMap::<String, BTreeSet<String>>::new();
    for snapshot in snapshots {
        for (group, runner) in &snapshot.last_force_find_runner_by_group {
            by_group
                .entry(group.clone())
                .or_default()
                .insert(runner.clone());
        }
    }
    by_group
        .into_iter()
        .map(|(group, runners)| (group, runners.into_iter().collect()))
        .collect()
}

fn status_facade_from_published_state(
    published_facade_status: crate::api::facade_status::PublishedFacadeStatusSnapshot,
) -> StatusFacade {
    StatusFacade {
        state: published_facade_status.state.as_str().to_string(),
        pending: published_facade_status
            .pending
            .map(|pending| StatusFacadePending {
                route_key: pending.route_key,
                generation: pending.generation,
                resource_ids: pending.resource_ids,
                runtime_managed: pending.runtime_managed,
                runtime_exposure_confirmed: pending.runtime_exposure_confirmed,
                reason: pending.reason.as_str().to_string(),
                retry_attempts: pending.retry_attempts,
                pending_since_us: pending.pending_since_us,
                last_error: pending.last_error,
                last_attempt_at_us: pending.last_attempt_at_us,
                last_error_at_us: pending.last_error_at_us,
                retry_backoff_ms: pending.retry_backoff_ms,
                next_retry_at_us: pending.next_retry_at_us,
            }),
    }
}

fn root_entry_from_spec(root: RootSpec) -> RootEntry {
    let selector = selector_entry_from_spec(&root.selector);
    RootEntry {
        id: root.id,
        selector,
        subpath_scope: root.subpath_scope.display().to_string(),
        watch: root.watch,
        scan: root.scan,
        audit_interval_ms: root.audit_interval_ms,
    }
}

fn root_spec_from_update(entry: RootUpdateEntry) -> Result<RootSpec, ApiError> {
    if entry.source_locator_present {
        return Err(ApiError::bad_request(
            "roots[].source_locator is forbidden; use roots[].selector mount descriptors",
        ));
    }
    if entry.path_present {
        return Err(ApiError::bad_request(
            "roots[].path is forbidden; use roots[].selector mount descriptors",
        ));
    }
    if entry.id.trim().is_empty() {
        return Err(ApiError::bad_request("roots[].id must not be empty"));
    }
    if entry.selector.is_empty() {
        return Err(ApiError::bad_request(
            "roots[].selector must include at least one descriptor field",
        ));
    }
    if let Some(mount_point) = entry.selector.mount_point.as_deref() {
        let path = PathBuf::from(mount_point.trim());
        if !path.is_absolute() {
            return Err(ApiError::bad_request(
                "roots[].selector.mount_point must be absolute",
            ));
        }
    }
    let subpath_scope = PathBuf::from(entry.subpath_scope.trim());
    if !subpath_scope.is_absolute() {
        return Err(ApiError::bad_request(
            "roots[].subpath_scope must be absolute",
        ));
    }
    if !entry.watch && !entry.scan {
        return Err(ApiError::bad_request(
            "roots[].watch or roots[].scan must be enabled",
        ));
    }
    Ok(RootSpec {
        id: entry.id,
        selector: selector_from_entry(&entry.selector),
        subpath_scope,
        watch: entry.watch,
        scan: entry.scan,
        audit_interval_ms: entry.audit_interval_ms,
    })
}

fn selector_entry_from_spec(selector: &RootSelector) -> RootSelectorEntry {
    RootSelectorEntry {
        mount_point: selector
            .mount_point
            .as_ref()
            .map(|path| path.display().to_string()),
        fs_source: selector.fs_source.clone(),
        fs_type: selector.fs_type.clone(),
        host_ip: selector.host_ip.clone(),
        host_ref: selector.host_ref.clone(),
    }
}

fn selector_from_entry(entry: &RootSelectorEntry) -> RootSelector {
    RootSelector {
        mount_point: entry.mount_point.as_deref().map(PathBuf::from),
        fs_source: entry.fs_source.clone(),
        fs_type: entry.fs_type.clone(),
        host_ip: entry.host_ip.clone(),
        host_ref: entry.host_ref.clone(),
    }
}

fn auth_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
}

fn authorize_management(
    state: &ApiState,
    headers: &HeaderMap,
) -> Result<SessionPrincipal, ApiError> {
    let (_, principal) = state
        .auth
        .authorize_management_session(auth_header(headers))?;
    Ok(principal)
}

fn is_stale_drained_pid_control_send_error(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::AccessDenied(message)
            if message.contains("drained/fenced")
                && message.contains("grant attachments")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RootSpec;
    use crate::api::auth::AuthService;
    use crate::api::config::ApiAuthConfig;
    use crate::api::facade_status::{
        shared_facade_pending_status_cell, shared_facade_service_state_cell,
    };
    use crate::domain_state::FacadeServiceState;
    use crate::query::api::ProjectionPolicy;
    use crate::runtime::routes::METHOD_SINK_STATUS;
    use crate::sink::SinkFileMeta;
    use crate::source::FSMetaSource;
    use crate::source::SourceStatusSnapshot;
    use crate::source::config::GrantedMountRoot;
    use crate::source::config::SourceConfig;
    use crate::source::{SourceConcreteRootHealthSnapshot, SourceLogicalRootHealthSnapshot};
    use crate::state::cell::SignalCell;
    use crate::workers::sink::SinkFacade;
    use crate::workers::source::SourceFacade;
    use crate::workers::source::SourceObservabilitySnapshot;
    use axum::Json;
    use axum::extract::State;
    use axum::http::HeaderValue;
    use capanix_app_sdk::CnxError;
    use capanix_app_sdk::runtime::{NodeId, in_memory_state_boundary};
    use capanix_runtime_entry_sdk::advanced::boundary::ChannelIoSubset;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex as StdMutex, RwLock};
    use tempfile::tempdir;

    struct NoopBoundary;

    impl ChannelIoSubset for NoopBoundary {}

    struct DeniedControlRouteBoundary {
        sent_routes: Arc<StdMutex<Vec<String>>>,
        denied_routes: BTreeSet<String>,
    }

    struct DelayedSourceStatusCollectBoundary {
        reply_channel: String,
        sent_correlation: StdMutex<Option<u64>>,
        recv_count: std::sync::atomic::AtomicUsize,
        fast_snapshot: SourceObservabilitySnapshot,
        delayed_snapshot: SourceObservabilitySnapshot,
    }

    struct RoutedSourceStatusCollectBoundary {
        request_channel: String,
        reply_channel: String,
        replies: tokio::sync::Mutex<Vec<Vec<Event>>>,
        sent_unit_ids: StdMutex<Vec<Option<String>>>,
        recv_unit_ids: StdMutex<Vec<Option<String>>>,
    }

    struct SourceStatusInternalRetryThenReplyBoundary {
        source_reply_channel: String,
        source_status_payloads: Vec<Vec<u8>>,
        send_batches_by_channel: StdMutex<std::collections::HashMap<String, usize>>,
        recv_batches_by_channel: StdMutex<std::collections::HashMap<String, usize>>,
        correlations_by_channel: StdMutex<std::collections::HashMap<String, u64>>,
        first_source_recv_failed: std::sync::atomic::AtomicBool,
    }

    struct SourceStatusPeerTransportRetryThenReplyBoundary {
        source_reply_channel: String,
        source_status_payloads: Vec<Vec<u8>>,
        send_batches_by_channel: StdMutex<std::collections::HashMap<String, usize>>,
        recv_batches_by_channel: StdMutex<std::collections::HashMap<String, usize>>,
        correlations_by_channel: StdMutex<std::collections::HashMap<String, u64>>,
        first_source_recv_failed: std::sync::atomic::AtomicBool,
    }

    struct SourceStatusIncompleteRetryThenReplyBoundary {
        source_reply_channel: String,
        first_source_status_payloads: Vec<Vec<u8>>,
        second_source_status_payloads: Vec<Vec<u8>>,
        send_batches_by_channel: StdMutex<std::collections::HashMap<String, usize>>,
        recv_batches_by_channel: StdMutex<std::collections::HashMap<String, usize>>,
        correlations_by_channel: StdMutex<std::collections::HashMap<String, u64>>,
        recv_state: StdMutex<(usize, usize)>,
    }

    struct StatusTransportRouteBoundary;
    struct StatusSlowTimeoutRouteBoundary;

    struct StatusRemoteReplyBoundary {
        source_request_channel: String,
        source_reply_channel: String,
        sink_request_channel: String,
        sink_reply_channel: String,
        source_status_payloads: Vec<(String, Vec<u8>)>,
        sink_status_payloads: Vec<(String, Vec<u8>)>,
        correlations_by_channel: StdMutex<std::collections::HashMap<String, u64>>,
        recv_batches_by_channel: StdMutex<std::collections::HashMap<String, usize>>,
    }

    struct StatusOverlapPoisonBoundary {
        source_request_channel: String,
        source_reply_channel: String,
        sink_request_channel: String,
        sink_reply_channel: String,
        source_status_payloads: Vec<(String, Vec<u8>)>,
        sink_status_payloads: Vec<(String, Vec<u8>)>,
        correlations_by_channel: StdMutex<std::collections::HashMap<String, u64>>,
        recv_batches_by_channel: StdMutex<std::collections::HashMap<String, usize>>,
        active_recvs: std::sync::atomic::AtomicUsize,
        poisoned: std::sync::atomic::AtomicBool,
    }

    struct StatusInternalRouteBoundary;

    #[derive(Default)]
    struct ConcurrentInternalThenSequentialOkStatusBoundary {
        in_flight: std::sync::atomic::AtomicUsize,
        concurrent_fail_triggered: std::sync::atomic::AtomicBool,
        sink_calls: std::sync::atomic::AtomicUsize,
        source_calls: std::sync::atomic::AtomicUsize,
    }

    #[derive(Default)]
    struct ConcurrentTransportThenSequentialOkStatusBoundary {
        in_flight: std::sync::atomic::AtomicUsize,
        concurrent_fail_triggered: std::sync::atomic::AtomicBool,
        sink_calls: std::sync::atomic::AtomicUsize,
        source_calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for DeniedControlRouteBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            let route = request.channel_key.0.clone();
            let mut sent = match self.sent_routes.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            sent.push(route.clone());
            drop(sent);
            if self.denied_routes.contains(&route) {
                return Err(CnxError::AccessDenied(
                    "pid Pid(4) is drained/fenced and cannot obtain new grant attachments".into(),
                ));
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for DelayedSourceStatusCollectBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            let correlation = request
                .events
                .first()
                .and_then(|event| event.metadata().correlation_id);
            let mut sent = self
                .sent_correlation
                .lock()
                .expect("delayed source-status sent correlation lock");
            *sent = correlation;
            Ok(())
        }

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            if request.channel_key.0 != self.reply_channel {
                return Err(CnxError::Timeout);
            }
            let correlation = self
                .sent_correlation
                .lock()
                .expect("delayed source-status sent correlation lock")
                .unwrap_or(1);
            match self
                .recv_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            {
                0 => Ok(vec![source_status_event(
                    "node-d-29775487306465848395300865",
                    correlation,
                    self.fast_snapshot.clone(),
                )]),
                1 => {
                    tokio::time::sleep(Duration::from_millis(750)).await;
                    Ok(vec![source_status_event(
                        "node-b-29775487306465848395300865",
                        correlation,
                        self.delayed_snapshot.clone(),
                    )])
                }
                _ => Err(CnxError::Timeout),
            }
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for RoutedSourceStatusCollectBoundary {
        async fn channel_send(
            &self,
            ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            if request.channel_key.0 != self.request_channel {
                return Ok(());
            }
            self.sent_unit_ids
                .lock()
                .expect("unit scoped source-status sent unit ids lock")
                .push(ctx.unit_id.clone());
            let correlation = request
                .events
                .first()
                .and_then(|event| event.metadata().correlation_id)
                .unwrap_or(1);
            let mut replies = self.replies.lock().await;
            match ctx.unit_id.as_deref() {
                // Routed collection via BoundRoute::call_collect uses default context and must
                // reach all active peer handlers, including query-peer-owned peers.
                None => {
                    replies.push(vec![
                        source_status_event(
                            "node-b-29775547640557521931862017",
                            correlation,
                            SourceObservabilitySnapshot {
                                scheduled_source_groups_by_node: BTreeMap::from([(
                                    "node-b".to_string(),
                                    vec!["nfs1".to_string(), "nfs2".to_string()],
                                )]),
                                scheduled_scan_groups_by_node: BTreeMap::from([(
                                    "node-b".to_string(),
                                    vec!["nfs1".to_string(), "nfs2".to_string()],
                                )]),
                                ..local_source_snapshot()
                            },
                        ),
                        source_status_event(
                            "node-c-29775547640557521931862017",
                            correlation,
                            SourceObservabilitySnapshot {
                                scheduled_source_groups_by_node: BTreeMap::from([(
                                    "node-c".to_string(),
                                    vec!["nfs1".to_string(), "nfs2".to_string()],
                                )]),
                                scheduled_scan_groups_by_node: BTreeMap::from([(
                                    "node-c".to_string(),
                                    vec!["nfs1".to_string(), "nfs2".to_string()],
                                )]),
                                ..local_source_snapshot()
                            },
                        ),
                        source_status_event(
                            "node-d-29775547640557521931862017",
                            correlation,
                            SourceObservabilitySnapshot {
                                scheduled_source_groups_by_node: BTreeMap::from([(
                                    "node-d".to_string(),
                                    vec!["nfs2".to_string()],
                                )]),
                                scheduled_scan_groups_by_node: BTreeMap::from([(
                                    "node-d".to_string(),
                                    vec!["nfs2".to_string()],
                                )]),
                                ..local_source_snapshot()
                            },
                        ),
                    ]);
                }
                // The current broken direct collector path uses unit-scoped raw boundary sends,
                // which in preserved exact evidence only yields the mixed local/query owner.
                Some(execution_units::QUERY_RUNTIME_UNIT_ID)
                | Some(execution_units::QUERY_PEER_RUNTIME_UNIT_ID) => {
                    replies.push(vec![source_status_event(
                        "node-d-29775547640557521931862017",
                        correlation,
                        SourceObservabilitySnapshot {
                            scheduled_source_groups_by_node: BTreeMap::from([(
                                "node-d".to_string(),
                                vec!["nfs2".to_string()],
                            )]),
                            scheduled_scan_groups_by_node: BTreeMap::from([(
                                "node-d".to_string(),
                                vec!["nfs2".to_string()],
                            )]),
                            ..local_source_snapshot()
                        },
                    )]);
                }
                _ => {}
            }
            Ok(())
        }

        async fn channel_recv(
            &self,
            ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            if request.channel_key.0 != self.reply_channel {
                return Err(CnxError::Timeout);
            }
            self.recv_unit_ids
                .lock()
                .expect("routed source-status recv unit ids lock")
                .push(ctx.unit_id.clone());
            let mut replies = self.replies.lock().await;
            if replies.is_empty() {
                return Err(CnxError::Timeout);
            }
            Ok(replies.remove(0))
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for SourceStatusInternalRetryThenReplyBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            if let Some(correlation) = request
                .events
                .first()
                .and_then(|event| event.metadata().correlation_id)
            {
                self.correlations_by_channel
                    .lock()
                    .expect("source internal retry boundary correlations lock")
                    .insert(request.channel_key.0.clone(), correlation);
            }
            let mut send_batches = self
                .send_batches_by_channel
                .lock()
                .expect("source internal retry boundary send batches lock");
            *send_batches.entry(request.channel_key.0).or_default() += 1;
            Ok(())
        }

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            let mut recv_batches = self
                .recv_batches_by_channel
                .lock()
                .expect("source internal retry boundary recv batches lock");
            *recv_batches
                .entry(request.channel_key.0.clone())
                .or_default() += 1;
            drop(recv_batches);

            if request.channel_key.0 != self.source_reply_channel {
                return Err(CnxError::Timeout);
            }

            if !self
                .first_source_recv_failed
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(CnxError::Internal(
                    "transient internal source-status collect gap before peer handler recv"
                        .to_string(),
                ));
            }

            let request_channel = self.source_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("source internal retry boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            Ok(self
                .source_status_payloads
                .iter()
                .enumerate()
                .map(|(idx, payload)| {
                    let origin = match idx {
                        0 => "node-b-29776175872804197050089473",
                        1 => "node-c-29776175872804197050089473",
                        _ => "node-d-29776175872804197050089473",
                    };
                    Event::new(
                        EventMetadata {
                            origin_id: NodeId(origin.to_string()),
                            timestamp_us: 1,
                            logical_ts: None,
                            correlation_id: Some(correlation),
                            ingress_auth: None,
                            trace: None,
                        },
                        bytes::Bytes::from(payload.clone()),
                    )
                })
                .collect())
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for SourceStatusPeerTransportRetryThenReplyBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            if let Some(correlation) = request
                .events
                .first()
                .and_then(|event| event.metadata().correlation_id)
            {
                self.correlations_by_channel
                    .lock()
                    .expect("source peer transport retry boundary correlations lock")
                    .insert(request.channel_key.0.clone(), correlation);
            }
            let mut send_batches = self
                .send_batches_by_channel
                .lock()
                .expect("source peer transport retry boundary send batches lock");
            *send_batches.entry(request.channel_key.0).or_default() += 1;
            Ok(())
        }

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            let mut recv_batches = self
                .recv_batches_by_channel
                .lock()
                .expect("source peer transport retry boundary recv batches lock");
            *recv_batches
                .entry(request.channel_key.0.clone())
                .or_default() += 1;
            drop(recv_batches);

            if request.channel_key.0 != self.source_reply_channel {
                return Err(CnxError::Timeout);
            }

            if !self
                .first_source_recv_failed
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(CnxError::PeerError(
                    "transport closed: sidecar control bridge stopped".to_string(),
                ));
            }

            let request_channel = self.source_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("source peer transport retry boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            Ok(self
                .source_status_payloads
                .iter()
                .enumerate()
                .map(|(idx, payload)| {
                    let origin = match idx {
                        0 => "node-b-29776275144172679041384449",
                        1 => "node-c-29776275144172679041384449",
                        _ => "node-d-29776275144172679041384449",
                    };
                    Event::new(
                        EventMetadata {
                            origin_id: NodeId(origin.to_string()),
                            timestamp_us: 1,
                            logical_ts: None,
                            correlation_id: Some(correlation),
                            ingress_auth: None,
                            trace: None,
                        },
                        bytes::Bytes::from(payload.clone()),
                    )
                })
                .collect())
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for SourceStatusIncompleteRetryThenReplyBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            if let Some(correlation) = request
                .events
                .first()
                .and_then(|event| event.metadata().correlation_id)
            {
                self.correlations_by_channel
                    .lock()
                    .expect("source incomplete retry boundary correlations lock")
                    .insert(request.channel_key.0.clone(), correlation);
            }
            let mut send_batches = self
                .send_batches_by_channel
                .lock()
                .expect("source incomplete retry boundary send batches lock");
            *send_batches.entry(request.channel_key.0).or_default() += 1;
            Ok(())
        }

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            let mut recv_batches = self
                .recv_batches_by_channel
                .lock()
                .expect("source incomplete retry boundary recv batches lock");
            *recv_batches
                .entry(request.channel_key.0.clone())
                .or_default() += 1;
            drop(recv_batches);

            if request.channel_key.0 != self.source_reply_channel {
                return Err(CnxError::Timeout);
            }

            let request_channel = self.source_reply_channel.trim_end_matches(":reply");
            let send_batch = self
                .send_batches_by_channel
                .lock()
                .expect("source incomplete retry boundary send batches lock")
                .get(request_channel)
                .copied()
                .unwrap_or_default();
            let recv_in_send = {
                let mut state = self
                    .recv_state
                    .lock()
                    .expect("source incomplete retry boundary recv state lock");
                if state.0 != send_batch {
                    *state = (send_batch, 0);
                }
                let recv_in_send = state.1;
                state.1 += 1;
                recv_in_send
            };
            if recv_in_send > 0 {
                return Err(CnxError::Timeout);
            }
            let payloads = if send_batch <= 1 {
                &self.first_source_status_payloads
            } else {
                &self.second_source_status_payloads
            };

            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("source incomplete retry boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            Ok(payloads
                .iter()
                .enumerate()
                .map(|(idx, payload)| {
                    let origin = match idx {
                        0 => "node-a-29776275144172679041384449",
                        1 => "node-b-29776275144172679041384449",
                        _ => "node-c-29776275144172679041384449",
                    };
                    Event::new(
                        EventMetadata {
                            origin_id: NodeId(origin.to_string()),
                            timestamp_us: 1,
                            logical_ts: None,
                            correlation_id: Some(correlation),
                            ingress_auth: None,
                            trace: None,
                        },
                        bytes::Bytes::from(payload.clone()),
                    )
                })
                .collect())
        }
    }

    impl StatusRemoteReplyBoundary {
        fn new(
            source_snapshots: Vec<(String, SourceObservabilitySnapshot)>,
            sink_snapshots: Vec<(String, SinkStatusSnapshot)>,
        ) -> Self {
            let source_route = default_route_bindings()
                .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
                .expect("resolve source-status route");
            let sink_route = default_route_bindings()
                .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
                .expect("resolve sink-status route");
            Self {
                source_request_channel: source_route.0.clone(),
                source_reply_channel: format!("{}:reply", source_route.0),
                sink_request_channel: sink_route.0.clone(),
                sink_reply_channel: format!("{}:reply", sink_route.0),
                source_status_payloads: source_snapshots
                    .into_iter()
                    .map(|(origin, snapshot)| {
                        let payload =
                            rmp_serde::to_vec_named(&snapshot).expect("encode source snapshot");
                        (origin, payload)
                    })
                    .collect(),
                sink_status_payloads: sink_snapshots
                    .into_iter()
                    .map(|(origin, snapshot)| {
                        let payload =
                            rmp_serde::to_vec_named(&snapshot).expect("encode sink snapshot");
                        (origin, payload)
                    })
                    .collect(),
                correlations_by_channel: StdMutex::new(std::collections::HashMap::new()),
                recv_batches_by_channel: StdMutex::new(std::collections::HashMap::new()),
            }
        }
    }

    impl StatusOverlapPoisonBoundary {
        fn new(
            source_snapshots: Vec<(String, SourceObservabilitySnapshot)>,
            sink_snapshots: Vec<(String, SinkStatusSnapshot)>,
        ) -> Self {
            let source_route = default_route_bindings()
                .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
                .expect("resolve source-status route");
            let sink_route = default_route_bindings()
                .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
                .expect("resolve sink-status route");
            Self {
                source_request_channel: source_route.0.clone(),
                source_reply_channel: format!("{}:reply", source_route.0),
                sink_request_channel: sink_route.0.clone(),
                sink_reply_channel: format!("{}:reply", sink_route.0),
                source_status_payloads: source_snapshots
                    .into_iter()
                    .map(|(origin, snapshot)| {
                        let payload =
                            rmp_serde::to_vec_named(&snapshot).expect("encode source snapshot");
                        (origin, payload)
                    })
                    .collect(),
                sink_status_payloads: sink_snapshots
                    .into_iter()
                    .map(|(origin, snapshot)| {
                        let payload =
                            rmp_serde::to_vec_named(&snapshot).expect("encode sink snapshot");
                        (origin, payload)
                    })
                    .collect(),
                correlations_by_channel: StdMutex::new(std::collections::HashMap::new()),
                recv_batches_by_channel: StdMutex::new(std::collections::HashMap::new()),
                active_recvs: std::sync::atomic::AtomicUsize::new(0),
                poisoned: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for StatusTransportRouteBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            _request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            Ok(())
        }

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            _request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            Err(CnxError::TransportClosed(
                "simulated transport remote status route failure".to_string(),
            ))
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for StatusSlowTimeoutRouteBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            _request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            Ok(())
        }

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            let timeout = request.timeout_ms.unwrap_or_default();
            tokio::time::sleep(Duration::from_millis(timeout + 50)).await;
            Err(CnxError::Timeout)
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for StatusRemoteReplyBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            if let Some(correlation) = request
                .events
                .first()
                .and_then(|event| event.metadata().correlation_id)
            {
                self.correlations_by_channel
                    .lock()
                    .expect("status remote reply correlations lock")
                    .insert(request.channel_key.0.clone(), correlation);
            }
            Ok(())
        }

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            let mut recv_batches = self
                .recv_batches_by_channel
                .lock()
                .expect("status remote reply recv batches lock");
            let recv_count = recv_batches
                .entry(request.channel_key.0.clone())
                .or_default();
            *recv_count += 1;
            let first_recv = *recv_count == 1;
            drop(recv_batches);

            if request.channel_key.0 == self.source_reply_channel {
                if !first_recv {
                    return Err(CnxError::Timeout);
                }
                let correlation = self
                    .correlations_by_channel
                    .lock()
                    .expect("status remote reply correlations lock")
                    .get(&self.source_request_channel)
                    .copied()
                    .unwrap_or(1);
                return Ok(self
                    .source_status_payloads
                    .iter()
                    .map(|(origin, payload)| {
                        Event::new(
                            EventMetadata {
                                origin_id: NodeId(origin.clone()),
                                timestamp_us: 1,
                                logical_ts: None,
                                correlation_id: Some(correlation),
                                ingress_auth: None,
                                trace: None,
                            },
                            bytes::Bytes::from(payload.clone()),
                        )
                    })
                    .collect());
            }
            if request.channel_key.0 == self.sink_reply_channel {
                if !first_recv {
                    return Err(CnxError::Timeout);
                }
                let correlation = self
                    .correlations_by_channel
                    .lock()
                    .expect("status remote reply correlations lock")
                    .get(&self.sink_request_channel)
                    .copied()
                    .unwrap_or(1);
                return Ok(self
                    .sink_status_payloads
                    .iter()
                    .map(|(origin, payload)| {
                        Event::new(
                            EventMetadata {
                                origin_id: NodeId(origin.clone()),
                                timestamp_us: 1,
                                logical_ts: None,
                                correlation_id: Some(correlation),
                                ingress_auth: None,
                                trace: None,
                            },
                            bytes::Bytes::from(payload.clone()),
                        )
                    })
                    .collect());
            }
            Err(CnxError::Timeout)
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for StatusOverlapPoisonBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            if let Some(correlation) = request
                .events
                .first()
                .and_then(|event| event.metadata().correlation_id)
            {
                self.correlations_by_channel
                    .lock()
                    .expect("status overlap poison correlations lock")
                    .insert(request.channel_key.0.clone(), correlation);
            }
            Ok(())
        }

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            let first_recv = {
                let mut recv_batches = self
                    .recv_batches_by_channel
                    .lock()
                    .expect("status overlap poison recv batches lock");
                let recv_count = recv_batches
                    .entry(request.channel_key.0.clone())
                    .or_default();
                *recv_count += 1;
                *recv_count == 1
            };

            if request.channel_key.0 != self.source_reply_channel
                && request.channel_key.0 != self.sink_reply_channel
            {
                return Err(CnxError::Timeout);
            }

            let previous = self
                .active_recvs
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::task::yield_now().await;
            if previous > 0 || self.active_recvs.load(std::sync::atomic::Ordering::SeqCst) > 1 {
                self.poisoned
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            let poisoned = self.poisoned.load(std::sync::atomic::Ordering::SeqCst);
            self.active_recvs
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            if poisoned {
                return Err(CnxError::TransportClosed(
                    "simulated same-boundary concurrent source/sink status collect poison"
                        .to_string(),
                ));
            }

            if request.channel_key.0 == self.source_reply_channel {
                if !first_recv {
                    return Err(CnxError::Timeout);
                }
                let correlation = self
                    .correlations_by_channel
                    .lock()
                    .expect("status overlap poison correlations lock")
                    .get(&self.source_request_channel)
                    .copied()
                    .unwrap_or(1);
                return Ok(self
                    .source_status_payloads
                    .iter()
                    .map(|(origin, payload)| {
                        Event::new(
                            EventMetadata {
                                origin_id: NodeId(origin.clone()),
                                timestamp_us: 1,
                                logical_ts: None,
                                correlation_id: Some(correlation),
                                ingress_auth: None,
                                trace: None,
                            },
                            bytes::Bytes::from(payload.clone()),
                        )
                    })
                    .collect());
            }

            if !first_recv {
                return Err(CnxError::Timeout);
            }
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("status overlap poison correlations lock")
                .get(&self.sink_request_channel)
                .copied()
                .unwrap_or(1);
            Ok(self
                .sink_status_payloads
                .iter()
                .map(|(origin, payload)| {
                    Event::new(
                        EventMetadata {
                            origin_id: NodeId(origin.clone()),
                            timestamp_us: 1,
                            logical_ts: None,
                            correlation_id: Some(correlation),
                            ingress_auth: None,
                            trace: None,
                        },
                        bytes::Bytes::from(payload.clone()),
                    )
                })
                .collect())
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for StatusInternalRouteBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            _request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            Ok(())
        }

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            _request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            Err(CnxError::Internal(
                "simulated internal remote status route failure".to_string(),
            ))
        }
    }

    impl ConcurrentInternalThenSequentialOkStatusBoundary {
        fn enter_call(&self) -> (usize, bool) {
            let previous = self
                .in_flight
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if previous > 0 {
                self.concurrent_fail_triggered
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            (previous, previous > 0)
        }

        fn leave_call(&self) {
            self.in_flight
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl ConcurrentTransportThenSequentialOkStatusBoundary {
        fn enter_call(&self) -> (usize, bool) {
            let previous = self
                .in_flight
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if previous > 0 {
                self.concurrent_fail_triggered
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            (previous, previous > 0)
        }

        fn leave_call(&self) {
            self.in_flight
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn write_auth_files(dir: &tempfile::TempDir) -> (PathBuf, PathBuf, PathBuf) {
        let passwd = dir.path().join("fs-meta.passwd");
        let shadow = dir.path().join("fs-meta.shadow");
        let query_keys = dir.path().join("fs-meta.query-keys.json");
        std::fs::write(
            &passwd,
            "admin:1000:1000:fsmeta_management:/home/admin:/bin/bash:0\n",
        )
        .expect("write passwd");
        std::fs::write(&shadow, "admin:plain$admin:0\n").expect("write shadow");
        std::fs::write(&query_keys, "{\n  \"keys\": []\n}\n").expect("write query keys");
        (passwd, shadow, query_keys)
    }

    fn management_headers(auth: &AuthService) -> HeaderMap {
        let (token, _, _) = auth.login("admin", "admin").expect("login admin");
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
        );
        headers
    }

    fn granted_mount_root(object_ref: &str, mount_point: &Path) -> GrantedMountRoot {
        GrantedMountRoot {
            object_ref: object_ref.to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "127.0.0.1".to_string(),
            host_name: Some("node-a".to_string()),
            site: None,
            zone: None,
            host_labels: BTreeMap::new(),
            mount_point: mount_point.to_path_buf(),
            fs_source: mount_point.display().to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
            active: true,
        }
    }

    fn source_status_event(
        origin: &str,
        correlation_id: u64,
        snapshot: SourceObservabilitySnapshot,
    ) -> Event {
        let payload = rmp_serde::to_vec_named(&snapshot).expect("encode source snapshot");
        Event::new(
            EventMetadata {
                origin_id: NodeId(origin.to_string()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: Some(correlation_id),
                ingress_auth: None,
                trace: None,
            },
            bytes::Bytes::from(payload),
        )
    }

    fn local_source_snapshot() -> SourceObservabilitySnapshot {
        SourceObservabilitySnapshot {
            lifecycle_state: "ready".to_string(),
            host_object_grants_version: 1,
            grants: vec![GrantedMountRoot {
                object_ref: "node-a::nfs1".to_string(),
                host_ref: "node-a".to_string(),
                host_ip: "10.0.0.11".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: BTreeMap::new(),
                mount_point: "/mnt/nfs1".into(),
                fs_source: "srv:/nfs1".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: Vec::new(),
                active: true,
            }],
            logical_roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            status: SourceStatusSnapshot {
                current_stream_generation: Some(11),
                logical_roots: vec![SourceLogicalRootHealthSnapshot {
                    root_id: "nfs1".to_string(),
                    status: "healthy".to_string(),
                    matched_grants: 1,
                    active_members: 1,
                    coverage_mode: "watch".to_string(),
                }],
                concrete_roots: vec![SourceConcreteRootHealthSnapshot {
                    root_key: "nfs1@node-a".to_string(),
                    logical_root_id: "nfs1".to_string(),
                    object_ref: "node-a::nfs1".to_string(),
                    status: "healthy".to_string(),
                    coverage_mode: "watch".to_string(),
                    watch_enabled: true,
                    scan_enabled: true,
                    is_group_primary: true,
                    active: true,
                    watch_lru_capacity: 1024,
                    audit_interval_ms: 300_000,
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
                    current_revision: Some(1),
                    current_stream_generation: Some(11),
                    candidate_revision: None,
                    candidate_stream_generation: None,
                    candidate_status: None,
                    draining_revision: None,
                    draining_stream_generation: None,
                    draining_status: None,
                }],
                degraded_roots: Vec::new(),
            },
            source_primary_by_group: BTreeMap::from([("nfs1".to_string(), "node-a".to_string())]),
            last_force_find_runner_by_group: BTreeMap::from([(
                "nfs1".to_string(),
                "node-a".to_string(),
            )]),
            force_find_inflight_groups: Vec::new(),
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            last_control_frame_signals_by_node: BTreeMap::new(),
            published_batches_by_node: BTreeMap::new(),
            published_events_by_node: BTreeMap::new(),
            published_control_events_by_node: BTreeMap::new(),
            published_data_events_by_node: BTreeMap::new(),
            last_published_at_us_by_node: BTreeMap::new(),
            last_published_origins_by_node: BTreeMap::new(),
            published_origin_counts_by_node: BTreeMap::new(),
            published_path_capture_target: None,
            enqueued_path_origin_counts_by_node: BTreeMap::new(),
            pending_path_origin_counts_by_node: BTreeMap::new(),
            yielded_path_origin_counts_by_node: BTreeMap::new(),
            summarized_path_origin_counts_by_node: BTreeMap::new(),
            published_path_origin_counts_by_node: BTreeMap::new(),
        }
    }

    fn local_sink_snapshot() -> SinkStatusSnapshot {
        SinkStatusSnapshot {
            live_nodes: 1,
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            groups: vec![crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "obj-a".to_string(),
                total_nodes: 1,
                live_nodes: 1,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 11,
                shadow_lag_us: 12,
                overflow_pending_audit: false,
                initial_audit_completed: true,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            }],
            ..SinkStatusSnapshot::default()
        }
    }

    #[test]
    fn snapshot_scoped_active_facade_candidate_groups_ignores_source_only_groups_without_local_sink_schedule()
     {
        let local_source = local_source_snapshot();
        let local_sink = SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::new(),
            groups: vec![crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "obj-a".to_string(),
                total_nodes: 1,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 11,
                shadow_lag_us: 12,
                overflow_pending_audit: false,
                initial_audit_completed: false,
                readiness: crate::sink::GroupReadinessState::PendingAudit,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            }],
            ..SinkStatusSnapshot::default()
        };

        let candidate_groups =
            snapshot_scoped_active_facade_candidate_groups(&local_source, &local_sink);

        assert!(
            candidate_groups.is_empty(),
            "source-only scheduled groups without any local sink schedule must not become active facade observation candidates: {candidate_groups:?}"
        );
    }

    #[tokio::test]
    async fn roots_put_succeeds_when_source_control_send_hits_drained_stale_pid_after_local_update()
    {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        std::fs::create_dir_all(&nfs2).expect("create nfs2");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let source_cfg = SourceConfig {
            roots: vec![RootSpec::new("nfs1", &nfs1)],
            host_object_grants: vec![
                granted_mount_root("node-a::nfs1", &nfs1),
                granted_mount_root("node-a::nfs2", &nfs2),
            ],
            ..SourceConfig::default()
        };
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::new(source_cfg.clone(), NodeId("node-a".into())).expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg.clone())
                .expect("sink"),
        )));
        let sent_routes = Arc::new(StdMutex::new(Vec::new()));
        let boundary = Arc::new(DeniedControlRouteBoundary {
            sent_routes: sent_routes.clone(),
            denied_routes: BTreeSet::from([format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL)]),
        });
        let headers = management_headers(auth.as_ref());
        let facade_service_state = shared_facade_service_state_cell();
        *facade_service_state
            .write()
            .expect("write facade service state") =
            crate::domain_state::FacadeServiceState::Serving;
        *facade_service_state
            .write()
            .expect("write facade service state") = FacadeServiceState::Pending;
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: Some(boundary),
            query_runtime_boundary: None,
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source: source.clone(),
            sink: sink.clone(),
            query_sink: sink.clone(),
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                facade_service_state,
                shared_facade_pending_status_cell(),
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let response = roots_put(
            State(state),
            headers,
            Json(RootsUpdateRequest {
                roots: vec![
                    RootUpdateEntry {
                        id: "nfs1".to_string(),
                        selector: RootSelectorEntry {
                            mount_point: Some(nfs1.display().to_string()),
                            fs_source: None,
                            fs_type: None,
                            host_ip: None,
                            host_ref: None,
                        },
                        subpath_scope: "/".to_string(),
                        watch: true,
                        scan: true,
                        audit_interval_ms: None,
                        source_locator_present: false,
                        path_present: false,
                    },
                    RootUpdateEntry {
                        id: "nfs2".to_string(),
                        selector: RootSelectorEntry {
                            mount_point: Some(nfs2.display().to_string()),
                            fs_source: None,
                            fs_type: None,
                            host_ip: None,
                            host_ref: None,
                        },
                        subpath_scope: "/".to_string(),
                        watch: true,
                        scan: true,
                        audit_interval_ms: None,
                        source_locator_present: false,
                        path_present: false,
                    },
                ],
            }),
        )
        .await
        .expect("roots_put should succeed even if the old pid is already drained");

        assert_eq!(response.0.roots_count, 2);
        assert_eq!(
            source
                .logical_roots_snapshot()
                .await
                .expect("source roots snapshot")
                .len(),
            2
        );
        assert_eq!(
            sink.cached_logical_roots_snapshot()
                .expect("sink roots snapshot")
                .len(),
            2
        );
        let sent_routes = match sent_routes.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        assert_eq!(
            sent_routes,
            vec![
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
            ]
        );
    }

    #[tokio::test]
    async fn rescan_succeeds_when_manual_rescan_control_send_hits_drained_stale_pid() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let root = RootSpec::new("nfs1", &nfs1);
        let source_cfg = SourceConfig {
            roots: vec![root],
            host_object_grants: vec![granted_mount_root("node-a::nfs1", &nfs1)],
            ..SourceConfig::default()
        };
        let state_boundary = in_memory_state_boundary();
        let signal = SignalCell::from_state_boundary(
            crate::runtime::execution_units::SOURCE_RUNTIME_UNIT_ID,
            "manual_rescan",
            state_boundary.clone(),
        )
        .expect("construct manual rescan signal cell");
        let signal_offset = signal.current_seq();
        let source_runtime = Arc::new(
            FSMetaSource::with_boundaries_and_state(
                source_cfg.clone(),
                NodeId("node-a".into()),
                None,
                state_boundary,
            )
            .expect("source"),
        );
        let source = Arc::new(SourceFacade::local(source_runtime.clone()));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg.clone())
                .expect("sink"),
        )));
        let sent_routes = Arc::new(StdMutex::new(Vec::new()));
        let boundary = Arc::new(DeniedControlRouteBoundary {
            sent_routes: sent_routes.clone(),
            denied_routes: BTreeSet::from([
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                format!("{}.req", source_rescan_route_key_for("node-a")),
            ]),
        });
        let headers = management_headers(auth.as_ref());
        let facade_service_state = crate::api::facade_status::shared_facade_service_state_cell();
        *facade_service_state
            .write()
            .expect("write facade service state") = FacadeServiceState::Serving;
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: Some(boundary),
            query_runtime_boundary: None,
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source: source.clone(),
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                facade_service_state,
                shared_facade_pending_status_cell(),
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let response = rescan(State(state), headers)
            .await
            .expect("rescan should tolerate a stale drained control pid by falling back to the signal carrier");
        assert!(response.0.accepted);
        let (_next_offset, updates) = signal
            .watch_since(signal_offset)
            .await
            .expect("watch signal updates");
        assert_eq!(
            updates.len(),
            1,
            "manual rescan signal should emit exactly one update"
        );
        assert_eq!(updates[0].requested_by, "node-a");

        let sent_routes = match sent_routes.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        assert_eq!(
            sent_routes,
            vec![format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL)]
        );
    }

    #[test]
    fn status_source_from_observability_preserves_debug_path_origin_counts() {
        let source = SourceObservabilitySnapshot {
            enqueued_path_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs2=15".to_string()],
            )]),
            pending_path_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs2=3".to_string()],
            )]),
            yielded_path_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs2=12".to_string()],
            )]),
            summarized_path_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs2=7".to_string()],
            )]),
            published_path_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=5".to_string()],
            )]),
            ..local_source_snapshot()
        };

        let status = status_source_from_observability(source, BTreeMap::new(), Vec::new());

        assert_eq!(
            status
                .debug
                .enqueued_path_origin_counts_by_node
                .get("node-a"),
            Some(&vec!["node-a::nfs2=15".to_string()])
        );
        assert_eq!(
            status
                .debug
                .pending_path_origin_counts_by_node
                .get("node-a"),
            Some(&vec!["node-a::nfs2=3".to_string()])
        );
        assert_eq!(
            status
                .debug
                .yielded_path_origin_counts_by_node
                .get("node-a"),
            Some(&vec!["node-a::nfs2=12".to_string()])
        );
        assert_eq!(
            status
                .debug
                .summarized_path_origin_counts_by_node
                .get("node-a"),
            Some(&vec!["node-a::nfs2=7".to_string()])
        );
        assert_eq!(
            status
                .debug
                .published_path_origin_counts_by_node
                .get("node-a"),
            Some(&vec!["node-a::nfs1=5".to_string()])
        );
    }

    #[test]
    fn status_source_from_observability_preserves_force_find_runner_fields() {
        let source = SourceObservabilitySnapshot {
            last_force_find_runner_by_group: BTreeMap::from([(
                "nfs1".to_string(),
                "node-a::nfs1".to_string(),
            )]),
            ..local_source_snapshot()
        };
        let runner_sets = BTreeMap::from([(
            "nfs1".to_string(),
            vec!["node-a::nfs1".to_string(), "node-b::nfs1".to_string()],
        )]);

        let status =
            status_source_from_observability(source, runner_sets.clone(), vec!["nfs1".to_string()]);

        assert_eq!(
            status.debug.last_force_find_runner_by_group.get("nfs1"),
            Some(&"node-a::nfs1".to_string())
        );
        assert_eq!(
            status.debug.last_force_find_runners_by_group.get("nfs1"),
            runner_sets.get("nfs1")
        );
        assert_eq!(
            status.debug.force_find_inflight_groups,
            vec!["nfs1".to_string()]
        );
    }

    #[test]
    fn status_source_from_observability_preserves_concrete_root_transition_fields() {
        let status =
            status_source_from_observability(local_source_snapshot(), BTreeMap::new(), Vec::new());

        assert_eq!(status.debug.current_stream_generation, Some(11));
        let root = status
            .concrete_roots
            .iter()
            .find(|entry| entry.object_ref == "node-a::nfs1")
            .expect("concrete root present");
        assert_eq!(root.current_revision, Some(1));
        assert_eq!(root.current_stream_generation, Some(11));
        assert_eq!(root.candidate_revision, None);
        assert_eq!(root.candidate_stream_generation, None);
        assert_eq!(root.draining_revision, None);
        assert_eq!(root.draining_stream_generation, None);
    }

    #[tokio::test]
    async fn status_remote_merge_returns_local_source_when_source_times_out() {
        let local_sink = local_sink_snapshot();
        let local_source = local_source_snapshot();
        let (sink, source, runner_sets, sink_outcome, source_outcome) =
            merge_remote_status_snapshots(
                local_sink.clone(),
                local_source.clone(),
                None,
                NodeId("node-a".into()),
                |_boundary, _origin_id| async move {
                    unreachable!("sink collector should not run without a boundary")
                },
                |_boundary, _origin_id| async move {
                    unreachable!("source collector should not run without a boundary")
                },
            )
            .await;

        assert_eq!(sink.live_nodes, local_sink.live_nodes);
        assert_eq!(
            source.source_primary_by_group,
            local_source.source_primary_by_group
        );
        assert_eq!(runner_sets, local_runner_sets(&local_source));
        assert_eq!(sink_outcome, StatusRouteOutcome::Skipped);
        assert_eq!(source_outcome, StatusRouteOutcome::Skipped);
    }

    #[tokio::test]
    async fn status_remote_merge_keeps_local_when_source_fails_and_sink_succeeds() {
        let local_sink = local_sink_snapshot();
        let local_source = local_source_snapshot();
        let remote_sink = SinkStatusSnapshot {
            live_nodes: 3,
            groups: vec![crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "obj-b".to_string(),
                total_nodes: 2,
                live_nodes: 2,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 21,
                shadow_lag_us: 22,
                overflow_pending_audit: false,
                initial_audit_completed: true,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            }],
            ..SinkStatusSnapshot::default()
        };
        let (sink, source, runner_sets, sink_outcome, source_outcome) =
            merge_remote_status_snapshots(
                local_sink,
                local_source.clone(),
                Some(std::sync::Arc::new(NoopBoundary)),
                NodeId("node-a".into()),
                move |_boundary, _origin_id| {
                    let remote_sink = remote_sink.clone();
                    async move { Ok(remote_sink) }
                },
                |_boundary, _origin_id| async move { Err(CnxError::Timeout) },
            )
            .await;

        assert_eq!(sink.groups.len(), 2);
        assert_eq!(
            source.source_primary_by_group,
            local_source.source_primary_by_group
        );
        assert_eq!(runner_sets, local_runner_sets(&local_source));
        assert_eq!(sink_outcome, StatusRouteOutcome::Ok);
        assert_eq!(source_outcome, StatusRouteOutcome::Timeout);
    }

    #[tokio::test]
    async fn status_remote_merge_preserves_remote_force_find_runner_fields() {
        let local_sink = local_sink_snapshot();
        let local_source = SourceObservabilitySnapshot {
            last_force_find_runner_by_group: BTreeMap::new(),
            ..local_source_snapshot()
        };
        let remote_source = SourceObservabilitySnapshot {
            source_primary_by_group: BTreeMap::from([(
                "nfs1".to_string(),
                "node-b::nfs1".to_string(),
            )]),
            last_force_find_runner_by_group: BTreeMap::from([(
                "nfs1".to_string(),
                "node-b::nfs1".to_string(),
            )]),
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..local_source_snapshot()
        };
        let remote_runner_sets = BTreeMap::from([(
            "nfs1".to_string(),
            vec!["node-a::nfs1".to_string(), "node-b::nfs1".to_string()],
        )]);

        let (_sink, source, runner_sets, sink_outcome, source_outcome) =
            merge_remote_status_snapshots(
                local_sink,
                local_source,
                Some(std::sync::Arc::new(NoopBoundary)),
                NodeId("node-a".into()),
                |_boundary, _origin_id| async move { Ok(SinkStatusSnapshot::default()) },
                move |_boundary, _origin_id| {
                    let remote_source = remote_source.clone();
                    let remote_runner_sets = remote_runner_sets.clone();
                    async move { Ok((remote_source, remote_runner_sets)) }
                },
            )
            .await;

        assert_eq!(
            source.last_force_find_runner_by_group.get("nfs1"),
            Some(&"node-b::nfs1".to_string())
        );
        let merged_runner_set = runner_sets.get("nfs1").expect("merged runner set for nfs1");
        assert!(
            merged_runner_set.contains(&"node-a::nfs1".to_string())
                && merged_runner_set.contains(&"node-b::nfs1".to_string()),
            "sequential retry should preserve merged local+remote runner evidence: {merged_runner_set:?}"
        );
        assert_eq!(sink_outcome, StatusRouteOutcome::Ok);
        assert_eq!(source_outcome, StatusRouteOutcome::Ok);
    }

    #[tokio::test]
    async fn status_remote_merge_runs_collectors_concurrently() {
        let started = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let local_sink = local_sink_snapshot();
        let local_source = local_source_snapshot();
        let started_for_sink = started.clone();
        let started_for_source = started.clone();

        let (_sink, _source, _runner_sets, sink_outcome, source_outcome) =
            merge_remote_status_snapshots(
                local_sink,
                local_source,
                Some(std::sync::Arc::new(NoopBoundary)),
                NodeId("node-a".into()),
                move |_boundary, _origin_id| {
                    let started = started_for_sink.clone();
                    async move {
                        started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok(SinkStatusSnapshot::default())
                    }
                },
                move |_boundary, _origin_id| {
                    let started = started_for_source.clone();
                    async move {
                        started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok((local_source_snapshot(), BTreeMap::new()))
                    }
                },
            )
            .await;

        assert_eq!(started.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(sink_outcome, StatusRouteOutcome::Ok);
        assert_eq!(source_outcome, StatusRouteOutcome::Ok);
    }

    #[test]
    fn classify_status_route_error_treats_internal_transport_close_strings_as_transport() {
        assert_eq!(
            classify_status_route_error(&CnxError::Internal(
                "transport closed: sidecar control bridge stopped".to_string(),
            )),
            StatusRouteOutcome::Transport
        );
        assert_eq!(
            classify_status_route_error(&CnxError::Internal("bound route is closed".to_string(),)),
            StatusRouteOutcome::Transport
        );
        assert_eq!(
            classify_status_route_error(&CnxError::ChannelClosed),
            StatusRouteOutcome::Transport
        );
    }

    #[test]
    fn classify_status_route_error_treats_peer_transport_close_strings_as_transport() {
        assert_eq!(
            classify_status_route_error(&CnxError::PeerError(
                "transport closed: sidecar control bridge stopped".to_string(),
            )),
            StatusRouteOutcome::Transport
        );
        assert_eq!(
            classify_status_route_error(&CnxError::PeerError(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof".to_string(),
            )),
            StatusRouteOutcome::Transport
        );
    }

    #[test]
    fn classify_status_route_error_treats_stale_grant_attachment_denials_as_transport() {
        assert_eq!(
            classify_status_route_error(&CnxError::AccessDenied(
                "pid Pid(4) is drained/fenced and cannot obtain new grant attachments".to_string(),
            )),
            StatusRouteOutcome::Transport
        );
        assert_eq!(
            classify_status_route_error(&CnxError::AccessDenied(
                "invalid or revoked grant attachment token".to_string(),
            )),
            StatusRouteOutcome::Transport
        );
    }

    #[tokio::test]
    async fn status_remote_merge_retries_sequentially_when_both_collectors_return_internal_concurrently()
     {
        let local_sink = local_sink_snapshot();
        let local_source = local_source_snapshot();
        let boundary =
            std::sync::Arc::new(ConcurrentInternalThenSequentialOkStatusBoundary::default());
        let remote_sink = SinkStatusSnapshot {
            live_nodes: 3,
            groups: vec![crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "obj-b".to_string(),
                total_nodes: 2,
                live_nodes: 2,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 21,
                shadow_lag_us: 22,
                overflow_pending_audit: false,
                initial_audit_completed: true,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            }],
            ..SinkStatusSnapshot::default()
        };
        let remote_source = SourceObservabilitySnapshot {
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            ..local_source_snapshot()
        };
        let remote_runner_sets = BTreeMap::from([(
            "nfs1".to_string(),
            vec!["node-a::nfs1".to_string(), "node-b::nfs1".to_string()],
        )]);
        let boundary_for_sink = boundary.clone();
        let boundary_for_source = boundary.clone();

        let (sink, source, runner_sets, sink_outcome, source_outcome) =
            merge_remote_status_snapshots(
                local_sink,
                local_source,
                Some(std::sync::Arc::new(NoopBoundary)),
                NodeId("node-a".into()),
                move |_boundary, _origin_id| {
                    let boundary = boundary_for_sink.clone();
                    let remote_sink = remote_sink.clone();
                    async move {
                        let call_idx = boundary
                            .sink_calls
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let (_previous, overlapped) = boundary.enter_call();
                        tokio::task::yield_now().await;
                        let first_round_conflict = call_idx == 0
                            && boundary
                                .concurrent_fail_triggered
                                .load(std::sync::atomic::Ordering::SeqCst);
                        boundary.leave_call();
                        if overlapped || first_round_conflict {
                            return Err(CnxError::Internal(
                                "simulated concurrent internal sink status-route collect gap"
                                    .to_string(),
                            ));
                        }
                        Ok(remote_sink)
                    }
                },
                move |_boundary, _origin_id| {
                    let boundary = boundary_for_source.clone();
                    let remote_source = remote_source.clone();
                    let remote_runner_sets = remote_runner_sets.clone();
                    async move {
                        let call_idx = boundary
                            .source_calls
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let (_previous, overlapped) = boundary.enter_call();
                        tokio::task::yield_now().await;
                        let first_round_conflict = call_idx == 0
                            && boundary
                                .concurrent_fail_triggered
                                .load(std::sync::atomic::Ordering::SeqCst);
                        boundary.leave_call();
                        if overlapped || first_round_conflict {
                            return Err(CnxError::Internal(
                                "simulated concurrent internal source status-route collect gap"
                                    .to_string(),
                            ));
                        }
                        Ok((remote_source, remote_runner_sets))
                    }
                },
            )
            .await;

        assert!(
            boundary
                .concurrent_fail_triggered
                .load(std::sync::atomic::Ordering::SeqCst),
            "fixture must prove the first concurrent merge attempt hit a shared internal collect gap"
        );
        assert_eq!(sink_outcome, StatusRouteOutcome::Ok);
        assert_eq!(source_outcome, StatusRouteOutcome::Ok);
        assert_eq!(sink.groups.len(), 2);
        assert_eq!(
            source.scheduled_source_groups_by_node.get("node-b"),
            Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
        );
        let merged_runner_set = runner_sets.get("nfs1").expect("merged runner set for nfs1");
        assert!(
            merged_runner_set.contains(&"node-a::nfs1".to_string())
                && merged_runner_set.contains(&"node-b::nfs1".to_string()),
            "sequential retry should preserve merged local+remote runner evidence: {merged_runner_set:?}"
        );
        assert!(
            boundary
                .sink_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                >= 2
        );
        assert!(
            boundary
                .source_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                >= 2
        );
    }

    #[tokio::test]
    async fn status_remote_merge_retries_sequentially_when_both_collectors_return_transport_concurrently()
     {
        let local_sink = local_sink_snapshot();
        let local_source = local_source_snapshot();
        let boundary =
            std::sync::Arc::new(ConcurrentTransportThenSequentialOkStatusBoundary::default());
        let remote_sink = SinkStatusSnapshot {
            live_nodes: 3,
            groups: vec![crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "obj-b".to_string(),
                total_nodes: 2,
                live_nodes: 2,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 21,
                shadow_lag_us: 22,
                overflow_pending_audit: false,
                initial_audit_completed: true,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            }],
            ..SinkStatusSnapshot::default()
        };
        let remote_source = SourceObservabilitySnapshot {
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            ..local_source_snapshot()
        };
        let remote_runner_sets = BTreeMap::from([(
            "nfs1".to_string(),
            vec!["node-a::nfs1".to_string(), "node-b::nfs1".to_string()],
        )]);
        let boundary_for_sink = boundary.clone();
        let boundary_for_source = boundary.clone();

        let (sink, source, runner_sets, sink_outcome, source_outcome) =
            merge_remote_status_snapshots(
                local_sink,
                local_source,
                Some(std::sync::Arc::new(NoopBoundary)),
                NodeId("node-a".into()),
                move |_boundary, _origin_id| {
                    let boundary = boundary_for_sink.clone();
                    let remote_sink = remote_sink.clone();
                    async move {
                        let call_idx = boundary
                            .sink_calls
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let (_previous, overlapped) = boundary.enter_call();
                        tokio::task::yield_now().await;
                        let first_round_conflict = call_idx == 0
                            && boundary
                                .concurrent_fail_triggered
                                .load(std::sync::atomic::Ordering::SeqCst);
                        boundary.leave_call();
                        if overlapped || first_round_conflict {
                            return Err(CnxError::TransportClosed(
                                "simulated concurrent transport sink status-route collect gap"
                                    .to_string(),
                            ));
                        }
                        Ok(remote_sink)
                    }
                },
                move |_boundary, _origin_id| {
                    let boundary = boundary_for_source.clone();
                    let remote_source = remote_source.clone();
                    let remote_runner_sets = remote_runner_sets.clone();
                    async move {
                        let call_idx = boundary
                            .source_calls
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let (_previous, overlapped) = boundary.enter_call();
                        tokio::task::yield_now().await;
                        let first_round_conflict = call_idx == 0
                            && boundary
                                .concurrent_fail_triggered
                                .load(std::sync::atomic::Ordering::SeqCst);
                        boundary.leave_call();
                        if overlapped || first_round_conflict {
                            return Err(CnxError::TransportClosed(
                                "simulated concurrent transport source status-route collect gap"
                                    .to_string(),
                            ));
                        }
                        Ok((remote_source, remote_runner_sets))
                    }
                },
            )
            .await;

        assert!(
            boundary
                .concurrent_fail_triggered
                .load(std::sync::atomic::Ordering::SeqCst),
            "fixture must prove the first concurrent merge attempt hit a shared transport collect gap"
        );
        assert_eq!(sink_outcome, StatusRouteOutcome::Ok);
        assert_eq!(source_outcome, StatusRouteOutcome::Ok);
        assert_eq!(sink.groups.len(), 2);
        assert_eq!(
            source.scheduled_source_groups_by_node.get("node-b"),
            Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
        );
        let merged_runner_set = runner_sets.get("nfs1").expect("merged runner set for nfs1");
        assert!(
            merged_runner_set.contains(&"node-a::nfs1".to_string())
                && merged_runner_set.contains(&"node-b::nfs1".to_string()),
            "sequential retry should preserve merged local+remote runner evidence: {merged_runner_set:?}"
        );
        assert!(
            boundary
                .sink_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                >= 2
        );
        assert!(
            boundary
                .source_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                >= 2
        );
    }

    #[test]
    fn merge_source_observability_prefers_live_collect_data_over_stale_cache() {
        let local = SourceObservabilitySnapshot {
            lifecycle_state: "degraded_worker_unreachable".to_string(),
            host_object_grants_version: 1,
            grants: vec![GrantedMountRoot {
                object_ref: "node-a::nfs1".to_string(),
                host_ref: "node-a".to_string(),
                host_ip: "10.0.0.11".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: BTreeMap::new(),
                mount_point: "/mnt/nfs1".into(),
                fs_source: "srv:/nfs1".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: Vec::new(),
                active: true,
            }],
            logical_roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            status: SourceStatusSnapshot {
                degraded_roots: vec![(
                    "source-worker".to_string(),
                    "source worker status served from cache".to_string(),
                )],
                ..SourceStatusSnapshot::default()
            },
            source_primary_by_group: BTreeMap::from([("nfs1".to_string(), "node-a".to_string())]),
            last_force_find_runner_by_group: BTreeMap::new(),
            force_find_inflight_groups: Vec::new(),
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            last_control_frame_signals_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["activate nfs1".to_string()],
            )]),
            published_batches_by_node: BTreeMap::new(),
            published_events_by_node: BTreeMap::new(),
            published_control_events_by_node: BTreeMap::new(),
            published_data_events_by_node: BTreeMap::new(),
            last_published_at_us_by_node: BTreeMap::new(),
            last_published_origins_by_node: BTreeMap::new(),
            published_origin_counts_by_node: BTreeMap::new(),
            published_path_capture_target: None,
            enqueued_path_origin_counts_by_node: BTreeMap::new(),
            pending_path_origin_counts_by_node: BTreeMap::new(),
            yielded_path_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=19".to_string()],
            )]),
            summarized_path_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=13".to_string()],
            )]),
            published_path_origin_counts_by_node: BTreeMap::new(),
        };
        let aggregated = SourceObservabilitySnapshot {
            lifecycle_state: "ready".to_string(),
            host_object_grants_version: 7,
            grants: vec![GrantedMountRoot {
                object_ref: "node-b::nfs2".to_string(),
                host_ref: "node-b".to_string(),
                host_ip: "10.0.0.12".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: BTreeMap::new(),
                mount_point: "/mnt/nfs2".into(),
                fs_source: "srv:/nfs2".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: Vec::new(),
                active: true,
            }],
            logical_roots: vec![RootSpec::new("nfs2", "/mnt/nfs2")],
            status: SourceStatusSnapshot {
                current_stream_generation: Some(13),
                logical_roots: vec![SourceLogicalRootHealthSnapshot {
                    root_id: "nfs2".to_string(),
                    status: "healthy".to_string(),
                    matched_grants: 1,
                    active_members: 1,
                    coverage_mode: "watch".to_string(),
                }],
                concrete_roots: vec![SourceConcreteRootHealthSnapshot {
                    root_key: "nfs2@node-b".to_string(),
                    logical_root_id: "nfs2".to_string(),
                    object_ref: "node-b::nfs2".to_string(),
                    status: "healthy".to_string(),
                    coverage_mode: "watch".to_string(),
                    watch_enabled: true,
                    scan_enabled: true,
                    is_group_primary: true,
                    active: true,
                    watch_lru_capacity: 1024,
                    audit_interval_ms: 300_000,
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
                    current_revision: Some(1),
                    current_stream_generation: Some(13),
                    candidate_revision: None,
                    candidate_stream_generation: None,
                    candidate_status: None,
                    draining_revision: None,
                    draining_stream_generation: None,
                    draining_status: None,
                }],
                degraded_roots: Vec::new(),
            },
            source_primary_by_group: BTreeMap::from([("nfs2".to_string(), "node-b".to_string())]),
            last_force_find_runner_by_group: BTreeMap::from([(
                "nfs2".to_string(),
                "node-b".to_string(),
            )]),
            force_find_inflight_groups: vec!["nfs2".to_string()],
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs2".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs2".to_string()],
            )]),
            last_control_frame_signals_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["activate nfs2".to_string()],
            )]),
            published_batches_by_node: BTreeMap::from([("node-b".to_string(), 7)]),
            published_events_by_node: BTreeMap::from([("node-b".to_string(), 77)]),
            published_control_events_by_node: BTreeMap::from([("node-b".to_string(), 3)]),
            published_data_events_by_node: BTreeMap::from([("node-b".to_string(), 74)]),
            last_published_at_us_by_node: BTreeMap::from([("node-b".to_string(), 42)]),
            last_published_origins_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=1".to_string()],
            )]),
            published_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=77".to_string()],
            )]),
            published_path_capture_target: None,
            enqueued_path_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=31".to_string()],
            )]),
            pending_path_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=2".to_string()],
            )]),
            yielded_path_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=29".to_string()],
            )]),
            summarized_path_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=23".to_string()],
            )]),
            published_path_origin_counts_by_node: BTreeMap::new(),
        };

        let merged = merge_source_observability(local, aggregated);

        assert_eq!(merged.lifecycle_state, "ready");
        assert_eq!(merged.host_object_grants_version, 7);
        assert_eq!(merged.grants.len(), 2);
        assert_eq!(merged.logical_roots.len(), 2);
        assert!(
            merged
                .status
                .degraded_roots
                .iter()
                .all(|(_, reason)| reason != "source worker status served from cache")
        );
        assert_eq!(
            merged.source_primary_by_group.get("nfs2"),
            Some(&"node-b".to_string())
        );
        assert_eq!(
            merged.scheduled_source_groups_by_node.get("node-a"),
            Some(&vec!["nfs1".to_string()])
        );
        assert_eq!(
            merged.scheduled_source_groups_by_node.get("node-b"),
            Some(&vec!["nfs2".to_string()])
        );
        assert_eq!(
            merged.last_control_frame_signals_by_node.get("node-a"),
            Some(&vec!["activate nfs1".to_string()])
        );
        assert_eq!(
            merged.last_control_frame_signals_by_node.get("node-b"),
            Some(&vec!["activate nfs2".to_string()])
        );
        assert_eq!(merged.published_batches_by_node.get("node-b"), Some(&7));
        assert_eq!(merged.published_events_by_node.get("node-b"), Some(&77));
        assert_eq!(
            merged.last_published_origins_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=1".to_string()])
        );
        assert_eq!(
            merged.published_origin_counts_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=77".to_string()])
        );
        assert_eq!(
            merged.yielded_path_origin_counts_by_node.get("node-a"),
            Some(&vec!["node-a::nfs1=19".to_string()])
        );
        assert_eq!(
            merged.yielded_path_origin_counts_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=29".to_string()])
        );
        assert_eq!(
            merged.enqueued_path_origin_counts_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=31".to_string()])
        );
        assert_eq!(
            merged.pending_path_origin_counts_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=2".to_string()])
        );
        assert_eq!(
            merged.summarized_path_origin_counts_by_node.get("node-a"),
            Some(&vec!["node-a::nfs1=13".to_string()])
        );
        assert_eq!(
            merged.summarized_path_origin_counts_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=23".to_string()])
        );
        assert_eq!(merged.force_find_inflight_groups, vec!["nfs2".to_string()]);
    }

    #[test]
    fn merge_source_observability_preserves_non_empty_fallback_scheduled_groups_when_live_snapshot_has_empty_entry()
     {
        let local = SourceObservabilitySnapshot {
            lifecycle_state: "degraded_worker_unreachable".to_string(),
            host_object_grants_version: 1,
            grants: Vec::new(),
            logical_roots: Vec::new(),
            status: SourceStatusSnapshot::default(),
            source_primary_by_group: BTreeMap::new(),
            last_force_find_runner_by_group: BTreeMap::new(),
            force_find_inflight_groups: Vec::new(),
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            last_control_frame_signals_by_node: BTreeMap::new(),
            published_batches_by_node: BTreeMap::new(),
            published_events_by_node: BTreeMap::new(),
            published_control_events_by_node: BTreeMap::new(),
            published_data_events_by_node: BTreeMap::new(),
            last_published_at_us_by_node: BTreeMap::new(),
            last_published_origins_by_node: BTreeMap::new(),
            published_origin_counts_by_node: BTreeMap::new(),
            published_path_capture_target: None,
            enqueued_path_origin_counts_by_node: BTreeMap::new(),
            pending_path_origin_counts_by_node: BTreeMap::new(),
            yielded_path_origin_counts_by_node: BTreeMap::new(),
            summarized_path_origin_counts_by_node: BTreeMap::new(),
            published_path_origin_counts_by_node: BTreeMap::new(),
        };
        let aggregated = SourceObservabilitySnapshot {
            lifecycle_state: "ready".to_string(),
            host_object_grants_version: 7,
            grants: vec![GrantedMountRoot {
                object_ref: "node-b::nfs1".to_string(),
                host_ref: "node-b".to_string(),
                host_ip: "10.0.0.12".to_string(),
                host_name: None,
                site: None,
                zone: None,
                host_labels: BTreeMap::new(),
                mount_point: "/mnt/nfs1".into(),
                fs_source: "srv:/nfs1".to_string(),
                fs_type: "nfs".to_string(),
                mount_options: Vec::new(),
                interfaces: Vec::new(),
                active: true,
            }],
            logical_roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            status: SourceStatusSnapshot::default(),
            source_primary_by_group: BTreeMap::new(),
            last_force_find_runner_by_group: BTreeMap::new(),
            force_find_inflight_groups: Vec::new(),
            scheduled_source_groups_by_node: BTreeMap::from([("node-b".to_string(), Vec::new())]),
            scheduled_scan_groups_by_node: BTreeMap::from([("node-b".to_string(), Vec::new())]),
            last_control_frame_signals_by_node: BTreeMap::new(),
            published_batches_by_node: BTreeMap::new(),
            published_events_by_node: BTreeMap::new(),
            published_control_events_by_node: BTreeMap::new(),
            published_data_events_by_node: BTreeMap::new(),
            last_published_at_us_by_node: BTreeMap::new(),
            last_published_origins_by_node: BTreeMap::new(),
            published_origin_counts_by_node: BTreeMap::new(),
            published_path_capture_target: None,
            enqueued_path_origin_counts_by_node: BTreeMap::new(),
            pending_path_origin_counts_by_node: BTreeMap::new(),
            yielded_path_origin_counts_by_node: BTreeMap::new(),
            summarized_path_origin_counts_by_node: BTreeMap::new(),
            published_path_origin_counts_by_node: BTreeMap::new(),
        };

        let merged = merge_source_observability(local, aggregated);

        assert_eq!(
            merged.scheduled_source_groups_by_node.get("node-b"),
            Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
        );
        assert_eq!(
            merged.scheduled_scan_groups_by_node.get("node-b"),
            Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
        );
    }

    #[test]
    fn merge_source_observability_snapshots_preserves_last_control_signals_by_node() {
        let node_a = SourceObservabilitySnapshot {
            lifecycle_state: "ready".to_string(),
            host_object_grants_version: 1,
            grants: Vec::new(),
            logical_roots: Vec::new(),
            status: SourceStatusSnapshot::default(),
            source_primary_by_group: BTreeMap::new(),
            last_force_find_runner_by_group: BTreeMap::new(),
            force_find_inflight_groups: Vec::new(),
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            last_control_frame_signals_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["activate nfs1".to_string()],
            )]),
            published_batches_by_node: BTreeMap::from([("node-a".to_string(), 11)]),
            published_events_by_node: BTreeMap::from([("node-a".to_string(), 111)]),
            published_control_events_by_node: BTreeMap::from([("node-a".to_string(), 2)]),
            published_data_events_by_node: BTreeMap::from([("node-a".to_string(), 109)]),
            last_published_at_us_by_node: BTreeMap::from([("node-a".to_string(), 123)]),
            last_published_origins_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=2".to_string()],
            )]),
            published_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=111".to_string()],
            )]),
            published_path_capture_target: None,
            enqueued_path_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=11".to_string()],
            )]),
            pending_path_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=2".to_string()],
            )]),
            yielded_path_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=9".to_string()],
            )]),
            summarized_path_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=7".to_string()],
            )]),
            published_path_origin_counts_by_node: BTreeMap::new(),
        };
        let node_b = SourceObservabilitySnapshot {
            lifecycle_state: "ready".to_string(),
            host_object_grants_version: 1,
            grants: Vec::new(),
            logical_roots: Vec::new(),
            status: SourceStatusSnapshot::default(),
            source_primary_by_group: BTreeMap::new(),
            last_force_find_runner_by_group: BTreeMap::new(),
            force_find_inflight_groups: Vec::new(),
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs2".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs2".to_string()],
            )]),
            last_control_frame_signals_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["activate nfs2".to_string()],
            )]),
            published_batches_by_node: BTreeMap::from([("node-b".to_string(), 22)]),
            published_events_by_node: BTreeMap::from([("node-b".to_string(), 222)]),
            published_control_events_by_node: BTreeMap::from([("node-b".to_string(), 3)]),
            published_data_events_by_node: BTreeMap::from([("node-b".to_string(), 219)]),
            last_published_at_us_by_node: BTreeMap::from([("node-b".to_string(), 456)]),
            last_published_origins_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=1".to_string()],
            )]),
            published_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=222".to_string()],
            )]),
            published_path_capture_target: None,
            enqueued_path_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=12".to_string()],
            )]),
            pending_path_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=2".to_string()],
            )]),
            yielded_path_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=10".to_string()],
            )]),
            summarized_path_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=8".to_string()],
            )]),
            published_path_origin_counts_by_node: BTreeMap::new(),
        };

        let merged = merge_source_observability_snapshots(vec![node_a, node_b]);

        assert_eq!(
            merged.last_control_frame_signals_by_node.get("node-a"),
            Some(&vec!["activate nfs1".to_string()])
        );
        assert_eq!(
            merged.last_control_frame_signals_by_node.get("node-b"),
            Some(&vec!["activate nfs2".to_string()])
        );
        assert_eq!(merged.published_batches_by_node.get("node-a"), Some(&11));
        assert_eq!(merged.published_batches_by_node.get("node-b"), Some(&22));
        assert_eq!(merged.published_events_by_node.get("node-a"), Some(&111));
        assert_eq!(merged.published_events_by_node.get("node-b"), Some(&222));
        assert_eq!(
            merged.last_published_origins_by_node.get("node-a"),
            Some(&vec!["node-a::nfs1=2".to_string()])
        );
        assert_eq!(
            merged.last_published_origins_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=1".to_string()])
        );
        assert_eq!(
            merged.published_origin_counts_by_node.get("node-a"),
            Some(&vec!["node-a::nfs1=111".to_string()])
        );
        assert_eq!(
            merged.published_origin_counts_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=222".to_string()])
        );
        assert_eq!(
            merged.enqueued_path_origin_counts_by_node.get("node-a"),
            Some(&vec!["node-a::nfs1=11".to_string()])
        );
        assert_eq!(
            merged.enqueued_path_origin_counts_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=12".to_string()])
        );
        assert_eq!(
            merged.pending_path_origin_counts_by_node.get("node-a"),
            Some(&vec!["node-a::nfs1=2".to_string()])
        );
        assert_eq!(
            merged.pending_path_origin_counts_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=2".to_string()])
        );
        assert_eq!(
            merged.yielded_path_origin_counts_by_node.get("node-a"),
            Some(&vec!["node-a::nfs1=9".to_string()])
        );
        assert_eq!(
            merged.yielded_path_origin_counts_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=10".to_string()])
        );
        assert_eq!(
            merged.summarized_path_origin_counts_by_node.get("node-a"),
            Some(&vec!["node-a::nfs1=7".to_string()])
        );
        assert_eq!(
            merged.summarized_path_origin_counts_by_node.get("node-b"),
            Some(&vec!["node-b::nfs2=8".to_string()])
        );
    }

    #[tokio::test]
    async fn route_source_observability_snapshot_waits_for_staggered_peer_replies_within_status_grace()
     {
        let source_status_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route");
        let boundary = Arc::new(DelayedSourceStatusCollectBoundary {
            reply_channel: format!("{}:reply", source_status_route.0),
            sent_correlation: StdMutex::new(None),
            recv_count: std::sync::atomic::AtomicUsize::new(0),
            fast_snapshot: SourceObservabilitySnapshot {
                scheduled_source_groups_by_node: BTreeMap::from([(
                    "node-d".to_string(),
                    vec!["nfs2".to_string()],
                )]),
                scheduled_scan_groups_by_node: BTreeMap::from([(
                    "node-d".to_string(),
                    vec!["nfs2".to_string()],
                )]),
                ..local_source_snapshot()
            },
            delayed_snapshot: SourceObservabilitySnapshot {
                scheduled_source_groups_by_node: BTreeMap::from([(
                    "node-b".to_string(),
                    vec!["nfs1".to_string(), "nfs2".to_string()],
                )]),
                scheduled_scan_groups_by_node: BTreeMap::from([(
                    "node-b".to_string(),
                    vec!["nfs1".to_string(), "nfs2".to_string()],
                )]),
                ..local_source_snapshot()
            },
        });

        let (snapshot, _runner_sets) = route_source_observability_snapshot(
            boundary,
            NodeId("node-a".into()),
            Duration::from_secs(5),
            STATUS_ROUTE_COLLECT_IDLE_GRACE,
        )
        .await
        .expect("collect source observability");

        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-d"),
            Some(&vec!["nfs2".to_string()])
        );
        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-b"),
            Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
        );
        assert_eq!(
            snapshot.scheduled_scan_groups_by_node.get("node-b"),
            Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
        );
    }

    #[tokio::test]
    async fn route_source_observability_snapshot_routes_query_peer_owned_peer_schedules_after_turnover()
     {
        let source_status_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route");
        let boundary = Arc::new(RoutedSourceStatusCollectBoundary {
            request_channel: source_status_route.0.clone(),
            reply_channel: format!("{}:reply", source_status_route.0),
            replies: tokio::sync::Mutex::new(Vec::new()),
            sent_unit_ids: StdMutex::new(Vec::new()),
            recv_unit_ids: StdMutex::new(Vec::new()),
        });

        let (snapshot, _runner_sets) = route_source_observability_snapshot(
            boundary.clone(),
            NodeId("node-a".into()),
            Duration::from_secs(5),
            STATUS_ROUTE_COLLECT_IDLE_GRACE,
        )
        .await
        .expect("collect source observability");

        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-b"),
            Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
            "query-peer-owned peer schedules must survive multi-peer source-status collection after turnover"
        );
        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-c"),
            Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
        );
        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-d"),
            Some(&vec!["nfs2".to_string()])
        );

        let sent_unit_ids = boundary
            .sent_unit_ids
            .lock()
            .expect("unit scoped source-status sent unit ids lock")
            .clone();
        assert!(
            sent_unit_ids.iter().any(|unit_id| unit_id.is_none()),
            "source-status collection must route query-peer-owned peer requests through the default bound-route path, not direct unit-scoped boundary sends: {sent_unit_ids:?}"
        );
        let recv_unit_ids = boundary
            .recv_unit_ids
            .lock()
            .expect("routed source-status recv unit ids lock")
            .clone();
        assert!(
            recv_unit_ids.iter().any(|unit_id| unit_id.is_none()),
            "routed source-status collection replies must arrive on the default bound-route return lane: {recv_unit_ids:?}"
        );
    }

    #[tokio::test]
    async fn route_source_observability_snapshot_retries_transient_internal_collect_gap_before_fallback()
     {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route");
        let node_b_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..local_source_snapshot()
        })
        .expect("encode node-b source status");
        let node_c_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-c".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-c".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            ..local_source_snapshot()
        })
        .expect("encode node-c source status");
        let node_d_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-d".to_string(),
                vec!["nfs2".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-d".to_string(),
                vec!["nfs2".to_string()],
            )]),
            ..local_source_snapshot()
        })
        .expect("encode node-d source status");
        let boundary = Arc::new(SourceStatusInternalRetryThenReplyBoundary {
            source_reply_channel: format!("{}:reply", source_route.0.clone()),
            source_status_payloads: vec![node_b_payload, node_c_payload, node_d_payload],
            send_batches_by_channel: StdMutex::new(std::collections::HashMap::new()),
            recv_batches_by_channel: StdMutex::new(std::collections::HashMap::new()),
            correlations_by_channel: StdMutex::new(std::collections::HashMap::new()),
            first_source_recv_failed: std::sync::atomic::AtomicBool::new(false),
        });

        let (snapshot, _runner_sets) = route_source_observability_snapshot(
            boundary.clone(),
            NodeId("node-a".into()),
            Duration::from_secs(5),
            STATUS_ROUTE_COLLECT_IDLE_GRACE,
        )
        .await
        .expect("source-status collection should retry transient internal collect gap");

        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-b"),
            Some(&vec!["nfs1".to_string()])
        );
        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-c"),
            Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
        );
        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-d"),
            Some(&vec!["nfs2".to_string()])
        );
        let send_batches = boundary
            .send_batches_by_channel
            .lock()
            .expect("source internal retry boundary send batches lock");
        assert_eq!(
            send_batches
                .get(&source_route.0)
                .copied()
                .unwrap_or_default(),
            2,
            "transient internal collect gap must trigger a second routed source-status issuance before falling back"
        );
    }

    #[tokio::test]
    async fn route_source_observability_snapshot_retries_peer_transport_close_before_fallback() {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route");
        let node_b_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..local_source_snapshot()
        })
        .expect("encode node-b source status");
        let node_c_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-c".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-c".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            ..local_source_snapshot()
        })
        .expect("encode node-c source status");
        let node_d_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-d".to_string(),
                vec!["nfs2".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-d".to_string(),
                vec!["nfs2".to_string()],
            )]),
            ..local_source_snapshot()
        })
        .expect("encode node-d source status");
        let boundary = Arc::new(SourceStatusPeerTransportRetryThenReplyBoundary {
            source_reply_channel: format!("{}:reply", source_route.0.clone()),
            source_status_payloads: vec![node_b_payload, node_c_payload, node_d_payload],
            send_batches_by_channel: StdMutex::new(std::collections::HashMap::new()),
            recv_batches_by_channel: StdMutex::new(std::collections::HashMap::new()),
            correlations_by_channel: StdMutex::new(std::collections::HashMap::new()),
            first_source_recv_failed: std::sync::atomic::AtomicBool::new(false),
        });

        let (snapshot, _runner_sets) = route_source_observability_snapshot(
            boundary.clone(),
            NodeId("node-a".into()),
            Duration::from_secs(5),
            STATUS_ROUTE_COLLECT_IDLE_GRACE,
        )
        .await
        .expect("source-status collection should retry peer transport close");

        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-b"),
            Some(&vec!["nfs1".to_string()])
        );
        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-c"),
            Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
        );
        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-d"),
            Some(&vec!["nfs2".to_string()])
        );
        let send_batches = boundary
            .send_batches_by_channel
            .lock()
            .expect("source peer transport retry boundary send batches lock");
        assert_eq!(
            send_batches
                .get(&source_route.0)
                .copied()
                .unwrap_or_default(),
            2,
            "peer transport close must trigger a second routed source-status issuance before falling back"
        );
    }

    #[tokio::test]
    async fn route_source_observability_snapshot_retries_incomplete_active_source_debug_before_accepting_snapshot()
     {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route");
        let incomplete_node_a = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
            scheduled_source_groups_by_node: BTreeMap::new(),
            scheduled_scan_groups_by_node: BTreeMap::new(),
            last_control_frame_signals_by_node: BTreeMap::new(),
            published_batches_by_node: BTreeMap::new(),
            published_events_by_node: BTreeMap::new(),
            published_control_events_by_node: BTreeMap::new(),
            published_data_events_by_node: BTreeMap::new(),
            last_published_at_us_by_node: BTreeMap::new(),
            last_published_origins_by_node: BTreeMap::new(),
            published_origin_counts_by_node: BTreeMap::new(),
            enqueued_path_origin_counts_by_node: BTreeMap::new(),
            pending_path_origin_counts_by_node: BTreeMap::new(),
            yielded_path_origin_counts_by_node: BTreeMap::new(),
            summarized_path_origin_counts_by_node: BTreeMap::new(),
            published_path_origin_counts_by_node: BTreeMap::new(),
            ..local_source_snapshot()
        })
        .expect("encode incomplete node-a source status");
        let complete_node_a = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            last_control_frame_signals_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["activate unit=runtime.exec.source route=source-logical-roots-control:v1.stream generation=11 scopes=[\"nfs1=>node-a::nfs1\"]".to_string()],
            )]),
            published_batches_by_node: BTreeMap::from([("node-a".to_string(), 7)]),
            published_events_by_node: BTreeMap::from([("node-a".to_string(), 321)]),
            published_control_events_by_node: BTreeMap::from([("node-a".to_string(), 3)]),
            published_data_events_by_node: BTreeMap::from([("node-a".to_string(), 318)]),
            last_published_at_us_by_node: BTreeMap::from([("node-a".to_string(), 123456)]),
            last_published_origins_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=7".to_string()],
            )]),
            published_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=321".to_string()],
            )]),
            ..local_source_snapshot()
        })
        .expect("encode complete node-a source status");
        let node_b_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
            scheduled_source_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string()],
            )]),
            scheduled_scan_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..local_source_snapshot()
        })
        .expect("encode node-b source status");
        let boundary = Arc::new(SourceStatusIncompleteRetryThenReplyBoundary {
            source_reply_channel: format!("{}:reply", source_route.0.clone()),
            first_source_status_payloads: vec![incomplete_node_a.clone(), node_b_payload.clone()],
            second_source_status_payloads: vec![complete_node_a, node_b_payload],
            send_batches_by_channel: StdMutex::new(std::collections::HashMap::new()),
            recv_batches_by_channel: StdMutex::new(std::collections::HashMap::new()),
            correlations_by_channel: StdMutex::new(std::collections::HashMap::new()),
            recv_state: StdMutex::new((0, 0)),
        });

        let (snapshot, _runner_sets) = route_source_observability_snapshot(
            boundary.clone(),
            NodeId("node-a".into()),
            Duration::from_secs(5),
            STATUS_ROUTE_COLLECT_IDLE_GRACE,
        )
        .await
        .expect("source-status collection should retry incomplete active source debug");

        assert_eq!(
            snapshot.scheduled_source_groups_by_node.get("node-a"),
            Some(&vec!["nfs1".to_string()])
        );
        assert_eq!(
            snapshot.scheduled_scan_groups_by_node.get("node-a"),
            Some(&vec!["nfs1".to_string()])
        );
        assert_eq!(
            snapshot.published_batches_by_node.get("node-a"),
            Some(&7)
        );
        assert!(
            snapshot
                .last_control_frame_signals_by_node
                .get("node-a")
                .is_some_and(|signals| !signals.is_empty())
        );
        let send_batches = boundary
            .send_batches_by_channel
            .lock()
            .expect("source incomplete retry boundary send batches lock");
        assert_eq!(
            send_batches
                .get(&source_route.0)
                .copied()
                .unwrap_or_default(),
            2,
            "incomplete active source debug must trigger a second routed source-status issuance before accepting snapshot"
        );
    }

    #[tokio::test]
    async fn status_reports_serving_facade_state_without_pending_diagnostics() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let source_cfg = SourceConfig {
            roots: vec![RootSpec::new("nfs1", &nfs1)],
            host_object_grants: vec![granted_mount_root("node-a::nfs1", &nfs1)],
            ..SourceConfig::default()
        };
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::new(source_cfg.clone(), NodeId("node-a".into())).expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg).expect("sink"),
        )));
        let headers = management_headers(auth.as_ref());
        let facade_service_state = shared_facade_service_state_cell();
        *facade_service_state
            .write()
            .expect("write facade service state") = FacadeServiceState::Serving;
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: None,
            query_runtime_boundary: None,
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source,
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                facade_service_state,
                shared_facade_pending_status_cell(),
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let response = status(State(state), headers)
            .await
            .expect("status should report serving facade state")
            .0;

        assert_eq!(response.facade.state, "serving");
        assert!(
            response.facade.pending.is_none(),
            "serving facade state must not synthesize pending diagnostics"
        );
    }

    #[test]
    fn status_sink_live_nodes_does_not_inflate_empty_sink_from_active_source_grants() {
        let sink_status = SinkStatusSnapshot::default();
        assert_eq!(
            status_sink_live_nodes(1, &sink_status),
            0,
            "status must not inflate sink.live_nodes from source active grants when sink has no groups"
        );
    }

    #[tokio::test]
    async fn status_reports_unavailable_facade_state_without_pending_diagnostics() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let source_cfg = SourceConfig {
            roots: vec![RootSpec::new("nfs1", &nfs1)],
            host_object_grants: vec![granted_mount_root("node-a::nfs1", &nfs1)],
            ..SourceConfig::default()
        };
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::new(source_cfg.clone(), NodeId("node-a".into())).expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg).expect("sink"),
        )));
        let headers = management_headers(auth.as_ref());
        let facade_service_state = shared_facade_service_state_cell();
        *facade_service_state
            .write()
            .expect("write facade service state") =
            crate::domain_state::FacadeServiceState::Unavailable;
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: None,
            query_runtime_boundary: None,
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source,
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                facade_service_state,
                shared_facade_pending_status_cell(),
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let response = status(State(state), headers)
            .await
            .expect("direct status handler should surface unavailable facade state")
            .0;

        assert_eq!(response.facade.state, "unavailable");
        assert!(response.facade.pending.is_none());
    }

    #[tokio::test]
    async fn status_reports_pending_facade_state_with_pending_diagnostics() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let source_cfg = SourceConfig {
            roots: vec![RootSpec::new("nfs1", &nfs1)],
            host_object_grants: vec![granted_mount_root("node-a::nfs1", &nfs1)],
            ..SourceConfig::default()
        };
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::new(source_cfg.clone(), NodeId("node-a".into())).expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg).expect("sink"),
        )));
        let headers = management_headers(auth.as_ref());
        let facade_pending = shared_facade_pending_status_cell();
        let facade_service_state = shared_facade_service_state_cell();
        *facade_service_state
            .write()
            .expect("write facade service state") =
            crate::domain_state::FacadeServiceState::Pending;
        *facade_pending.write().expect("pending facade status lock") =
            Some(SharedFacadePendingStatus {
                route_key: "fs-meta.internal.facade-control:v1.stream".to_string(),
                generation: 2,
                resource_ids: vec!["listener-b".to_string()],
                runtime_managed: true,
                runtime_exposure_confirmed: true,
                reason: FacadePendingReason::RetryingAfterError,
                retry_attempts: 3,
                pending_since_us: 44,
                last_error: Some("bind failed".to_string()),
                last_attempt_at_us: Some(55),
                last_error_at_us: Some(56),
                retry_backoff_ms: Some(57),
                next_retry_at_us: Some(58),
            });
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: None,
            query_runtime_boundary: None,
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source,
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                facade_service_state,
                facade_pending,
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let response = status(State(state), headers)
            .await
            .expect("status should report pending facade state")
            .0;

        assert_eq!(response.facade.state, "pending");
        let pending = response.facade.pending.expect("pending diagnostics");
        assert_eq!(pending.generation, 2);
        assert_eq!(pending.reason, "retrying_after_error");
        assert_eq!(pending.retry_attempts, 3);
        assert_eq!(pending.last_error.as_deref(), Some("bind failed"));
    }

    #[tokio::test]
    async fn status_fails_closed_when_both_remote_status_routes_are_internal() {
        tokio::time::pause();

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let source_cfg = SourceConfig {
            roots: vec![RootSpec::new("nfs1", &nfs1)],
            host_object_grants: vec![granted_mount_root("node-a::nfs1", &nfs1)],
            ..SourceConfig::default()
        };
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::new(source_cfg.clone(), NodeId("node-a".into())).expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg).expect("sink"),
        )));
        let headers = management_headers(auth.as_ref());
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: Some(Arc::new(StatusInternalRouteBoundary)),
            query_runtime_boundary: Some(Arc::new(StatusInternalRouteBoundary)),
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source,
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                shared_facade_service_state_cell(),
                shared_facade_pending_status_cell(),
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let status_task = tokio::spawn(async move { status(State(state), headers).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(12)).await;

        let err = status_task
            .await
            .expect("status join")
            .expect_err("status must fail closed when both remote status routes are internal");
        assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            err.message.contains("source_route=") && err.message.contains("sink_route="),
            "status fail-closed message must preserve remote route outcomes: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn status_prefers_runtime_boundary_when_query_runtime_boundary_transports() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let source_cfg = SourceConfig {
            roots: vec![RootSpec::new("nfs1", &nfs1)],
            host_object_grants: vec![granted_mount_root("node-a::nfs1", &nfs1)],
            ..SourceConfig::default()
        };
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::new(source_cfg.clone(), NodeId("node-a".into())).expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg).expect("sink"),
        )));
        let headers = management_headers(auth.as_ref());
        let runtime_boundary = Arc::new(StatusRemoteReplyBoundary::new(
            vec![(
                "node-b-29776275144172679041384449".to_string(),
                SourceObservabilitySnapshot {
                    scheduled_source_groups_by_node: BTreeMap::from([(
                        "node-b".to_string(),
                        vec!["nfs1".to_string()],
                    )]),
                    scheduled_scan_groups_by_node: BTreeMap::from([(
                        "node-b".to_string(),
                        vec!["nfs1".to_string()],
                    )]),
                    ..local_source_snapshot()
                },
            )],
            vec![(
                "node-b-29776275144172679041384449".to_string(),
                SinkStatusSnapshot {
                    live_nodes: 2,
                    scheduled_groups_by_node: BTreeMap::from([(
                        "node-b".to_string(),
                        vec!["nfs1".to_string()],
                    )]),
                    groups: vec![crate::sink::SinkGroupStatusSnapshot {
                        group_id: "nfs1".to_string(),
                        primary_object_ref: "obj-b".to_string(),
                        total_nodes: 2,
                        live_nodes: 2,
                        tombstoned_count: 0,
                        attested_count: 0,
                        suspect_count: 0,
                        blind_spot_count: 0,
                        shadow_time_us: 21,
                        shadow_lag_us: 22,
                        overflow_pending_audit: false,
                        initial_audit_completed: true,
                readiness: crate::sink::GroupReadinessState::Ready,
                        materialized_revision: 1,
                        estimated_heap_bytes: 0,
                    }],
                    ..SinkStatusSnapshot::default()
                },
            )],
        ));
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: Some(runtime_boundary),
            query_runtime_boundary: Some(Arc::new(StatusTransportRouteBoundary)),
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source,
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                shared_facade_service_state_cell(),
                shared_facade_pending_status_cell(),
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let response = status(State(state), headers)
            .await
            .expect("status should use runtime boundary when query boundary transport-fails")
            .0;

        assert_eq!(
            response
                .source
                .debug
                .scheduled_source_groups_by_node
                .get("node-b"),
            Some(&vec!["nfs1".to_string()])
        );
        assert_eq!(
            response.sink.debug.scheduled_groups_by_node.get("node-b"),
            Some(&vec!["nfs1".to_string()])
        );
    }

    #[tokio::test]
    async fn status_falls_back_to_query_boundary_when_runtime_boundary_transports() {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let source_cfg = SourceConfig {
            roots: vec![RootSpec::new("nfs1", &nfs1)],
            host_object_grants: vec![granted_mount_root("node-a::nfs1", &nfs1)],
            ..SourceConfig::default()
        };
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::new(source_cfg.clone(), NodeId("node-a".into())).expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg).expect("sink"),
        )));
        let headers = management_headers(auth.as_ref());
        let query_boundary = Arc::new(StatusRemoteReplyBoundary::new(
            vec![(
                "node-b-29776275144172679041384449".to_string(),
                SourceObservabilitySnapshot {
                    scheduled_source_groups_by_node: BTreeMap::from([(
                        "node-b".to_string(),
                        vec!["nfs1".to_string()],
                    )]),
                    scheduled_scan_groups_by_node: BTreeMap::from([(
                        "node-b".to_string(),
                        vec!["nfs1".to_string()],
                    )]),
                    ..local_source_snapshot()
                },
            )],
            vec![(
                "node-b-29776275144172679041384449".to_string(),
                SinkStatusSnapshot {
                    live_nodes: 2,
                    scheduled_groups_by_node: BTreeMap::from([(
                        "node-b".to_string(),
                        vec!["nfs1".to_string()],
                    )]),
                    groups: vec![crate::sink::SinkGroupStatusSnapshot {
                        group_id: "nfs1".to_string(),
                        primary_object_ref: "obj-b".to_string(),
                        total_nodes: 2,
                        live_nodes: 2,
                        tombstoned_count: 0,
                        attested_count: 0,
                        suspect_count: 0,
                        blind_spot_count: 0,
                        shadow_time_us: 21,
                        shadow_lag_us: 22,
                        overflow_pending_audit: false,
                        initial_audit_completed: true,
                readiness: crate::sink::GroupReadinessState::Ready,
                        materialized_revision: 1,
                        estimated_heap_bytes: 0,
                    }],
                    ..SinkStatusSnapshot::default()
                },
            )],
        ));
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: Some(Arc::new(StatusTransportRouteBoundary)),
            query_runtime_boundary: Some(query_boundary),
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source,
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                shared_facade_service_state_cell(),
                shared_facade_pending_status_cell(),
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let response = status(State(state), headers)
            .await
            .expect(
                "status should fall back to live query boundary when runtime boundary transports",
            )
            .0;

        assert_eq!(
            response
                .source
                .debug
                .scheduled_source_groups_by_node
                .get("node-b"),
            Some(&vec!["nfs1".to_string()])
        );
        assert_eq!(
            response.sink.debug.scheduled_groups_by_node.get("node-b"),
            Some(&vec!["nfs1".to_string()])
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_serializes_same_boundary_remote_source_and_sink_collects_when_overlap_would_transport_poison()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let source_cfg = SourceConfig {
            roots: vec![RootSpec::new("nfs1", &nfs1)],
            host_object_grants: vec![granted_mount_root("node-a::nfs1", &nfs1)],
            ..SourceConfig::default()
        };
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::new(source_cfg.clone(), NodeId("node-a".into())).expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg).expect("sink"),
        )));
        let headers = management_headers(auth.as_ref());
        let runtime_boundary = Arc::new(StatusOverlapPoisonBoundary::new(
            vec![(
                "node-b-29776275144172679041384449".to_string(),
                SourceObservabilitySnapshot {
                    scheduled_source_groups_by_node: BTreeMap::from([(
                        "node-b".to_string(),
                        vec!["nfs1".to_string()],
                    )]),
                    scheduled_scan_groups_by_node: BTreeMap::from([(
                        "node-b".to_string(),
                        vec!["nfs1".to_string()],
                    )]),
                    ..local_source_snapshot()
                },
            )],
            vec![(
                "node-b-29776275144172679041384449".to_string(),
                SinkStatusSnapshot {
                    live_nodes: 2,
                    scheduled_groups_by_node: BTreeMap::from([(
                        "node-b".to_string(),
                        vec!["nfs1".to_string()],
                    )]),
                    groups: vec![crate::sink::SinkGroupStatusSnapshot {
                        group_id: "nfs1".to_string(),
                        primary_object_ref: "obj-b".to_string(),
                        total_nodes: 2,
                        live_nodes: 2,
                        tombstoned_count: 0,
                        attested_count: 0,
                        suspect_count: 0,
                        blind_spot_count: 0,
                        shadow_time_us: 21,
                        shadow_lag_us: 22,
                        overflow_pending_audit: false,
                        initial_audit_completed: true,
                readiness: crate::sink::GroupReadinessState::Ready,
                        materialized_revision: 1,
                        estimated_heap_bytes: 0,
                    }],
                    ..SinkStatusSnapshot::default()
                },
            )],
        ));
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: Some(runtime_boundary.clone()),
            query_runtime_boundary: None,
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source,
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                shared_facade_service_state_cell(),
                shared_facade_pending_status_cell(),
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let response = status(State(state), headers)
            .await
            .expect("status should avoid same-boundary source/sink collect overlap that would self-poison transport")
            .0;

        assert_eq!(
            response
                .source
                .debug
                .scheduled_source_groups_by_node
                .get("node-b"),
            Some(&vec!["nfs1".to_string()])
        );
        assert_eq!(
            response.sink.debug.scheduled_groups_by_node.get("node-b"),
            Some(&vec!["nfs1".to_string()])
        );
        assert!(
            !runtime_boundary
                .poisoned
                .load(std::sync::atomic::Ordering::SeqCst),
            "status should not overlap source/sink remote collection on the same boundary"
        );
    }

    #[tokio::test]
    async fn status_traces_single_request_remote_collect_issuance_and_route_outcomes() {
        tokio::time::pause();

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let source_cfg = SourceConfig {
            roots: vec![RootSpec::new("nfs1", &nfs1)],
            host_object_grants: vec![granted_mount_root("node-a::nfs1", &nfs1)],
            ..SourceConfig::default()
        };
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::new(source_cfg.clone(), NodeId("node-a".into())).expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg).expect("sink"),
        )));
        let headers = management_headers(auth.as_ref());
        let trace_events = Arc::new(StdMutex::new(Vec::new()));
        install_status_route_trace_hook(StatusRouteTraceHook {
            events: trace_events.clone(),
        });
        struct StatusRouteTraceHookReset;
        impl Drop for StatusRouteTraceHookReset {
            fn drop(&mut self) {
                clear_status_route_trace_hook();
            }
        }
        let _reset = StatusRouteTraceHookReset;
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: Some(Arc::new(StatusTransportRouteBoundary)),
            query_runtime_boundary: None,
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source,
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                shared_facade_service_state_cell(),
                shared_facade_pending_status_cell(),
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let status_task = tokio::spawn(async move { status(State(state), headers).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(25)).await;

        let err = status_task
            .await
            .expect("status join")
            .expect_err("transport-only remote status routes should fail closed");
        assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE);

        let events = trace_events
            .lock()
            .expect("status route trace lock")
            .clone();
        assert!(
            !events.is_empty(),
            "status route trace must record one request lifecycle"
        );
        let request_id = events[0]
            .split(':')
            .next()
            .expect("request id prefix")
            .to_string();
        let request_events = events
            .into_iter()
            .filter_map(|event| {
                let (id, stage) = event.split_once(':')?;
                (id == request_id).then_some(stage.to_string())
            })
            .collect::<Vec<_>>();

        assert_eq!(
            request_events,
            vec![
                "status.enter".to_string(),
                "status.remote.begin".to_string(),
                "sink.collect.begin".to_string(),
                "source.collect.begin".to_string(),
                "sink.collect.outcome=timeout".to_string(),
                "source.collect.outcome=timeout".to_string(),
                "status.remote.done sink=timeout source=timeout".to_string(),
            ],
            "one /status request must remain traceable through remote source/sink route classification"
        );
    }

    #[tokio::test]
    async fn status_returns_local_status_when_published_facade_is_serving_and_both_remote_status_routes_timeout()
     {
        tokio::time::pause();

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let source_cfg = SourceConfig {
            roots: vec![RootSpec::new("nfs1", &nfs1)],
            host_object_grants: vec![granted_mount_root("node-a::nfs1", &nfs1)],
            ..SourceConfig::default()
        };
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::new(source_cfg.clone(), NodeId("node-a".into())).expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg).expect("sink"),
        )));
        let headers = management_headers(auth.as_ref());
        let trace_events = Arc::new(StdMutex::new(Vec::new()));
        install_status_route_trace_hook(StatusRouteTraceHook {
            events: trace_events.clone(),
        });
        struct StatusRouteTraceHookReset;
        impl Drop for StatusRouteTraceHookReset {
            fn drop(&mut self) {
                clear_status_route_trace_hook();
            }
        }
        let _reset = StatusRouteTraceHookReset;
        let facade_service_state = shared_facade_service_state_cell();
        *facade_service_state
            .write()
            .expect("write facade service state") = FacadeServiceState::Serving;
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: Some(Arc::new(StatusTransportRouteBoundary)),
            query_runtime_boundary: None,
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source,
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                facade_service_state,
                shared_facade_pending_status_cell(),
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let status_task = tokio::spawn(async move { status(State(state), headers).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(25)).await;

        let response = status_task
            .await
            .expect("status join")
            .expect("serving facade must keep /status available across remote route timeouts")
            .0;
        assert_eq!(response.facade.state, "serving");
        assert!(response.source.grants_count >= 1);
        assert_eq!(
            response.sink.live_nodes, 0,
            "when both remote status routes time out, /status must return the local sink truth instead of inflating sink.live_nodes from active source grants"
        );
        assert_eq!(
            response.sink.groups.len(),
            1,
            "local zero-state sink group should still be surfaced as the local sink truth"
        );

        let events = trace_events
            .lock()
            .expect("status route trace lock")
            .clone();
        assert!(
            !events.is_empty(),
            "status route trace must record one request lifecycle"
        );
        let request_id = events[0]
            .split(':')
            .next()
            .expect("request id prefix")
            .to_string();
        let request_events = events
            .into_iter()
            .filter_map(|event| {
                let (id, stage) = event.split_once(':')?;
                (id == request_id).then_some(stage.to_string())
            })
            .collect::<Vec<_>>();

        assert_eq!(
            request_events,
            vec![
                "status.enter".to_string(),
                "status.remote.begin".to_string(),
                "sink.collect.begin".to_string(),
                "source.collect.begin".to_string(),
                "sink.collect.outcome=timeout".to_string(),
                "source.collect.outcome=timeout".to_string(),
                "status.remote.done sink=timeout source=timeout".to_string(),
            ],
            "serving /status should preserve remote route traceability even when it falls back to local snapshots"
        );
    }

    #[tokio::test]
    async fn status_returns_local_status_when_published_facade_is_pending_and_both_remote_status_routes_timeout()
     {
        tokio::time::pause();

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let source_cfg = SourceConfig {
            roots: vec![RootSpec::new("nfs1", &nfs1)],
            host_object_grants: vec![granted_mount_root("node-a::nfs1", &nfs1)],
            ..SourceConfig::default()
        };
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::new(source_cfg.clone(), NodeId("node-a".into())).expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg).expect("sink"),
        )));
        let headers = management_headers(auth.as_ref());
        let facade_service_state = shared_facade_service_state_cell();
        let facade_pending_status = shared_facade_pending_status_cell();
        *facade_service_state
            .write()
            .expect("write facade service state") = FacadeServiceState::Pending;
        *facade_pending_status
            .write()
            .expect("write facade pending status") = Some(SharedFacadePendingStatus {
            route_key: "route".to_string(),
            generation: 2,
            resource_ids: vec!["listener-a".to_string()],
            runtime_managed: true,
            runtime_exposure_confirmed: true,
            reason: FacadePendingReason::AwaitingObservationEligibility,
            retry_attempts: 0,
            pending_since_us: 11,
            last_error: None,
            last_attempt_at_us: None,
            last_error_at_us: None,
            retry_backoff_ms: None,
            next_retry_at_us: None,
        });
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: Some(Arc::new(StatusTransportRouteBoundary)),
            query_runtime_boundary: None,
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source,
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                facade_service_state,
                facade_pending_status,
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let status_task = tokio::spawn(async move { status(State(state), headers).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(25)).await;

        let response = status_task
            .await
            .expect("status join")
            .expect("pending facade must keep /status available across remote route timeouts")
            .0;
        assert_eq!(response.facade.state, "pending");
        assert!(response.facade.pending.is_some());
        assert_eq!(response.sink.groups.len(), 1);
    }

    #[tokio::test]
    async fn status_pending_facade_settles_within_local_route_budget_when_remote_status_routes_never_reply()
     {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let source_cfg = SourceConfig {
            roots: vec![RootSpec::new("nfs1", &nfs1)],
            host_object_grants: vec![granted_mount_root("node-a::nfs1", &nfs1)],
            ..SourceConfig::default()
        };
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::new(source_cfg.clone(), NodeId("node-a".into())).expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("node-a".into()), None, source_cfg).expect("sink"),
        )));
        let headers = management_headers(auth.as_ref());
        let facade_service_state = shared_facade_service_state_cell();
        let facade_pending_status = shared_facade_pending_status_cell();
        *facade_service_state
            .write()
            .expect("write facade service state") = FacadeServiceState::Pending;
        *facade_pending_status
            .write()
            .expect("write facade pending status") = Some(SharedFacadePendingStatus {
            route_key: "route".to_string(),
            generation: 2,
            resource_ids: vec!["listener-a".to_string()],
            runtime_managed: true,
            runtime_exposure_confirmed: true,
            reason: FacadePendingReason::AwaitingObservationEligibility,
            retry_attempts: 0,
            pending_since_us: 11,
            last_error: None,
            last_attempt_at_us: None,
            last_error_at_us: None,
            retry_backoff_ms: None,
            next_retry_at_us: None,
        });
        let state = ApiState {
            node_id: NodeId("node-a".into()),
            runtime_boundary: Some(Arc::new(StatusSlowTimeoutRouteBoundary)),
            query_runtime_boundary: None,
            force_find_inflight: Arc::new(StdMutex::new(BTreeSet::new())),
            source,
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                facade_service_state,
                facade_pending_status,
            ),
            request_tracker: Arc::new(crate::api::ApiRequestTracker::default()),
        };

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            status(State(state), headers),
        )
        .await
        .expect("pending /status must settle within the local route budget")
        .expect("pending /status should fall back to local snapshots")
        .0;

        assert_eq!(response.facade.state, "pending");
        assert!(response.facade.pending.is_some());
    }
}
