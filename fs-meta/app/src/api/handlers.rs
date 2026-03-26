use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;
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

use crate::query::api::{
    internal_status_request_payload, merge_sink_status_snapshots,
    refresh_policy_from_host_object_grants, route_sink_status_snapshot,
};
use crate::runtime::orchestration::encode_logical_roots_control_payload;
use crate::runtime::orchestration::encode_manual_rescan_envelope;
use crate::runtime::routes::ROUTE_KEY_SOURCE_RESCAN_CONTROL;
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
use super::facade_status::SharedFacadePendingStatus;
use super::state::ApiState;
use super::types::{
    CreateQueryApiKeyRequest, CreateQueryApiKeyResponse, DegradedRoot, LoginRequest, LoginResponse,
    QueryApiKeysResponse, RescanResponse, RevokeQueryApiKeyResponse, RootEntry, RootPreviewItem,
    RootSelectorEntry, RootUpdateEntry, RootsPreviewResponse, RootsResponse, RootsUpdateRequest,
    RootsUpdateResponse, RuntimeGrantsResponse, StatusFacade, StatusFacadePending, StatusResponse,
    StatusSink, StatusSinkDebug, StatusSinkGroup, StatusSource, StatusSourceConcreteRoot,
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

fn summarize_groups_by_node(
    groups: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    groups
        .iter()
        .map(|(node_id, groups)| format!("{node_id}={}", groups.join("|")))
        .collect()
}

fn summarize_source_status_route_snapshot(snapshot: &SourceObservabilitySnapshot) -> String {
    format!(
        "primaries={} runners={} source={:?} scan={:?} control={:?} published_batches={:?} published_events={:?} published_origins={:?} published_origin_counts={:?}",
        snapshot.source_primary_by_group.len(),
        snapshot.last_force_find_runner_by_group.len(),
        summarize_groups_by_node(&snapshot.scheduled_source_groups_by_node),
        summarize_groups_by_node(&snapshot.scheduled_scan_groups_by_node),
        summarize_groups_by_node(&snapshot.last_control_frame_signals_by_node),
        snapshot.published_batches_by_node,
        snapshot.published_events_by_node,
        summarize_groups_by_node(&snapshot.last_published_origins_by_node),
        summarize_groups_by_node(&snapshot.published_origin_counts_by_node),
    )
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
    let (sink_status, source, runner_sets, sink_outcome, source_outcome) =
        merge_remote_status_snapshots(
            local_sink,
            local_source,
            state.query_runtime_boundary.clone(),
            state.node_id.clone(),
            |boundary, origin_id| async move {
                route_sink_status_snapshot(boundary, origin_id, STATUS_ROUTE_TIMEOUT).await
            },
            |boundary, origin_id| async move {
                route_source_observability_snapshot(
                    boundary,
                    origin_id,
                    STATUS_ROUTE_TIMEOUT,
                    STATUS_ROUTE_COLLECT_IDLE_GRACE,
                )
                .await
            },
        )
        .await;
    let live_source_nodes = source
        .grants
        .iter()
        .filter(|grant| grant.active)
        .map(|grant| grant.host_ip.clone())
        .collect::<BTreeSet<_>>()
        .len() as u64;
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

    Ok(Json(StatusResponse {
        source: status_source_from_observability(source, runner_sets, merged_inflight),
        sink: StatusSink {
            live_nodes: sink_status.live_nodes.max(live_source_nodes),
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
        facade: state
            .facade_pending
            .read()
            .ok()
            .and_then(|pending| pending.clone())
            .map(status_facade_from_pending),
    }))
}

const STATUS_ROUTE_TIMEOUT: Duration = Duration::from_secs(10);
const STATUS_ROUTE_COLLECT_IDLE_GRACE: Duration = Duration::from_millis(500);

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

    let (sink_result, source_result) = tokio::join!(
        sink_collect(boundary.clone(), node_id.clone()),
        source_collect(boundary, node_id),
    );
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

fn classify_status_route_error(err: &CnxError) -> StatusRouteOutcome {
    match err {
        CnxError::Timeout => StatusRouteOutcome::Timeout,
        CnxError::TransportClosed(_) => StatusRouteOutcome::Transport,
        CnxError::ProtocolViolation(_) => StatusRouteOutcome::Protocol,
        _ => StatusRouteOutcome::Internal,
    }
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
            boundary
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
                .map_err(|err| {
                    ApiError::internal(format!(
                        "logical roots control send failed route={route_key}: {err}"
                    ))
                })?;
            eprintln!(
                "fs_meta_api: roots_put control send ok route={} roots={}",
                route_key,
                roots.len()
            );
        }
    }
    refresh_policy_from_host_object_grants(&state.projection_policy, &grants);
    eprintln!("fs_meta_api: roots_put policy refresh ok");

    Ok(Json(RootsUpdateResponse {
        roots_count: state
            .source
            .logical_roots_snapshot()
            .await
            .map_err(|err| {
                ApiError::internal(format!("source logical roots snapshot failed: {err}"))
            })?
            .len(),
    }))
}

pub async fn rescan(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<RescanResponse>, ApiError> {
    let _ = authorize_management(&state, &headers)?;
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
        boundary
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
            .map_err(|err| {
                ApiError::internal(format!("manual rescan control send failed: {err}"))
            })?;
    } else {
        eprintln!(
            "fs_meta_api: rescan via local source node={}",
            state.node_id.0
        );
        state.source.trigger_rescan_when_ready().await?;
    }
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
        published_path_origin_counts_by_node,
    } = source;
    let crate::source::SourceStatusSnapshot {
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
            })
            .collect(),
        debug: super::types::StatusSourceDebug {
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
    let adapter = exchange_host_adapter(boundary, origin_id, default_route_bindings());
    let events = adapter
        .call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SOURCE_STATUS,
            internal_status_request_payload(),
            timeout,
            idle_after_first,
        )
        .await?;
    let snapshots = events
        .into_iter()
        .map(|event| {
            rmp_serde::from_slice::<SourceObservabilitySnapshot>(event.payload_bytes()).map_err(
                |err| CnxError::Internal(format!("decode source observability failed: {err}")),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
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
    Ok((merge_source_observability_snapshots(snapshots), runner_sets))
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
        merged
            .scheduled_source_groups_by_node
            .entry(node_id)
            .or_insert(groups);
    }
    for (node_id, groups) in fallback.scheduled_scan_groups_by_node {
        merged
            .scheduled_scan_groups_by_node
            .entry(node_id)
            .or_insert(groups);
    }
    for (node_id, signals) in fallback.last_control_frame_signals_by_node {
        merged
            .last_control_frame_signals_by_node
            .entry(node_id)
            .or_insert(signals);
    }
    for (node_id, count) in fallback.published_batches_by_node {
        merged.published_batches_by_node.entry(node_id).or_insert(count);
    }
    for (node_id, count) in fallback.published_events_by_node {
        merged.published_events_by_node.entry(node_id).or_insert(count);
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
        merged.last_published_at_us_by_node.entry(node_id).or_insert(ts);
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

fn status_facade_from_pending(pending: SharedFacadePendingStatus) -> StatusFacade {
    StatusFacade {
        pending: StatusFacadePending {
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
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RootSpec;
    use crate::source::SourceStatusSnapshot;
    use crate::source::config::GrantedMountRoot;
    use crate::source::{SourceConcreteRootHealthSnapshot, SourceLogicalRootHealthSnapshot};
    use crate::workers::source::SourceObservabilitySnapshot;
    use capanix_runtime_entry_sdk::advanced::boundary::ChannelIoSubset;

    struct NoopBoundary;

    impl ChannelIoSubset for NoopBoundary {}

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
                    current_revision: Some(1),
                    candidate_revision: None,
                    candidate_status: None,
                    draining_revision: None,
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
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            }],
            ..SinkStatusSnapshot::default()
        }
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
                    current_revision: Some(1),
                    candidate_revision: None,
                    candidate_status: None,
                    draining_revision: None,
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
        assert_eq!(merged.force_find_inflight_groups, vec!["nfs2".to_string()]);
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
    }
}
