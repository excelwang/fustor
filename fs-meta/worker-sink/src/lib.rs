use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use capanix_app_fs_meta::sink::SinkFileMeta;
use capanix_app_fs_meta::workers::sink_ipc::{
    SINK_WORKER_ROUTE_KEY, SinkWorkerRequest, SinkWorkerResponse, decode_request, encode_response,
    recv_opts,
};
use capanix_app_fs_meta::source::config::{SinkExecutionMode, SourceConfig};
use capanix_app_sdk::raw::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
    StateBoundary,
};
use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event, RuntimeBoundary, RuntimeBoundaryApp};

struct SinkWorkerState {
    sink: Option<SinkFileMeta>,
    node_id: Option<NodeId>,
    endpoints_started: bool,
}

enum PostReplyAction {
    StartRuntimeEndpoints,
}

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_micros() as u64,
        Err(_) => 0,
    }
}

fn process_worker_request(
    request: SinkWorkerRequest,
    state: &mut SinkWorkerState,
    runtime: &tokio::runtime::Runtime,
    state_boundary: Arc<dyn StateBoundary>,
) -> (SinkWorkerResponse, bool, Option<PostReplyAction>) {
    match request {
        SinkWorkerRequest::Init { node_id, config } => {
            let mut source_cfg = SourceConfig::default();
            source_cfg.roots = config.roots;
            source_cfg.host_object_grants = config.host_object_grants;
            source_cfg.sink_tombstone_ttl =
                Duration::from_millis(config.sink_tombstone_ttl_ms.max(1));
            source_cfg.sink_tombstone_tolerance_us = config.sink_tombstone_tolerance_us;
            source_cfg.sink_execution_mode = SinkExecutionMode::InProcess;
            match SinkFileMeta::with_state_boundary_deferred_runtime_endpoints(
                state_boundary.clone(),
                source_cfg,
            ) {
                Ok(inner) => {
                    state.sink = Some(inner);
                    state.node_id = Some(NodeId(node_id));
                    state.endpoints_started = false;
                    (SinkWorkerResponse::Ack, false, None)
                }
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false, None),
            }
        }
        SinkWorkerRequest::Start => match state.sink.as_ref() {
            Some(_) if state.endpoints_started => (SinkWorkerResponse::Ack, false, None),
            Some(_) => (
                SinkWorkerResponse::Ack,
                false,
                Some(PostReplyAction::StartRuntimeEndpoints),
            ),
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
                None,
            ),
        },
        SinkWorkerRequest::UpdateLogicalRoots {
            roots,
            host_object_grants,
        } => match state.sink.as_ref() {
            Some(sink) => match sink.update_logical_roots(roots, &host_object_grants) {
                Ok(_) => (SinkWorkerResponse::Ack, false, None),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false, None),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
                None,
            ),
        },
        SinkWorkerRequest::LogicalRootsSnapshot => match state.sink.as_ref() {
            Some(sink) => match sink.logical_roots_snapshot() {
                Ok(roots) => (SinkWorkerResponse::LogicalRoots(roots), false, None),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false, None),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
                None,
            ),
        },
        SinkWorkerRequest::ScheduledGroupIds => match state.sink.as_ref() {
            Some(sink) => match sink.scheduled_group_ids_snapshot() {
                Ok(groups) => (
                    SinkWorkerResponse::ScheduledGroupIds(
                        groups.map(|groups| groups.into_iter().collect()),
                    ),
                    false,
                    None,
                ),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false, None),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
                None,
            ),
        },
        SinkWorkerRequest::Health => match state.sink.as_ref() {
            Some(sink) => match sink.health() {
                Ok(health) => (SinkWorkerResponse::Health(health), false, None),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false, None),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
                None,
            ),
        },
        SinkWorkerRequest::StatusSnapshot => match state.sink.as_ref() {
            Some(sink) => match sink.status_snapshot() {
                Ok(snapshot) => (SinkWorkerResponse::StatusSnapshot(snapshot), false, None),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false, None),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
                None,
            ),
        },
        SinkWorkerRequest::VisibilityLagSamplesSince { since_us } => match state.sink.as_ref() {
            Some(sink) => (
                SinkWorkerResponse::VisibilityLagSamples(
                    sink.visibility_lag_samples_since(since_us),
                ),
                false,
                None,
            ),
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
                None,
            ),
        },
        SinkWorkerRequest::MaterializedQuery { request } => match state.sink.as_ref() {
            Some(sink) => match sink.materialized_query(&request) {
                Ok(events) => (SinkWorkerResponse::Events(events), false, None),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false, None),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
                None,
            ),
        },
        SinkWorkerRequest::Send { events } => match state.sink.as_ref() {
            Some(sink) => match runtime.block_on(sink.send(&events)) {
                Ok(_) => (SinkWorkerResponse::Ack, false, None),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false, None),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
                None,
            ),
        },
        SinkWorkerRequest::Recv { timeout_ms, limit } => match state.sink.as_ref() {
            Some(sink) => match runtime.block_on(sink.recv(recv_opts(timeout_ms, limit))) {
                Ok(events) => (SinkWorkerResponse::Events(events), false, None),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false, None),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
                None,
            ),
        },
        SinkWorkerRequest::OnControlFrame { envelopes } => match state.sink.as_ref() {
            Some(sink) => match runtime.block_on(sink.on_control_frame(&envelopes)) {
                Ok(_) => (SinkWorkerResponse::Ack, false, None),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false, None),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
                None,
            ),
        },
        SinkWorkerRequest::Close => {
            if let Some(sink) = state.sink.as_ref() {
                let _ = runtime.block_on(sink.close());
            }
            (SinkWorkerResponse::Ack, true, None)
        }
    }
}

pub fn run_sink_worker_server(worker_socket_path: &Path) -> std::io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let shared_boundary = Arc::new(
        runtime
            .block_on(
                capanix_unit_sidecar::UnitRuntimeIpcBoundary::bind_and_accept(worker_socket_path),
            )
            .map_err(|err| {
                std::io::Error::other(format!("sink worker sidecar bind failed: {err}"))
            })?,
    );
    let boundary: Arc<dyn RuntimeBoundary> = shared_boundary.clone();
    let io_boundary: Arc<dyn ChannelIoSubset> = shared_boundary;
    run_sink_worker_primitive_loop(boundary, io_boundary, &runtime)
}

fn run_sink_worker_primitive_loop(
    boundary: Arc<dyn RuntimeBoundary>,
    io_boundary: Arc<dyn ChannelIoSubset>,
    runtime: &tokio::runtime::Runtime,
) -> std::io::Result<()> {
    let ctx = BoundaryContext::default();
    let request_channel = ChannelKey(SINK_WORKER_ROUTE_KEY.to_string());
    let reply_channel = ChannelKey(format!("{}:reply", SINK_WORKER_ROUTE_KEY));

    let mut state = SinkWorkerState {
        sink: None,
        node_id: None,
        endpoints_started: false,
    };
    let state_boundary: Arc<dyn StateBoundary> = boundary.clone();
    let mut should_break = false;
    while !should_break {
        let requests = match io_boundary.channel_recv(
            ctx.clone(),
            ChannelRecvRequest {
                channel_key: request_channel.clone(),
                timeout_ms: Some(250),
            },
        ) {
            Ok(events) => events,
            Err(CnxError::Timeout) => continue,
            Err(err) => {
                return Err(std::io::Error::other(format!(
                    "sink worker receive request failed: {err}"
                )));
            }
        };
        if requests.is_empty() {
            continue;
        }

        let mut responses = Vec::with_capacity(requests.len());
        let mut post_reply_actions = Vec::new();
        for request_event in requests {
            let (response, should_stop, post_reply_action) =
                match decode_request(request_event.payload_bytes()) {
                Ok(request) => process_worker_request(
                    request,
                    &mut state,
                    runtime,
                    state_boundary.clone(),
                ),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false, None),
            };
            should_break |= should_stop;
            if let Some(post_reply_action) = post_reply_action {
                post_reply_actions.push(post_reply_action);
            }
            let payload = encode_response(&response).map_err(|err| {
                std::io::Error::other(format!("sink worker encode response failed: {err}"))
            })?;
            responses.push(Event::new(
                EventMetadata {
                    origin_id: request_event.metadata().origin_id.clone(),
                    timestamp_us: now_us(),
                    logical_ts: None,
                    correlation_id: request_event.metadata().correlation_id,
                    ingress_auth: None,
                    trace: None,
                },
                Bytes::from(payload),
            ));
        }

        io_boundary
            .channel_send(
                ctx.clone(),
                ChannelSendRequest {
                    channel_key: reply_channel.clone(),
                    events: responses,
                },
            )
            .map_err(|err| {
                std::io::Error::other(format!("sink worker send response failed: {err}"))
            })?;

        for action in post_reply_actions {
            match action {
                PostReplyAction::StartRuntimeEndpoints => {
                    let Some(sink) = state.sink.as_ref() else {
                        continue;
                    };
                    let Some(node_id) = state.node_id.clone() else {
                        continue;
                    };
                    sink.start_runtime_endpoints(io_boundary.clone(), node_id)
                        .map_err(|err| {
                            std::io::Error::other(format!(
                                "sink worker start runtime endpoints failed: {err}"
                            ))
                        })?;
                    state.endpoints_started = true;
                }
            }
        }
    }
    Ok(())
}
