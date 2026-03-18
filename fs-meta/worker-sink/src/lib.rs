use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use capanix_app_fs_meta::sink::SinkFileMeta;
use capanix_app_fs_meta::source::config::{SinkExecutionMode, SourceConfig};
use capanix_app_fs_meta::workers::sink_ipc::{
    SINK_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND, SinkWorkerRequest, SinkWorkerResponse,
    decode_request, encode_response, recv_opts, sink_worker_control_route_key_from_env,
};
use capanix_app_fs_meta_runtime_support::decode_control_payload;
use capanix_app_sdk::raw::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
    StateBoundary,
};
use capanix_app_sdk::runtime::{ControlEnvelope, EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event, RuntimeBoundary, RuntimeBoundaryApp};

fn block_on_runtime<F, T>(runtime: &tokio::runtime::Handle, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| runtime.block_on(fut))
    } else {
        runtime.block_on(fut)
    }
}

struct SinkWorkerState {
    sink: Option<SinkFileMeta>,
    node_id: Option<NodeId>,
    endpoints_started: bool,
}

fn classify_sink_worker_error(err: CnxError) -> SinkWorkerResponse {
    match err {
        CnxError::InvalidInput(message) => SinkWorkerResponse::InvalidInput(message),
        other => SinkWorkerResponse::Error(other.to_string()),
    }
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
    runtime: &tokio::runtime::Handle,
    state_boundary: Arc<dyn StateBoundary>,
    io_boundary: Arc<dyn ChannelIoSubset>,
) -> (SinkWorkerResponse, bool) {
    match request {
        SinkWorkerRequest::Ping => (SinkWorkerResponse::Ack, false),
        SinkWorkerRequest::Init { node_id, config } => {
            eprintln!("fs_meta_sink_worker: received Init for node_id={node_id}");
            let mut source_cfg = SourceConfig::default();
            source_cfg.roots = config.roots;
            source_cfg.host_object_grants = config.host_object_grants;
            source_cfg.sink_tombstone_ttl =
                Duration::from_millis(config.sink_tombstone_ttl_ms.max(1));
            source_cfg.sink_tombstone_tolerance_us = config.sink_tombstone_tolerance_us;
            source_cfg.sink_execution_mode = SinkExecutionMode::InProcess;
            match SinkFileMeta::with_boundaries_and_state(
                NodeId(node_id.clone()),
                None,
                state_boundary,
                source_cfg,
            ) {
                Ok(inner) => {
                    state.sink = Some(inner);
                    state.node_id = Some(NodeId(node_id));
                    state.endpoints_started = false;
                    eprintln!("fs_meta_sink_worker: Init completed");
                    (SinkWorkerResponse::Ack, false)
                }
                Err(err) => {
                    eprintln!("fs_meta_sink_worker: Init failed: {err}");
                    (SinkWorkerResponse::Error(err.to_string()), false)
                }
            }
        }
        SinkWorkerRequest::Start => match state.sink.as_ref() {
            Some(_) if state.endpoints_started => {
                eprintln!("fs_meta_sink_worker: Start ignored; endpoints already started");
                (SinkWorkerResponse::Ack, false)
            }
            Some(sink) => {
                let Some(node_id) = state.node_id.clone() else {
                    return (
                        SinkWorkerResponse::Error(
                            "sink worker missing node_id during start".into(),
                        ),
                        false,
                    );
                };
                eprintln!(
                    "fs_meta_sink_worker: Start received; starting runtime endpoints before Ack"
                );
                match sink.start_runtime_endpoints(io_boundary, node_id) {
                    Ok(()) => {
                        state.endpoints_started = true;
                        eprintln!("fs_meta_sink_worker: runtime endpoints started before Ack");
                        (SinkWorkerResponse::Ack, false)
                    }
                    Err(err) => {
                        eprintln!("fs_meta_sink_worker: start runtime endpoints failed: {err}");
                        (SinkWorkerResponse::Error(err.to_string()), false)
                    }
                }
            }
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::UpdateLogicalRoots {
            roots,
            host_object_grants,
        } => match state.sink.as_ref() {
            Some(sink) => match sink.update_logical_roots(roots, &host_object_grants) {
                Ok(_) => (SinkWorkerResponse::Ack, false),
                Err(err) => (classify_sink_worker_error(err), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::LogicalRootsSnapshot => match state.sink.as_ref() {
            Some(sink) => match sink.logical_roots_snapshot() {
                Ok(roots) => (SinkWorkerResponse::LogicalRoots(roots), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::ScheduledGroupIds => match state.sink.as_ref() {
            Some(sink) => match sink.scheduled_group_ids_snapshot() {
                Ok(groups) => (
                    SinkWorkerResponse::ScheduledGroupIds(
                        groups.map(|groups| groups.into_iter().collect()),
                    ),
                    false,
                ),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Health => match state.sink.as_ref() {
            Some(sink) => match sink.health() {
                Ok(health) => (SinkWorkerResponse::Health(health), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::StatusSnapshot => match state.sink.as_ref() {
            Some(sink) => match sink.status_snapshot() {
                Ok(snapshot) => (SinkWorkerResponse::StatusSnapshot(snapshot), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::VisibilityLagSamplesSince { since_us } => match state.sink.as_ref() {
            Some(sink) => (
                SinkWorkerResponse::VisibilityLagSamples(
                    sink.visibility_lag_samples_since(since_us),
                ),
                false,
            ),
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::MaterializedQuery { request } => match state.sink.as_ref() {
            Some(sink) => {
                eprintln!(
                    "fs_meta_sink_worker: MaterializedQuery selected_group={:?} recursive={} path={}",
                    request.scope.selected_group,
                    request.scope.recursive,
                    String::from_utf8_lossy(&request.scope.path)
                );
                match sink.materialized_query(&request) {
                    Ok(events) => {
                        eprintln!(
                            "fs_meta_sink_worker: MaterializedQuery response events={}",
                            events.len()
                        );
                        (SinkWorkerResponse::Events(events), false)
                    }
                    Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
                }
            }
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Send { events } => match state.sink.as_ref() {
            Some(sink) => match block_on_runtime(runtime, sink.send(&events)) {
                Ok(_) => (SinkWorkerResponse::Ack, false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Recv { timeout_ms, limit } => match state.sink.as_ref() {
            Some(sink) => {
                match block_on_runtime(runtime, sink.recv(recv_opts(timeout_ms, limit))) {
                    Ok(events) => (SinkWorkerResponse::Events(events), false),
                    Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
                }
            }
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::OnControlFrame { envelopes } => match state.sink.as_ref() {
            Some(sink) => match block_on_runtime(runtime, sink.on_control_frame(&envelopes)) {
                Ok(_) => (SinkWorkerResponse::Ack, false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Close => {
            if let Some(sink) = state.sink.as_ref() {
                let _ = block_on_runtime(runtime, sink.close());
            }
            (SinkWorkerResponse::Ack, true)
        }
    }
}

pub fn run_sink_worker_server(
    control_socket_path: &Path,
    data_socket_path: &Path,
) -> std::io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    eprintln!(
        "fs_meta_sink_worker: binding control={} data={}",
        control_socket_path.display(),
        data_socket_path.display()
    );
    let state = Arc::new(Mutex::new(SinkWorkerState {
        sink: None,
        node_id: None,
        endpoints_started: false,
    }));
    let runtime_handle = runtime.handle().clone();
    let boundary_holder: Arc<Mutex<Option<(Arc<dyn StateBoundary>, Arc<dyn ChannelIoSubset>)>>> =
        Arc::new(Mutex::new(None));
    let control_state = state.clone();
    let control_boundary_holder = boundary_holder.clone();
    let control_handler = Arc::new(
        move |envelopes: &[ControlEnvelope]| -> capanix_app_sdk::Result<()> {
            let (state_boundary, io_boundary) = control_boundary_holder
                .lock()
                .map_err(|_| {
                    CnxError::Internal("sink worker boundary holder lock poisoned".into())
                })?
                .clone()
                .ok_or_else(|| {
                    CnxError::NotReady("sink worker runtime boundary not ready".into())
                })?;
            let mut guard = control_state
                .lock()
                .map_err(|_| CnxError::Internal("sink worker state lock poisoned".into()))?;
            let mut runtime_envelopes = Vec::new();
            for envelope in envelopes {
                if let Ok(payload) =
                    decode_control_payload(envelope, SINK_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND)
                {
                    let request = decode_request(payload)?;
                    let (response, _) = process_worker_request(
                        request,
                        &mut guard,
                        &runtime_handle,
                        state_boundary.clone(),
                        io_boundary.clone(),
                    );
                    match response {
                        SinkWorkerResponse::Ack => {}
                        SinkWorkerResponse::Error(message) => {
                            return Err(CnxError::PeerError(message));
                        }
                        other => {
                            return Err(CnxError::ProtocolViolation(format!(
                                "unexpected sink bootstrap response: {:?}",
                                other
                            )));
                        }
                    }
                } else {
                    runtime_envelopes.push(envelope.clone());
                }
            }
            if !runtime_envelopes.is_empty() {
                let Some(sink) = guard.sink.as_ref() else {
                    return Err(CnxError::NotReady(
                        "sink worker runtime not initialized for runtime control frames".into(),
                    ));
                };
                block_on_runtime(&runtime_handle, sink.on_control_frame(&runtime_envelopes))?;
            }
            Ok(())
        },
    );
    let shared_boundary = Arc::new(
        runtime
            .block_on(
                capanix_unit_sidecar::UnitRuntimeIpcBoundary::bind_and_accept_with_control_handler(
                    control_socket_path,
                    data_socket_path,
                    control_handler,
                ),
            )
            .map_err(|err| {
                std::io::Error::other(format!("sink worker sidecar bind failed: {err}"))
            })?,
    );
    eprintln!("fs_meta_sink_worker: boundary accepted control/data sockets");
    let boundary: Arc<dyn RuntimeBoundary> = shared_boundary.clone();
    let io_boundary: Arc<dyn ChannelIoSubset> = shared_boundary;
    {
        let mut holder = boundary_holder
            .lock()
            .map_err(|_| std::io::Error::other("sink worker boundary holder lock poisoned"))?;
        *holder = Some((boundary.clone(), io_boundary.clone()));
    }
    run_sink_worker_primitive_loop(boundary, io_boundary, &runtime, state)
}

fn run_sink_worker_primitive_loop(
    boundary: Arc<dyn RuntimeBoundary>,
    io_boundary: Arc<dyn ChannelIoSubset>,
    runtime: &tokio::runtime::Runtime,
    state: Arc<Mutex<SinkWorkerState>>,
) -> std::io::Result<()> {
    let ctx = BoundaryContext::default();
    let worker_route_key = sink_worker_control_route_key_from_env();
    let request_channel = ChannelKey(worker_route_key.clone());
    let reply_channel = ChannelKey(format!("{}:reply", worker_route_key));

    eprintln!(
        "fs_meta_sink_worker: entering primitive loop route_key={}",
        worker_route_key
    );
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
        for request_event in requests {
            let (response, should_stop) = match decode_request(request_event.payload_bytes()) {
                Ok(request) => {
                    let mut guard = state
                        .lock()
                        .map_err(|_| std::io::Error::other("sink worker state lock poisoned"))?;
                    process_worker_request(
                        request,
                        &mut guard,
                        runtime.handle(),
                        state_boundary.clone(),
                        io_boundary.clone(),
                    )
                }
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            };
            should_break |= should_stop;
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
    }
    Ok(())
}
