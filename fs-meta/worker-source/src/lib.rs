use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use capanix_app_fs_meta::source::FSMetaSource;
use capanix_app_fs_meta::source::config::SourceExecutionMode;
use capanix_app_fs_meta::workers::source_ipc::{
    SOURCE_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND, SourceWorkerRequest, SourceWorkerResponse,
    decode_request, encode_response, source_worker_control_route_key_from_env,
};
use capanix_app_fs_meta_runtime_support::decode_control_payload;
use capanix_app_sdk::raw::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
    StateBoundary,
};
use capanix_app_sdk::runtime::{ControlEnvelope, EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event, RuntimeBoundary, RuntimeBoundaryApp};
use futures_util::StreamExt;
use tokio::task::JoinHandle;

const ROUTE_KEY_EVENTS: &str = "fs-meta.events:v1";
const SOURCE_WORKER_STOP_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const SOURCE_WORKER_STOP_ABORT_TIMEOUT: Duration = Duration::from_millis(250);

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_micros() as u64,
        Err(_) => 0,
    }
}

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

struct SourceWorkerState {
    source: Option<Arc<FSMetaSource>>,
    pending_init: Option<(NodeId, capanix_app_fs_meta::source::config::SourceConfig)>,
    pump_task: Option<JoinHandle<()>>,
}

async fn stop_source_runtime_with_timeouts(
    state: &mut SourceWorkerState,
    join_timeout: Duration,
    abort_timeout: Duration,
) {
    if let Some(source) = state.source.as_ref() {
        let _ = source.close().await;
    }
    if let Some(mut handle) = state.pump_task.take() {
        if tokio::time::timeout(join_timeout, &mut handle)
            .await
            .is_err()
        {
            handle.abort();
            let _ = tokio::time::timeout(abort_timeout, handle).await;
        }
    }
    state.source = None;
}

async fn stop_source_runtime(state: &mut SourceWorkerState) {
    stop_source_runtime_with_timeouts(
        state,
        SOURCE_WORKER_STOP_WAIT_TIMEOUT,
        SOURCE_WORKER_STOP_ABORT_TIMEOUT,
    )
    .await;
}

fn start_source_pump_with_stream<S>(
    stream: S,
    boundary: Arc<dyn ChannelIoSubset>,
    runtime: &tokio::runtime::Handle,
) -> JoinHandle<()>
where
    S: futures_util::Stream<Item = Vec<Event>> + Send + 'static,
{
    runtime.spawn(async move {
        futures_util::pin_mut!(stream);
        while let Some(batch) = stream.next().await {
            if let Err(err) = boundary.channel_send(
                BoundaryContext::default(),
                ChannelSendRequest {
                    channel_key: ChannelKey(format!("{}.stream", ROUTE_KEY_EVENTS)),
                    events: batch,
                },
            ) {
                log::error!(
                    "source worker pump failed to publish source batch on stream route: {:?}",
                    err
                );
            }
        }
    })
}

fn process_worker_request(
    request: SourceWorkerRequest,
    state: &mut SourceWorkerState,
    boundary: Arc<dyn ChannelIoSubset>,
    runtime: &tokio::runtime::Handle,
    state_boundary: Arc<dyn StateBoundary>,
) -> (SourceWorkerResponse, bool) {
    match request {
        SourceWorkerRequest::Ping => (SourceWorkerResponse::Ack, false),
        SourceWorkerRequest::Init {
            node_id,
            mut config,
        } => {
            eprintln!("fs_meta_source_worker: received Init for node_id={node_id}");
            let _ = block_on_runtime(runtime, stop_source_runtime(state));
            config.source_execution_mode = SourceExecutionMode::InProcess;
            state.pending_init = Some((NodeId(node_id), config));
            eprintln!("fs_meta_source_worker: Init accepted");
            (SourceWorkerResponse::Ack, false)
        }
        SourceWorkerRequest::Start => {
            eprintln!("fs_meta_source_worker: received Start");
            let should_restart = state
                .pump_task
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished);
            if should_restart {
                let _ = block_on_runtime(runtime, stop_source_runtime(state));
                return (
                    SourceWorkerResponse::Error(
                        "source worker start requested after source runtime stopped; re-init required"
                            .into(),
                    ),
                    false,
                );
            }
            if state.source.is_none() {
                let Some((node_id, config)) = state.pending_init.clone() else {
                    return (
                        SourceWorkerResponse::Error("source worker not initialized".into()),
                        false,
                    );
                };
                eprintln!("fs_meta_source_worker: materializing runtime on Start");
                match FSMetaSource::with_boundaries_and_state_deferred_runtime_endpoints(
                    config,
                    node_id,
                    boundary.clone(),
                    state_boundary,
                ) {
                    Ok(inner) => {
                        state.source = Some(Arc::new(inner));
                        eprintln!("fs_meta_source_worker: Start runtime materialized");
                    }
                    Err(err) => {
                        eprintln!("fs_meta_source_worker: Start materialization failed: {err}");
                        return (SourceWorkerResponse::Error(err.to_string()), false);
                    }
                }
            }
            if state.pump_task.is_none() {
                let Some(source) = state.source.as_ref() else {
                    return (
                        SourceWorkerResponse::Error(
                            "source worker runtime missing during start".into(),
                        ),
                        false,
                    );
                };
                eprintln!("fs_meta_source_worker: scheduling source stream after Ack");
                let source = source.clone();
                let boundary = boundary.clone();
                state.pump_task = Some(runtime.spawn(async move {
                    eprintln!("fs_meta_source_worker: starting source stream after Ack");
                    match source.pub_().await {
                        Ok(mut stream) => {
                            eprintln!("fs_meta_source_worker: source stream ready after Ack");
                            while let Some(batch) = stream.next().await {
                                if let Err(err) = boundary.channel_send(
                                    BoundaryContext::default(),
                                    ChannelSendRequest {
                                        channel_key: ChannelKey(format!("{}.stream", ROUTE_KEY_EVENTS)),
                                        events: batch,
                                    },
                                ) {
                                    eprintln!(
                                        "fs_meta_source_worker: source pump failed to publish source batch on stream route: {:?}",
                                        err
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            eprintln!("fs_meta_source_worker: Start stream acquisition failed after Ack: {err}");
                        }
                    }
                }));
            } else {
                eprintln!("fs_meta_source_worker: source pump already running");
            }
            (SourceWorkerResponse::Ack, false)
        }
        SourceWorkerRequest::UpdateLogicalRoots { roots } => match state.source.as_ref() {
            Some(source) => match runtime.block_on(source.update_logical_roots(roots)) {
                Ok(_) => (SourceWorkerResponse::Ack, false),
                Err(err) => (SourceWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::LogicalRootsSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::LogicalRoots(source.logical_roots_snapshot()),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::HostObjectGrantsSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::HostObjectGrants(source.host_object_grants_snapshot()),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::HostObjectGrantsVersionSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::HostObjectGrantsVersion(
                    source.host_object_grants_version_snapshot(),
                ),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::StatusSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::StatusSnapshot(source.status_snapshot()),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::LifecycleState => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::LifecycleState(
                    format!("{:?}", source.state()).to_ascii_lowercase(),
                ),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ScheduledSourceGroupIds => match state.source.as_ref() {
            Some(source) => match source.scheduled_source_group_ids() {
                Ok(groups) => (
                    SourceWorkerResponse::ScheduledGroupIds(
                        groups.map(|group_ids| group_ids.into_iter().collect()),
                    ),
                    false,
                ),
                Err(err) => (SourceWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ScheduledScanGroupIds => match state.source.as_ref() {
            Some(source) => match source.scheduled_scan_group_ids() {
                Ok(groups) => (
                    SourceWorkerResponse::ScheduledGroupIds(
                        groups.map(|group_ids| group_ids.into_iter().collect()),
                    ),
                    false,
                ),
                Err(err) => (SourceWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::SourcePrimaryByGroupSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::SourcePrimaryByGroup(
                    source.source_primary_by_group_snapshot(),
                ),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::LastForceFindRunnerByGroupSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::LastForceFindRunnerByGroup(
                    source.last_force_find_runner_by_group_snapshot(),
                ),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ForceFindInflightGroupsSnapshot => match state.source.as_ref() {
            Some(source) => (
                SourceWorkerResponse::ForceFindInflightGroups(
                    source.force_find_inflight_groups_snapshot(),
                ),
                false,
            ),
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::ResolveGroupIdForObjectRef { object_ref } => {
            match state.source.as_ref() {
                Some(source) => (
                    SourceWorkerResponse::ResolveGroupIdForObjectRef(
                        source.resolve_group_id_for_object_ref(&object_ref),
                    ),
                    false,
                ),
                None => (
                    SourceWorkerResponse::Error("source worker not initialized".into()),
                    false,
                ),
            }
        }
        SourceWorkerRequest::TriggerRescanWhenReady => match state.source.as_ref() {
            Some(source) => {
                eprintln!("fs_meta_source_worker: received TriggerRescanWhenReady");
                let source = source.clone();
                runtime.spawn(async move {
                    source.trigger_rescan_when_ready().await;
                });
                (SourceWorkerResponse::Ack, false)
            }
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::OnControlFrame { envelopes } => match state.source.as_ref() {
            Some(source) => match block_on_runtime(runtime, source.on_control_frame(&envelopes)) {
                Ok(_) => (SourceWorkerResponse::Ack, false),
                Err(err) => (SourceWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::Close => {
            let _ = block_on_runtime(runtime, stop_source_runtime(state));
            (SourceWorkerResponse::Ack, true)
        }
    }
}

pub fn run_source_worker_server(
    control_socket_path: &Path,
    data_socket_path: &Path,
) -> std::io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let state = Arc::new(Mutex::new(SourceWorkerState {
        source: None,
        pending_init: None,
        pump_task: None,
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
                    CnxError::Internal("source worker boundary holder lock poisoned".into())
                })?
                .clone()
                .ok_or_else(|| {
                    CnxError::NotReady("source worker runtime boundary not ready".into())
                })?;
            let mut guard = control_state
                .lock()
                .map_err(|_| CnxError::Internal("source worker state lock poisoned".into()))?;
            let mut runtime_envelopes = Vec::new();
            for envelope in envelopes {
                if let Ok(payload) =
                    decode_control_payload(envelope, SOURCE_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND)
                {
                    let request = decode_request(payload)?;
                    let (response, _) = process_worker_request(
                        request,
                        &mut guard,
                        io_boundary.clone(),
                        &runtime_handle,
                        state_boundary.clone(),
                    );
                    match response {
                        SourceWorkerResponse::Ack => {}
                        SourceWorkerResponse::Error(message) => {
                            return Err(CnxError::PeerError(message));
                        }
                        other => {
                            return Err(CnxError::ProtocolViolation(format!(
                                "unexpected source bootstrap response: {:?}",
                                other
                            )));
                        }
                    }
                } else {
                    runtime_envelopes.push(envelope.clone());
                }
            }
            if !runtime_envelopes.is_empty() {
                let Some(source) = guard.source.as_ref() else {
                    return Err(CnxError::NotReady(
                        "source worker runtime not initialized for runtime control frames".into(),
                    ));
                };
                block_on_runtime(&runtime_handle, source.on_control_frame(&runtime_envelopes))?;
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
                std::io::Error::other(format!("source worker sidecar bind failed: {err}"))
            })?,
    );
    let boundary: Arc<dyn RuntimeBoundary> = shared_boundary.clone();
    let io_boundary: Arc<dyn ChannelIoSubset> = shared_boundary;
    {
        let mut holder = boundary_holder
            .lock()
            .map_err(|_| std::io::Error::other("source worker boundary holder lock poisoned"))?;
        *holder = Some((boundary.clone(), io_boundary.clone()));
    }
    run_source_worker_runtime_loop_with_state(boundary, io_boundary, &runtime, state)
}

pub fn run_source_worker_runtime_loop(
    boundary: Arc<dyn RuntimeBoundary>,
    io_boundary: Arc<dyn ChannelIoSubset>,
    runtime: &tokio::runtime::Runtime,
) -> std::io::Result<()> {
    let state = Arc::new(Mutex::new(SourceWorkerState {
        source: None,
        pending_init: None,
        pump_task: None,
    }));
    run_source_worker_runtime_loop_with_state(boundary, io_boundary, runtime, state)
}

fn run_source_worker_runtime_loop_with_state(
    boundary: Arc<dyn RuntimeBoundary>,
    io_boundary: Arc<dyn ChannelIoSubset>,
    runtime: &tokio::runtime::Runtime,
    state: Arc<Mutex<SourceWorkerState>>,
) -> std::io::Result<()> {
    let ctx = BoundaryContext::default();
    let worker_route_key = source_worker_control_route_key_from_env();
    let request_channel = ChannelKey(worker_route_key.clone());
    let reply_channel = ChannelKey(format!("{}:reply", worker_route_key));

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
                    "source worker receive request failed: {err}"
                )));
            }
        };
        if requests.is_empty() {
            continue;
        }

        let mut responses = Vec::with_capacity(requests.len());
        for request_event in requests {
            let decoded = decode_request(request_event.payload_bytes());
            let (response, should_stop) = match decoded {
                Ok(request) => {
                    let mut guard = state
                        .lock()
                        .map_err(|_| std::io::Error::other("source worker state lock poisoned"))?;
                    process_worker_request(
                        request,
                        &mut guard,
                        io_boundary.clone(),
                        runtime.handle(),
                        state_boundary.clone(),
                    )
                }
                Err(err) => (SourceWorkerResponse::Error(err.to_string()), false),
            };
            should_break |= should_stop;
            let payload = encode_response(&response).map_err(|err| {
                std::io::Error::other(format!("source worker encode response failed: {err}"))
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
                std::io::Error::other(format!("source worker send response failed: {err}"))
            })?;
    }
    Ok(())
}
