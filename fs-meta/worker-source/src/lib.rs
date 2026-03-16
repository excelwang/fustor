use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use capanix_app_fs_meta::source::FSMetaSource;
use capanix_app_fs_meta::workers::source_ipc::{
    SOURCE_WORKER_ROUTE_KEY, SourceWorkerRequest, SourceWorkerResponse, decode_request,
    encode_response,
};
use capanix_app_fs_meta::source::config::SourceExecutionMode;
use capanix_app_sdk::raw::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
    StateBoundary,
};
use capanix_app_sdk::runtime::{EventMetadata, NodeId};
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

struct SourceWorkerState {
    source: Option<Arc<FSMetaSource>>,
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

fn start_source_pump(
    source: Arc<FSMetaSource>,
    boundary: Arc<dyn ChannelIoSubset>,
    runtime: &tokio::runtime::Runtime,
) -> JoinHandle<()> {
    runtime.spawn(async move {
        match source.pub_().await {
            Ok(stream) => {
                tokio::pin!(stream);
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
            }
            Err(err) => {
                log::error!("source worker failed to start source stream: {err}");
            }
        }
    })
}

fn process_worker_request(
    request: SourceWorkerRequest,
    state: &mut SourceWorkerState,
    boundary: Arc<dyn ChannelIoSubset>,
    runtime: &tokio::runtime::Runtime,
    state_boundary: Arc<dyn StateBoundary>,
) -> (SourceWorkerResponse, bool) {
    match request {
        SourceWorkerRequest::Init {
            node_id,
            mut config,
        } => {
            let _ = runtime.block_on(stop_source_runtime(state));
            config.source_execution_mode = SourceExecutionMode::InProcess;
            match FSMetaSource::with_boundaries_and_state(
                config,
                NodeId(node_id),
                Some(boundary.clone()),
                state_boundary,
            ) {
                Ok(inner) => {
                    state.source = Some(Arc::new(inner));
                    (SourceWorkerResponse::Ack, false)
                }
                Err(err) => (SourceWorkerResponse::Error(err.to_string()), false),
            }
        }
        SourceWorkerRequest::Start => match state.source.as_ref() {
            Some(source) => {
                let should_restart = state
                    .pump_task
                    .as_ref()
                    .is_some_and(tokio::task::JoinHandle::is_finished);
                if should_restart {
                    let _ = runtime.block_on(stop_source_runtime(state));
                    return (
                        SourceWorkerResponse::Error(
                            "source worker start requested after source runtime stopped; re-init required"
                                .into(),
                        ),
                        false,
                    );
                }
                if state.pump_task.is_none() {
                    state.pump_task = Some(start_source_pump(source.clone(), boundary, runtime));
                }
                (SourceWorkerResponse::Ack, false)
            }
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
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
                runtime.block_on(source.trigger_rescan_when_ready());
                (SourceWorkerResponse::Ack, false)
            }
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::OnControlFrame { envelopes } => match state.source.as_ref() {
            Some(source) => match runtime.block_on(source.on_control_frame(&envelopes)) {
                Ok(_) => (SourceWorkerResponse::Ack, false),
                Err(err) => (SourceWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SourceWorkerResponse::Error("source worker not initialized".into()),
                false,
            ),
        },
        SourceWorkerRequest::Close => {
            let _ = runtime.block_on(stop_source_runtime(state));
            (SourceWorkerResponse::Ack, true)
        }
    }
}

pub fn run_source_worker_server(worker_socket_path: &Path) -> std::io::Result<()> {
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
                std::io::Error::other(format!("source worker sidecar bind failed: {err}"))
            })?,
    );
    let boundary: Arc<dyn RuntimeBoundary> = shared_boundary.clone();
    let io_boundary: Arc<dyn ChannelIoSubset> = shared_boundary;
    run_source_worker_primitive_loop(boundary, io_boundary, &runtime)
}

fn run_source_worker_primitive_loop(
    boundary: Arc<dyn RuntimeBoundary>,
    io_boundary: Arc<dyn ChannelIoSubset>,
    runtime: &tokio::runtime::Runtime,
) -> std::io::Result<()> {
    let ctx = BoundaryContext::default();
    let request_channel = ChannelKey(SOURCE_WORKER_ROUTE_KEY.to_string());
    let reply_channel = ChannelKey(format!("{}:reply", SOURCE_WORKER_ROUTE_KEY));

    let state_boundary: Arc<dyn StateBoundary> = boundary.clone();
    let mut state = SourceWorkerState {
        source: None,
        pump_task: None,
    };
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
            let (response, should_stop) = match decode_request(request_event.payload_bytes()) {
                Ok(request) => process_worker_request(
                    request,
                    &mut state,
                    io_boundary.clone(),
                    runtime,
                    state_boundary.clone(),
                ),
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

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn stop_source_runtime_aborts_stuck_pump() {
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_flag = dropped.clone();
        let pump_task = tokio::spawn(async move {
            let _drop_flag = DropFlag(dropped_flag);
            pending::<()>().await;
        });
        let mut state = SourceWorkerState {
            source: None,
            pump_task: Some(pump_task),
        };

        stop_source_runtime_with_timeouts(
            &mut state,
            Duration::from_millis(10),
            Duration::from_millis(10),
        )
        .await;

        tokio::task::yield_now().await;
        assert!(dropped.load(Ordering::SeqCst));
        assert!(state.pump_task.is_none());
        assert!(state.source.is_none());
    }
}
