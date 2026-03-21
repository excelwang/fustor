use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use capanix_app_fs_meta::sink::SinkFileMeta;
use capanix_app_fs_meta::source::config::SourceConfig;
use capanix_app_fs_meta::workers::sink::SinkWorkerRpc;
use capanix_app_fs_meta::workers::sink_ipc::{
    SINK_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND, SinkWorkerRequest, SinkWorkerResponse,
    recv_opts, sink_worker_control_route_key_from_env,
};
use capanix_app_sdk::raw::{ChannelIoSubset, StateBoundary};
use capanix_app_sdk::runtime::{ControlEnvelope, NodeId};
use capanix_app_sdk::{CnxError, Event};
use capanix_worker_runtime_support::{
    TypedWorkerServer, TypedWorkerSession, WorkerLoopControl, WorkerSessionContext,
};

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
    sink: Option<Arc<SinkFileMeta>>,
    node_id: Option<NodeId>,
    endpoints_started: bool,
    send_tx: Option<tokio::sync::mpsc::UnboundedSender<Vec<Event>>>,
}

fn classify_sink_worker_error(err: CnxError) -> SinkWorkerResponse {
    match err {
        CnxError::InvalidInput(message) => SinkWorkerResponse::InvalidInput(message),
        other => SinkWorkerResponse::Error(other.to_string()),
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
            match SinkFileMeta::with_boundaries_and_state(
                NodeId(node_id.clone()),
                None,
                state_boundary,
                source_cfg,
            ) {
                Ok(inner) => {
                    let inner = Arc::new(inner);
                    state.sink = Some(inner);
                    state.node_id = Some(NodeId(node_id));
                    state.endpoints_started = false;
                    state.send_tx = None;
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
                        if state.send_tx.is_none() {
                            let (send_tx, mut send_rx) =
                                tokio::sync::mpsc::unbounded_channel::<Vec<Event>>();
                            let sink_for_send = sink.clone();
                            runtime.spawn(async move {
                                while let Some(events) = send_rx.recv().await {
                                    if let Err(err) = sink_for_send.send(&events).await {
                                        eprintln!(
                                            "fs_meta_sink_worker: async Send apply failed: {err}"
                                        );
                                    }
                                }
                            });
                            state.send_tx = Some(send_tx);
                        }
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
            Some(_) => match state.send_tx.as_ref() {
                Some(send_tx) => match send_tx.send(events) {
                    Ok(()) => (SinkWorkerResponse::Ack, false),
                    Err(err) => (
                        SinkWorkerResponse::Error(format!(
                            "sink worker send queue unavailable: {} event(s) dropped",
                            err.0.len()
                        )),
                        false,
                    ),
                },
                None => match state.sink.as_ref() {
                    Some(sink) => match block_on_runtime(runtime, sink.send(&events)) {
                        Ok(_) => (SinkWorkerResponse::Ack, false),
                        Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
                    },
                    None => (SinkWorkerResponse::Error("sink worker not initialized".into()), false),
                },
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
            Some(sink) => {
                match block_on_runtime(runtime, sink.on_control_frame(&envelopes)) {
                    Ok(_) => (SinkWorkerResponse::Ack, false),
                    Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
                }
            }
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Close => {
            state.send_tx.take();
            if let Some(sink) = state.sink.as_ref() {
                let _ = block_on_runtime(runtime, sink.close());
            }
            state.sink = None;
            (SinkWorkerResponse::Ack, true)
        }
    }
}

pub fn run_sink_worker_server(
    control_socket_path: &Path,
    data_socket_path: &Path,
) -> std::io::Result<()> {
    let state = Arc::new(Mutex::new(SinkWorkerState {
        sink: None,
        node_id: None,
        endpoints_started: false,
        send_tx: None,
    }));
    TypedWorkerServer::<SinkWorkerRpc, _>::new(
        sink_worker_control_route_key_from_env(),
        SINK_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND,
        SinkWorkerSession { state },
    )
    .run_with_sidecar(control_socket_path, data_socket_path)
}

struct SinkWorkerSession {
    state: Arc<Mutex<SinkWorkerState>>,
}

#[async_trait::async_trait]
impl TypedWorkerSession<SinkWorkerRpc> for SinkWorkerSession {
    async fn handle_request(
        &mut self,
        request: SinkWorkerRequest,
        context: &WorkerSessionContext,
    ) -> capanix_app_sdk::Result<WorkerLoopControl<SinkWorkerResponse>> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| CnxError::Internal("sink worker state lock poisoned".into()))?;
        let (response, stop) = process_worker_request(
            request,
            &mut guard,
            context.runtime_handle(),
            context.state_boundary(),
            context.io_boundary(),
        );
        Ok(if stop {
            WorkerLoopControl::Stop(response)
        } else {
            WorkerLoopControl::Continue(response)
        })
    }

    async fn on_runtime_control(
        &mut self,
        envelopes: &[ControlEnvelope],
        context: &WorkerSessionContext,
    ) -> capanix_app_sdk::Result<()> {
        let guard = self
            .state
            .lock()
            .map_err(|_| CnxError::Internal("sink worker state lock poisoned".into()))?;
        let Some(sink) = guard.sink.as_ref() else {
            return Err(CnxError::NotReady(
                "sink worker runtime not initialized for runtime control frames".into(),
            ));
        };
        block_on_runtime(context.runtime_handle(), sink.on_control_frame(envelopes))
    }
}
