use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use capanix_app_sdk::runtime::{ControlEnvelope, NodeId};
use capanix_app_sdk::{CnxError, Event};
use capanix_runtime_entry_sdk::advanced::boundary::{ChannelIoSubset, StateBoundary};
use capanix_runtime_entry_sdk::worker_runtime::{
    TypedWorkerBootstrapSession, TypedWorkerSession, WorkerLoopControl, WorkerSessionContext,
    run_worker_sidecar_server,
};

use crate::sink::SinkFileMeta;
use crate::source::config::SourceConfig;
use crate::workers::sink::SinkWorkerRpc;
use crate::workers::sink_ipc::{
    SinkWorkerInitConfig, SinkWorkerRequest, SinkWorkerResponse, recv_opts,
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

fn bootstrap_not_ready() -> CnxError {
    CnxError::NotReady("worker not initialized".into())
}

fn bootstrap_init_sink_runtime(
    node_id: NodeId,
    config: SinkWorkerInitConfig,
    state: &mut SinkWorkerState,
    state_boundary: Arc<dyn StateBoundary>,
) -> Result<(), CnxError> {
    let mut source_cfg = SourceConfig::default();
    source_cfg.roots = config.roots;
    source_cfg.host_object_grants = config.host_object_grants;
    source_cfg.sink_tombstone_ttl = Duration::from_millis(config.sink_tombstone_ttl_ms.max(1));
    source_cfg.sink_tombstone_tolerance_us = config.sink_tombstone_tolerance_us;
    let inner =
        SinkFileMeta::with_boundaries_and_state(node_id.clone(), None, state_boundary, source_cfg)?;
    state.sink = Some(Arc::new(inner));
    state.node_id = Some(node_id);
    state.endpoints_started = false;
    state.send_tx = None;
    Ok(())
}

fn bootstrap_start_sink_runtime(
    state: &mut SinkWorkerState,
    runtime: &tokio::runtime::Handle,
    io_boundary: Arc<dyn ChannelIoSubset>,
) -> Result<(), CnxError> {
    match state.sink.as_ref() {
        Some(_) if state.endpoints_started => Ok(()),
        Some(sink) => {
            let Some(node_id) = state.node_id.clone() else {
                return Err(CnxError::Internal(
                    "sink worker missing node_id during start".into(),
                ));
            };
            sink.start_runtime_endpoints(io_boundary, node_id)?;
            if state.send_tx.is_none() {
                let (send_tx, mut send_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<Event>>();
                let sink_for_send = sink.clone();
                runtime.spawn(async move {
                    while let Some(events) = send_rx.recv().await {
                        if let Err(err) = sink_for_send.send(&events).await {
                            eprintln!("fs_meta_sink_worker: async Send apply failed: {err}");
                        }
                    }
                });
                state.send_tx = Some(send_tx);
            }
            state.endpoints_started = true;
            Ok(())
        }
        None => Err(bootstrap_not_ready()),
    }
}

async fn bootstrap_stop_sink_runtime(state: &mut SinkWorkerState) {
    state.send_tx.take();
    if let Some(sink) = state.sink.as_ref() {
        let _ = sink.close().await;
    }
    state.sink = None;
    state.node_id = None;
    state.endpoints_started = false;
}

fn process_worker_request(
    request: SinkWorkerRequest,
    state: &mut SinkWorkerState,
    runtime: &tokio::runtime::Handle,
    state_boundary: Arc<dyn StateBoundary>,
    io_boundary: Arc<dyn ChannelIoSubset>,
) -> (SinkWorkerResponse, bool) {
    let _ = state_boundary;
    let _ = io_boundary;
    match request {
        SinkWorkerRequest::UpdateLogicalRoots {
            roots,
            host_object_grants,
        } => match state.sink.as_ref() {
            Some(sink) => match sink.update_logical_roots(roots, &host_object_grants) {
                Ok(_) => (SinkWorkerResponse::Ack, false),
                Err(err) => (classify_sink_worker_error(err), false),
            },
            None => (
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::LogicalRootsSnapshot => match state.sink.as_ref() {
            Some(sink) => match sink.logical_roots_snapshot() {
                Ok(roots) => (SinkWorkerResponse::LogicalRoots(roots), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("worker not initialized".into()),
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
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Health => match state.sink.as_ref() {
            Some(sink) => match sink.health() {
                Ok(health) => (SinkWorkerResponse::Health(health), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::StatusSnapshot => match state.sink.as_ref() {
            Some(sink) => match sink.status_snapshot() {
                Ok(snapshot) => (SinkWorkerResponse::StatusSnapshot(snapshot), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("worker not initialized".into()),
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
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::MaterializedQuery { request } => match state.sink.as_ref() {
            Some(sink) => match sink.materialized_query(&request) {
                Ok(events) => (SinkWorkerResponse::Events(events), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("worker not initialized".into()),
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
                    None => (
                        SinkWorkerResponse::Error("worker not initialized".into()),
                        false,
                    ),
                },
            },
            None => (
                SinkWorkerResponse::Error("worker not initialized".into()),
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
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::OnControlFrame { envelopes } => match state.sink.as_ref() {
            Some(sink) => match block_on_runtime(runtime, sink.on_control_frame(&envelopes)) {
                Ok(_) => (SinkWorkerResponse::Ack, false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
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
    run_worker_sidecar_server::<SinkWorkerRpc, _, SinkWorkerInitConfig>(
        control_socket_path,
        data_socket_path,
        SinkWorkerSession { state },
    )
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
                "worker runtime not initialized for runtime control frames".into(),
            ));
        };
        block_on_runtime(context.runtime_handle(), sink.on_control_frame(envelopes))
    }
}

#[async_trait::async_trait]
impl TypedWorkerBootstrapSession<SinkWorkerInitConfig> for SinkWorkerSession {
    async fn on_init(
        &mut self,
        node_id: NodeId,
        payload: SinkWorkerInitConfig,
        context: &WorkerSessionContext,
    ) -> capanix_app_sdk::Result<()> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| CnxError::Internal("sink worker state lock poisoned".into()))?;
        bootstrap_init_sink_runtime(node_id, payload, &mut guard, context.state_boundary())
    }

    async fn on_start(&mut self, context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| CnxError::Internal("sink worker state lock poisoned".into()))?;
        bootstrap_start_sink_runtime(&mut guard, context.runtime_handle(), context.io_boundary())
    }

    async fn on_ping(&mut self, _context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        let guard = self
            .state
            .lock()
            .map_err(|_| CnxError::Internal("sink worker state lock poisoned".into()))?;
        if guard.sink.is_some() {
            Ok(())
        } else {
            Err(bootstrap_not_ready())
        }
    }

    async fn on_close(&mut self, context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| CnxError::Internal("sink worker state lock poisoned".into()))?;
        block_on_runtime(
            context.runtime_handle(),
            bootstrap_stop_sink_runtime(&mut guard),
        );
        Ok(())
    }
}
