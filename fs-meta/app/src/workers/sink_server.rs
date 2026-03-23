use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use capanix_app_sdk::runtime::{ControlEnvelope, NodeId};
use capanix_app_sdk::{CnxError, Event};
use capanix_runtime_entry_sdk::advanced::boundary::{ChannelIoSubset, StateBoundary};
use capanix_runtime_entry_sdk::worker_runtime::{
    TypedWorkerBootstrapSession, TypedWorkerSession, WorkerLoopControl, WorkerSessionContext,
    run_worker_sidecar_server,
};
use tokio::sync::Mutex;

use crate::sink::SinkFileMeta;
use crate::source::config::SourceConfig;
use crate::workers::sink::SinkWorkerRpc;
use crate::workers::sink_ipc::{
    SinkWorkerInitConfig, SinkWorkerRequest, SinkWorkerResponse, recv_opts,
};

struct SinkWorkerState {
    sink: Option<Arc<SinkFileMeta>>,
    node_id: Option<NodeId>,
    pending_init: Option<(NodeId, SinkWorkerInitConfig)>,
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

async fn stop_sink_runtime(state: &mut SinkWorkerState) {
    bootstrap_stop_sink_runtime(state).await;
    state.pending_init = None;
}

async fn bootstrap_init_sink_runtime(
    node_id: NodeId,
    config: SinkWorkerInitConfig,
    state: &mut SinkWorkerState,
) -> Result<(), CnxError> {
    eprintln!(
        "fs_meta_sink_worker_server: bootstrap_init begin node={} roots={} grants={}",
        node_id.0,
        config.roots.len(),
        config.host_object_grants.len()
    );
    let _ = stop_sink_runtime(state).await;
    state.pending_init = Some((node_id.clone(), config));
    state.node_id = Some(node_id);
    state.endpoints_started = false;
    state.send_tx = None;
    eprintln!("fs_meta_sink_worker_server: bootstrap_init ok");
    Ok(())
}

fn bootstrap_start_sink_runtime(
    state: &mut SinkWorkerState,
    io_boundary: Arc<dyn ChannelIoSubset>,
    state_boundary: Arc<dyn StateBoundary>,
) -> Result<(), CnxError> {
    match state.sink.as_ref() {
        Some(_) if state.endpoints_started => Ok(()),
        Some(_) | None => {
            if state.sink.is_none() {
                let Some((node_id, config)) = state.pending_init.clone() else {
                    return Err(bootstrap_not_ready());
                };
                let mut source_cfg = SourceConfig::default();
                source_cfg.roots = config.roots;
                source_cfg.host_object_grants = config.host_object_grants;
                source_cfg.sink_tombstone_ttl =
                    Duration::from_millis(config.sink_tombstone_ttl_ms.max(1));
                source_cfg.sink_tombstone_tolerance_us = config.sink_tombstone_tolerance_us;
                let inner = SinkFileMeta::with_boundaries_and_state_deferred_authority(
                    node_id.clone(),
                    None,
                    state_boundary,
                    source_cfg,
                )?;
                state.sink = Some(Arc::new(inner));
            }
            let Some(sink) = state.sink.as_ref() else {
                return Err(bootstrap_not_ready());
            };
            let Some(node_id) = state.node_id.clone() else {
                return Err(CnxError::Internal(
                    "sink worker missing node_id during start".into(),
                ));
            };
            eprintln!(
                "fs_meta_sink_worker_server: bootstrap_start begin node={} endpoints_started={}",
                node_id.0, state.endpoints_started
            );
            sink.start_runtime_endpoints(io_boundary, node_id)?;
            eprintln!("fs_meta_sink_worker_server: bootstrap_start endpoints ok");
            if state.send_tx.is_none() {
                let (send_tx, mut send_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<Event>>();
                let sink_for_send = sink.clone();
                tokio::spawn(async move {
                    while let Some(events) = send_rx.recv().await {
                        if let Err(err) = sink_for_send.send(&events).await {
                            eprintln!("fs_meta_sink_worker: async Send apply failed: {err}");
                        }
                    }
                });
                state.send_tx = Some(send_tx);
            }
            state.endpoints_started = true;
            sink.enable_stream_receive();
            eprintln!("fs_meta_sink_worker_server: bootstrap_start ok");
            Ok(())
        }
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

async fn process_worker_request(
    request: SinkWorkerRequest,
    state: &mut SinkWorkerState,
) -> (SinkWorkerResponse, bool) {
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
                    Some(sink) => match sink.send(&events).await {
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
            Some(sink) => match sink.recv(recv_opts(timeout_ms, limit)).await {
                Ok(events) => (SinkWorkerResponse::Events(events), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::OnControlFrame { envelopes } => match state.sink.as_ref() {
            Some(sink) => match sink.on_control_frame(&envelopes).await {
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
        pending_init: None,
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
        _context: &WorkerSessionContext,
    ) -> capanix_app_sdk::Result<WorkerLoopControl<SinkWorkerResponse>> {
        let mut guard = self.state.lock().await;
        let (response, stop) = process_worker_request(request, &mut guard).await;
        Ok(if stop {
            WorkerLoopControl::Stop(response)
        } else {
            WorkerLoopControl::Continue(response)
        })
    }

    async fn on_runtime_control(
        &mut self,
        envelopes: &[ControlEnvelope],
        _context: &WorkerSessionContext,
    ) -> capanix_app_sdk::Result<()> {
        let guard = self.state.lock().await;
        let Some(sink) = guard.sink.as_ref() else {
            return Err(CnxError::NotReady(
                "worker runtime not initialized for runtime control frames".into(),
            ));
        };
        sink.on_control_frame(envelopes).await
    }
}

#[async_trait::async_trait]
impl TypedWorkerBootstrapSession<SinkWorkerInitConfig> for SinkWorkerSession {
    async fn on_init(
        &mut self,
        node_id: NodeId,
        payload: SinkWorkerInitConfig,
        _context: &WorkerSessionContext,
    ) -> capanix_app_sdk::Result<()> {
        eprintln!(
            "fs_meta_sink_worker_server: on_init node={} roots={} grants={}",
            node_id.0,
            payload.roots.len(),
            payload.host_object_grants.len()
        );
        let mut guard = self.state.lock().await;
        bootstrap_init_sink_runtime(node_id, payload, &mut guard).await
    }

    async fn on_start(&mut self, _context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        eprintln!("fs_meta_sink_worker_server: on_start begin");
        let mut guard = self.state.lock().await;
        let result = bootstrap_start_sink_runtime(
            &mut guard,
            _context.io_boundary(),
            _context.state_boundary(),
        );
        eprintln!(
            "fs_meta_sink_worker_server: on_start done ok={}",
            result.is_ok()
        );
        result
    }

    async fn on_ping(&mut self, _context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        eprintln!("fs_meta_sink_worker_server: on_ping begin");
        let guard = self.state.lock().await;
        if guard.sink.is_some() {
            eprintln!("fs_meta_sink_worker_server: on_ping ok");
            Ok(())
        } else {
            eprintln!("fs_meta_sink_worker_server: on_ping not_ready");
            Err(bootstrap_not_ready())
        }
    }

    async fn on_close(&mut self, _context: &WorkerSessionContext) -> capanix_app_sdk::Result<()> {
        let mut guard = self.state.lock().await;
        stop_sink_runtime(&mut guard).await;
        Ok(())
    }
}
