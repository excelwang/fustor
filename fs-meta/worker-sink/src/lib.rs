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

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_micros() as u64,
        Err(_) => 0,
    }
}

fn process_worker_request(
    request: SinkWorkerRequest,
    sink: &mut Option<SinkFileMeta>,
    _channel_boundary: Arc<dyn ChannelIoSubset>,
    runtime: &tokio::runtime::Runtime,
    state_boundary: Arc<dyn StateBoundary>,
) -> (SinkWorkerResponse, bool) {
    match request {
        SinkWorkerRequest::Init { node_id, config } => {
            let mut source_cfg = SourceConfig::default();
            source_cfg.roots = config.roots;
            source_cfg.host_object_grants = config.host_object_grants;
            source_cfg.sink_tombstone_ttl =
                Duration::from_millis(config.sink_tombstone_ttl_ms.max(1));
            source_cfg.sink_tombstone_tolerance_us = config.sink_tombstone_tolerance_us;
            source_cfg.sink_execution_mode = SinkExecutionMode::InProcess;
            match SinkFileMeta::with_boundaries_and_state(
                NodeId(node_id),
                None,
                state_boundary.clone(),
                source_cfg,
            ) {
                Ok(inner) => {
                    *sink = Some(inner);
                    (SinkWorkerResponse::Ack, false)
                }
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            }
        }
        SinkWorkerRequest::UpdateLogicalRoots {
            roots,
            host_object_grants,
        } => match sink.as_ref() {
            Some(sink) => match sink.update_logical_roots(roots, &host_object_grants) {
                Ok(_) => (SinkWorkerResponse::Ack, false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::LogicalRootsSnapshot => match sink.as_ref() {
            Some(sink) => match sink.logical_roots_snapshot() {
                Ok(roots) => (SinkWorkerResponse::LogicalRoots(roots), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::ScheduledGroupIds => match sink.as_ref() {
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
        SinkWorkerRequest::Health => match sink.as_ref() {
            Some(sink) => match sink.health() {
                Ok(health) => (SinkWorkerResponse::Health(health), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::StatusSnapshot => match sink.as_ref() {
            Some(sink) => match sink.status_snapshot() {
                Ok(snapshot) => (SinkWorkerResponse::StatusSnapshot(snapshot), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::VisibilityLagSamplesSince { since_us } => match sink.as_ref() {
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
        SinkWorkerRequest::MaterializedQuery { request } => match sink.as_ref() {
            Some(sink) => match sink.materialized_query(&request) {
                Ok(events) => (SinkWorkerResponse::Events(events), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Send { events } => match sink.as_ref() {
            Some(sink) => match runtime.block_on(sink.send(&events)) {
                Ok(_) => (SinkWorkerResponse::Ack, false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Recv { timeout_ms, limit } => match sink.as_ref() {
            Some(sink) => match runtime.block_on(sink.recv(recv_opts(timeout_ms, limit))) {
                Ok(events) => (SinkWorkerResponse::Events(events), false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::OnControlFrame { envelopes } => match sink.as_ref() {
            Some(sink) => match runtime.block_on(sink.on_control_frame(&envelopes)) {
                Ok(_) => (SinkWorkerResponse::Ack, false),
                Err(err) => (SinkWorkerResponse::Error(err.to_string()), false),
            },
            None => (
                SinkWorkerResponse::Error("sink worker not initialized".into()),
                false,
            ),
        },
        SinkWorkerRequest::Close => {
            if let Some(sink) = sink.as_ref() {
                let _ = runtime.block_on(sink.close());
            }
            (SinkWorkerResponse::Ack, true)
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

    let mut sink = None::<SinkFileMeta>;
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
                Ok(request) => process_worker_request(
                    request,
                    &mut sink,
                    io_boundary.clone(),
                    runtime,
                    state_boundary.clone(),
                ),
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
