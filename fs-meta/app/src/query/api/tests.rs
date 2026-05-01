use super::*;
use crate::runtime::endpoint::ManagedEndpointTask;
use crate::runtime::routes::{
    METHOD_SINK_QUERY, METHOD_SINK_QUERY_PROXY, METHOD_SOURCE_FIND, ROUTE_TOKEN_FS_META_INTERNAL,
    default_route_bindings, sink_query_request_route_for, source_find_request_route_for,
};
use crate::sink::SinkFileMeta;
use crate::source::FSMetaSource;
use crate::source::config::{GrantedMountRoot, RootSpec, SourceConfig};
use crate::workers::sink::{SinkFacade, SinkWorkerClientHandle};
use crate::workers::source::SourceFacade;
use crate::{ControlEvent, EpochType, FileMetaRecord};
use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::http::Request;
use bytes::Bytes;
use capanix_app_sdk::runtime::{
    EventMetadata, LogLevel, NodeId, RuntimeWorkerBinding, RuntimeWorkerLauncherKind,
    in_memory_state_boundary,
};
use capanix_app_sdk::worker::WorkerMode;
use capanix_host_fs_types::UnixStat;
use capanix_runtime_entry_sdk::advanced::boundary::{
    BoundaryContext, ChannelBoundary, ChannelIoSubset, ChannelKey, ChannelRecvRequest,
    ChannelSendRequest, StateBoundary,
};
use capanix_runtime_entry_sdk::worker_runtime::RuntimeWorkerClientFactory;
use futures_util::StreamExt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use tempfile::{TempDir, tempdir};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;
use tower::util::ServiceExt;

#[test]
fn tree_pit_session_machine_owns_orchestration_only() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/query/api.rs"),
    )
    .expect("read query/api.rs");

    assert!(
        source.contains("struct TreePitGroupMachine<'a, 'b>"),
        "expected TreePitGroupMachine owner to exist"
    );
    assert!(
        source.contains("TreePitGroupMachine {\n                input: &self.input,"),
        "expected TreePitSessionMachine to delegate ranked-group execution to TreePitGroupMachine"
    );
    assert!(
        !source.contains("self.run_ranked_group(rank_index, rank.group_key, &mut state)"),
        "TreePitSessionMachine must not keep session-local single-group execution"
    );
}

#[test]
fn force_find_pit_session_machine_owns_orchestration_only() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/query/api.rs"),
    )
    .expect("read query/api.rs");

    assert!(
        source.contains("struct ForceFindPitGroupMachine<'a, 'b>"),
        "expected ForceFindPitGroupMachine owner to exist"
    );
    assert!(
        source.contains("ForceFindPitGroupMachine {\n                    input: &self.input,"),
        "expected ForceFindPitSessionMachine to delegate single-group execution to ForceFindPitGroupMachine"
    );
    assert!(
        !source.contains("groups.push(self.run_ranked_group(rank).await?);"),
        "ForceFindPitSessionMachine must not keep session-local single-group execution"
    );
}

#[test]
fn stats_query_machine_owns_orchestration_only() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/query/api.rs"),
    )
    .expect("read query/api.rs");

    assert!(
        source.contains("struct StatsGroupMachine<'a, 'b>"),
        "expected StatsGroupMachine owner to exist"
    );
    assert!(
        source.contains("StatsGroupMachine {\n                    query: self,"),
        "expected StatsQueryMachine to delegate single-group execution to StatsGroupMachine"
    );
    assert!(
        !source.contains("self.collect_group(group_id).await?"),
        "StatsQueryMachine must not keep session-local single-group execution"
    );
}

#[test]
fn query_api_readiness_and_force_find_fallbacks_use_typed_cached_helpers() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/query/api.rs"),
    )
    .expect("read query/api.rs");

    for typed_surface in [
        "source\n                .cached_logical_roots_snapshot_with_failure()",
        "local_sink.cached_status_snapshot_with_failure()",
        ".status_snapshot_nonblocking_for_readiness_fan_in()",
        ".cached_logical_roots_snapshot_with_failure()\n            .map_err(SourceFailure::into_error)?",
    ] {
        assert!(
            source.contains(typed_surface),
            "query/api hard cut regressed; readiness and force-find fallbacks should stay on typed cached helpers: {typed_surface}"
        );
    }

    for legacy_surface in [
        "source.cached_logical_roots_snapshot().map(|roots| {",
        "if let Ok(local_sink_status) = local_sink.cached_status_snapshot() {",
        ".cached_logical_roots_snapshot()?",
    ] {
        assert!(
            !source.contains(legacy_surface),
            "query/api hard cut regressed; readiness and force-find fallbacks bounced back through raw cached helpers: {legacy_surface}"
        );
    }
}

#[derive(Clone, Copy)]
enum ForceFindFixtureScenario {
    Standard,
    FileAgeNoFiles,
}

struct ForceFindFixture {
    _tempdir: TempDir,
    app: Router,
}

#[derive(Default)]
struct TestSourceStatusResponder {
    payload: Option<Vec<u8>>,
    send_count: std::sync::atomic::AtomicUsize,
    delivered_send_count: std::sync::atomic::AtomicUsize,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
}

#[derive(Default)]
struct LoopbackRouteBoundary {
    channels: AsyncMutex<HashMap<String, Vec<Event>>>,
    closed: std::sync::Mutex<std::collections::HashSet<String>>,
    source_status: TestSourceStatusResponder,
    changed: Notify,
}

#[derive(Default)]
struct ReusableObservedRouteBoundary {
    channels: AsyncMutex<HashMap<String, Vec<Event>>>,
    closed: std::sync::Mutex<std::collections::HashSet<String>>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    changed: Notify,
}

struct MaterializedRouteRaceBoundary {
    state: std::sync::Mutex<MaterializedRouteRaceState>,
    proxy_call_channel: String,
    internal_call_channel: String,
    selected_group: String,
    path: Vec<u8>,
}

struct SourceStatusTimeoutSinkStatusOkBoundary {
    sink_reply_channel: String,
    sink_status_payload: Vec<u8>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    sink_reply_sent: std::sync::atomic::AtomicBool,
}

struct SourceStatusOkSinkStatusTimeoutBoundary {
    source_reply_channel: String,
    source_status_payload: Vec<u8>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    source_reply_sent: std::sync::atomic::AtomicBool,
}

struct SourceStatusSendMissingRouteStateSinkStatusOkBoundary {
    source_request_channel: String,
    sink_reply_channel: String,
    sink_status_payload: Vec<u8>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    source_send_attempts: std::sync::atomic::AtomicUsize,
    sink_reply_sent: std::sync::atomic::AtomicBool,
}

struct SourceStatusRetryThenReplyBoundary {
    source_reply_channel: String,
    sink_reply_channel: String,
    source_status_payload: Vec<u8>,
    sink_status_payload: Vec<u8>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    source_reply_sent: std::sync::atomic::AtomicBool,
    sink_reply_sent: std::sync::atomic::AtomicBool,
    first_retryable_gap_at: std::sync::Mutex<Option<std::time::Instant>>,
    second_send_at: std::sync::Mutex<Option<std::time::Instant>>,
}

struct SourceStatusMissingRouteStateThenReplyBoundary {
    source_reply_channel: String,
    sink_reply_channel: String,
    source_status_payload: Vec<u8>,
    sink_status_payload: Vec<u8>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    source_reply_attempts: std::sync::atomic::AtomicUsize,
    sink_reply_sent: std::sync::atomic::AtomicBool,
    first_retryable_gap_at: std::sync::Mutex<Option<std::time::Instant>>,
    second_send_at: std::sync::Mutex<Option<std::time::Instant>>,
    second_send_notifier: tokio::sync::Notify,
}

struct SourceStatusOkSinkStatusExplicitEmptyThenReadyBoundary {
    source_reply_channel: String,
    sink_reply_channel: String,
    source_status_payload: Vec<u8>,
    sink_status_payloads: std::sync::Mutex<std::collections::VecDeque<Vec<u8>>>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    source_reply_sent: std::sync::atomic::AtomicBool,
}

struct SinkStatusInternalRetryThenReplyBoundary {
    sink_reply_channel: String,
    sink_status_payload: Vec<u8>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    sink_reply_sent: std::sync::atomic::AtomicBool,
    first_retryable_gap_at: std::sync::Mutex<Option<std::time::Instant>>,
    second_send_at: std::sync::Mutex<Option<std::time::Instant>>,
}

struct SinkStatusPeerTransportRetryThenReplyBoundary {
    sink_reply_channel: String,
    sink_status_payload: Vec<u8>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    sink_reply_sent: std::sync::atomic::AtomicBool,
}

struct ForceFindProtocolRetryThenReplyBoundary {
    source_reply_channel: String,
    payload: Vec<u8>,
    source_status: TestSourceStatusResponder,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    source_reply_sent: std::sync::atomic::AtomicBool,
}

struct ForceFindGroupMissingThenReplyBoundary {
    source_reply_channel: String,
    payload: Vec<u8>,
    source_status: TestSourceStatusResponder,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    source_reply_sent: std::sync::atomic::AtomicBool,
}

struct ForceFindHostUnavailableThenNextRunnerBoundary {
    node_a_reply_channel: String,
    node_b_reply_channel: String,
    generic_request_channel: String,
    payload: Vec<u8>,
    source_status: TestSourceStatusResponder,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    node_b_reply_sent: std::sync::atomic::AtomicBool,
}

struct ForceFindSingleCandidateGroupMissingThenFallbackBoundary {
    source_reply_channel: String,
    payload: Vec<u8>,
    source_status: TestSourceStatusResponder,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    first_retryable_gap_at: std::sync::Mutex<Option<std::time::Instant>>,
    second_send_at: std::sync::Mutex<Option<std::time::Instant>>,
    source_reply_sent: std::sync::atomic::AtomicBool,
}

struct ForceFindSelectedGroupFallbackCollectBoundary {
    selected_reply_channel: String,
    generic_reply_channel: String,
    selected_group_payload: Vec<u8>,
    source_status: TestSourceStatusResponder,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    generic_reply_count: std::sync::atomic::AtomicUsize,
}

struct ForceFindRunnerBindingStatusBoundary {
    source_reply_channel: String,
    source_status_payload: Vec<u8>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    delivered_send_count: std::sync::atomic::AtomicUsize,
}

struct ForceFindDelayedRunnerBindingBoundary {
    source_status_reply_channel: String,
    source_status_payloads: std::sync::Mutex<std::collections::VecDeque<Vec<u8>>>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    delivered_source_status_send_count: std::sync::atomic::AtomicUsize,
    source_find_reply_sent: std::sync::atomic::AtomicBool,
}

struct MaterializedRouteAccessDeniedThenProxyBoundary {
    channels: AsyncMutex<HashMap<String, Vec<Event>>>,
    closed: std::sync::Mutex<std::collections::HashSet<String>>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    changed: Notify,
    owner_reply_channel: String,
    owner_reply_sent: std::sync::atomic::AtomicBool,
    sink_status_reply_channel: String,
    sink_status_payload: Vec<u8>,
    sink_reply_sent: std::sync::atomic::AtomicBool,
}

struct MaterializedRouteMissingRouteStateThenProxyBoundary {
    channels: AsyncMutex<HashMap<String, Vec<Event>>>,
    closed: std::sync::Mutex<std::collections::HashSet<String>>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    changed: Notify,
    owner_reply_channel: String,
    owner_reply_sent: std::sync::atomic::AtomicBool,
    sink_status_reply_channel: String,
    sink_status_payload: Vec<u8>,
    sink_reply_sent: std::sync::atomic::AtomicBool,
}

struct MaterializedProxyMissingRouteStateThenReplyBoundary {
    proxy_reply_channel: String,
    payload: Vec<u8>,
    send_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    recv_batches_by_channel: std::sync::Mutex<HashMap<String, usize>>,
    correlations_by_channel: std::sync::Mutex<HashMap<String, u64>>,
    proxy_reply_sent: std::sync::atomic::AtomicBool,
}

struct MaterializedSelectedGroupTreeStallBoundary {
    owner_request_channel: String,
    source_status_request_channel: String,
    source_status_payload: Vec<u8>,
    sink_status_request_channel: String,
    sink_status_payload: Vec<u8>,
    successful_tree_group: Option<String>,
    delayed_successful_tree_group: Option<String>,
    delayed_success_duration: Duration,
    owner_request_batches: std::sync::atomic::AtomicUsize,
    queued_requests: AsyncMutex<HashMap<String, Vec<Event>>>,
    changed: Notify,
}

#[derive(Default)]
struct MaterializedRouteRaceState {
    sent_call_channel: Option<String>,
    sent_correlation: Option<u64>,
    recv_counts_by_channel: HashMap<String, usize>,
}

#[async_trait]
impl ChannelIoSubset for LoopbackRouteBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        if self.source_status.on_send(&request) {
            return Ok(());
        }
        {
            let mut channels = self.channels.lock().await;
            channels
                .entry(request.channel_key.0)
                .or_default()
                .extend(request.events);
        }
        self.changed.notify_waiters();
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        if let Some(result) = self.source_status.try_recv(&request) {
            return result;
        }
        let deadline = request
            .timeout_ms
            .map(Duration::from_millis)
            .map(|timeout| tokio::time::Instant::now() + timeout);
        loop {
            {
                let mut channels = self.channels.lock().await;
                if let Some(events) = channels.remove(&request.channel_key.0)
                    && !events.is_empty()
                {
                    return Ok(events);
                }
            }
            {
                let closed = self.closed.lock().expect("loopback closed lock");
                if closed.contains(&request.channel_key.0) {
                    return Err(CnxError::ChannelClosed);
                }
            }
            let notified = self.changed.notified();
            if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, notified).await {
                    Ok(()) => {}
                    Err(_) => return Err(CnxError::Timeout),
                }
            } else {
                notified.await;
            }
        }
    }

    fn channel_close(
        &self,
        _ctx: BoundaryContext,
        channel: ChannelKey,
    ) -> capanix_app_sdk::Result<()> {
        self.closed
            .lock()
            .expect("loopback closed lock")
            .insert(channel.0);
        self.changed.notify_waiters();
        Ok(())
    }
}

impl MaterializedRouteRaceBoundary {
    fn new(
        proxy_call_channel: String,
        internal_call_channel: String,
        selected_group: impl Into<String>,
        path: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            state: std::sync::Mutex::new(MaterializedRouteRaceState::default()),
            proxy_call_channel,
            internal_call_channel,
            selected_group: selected_group.into(),
            path: path.into(),
        }
    }

    fn sent_call_channel(&self) -> Option<String> {
        self.state
            .lock()
            .expect("route race state lock")
            .sent_call_channel
            .clone()
    }
}

impl TestSourceStatusResponder {
    fn with_payload(payload: Vec<u8>) -> Self {
        Self {
            payload: Some(payload),
            send_count: std::sync::atomic::AtomicUsize::new(0),
            delivered_send_count: std::sync::atomic::AtomicUsize::new(0),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn request_channel() -> String {
        default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route")
            .0
    }

    fn reply_channel() -> String {
        format!("{}:reply", Self::request_channel())
    }

    fn on_send(&self, request: &ChannelSendRequest) -> bool {
        if self.payload.is_none() || request.channel_key.0 != Self::request_channel() {
            return false;
        }
        if let Some(correlation) = request
            .events
            .first()
            .and_then(|event| event.metadata().correlation_id)
        {
            self.correlations_by_channel
                .lock()
                .expect("test source-status correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        self.send_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        true
    }

    fn try_recv(
        &self,
        request: &ChannelRecvRequest,
    ) -> Option<capanix_app_sdk::Result<Vec<Event>>> {
        let payload = self.payload.as_ref()?;
        if request.channel_key.0 != Self::reply_channel() {
            return None;
        }
        let send_count = self.send_count.load(std::sync::atomic::Ordering::SeqCst);
        let delivered = self
            .delivered_send_count
            .load(std::sync::atomic::Ordering::SeqCst);
        if send_count == 0 || delivered >= send_count {
            return Some(Err(CnxError::Timeout));
        }
        self.delivered_send_count
            .store(send_count, std::sync::atomic::Ordering::SeqCst);
        let request_channel = Self::request_channel();
        let correlation = self
            .correlations_by_channel
            .lock()
            .expect("test source-status correlations lock")
            .get(&request_channel)
            .copied()
            .unwrap_or(1);
        Some(Ok(vec![mk_event_with_correlation(
            "source-status",
            correlation,
            payload.clone(),
        )]))
    }
}

impl LoopbackRouteBoundary {
    fn with_source_status_payload(payload: Vec<u8>) -> Self {
        Self {
            source_status: TestSourceStatusResponder::with_payload(payload),
            ..Self::default()
        }
    }
}

impl ReusableObservedRouteBoundary {
    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("reusable route boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }

    fn recv_batch_count(&self, channel: &str) -> usize {
        *self
            .recv_batches_by_channel
            .lock()
            .expect("reusable route boundary recv batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

impl SourceStatusTimeoutSinkStatusOkBoundary {
    fn new(sink_status_payload: Vec<u8>) -> Self {
        let sink_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
            .expect("resolve sink-status route");
        Self {
            sink_reply_channel: format!("{}:reply", sink_route.0),
            sink_status_payload,
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            sink_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("source/sink selective boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }

    fn recv_batch_count(&self, channel: &str) -> usize {
        *self
            .recv_batches_by_channel
            .lock()
            .expect("source/sink selective boundary recv batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

impl SourceStatusOkSinkStatusTimeoutBoundary {
    fn new(source_status_payload: Vec<u8>) -> Self {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route");
        Self {
            source_reply_channel: format!("{}:reply", source_route.0),
            source_status_payload,
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            source_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("source ok sink timeout boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

impl SourceStatusSendMissingRouteStateSinkStatusOkBoundary {
    fn new(sink_status_payload: Vec<u8>) -> Self {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route");
        let sink_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
            .expect("resolve sink-status route");
        Self {
            source_request_channel: source_route.0,
            sink_reply_channel: format!("{}:reply", sink_route.0),
            sink_status_payload,
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            source_send_attempts: std::sync::atomic::AtomicUsize::new(0),
            sink_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("source send-missing-route-state boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

impl SourceStatusRetryThenReplyBoundary {
    fn new(source_status_payload: Vec<u8>, sink_status_payload: Vec<u8>) -> Self {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route");
        let sink_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
            .expect("resolve sink-status route");
        Self {
            source_reply_channel: format!("{}:reply", source_route.0),
            sink_reply_channel: format!("{}:reply", sink_route.0),
            source_status_payload,
            sink_status_payload,
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            source_reply_sent: std::sync::atomic::AtomicBool::new(false),
            sink_reply_sent: std::sync::atomic::AtomicBool::new(false),
            first_retryable_gap_at: std::sync::Mutex::new(None),
            second_send_at: std::sync::Mutex::new(None),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("source retry boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }

    fn retry_reissue_delay(&self) -> Option<Duration> {
        let first_gap_at = *self
            .first_retryable_gap_at
            .lock()
            .expect("source retry boundary first gap lock");
        let second_send_at = *self
            .second_send_at
            .lock()
            .expect("source retry boundary second send lock");
        match (first_gap_at, second_send_at) {
            (Some(first_gap_at), Some(second_send_at)) => {
                Some(second_send_at.saturating_duration_since(first_gap_at))
            }
            _ => None,
        }
    }
}

impl SourceStatusMissingRouteStateThenReplyBoundary {
    fn new(source_status_payload: Vec<u8>, sink_status_payload: Vec<u8>) -> Self {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route");
        let sink_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
            .expect("resolve sink-status route");
        Self {
            source_reply_channel: format!("{}:reply", source_route.0),
            sink_reply_channel: format!("{}:reply", sink_route.0),
            source_status_payload,
            sink_status_payload,
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            source_reply_attempts: std::sync::atomic::AtomicUsize::new(0),
            sink_reply_sent: std::sync::atomic::AtomicBool::new(false),
            first_retryable_gap_at: std::sync::Mutex::new(None),
            second_send_at: std::sync::Mutex::new(None),
            second_send_notifier: tokio::sync::Notify::new(),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("source missing-route-state boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }

    fn retry_reissue_delay(&self) -> Option<Duration> {
        let first_gap_at = *self
            .first_retryable_gap_at
            .lock()
            .expect("source missing-route-state boundary first gap lock");
        let second_send_at = *self
            .second_send_at
            .lock()
            .expect("source missing-route-state boundary second send lock");
        match (first_gap_at, second_send_at) {
            (Some(first_gap_at), Some(second_send_at)) => {
                Some(second_send_at.saturating_duration_since(first_gap_at))
            }
            _ => None,
        }
    }

    async fn wait_for_second_send(&self) {
        if self
            .second_send_at
            .lock()
            .expect("source missing-route-state boundary second send lock")
            .is_some()
        {
            return;
        }
        self.second_send_notifier.notified().await;
    }
}

impl ForceFindRunnerBindingStatusBoundary {
    fn new(source_status_payload: Vec<u8>) -> Self {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route");
        Self {
            source_reply_channel: format!("{}:reply", source_route.0),
            source_status_payload,
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            delivered_send_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("force-find runner binding boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

impl ForceFindDelayedRunnerBindingBoundary {
    fn new(source_status_payloads: Vec<Vec<u8>>) -> Self {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route");
        Self {
            source_status_reply_channel: format!("{}:reply", source_route.0),
            source_status_payloads: std::sync::Mutex::new(
                source_status_payloads.into_iter().collect(),
            ),
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            delivered_source_status_send_count: std::sync::atomic::AtomicUsize::new(0),
            source_find_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn total_send_batch_count(&self) -> usize {
        self.send_batches_by_channel
            .lock()
            .expect("delayed runner binding boundary send batches lock")
            .values()
            .copied()
            .sum()
    }
}

impl SourceStatusOkSinkStatusExplicitEmptyThenReadyBoundary {
    fn new(source_status_payload: Vec<u8>, sink_status_payloads: Vec<Vec<u8>>) -> Self {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route");
        let sink_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
            .expect("resolve sink-status route");
        Self {
            source_reply_channel: format!("{}:reply", source_route.0),
            sink_reply_channel: format!("{}:reply", sink_route.0),
            source_status_payload,
            sink_status_payloads: std::sync::Mutex::new(sink_status_payloads.into_iter().collect()),
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            source_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("source/sink explicit-empty-then-ready boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }

    fn recv_batch_count(&self, channel: &str) -> usize {
        *self
            .recv_batches_by_channel
            .lock()
            .expect("source/sink explicit-empty-then-ready boundary recv batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

impl SinkStatusInternalRetryThenReplyBoundary {
    fn new(sink_status_payload: Vec<u8>) -> Self {
        let sink_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
            .expect("resolve sink-status route");
        Self {
            sink_reply_channel: format!("{}:reply", sink_route.0),
            sink_status_payload,
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            sink_reply_sent: std::sync::atomic::AtomicBool::new(false),
            first_retryable_gap_at: std::sync::Mutex::new(None),
            second_send_at: std::sync::Mutex::new(None),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("sink retry boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }

    fn retry_reissue_delay(&self) -> Option<Duration> {
        let first_gap_at = *self
            .first_retryable_gap_at
            .lock()
            .expect("sink retry boundary first gap lock");
        let second_send_at = *self
            .second_send_at
            .lock()
            .expect("sink retry boundary second send lock");
        match (first_gap_at, second_send_at) {
            (Some(first_gap_at), Some(second_send_at)) => {
                Some(second_send_at.saturating_duration_since(first_gap_at))
            }
            _ => None,
        }
    }
}

impl SinkStatusPeerTransportRetryThenReplyBoundary {
    fn new(sink_status_payload: Vec<u8>) -> Self {
        let sink_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
            .expect("resolve sink-status route");
        Self {
            sink_reply_channel: format!("{}:reply", sink_route.0),
            sink_status_payload,
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            sink_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("sink peer transport retry boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

impl ForceFindProtocolRetryThenReplyBoundary {
    fn new(payload: Vec<u8>, source_status_payload: Vec<u8>) -> Self {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND)
            .expect("resolve source-find route");
        Self {
            source_reply_channel: format!("{}:reply", source_route.0),
            payload,
            source_status: TestSourceStatusResponder::with_payload(source_status_payload),
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            source_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("force-find retry boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }

    fn recv_batch_count(&self, channel: &str) -> usize {
        *self
            .recv_batches_by_channel
            .lock()
            .expect("force-find retry boundary recv batches lock")
            .get(channel)
            .unwrap_or(&0)
    }

    fn total_send_batch_count(&self) -> usize {
        self.send_batches_by_channel
            .lock()
            .expect("force-find retry boundary send batches lock")
            .values()
            .copied()
            .sum()
    }

    fn total_recv_batch_count(&self) -> usize {
        self.recv_batches_by_channel
            .lock()
            .expect("force-find retry boundary recv batches lock")
            .values()
            .copied()
            .sum()
    }
}

impl ForceFindGroupMissingThenReplyBoundary {
    fn new(payload: Vec<u8>, source_status_payload: Vec<u8>) -> Self {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND)
            .expect("resolve source-find route");
        Self {
            source_reply_channel: format!("{}:reply", source_route.0),
            payload,
            source_status: TestSourceStatusResponder::with_payload(source_status_payload),
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            source_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("force-find group-missing boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }

    fn recv_batch_count(&self, channel: &str) -> usize {
        *self
            .recv_batches_by_channel
            .lock()
            .expect("force-find group-missing boundary recv batches lock")
            .get(channel)
            .unwrap_or(&0)
    }

    fn total_send_batch_count(&self) -> usize {
        self.send_batches_by_channel
            .lock()
            .expect("force-find group-missing boundary send batches lock")
            .values()
            .copied()
            .sum()
    }

    fn total_recv_batch_count(&self) -> usize {
        self.recv_batches_by_channel
            .lock()
            .expect("force-find group-missing boundary recv batches lock")
            .values()
            .copied()
            .sum()
    }
}

impl ForceFindHostUnavailableThenNextRunnerBoundary {
    fn new(payload: Vec<u8>, source_status_payload: Vec<u8>) -> Self {
        let generic_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND)
            .expect("resolve generic source-find route");
        Self {
            node_a_reply_channel: format!("{}:reply", source_find_request_route_for("node-a").0),
            node_b_reply_channel: format!("{}:reply", source_find_request_route_for("node-b").0),
            generic_request_channel: generic_route.0,
            payload,
            source_status: TestSourceStatusResponder::with_payload(source_status_payload),
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            node_b_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("force-find host-unavailable boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

impl ForceFindSingleCandidateGroupMissingThenFallbackBoundary {
    fn new(payload: Vec<u8>, source_status_payload: Vec<u8>) -> Self {
        let source_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND)
            .expect("resolve source-find route");
        Self {
            source_reply_channel: format!("{}:reply", source_route.0),
            payload,
            source_status: TestSourceStatusResponder::with_payload(source_status_payload),
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            first_retryable_gap_at: std::sync::Mutex::new(None),
            second_send_at: std::sync::Mutex::new(None),
            source_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("force-find single-candidate boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }

    fn retry_reissue_delay(&self) -> Option<Duration> {
        let first_gap_at = *self
            .first_retryable_gap_at
            .lock()
            .expect("force-find single-candidate boundary first gap lock");
        let second_send_at = *self
            .second_send_at
            .lock()
            .expect("force-find single-candidate boundary second send lock");
        match (first_gap_at, second_send_at) {
            (Some(first_gap_at), Some(second_send_at)) => {
                Some(second_send_at.saturating_duration_since(first_gap_at))
            }
            _ => None,
        }
    }

    fn total_send_batch_count(&self) -> usize {
        self.send_batches_by_channel
            .lock()
            .expect("force-find single-candidate boundary send batches lock")
            .values()
            .copied()
            .sum()
    }
}

impl ForceFindSelectedGroupFallbackCollectBoundary {
    fn new(selected_group_payload: Vec<u8>, source_status_payload: Vec<u8>) -> Self {
        let generic_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND)
            .expect("resolve generic source-find route");
        Self {
            selected_reply_channel: format!("{}:reply", source_find_request_route_for("node-a").0),
            generic_reply_channel: format!("{}:reply", generic_route.0),
            selected_group_payload,
            source_status: TestSourceStatusResponder::with_payload(source_status_payload),
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            generic_reply_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn total_recv_batch_count(&self) -> usize {
        self.recv_batches_by_channel
            .lock()
            .expect("force-find selected-group fallback collect recv batches lock")
            .values()
            .copied()
            .sum()
    }
}

impl MaterializedRouteAccessDeniedThenProxyBoundary {
    fn new(owner_request_channel: String, sink_status_payload: Vec<u8>) -> Self {
        let sink_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
            .expect("resolve sink-status route");
        Self {
            channels: AsyncMutex::new(HashMap::new()),
            closed: std::sync::Mutex::new(std::collections::HashSet::new()),
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            changed: Notify::new(),
            owner_reply_channel: format!("{owner_request_channel}:reply"),
            owner_reply_sent: std::sync::atomic::AtomicBool::new(false),
            sink_status_reply_channel: format!("{}:reply", sink_route.0),
            sink_status_payload,
            sink_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("materialized access-denied boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

impl MaterializedRouteMissingRouteStateThenProxyBoundary {
    fn new(owner_request_channel: String, sink_status_payload: Vec<u8>) -> Self {
        let sink_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
            .expect("resolve sink-status route");
        Self {
            channels: AsyncMutex::new(HashMap::new()),
            closed: std::sync::Mutex::new(std::collections::HashSet::new()),
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            changed: Notify::new(),
            owner_reply_channel: format!("{owner_request_channel}:reply"),
            owner_reply_sent: std::sync::atomic::AtomicBool::new(false),
            sink_status_reply_channel: format!("{}:reply", sink_route.0),
            sink_status_payload,
            sink_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("materialized missing-route-state boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

impl MaterializedProxyMissingRouteStateThenReplyBoundary {
    fn new(payload: Vec<u8>) -> Self {
        let proxy_route = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
            .expect("resolve sink-query-proxy route");
        Self {
            proxy_reply_channel: format!("{}:reply", proxy_route.0),
            payload,
            send_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            recv_batches_by_channel: std::sync::Mutex::new(HashMap::new()),
            correlations_by_channel: std::sync::Mutex::new(HashMap::new()),
            proxy_reply_sent: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("materialized proxy missing-route-state boundary send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

impl MaterializedSelectedGroupTreeStallBoundary {
    fn source_status_payload_for_sink_status_payload(sink_status_payload: &[u8]) -> Vec<u8> {
        let sink_status = rmp_serde::from_slice::<SinkStatusSnapshot>(sink_status_payload)
            .expect("decode sink-status payload for source-status payload");
        let mut groups = sink_status
            .groups
            .iter()
            .map(|group| group.group_id.clone())
            .collect::<BTreeSet<_>>();
        for scheduled in sink_status.scheduled_groups_by_node.values() {
            groups.extend(scheduled.iter().cloned());
        }
        rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
            lifecycle_state: "running".into(),
            host_object_grants_version: 1,
            grants: Vec::new(),
            logical_roots: Vec::new(),
            status: SourceStatusSnapshot {
                current_stream_generation: Some(1),
                logical_roots: groups
                    .into_iter()
                    .map(|group_id| crate::source::SourceLogicalRootHealthSnapshot {
                        root_id: group_id,
                        status: "ok".into(),
                        matched_grants: 1,
                        active_members: 1,
                        coverage_mode: "realtime_hotset_plus_audit".into(),
                    })
                    .collect(),
                ..SourceStatusSnapshot::default()
            },
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
            enqueued_path_origin_counts_by_node: BTreeMap::new(),
            pending_path_origin_counts_by_node: BTreeMap::new(),
            yielded_path_origin_counts_by_node: BTreeMap::new(),
            summarized_path_origin_counts_by_node: BTreeMap::new(),
        })
        .expect("encode source-status payload")
    }

    async fn wait_for_owner_request_batches(&self, expected: usize) {
        loop {
            if self
                .owner_request_batches
                .load(std::sync::atomic::Ordering::Acquire)
                >= expected
            {
                return;
            }
            self.changed.notified().await;
        }
    }

    fn new(owner_request_channel: String, sink_status_request_channel: String) -> Self {
        let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            groups: vec![sink_group_status("nfs1", true)],
            ..SinkStatusSnapshot::default()
        })
        .expect("encode sink-status payload");
        Self::new_with_sink_status_payload(
            owner_request_channel,
            sink_status_request_channel,
            sink_status_payload,
        )
    }

    fn new_with_sink_status_payload(
        owner_request_channel: String,
        sink_status_request_channel: String,
        sink_status_payload: Vec<u8>,
    ) -> Self {
        Self::new_with_sink_status_payload_and_successful_tree_group(
            owner_request_channel,
            sink_status_request_channel,
            sink_status_payload,
            None,
        )
    }

    fn new_with_sink_status_payload_and_successful_tree_group(
        owner_request_channel: String,
        sink_status_request_channel: String,
        sink_status_payload: Vec<u8>,
        successful_tree_group: Option<String>,
    ) -> Self {
        let source_status_request_channel = default_route_bindings()
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
            .expect("resolve source-status route")
            .0;
        let source_status_payload =
            Self::source_status_payload_for_sink_status_payload(&sink_status_payload);
        Self {
            owner_request_channel,
            source_status_request_channel,
            source_status_payload,
            sink_status_request_channel,
            sink_status_payload,
            successful_tree_group,
            delayed_successful_tree_group: None,
            delayed_success_duration: Duration::ZERO,
            owner_request_batches: std::sync::atomic::AtomicUsize::new(0),
            queued_requests: AsyncMutex::new(HashMap::new()),
            changed: Notify::new(),
        }
    }

    fn new_with_sink_status_payload_and_delayed_successful_tree_group(
        owner_request_channel: String,
        sink_status_request_channel: String,
        sink_status_payload: Vec<u8>,
        successful_tree_group: Option<String>,
        delayed_successful_tree_group: Option<String>,
        delayed_success_duration: Duration,
    ) -> Self {
        let mut boundary = Self::new_with_sink_status_payload_and_successful_tree_group(
            owner_request_channel,
            sink_status_request_channel,
            sink_status_payload,
            successful_tree_group,
        );
        boundary.delayed_successful_tree_group = delayed_successful_tree_group;
        boundary.delayed_success_duration = delayed_success_duration;
        boundary
    }
}

impl ChannelBoundary for ReusableObservedRouteBoundary {
    fn log(&self, _ctx: BoundaryContext, _level: LogLevel, _msg: &str) {}
}

impl StateBoundary for ReusableObservedRouteBoundary {}

#[async_trait]
impl ChannelIoSubset for MaterializedRouteRaceBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        let correlation = request
            .events
            .first()
            .and_then(|event| event.metadata().correlation_id);
        let mut state = self.state.lock().expect("route race state lock");
        state.sent_call_channel = Some(request.channel_key.0);
        state.sent_correlation = correlation;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        enum ReplyKind {
            Empty,
            Real,
        }

        let (delay, reply_kind, correlation) = {
            let mut state = self.state.lock().expect("route race state lock");
            let sent_call_channel = state
                .sent_call_channel
                .clone()
                .ok_or_else(|| CnxError::Internal("missing sent call channel".into()))?;
            let correlation = state
                .sent_correlation
                .ok_or_else(|| CnxError::Internal("missing sent correlation".into()))?;
            let recv_count = state
                .recv_counts_by_channel
                .entry(request.channel_key.0.clone())
                .or_default();
            let current_recv = *recv_count;
            *recv_count += 1;
            let proxy_reply_channel = format!("{}:reply", self.proxy_call_channel);
            let internal_reply_channel = format!("{}:reply", self.internal_call_channel);
            if request.channel_key.0 == proxy_reply_channel
                && sent_call_channel == self.proxy_call_channel
            {
                match current_recv {
                    0 => (Duration::ZERO, ReplyKind::Empty, correlation),
                    1 => (
                        MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE + Duration::from_millis(150),
                        ReplyKind::Real,
                        correlation,
                    ),
                    _ => return Err(CnxError::Timeout),
                }
            } else if request.channel_key.0 == internal_reply_channel
                && sent_call_channel == self.internal_call_channel
            {
                match current_recv {
                    0 => (Duration::from_millis(50), ReplyKind::Real, correlation),
                    _ => return Err(CnxError::Timeout),
                }
            } else {
                return Err(CnxError::Timeout);
            }
        };

        tokio::time::sleep(delay).await;
        let payload = match reply_kind {
            ReplyKind::Empty => empty_materialized_tree_payload_for_test(&self.path),
            ReplyKind::Real => real_materialized_tree_payload_for_test(&self.path),
        };
        Ok(vec![mk_event_with_correlation(
            &self.selected_group,
            correlation,
            payload,
        )])
    }
}

#[async_trait]
impl ChannelIoSubset for ReusableObservedRouteBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        {
            let mut send_batches = self
                .send_batches_by_channel
                .lock()
                .expect("reusable route boundary send batches lock");
            *send_batches
                .entry(request.channel_key.0.clone())
                .or_default() += 1;
        }
        {
            let mut channels = self.channels.lock().await;
            channels
                .entry(request.channel_key.0)
                .or_default()
                .extend(request.events);
        }
        self.changed.notify_waiters();
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let deadline = request
            .timeout_ms
            .map(Duration::from_millis)
            .map(|timeout| tokio::time::Instant::now() + timeout);
        loop {
            {
                let mut channels = self.channels.lock().await;
                if let Some(events) = channels.remove(&request.channel_key.0)
                    && !events.is_empty()
                {
                    let mut recv_batches = self
                        .recv_batches_by_channel
                        .lock()
                        .expect("reusable route boundary recv batches lock");
                    *recv_batches
                        .entry(request.channel_key.0.clone())
                        .or_default() += 1;
                    return Ok(events);
                }
            }
            {
                let closed = self
                    .closed
                    .lock()
                    .expect("reusable route boundary closed lock");
                if closed.contains(&request.channel_key.0) {
                    return Err(CnxError::ChannelClosed);
                }
            }
            let notified = self.changed.notified();
            if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, notified).await {
                    Ok(()) => {}
                    Err(_) => return Err(CnxError::Timeout),
                }
            } else {
                notified.await;
            }
        }
    }

    fn channel_close(
        &self,
        _ctx: BoundaryContext,
        channel: ChannelKey,
    ) -> capanix_app_sdk::Result<()> {
        self.closed
            .lock()
            .expect("reusable route boundary closed lock")
            .insert(channel.0);
        self.changed.notify_waiters();
        Ok(())
    }
}

#[async_trait]
impl ChannelIoSubset for SourceStatusTimeoutSinkStatusOkBoundary {
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
                .expect("source/sink selective boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("source/sink selective boundary send batches lock");
        *send_batches.entry(request.channel_key.0).or_default() += 1;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("source/sink selective boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);
        if request.channel_key.0 == self.sink_reply_channel
            && !self.sink_status_payload.is_empty()
            && !self
                .sink_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let request_channel = self.sink_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("source/sink selective boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "node-a",
                correlation,
                self.sink_status_payload.clone(),
            )]);
        }
        Err(CnxError::Timeout)
    }
}

#[async_trait]
impl ChannelIoSubset for SourceStatusOkSinkStatusTimeoutBoundary {
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
                .expect("source ok sink timeout boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("source ok sink timeout boundary send batches lock");
        *send_batches.entry(request.channel_key.0).or_default() += 1;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("source ok sink timeout boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);
        if request.channel_key.0 == self.source_reply_channel
            && !self
                .source_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let request_channel = self.source_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("source ok sink timeout boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "node-b",
                correlation,
                self.source_status_payload.clone(),
            )]);
        }
        Err(CnxError::Timeout)
    }
}

#[async_trait]
impl ChannelIoSubset for SourceStatusSendMissingRouteStateSinkStatusOkBoundary {
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
                .expect("source send-missing-route-state boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("source send-missing-route-state boundary send batches lock");
        *send_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(send_batches);
        if request.channel_key.0 == self.source_request_channel {
            let attempt = self
                .source_send_attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt == 0 {
                return Err(CnxError::Internal(
                    "missing route state for channel_buffer ChannelSlotId(17723)".to_string(),
                ));
            }
        }
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("source send-missing-route-state boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);
        if request.channel_key.0 == self.sink_reply_channel
            && !self
                .sink_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let request_channel = self.sink_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("source send-missing-route-state boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "node-a",
                correlation,
                self.sink_status_payload.clone(),
            )]);
        }
        Err(CnxError::Timeout)
    }
}

#[async_trait]
impl ChannelIoSubset for SourceStatusRetryThenReplyBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        let source_request_channel = self.source_reply_channel.trim_end_matches(":reply");
        if let Some(correlation) = request
            .events
            .first()
            .and_then(|event| event.metadata().correlation_id)
        {
            self.correlations_by_channel
                .lock()
                .expect("source retry boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("source retry boundary send batches lock");
        let send_count = send_batches
            .entry(request.channel_key.0.clone())
            .or_default();
        *send_count += 1;
        if request.channel_key.0 == source_request_channel && *send_count == 2 {
            *self
                .second_send_at
                .lock()
                .expect("source retry boundary second send lock") = Some(std::time::Instant::now());
        }
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("source retry boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);

        if request.channel_key.0 == self.source_reply_channel {
            let request_channel = self.source_reply_channel.trim_end_matches(":reply");
            let send_count = self
                .send_batches_by_channel
                .lock()
                .expect("source retry boundary send batches lock")
                .get(request_channel)
                .copied()
                .unwrap_or_default();
            if send_count < 2 {
                let mut first_retryable_gap_at = self
                    .first_retryable_gap_at
                    .lock()
                    .expect("source retry boundary first gap lock");
                if first_retryable_gap_at.is_none() {
                    *first_retryable_gap_at = Some(std::time::Instant::now());
                }
                return Err(CnxError::Timeout);
            }
            if self
                .source_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(CnxError::Timeout);
            }
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("source retry boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "node-b",
                correlation,
                self.source_status_payload.clone(),
            )]);
        }
        if request.channel_key.0 == self.sink_reply_channel
            && !self.sink_status_payload.is_empty()
            && !self
                .sink_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let request_channel = self.sink_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("source retry boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "node-a",
                correlation,
                self.sink_status_payload.clone(),
            )]);
        }

        Err(CnxError::Timeout)
    }
}

#[async_trait]
impl ChannelIoSubset for SourceStatusMissingRouteStateThenReplyBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        let source_request_channel = self.source_reply_channel.trim_end_matches(":reply");
        if let Some(correlation) = request
            .events
            .first()
            .and_then(|event| event.metadata().correlation_id)
        {
            self.correlations_by_channel
                .lock()
                .expect("source missing-route-state boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("source missing-route-state boundary send batches lock");
        let send_count = send_batches
            .entry(request.channel_key.0.clone())
            .or_default();
        *send_count += 1;
        if request.channel_key.0 == source_request_channel && *send_count == 2 {
            *self
                .second_send_at
                .lock()
                .expect("source missing-route-state boundary second send lock") =
                Some(std::time::Instant::now());
            self.second_send_notifier.notify_waiters();
        }
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("source missing-route-state boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);

        if request.channel_key.0 == self.source_reply_channel {
            let request_channel = self.source_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("source missing-route-state boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            let attempt = self
                .source_reply_attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt == 0 {
                let mut first_retryable_gap_at = self
                    .first_retryable_gap_at
                    .lock()
                    .expect("source missing-route-state boundary first gap lock");
                if first_retryable_gap_at.is_none() {
                    *first_retryable_gap_at = Some(std::time::Instant::now());
                }
                return Err(CnxError::Internal(
                    "missing route state for channel_buffer ChannelSlotId(17723)".to_string(),
                ));
            }
            return Ok(vec![mk_event_with_correlation(
                "node-b",
                correlation,
                self.source_status_payload.clone(),
            )]);
        }
        if request.channel_key.0 == self.sink_reply_channel
            && !self
                .sink_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let request_channel = self.sink_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("source missing-route-state boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "node-a",
                correlation,
                self.sink_status_payload.clone(),
            )]);
        }

        Err(CnxError::Timeout)
    }
}

#[async_trait]
impl ChannelIoSubset for SourceStatusOkSinkStatusExplicitEmptyThenReadyBoundary {
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
                .expect("source/sink explicit-empty-then-ready boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("source/sink explicit-empty-then-ready boundary send batches lock");
        *send_batches.entry(request.channel_key.0).or_default() += 1;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("source/sink explicit-empty-then-ready boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);

        if request.channel_key.0 == self.source_reply_channel {
            if self
                .source_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(CnxError::Timeout);
            }
            let request_channel = self.source_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("source/sink explicit-empty-then-ready boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "node-a",
                correlation,
                self.source_status_payload.clone(),
            )]);
        }
        if request.channel_key.0 == self.sink_reply_channel {
            let payload = self
                .sink_status_payloads
                .lock()
                .expect("source/sink explicit-empty-then-ready boundary sink payload lock")
                .pop_front()
                .unwrap_or_default();
            if payload.is_empty() {
                return Err(CnxError::Timeout);
            }
            let request_channel = self.sink_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("source/sink explicit-empty-then-ready boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "node-a",
                correlation,
                payload,
            )]);
        }
        Err(CnxError::Timeout)
    }
}

#[async_trait]
impl ChannelIoSubset for SinkStatusInternalRetryThenReplyBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        let request_channel = self.sink_reply_channel.trim_end_matches(":reply");
        if let Some(correlation) = request
            .events
            .first()
            .and_then(|event| event.metadata().correlation_id)
        {
            self.correlations_by_channel
                .lock()
                .expect("sink retry boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("sink retry boundary send batches lock");
        let send_count = send_batches
            .entry(request.channel_key.0.clone())
            .or_default();
        *send_count += 1;
        if request.channel_key.0 == request_channel && *send_count == 2 {
            *self
                .second_send_at
                .lock()
                .expect("sink retry boundary second send lock") = Some(std::time::Instant::now());
        }
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("sink retry boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);

        if request.channel_key.0 != self.sink_reply_channel {
            return Err(CnxError::Timeout);
        }
        let request_channel = self.sink_reply_channel.trim_end_matches(":reply");
        let send_count = self
            .send_batches_by_channel
            .lock()
            .expect("sink retry boundary send batches lock")
            .get(request_channel)
            .copied()
            .unwrap_or_default();
        if send_count < 2 {
            let mut first_retryable_gap_at = self
                .first_retryable_gap_at
                .lock()
                .expect("sink retry boundary first gap lock");
            if first_retryable_gap_at.is_none() {
                *first_retryable_gap_at = Some(std::time::Instant::now());
            }
            return Err(CnxError::Internal(
                "simulated transient internal sink-status collect gap".into(),
            ));
        }
        if self
            .sink_reply_sent
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(CnxError::Timeout);
        }
        let correlation = self
            .correlations_by_channel
            .lock()
            .expect("sink retry boundary correlations lock")
            .get(request_channel)
            .copied()
            .unwrap_or(1);
        Ok(vec![mk_event_with_correlation(
            "node-a",
            correlation,
            self.sink_status_payload.clone(),
        )])
    }
}

#[async_trait]
impl ChannelIoSubset for MaterializedRouteAccessDeniedThenProxyBoundary {
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
                .expect("materialized access-denied boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        {
            let mut send_batches = self
                .send_batches_by_channel
                .lock()
                .expect("materialized access-denied boundary send batches lock");
            *send_batches
                .entry(request.channel_key.0.clone())
                .or_default() += 1;
        }
        {
            let mut channels = self.channels.lock().await;
            channels
                .entry(request.channel_key.0)
                .or_default()
                .extend(request.events);
        }
        self.changed.notify_waiters();
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        {
            let mut recv_batches = self
                .recv_batches_by_channel
                .lock()
                .expect("materialized access-denied boundary recv batches lock");
            *recv_batches
                .entry(request.channel_key.0.clone())
                .or_default() += 1;
        }
        if request.channel_key.0 == self.owner_reply_channel {
            if !self
                .owner_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(CnxError::AccessDenied(
                        "access denied: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                            .to_string(),
                    ));
            }
            return Err(CnxError::Timeout);
        }
        if request.channel_key.0 == self.sink_status_reply_channel
            && !self
                .sink_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let request_channel = self.sink_status_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("materialized access-denied boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "node-a",
                correlation,
                self.sink_status_payload.clone(),
            )]);
        }
        let deadline = request
            .timeout_ms
            .map(Duration::from_millis)
            .map(|timeout| tokio::time::Instant::now() + timeout);
        loop {
            {
                let mut channels = self.channels.lock().await;
                if let Some(events) = channels.remove(&request.channel_key.0)
                    && !events.is_empty()
                {
                    return Ok(events);
                }
            }
            {
                let closed = self
                    .closed
                    .lock()
                    .expect("materialized access-denied boundary closed lock");
                if closed.contains(&request.channel_key.0) {
                    return Err(CnxError::ChannelClosed);
                }
            }
            let notified = self.changed.notified();
            if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, notified).await {
                    Ok(()) => {}
                    Err(_) => return Err(CnxError::Timeout),
                }
            } else {
                notified.await;
            }
        }
    }

    fn channel_close(
        &self,
        _ctx: BoundaryContext,
        channel: ChannelKey,
    ) -> capanix_app_sdk::Result<()> {
        self.closed
            .lock()
            .expect("materialized access-denied boundary closed lock")
            .insert(channel.0);
        self.changed.notify_waiters();
        Ok(())
    }
}

#[async_trait]
impl ChannelIoSubset for MaterializedRouteMissingRouteStateThenProxyBoundary {
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
                .expect("materialized missing-route-state boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        {
            let mut send_batches = self
                .send_batches_by_channel
                .lock()
                .expect("materialized missing-route-state boundary send batches lock");
            *send_batches
                .entry(request.channel_key.0.clone())
                .or_default() += 1;
        }
        {
            let mut channels = self.channels.lock().await;
            channels
                .entry(request.channel_key.0)
                .or_default()
                .extend(request.events);
        }
        self.changed.notify_waiters();
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        {
            let mut recv_batches = self
                .recv_batches_by_channel
                .lock()
                .expect("materialized missing-route-state boundary recv batches lock");
            *recv_batches
                .entry(request.channel_key.0.clone())
                .or_default() += 1;
        }
        if request.channel_key.0 == self.owner_reply_channel {
            if !self
                .owner_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(CnxError::Internal(
                    "missing route state for channel_buffer ChannelSlotId(17723)".to_string(),
                ));
            }
            return Err(CnxError::Timeout);
        }
        if request.channel_key.0 == self.sink_status_reply_channel
            && !self
                .sink_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let request_channel = self.sink_status_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("materialized missing-route-state boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "node-a",
                correlation,
                self.sink_status_payload.clone(),
            )]);
        }
        let deadline = request
            .timeout_ms
            .map(Duration::from_millis)
            .map(|timeout| tokio::time::Instant::now() + timeout);
        loop {
            {
                let mut channels = self.channels.lock().await;
                if let Some(events) = channels.remove(&request.channel_key.0)
                    && !events.is_empty()
                {
                    return Ok(events);
                }
            }
            {
                let closed = self
                    .closed
                    .lock()
                    .expect("materialized missing-route-state boundary closed lock");
                if closed.contains(&request.channel_key.0) {
                    return Err(CnxError::ChannelClosed);
                }
            }
            let notified = self.changed.notified();
            if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, notified).await {
                    Ok(()) => {}
                    Err(_) => return Err(CnxError::Timeout),
                }
            } else {
                notified.await;
            }
        }
    }

    fn channel_close(
        &self,
        _ctx: BoundaryContext,
        channel: ChannelKey,
    ) -> capanix_app_sdk::Result<()> {
        self.closed
            .lock()
            .expect("materialized missing-route-state boundary closed lock")
            .insert(channel.0);
        self.changed.notify_waiters();
        Ok(())
    }
}

#[async_trait]
impl ChannelIoSubset for MaterializedProxyMissingRouteStateThenReplyBoundary {
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
                .expect("materialized proxy missing-route-state boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("materialized proxy missing-route-state boundary send batches lock");
        *send_batches.entry(request.channel_key.0).or_default() += 1;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("materialized proxy missing-route-state boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);
        if request.channel_key.0 == self.proxy_reply_channel {
            let request_channel = self.proxy_reply_channel.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("materialized proxy missing-route-state boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            if !self
                .proxy_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(CnxError::Internal(
                    "missing route state for channel_buffer ChannelSlotId(17723)".to_string(),
                ));
            }
            return Ok(vec![mk_event_with_correlation(
                "nfs4",
                correlation,
                self.payload.clone(),
            )]);
        }
        Err(CnxError::Timeout)
    }
}

#[async_trait]
impl ChannelIoSubset for ForceFindProtocolRetryThenReplyBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        if self.source_status.on_send(&request) {
            return Ok(());
        }
        if let Some(correlation) = request
            .events
            .first()
            .and_then(|event| event.metadata().correlation_id)
        {
            self.correlations_by_channel
                .lock()
                .expect("force-find retry boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("force-find retry boundary send batches lock");
        *send_batches.entry(request.channel_key.0).or_default() += 1;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        if let Some(result) = self.source_status.try_recv(&request) {
            return result;
        }
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("force-find retry boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);

        if !request.channel_key.0.ends_with(":reply") {
            return Err(CnxError::Timeout);
        }
        let request_channel = request.channel_key.0.trim_end_matches(":reply");
        let send_count = self.total_send_batch_count();
        if send_count < 2 {
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("force-find retry boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "node-a::routed",
                correlation + 100,
                self.payload.clone(),
            )]);
        }
        if self
            .source_reply_sent
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(CnxError::Timeout);
        }
        let correlation = self
            .correlations_by_channel
            .lock()
            .expect("force-find retry boundary correlations lock")
            .get(request_channel)
            .copied()
            .unwrap_or(1);
        Ok(vec![mk_event_with_correlation(
            "node-a::routed",
            correlation,
            self.payload.clone(),
        )])
    }
}

#[async_trait]
impl ChannelIoSubset for ForceFindGroupMissingThenReplyBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        if self.source_status.on_send(&request) {
            return Ok(());
        }
        if let Some(correlation) = request
            .events
            .first()
            .and_then(|event| event.metadata().correlation_id)
        {
            self.correlations_by_channel
                .lock()
                .expect("force-find group-missing boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("force-find group-missing boundary send batches lock");
        *send_batches.entry(request.channel_key.0).or_default() += 1;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        if let Some(result) = self.source_status.try_recv(&request) {
            return result;
        }
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("force-find group-missing boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);

        if !request.channel_key.0.ends_with(":reply") {
            return Err(CnxError::Timeout);
        }
        let request_channel = request.channel_key.0.trim_end_matches(":reply");
        let send_count = self.total_send_batch_count();
        if send_count < 2 {
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("force-find group-missing boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "nfs1",
                correlation,
                b"force-find selected_group matched no group: nfs1".to_vec(),
            )]);
        }
        if self
            .source_reply_sent
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(CnxError::Timeout);
        }
        let correlation = self
            .correlations_by_channel
            .lock()
            .expect("force-find group-missing boundary correlations lock")
            .get(request_channel)
            .copied()
            .unwrap_or(1);
        Ok(vec![mk_event_with_correlation(
            "node-b::routed",
            correlation,
            self.payload.clone(),
        )])
    }
}

#[async_trait]
impl ChannelIoSubset for ForceFindHostUnavailableThenNextRunnerBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        if self.source_status.on_send(&request) {
            return Ok(());
        }
        let correlation = request
            .events
            .first()
            .and_then(|event| event.metadata().correlation_id)
            .unwrap_or(1);
        self.correlations_by_channel
            .lock()
            .expect("force-find host-unavailable boundary correlations lock")
            .insert(request.channel_key.0.clone(), correlation);
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("force-find host-unavailable boundary send batches lock");
        *send_batches.entry(request.channel_key.0).or_default() += 1;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        if let Some(result) = self.source_status.try_recv(&request) {
            return result;
        }
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("force-find host-unavailable boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);

        if !request.channel_key.0.ends_with(":reply") {
            return Err(CnxError::Timeout);
        }
        let request_channel = request.channel_key.0.trim_end_matches(":reply");
        let correlation = self
            .correlations_by_channel
            .lock()
            .expect("force-find host-unavailable boundary correlations lock")
            .get(request_channel)
            .copied()
            .unwrap_or(1);

        if request.channel_key.0 == self.node_a_reply_channel {
            return Ok(vec![mk_event_with_correlation(
                "nfs1",
                correlation,
                b"force-find failed on all targeted roots (1): node-a::nfs1: HOST_FS_UNAVAILABLE"
                    .to_vec(),
            )]);
        }
        if request.channel_key.0 == self.node_b_reply_channel {
            if self
                .node_b_reply_sent
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(CnxError::Timeout);
            }
            return Ok(vec![mk_event_with_correlation(
                "node-b::nfs1",
                correlation,
                self.payload.clone(),
            )]);
        }

        Err(CnxError::PeerError(
            "generic force-find fallback must not decide same-group HOST_FS_UNAVAILABLE reroute"
                .into(),
        ))
    }
}

#[async_trait]
impl ChannelIoSubset for ForceFindSingleCandidateGroupMissingThenFallbackBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        if self.source_status.on_send(&request) {
            return Ok(());
        }
        let correlation = request
            .events
            .first()
            .and_then(|event| event.metadata().correlation_id)
            .unwrap_or(1);
        self.correlations_by_channel
            .lock()
            .expect("force-find single-candidate boundary correlations lock")
            .insert(request.channel_key.0.clone(), correlation);
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("force-find single-candidate boundary send batches lock");
        let batch_count = send_batches.entry(request.channel_key.0).or_default();
        *batch_count += 1;
        let total_send_batches = send_batches.values().copied().sum::<usize>();
        drop(send_batches);
        if total_send_batches == 2 {
            *self
                .second_send_at
                .lock()
                .expect("force-find single-candidate boundary second send lock") =
                Some(std::time::Instant::now());
        }
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        if let Some(result) = self.source_status.try_recv(&request) {
            return result;
        }
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("force-find single-candidate boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);

        if !request.channel_key.0.ends_with(":reply") {
            return Err(CnxError::Timeout);
        }
        let request_channel = request.channel_key.0.trim_end_matches(":reply");
        let send_count = self.total_send_batch_count();
        if send_count < 2 {
            *self
                .first_retryable_gap_at
                .lock()
                .expect("force-find single-candidate boundary first gap lock") =
                Some(std::time::Instant::now());
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("force-find single-candidate boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "nfs2",
                correlation,
                b"force-find selected_group matched no group: nfs2".to_vec(),
            )]);
        }
        let retry_reissue_delay = self.retry_reissue_delay().unwrap_or(Duration::from_secs(1));
        if retry_reissue_delay >= FORCE_FIND_ROUTE_RETRY_BACKOFF {
            return Err(CnxError::PeerError(
                "force-find selected_group matched no group: nfs2".to_string(),
            ));
        }
        let correlation = self
            .correlations_by_channel
            .lock()
            .expect("force-find single-candidate boundary correlations lock")
            .get(request_channel)
            .copied()
            .unwrap_or(1);
        if self
            .source_reply_sent
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(CnxError::Timeout);
        }
        Ok(vec![mk_event_with_correlation(
            "node-d::fallback",
            correlation,
            self.payload.clone(),
        )])
    }
}

#[async_trait]
impl ChannelIoSubset for ForceFindSelectedGroupFallbackCollectBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        if self.source_status.on_send(&request) {
            return Ok(());
        }
        let correlation = request
            .events
            .first()
            .and_then(|event| event.metadata().correlation_id)
            .unwrap_or(1);
        self.correlations_by_channel
            .lock()
            .expect("force-find selected-group fallback collect correlations lock")
            .insert(request.channel_key.0.clone(), correlation);
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("force-find selected-group fallback collect send batches lock");
        *send_batches.entry(request.channel_key.0).or_default() += 1;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        if let Some(result) = self.source_status.try_recv(&request) {
            return result;
        }
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("force-find selected-group fallback collect recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);

        if request.channel_key.0 == self.selected_reply_channel {
            let request_channel = request.channel_key.0.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("force-find selected-group fallback collect correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "nfs1",
                correlation,
                b"force-find selected_group matched no group: nfs1".to_vec(),
            )]);
        }

        if request.channel_key.0 == self.generic_reply_channel {
            let request_channel = request.channel_key.0.trim_end_matches(":reply");
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("force-find selected-group fallback collect correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            let count = self
                .generic_reply_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if count == 0 {
                return Ok(vec![mk_event_with_correlation(
                    "nfs2",
                    correlation,
                    b"unrelated fresh group".to_vec(),
                )]);
            }
            if count == 1 {
                return Ok(vec![mk_event_with_correlation(
                    "node-b::nfs1",
                    correlation,
                    self.selected_group_payload.clone(),
                )]);
            }
        }

        Err(CnxError::Timeout)
    }
}

#[async_trait]
impl ChannelIoSubset for ForceFindRunnerBindingStatusBoundary {
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
                .expect("force-find runner binding boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("force-find runner binding boundary send batches lock");
        *send_batches.entry(request.channel_key.0).or_default() += 1;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("force-find runner binding boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);

        if request.channel_key.0 != self.source_reply_channel {
            return Err(CnxError::Timeout);
        }
        let request_channel = self.source_reply_channel.trim_end_matches(":reply");
        let send_count = self
            .send_batches_by_channel
            .lock()
            .expect("force-find runner binding boundary send batches lock")
            .get(request_channel)
            .copied()
            .unwrap_or_default();
        let delivered = self
            .delivered_send_count
            .load(std::sync::atomic::Ordering::SeqCst);
        if send_count == 0 || delivered >= send_count {
            return Err(CnxError::Timeout);
        }
        self.delivered_send_count
            .store(send_count, std::sync::atomic::Ordering::SeqCst);
        let correlation = self
            .correlations_by_channel
            .lock()
            .expect("force-find runner binding boundary correlations lock")
            .get(request_channel)
            .copied()
            .unwrap_or(1);
        Ok(vec![mk_event_with_correlation(
            "source-status",
            correlation,
            self.source_status_payload.clone(),
        )])
    }
}

#[async_trait]
impl ChannelIoSubset for ForceFindDelayedRunnerBindingBoundary {
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
                .expect("delayed runner binding boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("delayed runner binding boundary send batches lock");
        *send_batches.entry(request.channel_key.0).or_default() += 1;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("delayed runner binding boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);

        if request.channel_key.0 == self.source_status_reply_channel {
            let request_channel = self.source_status_reply_channel.trim_end_matches(":reply");
            let send_count = self
                .send_batches_by_channel
                .lock()
                .expect("delayed runner binding boundary send batches lock")
                .get(request_channel)
                .copied()
                .unwrap_or_default();
            let delivered = self
                .delivered_source_status_send_count
                .load(std::sync::atomic::Ordering::SeqCst);
            if send_count == 0 || delivered >= send_count {
                return Err(CnxError::Timeout);
            }
            self.delivered_source_status_send_count
                .store(send_count, std::sync::atomic::Ordering::SeqCst);
            let payload = self
                .source_status_payloads
                .lock()
                .expect("delayed runner binding boundary payload lock")
                .pop_front()
                .ok_or_else(|| CnxError::Timeout)?;
            let correlation = self
                .correlations_by_channel
                .lock()
                .expect("delayed runner binding boundary correlations lock")
                .get(request_channel)
                .copied()
                .unwrap_or(1);
            return Ok(vec![mk_event_with_correlation(
                "source-status",
                correlation,
                payload,
            )]);
        }

        if !request.channel_key.0.ends_with(":reply") {
            return Err(CnxError::Timeout);
        }
        if self
            .source_find_reply_sent
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(CnxError::Timeout);
        }
        let request_channel = request.channel_key.0.trim_end_matches(":reply");
        let correlation = self
            .correlations_by_channel
            .lock()
            .expect("delayed runner binding boundary correlations lock")
            .get(request_channel)
            .copied()
            .unwrap_or(1);
        Ok(vec![mk_event_with_correlation(
            "node-a::routed",
            correlation,
            vec![1, 2, 3],
        )])
    }
}

#[async_trait]
impl ChannelIoSubset for SinkStatusPeerTransportRetryThenReplyBoundary {
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
                .expect("sink peer transport retry boundary correlations lock")
                .insert(request.channel_key.0.clone(), correlation);
        }
        let mut send_batches = self
            .send_batches_by_channel
            .lock()
            .expect("sink peer transport retry boundary send batches lock");
        *send_batches.entry(request.channel_key.0).or_default() += 1;
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut recv_batches = self
            .recv_batches_by_channel
            .lock()
            .expect("sink peer transport retry boundary recv batches lock");
        *recv_batches
            .entry(request.channel_key.0.clone())
            .or_default() += 1;
        drop(recv_batches);

        if request.channel_key.0 != self.sink_reply_channel {
            return Err(CnxError::Timeout);
        }

        if !self
            .sink_reply_sent
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(CnxError::PeerError(
                "transport closed: sidecar control bridge stopped".to_string(),
            ));
        }

        let request_channel = self.sink_reply_channel.trim_end_matches(":reply");
        let correlation = self
            .correlations_by_channel
            .lock()
            .expect("sink peer transport retry boundary correlations lock")
            .get(request_channel)
            .copied()
            .unwrap_or(1);
        Ok(vec![mk_event_with_correlation(
            "node-a",
            correlation,
            self.sink_status_payload.clone(),
        )])
    }
}

#[async_trait]
impl ChannelIoSubset for MaterializedSelectedGroupTreeStallBoundary {
    async fn channel_send(
        &self,
        _ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        if request.channel_key.0 == self.owner_request_channel {
            self.owner_request_batches
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
        {
            let mut queued = self.queued_requests.lock().await;
            queued
                .entry(request.channel_key.0)
                .or_default()
                .extend(request.events);
        }
        self.changed.notify_waiters();
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let reply_channel = format!("{}:reply", self.owner_request_channel);
        let source_status_reply_channel = format!("{}:reply", self.source_status_request_channel);
        let sink_status_reply_channel = format!("{}:reply", self.sink_status_request_channel);
        loop {
            let reply_kind = if request.channel_key.0 == reply_channel {
                Some(("owner", self.owner_request_channel.as_str()))
            } else if request.channel_key.0 == source_status_reply_channel {
                Some(("source-status", self.source_status_request_channel.as_str()))
            } else if request.channel_key.0 == sink_status_reply_channel {
                Some(("sink-status", self.sink_status_request_channel.as_str()))
            } else {
                None
            };
            let Some((reply_kind, request_channel)) = reply_kind else {
                return Err(CnxError::Timeout);
            };
            let maybe_request = {
                let mut queued = self.queued_requests.lock().await;
                queued
                    .get_mut(request_channel)
                    .and_then(|events| (!events.is_empty()).then(|| events.remove(0)))
            };
            if let Some(req) = maybe_request {
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("owner request correlation");
                match reply_kind {
                    "source-status" => {
                        return Ok(vec![mk_event_with_correlation(
                            "node-a",
                            correlation,
                            self.source_status_payload.clone(),
                        )]);
                    }
                    "sink-status" => {
                        return Ok(vec![mk_event_with_correlation(
                            "node-a",
                            correlation,
                            self.sink_status_payload.clone(),
                        )]);
                    }
                    "owner" => {
                        let params =
                            rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                                .expect("decode owner internal query request");
                        match params.op {
                            QueryOp::Stats => {
                                let payload = rmp_serde::to_vec_named(
                                    &MaterializedQueryPayload::Stats(SubtreeStats {
                                        total_files: 7,
                                        ..SubtreeStats::default()
                                    }),
                                )
                                .expect("encode stats payload");
                                return Ok(vec![mk_event_with_correlation(
                                    params
                                        .scope
                                        .selected_group
                                        .as_deref()
                                        .expect("selected group for stats request"),
                                    correlation,
                                    payload,
                                )]);
                            }
                            QueryOp::Tree => {
                                if self.successful_tree_group.as_deref()
                                    == params.scope.selected_group.as_deref()
                                {
                                    let payload =
                                        real_materialized_tree_payload_for_test(&params.scope.path);
                                    return Ok(vec![mk_event_with_correlation(
                                        params
                                            .scope
                                            .selected_group
                                            .as_deref()
                                            .expect("selected group for tree request"),
                                        correlation,
                                        payload,
                                    )]);
                                }
                                if self.delayed_successful_tree_group.as_deref()
                                    == params.scope.selected_group.as_deref()
                                {
                                    tokio::time::sleep(self.delayed_success_duration).await;
                                    let payload =
                                        real_materialized_tree_payload_for_test(&params.scope.path);
                                    return Ok(vec![mk_event_with_correlation(
                                        params
                                            .scope
                                            .selected_group
                                            .as_deref()
                                            .expect("selected group for delayed tree request"),
                                        correlation,
                                        payload,
                                    )]);
                                }
                                std::future::pending::<()>().await;
                                unreachable!("pending tree request should never resolve");
                            }
                        }
                    }
                    _ => {
                        return Err(CnxError::Internal(
                            "unexpected reply kind in test boundary".into(),
                        ));
                    }
                }
            }
            let notified = self.changed.notified();
            if let Some(timeout_ms) = request.timeout_ms {
                let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
                match tokio::time::timeout_at(deadline, notified).await {
                    Ok(()) => {}
                    Err(_) => return Err(CnxError::Timeout),
                }
            } else {
                notified.await;
            }
        }
    }
}

impl ForceFindFixture {
    fn new(scenario: ForceFindFixtureScenario) -> Self {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let sink_a = tempdir.path().join("sink-a");
        let sink_b = tempdir.path().join("sink-b");
        fs::create_dir_all(&sink_a).expect("create sink-a dir");
        fs::create_dir_all(&sink_b).expect("create sink-b dir");

        match scenario {
            ForceFindFixtureScenario::Standard => {
                fs::write(sink_b.join("winner-b"), b"b").expect("write winner-b");
                fs::write(sink_b.join("extra-b-1"), b"b").expect("write extra-b-1");
                fs::write(sink_b.join("extra-b-2"), b"b").expect("write extra-b-2");
                std::thread::sleep(std::time::Duration::from_millis(20));
                fs::write(sink_a.join("winner-a"), b"a").expect("write winner-a");
            }
            ForceFindFixtureScenario::FileAgeNoFiles => {
                fs::create_dir_all(sink_a.join("empty-a")).expect("create empty-a");
                fs::create_dir_all(sink_b.join("empty-b")).expect("create empty-b");
            }
        }

        let grants = vec![
            granted_mount_root("sink-a", &sink_a),
            granted_mount_root("sink-b", &sink_b),
        ];
        let config = source_config_with_grants(&grants);
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::with_boundaries(config.clone(), NodeId("source-node".into()), None)
                .expect("build source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(NodeId("sink-node".into()), None, config.clone())
                .expect("build sink"),
        )));
        let policy = Arc::new(RwLock::new(projection_policy_from_host_object_grants(
            &grants,
        )));

        Self {
            _tempdir: tempdir,
            app: create_local_router(
                sink,
                source,
                None,
                NodeId("test-node".into()),
                policy,
                Arc::new(Mutex::new(BTreeSet::new())),
                crate::api::state::ForceFindRunnerEvidence::default(),
            ),
        }
    }
}

fn granted_mount_root(object_ref: &str, mount_point: &Path) -> GrantedMountRoot {
    GrantedMountRoot {
        object_ref: object_ref.to_string(),
        host_ref: "source-node".to_string(),
        host_ip: format!("10.0.0.{}", if object_ref == "sink-a" { 1 } else { 2 }),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: mount_point.to_path_buf(),
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }
}

fn source_config_with_grants(grants: &[GrantedMountRoot]) -> SourceConfig {
    SourceConfig {
        roots: grants
            .iter()
            .map(|grant| RootSpec::new(grant.object_ref.clone(), grant.mount_point.clone()))
            .collect(),
        host_object_grants: grants.to_vec(),
        ..SourceConfig::default()
    }
}

fn mk_event(origin: &str, payload: Vec<u8>) -> Event {
    Event::new(
        EventMetadata {
            origin_id: NodeId(origin.to_string()),
            timestamp_us: 1,
            logical_ts: None,
            correlation_id: None,
            ingress_auth: None,
            trace: None,
        },
        Bytes::from(payload),
    )
}

fn mk_event_with_correlation(origin: &str, correlation: u64, payload: Vec<u8>) -> Event {
    Event::new(
        EventMetadata {
            origin_id: NodeId(origin.to_string()),
            timestamp_us: 1,
            logical_ts: None,
            correlation_id: Some(correlation),
            ingress_auth: None,
            trace: None,
        },
        Bytes::from(payload),
    )
}

fn empty_materialized_tree_payload_for_test(path: &[u8]) -> Vec<u8> {
    rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(TreeGroupPayload {
        reliability: GroupReliability::from_reason(Some(
            crate::shared_types::query::UnreliableReason::Unattested,
        )),
        stability: TreeStability::not_evaluated(),
        root: TreePageRoot {
            path: path.to_vec(),
            size: 0,
            modified_time_us: 0,
            is_dir: true,
            exists: false,
            has_children: false,
        },
        entries: Vec::new(),
    }))
    .expect("encode empty materialized tree payload")
}

fn real_materialized_tree_payload_for_test(path: &[u8]) -> Vec<u8> {
    rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(TreeGroupPayload {
        reliability: GroupReliability {
            reliable: true,
            unreliable_reason: None,
        },
        stability: TreeStability::not_evaluated(),
        root: TreePageRoot {
            path: path.to_vec(),
            size: 0,
            modified_time_us: 1,
            is_dir: true,
            exists: true,
            has_children: true,
        },
        entries: Vec::new(),
    }))
    .expect("encode real materialized tree payload")
}

fn real_materialized_tree_payload_with_entries_for_test(
    root_path: &[u8],
    entry_paths: &[&[u8]],
) -> Vec<u8> {
    rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(TreeGroupPayload {
        reliability: GroupReliability {
            reliable: true,
            unreliable_reason: None,
        },
        stability: TreeStability::not_evaluated(),
        root: TreePageRoot {
            path: root_path.to_vec(),
            size: 0,
            modified_time_us: 1,
            is_dir: true,
            exists: true,
            has_children: true,
        },
        entries: entry_paths
            .iter()
            .map(|entry_path| TreePageEntry {
                path: (*entry_path).to_vec(),
                size: 7,
                modified_time_us: 2,
                is_dir: false,
                has_children: false,
                depth: 1,
            })
            .collect(),
    }))
    .expect("encode real materialized tree payload with entries")
}

fn real_materialized_tree_payload_with_entries_and_mtime_for_test(
    root_path: &[u8],
    entry_paths: &[&[u8]],
    root_modified_time_us: u64,
    entry_modified_time_us: u64,
) -> Vec<u8> {
    rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(TreeGroupPayload {
        reliability: GroupReliability {
            reliable: true,
            unreliable_reason: None,
        },
        stability: TreeStability::not_evaluated(),
        root: TreePageRoot {
            path: root_path.to_vec(),
            size: 0,
            modified_time_us: root_modified_time_us,
            is_dir: true,
            exists: true,
            has_children: true,
        },
        entries: entry_paths
            .iter()
            .map(|entry_path| TreePageEntry {
                path: (*entry_path).to_vec(),
                size: 7,
                modified_time_us: entry_modified_time_us,
                is_dir: false,
                has_children: false,
                depth: 1,
            })
            .collect(),
    }))
    .expect("encode real materialized tree payload with entries and mtime")
}

fn real_materialized_tree_payload_for_test_with_root_path(
    root_path: &[u8],
    entry_path: &[u8],
) -> Vec<u8> {
    rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(TreeGroupPayload {
        reliability: GroupReliability {
            reliable: true,
            unreliable_reason: None,
        },
        stability: TreeStability::not_evaluated(),
        root: TreePageRoot {
            path: root_path.to_vec(),
            size: 0,
            modified_time_us: 1,
            is_dir: true,
            exists: true,
            has_children: true,
        },
        entries: vec![TreePageEntry {
            path: entry_path.to_vec(),
            size: 7,
            modified_time_us: 2,
            is_dir: false,
            has_children: false,
            depth: 1,
        }],
    }))
    .expect("encode real materialized tree payload with mismatched root path")
}

fn real_materialized_exact_file_payload_for_test(
    path: &[u8],
    size: u64,
    modified_time_us: u64,
) -> Vec<u8> {
    rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(TreeGroupPayload {
        reliability: GroupReliability {
            reliable: true,
            unreliable_reason: None,
        },
        stability: TreeStability::not_evaluated(),
        root: TreePageRoot {
            path: path.to_vec(),
            size,
            modified_time_us,
            is_dir: false,
            exists: true,
            has_children: false,
        },
        entries: Vec::new(),
    }))
    .expect("encode real materialized exact-file payload")
}

fn mk_source_record_event(origin: &str, path: &[u8], file_name: &[u8], ts: u64) -> Event {
    let record = FileMetaRecord::scan_update(
        path.to_vec(),
        file_name.to_vec(),
        UnixStat {
            is_dir: false,
            size: 1,
            mtime_us: ts,
            ctime_us: ts,
            dev: None,
            ino: None,
        },
        b"/".to_vec(),
        ts,
        false,
    );
    let payload = rmp_serde::to_vec_named(&record).expect("encode source record");
    Event::new(
        EventMetadata {
            origin_id: NodeId(origin.to_string()),
            timestamp_us: ts,
            logical_ts: None,
            correlation_id: None,
            ingress_auth: None,
            trace: None,
        },
        Bytes::from(payload),
    )
}

fn mk_control_event(origin: &str, control: ControlEvent, ts: u64) -> Event {
    let payload = rmp_serde::to_vec_named(&control).expect("encode control event");
    Event::new(
        EventMetadata {
            origin_id: NodeId(origin.to_string()),
            timestamp_us: ts,
            logical_ts: None,
            correlation_id: None,
            ingress_auth: None,
            trace: None,
        },
        Bytes::from(payload),
    )
}

fn origin_policy() -> ProjectionPolicy {
    ProjectionPolicy {
        member_grouping: MemberGroupingStrategy::ObjectRef,
        ..ProjectionPolicy::default()
    }
}

fn source_facade_with_group(group_id: &str, grants: &[GrantedMountRoot]) -> Arc<SourceFacade> {
    let mut root = RootSpec::new(group_id, "/unused");
    root.selector = crate::source::config::RootSelector {
        fs_type: Some("nfs".to_string()),
        ..crate::source::config::RootSelector::default()
    };
    root.subpath_scope = std::path::PathBuf::from("/");
    let config = SourceConfig {
        roots: vec![root],
        host_object_grants: grants.to_vec(),
        ..SourceConfig::default()
    };
    Arc::new(SourceFacade::local(Arc::new(
        FSMetaSource::with_boundaries(config, NodeId("source-node".into()), None)
            .expect("build source"),
    )))
}

fn source_observability_payload_for_runner_nodes(
    group_id: &str,
    grants: &[GrantedMountRoot],
    runner_nodes: &[&str],
) -> Vec<u8> {
    let primary = grants
        .iter()
        .find(|grant| {
            grant
                .object_ref
                .rsplit_once("::")
                .map(|(_, group)| group == group_id)
                .unwrap_or(false)
        })
        .map(|grant| grant.object_ref.clone())
        .unwrap_or_else(|| format!("{}::{group_id}", runner_nodes.first().unwrap_or(&"node-a")));
    rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.to_vec(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(1),
            logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
                root_id: group_id.to_string(),
                status: "ok".into(),
                matched_grants: runner_nodes.len(),
                active_members: runner_nodes.len(),
                coverage_mode: "realtime_hotset_plus_audit".into(),
            }],
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::from([(group_id.to_string(), primary)]),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: runner_nodes
            .iter()
            .map(|node| (node.to_string(), vec![group_id.to_string()]))
            .collect(),
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
    })
    .expect("encode source observability")
}

fn source_facade_with_roots(
    roots: Vec<RootSpec>,
    grants: &[GrantedMountRoot],
) -> Arc<SourceFacade> {
    let config = SourceConfig {
        roots,
        host_object_grants: grants.to_vec(),
        ..SourceConfig::default()
    };
    Arc::new(SourceFacade::local(Arc::new(
        FSMetaSource::with_boundaries(config, NodeId("source-node".into()), None)
            .expect("build source"),
    )))
}

fn sink_facade_with_group(grants: &[GrantedMountRoot]) -> Arc<SinkFacade> {
    let mut root = RootSpec::new("nfs1", "/unused");
    root.selector = crate::source::config::RootSelector {
        fs_type: Some("nfs".to_string()),
        ..crate::source::config::RootSelector::default()
    };
    root.subpath_scope = std::path::PathBuf::from("/");
    let config = SourceConfig {
        roots: vec![root],
        host_object_grants: grants.to_vec(),
        ..SourceConfig::default()
    };
    Arc::new(SinkFacade::local(Arc::new(
        SinkFileMeta::with_boundaries(NodeId("sink-node".into()), None, config)
            .expect("build sink"),
    )))
}

fn test_api_state_for_source(source: Arc<SourceFacade>, sink: Arc<SinkFacade>) -> ApiState {
    ApiState {
        backend: QueryBackend::Local { sink, source },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: None,
        readiness_sink: None,
        materialized_source_status_cache: Arc::new(Mutex::new(None)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        materialized_stats_cache: Arc::new(Mutex::new(None)),
        materialized_tree_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    }
}

fn test_api_state_for_route_source(
    source: Arc<SourceFacade>,
    sink: Arc<SinkFacade>,
    boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
) -> ApiState {
    ApiState {
        backend: QueryBackend::Route {
            sink,
            boundary,
            origin_id,
            source,
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: None,
        readiness_sink: None,
        materialized_source_status_cache: Arc::new(Mutex::new(None)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        materialized_stats_cache: Arc::new(Mutex::new(None)),
        materialized_tree_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    }
}

#[test]
fn trusted_materialized_root_tree_route_backend_does_not_fail_before_peer_status_fanout() {
    let source = source_facade_with_group("nfs1", &[]);
    let sink = sink_facade_with_group(&[]);
    let state = test_api_state_for_route_source(
        source,
        sink,
        Arc::new(LoopbackRouteBoundary::default()),
        NodeId("api-node".into()),
    );

    assert!(
        !trusted_materialized_root_tree_should_fail_closed_before_status_fanout(&state, b"/")
            .expect("pre-fanout trusted root tree check"),
        "routed facade must let peer sink-status fan-in decide global schedule absence instead of treating local empty sink schedule as authoritative"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialized_target_groups_excludes_unscheduled_request_source_groups_after_root_transition()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_nfs2 = tmp.path().join("node-a-nfs2");
    let node_a_nfs4 = tmp.path().join("node-a-nfs4");
    fs::create_dir_all(node_a_nfs2.join("live-layout")).expect("create node-a nfs2 dir");
    fs::create_dir_all(node_a_nfs4.join("retired-layout")).expect("create node-a nfs4 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_nfs2,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_nfs4,
            fs_source: "nfs4".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            crate::source::config::RootSpec::new("nfs2", "/unused"),
            crate::source::config::RootSpec::new("nfs4", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let state = test_api_state_for_source(source, sink);
    let request_source_status = SourceStatusSnapshot {
        current_stream_generation: Some(1),
        logical_roots: vec![
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs2".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs4".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
        ],
        concrete_roots: Vec::new(),
        degraded_roots: Vec::new(),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs2".to_string()],
        )]),
        groups: vec![sink_group_status("nfs2", true)],
        ..SinkStatusSnapshot::default()
    };

    let groups = materialized_target_groups(
        &state,
        None,
        Some(&request_source_status),
        Some(&request_sink_status),
        Duration::from_millis(250),
        true,
        MaterializedTargetGroupSelectionMode::Tree,
    )
    .await
    .expect("materialized target groups");

    assert_eq!(
        groups,
        vec!["nfs2"],
        "non-root materialized target groups must drop request-source groups that current sink scheduling no longer binds/serves"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialized_target_groups_preserves_unscheduled_request_source_groups_for_non_root_stats()
{
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(root_a.join("nfs1")).expect("create node-a nfs1 dir");
    fs::create_dir_all(root_a.join("nfs2")).expect("create node-a nfs2 dir");
    fs::create_dir_all(root_b.join("nfs3")).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.join("nfs1"),
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.join("nfs2"),
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b.join("nfs3"),
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let state = test_api_state_for_source(source, sink);
    let request_source_status = SourceStatusSnapshot {
        current_stream_generation: Some(1),
        logical_roots: vec![
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs2".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs3".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
        ],
        concrete_roots: Vec::new(),
        degraded_roots: Vec::new(),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    };

    let groups = materialized_target_groups(
        &state,
        None,
        Some(&request_source_status),
        Some(&request_sink_status),
        Duration::from_millis(250),
        true,
        MaterializedTargetGroupSelectionMode::Stats,
    )
    .await
    .expect("materialized target groups");

    assert_eq!(
        groups,
        vec!["nfs1", "nfs2", "nfs3"],
        "non-root materialized stats target groups must preserve unscheduled request-source groups so later groups can still render zero-groups"
    );
}

fn fs_meta_runtime_lib_filename() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "libfs_meta_runtime.dylib"
    }
    #[cfg(target_os = "windows")]
    {
        "fs_meta_runtime.dll"
    }
    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    {
        "libfs_meta_runtime.so"
    }
}

fn fs_meta_runtime_workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn fs_meta_worker_module_path_candidates(root: &Path, lib_name: &str) -> [PathBuf; 4] {
    [
        root.join("target/debug").join(lib_name),
        root.join("target/debug/deps").join(lib_name),
        root.join(".target/debug").join(lib_name),
        root.join(".target/debug/deps").join(lib_name),
    ]
}

fn newest_existing_worker_module_path(
    candidates: impl IntoIterator<Item = PathBuf>,
) -> Option<PathBuf> {
    let mut best: Option<(std::time::SystemTime, usize, PathBuf)> = None;
    for (index, candidate) in candidates.into_iter().enumerate() {
        let Ok(metadata) = std::fs::metadata(&candidate) else {
            continue;
        };
        let modified = metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let replace = match best.as_ref() {
            None => true,
            Some((best_modified, best_index, _)) => {
                modified > *best_modified || (modified == *best_modified && index < *best_index)
            }
        };
        if replace {
            best = Some((modified, index, candidate));
        }
    }
    best.map(|(_, _, path)| path)
}

fn fs_meta_worker_module_path() -> PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        for name in ["CAPANIX_FS_META_APP_BINARY", "DATANIX_FS_META_APP_SO"] {
            if let Ok(path) = std::env::var(name) {
                let resolved = PathBuf::from(path);
                if resolved.exists() {
                    return resolved;
                }
            }
        }
        newest_existing_worker_module_path(fs_meta_worker_module_path_candidates(
            &fs_meta_runtime_workspace_root(),
            fs_meta_runtime_lib_filename(),
        ))
        .unwrap_or_else(|| {
            panic!("fs-meta worker module not found; set CAPANIX_FS_META_APP_BINARY")
        })
    })
    .clone()
}

fn external_sink_worker_binding(socket_dir: &Path) -> RuntimeWorkerBinding {
    RuntimeWorkerBinding {
        role_id: "sink".to_string(),
        mode: WorkerMode::External,
        launcher_kind: RuntimeWorkerLauncherKind::WorkerHost,
        module_path: Some(fs_meta_worker_module_path()),
        socket_dir: Some(socket_dir.to_path_buf()),
    }
}

fn external_worker_root(id: &str, path: &Path) -> RootSpec {
    let mut root = RootSpec::new(id, path);
    root.watch = false;
    root.scan = true;
    root
}

include!("tests/status_stats.rs");

#[test]
fn mount_point_grouping_keeps_object_ref_opaque_outside_descriptor_match() {
    let policy = ProjectionPolicy {
        member_grouping: MemberGroupingStrategy::MountPoint,
        mount_point_by_object_ref: HashMap::from([
            ("nfs1".to_string(), "/tmp/workdir/nfs1".to_string()),
            ("nfs2".to_string(), "/mnt/storage/nfs2".to_string()),
            ("node-a::nfs2".to_string(), "/mnt/storage/nfs2".to_string()),
            (
                "/tmp/workdir/nfs1".to_string(),
                "/tmp/workdir/nfs1".to_string(),
            ),
        ]),
        ..ProjectionPolicy::default()
    };
    assert_eq!(policy.group_key_for_object_ref("nfs1"), "nfs1");
    assert_eq!(policy.group_key_for_object_ref("nfs2"), "nfs2");
    assert_eq!(
        policy.group_key_for_object_ref("/tmp/workdir/nfs1"),
        "/tmp/workdir/nfs1"
    );
    assert_eq!(
        policy.group_key_for_object_ref("node-a::nfs2"),
        "/mnt/storage/nfs2"
    );
    assert_eq!(
        policy.group_key_for_object_ref("/tmp/workdir/nfs3"),
        "/tmp/workdir/nfs3"
    );
}

#[test]
fn object_ref_grouping_preserves_utf8_and_distinguishes_normalization_forms() {
    let composed = "café-👩🏽‍💻";
    let decomposed = "cafe\u{301}-👩🏽‍💻";
    assert_ne!(composed, decomposed);

    let policy = ProjectionPolicy {
        member_grouping: MemberGroupingStrategy::ObjectRef,
        ..ProjectionPolicy::default()
    };

    assert_eq!(policy.group_key_for_object_ref(composed), composed);
    assert_eq!(policy.group_key_for_object_ref(decomposed), decomposed);
    assert_ne!(
        policy.group_key_for_object_ref(composed),
        policy.group_key_for_object_ref(decomposed)
    );
}

#[test]
fn normalized_record_path_for_query_rebases_prefixed_absolute_path() {
    let query_path = b"/qf-e2e-job";
    let record_path = b"/tmp/capanix/data/nfs1/qf-e2e-job/file-a.txt";
    let normalized = normalized_path_for_query(record_path, query_path).expect("normalize path");
    assert_eq!(normalized, b"/qf-e2e-job/file-a.txt".to_vec());
}

#[test]
fn node_id_from_object_ref_extracts_node_prefix() {
    assert_eq!(
        node_id_from_object_ref("node-a::nfs1").map(|id| id.0),
        Some("node-a".to_string())
    );
    assert!(node_id_from_object_ref("nfs1").is_none());
}

#[test]
fn force_find_runner_node_selection_rotates_across_group_members() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(&root_a).expect("create node-a dir");
    fs::create_dir_all(&root_b).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs1".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let state = test_api_state_for_source(source.clone(), sink);

    let first = crate::runtime_app::shared_tokio_runtime()
        .block_on(select_force_find_runner_node_for_group(
            &state,
            source.as_ref(),
            "nfs1",
        ))
        .expect("first selection")
        .expect("first node");
    let second = crate::runtime_app::shared_tokio_runtime()
        .block_on(select_force_find_runner_node_for_group(
            &state,
            source.as_ref(),
            "nfs1",
        ))
        .expect("second selection")
        .expect("second node");
    let third = crate::runtime_app::shared_tokio_runtime()
        .block_on(select_force_find_runner_node_for_group(
            &state,
            source.as_ref(),
            "nfs1",
        ))
        .expect("third selection")
        .expect("third node");

    assert_eq!(first.0, "node-a");
    assert_eq!(second.0, "node-b");
    assert_eq!(third.0, "node-a");
}

#[test]
fn route_force_find_runner_selection_uses_current_runner_binding_evidence() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    fs::create_dir_all(&root_a).expect("create node-a dir");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: root_a,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(1),
            logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".into(),
                status: "ok".into(),
                matched_grants: 2,
                active_members: 2,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            }],
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::from([("nfs1".to_string(), "node-a::nfs1".to_string())]),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::from([
            ("node-b".to_string(), vec!["nfs1".to_string()]),
            ("node-c".to_string(), vec!["nfs1".to_string()]),
        ]),
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
    })
    .expect("encode source observability");
    let boundary = Arc::new(ForceFindRunnerBindingStatusBoundary::new(
        source_status_payload,
    ));
    let state = test_api_state_for_route_source(
        source.clone(),
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );

    let first = crate::runtime_app::shared_tokio_runtime()
        .block_on(select_force_find_runner_node_for_group(
            &state,
            source.as_ref(),
            "nfs1",
        ))
        .expect("first selection")
        .expect("first node");
    let second = crate::runtime_app::shared_tokio_runtime()
        .block_on(select_force_find_runner_node_for_group(
            &state,
            source.as_ref(),
            "nfs1",
        ))
        .expect("second selection")
        .expect("second node");

    assert_eq!(first.0, "node-b");
    assert_eq!(second.0, "node-c");
    let source_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
        .expect("resolve source-status route");
    assert_eq!(boundary.send_batch_count(&source_status_route.0), 2);
}

#[test]
fn route_force_find_runner_selection_expands_partial_schedule_from_current_root_grants() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    let node_c_root = tmp.path().join("node-c");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    fs::create_dir_all(&node_b_root).expect("create node-b dir");
    fs::create_dir_all(&node_c_root).expect("create node-c dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs1".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-c::nfs1".to_string(),
            host_ref: "node-c".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_c_root,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let mut root = RootSpec::new("nfs1", "/unused");
    root.selector = crate::source::config::RootSelector {
        fs_type: Some("nfs".to_string()),
        ..crate::source::config::RootSelector::default()
    };
    let source = source_facade_with_roots(vec![root.clone()], &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: vec![root],
        status: SourceStatusSnapshot {
            current_stream_generation: Some(1),
            logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".into(),
                status: "ok".into(),
                matched_grants: 3,
                active_members: 3,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            }],
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::from([("nfs1".to_string(), "node-a::nfs1".to_string())]),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
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
    })
    .expect("encode source observability");
    let boundary = Arc::new(ForceFindRunnerBindingStatusBoundary::new(
        source_status_payload,
    ));
    let state = test_api_state_for_route_source(
        source.clone(),
        sink,
        boundary,
        NodeId("node-d".to_string()),
    );

    let first = crate::runtime_app::shared_tokio_runtime()
        .block_on(select_force_find_runner_node_for_group(
            &state,
            source.as_ref(),
            "nfs1",
        ))
        .expect("first selection")
        .expect("first node");
    let second = crate::runtime_app::shared_tokio_runtime()
        .block_on(select_force_find_runner_node_for_group(
            &state,
            source.as_ref(),
            "nfs1",
        ))
        .expect("second selection")
        .expect("second node");
    let third = crate::runtime_app::shared_tokio_runtime()
        .block_on(select_force_find_runner_node_for_group(
            &state,
            source.as_ref(),
            "nfs1",
        ))
        .expect("third selection")
        .expect("third node");

    assert_eq!(first.0, "node-a");
    assert_eq!(second.0, "node-b");
    assert_eq!(third.0, "node-c");
}

#[test]
fn route_force_find_runner_selection_expands_active_grants_from_root_health_when_root_snapshot_missing()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    let node_c_root = tmp.path().join("node-c");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    fs::create_dir_all(&node_b_root).expect("create node-b dir");
    fs::create_dir_all(&node_c_root).expect("create node-c dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs1".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-c::nfs1".to_string(),
            host_ref: "node-c".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_c_root,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(1),
            logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".into(),
                status: "ok".into(),
                matched_grants: 3,
                active_members: 3,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            }],
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::from([("nfs1".to_string(), "node-a::nfs1".to_string())]),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
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
    })
    .expect("encode source observability");
    let boundary = Arc::new(ForceFindRunnerBindingStatusBoundary::new(
        source_status_payload,
    ));
    let state = test_api_state_for_route_source(
        source.clone(),
        sink,
        boundary,
        NodeId("node-d".to_string()),
    );

    let first = crate::runtime_app::shared_tokio_runtime()
        .block_on(select_force_find_runner_node_for_group(
            &state,
            source.as_ref(),
            "nfs1",
        ))
        .expect("first selection")
        .expect("first node");
    let second = crate::runtime_app::shared_tokio_runtime()
        .block_on(select_force_find_runner_node_for_group(
            &state,
            source.as_ref(),
            "nfs1",
        ))
        .expect("second selection")
        .expect("second node");
    let third = crate::runtime_app::shared_tokio_runtime()
        .block_on(select_force_find_runner_node_for_group(
            &state,
            source.as_ref(),
            "nfs1",
        ))
        .expect("third selection")
        .expect("third node");

    assert_eq!(first.0, "node-a");
    assert_eq!(second.0, "node-b");
    assert_eq!(third.0, "node-c");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_force_find_waits_for_current_runner_binding_evidence() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root = tmp.path().join("node-a");
    fs::create_dir_all(root.join("force-find-stress")).expect("create node-a dir");
    fs::write(root.join("force-find-stress").join("seed.txt"), b"a").expect("seed node-a file");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: root,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let not_ready_source_status =
        source_observability_payload_for_runner_nodes("nfs1", &grants, &[]);
    let ready_source_status =
        source_observability_payload_for_runner_nodes("nfs1", &grants, &["node-a"]);
    let boundary = Arc::new(ForceFindDelayedRunnerBindingBoundary::new(vec![
        not_ready_source_status,
        ready_source_status,
    ]));
    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );

    let result = query_force_find_group_tree(
        &state,
        b"/force-find-stress",
        true,
        None,
        ForceFindSessionPlan::new(Duration::from_secs(2)).route_plan(),
        "nfs1",
        true,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "force-find should wait for current runner binding evidence before emitting group-unavailable; sends={} err={:?}",
        boundary.total_send_batch_count(),
        result.as_ref().err(),
    );
    assert!(
        boundary.total_send_batch_count() >= 3,
        "force-find should re-read runner binding evidence and then dispatch source-find"
    );
    assert_eq!(
        result
            .expect("force-find result")
            .iter()
            .map(|event| event.metadata().origin_id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["node-a::routed"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_force_find_route_uses_source_find_endpoint_for_chosen_node() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root = tmp.path().join("node-a");
    fs::create_dir_all(root.join("force-find-stress")).expect("create node-a dir");
    fs::write(root.join("force-find-stress").join("seed.txt"), b"a").expect("seed node-a file");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: root,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload =
        source_observability_payload_for_runner_nodes("nfs1", &grants, &["node-a"]);
    let boundary = Arc::new(LoopbackRouteBoundary::with_source_status_payload(
        source_status_payload,
    ));
    let route = source_find_request_route_for("node-a");
    let routed_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let routed_calls_for_handler = routed_calls.clone();
    let mut endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        route,
        "test-source-find-endpoint",
        CancellationToken::new(),
        move |requests| {
            let routed_calls = routed_calls_for_handler.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        routed_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        mk_event_with_correlation(
                            "node-a::routed",
                            req.metadata()
                                .correlation_id
                                .expect("source-find request correlation"),
                            vec![1, 2, 3],
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));

    let result = query_force_find_group_tree(
        &state,
        b"/force-find-stress",
        true,
        None,
        ForceFindSessionPlan::new(Duration::from_secs(2)).route_plan(),
        "nfs1",
        true,
        false,
    )
    .await
    .expect("selected-group force-find route query");

    assert_eq!(
        routed_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "selected-group routed force-find should use the internal source-find endpoint on the chosen node"
    );
    assert_eq!(
        result
            .iter()
            .map(|event| event.metadata().origin_id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["node-a::routed"]
    );

    endpoint.shutdown(Duration::from_secs(2)).await;
}

#[test]
fn force_find_runner_candidates_expand_from_active_grants_when_schedule_health_reports_only_primary()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: tmp.path().join("node-a"),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs1".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: tmp.path().join("node-b"),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-c::nfs1".to_string(),
            host_ref: "node-c".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: tmp.path().join("node-c"),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let snapshot: SourceObservabilitySnapshot = rmp_serde::from_slice(
        &source_observability_payload_for_runner_nodes("nfs1", &grants, &["node-a"]),
    )
    .expect("decode source observability");

    let candidates = force_find_candidate_nodes_from_runner_binding_evidence(&[snapshot], "nfs1")
        .into_iter()
        .map(|node| node.0)
        .collect::<Vec<_>>();

    assert_eq!(
        candidates,
        vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string()
        ],
        "active same-epoch grants must expand force-find runner candidates even when schedule/active-member debug evidence only reports the current primary"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_force_find_route_retries_protocol_violation_and_reaches_chosen_node() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root = tmp.path().join("node-a");
    fs::create_dir_all(root.join("force-find-stress")).expect("create node-a dir");
    fs::write(root.join("force-find-stress").join("seed.txt"), b"a").expect("seed node-a file");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: root,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload =
        source_observability_payload_for_runner_nodes("nfs1", &grants, &["node-a"]);
    let boundary = Arc::new(ForceFindProtocolRetryThenReplyBoundary::new(
        vec![1, 2, 3],
        source_status_payload,
    ));
    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );

    let result = query_force_find_group_tree(
        &state,
        b"/force-find-stress",
        true,
        None,
        ForceFindSessionPlan::new(Duration::from_secs(2)).route_plan(),
        "nfs1",
        true,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group force-find should retry a transient correlation mismatch on the chosen source-find route; total_request_send_batches={} total_reply_recv_batches={} err={:?}",
        boundary.total_send_batch_count(),
        boundary.total_recv_batch_count(),
        result.as_ref().err(),
    );
    assert!(
        boundary.total_send_batch_count() >= 2,
        "selected-group force-find retry should send the chosen source-find route at least twice"
    );
    assert!(
        boundary.total_recv_batch_count() >= 2,
        "selected-group force-find retry should observe at least one failed reply attempt before the successful one"
    );
    assert_eq!(
        result
            .expect("force-find route result")
            .iter()
            .map(|event| event.metadata().origin_id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["node-a::routed"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_force_find_route_reroutes_when_chosen_runner_reports_selected_group_missing()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(root_a.join("force-find-stress")).expect("create node-a dir");
    fs::create_dir_all(root_b.join("force-find-stress")).expect("create node-b dir");
    fs::write(root_a.join("force-find-stress").join("seed.txt"), b"a").expect("seed node-a file");
    fs::write(root_b.join("force-find-stress").join("seed.txt"), b"b").expect("seed node-b file");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs1".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload =
        source_observability_payload_for_runner_nodes("nfs1", &grants, &["node-a", "node-b"]);
    let boundary = Arc::new(ForceFindGroupMissingThenReplyBoundary::new(
        vec![7, 8, 9],
        source_status_payload,
    ));
    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );

    let result = query_force_find_group_tree(
        &state,
        b"/force-find-stress",
        true,
        None,
        ForceFindSessionPlan::new(Duration::from_secs(2)).route_plan(),
        "nfs1",
        true,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group force-find should reroute when the chosen runner reports selected_group missing; total_request_send_batches={} total_reply_recv_batches={} err={:?}",
        boundary.total_send_batch_count(),
        boundary.total_recv_batch_count(),
        result.as_ref().err(),
    );
    assert!(
        boundary.total_send_batch_count() >= 2,
        "selected-group force-find reroute should send the source-find route at least twice"
    );
    assert!(
        boundary.total_recv_batch_count() >= 2,
        "selected-group force-find reroute should observe one failed selected-group-missing reply before succeeding"
    );
    assert_eq!(
        result
            .expect("force-find route result")
            .iter()
            .map(|event| event.metadata().origin_id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["node-b::routed"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_force_find_route_replans_same_group_runner_when_host_fs_unavailable() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(root_a.join("force-find-stress")).expect("create node-a dir");
    fs::create_dir_all(root_b.join("force-find-stress")).expect("create node-b dir");
    fs::write(root_a.join("force-find-stress").join("seed.txt"), b"a").expect("seed node-a file");
    fs::write(root_b.join("force-find-stress").join("seed.txt"), b"b").expect("seed node-b file");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs1".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload =
        source_observability_payload_for_runner_nodes("nfs1", &grants, &["node-a", "node-b"]);
    let boundary = Arc::new(ForceFindHostUnavailableThenNextRunnerBoundary::new(
        vec![7, 8, 9],
        source_status_payload,
    ));
    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );

    let result = query_force_find_group_tree(
        &state,
        b"/force-find-stress",
        true,
        None,
        ForceFindSessionPlan::new(Duration::from_secs(2)).route_plan(),
        "nfs1",
        true,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group force-find should route HOST_FS_UNAVAILABLE to another current same-group runner instead of generic fallback; err={:?}",
        result.as_ref().err(),
    );
    assert_eq!(
        result
            .expect("force-find route result")
            .iter()
            .map(|event| event.metadata().origin_id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["node-b::nfs1"]
    );
    assert_eq!(
        boundary.send_batch_count(&boundary.generic_request_channel),
        0,
        "same-group HOST_FS_UNAVAILABLE reroute must not use generic fallback that can omit the requested group"
    );
    assert_eq!(
        boundary.send_batch_count(&source_find_request_route_for("node-a").0),
        1
    );
    assert_eq!(
        boundary.send_batch_count(&source_find_request_route_for("node-b").0),
        1
    );
    let runner_evidence = state.force_find_runner_evidence.snapshot();
    assert!(
        runner_evidence
            .get("nfs1")
            .is_some_and(|runners| runners.iter().any(|runner| runner == "node-b::nfs1")),
        "successful selected-runner force-find response must update API runner evidence; evidence={runner_evidence:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_force_find_route_falls_back_when_single_candidate_runner_keeps_reporting_selected_group_missing()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(root_b.join("force-find-stress")).expect("create node-b dir");
    fs::write(root_b.join("force-find-stress").join("seed.txt"), b"b").expect("seed node-b file");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-b::nfs2".to_string(),
        host_ref: "node-b".to_string(),
        host_ip: "10.0.0.2".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: root_b,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs2", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload =
        source_observability_payload_for_runner_nodes("nfs2", &grants, &["node-b"]);
    let boundary = Arc::new(
        ForceFindSingleCandidateGroupMissingThenFallbackBoundary::new(
            vec![4, 5, 6],
            source_status_payload,
        ),
    );
    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );

    let result = query_force_find_group_tree(
        &state,
        b"/force-find-stress",
        true,
        None,
        ForceFindSessionPlan::new(Duration::from_millis(450)).route_plan(),
        "nfs2",
        true,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group force-find should immediately fall back within the same retry window when the only chosen runner reports selected_group missing; total_request_send_batches={} retry_reissue_delay_ms={:?} err={:?}",
        boundary.total_send_batch_count(),
        boundary
            .retry_reissue_delay()
            .map(|delay| delay.as_millis()),
        result.as_ref().err(),
    );
    assert!(
        boundary.total_send_batch_count() >= 2,
        "single-candidate reroute should issue an immediate re-send after the retryable runner gap"
    );
    assert!(
        boundary.retry_reissue_delay().expect("retry reissue delay")
            < FORCE_FIND_ROUTE_RETRY_BACKOFF,
        "single-candidate reroute should reissue before the normal retry backoff kicks in"
    );
    assert_eq!(
        result
            .expect("force-find route result")
            .iter()
            .map(|event| event.metadata().origin_id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["node-d::fallback"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_force_find_fallback_collects_until_selected_group_payload_arrives() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(root_a.join("force-find-stress")).expect("create node-a dir");
    fs::create_dir_all(root_b.join("force-find-stress")).expect("create node-b dir");
    fs::write(root_a.join("force-find-stress").join("seed.txt"), b"a").expect("seed node-a file");
    fs::write(root_b.join("force-find-stress").join("seed.txt"), b"b").expect("seed node-b file");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs1".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload =
        source_observability_payload_for_runner_nodes("nfs1", &grants, &["node-a"]);
    let boundary = Arc::new(ForceFindSelectedGroupFallbackCollectBoundary::new(
        b"selected nfs1 payload".to_vec(),
        source_status_payload,
    ));
    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );

    let result = query_force_find_group_tree(
        &state,
        b"/force-find-stress",
        true,
        None,
        ForceFindSessionPlan::new(Duration::from_secs(2)).route_plan(),
        "nfs1",
        true,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group force-find fallback must collect until the selected group payload arrives; recv_batches={} err={:?}",
        boundary.total_recv_batch_count(),
        result.as_ref().err(),
    );
    assert_eq!(
        result
            .expect("force-find fallback result")
            .iter()
            .map(|event| event.metadata().origin_id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs2", "node-b::nfs1"],
        "fallback must not settle on an unrelated group-only reply"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_uses_sink_query_on_chosen_node() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root = tmp.path().join("node-a");
    fs::create_dir_all(root.join("force-find-stress")).expect("create node-a dir");
    fs::write(root.join("force-find-stress").join("seed.txt"), b"a").expect("seed node-a file");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: root,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(LoopbackRouteBoundary::default());
    let route = sink_query_request_route_for("node-a");
    let observed_request_nodes = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed_request_nodes_for_handler = observed_request_nodes.clone();
    let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(TreeGroupPayload {
        reliability: GroupReliability {
            reliable: true,
            unreliable_reason: None,
        },
        stability: TreeStability::not_evaluated(),
        root: TreePageRoot {
            path: b"/force-find-stress".to_vec(),
            size: 0,
            modified_time_us: 1,
            is_dir: true,
            exists: true,
            has_children: true,
        },
        entries: Vec::new(),
    }))
    .expect("encode materialized tree payload");
    let mut endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        route,
        "test-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| {
            let observed_request_nodes = observed_request_nodes_for_handler.clone();
            let payload = payload.clone();
            async move {
                let mut responses = Vec::new();
                observed_request_nodes
                    .lock()
                    .expect("request nodes lock")
                    .extend(
                        requests
                            .iter()
                            .map(|req| req.metadata().origin_id.0.clone()),
                    );
                for req in requests {
                    responses.push(mk_event_with_correlation(
                        "nfs1",
                        req.metadata()
                            .correlation_id
                            .expect("route request correlation"),
                        payload.to_vec(),
                    ));
                }
                responses
            }
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));

    let result = query_materialized_events(
        state.backend.clone(),
        build_materialized_tree_request(
            b"/force-find-stress",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs1".to_string()),
        ),
        Duration::from_secs(2),
    )
    .await
    .expect("selected-group materialized route query");

    assert_eq!(
        observed_request_nodes
            .lock()
            .expect("request nodes lock")
            .as_slice(),
        &["node-a".to_string()],
        "selected-group routed materialized query should call the internal sink-query endpoint as the chosen owner node"
    );
    assert_eq!(
        result
            .iter()
            .map(|event| event.metadata().origin_id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"]
    );

    endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_targets_owner_scoped_internal_route() {
    let generic_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY)
        .expect("resolve generic sink-query route");
    let owner_route = sink_query_request_route_for("node-a");
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let non_owner_groups = Arc::new(Mutex::new(Vec::<String>::new()));
    let owner_groups = Arc::new(Mutex::new(Vec::<String>::new()));
    let non_owner_groups_for_handler = non_owner_groups.clone();
    let owner_groups_for_handler = owner_groups.clone();

    let mut non_owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        generic_route.clone(),
        "test-nonowner-generic-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| {
            let non_owner_groups = non_owner_groups_for_handler.clone();
            async move {
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode non-owner internal query request");
                    non_owner_groups
                        .lock()
                        .expect("non-owner groups lock")
                        .push(
                            params
                                .scope
                                .selected_group
                                .clone()
                                .expect("selected group for non-owner request"),
                        );
                }
                Vec::new()
            }
        },
    );
    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-owner-scoped-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_groups = owner_groups_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner internal query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner request");
                    owner_groups
                        .lock()
                        .expect("owner groups lock")
                        .push(group_id.clone());
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("route request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
                responses
            }
        },
    );

    let result = route_materialized_events_via_node(
        boundary.clone(),
        NodeId("node-a".to_string()),
        build_materialized_tree_request(
            b"/force-find-stress",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs2".to_string()),
        ),
        SelectedGroupOwnerRoutePlan::new(Duration::from_millis(250)),
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group routed materialized query should use an owner-scoped internal route; generic_send_batches={} owner_send_batches={} non_owner_groups={:?} owner_groups={:?} err={:?}",
        boundary.send_batch_count(&generic_route.0),
        boundary.send_batch_count(&owner_route.0),
        non_owner_groups
            .lock()
            .expect("non-owner groups lock")
            .clone(),
        owner_groups.lock().expect("owner groups lock").clone(),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("selected-group route result"),
        &ProjectionPolicy::default(),
        "nfs2",
        b"/force-find-stress",
    )
    .expect("decode selected-group response");
    assert!(payload.root.exists);
    assert!(
        non_owner_groups
            .lock()
            .expect("non-owner groups lock")
            .is_empty(),
        "generic non-owner internal sink-query route should not receive selected-group owner-targeted requests"
    );
    assert_eq!(
        owner_groups.lock().expect("owner groups lock").as_slice(),
        &["nfs2".to_string()],
        "owner-scoped internal sink-query route should receive the selected group"
    );

    non_owner_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_waits_for_owner_payload_instead_of_settling_on_fast_empty_proxy_reply()
 {
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let internal_route = sink_query_request_route_for("node-a");
    let boundary = Arc::new(MaterializedRouteRaceBoundary::new(
        proxy_route.0.clone(),
        internal_route.0.clone(),
        "nfs1",
        b"/force-find-stress".to_vec(),
    ));

    let events = route_materialized_events_via_node(
        boundary.clone(),
        NodeId("node-a".to_string()),
        build_materialized_tree_request(
            b"/force-find-stress",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs1".to_string()),
        ),
        SelectedGroupOwnerRoutePlan::new(Duration::from_secs(2)),
    )
    .await
    .expect("route selected-group materialized query");

    let payload = decode_materialized_selected_group_response(
        &events,
        &ProjectionPolicy::default(),
        "nfs1",
        b"/force-find-stress",
    )
    .expect("decode selected-group materialized response");

    assert_eq!(
        boundary.sent_call_channel().as_deref(),
        Some(internal_route.0.as_str()),
        "selected-group materialized routing must bypass the fan-in proxy and use the internal sink-query route for the chosen owner"
    );
    assert!(
        payload.root.exists,
        "selected-group materialized route must wait for the chosen owner's real payload instead of settling on an earlier empty reply"
    );
}

#[test]
fn decode_materialized_selected_group_response_prefers_newer_empty_same_path_payload_over_older_richer_payload()
 {
    let older_rich = Event::new(
        EventMetadata {
            origin_id: NodeId("nfs2".to_string()),
            timestamp_us: 1,
            logical_ts: None,
            correlation_id: Some(11),
            ingress_auth: None,
            trace: None,
        },
        Bytes::from(real_materialized_tree_payload_for_test_with_root_path(
            b"/nested",
            b"/nested/peer.txt",
        )),
    );
    let newer_empty = Event::new(
        EventMetadata {
            origin_id: NodeId("nfs2".to_string()),
            timestamp_us: 2,
            logical_ts: None,
            correlation_id: Some(11),
            ingress_auth: None,
            trace: None,
        },
        Bytes::from(empty_materialized_tree_payload_for_test(b"/nested")),
    );

    let payload = decode_materialized_selected_group_response(
        &[older_rich, newer_empty],
        &ProjectionPolicy::default(),
        "nfs2",
        b"/nested",
    )
    .expect("decode same-path duplicate selected-group response");

    assert!(
        !payload.root.exists,
        "newer empty same-path payload should beat an older richer payload so stale subtree data does not reopen a current empty /nested result"
    );
    assert!(
        payload.entries.is_empty(),
        "newer empty same-path payload should clear stale entries from older duplicate responses"
    );
}

#[test]
fn decode_force_find_selected_group_response_fails_without_selected_group_payload() {
    let payload = rmp_serde::to_vec_named(&ForceFindQueryPayload::Tree(TreeGroupPayload {
        reliability: GroupReliability {
            reliable: true,
            unreliable_reason: None,
        },
        stability: TreeStability::not_evaluated(),
        root: TreePageRoot {
            path: b"/force-find-stress".to_vec(),
            size: 0,
            modified_time_us: 1,
            is_dir: true,
            exists: true,
            has_children: true,
        },
        entries: Vec::new(),
    }))
    .expect("encode force-find payload");
    let events = vec![mk_event("nfs2", payload)];

    let err = decode_force_find_selected_group_response(
        &events,
        &ProjectionPolicy::default(),
        "nfs1",
        b"/force-find-stress",
    )
    .expect_err("missing selected-group payload must fail closed");

    assert!(
        err.to_string()
            .contains("force-find returned no selected group payload: nfs1"),
        "missing selected-group payload must not be rendered as an empty successful group: {err}"
    );
}

#[test]
fn decode_force_find_selected_group_response_prefers_non_empty_runner_payload() {
    let empty_payload = rmp_serde::to_vec_named(&ForceFindQueryPayload::Tree(TreeGroupPayload {
        reliability: GroupReliability {
            reliable: true,
            unreliable_reason: None,
        },
        stability: TreeStability::not_evaluated(),
        root: TreePageRoot {
            path: b"/force-find-stress".to_vec(),
            size: 0,
            modified_time_us: 0,
            is_dir: true,
            exists: false,
            has_children: false,
        },
        entries: Vec::new(),
    }))
    .expect("encode empty force-find payload");
    let rich_payload = rmp_serde::to_vec_named(&ForceFindQueryPayload::Tree(TreeGroupPayload {
        reliability: GroupReliability {
            reliable: true,
            unreliable_reason: None,
        },
        stability: TreeStability::not_evaluated(),
        root: TreePageRoot {
            path: b"/force-find-stress".to_vec(),
            size: 0,
            modified_time_us: 7,
            is_dir: true,
            exists: true,
            has_children: true,
        },
        entries: vec![TreePageEntry {
            path: b"/force-find-stress/file.txt".to_vec(),
            size: 4,
            modified_time_us: 9,
            is_dir: false,
            has_children: false,
            depth: 1,
        }],
    }))
    .expect("encode rich force-find payload");
    let events = vec![
        mk_event("nfs1", empty_payload),
        mk_event("nfs1", rich_payload),
    ];

    let payload = decode_force_find_selected_group_response(
        &events,
        &ProjectionPolicy::default(),
        "nfs1",
        b"/force-find-stress",
    )
    .expect("decode selected-group force-find response");

    assert!(
        payload.root.exists,
        "force-find fallback collect must not let an empty runner payload hide richer fresh evidence from another runner"
    );
    assert_eq!(payload.entries.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_falls_back_to_generic_proxy_when_owner_route_stale_grant_attachment_access_denied()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    fs::create_dir_all(node_b_root.join("layout-b")).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs4".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let owner_route = sink_query_request_route_for("node-b");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let boundary = Arc::new(MaterializedRouteAccessDeniedThenProxyBoundary::new(
        owner_route.0.clone(),
        sink_status_payload,
    ));

    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-proxy-fallback-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(1200),
        Some(selected_group_sink_status),
        true,
        true,
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group materialized route should fall back to generic proxy when the owner-scoped route hits a stale drained/fenced grant-attachment error; owner_send_batches={} proxy_send_batches={} err={:?}",
        boundary.send_batch_count(&owner_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("selected-group fallback result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/",
    )
    .expect("decode selected-group fallback response");
    assert!(payload.root.exists);
    assert_eq!(boundary.send_batch_count(&owner_route.0), 1);
    assert_eq!(boundary.send_batch_count(&proxy_route.0), 1);

    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_trusted_materialized_route_falls_back_to_generic_proxy_when_owner_route_missing_channel_buffer_state()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    fs::create_dir_all(node_b_root.join("layout-b")).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs4".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let owner_route = sink_query_request_route_for("node-b");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let boundary = Arc::new(MaterializedRouteMissingRouteStateThenProxyBoundary::new(
        owner_route.0.clone(),
        sink_status_payload,
    ));

    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-proxy-missing-route-state-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/layout-b",
            true,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(1200),
        Some(selected_group_sink_status),
        true,
        true,
    )
    .await;

    assert!(
        result.is_ok(),
        "trusted-materialized selected-group route should fall back to generic proxy when owner-scoped route loses channel_buffer state; owner_send_batches={} proxy_send_batches={} err={:?}",
        boundary.send_batch_count(&owner_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("selected-group missing-route-state fallback result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/layout-b",
    )
    .expect("decode selected-group missing-route-state fallback response");
    assert!(payload.root.exists);
    assert_eq!(boundary.send_batch_count(&owner_route.0), 1);
    assert_eq!(boundary.send_batch_count(&proxy_route.0), 1);

    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_materialized_events_via_generic_proxy_retries_missing_channel_buffer_state() {
    let boundary = Arc::new(MaterializedProxyMissingRouteStateThenReplyBoundary::new(
        real_materialized_tree_payload_for_test(b"/"),
    ));
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");

    let result = query_materialized_events_via_generic_proxy(
        boundary.clone(),
        NodeId("node-d".to_string()),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        TreePitProxyRoutePlan::new(Duration::from_millis(1200)).machine(),
    )
    .await;

    assert!(
        result.is_ok(),
        "generic proxy should retry a transient missing channel_buffer route-state continuity gap; proxy_send_batches={} err={:?}",
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("generic proxy retry result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/",
    )
    .expect("decode generic proxy retry response");
    assert!(payload.root.exists);
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        2,
        "generic proxy should retry once after the missing channel_buffer state continuity gap"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_materialized_events_via_generic_proxy_accepts_immediate_reply_with_timeout_smaller_than_idle_grace()
 {
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");

    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-generic-proxy-immediate-reply-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for proxy request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );

    let result = query_materialized_events_via_generic_proxy(
        boundary.clone(),
        NodeId("node-d".to_string()),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        TreePitProxyRoutePlan::new(TRUSTED_READY_SELECTED_GROUP_RETRY_BUDGET).machine(),
    )
    .await;

    assert!(
        result.is_ok(),
        "generic proxy should accept an immediate correlated reply even when the timeout budget is smaller than the idle grace; proxy_send_batches={} err={:?}",
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("generic proxy immediate reply result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/",
    )
    .expect("decode generic proxy immediate reply response");
    assert!(payload.root.exists);
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "generic proxy should settle on the first immediate reply"
    );

    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_request_scoped_materialized_sink_status_snapshot_accepts_immediate_reply_with_timeout_smaller_than_status_idle_grace()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_b_root.join("layout-b")).expect("create node-b dir");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-b::nfs4".to_string(),
        host_ref: "node-b".to_string(),
        host_ip: "10.0.0.2".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: node_b_root.clone(),
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![sink_group_status("nfs4", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");

    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_route.clone(),
        "test-request-scoped-sink-status-immediate-reply-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "nfs4",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );

    let state = ApiState {
        backend: QueryBackend::Route {
            sink: sink.clone(),
            boundary: boundary.clone(),
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink),
        materialized_source_status_cache: Arc::new(Mutex::new(None)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        materialized_stats_cache: Arc::new(Mutex::new(None)),
        materialized_tree_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let result = load_request_scoped_materialized_sink_status_snapshot(
        &state,
        TreePitSessionPlan::new(Duration::from_millis(250), 1).request_scoped_sink_status_plan(),
    )
    .await;

    assert!(
        result.is_some(),
        "request-scoped sink-status load should accept an immediate correlated reply even when the request budget is smaller than the status idle grace; sink_send_batches={}",
        boundary.send_batch_count(&sink_route.0),
    );
    let snapshot = result.expect("request-scoped sink-status snapshot");
    assert_eq!(snapshot.groups.len(), 1);
    assert_eq!(snapshot.groups[0].group_id, "nfs4");
    assert_eq!(
        boundary.send_batch_count(&sink_route.0),
        1,
        "request-scoped sink-status load should settle on the first immediate reply"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_materialized_events_via_generic_proxy_does_not_retry_protocol_violation_until_timeout()
 {
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");

    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-generic-proxy-protocol-violation-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for proxy request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation")
                            + 1,
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );

    let result = query_materialized_events_via_generic_proxy(
        boundary.clone(),
        NodeId("node-d".to_string()),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        TreePitProxyRoutePlan::new(Duration::from_millis(450)).machine(),
    )
    .await;

    assert!(
        matches!(result, Err(CnxError::ProtocolViolation(_))),
        "generic proxy should surface protocol-violating replies immediately instead of retrying until timeout; proxy_send_batches={} result={result:?}",
        boundary.send_batch_count(&proxy_route.0),
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "generic proxy should not retry after a protocol-violating reply"
    );

    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_materialized_events_via_generic_proxy_does_not_hold_delayed_reply_open_for_full_idle_grace()
 {
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");

    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-generic-proxy-bounded-idle-grace-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            tokio::time::sleep(Duration::from_millis(700)).await;
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for proxy request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );

    let result = tokio::time::timeout(
        Duration::from_millis(950),
        query_materialized_events_via_generic_proxy(
            boundary.clone(),
            NodeId("node-d".to_string()),
            build_materialized_tree_request(
                b"/",
                true,
                None,
                ReadClass::TrustedMaterialized,
                Some("nfs4".to_string()),
            ),
            TreePitProxyRoutePlan::new(Duration::from_millis(1150)).machine(),
        ),
    )
    .await;

    assert!(
        result.is_ok(),
        "generic proxy should not hold a delayed correlated reply open behind the full idle grace when the overall route timeout is 1150ms"
    );
    let payload = decode_materialized_selected_group_response(
        &result
            .expect("generic proxy should settle before the bounded wall clock")
            .expect("generic proxy delayed reply result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/",
    )
    .expect("decode generic proxy bounded-idle response");
    assert!(payload.root.exists);

    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_materialized_events_via_node_accepts_immediate_reply_with_timeout_smaller_than_idle_grace()
 {
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let owner_route = sink_query_request_route_for("node-a");

    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-owner-route-immediate-reply-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );

    let result = route_materialized_events_via_node(
        boundary.clone(),
        NodeId("node-a".to_string()),
        build_materialized_tree_request(
            b"/data",
            false,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        SelectedGroupOwnerRoutePlan::new(TRUSTED_READY_LATER_RANKED_NON_ROOT_RETRY_BUDGET),
    )
    .await;

    assert!(
        result.is_ok(),
        "owner-scoped route should accept an immediate correlated reply even when timeout is smaller than idle grace; owner_send_batches={} err={:?}",
        boundary.send_batch_count(&owner_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("owner route immediate reply result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/data",
    )
    .expect("decode owner route immediate reply response");
    assert!(payload.root.exists);
    assert_eq!(
        boundary.send_batch_count(&owner_route.0),
        1,
        "owner-scoped route should settle on the first immediate reply"
    );

    owner_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_materialized_events_via_node_does_not_hold_immediate_reply_open_for_full_idle_grace()
{
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let owner_route = sink_query_request_route_for("node-a");

    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-owner-route-bounded-idle-grace-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );

    let result = tokio::time::timeout(
        Duration::from_millis(250),
        route_materialized_events_via_node(
            boundary.clone(),
            NodeId("node-a".to_string()),
            build_materialized_tree_request(
                b"/data",
                false,
                None,
                ReadClass::TrustedMaterialized,
                Some("nfs4".to_string()),
            ),
            SelectedGroupOwnerRoutePlan::new(Duration::from_millis(800)),
        ),
    )
    .await;

    assert!(
        result.is_ok(),
        "owner-scoped route should not hold an immediate correlated reply open behind the full idle grace when the overall route timeout is 800ms"
    );
    let payload = decode_materialized_selected_group_response(
        &result
            .expect("owner route should settle before the bounded wall clock")
            .expect("owner route immediate reply result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/data",
    )
    .expect("decode owner route bounded-idle response");
    assert!(payload.root.exists);

    owner_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_falls_back_to_generic_proxy_when_owner_route_times_out()
{
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    fs::create_dir_all(node_b_root.join("layout-b")).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs4".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let owner_route = sink_query_request_route_for("node-b");

    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-proxy-fallback-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(1200),
        Some(selected_group_sink_status),
        true,
        true,
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group materialized route should fall back to generic proxy when the owner-scoped route times out; owner_send_batches={} proxy_send_batches={} err={:?}",
        boundary.send_batch_count(&owner_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("selected-group fallback result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/",
    )
    .expect("decode selected-group fallback response");
    assert!(payload.root.exists);
    assert_eq!(
        boundary.send_batch_count(&owner_route.0),
        1,
        "owner-scoped route should still be attempted first"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "generic proxy route should be used once after the owner-scoped timeout"
    );

    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_trusted_materialized_route_reserves_proxy_budget_after_owner_timeout() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    fs::create_dir_all(node_b_root.join("layout-b")).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs4".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let owner_route = sink_query_request_route_for("node-b");

    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-proxy-budget-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/layout-b",
            true,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        MATERIALIZED_ROUTE_COLLECT_IDLE_GRACE,
        Some(selected_group_sink_status),
        true,
        true,
    )
    .await;

    assert!(
        result.is_ok(),
        "trusted-materialized selected-group fallback must reserve proxy budget even when the owner route times out near the collect grace edge; owner_send_batches={} proxy_send_batches={} err={:?}",
        boundary.send_batch_count(&owner_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("trusted-materialized fallback result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/layout-b",
    )
    .expect("decode trusted-materialized fallback response");
    assert!(payload.root.exists);
    assert_eq!(
        boundary.send_batch_count(&owner_route.0),
        1,
        "owner-scoped route should still be attempted first"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "generic proxy route should still be attempted once after the owner timeout"
    );

    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_trusted_materialized_route_reserves_enough_proxy_budget_for_delayed_proxy_reply_after_owner_timeout()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    fs::create_dir_all(node_b_root.join("layout-b")).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs4".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let owner_route = sink_query_request_route_for("node-b");

    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-delayed-proxy-budget-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            tokio::time::sleep(Duration::from_millis(650)).await;
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 7,
            live_nodes: 6,
            tombstoned_count: 1,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/layout-b",
            true,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(1600),
        Some(selected_group_sink_status),
        true,
        true,
    )
    .await;

    assert!(
        result.is_ok(),
        "trusted-materialized selected-group route should leave enough proxy budget for a delayed proxy reply after the owner route times out; owner_send_batches={} proxy_send_batches={} err={:?}",
        boundary.send_batch_count(&owner_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("delayed proxy fallback result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/layout-b",
    )
    .expect("decode delayed proxy fallback response");
    assert!(payload.root.exists);
    assert_eq!(boundary.send_batch_count(&owner_route.0), 1);
    assert_eq!(boundary.send_batch_count(&proxy_route.0), 1);

    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_trusted_materialized_route_preserves_timeout_when_owner_and_proxy_both_stall_for_later_ranked_root_group()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    fs::create_dir_all(node_b_root.join("layout-b")).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs4".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let owner_route = sink_query_request_route_for("node-b");
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(1200),
        Some(selected_group_sink_status),
        true,
        false,
    )
    .await;

    assert!(
        matches!(result, Err(CnxError::Timeout)),
        "later-ranked trusted root owner/proxy stalls must preserve the raw timeout instead of flattening it to not-ready; owner_send_batches={} proxy_send_batches={} result={result:?}",
        boundary.send_batch_count(&owner_route.0),
        boundary.send_batch_count(&proxy_route.0),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_fails_closed_when_owner_and_proxy_both_return_empty_tree_for_ready_group()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    fs::create_dir_all(node_b_root.join("layout-b")).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs4".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let owner_route = sink_query_request_route_for("node-b");

    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-empty-owner-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode owner query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for owner request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("owner request correlation"),
                    empty_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-empty-proxy-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    empty_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 7,
            live_nodes: 6,
            tombstoned_count: 1,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(1200),
        Some(selected_group_sink_status),
        true,
        true,
    )
    .await;

    assert!(
        matches!(result, Err(CnxError::NotReady(_))),
        "trusted-materialized ready group must fail closed when both owner and generic proxy return empty trees; owner_send_batches={} proxy_send_batches={} result={result:?}",
        boundary.send_batch_count(&owner_route.0),
        boundary.send_batch_count(&proxy_route.0),
    );

    owner_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_falls_back_to_generic_proxy_when_owner_route_returns_empty_tree_for_ready_group()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    fs::create_dir_all(node_b_root.join("layout-b")).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs4".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let owner_route = sink_query_request_route_for("node-b");

    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-empty-owner-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode owner query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for owner request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("owner request correlation"),
                    empty_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-proxy-fallback-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 7,
            live_nodes: 6,
            tombstoned_count: 1,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(1200),
        Some(selected_group_sink_status),
        true,
        true,
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group materialized route should fall back to generic proxy when the chosen owner returns an empty tree despite ready sink status; owner_send_batches={} proxy_send_batches={} err={:?}",
        boundary.send_batch_count(&owner_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("selected-group empty-owner fallback result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/",
    )
    .expect("decode selected-group empty-owner fallback response");
    assert!(
        payload.root.exists,
        "ready sink status with live materialized nodes must not let an empty owner-tree reply settle `/tree` without proxy fallback"
    );
    assert_eq!(
        boundary.send_batch_count(&owner_route.0),
        1,
        "owner-scoped route should still be attempted first"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "generic proxy route should be used once after the empty owner-tree reply"
    );

    owner_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_settles_request_scoped_omitted_ready_root_group_as_empty_tree()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs4".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: node_a_root,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));

    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs5".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs5".to_string(),
            primary_object_ref: "node-b::nfs5".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };
    let omitted_ready_groups = BTreeSet::from(["nfs4".to_string()]);

    let result =
        query_materialized_events_with_selected_group_owner_snapshot_and_request_scoped_omissions(
            &state,
            &ProjectionPolicy::default(),
            build_materialized_tree_request(
                b"/",
                true,
                None,
                ReadClass::TrustedMaterialized,
                Some("nfs4".to_string()),
            ),
            Duration::from_millis(1200),
            Some(selected_group_sink_status),
            Some(&omitted_ready_groups),
            TreePitSessionPlan::new(Duration::from_millis(1200), 1).selected_group_stage_plan(
                TreePitGroupPlanInput {
                    read_class: ReadClass::TrustedMaterialized,
                    observation_state: ObservationState::TrustedMaterialized,
                    selected_group_sink_reports_live_materialized: true,
                    prior_materialized_group_decoded: false,
                    prior_materialized_exact_file_decoded: false,
                    rank_index: 0,
                    is_last_ranked_group: true,
                    selected_group_sink_unready_empty: false,
                    empty_root_requires_fail_closed: false,
                },
            ),
        )
        .await;

    assert!(
        result.is_ok(),
        "request-scoped omitted ready selected-group root query should settle as a synthetic empty tree instead of plain empty/no-payload: err={:?}",
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("request-scoped omitted ready selected-group result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/",
    )
    .expect("decode request-scoped omitted ready selected-group response");
    assert!(
        !payload.root.exists,
        "request-scoped omitted ready selected-group root query must synthesize an empty tree payload"
    );
    assert_eq!(
        payload.root.path,
        b"/".to_vec(),
        "synthetic empty selected-group tree should preserve the root request path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_falls_back_to_generic_proxy_after_first_ranked_trusted_non_root_empty_owner_retry()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_b_root.join("nested")).expect("create node-b nested dir");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-b::nfs4".to_string(),
        host_ref: "node-b".to_string(),
        host_ip: "10.0.0.2".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: node_b_root.clone(),
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_roots(vec![RootSpec::new("nfs4", &node_b_root)], &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let owner_route = sink_query_request_route_for("node-b");
    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-empty-owner-non-root-defer-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode owner query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for owner request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("owner request correlation"),
                    empty_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-empty-owner-non-root-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/nested/child/deep.txt",
            true,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(1200),
        Some(selected_group_sink_status),
        true,
        true,
    )
    .await;

    assert!(
        result.is_ok(),
        "first-ranked trusted non-root empty-owner lane should keep going to generic proxy after the bounded owner retry; owner_send_batches={} proxy_send_batches={} err={:?}",
        boundary.send_batch_count(&owner_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("first-ranked trusted non-root proxy fallback result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/nested/child/deep.txt",
    )
    .expect("decode first-ranked trusted non-root proxy fallback response");
    assert!(
        payload.root.exists,
        "first-ranked trusted non-root empty-owner lane must not let an empty owner retry settle when generic proxy can still recover the selected path"
    );
    assert_eq!(
        boundary.send_batch_count(&owner_route.0),
        2,
        "first-ranked trusted non-root empty-owner lane should consume one owner retry before using generic proxy"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "first-ranked trusted non-root empty-owner lane should spend generic proxy exactly once after the bounded owner retry stays empty"
    );

    owner_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_reroutes_to_sink_primary_object_ref_when_scheduled_owner_returns_empty_tree_for_ready_group()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    fs::create_dir_all(node_b_root.join("layout-b")).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs4".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let node_a_route = sink_query_request_route_for("node-a");
    let node_b_route = sink_query_request_route_for("node-b");

    let mut stale_owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_b_route.clone(),
        "test-stale-scheduled-owner-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-b query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-b request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("node-b request correlation"),
                    empty_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );
    let mut primary_owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-primary-owner-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-a query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-a request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("node-a request correlation"),
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-empty-generic-proxy-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    empty_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-a::nfs4".to_string(),
            total_nodes: 7,
            live_nodes: 6,
            tombstoned_count: 1,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(1200),
        Some(selected_group_sink_status),
        true,
        true,
    )
    .await;

    assert!(
        result.is_ok(),
        "trusted-materialized selected-group route should reroute to the sink primary owner when the scheduled owner returns an empty tree but sink status still reports a different ready primary; node_b_calls={} node_a_calls={} proxy_calls={} err={:?}",
        boundary.send_batch_count(&node_b_route.0),
        boundary.send_batch_count(&node_a_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("selected-group primary-owner reroute result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/",
    )
    .expect("decode selected-group primary-owner reroute response");
    assert!(
        payload.root.exists,
        "ready sink status with a different primary_object_ref must not settle an empty scheduled-owner tree when the primary owner can still materialize `/`"
    );
    assert_eq!(
        boundary.send_batch_count(&node_b_route.0),
        1,
        "stale scheduled owner should still be attempted first"
    );
    assert_eq!(
        boundary.send_batch_count(&node_a_route.0),
        1,
        "sink primary object ref should receive one rerouted selected-group request"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        0,
        "generic proxy should not be needed when the sink primary owner can satisfy the reroute"
    );

    stale_owner_endpoint.shutdown(Duration::from_secs(2)).await;
    primary_owner_endpoint
        .shutdown(Duration::from_secs(2))
        .await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_reroutes_to_sink_primary_object_ref_when_scheduled_owner_returns_empty_non_root_tree_for_ready_group()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_a_root.join("nested")).expect("create node-a dir");
    fs::create_dir_all(node_b_root.join("nested")).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs4".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let node_a_route = sink_query_request_route_for("node-a");
    let node_b_route = sink_query_request_route_for("node-b");

    let mut stale_owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_b_route.clone(),
        "test-stale-scheduled-owner-non-root-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-b query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-b request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("node-b request correlation"),
                    empty_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );
    let mut primary_owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-primary-owner-non-root-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-a query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-a request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("node-a request correlation"),
                    real_materialized_tree_payload_with_entries_for_test(
                        &params.scope.path,
                        &[b"/nested/peer.txt"],
                    ),
                ));
            }
            responses
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-empty-generic-proxy-non-root-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    empty_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-a::nfs4".to_string(),
            total_nodes: 7,
            live_nodes: 6,
            tombstoned_count: 1,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/nested",
            true,
            Some(1),
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(1200),
        Some(selected_group_sink_status),
        true,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "trusted-materialized selected-group route should reroute to the sink primary owner when the scheduled owner returns an empty non-root tree but sink status still reports a different ready primary; node_b_calls={} node_a_calls={} proxy_calls={} err={:?}",
        boundary.send_batch_count(&node_b_route.0),
        boundary.send_batch_count(&node_a_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("selected-group primary-owner reroute result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/nested",
    )
    .expect("decode selected-group primary-owner reroute response");
    assert!(
        payload.root.exists,
        "ready sink status with a different primary_object_ref must not settle an empty scheduled-owner tree when the primary owner can still materialize `/nested`"
    );
    assert_eq!(
        payload.entries.len(),
        1,
        "primary owner reroute should preserve the non-root subtree entry payload"
    );
    assert_eq!(
        boundary.send_batch_count(&node_b_route.0),
        1,
        "stale scheduled owner should still be attempted first"
    );
    assert_eq!(
        boundary.send_batch_count(&node_a_route.0),
        1,
        "sink primary object ref should receive one rerouted selected-group request for the non-root subtree"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        0,
        "generic proxy should not be needed when the sink primary owner can satisfy the non-root reroute"
    );

    stale_owner_endpoint.shutdown(Duration::from_secs(2)).await;
    primary_owner_endpoint
        .shutdown(Duration::from_secs(2))
        .await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

include!("tests/selected_group_owner.rs");

include!("tests/pit_public.rs");
