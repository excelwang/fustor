use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::time::Duration;

#[cfg(test)]
use std::sync::atomic::AtomicBool;

use capanix_app_sdk::runtime::RouteKey;
use capanix_app_sdk::{CnxError, Event};
use capanix_runtime_entry_sdk::advanced::boundary::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use crate::runtime::routes::ROUTE_KEY_SINK_QUERY_INTERNAL;
#[cfg(test)]
use crate::runtime::routes::sink_query_request_route_for;

#[cfg(test)]
const ENDPOINT_READY_WAIT_TIMEOUT: Duration = Duration::from_millis(50);
const ENDPOINT_RETRY_LOG_INTERVAL: Duration = Duration::from_secs(1);
const REQUEST_RETRYABLE_RECV_GAP_RETRY_LIMIT: usize = 12;
const REQUEST_REPLY_SEND_RETRY_LIMIT: usize = 12;
const REQUEST_REPLY_SEND_RETRY_DELAY: Duration = Duration::from_millis(50);
const STREAM_STALE_RECV_GAP_RETRY_LIMIT: usize = 12;
const IDLE_RECV_QUIET_WINDOW: Duration = Duration::from_millis(1_500);

struct RecvEntryObservedBoundary<H> {
    inner: Arc<dyn ChannelIoSubset>,
    before_recv: Arc<H>,
}

struct RecvPendingObservedBoundary<H> {
    inner: Arc<dyn ChannelIoSubset>,
    on_pending_recv: Arc<H>,
}

#[async_trait::async_trait]
impl<H> ChannelIoSubset for RecvEntryObservedBoundary<H>
where
    H: Fn() + Send + Sync + 'static,
{
    async fn channel_send(
        &self,
        ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        self.inner.channel_send(ctx, request).await
    }

    async fn channel_recv(
        &self,
        ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut inner = Box::pin(self.inner.channel_recv(ctx, request));
        let before_recv = self.before_recv.clone();
        let mut observed = false;
        futures_util::future::poll_fn(move |cx| {
            let poll = inner.as_mut().poll(cx);
            if !observed {
                observed = true;
                (before_recv)();
            }
            poll
        })
        .await
    }

    fn channel_close(
        &self,
        ctx: BoundaryContext,
        channel: ChannelKey,
    ) -> capanix_app_sdk::Result<()> {
        self.inner.channel_close(ctx, channel)
    }
}

#[async_trait::async_trait]
impl<H> ChannelIoSubset for RecvPendingObservedBoundary<H>
where
    H: Fn() + Send + Sync + 'static,
{
    async fn channel_send(
        &self,
        ctx: BoundaryContext,
        request: ChannelSendRequest,
    ) -> capanix_app_sdk::Result<()> {
        self.inner.channel_send(ctx, request).await
    }

    async fn channel_recv(
        &self,
        ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> capanix_app_sdk::Result<Vec<Event>> {
        let mut inner = Box::pin(self.inner.channel_recv(ctx, request));
        let on_pending_recv = self.on_pending_recv.clone();
        let mut observed = false;
        futures_util::future::poll_fn(move |cx| {
            let poll = inner.as_mut().poll(cx);
            if !observed && poll.is_pending() {
                observed = true;
                (on_pending_recv)();
            }
            poll
        })
        .await
    }

    fn channel_close(
        &self,
        ctx: BoundaryContext,
        channel: ChannelKey,
    ) -> capanix_app_sdk::Result<()> {
        self.inner.channel_close(ctx, channel)
    }
}

fn debug_source_status_lifecycle_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_SOURCE_STATUS_LIFECYCLE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn debug_stream_delivery_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_STREAM_DELIVERY")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn debug_materialized_route_lifecycle_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_MATERIALIZED_ROUTE_LIFECYCLE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn debug_endpoint_request_lifecycle_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_ENDPOINT_REQUEST_LIFECYCLE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn is_materialized_internal_query_route(route: &RouteKey) -> bool {
    let Some(request_route) = route.0.strip_suffix(".req") else {
        return false;
    };
    let Some((stem, version)) = ROUTE_KEY_SINK_QUERY_INTERNAL.rsplit_once(':') else {
        return request_route == ROUTE_KEY_SINK_QUERY_INTERNAL
            || request_route.starts_with(&format!("{ROUTE_KEY_SINK_QUERY_INTERNAL}."));
    };
    if request_route == ROUTE_KEY_SINK_QUERY_INTERNAL {
        return true;
    }
    let Some(route_stem) = request_route.strip_suffix(&format!(":{version}")) else {
        return false;
    };
    route_stem.starts_with(&format!("{stem}."))
}

fn is_stale_grant_attachment_recv_gap(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::AccessDenied(message) | CnxError::PeerError(message)
            if (message.contains("drained/fenced")
                && message.contains("grant attachments"))
                || message.contains("invalid or revoked grant attachment token")
    )
}

fn is_retryable_worker_bridge_transport_error_message(message: &str) -> bool {
    message.contains("transport closed")
        && (message.contains("Connection reset by peer")
            || message.contains("early eof")
            || message.contains("Broken pipe")
            || message.contains("bridge stopped"))
}

fn is_authoritative_ipc_transport_close(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::TransportClosed(message)
            if message.contains("IPC control transport closed")
                || message.contains("IPC data transport closed")
    )
}

fn is_authoritative_sidecar_transport_close(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::TransportClosed(message)
            if message.contains("transport closed")
                && message.contains("sidecar")
                && (message.contains("bridge stopped")
                    || message.contains("bridge closed")
                    || message.contains("Broken pipe")
                    || message.contains("early eof")
                    || message.contains("Connection reset by peer"))
    )
}

fn is_authoritative_endpoint_transport_close(err: &CnxError) -> bool {
    is_authoritative_ipc_transport_close(err) || is_authoritative_sidecar_transport_close(err)
}

fn is_retryable_worker_bridge_peer_error(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::PeerError(message) | CnxError::Internal(message)
            if is_retryable_worker_bridge_transport_error_message(message)
    )
}

fn should_emit_endpoint_retry_log(key: &str) -> bool {
    static LAST_EMIT: std::sync::OnceLock<
        StdMutex<std::collections::HashMap<String, std::time::Instant>>,
    > = std::sync::OnceLock::new();
    let now = std::time::Instant::now();
    let mut guard = LAST_EMIT
        .get_or_init(|| StdMutex::new(std::collections::HashMap::new()))
        .lock()
        .expect("endpoint retry log limiter lock");
    match guard.get(key).copied() {
        Some(last) if now.duration_since(last) < ENDPOINT_RETRY_LOG_INTERVAL => false,
        _ => {
            guard.insert(key.to_string(), now);
            true
        }
    }
}

async fn close_stale_recv_channel(
    boundary: Arc<dyn ChannelIoSubset>,
    ctx: BoundaryContext,
    channel: ChannelKey,
) {
    let close_boundary = boundary.clone();
    let close_ctx = ctx.clone();
    let close_channel = channel.clone();
    match tokio::task::spawn_blocking(move || {
        close_boundary.channel_close(close_ctx, close_channel)
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            log::debug!(
                "runtime endpoint channel_close retry reset failed for {}: {:?}",
                channel.0,
                err
            );
        }
        Err(err) => {
            log::warn!(
                "runtime endpoint channel_close retry reset join failed for {}: {:?}",
                channel.0,
                err
            );
        }
    }
}

fn summarize_event_origins(events: &[Event]) -> Vec<String> {
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for event in events {
        *counts
            .entry(event.metadata().origin_id.0.clone())
            .or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(origin, count)| format!("{origin}={count}"))
        .collect()
}

enum EndpointJoin {
    Thread(std::thread::JoinHandle<()>),
}

impl EndpointJoin {
    fn is_finished(&self) -> bool {
        match self {
            Self::Thread(join) => join.is_finished(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EndpointReadyMode {
    FirstRecvPoll,
    FirstReceivable,
}

#[derive(Clone)]
struct EndpointReadySignal {
    tx: Arc<StdMutex<Option<SyncSender<()>>>>,
}

impl EndpointReadySignal {
    fn new(tx: SyncSender<()>) -> Self {
        Self {
            tx: Arc::new(StdMutex::new(Some(tx))),
        }
    }

    fn signal(&self) {
        if let Some(tx) = self.tx.lock().expect("endpoint ready tx lock").take() {
            let _ = tx.send(());
        }
    }
}

#[derive(Default)]
struct EndpointReceiveState {
    inflight_recv_count: AtomicUsize,
    receivable_observation_count: AtomicU64,
}

impl EndpointReceiveState {
    fn enter_recv(self: &Arc<Self>) -> EndpointReceiveGuard {
        self.inflight_recv_count.fetch_add(1, Ordering::AcqRel);
        EndpointReceiveGuard {
            state: self.clone(),
        }
    }

    fn mark_receivable(&self) {
        self.receivable_observation_count
            .fetch_add(1, Ordering::AcqRel);
    }

    fn is_receive_armed(&self) -> bool {
        self.inflight_recv_count.load(Ordering::Acquire) > 0
            && self.receivable_observation_count.load(Ordering::Acquire) > 0
    }
}

struct EndpointReceiveGuard {
    state: Arc<EndpointReceiveState>,
}

impl Drop for EndpointReceiveGuard {
    fn drop(&mut self) {
        self.state
            .inflight_recv_count
            .fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) struct ManagedEndpointTask {
    name: String,
    route_key: String,
    boundary_id: usize,
    unit_ids: Vec<String>,
    shutdown: CancellationToken,
    terminal_reason: Arc<StdMutex<Option<String>>>,
    receive_state: Arc<EndpointReceiveState>,
    join: Option<EndpointJoin>,
}

#[cfg(test)]
static SHUTDOWN_BLOCKING_JOIN_INFLIGHT: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
fn shutdown_blocking_join_inflight() -> usize {
    SHUTDOWN_BLOCKING_JOIN_INFLIGHT.load(Ordering::SeqCst)
}

impl ManagedEndpointTask {
    fn boundary_id_for(boundary: &Arc<dyn ChannelIoSubset>) -> usize {
        Arc::as_ptr(boundary) as *const () as usize
    }

    fn spawn_join<Fut>(runner: Fut) -> EndpointJoin
    where
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        if debug_source_status_lifecycle_enabled() {
            eprintln!("fs_meta_runtime_endpoint: spawn_join mode=dedicated-runtime-thread");
        }
        EndpointJoin::Thread(std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build dedicated fs-meta endpoint runtime")
                .block_on(runner)
        }))
    }

    fn spawn_join_with_ready<Fut>(runner: Fut) -> (EndpointJoin, Receiver<()>)
    where
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let (ready_tx, ready_rx) = sync_channel(1);
        let join = Self::spawn_join(async move {
            let _ = ready_tx.send(());
            runner.await;
        });
        (join, ready_rx)
    }

    fn wait_until_ready(name: &str, ready_rx: Receiver<()>) {
        match ready_rx.recv() {
            Ok(()) => {}
            Err(err) => log::warn!("endpoint task {} ready wait failed: {}", name, err),
        }
    }

    pub(crate) fn spawn<F, Fut>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
        shutdown: CancellationToken,
        handler: F,
    ) -> Self
    where
        F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Vec<Event>> + Send + 'static,
    {
        Self::spawn_with_context(
            boundary,
            route,
            name,
            BoundaryContext::default(),
            shutdown,
            handler,
        )
    }

    pub(crate) fn spawn_with_unit<F, Fut>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
        unit_id: impl Into<String>,
        shutdown: CancellationToken,
        handler: F,
    ) -> Self
    where
        F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Vec<Event>> + Send + 'static,
    {
        Self::spawn_with_context(
            boundary,
            route,
            name,
            BoundaryContext::for_unit(unit_id),
            shutdown,
            handler,
        )
    }

    pub(crate) fn spawn_with_units<F, Fut, I, S>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
        unit_ids: I,
        shutdown: CancellationToken,
        handler: F,
    ) -> Self
    where
        F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Vec<Event>> + Send + 'static,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let contexts: Vec<BoundaryContext> = unit_ids
            .into_iter()
            .map(|unit_id| BoundaryContext::for_unit(unit_id.into()))
            .collect();
        assert!(
            !contexts.is_empty(),
            "spawn_with_units requires at least one endpoint recv context"
        );
        Self::spawn_with_contexts(boundary, route, name, contexts, shutdown, handler)
    }

    fn spawn_with_context<F, Fut>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
        ctx: BoundaryContext,
        shutdown: CancellationToken,
        handler: F,
    ) -> Self
    where
        F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Vec<Event>> + Send + 'static,
    {
        Self::spawn_with_contexts(boundary, route, name, vec![ctx], shutdown, handler)
    }

    fn spawn_with_contexts<F, Fut>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
        contexts: Vec<BoundaryContext>,
        shutdown: CancellationToken,
        handler: F,
    ) -> Self
    where
        F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Vec<Event>> + Send + 'static,
    {
        Self::spawn_with_contexts_and_ready_mode(
            boundary,
            route,
            name,
            contexts,
            shutdown,
            handler,
            EndpointReadyMode::FirstRecvPoll,
        )
    }

    pub(crate) fn spawn_with_unit_wait_receivable<F, Fut>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
        unit_id: impl Into<String>,
        shutdown: CancellationToken,
        handler: F,
    ) -> Self
    where
        F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Vec<Event>> + Send + 'static,
    {
        Self::spawn_with_contexts_and_ready_mode(
            boundary,
            route,
            name,
            vec![BoundaryContext::for_unit(unit_id)],
            shutdown,
            handler,
            EndpointReadyMode::FirstReceivable,
        )
    }

    fn spawn_with_contexts_and_ready_mode<F, Fut>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
        contexts: Vec<BoundaryContext>,
        shutdown: CancellationToken,
        handler: F,
        ready_mode: EndpointReadyMode,
    ) -> Self
    where
        F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Vec<Event>> + Send + 'static,
    {
        let name_owned = name.into();
        let route_key = route.0.clone();
        let boundary_id = Self::boundary_id_for(&boundary);
        let unit_ids = contexts
            .iter()
            .filter_map(|ctx| ctx.unit_id.clone())
            .collect::<Vec<_>>();
        let join_name = name_owned.clone();
        let route_key_for_runner = route_key.clone();
        let task_shutdown = shutdown.child_token();
        let shutdown_for_task = task_shutdown.clone();
        let terminal_reason = Arc::new(StdMutex::new(None));
        let terminal_reason_for_runner = terminal_reason.clone();
        let receive_state = Arc::new(EndpointReceiveState::default());
        let receive_state_for_runner = receive_state.clone();
        let (ready_tx, ready_rx) = sync_channel(1);
        let ready_signal = EndpointReadySignal::new(ready_tx);
        let boundary: Arc<dyn ChannelIoSubset> = match ready_mode {
            EndpointReadyMode::FirstRecvPoll => {
                let ready_signal = ready_signal.clone();
                Arc::new(RecvEntryObservedBoundary {
                    inner: boundary,
                    before_recv: Arc::new(move || ready_signal.signal()),
                })
            }
            EndpointReadyMode::FirstReceivable => {
                let ready_signal = ready_signal.clone();
                let receive_state = receive_state.clone();
                Arc::new(RecvPendingObservedBoundary {
                    inner: boundary,
                    on_pending_recv: Arc::new(move || {
                        receive_state.mark_receivable();
                        ready_signal.signal();
                    }),
                })
            }
        };
        let handler = Arc::new(handler);
        let receivable_ready =
            (ready_mode == EndpointReadyMode::FirstReceivable).then_some(ready_signal.clone());
        let runner = run_endpoint_loop_with_contexts_and_ready_signal(
            boundary,
            route,
            join_name.clone(),
            contexts,
            shutdown_for_task,
            handler,
            terminal_reason_for_runner.clone(),
            receivable_ready,
            receive_state_for_runner,
        );
        let runner = async move {
            let outcome = AssertUnwindSafe(runner).catch_unwind().await;
            if let Err(panic) = outcome {
                let panic_message = if let Some(message) = panic.downcast_ref::<&str>() {
                    (*message).to_string()
                } else if let Some(message) = panic.downcast_ref::<String>() {
                    message.clone()
                } else {
                    "unknown panic".to_string()
                };
                *terminal_reason_for_runner
                    .lock()
                    .expect("terminal_reason lock") = Some(format!("panic:{panic_message}"));
                log::warn!(
                    "endpoint task {} panicked for {}: {}",
                    join_name,
                    route_key_for_runner,
                    panic_message
                );
            }
        };
        let join = Self::spawn_join(runner);
        Self::wait_until_ready(&name_owned, ready_rx);

        Self {
            name: name_owned,
            route_key,
            boundary_id,
            unit_ids,
            shutdown: task_shutdown,
            terminal_reason,
            receive_state,
            join: Some(join),
        }
    }

    pub(crate) fn spawn_stream<F, Fut, G>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
        unit_id: impl Into<String>,
        shutdown: CancellationToken,
        should_recv: G,
        handler: F,
    ) -> Self
    where
        F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
        G: Fn() -> bool + Send + Sync + 'static,
    {
        Self::spawn_stream_with_before_recv_and_wait(
            boundary,
            route,
            name,
            unit_id,
            shutdown,
            should_recv,
            || {
                async move {
                    tokio::time::sleep(REQUEST_REPLY_SEND_RETRY_DELAY).await;
                }
                .boxed()
            },
            || {},
            handler,
        )
    }

    pub(crate) fn spawn_stream_with_before_recv<F, Fut, G, H>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
        unit_id: impl Into<String>,
        shutdown: CancellationToken,
        should_recv: G,
        before_recv: H,
        handler: F,
    ) -> Self
    where
        F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
        G: Fn() -> bool + Send + Sync + 'static,
        H: Fn() + Send + Sync + 'static,
    {
        Self::spawn_stream_with_before_recv_and_wait(
            boundary,
            route,
            name,
            unit_id,
            shutdown,
            should_recv,
            || {
                async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                .boxed()
            },
            before_recv,
            handler,
        )
    }

    pub(crate) fn spawn_stream_with_before_recv_and_wait<F, Fut, G, W, H>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
        unit_id: impl Into<String>,
        shutdown: CancellationToken,
        should_recv: G,
        wait_until_receivable: W,
        before_recv: H,
        handler: F,
    ) -> Self
    where
        F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
        G: Fn() -> bool + Send + Sync + 'static,
        W: Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
        H: Fn() + Send + Sync + 'static,
    {
        let name_owned = name.into();
        let route_key = route.0.clone();
        let boundary_id = Self::boundary_id_for(&boundary);
        let unit_id = unit_id.into();
        let join_name = name_owned.clone();
        let name_for_runner = name_owned.clone();
        let route_key_for_runner = route_key.clone();
        let task_shutdown = shutdown.child_token();
        let shutdown_for_task = task_shutdown.clone();
        let terminal_reason = Arc::new(StdMutex::new(None));
        let terminal_reason_for_runner = terminal_reason.clone();
        let receive_state = Arc::new(EndpointReceiveState::default());
        let should_recv = Arc::new(should_recv);
        let wait_until_receivable = Arc::new(wait_until_receivable);
        let before_recv = Arc::new(before_recv);
        let boundary: Arc<dyn ChannelIoSubset> = Arc::new(RecvEntryObservedBoundary {
            inner: boundary,
            before_recv: before_recv.clone(),
        });
        let handler = Arc::new(handler);
        let runner = run_stream_loop_with_wait(
            boundary,
            route,
            join_name,
            unit_id.clone(),
            shutdown_for_task,
            should_recv,
            wait_until_receivable,
            before_recv,
            handler,
            terminal_reason_for_runner.clone(),
        );
        let runner = async move {
            let outcome = AssertUnwindSafe(runner).catch_unwind().await;
            if let Err(panic) = outcome {
                let panic_message = if let Some(message) = panic.downcast_ref::<&str>() {
                    (*message).to_string()
                } else if let Some(message) = panic.downcast_ref::<String>() {
                    message.clone()
                } else {
                    "unknown panic".to_string()
                };
                *terminal_reason_for_runner
                    .lock()
                    .expect("terminal_reason lock") = Some(format!("panic:{panic_message}"));
                log::warn!(
                    "stream task {} panicked for {}: {}",
                    name_for_runner,
                    route_key_for_runner,
                    panic_message
                );
            }
        };
        let (join, ready_rx) = Self::spawn_join_with_ready(runner);
        Self::wait_until_ready(&name_owned, ready_rx);

        Self {
            name: name_owned,
            route_key,
            boundary_id,
            unit_ids: vec![unit_id],
            shutdown: task_shutdown,
            terminal_reason,
            receive_state,
            join: Some(join),
        }
    }

    pub(crate) fn route_key(&self) -> &str {
        &self.route_key
    }

    pub(crate) fn unit_ids(&self) -> &[String] {
        &self.unit_ids
    }

    pub(crate) fn belongs_to_boundary(&self, boundary: &Arc<dyn ChannelIoSubset>) -> bool {
        self.boundary_id == Self::boundary_id_for(boundary)
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.join.as_ref().is_none_or(EndpointJoin::is_finished)
    }

    pub(crate) fn finish_reason(&self) -> Option<String> {
        self.terminal_reason
            .lock()
            .expect("terminal_reason lock")
            .clone()
    }

    pub(crate) fn is_receive_armed(&self) -> bool {
        self.receive_state.is_receive_armed()
    }

    pub(crate) fn request_shutdown(&self) {
        self.shutdown.cancel();
    }

    pub(crate) async fn shutdown(&mut self, wait_timeout: Duration) {
        self.shutdown.cancel();
        let Some(join) = self.join.take() else {
            return;
        };
        match join {
            EndpointJoin::Thread(join) => {
                if tokio::runtime::Handle::try_current().is_ok() {
                    let deadline = tokio::time::Instant::now() + wait_timeout;
                    while !join.is_finished() {
                        if tokio::time::Instant::now() >= deadline {
                            log::warn!(
                                "endpoint task {} thread did not exit within {:?}",
                                self.name,
                                wait_timeout
                            );
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    if let Err(err) = join.join() {
                        log::warn!("endpoint task {} thread panicked: {:?}", self.name, err);
                    }
                } else if let Err(err) = join.join() {
                    log::warn!("endpoint task {} thread panicked: {:?}", self.name, err);
                }
            }
        }
    }
}

async fn run_endpoint_loop<F, Fut>(
    boundary: Arc<dyn ChannelIoSubset>,
    route: RouteKey,
    join_name: String,
    ctx: BoundaryContext,
    shutdown_for_task: CancellationToken,
    handler: Arc<F>,
    terminal_reason: Arc<StdMutex<Option<String>>>,
) where
    F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Vec<Event>> + Send + 'static,
{
    run_endpoint_loop_with_contexts(
        boundary,
        route,
        join_name,
        vec![ctx],
        shutdown_for_task,
        handler,
        terminal_reason,
    )
    .await
}

async fn run_endpoint_loop_with_contexts<F, Fut>(
    boundary: Arc<dyn ChannelIoSubset>,
    route: RouteKey,
    join_name: String,
    contexts: Vec<BoundaryContext>,
    shutdown_for_task: CancellationToken,
    handler: Arc<F>,
    terminal_reason: Arc<StdMutex<Option<String>>>,
) where
    F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Vec<Event>> + Send + 'static,
{
    run_endpoint_loop_with_contexts_and_ready_signal(
        boundary,
        route,
        join_name,
        contexts,
        shutdown_for_task,
        handler,
        terminal_reason,
        None,
        Arc::new(EndpointReceiveState::default()),
    )
    .await
}

async fn run_endpoint_loop_with_contexts_and_ready_signal<F, Fut>(
    boundary: Arc<dyn ChannelIoSubset>,
    route: RouteKey,
    join_name: String,
    contexts: Vec<BoundaryContext>,
    shutdown_for_task: CancellationToken,
    handler: Arc<F>,
    terminal_reason: Arc<StdMutex<Option<String>>>,
    receivable_ready: Option<EndpointReadySignal>,
    receive_state: Arc<EndpointReceiveState>,
) where
    F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Vec<Event>> + Send + 'static,
{
    let debug_materialized_route = debug_materialized_route_lifecycle_enabled()
        && is_materialized_internal_query_route(&route);
    let debug_endpoint_requests = debug_endpoint_request_lifecycle_enabled()
        || debug_source_status_lifecycle_enabled()
        || debug_materialized_route;
    #[cfg(test)]
    if let Some(delay) = take_endpoint_loop_start_delay_hook(&route, &join_name) {
        std::thread::sleep(delay);
    }
    if debug_source_status_lifecycle_enabled() || debug_materialized_route {
        eprintln!(
            "fs_meta_runtime_endpoint: loop_start route={} task={} thread={:?}",
            route.0,
            join_name,
            std::thread::current().name()
        );
    }
    let request_channel = ChannelKey(route.0.clone());
    let reply_channel = ChannelKey(format!("{}:reply", route.0));
    let mut exit_reason = None::<String>;
    let mut retryable_recv_gap_count = 0usize;
    let mut stale_recv_gap_count = 0usize;

    'recv_loop: loop {
        if shutdown_for_task.is_cancelled() {
            exit_reason = Some("shutdown_cancelled".into());
            break;
        }
        let mut received = None::<(BoundaryContext, Vec<Event>)>;
        let mut fatal_err = None::<CnxError>;
        let mut saw_retryable_gap = false;
        let recv_timeout_ms = IDLE_RECV_QUIET_WINDOW.as_millis() as u64;
        #[cfg(test)]
        maybe_run_endpoint_before_first_recv_poll_hook(&route, &join_name);
        let mut pending = contexts
            .iter()
            .cloned()
            .map(|ctx| {
                let boundary = boundary.clone();
                let request_channel = request_channel.clone();
                let receive_state = receive_state.clone();
                async move {
                    let _recv_guard = receive_state.enter_recv();
                    let recv_result = AssertUnwindSafe(boundary.channel_recv(
                        ctx.clone(),
                        ChannelRecvRequest {
                            channel_key: request_channel,
                            timeout_ms: Some(recv_timeout_ms),
                        },
                    ))
                    .catch_unwind()
                    .await;
                    (ctx, recv_result)
                }
                .boxed()
            })
            .collect::<Vec<_>>();
        while !pending.is_empty() {
            let ((ctx, recv_result), _, rest) = tokio::select! {
                result = futures_util::future::select_all(pending) => result,
                _ = shutdown_for_task.cancelled() => {
                    exit_reason = Some("shutdown_cancelled".into());
                    break 'recv_loop;
                }
            };
            pending = rest;
            let requests = match recv_result {
                Err(_) => {
                    log::warn!(
                        "endpoint task {} recv panicked for {}; retrying",
                        join_name,
                        route.0
                    );
                    saw_retryable_gap = true;
                    continue;
                }
                Ok(result) => match result {
                    Ok(events) => {
                        receive_state.mark_receivable();
                        if let Some(ready) = &receivable_ready {
                            ready.signal();
                        }
                        retryable_recv_gap_count = 0;
                        stale_recv_gap_count = 0;
                        events
                    }
                    Err(CnxError::Timeout) => {
                        receive_state.mark_receivable();
                        if let Some(ready) = &receivable_ready {
                            ready.signal();
                        }
                        continue;
                    }
                    Err(err) if is_retryable_worker_bridge_peer_error(&err) => {
                        stale_recv_gap_count = 0;
                        retryable_recv_gap_count = retryable_recv_gap_count.saturating_add(1);
                        if retryable_recv_gap_count >= REQUEST_RETRYABLE_RECV_GAP_RETRY_LIMIT {
                            if fatal_err.is_none() {
                                fatal_err = Some(err);
                            }
                            continue;
                        }
                        if debug_materialized_route {
                            eprintln!(
                                "fs_meta_runtime_endpoint: materialized_route recv_retry route={} task={} err={}",
                                route.0, join_name, err
                            );
                        }
                        log::debug!(
                            "endpoint task {} recv retry for {} after retryable worker-bridge error: {:?}",
                            join_name,
                            route.0,
                            err
                        );
                        saw_retryable_gap = true;
                        continue;
                    }
                    Err(err @ CnxError::NotSupported(_))
                    | Err(err @ CnxError::NotReady(_))
                    | Err(err @ CnxError::TransportClosed(_))
                    | Err(err @ CnxError::ChannelClosed)
                    | Err(err @ CnxError::LinkError(_)) => {
                        if is_authoritative_endpoint_transport_close(&err) {
                            if fatal_err.is_none() {
                                fatal_err = Some(err);
                            }
                            continue;
                        }
                        if debug_materialized_route {
                            eprintln!(
                                "fs_meta_runtime_endpoint: materialized_route recv_retry route={} task={} err={}",
                                route.0, join_name, err
                            );
                        }
                        log::debug!(
                            "endpoint task {} recv retry for {} after transient error: {:?}",
                            join_name,
                            route.0,
                            err
                        );
                        saw_retryable_gap = true;
                        continue;
                    }
                    Err(err) if is_stale_grant_attachment_recv_gap(&err) => {
                        stale_recv_gap_count = stale_recv_gap_count.saturating_add(1);
                        close_stale_recv_channel(
                            boundary.clone(),
                            ctx.clone(),
                            request_channel.clone(),
                        )
                        .await;
                        log::debug!(
                            "endpoint task {} recv retry for {} after stale grant-attachment gap: {:?}",
                            join_name,
                            route.0,
                            err
                        );
                        if stale_recv_gap_count >= REQUEST_RETRYABLE_RECV_GAP_RETRY_LIMIT {
                            if fatal_err.is_none() {
                                let reason = format!(
                                    "stale grant-attachment recv gap retry exhausted after {} attempts",
                                    stale_recv_gap_count
                                );
                                *terminal_reason.lock().expect("terminal_reason lock") =
                                    Some(format!("recv_failed:access denied: {reason}"));
                                fatal_err = Some(CnxError::AccessDenied(reason));
                            }
                            continue;
                        }
                        saw_retryable_gap = true;
                        continue;
                    }
                    Err(err) => {
                        if fatal_err.is_none() {
                            fatal_err = Some(err);
                        }
                        continue;
                    }
                },
            };
            received = Some((ctx, requests));
            break;
        }

        let Some((recv_ctx, requests)) = received else {
            if let Some(err) = fatal_err {
                exit_reason = Some(format!("recv_failed:{err}"));
                log::warn!(
                    "endpoint task {} recv failed for {}: {:?}",
                    join_name,
                    route.0,
                    err
                );
                break;
            }
            if saw_retryable_gap {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            continue;
        };

        if requests.is_empty() {
            continue;
        }
        if debug_endpoint_requests {
            eprintln!(
                "fs_meta_runtime_endpoint: request batch recv route={} task={} events={}",
                request_channel.0,
                join_name,
                requests.len()
            );
        }

        let correlation_id = shared_correlation_id(&requests);
        let mut responses = handler(requests).await;
        if let Some(cid) = correlation_id {
            responses = responses
                .into_iter()
                .map(|response| {
                    if response.metadata().correlation_id.is_none() {
                        response.with_correlation_id(Some(cid))
                    } else {
                        response
                    }
                })
                .collect();
        }

        if responses.is_empty() {
            if debug_endpoint_requests {
                eprintln!(
                    "fs_meta_runtime_endpoint: request batch handled with no replies route={} task={}",
                    request_channel.0, join_name
                );
            }
            continue;
        }
        let response_count = responses.len();
        let mut reply_sent = false;
        for attempt in 1..=REQUEST_REPLY_SEND_RETRY_LIMIT {
            match boundary
                .channel_send(
                    recv_ctx.clone(),
                    ChannelSendRequest {
                        channel_key: reply_channel.clone(),
                        events: responses.clone(),
                        timeout_ms: Some(Duration::from_millis(250).as_millis() as u64),
                    },
                )
                .await
            {
                Ok(()) => {
                    reply_sent = true;
                    break;
                }
                Err(
                    err @ CnxError::Timeout
                    | err @ CnxError::Backpressure
                    | err @ CnxError::NotSupported(_)
                    | err @ CnxError::NotReady(_)
                    | err @ CnxError::TransportClosed(_)
                    | err @ CnxError::ChannelClosed
                    | err @ CnxError::LinkError(_),
                ) if attempt < REQUEST_REPLY_SEND_RETRY_LIMIT => {
                    if debug_materialized_route {
                        eprintln!(
                            "fs_meta_runtime_endpoint: materialized_route send_retry route={} task={} attempt={} err={}",
                            route.0, join_name, attempt, err
                        );
                    }
                    log::debug!(
                        "endpoint task {} reply send retry {}/{} for {} after transient error: {:?}",
                        join_name,
                        attempt,
                        REQUEST_REPLY_SEND_RETRY_LIMIT,
                        route.0,
                        err
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(err) => {
                    log::warn!(
                        "endpoint task {} send failed for {} after {} attempts: {:?}",
                        join_name,
                        route.0,
                        attempt,
                        err
                    );
                    exit_reason = Some(format!("send_failed:{err}"));
                    break;
                }
            }
        }
        if !reply_sent {
            break;
        }
        if debug_endpoint_requests {
            eprintln!(
                "fs_meta_runtime_endpoint: request batch replies sent route={} task={} events={}",
                reply_channel.0, join_name, response_count
            );
        }
    }

    let final_reason = exit_reason.unwrap_or_else(|| "loop_returned".into());
    *terminal_reason.lock().expect("terminal_reason lock") = Some(final_reason.clone());
    if let Some(ready) = &receivable_ready {
        ready.signal();
    }
    if debug_materialized_route {
        eprintln!(
            "fs_meta_runtime_endpoint: materialized_route loop_exit route={} task={} reason={}",
            route.0, join_name, final_reason
        );
    }
}

async fn run_stream_loop<F, Fut, G, H>(
    boundary: Arc<dyn ChannelIoSubset>,
    route: RouteKey,
    join_name: String,
    unit_id: String,
    shutdown_for_task: CancellationToken,
    should_recv: Arc<G>,
    _before_recv: Arc<H>,
    handler: Arc<F>,
    terminal_reason: Arc<StdMutex<Option<String>>>,
) where
    F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + Sync + 'static,
    G: Fn() -> bool + Send + Sync + 'static,
    H: Fn() + Send + Sync + 'static,
{
    run_stream_loop_with_wait(
        boundary,
        route,
        join_name,
        unit_id,
        shutdown_for_task,
        should_recv,
        Arc::new(|| {
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            .boxed()
        }),
        _before_recv,
        handler,
        terminal_reason,
    )
    .await
}

async fn run_stream_loop_with_wait<F, Fut, G, W, H>(
    boundary: Arc<dyn ChannelIoSubset>,
    route: RouteKey,
    join_name: String,
    unit_id: String,
    shutdown_for_task: CancellationToken,
    should_recv: Arc<G>,
    wait_until_receivable: Arc<W>,
    _before_recv: Arc<H>,
    handler: Arc<F>,
    terminal_reason: Arc<StdMutex<Option<String>>>,
) where
    F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
    G: Fn() -> bool + Send + Sync + 'static,
    W: Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
    H: Fn() + Send + Sync + 'static,
{
    let ctx = BoundaryContext::for_unit(unit_id);
    let stream_channel = ChannelKey(route.0.clone());
    let mut stale_recv_gap_count = 0usize;

    loop {
        if shutdown_for_task.is_cancelled() {
            *terminal_reason.lock().expect("terminal_reason lock") =
                Some("shutdown_cancelled".to_string());
            break;
        }
        if !should_recv() {
            stale_recv_gap_count = 0;
            wait_until_receivable().await;
            continue;
        }
        if should_emit_endpoint_retry_log(&format!("recv_begin:{}:{}", stream_channel.0, join_name))
        {
            eprintln!(
                "fs_meta_runtime_endpoint: stream loop recv route={} task={}",
                stream_channel.0, join_name
            );
        }
        let recv = boundary.channel_recv(
            ctx.clone(),
            ChannelRecvRequest {
                channel_key: stream_channel.clone(),
                timeout_ms: Some(IDLE_RECV_QUIET_WINDOW.as_millis() as u64),
            },
        );
        let events = match tokio::select! {
            result = recv => result,
            _ = shutdown_for_task.cancelled() => {
                *terminal_reason.lock().expect("terminal_reason lock") =
                    Some("shutdown_cancelled".to_string());
                break;
            }
        } {
            Ok(events) => {
                stale_recv_gap_count = 0;
                events
            }
            Err(CnxError::Timeout) => {
                stale_recv_gap_count = 0;
                continue;
            }
            Err(err) if is_retryable_worker_bridge_peer_error(&err) => {
                stale_recv_gap_count = 0;
                if should_emit_endpoint_retry_log(&format!(
                    "retryable_gap:{}:{}",
                    stream_channel.0, join_name
                )) {
                    eprintln!(
                        "fs_meta_runtime_endpoint: retryable stream recv gap task={} route={} err={:?}",
                        join_name, stream_channel.0, err
                    );
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(err @ CnxError::NotSupported(_))
            | Err(err @ CnxError::NotReady(_))
            | Err(err @ CnxError::TransportClosed(_))
            | Err(err @ CnxError::ChannelClosed)
            | Err(err @ CnxError::LinkError(_)) => {
                stale_recv_gap_count = 0;
                if is_authoritative_endpoint_transport_close(&err) {
                    eprintln!(
                        "fs_meta_runtime_endpoint: terminal stream recv failure task={} route={} err={:?}",
                        join_name, stream_channel.0, err
                    );
                    log::warn!(
                        "stream task {} recv failed for {}: {:?}",
                        join_name,
                        stream_channel.0,
                        err
                    );
                    *terminal_reason.lock().expect("terminal_reason lock") =
                        Some(format!("recv_failed:{err}"));
                    break;
                }
                if should_emit_endpoint_retry_log(&format!(
                    "transient_gap:{}:{}",
                    stream_channel.0, join_name
                )) {
                    eprintln!(
                        "fs_meta_runtime_endpoint: transient stream recv gap task={} route={} err={:?}",
                        join_name, stream_channel.0, err
                    );
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(err) if is_stale_grant_attachment_recv_gap(&err) => {
                stale_recv_gap_count = stale_recv_gap_count.saturating_add(1);
                close_stale_recv_channel(boundary.clone(), ctx.clone(), stream_channel.clone())
                    .await;
                if should_emit_endpoint_retry_log(&format!(
                    "stale_gap:{}:{}",
                    stream_channel.0, join_name
                )) {
                    eprintln!(
                        "fs_meta_runtime_endpoint: stale grant-attachment recv gap task={} route={} err={:?}",
                        join_name, stream_channel.0, err
                    );
                }
                if stale_recv_gap_count >= STREAM_STALE_RECV_GAP_RETRY_LIMIT {
                    let reason = format!(
                        "stale grant-attachment recv gap retry exhausted after {} attempts",
                        stale_recv_gap_count
                    );
                    log::warn!(
                        "stream task {} recv failed for {}: {}",
                        join_name,
                        stream_channel.0,
                        reason
                    );
                    *terminal_reason.lock().expect("terminal_reason lock") =
                        Some(format!("recv_failed:{reason}"));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(err) => {
                eprintln!(
                    "fs_meta_runtime_endpoint: terminal stream recv failure task={} route={} err={:?}",
                    join_name, stream_channel.0, err
                );
                log::warn!(
                    "stream task {} recv failed for {}: {:?}",
                    join_name,
                    stream_channel.0,
                    err
                );
                *terminal_reason.lock().expect("terminal_reason lock") =
                    Some(format!("recv_failed:{err}"));
                break;
            }
        };

        if events.is_empty() {
            continue;
        }

        if debug_stream_delivery_enabled() {
            eprintln!(
                "fs_meta_runtime_endpoint: stream delivery route={} task={} events={} origins={:?}",
                stream_channel.0,
                join_name,
                events.len(),
                summarize_event_origins(&events)
            );
        }

        handler(events).await;
        eprintln!(
            "fs_meta_runtime_endpoint: stream loop handler returned route={} task={}",
            stream_channel.0, join_name
        );
    }

    if terminal_reason
        .lock()
        .expect("terminal_reason lock")
        .is_none()
    {
        *terminal_reason.lock().expect("terminal_reason lock") = Some("loop_returned".to_string());
    }
}

#[cfg(test)]
fn endpoint_loop_start_delay_hook_cell() -> &'static StdMutex<Option<(String, String, Duration)>> {
    static CELL: std::sync::OnceLock<StdMutex<Option<(String, String, Duration)>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_endpoint_loop_start_delay_hook(route_key: &str, join_name: &str, delay: Duration) {
    let mut guard = endpoint_loop_start_delay_hook_cell()
        .lock()
        .expect("endpoint_loop_start_delay_hook lock");
    *guard = Some((route_key.to_string(), join_name.to_string(), delay));
}

#[cfg(test)]
fn take_endpoint_loop_start_delay_hook(route: &RouteKey, join_name: &str) -> Option<Duration> {
    let mut guard = endpoint_loop_start_delay_hook_cell()
        .lock()
        .expect("endpoint_loop_start_delay_hook lock");
    let Some((hook_route, hook_name, _)) = guard.as_ref() else {
        return None;
    };
    if hook_route == &route.0 && hook_name == join_name {
        let (_, _, delay) = guard
            .take()
            .expect("endpoint_loop_start_delay_hook present");
        Some(delay)
    } else {
        None
    }
}

#[cfg(test)]
type EndpointBeforeFirstRecvPollHook = Arc<dyn Fn() + Send + Sync + 'static>;

#[cfg(test)]
fn endpoint_before_first_recv_poll_hook_cell()
-> &'static StdMutex<Option<(String, String, EndpointBeforeFirstRecvPollHook)>> {
    static CELL: std::sync::OnceLock<
        StdMutex<Option<(String, String, EndpointBeforeFirstRecvPollHook)>>,
    > = std::sync::OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_endpoint_before_first_recv_poll_hook<F>(route_key: &str, join_name: &str, hook: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let mut guard = endpoint_before_first_recv_poll_hook_cell()
        .lock()
        .expect("endpoint_before_first_recv_poll_hook lock");
    *guard = Some((route_key.to_string(), join_name.to_string(), Arc::new(hook)));
}

#[cfg(test)]
fn maybe_run_endpoint_before_first_recv_poll_hook(route: &RouteKey, join_name: &str) {
    let hook = {
        let mut guard = endpoint_before_first_recv_poll_hook_cell()
            .lock()
            .expect("endpoint_before_first_recv_poll_hook lock");
        let Some((hook_route, hook_name, _)) = guard.as_ref() else {
            return;
        };
        if hook_route == &route.0 && hook_name == join_name {
            let (_, _, hook) = guard
                .take()
                .expect("endpoint_before_first_recv_poll_hook present");
            Some(hook)
        } else {
            None
        }
    };
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use capanix_app_sdk::runtime::{EventMetadata, NodeId};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    struct RecordingBoundary {
        recv_keys: Mutex<Vec<String>>,
        recv_unit_ids: Mutex<Vec<Option<String>>>,
        close_keys: Mutex<Vec<String>>,
        first_failure: Mutex<Option<FirstFailure>>,
    }

    struct ThreadRecordingBoundary {
        thread_name_tx: Mutex<Option<std::sync::mpsc::SyncSender<Option<String>>>>,
    }

    struct ReplyTimeoutThenRecoverBoundary {
        recv_keys: Mutex<Vec<String>>,
        send_keys: Mutex<Vec<String>>,
        send_payloads: Mutex<Vec<Vec<u8>>>,
        recv_steps: Mutex<VecDeque<RecvStep>>,
        send_steps: Mutex<VecDeque<SendStep>>,
    }

    struct PanicOnceThenStopBoundary {
        recv_keys: Mutex<Vec<String>>,
        recv_panicked: AtomicBool,
    }

    struct ConcurrentFallbackRecvInterestBoundary {
        recv_keys: Mutex<Vec<String>>,
        recv_unit_ids: Mutex<Vec<Option<String>>>,
        query_peer_recv_started: tokio::sync::Notify,
        query_recv_armed: AtomicBool,
        request_available: tokio::sync::Notify,
        queued_requests: tokio::sync::Mutex<Vec<Event>>,
        dropped_without_query_recv_interest: AtomicBool,
    }

    struct PersistentStaleGrantAttachmentBoundary {
        recv_attempts: AtomicUsize,
        close_keys: Mutex<Vec<String>>,
    }

    struct IdleTimeoutBoundary {
        recv_attempts: AtomicUsize,
        last_timeout_ms: AtomicUsize,
    }

    #[derive(Clone, Copy)]
    enum FirstFailure {
        NotSupported,
        Timeout,
        StaleGrantAttachment,
        RevokedGrantAttachmentToken,
        RetryableBridgePeerError,
        RetryableBridgeInternalError,
        AuthoritativeIpcTransportClosed,
        SidecarTransportClosed,
    }

    enum RecvStep {
        Events(Vec<Event>),
        InternalStop,
    }

    enum SendStep {
        Timeout,
        Backpressure,
        Ok,
    }

    impl RecordingBoundary {
        fn new() -> Self {
            Self {
                recv_keys: Mutex::new(Vec::new()),
                recv_unit_ids: Mutex::new(Vec::new()),
                close_keys: Mutex::new(Vec::new()),
                first_failure: Mutex::new(None),
            }
        }

        fn fail_once() -> Self {
            Self {
                recv_keys: Mutex::new(Vec::new()),
                recv_unit_ids: Mutex::new(Vec::new()),
                close_keys: Mutex::new(Vec::new()),
                first_failure: Mutex::new(Some(FirstFailure::NotSupported)),
            }
        }

        fn timeout_once() -> Self {
            Self {
                recv_keys: Mutex::new(Vec::new()),
                recv_unit_ids: Mutex::new(Vec::new()),
                close_keys: Mutex::new(Vec::new()),
                first_failure: Mutex::new(Some(FirstFailure::Timeout)),
            }
        }
    }

    impl ThreadRecordingBoundary {
        fn new(tx: std::sync::mpsc::SyncSender<Option<String>>) -> Self {
            Self {
                thread_name_tx: Mutex::new(Some(tx)),
            }
        }
    }

    impl ReplyTimeoutThenRecoverBoundary {
        fn new(first: Vec<Event>, second: Vec<Event>) -> Self {
            Self::new_with_first_send_step(first, second, SendStep::Timeout)
        }

        fn new_with_first_send_step(
            first: Vec<Event>,
            second: Vec<Event>,
            first_send_step: SendStep,
        ) -> Self {
            Self {
                recv_keys: Mutex::new(Vec::new()),
                send_keys: Mutex::new(Vec::new()),
                send_payloads: Mutex::new(Vec::new()),
                recv_steps: Mutex::new(VecDeque::from([
                    RecvStep::Events(first),
                    RecvStep::Events(second),
                    RecvStep::InternalStop,
                ])),
                send_steps: Mutex::new(VecDeque::from([first_send_step, SendStep::Ok])),
            }
        }
    }

    impl PanicOnceThenStopBoundary {
        fn new() -> Self {
            Self {
                recv_keys: Mutex::new(Vec::new()),
                recv_panicked: AtomicBool::new(false),
            }
        }
    }

    impl ConcurrentFallbackRecvInterestBoundary {
        fn new() -> Self {
            Self {
                recv_keys: Mutex::new(Vec::new()),
                recv_unit_ids: Mutex::new(Vec::new()),
                query_peer_recv_started: tokio::sync::Notify::new(),
                query_recv_armed: AtomicBool::new(false),
                request_available: tokio::sync::Notify::new(),
                queued_requests: tokio::sync::Mutex::new(Vec::new()),
                dropped_without_query_recv_interest: AtomicBool::new(false),
            }
        }
    }

    impl PersistentStaleGrantAttachmentBoundary {
        fn new() -> Self {
            Self {
                recv_attempts: AtomicUsize::new(0),
                close_keys: Mutex::new(Vec::new()),
            }
        }
    }

    impl IdleTimeoutBoundary {
        fn new() -> Self {
            Self {
                recv_attempts: AtomicUsize::new(0),
                last_timeout_ms: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for RecordingBoundary {
        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            self.recv_keys
                .lock()
                .expect("recv_keys lock")
                .push(request.channel_key.0);
            self.recv_unit_ids
                .lock()
                .expect("recv_unit_ids lock")
                .push(_ctx.unit_id.clone());
            let mut first_failure = self.first_failure.lock().expect("first_failure lock");
            if let Some(failure) = first_failure.take() {
                return match failure {
                    FirstFailure::NotSupported => {
                        Err(CnxError::NotSupported("transient attach gap".into()))
                    }
                    FirstFailure::Timeout => Err(CnxError::Timeout),
                    FirstFailure::StaleGrantAttachment => Err(CnxError::AccessDenied(
                        "pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                            .into(),
                    )),
                    FirstFailure::RevokedGrantAttachmentToken => Err(CnxError::AccessDenied(
                        "invalid or revoked grant attachment token".into(),
                    )),
                    FirstFailure::RetryableBridgePeerError => Err(CnxError::PeerError(
                        "transport closed: sidecar control bridge stopped".into(),
                    )),
                    FirstFailure::RetryableBridgeInternalError => Err(CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .into(),
                    )),
                    FirstFailure::AuthoritativeIpcTransportClosed => Err(
                        CnxError::TransportClosed("IPC control transport closed".into()),
                    ),
                    FirstFailure::SidecarTransportClosed => Err(CnxError::TransportClosed(
                        "transport closed: sidecar control bridge stopped".into(),
                    )),
                };
            }
            Err(CnxError::Internal("stop after first recv".into()))
        }

        fn channel_close(
            &self,
            _ctx: BoundaryContext,
            channel: ChannelKey,
        ) -> capanix_app_sdk::Result<()> {
            self.close_keys
                .lock()
                .expect("close_keys lock")
                .push(channel.0);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for ThreadRecordingBoundary {
        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            _request: ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            if let Some(tx) = self
                .thread_name_tx
                .lock()
                .expect("thread_name_tx lock")
                .take()
            {
                let _ = tx.send(std::thread::current().name().map(str::to_string));
            }
            Err(CnxError::Internal("stop after first recv".into()))
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for ReplyTimeoutThenRecoverBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            self.send_keys
                .lock()
                .expect("send_keys lock")
                .push(request.channel_key.0);
            self.send_payloads
                .lock()
                .expect("send_payloads lock")
                .extend(
                    request
                        .events
                        .iter()
                        .map(|event| event.payload_bytes().to_vec()),
                );
            match self
                .send_steps
                .lock()
                .expect("send_steps lock")
                .pop_front()
                .unwrap_or(SendStep::Ok)
            {
                SendStep::Timeout => Err(CnxError::Timeout),
                SendStep::Backpressure => Err(CnxError::Backpressure),
                SendStep::Ok => Ok(()),
            }
        }

        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            self.recv_keys
                .lock()
                .expect("recv_keys lock")
                .push(request.channel_key.0);
            match self
                .recv_steps
                .lock()
                .expect("recv_steps lock")
                .pop_front()
                .unwrap_or(RecvStep::InternalStop)
            {
                RecvStep::Events(events) => Ok(events),
                RecvStep::InternalStop => Err(CnxError::Internal("stop after second batch".into())),
            }
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for PanicOnceThenStopBoundary {
        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            self.recv_keys
                .lock()
                .expect("recv_keys lock")
                .push(request.channel_key.0);
            if !self.recv_panicked.swap(true, Ordering::SeqCst) {
                panic!("panic in boundary recv before any request-batch recv evidence");
            }
            Err(CnxError::Internal("stop after panic retry".into()))
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for ConcurrentFallbackRecvInterestBoundary {
        async fn channel_send(
            &self,
            _ctx: BoundaryContext,
            request: ChannelSendRequest,
        ) -> capanix_app_sdk::Result<()> {
            if request.channel_key.0 == "source-status:v1.req" {
                if !self.query_recv_armed.load(Ordering::SeqCst) {
                    self.dropped_without_query_recv_interest
                        .store(true, Ordering::SeqCst);
                    return Ok(());
                }
                let mut queued = self.queued_requests.lock().await;
                queued.extend(request.events);
                drop(queued);
                self.request_available.notify_waiters();
                return Ok(());
            }
            Ok(())
        }

        async fn channel_recv(
            &self,
            ctx: BoundaryContext,
            request: ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            self.recv_keys
                .lock()
                .expect("recv_keys lock")
                .push(request.channel_key.0.clone());
            self.recv_unit_ids
                .lock()
                .expect("recv_unit_ids lock")
                .push(ctx.unit_id.clone());
            if request.channel_key.0 != "source-status:v1.req" {
                return Err(CnxError::Timeout);
            }
            match ctx.unit_id.as_deref() {
                Some("runtime.exec.query-peer") => {
                    self.query_peer_recv_started.notify_waiters();
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    Err(CnxError::Timeout)
                }
                Some("runtime.exec.query") => {
                    self.query_recv_armed.store(true, Ordering::SeqCst);
                    let wait = self.request_available.notified();
                    if let Some(timeout_ms) = request.timeout_ms {
                        tokio::time::timeout(Duration::from_millis(timeout_ms), wait)
                            .await
                            .map_err(|_| CnxError::Timeout)?;
                    } else {
                        wait.await;
                    }
                    let mut queued = self.queued_requests.lock().await;
                    if queued.is_empty() {
                        return Err(CnxError::Timeout);
                    }
                    Ok(std::mem::take(&mut *queued))
                }
                _ => Err(CnxError::Timeout),
            }
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for PersistentStaleGrantAttachmentBoundary {
        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            _request: ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            self.recv_attempts.fetch_add(1, Ordering::SeqCst);
            Err(CnxError::AccessDenied(
                "pid Pid(1) is drained/fenced and cannot obtain new grant attachments".into(),
            ))
        }

        fn channel_close(
            &self,
            _ctx: BoundaryContext,
            channel: ChannelKey,
        ) -> capanix_app_sdk::Result<()> {
            self.close_keys
                .lock()
                .expect("close_keys lock")
                .push(channel.0);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for IdleTimeoutBoundary {
        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            self.recv_attempts.fetch_add(1, Ordering::SeqCst);
            let timeout_ms = request.timeout_ms.unwrap_or(0) as usize;
            self.last_timeout_ms.store(timeout_ms, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(timeout_ms as u64)).await;
            Err(CnxError::Timeout)
        }
    }

    fn test_event(correlation_id: u64, payload: &'static [u8]) -> Event {
        Event::new(
            EventMetadata {
                origin_id: NodeId("nfs-test".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: Some(correlation_id),
                ingress_auth: None,
                trace: None,
            },
            Bytes::from_static(payload),
        )
    }

    fn test_terminal_reason() -> Arc<StdMutex<Option<String>>> {
        Arc::new(StdMutex::new(None))
    }

    #[test]
    fn spawned_endpoint_thread_does_not_run_on_shared_runtime_worker() {
        let (tx, rx) = sync_channel(1);
        let boundary = Arc::new(ThreadRecordingBoundary::new(tx));
        let mut endpoint = ManagedEndpointTask::spawn(
            boundary,
            RouteKey("sink-status:v1.req".into()),
            "test-endpoint",
            CancellationToken::new(),
            |_events: Vec<Event>| std::future::ready(Vec::new()),
        );

        let thread_name = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("endpoint loop should attempt recv");

        crate::runtime_app::shared_tokio_runtime()
            .block_on(endpoint.shutdown(Duration::from_secs(1)));

        assert_ne!(thread_name.as_deref(), Some("fs-meta-shared-runtime"));
    }

    #[test]
    fn spawned_endpoint_from_shared_runtime_moves_off_shared_runtime_worker() {
        let (tx, rx) = sync_channel(1);
        let mut endpoint = crate::runtime_app::shared_tokio_runtime().block_on(async {
            let boundary = Arc::new(ThreadRecordingBoundary::new(tx));
            ManagedEndpointTask::spawn(
                boundary,
                RouteKey("sink-status:v1.req".into()),
                "test-endpoint",
                CancellationToken::new(),
                |_events: Vec<Event>| std::future::ready(Vec::new()),
            )
        });

        let thread_name = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("endpoint loop should attempt recv");

        crate::runtime_app::shared_tokio_runtime()
            .block_on(endpoint.shutdown(Duration::from_secs(1)));

        assert_ne!(thread_name.as_deref(), Some("fs-meta-shared-runtime"));
    }

    #[test]
    fn materialized_route_debug_scope_includes_per_peer_sink_query_routes_only() {
        assert!(is_materialized_internal_query_route(&RouteKey(
            "materialized-find:v1.req".into()
        )));
        assert!(is_materialized_internal_query_route(
            &sink_query_request_route_for("node-b")
        ));
        assert!(!is_materialized_internal_query_route(&RouteKey(
            "materialized-find-proxy:v1.req".into()
        )));
        assert!(!is_materialized_internal_query_route(&RouteKey(
            "sink-status:v1.req".into()
        )));
    }

    #[test]
    fn spawned_endpoint_waits_briefly_for_loop_start_before_returning() {
        install_endpoint_loop_start_delay_hook(
            "source-status.loop-start-delay-test:v1.req",
            "loop-start-delay-test-endpoint",
            Duration::from_millis(40),
        );
        let boundary = Arc::new(RecordingBoundary::new());
        let started = std::time::Instant::now();
        let mut endpoint = ManagedEndpointTask::spawn(
            boundary,
            RouteKey("source-status.loop-start-delay-test:v1.req".into()),
            "loop-start-delay-test-endpoint",
            CancellationToken::new(),
            |_events: Vec<Event>| std::future::ready(Vec::new()),
        );
        let elapsed = started.elapsed();

        crate::runtime_app::shared_tokio_runtime()
            .block_on(endpoint.shutdown(Duration::from_secs(1)));

        assert!(
            elapsed >= Duration::from_millis(30),
            "endpoint spawn must wait for the recv loop to enter loop_start before returning; elapsed={elapsed:?}"
        );
    }

    #[test]
    fn spawned_endpoint_waits_for_first_request_recv_poll_before_returning() {
        let route_key = "source-manual-rescan.node-b:v1.req";
        let join_name = "first-recv-poll-test-endpoint";
        let (hook_entered_tx, hook_entered_rx) = sync_channel(1);
        let (hook_release_tx, hook_release_rx) = sync_channel(1);
        let (spawn_returned_tx, spawn_returned_rx) = sync_channel(1);
        let hook_release_rx = Arc::new(Mutex::new(hook_release_rx));
        install_endpoint_before_first_recv_poll_hook(route_key, join_name, move || {
            let _ = hook_entered_tx.send(());
            let _ = hook_release_rx
                .lock()
                .expect("hook_release_rx lock")
                .recv_timeout(Duration::from_secs(1));
        });

        let route_key_owned = route_key.to_string();
        let join_name_owned = join_name.to_string();
        let handle = std::thread::spawn(move || {
            let boundary = Arc::new(RecordingBoundary::new());
            let mut endpoint = ManagedEndpointTask::spawn(
                boundary,
                RouteKey(route_key_owned),
                join_name_owned,
                CancellationToken::new(),
                |_events: Vec<Event>| std::future::ready(Vec::new()),
            );
            let _ = spawn_returned_tx.send(());
            crate::runtime_app::shared_tokio_runtime()
                .block_on(endpoint.shutdown(Duration::from_secs(1)));
        });

        hook_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("endpoint loop should pause before first recv poll");
        assert!(
            spawn_returned_rx
                .recv_timeout(Duration::from_millis(30))
                .is_err(),
            "endpoint spawn returned before the request recv interest could be armed"
        );
        let _ = hook_release_tx.send(());
        spawn_returned_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("endpoint spawn should return after first recv poll is allowed");
        handle.join().expect("endpoint spawn thread should join");
    }

    #[test]
    fn spawned_endpoint_does_not_return_after_ready_wait_timeout_before_first_recv_poll() {
        let route_key = "source-manual-rescan.node-c:v1.req";
        let join_name = "first-recv-poll-timeout-test-endpoint";
        let (hook_entered_tx, hook_entered_rx) = sync_channel(1);
        let (hook_release_tx, hook_release_rx) = sync_channel(1);
        let (spawn_returned_tx, spawn_returned_rx) = sync_channel(1);
        let hook_release_rx = Arc::new(Mutex::new(hook_release_rx));
        install_endpoint_before_first_recv_poll_hook(route_key, join_name, move || {
            let _ = hook_entered_tx.send(());
            let _ = hook_release_rx
                .lock()
                .expect("hook_release_rx lock")
                .recv_timeout(Duration::from_secs(1));
        });

        let route_key_owned = route_key.to_string();
        let join_name_owned = join_name.to_string();
        let handle = std::thread::spawn(move || {
            let boundary = Arc::new(RecordingBoundary::new());
            let mut endpoint = ManagedEndpointTask::spawn(
                boundary,
                RouteKey(route_key_owned),
                join_name_owned,
                CancellationToken::new(),
                |_events: Vec<Event>| std::future::ready(Vec::new()),
            );
            let _ = spawn_returned_tx.send(());
            crate::runtime_app::shared_tokio_runtime()
                .block_on(endpoint.shutdown(Duration::from_secs(1)));
        });

        hook_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("endpoint loop should pause before first recv poll");
        assert!(
            spawn_returned_rx
                .recv_timeout(ENDPOINT_READY_WAIT_TIMEOUT + Duration::from_millis(80))
                .is_err(),
            "endpoint spawn must not return after the old bounded ready wait while request recv interest is still unarmed"
        );
        let _ = hook_release_tx.send(());
        spawn_returned_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("endpoint spawn should return after first recv poll is allowed");
        handle.join().expect("endpoint spawn thread should join");
    }

    #[test]
    fn request_endpoint_idle_receive_uses_quiet_cancellable_window() {
        let boundary = Arc::new(IdleTimeoutBoundary::new());
        let mut endpoint = ManagedEndpointTask::spawn(
            boundary.clone(),
            RouteKey("sink-status:v1.req".into()),
            "idle-request-endpoint",
            CancellationToken::new(),
            |_events: Vec<Event>| std::future::ready(Vec::new()),
        );

        std::thread::sleep(Duration::from_millis(700));
        let attempts = boundary.recv_attempts.load(Ordering::SeqCst);
        let timeout_ms = boundary.last_timeout_ms.load(Ordering::SeqCst);
        crate::runtime_app::shared_tokio_runtime()
            .block_on(endpoint.shutdown(Duration::from_millis(300)));

        assert!(
            attempts <= 1,
            "idle request endpoints must not poll empty runtime routes multiple times inside one quiet window; attempts={attempts}"
        );
        assert!(
            timeout_ms >= 1_000,
            "idle request endpoint receive timeout must be a quiet window, not a high-frequency CPU polling interval; timeout_ms={timeout_ms}"
        );
    }

    #[test]
    fn stream_endpoint_idle_receive_uses_quiet_cancellable_window() {
        let boundary = Arc::new(IdleTimeoutBoundary::new());
        let mut endpoint = ManagedEndpointTask::spawn_stream(
            boundary.clone(),
            RouteKey("source-logical-roots-control:v1.stream".into()),
            "idle-stream-endpoint",
            "runtime.exec.source",
            CancellationToken::new(),
            || true,
            |_events: Vec<Event>| std::future::ready(()),
        );

        std::thread::sleep(Duration::from_millis(700));
        let attempts = boundary.recv_attempts.load(Ordering::SeqCst);
        let timeout_ms = boundary.last_timeout_ms.load(Ordering::SeqCst);
        crate::runtime_app::shared_tokio_runtime()
            .block_on(endpoint.shutdown(Duration::from_millis(300)));

        assert!(
            attempts <= 1,
            "idle stream endpoints must not poll empty runtime routes multiple times inside one quiet window; attempts={attempts}"
        );
        assert!(
            timeout_ms >= 1_000,
            "idle stream endpoint receive timeout must be a quiet window, not a high-frequency CPU polling interval; timeout_ms={timeout_ms}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn endpoint_shutdown_timeout_must_not_leave_blocking_join_task_inflight() {
        SHUTDOWN_BLOCKING_JOIN_INFLIGHT.store(0, Ordering::SeqCst);
        let (release_tx, release_rx) = sync_channel::<()>(1);
        let thread_exited = Arc::new(AtomicBool::new(false));
        let thread_exited_flag = thread_exited.clone();
        let join = std::thread::spawn(move || {
            let _ = release_rx.recv();
            thread_exited_flag.store(true, Ordering::SeqCst);
        });
        let mut endpoint = ManagedEndpointTask {
            name: "test-endpoint".to_string(),
            route_key: "sink-status:v1.req".to_string(),
            boundary_id: 0,
            unit_ids: Vec::new(),
            shutdown: CancellationToken::new(),
            terminal_reason: Arc::new(StdMutex::new(None)),
            receive_state: Arc::new(EndpointReceiveState::default()),
            join: Some(EndpointJoin::Thread(join)),
        };

        endpoint.shutdown(Duration::from_millis(30)).await;
        let returned_while_thread_alive = !thread_exited.load(Ordering::SeqCst);
        let inflight_after_shutdown_return = shutdown_blocking_join_inflight();

        let _ = release_tx.send(());
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if thread_exited.load(Ordering::SeqCst) && shutdown_blocking_join_inflight() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("endpoint join task should settle after releasing synthetic thread");

        assert_eq!(
            inflight_after_shutdown_return, 0,
            "endpoint shutdown returned with an inflight blocking join task; returned_while_thread_alive={returned_while_thread_alive}"
        );
    }

    #[test]
    fn endpoint_loop_uses_runtime_unit_context_for_recv() {
        let boundary = Arc::new(RecordingBoundary::new());
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("materialized-find:v1.req".into()),
            "test-endpoint".into(),
            BoundaryContext::for_unit("runtime.exec.sink"),
            CancellationToken::new(),
            Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
            test_terminal_reason(),
        ));

        let recv_unit_ids = boundary
            .recv_unit_ids
            .lock()
            .expect("recv_unit_ids lock")
            .clone();
        assert_eq!(
            recv_unit_ids,
            vec![Some("runtime.exec.sink".to_string())],
            "endpoint recv must carry the owning runtime unit identity so request routes do not reattach against drained predecessors",
        );
    }

    #[test]
    fn stream_loop_uses_exact_route_key_without_double_suffix() {
        let boundary = Arc::new(RecordingBoundary::new());
        crate::runtime_app::shared_tokio_runtime().block_on(run_stream_loop(
            boundary.clone(),
            RouteKey("fs-meta.events:v1.stream".into()),
            "test-stream".into(),
            "runtime.exec.sink".to_string(),
            CancellationToken::new(),
            Arc::new(|| true),
            Arc::new(|| {}),
            Arc::new(|_events: Vec<Event>| std::future::ready(())),
            test_terminal_reason(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(recv_keys, vec!["fs-meta.events:v1.stream".to_string()]);
    }

    #[test]
    fn stream_loop_retries_transient_recv_errors() {
        let boundary = Arc::new(RecordingBoundary::fail_once());
        crate::runtime_app::shared_tokio_runtime().block_on(run_stream_loop(
            boundary.clone(),
            RouteKey("fs-meta.events:v1.stream".into()),
            "test-stream".into(),
            "runtime.exec.sink".to_string(),
            CancellationToken::new(),
            Arc::new(|| true),
            Arc::new(|| {}),
            Arc::new(|_events: Vec<Event>| std::future::ready(())),
            test_terminal_reason(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec![
                "fs-meta.events:v1.stream".to_string(),
                "fs-meta.events:v1.stream".to_string()
            ]
        );
    }

    #[test]
    fn stream_loop_uses_runtime_unit_context_for_recv() {
        let boundary = Arc::new(RecordingBoundary::new());
        crate::runtime_app::shared_tokio_runtime().block_on(run_stream_loop(
            boundary.clone(),
            RouteKey("sink-logical-roots-control:v1.stream".into()),
            "test-stream".into(),
            "runtime.exec.sink".into(),
            CancellationToken::new(),
            Arc::new(|| true),
            Arc::new(|| {}),
            Arc::new(|_events: Vec<Event>| std::future::ready(())),
            test_terminal_reason(),
        ));

        let recv_unit_ids = boundary
            .recv_unit_ids
            .lock()
            .expect("recv_unit_ids lock")
            .clone();
        assert_eq!(
            recv_unit_ids,
            vec![Some("runtime.exec.sink".to_string())],
            "stream recv must carry the owning runtime unit identity so grant attachment resolution stays on the live worker generation",
        );
    }

    #[test]
    fn endpoint_loop_retries_timeout_recv_errors() {
        let boundary = Arc::new(RecordingBoundary::timeout_once());
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("sink-status:v1.req".into()),
            "test-endpoint".into(),
            BoundaryContext::default(),
            CancellationToken::new(),
            Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
            test_terminal_reason(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec![
                "sink-status:v1.req".to_string(),
                "sink-status:v1.req".to_string()
            ]
        );
    }

    #[test]
    fn endpoint_loop_retries_transient_recv_errors() {
        let boundary = Arc::new(RecordingBoundary::fail_once());
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("sink-status:v1.req".into()),
            "test-endpoint".into(),
            BoundaryContext::default(),
            CancellationToken::new(),
            Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
            test_terminal_reason(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec![
                "sink-status:v1.req".to_string(),
                "sink-status:v1.req".to_string()
            ]
        );
    }

    #[test]
    fn endpoint_loop_retries_stale_drained_fenced_grant_attachment_errors() {
        let boundary = Arc::new(RecordingBoundary {
            recv_keys: Mutex::new(Vec::new()),
            recv_unit_ids: Mutex::new(Vec::new()),
            close_keys: Mutex::new(Vec::new()),
            first_failure: Mutex::new(Some(FirstFailure::StaleGrantAttachment)),
        });
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("sink-status:v1.req".into()),
            "test-endpoint".into(),
            BoundaryContext::for_unit("runtime.exec.sink"),
            CancellationToken::new(),
            Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
            test_terminal_reason(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec![
                "sink-status:v1.req".to_string(),
                "sink-status:v1.req".to_string()
            ]
        );
        let close_keys = boundary.close_keys.lock().expect("close_keys lock").clone();
        assert_eq!(
            close_keys.first().map(String::as_str),
            Some("sink-status:v1.req"),
            "stale grant retry must still close the stale request route before retry; close_keys={close_keys:?}"
        );
        assert!(
            !close_keys.contains(&"sink-status:v1.req:reply".to_string()),
            "terminal endpoint cleanup must not permanently close reusable reply route after stale request-route reset; close_keys={close_keys:?}"
        );
    }

    #[test]
    fn endpoint_loop_must_not_spin_forever_on_persistent_stale_grant_attachment_recv_gaps() {
        let boundary = Arc::new(PersistentStaleGrantAttachmentBoundary::new());
        let terminal_reason = test_terminal_reason();
        let result = crate::runtime_app::shared_tokio_runtime().block_on(async {
            tokio::time::timeout(
                Duration::from_millis(800),
                run_endpoint_loop(
                    boundary.clone(),
                    RouteKey("source-status:v1.req".into()),
                    "test-endpoint".into(),
                    BoundaryContext::for_unit("runtime.exec.source"),
                    CancellationToken::new(),
                    Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
                    terminal_reason.clone(),
                ),
            )
            .await
        });

        assert!(
            result.is_ok(),
            "persistent stale grant-attachment recv gaps must terminate this request endpoint instead of repeatedly closing a shared request route"
        );
        let reason = terminal_reason
            .lock()
            .expect("terminal_reason lock")
            .clone()
            .unwrap_or_default();
        assert!(
            reason.contains("stale grant-attachment"),
            "terminal reason should preserve stale grant-attachment continuity evidence; reason={reason}"
        );
        assert!(
            boundary.recv_attempts.load(Ordering::SeqCst) > 1,
            "request endpoint should still retry stale recv gaps before escalating"
        );
    }

    #[test]
    fn endpoint_loop_retries_invalid_or_revoked_grant_attachment_tokens() {
        let boundary = Arc::new(RecordingBoundary {
            recv_keys: Mutex::new(Vec::new()),
            recv_unit_ids: Mutex::new(Vec::new()),
            close_keys: Mutex::new(Vec::new()),
            first_failure: Mutex::new(Some(FirstFailure::RevokedGrantAttachmentToken)),
        });
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("source-status:v1.req".into()),
            "test-endpoint".into(),
            BoundaryContext::for_unit("runtime.exec.query-peer"),
            CancellationToken::new(),
            Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
            test_terminal_reason(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec![
                "source-status:v1.req".to_string(),
                "source-status:v1.req".to_string()
            ],
            "endpoint recv should treat revoked grant attachment tokens as a retryable continuity gap during generation handoff instead of exiting and relying on respawn",
        );
        let close_keys = boundary.close_keys.lock().expect("close_keys lock").clone();
        assert_eq!(
            close_keys.first().map(String::as_str),
            Some("source-status:v1.req"),
            "revoked grant attachment recv gaps should close the stale channel before retry; close_keys={close_keys:?}"
        );
        assert!(
            !close_keys.contains(&"source-status:v1.req:reply".to_string()),
            "terminal endpoint cleanup must not permanently close reusable reply route after stale request-route reset; close_keys={close_keys:?}"
        );
    }

    #[test]
    fn endpoint_receivable_ready_wait_does_not_return_on_stale_grant_gap() {
        let boundary = Arc::new(RecordingBoundary {
            recv_keys: Mutex::new(Vec::new()),
            recv_unit_ids: Mutex::new(Vec::new()),
            close_keys: Mutex::new(Vec::new()),
            first_failure: Mutex::new(Some(FirstFailure::StaleGrantAttachment)),
        });
        let mut endpoint = ManagedEndpointTask::spawn_with_unit_wait_receivable(
            boundary.clone(),
            RouteKey("source-manual-rescan.node_a:v1.req".into()),
            "source:source-manual-rescan.node_a:v1.req",
            "runtime.exec.source",
            CancellationToken::new(),
            |_events: Vec<Event>| std::future::ready(Vec::new()),
        );

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert!(
            recv_keys.len() >= 2,
            "receivable-ready spawn must wait through stale grant gaps until a recv can arm; recv_keys={recv_keys:?}"
        );
        let close_keys = boundary.close_keys.lock().expect("close_keys lock").clone();
        assert_eq!(
            close_keys.first().map(String::as_str),
            Some("source-manual-rescan.node_a:v1.req"),
            "stale scoped source-rescan recv must close the stale channel before readiness is reported"
        );

        crate::runtime_app::shared_tokio_runtime()
            .block_on(endpoint.shutdown(Duration::from_secs(1)));
    }

    #[test]
    fn endpoint_loop_retries_retryable_worker_bridge_peer_errors() {
        let boundary = Arc::new(RecordingBoundary {
            recv_keys: Mutex::new(Vec::new()),
            recv_unit_ids: Mutex::new(Vec::new()),
            close_keys: Mutex::new(Vec::new()),
            first_failure: Mutex::new(Some(FirstFailure::RetryableBridgePeerError)),
        });
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("source-status:v1.req".into()),
            "test-endpoint".into(),
            BoundaryContext::for_unit("runtime.exec.query-peer"),
            CancellationToken::new(),
            Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
            test_terminal_reason(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec![
                "source-status:v1.req".to_string(),
                "source-status:v1.req".to_string()
            ],
            "endpoint recv should retry retryable worker-bridge PeerError values instead of exiting and relying on route respawn"
        );
    }

    #[test]
    fn endpoint_loop_retries_retryable_worker_bridge_internal_errors() {
        let boundary = Arc::new(RecordingBoundary {
            recv_keys: Mutex::new(Vec::new()),
            recv_unit_ids: Mutex::new(Vec::new()),
            close_keys: Mutex::new(Vec::new()),
            first_failure: Mutex::new(Some(FirstFailure::RetryableBridgeInternalError)),
        });
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("source-status:v1.req".into()),
            "test-endpoint".into(),
            BoundaryContext::for_unit("runtime.exec.query-peer"),
            CancellationToken::new(),
            Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
            test_terminal_reason(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec![
                "source-status:v1.req".to_string(),
                "source-status:v1.req".to_string()
            ],
            "endpoint recv should retry retryable worker-bridge Internal values instead of exiting and relying on route respawn"
        );
    }

    #[test]
    fn endpoint_loop_exits_on_authoritative_ipc_transport_close() {
        let boundary = Arc::new(RecordingBoundary {
            recv_keys: Mutex::new(Vec::new()),
            recv_unit_ids: Mutex::new(Vec::new()),
            close_keys: Mutex::new(Vec::new()),
            first_failure: Mutex::new(Some(FirstFailure::AuthoritativeIpcTransportClosed)),
        });
        let terminal_reason = test_terminal_reason();
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("source-status:v1.req".into()),
            "test-endpoint".into(),
            BoundaryContext::for_unit("runtime.exec.query-peer"),
            CancellationToken::new(),
            Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
            terminal_reason.clone(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec!["source-status:v1.req".to_string()],
            "authoritative IPC transport close must stop the endpoint loop instead of retrying forever on a dead worker-side IPC plane"
        );
        assert!(
            terminal_reason
                .lock()
                .expect("terminal_reason lock")
                .as_deref()
                .is_some_and(|reason| reason.contains("IPC control transport closed")),
            "terminal reason should preserve the authoritative IPC transport-close evidence"
        );
    }

    #[test]
    fn endpoint_loop_exits_on_sidecar_transport_close() {
        let boundary = Arc::new(RecordingBoundary {
            recv_keys: Mutex::new(Vec::new()),
            recv_unit_ids: Mutex::new(Vec::new()),
            close_keys: Mutex::new(Vec::new()),
            first_failure: Mutex::new(Some(FirstFailure::SidecarTransportClosed)),
        });
        let terminal_reason = test_terminal_reason();
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("source-manual-rescan.node_a:v1.req".into()),
            "test-endpoint".into(),
            BoundaryContext::for_unit("runtime.exec.source"),
            CancellationToken::new(),
            Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
            terminal_reason.clone(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec!["source-manual-rescan.node_a:v1.req".to_string()],
            "sidecar transport close means the endpoint is pinned to a stale worker boundary and must exit for same-turn rearm"
        );
        assert!(
            terminal_reason
                .lock()
                .expect("terminal_reason lock")
                .as_deref()
                .is_some_and(|reason| reason.contains("sidecar control bridge stopped")),
            "terminal reason should preserve the sidecar transport-close evidence"
        );
    }

    #[test]
    fn endpoint_loop_retries_recv_panics_without_silent_task_death() {
        let boundary = Arc::new(PanicOnceThenStopBoundary::new());
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("source-status:v1.req".into()),
            "test-endpoint".into(),
            BoundaryContext::for_unit("runtime.exec.query-peer"),
            CancellationToken::new(),
            Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
            test_terminal_reason(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec![
                "source-status:v1.req".to_string(),
                "source-status:v1.req".to_string()
            ],
            "endpoint recv panic should be retried instead of letting the endpoint thread die and rely on respawn"
        );
    }

    #[test]
    fn endpoint_loop_arms_fallback_query_recv_concurrently_with_idle_query_peer_lane() {
        let boundary = Arc::new(ConcurrentFallbackRecvInterestBoundary::new());
        let handler_called = Arc::new(AtomicBool::new(false));
        let mut endpoint = ManagedEndpointTask::spawn_with_units(
            boundary.clone(),
            RouteKey("source-status:v1.req".into()),
            "test-endpoint",
            [
                "runtime.exec.query-peer".to_string(),
                "runtime.exec.query".to_string(),
            ],
            CancellationToken::new(),
            {
                let handler_called = handler_called.clone();
                move |_events: Vec<Event>| {
                    handler_called.store(true, Ordering::SeqCst);
                    std::future::ready(Vec::new())
                }
            },
        );

        crate::runtime_app::shared_tokio_runtime().block_on(async {
            boundary.query_peer_recv_started.notified().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            boundary
                .channel_send(
                    BoundaryContext::default(),
                    ChannelSendRequest {
                        channel_key: ChannelKey("source-status:v1.req".into()),
                        events: vec![test_event(1, b"request")],
                        timeout_ms: Some(100),
                    },
                )
                .await
                .expect("queue source-status request");
            tokio::time::sleep(Duration::from_millis(100)).await;
            endpoint.shutdown(Duration::from_secs(1)).await;
        });

        assert!(
            handler_called.load(Ordering::SeqCst),
            "multi-unit source-status endpoint must keep the fallback query recv interest armed while the preferred query-peer lane is idle so peer-only requests can reach handler recv instead of being dropped before any request-batch recv evidence"
        );
        assert!(
            !boundary
                .dropped_without_query_recv_interest
                .load(Ordering::SeqCst),
            "source-status request must not be dropped before the fallback query recv interest is armed"
        );
    }

    #[test]
    fn finished_endpoint_records_classified_terminal_reason_before_respawn() {
        let boundary = Arc::new(RecordingBoundary::new());
        let mut endpoint = ManagedEndpointTask::spawn(
            boundary,
            RouteKey("source-status:v1.req".into()),
            "test-endpoint",
            CancellationToken::new(),
            |_events: Vec<Event>| std::future::ready(Vec::new()),
        );

        let started = std::time::Instant::now();
        while !endpoint.is_finished() && started.elapsed() < Duration::from_secs(1) {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            endpoint.is_finished(),
            "endpoint should have exited for the test seam"
        );
        assert_eq!(
            endpoint.finish_reason().as_deref(),
            Some("recv_failed:internal error: stop after first recv"),
            "finished endpoints must carry a classified reason before runtime_app prunes and respawns the route"
        );

        crate::runtime_app::shared_tokio_runtime()
            .block_on(endpoint.shutdown(Duration::from_secs(1)));
    }

    #[test]
    fn endpoint_loop_keeps_reusable_request_and_reply_routes_open_on_terminal_exit() {
        let boundary = Arc::new(RecordingBoundary::new());
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("source-status:v1.req".into()),
            "test-endpoint".into(),
            BoundaryContext::for_unit("runtime.exec.query"),
            CancellationToken::new(),
            Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
            test_terminal_reason(),
        ));

        let close_keys = boundary.close_keys.lock().expect("close_keys lock").clone();
        assert!(
            !close_keys.contains(&"source-status:v1.req".to_string())
                && !close_keys.contains(&"source-status:v1.req:reply".to_string()),
            "endpoint exit must not permanently close reusable request/reply routes before respawn; close_keys={close_keys:?}"
        );
    }

    #[test]
    fn endpoint_loop_retries_transient_reply_send_timeout_and_handles_later_batches() {
        let boundary = Arc::new(ReplyTimeoutThenRecoverBoundary::new(
            vec![test_event(1, b"first")],
            vec![test_event(2, b"second")],
        ));
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("materialized-find:v1.req".into()),
            "sink:fs-meta.internal:sink.query".into(),
            BoundaryContext::for_unit("runtime.exec.sink"),
            CancellationToken::new(),
            Arc::new(|events: Vec<Event>| std::future::ready(events)),
            test_terminal_reason(),
        ));

        let send_keys = boundary.send_keys.lock().expect("send_keys lock").clone();
        assert_eq!(
            send_keys,
            vec![
                "materialized-find:v1.req:reply".to_string(),
                "materialized-find:v1.req:reply".to_string(),
                "materialized-find:v1.req:reply".to_string()
            ],
            "endpoint loop must retry the same reply batch before serving later materialized batches after a transient reply send timeout"
        );
        let send_payloads = boundary
            .send_payloads
            .lock()
            .expect("send_payloads lock")
            .clone();
        assert_eq!(
            send_payloads,
            vec![b"first".to_vec(), b"first".to_vec(), b"second".to_vec()],
            "transient reply send timeout must not drop the already generated first reply"
        );
    }

    #[test]
    fn endpoint_loop_retries_transient_reply_send_backpressure_and_handles_later_batches() {
        let boundary = Arc::new(ReplyTimeoutThenRecoverBoundary::new_with_first_send_step(
            vec![test_event(1, b"first")],
            vec![test_event(2, b"second")],
            SendStep::Backpressure,
        ));
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("materialized-find:v1.req".into()),
            "sink:fs-meta.internal:sink.query".into(),
            BoundaryContext::for_unit("runtime.exec.sink"),
            CancellationToken::new(),
            Arc::new(|events: Vec<Event>| std::future::ready(events)),
            test_terminal_reason(),
        ));

        let send_keys = boundary.send_keys.lock().expect("send_keys lock").clone();
        assert_eq!(
            send_keys,
            vec![
                "materialized-find:v1.req:reply".to_string(),
                "materialized-find:v1.req:reply".to_string(),
                "materialized-find:v1.req:reply".to_string()
            ],
            "endpoint loop must retry the same reply batch before serving later materialized batches after transient reply send backpressure"
        );
        let send_payloads = boundary
            .send_payloads
            .lock()
            .expect("send_payloads lock")
            .clone();
        assert_eq!(
            send_payloads,
            vec![b"first".to_vec(), b"first".to_vec(), b"second".to_vec()],
            "transient reply send backpressure must not drop the already generated first reply"
        );
    }

    #[test]
    fn stream_loop_retries_stale_drained_fenced_grant_attachment_errors() {
        let boundary = Arc::new(RecordingBoundary {
            recv_keys: Mutex::new(Vec::new()),
            recv_unit_ids: Mutex::new(Vec::new()),
            close_keys: Mutex::new(Vec::new()),
            first_failure: Mutex::new(Some(FirstFailure::StaleGrantAttachment)),
        });
        crate::runtime_app::shared_tokio_runtime().block_on(run_stream_loop(
            boundary.clone(),
            RouteKey("fs-meta.events:v1.stream".into()),
            "test-stream".into(),
            "runtime.exec.sink".to_string(),
            CancellationToken::new(),
            Arc::new(|| true),
            Arc::new(|| {}),
            Arc::new(|_events: Vec<Event>| std::future::ready(())),
            test_terminal_reason(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec![
                "fs-meta.events:v1.stream".to_string(),
                "fs-meta.events:v1.stream".to_string()
            ]
        );
        let close_keys = boundary.close_keys.lock().expect("close_keys lock").clone();
        assert_eq!(close_keys, vec!["fs-meta.events:v1.stream".to_string()]);
    }

    #[test]
    fn stream_loop_must_not_spin_forever_on_persistent_stale_grant_attachment_recv_gaps() {
        let boundary = Arc::new(PersistentStaleGrantAttachmentBoundary::new());
        let terminal_reason = test_terminal_reason();
        let result = crate::runtime_app::shared_tokio_runtime().block_on(async {
            tokio::time::timeout(
                Duration::from_millis(800),
                run_stream_loop(
                    boundary.clone(),
                    RouteKey("source-logical-roots-control:v1.stream".into()),
                    "test-stream".into(),
                    "runtime.exec.source".to_string(),
                    CancellationToken::new(),
                    Arc::new(|| true),
                    Arc::new(|| {}),
                    Arc::new(|_events: Vec<Event>| std::future::ready(())),
                    terminal_reason.clone(),
                ),
            )
            .await
        });

        assert!(
            result.is_ok(),
            "persistent stale grant-attachment recv gaps must terminate this stream loop with a classified reason instead of spinning forever on one stale generation attachment"
        );
        let reason = terminal_reason
            .lock()
            .expect("terminal_reason lock")
            .clone()
            .unwrap_or_default();
        assert!(
            reason.contains("stale grant-attachment"),
            "terminal reason should preserve stale grant-attachment continuity evidence; reason={reason}"
        );
        assert!(
            boundary.recv_attempts.load(Ordering::SeqCst) > 1,
            "stream loop should still retry stale recv gaps before escalating"
        );
    }

    #[test]
    fn stream_loop_exits_on_authoritative_ipc_transport_close() {
        let boundary = Arc::new(RecordingBoundary {
            recv_keys: Mutex::new(Vec::new()),
            recv_unit_ids: Mutex::new(Vec::new()),
            close_keys: Mutex::new(Vec::new()),
            first_failure: Mutex::new(Some(FirstFailure::AuthoritativeIpcTransportClosed)),
        });
        let terminal_reason = test_terminal_reason();
        crate::runtime_app::shared_tokio_runtime().block_on(run_stream_loop(
            boundary.clone(),
            RouteKey("fs-meta.events:v1.stream".into()),
            "test-stream".into(),
            "runtime.exec.sink".to_string(),
            CancellationToken::new(),
            Arc::new(|| true),
            Arc::new(|| {}),
            Arc::new(|_events: Vec<Event>| std::future::ready(())),
            terminal_reason.clone(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec!["fs-meta.events:v1.stream".to_string()],
            "authoritative IPC transport close must stop the stream loop instead of retrying forever on a dead worker-side IPC plane"
        );
        assert!(
            terminal_reason
                .lock()
                .expect("terminal_reason lock")
                .as_deref()
                .is_some_and(|reason| reason.contains("IPC control transport closed")),
            "terminal stream reason should preserve the authoritative IPC transport-close evidence"
        );
    }

    #[test]
    fn stream_loop_exits_on_sidecar_transport_close() {
        let boundary = Arc::new(RecordingBoundary {
            recv_keys: Mutex::new(Vec::new()),
            recv_unit_ids: Mutex::new(Vec::new()),
            close_keys: Mutex::new(Vec::new()),
            first_failure: Mutex::new(Some(FirstFailure::SidecarTransportClosed)),
        });
        let terminal_reason = test_terminal_reason();
        crate::runtime_app::shared_tokio_runtime().block_on(run_stream_loop(
            boundary.clone(),
            RouteKey("source-manual-rescan-control:v1.stream".into()),
            "test-stream".into(),
            "runtime.exec.source".to_string(),
            CancellationToken::new(),
            Arc::new(|| true),
            Arc::new(|| {}),
            Arc::new(|_events: Vec<Event>| std::future::ready(())),
            terminal_reason.clone(),
        ));

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec!["source-manual-rescan-control:v1.stream".to_string()],
            "sidecar transport close means the stream endpoint is pinned to a stale worker boundary and must exit for same-turn rearm"
        );
        assert!(
            terminal_reason
                .lock()
                .expect("terminal_reason lock")
                .as_deref()
                .is_some_and(|reason| reason.contains("sidecar control bridge stopped")),
            "terminal stream reason should preserve the sidecar transport-close evidence"
        );
    }

    #[test]
    fn wait_until_ready_waits_for_receive_arm_signal_inside_runtime() {
        let (tx, rx) = sync_channel::<()>(1);
        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(80));
            let _ = tx.send(());
        });
        let started = std::time::Instant::now();
        crate::runtime_app::shared_tokio_runtime().block_on(async {
            ManagedEndpointTask::wait_until_ready("test-endpoint", rx);
        });
        sender
            .join()
            .expect("ready signal sender should join cleanly");
        assert!(
            started.elapsed() >= Duration::from_millis(60),
            "ready wait must not return before endpoint receive interest is armed"
        );
    }
}

fn shared_correlation_id(requests: &[Event]) -> Option<u64> {
    let first = requests.first()?.metadata().correlation_id?;
    if requests
        .iter()
        .all(|event| event.metadata().correlation_id == Some(first))
    {
        Some(first)
    } else {
        None
    }
}
