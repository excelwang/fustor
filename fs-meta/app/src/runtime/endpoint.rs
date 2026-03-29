use std::sync::Arc;
use std::sync::mpsc::{Receiver, TryRecvError, sync_channel};
use std::time::Duration;

use capanix_app_sdk::runtime::RouteKey;
use capanix_app_sdk::{CnxError, Event};
use capanix_runtime_entry_sdk::advanced::boundary::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
};
use tokio_util::sync::CancellationToken;

use crate::runtime::routes::ROUTE_KEY_SINK_QUERY_INTERNAL;

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

fn is_materialized_internal_query_route(route: &RouteKey) -> bool {
    route.0 == format!("{}.req", ROUTE_KEY_SINK_QUERY_INTERNAL)
}

fn is_stale_grant_attachment_recv_gap(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::AccessDenied(message) | CnxError::PeerError(message)
            if message.contains("drained/fenced")
                && message.contains("grant attachments")
    )
}

async fn close_stale_recv_channel(
    boundary: Arc<dyn ChannelIoSubset>,
    ctx: BoundaryContext,
    channel: ChannelKey,
) {
    let close_boundary = boundary.clone();
    let close_ctx = ctx.clone();
    let close_channel = channel.clone();
    match tokio::task::spawn_blocking(move || close_boundary.channel_close(close_ctx, close_channel))
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
        *counts.entry(event.metadata().origin_id.0.clone()).or_default() += 1;
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

pub(crate) struct ManagedEndpointTask {
    name: String,
    route_key: String,
    shutdown: CancellationToken,
    join: Option<EndpointJoin>,
}

impl ManagedEndpointTask {
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
        match ready_rx.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => {}
            Err(TryRecvError::Empty) => {
                log::debug!("endpoint task {} ready wait deferred", name);
            }
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
        let name_owned = name.into();
        let route_key = route.0.clone();
        let join_name = name_owned.clone();
        let shutdown_for_task = shutdown.clone();
        let handler = Arc::new(handler);
        let runner = run_endpoint_loop(boundary, route, join_name, ctx, shutdown_for_task, handler);
        let (join, ready_rx) = Self::spawn_join_with_ready(runner);
        Self::wait_until_ready(&name_owned, ready_rx);

        Self {
            name: name_owned,
            route_key,
            shutdown,
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
        let name_owned = name.into();
        let route_key = route.0.clone();
        let unit_id = unit_id.into();
        let join_name = name_owned.clone();
        let shutdown_for_task = shutdown.clone();
        let should_recv = Arc::new(should_recv);
        let handler = Arc::new(handler);
        let runner = run_stream_loop(
            boundary,
            route,
            join_name,
            unit_id,
            shutdown_for_task,
            should_recv,
            handler,
        );
        let (join, ready_rx) = Self::spawn_join_with_ready(runner);
        Self::wait_until_ready(&name_owned, ready_rx);

        Self {
            name: name_owned,
            route_key,
            shutdown,
            join: Some(join),
        }
    }

    pub(crate) fn route_key(&self) -> &str {
        &self.route_key
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.join.as_ref().is_none_or(EndpointJoin::is_finished)
    }

    pub(crate) async fn shutdown(&mut self, wait_timeout: Duration) {
        self.shutdown.cancel();
        let Some(join) = self.join.take() else {
            return;
        };
        match join {
            EndpointJoin::Thread(join) => {
                if tokio::runtime::Handle::try_current().is_ok() {
                    let blocking_join = tokio::task::spawn_blocking(move || join.join());
                    match tokio::time::timeout(wait_timeout, blocking_join).await {
                        Ok(Ok(Ok(()))) => {}
                        Ok(Ok(Err(err))) => {
                            log::warn!("endpoint task {} thread panicked: {:?}", self.name, err);
                        }
                        Ok(Err(err)) => {
                            log::warn!(
                                "endpoint task {} join wrapper failed: {:?}",
                                self.name,
                                err
                            );
                        }
                        Err(_) => {
                            log::warn!(
                                "endpoint task {} thread did not exit within {:?}",
                                self.name,
                                wait_timeout
                            );
                        }
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
) where
    F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Vec<Event>> + Send + 'static,
{
    let debug_materialized_route = debug_materialized_route_lifecycle_enabled()
        && is_materialized_internal_query_route(&route);
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

    loop {
        if shutdown_for_task.is_cancelled() {
            exit_reason = Some("shutdown_cancelled".into());
            break;
        }
        let requests = match boundary
            .channel_recv(
                ctx.clone(),
                ChannelRecvRequest {
                    channel_key: request_channel.clone(),
                    timeout_ms: Some(Duration::from_millis(250).as_millis() as u64),
                },
            )
            .await
        {
            Ok(events) => events,
            Err(CnxError::Timeout) => continue,
            Err(err @ CnxError::NotSupported(_))
            | Err(err @ CnxError::NotReady(_))
            | Err(err @ CnxError::TransportClosed(_))
            | Err(err @ CnxError::ChannelClosed)
            | Err(err @ CnxError::LinkError(_)) => {
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
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(err) if is_stale_grant_attachment_recv_gap(&err) => {
                close_stale_recv_channel(boundary.clone(), ctx.clone(), request_channel.clone())
                    .await;
                log::debug!(
                    "endpoint task {} recv retry for {} after stale grant-attachment gap: {:?}",
                    join_name,
                    route.0,
                    err
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(err) => {
                exit_reason = Some(format!("recv_failed:{err}"));
                log::warn!(
                    "endpoint task {} recv failed for {}: {:?}",
                    join_name,
                    route.0,
                    err
                );
                break;
            }
        };

        if requests.is_empty() {
            continue;
        }
        eprintln!(
            "fs_meta_runtime_endpoint: request batch recv route={} task={} events={}",
            request_channel.0,
            join_name,
            requests.len()
        );

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
            eprintln!(
                "fs_meta_runtime_endpoint: request batch handled with no replies route={} task={}",
                request_channel.0, join_name
            );
            continue;
        }
        let response_count = responses.len();
        if let Err(err) = boundary
            .channel_send(
                ctx.clone(),
                ChannelSendRequest {
                    channel_key: reply_channel.clone(),
                    events: responses,
                    timeout_ms: Some(Duration::from_millis(250).as_millis() as u64),
                },
            )
            .await
        {
            match err {
                err @ CnxError::Timeout
                | err @ CnxError::NotSupported(_)
                | err @ CnxError::NotReady(_)
                | err @ CnxError::TransportClosed(_)
                | err @ CnxError::ChannelClosed
                | err @ CnxError::LinkError(_) => {
                    if debug_materialized_route {
                        eprintln!(
                            "fs_meta_runtime_endpoint: materialized_route send_retry route={} task={} err={}",
                            route.0, join_name, err
                        );
                    }
                    log::debug!(
                        "endpoint task {} send retry for {} after transient error: {:?}",
                        join_name,
                        route.0,
                        err
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                err => {
                    log::warn!(
                        "endpoint task {} send failed for {}: {:?}",
                        join_name,
                        route.0,
                        err
                    );
                    exit_reason = Some(format!("send_failed:{err}"));
                    break;
                }
            }
        }
        eprintln!(
            "fs_meta_runtime_endpoint: request batch replies sent route={} task={} events={}",
            reply_channel.0, join_name, response_count
        );
    }

    if debug_materialized_route {
        eprintln!(
            "fs_meta_runtime_endpoint: materialized_route loop_exit route={} task={} reason={}",
            route.0,
            join_name,
            exit_reason.unwrap_or_else(|| "loop_returned".into())
        );
    }
}

async fn run_stream_loop<F, Fut, G>(
    boundary: Arc<dyn ChannelIoSubset>,
    route: RouteKey,
    join_name: String,
    unit_id: String,
    shutdown_for_task: CancellationToken,
    should_recv: Arc<G>,
    handler: Arc<F>,
) where
    F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
    G: Fn() -> bool + Send + Sync + 'static,
{
    let ctx = BoundaryContext::for_unit(unit_id);
    let stream_channel = ChannelKey(route.0.clone());

    loop {
        if shutdown_for_task.is_cancelled() {
            break;
        }
        if !should_recv() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        eprintln!(
            "fs_meta_runtime_endpoint: stream loop recv route={} task={}",
            stream_channel.0, join_name
        );
        let events = match boundary
            .channel_recv(
                ctx.clone(),
                ChannelRecvRequest {
                    channel_key: stream_channel.clone(),
                    timeout_ms: Some(Duration::from_millis(250).as_millis() as u64),
                },
            )
            .await
        {
            Ok(events) => events,
            Err(CnxError::Timeout) => continue,
            Err(err @ CnxError::NotSupported(_))
            | Err(err @ CnxError::NotReady(_))
            | Err(err @ CnxError::TransportClosed(_))
            | Err(err @ CnxError::ChannelClosed)
            | Err(err @ CnxError::LinkError(_)) => {
                eprintln!(
                    "fs_meta_runtime_endpoint: transient stream recv gap task={} route={} err={:?}",
                    join_name, stream_channel.0, err
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(err) if is_stale_grant_attachment_recv_gap(&err) => {
                close_stale_recv_channel(boundary.clone(), ctx.clone(), stream_channel.clone())
                    .await;
                eprintln!(
                    "fs_meta_runtime_endpoint: stale grant-attachment recv gap task={} route={} err={:?}",
                    join_name, stream_channel.0, err
                );
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
        recv_steps: Mutex<VecDeque<RecvStep>>,
        send_steps: Mutex<VecDeque<SendStep>>,
    }

    #[derive(Clone, Copy)]
    enum FirstFailure {
        NotSupported,
        Timeout,
        StaleGrantAttachment,
    }

    enum RecvStep {
        Events(Vec<Event>),
        InternalStop,
    }

    enum SendStep {
        Timeout,
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
            Self {
                recv_keys: Mutex::new(Vec::new()),
                send_keys: Mutex::new(Vec::new()),
                recv_steps: Mutex::new(VecDeque::from([
                    RecvStep::Events(first),
                    RecvStep::Events(second),
                    RecvStep::InternalStop,
                ])),
                send_steps: Mutex::new(VecDeque::from([SendStep::Timeout, SendStep::Ok])),
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
            match self
                .send_steps
                .lock()
                .expect("send_steps lock")
                .pop_front()
                .unwrap_or(SendStep::Ok)
            {
                SendStep::Timeout => Err(CnxError::Timeout),
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
    fn endpoint_loop_uses_runtime_unit_context_for_recv() {
        let boundary = Arc::new(RecordingBoundary::new());
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("materialized-find:v1.req".into()),
            "test-endpoint".into(),
            BoundaryContext::for_unit("runtime.exec.sink"),
            CancellationToken::new(),
            Arc::new(|_events: Vec<Event>| std::future::ready(Vec::new())),
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
            Arc::new(|_events: Vec<Event>| std::future::ready(())),
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
            Arc::new(|_events: Vec<Event>| std::future::ready(())),
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
            Arc::new(|_events: Vec<Event>| std::future::ready(())),
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
        assert_eq!(close_keys, vec!["sink-status:v1.req".to_string()]);
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
        ));

        let send_keys = boundary.send_keys.lock().expect("send_keys lock").clone();
        assert_eq!(
            send_keys,
            vec![
                "materialized-find:v1.req:reply".to_string(),
                "materialized-find:v1.req:reply".to_string()
            ],
            "endpoint loop should keep serving later materialized batches after a transient reply send timeout"
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
            Arc::new(|_events: Vec<Event>| std::future::ready(())),
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
    fn wait_until_ready_does_not_block_inside_runtime() {
        let (_tx, rx) = sync_channel::<()>(1);
        let started = std::time::Instant::now();
        crate::runtime_app::shared_tokio_runtime().block_on(async {
            ManagedEndpointTask::wait_until_ready("test-endpoint", rx);
        });
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "ready wait should not block tokio runtime threads"
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
