use std::sync::Arc;
use std::sync::mpsc::{Receiver, TryRecvError, sync_channel};
use std::time::Duration;

use capanix_app_sdk::runtime::RouteKey;
use capanix_app_sdk::{CnxError, Event};
use capanix_runtime_entry_sdk::advanced::boundary::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
};
use tokio_util::sync::CancellationToken;

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

pub(crate) struct ManagedEndpointTask {
    name: String,
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
        let name_owned = name.into();
        let join_name = name_owned.clone();
        let shutdown_for_task = shutdown.clone();
        let handler = Arc::new(handler);
        let runner = run_endpoint_loop(boundary, route, join_name, shutdown_for_task, handler);
        let (join, ready_rx) = Self::spawn_join_with_ready(runner);
        Self::wait_until_ready(&name_owned, ready_rx);

        Self {
            name: name_owned,
            shutdown,
            join: Some(join),
        }
    }

    pub(crate) fn spawn_stream<F, Fut, G>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
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
        let join_name = name_owned.clone();
        let shutdown_for_task = shutdown.clone();
        let should_recv = Arc::new(should_recv);
        let handler = Arc::new(handler);
        let runner = run_stream_loop(
            boundary,
            route,
            join_name,
            shutdown_for_task,
            should_recv,
            handler,
        );
        let (join, ready_rx) = Self::spawn_join_with_ready(runner);
        Self::wait_until_ready(&name_owned, ready_rx);

        Self {
            name: name_owned,
            shutdown,
            join: Some(join),
        }
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
    shutdown_for_task: CancellationToken,
    handler: Arc<F>,
) where
    F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Vec<Event>> + Send + 'static,
{
    if debug_source_status_lifecycle_enabled() {
        eprintln!(
            "fs_meta_runtime_endpoint: loop_start route={} task={} thread={:?}",
            route.0,
            join_name,
            std::thread::current().name()
        );
    }
    let ctx = BoundaryContext::default();
    let request_channel = ChannelKey(route.0.clone());
    let reply_channel = ChannelKey(format!("{}:reply", route.0));

    loop {
        if shutdown_for_task.is_cancelled() {
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
                log::debug!(
                    "endpoint task {} recv retry for {} after transient error: {:?}",
                    join_name,
                    route.0,
                    err
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(err) => {
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
            log::warn!(
                "endpoint task {} send failed for {}: {:?}",
                join_name,
                route.0,
                err
            );
            break;
        }
        eprintln!(
            "fs_meta_runtime_endpoint: request batch replies sent route={} task={} events={}",
            reply_channel.0, join_name, response_count
        );
    }
}

async fn run_stream_loop<F, Fut, G>(
    boundary: Arc<dyn ChannelIoSubset>,
    route: RouteKey,
    join_name: String,
    shutdown_for_task: CancellationToken,
    should_recv: Arc<G>,
    handler: Arc<F>,
) where
    F: Fn(Vec<Event>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
    G: Fn() -> bool + Send + Sync + 'static,
{
    let ctx = BoundaryContext::default();
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
    use std::sync::Mutex;

    struct RecordingBoundary {
        recv_keys: Mutex<Vec<String>>,
        first_failure: Mutex<Option<FirstFailure>>,
    }

    struct ThreadRecordingBoundary {
        thread_name_tx: Mutex<Option<std::sync::mpsc::SyncSender<Option<String>>>>,
    }

    #[derive(Clone, Copy)]
    enum FirstFailure {
        NotSupported,
        Timeout,
    }

    impl RecordingBoundary {
        fn new() -> Self {
            Self {
                recv_keys: Mutex::new(Vec::new()),
                first_failure: Mutex::new(None),
            }
        }

        fn fail_once() -> Self {
            Self {
                recv_keys: Mutex::new(Vec::new()),
                first_failure: Mutex::new(Some(FirstFailure::NotSupported)),
            }
        }

        fn timeout_once() -> Self {
            Self {
                recv_keys: Mutex::new(Vec::new()),
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
            let mut first_failure = self.first_failure.lock().expect("first_failure lock");
            if let Some(failure) = first_failure.take() {
                return match failure {
                    FirstFailure::NotSupported => {
                        Err(CnxError::NotSupported("transient attach gap".into()))
                    }
                    FirstFailure::Timeout => Err(CnxError::Timeout),
                };
            }
            Err(CnxError::Internal("stop after first recv".into()))
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
    fn stream_loop_uses_exact_route_key_without_double_suffix() {
        let boundary = Arc::new(RecordingBoundary::new());
        crate::runtime_app::shared_tokio_runtime().block_on(run_stream_loop(
            boundary.clone(),
            RouteKey("fs-meta.events:v1.stream".into()),
            "test-stream".into(),
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
    fn endpoint_loop_retries_timeout_recv_errors() {
        let boundary = Arc::new(RecordingBoundary::timeout_once());
        crate::runtime_app::shared_tokio_runtime().block_on(run_endpoint_loop(
            boundary.clone(),
            RouteKey("sink-status:v1.req".into()),
            "test-endpoint".into(),
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
