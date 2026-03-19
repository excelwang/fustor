use std::sync::Arc;
use std::sync::mpsc::{Receiver, sync_channel};
use std::time::Duration;

use capanix_app_sdk::raw::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
};
use capanix_app_sdk::runtime::RouteKey;
use capanix_app_sdk::{CnxError, Event};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

enum EndpointJoin {
    Tokio(JoinHandle<()>),
    Thread(std::thread::JoinHandle<()>),
}

const ENDPOINT_TASK_READY_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) struct ManagedEndpointTask {
    name: String,
    shutdown: CancellationToken,
    join: Option<EndpointJoin>,
}

impl ManagedEndpointTask {
    fn spawn_join(runner: impl FnOnce() + Send + 'static) -> EndpointJoin {
        if tokio::runtime::Handle::try_current().is_ok() {
            EndpointJoin::Tokio(tokio::task::spawn_blocking(runner))
        } else {
            EndpointJoin::Thread(std::thread::spawn(runner))
        }
    }

    fn spawn_join_with_ready(
        runner: impl FnOnce() + Send + 'static,
    ) -> (EndpointJoin, Receiver<()>) {
        let (ready_tx, ready_rx) = sync_channel(1);
        let join = Self::spawn_join(move || {
            let _ = ready_tx.send(());
            runner();
        });
        (join, ready_rx)
    }

    fn wait_until_ready(name: &str, ready_rx: Receiver<()>) {
        if ready_rx.recv_timeout(ENDPOINT_TASK_READY_TIMEOUT).is_err() {
            log::warn!(
                "endpoint task {} did not report ready within {:?}",
                name,
                ENDPOINT_TASK_READY_TIMEOUT
            );
        }
    }

    pub(crate) fn spawn<F>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
        shutdown: CancellationToken,
        handler: F,
    ) -> Self
    where
        F: Fn(Vec<Event>) -> Vec<Event> + Send + Sync + 'static,
    {
        let name_owned = name.into();
        let join_name = name_owned.clone();
        let shutdown_for_task = shutdown.clone();
        let handler = Arc::new(handler);
        let runner =
            move || run_endpoint_loop(boundary, route, join_name, shutdown_for_task, handler);
        let (join, ready_rx) = Self::spawn_join_with_ready(runner);
        Self::wait_until_ready(&name_owned, ready_rx);

        Self {
            name: name_owned,
            shutdown,
            join: Some(join),
        }
    }

    pub(crate) fn spawn_stream<F, G>(
        boundary: Arc<dyn ChannelIoSubset>,
        route: RouteKey,
        name: impl Into<String>,
        shutdown: CancellationToken,
        should_recv: G,
        handler: F,
    ) -> Self
    where
        F: Fn(Vec<Event>) + Send + Sync + 'static,
        G: Fn() -> bool + Send + Sync + 'static,
    {
        let name_owned = name.into();
        let join_name = name_owned.clone();
        let shutdown_for_task = shutdown.clone();
        let should_recv = Arc::new(should_recv);
        let handler = Arc::new(handler);
        let runner = move || {
            run_stream_loop(
                boundary,
                route,
                join_name,
                shutdown_for_task,
                should_recv,
                handler,
            )
        };
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
            EndpointJoin::Tokio(join) => match tokio::time::timeout(wait_timeout, join).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    log::warn!("endpoint task {} join failed: {:?}", self.name, err);
                }
                Err(_) => {
                    log::warn!(
                        "endpoint task {} did not exit within {:?}",
                        self.name,
                        wait_timeout
                    );
                }
            },
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

fn run_endpoint_loop<F>(
    boundary: Arc<dyn ChannelIoSubset>,
    route: RouteKey,
    join_name: String,
    shutdown_for_task: CancellationToken,
    handler: Arc<F>,
) where
    F: Fn(Vec<Event>) -> Vec<Event> + Send + Sync + 'static,
{
    let ctx = BoundaryContext::default();
    let request_channel = ChannelKey(route.0.clone());
    let reply_channel = ChannelKey(format!("{}:reply", route.0));

    loop {
        if shutdown_for_task.is_cancelled() {
            break;
        }
        let requests = match boundary.channel_recv(
            ctx.clone(),
            ChannelRecvRequest {
                channel_key: request_channel.clone(),
                timeout_ms: Some(Duration::from_millis(250).as_millis() as u64),
            },
        ) {
            Ok(events) => events,
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

        let correlation_id = shared_correlation_id(&requests);
        let mut responses = handler(requests);
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
            continue;
        }
        if let Err(err) = boundary.channel_send(
            ctx.clone(),
            ChannelSendRequest {
                channel_key: reply_channel.clone(),
                events: responses,
            },
        ) {
            log::warn!(
                "endpoint task {} send failed for {}: {:?}",
                join_name,
                route.0,
                err
            );
            break;
        }
    }
}

fn run_stream_loop<F, G>(
    boundary: Arc<dyn ChannelIoSubset>,
    route: RouteKey,
    join_name: String,
    shutdown_for_task: CancellationToken,
    should_recv: Arc<G>,
    handler: Arc<F>,
) where
    F: Fn(Vec<Event>) + Send + Sync + 'static,
    G: Fn() -> bool + Send + Sync + 'static,
{
    let ctx = BoundaryContext::default();
    let stream_channel = ChannelKey(route.0.clone());

    loop {
        if shutdown_for_task.is_cancelled() {
            break;
        }
        if !should_recv() {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        }
        let events = match boundary.channel_recv(
            ctx.clone(),
            ChannelRecvRequest {
                channel_key: stream_channel.clone(),
                timeout_ms: Some(Duration::from_millis(250).as_millis() as u64),
            },
        ) {
            Ok(events) => events,
            Err(CnxError::Timeout) => continue,
            Err(CnxError::NotSupported(_))
            | Err(CnxError::NotReady(_))
            | Err(CnxError::TransportClosed(_))
            | Err(CnxError::ChannelClosed)
            | Err(CnxError::LinkError(_)) => {
                eprintln!(
                    "fs_meta_runtime_endpoint: transient stream recv gap task={} route={}",
                    join_name,
                    stream_channel.0
                );
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
            Err(err) => {
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

        handler(events);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct RecordingBoundary {
        recv_keys: Mutex<Vec<String>>,
        fail_once: Mutex<bool>,
    }

    impl RecordingBoundary {
        fn new() -> Self {
            Self {
                recv_keys: Mutex::new(Vec::new()),
                fail_once: Mutex::new(false),
            }
        }

        fn fail_once() -> Self {
            Self {
                recv_keys: Mutex::new(Vec::new()),
                fail_once: Mutex::new(true),
            }
        }
    }

    impl ChannelIoSubset for RecordingBoundary {
        fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: ChannelRecvRequest,
        ) -> capanix_app_sdk::Result<Vec<Event>> {
            self.recv_keys
                .lock()
                .expect("recv_keys lock")
                .push(request.channel_key.0);
            let mut fail_once = self.fail_once.lock().expect("fail_once lock");
            if *fail_once {
                *fail_once = false;
                return Err(CnxError::NotSupported("transient attach gap".into()));
            }
            Err(CnxError::Internal("stop after first recv".into()))
        }
    }

    #[test]
    fn stream_loop_uses_exact_route_key_without_double_suffix() {
        let boundary = Arc::new(RecordingBoundary::new());
        run_stream_loop(
            boundary.clone(),
            RouteKey("fs-meta.events:v1.stream".into()),
            "test-stream".into(),
            CancellationToken::new(),
            Arc::new(|| true),
            Arc::new(|_events: Vec<Event>| {}),
        );

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(recv_keys, vec!["fs-meta.events:v1.stream".to_string()]);
    }

    #[test]
    fn stream_loop_retries_transient_recv_errors() {
        let boundary = Arc::new(RecordingBoundary::fail_once());
        run_stream_loop(
            boundary.clone(),
            RouteKey("fs-meta.events:v1.stream".into()),
            "test-stream".into(),
            CancellationToken::new(),
            Arc::new(|| true),
            Arc::new(|_events: Vec<Event>| {}),
        );

        let recv_keys = boundary.recv_keys.lock().expect("recv_keys lock").clone();
        assert_eq!(
            recv_keys,
            vec![
                "fs-meta.events:v1.stream".to_string(),
                "fs-meta.events:v1.stream".to_string()
            ]
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
