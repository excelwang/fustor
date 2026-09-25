#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use capanix_app_sdk::raw::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
};
use capanix_app_sdk::runtime::{NodeId, RouteKey};
use capanix_app_sdk::{BoundRouteClient, BoundRouteServer, CnxError, Event, Result};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct InMemoryChannelIo {
    queues: Mutex<HashMap<String, VecDeque<Vec<Event>>>>,
    closed: Mutex<HashSet<String>>,
    notify: Notify,
}

#[async_trait]
impl ChannelIoSubset for InMemoryChannelIo {
    async fn channel_send(&self, _ctx: BoundaryContext, request: ChannelSendRequest) -> Result<()> {
        if self.closed.lock().unwrap().contains(&request.channel_key.0) {
            return Err(CnxError::ChannelClosed);
        }
        self.queues
            .lock()
            .unwrap()
            .entry(request.channel_key.0)
            .or_default()
            .push_back(request.events);
        self.notify.notify_waiters();
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> Result<Vec<Event>> {
        let channel = request.channel_key.0;
        let deadline = request
            .timeout_ms
            .map(|timeout_ms| Instant::now() + Duration::from_millis(timeout_ms));
        loop {
            if let Some(batch) = self
                .queues
                .lock()
                .unwrap()
                .get_mut(&channel)
                .and_then(VecDeque::pop_front)
            {
                return Ok(batch);
            }
            if self.closed.lock().unwrap().contains(&channel) {
                return Err(CnxError::ChannelClosed);
            }
            match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(CnxError::Timeout);
                    }
                    if tokio::time::timeout(deadline - now, self.notify.notified())
                        .await
                        .is_err()
                    {
                        return Err(CnxError::Timeout);
                    }
                }
                None => self.notify.notified().await,
            }
        }
    }

    fn channel_close(&self, _ctx: BoundaryContext, channel: ChannelKey) -> Result<()> {
        self.closed.lock().unwrap().insert(channel.0);
        self.notify.notify_waiters();
        Ok(())
    }
}

pub async fn ask_bound_route<F>(
    route: RouteKey,
    request: Bytes,
    timeout: Duration,
    handler: F,
) -> Result<Vec<Event>>
where
    F: Fn(Vec<Event>) -> Vec<Event> + Send + Sync + 'static,
{
    let channel = Arc::new(InMemoryChannelIo::default());
    let server = BoundRouteServer::open(channel.clone(), route.clone())?;
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let handler = Arc::new(handler);
    let server_task = tokio::spawn(async move {
        server
            .bind_concurrent_until(&server_shutdown, 4, move |events| handler(events))
            .await
    });
    let client = BoundRouteClient::open(channel, route, NodeId("source-acceptance-caller".into()))?;
    let result = client.ask(request, timeout).await;
    shutdown.cancel();
    match server_task.await {
        Ok(server_result) => server_result?,
        Err(error) => {
            return Err(CnxError::Internal(format!(
                "source acceptance bound-route server task failed: {error}"
            )));
        }
    }
    result
}
