use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

use tokio::sync::Notify;

use capanix_app_sdk::runtime::NodeId;
use capanix_runtime_entry_sdk::advanced::boundary::ChannelIoSubset;

use super::facade_status::SharedFacadePendingStatusCell;
use crate::query::api::ProjectionPolicy;
use crate::workers::sink::SinkFacade;
use crate::workers::source::SourceFacade;

use super::auth::AuthService;

#[derive(Default)]
pub struct ApiRequestTracker {
    inflight: AtomicUsize,
    changed: Notify,
}

pub struct ApiControlGate {
    ready: AtomicBool,
    epoch: AtomicU64,
    changed: Notify,
    facade_request_tracker: Arc<ApiRequestTracker>,
}

pub struct ApiRequestGuard {
    tracker: Arc<ApiRequestTracker>,
}

impl ApiRequestTracker {
    pub fn begin(self: &Arc<Self>) -> ApiRequestGuard {
        self.inflight.fetch_add(1, Ordering::Relaxed);
        ApiRequestGuard {
            tracker: self.clone(),
        }
    }

    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::Relaxed)
    }

    pub async fn wait_for_drain(&self) {
        loop {
            if self.inflight() == 0 {
                return;
            }
            self.changed.notified().await;
        }
    }
}

impl ApiControlGate {
    pub fn new(ready: bool) -> Self {
        Self {
            ready: AtomicBool::new(ready),
            epoch: AtomicU64::new(0),
            changed: Notify::new(),
            facade_request_tracker: Arc::new(ApiRequestTracker::default()),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn set_ready(&self, ready: bool) {
        if !ready {
            self.epoch.fetch_add(1, Ordering::AcqRel);
        }
        self.ready.store(ready, Ordering::Release);
        self.changed.notify_waiters();
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub async fn wait_ready(&self) {
        loop {
            if self.is_ready() {
                return;
            }
            let notified = self.changed.notified();
            if self.is_ready() {
                return;
            }
            notified.await;
        }
    }

    pub fn begin_facade_request(self: &Arc<Self>) -> ApiRequestGuard {
        self.facade_request_tracker.begin()
    }

    pub async fn wait_for_facade_request_drain(&self) {
        self.facade_request_tracker.wait_for_drain().await;
    }
}

impl Drop for ApiRequestGuard {
    fn drop(&mut self) {
        self.tracker.inflight.fetch_sub(1, Ordering::Relaxed);
        self.tracker.changed.notify_waiters();
    }
}

#[derive(Clone)]
pub struct ApiState {
    pub node_id: NodeId,
    pub runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    pub query_runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    pub force_find_inflight: Arc<Mutex<BTreeSet<String>>>,
    pub source: Arc<SourceFacade>,
    pub sink: Arc<SinkFacade>,
    pub query_sink: Arc<SinkFacade>,
    pub auth: Arc<AuthService>,
    pub projection_policy: Arc<RwLock<ProjectionPolicy>>,
    pub facade_pending: SharedFacadePendingStatusCell,
    pub request_tracker: Arc<ApiRequestTracker>,
    pub control_gate: Arc<ApiControlGate>,
}
