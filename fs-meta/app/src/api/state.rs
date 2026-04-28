use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

use tokio::sync::Notify;

use capanix_app_sdk::runtime::NodeId;
use capanix_runtime_entry_sdk::advanced::boundary::ChannelIoSubset;

use super::facade_status::PublishedFacadeStatusReader;
use super::rollout_status::PublishedRolloutStatusReader;
use crate::query::api::ProjectionPolicy;
use crate::workers::sink::SinkFacade;
use crate::workers::source::SourceFacade;

use super::auth::AuthService;

pub type ManagementWriteRecoveryFuture =
    Pin<Box<dyn Future<Output = Result<(), capanix_app_sdk::CnxError>> + Send>>;
pub type ManagementWriteRecovery =
    Arc<dyn Fn() -> ManagementWriteRecoveryFuture + Send + Sync + 'static>;

#[derive(Clone, Default)]
pub struct ForceFindRunnerEvidence {
    runners_by_group: Arc<Mutex<BTreeMap<String, BTreeSet<String>>>>,
}

impl ForceFindRunnerEvidence {
    pub fn record(&self, group_id: impl Into<String>, runner: impl Into<String>) {
        if let Ok(mut guard) = self.runners_by_group.lock() {
            guard
                .entry(group_id.into())
                .or_default()
                .insert(runner.into());
        }
    }

    pub fn snapshot(&self) -> BTreeMap<String, Vec<String>> {
        self.runners_by_group
            .lock()
            .map(|guard| {
                guard
                    .iter()
                    .map(|(group, runners)| {
                        (group.clone(), runners.iter().cloned().collect::<Vec<_>>())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Default)]
pub struct ApiRequestTracker {
    inflight: AtomicUsize,
    changed: Notify,
}

pub struct ApiControlGate {
    ready: AtomicBool,
    management_write_ready: AtomicBool,
    epoch: AtomicU64,
    changed: Notify,
    facade_request_tracker: Arc<ApiRequestTracker>,
    management_write_recovery: RwLock<Option<ManagementWriteRecovery>>,
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
            management_write_ready: AtomicBool::new(ready),
            epoch: AtomicU64::new(0),
            changed: Notify::new(),
            facade_request_tracker: Arc::new(ApiRequestTracker::default()),
            management_write_recovery: RwLock::new(None),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn is_management_write_ready(&self) -> bool {
        self.management_write_ready.load(Ordering::Acquire)
    }

    pub fn readiness_snapshot(&self) -> (bool, bool, u64) {
        (
            self.is_ready(),
            self.is_management_write_ready(),
            self.epoch(),
        )
    }

    pub fn set_ready(&self, ready: bool) {
        self.set_ready_state(ready, ready);
    }

    pub fn set_ready_state(&self, ready: bool, management_write_ready: bool) {
        if !ready || !management_write_ready {
            self.epoch.fetch_add(1, Ordering::AcqRel);
        }
        self.ready.store(ready, Ordering::Release);
        self.management_write_ready
            .store(management_write_ready, Ordering::Release);
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

    pub async fn wait_management_write_ready(&self) {
        loop {
            if self.is_management_write_ready() {
                return;
            }
            let notified = self.changed.notified();
            if self.is_management_write_ready() {
                return;
            }
            notified.await;
        }
    }

    pub fn set_management_write_recovery(&self, recovery: Option<ManagementWriteRecovery>) {
        if let Ok(mut guard) = self.management_write_recovery.write() {
            *guard = recovery;
        }
    }

    pub fn management_write_recovery(&self) -> Option<ManagementWriteRecovery> {
        self.management_write_recovery
            .read()
            .ok()
            .and_then(|guard| guard.clone())
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
    pub force_find_runner_evidence: ForceFindRunnerEvidence,
    pub source: Arc<SourceFacade>,
    pub sink: Arc<SinkFacade>,
    pub query_sink: Arc<SinkFacade>,
    pub auth: Arc<AuthService>,
    pub projection_policy: Arc<RwLock<ProjectionPolicy>>,
    pub published_facade_status: PublishedFacadeStatusReader,
    pub published_rollout_status: PublishedRolloutStatusReader,
    pub request_tracker: Arc<ApiRequestTracker>,
    pub control_gate: Arc<ApiControlGate>,
}
