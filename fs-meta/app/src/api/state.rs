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
use super::types::{StatusRepairLaneEvidence, repair_lane_now_us, repair_lane_signature};

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

#[derive(Clone, Copy, Debug)]
pub struct ApiRequestControlReadinessSnapshot {
    pub control_gate_fully_ready: bool,
}

pub struct ApiControlGate {
    ready: AtomicBool,
    management_write_ready: AtomicBool,
    source_repair_ready: AtomicBool,
    management_write_drain_closed: AtomicBool,
    sink_observation_repair_force_republish: AtomicBool,
    sink_observation_repair_republish_groups: Mutex<BTreeSet<String>>,
    epoch: AtomicU64,
    changed: Notify,
    facade_request_tracker: Arc<ApiRequestTracker>,
    status_remote_collection_tracker: Arc<ApiRequestTracker>,
    repair_lane_evidence: Mutex<BTreeMap<String, StatusRepairLaneEvidence>>,
    management_write_recovery: RwLock<Option<ManagementWriteRecovery>>,
    source_repair_recovery: RwLock<Option<ManagementWriteRecovery>>,
    sink_repair_recovery: RwLock<Option<ManagementWriteRecovery>>,
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
            source_repair_ready: AtomicBool::new(ready),
            management_write_drain_closed: AtomicBool::new(false),
            sink_observation_repair_force_republish: AtomicBool::new(false),
            sink_observation_repair_republish_groups: Mutex::new(BTreeSet::new()),
            epoch: AtomicU64::new(0),
            changed: Notify::new(),
            facade_request_tracker: Arc::new(ApiRequestTracker::default()),
            status_remote_collection_tracker: Arc::new(ApiRequestTracker::default()),
            repair_lane_evidence: Mutex::new(BTreeMap::new()),
            management_write_recovery: RwLock::new(None),
            source_repair_recovery: RwLock::new(None),
            sink_repair_recovery: RwLock::new(None),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn is_management_write_ready(&self) -> bool {
        self.management_write_ready.load(Ordering::Acquire)
    }

    pub fn is_source_repair_ready(&self) -> bool {
        self.source_repair_ready.load(Ordering::Acquire)
    }

    pub fn is_management_write_drain_closed(&self) -> bool {
        self.management_write_drain_closed.load(Ordering::Acquire)
    }

    pub fn readiness_snapshot(&self) -> (bool, bool, bool, u64) {
        (
            self.is_ready(),
            self.is_management_write_ready(),
            self.is_source_repair_ready(),
            self.epoch(),
        )
    }

    pub fn set_ready(&self, ready: bool) {
        self.set_ready_state(ready, ready);
    }

    pub fn set_ready_state(&self, ready: bool, management_write_ready: bool) {
        self.set_ready_state_with_source_repair(
            ready,
            management_write_ready,
            management_write_ready,
        );
    }

    pub fn set_ready_state_with_source_repair(
        &self,
        ready: bool,
        management_write_ready: bool,
        source_repair_ready: bool,
    ) {
        if !ready || !management_write_ready || !source_repair_ready {
            self.epoch.fetch_add(1, Ordering::AcqRel);
        }
        if management_write_ready && source_repair_ready {
            self.management_write_drain_closed
                .store(false, Ordering::Release);
        }
        self.ready.store(ready, Ordering::Release);
        self.management_write_ready
            .store(management_write_ready, Ordering::Release);
        self.source_repair_ready
            .store(source_repair_ready, Ordering::Release);
        self.changed.notify_waiters();
    }

    pub fn close_management_write_gate(&self) {
        self.management_write_drain_closed
            .store(true, Ordering::Release);
        self.set_ready_state_with_source_repair(
            self.is_ready(),
            false,
            self.is_source_repair_ready(),
        );
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    #[cfg(test)]
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

    pub async fn wait_source_repair_ready(&self) {
        loop {
            if self.is_source_repair_ready() {
                return;
            }
            let notified = self.changed.notified();
            if self.is_source_repair_ready() {
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

    pub fn set_source_repair_recovery(&self, recovery: Option<ManagementWriteRecovery>) {
        if let Ok(mut guard) = self.source_repair_recovery.write() {
            *guard = recovery;
        }
    }

    pub fn source_repair_recovery(&self) -> Option<ManagementWriteRecovery> {
        self.source_repair_recovery
            .read()
            .ok()
            .and_then(|guard| guard.clone())
    }

    pub fn set_sink_repair_recovery(&self, recovery: Option<ManagementWriteRecovery>) {
        if let Ok(mut guard) = self.sink_repair_recovery.write() {
            *guard = recovery;
        }
    }

    pub fn sink_repair_recovery(&self) -> Option<ManagementWriteRecovery> {
        self.sink_repair_recovery
            .read()
            .ok()
            .and_then(|guard| guard.clone())
    }

    pub fn request_sink_observation_repair_republish(&self) {
        self.sink_observation_repair_force_republish
            .store(true, Ordering::Release);
    }

    pub fn request_sink_observation_repair_republish_for_groups(&self, groups: &BTreeSet<String>) {
        if !groups.is_empty()
            && let Ok(mut guard) = self.sink_observation_repair_republish_groups.lock()
        {
            guard.extend(groups.iter().filter(|group| !group.is_empty()).cloned());
        }
        self.request_sink_observation_repair_republish();
    }

    pub fn take_sink_observation_repair_republish_groups(&self) -> Option<BTreeSet<String>> {
        if !self
            .sink_observation_repair_force_republish
            .swap(false, Ordering::AcqRel)
        {
            return None;
        }
        Some(
            self.sink_observation_repair_republish_groups
                .lock()
                .map(|mut guard| std::mem::take(&mut *guard))
                .unwrap_or_default(),
        )
    }

    #[cfg(test)]
    pub fn take_sink_observation_repair_republish(&self) -> bool {
        self.take_sink_observation_repair_republish_groups()
            .is_some()
    }

    pub fn record_repair_lane_evidence(&self, mut evidence: StatusRepairLaneEvidence) {
        if evidence.signature.is_empty() {
            evidence.signature = repair_lane_signature(
                &evidence.owner,
                &evidence.lane,
                &evidence.trigger,
                evidence.route_outcome.as_deref(),
            );
        }
        if evidence.updated_at_us == 0 {
            evidence.updated_at_us = repair_lane_now_us();
        }
        let key = evidence.signature.clone();
        if let Ok(mut guard) = self.repair_lane_evidence.lock() {
            guard.insert(key, evidence);
        }
    }

    pub fn claim_repair_lane_schedule(
        &self,
        mut evidence: StatusRepairLaneEvidence,
    ) -> (bool, StatusRepairLaneEvidence) {
        if evidence.signature.is_empty() {
            evidence.signature = repair_lane_signature(
                &evidence.owner,
                &evidence.lane,
                &evidence.trigger,
                evidence.route_outcome.as_deref(),
            );
        }
        if evidence.updated_at_us == 0 {
            evidence.updated_at_us = repair_lane_now_us();
        }
        let key = evidence.signature.clone();
        let Ok(mut guard) = self.repair_lane_evidence.lock() else {
            return (true, evidence);
        };
        if let Some(existing) = guard.get(&key) {
            if matches!(existing.state.as_str(), "scheduled" | "inflight") {
                return (false, existing.clone());
            }
        }
        guard.insert(key, evidence.clone());
        (true, evidence)
    }

    pub fn record_repair_lane_state_with_metadata(
        &self,
        lane: &str,
        owner: &str,
        state: &str,
        trigger: &str,
        blocking: bool,
        reason: Option<String>,
        generation: Option<u64>,
        attempt: Option<u32>,
        deadline_at_us: Option<u64>,
    ) {
        self.record_repair_lane_evidence(StatusRepairLaneEvidence {
            lane: lane.to_string(),
            owner: owner.to_string(),
            state: state.to_string(),
            trigger: trigger.to_string(),
            blocking,
            signature: repair_lane_signature(owner, lane, trigger, None),
            updated_at_us: repair_lane_now_us(),
            generation,
            attempt,
            deadline_at_us,
            reason,
            route_outcome: None,
        });
    }

    pub fn repair_lane_evidence_for_signature(
        &self,
        signature: &str,
    ) -> Option<StatusRepairLaneEvidence> {
        self.repair_lane_evidence
            .lock()
            .ok()
            .and_then(|guard| guard.get(signature).cloned())
    }

    pub fn repair_lane_snapshot(&self) -> Vec<StatusRepairLaneEvidence> {
        self.repair_lane_evidence
            .lock()
            .map(|guard| guard.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn begin_facade_request(self: &Arc<Self>) -> ApiRequestGuard {
        self.facade_request_tracker.begin()
    }

    pub async fn wait_for_facade_request_drain(&self) {
        self.facade_request_tracker.wait_for_drain().await;
    }

    pub fn begin_status_remote_collection(self: &Arc<Self>) -> ApiRequestGuard {
        self.status_remote_collection_tracker.begin()
    }

    pub async fn wait_for_status_remote_collection_drain(&self) {
        self.status_remote_collection_tracker.wait_for_drain().await;
    }
}

impl Drop for ApiRequestGuard {
    fn drop(&mut self) {
        self.tracker.inflight.fetch_sub(1, Ordering::Relaxed);
        self.tracker.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::ApiControlGate;

    #[test]
    fn close_management_write_gate_preserves_source_repair_readiness() {
        let gate = ApiControlGate::new(true);

        gate.close_management_write_gate();

        assert!(
            !gate.is_management_write_ready(),
            "closing management-write must close only the management plane"
        );
        assert!(
            gate.is_source_repair_ready(),
            "closing management-write must not force the independent source-repair plane closed"
        );
    }

    #[test]
    fn close_management_write_gate_preserves_closed_source_repair_plane() {
        let gate = ApiControlGate::new(true);
        gate.set_ready_state_with_source_repair(true, true, false);

        gate.close_management_write_gate();

        assert!(!gate.is_management_write_ready());
        assert!(
            !gate.is_source_repair_ready(),
            "an explicitly closed source-repair plane should stay closed"
        );
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
