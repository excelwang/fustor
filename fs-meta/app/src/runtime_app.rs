use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use crate::api::facade_status::{
    FacadePendingReason, SharedFacadePendingStatus, SharedFacadePendingStatusCell,
    SharedFacadeServiceStateCell, shared_facade_pending_status_cell,
    shared_facade_service_state_cell,
};
use crate::api::rollout_status::{
    PublishedRolloutStatusSnapshot, SharedRolloutStatusCell, shared_rollout_status_cell,
};
use crate::api::{ApiControlGate, ApiRequestTracker, ManagementWriteRecovery};
use crate::domain_state::{FacadeServiceState, RolloutGenerationState};
use crate::query::TreeGroupPayload;
use crate::query::observation::{
    ObservationEvidence, ObservationTrustPolicy, candidate_group_observation_evidence,
    evaluate_observation_status,
};
use crate::query::reliability::GroupReliability;
use crate::query::tree::ObservationState;
use crate::query::tree::{TreePageRoot, TreeStability};
use crate::query::{InternalQueryRequest, MaterializedQueryPayload, QueryNode, SubtreeStats};
use crate::runtime::endpoint::ManagedEndpointTask;
use crate::runtime::execution_units;
use crate::runtime::orchestration::{
    FacadeControlSignal, FacadeRuntimeUnit, SinkControlSignal, SinkRuntimeUnit,
    SourceControlSignal, SourceRuntimeUnit, decode_logical_roots_control_payload,
    encode_logical_roots_control_payload_with_generation_and_sink_replay,
    manual_rescan_scoped_target_acceptance_timeout_from_payload,
    sink_control_signals_from_envelopes, split_app_control_signals,
};
use crate::runtime::routes::{
    METHOD_QUERY, METHOD_SINK_QUERY, METHOD_SINK_QUERY_PROXY, METHOD_SINK_ROOTS_CONTROL,
    METHOD_SINK_STATUS, METHOD_SOURCE_FIND, METHOD_SOURCE_RESCAN, METHOD_SOURCE_ROOTS_CONTROL,
    METHOD_SOURCE_STATUS, ROUTE_KEY_FACADE_CONTROL, ROUTE_KEY_FORCE_FIND, ROUTE_KEY_QUERY,
    ROUTE_KEY_SINK_QUERY_INTERNAL, ROUTE_KEY_SINK_QUERY_PROXY, ROUTE_KEY_SINK_ROOTS_CONTROL,
    ROUTE_KEY_SINK_STATUS_INTERNAL, ROUTE_KEY_SOURCE_FIND_INTERNAL,
    ROUTE_KEY_SOURCE_RESCAN_CONTROL, ROUTE_KEY_SOURCE_RESCAN_INTERNAL,
    ROUTE_KEY_SOURCE_ROOTS_CONTROL, ROUTE_KEY_SOURCE_STATUS_INTERNAL, ROUTE_TOKEN_FS_META,
    ROUTE_TOKEN_FS_META_INTERNAL, default_route_bindings, events_stream_route_for_scope,
    is_events_stream_route_key, sink_query_request_route_for, sink_query_route_bindings_for,
    sink_roots_control_stream_route_for, sink_status_request_route_for,
    source_find_route_bindings_for, source_rescan_request_route_for,
    source_roots_control_stream_route_for, source_status_request_route_for,
};
use crate::runtime::unit_gate::RuntimeUnitGate;
use crate::workers::sink::{SinkFacade, SinkFailure, SinkWorkerClientHandle};
use crate::workers::source::{
    SourceFacade, SourceFailure, SourcePumpHandle, SourceWorkerClientHandle,
    annotate_manual_rescan_route_receivable_evidence,
    annotate_manual_rescan_route_receivable_evidence_for_current_groups_and_generation,
    annotate_manual_rescan_route_receivable_evidence_for_route_groups,
    source_observability_snapshot_is_degraded_worker_cache,
};
use crate::{FSMetaConfig, api, source};
use async_trait::async_trait;
#[cfg(test)]
use capanix_app_sdk::runtime::ConfigValue;
use capanix_app_sdk::runtime::{
    ControlEnvelope, EventMetadata, NodeId, RecvOpts, RouteKey, RuntimeWorkerBinding,
    RuntimeWorkerBindings, RuntimeWorkerLauncherKind, in_memory_state_boundary,
};
use capanix_app_sdk::worker::WorkerMode;
use capanix_app_sdk::{CnxError, Event, Result, RuntimeBoundary, RuntimeBoundaryApp};
use capanix_managed_state_sdk::{ManagedStateDeclaration, ManagedStateProfile};
use capanix_runtime_entry_sdk::advanced::boundary::{
    BoundaryContext, ChannelBoundary, ChannelIoSubset, ChannelKey, ChannelRecvRequest,
    ChannelSendRequest, StateBoundary, boundary_handles,
};
use capanix_runtime_entry_sdk::control::{
    RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeExecDeactivate,
    RuntimeHostDescriptor, RuntimeHostGrant, RuntimeHostGrantChange, RuntimeHostGrantState,
    RuntimeHostObjectType, RuntimeObjectDescriptor, RuntimeUnitTick, decode_runtime_exec_control,
    decode_runtime_unit_exposure, encode_runtime_exec_control, encode_runtime_host_grant_change,
    encode_runtime_unit_tick,
};
use capanix_runtime_entry_sdk::worker_runtime::RuntimeWorkerClientFactory;
use capanix_runtime_entry_sdk::{RuntimeBootstrapContext, RuntimeLoadedServiceApp};
use capanix_service_sdk::AppBuilder;
use tokio::sync::Mutex;

use crate::sink::SinkFileMeta;
#[cfg(test)]
use crate::sink::{SinkGroupStatusSnapshot, SinkStatusSnapshot};
use crate::source::SourceTargetedRescanDeliveryAcceptance;
#[cfg(test)]
use crate::source::config::SourceConfig;

// Top-level fs-meta runtime-entry/bootstrap glue lowers through
// `service-sdk -> runtime-entry-sdk -> app-sdk`; lower runtime mirror/control
// carriers stay behind the sanctioned helper layer.
const ACTIVE_FACADE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);
const SOURCE_CONTROL_RECOVERY_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_SCOPED_SINK_ROOTS_CONTROL_REPLAY_ATTEMPTS: usize = 3;
const SOURCE_SCOPED_SINK_ROOTS_CONTROL_REPLAY_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const SOURCE_CONTROL_RECOVERY_MAX_RETRYABLE_RESETS: usize = 64;
const DEFERRED_SOURCE_REPAIR_QUIET_WINDOW: Duration = Duration::from_millis(250);
const DEFERRED_SOURCE_REPAIR_RETRY_INTERVAL: Duration = Duration::from_millis(20);
const DEFERRED_SINK_REPAIR_RETRY_INTERVAL: Duration = Duration::from_millis(20);
const INTERNAL_SINK_STATUS_UNINITIALIZED_READY_BUDGET: Duration = Duration::from_millis(1500);
const CONTROL_FRAME_LEASE_ACQUIRE_BUDGET: Duration = Duration::from_secs(1);
const CONTROL_FRAME_LEASE_RETRY_INTERVAL: Duration = Duration::from_millis(10);
const INTERNAL_SINK_STATUS_BLOCKING_FALLBACK_BUDGET: Duration = Duration::from_millis(250);
const HOST_GRANT_CONTROL_FAST_LANE_TIMEOUT: Duration = Duration::from_secs(5);
const MANUAL_RESCAN_SOURCE_STATUS_DEFAULT_PROBE_BUDGET: Duration = Duration::from_secs(15);
const SOURCE_WORKER_RUNTIME_SCOPE_CACHE_REASON: &str =
    "source worker runtime scope served from control cache";
const SOURCE_WORKER_PENDING_SOURCE_STATE_OBSERVATION_REASON: &str =
    "source worker source state pending; delivery proof withheld";

struct FacadeActivation {
    route_key: String,
    generation: u64,
    resource_ids: Vec<String>,
    handle: api::ApiServerHandle,
}

#[derive(Clone)]
struct FacadeSpawnInProgress {
    route_key: String,
    resource_ids: Vec<String>,
}

#[derive(Clone)]
struct ProcessFacadeClaim {
    owner_instance_id: u64,
    bind_addr: String,
}

fn source_signal_generation(signal: &SourceControlSignal) -> u64 {
    match signal {
        SourceControlSignal::Activate { generation, .. }
        | SourceControlSignal::Deactivate { generation, .. }
        | SourceControlSignal::Tick { generation, .. } => *generation,
        SourceControlSignal::RuntimeHostGrantChange { .. }
        | SourceControlSignal::ManualRescan { .. }
        | SourceControlSignal::Passthrough(_) => 0,
    }
}

fn sink_signal_generation(signal: &SinkControlSignal) -> u64 {
    match signal {
        SinkControlSignal::Activate { generation, .. }
        | SinkControlSignal::Deactivate { generation, .. }
        | SinkControlSignal::Tick { generation, .. } => *generation,
        SinkControlSignal::RuntimeHostGrantChange { .. } | SinkControlSignal::Passthrough(_) => 0,
    }
}

fn facade_signal_generation(signal: &FacadeControlSignal) -> u64 {
    match signal {
        FacadeControlSignal::Activate { generation, .. }
        | FacadeControlSignal::Deactivate { generation, .. }
        | FacadeControlSignal::Tick { generation, .. }
        | FacadeControlSignal::ExposureConfirmed { generation, .. } => *generation,
        FacadeControlSignal::RuntimeHostGrantChange { .. } | FacadeControlSignal::Passthrough => 0,
    }
}

fn facade_publication_signal_is_sink_status_activate(signal: &FacadeControlSignal) -> bool {
    matches!(
        signal,
        FacadeControlSignal::Activate {
            unit: FacadeRuntimeUnit::QueryPeer,
            route_key,
            ..
        } if is_sink_status_query_request_route(route_key)
    )
}

fn facade_publication_signal_is_source_status_activate(signal: &FacadeControlSignal) -> bool {
    matches!(
        signal,
        FacadeControlSignal::Activate {
            unit: FacadeRuntimeUnit::QueryPeer,
            route_key,
            ..
        } if is_source_status_request_route(route_key)
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FacadeReadyTailDecision {
    control_gate_ready: bool,
    published_state: FacadeServiceState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FacadePublicationReadinessDecision {
    BlockedControlGate,
    BlockedWithoutActiveControlStream,
    Ready,
    FixedBindHandoffPending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeLifecyclePhase {
    Bootstrapping,
    Live,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RuntimeControlState {
    lifecycle: RuntimeLifecyclePhase,
    source_replay_required: bool,
    sink_replay_required: bool,
    source_control_apply_inflight: bool,
}

impl RuntimeControlState {
    const fn bootstrapping(source_replay_required: bool, sink_replay_required: bool) -> Self {
        Self {
            lifecycle: RuntimeLifecyclePhase::Bootstrapping,
            source_replay_required,
            sink_replay_required,
            source_control_apply_inflight: false,
        }
    }

    #[cfg(test)]
    const fn live(source_replay_required: bool, sink_replay_required: bool) -> Self {
        Self {
            lifecycle: RuntimeLifecyclePhase::Live,
            source_replay_required,
            sink_replay_required,
            source_control_apply_inflight: false,
        }
    }

    fn from_state_cell(state: &Arc<StdMutex<RuntimeControlState>>) -> Self {
        *state.lock().expect("lock runtime gate state")
    }

    const fn control_initialized(self) -> bool {
        matches!(self.lifecycle, RuntimeLifecyclePhase::Live)
    }

    const fn source_state_replay_required(self) -> bool {
        self.source_replay_required
    }

    const fn source_control_apply_inflight(self) -> bool {
        self.source_control_apply_inflight
    }

    const fn sink_state_replay_required(self) -> bool {
        self.sink_replay_required
    }

    const fn source_state_current(self) -> bool {
        !self.source_replay_required && !self.source_control_apply_inflight
    }

    const fn replay_fully_cleared(self) -> bool {
        !self.source_replay_required && !self.sink_replay_required
    }

    const fn control_gate_ready(self, allow_facade_only_handoff: bool) -> bool {
        self.replay_fully_cleared()
            && !self.source_control_apply_inflight
            && (self.control_initialized() || allow_facade_only_handoff)
    }

    const fn source_repair_ready(self) -> bool {
        self.source_state_current()
    }

    const fn initial_mixed_source_to_sink_pretrigger_eligible(
        self,
        control_initialized_at_entry: bool,
        current: Self,
        sink_signals_present: bool,
    ) -> bool {
        !control_initialized_at_entry
            && sink_signals_present
            && self.replay_fully_cleared()
            && current.control_initialized()
            && current.replay_fully_cleared()
    }

    const fn current_facade_service_state(
        self,
        pending_facade_present: bool,
        publication_ready: bool,
        allow_facade_only_handoff: bool,
    ) -> FacadeServiceState {
        if pending_facade_present {
            FacadeServiceState::Pending
        } else if self.control_gate_ready(allow_facade_only_handoff) && publication_ready {
            FacadeServiceState::Serving
        } else {
            FacadeServiceState::Unavailable
        }
    }

    const fn facade_ready_tail_decision(
        self,
        publication_ready: bool,
        pending_facade_present: bool,
        allow_facade_only_handoff: bool,
    ) -> FacadeReadyTailDecision {
        FacadeReadyTailDecision {
            control_gate_ready: publication_ready
                && self.control_gate_ready(allow_facade_only_handoff),
            published_state: self.current_facade_service_state(
                pending_facade_present,
                publication_ready,
                allow_facade_only_handoff,
            ),
        }
    }

    const fn facade_publication_readiness_decision(
        self,
        pending_facade_present: bool,
        pending_facade_is_control_route: bool,
        active_control_stream_present: bool,
        active_pending_control_stream_present: bool,
        allow_facade_only_handoff: bool,
    ) -> FacadePublicationReadinessDecision {
        if !self.control_gate_ready(allow_facade_only_handoff) {
            return FacadePublicationReadinessDecision::BlockedControlGate;
        }
        if !pending_facade_present {
            return if active_control_stream_present {
                FacadePublicationReadinessDecision::Ready
            } else {
                FacadePublicationReadinessDecision::BlockedWithoutActiveControlStream
            };
        }
        if !pending_facade_is_control_route {
            return if active_control_stream_present {
                FacadePublicationReadinessDecision::Ready
            } else {
                FacadePublicationReadinessDecision::BlockedWithoutActiveControlStream
            };
        }
        if active_pending_control_stream_present {
            FacadePublicationReadinessDecision::Ready
        } else {
            FacadePublicationReadinessDecision::FixedBindHandoffPending
        }
    }

    fn require_source_replay(&mut self) {
        self.source_replay_required = true;
    }

    fn require_sink_replay(&mut self) {
        self.sink_replay_required = true;
    }

    fn clear_source_replay(&mut self) {
        self.source_replay_required = false;
    }

    fn clear_sink_replay(&mut self) {
        self.sink_replay_required = false;
    }

    fn begin_source_control_apply(&mut self) {
        self.source_control_apply_inflight = true;
    }

    fn finish_source_control_apply(&mut self) {
        self.source_control_apply_inflight = false;
    }

    #[cfg(test)]
    fn set_control_initialized(&mut self, control_initialized: bool) {
        if control_initialized {
            self.mark_initialized();
        } else {
            self.mark_uninitialized_preserving_replay();
        }
    }

    fn mark_initialized(&mut self) {
        self.lifecycle = RuntimeLifecyclePhase::Live;
    }

    fn mark_uninitialized_with_full_replay(&mut self) {
        self.lifecycle = RuntimeLifecyclePhase::Bootstrapping;
        self.source_replay_required = true;
        self.sink_replay_required = true;
    }

    fn mark_uninitialized_preserving_replay(&mut self) {
        self.lifecycle = RuntimeLifecyclePhase::Bootstrapping;
    }
}

struct SourceControlApplyInflightGuard {
    runtime_gate_state: Arc<StdMutex<RuntimeControlState>>,
    runtime_state_changed: Arc<tokio::sync::Notify>,
}

impl Drop for SourceControlApplyInflightGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.runtime_gate_state.lock() {
            state.finish_source_control_apply();
        }
        self.runtime_state_changed.notify_waiters();
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FacadeServiceStateDecisionInput {
    control_gate_ready: bool,
    publication_ready: bool,
    pending_facade_present: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FacadeOnlyHandoffAllowanceDecisionInput {
    pending_facade_present: bool,
    pending_facade_is_control_route: bool,
    pending_fixed_bind_has_suppressed_dependent_routes: bool,
    pending_bind_is_ephemeral: bool,
    pending_bind_owned_by_instance: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FacadeOnlyHandoffAllowanceDecision {
    Blocked,
    AllowedControlRouteWithoutSuppressedRoutes,
    AllowedOwnedNonEphemeralFixedBind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FacadeOnlyHandoffObservationPolicy {
    ForceBlocked,
    ForceAllowed,
    DeriveFromPendingBind,
}

#[derive(Clone)]
struct FacadeGateObservation {
    runtime: RuntimeControlState,
    current_pending: Option<PendingFacadeActivation>,
    pending_facade_present: bool,
    pending_facade_is_control_route: bool,
    active_control_stream_present: bool,
    active_pending_control_stream_present: bool,
    allow_facade_only_handoff: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FacadePublicationPhase {
    NoFacade,
    Pending,
    Serving,
    Withheld,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FacadePublicationSnapshot {
    phase: FacadePublicationPhase,
    control_gate_ready: bool,
    management_write_ready: bool,
    source_repair_ready: bool,
    publication_ready: bool,
    published_state: FacadeServiceState,
}

#[derive(Clone)]
struct FacadeFixedBindSnapshot {
    pending_publication: Option<PendingFacadeActivation>,
    conflicting_process_claim: Option<ProcessFacadeClaim>,
    claim_release_followup_pending: bool,
}

#[derive(Clone)]
struct FixedBindLifecycleFacts {
    observation: FacadeGateObservation,
    publication_ready: bool,
    fixed_bind: FacadeFixedBindSnapshot,
    release_handoff: Option<PendingFixedBindHandoffContinuation>,
    shutdown_handoff: Option<ActiveFixedBindShutdownContinuation>,
}

#[derive(Clone)]
struct FixedBindLifecycleMachine {
    observation: FacadeGateObservation,
    publication_ready: bool,
    fixed_bind: FacadeFixedBindSnapshot,
    release_handoff: Option<PendingFixedBindHandoffContinuation>,
    shutdown_handoff: Option<ActiveFixedBindShutdownContinuation>,
}

#[derive(Clone)]
struct FixedBindClaimReleaseFollowupSnapshot {
    claim_release_followup_pending: bool,
    pending_publication: Option<PendingFacadeActivation>,
    conflicting_process_claim: Option<ProcessFacadeClaim>,
}

impl FixedBindClaimReleaseFollowupSnapshot {
    fn pending_fixed_bind_facade_publication_unconfirmed(&self) -> bool {
        self.pending_publication.as_ref().is_some_and(|pending| {
            pending.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                && !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
                && !pending.runtime_exposure_confirmed
        })
    }

    fn claim_release_publication_waiting_without_claim(&self) -> bool {
        self.claim_release_followup_pending
            && self.pending_publication.is_some()
            && self.conflicting_process_claim.is_none()
    }

    fn blocks_facade_dependent_routes_without_claim(&self) -> bool {
        self.conflicting_process_claim.is_none()
            && (self.claim_release_publication_waiting_without_claim()
                || self.pending_fixed_bind_facade_publication_unconfirmed())
    }

    fn blocks_facade_dependent_routes(&self) -> bool {
        self.conflicting_process_claim.is_some()
            || self.blocks_facade_dependent_routes_without_claim()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RolloutGenerationMachine {
    runtime: RuntimeControlState,
    active_generation: Option<u64>,
    candidate_generation: Option<u64>,
    candidate_runtime_exposure_confirmed: bool,
    publication_ready: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingFixedBindHandoffAttemptDecisionInput {
    claim_conflict: bool,
    pending_runtime_exposure_confirmed: bool,
    active_owner_present: bool,
    active_owner_failed_control_uninitialized: bool,
    conflicting_process_claim_owner_instance_id: Option<u64>,
}

enum FixedBindLifecycleRequest {
    Deactivate {
        route_key: String,
        generation: u64,
        retain_active_facade: bool,
        retain_pending_spawn: bool,
        restart_deferred_retire_pending: bool,
    },
    Shutdown,
}

enum FixedBindLifecycleExecution {
    RetainActiveContinuity {
        route_key: String,
        generation: u64,
    },
    RetainActiveWhilePendingFixedBindClaimConflict {
        route_key: String,
        generation: u64,
    },
    RetainPendingWhilePendingFixedBindClaimConflict {
        route_key: String,
        generation: u64,
    },
    ReleaseActiveForFixedBindHandoff {
        route_key: String,
        generation: u64,
        handoff: ActiveFixedBindShutdownContinuation,
    },
    RetainPendingForFixedBindHandoff {
        route_key: String,
        generation: u64,
    },
    RetainPendingWhileSpawnInFlight {
        route_key: String,
        generation: u64,
    },
    Shutdown {
        handoff: Option<ActiveFixedBindShutdownContinuation>,
    },
}

#[derive(Clone)]
struct ActiveFixedBindShutdownContinuation {
    bind_addr: String,
    pending_handoff: Option<PendingFixedBindHandoffContinuation>,
    release_mode: FixedBindOwnerReleaseMode,
}

#[derive(Clone)]
struct PendingFixedBindHandoffContinuation {
    bind_addr: String,
    registrant: PendingFixedBindHandoffRegistrant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FixedBindOwnerReleaseMode {
    GracefulHandoff,
    FailedOwnerCutover,
}

impl FixedBindOwnerReleaseMode {
    fn drains_predecessor_facade_reads(self) -> bool {
        matches!(self, Self::GracefulHandoff)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FixedBindAfterReleaseMachine {
    deadline: tokio::time::Instant,
    last_retry_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FixedBindAfterReleaseAction {
    SpawnAttempt,
    WaitForProgress(Duration),
    Complete,
    ReturnTimeout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FixedBindAfterReleaseEvent {
    SpawnAttemptCompleted {
        retry_error: Option<String>,
        completion_ready: bool,
    },
    WaitForProgressCompleted {
        completion_ready: bool,
    },
}

enum FixedBindAfterReleaseExecutorOutcome {
    Event(FixedBindAfterReleaseEvent),
    Complete(bool),
}

const FIXED_BIND_AFTER_RELEASE_TIMEOUT: Duration = Duration::from_secs(8);
const FIXED_BIND_AFTER_RELEASE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

impl FixedBindAfterReleaseMachine {
    fn new_with_deadline(deadline: tokio::time::Instant) -> Self {
        Self {
            deadline,
            last_retry_error: None,
        }
    }

    fn remaining_at(&self, now: tokio::time::Instant) -> Duration {
        self.deadline.saturating_duration_since(now)
    }

    fn start(&self, now: tokio::time::Instant) -> FixedBindAfterReleaseAction {
        if self.remaining_at(now).is_zero() {
            FixedBindAfterReleaseAction::ReturnTimeout
        } else {
            FixedBindAfterReleaseAction::SpawnAttempt
        }
    }

    fn next_action_at(
        &self,
        now: tokio::time::Instant,
        completion_ready: bool,
    ) -> FixedBindAfterReleaseAction {
        if completion_ready {
            FixedBindAfterReleaseAction::Complete
        } else {
            let remaining = self.remaining_at(now);
            if remaining.is_zero() {
                FixedBindAfterReleaseAction::ReturnTimeout
            } else {
                FixedBindAfterReleaseAction::WaitForProgress(std::cmp::min(
                    remaining,
                    FIXED_BIND_AFTER_RELEASE_RETRY_INTERVAL,
                ))
            }
        }
    }

    fn advance(
        &mut self,
        now: tokio::time::Instant,
        event: FixedBindAfterReleaseEvent,
    ) -> FixedBindAfterReleaseAction {
        match event {
            FixedBindAfterReleaseEvent::SpawnAttemptCompleted {
                retry_error,
                completion_ready,
            } => {
                self.last_retry_error = retry_error;
                self.next_action_at(now, completion_ready)
            }
            FixedBindAfterReleaseEvent::WaitForProgressCompleted { completion_ready } => {
                if completion_ready {
                    FixedBindAfterReleaseAction::Complete
                } else if self.remaining_at(now).is_zero() {
                    FixedBindAfterReleaseAction::ReturnTimeout
                } else {
                    FixedBindAfterReleaseAction::SpawnAttempt
                }
            }
        }
    }

    fn last_retry_error_message(&self) -> Option<&str> {
        self.last_retry_error.as_deref()
    }
}

#[test]
fn fixed_bind_after_release_machine_owns_deadline_and_retry_wait() {
    let now = tokio::time::Instant::now();
    let mut live_runtime =
        FixedBindAfterReleaseMachine::new_with_deadline(now + Duration::from_secs(1));
    let expired_runtime = FixedBindAfterReleaseMachine::new_with_deadline(now);

    assert_eq!(
        live_runtime.start(now),
        FixedBindAfterReleaseAction::SpawnAttempt,
        "after-release handoff should keep spawn-attempt eligibility inside the machine instead of leaving execute(...) to compare raw instants inline",
    );
    assert_eq!(
        live_runtime.advance(
            now + Duration::from_millis(200),
            FixedBindAfterReleaseEvent::SpawnAttemptCompleted {
                retry_error: Some("bind busy".to_string()),
                completion_ready: false,
            },
        ),
        FixedBindAfterReleaseAction::WaitForProgress(FIXED_BIND_AFTER_RELEASE_RETRY_INTERVAL),
        "after-release handoff should derive bounded retry wait duration from the machine instead of passively waiting for the whole deadline",
    );
    assert_eq!(
        live_runtime.advance(
            now + Duration::from_millis(400),
            FixedBindAfterReleaseEvent::WaitForProgressCompleted {
                completion_ready: false,
            },
        ),
        FixedBindAfterReleaseAction::SpawnAttempt,
        "after-release handoff must retry successor publication after bounded progress waits instead of passively waiting until the deadline",
    );
    assert_eq!(
        live_runtime.last_retry_error_message(),
        Some("bind busy"),
        "after-release machine should retain the last retry failure message so timeout logging stays owned by the time-machine flow instead of an outer helper-local variable",
    );
    assert_eq!(
        expired_runtime.start(now),
        FixedBindAfterReleaseAction::ReturnTimeout,
        "after-release timeout should come from the machine-owned deadline instead of a helper-local deadline loop",
    );
}

#[test]
fn fixed_bind_after_release_machine_promotes_completion_without_outer_branching() {
    let now = tokio::time::Instant::now();
    let mut runtime = FixedBindAfterReleaseMachine::new_with_deadline(now + Duration::from_secs(1));

    assert_eq!(
        runtime.advance(
            now,
            FixedBindAfterReleaseEvent::SpawnAttemptCompleted {
                retry_error: None,
                completion_ready: true,
            },
        ),
        FixedBindAfterReleaseAction::Complete,
        "after-release handoff should surface completion through a machine-owned action instead of leaving execute(...) to branch on completion-ready inline",
    );
    assert_eq!(
        runtime.advance(
            now,
            FixedBindAfterReleaseEvent::WaitForProgressCompleted {
                completion_ready: false,
            },
        ),
        FixedBindAfterReleaseAction::SpawnAttempt,
        "after-release handoff should keep retry ownership in the machine after incomplete progress waits",
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingFixedBindHandoffAttemptDisposition {
    NoAttempt,
    ReleaseActiveOwner,
    ReleaseConflictingProcessClaim { owner_instance_id: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingFacadeSpawnMode {
    Normal,
    AfterFixedBindOwnerRelease,
}

impl PendingFacadeSpawnMode {
    fn permits_unconfirmed_fixed_bind_boundary(self, pending: &PendingFacadeActivation) -> bool {
        matches!(self, Self::AfterFixedBindOwnerRelease)
            && pending.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
            && !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
    }
}

impl FacadeOnlyHandoffAllowanceDecision {
    fn allows_handoff(self) -> bool {
        !matches!(self, Self::Blocked)
    }
}

impl FacadeGateObservation {
    fn inert() -> Self {
        Self {
            runtime: RuntimeControlState::bootstrapping(false, false),
            current_pending: None,
            pending_facade_present: false,
            pending_facade_is_control_route: false,
            active_control_stream_present: false,
            active_pending_control_stream_present: false,
            allow_facade_only_handoff: false,
        }
    }
}

impl FixedBindLifecycleMachine {
    fn from_facts(facts: FixedBindLifecycleFacts) -> Self {
        let FixedBindLifecycleFacts {
            observation,
            publication_ready,
            fixed_bind,
            release_handoff,
            shutdown_handoff,
        } = facts;
        Self {
            observation,
            publication_ready,
            fixed_bind,
            release_handoff,
            shutdown_handoff,
        }
    }

    fn snapshot(&self) -> FacadePublicationSnapshot {
        let ready_tail = self.observation.runtime.facade_ready_tail_decision(
            self.publication_ready,
            self.observation.pending_facade_present,
            self.observation.allow_facade_only_handoff,
        );
        FacadePublicationSnapshot {
            phase: if self.observation.pending_facade_present {
                FacadePublicationPhase::Pending
            } else if self
                .observation
                .runtime
                .control_gate_ready(self.observation.allow_facade_only_handoff)
                && self.publication_ready
            {
                FacadePublicationPhase::Serving
            } else if self.observation.active_control_stream_present {
                FacadePublicationPhase::Withheld
            } else {
                FacadePublicationPhase::NoFacade
            },
            control_gate_ready: ready_tail.control_gate_ready,
            management_write_ready: self
                .observation
                .runtime
                .control_gate_ready(self.observation.allow_facade_only_handoff)
                && self.observation.active_control_stream_present,
            source_repair_ready: self.observation.runtime.source_repair_ready(),
            publication_ready: self.publication_ready,
            published_state: ready_tail.published_state,
        }
    }

    fn claim_release_followup_snapshot(&self) -> FixedBindClaimReleaseFollowupSnapshot {
        FixedBindClaimReleaseFollowupSnapshot {
            claim_release_followup_pending: self.fixed_bind.claim_release_followup_pending,
            pending_publication: self.fixed_bind.pending_publication.clone(),
            conflicting_process_claim: self.fixed_bind.conflicting_process_claim.clone(),
        }
    }

    fn evaluate(&self, request: FixedBindLifecycleRequest) -> FixedBindLifecycleExecution {
        match request {
            FixedBindLifecycleRequest::Deactivate {
                route_key,
                generation,
                retain_active_facade,
                retain_pending_spawn,
                restart_deferred_retire_pending,
            } => {
                let pending_fixed_bind_conflict =
                    self.fixed_bind.conflicting_process_claim.is_some();
                if retain_active_facade
                    && !restart_deferred_retire_pending
                    && !pending_fixed_bind_conflict
                    && self.release_handoff.is_none()
                {
                    return FixedBindLifecycleExecution::RetainActiveContinuity {
                        route_key,
                        generation,
                    };
                }
                if pending_fixed_bind_conflict {
                    return if retain_active_facade {
                        FixedBindLifecycleExecution::RetainActiveWhilePendingFixedBindClaimConflict {
                            route_key,
                            generation,
                        }
                    } else {
                        FixedBindLifecycleExecution::RetainPendingWhilePendingFixedBindClaimConflict {
                            route_key,
                            generation,
                        }
                    };
                }
                if let Some(handoff) = self.release_handoff.clone() {
                    return if retain_active_facade {
                        FixedBindLifecycleExecution::ReleaseActiveForFixedBindHandoff {
                            route_key,
                            generation,
                            handoff: ActiveFixedBindShutdownContinuation {
                                bind_addr: handoff.bind_addr.clone(),
                                pending_handoff: Some(handoff),
                                release_mode: FixedBindOwnerReleaseMode::GracefulHandoff,
                            },
                        }
                    } else {
                        FixedBindLifecycleExecution::RetainPendingForFixedBindHandoff {
                            route_key,
                            generation,
                        }
                    };
                }
                if retain_pending_spawn {
                    return FixedBindLifecycleExecution::RetainPendingWhileSpawnInFlight {
                        route_key,
                        generation,
                    };
                }
                FixedBindLifecycleExecution::Shutdown {
                    handoff: self.shutdown_handoff.clone(),
                }
            }
            FixedBindLifecycleRequest::Shutdown => FixedBindLifecycleExecution::Shutdown {
                handoff: self.shutdown_handoff.clone(),
            },
        }
    }
}

#[derive(Clone)]
struct FixedBindLifecycleView {
    machine: FixedBindLifecycleMachine,
}

impl FixedBindLifecycleView {
    fn from_facts(facts: FixedBindLifecycleFacts) -> Self {
        Self {
            machine: FixedBindLifecycleMachine::from_facts(facts),
        }
    }

    fn evaluate(&self, request: FixedBindLifecycleRequest) -> FixedBindLifecycleExecution {
        self.machine.evaluate(request)
    }

    fn snapshot(&self) -> FacadePublicationSnapshot {
        self.machine.snapshot()
    }

    fn claim_release_followup_snapshot(&self) -> FixedBindClaimReleaseFollowupSnapshot {
        self.machine.claim_release_followup_snapshot()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FixedBindLifecycleRootReason {
    PublicStart,
    PublicOnControlFrame,
    PublicClose,
    #[cfg(test)]
    TestFixedBindSession,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FixedBindLifecycleRebuildReason {
    PublishFacadeServiceStateAfterSinkRecoveryTailDeferredQueryPeerPublicationSuppression,
    PublishFacadeServiceStateAfterSinkRecoveryTail,
    PublishFacadeServiceStateAfterFixedBindShutdownHandoff,
    PublishFacadeServiceStateAfterControlFailureUninitialized,
    PublishFacadeServiceStateBeforeFixedBindReleaseHandoff,
    PublishFacadeServiceStateBeforeFixedBindShutdown,
    RetryPendingFacadeAfterClaimReleaseFollowup,
    ReplaySuppressedFacadeDependentRoutesAfterPublication,
    PublishFacadeServiceStateAfterFacadeActivateGenerationUpdate,
    PublishFacadeServiceStateAfterPendingFacadeActivate,
    PublishFacadeServiceStateAfterPendingFacadeSpawnAttempt,
    AfterOnControlFrameInitializeFromControl,
    PublishFacadeServiceStateAfterServiceCloseControlUninitialized,
}

#[derive(Clone)]
struct FixedBindLifecycleSession {
    view: FixedBindLifecycleView,
    lineage_token: u64,
    root_reason: FixedBindLifecycleRootReason,
    last_rebuild_reason: Option<FixedBindLifecycleRebuildReason>,
}

impl FixedBindLifecycleSession {
    fn new(view: FixedBindLifecycleView, root_reason: FixedBindLifecycleRootReason) -> Self {
        Self {
            view,
            lineage_token: next_fixed_bind_lifecycle_session_token(),
            root_reason,
            last_rebuild_reason: None,
        }
    }

    fn rebuild(
        &self,
        view: FixedBindLifecycleView,
        reason: FixedBindLifecycleRebuildReason,
    ) -> Self {
        Self {
            view,
            lineage_token: self.lineage_token,
            root_reason: self.root_reason,
            last_rebuild_reason: Some(reason),
        }
    }

    fn evaluate(&self, request: FixedBindLifecycleRequest) -> FixedBindLifecycleExecution {
        self.view.evaluate(request)
    }

    fn lineage_token(&self) -> u64 {
        self.lineage_token
    }

    fn root_reason(&self) -> FixedBindLifecycleRootReason {
        self.root_reason
    }

    fn last_rebuild_reason(&self) -> Option<FixedBindLifecycleRebuildReason> {
        self.last_rebuild_reason
    }

    fn publication_snapshot(&self) -> FacadePublicationSnapshot {
        self.view.snapshot()
    }

    fn claim_release_followup_snapshot(&self) -> FixedBindClaimReleaseFollowupSnapshot {
        self.view.claim_release_followup_snapshot()
    }
}

fn next_fixed_bind_lifecycle_session_token() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

async fn fixed_bind_completion_ready_after_release(
    handoff: &PendingFixedBindHandoffContinuation,
) -> bool {
    let pending_facade_present = handoff.registrant.pending_facade.lock().await.is_some();
    let active_control_stream_present = handoff
        .registrant
        .api_task
        .lock()
        .await
        .as_ref()
        .is_some_and(|active| active.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL));
    !pending_facade_present && active_control_stream_present
}

async fn wait_for_fixed_bind_after_release_completion_progress(
    handoff: &PendingFixedBindHandoffContinuation,
    remaining: Duration,
    last_retry_error: Option<&str>,
) -> bool {
    if remaining.is_zero() {
        if let Some(err) = last_retry_error {
            eprintln!(
                "fs_meta_runtime_app: fixed-bind handoff completion retry failed bind_addr={} err={}",
                handoff.bind_addr, err
            );
        }
        return false;
    }
    let _ = tokio::time::timeout(
        remaining,
        handoff.registrant.runtime_state_changed.notified(),
    )
    .await;
    true
}

async fn execute_fixed_bind_after_release_action(
    handoff: &PendingFixedBindHandoffContinuation,
    machine: &FixedBindAfterReleaseMachine,
    action: FixedBindAfterReleaseAction,
) -> Result<FixedBindAfterReleaseExecutorOutcome> {
    match action {
        FixedBindAfterReleaseAction::SpawnAttempt => {
            let retry_error = handoff
                .registrant
                .try_spawn_pending_facade_after_owner_release()
                .await
                .err()
                .map(|err| err.to_string());
            Ok(FixedBindAfterReleaseExecutorOutcome::Event(
                FixedBindAfterReleaseEvent::SpawnAttemptCompleted {
                    retry_error,
                    completion_ready: fixed_bind_completion_ready_after_release(handoff).await,
                },
            ))
        }
        FixedBindAfterReleaseAction::WaitForProgress(remaining) => {
            let progressed = wait_for_fixed_bind_after_release_completion_progress(
                handoff,
                remaining,
                machine.last_retry_error_message(),
            )
            .await;
            Ok(FixedBindAfterReleaseExecutorOutcome::Event(
                FixedBindAfterReleaseEvent::WaitForProgressCompleted {
                    completion_ready: progressed
                        && fixed_bind_completion_ready_after_release(handoff).await,
                },
            ))
        }
        FixedBindAfterReleaseAction::Complete => {
            #[cfg(test)]
            maybe_pause_before_pending_fixed_bind_handoff_completion_gate_reopen().await;
            let _ = handoff
                .registrant
                .apply_forced_handoff_ready_tail(&handoff.bind_addr)
                .await;
            #[cfg(test)]
            notify_pending_fixed_bind_handoff_completion_completion();
            Ok(FixedBindAfterReleaseExecutorOutcome::Complete(true))
        }
        FixedBindAfterReleaseAction::ReturnTimeout => {
            Ok(FixedBindAfterReleaseExecutorOutcome::Complete(false))
        }
    }
}

async fn drive_fixed_bind_after_release_loop(
    handoff: &PendingFixedBindHandoffContinuation,
    machine: &mut FixedBindAfterReleaseMachine,
) -> Result<bool> {
    let mut action = machine.start(tokio::time::Instant::now());
    loop {
        match execute_fixed_bind_after_release_action(handoff, machine, action).await? {
            FixedBindAfterReleaseExecutorOutcome::Event(event) => {
                action = machine.advance(tokio::time::Instant::now(), event);
            }
            FixedBindAfterReleaseExecutorOutcome::Complete(completed) => {
                return Ok(completed);
            }
        }
    }
}

async fn execute_fixed_bind_after_release_handoff(
    handoff: PendingFixedBindHandoffContinuation,
) -> Result<bool> {
    let mut machine = FixedBindAfterReleaseMachine::new_with_deadline(
        tokio::time::Instant::now() + FIXED_BIND_AFTER_RELEASE_TIMEOUT,
    );
    drive_fixed_bind_after_release_loop(&handoff, &mut machine).await
}

impl RolloutGenerationMachine {
    fn snapshot(self) -> PublishedRolloutStatusSnapshot {
        let retiring_generation = match (self.active_generation, self.candidate_generation) {
            (Some(active), Some(candidate)) if active != candidate => Some(active),
            _ => None,
        };
        let state = match (self.active_generation, self.candidate_generation) {
            (_, Some(_))
                if !self.runtime.control_initialized()
                    || self.runtime.source_state_replay_required()
                    || self.runtime.sink_state_replay_required() =>
            {
                RolloutGenerationState::CatchUp
            }
            (_, Some(_)) if !self.candidate_runtime_exposure_confirmed => {
                RolloutGenerationState::Eligible
            }
            (Some(active), Some(candidate)) if active != candidate && self.publication_ready => {
                RolloutGenerationState::Drain
            }
            (Some(active), Some(candidate)) if active != candidate => {
                RolloutGenerationState::Cutover
            }
            (None, Some(_)) if self.publication_ready => RolloutGenerationState::Cutover,
            (None, Some(_)) => RolloutGenerationState::Eligible,
            (Some(_), None)
                if self.runtime.control_initialized()
                    && !self.runtime.source_state_replay_required()
                    && !self.runtime.sink_state_replay_required()
                    && self.publication_ready =>
            {
                RolloutGenerationState::Stable
            }
            _ => RolloutGenerationState::CatchUp,
        };
        PublishedRolloutStatusSnapshot {
            state,
            serving_generation: self.active_generation,
            candidate_generation: self.candidate_generation,
            retiring_generation,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UninitializedCleanupDisposition {
    None,
    FacadeOnly,
    SourceOnly,
    SinkOnly { query_only: bool },
}

impl UninitializedCleanupDisposition {
    fn is_facade_only(self) -> bool {
        matches!(self, Self::FacadeOnly)
    }

    fn is_source_only(self) -> bool {
        matches!(self, Self::SourceOnly)
    }

    fn is_sink_only(self) -> bool {
        matches!(self, Self::SinkOnly { .. })
    }

    fn is_sink_query_only(self) -> bool {
        matches!(self, Self::SinkOnly { query_only: true })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceControlWaveDisposition {
    Idle,
    CleanupOnlyWhileUninitialized,
    EmptyRootRouteLivenessOnly,
    RetainedTickGateOnlyWhileSinkReplayPending,
    SteadyTickNoop,
    ApplySignals,
    ReplayRetained,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkControlWaveDisposition {
    Idle,
    CleanupOnlyWhileUninitialized,
    CleanupOnlyQueryWhileUninitialized,
    RetainedTickGateOnlyWhileSourceReplayPending,
    RetainedTickGateOnlyWhileSinkReplayPending,
    SteadyTickNoop,
    ApplyBeforeInitialSourceWave,
    ApplySignals {
        wait_for_status_republish_after_apply: bool,
    },
    ReplayRetained,
}

#[derive(Clone, Copy, Debug)]
struct ControlFrameWaveObservation {
    control_initialized_at_entry: bool,
    control_initialized_now: bool,
    retained_sink_state_present_at_entry: bool,
    source_state_replay_required: bool,
    sink_state_replay_required: bool,
    sink_tick_fast_path_eligible: bool,
    cleanup_disposition: UninitializedCleanupDisposition,
    facade_claim_signals_present: bool,
    facade_publication_signals_present: bool,
    source_replay_tick_only_while_sink_replay_pending: bool,
    source_tick_gate_only_while_sink_replay_pending: bool,
    sink_tick_gate_only_while_source_replay_pending: bool,
    sink_tick_gate_only_while_sink_replay_pending: bool,
    source_signals_present: bool,
    sink_signals_present: bool,
}

#[derive(Clone, Copy, Debug)]
struct UninitializedCleanupDecisionInput<'a> {
    control_initialized_now: bool,
    source_signals: &'a [SourceControlSignal],
    sink_signals: &'a [SinkControlSignal],
    facade_signals: &'a [FacadeControlSignal],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceGenerationCutoverDisposition {
    None,
    FailClosedRestartDeferredRetirePending,
    FailClosedRetainedReplayOnRetryableReset,
    FailClosedColdSuccessorCandidateOnRetryableReset,
}

#[derive(Clone, Copy, Debug)]
struct SourceGenerationCutoverDecisionInput<'a> {
    control_initialized_at_entry: bool,
    source_state_replay_required_at_entry: bool,
    retained_source_route_state_present_at_entry: bool,
    fixed_bind_publication_continuation_active: bool,
    source_signals: &'a [SourceControlSignal],
    sink_signals: &'a [SinkControlSignal],
    facade_signals: &'a [FacadeControlSignal],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkGenerationCutoverDisposition {
    None,
    FailClosedSharedGenerationCutover,
    FailClosedRetainedReplayOnRetryableReset,
    FailClosedColdSuccessorCandidateOnRetryableReset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlFailureRecoveryLanePolicy {
    WithdrawInternalStatus,
    PreserveSourceStatus,
    PreserveInternalStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlFailureReplayRequirement {
    None,
    Full,
    Source,
    SourceAndSink,
    Sink,
}

#[derive(Clone, Copy, Debug)]
struct SinkGenerationCutoverDecisionInput<'a> {
    control_initialized_at_entry: bool,
    retained_replay_pending_at_entry: bool,
    retained_sink_route_state_present_at_entry: bool,
    fixed_bind_publication_continuation_active: bool,
    source_signals: &'a [SourceControlSignal],
    sink_signals: &'a [SinkControlSignal],
    facade_signals: &'a [FacadeControlSignal],
    sink_signals_in_shared_generation_cutover_lane: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkGenerationCutoverInlineAction {
    DeferReplay,
    ApplyAsInitialAuditCatchup,
    Apply,
}

fn sink_generation_cutover_inline_action_from_evidence(
    generation_cutover_disposition: SinkGenerationCutoverDisposition,
    has_post_initial_single_route_activate: bool,
    source_state_replay_required: bool,
    sink_generation_cutover_replay_deferred: bool,
    initial_materialization_catchup: bool,
) -> SinkGenerationCutoverInlineAction {
    let can_defer_replay = matches!(
        generation_cutover_disposition,
        SinkGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
    ) && has_post_initial_single_route_activate;
    if !can_defer_replay {
        return SinkGenerationCutoverInlineAction::Apply;
    }
    if !source_state_replay_required && initial_materialization_catchup {
        return SinkGenerationCutoverInlineAction::ApplyAsInitialAuditCatchup;
    }
    if !sink_generation_cutover_replay_deferred && !source_state_replay_required {
        SinkGenerationCutoverInlineAction::DeferReplay
    } else {
        SinkGenerationCutoverInlineAction::Apply
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SinkRecoveryGateReopenDisposition {
    None,
    WaitForLocalSinkStatusRepublish {
        expected_groups: std::collections::BTreeSet<String>,
    },
    DeferGateReopenUntilSinkStatusReady {
        expected_groups: std::collections::BTreeSet<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SinkRecoveryGateReopenDecisionInput {
    source_led_uninitialized_mixed_recovery: bool,
    public_facade_or_query_publication_present: bool,
    deferred_local_sink_replay_present: bool,
    cached_sink_status_ready_for_expected_groups: bool,
    source_led_expected_groups: std::collections::BTreeSet<String>,
    recovered_expected_groups: Option<std::collections::BTreeSet<String>>,
}

impl SinkRecoveryGateReopenDisposition {
    fn from_decision_input(input: SinkRecoveryGateReopenDecisionInput) -> Self {
        let SinkRecoveryGateReopenDecisionInput {
            source_led_uninitialized_mixed_recovery,
            public_facade_or_query_publication_present,
            deferred_local_sink_replay_present,
            cached_sink_status_ready_for_expected_groups,
            source_led_expected_groups,
            recovered_expected_groups,
        } = input;

        if source_led_uninitialized_mixed_recovery && !source_led_expected_groups.is_empty() {
            if public_facade_or_query_publication_present {
                return Self::None;
            }
            if cached_sink_status_ready_for_expected_groups {
                if deferred_local_sink_replay_present {
                    return Self::WaitForLocalSinkStatusRepublish {
                        expected_groups: source_led_expected_groups,
                    };
                }
                return Self::None;
            }
            return Self::DeferGateReopenUntilSinkStatusReady {
                expected_groups: source_led_expected_groups,
            };
        }

        recovered_expected_groups.map_or(Self::None, |expected_groups| {
            Self::WaitForLocalSinkStatusRepublish { expected_groups }
        })
    }

    fn defers_gate_reopen(&self) -> bool {
        matches!(self, Self::DeferGateReopenUntilSinkStatusReady { .. })
    }

    fn holds_sink_owned_query_peer_publication(&self) -> bool {
        matches!(
            self,
            Self::WaitForLocalSinkStatusRepublish { .. }
                | Self::DeferGateReopenUntilSinkStatusReady { .. }
        )
    }

    fn local_wait_expected_groups(&self) -> Option<&std::collections::BTreeSet<String>> {
        match self {
            Self::WaitForLocalSinkStatusRepublish { expected_groups } => Some(expected_groups),
            Self::None | Self::DeferGateReopenUntilSinkStatusReady { .. } => None,
        }
    }

    fn deferred_expected_groups(&self) -> Option<&std::collections::BTreeSet<String>> {
        match self {
            Self::DeferGateReopenUntilSinkStatusReady { expected_groups } => Some(expected_groups),
            Self::None | Self::WaitForLocalSinkStatusRepublish { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SinkRecoveryTailPlan {
    immediate_local_sink_status_republish_waits: Vec<std::collections::BTreeSet<String>>,
    gate_reopen_disposition: SinkRecoveryGateReopenDisposition,
}

impl SinkRecoveryTailPlan {
    fn new() -> Self {
        Self {
            immediate_local_sink_status_republish_waits: Vec::new(),
            gate_reopen_disposition: SinkRecoveryGateReopenDisposition::None,
        }
    }

    fn push_immediate_local_sink_status_republish_wait(
        &mut self,
        expected_groups: std::collections::BTreeSet<String>,
    ) {
        if expected_groups.is_empty() {
            return;
        }
        self.immediate_local_sink_status_republish_waits
            .push(expected_groups);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalSinkStatusRepublishWaitMode {
    AllowCachedReadyFastPath,
    RequireProbeBeforeReady,
}

#[derive(Debug)]
struct RuntimeScopeConvergenceObservation {
    source_groups: std::collections::BTreeSet<String>,
    scan_groups: std::collections::BTreeSet<String>,
    sink_groups: std::collections::BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeScopeExpectedGroups {
    source_groups: std::collections::BTreeSet<String>,
    scan_groups: std::collections::BTreeSet<String>,
    sink_groups: std::collections::BTreeSet<String>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum RuntimeScopeConvergenceExpectation<'a> {
    ExpectedGroups(&'a RuntimeScopeExpectedGroups),
}

impl RuntimeScopeConvergenceObservation {
    #[cfg(test)]
    fn matches_expected(&self, expected_groups: &RuntimeScopeExpectedGroups) -> bool {
        self.source_groups == expected_groups.source_groups
            && self.scan_groups == expected_groups.scan_groups
            && self.sink_groups == expected_groups.sink_groups
    }

    fn matches_node_local_source_scope(
        &self,
        expected_groups: &RuntimeScopeExpectedGroups,
    ) -> bool {
        self.sink_groups == expected_groups.sink_groups
            && self.source_groups.is_subset(&expected_groups.source_groups)
            && self.scan_groups.is_subset(&expected_groups.scan_groups)
            && self.source_groups.is_subset(&self.sink_groups)
            && self.scan_groups.is_subset(&self.sink_groups)
    }

    fn timeout_context(&self) -> String {
        format!(
            "source={:?} scan={:?} sink={:?}",
            self.source_groups, self.scan_groups, self.sink_groups
        )
    }

    fn expected_groups_for_logical_roots(
        &self,
        logical_roots: &[source::config::RootSpec],
    ) -> RuntimeScopeExpectedGroups {
        let mut sink_groups = self.sink_groups.clone();
        if sink_groups.is_empty() {
            sink_groups.extend(self.source_groups.iter().cloned());
            sink_groups.extend(self.scan_groups.iter().cloned());
        }
        RuntimeScopeExpectedGroups::from_logical_roots(logical_roots, &sink_groups)
    }

    #[cfg(test)]
    fn matches_expectation(
        self: &Self,
        expectation: RuntimeScopeConvergenceExpectation<'_>,
    ) -> bool {
        match expectation {
            RuntimeScopeConvergenceExpectation::ExpectedGroups(expected_groups) => {
                self.matches_expected(expected_groups)
            }
        }
    }
}

impl RuntimeScopeExpectedGroups {
    fn from_logical_roots(
        logical_roots: &[source::config::RootSpec],
        sink_groups: &std::collections::BTreeSet<String>,
    ) -> Self {
        let roots_by_id = logical_roots
            .iter()
            .map(|root| (root.id.as_str(), root))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut source_groups = std::collections::BTreeSet::new();
        let mut scan_groups = std::collections::BTreeSet::new();
        for group_id in sink_groups {
            match roots_by_id.get(group_id.as_str()) {
                Some(root) => {
                    if root.watch || root.scan {
                        source_groups.insert(group_id.clone());
                    }
                    if root.scan {
                        scan_groups.insert(group_id.clone());
                    }
                }
                None => {
                    // Fail closed for unknown roots by requiring the legacy
                    // source/scan convergence semantics until runtime control
                    // can describe the lane coverage explicitly.
                    source_groups.insert(group_id.clone());
                    scan_groups.insert(group_id.clone());
                }
            }
        }
        Self {
            source_groups,
            scan_groups,
            sink_groups: sink_groups.clone(),
        }
    }
}

#[derive(Debug)]
struct SinkRecoveryMachine {
    state: SinkRecoveryState,
    #[cfg(test)]
    first_sink_probe_pending: bool,
}

fn debug_sink_recovery_capture_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
}

#[derive(Debug)]
enum SinkRecoveryState {
    PostRecovery(PostRecoverySinkRecoveryState),
    LocalRepublish(LocalSinkRepublishState),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PostRecoverySinkRecoveryState {
    scope_trigger_state: SinkRecoveryScopeTriggerState,
    sink_timeout_retry_state: SinkRecoveryTimeoutRetryState,
    source_rescan_request_epoch: Option<u64>,
}

#[derive(Debug)]
struct LocalSinkRepublishState {
    deadline: tokio::time::Instant,
    allow_cached_ready_fast_path: bool,
    source_rescan_request_epoch: Option<u64>,
    source_publication_epoch_floor: Option<u64>,
    retrigger_state: SinkRecoveryRetriggerState,
    manual_rescan_state: SinkRecoveryManualRescanState,
    retained_sink_replay_state: SinkRecoveryRetainedSinkReplayState,
    #[cfg(test)]
    retrigger_probe_pause_pending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkRecoveryScopeTriggerState {
    InitialNotTriggered,
    Triggered,
    RetryPending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkRecoveryTimeoutRetryState {
    NotRetried,
    RetriedAfterManualRescan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkRecoveryScopeConvergenceAction {
    Converged,
    TriggerInitialAndWait,
    Wait,
    ReturnTimeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkRecoveryRetriggerState {
    Pending { count: usize },
    Idle { count: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkRecoveryManualRescanState {
    NotPublished,
    Published,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkRecoveryRetainedSinkReplayState {
    None,
    Pending,
    Replayed,
    RequireReadyProbe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkRecoveryStep {
    WaitForRuntimeScopeConvergence,
    ReturnReady,
    TriggerSourceToSinkConvergence,
    ReplayRetainedSinkWave,
    ProbeReadiness,
    PublishManualRescanFallbackAndWait,
    ReturnTimeout,
}

impl ControlFrameWaveObservation {
    fn classify_source_control_wave_disposition(
        self,
        source_signals: &[SourceControlSignal],
    ) -> SourceControlWaveDisposition {
        if !source_signals.is_empty() {
            if self.cleanup_disposition.is_source_only() {
                SourceControlWaveDisposition::CleanupOnlyWhileUninitialized
            } else if self.source_tick_gate_only_while_sink_replay_pending {
                SourceControlWaveDisposition::RetainedTickGateOnlyWhileSinkReplayPending
            } else if self.control_initialized_at_entry
                && self.control_initialized_now
                && !self.source_state_replay_required
                && ((!self.facade_claim_signals_present
                    && !self.facade_publication_signals_present)
                    || self.sink_signals_present)
                && source_signals
                    .iter()
                    .all(|signal| matches!(signal, SourceControlSignal::Tick { .. }))
            {
                SourceControlWaveDisposition::SteadyTickNoop
            } else {
                SourceControlWaveDisposition::ApplySignals
            }
        } else if self.source_state_replay_required
            && !self.cleanup_disposition.is_source_only()
            && !self.cleanup_disposition.is_sink_only()
        {
            SourceControlWaveDisposition::ReplayRetained
        } else {
            SourceControlWaveDisposition::Idle
        }
    }

    fn classify_sink_control_wave_disposition(
        self,
        source_wave_disposition: SourceControlWaveDisposition,
        sink_signals: &[SinkControlSignal],
    ) -> SinkControlWaveDisposition {
        if !sink_signals.is_empty() {
            let steady_tick_noop = self.control_initialized_at_entry
                && self.control_initialized_now
                && !self.sink_state_replay_required
                && self.sink_tick_fast_path_eligible
                && sink_signals
                    .iter()
                    .all(|signal| matches!(signal, SinkControlSignal::Tick { .. }));
            let apply_before_initial_source_wave = !self.control_initialized_at_entry
                && !self.retained_sink_state_present_at_entry
                && self.source_signals_present
                && !self.cleanup_disposition.is_source_only()
                && !self.cleanup_disposition.is_sink_query_only()
                && matches!(
                    source_wave_disposition,
                    SourceControlWaveDisposition::ApplySignals
                )
                && !steady_tick_noop;
            if apply_before_initial_source_wave {
                SinkControlWaveDisposition::ApplyBeforeInitialSourceWave
            } else if self.sink_tick_gate_only_while_source_replay_pending {
                SinkControlWaveDisposition::RetainedTickGateOnlyWhileSourceReplayPending
            } else if self.sink_tick_gate_only_while_sink_replay_pending {
                SinkControlWaveDisposition::RetainedTickGateOnlyWhileSinkReplayPending
            } else if self.cleanup_disposition.is_sink_only() {
                if self.cleanup_disposition.is_sink_query_only() {
                    SinkControlWaveDisposition::CleanupOnlyQueryWhileUninitialized
                } else {
                    SinkControlWaveDisposition::CleanupOnlyWhileUninitialized
                }
            } else if self.cleanup_disposition.is_sink_query_only() {
                SinkControlWaveDisposition::CleanupOnlyQueryWhileUninitialized
            } else if steady_tick_noop {
                SinkControlWaveDisposition::SteadyTickNoop
            } else {
                SinkControlWaveDisposition::ApplySignals {
                    wait_for_status_republish_after_apply: self.control_initialized_at_entry
                        && self.retained_sink_state_present_at_entry
                        && !self.sink_tick_fast_path_eligible
                        && !self.source_signals_present
                        && !self.facade_claim_signals_present
                        && !self.facade_publication_signals_present
                        && sink_signals
                            .iter()
                            .all(|signal| matches!(signal, SinkControlSignal::Tick { .. })),
                }
            }
        } else if self.sink_state_replay_required
            && !self.cleanup_disposition.is_source_only()
            && !self.cleanup_disposition.is_sink_query_only()
        {
            if matches!(
                source_wave_disposition,
                SourceControlWaveDisposition::RetainedTickGateOnlyWhileSinkReplayPending
            ) || self.source_replay_tick_only_while_sink_replay_pending
            {
                return SinkControlWaveDisposition::Idle;
            }
            SinkControlWaveDisposition::ReplayRetained
        } else {
            SinkControlWaveDisposition::Idle
        }
    }
}

impl SinkRecoveryRetriggerState {
    fn is_pending(self) -> bool {
        matches!(self, Self::Pending { .. })
    }

    fn count(self) -> usize {
        match self {
            Self::Pending { count } | Self::Idle { count } => count,
        }
    }

    fn mark_triggered(&mut self) {
        *self = Self::Idle {
            count: self.count() + 1,
        };
    }
}

impl SinkRecoveryRetainedSinkReplayState {
    fn is_pending(self) -> bool {
        matches!(self, Self::Pending)
    }
}

impl SinkRecoveryMachine {
    fn new_post_recovery(source_to_sink_convergence_pretriggered_epoch: Option<u64>) -> Self {
        Self {
            state: SinkRecoveryState::PostRecovery(PostRecoverySinkRecoveryState {
                scope_trigger_state: if source_to_sink_convergence_pretriggered_epoch.is_some() {
                    SinkRecoveryScopeTriggerState::Triggered
                } else {
                    SinkRecoveryScopeTriggerState::InitialNotTriggered
                },
                sink_timeout_retry_state: SinkRecoveryTimeoutRetryState::NotRetried,
                source_rescan_request_epoch: source_to_sink_convergence_pretriggered_epoch,
            }),
            #[cfg(test)]
            first_sink_probe_pending: true,
        }
    }

    fn new_local(mode: LocalSinkStatusRepublishWaitMode) -> Self {
        Self::new_local_with_deadline(mode, tokio::time::Instant::now() + Duration::from_secs(5))
    }

    fn new_local_pretriggered(mode: LocalSinkStatusRepublishWaitMode, request_epoch: u64) -> Self {
        Self::new_local_pretriggered_with_deadline(
            mode,
            tokio::time::Instant::now() + Duration::from_secs(5),
            request_epoch,
        )
    }

    fn new_local_with_deadline(
        mode: LocalSinkStatusRepublishWaitMode,
        deadline: tokio::time::Instant,
    ) -> Self {
        Self {
            state: SinkRecoveryState::LocalRepublish(LocalSinkRepublishState {
                deadline,
                allow_cached_ready_fast_path: matches!(
                    mode,
                    LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath
                ),
                source_rescan_request_epoch: None,
                source_publication_epoch_floor: None,
                retrigger_state: match mode {
                    LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath => {
                        SinkRecoveryRetriggerState::Pending { count: 0 }
                    }
                    LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady => {
                        SinkRecoveryRetriggerState::Pending { count: 0 }
                    }
                },
                manual_rescan_state: SinkRecoveryManualRescanState::NotPublished,
                retained_sink_replay_state: match mode {
                    LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath => {
                        SinkRecoveryRetainedSinkReplayState::None
                    }
                    LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady => {
                        SinkRecoveryRetainedSinkReplayState::RequireReadyProbe
                    }
                },
                #[cfg(test)]
                retrigger_probe_pause_pending: false,
            }),
            #[cfg(test)]
            first_sink_probe_pending: true,
        }
    }

    fn new_local_pretriggered_with_deadline(
        mode: LocalSinkStatusRepublishWaitMode,
        deadline: tokio::time::Instant,
        request_epoch: u64,
    ) -> Self {
        Self {
            state: SinkRecoveryState::LocalRepublish(LocalSinkRepublishState {
                deadline,
                allow_cached_ready_fast_path: matches!(
                    mode,
                    LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath
                ),
                source_rescan_request_epoch: Some(request_epoch),
                source_publication_epoch_floor: Some(request_epoch),
                retrigger_state: SinkRecoveryRetriggerState::Pending { count: 1 },
                manual_rescan_state: SinkRecoveryManualRescanState::NotPublished,
                retained_sink_replay_state: match mode {
                    LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath => {
                        SinkRecoveryRetainedSinkReplayState::None
                    }
                    LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady => {
                        SinkRecoveryRetainedSinkReplayState::RequireReadyProbe
                    }
                },
                #[cfg(test)]
                retrigger_probe_pause_pending: false,
            }),
            #[cfg(test)]
            first_sink_probe_pending: true,
        }
    }

    fn post_recovery_state(&self) -> &PostRecoverySinkRecoveryState {
        match &self.state {
            SinkRecoveryState::PostRecovery(state) => state,
            SinkRecoveryState::LocalRepublish(_) => {
                panic!("sink recovery machine accessed post-recovery state in local mode")
            }
        }
    }

    fn post_recovery_state_mut(&mut self) -> &mut PostRecoverySinkRecoveryState {
        match &mut self.state {
            SinkRecoveryState::PostRecovery(state) => state,
            SinkRecoveryState::LocalRepublish(_) => {
                panic!("sink recovery machine accessed post-recovery state in local mode")
            }
        }
    }

    fn local_state(&self) -> &LocalSinkRepublishState {
        match &self.state {
            SinkRecoveryState::LocalRepublish(state) => state,
            SinkRecoveryState::PostRecovery(_) => {
                panic!("sink recovery machine accessed local state in post-recovery mode")
            }
        }
    }

    fn local_state_mut(&mut self) -> &mut LocalSinkRepublishState {
        match &mut self.state {
            SinkRecoveryState::LocalRepublish(state) => state,
            SinkRecoveryState::PostRecovery(_) => {
                panic!("sink recovery machine accessed local state in post-recovery mode")
            }
        }
    }

    fn remaining(&self) -> Duration {
        self.local_state()
            .deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_default()
    }

    fn deadline(&self) -> tokio::time::Instant {
        self.local_state().deadline
    }

    fn local_requires_probe_before_ready(&self) -> bool {
        matches!(
            self.local_state().retained_sink_replay_state,
            SinkRecoveryRetainedSinkReplayState::RequireReadyProbe
        )
    }

    #[cfg(test)]
    async fn maybe_pause_at_retrigger_probe_boundary(&mut self) {
        if self.local_state().retrigger_probe_pause_pending {
            maybe_pause_after_local_sink_status_republish_retrigger().await;
            self.local_state_mut().retrigger_probe_pause_pending = false;
        }
    }

    fn post_recovery_source_rescan_pending(&self) -> bool {
        self.post_recovery_state()
            .source_rescan_request_epoch
            .is_some()
    }

    fn post_recovery_scope_action(
        &self,
        observation: &RuntimeScopeConvergenceObservation,
        expected_scope_groups: &RuntimeScopeExpectedGroups,
        deadline: tokio::time::Instant,
    ) -> SinkRecoveryScopeConvergenceAction {
        let scope_converged = observation.matches_node_local_source_scope(expected_scope_groups);
        let source_rescan_pending = self.post_recovery_source_rescan_pending();
        if scope_converged && !source_rescan_pending {
            SinkRecoveryScopeConvergenceAction::Converged
        } else if scope_converged && tokio::time::Instant::now() >= deadline {
            SinkRecoveryScopeConvergenceAction::Converged
        } else if tokio::time::Instant::now() >= deadline {
            SinkRecoveryScopeConvergenceAction::ReturnTimeout
        } else if matches!(
            self.post_recovery_state().scope_trigger_state,
            SinkRecoveryScopeTriggerState::InitialNotTriggered
        ) {
            SinkRecoveryScopeConvergenceAction::TriggerInitialAndWait
        } else {
            SinkRecoveryScopeConvergenceAction::Wait
        }
    }

    async fn maybe_drive_pending_post_recovery_scope_convergence_retrigger(
        &mut self,
        source: &Arc<SourceFacade>,
    ) -> std::result::Result<(), RuntimeWorkerObservationFailure> {
        if matches!(
            self.post_recovery_state().scope_trigger_state,
            SinkRecoveryScopeTriggerState::RetryPending
        ) {
            let request_epoch = source
                .submit_rescan_when_ready_epoch_with_failure()
                .await
                .map_err(RuntimeWorkerObservationFailure::from)?;
            let state = self.post_recovery_state_mut();
            state.source_rescan_request_epoch = Some(request_epoch);
            state.scope_trigger_state = SinkRecoveryScopeTriggerState::Triggered;
        }
        Ok(())
    }

    async fn maybe_trigger_initial_post_recovery_scope_convergence(
        &mut self,
        source: &Arc<SourceFacade>,
    ) -> std::result::Result<(), RuntimeWorkerObservationFailure> {
        if matches!(
            self.post_recovery_state().scope_trigger_state,
            SinkRecoveryScopeTriggerState::InitialNotTriggered
        ) {
            let request_epoch = source
                .submit_rescan_when_ready_epoch_with_failure()
                .await
                .map_err(RuntimeWorkerObservationFailure::from)?;
            let state = self.post_recovery_state_mut();
            state.source_rescan_request_epoch = Some(request_epoch);
            state.scope_trigger_state = SinkRecoveryScopeTriggerState::Triggered;
        }
        Ok(())
    }

    async fn refresh_post_recovery_source_rescan_observation(
        &mut self,
        source: &Arc<SourceFacade>,
    ) {
        if let Some(request_epoch) = self.post_recovery_state().source_rescan_request_epoch {
            if let Ok(snapshot) = source.progress_snapshot_with_failure().await {
                if snapshot.rescan_observed_epoch >= request_epoch {
                    self.post_recovery_state_mut().source_rescan_request_epoch = None;
                }
            }
        }
    }

    async fn wait_for_post_recovery_progress(
        &self,
        runtime_state_changed: &Arc<tokio::sync::Notify>,
        source: &Arc<SourceFacade>,
        sink: &Arc<SinkFacade>,
        deadline: tokio::time::Instant,
    ) {
        if self.post_recovery_source_rescan_pending() {
            FSMetaApp::wait_for_runtime_source_or_sink_progress_until(
                runtime_state_changed,
                source,
                sink,
                deadline,
            )
            .await;
        } else {
            FSMetaApp::wait_for_runtime_or_sink_progress_until(
                runtime_state_changed,
                sink,
                deadline,
            )
            .await;
        }
    }

    async fn maybe_schedule_post_recovery_sink_timeout_retry(
        &mut self,
        source: &Arc<SourceFacade>,
        before_deadline: bool,
    ) -> Result<bool> {
        if !before_deadline
            || !matches!(
                self.post_recovery_state().sink_timeout_retry_state,
                SinkRecoveryTimeoutRetryState::NotRetried
            )
        {
            return Ok(false);
        }
        // A later source-only recovery can converge route scopes from primed
        // cached groups before the post-recovery rescan has actually
        // rematerialized baseline source publication. Request one explicit
        // manual rescan before the final source->sink retry so sink readiness
        // wait does not return on a scheduled-only zero-state sink.
        source
            .publish_manual_rescan_signal_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)
            .map_err(RuntimeWorkerObservationFailure::into_error)?;
        let state = self.post_recovery_state_mut();
        state.sink_timeout_retry_state = SinkRecoveryTimeoutRetryState::RetriedAfterManualRescan;
        state.scope_trigger_state = SinkRecoveryScopeTriggerState::RetryPending;
        Ok(true)
    }

    async fn run_post_recovery_readiness(
        &mut self,
        source: &Arc<SourceFacade>,
        sink: &Arc<SinkFacade>,
        runtime_state_changed: &Arc<tokio::sync::Notify>,
    ) -> std::result::Result<
        Option<std::collections::BTreeSet<String>>,
        RuntimeWorkerObservationFailure,
    > {
        let summarize_cached_sink_status_after_scope_convergence_timeout =
            |sink: &Arc<SinkFacade>| {
                sink.cached_status_snapshot_with_failure()
                    .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                    .unwrap_or_else(|failure| {
                        format!("cached_sink_status_unavailable err={}", failure.as_error())
                    })
            };
        let cached_sink_status_should_defer_republish = |sink: &Arc<SinkFacade>| {
            sink.cached_status_snapshot_with_failure()
                .ok()
                .is_some_and(|snapshot| {
                    FSMetaApp::sink_status_snapshot_should_defer_republish_after_retained_replay_timeout(&snapshot)
                })
        };
        let cached_sink_status_has_pending_materialization_progress =
            |sink: &Arc<SinkFacade>, expected_groups: &std::collections::BTreeSet<String>| {
                sink.cached_status_snapshot_with_failure()
                    .ok()
                    .is_some_and(|snapshot| {
                        FSMetaApp::sink_status_snapshot_has_pending_materialization_progress_for_expected_groups(
                            &snapshot,
                            expected_groups,
                        )
                    })
            };
        loop {
            let scope_convergence_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let converged_groups = loop {
                self.maybe_drive_pending_post_recovery_scope_convergence_retrigger(source)
                    .await?;
                self.refresh_post_recovery_source_rescan_observation(source)
                    .await;
                let scope_observation =
                    FSMetaApp::observe_runtime_scope_convergence(source, sink).await?;
                let expected_scope_groups = scope_observation.expected_groups_for_logical_roots(
                    &source
                        .logical_roots_snapshot_with_failure()
                        .await
                        .map_err(RuntimeWorkerObservationFailure::from)?,
                );
                match self.post_recovery_scope_action(
                    &scope_observation,
                    &expected_scope_groups,
                    scope_convergence_deadline,
                ) {
                    SinkRecoveryScopeConvergenceAction::Converged => {
                        break expected_scope_groups.sink_groups;
                    }
                    SinkRecoveryScopeConvergenceAction::ReturnTimeout => {
                        return Err(RuntimeWorkerObservationFailure::from_cause(
                            CnxError::Internal(format!(
                                "runtime scope convergence not observed after retained replay: {}",
                                scope_observation.timeout_context()
                            )),
                        ));
                    }
                    SinkRecoveryScopeConvergenceAction::TriggerInitialAndWait => {
                        self.maybe_trigger_initial_post_recovery_scope_convergence(source)
                            .await?;
                        self.wait_for_post_recovery_progress(
                            runtime_state_changed,
                            source,
                            sink,
                            scope_convergence_deadline,
                        )
                        .await;
                    }
                    SinkRecoveryScopeConvergenceAction::Wait => {
                        self.wait_for_post_recovery_progress(
                            runtime_state_changed,
                            source,
                            sink,
                            scope_convergence_deadline,
                        )
                        .await;
                    }
                }
            };

            if converged_groups.is_empty() {
                return Ok(None);
            }

            let sink_readiness_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                self.refresh_post_recovery_source_rescan_observation(source)
                    .await;
                match tokio::time::timeout(
                    Duration::from_millis(250),
                    sink.status_snapshot_with_failure(),
                )
                .await
                {
                    Ok(Ok(snapshot)) => {
                        if sink_status_snapshot_ready_for_expected_groups(
                            &snapshot,
                            &converged_groups,
                        ) {
                            return Ok(None);
                        }
                        if FSMetaApp::sink_status_snapshot_has_pending_materialization_progress_for_expected_groups(
                            &snapshot,
                            &converged_groups,
                        ) {
                            return Ok(None);
                        }
                        if FSMetaApp::sink_status_snapshot_has_materialized_pending_progress_for_expected_groups(
                            &snapshot,
                            &converged_groups,
                        ) && source
                            .status_snapshot_with_failure()
                            .await
                            .ok()
                            .is_some_and(|source_snapshot| {
                                FSMetaApp::source_status_snapshot_has_inflight_initial_audit_for_expected_groups(
                                    &source_snapshot,
                                    &converged_groups,
                                )
                            })
                        {
                            return Ok(None);
                        }
                        if FSMetaApp::sink_status_snapshot_should_defer_republish_after_retained_replay_timeout(&snapshot)
                        {
                            if self
                                .maybe_schedule_post_recovery_sink_timeout_retry(
                                    source,
                                    tokio::time::Instant::now() < sink_readiness_deadline,
                                )
                                .await
                                .map_err(RuntimeWorkerObservationFailure::from_cause)?
                            {
                                break;
                            }
                            return Ok(Some(converged_groups));
                        }
                        if tokio::time::Instant::now() >= sink_readiness_deadline {
                            return Err(RuntimeWorkerObservationFailure::from_cause(
                                CnxError::Internal(format!(
                                    "sink status readiness not restored after retained replay once runtime scope converged: {}",
                                    summarize_sink_status_endpoint(&snapshot)
                                )),
                            ));
                        }
                    }
                    Ok(Err(err)) if matches!(err.as_error(), CnxError::Timeout) => {
                        if self
                            .maybe_schedule_post_recovery_sink_timeout_retry(
                                source,
                                tokio::time::Instant::now() < sink_readiness_deadline,
                            )
                            .await
                            .map_err(RuntimeWorkerObservationFailure::from_cause)?
                        {
                            break;
                        }
                        if cached_sink_status_has_pending_materialization_progress(
                            sink,
                            &converged_groups,
                        ) {
                            return Ok(None);
                        }
                        if cached_sink_status_should_defer_republish(sink) {
                            return Ok(Some(converged_groups));
                        }
                        if tokio::time::Instant::now() < sink_readiness_deadline {
                            continue;
                        }
                        return Err(RuntimeWorkerObservationFailure::from_cause(
                            CnxError::Internal(format!(
                                "sink status readiness not restored after retained replay once runtime scope converged: groups={:?} raw_timeout=true {}",
                                converged_groups,
                                summarize_cached_sink_status_after_scope_convergence_timeout(sink)
                            )),
                        ));
                    }
                    Ok(Err(_err)) if tokio::time::Instant::now() < sink_readiness_deadline => {}
                    Ok(Err(err)) => return Err(RuntimeWorkerObservationFailure::from(err)),
                    Err(_)
                        if self
                            .maybe_schedule_post_recovery_sink_timeout_retry(
                                source,
                                tokio::time::Instant::now() < sink_readiness_deadline,
                            )
                            .await
                            .map_err(RuntimeWorkerObservationFailure::from_cause)? =>
                    {
                        break;
                    }
                    Err(_) if tokio::time::Instant::now() < sink_readiness_deadline => {}
                    Err(_) => {
                        if cached_sink_status_has_pending_materialization_progress(
                            sink,
                            &converged_groups,
                        ) {
                            return Ok(None);
                        }
                        if cached_sink_status_should_defer_republish(sink) {
                            return Ok(Some(converged_groups));
                        }
                        return Err(RuntimeWorkerObservationFailure::from_cause(
                            CnxError::Internal(format!(
                                "sink status readiness not restored after retained replay once runtime scope converged: groups={:?} raw_timeout=true {}",
                                converged_groups,
                                summarize_cached_sink_status_after_scope_convergence_timeout(sink)
                            )),
                        ));
                    }
                }
                self.wait_for_post_recovery_progress(
                    runtime_state_changed,
                    source,
                    sink,
                    sink_readiness_deadline,
                )
                .await;
            }
        }
    }

    fn local_source_request_observed(
        &self,
        source_progress: &crate::source::SourceProgressSnapshot,
    ) -> bool {
        !self
            .local_state()
            .source_rescan_request_epoch
            .is_some_and(|request_epoch| source_progress.rescan_observed_epoch < request_epoch)
    }

    fn local_source_publication_satisfied(
        &self,
        source_progress: &crate::source::SourceProgressSnapshot,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> bool {
        !self
            .local_state()
            .source_publication_epoch_floor
            .is_some_and(|request_epoch| {
                !source_progress.published_expected_groups_since(request_epoch, expected_groups)
            })
    }

    fn local_cached_ready_for_expected_groups(
        &self,
        cached_sink_progress: Option<&crate::sink::SinkProgressSnapshot>,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> bool {
        self.local_state().allow_cached_ready_fast_path
            && cached_sink_progress
                .is_some_and(|progress| progress.ready_for_expected_groups(expected_groups))
    }

    fn local_sink_materialized_for_expected_groups(
        &self,
        cached_sink_progress: Option<&crate::sink::SinkProgressSnapshot>,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> bool {
        !expected_groups.is_empty()
            && cached_sink_progress
                .is_some_and(|progress| expected_groups.is_subset(&progress.materialized_groups))
    }

    fn local_step_for_progress(
        &self,
        scope_converged: bool,
        source_progress: &crate::source::SourceProgressSnapshot,
        cached_sink_progress: Option<&crate::sink::SinkProgressSnapshot>,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> SinkRecoveryStep {
        let source_request_observed = self.local_source_request_observed(source_progress);
        let source_publication_satisfied =
            self.local_source_publication_satisfied(source_progress, expected_groups);
        let cached_ready_for_expected_groups =
            self.local_cached_ready_for_expected_groups(cached_sink_progress, expected_groups);
        if !scope_converged {
            return if self.remaining().is_zero() {
                SinkRecoveryStep::ReturnTimeout
            } else {
                SinkRecoveryStep::WaitForRuntimeScopeConvergence
            };
        }
        let retrigger_count = self.local_state().retrigger_state.count();
        let retrigger_pending = self.local_state().retrigger_state.is_pending();
        if cached_ready_for_expected_groups && source_publication_satisfied && retrigger_count == 0
        {
            return SinkRecoveryStep::ReturnReady;
        }
        if self.remaining().is_zero() {
            return SinkRecoveryStep::ReturnTimeout;
        }
        if self.local_requires_probe_before_ready() && source_publication_satisfied {
            return SinkRecoveryStep::ProbeReadiness;
        }
        if retrigger_pending {
            return SinkRecoveryStep::TriggerSourceToSinkConvergence;
        }
        if self.local_requires_probe_before_ready() {
            if !source_publication_satisfied {
                return SinkRecoveryStep::WaitForRuntimeScopeConvergence;
            }
        } else if !source_request_observed {
            return SinkRecoveryStep::WaitForRuntimeScopeConvergence;
        }
        self.next_local_step_after_scope_convergence()
    }

    async fn trigger_local_source_to_sink_convergence(
        &mut self,
        source: &Arc<SourceFacade>,
    ) -> std::result::Result<(), RuntimeWorkerObservationFailure> {
        let request_epoch = source
            .submit_rescan_when_ready_epoch_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)?;
        self.local_state_mut().source_rescan_request_epoch = Some(request_epoch);
        self.local_state_mut().source_publication_epoch_floor = Some(request_epoch);
        self.local_state_mut().retrigger_state.mark_triggered();
        #[cfg(test)]
        {
            self.local_state_mut().retrigger_probe_pause_pending = true;
        }
        Ok(())
    }

    async fn replay_local_retained_sink_wave(
        &mut self,
        sink: &Arc<SinkFacade>,
        post_return_sink_replay_signals: &[SinkControlSignal],
    ) -> std::result::Result<(), RuntimeWorkerObservationFailure> {
        sink.apply_orchestration_signals_with_total_timeout_with_failure(
            post_return_sink_replay_signals,
            self.remaining(),
        )
        .await
        .map_err(RuntimeWorkerObservationFailure::from)?;
        if post_return_sink_replay_signals.is_empty() {
            self.local_state_mut().retained_sink_replay_state =
                SinkRecoveryRetainedSinkReplayState::None;
        } else {
            let retrigger_state = self.local_state().retrigger_state;
            let local_state = self.local_state_mut();
            local_state.retained_sink_replay_state = SinkRecoveryRetainedSinkReplayState::Replayed;
            if !retrigger_state.is_pending() && retrigger_state.count() > 0 {
                local_state.retrigger_state = SinkRecoveryRetriggerState::Pending {
                    count: retrigger_state.count(),
                };
                local_state.source_rescan_request_epoch = None;
                local_state.source_publication_epoch_floor = None;
            }
        }
        Ok(())
    }

    async fn maybe_pause_before_probe(&mut self) {
        #[cfg(test)]
        if self.first_sink_probe_pending {
            maybe_pause_before_local_sink_status_republish_probe().await;
            self.first_sink_probe_pending = false;
        }
    }

    async fn schedule_local_manual_rescan_fallback(
        &mut self,
        source: &Arc<SourceFacade>,
        post_return_sink_replay_signals: &[SinkControlSignal],
    ) -> Result<()> {
        if matches!(
            self.local_state().manual_rescan_state,
            SinkRecoveryManualRescanState::Published
        ) {
            return Ok(());
        }
        // Retained sink replay can leave the local sink status lane effectively fresh
        // again. Force one baseline replay pass, then retrigger source->sink
        // convergence one more time so readiness probing always follows an
        // observed rescan epoch instead of a best-effort probe guess.
        source
            .publish_manual_rescan_signal_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)
            .map_err(RuntimeWorkerObservationFailure::into_error)?;
        let should_retrigger = {
            let state = self.local_state_mut();
            state.manual_rescan_state = SinkRecoveryManualRescanState::Published;
            state.retained_sink_replay_state = match (
                state.retained_sink_replay_state,
                post_return_sink_replay_signals.is_empty(),
            ) {
                (_, true) => SinkRecoveryRetainedSinkReplayState::None,
                (SinkRecoveryRetainedSinkReplayState::Replayed, false) => {
                    SinkRecoveryRetainedSinkReplayState::Replayed
                }
                (_, false) => SinkRecoveryRetainedSinkReplayState::Pending,
            };
            state.retrigger_state.count() < 3
        };
        if should_retrigger {
            self.trigger_local_source_to_sink_convergence(source)
                .await
                .map_err(RuntimeWorkerObservationFailure::into_error)?;
        }
        Ok(())
    }

    fn local_probe_action(
        &mut self,
        probe_outcome: SinkRecoveryProbeOutcome,
        source_progress: &crate::source::SourceProgressSnapshot,
        post_return_sink_replay_signals: &[SinkControlSignal],
    ) -> SinkRecoveryStep {
        let source_request_observed = self.local_source_request_observed(source_progress);
        match probe_outcome {
            SinkRecoveryProbeOutcome::BlockingReady => {
                self.local_state_mut().retained_sink_replay_state =
                    SinkRecoveryRetainedSinkReplayState::None;
                SinkRecoveryStep::ReturnReady
            }
            SinkRecoveryProbeOutcome::Ready => {
                match self.local_state().retained_sink_replay_state {
                    SinkRecoveryRetainedSinkReplayState::None
                        if !post_return_sink_replay_signals.is_empty() =>
                    {
                        self.local_state_mut().retained_sink_replay_state =
                            SinkRecoveryRetainedSinkReplayState::Pending;
                        SinkRecoveryStep::ReplayRetainedSinkWave
                    }
                    SinkRecoveryRetainedSinkReplayState::RequireReadyProbe => {
                        let retained_sink_replay_state =
                            if post_return_sink_replay_signals.is_empty() {
                                SinkRecoveryRetainedSinkReplayState::None
                            } else {
                                SinkRecoveryRetainedSinkReplayState::Pending
                            };
                        self.local_state_mut().retained_sink_replay_state =
                            retained_sink_replay_state;
                        if retained_sink_replay_state.is_pending() {
                            let retrigger_count = self.local_state().retrigger_state.count();
                            self.local_state_mut().retrigger_state =
                                SinkRecoveryRetriggerState::Idle {
                                    count: retrigger_count,
                                };
                            SinkRecoveryStep::ReplayRetainedSinkWave
                        } else {
                            SinkRecoveryStep::ReturnReady
                        }
                    }
                    _ => SinkRecoveryStep::ReturnReady,
                }
            }
            SinkRecoveryProbeOutcome::NotReady
                if matches!(
                    self.local_state().retained_sink_replay_state,
                    SinkRecoveryRetainedSinkReplayState::RequireReadyProbe
                ) =>
            {
                let retained_sink_replay_state = if post_return_sink_replay_signals.is_empty() {
                    SinkRecoveryRetainedSinkReplayState::None
                } else {
                    SinkRecoveryRetainedSinkReplayState::Pending
                };
                self.local_state_mut().retained_sink_replay_state = retained_sink_replay_state;
                if retained_sink_replay_state.is_pending() {
                    SinkRecoveryStep::ReplayRetainedSinkWave
                } else if self.remaining().is_zero() {
                    SinkRecoveryStep::ReturnTimeout
                } else {
                    SinkRecoveryStep::PublishManualRescanFallbackAndWait
                }
            }
            SinkRecoveryProbeOutcome::NotReady
                if !post_return_sink_replay_signals.is_empty()
                    && !matches!(
                        self.local_state().retained_sink_replay_state,
                        SinkRecoveryRetainedSinkReplayState::Pending
                            | SinkRecoveryRetainedSinkReplayState::Replayed
                    ) =>
            {
                self.local_state_mut().retained_sink_replay_state =
                    SinkRecoveryRetainedSinkReplayState::Pending;
                SinkRecoveryStep::ReplayRetainedSinkWave
            }
            SinkRecoveryProbeOutcome::NotReady
                if !source_request_observed
                    && self.local_state().manual_rescan_state
                        == SinkRecoveryManualRescanState::NotPublished
                    && self.local_state().retrigger_state.count() >= 2 =>
            {
                SinkRecoveryStep::PublishManualRescanFallbackAndWait
            }
            SinkRecoveryProbeOutcome::NotReady if !source_request_observed => {
                SinkRecoveryStep::WaitForRuntimeScopeConvergence
            }
            SinkRecoveryProbeOutcome::NotReady if self.remaining().is_zero() => {
                SinkRecoveryStep::ReturnTimeout
            }
            SinkRecoveryProbeOutcome::NotReady => {
                SinkRecoveryStep::PublishManualRescanFallbackAndWait
            }
        }
    }

    fn next_local_step_after_scope_convergence(&self) -> SinkRecoveryStep {
        let state = self.local_state();
        if state.retrigger_state.is_pending() {
            return SinkRecoveryStep::TriggerSourceToSinkConvergence;
        }
        if state.retained_sink_replay_state.is_pending() {
            return SinkRecoveryStep::ReplayRetainedSinkWave;
        }
        SinkRecoveryStep::ProbeReadiness
    }

    fn should_replay_retained_sink_before_scope_convergence(
        &self,
        scope_converged: bool,
        post_return_sink_replay_signals: &[SinkControlSignal],
    ) -> bool {
        !scope_converged
            && !post_return_sink_replay_signals.is_empty()
            && matches!(
                self.local_state().retained_sink_replay_state,
                SinkRecoveryRetainedSinkReplayState::Pending
                    | SinkRecoveryRetainedSinkReplayState::RequireReadyProbe
            )
    }

    async fn run_local_republish(
        &mut self,
        source: &Arc<SourceFacade>,
        sink: &Arc<SinkFacade>,
        runtime_state_changed: &Arc<tokio::sync::Notify>,
        expected_groups: &std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &[SinkControlSignal],
    ) -> std::result::Result<(), RuntimeWorkerObservationFailure> {
        #[cfg(test)]
        note_local_sink_status_republish_helper_entry_for_tests();
        let summarize_cached_sink_status = |sink: &Arc<SinkFacade>| {
            sink.cached_status_snapshot_with_failure()
                .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                .unwrap_or_else(|failure| {
                    format!("cached_sink_status_unavailable err={}", failure.as_error())
                })
        };
        loop {
            let expected_scope_groups = RuntimeScopeExpectedGroups::from_logical_roots(
                &source
                    .logical_roots_snapshot_with_failure()
                    .await
                    .map_err(RuntimeWorkerObservationFailure::from)?,
                expected_groups,
            );
            let scope_observation =
                FSMetaApp::observe_runtime_scope_convergence(source, sink).await?;
            let source_progress = source
                .progress_snapshot_with_failure()
                .await
                .map_err(RuntimeWorkerObservationFailure::from)?;
            if let Some(request_epoch) = self.local_state().source_rescan_request_epoch {
                if source_progress.rescan_observed_epoch >= request_epoch {
                    self.local_state_mut().source_rescan_request_epoch = None;
                }
            }
            if let Some(request_epoch) = self.local_state().source_publication_epoch_floor {
                if source_progress.published_expected_groups_since(request_epoch, expected_groups) {
                    self.local_state_mut().source_publication_epoch_floor = None;
                }
            }
            let scope_converged =
                scope_observation.matches_node_local_source_scope(&expected_scope_groups);
            let cached_sink_progress = sink.cached_progress_snapshot_with_failure().ok();
            let source_request_observed = self.local_source_request_observed(&source_progress);
            let source_publication_satisfied =
                self.local_source_publication_satisfied(&source_progress, expected_groups);
            let cached_ready_for_expected_groups = self.local_cached_ready_for_expected_groups(
                cached_sink_progress.as_ref(),
                expected_groups,
            );
            let sink_materialized_for_expected_groups = self
                .local_sink_materialized_for_expected_groups(
                    cached_sink_progress.as_ref(),
                    expected_groups,
                );
            let mut step = self.local_step_for_progress(
                scope_converged,
                &source_progress,
                cached_sink_progress.as_ref(),
                expected_groups,
            );
            if self.should_replay_retained_sink_before_scope_convergence(
                scope_converged,
                post_return_sink_replay_signals,
            ) {
                step = SinkRecoveryStep::ReplayRetainedSinkWave;
            }
            if debug_sink_recovery_capture_enabled() {
                eprintln!(
                    "fs_meta_runtime_app: local_sink_recovery progress expected={expected_groups:?} scope_converged={} cached_ready_for_expected_groups={} sink_materialized_for_expected_groups={} source_request_observed={} source_publication_satisfied={} retrigger_state={:?} manual_state={:?} retained_state={:?} step={:?}",
                    scope_converged,
                    cached_ready_for_expected_groups,
                    sink_materialized_for_expected_groups,
                    source_request_observed,
                    source_publication_satisfied,
                    self.local_state().retrigger_state,
                    self.local_state().manual_rescan_state,
                    self.local_state().retained_sink_replay_state,
                    step,
                );
            }
            loop {
                match step {
                    SinkRecoveryStep::WaitForRuntimeScopeConvergence => {
                        if !source_request_observed || !source_publication_satisfied {
                            FSMetaApp::wait_for_runtime_or_source_progress_until(
                                runtime_state_changed,
                                source,
                                self.deadline(),
                            )
                            .await;
                        } else {
                            FSMetaApp::wait_for_runtime_or_sink_progress_until(
                                runtime_state_changed,
                                sink,
                                self.deadline(),
                            )
                            .await;
                        }
                        break;
                    }
                    SinkRecoveryStep::ReturnReady => return Ok(()),
                    SinkRecoveryStep::TriggerSourceToSinkConvergence => {
                        if debug_sink_recovery_capture_enabled() {
                            eprintln!(
                                "fs_meta_runtime_app: local_sink_recovery step=TriggerSourceToSinkConvergence expected={expected_groups:?}"
                            );
                        }
                        self.trigger_local_source_to_sink_convergence(source)
                            .await?;
                        if !self.local_requires_probe_before_ready() {
                            FSMetaApp::wait_for_runtime_or_source_progress_until(
                                runtime_state_changed,
                                source,
                                self.deadline(),
                            )
                            .await;
                        }
                        break;
                    }
                    SinkRecoveryStep::ReplayRetainedSinkWave => {
                        if debug_sink_recovery_capture_enabled() {
                            eprintln!(
                                "fs_meta_runtime_app: local_sink_recovery step=ReplayRetainedSinkWave expected={expected_groups:?} signals={}",
                                post_return_sink_replay_signals.len()
                            );
                        }
                        self.replay_local_retained_sink_wave(sink, post_return_sink_replay_signals)
                            .await?;
                        FSMetaApp::wait_for_runtime_or_sink_progress_until(
                            runtime_state_changed,
                            sink,
                            self.deadline(),
                        )
                        .await;
                        break;
                    }
                    SinkRecoveryStep::ProbeReadiness => {
                        #[cfg(test)]
                        self.maybe_pause_at_retrigger_probe_boundary().await;
                        if debug_sink_recovery_capture_enabled() {
                            eprintln!(
                                "fs_meta_runtime_app: local_sink_recovery step=ProbeReadiness expected={expected_groups:?}"
                            );
                        }
                        self.maybe_pause_before_probe().await;
                        let probe_outcome = FSMetaApp::probe_local_sink_status_republish_readiness(
                            sink,
                            expected_groups,
                            self.deadline(),
                        )
                        .await;
                        step = self.local_probe_action(
                            probe_outcome,
                            &source_progress,
                            post_return_sink_replay_signals,
                        );
                        if debug_sink_recovery_capture_enabled() {
                            eprintln!(
                                "fs_meta_runtime_app: local_sink_recovery probe_outcome={:?} next_step={:?} expected={expected_groups:?}",
                                probe_outcome, step,
                            );
                        }
                    }
                    SinkRecoveryStep::PublishManualRescanFallbackAndWait => {
                        if debug_sink_recovery_capture_enabled() {
                            eprintln!(
                                "fs_meta_runtime_app: local_sink_recovery step=PublishManualRescanFallbackAndWait expected={expected_groups:?}"
                            );
                        }
                        self.schedule_local_manual_rescan_fallback(
                            source,
                            post_return_sink_replay_signals,
                        )
                        .await
                        .map_err(RuntimeWorkerObservationFailure::from_cause)?;
                        FSMetaApp::wait_for_runtime_or_source_progress_until(
                            runtime_state_changed,
                            source,
                            self.deadline(),
                        )
                        .await;
                        break;
                    }
                    SinkRecoveryStep::ReturnTimeout => {
                        if sink
                            .cached_status_snapshot_with_failure()
                            .ok()
                            .is_some_and(|snapshot| {
                                FSMetaApp::sink_status_snapshot_should_defer_local_republish_after_retained_replay(
                                    &snapshot,
                                    expected_groups,
                                )
                            })
                        {
                            return Ok(());
                        }
                        return Err(RuntimeWorkerObservationFailure::from_cause(
                            CnxError::Internal(format!(
                                "local sink status republish not restored after retained replay once runtime scope converged: expected_groups={expected_groups:?} {}",
                                summarize_cached_sink_status(sink)
                            )),
                        ));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
fn sink_recovery_test_expected_groups() -> std::collections::BTreeSet<String> {
    std::collections::BTreeSet::from([String::from("g1")])
}

#[cfg(test)]
fn sink_recovery_test_source_progress(
    request_observed: bool,
    publication_satisfied: bool,
) -> crate::source::SourceProgressSnapshot {
    let expected_groups = sink_recovery_test_expected_groups();
    crate::source::SourceProgressSnapshot {
        rescan_observed_epoch: u64::from(request_observed),
        scheduled_source_groups: expected_groups.clone(),
        scheduled_scan_groups: std::collections::BTreeSet::new(),
        published_group_epoch: if publication_satisfied {
            expected_groups
                .into_iter()
                .map(|group_id| (group_id, 1))
                .collect()
        } else {
            std::collections::BTreeMap::new()
        },
    }
}

#[cfg(test)]
fn sink_recovery_test_cached_sink_progress(
    materialized: bool,
    ready: bool,
) -> crate::sink::SinkProgressSnapshot {
    let expected_groups = sink_recovery_test_expected_groups();
    crate::sink::SinkProgressSnapshot {
        scheduled_groups: expected_groups.clone(),
        materialized_groups: if materialized {
            expected_groups.clone()
        } else {
            std::collections::BTreeSet::new()
        },
        ready_groups: if ready {
            expected_groups
        } else {
            std::collections::BTreeSet::new()
        },
        empty_summary: false,
    }
}

#[cfg(test)]
fn sink_recovery_test_local_step(
    runtime: &SinkRecoveryMachine,
    scope_converged: bool,
    request_observed: bool,
    publication_satisfied: bool,
    materialized: bool,
    cached_ready: bool,
) -> SinkRecoveryStep {
    let expected_groups = sink_recovery_test_expected_groups();
    let source_progress =
        sink_recovery_test_source_progress(request_observed, publication_satisfied);
    let cached_sink_progress = sink_recovery_test_cached_sink_progress(materialized, cached_ready);
    runtime.local_step_for_progress(
        scope_converged,
        &source_progress,
        Some(&cached_sink_progress),
        &expected_groups,
    )
}

#[cfg(test)]
fn sink_recovery_test_probe_step(
    runtime: &mut SinkRecoveryMachine,
    outcome: SinkRecoveryProbeOutcome,
    request_observed: bool,
    publication_satisfied: bool,
    post_return_sink_replay_signals: &[SinkControlSignal],
) -> SinkRecoveryStep {
    let source_progress =
        sink_recovery_test_source_progress(request_observed, publication_satisfied);
    runtime.local_probe_action(outcome, &source_progress, post_return_sink_replay_signals)
}

#[test]
fn sink_recovery_machine_local_runtime_owns_deadline_for_iteration_and_probe_timeout() {
    let live_runtime = SinkRecoveryMachine::new_local_with_deadline(
        LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );
    let expired_runtime = SinkRecoveryMachine::new_local_with_deadline(
        LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath,
        tokio::time::Instant::now(),
    );

    assert_eq!(
        sink_recovery_test_local_step(&live_runtime, false, true, true, false, false),
        SinkRecoveryStep::WaitForRuntimeScopeConvergence,
        "local republish waits should keep using the runtime-owned deadline while scope convergence is still pending",
    );
    assert_eq!(
        sink_recovery_test_local_step(&expired_runtime, true, true, true, false, false),
        SinkRecoveryStep::ReturnTimeout,
        "local republish timeout should come from the runtime-owned deadline instead of outer helper-local deadline booleans",
    );
    let mut expired_probe_runtime = expired_runtime;
    assert_eq!(
        sink_recovery_test_probe_step(
            &mut expired_probe_runtime,
            SinkRecoveryProbeOutcome::NotReady,
            true,
            true,
            &[],
        ),
        SinkRecoveryStep::ReturnTimeout,
        "post-probe timeout should also come from the same runtime-owned deadline",
    );
}

#[test]
fn sink_recovery_machine_local_require_probe_mode_promotes_first_ready_probe_internally() {
    let mut runtime = SinkRecoveryMachine::new_local_with_deadline(
        LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );

    assert_eq!(
        sink_recovery_test_probe_step(
            &mut runtime,
            SinkRecoveryProbeOutcome::Ready,
            true,
            true,
            &[],
        ),
        SinkRecoveryStep::ReturnReady,
        "require-probe mode should resolve the first ready probe inside the runtime instead of bouncing through outer post-probe helper decisions",
    );
    assert_eq!(
        runtime.local_state().retained_sink_replay_state,
        SinkRecoveryRetainedSinkReplayState::None,
        "the runtime should clear the synthetic require-probe latch once the first ready probe lands",
    );
}

#[test]
fn sink_recovery_machine_local_not_ready_probe_requests_manual_rescan_wait() {
    let mut runtime = SinkRecoveryMachine::new_local_with_deadline(
        LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );

    assert_eq!(
        sink_recovery_test_probe_step(
            &mut runtime,
            SinkRecoveryProbeOutcome::NotReady,
            true,
            true,
            &[],
        ),
        SinkRecoveryStep::PublishManualRescanFallbackAndWait,
        "local republish runtime should own the manual-rescan publish-plus-wait lane instead of leaving the outer helper loop to glue together a probe decision, a fallback publication, and a separate wait branch",
    );
}

#[test]
fn sink_recovery_machine_local_unobserved_source_publication_only_accepts_ready_probe_truth() {
    let mut runtime = SinkRecoveryMachine::new_local_with_deadline(
        LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );
    runtime.local_state_mut().retrigger_state = SinkRecoveryRetriggerState::Idle { count: 1 };
    runtime.local_state_mut().source_rescan_request_epoch = Some(1);
    runtime.local_state_mut().source_publication_epoch_floor = Some(1);

    assert_eq!(
        sink_recovery_test_probe_step(
            &mut runtime,
            SinkRecoveryProbeOutcome::NotReady,
            false,
            false,
            &[],
        ),
        SinkRecoveryStep::WaitForRuntimeScopeConvergence,
        "until the owned source-rescan publication is observed, a not-ready probe should keep waiting instead of prematurely escalating into manual-rescan fallback",
    );
}

#[test]
fn sink_recovery_machine_local_next_action_owns_pending_step_and_probe_followup() {
    let expected = RuntimeScopeExpectedGroups {
        source_groups: std::collections::BTreeSet::from([String::from("g1")]),
        scan_groups: std::collections::BTreeSet::from([String::from("g1")]),
        sink_groups: std::collections::BTreeSet::from([String::from("g1")]),
    };
    let converged = RuntimeScopeConvergenceObservation {
        source_groups: expected.source_groups.clone(),
        scan_groups: expected.scan_groups.clone(),
        sink_groups: expected.sink_groups.clone(),
    };
    let mut runtime = SinkRecoveryMachine::new_local_with_deadline(
        LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );

    assert_eq!(
        sink_recovery_test_local_step(
            &runtime,
            converged.matches_expectation(RuntimeScopeConvergenceExpectation::ExpectedGroups(
                &expected
            )),
            true,
            true,
            false,
            false,
        ),
        SinkRecoveryStep::TriggerSourceToSinkConvergence,
        "local republish runtime should own the first pending step directly instead of collapsing it into an outer helper-local apply-pending-steps bundle",
    );

    runtime.local_state_mut().retrigger_state = SinkRecoveryRetriggerState::Idle { count: 1 };
    assert_eq!(
        sink_recovery_test_local_step(
            &runtime,
            converged.matches_expectation(RuntimeScopeConvergenceExpectation::ExpectedGroups(
                &expected
            )),
            true,
            true,
            false,
            false,
        ),
        SinkRecoveryStep::ProbeReadiness,
        "once pending steps clear, the runtime should advance directly into probe readiness instead of forcing the outer helper to decide when probing starts",
    );

    assert_eq!(
        sink_recovery_test_probe_step(
            &mut runtime,
            SinkRecoveryProbeOutcome::NotReady,
            true,
            true,
            &[],
        ),
        SinkRecoveryStep::PublishManualRescanFallbackAndWait,
        "post-probe not-ready handling should also stay inside the runtime instead of bouncing back out to an outer post-probe branch",
    );
}

#[test]
fn sink_recovery_machine_local_scope_action_owns_expected_group_convergence() {
    let runtime = SinkRecoveryMachine::new_local_with_deadline(
        LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );
    let expected = RuntimeScopeExpectedGroups {
        source_groups: std::collections::BTreeSet::from([String::from("g1")]),
        scan_groups: std::collections::BTreeSet::from([String::from("g1")]),
        sink_groups: std::collections::BTreeSet::from([String::from("g1")]),
    };
    let missing_sink = RuntimeScopeConvergenceObservation {
        source_groups: expected.source_groups.clone(),
        scan_groups: expected.scan_groups.clone(),
        sink_groups: std::collections::BTreeSet::new(),
    };
    let converged = RuntimeScopeConvergenceObservation {
        source_groups: expected.source_groups.clone(),
        scan_groups: expected.scan_groups.clone(),
        sink_groups: expected.sink_groups.clone(),
    };

    assert_eq!(
        sink_recovery_test_local_step(
            &runtime,
            missing_sink.matches_expectation(RuntimeScopeConvergenceExpectation::ExpectedGroups(
                &expected
            )),
            true,
            true,
            false,
            false,
        ),
        SinkRecoveryStep::WaitForRuntimeScopeConvergence,
        "local republish should derive expected-group convergence inside the runtime instead of relying on an outer convergence decision helper",
    );
    assert_eq!(
        sink_recovery_test_local_step(
            &runtime,
            converged.matches_expectation(RuntimeScopeConvergenceExpectation::ExpectedGroups(
                &expected
            )),
            true,
            true,
            true,
            true,
        ),
        SinkRecoveryStep::ReturnReady,
        "once expected groups converge, the runtime should promote the cached-ready fast path itself",
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkRecoveryProbeOutcome {
    Ready,
    BlockingReady,
    NotReady,
}

#[test]
fn sink_recovery_machine_post_recovery_scope_action_owns_deadline_and_trigger_lane() {
    let waiting = SinkRecoveryMachine::new_post_recovery(None);
    let pretriggered = SinkRecoveryMachine::new_post_recovery(Some(1));
    let expected = RuntimeScopeExpectedGroups {
        source_groups: std::collections::BTreeSet::from([String::from("g1")]),
        scan_groups: std::collections::BTreeSet::from([String::from("g1")]),
        sink_groups: std::collections::BTreeSet::from([String::from("g1")]),
    };
    let observation = RuntimeScopeConvergenceObservation {
        source_groups: std::collections::BTreeSet::from([String::from("g1")]),
        scan_groups: std::collections::BTreeSet::from([String::from("g1")]),
        sink_groups: std::collections::BTreeSet::new(),
    };

    assert_eq!(
        waiting.post_recovery_scope_action(
            &observation,
            &expected,
            tokio::time::Instant::now() + Duration::from_secs(1)
        ),
        SinkRecoveryScopeConvergenceAction::TriggerInitialAndWait,
        "post-recovery readiness waits should derive the initial convergence trigger lane from the wait state itself",
    );
    assert_eq!(
        pretriggered.post_recovery_scope_action(
            &observation,
            &expected,
            tokio::time::Instant::now() + Duration::from_secs(1)
        ),
        SinkRecoveryScopeConvergenceAction::Wait,
        "once pretriggered, the wait state should stay in pure wait mode without reopening the initial trigger helper seam",
    );
    assert_eq!(
        pretriggered.post_recovery_scope_action(
            &observation,
            &expected,
            tokio::time::Instant::now()
        ),
        SinkRecoveryScopeConvergenceAction::ReturnTimeout,
        "post-recovery scope convergence timeout should come from the wait-state-owned deadline check",
    );
}

#[test]
fn sink_recovery_machine_post_recovery_accepts_scan_only_expected_groups() {
    let waiting = SinkRecoveryMachine::new_post_recovery(None);
    let expected = RuntimeScopeExpectedGroups {
        source_groups: std::collections::BTreeSet::new(),
        scan_groups: std::collections::BTreeSet::from([String::from("g1")]),
        sink_groups: std::collections::BTreeSet::from([String::from("g1")]),
    };
    let observation = RuntimeScopeConvergenceObservation {
        source_groups: std::collections::BTreeSet::new(),
        scan_groups: std::collections::BTreeSet::from([String::from("g1")]),
        sink_groups: std::collections::BTreeSet::from([String::from("g1")]),
    };

    assert_eq!(
        waiting.post_recovery_scope_action(
            &observation,
            &expected,
            tokio::time::Instant::now() + Duration::from_secs(1)
        ),
        SinkRecoveryScopeConvergenceAction::Converged,
        "post-recovery readiness waits should treat scan-only roots as converged once scan and sink lanes match the expected groups",
    );
}

#[test]
fn sink_recovery_machine_post_recovery_accepts_node_local_source_scope_subset() {
    let waiting = SinkRecoveryMachine::new_post_recovery(None);
    let expected = RuntimeScopeExpectedGroups {
        source_groups: std::collections::BTreeSet::from([
            String::from("nfs1"),
            String::from("nfs2"),
        ]),
        scan_groups: std::collections::BTreeSet::from([String::from("nfs1"), String::from("nfs2")]),
        sink_groups: std::collections::BTreeSet::from([String::from("nfs1"), String::from("nfs2")]),
    };
    let observation = RuntimeScopeConvergenceObservation {
        source_groups: std::collections::BTreeSet::from([String::from("nfs2")]),
        scan_groups: std::collections::BTreeSet::from([String::from("nfs2")]),
        sink_groups: std::collections::BTreeSet::from([String::from("nfs1"), String::from("nfs2")]),
    };

    assert_eq!(
        waiting.post_recovery_scope_action(
            &observation,
            &expected,
            tokio::time::Instant::now() + Duration::from_secs(1)
        ),
        SinkRecoveryScopeConvergenceAction::Converged,
        "post-recovery sink replay must treat source/scan as node-local ownership evidence; requiring every sink group on each source owner keeps recovery fail-closed forever on distributed real-NFS scopes",
    );
}

#[test]
fn sink_recovery_machine_post_recovery_accepts_empty_roots_empty_scope() {
    let waiting = SinkRecoveryMachine::new_post_recovery(None);
    let observation = RuntimeScopeConvergenceObservation {
        source_groups: std::collections::BTreeSet::new(),
        scan_groups: std::collections::BTreeSet::new(),
        sink_groups: std::collections::BTreeSet::new(),
    };
    let expected = observation.expected_groups_for_logical_roots(&[]);

    assert_eq!(
        expected,
        RuntimeScopeExpectedGroups {
            source_groups: std::collections::BTreeSet::new(),
            scan_groups: std::collections::BTreeSet::new(),
            sink_groups: std::collections::BTreeSet::new(),
        },
        "empty-roots deployment should carry an explicit empty expected scope"
    );
    assert_eq!(
        waiting.post_recovery_scope_action(
            &observation,
            &expected,
            tokio::time::Instant::now() + Duration::from_secs(1),
        ),
        SinkRecoveryScopeConvergenceAction::Converged,
        "empty-roots deployment is a valid converged runtime scope and must not trigger sink-generation-cutover repair loops",
    );
}

fn control_frame_has_runtime_unit_exposure(envelopes: &[ControlEnvelope]) -> Result<bool> {
    for envelope in envelopes {
        if decode_runtime_unit_exposure(envelope)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

#[test]
fn post_recovery_sink_timeout_defers_scheduled_only_zero_state() {
    let snapshot = crate::sink::SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            vec![String::from("g1"), String::from("g2")],
        )]),
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        FSMetaApp::sink_status_snapshot_should_defer_republish_after_retained_replay_timeout(
            &snapshot
        ),
        "scheduled-only sink status after retained replay is deferred republish evidence, not a terminal replay failure"
    );
}

#[test]
fn post_recovery_sink_timeout_defers_pending_materialization_state() {
    let expected_groups =
        std::collections::BTreeSet::from([String::from("g1"), String::from("g2")]);
    let snapshot = crate::sink::SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            vec![String::from("g1"), String::from("g2")],
        )]),
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("g1"),
                primary_object_ref: String::from("node-a::g1"),
                total_nodes: 1,
                live_nodes: 1,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("g2"),
                primary_object_ref: String::from("node-a::g2"),
                total_nodes: 3,
                live_nodes: 2,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
        ],
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        FSMetaApp::sink_status_snapshot_should_defer_republish_after_retained_replay_timeout(
            &snapshot
        ),
        "pending materialization without sink delivery evidence still needs a bounded recovery follow-up"
    );
    assert!(
        !FSMetaApp::sink_status_snapshot_has_pending_materialization_progress_for_expected_groups(
            &snapshot,
            &expected_groups
        ),
        "pending materialization without sink delivery evidence must not be treated as normal initial-audit catch-up"
    );
}

#[test]
fn post_recovery_pending_materialization_with_delivery_progress_exits_replay_repair() {
    let expected_groups =
        std::collections::BTreeSet::from([String::from("g1"), String::from("g2")]);
    let snapshot = crate::sink::SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            vec![String::from("g1"), String::from("g2")],
        )]),
        stream_received_batches_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            184,
        )]),
        stream_applied_data_events_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            27_000,
        )]),
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("g1"),
                primary_object_ref: String::from("node-a::g1"),
                total_nodes: 12_000,
                live_nodes: 11_999,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 12_001,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("g2"),
                primary_object_ref: String::from("node-a::g2"),
                total_nodes: 15_000,
                live_nodes: 14_999,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 15_001,
                estimated_heap_bytes: 0,
            },
        ],
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        !sink_status_snapshot_ready_for_expected_groups(&snapshot, &expected_groups),
        "precondition: pending materialization remains unready for trusted materialized serving"
    );
    assert!(
        FSMetaApp::sink_status_snapshot_has_pending_materialization_progress_for_expected_groups(
            &snapshot,
            &expected_groups
        ),
        "post-recovery must classify delivered pending materialization as initial-audit catch-up, not as a new sink-generation-cutover repair trigger"
    );
}

#[test]
fn post_recovery_materialized_pending_exits_replay_repair_when_initial_audit_inflight() {
    let expected_groups =
        std::collections::BTreeSet::from([String::from("g1"), String::from("g2")]);
    let sink_snapshot = crate::sink::SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            vec![String::from("g1"), String::from("g2")],
        )]),
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("g1"),
                primary_object_ref: String::from("node-a::g1"),
                total_nodes: 12_000,
                live_nodes: 11_999,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 12_001,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("g2"),
                primary_object_ref: String::from("node-a::g2"),
                total_nodes: 15_000,
                live_nodes: 14_999,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 15_001,
                estimated_heap_bytes: 0,
            },
        ],
        ..crate::sink::SinkStatusSnapshot::default()
    };

    let mut source_snapshot = crate::source::SourceStatusSnapshot::default();
    for group_id in ["g1", "g2"] {
        source_snapshot
            .concrete_roots
            .push(crate::source::SourceConcreteRootHealthSnapshot {
                root_key: format!("node-a::{group_id}"),
                logical_root_id: group_id.to_string(),
                object_ref: format!("node-a::{group_id}"),
                status: "ready".to_string(),
                coverage_mode: "local-primary".to_string(),
                watch_enabled: true,
                scan_enabled: true,
                is_group_primary: true,
                active: true,
                watch_lru_capacity: 0,
                audit_interval_ms: 0,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_requested_at_us: None,
                last_rescan_reason: Some("initial_scan".to_string()),
                last_error: None,
                last_audit_started_at_us: Some(10),
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
                emitted_batch_count: 0,
                emitted_event_count: 0,
                emitted_control_event_count: 0,
                emitted_data_event_count: 0,
                emitted_path_capture_target: None,
                emitted_path_event_count: 0,
                last_emitted_at_us: None,
                last_emitted_origins: Vec::new(),
                forwarded_batch_count: 0,
                forwarded_event_count: 0,
                forwarded_path_event_count: 0,
                last_forwarded_at_us: None,
                last_forwarded_origins: Vec::new(),
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            });
    }

    assert!(
        !FSMetaApp::sink_status_snapshot_has_pending_materialization_progress_for_expected_groups(
            &sink_snapshot,
            &expected_groups,
        ),
        "precondition: this production-shaped snapshot has no stream delivery maps"
    );
    assert!(
        FSMetaApp::sink_status_snapshot_has_materialized_pending_progress_for_expected_groups(
            &sink_snapshot,
            &expected_groups,
        ),
        "materialized pending rows are progress evidence while source audit is still running"
    );
    assert!(
        FSMetaApp::source_status_snapshot_has_inflight_initial_audit_for_expected_groups(
            &source_snapshot,
            &expected_groups,
        ),
        "source audit started without completion is initial-audit catch-up evidence"
    );
}

#[test]
fn initial_audit_catchup_applies_first_post_initial_sink_cutover_without_deferring_replay() {
    assert_eq!(
        sink_generation_cutover_inline_action_from_evidence(
            SinkGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset,
            true,
            false,
            false,
            true,
        ),
        SinkGenerationCutoverInlineAction::ApplyAsInitialAuditCatchup,
        "online initial audit catch-up with materialized sink progress should not first schedule a sink-generation-cutover replay"
    );
    assert_eq!(
        sink_generation_cutover_inline_action_from_evidence(
            SinkGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset,
            true,
            false,
            false,
            false,
        ),
        SinkGenerationCutoverInlineAction::DeferReplay,
        "without initial audit catch-up evidence, the fail-closed first-defer behavior is preserved"
    );
    assert_eq!(
        sink_generation_cutover_inline_action_from_evidence(
            SinkGenerationCutoverDisposition::FailClosedSharedGenerationCutover,
            true,
            false,
            false,
            true,
        ),
        SinkGenerationCutoverInlineAction::Apply,
        "shared generation cutover is outside the initial-audit catch-up exception"
    );
    assert_eq!(
        sink_generation_cutover_inline_action_from_evidence(
            SinkGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset,
            true,
            true,
            false,
            true,
        ),
        SinkGenerationCutoverInlineAction::Apply,
        "source replay still owns fail-closed recovery instead of being masked by sink catch-up evidence"
    );
}

#[test]
fn inflight_initial_audit_evidence_rejects_completed_or_error_roots() {
    let expected_groups = std::collections::BTreeSet::from([String::from("g1")]);
    let mut root = crate::source::SourceConcreteRootHealthSnapshot {
        root_key: String::from("node-a::g1"),
        logical_root_id: String::from("g1"),
        object_ref: String::from("node-a::g1"),
        status: String::from("ready"),
        coverage_mode: String::from("local-primary"),
        watch_enabled: true,
        scan_enabled: true,
        is_group_primary: true,
        active: true,
        watch_lru_capacity: 0,
        audit_interval_ms: 0,
        overflow_count: 0,
        overflow_pending: false,
        rescan_pending: false,
        last_rescan_requested_at_us: None,
        last_rescan_reason: Some(String::from("initial_scan")),
        last_error: None,
        last_audit_started_at_us: Some(10),
        last_audit_completed_at_us: None,
        last_audit_duration_ms: None,
        emitted_batch_count: 0,
        emitted_event_count: 0,
        emitted_control_event_count: 0,
        emitted_data_event_count: 0,
        emitted_path_capture_target: None,
        emitted_path_event_count: 0,
        last_emitted_at_us: None,
        last_emitted_origins: Vec::new(),
        forwarded_batch_count: 0,
        forwarded_event_count: 0,
        forwarded_path_event_count: 0,
        last_forwarded_at_us: None,
        last_forwarded_origins: Vec::new(),
        current_revision: None,
        current_stream_generation: None,
        candidate_revision: None,
        candidate_stream_generation: None,
        candidate_status: None,
        draining_revision: None,
        draining_stream_generation: None,
        draining_status: None,
    };
    let inflight_snapshot = crate::source::SourceStatusSnapshot {
        concrete_roots: vec![root.clone()],
        ..crate::source::SourceStatusSnapshot::default()
    };
    assert!(
        FSMetaApp::source_status_snapshot_has_inflight_initial_audit_for_expected_groups(
            &inflight_snapshot,
            &expected_groups,
        )
    );

    root.last_audit_completed_at_us = Some(20);
    let completed_snapshot = crate::source::SourceStatusSnapshot {
        concrete_roots: vec![root.clone()],
        ..crate::source::SourceStatusSnapshot::default()
    };
    assert!(
        !FSMetaApp::source_status_snapshot_has_inflight_initial_audit_for_expected_groups(
            &completed_snapshot,
            &expected_groups,
        )
    );

    root.last_audit_completed_at_us = None;
    root.last_error = Some(String::from("scan failed"));
    let error_snapshot = crate::source::SourceStatusSnapshot {
        concrete_roots: vec![root],
        ..crate::source::SourceStatusSnapshot::default()
    };
    assert!(
        !FSMetaApp::source_status_snapshot_has_inflight_initial_audit_for_expected_groups(
            &error_snapshot,
            &expected_groups,
        )
    );
}

#[test]
fn post_recovery_sink_timeout_defers_stale_pending_group_rows() {
    let snapshot = crate::sink::SinkStatusSnapshot {
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("g1"),
                primary_object_ref: String::from("node-a::g1"),
                total_nodes: 0,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("g2"),
                primary_object_ref: String::from("node-a::g2"),
                total_nodes: 0,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
        ],
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        FSMetaApp::sink_status_snapshot_should_defer_republish_after_retained_replay_timeout(
            &snapshot
        ),
        "stale zero-live group rows after retained replay are catch-up evidence and must defer republish instead of reopening the gate as if sink status were terminally restored"
    );
}

#[test]
fn post_recovery_sink_timeout_defers_scheduled_ready_rows_without_live_materialized_owner() {
    let snapshot = crate::sink::SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: String::from("g1"),
            primary_object_ref: String::from("g1"),
            total_nodes: 2,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,
            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            vec![String::from("g1")],
        )]),
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        !sink_status_snapshot_ready_for_expected_groups(
            &snapshot,
            &std::collections::BTreeSet::from([String::from("g1")])
        ),
        "precondition: scheduled Ready rows without a live materialized owner are not query-service ready"
    );
    assert!(
        FSMetaApp::sink_status_snapshot_should_defer_republish_after_retained_replay_timeout(
            &snapshot
        ),
        "post-replay scheduled rows that are normalized Ready but not service-live-ready are catch-up evidence for local republish, not terminal replay failure"
    );
}

#[test]
fn local_sink_status_republish_timeout_defers_scheduled_only_zero_state() {
    let expected_groups =
        std::collections::BTreeSet::from([String::from("g1"), String::from("g2")]);
    let snapshot = crate::sink::SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            vec![String::from("g1"), String::from("g2")],
        )]),
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        FSMetaApp::sink_status_snapshot_should_defer_local_republish_after_retained_replay(
            &snapshot,
            &expected_groups
        ),
        "scheduled-only local sink status after retained replay is catch-up evidence; it must not turn a steady sink tick into a terminal republish failure"
    );
    assert!(
        !FSMetaApp::sink_status_snapshot_should_defer_local_republish_after_retained_replay(
            &crate::sink::SinkStatusSnapshot::default(),
            &expected_groups
        ),
        "an empty status without scheduled-group evidence must still fail closed"
    );
}

#[test]
fn local_sink_status_republish_timeout_defers_incomplete_scheduled_group_state() {
    let expected_groups =
        std::collections::BTreeSet::from([String::from("g1"), String::from("g2")]);
    let snapshot = crate::sink::SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            vec![String::from("g1"), String::from("g2")],
        )]),
        stream_received_batches_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            144,
        )]),
        stream_applied_control_events_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            2,
        )]),
        stream_applied_data_events_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            17_750,
        )]),
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("g1"),
                primary_object_ref: String::from("node-a::g1"),
                total_nodes: 8_750,
                live_nodes: 8_749,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 8_751,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("g2"),
                primary_object_ref: String::from("node-a::g2"),
                total_nodes: 9_000,
                live_nodes: 8_999,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 9_001,
                estimated_heap_bytes: 0,
            },
        ],
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        !sink_status_snapshot_ready_for_expected_groups(
            &snapshot,
            &std::collections::BTreeSet::from([String::from("g1"), String::from("g2")])
        ),
        "precondition: pending scheduled groups are not query-service ready"
    );
    assert!(
        FSMetaApp::sink_status_snapshot_should_defer_local_republish_after_retained_replay(
            &snapshot,
            &expected_groups
        ),
        "scheduled pending groups with retained stream replay evidence are local catch-up evidence, not a terminal republish failure"
    );
}

#[test]
fn local_sink_status_republish_timeout_defers_stale_replaced_schedule_after_scope_convergence() {
    let expected_groups = std::collections::BTreeSet::from([
        String::from("nfs1"),
        String::from("nfs2"),
        String::from("nfs4"),
    ]);
    let snapshot = crate::sink::SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            vec![
                String::from("nfs1"),
                String::from("nfs2"),
                String::from("nfs3"),
            ],
        )]),
        stream_received_batches_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            100,
        )]),
        stream_received_events_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            788,
        )]),
        stream_dropped_origin_counts_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            vec![String::from("node-b::nfs3=48")],
        )]),
        stream_applied_batches_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            79,
        )]),
        stream_applied_events_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            649,
        )]),
        stream_applied_control_events_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            122,
        )]),
        stream_applied_data_events_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            527,
        )]),
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("nfs1"),
                primary_object_ref: String::from("node-a::nfs1"),
                total_nodes: 10,
                live_nodes: 9,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 11,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("nfs2"),
                primary_object_ref: String::from("node-a::nfs2"),
                total_nodes: 8,
                live_nodes: 7,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 9,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("nfs3"),
                primary_object_ref: String::from("node-a::nfs3"),
                total_nodes: 6,
                live_nodes: 5,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 7,
                estimated_heap_bytes: 0,
            },
        ],
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        !sink_status_snapshot_ready_for_expected_groups(&snapshot, &expected_groups),
        "precondition: old nfs3 cached status must not count as ready for the new nfs4 runtime scope"
    );
    assert!(
        FSMetaApp::sink_status_snapshot_should_defer_local_republish_after_retained_replay(
            &snapshot,
            &expected_groups
        ),
        "stale replaced cached schedule after runtime-scope convergence is local catch-up evidence; route activation must defer sink-owned publication instead of failing the whole config commit"
    );
}

#[test]
fn local_sink_status_republish_timeout_defers_matching_ready_schedule_after_retained_replay() {
    let expected_groups = std::collections::BTreeSet::from([String::from("nfs3")]);
    let snapshot = crate::sink::SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            String::from("node-b"),
            vec![String::from("nfs3")],
        )]),
        stream_received_batches_by_node: std::collections::BTreeMap::from([(
            String::from("node-b"),
            182,
        )]),
        stream_received_events_by_node: std::collections::BTreeMap::from([(
            String::from("node-b"),
            776,
        )]),
        stream_ready_origin_counts_by_node: std::collections::BTreeMap::from([(
            String::from("node-b"),
            vec![
                String::from("node-b::nfs3=200"),
                String::from("node-d::nfs3=24"),
                String::from("node-e::nfs3=24"),
            ],
        )]),
        stream_applied_origin_counts_by_node: std::collections::BTreeMap::from([(
            String::from("node-b"),
            vec![
                String::from("node-b::nfs3=200"),
                String::from("node-d::nfs3=24"),
                String::from("node-e::nfs3=24"),
            ],
        )]),
        stream_last_applied_at_us_by_node: std::collections::BTreeMap::from([(
            String::from("node-b"),
            1_779_462_654_701_725,
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: String::from("nfs3"),
            primary_object_ref: String::from("node-b::nfs3"),
            total_nodes: 6,
            live_nodes: 5,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,
            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 7,
            estimated_heap_bytes: 0,
        }],
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        sink_status_snapshot_ready_for_expected_groups(&snapshot, &expected_groups),
        "precondition: matching scheduled/cached nfs3 with retained stream evidence is ready for the expected group"
    );
    assert!(
        FSMetaApp::sink_status_snapshot_should_defer_local_republish_after_retained_replay(
            &snapshot,
            &expected_groups
        ),
        "matching ready cached schedule after retained replay is local catch-up evidence; route activation must not terminal-fail while the local status republish catches up"
    );
}

#[test]
fn local_sink_status_republish_timeout_does_not_defer_replaced_schedule_without_replay_evidence() {
    let expected_groups = std::collections::BTreeSet::from([
        String::from("nfs1"),
        String::from("nfs2"),
        String::from("nfs4"),
    ]);
    let snapshot = crate::sink::SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            vec![
                String::from("nfs1"),
                String::from("nfs2"),
                String::from("nfs3"),
            ],
        )]),
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("nfs1"),
                primary_object_ref: String::from("node-a::nfs1"),
                total_nodes: 10,
                live_nodes: 9,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 11,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("nfs2"),
                primary_object_ref: String::from("node-a::nfs2"),
                total_nodes: 8,
                live_nodes: 7,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 9,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("nfs3"),
                primary_object_ref: String::from("node-a::nfs3"),
                total_nodes: 6,
                live_nodes: 5,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 7,
                estimated_heap_bytes: 0,
            },
        ],
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        !FSMetaApp::sink_status_snapshot_should_defer_local_republish_after_retained_replay(
            &snapshot,
            &expected_groups
        ),
        "a replaced schedule without retained stream/replay evidence is still a terminal control mismatch, not catch-up evidence"
    );
}

#[test]
fn local_sink_status_republish_timeout_does_not_defer_zero_pending_without_schedule_evidence() {
    let expected_groups =
        std::collections::BTreeSet::from([String::from("g1"), String::from("g2")]);
    let snapshot = crate::sink::SinkStatusSnapshot {
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("g1"),
                primary_object_ref: String::from("node-a::g1"),
                total_nodes: 0,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 0,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: String::from("g2"),
                primary_object_ref: String::from("node-a::g2"),
                total_nodes: 0,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 0,
                estimated_heap_bytes: 0,
            },
        ],
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        !FSMetaApp::sink_status_snapshot_should_defer_local_republish_after_retained_replay(
            &snapshot,
            &expected_groups
        ),
        "local republish must not treat zero pending rows without schedule/control evidence as restored sink ownership"
    );
}

#[test]
fn sink_recovery_machine_local_prioritizes_owned_retrigger_before_waiting_on_source_publication() {
    let runtime = SinkRecoveryMachine::new_local_pretriggered_with_deadline(
        LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath,
        tokio::time::Instant::now() + Duration::from_secs(1),
        1,
    );

    assert_eq!(
        sink_recovery_test_local_step(&runtime, true, false, false, true, true),
        SinkRecoveryStep::TriggerSourceToSinkConvergence,
        "local republish must spend its owned post-return retrigger before cached-ready or probe paths can win, even if the pretriggered source publication has not yet been observed",
    );
    let mut waiting_after_retrigger = runtime;
    waiting_after_retrigger.local_state_mut().retrigger_state =
        SinkRecoveryRetriggerState::Idle { count: 1 };
    assert_eq!(
        sink_recovery_test_local_step(&waiting_after_retrigger, true, false, false, false, false),
        SinkRecoveryStep::WaitForRuntimeScopeConvergence,
        "once the owned retrigger has been spent, the local helper must wait for the retriggered source rescan epoch before any sink probe can run",
    );
    assert_eq!(
        sink_recovery_test_local_step(&waiting_after_retrigger, true, true, true, false, false),
        SinkRecoveryStep::ProbeReadiness,
        "after the retriggered source publication is observed, the deferred helper can advance into readiness probing",
    );
}

#[test]
fn sink_recovery_machine_local_pretriggered_not_ready_probe_can_escalate_to_manual_rescan_after_owned_retrigger()
 {
    let mut runtime = SinkRecoveryMachine::new_local_pretriggered_with_deadline(
        LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath,
        tokio::time::Instant::now() + Duration::from_secs(1),
        1,
    );
    runtime.local_state_mut().retrigger_state = SinkRecoveryRetriggerState::Idle { count: 2 };

    assert_eq!(
        sink_recovery_test_probe_step(
            &mut runtime,
            SinkRecoveryProbeOutcome::NotReady,
            false,
            false,
            &[],
        ),
        SinkRecoveryStep::PublishManualRescanFallbackAndWait,
        "once the deferred helper has already spent its owned post-return retrigger, a still-unobserved source publication must escalate into the machine-owned manual-rescan fallback instead of timing out in pure wait mode",
    );
}

#[test]
fn sink_recovery_machine_local_pretriggered_unobserved_source_publication_escalates_before_probe_after_owned_retrigger()
 {
    let mut runtime = SinkRecoveryMachine::new_local_pretriggered_with_deadline(
        LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath,
        tokio::time::Instant::now() + Duration::from_secs(1),
        1,
    );
    runtime.local_state_mut().retrigger_state = SinkRecoveryRetriggerState::Idle { count: 2 };

    assert_eq!(
        sink_recovery_test_local_step(&runtime, true, false, false, false, false),
        SinkRecoveryStep::WaitForRuntimeScopeConvergence,
        "once the owned post-return retrigger has been spent, a pretriggered deferred helper must keep waiting for the owned source-rescan epoch instead of probing sink readiness early",
    );
}

#[test]
fn sink_recovery_machine_local_require_probe_not_ready_probe_replays_retained_sink_wave_before_manual_fallback()
 {
    let route_key = events_stream_route_for_scope("node-a").0;
    let mut runtime = SinkRecoveryMachine::new_local_pretriggered_with_deadline(
        LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady,
        tokio::time::Instant::now() + Duration::from_secs(1),
        1,
    );
    runtime.local_state_mut().retrigger_state = SinkRecoveryRetriggerState::Idle { count: 2 };

    assert_eq!(
        sink_recovery_test_probe_step(
            &mut runtime,
            SinkRecoveryProbeOutcome::NotReady,
            false,
            false,
            &[SinkControlSignal::Tick {
                unit: SinkRuntimeUnit::Sink,
                route_key: route_key.clone(),
                generation: 2,
                envelope: encode_runtime_unit_tick(&RuntimeUnitTick {
                    route_key,
                    unit_id: execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    generation: 2,
                    at_ms: 1,
                })
                .expect("encode synthetic test sink replay tick"),
            }],
        ),
        SinkRecoveryStep::ReplayRetainedSinkWave,
        "require-probe deferred helpers should replay the retained sink wave after the first not-ready probe before they escalate into later source/manual fallback lanes",
    );
    assert_eq!(
        runtime.local_state().retained_sink_replay_state,
        SinkRecoveryRetainedSinkReplayState::Pending,
        "the retained sink replay lane should become machine-owned pending state once the first not-ready probe lands in require-probe mode",
    );
}

#[test]
fn sink_recovery_machine_local_require_probe_pretriggered_waits_for_expected_group_publication_after_owned_retrigger()
 {
    let runtime = SinkRecoveryMachine::new_local_pretriggered_with_deadline(
        LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady,
        tokio::time::Instant::now() + Duration::from_secs(1),
        1,
    );
    let mut waiting_after_retrigger = runtime;
    waiting_after_retrigger.local_state_mut().retrigger_state =
        SinkRecoveryRetriggerState::Idle { count: 2 };

    assert_eq!(
        sink_recovery_test_local_step(&waiting_after_retrigger, true, false, false, false, false),
        SinkRecoveryStep::WaitForRuntimeScopeConvergence,
        "require-probe deferred helpers must keep waiting until the owned retrigger has published all expected source groups before the first blocking sink probe can run",
    );
}

#[test]
fn sink_recovery_machine_local_require_probe_pretriggered_publication_ready_probes_after_owned_retrigger()
 {
    let runtime = SinkRecoveryMachine::new_local_pretriggered_with_deadline(
        LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady,
        tokio::time::Instant::now() + Duration::from_secs(1),
        1,
    );
    let mut waiting_after_retrigger = runtime;
    waiting_after_retrigger.local_state_mut().retrigger_state =
        SinkRecoveryRetriggerState::Idle { count: 2 };

    assert_eq!(
        sink_recovery_test_local_step(&waiting_after_retrigger, true, true, true, true, false),
        SinkRecoveryStep::ProbeReadiness,
        "once the owned retrigger has published all expected source groups, require-probe helpers can advance into the first blocking sink probe",
    );
}

#[test]
fn sink_recovery_machine_local_require_probe_publication_ready_probes_before_sink_materialization()
{
    let runtime = SinkRecoveryMachine::new_local_pretriggered_with_deadline(
        LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady,
        tokio::time::Instant::now() + Duration::from_secs(1),
        1,
    );
    let mut waiting_after_retrigger = runtime;
    waiting_after_retrigger.local_state_mut().retrigger_state =
        SinkRecoveryRetriggerState::Idle { count: 2 };

    assert_eq!(
        sink_recovery_test_local_step(&waiting_after_retrigger, true, true, true, false, false),
        SinkRecoveryStep::ProbeReadiness,
        "require-probe helpers must trigger the readiness probe and retained sink replay once source publication is satisfied; waiting for sink materialization first deadlocks the repair path",
    );
}

#[test]
fn sink_recovery_machine_local_replays_retained_sink_wave_before_scope_wait() {
    let route_key = events_stream_route_for_scope("node-a").0;
    let runtime = SinkRecoveryMachine::new_local_with_deadline(
        LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );
    let retained_signals = vec![SinkControlSignal::Tick {
        unit: SinkRuntimeUnit::Sink,
        route_key: route_key.clone(),
        generation: 2,
        envelope: encode_runtime_unit_tick(&RuntimeUnitTick {
            route_key,
            unit_id: execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            generation: 2,
            at_ms: 1,
        })
        .expect("encode synthetic test sink replay tick"),
    }];

    assert!(
        runtime.should_replay_retained_sink_before_scope_convergence(false, &retained_signals),
        "retained sink replay must be applied before waiting for sink-side scope convergence; otherwise local sink-status recovery can deadlock on a sink worker that has not yet received the retained route wave",
    );
    assert!(
        !runtime.should_replay_retained_sink_before_scope_convergence(true, &retained_signals),
        "once scope has converged, retained sink replay should stay behind the normal probe path"
    );
}

#[test]
fn sink_recovery_machine_removes_observation_bundle_phase_inputs() {
    let source = include_str!("runtime_app.rs");
    let production_source = source
        .split("#[test]")
        .next()
        .expect("runtime_app.rs test split should yield production prefix");
    assert!(
        !production_source.contains("struct SinkRecoveryObservation")
            && !production_source.contains("fn local_step_for_observation(")
            && !production_source.contains("cached_ready_fast_path_satisfied:")
            && !production_source.contains("source_rescan_request_observed:")
            && !production_source.contains("source_publication_satisfied:")
            && !production_source.contains("enum SinkRecoveryCachedReadiness")
            && !production_source.contains("enum SinkRecoverySourceProgressState")
            && !production_source.contains("fn local_cached_sink_readiness(")
            && !production_source.contains("fn local_source_progress_state("),
        "sink recovery should consume typed source/sink progress inputs directly; old observation-bundle phase inputs must not return",
    );
}

#[test]
fn sink_recovery_machine_post_recovery_waits_for_pretriggered_source_publication_before_accepting_scope_convergence()
 {
    let pretriggered = SinkRecoveryMachine::new_post_recovery(Some(1));
    let expected = RuntimeScopeExpectedGroups {
        source_groups: std::collections::BTreeSet::from([String::from("g1")]),
        scan_groups: std::collections::BTreeSet::from([String::from("g1")]),
        sink_groups: std::collections::BTreeSet::from([String::from("g1")]),
    };
    let converged = RuntimeScopeConvergenceObservation {
        source_groups: expected.source_groups.clone(),
        scan_groups: expected.scan_groups.clone(),
        sink_groups: expected.sink_groups.clone(),
    };

    assert_eq!(
        pretriggered.post_recovery_scope_action(
            &converged,
            &expected,
            tokio::time::Instant::now() + Duration::from_secs(1),
        ),
        SinkRecoveryScopeConvergenceAction::Wait,
        "post-recovery readiness must keep waiting while the owned pretriggered source-rescan request has not yet been observed, even if runtime scope convergence arrives early",
    );
}

#[test]
fn sink_recovery_machine_post_recovery_hands_converged_scope_to_sink_gate_when_pretrigger_epoch_is_lost()
 {
    let pretriggered = SinkRecoveryMachine::new_post_recovery(Some(1));
    let expected = RuntimeScopeExpectedGroups {
        source_groups: std::collections::BTreeSet::from([String::from("g1")]),
        scan_groups: std::collections::BTreeSet::from([String::from("g1")]),
        sink_groups: std::collections::BTreeSet::from([String::from("g1")]),
    };
    let converged = RuntimeScopeConvergenceObservation {
        source_groups: expected.source_groups.clone(),
        scan_groups: expected.scan_groups.clone(),
        sink_groups: expected.sink_groups.clone(),
    };

    assert_eq!(
        pretriggered
            .post_recovery_scope_action(&converged, &expected, tokio::time::Instant::now(),),
        SinkRecoveryScopeConvergenceAction::Converged,
        "if a worker restart loses the request epoch after scope convergence, recovery must continue into the sink readiness gate instead of failing before sink materialization can be retriggered",
    );
}

fn sink_group_status_counts_as_ready(group: &crate::sink::SinkGroupStatusSnapshot) -> bool {
    matches!(
        group.normalized_readiness(),
        crate::sink::GroupReadinessState::Ready
    )
}

fn sink_status_snapshot_has_ready_scheduled_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
) -> bool {
    snapshot.progress_snapshot().has_ready_scheduled_groups()
}

fn sink_group_status_ready_for_republish_completion(
    group: &crate::sink::SinkGroupStatusSnapshot,
    has_stream_group_evidence: bool,
) -> bool {
    group.materialized_service_live_ready()
        || (sink_group_status_counts_as_ready(group)
            && group.live_nodes > 0
            && (group.total_nodes <= group.live_nodes || has_stream_group_evidence))
}

fn sink_status_snapshot_ready_for_expected_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
    expected_groups: &std::collections::BTreeSet<String>,
) -> bool {
    if expected_groups.is_empty() {
        return false;
    }
    let scheduled_groups = snapshot.scheduled_groups();
    if !expected_groups.is_subset(&scheduled_groups) {
        return false;
    }
    let has_stream_group_evidence = snapshot.readiness_summary().has_stream_group_evidence;
    let groups_by_id = snapshot
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<std::collections::BTreeMap<_, _>>();
    expected_groups.iter().all(|group_id| {
        groups_by_id.get(group_id.as_str()).is_some_and(|group| {
            sink_group_status_ready_for_republish_completion(group, has_stream_group_evidence)
        })
    })
}

fn sink_status_snapshot_republish_observed_for_expected_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
    expected_groups: &std::collections::BTreeSet<String>,
) -> bool {
    if expected_groups.is_empty() {
        return false;
    }
    let summary = snapshot.readiness_summary();
    let progress = snapshot.progress_snapshot();
    expected_groups.is_subset(&summary.scheduled_groups)
        && expected_groups.is_subset(&progress.materialized_groups)
        && summary.has_stream_group_evidence
}

fn sink_status_snapshot_ready_for_scheduled_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
) -> bool {
    let scheduled_groups = snapshot.scheduled_groups();
    sink_status_snapshot_ready_for_expected_groups(snapshot, &scheduled_groups)
}

fn sink_status_snapshot_observed_control_generation(
    snapshot: &crate::sink::SinkStatusSnapshot,
    generation: u64,
) -> bool {
    if generation == 0 {
        return false;
    }
    let needle = format!("generation={generation}");
    snapshot
        .last_control_frame_signals_by_node
        .values()
        .flat_map(|signals| signals.iter())
        .any(|signal| signal.contains(&needle))
}

fn current_generation_sink_replay_tick(
    node_id: &str,
    source_signals: &[SourceControlSignal],
    facade_signals: &[FacadeControlSignal],
) -> Option<SinkControlSignal> {
    let generation = source_signals
        .iter()
        .map(source_signal_generation)
        .chain(facade_signals.iter().map(facade_signal_generation))
        .find(|generation| *generation > 0)?;
    let route_key = events_stream_route_for_scope(node_id).0;
    Some(SinkControlSignal::Tick {
        unit: SinkRuntimeUnit::Sink,
        route_key: route_key.clone(),
        generation,
        envelope: encode_runtime_unit_tick(&RuntimeUnitTick {
            route_key,
            unit_id: execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            generation,
            at_ms: 1,
        })
        .expect("encode synthetic sink replay tick"),
    })
}

#[derive(Clone)]
struct PendingFixedBindHandoffRegistrant {
    instance_id: u64,
    api_task: Arc<Mutex<Option<FacadeActivation>>>,
    pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
    pending_fixed_bind_claim_release_followup: Arc<AtomicBool>,
    pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
    retained_active_facade_continuity: Arc<AtomicBool>,
    facade_spawn_in_progress: Arc<Mutex<Option<FacadeSpawnInProgress>>>,
    facade_pending_status: SharedFacadePendingStatusCell,
    facade_service_state: SharedFacadeServiceStateCell,
    rollout_status: SharedRolloutStatusCell,
    api_request_tracker: Arc<ApiRequestTracker>,
    api_control_gate: Arc<ApiControlGate>,
    control_failure_uninitialized: Arc<AtomicBool>,
    runtime_gate_state: Arc<StdMutex<RuntimeControlState>>,
    runtime_state_changed: Arc<tokio::sync::Notify>,
    node_id: NodeId,
    runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    source: Arc<SourceFacade>,
    sink: Arc<SinkFacade>,
    query_sink: Arc<SinkFacade>,
    query_runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
}

#[derive(Clone)]
struct ManagementWriteRecoveryContext {
    instance_id: u64,
    node_id: NodeId,
    source: Arc<SourceFacade>,
    sink: Arc<SinkFacade>,
    api_task: Arc<Mutex<Option<FacadeActivation>>>,
    pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
    facade_pending_status: SharedFacadePendingStatusCell,
    facade_service_state: SharedFacadeServiceStateCell,
    api_control_gate: Arc<ApiControlGate>,
    control_failure_uninitialized: Arc<AtomicBool>,
    runtime_gate_state: Arc<StdMutex<RuntimeControlState>>,
    runtime_state_changed: Arc<tokio::sync::Notify>,
    retained_sink_control_state: Arc<Mutex<RetainedSinkControlState>>,
    source_scoped_sink_observation_repair_signature:
        Arc<StdMutex<Option<SourceScopedSinkObservationRepairSignature>>>,
    pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
    source_generation_cutover_replay_deferred: Arc<AtomicBool>,
    sink_generation_cutover_replay_deferred: Arc<AtomicBool>,
    facade_gate: RuntimeUnitGate,
    source_rescan_proxy_ready_generation: Arc<AtomicU64>,
    runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    runtime_endpoint_tasks: Arc<Mutex<Vec<ManagedEndpointTask>>>,
    control_frame_serial: Arc<Mutex<()>>,
    inflight: Arc<AtomicBool>,
}

type LogicalRootsRepairSignature = Vec<(
    String,
    Option<std::path::PathBuf>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    std::path::PathBuf,
    bool,
    bool,
    Option<u64>,
)>;

#[derive(Clone, Debug, PartialEq, Eq)]
struct SourceScopedSinkObservationRepairSignature {
    signal_parts: Vec<String>,
    local_groups: BTreeSet<String>,
    logical_roots_generation: u64,
    logical_roots: LogicalRootsRepairSignature,
}

struct ManagementWriteRecoveryInflightGuard {
    inflight: Arc<AtomicBool>,
}

impl Drop for ManagementWriteRecoveryInflightGuard {
    fn drop(&mut self) {
        self.inflight.store(false, Ordering::Release);
    }
}

impl ManagementWriteRecoveryContext {
    fn try_begin(&self) -> Option<ManagementWriteRecoveryInflightGuard> {
        self.inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| ManagementWriteRecoveryInflightGuard {
                inflight: self.inflight.clone(),
            })
    }

    async fn try_begin_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Option<ManagementWriteRecoveryInflightGuard> {
        loop {
            if let Some(guard) = self.try_begin() {
                return Some(guard);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return None;
            }
            let remaining = deadline.saturating_duration_since(now);
            tokio::select! {
                _ = self.runtime_state_changed.notified() => {}
                _ = tokio::time::sleep(DEFERRED_SINK_REPAIR_RETRY_INTERVAL.min(remaining)) => {}
            }
        }
    }

    fn runtime_control_state(&self) -> RuntimeControlState {
        RuntimeControlState::from_state_cell(&self.runtime_gate_state)
    }

    fn update_runtime_control_state(&self, update: impl FnOnce(&mut RuntimeControlState)) {
        if let Ok(mut guard) = self.runtime_gate_state.lock() {
            update(&mut guard);
        }
        self.runtime_state_changed.notify_waiters();
    }

    fn clear_source_replay_after_repair(&self) {
        self.update_runtime_control_state(|state| state.clear_source_replay());
        self.source_generation_cutover_replay_deferred
            .store(false, Ordering::Release);
    }

    fn clear_sink_replay_after_repair(&self) {
        self.update_runtime_control_state(|state| state.clear_sink_replay());
        self.sink_generation_cutover_replay_deferred
            .store(false, Ordering::Release);
    }

    async fn app_retained_sink_replay_signals(&self) -> Vec<SinkControlSignal> {
        let retained = self.retained_sink_control_state.lock().await.clone();
        let retained_sink_generation = retained
            .active_by_route
            .values()
            .filter_map(|signal| match signal {
                SinkControlSignal::Activate { generation, .. } => Some(*generation),
                _ => None,
            })
            .max();
        let retained_source_generation = self
            .source
            .control_signals_with_replay(&[])
            .await
            .iter()
            .map(source_signal_generation)
            .max();
        let generation = retained_sink_generation
            .into_iter()
            .chain(retained_source_generation)
            .max();
        let mut replayed = FSMetaApp::sink_signals_from_state(&retained);
        if let Some(generation) = generation {
            replayed = replayed
                .iter()
                .map(|signal| FSMetaApp::rebase_sink_signal_generation(signal, generation))
                .collect();
        }
        replayed
    }

    async fn replay_app_retained_sink_state_if_present(
        &self,
    ) -> std::result::Result<bool, CnxError> {
        let signals = self.app_retained_sink_replay_signals().await;
        if signals.is_empty() {
            return Ok(false);
        }
        #[cfg(test)]
        note_sink_apply_entry_for_tests(self.instance_id);
        self.sink
            .apply_retained_orchestration_signals_with_total_timeout_with_failure(
                &signals,
                SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT,
            )
            .await
            .map_err(SinkFailure::into_error)?;
        Ok(true)
    }

    fn source_scoped_sink_replay_scopes(
        roots: &[source::config::RootSpec],
        grants: &[source::config::GrantedMountRoot],
        groups: &std::collections::BTreeSet<String>,
    ) -> Vec<RuntimeBoundScope> {
        roots
            .iter()
            .filter(|root| groups.contains(&root.id))
            .map(|root| {
                let mut resource_ids = grants
                    .iter()
                    .filter(|grant| grant.active && root.selector.matches(grant))
                    .map(|grant| grant.object_ref.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                if resource_ids.is_empty() {
                    resource_ids.extend(
                        grants
                            .iter()
                            .filter(|grant| root.selector.matches(grant))
                            .map(|grant| grant.object_ref.clone()),
                    );
                }
                RuntimeBoundScope {
                    scope_id: root.id.clone(),
                    resource_ids: resource_ids.into_iter().collect(),
                }
            })
            .collect()
    }

    fn runtime_host_grant_from_source_grant(
        grant: &source::config::GrantedMountRoot,
    ) -> RuntimeHostGrant {
        RuntimeHostGrant {
            object_ref: grant.object_ref.clone(),
            object_type: RuntimeHostObjectType::MountRoot,
            interfaces: grant.interfaces.clone(),
            host: RuntimeHostDescriptor {
                host_ref: grant.host_ref.clone(),
                host_ip: grant.host_ip.clone(),
                host_name: grant.host_name.clone(),
                site: grant.site.clone(),
                zone: grant.zone.clone(),
                host_labels: grant.host_labels.clone(),
            },
            object: RuntimeObjectDescriptor {
                mount_point: grant.mount_point.display().to_string(),
                fs_source: grant.fs_source.clone(),
                fs_type: grant.fs_type.clone(),
                mount_options: grant.mount_options.clone(),
            },
            grant_state: if grant.active {
                RuntimeHostGrantState::Active
            } else {
                RuntimeHostGrantState::Revoked
            },
        }
    }

    fn source_grant_from_runtime_host_grant(
        grant: &RuntimeHostGrant,
    ) -> Option<source::config::GrantedMountRoot> {
        if !matches!(&grant.object_type, RuntimeHostObjectType::MountRoot) {
            return None;
        }
        Some(source::config::GrantedMountRoot {
            object_ref: grant.object_ref.clone(),
            host_ref: grant.host.host_ref.clone(),
            host_ip: grant.host.host_ip.clone(),
            host_name: grant.host.host_name.clone(),
            site: grant.host.site.clone(),
            zone: grant.host.zone.clone(),
            host_labels: grant.host.host_labels.clone(),
            mount_point: std::path::PathBuf::from(&grant.object.mount_point),
            fs_source: grant.object.fs_source.clone(),
            fs_type: grant.object.fs_type.clone(),
            mount_options: grant.object.mount_options.clone(),
            interfaces: grant.interfaces.clone(),
            active: matches!(&grant.grant_state, RuntimeHostGrantState::Active),
        })
    }

    fn sink_roots_control_grants_from_replay_signals(
        replay_signals: &[SinkControlSignal],
    ) -> Option<Vec<source::config::GrantedMountRoot>> {
        let grants = replay_signals
            .iter()
            .filter_map(|signal| match signal {
                SinkControlSignal::RuntimeHostGrantChange { changed, .. } => Some(changed),
                _ => None,
            })
            .flat_map(|changed| changed.grants.iter())
            .filter_map(Self::source_grant_from_runtime_host_grant)
            .collect::<Vec<_>>();
        (!grants.is_empty()).then_some(grants)
    }

    fn sink_roots_control_generation_is_stale(
        generation_cell: &AtomicU64,
        generation: u64,
    ) -> bool {
        generation == 0 || generation <= generation_cell.load(Ordering::Acquire)
    }

    fn sink_roots_control_generation_can_replay_same_declaration(
        generation_cell: &AtomicU64,
        generation: u64,
    ) -> bool {
        generation != 0 && generation == generation_cell.load(Ordering::Acquire)
    }

    fn mark_sink_roots_control_generation(generation_cell: &AtomicU64, generation: u64) {
        if generation == 0 {
            return;
        }
        let mut current = generation_cell.load(Ordering::Acquire);
        while generation > current {
            match generation_cell.compare_exchange(
                current,
                generation,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }

    fn source_roots_control_generation_is_stale(
        generation_cell: &AtomicU64,
        generation: u64,
    ) -> bool {
        generation == 0 || generation <= generation_cell.load(Ordering::Acquire)
    }

    fn mark_source_roots_control_generation(generation_cell: &AtomicU64, generation: u64) {
        if generation == 0 {
            return;
        }
        let mut current = generation_cell.load(Ordering::Acquire);
        while generation > current {
            match generation_cell.compare_exchange(
                current,
                generation,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }

    fn source_scoped_sink_activate_signal(
        route_key: String,
        generation: u64,
        bound_scopes: &[RuntimeBoundScope],
    ) -> std::result::Result<SinkControlSignal, CnxError> {
        Ok(SinkControlSignal::Activate {
            unit: SinkRuntimeUnit::Sink,
            route_key: route_key.clone(),
            generation,
            bound_scopes: bound_scopes.to_vec(),
            envelope: encode_runtime_exec_control(&RuntimeExecControl::Activate(
                RuntimeExecActivate {
                    route_key,
                    unit_id: execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 60_000,
                    bound_scopes: bound_scopes.to_vec(),
                },
            ))?,
        })
    }

    fn source_scoped_sink_replay_owner_node_ids(
        bound_scopes: &[RuntimeBoundScope],
    ) -> BTreeSet<String> {
        bound_scopes
            .iter()
            .flat_map(|scope| scope.resource_ids.iter())
            .filter_map(|resource_id| {
                resource_id
                    .split_once("::")
                    .map(|(node_id, _)| node_id.trim())
                    .filter(|node_id| !node_id.is_empty())
                    .map(ToString::to_string)
            })
            .collect()
    }

    fn source_scoped_sink_replay_scopes_for_owner(
        bound_scopes: &[RuntimeBoundScope],
        owner_node_id: &str,
    ) -> Vec<RuntimeBoundScope> {
        bound_scopes
            .iter()
            .filter_map(|scope| {
                let resource_ids = scope
                    .resource_ids
                    .iter()
                    .filter(|resource_id| {
                        resource_id
                            .split_once("::")
                            .map(|(node_id, _)| node_id.trim() == owner_node_id)
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                (!resource_ids.is_empty()).then(|| RuntimeBoundScope {
                    scope_id: scope.scope_id.clone(),
                    resource_ids,
                })
            })
            .collect()
    }

    fn source_scoped_sink_replay_route_scopes(
        local_node_id: &str,
        bound_scopes: &[RuntimeBoundScope],
    ) -> BTreeMap<String, Vec<RuntimeBoundScope>> {
        let owner_node_ids = Self::source_scoped_sink_replay_owner_node_ids(bound_scopes);
        let local_scopes =
            Self::source_scoped_sink_replay_scopes_for_owner(bound_scopes, local_node_id);
        let local_or_unowned_scopes = if local_scopes.is_empty() && owner_node_ids.is_empty() {
            bound_scopes.to_vec()
        } else {
            local_scopes
        };
        let mut route_scopes = BTreeMap::from([
            (
                format!("{}.stream", ROUTE_KEY_FACADE_CONTROL),
                local_or_unowned_scopes.clone(),
            ),
            (
                format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
                local_or_unowned_scopes.clone(),
            ),
            (
                format!("{}.req", ROUTE_KEY_FORCE_FIND),
                local_or_unowned_scopes,
            ),
        ]);
        for node_id in owner_node_ids {
            let owner_scopes =
                Self::source_scoped_sink_replay_scopes_for_owner(bound_scopes, &node_id);
            if owner_scopes.is_empty() {
                continue;
            }
            route_scopes.insert(
                events_stream_route_for_scope(&node_id).0,
                owner_scopes.clone(),
            );
            route_scopes.insert(
                sink_query_request_route_for(&node_id).0,
                owner_scopes.clone(),
            );
            route_scopes.insert(
                sink_status_request_route_for(&node_id).0,
                owner_scopes.clone(),
            );
            route_scopes.insert(
                sink_roots_control_stream_route_for(&node_id).0,
                owner_scopes,
            );
        }
        route_scopes
    }

    fn source_scoped_sink_replay_roots_control_envelopes_by_route(
        signals: &[SinkControlSignal],
    ) -> BTreeMap<String, Vec<ControlEnvelope>> {
        let shared_envelopes = signals
            .iter()
            .filter_map(|signal| match signal {
                SinkControlSignal::RuntimeHostGrantChange { .. } => Some(signal.envelope()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut owner_scoped = BTreeMap::new();
        let mut owner_activate_envelopes: BTreeMap<String, Vec<ControlEnvelope>> = BTreeMap::new();
        let mut owner_roots_control_routes: BTreeMap<String, String> = BTreeMap::new();
        let mut generic = None;
        let generic_route_key = format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL);
        for signal in signals {
            let SinkControlSignal::Activate {
                route_key,
                bound_scopes,
                ..
            } = signal
            else {
                continue;
            };
            if bound_scopes.is_empty() {
                continue;
            }
            let mut replay_envelopes = shared_envelopes.clone();
            replay_envelopes.push(signal.envelope());
            if is_per_peer_sink_roots_control_stream_route(route_key) {
                let mut matched_owner = false;
                for owner_node_id in Self::source_scoped_sink_replay_owner_node_ids(bound_scopes) {
                    if route_key == &sink_roots_control_stream_route_for(&owner_node_id).0 {
                        owner_activate_envelopes
                            .entry(owner_node_id.clone())
                            .or_default()
                            .push(signal.envelope());
                        owner_roots_control_routes.insert(owner_node_id, route_key.clone());
                        matched_owner = true;
                    }
                }
                if !matched_owner {
                    owner_scoped.insert(route_key.clone(), replay_envelopes);
                }
            } else if route_key == &generic_route_key {
                generic = Some((route_key.clone(), replay_envelopes));
            } else {
                for owner_node_id in Self::source_scoped_sink_replay_owner_node_ids(bound_scopes) {
                    if route_key == &events_stream_route_for_scope(&owner_node_id).0
                        || route_key == &sink_query_request_route_for(&owner_node_id).0
                        || route_key == &sink_status_request_route_for(&owner_node_id).0
                    {
                        owner_activate_envelopes
                            .entry(owner_node_id)
                            .or_default()
                            .push(signal.envelope());
                    }
                }
            }
        }
        for (owner_node_id, route_key) in owner_roots_control_routes {
            let mut replay_envelopes = shared_envelopes.clone();
            replay_envelopes.extend(
                owner_activate_envelopes
                    .remove(&owner_node_id)
                    .unwrap_or_default(),
            );
            owner_scoped.insert(route_key, replay_envelopes);
        }
        if !owner_scoped.is_empty() {
            return owner_scoped;
        }
        generic.into_iter().collect()
    }

    fn source_scoped_sink_replay_groups_from_retained_source_signals(
        retained_source_signals: &[SourceControlSignal],
    ) -> BTreeSet<String> {
        retained_source_signals
            .iter()
            .filter_map(|signal| match signal {
                SourceControlSignal::Activate {
                    unit, bound_scopes, ..
                } if matches!(unit, SourceRuntimeUnit::Source | SourceRuntimeUnit::Scan) => {
                    Some(bound_scopes)
                }
                _ => None,
            })
            .flat_map(|bound_scopes| bound_scopes.iter())
            .filter_map(|scope| {
                let scope_id = scope.scope_id.trim();
                (!scope_id.is_empty()).then(|| scope_id.to_string())
            })
            .collect()
    }

    fn source_scoped_sink_replay_groups_from_source_observation(
        snapshot: &crate::workers::source::SourceObservabilitySnapshot,
    ) -> BTreeSet<String> {
        snapshot
            .scheduled_source_groups_by_node
            .values()
            .chain(snapshot.scheduled_scan_groups_by_node.values())
            .flatten()
            .filter(|group| !group.is_empty())
            .cloned()
            .collect()
    }

    async fn pending_source_observation_for_source_scoped_sink_repair(
        &self,
    ) -> Option<crate::workers::source::SourceObservabilitySnapshot> {
        match tokio::time::timeout(
            MANUAL_RESCAN_SOURCE_STATUS_DEFAULT_PROBE_BUDGET.min(Duration::from_secs(5)),
            self.source
                .source_state_pending_observability_snapshot_for_status_route(),
        )
        .await
        {
            Ok((snapshot, _used_cached_fallback)) => Some(snapshot),
            Err(_) => {
                eprintln!(
                    "fs_meta_runtime_app: source-scoped sink repair pending source observation timed out"
                );
                None
            }
        }
    }

    async fn source_scoped_sink_replay_has_route_evidence(&self) -> bool {
        let retained_source_signals = self.source.control_signals_with_replay(&[]).await;
        if !Self::source_scoped_sink_replay_groups_from_retained_source_signals(
            &retained_source_signals,
        )
        .is_empty()
        {
            return true;
        }
        if self
            .source
            .scheduled_source_group_ids_with_failure()
            .await
            .ok()
            .flatten()
            .is_some_and(|groups| !groups.is_empty())
        {
            return true;
        }
        self.source
            .scheduled_scan_group_ids_with_failure()
            .await
            .ok()
            .flatten()
            .is_some_and(|groups| !groups.is_empty())
            || self
                .pending_source_observation_for_source_scoped_sink_repair()
                .await
                .as_ref()
                .is_some_and(|snapshot| {
                    !Self::source_scoped_sink_replay_groups_from_source_observation(snapshot)
                        .is_empty()
                })
    }

    async fn source_scoped_sink_replay_signals(
        &self,
    ) -> std::result::Result<Vec<SinkControlSignal>, CnxError> {
        let retained_source_signals = self.source.control_signals_with_replay(&[]).await;
        let mut groups = Self::source_scoped_sink_replay_groups_from_retained_source_signals(
            &retained_source_signals,
        );
        match self.source.scheduled_source_group_ids_with_failure().await {
            Ok(Some(scheduled_groups)) => groups.extend(scheduled_groups),
            Ok(None) => {}
            Err(err) if groups.is_empty() => return Err(SourceFailure::into_error(err)),
            Err(err) => {
                eprintln!(
                    "fs_meta_runtime_app: source-scoped sink replay using retained source scopes after scheduled-source probe failed err={}",
                    err.as_error()
                );
            }
        }
        match self.source.scheduled_scan_group_ids_with_failure().await {
            Ok(Some(scheduled_groups)) => groups.extend(scheduled_groups),
            Ok(None) => {}
            Err(err) if groups.is_empty() => return Err(SourceFailure::into_error(err)),
            Err(err) => {
                eprintln!(
                    "fs_meta_runtime_app: source-scoped sink replay using retained source scopes after scheduled-scan probe failed err={}",
                    err.as_error()
                );
            }
        }
        let observed_source_scope = if self.runtime_control_state().source_state_replay_required() {
            self.pending_source_observation_for_source_scoped_sink_repair()
                .await
        } else {
            None
        };
        if let Some(snapshot) = observed_source_scope.as_ref() {
            groups.extend(Self::source_scoped_sink_replay_groups_from_source_observation(snapshot));
        }
        let mut roots = self
            .source
            .logical_roots_snapshot_with_failure()
            .await
            .map_err(SourceFailure::into_error)?;
        let mut grants = self
            .source
            .host_object_grants_snapshot_with_failure()
            .await
            .map_err(SourceFailure::into_error)?;
        if let Some(snapshot) = observed_source_scope {
            if !snapshot.logical_roots.is_empty() {
                roots = snapshot.logical_roots;
            }
            if !snapshot.grants.is_empty() {
                grants = snapshot.grants;
            }
        }
        groups.extend(
            roots
                .iter()
                .filter(|root| root.scan)
                .map(|root| root.id.clone()),
        );
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        let bound_scopes = Self::source_scoped_sink_replay_scopes(&roots, &grants, &groups);
        if bound_scopes.is_empty() {
            return Ok(Vec::new());
        }

        let status_generation = self
            .source
            .status_snapshot_with_failure()
            .await
            .map(|status| {
                status.current_stream_generation.unwrap_or_else(|| {
                    status
                        .concrete_roots
                        .iter()
                        .filter_map(|entry| entry.current_stream_generation)
                        .max()
                        .unwrap_or_default()
                })
            })
            .unwrap_or_default();
        let retained_generation = retained_source_signals
            .iter()
            .map(source_signal_generation)
            .max()
            .unwrap_or_default();
        let generation = status_generation.max(retained_generation).max(1);

        let mut signals = Vec::new();
        if !grants.is_empty() {
            let changed = RuntimeHostGrantChange {
                version: generation,
                grants: grants
                    .iter()
                    .map(Self::runtime_host_grant_from_source_grant)
                    .collect(),
            };
            signals.push(SinkControlSignal::RuntimeHostGrantChange {
                envelope: encode_runtime_host_grant_change(&changed)?,
                changed,
            });
        }

        for (route_key, route_bound_scopes) in
            Self::source_scoped_sink_replay_route_scopes(&self.node_id.0, &bound_scopes)
        {
            signals.push(Self::source_scoped_sink_activate_signal(
                route_key,
                generation,
                &route_bound_scopes,
            )?);
        }
        Ok(signals)
    }

    async fn replay_source_scoped_sink_state_if_present(
        &self,
    ) -> std::result::Result<bool, CnxError> {
        let signals = self.source_scoped_sink_replay_signals().await?;
        if signals.is_empty() {
            return Ok(false);
        }
        let roots = self
            .source
            .logical_roots_snapshot_with_failure()
            .await
            .map_err(SourceFailure::into_error)?;
        let generation = self
            .source
            .logical_roots_generation_with_failure()
            .await
            .map_err(SourceFailure::into_error)?
            .max(1);
        let signature = Self::source_scoped_sink_observation_repair_signature(
            &signals,
            &self.node_id.0,
            &roots,
            generation,
        );
        if self
            .source_scoped_sink_observation_repair_already_applied(&signature)
            .await
        {
            if debug_sink_recovery_capture_enabled() {
                eprintln!(
                    "fs_meta_runtime_app: source-scoped sink observation repair skipped existing signature local_groups={:?}",
                    signature.local_groups
                );
            }
            return Ok(false);
        }
        #[cfg(test)]
        note_sink_apply_entry_for_tests(self.instance_id);
        self.sink
            .apply_retained_orchestration_signals_with_total_timeout_with_failure(
                &signals,
                SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT,
            )
            .await
            .map_err(SinkFailure::into_error)?;
        self.publish_source_scoped_sink_roots_control_replay(&signals, &roots, generation)
            .await?;
        self.record_source_scoped_sink_observation_repair_signature(signature);
        Ok(true)
    }

    fn logical_roots_repair_signature(
        roots: &[source::config::RootSpec],
    ) -> LogicalRootsRepairSignature {
        roots
            .iter()
            .map(|root| {
                (
                    root.id.clone(),
                    root.selector.mount_point.clone(),
                    root.selector.fs_source.clone(),
                    root.selector.fs_type.clone(),
                    root.selector.host_ip.clone(),
                    root.selector.host_ref.clone(),
                    root.subpath_scope.clone(),
                    root.watch,
                    root.scan,
                    root.audit_interval_ms,
                )
            })
            .collect()
    }

    fn source_scoped_sink_observation_repair_signature(
        signals: &[SinkControlSignal],
        local_node_id: &str,
        roots: &[source::config::RootSpec],
        logical_roots_generation: u64,
    ) -> SourceScopedSinkObservationRepairSignature {
        let mut signal_parts = Vec::new();
        let mut local_groups = BTreeSet::new();
        for signal in signals {
            match signal {
                SinkControlSignal::RuntimeHostGrantChange { changed, .. } => {
                    let mut grants = changed
                        .grants
                        .iter()
                        .map(|grant| {
                            format!(
                                "object_ref={};object_type={:?};interfaces={:?};host_ref={};host_ip={:?};host_name={:?};site={:?};zone={:?};host_labels={:?};mount_point={};fs_source={:?};fs_type={:?};mount_options={:?};active={}",
                                grant.object_ref,
                                grant.object_type,
                                grant.interfaces,
                                grant.host.host_ref,
                                grant.host.host_ip,
                                grant.host.host_name,
                                grant.host.site,
                                grant.host.zone,
                                grant.host.host_labels,
                                grant.object.mount_point,
                                grant.object.fs_source,
                                grant.object.fs_type,
                                grant.object.mount_options,
                                matches!(grant.grant_state, RuntimeHostGrantState::Active)
                            )
                        })
                        .collect::<Vec<_>>();
                    grants.sort();
                    signal_parts.push(format!("grants:{}", grants.join(",")));
                }
                SinkControlSignal::Activate {
                    route_key,
                    bound_scopes,
                    ..
                } => {
                    let mut scopes = Vec::new();
                    for scope in bound_scopes {
                        let mut resource_ids = scope.resource_ids.clone();
                        resource_ids.sort();
                        if resource_ids.iter().any(|resource_id| {
                            resource_id
                                .split_once("::")
                                .map(|(node_id, _)| node_id == local_node_id)
                                .unwrap_or(false)
                        }) {
                            local_groups.insert(scope.scope_id.clone());
                        }
                        scopes.push(format!("{}={}", scope.scope_id, resource_ids.join("|")));
                    }
                    scopes.sort();
                    signal_parts.push(format!("activate:{}:{}", route_key, scopes.join(",")));
                }
                SinkControlSignal::Deactivate { .. }
                | SinkControlSignal::Tick { .. }
                | SinkControlSignal::Passthrough(_) => {}
            }
        }
        signal_parts.sort();
        SourceScopedSinkObservationRepairSignature {
            signal_parts,
            local_groups,
            logical_roots_generation,
            logical_roots: Self::logical_roots_repair_signature(roots),
        }
    }

    async fn source_scoped_sink_observation_repair_already_applied(
        &self,
        signature: &SourceScopedSinkObservationRepairSignature,
    ) -> bool {
        self.source_scoped_sink_observation_repair_signature
            .lock()
            .map(|guard| guard.as_ref() == Some(signature))
            .unwrap_or(false)
    }

    fn record_source_scoped_sink_observation_repair_signature(
        &self,
        signature: SourceScopedSinkObservationRepairSignature,
    ) {
        if let Ok(mut guard) = self.source_scoped_sink_observation_repair_signature.lock() {
            *guard = Some(signature);
        }
    }

    async fn publish_source_scoped_sink_roots_control_replay(
        &self,
        signals: &[SinkControlSignal],
        roots: &[source::config::RootSpec],
        generation: u64,
    ) -> std::result::Result<bool, CnxError> {
        let Some(boundary) = self.runtime_boundary.clone() else {
            return Ok(false);
        };
        let replay_by_route =
            Self::source_scoped_sink_replay_roots_control_envelopes_by_route(signals);
        if replay_by_route.is_empty() {
            return Ok(false);
        }
        for attempt in 1..=SOURCE_SCOPED_SINK_ROOTS_CONTROL_REPLAY_ATTEMPTS {
            if attempt > 1 {
                tokio::time::sleep(SOURCE_SCOPED_SINK_ROOTS_CONTROL_REPLAY_RETRY_INTERVAL).await;
            }
            for (route_key, replay_envelopes) in &replay_by_route {
                let payload = encode_logical_roots_control_payload_with_generation_and_sink_replay(
                    &roots,
                    generation,
                    replay_envelopes.clone(),
                )?;
                eprintln!(
                    "fs_meta_runtime_app: source-scoped sink roots-control replay send begin route={} roots={} generation={} attempt={}/{}",
                    route_key,
                    roots.len(),
                    generation,
                    attempt,
                    SOURCE_SCOPED_SINK_ROOTS_CONTROL_REPLAY_ATTEMPTS
                );
                boundary
                    .channel_send(
                        BoundaryContext::default(),
                        ChannelSendRequest {
                            channel_key: ChannelKey(route_key.clone()),
                            events: vec![Event::new(
                                EventMetadata {
                                    origin_id: self.node_id.clone(),
                                    timestamp_us: now_us(),
                                    logical_ts: None,
                                    correlation_id: None,
                                    ingress_auth: None,
                                    trace: None,
                                },
                                bytes::Bytes::from(payload),
                            )],
                            timeout_ms: Some(Duration::from_secs(5).as_millis() as u64),
                        },
                    )
                    .await?;
                eprintln!(
                    "fs_meta_runtime_app: source-scoped sink roots-control replay send ok route={} roots={} generation={} attempt={}/{}",
                    route_key,
                    roots.len(),
                    generation,
                    attempt,
                    SOURCE_SCOPED_SINK_ROOTS_CONTROL_REPLAY_ATTEMPTS
                );
            }
        }
        Ok(true)
    }

    async fn retained_sink_replay_signals_for_local_republish(&self) -> Vec<SinkControlSignal> {
        let retained_sink_generation = self
            .retained_sink_control_state
            .lock()
            .await
            .active_by_route
            .values()
            .filter_map(|signal| match signal {
                SinkControlSignal::Activate { generation, .. } => Some(*generation),
                _ => None,
            })
            .max();
        let retained_source_generation = self
            .source
            .control_signals_with_replay(&[])
            .await
            .iter()
            .map(source_signal_generation)
            .max();
        let generation = retained_sink_generation
            .into_iter()
            .chain(retained_source_generation)
            .max();
        let Some(generation) = generation else {
            return Vec::new();
        };
        let route_key = events_stream_route_for_scope(&self.node_id.0).0;
        let tick = SinkControlSignal::Tick {
            unit: SinkRuntimeUnit::Sink,
            route_key: route_key.clone(),
            generation,
            envelope: encode_runtime_unit_tick(&RuntimeUnitTick {
                route_key,
                unit_id: execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                generation,
                at_ms: 1,
            })
            .expect("encode deferred repair sink-status replay tick"),
        };
        let mut replayed = self.app_retained_sink_replay_signals().await;
        replayed.push(tick);
        replayed
    }

    async fn wait_for_sink_replay_readiness_after_repair(
        &self,
    ) -> std::result::Result<(), CnxError> {
        let expected_groups = SinkRecoveryMachine::new_post_recovery(None)
            .run_post_recovery_readiness(&self.source, &self.sink, &self.runtime_state_changed)
            .await
            .map_err(RuntimeWorkerObservationFailure::into_error)?;
        if let Some(expected_groups) = expected_groups {
            let post_return_sink_replay_signals = self
                .retained_sink_replay_signals_for_local_republish()
                .await;
            FSMetaApp::wait_for_local_sink_status_republish_after_recovery_requiring_probe_from_parts(
                self.source.clone(),
                self.sink.clone(),
                self.runtime_state_changed.clone(),
                &expected_groups,
                &post_return_sink_replay_signals,
            )
            .await?;
        }
        Ok(())
    }

    async fn should_replay_source_scoped_sink_state_after_app_retained_replay(
        &self,
    ) -> std::result::Result<bool, CnxError> {
        if self.sink.retained_replay_required() {
            return Ok(false);
        }
        let scope_observation =
            FSMetaApp::observe_runtime_scope_convergence(&self.source, &self.sink)
                .await
                .map_err(RuntimeWorkerObservationFailure::into_error)?;
        let logical_roots = self
            .source
            .logical_roots_snapshot_with_failure()
            .await
            .map_err(SourceFailure::into_error)?;
        let expected_scope_groups =
            scope_observation.expected_groups_for_logical_roots(&logical_roots);
        Ok(!expected_scope_groups.sink_groups.is_empty()
            && !scope_observation.matches_node_local_source_scope(&expected_scope_groups)
            && self.source_scoped_sink_replay_has_route_evidence().await)
    }

    fn restore_control_after_retained_replay_recovery(&self) {
        self.restore_control_after_retained_replay_recovery_with_entry_replay(false);
    }

    fn restore_control_after_retained_replay_recovery_with_entry_replay(
        &self,
        entry_replay_required: bool,
    ) {
        let state_after_repair = self.runtime_control_state();
        let recovery_can_reopen_control = state_after_repair.control_initialized()
            || self.control_failure_uninitialized.load(Ordering::Acquire)
            || entry_replay_required;
        if state_after_repair.replay_fully_cleared() && recovery_can_reopen_control {
            self.update_runtime_control_state(|state| state.mark_initialized());
            self.control_failure_uninitialized
                .store(false, Ordering::Release);
        }
    }

    async fn publish_recovered_facade_state(&self) {
        let observation = FSMetaApp::observe_facade_gate_from_parts(
            self.instance_id,
            self.api_task.clone(),
            self.pending_facade.clone(),
            Some(&self.facade_pending_status),
            self.runtime_control_state(),
            self.pending_fixed_bind_has_suppressed_dependent_routes
                .load(Ordering::Acquire),
            FacadeOnlyHandoffObservationPolicy::ForceBlocked,
        )
        .await;
        let publication_ready = matches!(
            observation.runtime.facade_publication_readiness_decision(
                observation.current_pending.is_some(),
                observation.pending_facade_is_control_route,
                observation.active_control_stream_present,
                observation.active_pending_control_stream_present,
                observation.allow_facade_only_handoff,
            ),
            FacadePublicationReadinessDecision::Ready
        );
        let snapshot =
            FSMetaApp::fixed_bind_gate_publication_snapshot(observation, publication_ready);
        FSMetaApp::publish_facade_publication_snapshot(
            &self.facade_service_state,
            &self.api_control_gate,
            snapshot,
        );
    }

    async fn source_repair_ready_from_runtime_or_observation(&self) -> bool {
        if self.runtime_control_state().source_repair_ready() {
            return true;
        }
        let probe_budget =
            MANUAL_RESCAN_SOURCE_STATUS_DEFAULT_PROBE_BUDGET.min(Duration::from_secs(5));
        if self.runtime_control_state().source_state_replay_required() {
            return self
                .source_repair_ready_from_pending_source_observation(probe_budget)
                .await;
        }
        if self.source.retained_replay_required().await {
            return self
                .source_repair_ready_from_pending_source_observation(probe_budget)
                .await;
        }
        match tokio::time::timeout(
            probe_budget,
            self.source.observability_snapshot_with_failure(),
        )
        .await
        {
            Ok(Ok(snapshot)) => {
                FSMetaApp::source_observation_supports_source_repair_ready(&snapshot)
            }
            Ok(Err(err)) => {
                eprintln!(
                    "fs_meta_runtime_app: source repair gate observation failed err={}",
                    err.as_error()
                );
                false
            }
            Err(_) => {
                eprintln!("fs_meta_runtime_app: source repair gate observation timed out");
                false
            }
        }
    }

    async fn source_repair_ready_from_pending_source_observation(
        &self,
        probe_budget: Duration,
    ) -> bool {
        match tokio::time::timeout(
            probe_budget,
            self.source
                .source_state_pending_observability_snapshot_for_status_route(),
        )
        .await
        {
            Ok((snapshot, _used_cached_fallback)) => {
                FSMetaApp::source_observation_supports_source_repair_ready(&snapshot)
            }
            Err(_) => {
                eprintln!("fs_meta_runtime_app: source repair gate observation timed out");
                false
            }
        }
    }

    async fn publish_source_repair_gate_after_source_state_current(&self) {
        let source_repair_ready = self.source_repair_ready_from_runtime_or_observation().await;
        self.api_control_gate.set_ready_state_with_source_repair(
            self.api_control_gate.is_ready(),
            self.api_control_gate.is_management_write_ready(),
            source_repair_ready,
        );
    }

    async fn repair_source_replay_if_required(&self) -> std::result::Result<(), CnxError> {
        if self.runtime_control_state().source_state_replay_required() {
            if !self.source.retained_replay_required().await {
                self.clear_source_replay_after_repair();
            } else {
                self.source
                    .replay_retained_control_for_source_repair_with_failure()
                    .await
                    .map_err(SourceFailure::into_error)?;
                if !self.source.retained_replay_required().await {
                    self.clear_source_replay_after_repair();
                }
            }
        }
        Ok(())
    }

    async fn repair_worker_source_replay_if_retained_after_app_replay_cleared(
        &self,
    ) -> std::result::Result<(), CnxError> {
        if self.runtime_control_state().source_state_replay_required()
            || !self.source.retained_replay_required().await
        {
            return Ok(());
        }
        self.source
            .replay_retained_control_for_source_repair_with_failure()
            .await
            .map_err(SourceFailure::into_error)?;
        if !self.source.retained_replay_required().await {
            self.clear_source_replay_after_repair();
        }
        Ok(())
    }

    async fn ensure_source_rescan_proxy_ready_after_source_repair(&self) {
        let runtime_state = self.runtime_control_state();
        if runtime_state.source_control_apply_inflight() || !runtime_state.control_initialized() {
            return;
        }
        let Some(boundary) = self.runtime_boundary.clone() else {
            return;
        };
        let route_key = source_rescan_request_route_for(&self.node_id.0).0;
        let deadline =
            tokio::time::Instant::now() + MANUAL_RESCAN_SOURCE_STATUS_DEFAULT_PROBE_BUDGET;
        let context = self.clone();
        let source_repair_recovery: ManagementWriteRecovery = Arc::new(move || {
            let context = context.clone();
            Box::pin(async move { context.run_source_repair_without_proxy_ready().await })
        });
        let ready = ensure_worker_source_rescan_proxy_ready_until(
            boundary,
            self.source.clone(),
            self.node_id.clone(),
            &self.facade_gate,
            &route_key,
            self.source_rescan_proxy_ready_generation.clone(),
            &self.runtime_endpoint_tasks,
            source_repair_recovery,
            deadline,
            true,
        )
        .await;
        if !ready {
            self.source_rescan_proxy_ready_generation
                .store(0, Ordering::Release);
        }
    }

    async fn run_source_repair_without_proxy_ready(&self) -> std::result::Result<(), CnxError> {
        if !self.runtime_control_state().source_state_replay_required() {
            if self.source.retained_replay_required().await {
                let Some(_inflight) = self.try_begin() else {
                    return Ok(());
                };
                let _serial = self.control_frame_serial.lock().await;
                self.repair_worker_source_replay_if_retained_after_app_replay_cleared()
                    .await?;
                self.restore_control_after_retained_replay_recovery();
                self.publish_recovered_facade_state().await;
                self.publish_source_repair_gate_after_source_state_current()
                    .await;
                return Ok(());
            }
            if !self.api_control_gate.is_source_repair_ready() {
                self.publish_recovered_facade_state().await;
                self.publish_source_repair_gate_after_source_state_current()
                    .await;
            }
            return Ok(());
        }
        let Some(_inflight) = self.try_begin() else {
            self.publish_source_repair_gate_after_source_state_current()
                .await;
            return Ok(());
        };
        let _serial = self.control_frame_serial.lock().await;
        self.repair_source_replay_if_required().await?;
        self.restore_control_after_retained_replay_recovery();
        self.publish_recovered_facade_state().await;
        self.publish_source_repair_gate_after_source_state_current()
            .await;
        Ok(())
    }

    async fn repair_sink_replay_if_required(&self) -> std::result::Result<(), CnxError> {
        if self.runtime_control_state().sink_state_replay_required() {
            if self.replay_app_retained_sink_state_if_present().await? {
                if !self.sink.retained_replay_required() {
                    if self
                        .should_replay_source_scoped_sink_state_after_app_retained_replay()
                        .await?
                    {
                        self.replay_source_scoped_sink_state_if_present().await?;
                    }
                    self.wait_for_sink_replay_readiness_after_repair().await?;
                    self.clear_sink_replay_after_repair();
                }
            } else if !self.sink.retained_replay_required()
                && self.replay_source_scoped_sink_state_if_present().await?
            {
                self.wait_for_sink_replay_readiness_after_repair().await?;
                self.clear_sink_replay_after_repair();
            } else if !self.sink.retained_replay_required() {
                self.clear_sink_replay_after_repair();
            } else {
                self.sink
                    .status_snapshot_with_failure()
                    .await
                    .map_err(SinkFailure::into_error)?;
                if !self.sink.retained_replay_required() {
                    self.wait_for_sink_replay_readiness_after_repair().await?;
                    self.clear_sink_replay_after_repair();
                }
            }
        }
        Ok(())
    }

    async fn run(&self) -> std::result::Result<(), CnxError> {
        let Some(_inflight) = self.try_begin() else {
            return Ok(());
        };
        let _serial = self.control_frame_serial.lock().await;
        let state_at_entry = self.runtime_control_state();
        if state_at_entry.control_gate_ready(false) {
            return Ok(());
        }
        if !state_at_entry.control_initialized()
            && state_at_entry.replay_fully_cleared()
            && !self.control_failure_uninitialized.load(Ordering::Acquire)
        {
            return Ok(());
        }

        if state_at_entry.source_state_replay_required() {
            self.repair_source_replay_if_required().await?;
            self.ensure_source_rescan_proxy_ready_after_source_repair()
                .await;
            if !self.runtime_control_state().source_state_replay_required()
                && self.runtime_control_state().sink_state_replay_required()
            {
                self.publish_recovered_facade_state().await;
                self.publish_source_repair_gate_after_source_state_current()
                    .await;
            }
        }

        self.repair_sink_replay_if_required().await?;

        self.restore_control_after_retained_replay_recovery_with_entry_replay(
            state_at_entry.source_state_replay_required()
                || state_at_entry.sink_state_replay_required(),
        );
        self.publish_recovered_facade_state().await;
        Ok(())
    }

    async fn run_source_repair(&self) -> std::result::Result<(), CnxError> {
        self.run_source_repair_without_proxy_ready().await?;
        self.ensure_source_rescan_proxy_ready_after_source_repair()
            .await;
        Ok(())
    }

    async fn run_sink_repair(&self) -> std::result::Result<(), CnxError> {
        let state_at_entry = self.runtime_control_state();
        if !state_at_entry.sink_state_replay_required() {
            return Ok(());
        }
        if state_at_entry.source_state_replay_required() {
            return Ok(());
        }
        let Some(_inflight) = self.try_begin() else {
            return Ok(());
        };
        let wait_for_readiness_after_replay = {
            let _serial = self.control_frame_serial.lock().await;
            let state_after_serial = self.runtime_control_state();
            if !state_after_serial.sink_state_replay_required() {
                return Ok(());
            }
            if state_after_serial.source_state_replay_required() {
                return Ok(());
            }

            if self.replay_app_retained_sink_state_if_present().await? {
                if !self.sink.retained_replay_required()
                    && self
                        .should_replay_source_scoped_sink_state_after_app_retained_replay()
                        .await?
                {
                    self.replay_source_scoped_sink_state_if_present().await?;
                }
                !self.sink.retained_replay_required()
            } else if !self.sink.retained_replay_required()
                && self.replay_source_scoped_sink_state_if_present().await?
            {
                true
            } else if !self.sink.retained_replay_required() {
                self.clear_sink_replay_after_repair();
                false
            } else {
                self.sink
                    .status_snapshot_with_failure()
                    .await
                    .map_err(SinkFailure::into_error)?;
                !self.sink.retained_replay_required()
            }
        };

        if wait_for_readiness_after_replay {
            self.wait_for_sink_replay_readiness_after_repair().await?;
            let _serial = self.control_frame_serial.lock().await;
            let state_after_wait = self.runtime_control_state();
            if state_after_wait.sink_state_replay_required()
                && !state_after_wait.source_state_replay_required()
                && !self.sink.retained_replay_required()
            {
                self.clear_sink_replay_after_repair();
            }
        }

        self.restore_control_after_retained_replay_recovery();
        self.publish_recovered_facade_state().await;
        Ok(())
    }

    async fn run_source_scoped_sink_observation_repair(&self) -> std::result::Result<(), CnxError> {
        let source_replay_required_at_entry =
            self.runtime_control_state().source_state_replay_required();
        let source_scoped_sink_route_evidence_at_entry = source_replay_required_at_entry
            && !self.api_control_gate.is_source_repair_ready()
            && self.source_scoped_sink_replay_has_route_evidence().await;
        let source_repair_ready_at_entry = !source_replay_required_at_entry
            || self.api_control_gate.is_source_repair_ready()
            || source_scoped_sink_route_evidence_at_entry
            || self.source_repair_ready_from_runtime_or_observation().await;
        if !source_repair_ready_at_entry {
            return Ok(());
        }
        let sink_repair_deadline =
            tokio::time::Instant::now() + SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT;
        let Some(_inflight) = self.try_begin_until(sink_repair_deadline).await else {
            return Ok(());
        };
        let replayed_sink_state = {
            let _serial = self.control_frame_serial.lock().await;
            if self.runtime_control_state().source_state_replay_required()
                && !self.api_control_gate.is_source_repair_ready()
                && !source_repair_ready_at_entry
                && !self.source_scoped_sink_replay_has_route_evidence().await
            {
                return Ok(());
            }
            self.replay_source_scoped_sink_state_if_present().await?
        };

        if replayed_sink_state {
            self.wait_for_sink_replay_readiness_after_repair().await?;
        }
        self.restore_control_after_retained_replay_recovery();
        self.publish_recovered_facade_state().await;
        Ok(())
    }
}

#[derive(Clone)]
struct ActiveFixedBindFacadeRegistrant {
    instance_id: u64,
    api_task: Arc<Mutex<Option<FacadeActivation>>>,
    api_request_tracker: Arc<ApiRequestTracker>,
    api_control_gate: Arc<ApiControlGate>,
    control_failure_uninitialized: Arc<AtomicBool>,
    retained_active_facade_continuity: Arc<AtomicBool>,
}

impl PendingFixedBindHandoffRegistrant {
    fn runtime_control_state(&self) -> RuntimeControlState {
        RuntimeControlState::from_state_cell(&self.runtime_gate_state)
    }

    fn notify_runtime_state_changed(&self) {
        self.runtime_state_changed.notify_waiters();
    }

    async fn release_handoff_blocker_for_publication(
        &self,
        pending: &PendingFacadeActivation,
    ) -> Option<PendingFixedBindHandoffContinuation> {
        let bind_addr = pending.resolved.bind_addr.clone();
        mark_pending_fixed_bind_handoff_ready_with_registrant(&bind_addr, self.clone());
        let fixed_bind = FSMetaApp::build_fixed_bind_snapshot_for_pending_publication(
            self.instance_id,
            self.api_task.clone(),
            Some(pending.clone()),
            self.pending_fixed_bind_claim_release_followup
                .load(Ordering::Acquire),
        )
        .await;
        let active_owner = active_fixed_bind_facade_owner_for(&bind_addr, self.instance_id);
        match FSMetaApp::pending_fixed_bind_handoff_attempt_disposition(
            PendingFixedBindHandoffAttemptDecisionInput {
                claim_conflict: fixed_bind.conflicting_process_claim.is_some(),
                pending_runtime_exposure_confirmed: pending.runtime_exposure_confirmed,
                active_owner_present: active_owner.is_some(),
                active_owner_failed_control_uninitialized: active_owner.as_ref().is_some_and(
                    |owner| owner.control_failure_uninitialized.load(Ordering::Acquire),
                ),
                conflicting_process_claim_owner_instance_id: fixed_bind
                    .conflicting_process_claim
                    .as_ref()
                    .map(|claim| claim.owner_instance_id),
            },
        ) {
            PendingFixedBindHandoffAttemptDisposition::NoAttempt => None,
            PendingFixedBindHandoffAttemptDisposition::ReleaseActiveOwner => {
                let owner = active_owner
                    .expect("active fixed-bind handoff disposition must retain an active owner");
                eprintln!(
                    "fs_meta_runtime_app: release active fixed-bind owner for pending successor bind_addr={} owner_failed_uninitialized={}",
                    bind_addr,
                    owner.control_failure_uninitialized.load(Ordering::Acquire)
                );
                let release_mode = if owner.control_failure_uninitialized.load(Ordering::Acquire) {
                    FixedBindOwnerReleaseMode::FailedOwnerCutover
                } else {
                    FixedBindOwnerReleaseMode::GracefulHandoff
                };
                owner.release_for_handoff(&bind_addr, release_mode).await;
                Some(PendingFixedBindHandoffContinuation {
                    bind_addr,
                    registrant: self.clone(),
                })
            }
            PendingFixedBindHandoffAttemptDisposition::ReleaseConflictingProcessClaim {
                owner_instance_id,
            } => {
                clear_process_facade_claim_for_bind_addr(&bind_addr, owner_instance_id);
                Some(PendingFixedBindHandoffContinuation {
                    bind_addr,
                    registrant: self.clone(),
                })
            }
        }
    }

    async fn try_spawn_pending_facade(&self) -> Result<bool> {
        self.try_spawn_pending_facade_with_mode(PendingFacadeSpawnMode::Normal)
            .await
    }

    async fn try_spawn_pending_facade_after_owner_release(&self) -> Result<bool> {
        self.try_spawn_pending_facade_with_mode(PendingFacadeSpawnMode::AfterFixedBindOwnerRelease)
            .await
    }

    async fn try_spawn_pending_facade_with_mode(
        &self,
        spawn_mode: PendingFacadeSpawnMode,
    ) -> Result<bool> {
        FSMetaApp::try_spawn_pending_facade_from_registrant_with_spawn(
            self.clone(),
            spawn_mode,
            |resolved,
             node_id,
             runtime_boundary,
             source,
             sink,
             query_sink,
             query_runtime_boundary,
             facade_pending_status,
             facade_service_state,
             rollout_status,
             api_request_tracker,
             api_control_gate| async move {
                api::spawn_with_rollout_status(
                    resolved,
                    node_id,
                    runtime_boundary,
                    source,
                    sink,
                    query_sink,
                    query_runtime_boundary,
                    facade_pending_status,
                    facade_service_state,
                    rollout_status,
                    api_request_tracker,
                    api_control_gate,
                )
                .await
            },
        )
        .await
    }

    async fn apply_forced_handoff_ready_tail(&self, bind_addr: &str) -> bool {
        let observation = FSMetaApp::observe_facade_gate_from_parts(
            self.instance_id,
            self.api_task.clone(),
            self.pending_facade.clone(),
            Some(&self.facade_pending_status),
            self.runtime_control_state(),
            self.pending_fixed_bind_has_suppressed_dependent_routes
                .load(Ordering::Acquire),
            FacadeOnlyHandoffObservationPolicy::ForceAllowed,
        )
        .await;
        let applied = matches!(
            observation.runtime.facade_publication_readiness_decision(
                observation.current_pending.is_some(),
                observation.pending_facade_is_control_route,
                observation.active_control_stream_present,
                observation.active_pending_control_stream_present,
                observation.allow_facade_only_handoff,
            ),
            FacadePublicationReadinessDecision::Ready
        );
        if applied {
            mark_active_fixed_bind_facade_owner(
                bind_addr,
                ActiveFixedBindFacadeRegistrant {
                    instance_id: self.instance_id,
                    api_task: self.api_task.clone(),
                    api_request_tracker: self.api_request_tracker.clone(),
                    api_control_gate: self.api_control_gate.clone(),
                    control_failure_uninitialized: self.control_failure_uninitialized.clone(),
                    retained_active_facade_continuity: self
                        .retained_active_facade_continuity
                        .clone(),
                },
            );
        }
        let snapshot = FSMetaApp::fixed_bind_gate_publication_snapshot(observation, applied);
        FSMetaApp::publish_facade_publication_snapshot(
            &self.facade_service_state,
            &self.api_control_gate,
            snapshot,
        );
        self.notify_runtime_state_changed();
        applied
    }

    async fn release_handoff_for_active_owner(
        &self,
        bind_addr: &str,
        current_instance_id: u64,
    ) -> Option<PendingFixedBindHandoffContinuation> {
        if self.instance_id == current_instance_id {
            return None;
        }
        let pending = self.pending_facade.lock().await.clone()?;
        if pending.route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL) {
            return None;
        }
        if pending.runtime_exposure_confirmed {
            Some(PendingFixedBindHandoffContinuation {
                bind_addr: bind_addr.to_string(),
                registrant: self.clone(),
            })
        } else {
            None
        }
    }
}

impl ActiveFixedBindFacadeRegistrant {
    async fn release_for_handoff(self, bind_addr: &str, release_mode: FixedBindOwnerReleaseMode) {
        self.api_control_gate.close_management_write_gate();
        self.api_request_tracker.wait_for_drain().await;
        if release_mode.drains_predecessor_facade_reads() {
            self.api_control_gate.wait_for_facade_request_drain().await;
        }
        #[cfg(test)]
        maybe_pause_facade_shutdown_started().await;
        if let Some(current) = self.api_task.lock().await.take() {
            self.api_request_tracker.wait_for_drain().await;
            if release_mode.drains_predecessor_facade_reads() {
                self.api_control_gate.wait_for_facade_request_drain().await;
            }
            current
                .handle
                .shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT)
                .await;
        }
        clear_owned_process_facade_claim(self.instance_id);
        clear_process_facade_claim_for_retired_fixed_bind(bind_addr);
        clear_active_fixed_bind_facade_owner(bind_addr, self.instance_id);
        self.retained_active_facade_continuity
            .store(false, Ordering::Release);
    }
}

#[derive(Clone)]
#[cfg_attr(not(test), allow(dead_code))]
struct PendingFacadeActivation {
    route_key: String,
    generation: u64,
    resource_ids: Vec<String>,
    bound_scopes: Vec<RuntimeBoundScope>,
    group_ids: Vec<String>,
    runtime_managed: bool,
    runtime_exposure_confirmed: bool,
    resolved: api::config::ResolvedApiConfig,
}

impl PendingFacadeActivation {}

fn default_runtime_worker_binding(
    role_id: &str,
    mode: WorkerMode,
    module_path: Option<&std::path::Path>,
) -> RuntimeWorkerBinding {
    RuntimeWorkerBinding {
        role_id: role_id.to_string(),
        mode,
        launcher_kind: match mode {
            WorkerMode::Embedded => RuntimeWorkerLauncherKind::Embedded,
            WorkerMode::External => RuntimeWorkerLauncherKind::WorkerHost,
        },
        module_path: match mode {
            WorkerMode::Embedded => None,
            WorkerMode::External => module_path.map(std::path::Path::to_path_buf),
        },
        socket_dir: None,
    }
}

fn local_runtime_worker_binding(role_id: &str) -> RuntimeWorkerBinding {
    default_runtime_worker_binding(role_id, WorkerMode::Embedded, None)
}

fn required_runtime_worker_binding(
    bindings: &RuntimeWorkerBindings,
    role_id: &str,
) -> Result<RuntimeWorkerBinding> {
    bindings.roles.get(role_id).cloned().ok_or_else(|| {
        CnxError::InvalidInput(format!(
            "compiled runtime worker bindings must declare role '{role_id}'"
        ))
    })
}

fn runtime_worker_client_bindings(
    bindings: &RuntimeWorkerBindings,
) -> Result<(RuntimeWorkerBinding, RuntimeWorkerBinding)> {
    let facade = required_runtime_worker_binding(bindings, "facade")?;
    if facade.mode != WorkerMode::Embedded {
        return Err(CnxError::InvalidInput(
            "runtime worker binding for 'facade' must remain embedded".into(),
        ));
    }
    let source = required_runtime_worker_binding(bindings, "source")?;
    let sink = required_runtime_worker_binding(bindings, "sink")?;
    Ok((source, sink))
}

pub(crate) fn shared_tokio_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        let worker_threads = std::thread::available_parallelism()
            .map(|n| n.get().clamp(1, 2))
            .unwrap_or(1);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("fs-meta-shared-runtime")
            .enable_all()
            .build()
            .expect("build shared fs-meta tokio runtime")
    })
}

pub(crate) fn block_on_shared_runtime<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T> + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(_) => std::thread::scope(|scope| {
            let join = scope.spawn(|| shared_tokio_runtime().block_on(fut));
            join.join()
                .expect("shared fs-meta runtime helper thread must not panic")
        }),
        Err(_) => shared_tokio_runtime().block_on(fut),
    }
}

fn debug_source_status_lifecycle_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_SOURCE_STATUS_LIFECYCLE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn debug_force_find_runner_capture_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_FORCE_FIND_RUNNER_CAPTURE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn debug_sink_query_route_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_SINK_QUERY_ROUTE_TRACE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn debug_control_frame_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_CONTROL_FRAMES")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
            || std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
    })
}

fn debug_status_endpoint_response_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_STATUS_ENDPOINT_RESPONSE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
            || std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
    })
}

fn next_source_status_endpoint_trace_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

struct SourceStatusEndpointTraceGuard {
    route: String,
    correlation: Option<u64>,
    trace_id: u64,
    phase: &'static str,
    completed: bool,
}

impl SourceStatusEndpointTraceGuard {
    fn new(route: String, correlation: Option<u64>, trace_id: u64, phase: &'static str) -> Self {
        Self {
            route,
            correlation,
            trace_id,
            phase,
            completed: false,
        }
    }

    fn phase(&mut self, phase: &'static str) {
        self.phase = phase;
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for SourceStatusEndpointTraceGuard {
    fn drop(&mut self) {
        if debug_source_status_lifecycle_enabled() && !self.completed {
            eprintln!(
                "fs_meta_runtime_app: source status endpoint dropped route={} correlation={:?} trace_id={} phase={}",
                self.route, self.correlation, self.trace_id, self.phase
            );
        }
    }
}

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_micros() as u64,
        Err(_) => 0,
    }
}

fn summarize_groups_by_node(groups: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    groups
        .iter()
        .map(|(node_id, groups)| format!("{node_id}={}", groups.join("|")))
        .collect()
}

fn summarize_group_string_map(groups: &BTreeMap<String, String>) -> Vec<String> {
    groups
        .iter()
        .map(|(group_id, value)| format!("{group_id}={value}"))
        .collect()
}

fn summarize_counts_by_node(counts: &BTreeMap<String, u64>) -> Vec<String> {
    counts
        .iter()
        .map(|(node_id, count)| format!("{node_id}={count}"))
        .collect()
}

fn summarize_sink_groups(groups: &[crate::sink::SinkGroupStatusSnapshot]) -> Vec<String> {
    groups
        .iter()
        .map(|group| {
            format!(
                "{}:live={} total={} readiness={:?} rev={}",
                group.group_id,
                group.live_nodes,
                group.total_nodes,
                group.normalized_readiness(),
                group.materialized_revision
            )
        })
        .collect()
}

fn summarize_sink_status_endpoint(snapshot: &crate::sink::SinkStatusSnapshot) -> String {
    format!(
        "groups={} group_details={:?} scheduled={:?} received_batches={:?} received_events={:?} received_origin_counts={:?} stream_path_capture_target={:?} stream_received_batches={:?} stream_received_events={:?} stream_received_origin_counts={:?} stream_received_path_origin_counts={:?} stream_ready_origin_counts={:?} stream_ready_path_origin_counts={:?} stream_deferred_origin_counts={:?} stream_dropped_origin_counts={:?} stream_applied_batches={:?} stream_applied_events={:?} stream_applied_control_events={:?} stream_applied_data_events={:?} stream_applied_origin_counts={:?} stream_applied_path_origin_counts={:?} stream_last_applied_at_us={:?}",
        snapshot.groups.len(),
        summarize_sink_groups(&snapshot.groups),
        summarize_groups_by_node(&snapshot.scheduled_groups_by_node),
        summarize_counts_by_node(&snapshot.received_batches_by_node),
        summarize_counts_by_node(&snapshot.received_events_by_node),
        summarize_groups_by_node(&snapshot.received_origin_counts_by_node),
        snapshot.stream_path_capture_target,
        summarize_counts_by_node(&snapshot.stream_received_batches_by_node),
        summarize_counts_by_node(&snapshot.stream_received_events_by_node),
        summarize_groups_by_node(&snapshot.stream_received_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_received_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_ready_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_ready_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_deferred_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_dropped_origin_counts_by_node),
        summarize_counts_by_node(&snapshot.stream_applied_batches_by_node),
        summarize_counts_by_node(&snapshot.stream_applied_events_by_node),
        summarize_counts_by_node(&snapshot.stream_applied_control_events_by_node),
        summarize_counts_by_node(&snapshot.stream_applied_data_events_by_node),
        summarize_groups_by_node(&snapshot.stream_applied_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_applied_path_origin_counts_by_node),
        summarize_counts_by_node(&snapshot.stream_last_applied_at_us_by_node),
    )
}

#[cfg(test)]
fn summarize_source_observability_endpoint(
    snapshot: &crate::workers::source::SourceObservabilitySnapshot,
) -> String {
    format!(
        "scheduled_source_groups={:?} scheduled_scan_groups={:?} published_batches={:?} published_events={:?} published_control_events={:?} published_data_events={:?} last_published_at_us={:?} published_origin_counts={:?} published_path_origin_counts={:?}",
        summarize_groups_by_node(&snapshot.scheduled_source_groups_by_node),
        summarize_groups_by_node(&snapshot.scheduled_scan_groups_by_node),
        summarize_counts_by_node(&snapshot.published_batches_by_node),
        summarize_counts_by_node(&snapshot.published_events_by_node),
        summarize_counts_by_node(&snapshot.published_control_events_by_node),
        summarize_counts_by_node(&snapshot.published_data_events_by_node),
        summarize_counts_by_node(&snapshot.last_published_at_us_by_node),
        summarize_groups_by_node(&snapshot.published_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.published_path_origin_counts_by_node),
    )
}

fn should_emit_selected_group_empty_materialized_reply(
    _node_id: &NodeId,
    source_primary_by_group: &BTreeMap<String, String>,
    request: &InternalQueryRequest,
) -> bool {
    let Some(group_id) = request.scope.selected_group.as_deref() else {
        return false;
    };
    // Selected-group proxy requests must settle explicitly even on non-owner
    // peers; returning no reply here can leave fanout batches unresolved.
    source_primary_by_group.contains_key(group_id)
}

fn selected_group_empty_materialized_reply(
    request: &InternalQueryRequest,
    correlation_id: Option<u64>,
) -> Result<Option<Event>> {
    let Some(group_id) = request.scope.selected_group.as_ref() else {
        return Ok(None);
    };
    let payload = match request.op {
        crate::query::QueryOp::Tree => {
            rmp_serde::to_vec_named(&MaterializedQueryPayload::Tree(TreeGroupPayload {
                reliability: GroupReliability::from_reason(Some(
                    crate::shared_types::query::UnreliableReason::Unattested,
                )),
                stability: TreeStability::not_evaluated(),
                root: TreePageRoot {
                    path: request.scope.path.clone(),
                    size: 0,
                    modified_time_us: 0,
                    is_dir: true,
                    exists: false,
                    has_children: false,
                },
                entries: Vec::new(),
            }))
            .map_err(|err| CnxError::Internal(format!("encode empty tree payload failed: {err}")))?
        }
        crate::query::QueryOp::Stats => {
            rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats::default()))
                .map_err(|err| {
                    CnxError::Internal(format!("encode empty stats payload failed: {err}"))
                })?
        }
    };
    Ok(Some(Event::new(
        EventMetadata {
            origin_id: NodeId(group_id.clone()),
            timestamp_us: now_us(),
            logical_ts: None,
            correlation_id,
            ingress_auth: None,
            trace: None,
        },
        bytes::Bytes::from(payload),
    )))
}

fn sink_query_proxy_error_reply(err: &CnxError, correlation_id: Option<u64>) -> Event {
    Event::new(
        EventMetadata {
            origin_id: NodeId("sink-query-proxy".to_string()),
            timestamp_us: now_us(),
            logical_ts: None,
            correlation_id,
            ingress_auth: None,
            trace: None,
        },
        bytes::Bytes::from(err.to_string()),
    )
}

fn sink_query_reply_event_from_materialized_event(
    event: &Event,
    correlation_id: Option<u64>,
) -> Event {
    let mut meta = event.metadata().clone();
    meta.correlation_id = correlation_id;
    meta.trace = None;
    Event::new(meta, bytes::Bytes::copy_from_slice(event.payload_bytes()))
}

fn explicit_empty_sink_status_reply(
    origin_id: &NodeId,
    correlation_id: Option<u64>,
) -> Result<Event> {
    let snapshot = crate::sink::SinkStatusSnapshot::default();
    sink_status_reply(&snapshot, origin_id, correlation_id)
}

fn sink_status_reply(
    snapshot: &crate::sink::SinkStatusSnapshot,
    origin_id: &NodeId,
    correlation_id: Option<u64>,
) -> Result<Event> {
    let payload = rmp_serde::to_vec_named(snapshot)
        .map_err(|err| CnxError::Internal(format!("encode sink-status payload failed: {err}")))?;
    Ok(Event::new(
        EventMetadata {
            origin_id: origin_id.clone(),
            timestamp_us: now_us(),
            logical_ts: None,
            correlation_id,
            ingress_auth: None,
            trace: None,
        },
        bytes::Bytes::from(payload),
    ))
}

fn selected_group_bridge_eligible_from_sink_status(
    request: &InternalQueryRequest,
    snapshot: &crate::sink::SinkStatusSnapshot,
) -> bool {
    let Some(selected_group) = request.scope.selected_group.as_ref() else {
        return true;
    };
    selected_group_bridge_state_from_sink_status(request, snapshot, selected_group).eligible()
}

fn scheduled_sink_owner_node_for_group(
    snapshot: &crate::sink::SinkStatusSnapshot,
    group_id: &str,
) -> Option<NodeId> {
    if let Some(node_id) = snapshot
        .primary_host_ref_by_group
        .get(group_id)
        .map(|host_ref| host_ref.trim())
        .filter(|host_ref| !host_ref.is_empty())
        .map(|host_ref| NodeId(host_ref.to_string()))
    {
        return Some(node_id);
    }

    let mut scheduled_nodes = snapshot
        .scheduled_groups_by_node
        .iter()
        .filter(|(_, groups)| groups.iter().any(|group| group == group_id))
        .map(|(node_id, _)| NodeId(node_id.clone()))
        .collect::<Vec<_>>();
    scheduled_nodes.sort_by(|a, b| a.0.cmp(&b.0));
    scheduled_nodes.dedup_by(|a, b| a.0 == b.0);

    scheduled_nodes.into_iter().next()
}

fn selected_group_bridge_group_readiness_rank(
    request: &InternalQueryRequest,
    group: &crate::sink::SinkGroupStatusSnapshot,
) -> u8 {
    let trusted_root_tree_request = request.op == crate::query::QueryOp::Tree
        && request.scope.path.as_slice() == b"/"
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    if trusted_root_tree_request
        && sink_group_status_counts_as_ready(group)
        && group.total_nodes > 0
    {
        2
    } else if group.live_nodes > 0 {
        1
    } else {
        0
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SelectedGroupBridgeState {
    readiness_rank: u8,
    owner_node: Option<NodeId>,
}

impl SelectedGroupBridgeState {
    fn eligible(&self) -> bool {
        self.readiness_rank > 0
    }
}

fn selected_group_bridge_state_from_sink_status(
    request: &InternalQueryRequest,
    snapshot: &crate::sink::SinkStatusSnapshot,
    selected_group: &str,
) -> SelectedGroupBridgeState {
    let readiness_rank = snapshot
        .groups
        .iter()
        .find(|group| group.group_id == selected_group)
        .map(|group| selected_group_bridge_group_readiness_rank(request, group))
        .unwrap_or(0);
    let owner_node = scheduled_sink_owner_node_for_group(snapshot, selected_group);
    SelectedGroupBridgeState {
        readiness_rank,
        owner_node,
    }
}

fn selected_group_sink_query_bridge_snapshot<'a>(
    request: &InternalQueryRequest,
    live_snapshot: Option<&'a crate::sink::SinkStatusSnapshot>,
    cached_snapshot: Option<&'a crate::sink::SinkStatusSnapshot>,
) -> Option<&'a crate::sink::SinkStatusSnapshot> {
    let selected_group = request.scope.selected_group.as_deref();
    let live_eligible = live_snapshot.and_then(|snapshot| {
        selected_group.map_or(
            Some((snapshot, SelectedGroupBridgeState::default())),
            |group_id| {
                let state =
                    selected_group_bridge_state_from_sink_status(request, snapshot, group_id);
                state.eligible().then_some((snapshot, state))
            },
        )
    });
    let cached_eligible = cached_snapshot.and_then(|snapshot| {
        selected_group.map_or(
            Some((snapshot, SelectedGroupBridgeState::default())),
            |group_id| {
                let state =
                    selected_group_bridge_state_from_sink_status(request, snapshot, group_id);
                state.eligible().then_some((snapshot, state))
            },
        )
    });

    match (live_eligible, cached_eligible) {
        (Some((live, live_state)), Some((cached, cached_state))) if selected_group.is_some() => {
            if cached_state.readiness_rank > live_state.readiness_rank {
                Some(cached)
            } else {
                Some(live)
            }
        }
        (Some((live, _)), _) => Some(live),
        (_, Some((cached, _))) => Some(cached),
        _ => live_snapshot.or(cached_snapshot),
    }
}

fn selected_group_sink_query_bridge_bindings(
    request: &InternalQueryRequest,
    snapshot: Option<&crate::sink::SinkStatusSnapshot>,
) -> Arc<capanix_host_adapter_fs::PostBindDispatchTable> {
    let Some(selected_group) = request.scope.selected_group.as_deref() else {
        return default_route_bindings();
    };
    let Some(snapshot) = snapshot else {
        return default_route_bindings();
    };
    let state = selected_group_bridge_state_from_sink_status(request, snapshot, selected_group);
    if let Some(owner_node) = state.owner_node {
        return sink_query_route_bindings_for(&owner_node.0);
    }
    default_route_bindings()
}

fn selected_group_tree_payload_has_materialized_data(
    request: &InternalQueryRequest,
    payload: &TreeGroupPayload,
) -> bool {
    let trusted_root_tree_request = request.op == crate::query::QueryOp::Tree
        && request.scope.path.as_slice() == b"/"
        && request.scope.recursive
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    if trusted_root_tree_request {
        payload.root.exists && (payload.root.has_children || !payload.entries.is_empty())
    } else {
        payload.root.exists || payload.root.has_children || !payload.entries.is_empty()
    }
}

fn selected_group_payload_has_materialized_data(
    request: &InternalQueryRequest,
    payload: &MaterializedQueryPayload,
) -> bool {
    match payload {
        MaterializedQueryPayload::Tree(payload) => {
            selected_group_tree_payload_has_materialized_data(request, payload)
        }
        MaterializedQueryPayload::Stats(_) => false,
    }
}

fn selected_group_tree_payload_is_proxy_empty_placeholder(
    request: &InternalQueryRequest,
    payload: &TreeGroupPayload,
) -> bool {
    payload.root.path == request.scope.path
        && !payload.root.exists
        && !payload.root.has_children
        && payload.entries.is_empty()
        && payload.root.modified_time_us == 0
        && payload.reliability.unreliable_reason
            == Some(crate::shared_types::query::UnreliableReason::Unattested)
}

fn should_bridge_selected_group_sink_query(
    request: &InternalQueryRequest,
    local_events: &[Event],
    local_selected_group_bridge_eligible: bool,
) -> bool {
    if request.op != crate::query::QueryOp::Tree || request.scope.selected_group.is_none() {
        return false;
    }
    let selected_group = request
        .scope
        .selected_group
        .as_deref()
        .expect("selected_group must be present for bridge decision");
    let trusted_tree_request = request.op == crate::query::QueryOp::Tree
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    let trusted_non_root_unbounded_tree_request = trusted_tree_request
        && request.scope.path.as_slice() != b"/"
        && request.scope.recursive
        && request.scope.max_depth.is_none();
    let selected_group_tree_has_materialized_data = local_events.iter().any(|event| {
        event.metadata().origin_id.0 == selected_group
            && matches!(
                rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
                Ok(payload) if selected_group_payload_has_materialized_data(request, &payload)
            )
    });
    if selected_group_tree_has_materialized_data {
        return false;
    }
    let has_selected_group_tree_payload = local_events.iter().any(|event| {
        event.metadata().origin_id.0 == selected_group
            && matches!(
                rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
                Ok(MaterializedQueryPayload::Tree(_))
            )
    });
    if has_selected_group_tree_payload && trusted_non_root_unbounded_tree_request {
        return false;
    }
    if has_selected_group_tree_payload
        && !trusted_tree_request
        && local_selected_group_bridge_eligible
    {
        return false;
    }
    let has_materialized_tree_data = local_events.iter().any(|event| {
        matches!(
            rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
            Ok(payload) if selected_group_payload_has_materialized_data(request, &payload)
        )
    });
    if !local_selected_group_bridge_eligible {
        return !selected_group_tree_has_materialized_data && !has_materialized_tree_data;
    }
    !has_materialized_tree_data
}

fn selected_group_sink_query_bridge_decision_without_status(
    request: &InternalQueryRequest,
    local_events: &[Event],
) -> Option<bool> {
    if request.op != crate::query::QueryOp::Tree || request.scope.selected_group.is_none() {
        return Some(false);
    }
    let selected_group = request
        .scope
        .selected_group
        .as_deref()
        .expect("selected_group must be present for bridge decision");
    let trusted_tree_request = request.op == crate::query::QueryOp::Tree
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    let trusted_non_root_unbounded_tree_request = trusted_tree_request
        && request.scope.path.as_slice() != b"/"
        && request.scope.recursive
        && request.scope.max_depth.is_none();
    let selected_group_tree_has_materialized_data = local_events.iter().any(|event| {
        event.metadata().origin_id.0 == selected_group
            && matches!(
                rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
                Ok(payload) if selected_group_payload_has_materialized_data(request, &payload)
            )
    });
    if selected_group_tree_has_materialized_data {
        return Some(false);
    }
    let has_selected_group_tree_payload = local_events.iter().any(|event| {
        event.metadata().origin_id.0 == selected_group
            && matches!(
                rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
                Ok(MaterializedQueryPayload::Tree(_))
            )
    });
    if has_selected_group_tree_payload && trusted_non_root_unbounded_tree_request {
        return Some(false);
    }
    if has_selected_group_tree_payload && !trusted_tree_request {
        return None;
    }
    let has_materialized_tree_data = local_events.iter().any(|event| {
        matches!(
            rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
            Ok(payload) if selected_group_payload_has_materialized_data(request, &payload)
        )
    });
    if has_materialized_tree_data {
        return Some(false);
    }
    None
}

fn discard_selected_group_empty_tree_payloads_shadowed_by_data(
    request: &InternalQueryRequest,
    events: &mut Vec<Event>,
) {
    if request.op != crate::query::QueryOp::Tree {
        return;
    }
    let Some(selected_group) = request.scope.selected_group.as_deref() else {
        return;
    };
    let has_selected_group_data = events.iter().any(|event| {
        event.metadata().origin_id.0 == selected_group
            && matches!(
                rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
                Ok(MaterializedQueryPayload::Tree(payload))
                    if payload.root.path == request.scope.path
                        && selected_group_tree_payload_has_materialized_data(request, &payload)
            )
    });
    if !has_selected_group_data {
        return;
    }
    events.retain(|event| {
        if event.metadata().origin_id.0 != selected_group {
            return true;
        }
        !matches!(
            rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
            Ok(MaterializedQueryPayload::Tree(payload))
                if selected_group_tree_payload_is_proxy_empty_placeholder(request, &payload)
        )
    });
}

fn should_fail_closed_selected_group_empty_after_bridge_failure(
    request: &InternalQueryRequest,
    local_events: &[Event],
    local_selected_group_bridge_eligible: bool,
    err: &CnxError,
) -> bool {
    let trusted_tree_request = request.op == crate::query::QueryOp::Tree
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    let trusted_root_tree_request =
        trusted_tree_request && request.scope.path.as_slice() == b"/" && request.scope.recursive;
    let trusted_non_root_bounded_tree_request = trusted_tree_request
        && request.scope.path.as_slice() != b"/"
        && (!request.scope.recursive || request.scope.max_depth.is_some());
    let trusted_continuity_sensitive_tree_request =
        trusted_root_tree_request || trusted_non_root_bounded_tree_request;
    let continuity_gap = if trusted_tree_request
        && request.scope.path.as_slice() != b"/"
        && !request.scope.recursive
    {
        matches!(err, CnxError::ProtocolViolation(_))
    } else {
        matches!(
            err,
            CnxError::Timeout | CnxError::TransportClosed(_) | CnxError::ProtocolViolation(_)
        )
    };
    continuity_gap
        && trusted_continuity_sensitive_tree_request
        && should_bridge_selected_group_sink_query(
            request,
            local_events,
            local_selected_group_bridge_eligible,
        )
        && !local_events.iter().any(|event| {
            matches!(
                rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
                Ok(payload) if selected_group_payload_has_materialized_data(request, &payload)
            )
        })
}

fn selected_group_empty_materialized_reply_after_bridge_failure(
    request: &InternalQueryRequest,
    local_events: &[Event],
    local_selected_group_bridge_eligible: bool,
    err: &CnxError,
    correlation_id: Option<u64>,
) -> Result<Option<Event>> {
    let Some(selected_group) = request.scope.selected_group.as_deref() else {
        return Ok(None);
    };
    let trusted_root_tree_request = request.op == crate::query::QueryOp::Tree
        && request.scope.path.as_slice() == b"/"
        && request.scope.recursive
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    let trusted_non_root_bounded_tree_request = request.op == crate::query::QueryOp::Tree
        && request.scope.path.as_slice() != b"/"
        && request.scope.recursive
        && request.scope.max_depth.is_some()
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    if !(trusted_root_tree_request || trusted_non_root_bounded_tree_request)
        || !should_fail_closed_selected_group_empty_after_bridge_failure(
            request,
            local_events,
            local_selected_group_bridge_eligible,
            err,
        )
    {
        return Ok(None);
    }
    if trusted_root_tree_request {
        return selected_group_empty_materialized_reply(request, correlation_id);
    }
    let has_selected_group_explicit_empty_tree_payload = local_events.iter().any(|event| {
        event.metadata().origin_id.0 == selected_group
            && matches!(
                rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
                Ok(MaterializedQueryPayload::Tree(payload))
                    if payload.root.path == request.scope.path
                        && !payload.root.exists
                        && !payload.root.has_children
                        && payload.entries.is_empty()
            )
    });
    if !has_selected_group_explicit_empty_tree_payload {
        return Ok(None);
    }
    selected_group_empty_materialized_reply(request, correlation_id)
}

// Bridge from query-peer proxy to internal sink query must stay best-effort.
// Keep this timeout short so proxy requests are never pinned behind an
// unavailable internal sink-query route.
const SINK_QUERY_PROXY_BRIDGE_TIMEOUT: Duration = Duration::from_millis(750);
const SINK_QUERY_PROXY_BRIDGE_IDLE_GRACE: Duration = Duration::from_millis(150);

fn facade_route_key_matches(unit: FacadeRuntimeUnit, route_key: &str) -> bool {
    match unit {
        FacadeRuntimeUnit::Facade => route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL),
        FacadeRuntimeUnit::Query => {
            route_key == format!("{}.req", ROUTE_KEY_QUERY)
                || is_materialized_query_request_route(route_key)
                || is_sink_status_query_request_route(route_key)
                || is_source_status_request_route(route_key)
        }
        FacadeRuntimeUnit::QueryPeer => {
            is_materialized_query_request_route(route_key)
                || is_sink_status_query_request_route(route_key)
                || is_source_status_request_route(route_key)
        }
    }
}

fn preferred_internal_query_endpoint_units(
    query_active: bool,
    query_peer_active: bool,
    prefer_query_peer_first: bool,
) -> Vec<&'static str> {
    match (query_active, query_peer_active, prefer_query_peer_first) {
        (true, true, true) => vec![
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            execution_units::QUERY_RUNTIME_UNIT_ID,
        ],
        (true, true, false) => vec![
            execution_units::QUERY_RUNTIME_UNIT_ID,
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
        ],
        (true, false, _) => vec![execution_units::QUERY_RUNTIME_UNIT_ID],
        (false, true, _) => vec![
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            execution_units::QUERY_RUNTIME_UNIT_ID,
        ],
        (false, false, _) => Vec::new(),
    }
}

fn is_dual_lane_internal_query_route(route_key: &str) -> bool {
    is_materialized_query_request_route(route_key)
        || is_sink_status_query_request_route(route_key)
        || is_source_status_request_route(route_key)
}

fn internal_query_route_still_active(facade_gate: &RuntimeUnitGate, route_key: &str) -> bool {
    let query_active = facade_gate
        .route_active(execution_units::QUERY_RUNTIME_UNIT_ID, route_key)
        .unwrap_or(false);
    let query_peer_active = facade_gate
        .route_active(execution_units::QUERY_PEER_RUNTIME_UNIT_ID, route_key)
        .unwrap_or(false);
    query_active || query_peer_active
}

fn route_still_active_for_units(
    facade_gate: &RuntimeUnitGate,
    route_key: &str,
    unit_ids: &[&'static str],
) -> bool {
    unit_ids.iter().any(|unit_id| {
        facade_gate
            .route_active(unit_id, route_key)
            .unwrap_or(false)
    })
}

fn is_direct_sink_query_request_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_SINK_QUERY_INTERNAL)
        || is_per_peer_sink_query_request_route(route_key)
}

fn sink_query_route_active_for_sink_unit(facade_gate: &RuntimeUnitGate, route_key: &str) -> bool {
    facade_gate
        .route_active(execution_units::SINK_RUNTIME_UNIT_ID, route_key)
        .unwrap_or(false)
        || (is_direct_sink_query_request_route(route_key)
            && facade_gate
                .route_active(execution_units::SINK_RUNTIME_UNIT_ID, ROUTE_KEY_QUERY)
                .unwrap_or(false))
}

fn worker_sink_query_route_still_active_for_units(
    facade_gate: &RuntimeUnitGate,
    route_key: &str,
    unit_ids: &[&'static str],
) -> bool {
    unit_ids.iter().any(|unit_id| {
        if *unit_id == execution_units::SINK_RUNTIME_UNIT_ID {
            sink_query_route_active_for_sink_unit(facade_gate, route_key)
        } else {
            facade_gate
                .route_active(unit_id, route_key)
                .unwrap_or(false)
        }
    })
}

fn runtime_endpoint_task_route_still_active(
    facade_gate: &RuntimeUnitGate,
    task: &ManagedEndpointTask,
) -> bool {
    if task.is_shutdown_requested() || task.finish_reason().is_some() {
        return false;
    }
    if is_source_status_request_route(task.route_key()) {
        let source_active = facade_gate
            .unit_state(execution_units::SOURCE_RUNTIME_UNIT_ID)
            .ok()
            .flatten()
            .map(|(active, _)| active)
            .unwrap_or(false);
        if !source_active {
            return false;
        }
        return task.unit_ids().iter().any(|unit_id| {
            facade_gate
                .route_active(unit_id.as_str(), task.route_key())
                .unwrap_or(false)
        });
    }
    if !task.unit_ids().is_empty() {
        if task
            .unit_ids()
            .iter()
            .any(|unit_id| unit_id.as_str() == execution_units::SINK_RUNTIME_UNIT_ID)
            && sink_query_route_active_for_sink_unit(facade_gate, task.route_key())
        {
            return true;
        }
        return task.unit_ids().iter().any(|unit_id| {
            facade_gate
                .route_active(unit_id.as_str(), task.route_key())
                .unwrap_or(false)
        });
    }
    let public_query_route = format!("{}.req", ROUTE_KEY_QUERY);
    task.route_key() == public_query_route
        && facade_gate
            .route_active(execution_units::QUERY_RUNTIME_UNIT_ID, task.route_key())
            .unwrap_or(false)
}

fn source_rescan_route_active_for_current_generation(
    facade_gate: &RuntimeUnitGate,
    route_key: &str,
) -> bool {
    let Ok(true) = facade_gate.route_active(execution_units::SOURCE_RUNTIME_UNIT_ID, route_key)
    else {
        return false;
    };
    let Ok(Some(current_generation)) =
        facade_gate.route_generation(execution_units::SOURCE_RUNTIME_UNIT_ID, route_key)
    else {
        return false;
    };
    current_generation != 0
}

fn source_rescan_proxy_allowed_for_runtime_state(
    source: &Arc<SourceFacade>,
    runtime_state: RuntimeControlState,
) -> bool {
    matches!(&**source, SourceFacade::Worker(_))
        && runtime_state.control_initialized()
        && !runtime_state.source_control_apply_inflight()
}

fn source_rescan_route_groups_for_current_generation(
    facade_gate: &RuntimeUnitGate,
    route_key: &str,
) -> Option<(u64, BTreeSet<String>)> {
    let (generation, bound_scopes) = facade_gate
        .active_route_state(execution_units::SOURCE_RUNTIME_UNIT_ID, route_key)
        .ok()
        .flatten()?;
    if generation == 0 {
        return None;
    }
    let groups = bound_scopes
        .iter()
        .filter_map(|scope| {
            let scope_id = scope.scope_id.trim();
            (!scope_id.is_empty() && !scope_id.starts_with("__fsmeta_empty_roots_bootstrap"))
                .then(|| scope_id.to_string())
        })
        .collect::<BTreeSet<_>>();
    if groups.is_empty() {
        None
    } else {
        Some((generation, groups))
    }
}

fn runtime_node_identity_matches(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    left.strip_prefix(right)
        .is_some_and(|suffix| suffix.starts_with('-'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('-'))
}

fn runtime_resource_id_targets_node(resource_id: &str, node_id: &NodeId) -> bool {
    let resource_id = resource_id.trim();
    if resource_id.is_empty() {
        return false;
    }
    runtime_node_identity_matches(resource_id, &node_id.0)
        || resource_id
            .split_once("::")
            .is_some_and(|(host_ref, _)| runtime_node_identity_matches(host_ref.trim(), &node_id.0))
}

fn source_rescan_route_targets_node(
    facade_gate: &RuntimeUnitGate,
    route_key: &str,
    node_id: &NodeId,
) -> bool {
    let Some((_generation, bound_scopes)) = facade_gate
        .active_route_state(execution_units::SOURCE_RUNTIME_UNIT_ID, route_key)
        .ok()
        .flatten()
    else {
        return true;
    };
    let mut has_resource_id = false;
    for scope in &bound_scopes {
        for resource_id in &scope.resource_ids {
            if !resource_id.trim().is_empty() {
                has_resource_id = true;
            }
            if runtime_resource_id_targets_node(resource_id, node_id) {
                return true;
            }
        }
    }
    !has_resource_id
}

fn source_rescan_route_semantic_generation(facade_gate: &RuntimeUnitGate, route_key: &str) -> u64 {
    facade_gate
        .route_semantic_generation(execution_units::SOURCE_RUNTIME_UNIT_ID, route_key)
        .ok()
        .flatten()
        .unwrap_or(0)
}

async fn worker_source_rescan_proxy_task_ready_for_current_generation(
    facade_gate: &RuntimeUnitGate,
    route_key: &str,
    ready_generation: &AtomicU64,
    runtime_endpoint_tasks: &Mutex<Vec<ManagedEndpointTask>>,
    allow_unmanaged_local_route: bool,
) -> bool {
    let route_active = source_rescan_route_active_for_current_generation(facade_gate, route_key);
    if !route_active && !allow_unmanaged_local_route {
        ready_generation.store(0, Ordering::Release);
        return false;
    }
    let current_generation = source_rescan_route_semantic_generation(facade_gate, route_key);
    let tasks = runtime_endpoint_tasks.lock().await;
    let task_ready = tasks.iter().any(|task| {
        let route_usable = if route_active {
            runtime_endpoint_task_route_still_active(facade_gate, task)
        } else {
            allow_unmanaged_local_route
        };
        task.route_key() == route_key
            && task
                .route_generation()
                .is_none_or(|generation| generation == current_generation)
            && !task.is_finished()
            && task.finish_reason().is_none()
            && !task.is_shutdown_requested()
            && route_usable
            && task.is_receive_polling()
    });
    drop(tasks);
    if task_ready {
        ready_generation.store(current_generation, Ordering::Release);
        true
    } else {
        ready_generation.store(0, Ordering::Release);
        false
    }
}

fn spawn_worker_source_rescan_proxy_endpoint(
    boundary: Arc<dyn ChannelIoSubset>,
    source: Arc<SourceFacade>,
    node_id: NodeId,
    source_rescan_route_key: String,
    facade_gate: RuntimeUnitGate,
    ready_generation: Arc<AtomicU64>,
    source_repair_recovery: ManagementWriteRecovery,
) -> ManagedEndpointTask {
    let source_rescan_route = RouteKey(source_rescan_route_key.clone());
    let route_generation =
        source_rescan_route_semantic_generation(&facade_gate, &source_rescan_route_key);
    let boundary_identity = boundary.clone();
    let source_rescan_boundary = Arc::new(SourceRescanProxyReceiveReadyBoundary::new(
        boundary,
        source_rescan_route_key.clone(),
        facade_gate,
        ready_generation,
    ));
    eprintln!(
        "fs_meta_runtime_app: spawning source rescan proxy endpoint route={}",
        source_rescan_route_key
    );
    ManagedEndpointTask::spawn_with_unit_wait_receive_poll(
        source_rescan_boundary,
        source_rescan_route,
        format!(
            "app:{}:{}:scoped",
            ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN
        ),
        execution_units::SOURCE_RUNTIME_UNIT_ID,
        tokio_util::sync::CancellationToken::new(),
        move |requests| {
            let source = source.clone();
            let node_id = node_id.clone();
            let source_repair_recovery = source_repair_recovery.clone();
            async move {
                if requests.is_empty() {
                    return Vec::new();
                }
                let scoped_target_acceptance_timeout = requests.iter().find_map(|request| {
                    manual_rescan_scoped_target_acceptance_timeout_from_payload(
                        request.payload_bytes(),
                    )
                });
                let delivery_result = submit_worker_source_rescan_proxy_request_with_repair(
                    source,
                    source_repair_recovery,
                    scoped_target_acceptance_timeout,
                )
                .await;
                let mut responses = Vec::with_capacity(requests.len());
                for req in requests {
                    match &delivery_result {
                        Ok(_) => responses.push(Event::new(
                            EventMetadata {
                                origin_id: node_id.clone(),
                                timestamp_us: now_us(),
                                logical_ts: None,
                                correlation_id: req.metadata().correlation_id,
                                ingress_auth: None,
                                trace: None,
                            },
                            bytes::Bytes::from_static(b"accepted"),
                        )),
                        Err(err) => {
                            let payload = if source_rescan_delivery_error_is_pending(err.as_error())
                            {
                                bytes::Bytes::from(format!("pending: {}", err.as_error()))
                            } else {
                                bytes::Bytes::from(err.as_error().to_string())
                            };
                            eprintln!(
                                "fs_meta_runtime_app: source rescan proxy failed node={} err={}",
                                node_id.0,
                                err.as_error()
                            );
                            responses.push(Event::new(
                                EventMetadata {
                                    origin_id: node_id.clone(),
                                    timestamp_us: now_us(),
                                    logical_ts: None,
                                    correlation_id: req.metadata().correlation_id,
                                    ingress_auth: None,
                                    trace: None,
                                },
                                payload,
                            ));
                        }
                    }
                }
                responses
            }
        },
    )
    .with_route_generation(route_generation)
    .with_boundary_identity(&boundary_identity)
}

async fn submit_worker_source_rescan_proxy_request_with_repair(
    source: Arc<SourceFacade>,
    source_repair_recovery: ManagementWriteRecovery,
    acceptance_timeout: Option<Duration>,
) -> std::result::Result<u64, SourceFailure> {
    let budget = acceptance_timeout.unwrap_or(MANUAL_RESCAN_SOURCE_STATUS_DEFAULT_PROBE_BUDGET);
    let deadline = tokio::time::Instant::now()
        .checked_add(budget)
        .unwrap_or_else(tokio::time::Instant::now);
    let remaining = || deadline.saturating_duration_since(tokio::time::Instant::now());
    let first_budget = remaining();
    if first_budget.is_zero() {
        return Err(SourceFailure::from(CnxError::Timeout));
    }
    let first_result = source
        .submit_targeted_rescan_request_epoch_with_acceptance_timeout_with_failure(Some(
            first_budget,
        ))
        .await;
    let first_err = match first_result {
        Ok(epoch) => return Ok(epoch),
        Err(err) if source_rescan_delivery_error_is_pending(err.as_error()) => err,
        Err(err) => return Err(err),
    };
    let repair_budget = remaining();
    if repair_budget.is_zero() {
        return Err(first_err);
    }
    match tokio::time::timeout(repair_budget, (source_repair_recovery)()).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => return Err(SourceFailure::from(err)),
        Err(_) => return Err(SourceFailure::from(CnxError::Timeout)),
    }
    let retry_budget = remaining();
    if retry_budget.is_zero() {
        return Err(first_err);
    }
    source
        .submit_targeted_rescan_request_epoch_with_acceptance_timeout_with_failure(Some(
            retry_budget,
        ))
        .await
}

async fn ensure_worker_source_rescan_proxy_endpoint_started(
    boundary: Arc<dyn ChannelIoSubset>,
    source: Arc<SourceFacade>,
    node_id: NodeId,
    facade_gate: &RuntimeUnitGate,
    route_key: &str,
    ready_generation: Arc<AtomicU64>,
    runtime_endpoint_tasks: &Mutex<Vec<ManagedEndpointTask>>,
    source_repair_recovery: ManagementWriteRecovery,
    allow_unmanaged_local_route: bool,
) {
    if !matches!(&*source, SourceFacade::Worker(_)) {
        return;
    }
    let route_active = source_rescan_route_active_for_current_generation(facade_gate, route_key);
    if !route_active && !allow_unmanaged_local_route {
        ready_generation.store(0, Ordering::Release);
        return;
    }
    let current_generation = source_rescan_route_semantic_generation(facade_gate, route_key);
    let mut tasks = runtime_endpoint_tasks.lock().await;
    let mut route_present = false;
    tasks.retain(|task| {
        if task.route_key() != route_key {
            return true;
        }
        if task.is_finished() {
            ready_generation.store(0, Ordering::Release);
            return false;
        }
        if task.finish_reason().is_some() || task.is_shutdown_requested() {
            return true;
        }
        if !task.belongs_to_boundary(&boundary) {
            eprintln!(
                "fs_meta_runtime_app: retiring stale-boundary source rescan proxy endpoint route={} task_generation={:?} current_generation={}",
                task.route_key(),
                task.route_generation(),
                current_generation
            );
            task.request_shutdown_and_close();
            ready_generation.store(0, Ordering::Release);
            return true;
        }
        let generation_stale = task
            .route_generation()
            .is_some_and(|generation| generation != current_generation);
        let route_usable = if route_active {
            runtime_endpoint_task_route_still_active(facade_gate, task)
        } else {
            allow_unmanaged_local_route
        };
        if generation_stale || !route_usable {
            eprintln!(
                "fs_meta_runtime_app: retiring stale source rescan proxy endpoint route={} task_generation={:?} current_generation={}",
                task.route_key(),
                task.route_generation(),
                current_generation
            );
            task.request_shutdown_and_close_on(&boundary);
            ready_generation.store(0, Ordering::Release);
            true
        } else {
            route_present = true;
            true
        }
    });
    if !route_present {
        tasks.push(spawn_worker_source_rescan_proxy_endpoint(
            boundary,
            source,
            node_id,
            route_key.to_string(),
            facade_gate.clone(),
            ready_generation,
            source_repair_recovery,
        ));
    }
}

async fn ensure_worker_source_rescan_proxy_ready_until(
    boundary: Arc<dyn ChannelIoSubset>,
    source: Arc<SourceFacade>,
    node_id: NodeId,
    facade_gate: &RuntimeUnitGate,
    route_key: &str,
    ready_generation: Arc<AtomicU64>,
    runtime_endpoint_tasks: &Mutex<Vec<ManagedEndpointTask>>,
    source_repair_recovery: ManagementWriteRecovery,
    deadline: tokio::time::Instant,
    allow_unmanaged_local_route: bool,
) -> bool {
    ensure_worker_source_rescan_proxy_endpoint_started(
        boundary,
        source,
        node_id,
        facade_gate,
        route_key,
        ready_generation.clone(),
        runtime_endpoint_tasks,
        source_repair_recovery,
        allow_unmanaged_local_route,
    )
    .await;
    loop {
        if worker_source_rescan_proxy_task_ready_for_current_generation(
            facade_gate,
            route_key,
            &ready_generation,
            runtime_endpoint_tasks,
            allow_unmanaged_local_route,
        )
        .await
        {
            return true;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        tokio::time::sleep(remaining.min(Duration::from_millis(25))).await;
    }
}

fn mark_source_rescan_proxy_ready_for_current_generation_if_receive_armed(
    facade_gate: &RuntimeUnitGate,
    route_key: &str,
    ready_generation: &AtomicU64,
    receive_armed: bool,
) -> bool {
    if !receive_armed {
        return false;
    }
    let Ok(true) = facade_gate.route_active(execution_units::SOURCE_RUNTIME_UNIT_ID, route_key)
    else {
        return false;
    };
    let current_generation = source_rescan_route_semantic_generation(facade_gate, route_key);
    if current_generation == 0 {
        return false;
    }
    ready_generation.store(current_generation, Ordering::Release);
    true
}

fn is_scoped_source_rescan_request_route(route_key: &str) -> bool {
    let Some(request_route) = route_key.strip_suffix(".req") else {
        return false;
    };
    let Some((route_stem, route_version)) = ROUTE_KEY_SOURCE_RESCAN_INTERNAL.rsplit_once(':')
    else {
        return request_route.starts_with(&format!("{ROUTE_KEY_SOURCE_RESCAN_INTERNAL}."));
    };
    let Some(route_suffix) = request_route.strip_suffix(&format!(":{route_version}")) else {
        return false;
    };
    route_suffix.starts_with(&format!("{route_stem}."))
}

fn source_rescan_delivery_error_is_pending(err: &CnxError) -> bool {
    matches!(err, CnxError::Timeout | CnxError::NotReady(_))
}

struct SourceRescanProxyReceiveReadyBoundary {
    inner: Arc<dyn ChannelIoSubset>,
    route_key: String,
    facade_gate: RuntimeUnitGate,
    ready_generation: Arc<AtomicU64>,
}

impl SourceRescanProxyReceiveReadyBoundary {
    fn new(
        inner: Arc<dyn ChannelIoSubset>,
        route_key: String,
        facade_gate: RuntimeUnitGate,
        ready_generation: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner,
            route_key,
            facade_gate,
            ready_generation,
        }
    }
}

#[async_trait]
impl ChannelIoSubset for SourceRescanProxyReceiveReadyBoundary {
    async fn channel_send(&self, ctx: BoundaryContext, request: ChannelSendRequest) -> Result<()> {
        self.inner.channel_send(ctx, request).await
    }

    async fn channel_recv(
        &self,
        ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> Result<Vec<Event>> {
        if request.channel_key.0 == self.route_key
            && let Ok(true) = self
                .facade_gate
                .route_active(execution_units::SOURCE_RUNTIME_UNIT_ID, &self.route_key)
        {
            let current_generation =
                source_rescan_route_semantic_generation(&self.facade_gate, &self.route_key);
            if current_generation != 0 {
                self.ready_generation
                    .store(current_generation, Ordering::Release);
            }
        }
        self.inner.channel_recv(ctx, request).await
    }

    fn channel_close(&self, ctx: BoundaryContext, channel: ChannelKey) -> Result<()> {
        self.inner.channel_close(ctx, channel)
    }
}

fn is_internal_status_route(route_key: &str) -> bool {
    is_sink_status_query_request_route(route_key) || is_source_status_request_route(route_key)
}

fn is_sink_status_query_request_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)
        || is_per_peer_sink_status_request_route(route_key)
}

fn is_per_peer_sink_status_request_route(route_key: &str) -> bool {
    let Some(request_route) = route_key.strip_suffix(".req") else {
        return false;
    };
    let Some((stem, version)) = ROUTE_KEY_SINK_STATUS_INTERNAL.rsplit_once(':') else {
        return request_route.starts_with(&format!("{ROUTE_KEY_SINK_STATUS_INTERNAL}."));
    };
    let Some(route_stem) = request_route.strip_suffix(&format!(":{version}")) else {
        return false;
    };
    route_stem.starts_with(&format!("{stem}."))
}

fn is_materialized_query_request_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)
        || route_key == format!("{}.req", ROUTE_KEY_SINK_QUERY_INTERNAL)
        || is_per_peer_sink_query_request_route(route_key)
}

fn is_facade_dependent_query_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_QUERY)
        || is_materialized_query_request_route(route_key)
        || is_sink_status_query_request_route(route_key)
        || is_source_status_request_route(route_key)
}

fn is_fixed_bind_facade_business_read_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_QUERY)
        || is_materialized_query_request_route(route_key)
}

fn is_serving_facade_business_read_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_QUERY)
        || is_materialized_query_request_route(route_key)
        || is_sink_status_query_request_route(route_key)
}

fn is_uninitialized_cleanup_query_route_with_policy(
    route_key: &str,
    recovery_lane_policy: ControlFailureRecoveryLanePolicy,
) -> bool {
    is_source_status_request_route(route_key)
        && recovery_lane_policy == ControlFailureRecoveryLanePolicy::WithdrawInternalStatus
        || is_materialized_query_request_route(route_key)
            && matches!(
                recovery_lane_policy,
                ControlFailureRecoveryLanePolicy::WithdrawInternalStatus
                    | ControlFailureRecoveryLanePolicy::PreserveSourceStatus
            )
}

fn is_uninitialized_cleanup_query_route_tail_with_policy(
    route_key: &str,
    recovery_lane_policy: ControlFailureRecoveryLanePolicy,
) -> bool {
    is_uninitialized_cleanup_query_route_with_policy(route_key, recovery_lane_policy)
        && !is_serving_facade_business_read_route(route_key)
}

fn is_per_peer_sink_query_request_route(route_key: &str) -> bool {
    let Some(request_route) = route_key.strip_suffix(".req") else {
        return false;
    };
    let Some((stem, version)) = ROUTE_KEY_SINK_QUERY_INTERNAL.rsplit_once(':') else {
        return request_route.starts_with(&format!("{ROUTE_KEY_SINK_QUERY_INTERNAL}."));
    };
    let Some(route_stem) = request_route.strip_suffix(&format!(":{version}")) else {
        return false;
    };
    route_stem.starts_with(&format!("{stem}."))
}

fn is_per_peer_sink_roots_control_stream_route(route_key: &str) -> bool {
    let Some(stream_route) = route_key.strip_suffix(".stream") else {
        return false;
    };
    let Some((stem, version)) = ROUTE_KEY_SINK_ROOTS_CONTROL.rsplit_once(':') else {
        return stream_route.starts_with(&format!("{ROUTE_KEY_SINK_ROOTS_CONTROL}."));
    };
    let Some(route_stem) = stream_route.strip_suffix(&format!(":{version}")) else {
        return false;
    };
    route_stem.starts_with(&format!("{stem}."))
}

fn is_source_status_request_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL)
        || is_per_peer_source_status_request_route(route_key)
}

fn is_per_peer_source_status_request_route(route_key: &str) -> bool {
    let Some(request_route) = route_key.strip_suffix(".req") else {
        return false;
    };
    let Some((stem, version)) = ROUTE_KEY_SOURCE_STATUS_INTERNAL.rsplit_once(':') else {
        return request_route.starts_with(&format!("{ROUTE_KEY_SOURCE_STATUS_INTERNAL}."));
    };
    let Some(route_stem) = request_route.strip_suffix(&format!(":{version}")) else {
        return false;
    };
    route_stem.starts_with(&format!("{stem}."))
}

fn is_retryable_worker_control_reset(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::TransportClosed(_) | CnxError::Timeout | CnxError::ChannelClosed
    ) || matches!(
        err,
        CnxError::PeerError(message) | CnxError::Internal(message)
            if message.contains("operation timed out")
    ) || matches!(
        err,
        CnxError::PeerError(message) | CnxError::Internal(message)
            if message.contains("transport closed")
                && (message.contains("Connection reset by peer")
                    || message.contains("early eof")
                    || message.contains("Broken pipe")
                    || message.contains("bridge stopped"))
    ) || matches!(
        err,
        CnxError::AccessDenied(message) | CnxError::PeerError(message)
            if message.contains("drained/fenced")
                && message.contains("grant attachments")
    ) || matches!(
        err,
        CnxError::PeerError(message) | CnxError::Internal(message)
            if message.contains("statecell_write returned non-committed status for sink state")
                && message.contains("fenced")
    )
}

fn is_stale_drained_fenced_grant_attachment_retry(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::AccessDenied(message) | CnxError::PeerError(message)
            if message.contains("drained/fenced")
                && message.contains("grant attachments")
    )
}

fn is_retryable_worker_transport_close_reset(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::TransportClosed(_) | CnxError::Timeout | CnxError::ChannelClosed
    ) || matches!(
        err,
        CnxError::PeerError(message) | CnxError::Internal(message)
            if message.contains("transport closed")
                && (message.contains("Connection reset by peer")
                    || message.contains("early eof")
                    || message.contains("Broken pipe")
                    || message.contains("bridge stopped"))
    )
}

fn is_source_post_ack_scheduled_groups_refresh_exhaustion(err: &CnxError) -> bool {
    matches!(
        err,
        CnxError::Internal(message)
            if message.contains("source worker post-ack scheduled-groups refresh exhausted")
                || message
                    .contains("source worker replay-required scheduled-groups refresh exhausted")
    )
}

fn should_fail_closed_source_control_recovery_exhaustion(err: &CnxError) -> bool {
    is_retryable_worker_control_reset(err)
        || is_source_post_ack_scheduled_groups_refresh_exhaustion(err)
}

fn should_fail_closed_sink_control_recovery_exhaustion(err: &CnxError) -> bool {
    is_retryable_worker_control_reset(err)
}

fn facade_bind_addr_is_ephemeral(bind_addr: &str) -> bool {
    bind_addr
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .is_some_and(|port| port == 0)
}

fn process_facade_claim_cell() -> &'static StdMutex<BTreeMap<String, ProcessFacadeClaim>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, ProcessFacadeClaim>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(BTreeMap::new()))
}

fn shared_api_request_tracker_for_config(config: &api::ApiConfig) -> Arc<ApiRequestTracker> {
    let mut fixed_bind_addrs = config
        .local_listener_resources
        .iter()
        .map(|resource| resource.bind_addr.clone())
        .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
        .collect::<Vec<_>>();
    fixed_bind_addrs.sort();
    fixed_bind_addrs.dedup();
    if fixed_bind_addrs.is_empty() {
        return Arc::new(ApiRequestTracker::default());
    }
    static CELL: OnceLock<StdMutex<BTreeMap<Vec<String>, Arc<ApiRequestTracker>>>> =
        OnceLock::new();
    let mut guard = match CELL.get_or_init(|| StdMutex::new(BTreeMap::new())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard
        .entry(fixed_bind_addrs)
        .or_insert_with(|| Arc::new(ApiRequestTracker::default()))
        .clone()
}

fn runtime_worker_binding_serial_key(binding: &RuntimeWorkerBinding) -> String {
    format!(
        "{}|{:?}|{:?}",
        binding.role_id, binding.mode, binding.launcher_kind
    )
}

fn shared_control_frame_serial_for_runtime(
    _node_id: &NodeId,
    source_worker_binding: &RuntimeWorkerBinding,
    sink_worker_binding: &RuntimeWorkerBinding,
) -> Arc<Mutex<()>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, Arc<Mutex<()>>>>> = OnceLock::new();
    let key = format!(
        "{}|{}",
        runtime_worker_binding_serial_key(source_worker_binding),
        runtime_worker_binding_serial_key(sink_worker_binding)
    );
    let mut guard = match CELL.get_or_init(|| StdMutex::new(BTreeMap::new())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn shared_control_frame_lease_path_for_runtime(
    node_id: &NodeId,
    source_worker_binding: &RuntimeWorkerBinding,
    sink_worker_binding: &RuntimeWorkerBinding,
) -> Option<std::path::PathBuf> {
    if source_worker_binding.mode != WorkerMode::External
        && sink_worker_binding.mode != WorkerMode::External
    {
        return None;
    }
    let lease_root = std::env::var("DATANIX_E2E_TMP_ROOT")
        .or_else(|_| std::env::var("CAPANIX_E2E_TMP_ROOT"))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir())
        .join("fs-meta-control-frame-leases");
    let node_token = node_id
        .0
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '-',
        })
        .collect::<String>();
    let serial_token = format!(
        "{}--{}",
        runtime_worker_binding_serial_key(source_worker_binding),
        runtime_worker_binding_serial_key(sink_worker_binding)
    )
    .chars()
    .map(|ch| match ch {
        'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
        _ => '-',
    })
    .collect::<String>();
    Some(lease_root.join(format!("control-frame-{node_token}-{serial_token}.lock")))
}

struct ControlFrameLeaseGuard {
    file: File,
}

impl ControlFrameLeaseGuard {
    fn flock(file: &File, op: libc::c_int) -> std::io::Result<()> {
        let rc = unsafe { libc::flock(std::os::fd::AsRawFd::as_raw_fd(file), op) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    fn try_acquire(path: &std::path::Path) -> std::io::Result<Option<Self>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        match Self::flock(&file, libc::LOCK_EX | libc::LOCK_NB) {
            Ok(()) => Ok(Some(Self { file })),
            Err(err)
                if matches!(
                    err.raw_os_error(),
                    Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN
                ) =>
            {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    async fn acquire(
        path: &std::path::Path,
        deadline: tokio::time::Instant,
        should_abort: impl Fn() -> bool,
    ) -> std::io::Result<Self> {
        loop {
            if should_abort() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    format!(
                        "control-frame lease acquisition aborted while waiting for {}",
                        path.display()
                    ),
                ));
            }
            if let Some(guard) = Self::try_acquire(path)? {
                return Ok(guard);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "control-frame lease acquisition exceeded {:?} for {}",
                        CONTROL_FRAME_LEASE_ACQUIRE_BUDGET,
                        path.display()
                    ),
                ));
            }
            tokio::time::sleep(std::cmp::min(
                CONTROL_FRAME_LEASE_RETRY_INTERVAL,
                deadline.saturating_duration_since(now),
            ))
            .await;
        }
    }
}

impl Drop for ControlFrameLeaseGuard {
    fn drop(&mut self) {
        let _ = Self::flock(&self.file, libc::LOCK_UN);
    }
}

type SharedRouteClaimId = (String, String);
type SharedRouteClaims =
    std::collections::BTreeMap<SharedRouteClaimId, std::collections::BTreeSet<u64>>;
type SharedSinkRouteClaims = SharedRouteClaims;
type SharedSourceRouteClaims = SharedRouteClaims;

enum SharedRouteClaimDelta {
    Claim(SharedRouteClaimId),
    Release(SharedRouteClaimId),
}

fn shared_sink_route_claims_for_runtime(
    node_id: &NodeId,
    source_worker_binding: &RuntimeWorkerBinding,
    sink_worker_binding: &RuntimeWorkerBinding,
) -> Arc<Mutex<SharedSinkRouteClaims>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, Arc<Mutex<SharedSinkRouteClaims>>>>> =
        OnceLock::new();
    let key = format!(
        "{}|{}|{}",
        node_id.0,
        runtime_worker_binding_serial_key(source_worker_binding),
        runtime_worker_binding_serial_key(sink_worker_binding)
    );
    let mut guard = match CELL.get_or_init(|| StdMutex::new(BTreeMap::new())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(SharedSinkRouteClaims::default())))
        .clone()
}

fn shared_source_route_claims_for_runtime(
    node_id: &NodeId,
    source_worker_binding: &RuntimeWorkerBinding,
    sink_worker_binding: &RuntimeWorkerBinding,
) -> Arc<Mutex<SharedSourceRouteClaims>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, Arc<Mutex<SharedSourceRouteClaims>>>>> =
        OnceLock::new();
    let key = format!(
        "{}|{}|{}",
        node_id.0,
        runtime_worker_binding_serial_key(source_worker_binding),
        runtime_worker_binding_serial_key(sink_worker_binding)
    );
    let mut guard = match CELL.get_or_init(|| StdMutex::new(BTreeMap::new())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(SharedSourceRouteClaims::default())))
        .clone()
}

fn next_app_instance_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn clear_owned_process_facade_claim(instance_id: u64) {
    let mut guard = match process_facade_claim_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.retain(|_, claim| claim.owner_instance_id != instance_id);
}

#[cfg(test)]
fn clear_process_facade_claim_for_tests() {
    clear_owned_process_facade_claim(0);
    let mut guard = match process_facade_claim_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.clear();
    clear_pending_fixed_bind_handoff_ready_for_tests();
    let mut active_guard = match active_fixed_bind_facade_owner_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    active_guard.clear();
}

fn pending_fixed_bind_handoff_ready_cell()
-> &'static StdMutex<BTreeMap<String, PendingFixedBindHandoffRegistrant>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, PendingFixedBindHandoffRegistrant>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(BTreeMap::new()))
}

fn mark_pending_fixed_bind_handoff_ready_with_registrant(
    bind_addr: &str,
    registrant: PendingFixedBindHandoffRegistrant,
) {
    let mut guard = match pending_fixed_bind_handoff_ready_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.insert(bind_addr.to_string(), registrant);
}

#[cfg(test)]
fn mark_pending_fixed_bind_handoff_ready(
    bind_addr: &str,
    registrant: PendingFixedBindHandoffRegistrant,
) {
    mark_pending_fixed_bind_handoff_ready_with_registrant(bind_addr, registrant);
}

fn clear_pending_fixed_bind_handoff_ready(bind_addr: &str) {
    let mut guard = match pending_fixed_bind_handoff_ready_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.remove(bind_addr);
}

fn pending_fixed_bind_handoff_ready_for(
    bind_addr: &str,
) -> Option<PendingFixedBindHandoffRegistrant> {
    let guard = match pending_fixed_bind_handoff_ready_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.get(bind_addr).cloned()
}

fn active_fixed_bind_facade_owner_cell()
-> &'static StdMutex<BTreeMap<String, ActiveFixedBindFacadeRegistrant>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, ActiveFixedBindFacadeRegistrant>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(BTreeMap::new()))
}

fn mark_active_fixed_bind_facade_owner(
    bind_addr: &str,
    registrant: ActiveFixedBindFacadeRegistrant,
) {
    let mut guard = match active_fixed_bind_facade_owner_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.insert(bind_addr.to_string(), registrant);
}

fn clear_active_fixed_bind_facade_owner(bind_addr: &str, instance_id: u64) {
    let mut guard = match active_fixed_bind_facade_owner_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if guard
        .get(bind_addr)
        .is_some_and(|registrant| registrant.instance_id == instance_id)
    {
        guard.remove(bind_addr);
    }
}

fn active_fixed_bind_facade_owner_for(
    bind_addr: &str,
    requester_instance_id: u64,
) -> Option<ActiveFixedBindFacadeRegistrant> {
    let guard = match active_fixed_bind_facade_owner_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.get(bind_addr).and_then(|registrant| {
        (registrant.instance_id != requester_instance_id).then_some(registrant.clone())
    })
}

fn active_fixed_bind_facade_owned_by(bind_addr: &str, instance_id: u64) -> bool {
    let guard = match active_fixed_bind_facade_owner_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard
        .get(bind_addr)
        .is_some_and(|registrant| registrant.instance_id == instance_id)
}

fn clear_process_facade_claim_for_bind_addr(bind_addr: &str, owner_instance_id: u64) {
    let mut guard = match process_facade_claim_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if guard
        .get(bind_addr)
        .is_some_and(|claim| claim.owner_instance_id == owner_instance_id)
    {
        guard.remove(bind_addr);
    }
}

fn clear_process_facade_claim_for_retired_fixed_bind(bind_addr: &str) {
    let mut guard = match process_facade_claim_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.remove(bind_addr);
}

#[cfg(test)]
fn clear_pending_fixed_bind_handoff_ready_for_tests() {
    let mut guard = match pending_fixed_bind_handoff_ready_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.clear();
}

#[cfg(test)]
#[derive(Clone)]
struct FacadeShutdownStartHook {
    entered: Arc<tokio::sync::Notify>,
    release: Option<Arc<tokio::sync::Notify>>,
}

#[cfg(test)]
#[derive(Clone)]
struct RuntimeProxyRequestPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct RuntimeControlFrameStartHook {
    entered: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct FacadeDeactivatePauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct SourceApplyPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct InitialMixedSourceToSinkPretriggerPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct SinkApplyPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
struct SourceApplyErrorQueueHook {
    errs: std::collections::VecDeque<CnxError>,
}

#[cfg(test)]
struct SinkApplyErrorQueueHook {
    errs: std::collections::VecDeque<CnxError>,
}

#[cfg(test)]
struct SourceControlRecoveryErrorQueueHook {
    errs: std::collections::VecDeque<CnxError>,
}

#[cfg(test)]
struct SinkControlRecoveryErrorQueueHook {
    errs: std::collections::VecDeque<CnxError>,
}

#[cfg(test)]
fn source_apply_entry_count_hook_cell()
-> &'static StdMutex<std::collections::BTreeMap<u64, Arc<AtomicUsize>>> {
    static CELL: OnceLock<StdMutex<std::collections::BTreeMap<u64, Arc<AtomicUsize>>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(std::collections::BTreeMap::new()))
}

#[cfg(test)]
const GLOBAL_APPLY_ENTRY_COUNT_HOOK_INSTANCE_ID: u64 = 0;

#[cfg(test)]
fn install_source_apply_entry_count_hook(count: Arc<AtomicUsize>) {
    install_source_apply_entry_count_hook_for_app(GLOBAL_APPLY_ENTRY_COUNT_HOOK_INSTANCE_ID, count);
}

#[cfg(test)]
fn install_source_apply_entry_count_hook_for_app(instance_id: u64, count: Arc<AtomicUsize>) {
    let mut guard = match source_apply_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.insert(instance_id, count);
}

#[cfg(test)]
fn clear_source_apply_entry_count_hook() {
    clear_source_apply_entry_count_hook_for_app(GLOBAL_APPLY_ENTRY_COUNT_HOOK_INSTANCE_ID);
}

#[cfg(test)]
fn clear_source_apply_entry_count_hook_for_app(instance_id: u64) {
    let mut guard = match source_apply_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.remove(&instance_id);
}

#[cfg(test)]
fn note_source_apply_entry_for_tests(instance_id: u64) {
    let hook = {
        let guard = match source_apply_entry_count_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.get(&instance_id).cloned().or_else(|| {
            guard
                .get(&GLOBAL_APPLY_ENTRY_COUNT_HOOK_INSTANCE_ID)
                .cloned()
        })
    };
    if let Some(count) = hook {
        count.fetch_add(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
fn runtime_initialize_entry_count_hook_cell() -> &'static StdMutex<Option<Arc<AtomicUsize>>> {
    static CELL: OnceLock<StdMutex<Option<Arc<AtomicUsize>>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_runtime_initialize_entry_count_hook(count: Arc<AtomicUsize>) {
    let mut guard = match runtime_initialize_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(count);
}

#[cfg(test)]
fn clear_runtime_initialize_entry_count_hook() {
    let mut guard = match runtime_initialize_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn note_runtime_initialize_entry_for_tests() {
    let hook = {
        let guard = match runtime_initialize_entry_count_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(count) = hook {
        count.fetch_add(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
fn sink_apply_entry_count_hook_cell()
-> &'static StdMutex<std::collections::BTreeMap<u64, Arc<AtomicUsize>>> {
    static CELL: OnceLock<StdMutex<std::collections::BTreeMap<u64, Arc<AtomicUsize>>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(std::collections::BTreeMap::new()))
}

#[cfg(test)]
fn install_sink_apply_entry_count_hook(count: Arc<AtomicUsize>) {
    install_sink_apply_entry_count_hook_for_app(GLOBAL_APPLY_ENTRY_COUNT_HOOK_INSTANCE_ID, count);
}

#[cfg(test)]
fn install_sink_apply_entry_count_hook_for_app(instance_id: u64, count: Arc<AtomicUsize>) {
    let mut guard = match sink_apply_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.insert(instance_id, count);
}

#[cfg(test)]
fn clear_sink_apply_entry_count_hook() {
    clear_sink_apply_entry_count_hook_for_app(GLOBAL_APPLY_ENTRY_COUNT_HOOK_INSTANCE_ID);
}

#[cfg(test)]
fn clear_sink_apply_entry_count_hook_for_app(instance_id: u64) {
    let mut guard = match sink_apply_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.remove(&instance_id);
}

#[cfg(test)]
fn note_sink_apply_entry_for_tests(instance_id: u64) {
    let hook = {
        let guard = match sink_apply_entry_count_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.get(&instance_id).cloned().or_else(|| {
            guard
                .get(&GLOBAL_APPLY_ENTRY_COUNT_HOOK_INSTANCE_ID)
                .cloned()
        })
    };
    if let Some(count) = hook {
        count.fetch_add(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
fn local_sink_status_republish_helper_entry_count_hook_cell()
-> &'static StdMutex<Option<Arc<AtomicUsize>>> {
    static CELL: OnceLock<StdMutex<Option<Arc<AtomicUsize>>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_local_sink_status_republish_helper_entry_count_hook(count: Arc<AtomicUsize>) {
    let mut guard = match local_sink_status_republish_helper_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(count);
}

#[cfg(test)]
fn clear_local_sink_status_republish_helper_entry_count_hook() {
    let mut guard = match local_sink_status_republish_helper_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn note_local_sink_status_republish_helper_entry_for_tests() {
    let hook = {
        let guard = match local_sink_status_republish_helper_entry_count_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(count) = hook {
        count.fetch_add(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
#[derive(Clone)]
struct LocalSinkStatusRepublishProbePauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct LocalSinkStatusRepublishRetriggerPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct DeferredSinkOwnedQueryPeerPublicationGateReopenPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct DeferredSinkOwnedQueryPeerPublicationCompletionHook {
    entered: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct PendingFixedBindHandoffCompletionGateReopenPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct PendingFixedBindHandoffCompletionCompletionHook {
    entered: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
struct UninitializedQueryWithdrawPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
fn facade_shutdown_start_hook_cell() -> &'static StdMutex<Option<FacadeShutdownStartHook>> {
    static CELL: OnceLock<StdMutex<Option<FacadeShutdownStartHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_facade_shutdown_start_hook(hook: FacadeShutdownStartHook) {
    let mut guard = match facade_shutdown_start_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_facade_shutdown_start_hook() {
    let mut guard = match facade_shutdown_start_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
async fn maybe_pause_facade_shutdown_started() {
    let hook = {
        let guard = match facade_shutdown_start_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        hook.entered.notify_waiters();
        if let Some(release) = hook.release {
            release.notified().await;
        }
    }
}

#[cfg(test)]
fn uninitialized_query_withdraw_pause_hook_cell()
-> &'static StdMutex<Option<UninitializedQueryWithdrawPauseHook>> {
    static CELL: OnceLock<StdMutex<Option<UninitializedQueryWithdrawPauseHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_uninitialized_query_withdraw_pause_hook(hook: UninitializedQueryWithdrawPauseHook) {
    let mut guard = match uninitialized_query_withdraw_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_uninitialized_query_withdraw_pause_hook() {
    let mut guard = match uninitialized_query_withdraw_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
async fn maybe_pause_before_uninitialized_query_withdraw() {
    let hook = {
        let guard = match uninitialized_query_withdraw_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        hook.entered.notify_waiters();
        hook.release.notified().await;
    }
}

#[cfg(test)]
fn runtime_proxy_request_pause_hook_cell()
-> &'static StdMutex<BTreeMap<&'static str, RuntimeProxyRequestPauseHook>> {
    static CELL: OnceLock<StdMutex<BTreeMap<&'static str, RuntimeProxyRequestPauseHook>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn install_runtime_proxy_request_pause_hook(
    label: &'static str,
    hook: RuntimeProxyRequestPauseHook,
) {
    let mut guard = match runtime_proxy_request_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.insert(label, hook);
}

#[cfg(test)]
fn clear_runtime_proxy_request_pause_hook(label: &'static str) {
    let mut guard = match runtime_proxy_request_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.remove(label);
}

#[cfg(test)]
async fn maybe_pause_runtime_proxy_request(label: &'static str) {
    let hook = {
        let guard = match runtime_proxy_request_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.get(label).cloned()
    };
    if let Some(hook) = hook {
        eprintln!(
            "fs_meta_runtime_app: runtime proxy pause hook hit label={}",
            label
        );
        let mut release = std::pin::pin!(hook.release.notified());
        std::future::poll_fn(|cx| {
            let _ = std::future::Future::poll(release.as_mut(), cx);
            std::task::Poll::Ready(())
        })
        .await;
        hook.entered.notify_waiters();
        release.await;
    }
}

#[cfg(test)]
fn runtime_control_frame_start_hook_cell() -> &'static StdMutex<Option<RuntimeControlFrameStartHook>>
{
    static CELL: OnceLock<StdMutex<Option<RuntimeControlFrameStartHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_runtime_control_frame_start_hook(hook: RuntimeControlFrameStartHook) {
    let mut guard = match runtime_control_frame_start_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_runtime_control_frame_start_hook() {
    let mut guard = match runtime_control_frame_start_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn notify_runtime_control_frame_started() {
    let hook = {
        let guard = match runtime_control_frame_start_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        hook.entered.notify_waiters();
    }
}

#[cfg(test)]
fn facade_deactivate_pause_hook_cell()
-> &'static StdMutex<BTreeMap<(String, String), FacadeDeactivatePauseHook>> {
    static CELL: OnceLock<StdMutex<BTreeMap<(String, String), FacadeDeactivatePauseHook>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn install_facade_deactivate_pause_hook(
    unit_id: &str,
    route_key: &str,
    hook: FacadeDeactivatePauseHook,
) {
    let mut guard = match facade_deactivate_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.insert((unit_id.to_string(), route_key.to_string()), hook);
}

#[cfg(test)]
fn clear_facade_deactivate_pause_hook(unit_id: &str, route_key: &str) {
    let mut guard = match facade_deactivate_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.remove(&(unit_id.to_string(), route_key.to_string()));
}

#[cfg(test)]
async fn maybe_pause_facade_deactivate(unit_id: &str, route_key: &str) {
    let hook = {
        let guard = match facade_deactivate_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .get(&(unit_id.to_string(), route_key.to_string()))
            .cloned()
    };
    if let Some(hook) = hook {
        let mut release = std::pin::pin!(hook.release.notified());
        std::future::poll_fn(|cx| {
            let _ = std::future::Future::poll(release.as_mut(), cx);
            std::task::Poll::Ready(())
        })
        .await;
        hook.entered.notify_waiters();
        release.await;
    }
}

#[cfg(test)]
fn source_apply_pause_hook_cell() -> &'static StdMutex<Option<SourceApplyPauseHook>> {
    static CELL: OnceLock<StdMutex<Option<SourceApplyPauseHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_source_apply_pause_hook(hook: SourceApplyPauseHook) {
    let mut guard = match source_apply_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_source_apply_pause_hook() {
    let mut guard = match source_apply_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn initial_mixed_source_to_sink_pretrigger_pause_hook_cell()
-> &'static StdMutex<Option<InitialMixedSourceToSinkPretriggerPauseHook>> {
    static CELL: OnceLock<StdMutex<Option<InitialMixedSourceToSinkPretriggerPauseHook>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_initial_mixed_source_to_sink_pretrigger_pause_hook(
    hook: InitialMixedSourceToSinkPretriggerPauseHook,
) {
    let mut guard = match initial_mixed_source_to_sink_pretrigger_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_initial_mixed_source_to_sink_pretrigger_pause_hook() {
    let mut guard = match initial_mixed_source_to_sink_pretrigger_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn source_apply_error_queue_hook_cell() -> &'static StdMutex<Option<SourceApplyErrorQueueHook>> {
    static CELL: OnceLock<StdMutex<Option<SourceApplyErrorQueueHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_source_apply_error_queue_hook(hook: SourceApplyErrorQueueHook) {
    let mut guard = match source_apply_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_source_apply_error_queue_hook() {
    let mut guard = match source_apply_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn take_source_apply_error_queue_hook() -> Option<CnxError> {
    let mut guard = match source_apply_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    let err = hook.errs.pop_front();
    if hook.errs.is_empty() {
        *guard = None;
    }
    err
}

#[cfg(test)]
fn sink_apply_error_queue_hook_cell() -> &'static StdMutex<Option<SinkApplyErrorQueueHook>> {
    static CELL: OnceLock<StdMutex<Option<SinkApplyErrorQueueHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_sink_apply_error_queue_hook(hook: SinkApplyErrorQueueHook) {
    let mut guard = match sink_apply_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_sink_apply_error_queue_hook() {
    let mut guard = match sink_apply_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn take_sink_apply_error_queue_hook() -> Option<CnxError> {
    let mut guard = match sink_apply_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    let err = hook.errs.pop_front();
    if hook.errs.is_empty() {
        *guard = None;
    }
    err
}

#[cfg(test)]
fn source_control_recovery_error_queue_hook_cell()
-> &'static StdMutex<Option<SourceControlRecoveryErrorQueueHook>> {
    static CELL: OnceLock<StdMutex<Option<SourceControlRecoveryErrorQueueHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_source_control_recovery_error_queue_hook(hook: SourceControlRecoveryErrorQueueHook) {
    let mut guard = match source_control_recovery_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_source_control_recovery_error_queue_hook() {
    let mut guard = match source_control_recovery_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn take_source_control_recovery_error_queue_hook() -> Option<CnxError> {
    let mut guard = match source_control_recovery_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    let err = hook.errs.pop_front();
    if hook.errs.is_empty() {
        *guard = None;
    }
    err
}

#[cfg(test)]
fn sink_control_recovery_error_queue_hook_cell()
-> &'static StdMutex<Option<SinkControlRecoveryErrorQueueHook>> {
    static CELL: OnceLock<StdMutex<Option<SinkControlRecoveryErrorQueueHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_sink_control_recovery_error_queue_hook(hook: SinkControlRecoveryErrorQueueHook) {
    let mut guard = match sink_control_recovery_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_sink_control_recovery_error_queue_hook() {
    let mut guard = match sink_control_recovery_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn take_sink_control_recovery_error_queue_hook() -> Option<CnxError> {
    let mut guard = match sink_control_recovery_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    let err = hook.errs.pop_front();
    if hook.errs.is_empty() {
        *guard = None;
    }
    err
}

#[cfg(test)]
async fn maybe_pause_before_source_apply() {
    let hook = {
        let guard = match source_apply_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        let mut release = std::pin::pin!(hook.release.notified());
        std::future::poll_fn(|cx| {
            let _ = std::future::Future::poll(release.as_mut(), cx);
            std::task::Poll::Ready(())
        })
        .await;
        hook.entered.notify_waiters();
        release.await;
    }
}

#[cfg(test)]
async fn maybe_pause_before_initial_mixed_source_to_sink_pretrigger() {
    let hook = {
        let guard = match initial_mixed_source_to_sink_pretrigger_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        let mut release = std::pin::pin!(hook.release.notified());
        std::future::poll_fn(|cx| {
            let _ = std::future::Future::poll(release.as_mut(), cx);
            std::task::Poll::Ready(())
        })
        .await;
        hook.entered.notify_waiters();
        release.await;
    }
}

#[cfg(test)]
fn sink_apply_pause_hook_cell() -> &'static StdMutex<Option<SinkApplyPauseHook>> {
    static CELL: OnceLock<StdMutex<Option<SinkApplyPauseHook>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_sink_apply_pause_hook(hook: SinkApplyPauseHook) {
    let mut guard = match sink_apply_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_sink_apply_pause_hook() {
    let mut guard = match sink_apply_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
async fn maybe_pause_before_sink_apply() {
    let hook = {
        let guard = match sink_apply_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        let mut release = std::pin::pin!(hook.release.notified());
        std::future::poll_fn(|cx| {
            let _ = std::future::Future::poll(release.as_mut(), cx);
            std::task::Poll::Ready(())
        })
        .await;
        hook.entered.notify_waiters();
        release.await;
    }
}

#[cfg(test)]
fn local_sink_status_republish_probe_pause_hook_cell()
-> &'static StdMutex<Option<LocalSinkStatusRepublishProbePauseHook>> {
    static CELL: OnceLock<StdMutex<Option<LocalSinkStatusRepublishProbePauseHook>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_local_sink_status_republish_probe_pause_hook(
    hook: LocalSinkStatusRepublishProbePauseHook,
) {
    let mut guard = match local_sink_status_republish_probe_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_local_sink_status_republish_probe_pause_hook() {
    let mut guard = match local_sink_status_republish_probe_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
async fn maybe_pause_before_local_sink_status_republish_probe() {
    let hook = {
        let guard = match local_sink_status_republish_probe_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        let mut release = std::pin::pin!(hook.release.notified());
        std::future::poll_fn(|cx| {
            let _ = std::future::Future::poll(release.as_mut(), cx);
            std::task::Poll::Ready(())
        })
        .await;
        hook.entered.notify_waiters();
        release.await;
    }
}

#[cfg(test)]
fn local_sink_status_republish_retrigger_pause_hook_cell()
-> &'static StdMutex<Option<LocalSinkStatusRepublishRetriggerPauseHook>> {
    static CELL: OnceLock<StdMutex<Option<LocalSinkStatusRepublishRetriggerPauseHook>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_local_sink_status_republish_retrigger_pause_hook(
    hook: LocalSinkStatusRepublishRetriggerPauseHook,
) {
    let mut guard = match local_sink_status_republish_retrigger_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_local_sink_status_republish_retrigger_pause_hook() {
    let mut guard = match local_sink_status_republish_retrigger_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
async fn maybe_pause_after_local_sink_status_republish_retrigger() {
    let hook = {
        let mut guard = match local_sink_status_republish_retrigger_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.take()
    };
    if let Some(hook) = hook {
        hook.entered.notify_waiters();
        let mut release = std::pin::pin!(hook.release.notified());
        std::future::poll_fn(|cx| {
            let _ = std::future::Future::poll(release.as_mut(), cx);
            std::task::Poll::Ready(())
        })
        .await;
        release.await;
    }
}

#[cfg(test)]
fn deferred_sink_owned_query_peer_publication_gate_reopen_pause_hook_cell()
-> &'static StdMutex<Option<DeferredSinkOwnedQueryPeerPublicationGateReopenPauseHook>> {
    static CELL: OnceLock<
        StdMutex<Option<DeferredSinkOwnedQueryPeerPublicationGateReopenPauseHook>>,
    > = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_deferred_sink_owned_query_peer_publication_gate_reopen_pause_hook(
    hook: DeferredSinkOwnedQueryPeerPublicationGateReopenPauseHook,
) {
    let mut guard =
        match deferred_sink_owned_query_peer_publication_gate_reopen_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_deferred_sink_owned_query_peer_publication_gate_reopen_pause_hook() {
    let mut guard =
        match deferred_sink_owned_query_peer_publication_gate_reopen_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
    *guard = None;
}

#[cfg(test)]
async fn maybe_pause_before_deferred_sink_owned_query_peer_publication_gate_reopen() {
    let hook = {
        let guard =
            match deferred_sink_owned_query_peer_publication_gate_reopen_pause_hook_cell().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        guard.clone()
    };
    if let Some(hook) = hook {
        let mut release = std::pin::pin!(hook.release.notified());
        std::future::poll_fn(|cx| {
            let _ = std::future::Future::poll(release.as_mut(), cx);
            std::task::Poll::Ready(())
        })
        .await;
        hook.entered.notify_waiters();
        release.await;
    }
}

#[cfg(test)]
fn deferred_sink_owned_query_peer_publication_completion_hook_cell()
-> &'static StdMutex<Option<DeferredSinkOwnedQueryPeerPublicationCompletionHook>> {
    static CELL: OnceLock<StdMutex<Option<DeferredSinkOwnedQueryPeerPublicationCompletionHook>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_deferred_sink_owned_query_peer_publication_completion_hook(
    hook: DeferredSinkOwnedQueryPeerPublicationCompletionHook,
) {
    let mut guard = match deferred_sink_owned_query_peer_publication_completion_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_deferred_sink_owned_query_peer_publication_completion_hook() {
    let mut guard = match deferred_sink_owned_query_peer_publication_completion_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn notify_deferred_sink_owned_query_peer_publication_completion() {
    let hook = {
        let guard = match deferred_sink_owned_query_peer_publication_completion_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        hook.entered.notify_waiters();
    }
}

#[cfg(test)]
fn pending_fixed_bind_handoff_completion_gate_reopen_pause_hook_cell()
-> &'static StdMutex<Option<PendingFixedBindHandoffCompletionGateReopenPauseHook>> {
    static CELL: OnceLock<StdMutex<Option<PendingFixedBindHandoffCompletionGateReopenPauseHook>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_pending_fixed_bind_handoff_completion_gate_reopen_pause_hook(
    hook: PendingFixedBindHandoffCompletionGateReopenPauseHook,
) {
    let mut guard = match pending_fixed_bind_handoff_completion_gate_reopen_pause_hook_cell().lock()
    {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_pending_fixed_bind_handoff_completion_gate_reopen_pause_hook() {
    let mut guard = match pending_fixed_bind_handoff_completion_gate_reopen_pause_hook_cell().lock()
    {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
async fn maybe_pause_before_pending_fixed_bind_handoff_completion_gate_reopen() {
    let hook = {
        let guard = match pending_fixed_bind_handoff_completion_gate_reopen_pause_hook_cell().lock()
        {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        hook.entered.notify_waiters();
        hook.release.notified().await;
    }
}

#[cfg(test)]
fn pending_fixed_bind_handoff_completion_completion_hook_cell()
-> &'static StdMutex<Option<PendingFixedBindHandoffCompletionCompletionHook>> {
    static CELL: OnceLock<StdMutex<Option<PendingFixedBindHandoffCompletionCompletionHook>>> =
        OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_pending_fixed_bind_handoff_completion_completion_hook(
    hook: PendingFixedBindHandoffCompletionCompletionHook,
) {
    let mut guard = match pending_fixed_bind_handoff_completion_completion_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
fn clear_pending_fixed_bind_handoff_completion_completion_hook() {
    let mut guard = match pending_fixed_bind_handoff_completion_completion_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn notify_pending_fixed_bind_handoff_completion_completion() {
    let hook = {
        let guard = match pending_fixed_bind_handoff_completion_completion_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        hook.entered.notify_waiters();
    }
}

pub struct FSMetaApp {
    instance_id: u64,
    config: FSMetaConfig,
    node_id: NodeId,
    runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    source: Arc<SourceFacade>,
    sink: Arc<SinkFacade>,
    query_sink: Arc<SinkFacade>,
    pump_task: Mutex<Option<SourcePumpHandle>>,
    runtime_endpoint_tasks: Arc<Mutex<Vec<ManagedEndpointTask>>>,
    runtime_endpoint_routes: Mutex<std::collections::BTreeSet<String>>,
    api_task: Arc<Mutex<Option<FacadeActivation>>>,
    pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
    facade_spawn_in_progress: Arc<Mutex<Option<FacadeSpawnInProgress>>>,
    retained_active_facade_continuity: Arc<AtomicBool>,
    pending_fixed_bind_claim_release_followup: Arc<AtomicBool>,
    pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
    control_failure_uninitialized: Arc<AtomicBool>,
    management_write_recovery_inflight: Arc<AtomicBool>,
    source_fail_closed_reinitialize_required: Arc<AtomicBool>,
    source_generation_cutover_replay_deferred: Arc<AtomicBool>,
    deferred_source_repair_recovery_scheduled: Arc<AtomicBool>,
    deferred_sink_repair_recovery_scheduled: Arc<AtomicBool>,
    sink_generation_cutover_replay_deferred: Arc<AtomicBool>,
    api_request_tracker: Arc<ApiRequestTracker>,
    api_control_gate: Arc<ApiControlGate>,
    control_frame_serial: Arc<Mutex<()>>,
    shared_control_frame_serial: Arc<Mutex<()>>,
    facade_pending_status: SharedFacadePendingStatusCell,
    facade_service_state: SharedFacadeServiceStateCell,
    rollout_status: SharedRolloutStatusCell,
    facade_gate: RuntimeUnitGate,
    source_rescan_proxy_ready_generation: Arc<AtomicU64>,
    source_logical_roots_control_generation: Arc<AtomicU64>,
    sink_logical_roots_control_generation: Arc<AtomicU64>,
    mirrored_query_peer_routes: Arc<Mutex<std::collections::BTreeMap<String, u64>>>,
    runtime_gate_state: Arc<StdMutex<RuntimeControlState>>,
    retained_sink_control_state: Arc<Mutex<RetainedSinkControlState>>,
    source_scoped_sink_observation_repair_signature:
        Arc<StdMutex<Option<SourceScopedSinkObservationRepairSignature>>>,
    runtime_state_changed: Arc<tokio::sync::Notify>,
    retained_suppressed_public_query_activates:
        Mutex<std::collections::BTreeMap<(String, String), FacadeControlSignal>>,
    shared_source_route_claims: Arc<Mutex<SharedSourceRouteClaims>>,
    shared_sink_route_claims: Arc<Mutex<SharedSinkRouteClaims>>,
    control_frame_lease_path: Option<std::path::PathBuf>,
    control_init_lock: Mutex<()>,
    closing: AtomicBool,
}

#[derive(Default, Clone)]
struct RetainedSinkControlState {
    latest_host_grant_change: Option<SinkControlSignal>,
    active_by_route: std::collections::BTreeMap<(String, String), SinkControlSignal>,
}

#[derive(Debug)]
enum RuntimeWorkerObservationFailure {
    Source(SourceFailure),
    Sink(SinkFailure),
    Other(CnxError),
}

impl RuntimeWorkerObservationFailure {
    fn from_cause(cause: CnxError) -> Self {
        Self::Other(cause)
    }

    fn into_error(self) -> CnxError {
        match self {
            Self::Source(err) => err.into_error(),
            Self::Sink(err) => err.into_error(),
            Self::Other(err) => err,
        }
    }
}

impl From<SourceFailure> for RuntimeWorkerObservationFailure {
    fn from(value: SourceFailure) -> Self {
        Self::Source(value)
    }
}

impl From<SinkFailure> for RuntimeWorkerObservationFailure {
    fn from(value: SinkFailure) -> Self {
        Self::Sink(value)
    }
}

impl From<CnxError> for RuntimeWorkerObservationFailure {
    fn from(value: CnxError) -> Self {
        Self::Other(value)
    }
}

#[derive(Debug)]
enum RuntimeWorkerControlApplyFailure {
    Source(SourceFailure),
    Sink(SinkFailure),
    Initialize(RuntimeInitializeFailure),
    Other(CnxError),
}

impl RuntimeWorkerControlApplyFailure {
    fn as_error(&self) -> &CnxError {
        match self {
            Self::Source(err) => err.as_error(),
            Self::Sink(err) => err.as_error(),
            Self::Initialize(err) => match err {
                RuntimeInitializeFailure::Source(source) => source.as_error(),
                RuntimeInitializeFailure::Sink(sink) => sink.as_error(),
                RuntimeInitializeFailure::Other(err) => err,
            },
            Self::Other(err) => err,
        }
    }

    fn into_error(self) -> CnxError {
        match self {
            Self::Source(err) => err.into_error(),
            Self::Sink(err) => err.into_error(),
            Self::Initialize(err) => err.into_error(),
            Self::Other(err) => err,
        }
    }
}

impl From<SourceFailure> for RuntimeWorkerControlApplyFailure {
    fn from(value: SourceFailure) -> Self {
        Self::Source(value)
    }
}

impl From<SinkFailure> for RuntimeWorkerControlApplyFailure {
    fn from(value: SinkFailure) -> Self {
        Self::Sink(value)
    }
}

impl From<RuntimeInitializeFailure> for RuntimeWorkerControlApplyFailure {
    fn from(value: RuntimeInitializeFailure) -> Self {
        Self::Initialize(value)
    }
}

#[derive(Debug)]
enum RuntimeControlFrameFailure {
    Observation(RuntimeWorkerObservationFailure),
    ControlApply(RuntimeWorkerControlApplyFailure),
    Initialize(RuntimeInitializeFailure),
    Other(CnxError),
}

trait HostGrantFastLaneSignal: Sized {
    fn record_retained<'a>(
        app: &'a FSMetaApp,
        signals: &'a [Self],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;
}

impl HostGrantFastLaneSignal for SourceControlSignal {
    fn record_retained<'a>(
        app: &'a FSMetaApp,
        signals: &'a [Self],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move { app.record_retained_source_control_state(signals).await })
    }
}

impl HostGrantFastLaneSignal for SinkControlSignal {
    fn record_retained<'a>(
        app: &'a FSMetaApp,
        signals: &'a [Self],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move { app.record_retained_sink_control_state(signals).await })
    }
}

impl RuntimeControlFrameFailure {
    fn into_error(self) -> CnxError {
        match self {
            Self::Observation(err) => err.into_error(),
            Self::ControlApply(err) => err.into_error(),
            Self::Initialize(err) => err.into_error(),
            Self::Other(err) => err,
        }
    }
}

impl From<RuntimeWorkerObservationFailure> for RuntimeControlFrameFailure {
    fn from(value: RuntimeWorkerObservationFailure) -> Self {
        Self::Observation(value)
    }
}

impl From<RuntimeWorkerControlApplyFailure> for RuntimeControlFrameFailure {
    fn from(value: RuntimeWorkerControlApplyFailure) -> Self {
        Self::ControlApply(value)
    }
}

impl From<CnxError> for RuntimeControlFrameFailure {
    fn from(value: CnxError) -> Self {
        Self::Other(value)
    }
}

#[derive(Debug)]
enum RuntimeInitializeFailure {
    Source(SourceFailure),
    Sink(SinkFailure),
    Other(CnxError),
}

impl RuntimeInitializeFailure {
    fn as_error(&self) -> &CnxError {
        match self {
            Self::Source(err) => err.as_error(),
            Self::Sink(err) => err.as_error(),
            Self::Other(err) => err,
        }
    }

    fn into_error(self) -> CnxError {
        match self {
            Self::Source(err) => err.into_error(),
            Self::Sink(err) => err.into_error(),
            Self::Other(err) => err,
        }
    }
}

impl From<SourceFailure> for RuntimeInitializeFailure {
    fn from(value: SourceFailure) -> Self {
        Self::Source(value)
    }
}

impl From<SinkFailure> for RuntimeInitializeFailure {
    fn from(value: SinkFailure) -> Self {
        Self::Sink(value)
    }
}

impl From<CnxError> for RuntimeInitializeFailure {
    fn from(value: CnxError) -> Self {
        Self::Other(value)
    }
}

impl From<RuntimeInitializeFailure> for RuntimeControlFrameFailure {
    fn from(value: RuntimeInitializeFailure) -> Self {
        Self::Initialize(value)
    }
}

#[derive(Debug)]
enum RuntimeCloseFailure {
    Source(SourceFailure),
    Sink(SinkFailure),
    Other(CnxError),
}

impl RuntimeCloseFailure {
    fn into_error(self) -> CnxError {
        match self {
            Self::Source(err) => err.into_error(),
            Self::Sink(err) => err.into_error(),
            Self::Other(err) => err,
        }
    }
}

impl From<SourceFailure> for RuntimeCloseFailure {
    fn from(value: SourceFailure) -> Self {
        Self::Source(value)
    }
}

impl From<SinkFailure> for RuntimeCloseFailure {
    fn from(value: SinkFailure) -> Self {
        Self::Sink(value)
    }
}

impl From<CnxError> for RuntimeCloseFailure {
    fn from(value: CnxError) -> Self {
        Self::Other(value)
    }
}

#[derive(Debug)]
enum RuntimeServiceIoFailure {
    Sink(SinkFailure),
    Other(CnxError),
}

impl RuntimeServiceIoFailure {
    fn into_error(self) -> CnxError {
        match self {
            Self::Sink(err) => err.into_error(),
            Self::Other(err) => err,
        }
    }
}

impl From<SinkFailure> for RuntimeServiceIoFailure {
    fn from(value: SinkFailure) -> Self {
        Self::Sink(value)
    }
}

impl From<CnxError> for RuntimeServiceIoFailure {
    fn from(value: CnxError) -> Self {
        Self::Other(value)
    }
}

#[derive(Debug)]
enum RuntimeAppQueryFailure {
    Sink(SinkFailure),
    Other(CnxError),
}

impl RuntimeAppQueryFailure {
    fn from_cause(cause: CnxError) -> Self {
        Self::Other(cause)
    }

    fn into_error(self) -> CnxError {
        match self {
            Self::Sink(failure) => failure.into_error(),
            Self::Other(cause) => cause,
        }
    }
}

impl From<SinkFailure> for RuntimeAppQueryFailure {
    fn from(value: SinkFailure) -> Self {
        Self::Sink(value)
    }
}

impl From<CnxError> for RuntimeWorkerControlApplyFailure {
    fn from(value: CnxError) -> Self {
        Self::Other(value)
    }
}

impl FSMetaApp {
    fn management_write_recovery_context(&self) -> ManagementWriteRecoveryContext {
        ManagementWriteRecoveryContext {
            instance_id: self.instance_id,
            node_id: self.node_id.clone(),
            source: self.source.clone(),
            sink: self.sink.clone(),
            api_task: self.api_task.clone(),
            pending_facade: self.pending_facade.clone(),
            facade_pending_status: self.facade_pending_status.clone(),
            facade_service_state: self.facade_service_state.clone(),
            api_control_gate: self.api_control_gate.clone(),
            control_failure_uninitialized: self.control_failure_uninitialized.clone(),
            runtime_gate_state: self.runtime_gate_state.clone(),
            runtime_state_changed: self.runtime_state_changed.clone(),
            retained_sink_control_state: self.retained_sink_control_state.clone(),
            source_scoped_sink_observation_repair_signature: self
                .source_scoped_sink_observation_repair_signature
                .clone(),
            pending_fixed_bind_has_suppressed_dependent_routes: self
                .pending_fixed_bind_has_suppressed_dependent_routes
                .clone(),
            source_generation_cutover_replay_deferred: self
                .source_generation_cutover_replay_deferred
                .clone(),
            sink_generation_cutover_replay_deferred: self
                .sink_generation_cutover_replay_deferred
                .clone(),
            facade_gate: self.facade_gate.clone(),
            source_rescan_proxy_ready_generation: self.source_rescan_proxy_ready_generation.clone(),
            runtime_boundary: self.runtime_boundary.clone(),
            runtime_endpoint_tasks: self.runtime_endpoint_tasks.clone(),
            control_frame_serial: self.control_frame_serial.clone(),
            inflight: self.management_write_recovery_inflight.clone(),
        }
    }

    fn management_write_recovery(&self) -> ManagementWriteRecovery {
        let context = self.management_write_recovery_context();
        Arc::new(move || {
            let context = context.clone();
            Box::pin(async move { context.run().await })
        })
    }

    fn source_repair_recovery(&self) -> ManagementWriteRecovery {
        let context = self.management_write_recovery_context();
        Arc::new(move || {
            let context = context.clone();
            Box::pin(async move { context.run_source_repair().await })
        })
    }

    fn source_repair_recovery_without_proxy_ready(&self) -> ManagementWriteRecovery {
        let context = self.management_write_recovery_context();
        Arc::new(move || {
            let context = context.clone();
            Box::pin(async move { context.run_source_repair_without_proxy_ready().await })
        })
    }

    fn sink_repair_recovery(&self) -> ManagementWriteRecovery {
        let context = self.management_write_recovery_context();
        Arc::new(move || {
            let context = context.clone();
            Box::pin(async move { context.run_sink_repair().await })
        })
    }

    fn sink_observation_repair_recovery(&self) -> ManagementWriteRecovery {
        let context = self.management_write_recovery_context();
        Arc::new(move || {
            let context = context.clone();
            Box::pin(async move { context.run_source_scoped_sink_observation_repair().await })
        })
    }

    fn source_repair_still_required(
        runtime_gate_state: &Arc<StdMutex<RuntimeControlState>>,
    ) -> bool {
        RuntimeControlState::from_state_cell(runtime_gate_state).source_state_replay_required()
    }

    fn sink_repair_still_required(runtime_gate_state: &Arc<StdMutex<RuntimeControlState>>) -> bool {
        RuntimeControlState::from_state_cell(runtime_gate_state).sink_state_replay_required()
    }

    fn schedule_deferred_source_repair_recovery(&self, reason: &'static str) {
        if self.closing.load(Ordering::Acquire) {
            return;
        }
        if self
            .deferred_source_repair_recovery_scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let recovery = self.source_repair_recovery();
        let runtime_gate_state = self.runtime_gate_state.clone();
        let runtime_state_changed = self.runtime_state_changed.clone();
        let scheduled = self.deferred_source_repair_recovery_scheduled.clone();
        tokio::spawn(async move {
            eprintln!(
                "fs_meta_runtime_app: deferred source repair scheduled reason={}",
                reason
            );
            let deadline = tokio::time::Instant::now() + SOURCE_CONTROL_RECOVERY_TOTAL_TIMEOUT;
            loop {
                if !Self::source_repair_still_required(&runtime_gate_state) {
                    eprintln!(
                        "fs_meta_runtime_app: deferred source repair done reason={}",
                        reason
                    );
                    scheduled.store(false, Ordering::Release);
                    return;
                }

                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    eprintln!(
                        "fs_meta_runtime_app: deferred source repair timed out reason={}",
                        reason
                    );
                    scheduled.store(false, Ordering::Release);
                    return;
                }

                tokio::select! {
                    _ = runtime_state_changed.notified() => {
                        continue;
                    }
                    _ = tokio::time::sleep(DEFERRED_SOURCE_REPAIR_QUIET_WINDOW.min(remaining)) => {}
                }

                match tokio::time::timeout(remaining, recovery()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        eprintln!(
                            "fs_meta_runtime_app: deferred source repair failed reason={} err={}",
                            reason, err
                        );
                    }
                    Err(_) => {
                        eprintln!(
                            "fs_meta_runtime_app: deferred source repair timed out reason={}",
                            reason
                        );
                        scheduled.store(false, Ordering::Release);
                        return;
                    }
                }

                if !Self::source_repair_still_required(&runtime_gate_state) {
                    eprintln!(
                        "fs_meta_runtime_app: deferred source repair done reason={}",
                        reason
                    );
                    scheduled.store(false, Ordering::Release);
                    return;
                }

                tokio::select! {
                    _ = runtime_state_changed.notified() => {}
                    _ = tokio::time::sleep(DEFERRED_SOURCE_REPAIR_RETRY_INTERVAL) => {}
                }
            }
        });
    }

    fn schedule_deferred_sink_repair_recovery(&self, reason: &'static str) {
        if self.closing.load(Ordering::Acquire) {
            return;
        }
        if self
            .deferred_sink_repair_recovery_scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let recovery = self.sink_repair_recovery();
        let runtime_gate_state = self.runtime_gate_state.clone();
        let runtime_state_changed = self.runtime_state_changed.clone();
        let scheduled = self.deferred_sink_repair_recovery_scheduled.clone();
        tokio::spawn(async move {
            eprintln!(
                "fs_meta_runtime_app: deferred sink repair scheduled reason={}",
                reason
            );
            let deadline = tokio::time::Instant::now() + SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT;
            loop {
                if !Self::sink_repair_still_required(&runtime_gate_state) {
                    eprintln!(
                        "fs_meta_runtime_app: deferred sink repair done reason={}",
                        reason
                    );
                    scheduled.store(false, Ordering::Release);
                    return;
                }

                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    eprintln!(
                        "fs_meta_runtime_app: deferred sink repair timed out reason={}",
                        reason
                    );
                    scheduled.store(false, Ordering::Release);
                    return;
                }

                match tokio::time::timeout(remaining, recovery()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        eprintln!(
                            "fs_meta_runtime_app: deferred sink repair failed reason={} err={}",
                            reason, err
                        );
                    }
                    Err(_) => {
                        eprintln!(
                            "fs_meta_runtime_app: deferred sink repair timed out reason={}",
                            reason
                        );
                        scheduled.store(false, Ordering::Release);
                        return;
                    }
                }

                if !Self::sink_repair_still_required(&runtime_gate_state) {
                    eprintln!(
                        "fs_meta_runtime_app: deferred sink repair done reason={}",
                        reason
                    );
                    scheduled.store(false, Ordering::Release);
                    return;
                }

                tokio::select! {
                    _ = runtime_state_changed.notified() => {}
                    _ = tokio::time::sleep(DEFERRED_SINK_REPAIR_RETRY_INTERVAL) => {}
                }
            }
        });
    }

    #[cfg(test)]
    fn management_write_recovery_for_tests(&self) -> ManagementWriteRecovery {
        self.management_write_recovery()
    }

    #[cfg(test)]
    fn source_repair_recovery_for_tests(&self) -> ManagementWriteRecovery {
        self.source_repair_recovery()
    }

    #[cfg(test)]
    fn sink_observation_repair_recovery_for_tests(&self) -> ManagementWriteRecovery {
        self.sink_observation_repair_recovery()
    }

    fn try_begin_management_write_recovery(&self) -> Option<ManagementWriteRecoveryInflightGuard> {
        self.management_write_recovery_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| ManagementWriteRecoveryInflightGuard {
                inflight: self.management_write_recovery_inflight.clone(),
            })
    }

    async fn publish_recovered_facade_state_after_retained_replay(&self) {
        let observation = Self::observe_facade_gate_from_parts(
            self.instance_id,
            self.api_task.clone(),
            self.pending_facade.clone(),
            Some(&self.facade_pending_status),
            self.runtime_control_state(),
            self.pending_fixed_bind_has_suppressed_dependent_routes
                .load(Ordering::Acquire),
            FacadeOnlyHandoffObservationPolicy::ForceBlocked,
        )
        .await;
        let publication_ready = matches!(
            observation.runtime.facade_publication_readiness_decision(
                observation.current_pending.is_some(),
                observation.pending_facade_is_control_route,
                observation.active_control_stream_present,
                observation.active_pending_control_stream_present,
                observation.allow_facade_only_handoff,
            ),
            FacadePublicationReadinessDecision::Ready
        );
        let snapshot = Self::fixed_bind_gate_publication_snapshot(observation, publication_ready);
        Self::publish_facade_publication_snapshot(
            &self.facade_service_state,
            &self.api_control_gate,
            snapshot,
        );
    }

    fn source_observation_supports_source_repair_ready(
        snapshot: &crate::workers::source::SourceObservabilitySnapshot,
    ) -> bool {
        let has_runtime_scope_routes = !snapshot.scheduled_source_groups_by_node.is_empty()
            || !snapshot.scheduled_scan_groups_by_node.is_empty();
        let has_only_runtime_scope_route_lag =
            snapshot
                .status
                .degraded_roots
                .iter()
                .all(|(root_key, reason)| {
                    root_key == "source-worker"
                        && (reason == SOURCE_WORKER_PENDING_SOURCE_STATE_OBSERVATION_REASON
                            || reason == SOURCE_WORKER_RUNTIME_SCOPE_CACHE_REASON)
                });

        !source_observability_snapshot_is_degraded_worker_cache(snapshot)
            && !snapshot
                .status
                .degraded_roots
                .iter()
                .any(|(_, reason)| reason == SOURCE_WORKER_PENDING_SOURCE_STATE_OBSERVATION_REASON)
            && (snapshot.lifecycle_state == "ready"
                || snapshot.lifecycle_state == "runtime-scope-ready")
            || snapshot.lifecycle_state == "runtime-scope-ready"
                && has_runtime_scope_routes
                && has_only_runtime_scope_route_lag
    }

    async fn source_repair_ready_from_runtime_or_observation(&self) -> bool {
        if self.runtime_control_state().source_repair_ready() {
            return true;
        }
        let probe_budget =
            MANUAL_RESCAN_SOURCE_STATUS_DEFAULT_PROBE_BUDGET.min(Duration::from_secs(5));
        if self.runtime_control_state().source_state_replay_required() {
            return self
                .source_repair_ready_from_pending_source_observation(probe_budget)
                .await;
        }
        if self.source.retained_replay_required().await {
            return self
                .source_repair_ready_from_pending_source_observation(probe_budget)
                .await;
        }
        match tokio::time::timeout(
            probe_budget,
            self.source.observability_snapshot_with_failure(),
        )
        .await
        {
            Ok(Ok(snapshot)) => Self::source_observation_supports_source_repair_ready(&snapshot),
            Ok(Err(err)) => {
                eprintln!(
                    "fs_meta_runtime_app: source repair gate observation failed err={}",
                    err.as_error()
                );
                false
            }
            Err(_) => {
                eprintln!("fs_meta_runtime_app: source repair gate observation timed out");
                false
            }
        }
    }

    async fn source_repair_ready_from_pending_source_observation(
        &self,
        probe_budget: Duration,
    ) -> bool {
        match tokio::time::timeout(
            probe_budget,
            self.source
                .source_state_pending_observability_snapshot_for_status_route(),
        )
        .await
        {
            Ok((snapshot, _used_cached_fallback)) => {
                Self::source_observation_supports_source_repair_ready(&snapshot)
            }
            Err(_) => {
                eprintln!("fs_meta_runtime_app: source repair gate observation timed out");
                false
            }
        }
    }

    async fn publish_source_repair_gate_after_source_state_current(&self) {
        let source_repair_ready = self.source_repair_ready_from_runtime_or_observation().await;
        self.api_control_gate.set_ready_state_with_source_repair(
            self.api_control_gate.is_ready(),
            self.api_control_gate.is_management_write_ready(),
            source_repair_ready,
        );
    }

    async fn finish_source_control_apply_and_publish_source_repair(
        &self,
        source_control_apply_inflight: &mut Option<SourceControlApplyInflightGuard>,
        start_source_owned_endpoints: bool,
    ) -> std::result::Result<(), RuntimeWorkerControlApplyFailure> {
        if let Some(inflight) = source_control_apply_inflight.take() {
            drop(inflight);
            if start_source_owned_endpoints {
                self.ensure_runtime_endpoints_started().await?;
            }
            self.publish_source_repair_gate_after_source_state_current()
                .await;
        }
        Ok(())
    }

    async fn recover_retained_replay_readiness_inside_control_frame(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
    ) -> std::result::Result<(), RuntimeControlFrameFailure> {
        self.recover_retained_replay_readiness_with_policy(fixed_bind_session, false, false)
            .await
    }

    async fn recover_retained_replay_readiness_from_runtime_unit_exposure(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
    ) -> std::result::Result<(), RuntimeControlFrameFailure> {
        self.recover_retained_replay_readiness_with_policy(fixed_bind_session, true, false)
            .await
    }

    async fn recover_retained_replay_readiness_with_policy(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        apply_deferred_source_replay: bool,
        apply_deferred_sink_replay: bool,
    ) -> std::result::Result<(), RuntimeControlFrameFailure> {
        let Some(_inflight) = self.try_begin_management_write_recovery() else {
            return Ok(());
        };
        let state_at_entry = self.runtime_control_state();
        if state_at_entry.control_gate_ready(false)
            || (!state_at_entry.source_state_replay_required()
                && !state_at_entry.sink_state_replay_required())
        {
            return Ok(());
        }

        if state_at_entry.source_state_replay_required() {
            if !apply_deferred_source_replay
                && self
                    .source_generation_cutover_replay_should_defer_inline(&[])
                    .await
            {
                self.mark_control_uninitialized_after_deferred_source_replay_without_scheduled_repair_with_session(
                    fixed_bind_session,
                    state_at_entry.sink_state_replay_required(),
                )
                .await;
                self.publish_recovered_facade_state_after_retained_replay()
                    .await;
                eprintln!(
                    "fs_meta_runtime_app: retained source generation cutover replay kept out of control-frame recovery"
                );
                return Ok(());
            }
            self.apply_source_signals_with_recovery(
                fixed_bind_session,
                &[],
                false,
                SourceGenerationCutoverDisposition::None,
                state_at_entry.sink_state_replay_required(),
            )
            .await?;
            if self.runtime_control_state().source_state_replay_required() {
                self.publish_recovered_facade_state_after_retained_replay()
                    .await;
                return Ok(());
            }
        }

        if self.runtime_control_state().sink_state_replay_required() {
            if !apply_deferred_sink_replay
                && self
                    .sink_generation_cutover_replay_should_defer_inline()
                    .await
            {
                self.mark_control_uninitialized_after_deferred_sink_replay_with_session(
                    fixed_bind_session,
                )
                .await;
                self.publish_recovered_facade_state_after_retained_replay()
                    .await;
                eprintln!(
                    "fs_meta_runtime_app: retained sink generation cutover replay kept out of control-frame recovery"
                );
                return Ok(());
            }
            if apply_deferred_sink_replay {
                eprintln!(
                    "fs_meta_runtime_app: retained sink generation cutover replay running from runtime-unit exposure recovery"
                );
            }
            self.apply_sink_signals_with_recovery(fixed_bind_session, &[], true, false, false)
                .await?;
            if self.runtime_control_state().sink_state_replay_required() {
                self.publish_recovered_facade_state_after_retained_replay()
                    .await;
                return Ok(());
            }
        }

        if self.runtime_control_state().replay_fully_cleared() {
            self.update_runtime_control_state(|state| state.mark_initialized());
            self.control_failure_uninitialized
                .store(false, Ordering::Release);
        }
        self.publish_recovered_facade_state_after_retained_replay()
            .await;
        Ok(())
    }

    fn sink_status_snapshot_summary_is_empty(snapshot: &crate::sink::SinkStatusSnapshot) -> bool {
        snapshot.progress_snapshot().empty_summary
    }

    fn sink_status_snapshot_has_scheduled_only_republish_evidence(
        snapshot: &crate::sink::SinkStatusSnapshot,
    ) -> bool {
        let progress = snapshot.progress_snapshot();
        snapshot.groups.is_empty() && !progress.scheduled_groups.is_empty()
    }

    fn sink_status_snapshot_has_pending_republish_evidence(
        snapshot: &crate::sink::SinkStatusSnapshot,
    ) -> bool {
        matches!(
            snapshot.concern_projection().concern,
            Some(crate::sink::SinkStatusConcern::ReplayPending)
                | Some(crate::sink::SinkStatusConcern::MixedReadiness)
        )
    }

    fn sink_status_snapshot_has_pending_materialization_progress_for_expected_groups(
        snapshot: &crate::sink::SinkStatusSnapshot,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> bool {
        let projection = snapshot.concern_projection();
        !expected_groups.is_empty()
            && expected_groups.is_subset(&projection.summary.scheduled_groups)
            && expected_groups.is_disjoint(&projection.summary.missing_scheduled_groups)
            && !expected_groups.is_disjoint(&projection.summary.pending_materialization_groups)
            && FSMetaApp::sink_status_snapshot_has_delivery_evidence_for_republish(snapshot)
    }

    fn sink_status_snapshot_has_materialized_pending_progress_for_expected_groups(
        snapshot: &crate::sink::SinkStatusSnapshot,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> bool {
        let projection = snapshot.concern_projection();
        !expected_groups.is_empty()
            && expected_groups.is_subset(&projection.summary.scheduled_groups)
            && expected_groups.is_disjoint(&projection.summary.missing_scheduled_groups)
            && !expected_groups.is_disjoint(&projection.summary.pending_materialization_groups)
            && snapshot.groups.iter().any(|group| {
                expected_groups.contains(&group.group_id)
                    && matches!(
                        group.normalized_readiness(),
                        crate::sink::GroupReadinessState::PendingMaterialization
                    )
                    && (group.live_nodes > 0
                        || group.total_nodes > 0
                        || group.materialized_revision > 0)
            })
    }

    fn source_status_snapshot_has_inflight_initial_audit_for_expected_groups(
        snapshot: &crate::source::SourceStatusSnapshot,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> bool {
        !expected_groups.is_empty()
            && expected_groups.iter().all(|expected_group| {
                snapshot.concrete_roots.iter().any(|root| {
                    root.logical_root_id == *expected_group
                        && root.active
                        && root.is_group_primary
                        && root.scan_enabled
                        && root.last_error.is_none()
                        && root.last_audit_started_at_us.is_some()
                        && root.last_audit_completed_at_us.is_none()
                })
            })
    }

    fn sink_status_snapshot_has_stale_group_republish_evidence(
        snapshot: &crate::sink::SinkStatusSnapshot,
    ) -> bool {
        !snapshot.groups.is_empty()
            && matches!(
                snapshot.concern_projection().concern,
                Some(crate::sink::SinkStatusConcern::StaleEmpty)
            )
    }

    fn sink_status_snapshot_has_incomplete_scheduled_group_republish_evidence(
        snapshot: &crate::sink::SinkStatusSnapshot,
    ) -> bool {
        let projection = snapshot.concern_projection();
        !snapshot.groups.is_empty()
            && !projection.summary.scheduled_groups.is_empty()
            && projection.summary.missing_scheduled_groups.is_empty()
            && projection.summary.ready_groups != projection.summary.scheduled_groups
    }

    fn sink_status_snapshot_has_delivery_evidence_for_republish(
        snapshot: &crate::sink::SinkStatusSnapshot,
    ) -> bool {
        !snapshot.received_batches_by_node.is_empty()
            || !snapshot.received_events_by_node.is_empty()
            || !snapshot.received_control_events_by_node.is_empty()
            || !snapshot.received_data_events_by_node.is_empty()
            || !snapshot.last_received_at_us_by_node.is_empty()
            || !snapshot.last_received_origins_by_node.is_empty()
            || !snapshot.received_origin_counts_by_node.is_empty()
            || !snapshot.stream_received_batches_by_node.is_empty()
            || !snapshot.stream_received_events_by_node.is_empty()
            || !snapshot.stream_received_origin_counts_by_node.is_empty()
            || !snapshot
                .stream_received_path_origin_counts_by_node
                .is_empty()
            || !snapshot.stream_ready_origin_counts_by_node.is_empty()
            || !snapshot.stream_ready_path_origin_counts_by_node.is_empty()
            || !snapshot.stream_deferred_origin_counts_by_node.is_empty()
            || !snapshot.stream_dropped_origin_counts_by_node.is_empty()
            || !snapshot.stream_applied_batches_by_node.is_empty()
            || !snapshot.stream_applied_events_by_node.is_empty()
            || !snapshot.stream_applied_control_events_by_node.is_empty()
            || !snapshot.stream_applied_data_events_by_node.is_empty()
            || !snapshot.stream_applied_origin_counts_by_node.is_empty()
            || !snapshot
                .stream_applied_path_origin_counts_by_node
                .is_empty()
            || !snapshot.stream_last_applied_at_us_by_node.is_empty()
    }

    fn sink_status_snapshot_has_replaced_schedule_republish_evidence(
        snapshot: &crate::sink::SinkStatusSnapshot,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> bool {
        let scheduled_groups = snapshot.scheduled_groups();
        !expected_groups.is_empty()
            && !scheduled_groups.is_empty()
            && scheduled_groups != *expected_groups
            && FSMetaApp::sink_status_snapshot_has_delivery_evidence_for_republish(snapshot)
    }

    fn sink_status_snapshot_has_retained_replay_ready_republish_evidence(
        snapshot: &crate::sink::SinkStatusSnapshot,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> bool {
        !expected_groups.is_empty()
            && sink_status_snapshot_ready_for_expected_groups(snapshot, expected_groups)
            && FSMetaApp::sink_status_snapshot_has_delivery_evidence_for_republish(snapshot)
    }

    fn sink_status_snapshot_should_defer_republish_after_retained_replay_timeout(
        snapshot: &crate::sink::SinkStatusSnapshot,
    ) -> bool {
        FSMetaApp::sink_status_snapshot_summary_is_empty(snapshot)
            || FSMetaApp::sink_status_snapshot_has_scheduled_only_republish_evidence(snapshot)
            || FSMetaApp::sink_status_snapshot_has_pending_republish_evidence(snapshot)
            || FSMetaApp::sink_status_snapshot_has_stale_group_republish_evidence(snapshot)
            || FSMetaApp::sink_status_snapshot_has_incomplete_scheduled_group_republish_evidence(
                snapshot,
            )
    }

    fn sink_status_snapshot_should_defer_local_republish_after_retained_replay(
        snapshot: &crate::sink::SinkStatusSnapshot,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> bool {
        FSMetaApp::sink_status_snapshot_has_scheduled_only_republish_evidence(snapshot)
            || FSMetaApp::sink_status_snapshot_has_replaced_schedule_republish_evidence(
                snapshot,
                expected_groups,
            )
            || FSMetaApp::sink_status_snapshot_has_retained_replay_ready_republish_evidence(
                snapshot,
                expected_groups,
            )
            || FSMetaApp::sink_status_snapshot_has_incomplete_scheduled_group_republish_evidence(
                snapshot,
            )
    }

    async fn wait_for_runtime_progress_observation_window_until<F, Fut>(
        runtime_state_changed: &Arc<tokio::sync::Notify>,
        deadline: tokio::time::Instant,
        observe: F,
    ) where
        F: FnOnce(Duration) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        let wait_budget = remaining.min(Duration::from_millis(350));
        let notified = runtime_state_changed.notified();
        tokio::pin!(notified);
        tokio::select! {
            _ = &mut notified => {}
            _ = async {
                let observation = tokio::time::timeout(wait_budget, observe(wait_budget));
                let quiet_window = tokio::time::sleep(wait_budget);
                tokio::pin!(observation);
                tokio::pin!(quiet_window);
                let _ = tokio::join!(observation, quiet_window);
            } => {}
        }
    }

    async fn observe_runtime_scope_convergence(
        source: &Arc<SourceFacade>,
        sink: &Arc<SinkFacade>,
    ) -> std::result::Result<RuntimeScopeConvergenceObservation, RuntimeWorkerObservationFailure>
    {
        Ok(RuntimeScopeConvergenceObservation {
            source_groups: source
                .scheduled_source_group_ids_with_failure()
                .await
                .map_err(RuntimeWorkerObservationFailure::from)?
                .unwrap_or_default(),
            scan_groups: source
                .scheduled_scan_group_ids_with_failure()
                .await
                .map_err(RuntimeWorkerObservationFailure::from)?
                .unwrap_or_default(),
            sink_groups: sink
                .scheduled_group_ids_with_failure()
                .await
                .map_err(RuntimeWorkerObservationFailure::from)?
                .unwrap_or_default(),
        })
    }

    async fn wait_for_runtime_or_sink_progress_until(
        runtime_state_changed: &Arc<tokio::sync::Notify>,
        sink: &Arc<SinkFacade>,
        deadline: tokio::time::Instant,
    ) {
        Self::wait_for_runtime_progress_observation_window_until(
            runtime_state_changed,
            deadline,
            |sink_wait_budget| async move {
                let _ = tokio::time::timeout(sink_wait_budget, sink.status_snapshot_with_failure())
                    .await;
            },
        )
        .await;
    }

    async fn wait_for_runtime_source_or_sink_progress_until(
        runtime_state_changed: &Arc<tokio::sync::Notify>,
        source: &Arc<SourceFacade>,
        sink: &Arc<SinkFacade>,
        deadline: tokio::time::Instant,
    ) {
        Self::wait_for_runtime_progress_observation_window_until(
            runtime_state_changed,
            deadline,
            |wait_budget| async move {
                let _ = tokio::join!(
                    tokio::time::timeout(wait_budget, source.observability_snapshot_with_failure()),
                    tokio::time::timeout(wait_budget, sink.status_snapshot_with_failure()),
                );
            },
        )
        .await;
    }

    async fn wait_for_runtime_or_source_progress_until(
        runtime_state_changed: &Arc<tokio::sync::Notify>,
        source: &Arc<SourceFacade>,
        deadline: tokio::time::Instant,
    ) {
        Self::wait_for_runtime_progress_observation_window_until(
            runtime_state_changed,
            deadline,
            |wait_budget| async move {
                let _ =
                    tokio::time::timeout(wait_budget, source.observability_snapshot_with_failure())
                        .await;
            },
        )
        .await;
    }

    async fn wait_for_sink_status_republish_readiness_after_recovery(
        &self,
        source_to_sink_convergence_pretriggered_epoch: Option<u64>,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        SinkRecoveryMachine::new_post_recovery(source_to_sink_convergence_pretriggered_epoch)
            .run_post_recovery_readiness(&self.source, &self.sink, &self.runtime_state_changed)
            .await
            .map_err(RuntimeWorkerObservationFailure::into_error)
    }

    fn execute_local_sink_status_republish_until_terminal<'a>(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        runtime_state_changed: Arc<tokio::sync::Notify>,
        expected_groups: &'a std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &'a [SinkControlSignal],
        mode: LocalSinkStatusRepublishWaitMode,
        pretrigger_request_epoch: Option<u64>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let mut machine = match pretrigger_request_epoch {
                Some(request_epoch) => {
                    SinkRecoveryMachine::new_local_pretriggered(mode, request_epoch)
                }
                None => SinkRecoveryMachine::new_local(mode),
            };
            machine
                .run_local_republish(
                    &source,
                    &sink,
                    &runtime_state_changed,
                    expected_groups,
                    post_return_sink_replay_signals,
                )
                .await
                .map_err(RuntimeWorkerObservationFailure::into_error)
        })
    }

    fn wait_for_local_sink_status_republish_after_recovery_from_parts<'a>(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        runtime_state_changed: Arc<tokio::sync::Notify>,
        expected_groups: &'a std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &'a [SinkControlSignal],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Self::execute_local_sink_status_republish_until_terminal(
            source,
            sink,
            runtime_state_changed,
            expected_groups,
            post_return_sink_replay_signals,
            LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath,
            None,
        )
    }

    fn wait_for_local_sink_status_republish_after_recovery_requiring_probe_from_parts_with_pretrigger_epoch<
        'a,
    >(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        runtime_state_changed: Arc<tokio::sync::Notify>,
        expected_groups: &'a std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &'a [SinkControlSignal],
        pretrigger_request_epoch: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Self::execute_local_sink_status_republish_until_terminal(
            source,
            sink,
            runtime_state_changed,
            expected_groups,
            post_return_sink_replay_signals,
            LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady,
            Some(pretrigger_request_epoch),
        )
    }

    async fn probe_local_sink_status_republish_readiness(
        sink: &Arc<SinkFacade>,
        expected_groups: &std::collections::BTreeSet<String>,
        deadline: tokio::time::Instant,
    ) -> SinkRecoveryProbeOutcome {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if Self::nonblocking_sink_status_probe_ready_for_expected_groups(
            sink,
            expected_groups,
            remaining,
        )
        .await
        {
            return SinkRecoveryProbeOutcome::Ready;
        }
        let remaining_after_nonblocking_probe =
            deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining_after_nonblocking_probe.is_zero() {
            return SinkRecoveryProbeOutcome::NotReady;
        }
        if Self::blocking_sink_status_probe_ready_for_expected_groups(
            sink,
            expected_groups,
            remaining_after_nonblocking_probe,
        )
        .await
        {
            SinkRecoveryProbeOutcome::BlockingReady
        } else {
            SinkRecoveryProbeOutcome::NotReady
        }
    }

    async fn nonblocking_sink_status_probe_ready_for_expected_groups(
        sink: &Arc<SinkFacade>,
        expected_groups: &std::collections::BTreeSet<String>,
        remaining: Duration,
    ) -> bool {
        match tokio::time::timeout(remaining.min(Duration::from_millis(350)), async {
            sink.status_snapshot_nonblocking_with_failure().await
        })
        .await
        {
            Ok(Ok(snapshot)) => {
                sink_status_snapshot_ready_for_expected_groups(&snapshot, expected_groups)
                    || sink_status_snapshot_republish_observed_for_expected_groups(
                        &snapshot,
                        expected_groups,
                    )
            }
            Ok(Err(_)) | Err(_) => false,
        }
    }

    async fn blocking_sink_status_probe_ready_for_expected_groups(
        sink: &Arc<SinkFacade>,
        expected_groups: &std::collections::BTreeSet<String>,
        remaining: Duration,
    ) -> bool {
        match tokio::time::timeout(
            remaining.min(Duration::from_millis(350)),
            sink.status_snapshot_with_failure(),
        )
        .await
        {
            Ok(Ok(snapshot)) => {
                sink_status_snapshot_ready_for_expected_groups(&snapshot, expected_groups)
                    || sink_status_snapshot_republish_observed_for_expected_groups(
                        &snapshot,
                        expected_groups,
                    )
            }
            Ok(Err(_)) | Err(_) => false,
        }
    }

    fn wait_for_local_sink_status_republish_after_recovery_requiring_probe_from_parts<'a>(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        runtime_state_changed: Arc<tokio::sync::Notify>,
        expected_groups: &'a std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &'a [SinkControlSignal],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Self::execute_local_sink_status_republish_until_terminal(
            source,
            sink,
            runtime_state_changed,
            expected_groups,
            post_return_sink_replay_signals,
            LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady,
            None,
        )
    }

    async fn wait_for_local_sink_status_republish_after_recovery(
        &self,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        let post_return_sink_replay_signals = self
            .current_generation_retained_sink_replay_signals_for_local_republish()
            .await;
        Self::wait_for_local_sink_status_republish_after_recovery_from_parts(
            self.source.clone(),
            self.sink.clone(),
            self.runtime_state_changed.clone(),
            expected_groups,
            &post_return_sink_replay_signals,
        )
        .await
    }

    async fn current_generation_retained_sink_replay_signals_for_local_republish(
        &self,
    ) -> Vec<SinkControlSignal> {
        let retained_sink_generation = self
            .retained_sink_control_state
            .lock()
            .await
            .active_by_route
            .values()
            .filter_map(|signal| match signal {
                SinkControlSignal::Activate { generation, .. } => Some(*generation),
                _ => None,
            })
            .max();
        let retained_source_generation = self
            .source
            .control_signals_with_replay(&[])
            .await
            .iter()
            .map(source_signal_generation)
            .max();
        let generation = retained_sink_generation
            .into_iter()
            .chain(retained_source_generation)
            .max();
        let Some(generation) = generation else {
            return Vec::new();
        };
        let route_key = events_stream_route_for_scope(&self.node_id.0).0;
        let tick = SinkControlSignal::Tick {
            unit: SinkRuntimeUnit::Sink,
            route_key: route_key.clone(),
            generation,
            envelope: encode_runtime_unit_tick(&RuntimeUnitTick {
                route_key,
                unit_id: execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                generation,
                at_ms: 1,
            })
            .expect("encode deferred local sink-status replay tick"),
        };
        self.sink_signals_with_replay(std::slice::from_ref(&tick))
            .await
    }

    async fn determine_sink_recovery_gate_reopen_disposition(
        &self,
        replayed_sink_state_after_uninitialized_source_recovery: bool,
        public_facade_or_query_publication_present: bool,
        sink_status_publication_present: bool,
        source_signals: &[SourceControlSignal],
        sink_signals: &[SinkControlSignal],
        pretriggered_source_to_sink_convergence_epoch: Option<u64>,
        deferred_local_sink_replay_signals: &[SinkControlSignal],
    ) -> Result<SinkRecoveryGateReopenDisposition> {
        if !replayed_sink_state_after_uninitialized_source_recovery
            || !sink_status_publication_present
        {
            return Ok(SinkRecoveryGateReopenDisposition::None);
        }

        let source_led_uninitialized_mixed_recovery =
            !source_signals.is_empty() && sink_signals.is_empty();
        let source_led_expected_groups = if source_led_uninitialized_mixed_recovery {
            Self::runtime_scoped_facade_group_ids(&self.source, &self.sink)
                .await
                .map_err(RuntimeWorkerObservationFailure::into_error)?
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        } else {
            std::collections::BTreeSet::new()
        };
        let cached_sink_status_ready_for_expected_groups = !source_led_expected_groups.is_empty()
            && self
                .sink
                .cached_status_snapshot_with_failure()
                .ok()
                .is_some_and(|snapshot| {
                    sink_status_snapshot_ready_for_expected_groups(
                        &snapshot,
                        &source_led_expected_groups,
                    )
                });
        let recovered_expected_groups = if source_led_uninitialized_mixed_recovery {
            None
        } else {
            self.wait_for_sink_status_republish_readiness_after_recovery(
                pretriggered_source_to_sink_convergence_epoch,
            )
            .await?
        };
        let disposition = SinkRecoveryGateReopenDisposition::from_decision_input(
            SinkRecoveryGateReopenDecisionInput {
                source_led_uninitialized_mixed_recovery,
                public_facade_or_query_publication_present,
                deferred_local_sink_replay_present: !deferred_local_sink_replay_signals.is_empty(),
                cached_sink_status_ready_for_expected_groups,
                source_led_expected_groups,
                recovered_expected_groups,
            },
        );
        match &disposition {
            SinkRecoveryGateReopenDisposition::None
                if source_led_uninitialized_mixed_recovery
                    && cached_sink_status_ready_for_expected_groups =>
            {
                eprintln!(
                    "fs_meta_runtime_app: skipping deferred sink-status republish wait after source-led uninitialized mixed recovery"
                );
            }
            SinkRecoveryGateReopenDisposition::DeferGateReopenUntilSinkStatusReady {
                expected_groups,
            } if source_led_uninitialized_mixed_recovery => {
                eprintln!(
                    "fs_meta_runtime_app: deferring sink-status republish wait after source-led uninitialized mixed recovery groups={expected_groups:?}"
                );
            }
            SinkRecoveryGateReopenDisposition::None
            | SinkRecoveryGateReopenDisposition::WaitForLocalSinkStatusRepublish { .. }
            | SinkRecoveryGateReopenDisposition::DeferGateReopenUntilSinkStatusReady { .. } => {}
        }
        Ok(disposition)
    }

    async fn apply_sink_recovery_tail_plan(
        &self,
        mut fixed_bind_session: FixedBindLifecycleSession,
        request_sensitive: bool,
        sink_cleanup_only_while_uninitialized: bool,
        sink_recovery_tail_plan: SinkRecoveryTailPlan,
        _deferred_local_sink_replay_signals: Vec<SinkControlSignal>,
        facade_publication_signals: Vec<FacadeControlSignal>,
    ) -> Result<()> {
        let (deferred_sink_owned_query_peer_publication_signals, facade_publication_signals): (
            Vec<_>,
            Vec<_>,
        ) = if sink_recovery_tail_plan
            .gate_reopen_disposition
            .holds_sink_owned_query_peer_publication()
        {
            facade_publication_signals
                .into_iter()
                .partition(Self::facade_publication_signal_is_sink_owned_query_peer_activate)
        } else {
            (Vec::new(), facade_publication_signals)
        };
        let public_query_replay_followup_present = facade_publication_signals.iter().any(|signal| {
            matches!(
                signal,
                FacadeControlSignal::Activate {
                    unit,
                    route_key,
                    ..
                } if Self::suppressed_facade_business_read_activate_is_replayable(*unit, route_key)
            )
        });
        if !sink_cleanup_only_while_uninitialized && !public_query_replay_followup_present {
            let (_, session) = self
                .settle_fixed_bind_claim_release_followup_with_session(fixed_bind_session)
                .await?;
            fixed_bind_session = session;
        }
        for signal in facade_publication_signals {
            fixed_bind_session = self
                .apply_facade_signal_with_session(fixed_bind_session, signal)
                .await?;
        }
        fixed_bind_session = self
            .replay_suppressed_public_query_activates_after_publication_with_session(
                fixed_bind_session,
                public_query_replay_followup_present,
            )
            .await?;
        if !sink_cleanup_only_while_uninitialized {
            self.ensure_runtime_endpoints_started().await?;
        }
        for expected_groups in &sink_recovery_tail_plan.immediate_local_sink_status_republish_waits
        {
            self.wait_for_local_sink_status_republish_after_recovery(expected_groups)
                .await?;
        }
        if let Some(expected_groups) = sink_recovery_tail_plan
            .gate_reopen_disposition
            .local_wait_expected_groups()
        {
            let post_return_sink_replay_signals = self
                .current_generation_retained_sink_replay_signals_for_local_republish()
                .await;
            Self::wait_for_local_sink_status_republish_after_recovery_requiring_probe_from_parts(
                self.source.clone(),
                self.sink.clone(),
                self.runtime_state_changed.clone(),
                expected_groups,
                &post_return_sink_replay_signals,
            )
            .await?;
            self.update_runtime_control_state(|state| state.clear_sink_replay());
            self.sink_generation_cutover_replay_deferred
                .store(false, Ordering::Release);
            for signal in deferred_sink_owned_query_peer_publication_signals.clone() {
                Self::apply_deferred_sink_owned_query_peer_publication_signal_from_parts(
                    self.facade_gate.clone(),
                    self.mirrored_query_peer_routes.clone(),
                    signal,
                )
                .await?;
            }
            self.ensure_runtime_endpoints_started().await?;
            if self.runtime_control_state().replay_fully_cleared() {
                self.update_runtime_control_state(|state| state.mark_initialized());
                self.control_failure_uninitialized
                    .store(false, Ordering::Release);
                fixed_bind_session = self
                    .rebuild_fixed_bind_lifecycle_session(
                        &fixed_bind_session,
                        FixedBindLifecycleRebuildReason::PublishFacadeServiceStateAfterSinkRecoveryTail,
                    )
                    .await;
                let _ = self
                    .publish_facade_service_state_with_session(&fixed_bind_session)
                    .await;
            }
        }
        if request_sensitive && !sink_cleanup_only_while_uninitialized {
            if let Some(expected_groups) = sink_recovery_tail_plan
                .gate_reopen_disposition
                .deferred_expected_groups()
                .cloned()
            {
                let post_return_sink_replay_signals = self
                    .current_generation_retained_sink_replay_signals_for_local_republish()
                    .await;
                let pretrigger_request_epoch = Some(
                    self.source
                        .submit_rescan_when_ready_epoch_with_failure()
                        .await
                        .map_err(RuntimeWorkerObservationFailure::from)
                        .map_err(RuntimeWorkerObservationFailure::into_error)?,
                );
                self.suppress_deferred_sink_owned_query_peer_publication_signals(
                    &deferred_sink_owned_query_peer_publication_signals,
                )
                .await?;
                fixed_bind_session = self
                    .rebuild_fixed_bind_lifecycle_session(
                        &fixed_bind_session,
                        FixedBindLifecycleRebuildReason::PublishFacadeServiceStateAfterSinkRecoveryTailDeferredQueryPeerPublicationSuppression,
                    )
                    .await;
                let _ = self
                    .publish_facade_service_state_with_session(&fixed_bind_session)
                    .await;
                Self::spawn_deferred_sink_owned_query_peer_publication_after_local_sink_status_ready(
                    self.instance_id,
                    self.source.clone(),
                    self.sink.clone(),
                    expected_groups,
                    pretrigger_request_epoch,
                    post_return_sink_replay_signals,
                    deferred_sink_owned_query_peer_publication_signals,
                    self.facade_gate.clone(),
                    self.mirrored_query_peer_routes.clone(),
                    self.pending_facade.clone(),
                    self.facade_pending_status.clone(),
                    self.api_task.clone(),
                    self.api_control_gate.clone(),
                    self.facade_service_state.clone(),
                    self.runtime_gate_state.clone(),
                    self.runtime_state_changed.clone(),
                    self.pending_fixed_bind_has_suppressed_dependent_routes.clone(),
                    self.control_failure_uninitialized.clone(),
                );
            } else {
                fixed_bind_session = self
                    .rebuild_fixed_bind_lifecycle_session(
                        &fixed_bind_session,
                        FixedBindLifecycleRebuildReason::PublishFacadeServiceStateAfterSinkRecoveryTail,
                    )
                    .await;
                let _ = self
                    .publish_facade_service_state_with_session(&fixed_bind_session)
                    .await;
            }
        }
        Ok(())
    }

    async fn apply_deferred_sink_owned_query_peer_publication_signal_from_parts(
        facade_gate: RuntimeUnitGate,
        mirrored_query_peer_routes: Arc<Mutex<std::collections::BTreeMap<String, u64>>>,
        signal: FacadeControlSignal,
    ) -> Result<()> {
        let FacadeControlSignal::Activate {
            unit,
            route_key,
            generation,
            bound_scopes,
        } = signal
        else {
            return Ok(());
        };
        if !matches!(unit, FacadeRuntimeUnit::QueryPeer)
            || !facade_route_key_matches(unit, &route_key)
        {
            return Ok(());
        }
        let accepted =
            facade_gate.apply_activate(unit.unit_id(), &route_key, generation, &bound_scopes)?;
        if !accepted {
            return Ok(());
        }
        if is_dual_lane_internal_query_route(&route_key) {
            let query_route_active =
                facade_gate.route_active(execution_units::QUERY_RUNTIME_UNIT_ID, &route_key)?;
            let query_route_generation = facade_gate
                .route_generation(execution_units::QUERY_RUNTIME_UNIT_ID, &route_key)?
                .unwrap_or(0);
            let mut mirrored = mirrored_query_peer_routes.lock().await;
            let route_was_mirrored = mirrored.contains_key(&route_key);
            if route_was_mirrored || !query_route_active || query_route_generation < generation {
                facade_gate.apply_activate(
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    &route_key,
                    generation,
                    &bound_scopes,
                )?;
                mirrored.insert(route_key, generation);
            }
        }
        Ok(())
    }

    async fn suppress_deferred_sink_owned_query_peer_publication_signals(
        &self,
        signals: &[FacadeControlSignal],
    ) -> Result<()> {
        for signal in signals {
            let FacadeControlSignal::Activate {
                unit, route_key, ..
            } = signal
            else {
                continue;
            };
            if !matches!(unit, FacadeRuntimeUnit::QueryPeer)
                || !Self::facade_publication_signal_is_sink_owned_query_peer_activate(signal)
            {
                continue;
            }
            self.facade_gate
                .clear_route(execution_units::QUERY_PEER_RUNTIME_UNIT_ID, route_key)?;
            if is_dual_lane_internal_query_route(route_key) {
                self.mirrored_query_peer_routes
                    .lock()
                    .await
                    .remove(route_key);
            }
        }
        Ok(())
    }

    fn spawn_deferred_sink_owned_query_peer_publication_after_local_sink_status_ready(
        instance_id: u64,
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        expected_groups: std::collections::BTreeSet<String>,
        pretrigger_request_epoch: Option<u64>,
        post_return_sink_replay_signals: Vec<SinkControlSignal>,
        deferred_signals: Vec<FacadeControlSignal>,
        facade_gate: RuntimeUnitGate,
        mirrored_query_peer_routes: Arc<Mutex<std::collections::BTreeMap<String, u64>>>,
        pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
        facade_pending_status: SharedFacadePendingStatusCell,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        api_control_gate: Arc<ApiControlGate>,
        facade_service_state: SharedFacadeServiceStateCell,
        runtime_gate_state: Arc<StdMutex<RuntimeControlState>>,
        runtime_state_changed: Arc<tokio::sync::Notify>,
        pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
        control_failure_uninitialized: Arc<AtomicBool>,
    ) {
        tokio::spawn(async move {
            let retained_sink_replay_present = !post_return_sink_replay_signals.is_empty();
            let helper_result = match (pretrigger_request_epoch, retained_sink_replay_present) {
                (Some(request_epoch), _) => {
                    Self::wait_for_local_sink_status_republish_after_recovery_requiring_probe_from_parts_with_pretrigger_epoch(
                        source,
                        sink.clone(),
                        runtime_state_changed.clone(),
                        &expected_groups,
                        &post_return_sink_replay_signals,
                        request_epoch,
                    )
                    .await
                }
                (None, true) => {
                    Self::wait_for_local_sink_status_republish_after_recovery_requiring_probe_from_parts(
                        source,
                        sink.clone(),
                        runtime_state_changed.clone(),
                        &expected_groups,
                        &post_return_sink_replay_signals,
                    )
                    .await
                }
                (None, false) => {
                    Self::wait_for_local_sink_status_republish_after_recovery_from_parts(
                        source,
                        sink.clone(),
                        runtime_state_changed.clone(),
                        &expected_groups,
                        &post_return_sink_replay_signals,
                    )
                    .await
                }
            };
            if let Err(err) = helper_result {
                eprintln!(
                    "fs_meta_runtime_app: deferred local sink-status republish wait failed err={}",
                    err
                );
                return;
            }
            for signal in deferred_signals {
                if let Err(err) =
                    Self::apply_deferred_sink_owned_query_peer_publication_signal_from_parts(
                        facade_gate.clone(),
                        mirrored_query_peer_routes.clone(),
                        signal,
                    )
                    .await
                {
                    eprintln!(
                        "fs_meta_runtime_app: deferred sink-owned peer publication failed err={}",
                        err
                    );
                    return;
                }
            }
            #[cfg(test)]
            maybe_pause_before_deferred_sink_owned_query_peer_publication_gate_reopen().await;
            {
                let mut state = runtime_gate_state
                    .lock()
                    .expect("lock runtime gate state after deferred sink-status recovery");
                if state.replay_fully_cleared() {
                    state.mark_initialized();
                    control_failure_uninitialized.store(false, Ordering::Release);
                }
            }
            runtime_state_changed.notify_waiters();
            let observation = Self::observe_facade_gate_from_parts(
                instance_id,
                api_task.clone(),
                pending_facade.clone(),
                Some(&facade_pending_status),
                RuntimeControlState::from_state_cell(&runtime_gate_state),
                pending_fixed_bind_has_suppressed_dependent_routes.load(Ordering::Acquire),
                FacadeOnlyHandoffObservationPolicy::ForceBlocked,
            )
            .await;
            let sink_service_ready_for_publication = sink
                .cached_status_snapshot_with_failure()
                .ok()
                .is_some_and(|snapshot| {
                    sink_status_snapshot_ready_for_expected_groups(&snapshot, &expected_groups)
                        || sink_status_snapshot_republish_observed_for_expected_groups(
                            &snapshot,
                            &expected_groups,
                        )
                });
            let snapshot = Self::fixed_bind_gate_publication_snapshot(
                observation.clone(),
                matches!(
                    observation.runtime.facade_publication_readiness_decision(
                        observation.current_pending.is_some(),
                        observation.pending_facade_is_control_route,
                        observation.active_control_stream_present,
                        observation.active_pending_control_stream_present,
                        observation.allow_facade_only_handoff,
                    ),
                    FacadePublicationReadinessDecision::Ready
                ) && sink_service_ready_for_publication,
            );
            Self::publish_facade_publication_snapshot(
                &facade_service_state,
                &api_control_gate,
                snapshot,
            );
            #[cfg(test)]
            notify_deferred_sink_owned_query_peer_publication_completion();
        });
    }

    pub fn new<C>(config: C, node_id: NodeId) -> Result<Self>
    where
        C: Into<FSMetaConfig>,
    {
        Self::with_runtime_workers_and_state(
            config.into(),
            local_runtime_worker_binding("source"),
            local_runtime_worker_binding("sink"),
            node_id,
            None,
            None,
            in_memory_state_boundary(),
        )
    }

    pub fn with_boundaries<C>(
        config: C,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> Result<Self>
    where
        C: Into<FSMetaConfig>,
    {
        Self::with_runtime_workers_and_state(
            config.into(),
            local_runtime_worker_binding("source"),
            local_runtime_worker_binding("sink"),
            node_id,
            boundary,
            None,
            in_memory_state_boundary(),
        )
    }

    pub(crate) fn with_boundaries_and_state<C>(
        config: C,
        source_worker_binding: RuntimeWorkerBinding,
        sink_worker_binding: RuntimeWorkerBinding,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        ordinary_boundary: Option<Arc<dyn ChannelBoundary>>,
        state_boundary: Arc<dyn StateBoundary>,
    ) -> Result<Self>
    where
        C: Into<FSMetaConfig>,
    {
        Self::with_runtime_workers_and_state(
            config.into(),
            source_worker_binding,
            sink_worker_binding,
            node_id,
            boundary,
            ordinary_boundary,
            state_boundary,
        )
    }

    fn with_runtime_workers_and_state(
        config: FSMetaConfig,
        source_worker_binding: RuntimeWorkerBinding,
        sink_worker_binding: RuntimeWorkerBinding,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        ordinary_boundary: Option<Arc<dyn ChannelBoundary>>,
        state_boundary: Arc<dyn StateBoundary>,
    ) -> Result<Self> {
        Self::with_runtime_workers_and_state_with_failure(
            config,
            source_worker_binding,
            sink_worker_binding,
            node_id,
            boundary,
            ordinary_boundary,
            state_boundary,
        )
        .map_err(RuntimeInitializeFailure::into_error)
    }

    fn with_runtime_workers_and_state_with_failure(
        config: FSMetaConfig,
        source_worker_binding: RuntimeWorkerBinding,
        sink_worker_binding: RuntimeWorkerBinding,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        ordinary_boundary: Option<Arc<dyn ChannelBoundary>>,
        state_boundary: Arc<dyn StateBoundary>,
    ) -> std::result::Result<Self, RuntimeInitializeFailure> {
        let source_cfg = config.source.clone();
        let sink_source_cfg = config.source.clone();
        let api_request_tracker = shared_api_request_tracker_for_config(&config.api);
        let api_control_gate = Arc::new(ApiControlGate::new(false));
        let source = match source_worker_binding.mode {
            WorkerMode::Embedded => Arc::new(SourceFacade::local(Arc::new(
                source::FSMetaSource::with_boundaries_and_state_with_failure(
                    source_cfg,
                    node_id.clone(),
                    boundary.clone(),
                    state_boundary.clone(),
                )?,
            ))),
            WorkerMode::External => match boundary.clone() {
                Some(channel_boundary) => {
                    let control_boundary = ordinary_boundary.clone().ok_or_else(|| {
                        CnxError::InvalidInput(
                            "source worker mode requires ordinary runtime-boundary injection"
                                .to_string(),
                        )
                    })?;
                    let worker_factory = RuntimeWorkerClientFactory::new(
                        control_boundary,
                        channel_boundary.clone(),
                        state_boundary.clone(),
                    );
                    Arc::new(SourceFacade::worker(Arc::new(
                        SourceWorkerClientHandle::new(
                            node_id.clone(),
                            config.source.clone(),
                            source_worker_binding.clone(),
                            worker_factory,
                        )?,
                    )))
                }
                None => {
                    return Err(RuntimeInitializeFailure::Other(CnxError::InvalidInput(
                        "source worker mode requires runtime-boundary injection".to_string(),
                    )));
                }
            },
        };
        let sink = match sink_worker_binding.mode {
            WorkerMode::Embedded => Arc::new(SinkFacade::local(Arc::new(
                SinkFileMeta::with_boundaries_and_state_with_failure(
                    node_id.clone(),
                    boundary.clone(),
                    state_boundary.clone(),
                    sink_source_cfg.clone(),
                )?,
            ))),
            WorkerMode::External => match boundary.clone() {
                Some(channel_boundary) => {
                    let control_boundary = ordinary_boundary.clone().ok_or_else(|| {
                        CnxError::InvalidInput(
                            "sink worker mode requires ordinary runtime-boundary injection"
                                .to_string(),
                        )
                    })?;
                    let worker_factory = RuntimeWorkerClientFactory::new(
                        control_boundary,
                        channel_boundary.clone(),
                        state_boundary.clone(),
                    );
                    Arc::new(SinkFacade::worker(Arc::new(SinkWorkerClientHandle::new(
                        node_id.clone(),
                        sink_source_cfg.clone(),
                        sink_worker_binding.clone(),
                        worker_factory,
                    )?)))
                }
                None => {
                    return Err(RuntimeInitializeFailure::Other(CnxError::InvalidInput(
                        "sink worker mode requires runtime-boundary injection".to_string(),
                    )));
                }
            },
        };
        let query_sink = sink.clone();
        let shared_control_frame_serial = shared_control_frame_serial_for_runtime(
            &node_id,
            &source_worker_binding,
            &sink_worker_binding,
        );
        let shared_source_route_claims = shared_source_route_claims_for_runtime(
            &node_id,
            &source_worker_binding,
            &sink_worker_binding,
        );
        let shared_sink_route_claims = shared_sink_route_claims_for_runtime(
            &node_id,
            &source_worker_binding,
            &sink_worker_binding,
        );
        let control_frame_lease_path = shared_control_frame_lease_path_for_runtime(
            &node_id,
            &source_worker_binding,
            &sink_worker_binding,
        );
        let app = Self {
            instance_id: next_app_instance_id(),
            config,
            node_id,
            runtime_boundary: boundary,
            source,
            sink,
            query_sink,
            pump_task: Mutex::new(None),
            runtime_endpoint_tasks: Arc::new(Mutex::new(Vec::new())),
            runtime_endpoint_routes: Mutex::new(std::collections::BTreeSet::new()),
            api_task: Arc::new(Mutex::new(None)),
            pending_facade: Arc::new(Mutex::new(None)),
            facade_spawn_in_progress: Arc::new(Mutex::new(None)),
            retained_active_facade_continuity: Arc::new(AtomicBool::new(false)),
            pending_fixed_bind_claim_release_followup: Arc::new(AtomicBool::new(false)),
            pending_fixed_bind_has_suppressed_dependent_routes: Arc::new(AtomicBool::new(false)),
            control_failure_uninitialized: Arc::new(AtomicBool::new(false)),
            management_write_recovery_inflight: Arc::new(AtomicBool::new(false)),
            source_fail_closed_reinitialize_required: Arc::new(AtomicBool::new(false)),
            source_generation_cutover_replay_deferred: Arc::new(AtomicBool::new(false)),
            deferred_source_repair_recovery_scheduled: Arc::new(AtomicBool::new(false)),
            deferred_sink_repair_recovery_scheduled: Arc::new(AtomicBool::new(false)),
            sink_generation_cutover_replay_deferred: Arc::new(AtomicBool::new(false)),
            api_request_tracker,
            api_control_gate,
            control_frame_serial: Arc::new(Mutex::new(())),
            shared_control_frame_serial,
            facade_pending_status: shared_facade_pending_status_cell(),
            facade_service_state: shared_facade_service_state_cell(),
            rollout_status: shared_rollout_status_cell(),
            facade_gate: RuntimeUnitGate::new(
                "fs-meta",
                &[
                    execution_units::FACADE_RUNTIME_UNIT_ID,
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                    execution_units::SINK_RUNTIME_UNIT_ID,
                ],
            ),
            source_rescan_proxy_ready_generation: Arc::new(AtomicU64::new(0)),
            source_logical_roots_control_generation: Arc::new(AtomicU64::new(0)),
            sink_logical_roots_control_generation: Arc::new(AtomicU64::new(0)),
            mirrored_query_peer_routes: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            runtime_gate_state: Arc::new(StdMutex::new(RuntimeControlState::bootstrapping(
                false, false,
            ))),
            retained_sink_control_state: Arc::new(Mutex::new(RetainedSinkControlState::default())),
            source_scoped_sink_observation_repair_signature: Arc::new(StdMutex::new(None)),
            runtime_state_changed: Arc::new(tokio::sync::Notify::new()),
            retained_suppressed_public_query_activates: Mutex::new(
                std::collections::BTreeMap::new(),
            ),
            shared_source_route_claims,
            shared_sink_route_claims,
            control_frame_lease_path,
            control_init_lock: Mutex::new(()),
            closing: AtomicBool::new(false),
        };
        app.api_control_gate
            .set_management_write_recovery(Some(app.management_write_recovery()));
        app.api_control_gate
            .set_source_repair_recovery(Some(app.source_repair_recovery()));
        app.api_control_gate
            .set_sink_repair_recovery(Some(app.sink_observation_repair_recovery()));
        Ok(app)
    }

    fn control_initialized(&self) -> bool {
        self.runtime_control_state().control_initialized()
    }

    #[cfg(test)]
    fn source_state_replay_required(&self) -> bool {
        self.runtime_control_state().source_state_replay_required()
    }

    #[cfg(test)]
    fn sink_state_replay_required(&self) -> bool {
        self.runtime_control_state().sink_state_replay_required()
    }

    fn runtime_control_state(&self) -> RuntimeControlState {
        RuntimeControlState::from_state_cell(&self.runtime_gate_state)
    }

    fn update_runtime_control_state(
        &self,
        update: impl FnOnce(&mut RuntimeControlState),
    ) -> RuntimeControlState {
        let mut state = self
            .runtime_gate_state
            .lock()
            .expect("lock runtime gate state for update");
        update(&mut state);
        let facts = *state;
        drop(state);
        facts
    }

    fn begin_source_control_apply(&self) -> SourceControlApplyInflightGuard {
        self.update_runtime_control_state(|state| state.begin_source_control_apply());
        self.notify_runtime_state_changed();
        SourceControlApplyInflightGuard {
            runtime_gate_state: self.runtime_gate_state.clone(),
            runtime_state_changed: self.runtime_state_changed.clone(),
        }
    }

    #[cfg(test)]
    fn update_runtime_control_state_for_tests(
        &self,
        update: impl FnOnce(&mut RuntimeControlState),
    ) -> RuntimeControlState {
        self.update_runtime_control_state(update)
    }

    fn notify_runtime_state_changed(&self) {
        self.runtime_state_changed.notify_waiters();
    }

    fn facade_only_handoff_allowance_decision(
        input: FacadeOnlyHandoffAllowanceDecisionInput,
    ) -> FacadeOnlyHandoffAllowanceDecision {
        if !input.pending_facade_present {
            return FacadeOnlyHandoffAllowanceDecision::Blocked;
        }
        if input.pending_facade_is_control_route {
            return if input.pending_fixed_bind_has_suppressed_dependent_routes {
                FacadeOnlyHandoffAllowanceDecision::Blocked
            } else {
                FacadeOnlyHandoffAllowanceDecision::AllowedControlRouteWithoutSuppressedRoutes
            };
        }
        if !input.pending_bind_is_ephemeral && input.pending_bind_owned_by_instance {
            return FacadeOnlyHandoffAllowanceDecision::AllowedOwnedNonEphemeralFixedBind;
        }
        FacadeOnlyHandoffAllowanceDecision::Blocked
    }

    fn pending_fixed_bind_handoff_attempt_disposition(
        input: PendingFixedBindHandoffAttemptDecisionInput,
    ) -> PendingFixedBindHandoffAttemptDisposition {
        if !input.claim_conflict || !input.pending_runtime_exposure_confirmed {
            if input.claim_conflict
                && input.active_owner_present
                && input.active_owner_failed_control_uninitialized
            {
                return PendingFixedBindHandoffAttemptDisposition::ReleaseActiveOwner;
            }
            return PendingFixedBindHandoffAttemptDisposition::NoAttempt;
        }
        if input.active_owner_present {
            return PendingFixedBindHandoffAttemptDisposition::ReleaseActiveOwner;
        }
        if let Some(owner_instance_id) = input.conflicting_process_claim_owner_instance_id {
            return PendingFixedBindHandoffAttemptDisposition::ReleaseConflictingProcessClaim {
                owner_instance_id,
            };
        }
        PendingFixedBindHandoffAttemptDisposition::NoAttempt
    }

    fn classify_source_control_wave_disposition(
        observation: ControlFrameWaveObservation,
        source_signals: &[SourceControlSignal],
    ) -> SourceControlWaveDisposition {
        observation.classify_source_control_wave_disposition(source_signals)
    }

    fn classify_sink_control_wave_disposition(
        observation: ControlFrameWaveObservation,
        source_wave_disposition: SourceControlWaveDisposition,
        sink_signals: &[SinkControlSignal],
    ) -> SinkControlWaveDisposition {
        observation.classify_sink_control_wave_disposition(source_wave_disposition, sink_signals)
    }

    fn classify_uninitialized_cleanup_disposition(
        input: UninitializedCleanupDecisionInput<'_>,
    ) -> UninitializedCleanupDisposition {
        if input.control_initialized_now {
            UninitializedCleanupDisposition::None
        } else if input.source_signals.is_empty()
            && input.sink_signals.is_empty()
            && !input.facade_signals.is_empty()
            && input
                .facade_signals
                .iter()
                .all(Self::facade_signal_is_cleanup_only)
        {
            UninitializedCleanupDisposition::FacadeOnly
        } else if input.sink_signals.is_empty()
            && input.facade_signals.is_empty()
            && !input.source_signals.is_empty()
            && input
                .source_signals
                .iter()
                .all(Self::source_signal_is_drained_retire_cleanup)
        {
            UninitializedCleanupDisposition::SourceOnly
        } else if input.source_signals.is_empty()
            && input.facade_signals.is_empty()
            && !input.sink_signals.is_empty()
            && input
                .sink_signals
                .iter()
                .all(Self::sink_signal_is_cleanup_only)
        {
            UninitializedCleanupDisposition::SinkOnly {
                query_only: input
                    .sink_signals
                    .iter()
                    .all(Self::sink_signal_is_cleanup_only_query_request_route),
            }
        } else {
            UninitializedCleanupDisposition::None
        }
    }

    fn classify_source_generation_cutover_disposition(
        input: SourceGenerationCutoverDecisionInput<'_>,
    ) -> SourceGenerationCutoverDisposition {
        if input.control_initialized_at_entry
            && input.facade_signals.is_empty()
            && !input.source_signals.is_empty()
            && input
                .source_signals
                .iter()
                .all(Self::source_signal_is_drained_retire_cleanup)
            && (input.sink_signals.is_empty()
                || input
                    .sink_signals
                    .iter()
                    .all(Self::sink_signal_is_cleanup_only))
        {
            return SourceGenerationCutoverDisposition::FailClosedRestartDeferredRetirePending;
        }
        if input.fixed_bind_publication_continuation_active {
            return SourceGenerationCutoverDisposition::None;
        }
        let post_initial_route_cutover =
            Self::source_signals_are_post_initial_route_cutover(input.source_signals);
        let facade_lifecycle_signals_present = input
            .facade_signals
            .iter()
            .any(Self::facade_signal_updates_facade_claim);
        if (input.source_state_replay_required_at_entry
            || input.retained_source_route_state_present_at_entry
            || (input.control_initialized_at_entry
                && (!input.sink_signals.is_empty() || facade_lifecycle_signals_present)))
            && !input.source_signals.is_empty()
            && post_initial_route_cutover
        {
            SourceGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
        } else if !input.source_signals.is_empty()
            && (!input.sink_signals.is_empty() || facade_lifecycle_signals_present)
            && input
                .source_signals
                .iter()
                .any(Self::source_signal_is_post_initial_activate)
        {
            SourceGenerationCutoverDisposition::FailClosedColdSuccessorCandidateOnRetryableReset
        } else {
            SourceGenerationCutoverDisposition::None
        }
    }

    fn classify_sink_generation_cutover_disposition(
        input: SinkGenerationCutoverDecisionInput<'_>,
    ) -> SinkGenerationCutoverDisposition {
        if input.fixed_bind_publication_continuation_active {
            return SinkGenerationCutoverDisposition::None;
        }
        if input.control_initialized_at_entry
            && input.source_signals.is_empty()
            && input.facade_signals.is_empty()
            && input.sink_signals.len() == 1
            && input.sink_signals_in_shared_generation_cutover_lane
        {
            SinkGenerationCutoverDisposition::FailClosedSharedGenerationCutover
        } else if input.control_initialized_at_entry
            && input.facade_signals.is_empty()
            && !input.source_signals.is_empty()
            && input
                .source_signals
                .iter()
                .all(Self::source_signal_is_drained_retire_cleanup)
            && !input.sink_signals.is_empty()
            && input
                .sink_signals
                .iter()
                .all(Self::sink_signal_is_cleanup_only)
        {
            SinkGenerationCutoverDisposition::FailClosedSharedGenerationCutover
        } else if (input.control_initialized_at_entry
            || input.retained_replay_pending_at_entry
            || input.retained_sink_route_state_present_at_entry)
            && !input.sink_signals.is_empty()
            && input
                .sink_signals
                .iter()
                .any(Self::sink_signal_is_post_initial_route_state)
        {
            SinkGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
        } else if !input.sink_signals.is_empty()
            && input
                .sink_signals
                .iter()
                .any(Self::sink_signal_is_post_initial_route_state)
        {
            SinkGenerationCutoverDisposition::FailClosedColdSuccessorCandidateOnRetryableReset
        } else {
            SinkGenerationCutoverDisposition::None
        }
    }

    fn should_defer_sink_cleanup_wakeup_after_fail_closed_cutover(
        &self,
        disposition: SinkGenerationCutoverDisposition,
        sink_signals: &[SinkControlSignal],
    ) -> bool {
        matches!(
            disposition,
            SinkGenerationCutoverDisposition::FailClosedSharedGenerationCutover
        ) && !self.control_initialized()
            && self.runtime_control_state().sink_state_replay_required()
            && !sink_signals.is_empty()
            && sink_signals.iter().all(Self::sink_signal_is_cleanup_only)
    }

    #[cfg(test)]
    fn publish_facade_service_state_from_runtime_state(
        facade_service_state: &SharedFacadeServiceStateCell,
        input: FacadeServiceStateDecisionInput,
    ) -> FacadeServiceState {
        let state = if input.pending_facade_present {
            FacadeServiceState::Pending
        } else if input.control_gate_ready && input.publication_ready {
            FacadeServiceState::Serving
        } else {
            FacadeServiceState::Unavailable
        };
        *facade_service_state
            .write()
            .expect("write published facade service state") = state;
        state
    }

    fn publish_facade_publication_snapshot(
        facade_service_state: &SharedFacadeServiceStateCell,
        api_control_gate: &ApiControlGate,
        snapshot: FacadePublicationSnapshot,
    ) -> FacadeServiceState {
        api_control_gate.set_ready_state_with_source_repair(
            snapshot.control_gate_ready,
            snapshot.management_write_ready,
            snapshot.source_repair_ready,
        );
        *facade_service_state
            .write()
            .expect("write published facade service state") = snapshot.published_state;
        snapshot.published_state
    }

    fn internal_status_route_requires_facade_service_state<T: AsRef<str>>(
        active_unit_ids: &[T],
    ) -> bool {
        active_unit_ids.iter().any(|unit_id| {
            matches!(
                unit_id.as_ref(),
                execution_units::FACADE_RUNTIME_UNIT_ID
                    | execution_units::QUERY_RUNTIME_UNIT_ID
                    | execution_units::QUERY_PEER_RUNTIME_UNIT_ID
            )
        })
    }

    async fn internal_status_available_for_local_facade_owner<T: AsRef<str>>(
        active_unit_ids: &[T],
        runtime_gate_state: &Arc<StdMutex<RuntimeControlState>>,
        api_task: &Arc<Mutex<Option<FacadeActivation>>>,
        pending_facade: &Arc<Mutex<Option<PendingFacadeActivation>>>,
        facade_service_state: &SharedFacadeServiceStateCell,
    ) -> bool {
        if !Self::internal_status_route_requires_facade_service_state(active_unit_ids) {
            return true;
        }
        let runtime_state = RuntimeControlState::from_state_cell(runtime_gate_state);
        let control_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
        let active_facade = api_task.lock().await.as_ref().and_then(|active| {
            (active.route_key == control_route_key && active.handle.is_running())
                .then(|| active.resource_ids.clone())
        });
        let Some(active_resource_ids) = active_facade else {
            return true;
        };
        let published_state = *facade_service_state
            .read()
            .expect("read published facade service state for internal status");
        if matches!(published_state, FacadeServiceState::Serving) {
            return true;
        }
        if !matches!(published_state, FacadeServiceState::Pending) {
            return false;
        }
        if !runtime_state.control_initialized()
            && runtime_state.source_state_replay_required()
                == runtime_state.sink_state_replay_required()
        {
            return false;
        }
        matches!(published_state, FacadeServiceState::Pending)
            && runtime_state.replay_fully_cleared()
            && !active_resource_ids.is_empty()
            && pending_facade.lock().await.as_ref().is_some_and(|pending| {
                pending.route_key == control_route_key
                    && !pending.resource_ids.is_empty()
                    && !pending
                        .resource_ids
                        .iter()
                        .any(|resource_id| active_resource_ids.contains(resource_id))
            })
    }

    async fn retained_control_generation_for_sink_status(
        source: &Arc<SourceFacade>,
        retained_sink_control_state: &Arc<Mutex<RetainedSinkControlState>>,
    ) -> Option<u64> {
        let retained_sink_generation = retained_sink_control_state
            .lock()
            .await
            .active_by_route
            .values()
            .map(sink_signal_generation)
            .max();
        let retained_source_generation = source
            .control_signals_with_replay(&[])
            .await
            .iter()
            .map(source_signal_generation)
            .max();
        retained_sink_generation
            .into_iter()
            .chain(retained_source_generation)
            .max()
    }

    async fn sink_status_ready_for_uninitialized_facade_route(
        sink: &Arc<SinkFacade>,
        expected_generation: Option<u64>,
    ) -> bool {
        let Some(expected_generation) = expected_generation else {
            return false;
        };
        sink.cached_status_snapshot_with_failure()
            .ok()
            .is_some_and(|snapshot| {
                let ready = sink_status_snapshot_ready_for_scheduled_groups(&snapshot);
                let generation_observed =
                    sink_status_snapshot_observed_control_generation(&snapshot, expected_generation);
                if debug_status_endpoint_response_enabled() {
                    eprintln!(
                        "fs_meta_runtime_app: uninitialized sink-status readiness expected_generation={} ready={} generation_observed={} control={:?} summary={}",
                        expected_generation,
                        ready,
                        generation_observed,
                        snapshot.last_control_frame_signals_by_node,
                        summarize_sink_status_endpoint(&snapshot)
                    );
                }
                ready && (generation_observed || snapshot.progress_snapshot().has_ready_scheduled_groups())
            })
    }

    async fn wait_for_uninitialized_sink_status_ready_for_facade_route(
        sink: &Arc<SinkFacade>,
        runtime_state_changed: &Arc<tokio::sync::Notify>,
        expected_generation: Option<u64>,
    ) -> bool {
        if Self::sink_status_ready_for_uninitialized_facade_route(sink, expected_generation).await {
            return true;
        }
        let deadline =
            tokio::time::Instant::now() + INTERNAL_SINK_STATUS_UNINITIALIZED_READY_BUDGET;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Self::sink_status_ready_for_uninitialized_facade_route(
                    sink,
                    expected_generation,
                )
                .await;
            }
            tokio::select! {
                _ = runtime_state_changed.notified() => {}
                _ = tokio::time::sleep(remaining.min(DEFERRED_SINK_REPAIR_RETRY_INTERVAL)) => {}
            }
            if Self::sink_status_ready_for_uninitialized_facade_route(sink, expected_generation)
                .await
            {
                return true;
            }
        }
    }

    fn facade_signals_are_query_lane_deactivate_only(signals: &[FacadeControlSignal]) -> bool {
        !signals.is_empty()
            && signals.iter().all(|signal| {
                matches!(
                    signal,
                    FacadeControlSignal::Deactivate {
                        unit: FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer,
                        ..
                    }
                )
            })
    }

    fn fixed_bind_gate_publication_snapshot(
        observation: FacadeGateObservation,
        publication_ready: bool,
    ) -> FacadePublicationSnapshot {
        FixedBindLifecycleMachine {
            observation,
            publication_ready,
            fixed_bind: FacadeFixedBindSnapshot {
                pending_publication: None,
                conflicting_process_claim: None,
                claim_release_followup_pending: false,
            },
            release_handoff: None,
            shutdown_handoff: None,
        }
        .snapshot()
    }

    fn publish_rollout_status(
        rollout_status: &SharedRolloutStatusCell,
        snapshot: PublishedRolloutStatusSnapshot,
    ) -> PublishedRolloutStatusSnapshot {
        *rollout_status
            .write()
            .expect("write published rollout status") = snapshot;
        snapshot
    }

    async fn observe_facade_gate_from_parts(
        instance_id: u64,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
        facade_pending_status: Option<&SharedFacadePendingStatusCell>,
        runtime: RuntimeControlState,
        pending_fixed_bind_has_suppressed_dependent_routes: bool,
        handoff_policy: FacadeOnlyHandoffObservationPolicy,
    ) -> FacadeGateObservation {
        let current_pending = pending_facade.lock().await.clone();
        let control_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
        let pending_facade_is_control_route = current_pending
            .as_ref()
            .is_some_and(|pending| pending.route_key == control_route_key);
        let pending_facade_present = current_pending.is_some()
            || facade_pending_status
                .and_then(|status| status.read().ok())
                .is_some_and(|status| status.is_some());
        let (active_control_stream_present, active_pending_control_stream_present) = {
            let active_task = api_task.lock().await;
            let active_ref = active_task.as_ref();
            let active_control_stream_present =
                active_ref.is_some_and(|active| active.route_key == control_route_key);
            let active_pending_control_stream_present = match (active_ref, current_pending.as_ref())
            {
                (Some(active), Some(pending)) => {
                    active.route_key == pending.route_key
                        && active.resource_ids == pending.resource_ids
                        && active.generation == pending.generation
                }
                _ => false,
            };
            (
                active_control_stream_present,
                active_pending_control_stream_present,
            )
        };
        let allow_facade_only_handoff =
            match handoff_policy {
                FacadeOnlyHandoffObservationPolicy::ForceBlocked => false,
                FacadeOnlyHandoffObservationPolicy::ForceAllowed => true,
                FacadeOnlyHandoffObservationPolicy::DeriveFromPendingBind => {
                    current_pending.as_ref().is_some_and(|pending| {
                        let bind_addr = pending.resolved.bind_addr.as_str();
                        Self::facade_only_handoff_allowance_decision(
                            FacadeOnlyHandoffAllowanceDecisionInput {
                                pending_facade_present: true,
                                pending_facade_is_control_route,
                                pending_fixed_bind_has_suppressed_dependent_routes,
                                pending_bind_is_ephemeral: facade_bind_addr_is_ephemeral(bind_addr),
                                pending_bind_owned_by_instance: !facade_bind_addr_is_ephemeral(
                                    bind_addr,
                                )
                                    && active_fixed_bind_facade_owned_by(bind_addr, instance_id),
                            },
                        )
                        .allows_handoff()
                    })
                }
            };
        FacadeGateObservation {
            runtime,
            current_pending,
            pending_facade_present,
            pending_facade_is_control_route,
            active_control_stream_present,
            active_pending_control_stream_present,
            allow_facade_only_handoff,
        }
    }

    async fn resolve_facade_publication_ready(
        &self,
        observation: &FacadeGateObservation,
    ) -> Result<bool> {
        match observation.runtime.facade_publication_readiness_decision(
            observation.current_pending.is_some(),
            observation.pending_facade_is_control_route,
            observation.active_control_stream_present,
            observation.active_pending_control_stream_present,
            observation.allow_facade_only_handoff,
        ) {
            FacadePublicationReadinessDecision::BlockedControlGate
            | FacadePublicationReadinessDecision::BlockedWithoutActiveControlStream => Ok(false),
            FacadePublicationReadinessDecision::Ready => {
                if let Some(bind_addr) = observation.current_pending.as_ref().and_then(|pending| {
                    (observation.pending_facade_is_control_route
                        && observation.active_pending_control_stream_present)
                        .then_some(pending.resolved.bind_addr.as_str())
                }) {
                    clear_pending_fixed_bind_handoff_ready(bind_addr);
                }
                Ok(true)
            }
            FacadePublicationReadinessDecision::FixedBindHandoffPending => {
                let Some(pending) = observation
                    .current_pending
                    .clone()
                    .filter(|pending| !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr))
                else {
                    return Ok(false);
                };
                let bind_addr = pending.resolved.bind_addr.clone();
                let registrant = PendingFixedBindHandoffRegistrant {
                    instance_id: self.instance_id,
                    api_task: self.api_task.clone(),
                    pending_facade: self.pending_facade.clone(),
                    pending_fixed_bind_claim_release_followup: self
                        .pending_fixed_bind_claim_release_followup
                        .clone(),
                    pending_fixed_bind_has_suppressed_dependent_routes: self
                        .pending_fixed_bind_has_suppressed_dependent_routes
                        .clone(),
                    retained_active_facade_continuity: self
                        .retained_active_facade_continuity
                        .clone(),
                    facade_spawn_in_progress: self.facade_spawn_in_progress.clone(),
                    facade_pending_status: self.facade_pending_status.clone(),
                    facade_service_state: self.facade_service_state.clone(),
                    rollout_status: self.rollout_status.clone(),
                    api_request_tracker: self.api_request_tracker.clone(),
                    api_control_gate: self.api_control_gate.clone(),
                    control_failure_uninitialized: self.control_failure_uninitialized.clone(),
                    runtime_gate_state: self.runtime_gate_state.clone(),
                    runtime_state_changed: self.runtime_state_changed.clone(),
                    node_id: self.node_id.clone(),
                    runtime_boundary: self.runtime_boundary.clone(),
                    source: self.source.clone(),
                    sink: self.sink.clone(),
                    query_sink: self.query_sink.clone(),
                    query_runtime_boundary: self.runtime_boundary.clone(),
                };
                mark_pending_fixed_bind_handoff_ready_with_registrant(
                    &bind_addr,
                    registrant.clone(),
                );
                let fixed_bind = FSMetaApp::build_fixed_bind_snapshot_for_pending_publication(
                    registrant.instance_id,
                    registrant.api_task.clone(),
                    Some(pending.clone()),
                    registrant
                        .pending_fixed_bind_claim_release_followup
                        .load(Ordering::Acquire),
                )
                .await;
                let handoff = if fixed_bind.conflicting_process_claim.is_none()
                    || !pending.runtime_exposure_confirmed
                {
                    None
                } else if let Some(owner) =
                    active_fixed_bind_facade_owner_for(&bind_addr, self.instance_id)
                {
                    owner
                        .release_for_handoff(&bind_addr, FixedBindOwnerReleaseMode::GracefulHandoff)
                        .await;
                    Some(PendingFixedBindHandoffContinuation {
                        bind_addr: bind_addr.clone(),
                        registrant,
                    })
                } else if let Some(owner_instance_id) = fixed_bind
                    .claim_release_followup_pending
                    .then(|| fixed_bind.conflicting_process_claim.clone())
                    .flatten()
                    .map(|claim| claim.owner_instance_id)
                {
                    clear_process_facade_claim_for_bind_addr(&bind_addr, owner_instance_id);
                    Some(PendingFixedBindHandoffContinuation {
                        bind_addr: bind_addr.clone(),
                        registrant,
                    })
                } else {
                    None
                };
                let Some(handoff) = handoff else {
                    return Ok(false);
                };
                let after_release_completed =
                    execute_fixed_bind_after_release_handoff(handoff).await?;
                if !after_release_completed {
                    return Ok(false);
                }
                if self.api_task.lock().await.as_ref().is_some_and(|active| {
                    active.route_key == pending.route_key
                        && active.resource_ids == pending.resource_ids
                        && active.generation == pending.generation
                }) {
                    clear_pending_fixed_bind_handoff_ready(&bind_addr);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    async fn drive_fixed_bind_shutdown_handoff(
        &self,
        session: FixedBindLifecycleSession,
        handoff: Option<ActiveFixedBindShutdownContinuation>,
    ) -> Result<FixedBindLifecycleSession> {
        let release_mode = handoff
            .as_ref()
            .map(|handoff| handoff.release_mode)
            .unwrap_or(FixedBindOwnerReleaseMode::GracefulHandoff);
        self.api_control_gate.close_management_write_gate();
        self.api_request_tracker.wait_for_drain().await;
        if release_mode.drains_predecessor_facade_reads() {
            self.api_control_gate.wait_for_facade_request_drain().await;
        }
        #[cfg(test)]
        maybe_pause_facade_shutdown_started().await;
        eprintln!("fs_meta_runtime_app: shutdown_active_facade");
        let mut retired_fixed_bind_addr = None;
        if let Some(current) = self.api_task.lock().await.take() {
            eprintln!(
                "fs_meta_runtime_app: shutting down previous active facade generation={}",
                current.generation
            );
            retired_fixed_bind_addr = (current.route_key
                == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL))
            .then(|| {
                self.config
                    .api
                    .resolve_for_candidate_ids(&current.resource_ids)
            })
            .flatten()
            .map(|resolved| resolved.bind_addr)
            .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr));
            current
                .handle
                .shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT)
                .await;
        }
        clear_owned_process_facade_claim(self.instance_id);
        if let Some(bind_addr) = retired_fixed_bind_addr.as_deref() {
            clear_process_facade_claim_for_retired_fixed_bind(bind_addr);
        }
        if let Some(handoff) = handoff {
            clear_active_fixed_bind_facade_owner(&handoff.bind_addr, self.instance_id);
            if let Some(pending_handoff) = handoff.pending_handoff {
                let _ = execute_fixed_bind_after_release_handoff(pending_handoff).await?;
            }
        }
        if let Some(pending) = self.pending_facade.lock().await.clone() {
            clear_pending_fixed_bind_handoff_ready(&pending.resolved.bind_addr);
        }
        let session = self
            .rebuild_fixed_bind_lifecycle_session(
                &session,
                FixedBindLifecycleRebuildReason::PublishFacadeServiceStateAfterFixedBindShutdownHandoff,
            )
            .await;
        let _ = self
            .publish_facade_service_state_with_session(&session)
            .await;
        Ok(session)
    }

    async fn execute_fixed_bind_lifecycle_execution(
        &self,
        session: FixedBindLifecycleSession,
        execution: FixedBindLifecycleExecution,
    ) -> Result<FixedBindLifecycleSession> {
        match execution {
            FixedBindLifecycleExecution::RetainActiveContinuity {
                route_key,
                generation,
            } => {
                self.retained_active_facade_continuity
                    .store(true, Ordering::Release);
                eprintln!(
                    "fs_meta_runtime_app: retain active facade during continuity-preserving deactivate route_key={} generation={}",
                    route_key, generation
                );
                Ok(session)
            }
            FixedBindLifecycleExecution::RetainActiveWhilePendingFixedBindClaimConflict {
                route_key,
                generation,
            } => {
                self.retained_active_facade_continuity
                    .store(true, Ordering::Release);
                eprintln!(
                    "fs_meta_runtime_app: retain active facade while pending fixed-bind claim remains owned elsewhere route_key={} generation={}",
                    route_key, generation
                );
                Ok(session)
            }
            FixedBindLifecycleExecution::RetainPendingWhilePendingFixedBindClaimConflict {
                route_key,
                generation,
            } => {
                eprintln!(
                    "fs_meta_runtime_app: retain pending facade during fixed-bind continuity-preserving deactivate route_key={} generation={}",
                    route_key, generation
                );
                Ok(session)
            }
            FixedBindLifecycleExecution::ReleaseActiveForFixedBindHandoff {
                route_key,
                generation,
                handoff,
            } => {
                eprintln!(
                    "fs_meta_runtime_app: release active facade for fixed-bind handoff route_key={} generation={}",
                    route_key, generation
                );
                self.retained_active_facade_continuity
                    .store(false, Ordering::Release);
                *self.pending_facade.lock().await = None;
                Self::clear_pending_facade_status(&self.facade_pending_status);
                let session = self
                    .rebuild_fixed_bind_lifecycle_session(
                        &session,
                        FixedBindLifecycleRebuildReason::PublishFacadeServiceStateBeforeFixedBindReleaseHandoff,
                    )
                    .await;
                let _ = self
                    .publish_facade_service_state_with_session(&session)
                    .await;
                self.wait_for_shared_worker_control_handoff().await;
                self.drive_fixed_bind_shutdown_handoff(session, Some(handoff))
                    .await
            }
            FixedBindLifecycleExecution::RetainPendingForFixedBindHandoff {
                route_key,
                generation,
            } => {
                eprintln!(
                    "fs_meta_runtime_app: retain pending facade during fixed-bind continuity-preserving deactivate route_key={} generation={}",
                    route_key, generation
                );
                Ok(session)
            }
            FixedBindLifecycleExecution::RetainPendingWhileSpawnInFlight {
                route_key,
                generation,
            } => {
                eprintln!(
                    "fs_meta_runtime_app: retain pending facade during in-flight spawn route_key={} generation={}",
                    route_key, generation
                );
                Ok(session)
            }
            FixedBindLifecycleExecution::Shutdown { handoff } => {
                self.retained_active_facade_continuity
                    .store(false, Ordering::Release);
                *self.pending_facade.lock().await = None;
                Self::clear_pending_facade_status(&self.facade_pending_status);
                let session = self
                    .rebuild_fixed_bind_lifecycle_session(
                        &session,
                        FixedBindLifecycleRebuildReason::PublishFacadeServiceStateBeforeFixedBindShutdown,
                    )
                    .await;
                let _ = self
                    .publish_facade_service_state_with_session(&session)
                    .await;
                self.wait_for_shared_worker_control_handoff().await;
                self.drive_fixed_bind_shutdown_handoff(session, handoff)
                    .await
            }
        }
    }

    async fn drive_fixed_bind_lifecycle_request_with_session(
        &self,
        session: FixedBindLifecycleSession,
        request: FixedBindLifecycleRequest,
    ) -> Result<FixedBindLifecycleSession> {
        let execution = session.evaluate(request);
        self.execute_fixed_bind_lifecycle_execution(session, execution)
            .await
    }

    #[cfg(test)]
    async fn drive_fixed_bind_lifecycle_request(
        &self,
        request: FixedBindLifecycleRequest,
    ) -> Result<FixedBindLifecycleSession> {
        let session = self.begin_fixed_bind_lifecycle_session().await;
        self.drive_fixed_bind_lifecycle_request_with_session(session, request)
            .await
    }

    async fn begin_fixed_bind_lifecycle_root_session(
        &self,
        reason: FixedBindLifecycleRootReason,
    ) -> FixedBindLifecycleSession {
        FixedBindLifecycleSession::new(self.current_fixed_bind_lifecycle_view().await, reason)
    }

    #[cfg(test)]
    async fn begin_fixed_bind_lifecycle_session(&self) -> FixedBindLifecycleSession {
        self.begin_fixed_bind_lifecycle_root_session(
            FixedBindLifecycleRootReason::TestFixedBindSession,
        )
        .await
    }

    async fn rebuild_fixed_bind_lifecycle_session(
        &self,
        session: &FixedBindLifecycleSession,
        reason: FixedBindLifecycleRebuildReason,
    ) -> FixedBindLifecycleSession {
        let rebuilt = session.rebuild(self.current_fixed_bind_lifecycle_view().await, reason);
        debug_assert_eq!(rebuilt.lineage_token(), session.lineage_token());
        debug_assert_eq!(rebuilt.root_reason(), session.root_reason());
        debug_assert_eq!(rebuilt.last_rebuild_reason(), Some(reason));
        rebuilt
    }

    async fn current_fixed_bind_lifecycle_view(&self) -> FixedBindLifecycleView {
        FixedBindLifecycleView::from_facts(self.collect_fixed_bind_lifecycle_facts().await)
    }

    async fn settle_fixed_bind_claim_release_followup_with_session(
        &self,
        mut session: FixedBindLifecycleSession,
    ) -> Result<(
        FixedBindClaimReleaseFollowupSnapshot,
        FixedBindLifecycleSession,
    )> {
        let mut snapshot = session.claim_release_followup_snapshot();
        let current_claim_release_followup_pending = self
            .pending_fixed_bind_claim_release_followup
            .load(Ordering::Acquire);
        if snapshot.conflicting_process_claim.is_some() && current_claim_release_followup_pending {
            session = self
                .rebuild_fixed_bind_lifecycle_session(
                    &session,
                    FixedBindLifecycleRebuildReason::RetryPendingFacadeAfterClaimReleaseFollowup,
                )
                .await;
            snapshot = session.claim_release_followup_snapshot();
        }
        if snapshot.claim_release_followup_pending
            && snapshot.pending_publication.is_some()
            && snapshot.conflicting_process_claim.is_none()
        {
            let pending = snapshot.pending_publication.clone().expect(
                "fixed-bind claim-release retry requires pending facade while publication followup remains incomplete",
            );
            self.retry_pending_facade(&pending.route_key, pending.generation, false)
                .await?;
            session = self
                .rebuild_fixed_bind_lifecycle_session(
                    &session,
                    FixedBindLifecycleRebuildReason::RetryPendingFacadeAfterClaimReleaseFollowup,
                )
                .await;
            snapshot = session.claim_release_followup_snapshot();
        }
        if snapshot.claim_release_followup_pending && snapshot.pending_publication.is_none() {
            self.pending_fixed_bind_claim_release_followup
                .store(false, Ordering::Release);
        }
        Ok((snapshot, session))
    }

    async fn publish_facade_service_state_with_session(
        &self,
        session: &FixedBindLifecycleSession,
    ) -> FacadeServiceState {
        let facade_snapshot = session.publication_snapshot();
        let published = Self::publish_facade_publication_snapshot(
            &self.facade_service_state,
            &self.api_control_gate,
            facade_snapshot,
        );
        let _ = self
            .publish_current_rollout_status(facade_snapshot.publication_ready)
            .await;
        published
    }

    async fn collect_fixed_bind_lifecycle_facts(&self) -> FixedBindLifecycleFacts {
        let observation = Self::observe_facade_gate_from_parts(
            self.instance_id,
            self.api_task.clone(),
            self.pending_facade.clone(),
            Some(&self.facade_pending_status),
            self.runtime_control_state(),
            self.pending_fixed_bind_has_suppressed_dependent_routes
                .load(Ordering::Acquire),
            FacadeOnlyHandoffObservationPolicy::DeriveFromPendingBind,
        )
        .await;
        let publication_ready = match self.resolve_facade_publication_ready(&observation).await {
            Ok(publication_ready) => publication_ready,
            Err(err) => {
                eprintln!(
                    "fs_meta_runtime_app: facade publication readiness execution failed err={}",
                    err
                );
                false
            }
        };
        let pending = self.pending_facade.lock().await.clone();
        let fixed_bind = Self::build_fixed_bind_snapshot_for_pending_publication(
            self.instance_id,
            self.api_task.clone(),
            pending,
            self.pending_fixed_bind_claim_release_followup
                .load(Ordering::Acquire),
        )
        .await;
        let active_fixed_bind_bind_addr = {
            let api_task = self.api_task.lock().await;
            api_task.as_ref().and_then(|active| {
                (active.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL))
                    .then(|| {
                        self.config
                            .api
                            .resolve_for_candidate_ids(&active.resource_ids)
                    })
                    .flatten()
                    .map(|resolved| resolved.bind_addr)
                    .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
            })
        };
        let (release_handoff, shutdown_handoff) = if let Some(bind_addr) =
            active_fixed_bind_bind_addr
        {
            let ready = pending_fixed_bind_handoff_ready_for(&bind_addr);
            let pending_handoff = ready
                .as_ref()
                .and_then(|ready| (ready.instance_id != self.instance_id).then_some(ready.clone()))
                .map(|registrant| PendingFixedBindHandoffContinuation {
                    bind_addr: bind_addr.clone(),
                    registrant,
                });
            let shutdown_handoff = ActiveFixedBindShutdownContinuation {
                bind_addr: bind_addr.clone(),
                pending_handoff: pending_handoff.clone(),
                release_mode: FixedBindOwnerReleaseMode::GracefulHandoff,
            };
            let release_handoff = match ready.as_ref() {
                Some(ready) => {
                    ready
                        .release_handoff_for_active_owner(&bind_addr, self.instance_id)
                        .await
                }
                None => None,
            };
            (release_handoff, Some(shutdown_handoff))
        } else {
            (None, None)
        };
        FixedBindLifecycleFacts {
            observation,
            publication_ready,
            fixed_bind,
            release_handoff,
            shutdown_handoff,
        }
    }

    #[cfg(test)]
    async fn current_facade_service_state(&self) -> FacadeServiceState {
        let session = self.begin_fixed_bind_lifecycle_session().await;
        self.publish_facade_service_state_with_session(&session)
            .await
    }

    async fn publish_current_rollout_status(
        &self,
        publication_ready: bool,
    ) -> PublishedRolloutStatusSnapshot {
        let runtime = self.runtime_control_state();
        let active_generation = self
            .api_task
            .lock()
            .await
            .as_ref()
            .map(|active| active.generation);
        let pending_generation = if let Some(pending) = self.pending_facade.lock().await.clone() {
            Some((pending.generation, pending.runtime_exposure_confirmed))
        } else {
            self.facade_pending_status.read().ok().and_then(|status| {
                status
                    .as_ref()
                    .map(|status| (status.generation, status.runtime_exposure_confirmed))
            })
        };
        let candidate_generation = pending_generation.map(|(generation, _)| generation);
        let candidate_runtime_exposure_confirmed =
            pending_generation.is_some_and(|(_, confirmed)| confirmed);
        Self::publish_rollout_status(
            &self.rollout_status,
            RolloutGenerationMachine {
                runtime,
                active_generation,
                candidate_generation,
                candidate_runtime_exposure_confirmed,
                publication_ready,
            }
            .snapshot(),
        )
    }

    #[cfg(test)]
    async fn install_active_facade_for_tests(
        &self,
        resource_ids: &[&str],
        generation: u64,
    ) -> bool {
        let resource_ids = resource_ids
            .iter()
            .map(|resource_id| (*resource_id).to_string())
            .collect::<Vec<_>>();
        let active_facade = match api::spawn(
            self.config
                .api
                .resolve_for_candidate_ids(&resource_ids)
                .expect("resolve facade config"),
            self.node_id.clone(),
            self.runtime_boundary.clone(),
            self.source.clone(),
            self.sink.clone(),
            self.query_sink.clone(),
            self.runtime_boundary.clone(),
            self.facade_pending_status.clone(),
            self.facade_service_state.clone(),
            self.api_request_tracker.clone(),
            self.api_control_gate.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return false;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *self.api_task.lock().await = Some(FacadeActivation {
            route_key: format!("{}.stream", ROUTE_KEY_FACADE_CONTROL),
            generation,
            resource_ids,
            handle: active_facade,
        });
        let fixed_bind_session = self.begin_fixed_bind_lifecycle_session().await;
        let _ = self
            .publish_facade_service_state_with_session(&fixed_bind_session)
            .await;
        true
    }
    fn should_initialize_from_control(
        source_signals: &[SourceControlSignal],
        sink_signals: &[SinkControlSignal],
        facade_signals: &[FacadeControlSignal],
    ) -> bool {
        source_signals
            .iter()
            .any(Self::source_signal_can_initialize)
            || sink_signals.iter().any(Self::sink_signal_can_initialize)
            || facade_signals
                .iter()
                .any(Self::facade_signal_can_initialize)
    }

    fn source_signal_can_initialize(signal: &SourceControlSignal) -> bool {
        matches!(
            signal,
            SourceControlSignal::Activate { .. }
                | SourceControlSignal::Deactivate { .. }
                | SourceControlSignal::Tick { .. }
        )
    }

    fn sink_signal_can_initialize(signal: &SinkControlSignal) -> bool {
        matches!(
            signal,
            SinkControlSignal::Activate { .. }
                | SinkControlSignal::Deactivate { .. }
                | SinkControlSignal::Tick { .. }
        )
    }

    fn sink_signal_is_cleanup_only(signal: &SinkControlSignal) -> bool {
        matches!(
            signal,
            SinkControlSignal::Deactivate { .. } | SinkControlSignal::Tick { .. }
        )
    }

    fn source_signal_is_drained_retire_cleanup(signal: &SourceControlSignal) -> bool {
        let SourceControlSignal::Deactivate { envelope, .. } = signal else {
            return false;
        };
        matches!(
            decode_runtime_exec_control(envelope),
            Ok(Some(RuntimeExecControl::Deactivate(deactivate)))
                if matches!(
                    deactivate.reason.as_str(),
                    "restart_deferred_retire_pending" | "deferred_retire"
                )
                    && deactivate
                        .lease
                        .as_ref()
                        .and_then(|lease| lease.drain_started_at_ms)
                        .is_some()
        )
    }

    fn source_signal_is_post_initial_activate(signal: &SourceControlSignal) -> bool {
        matches!(
            signal,
            SourceControlSignal::Activate { generation, .. } if *generation > 1
        )
    }

    fn source_signal_is_post_initial_route_state(signal: &SourceControlSignal) -> bool {
        matches!(
            signal,
            SourceControlSignal::Activate { generation, .. }
                | SourceControlSignal::Deactivate { generation, .. } if *generation > 1
        )
    }

    fn source_signals_are_post_initial_route_cutover(
        source_signals: &[SourceControlSignal],
    ) -> bool {
        !source_signals.is_empty()
            && source_signals
                .iter()
                .any(Self::source_signal_is_post_initial_activate)
            && source_signals.iter().all(|signal| {
                !Self::source_signal_is_route_state(signal)
                    || Self::source_signal_is_post_initial_route_state(signal)
            })
    }

    fn source_signal_route_generation(signal: &SourceControlSignal) -> Option<u64> {
        match signal {
            SourceControlSignal::Activate { generation, .. }
            | SourceControlSignal::Deactivate { generation, .. } => Some(*generation),
            _ => None,
        }
    }

    fn source_signals_are_late_post_initial_route_cutover(
        source_signals: &[SourceControlSignal],
    ) -> bool {
        Self::source_signals_are_post_initial_route_cutover(source_signals)
            && source_signals
                .iter()
                .filter_map(Self::source_signal_route_generation)
                .any(|generation| generation > 2)
    }

    fn source_signals_are_single_route_state_cutover(
        source_signals: &[SourceControlSignal],
    ) -> bool {
        source_signals
            .iter()
            .filter(|signal| Self::source_signal_is_route_state(signal))
            .count()
            == 1
    }

    fn source_signal_is_route_state(signal: &SourceControlSignal) -> bool {
        matches!(
            signal,
            SourceControlSignal::Activate { .. } | SourceControlSignal::Deactivate { .. }
        )
    }

    fn source_signal_is_empty_scope_route_liveness(signal: &SourceControlSignal) -> bool {
        match signal {
            SourceControlSignal::Activate { bound_scopes, .. } => bound_scopes.is_empty(),
            SourceControlSignal::Deactivate { .. } | SourceControlSignal::Tick { .. } => true,
            SourceControlSignal::RuntimeHostGrantChange { .. }
            | SourceControlSignal::ManualRescan { .. }
            | SourceControlSignal::Passthrough(_) => false,
        }
    }

    fn source_current_logical_roots_empty_nonblocking(&self) -> bool {
        self.config.source.roots.is_empty()
            && self
                .source
                .cached_logical_roots_snapshot_with_failure()
                .is_ok_and(|roots| roots.is_empty())
    }

    fn source_signals_are_empty_root_route_liveness_only(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> bool {
        !source_signals.is_empty()
            && !self.runtime_control_state().source_state_replay_required()
            && self.source_current_logical_roots_empty_nonblocking()
            && source_signals
                .iter()
                .all(Self::source_signal_is_empty_scope_route_liveness)
    }

    fn source_signal_is_source_control_route(signal: &SourceControlSignal) -> bool {
        let route_key = match signal {
            SourceControlSignal::Activate { route_key, .. }
            | SourceControlSignal::Deactivate { route_key, .. }
            | SourceControlSignal::Tick { route_key, .. } => route_key,
            SourceControlSignal::RuntimeHostGrantChange { .. }
            | SourceControlSignal::ManualRescan { .. }
            | SourceControlSignal::Passthrough(_) => return false,
        };
        route_key == &format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL)
            || route_key == &format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL)
            || route_key == &format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL)
    }

    fn source_signals_include_source_control_route(source_signals: &[SourceControlSignal]) -> bool {
        source_signals
            .iter()
            .any(Self::source_signal_is_source_control_route)
    }

    fn peer_source_query_route_active_for_replay_preservation(&self) -> bool {
        let source_status_route = format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
        let source_find_route = format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL);
        self.facade_gate
            .route_active(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                &source_status_route,
            )
            .unwrap_or(false)
            || self
                .facade_gate
                .route_active(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    &source_find_route,
                )
                .unwrap_or(false)
    }

    fn sink_signal_is_restart_deferred_retire_pending(signal: &SinkControlSignal) -> bool {
        let SinkControlSignal::Deactivate {
            route_key,
            envelope,
            ..
        } = signal
        else {
            return false;
        };
        if !is_events_stream_route_key(route_key)
            && !is_per_peer_sink_query_request_route(route_key)
        {
            return false;
        }
        matches!(
            decode_runtime_exec_control(envelope),
            Ok(Some(RuntimeExecControl::Deactivate(deactivate)))
                if deactivate.reason == "restart_deferred_retire_pending"
                    && deactivate
                        .lease
                        .as_ref()
                        .and_then(|lease| lease.drain_started_at_ms)
                        .is_some()
        )
    }

    fn sink_signal_requires_fail_closed_retry_after_generation_cutover(
        signal: &SinkControlSignal,
    ) -> bool {
        match signal {
            SinkControlSignal::Activate { route_key, .. }
            | SinkControlSignal::Deactivate { route_key, .. }
                if is_events_stream_route_key(route_key) =>
            {
                true
            }
            _ => Self::sink_signal_is_restart_deferred_retire_pending(signal),
        }
    }

    fn facade_signal_can_initialize(signal: &FacadeControlSignal) -> bool {
        matches!(signal, FacadeControlSignal::Activate { .. })
    }

    fn facade_signal_is_cleanup_only(signal: &FacadeControlSignal) -> bool {
        matches!(
            signal,
            FacadeControlSignal::Deactivate { .. }
                | FacadeControlSignal::Tick { .. }
                | FacadeControlSignal::ExposureConfirmed { .. }
        )
    }

    fn facade_signal_requires_shared_serial_while_uninitialized(
        signal: &FacadeControlSignal,
    ) -> bool {
        matches!(
            signal,
            FacadeControlSignal::Deactivate {
                unit: FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer,
                ..
            }
        )
    }

    fn not_ready_error() -> CnxError {
        CnxError::NotReady(
            "fs-meta request handling is unavailable until runtime control initializes the app"
                .into(),
        )
    }

    fn remaining_initialize_budget(
        deadline: Option<tokio::time::Instant>,
    ) -> std::result::Result<Duration, RuntimeInitializeFailure> {
        match deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    Err(RuntimeInitializeFailure::from(CnxError::Timeout))
                } else {
                    Ok(remaining)
                }
            }
            None => Ok(Duration::MAX),
        }
    }

    fn map_runtime_initialize_timeout_result<T>(
        result: std::result::Result<T, tokio::time::error::Elapsed>,
    ) -> std::result::Result<T, RuntimeInitializeFailure> {
        result.map_err(|_| RuntimeInitializeFailure::from(CnxError::Timeout))
    }

    async fn initialize_from_control_with_deadline_and_session(
        &self,
        wait_for_source_worker_handoff: bool,
        wait_for_sink_worker_handoff: bool,
        deadline: Option<tokio::time::Instant>,
        fixed_bind_session: &FixedBindLifecycleSession,
    ) -> std::result::Result<(), RuntimeInitializeFailure> {
        #[cfg(test)]
        note_runtime_initialize_entry_for_tests();
        eprintln!(
            "fs_meta_runtime_app: initialize_from_control begin initialized={}",
            self.control_initialized()
        );
        if self.control_initialized() {
            return Ok(());
        }
        let _guard = self.control_init_lock.lock().await;
        if self.control_initialized() {
            return Ok(());
        }
        if !self.config.api.enabled {
            return Err(RuntimeInitializeFailure::from(CnxError::InvalidInput(
                "api.enabled must be true; fs-meta management API boundary is mandatory".into(),
            )));
        }

        if self.sink.is_worker() {
            eprintln!("fs_meta_runtime_app: initialize_from_control sink.ensure_started begin");
            let ensure_started = self.sink.ensure_started_with_failure();
            if let Some(deadline) = deadline {
                Self::map_runtime_initialize_timeout_result(
                    tokio::time::timeout(
                        Self::remaining_initialize_budget(Some(deadline))?,
                        ensure_started,
                    )
                    .await,
                )??;
            } else {
                ensure_started.await?;
            }
            eprintln!("fs_meta_runtime_app: initialize_from_control sink.ensure_started ok");
        }
        let skip_rootless_external_source_start = self.source.is_worker()
            && self.source_current_logical_roots_empty_nonblocking()
            && !wait_for_source_worker_handoff;
        let mut guard = self.pump_task.lock().await;
        if guard.is_none() {
            if wait_for_source_worker_handoff && wait_for_sink_worker_handoff {
                if let Some(deadline) = deadline {
                    Self::map_runtime_initialize_timeout_result(
                        tokio::time::timeout(
                            Self::remaining_initialize_budget(Some(deadline))?,
                            self.wait_for_shared_worker_control_handoff(),
                        )
                        .await,
                    )?;
                } else {
                    self.wait_for_shared_worker_control_handoff().await;
                }
            } else if wait_for_source_worker_handoff {
                if let Some(deadline) = deadline {
                    Self::map_runtime_initialize_timeout_result(
                        tokio::time::timeout(
                            Self::remaining_initialize_budget(Some(deadline))?,
                            self.source.wait_for_control_ops_to_drain_for_handoff(),
                        )
                        .await,
                    )?;
                } else {
                    self.source
                        .wait_for_control_ops_to_drain_for_handoff()
                        .await;
                }
            } else if wait_for_sink_worker_handoff {
                if let Some(deadline) = deadline {
                    Self::map_runtime_initialize_timeout_result(
                        tokio::time::timeout(
                            Self::remaining_initialize_budget(Some(deadline))?,
                            self.sink.wait_for_control_ops_to_drain_for_handoff(),
                        )
                        .await,
                    )?;
                } else {
                    self.sink.wait_for_control_ops_to_drain_for_handoff().await;
                }
            }
            if skip_rootless_external_source_start {
                eprintln!(
                    "fs_meta_runtime_app: initialize_from_control source.start skipped reason=empty_root_external_source_liveness"
                );
            } else {
                eprintln!("fs_meta_runtime_app: initialize_from_control source.start begin");
                let start = self
                    .source
                    .start_with_failure(self.sink.clone(), self.runtime_boundary.clone());
                *guard = if let Some(deadline) = deadline {
                    let start = Self::map_runtime_initialize_timeout_result(
                        tokio::time::timeout(
                            Self::remaining_initialize_budget(Some(deadline))?,
                            start,
                        )
                        .await,
                    )?;
                    start.map_err(RuntimeInitializeFailure::from)?
                } else {
                    start.await.map_err(RuntimeInitializeFailure::from)?
                };
                eprintln!("fs_meta_runtime_app: initialize_from_control source.start ok");
            }
        }
        drop(guard);

        eprintln!("fs_meta_runtime_app: initialize_from_control endpoints begin");
        let ensure_endpoints = self.ensure_runtime_endpoints_started();
        if let Some(deadline) = deadline {
            Self::map_runtime_initialize_timeout_result(
                tokio::time::timeout(
                    Self::remaining_initialize_budget(Some(deadline))?,
                    ensure_endpoints,
                )
                .await,
            )??;
        } else {
            ensure_endpoints.await?;
        }
        eprintln!("fs_meta_runtime_app: initialize_from_control endpoints ok");
        self.update_runtime_control_state(|state| state.mark_initialized());
        self.control_failure_uninitialized
            .store(false, Ordering::Release);
        let _ = self
            .publish_facade_service_state_with_session(fixed_bind_session)
            .await;
        eprintln!("fs_meta_runtime_app: initialize_from_control done");

        Ok(())
    }

    pub async fn start(&self) -> Result<()> {
        let fixed_bind_session = self
            .begin_fixed_bind_lifecycle_root_session(FixedBindLifecycleRootReason::PublicStart)
            .await;
        self.initialize_from_control_with_deadline_and_session(
            false,
            false,
            None,
            &fixed_bind_session,
        )
        .await
        .map_err(RuntimeInitializeFailure::into_error)
    }

    pub async fn send(&self, events: &[Event]) -> Result<()> {
        self.service_send_with_failure(events)
            .await
            .map_err(RuntimeServiceIoFailure::into_error)
    }

    pub async fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        self.service_recv_with_failure(opts)
            .await
            .map_err(RuntimeServiceIoFailure::into_error)
    }

    async fn service_send_with_failure(
        &self,
        events: &[Event],
    ) -> std::result::Result<(), RuntimeServiceIoFailure> {
        if !self.control_initialized() {
            return Err(Self::not_ready_error().into());
        }
        self.sink
            .send_with_failure(events)
            .await
            .map_err(RuntimeServiceIoFailure::from)
    }

    async fn service_recv_with_failure(
        &self,
        opts: RecvOpts,
    ) -> std::result::Result<Vec<Event>, RuntimeServiceIoFailure> {
        if !self.control_initialized() {
            return Err(Self::not_ready_error().into());
        }
        self.sink
            .recv_with_failure(opts)
            .await
            .map_err(RuntimeServiceIoFailure::from)
    }

    async fn ensure_runtime_endpoints_started(&self) -> Result<()> {
        self.ensure_embedded_runtime_worker_endpoints_started()
            .await?;
        self.ensure_runtime_proxy_endpoints_started().await
    }

    async fn refresh_source_rescan_proxy_ready_from_retained_endpoint(&self) {
        let tasks = self.runtime_endpoint_tasks.lock().await;
        let scoped_source_rescan_route_key = source_rescan_request_route_for(&self.node_id.0).0;
        let current_generation = source_rescan_route_semantic_generation(
            &self.facade_gate,
            &scoped_source_rescan_route_key,
        );
        for task in tasks.iter() {
            if task.route_key() == scoped_source_rescan_route_key
                && runtime_endpoint_task_route_still_active(&self.facade_gate, task)
                && !task.is_finished()
                && task
                    .route_generation()
                    .is_none_or(|generation| generation == current_generation)
            {
                mark_source_rescan_proxy_ready_for_current_generation_if_receive_armed(
                    &self.facade_gate,
                    task.route_key(),
                    &self.source_rescan_proxy_ready_generation,
                    task.is_receive_polling(),
                );
            }
        }
    }

    async fn ensure_embedded_runtime_worker_endpoints_started(&self) -> Result<()> {
        let Some(boundary) = self.runtime_boundary.clone() else {
            return Ok(());
        };
        match &*self.source {
            SourceFacade::Local(source) => {
                source.start_runtime_endpoints(boundary.clone()).await?;
            }
            SourceFacade::Worker(_) => {}
        }
        match &*self.sink {
            SinkFacade::Local(sink) => {
                sink.start_runtime_endpoints(boundary, self.node_id.clone())?;
            }
            SinkFacade::Worker(_) => {}
        }
        Ok(())
    }

    async fn ensure_runtime_proxy_endpoints_started(&self) -> Result<()> {
        let Some(boundary) = self.runtime_boundary.clone() else {
            return Ok(());
        };
        let mut endpoint_tasks = std::mem::take(&mut *self.runtime_endpoint_tasks.lock().await);
        let mut retained_tasks = Vec::with_capacity(endpoint_tasks.len());
        let scoped_source_rescan_route_key = source_rescan_request_route_for(&self.node_id.0).0;
        let scoped_source_roots_control_route_key =
            source_roots_control_stream_route_for(&self.node_id.0).0;
        let scoped_sink_roots_control_route_key =
            sink_roots_control_stream_route_for(&self.node_id.0).0;
        let scoped_source_rescan_generation = source_rescan_route_semantic_generation(
            &self.facade_gate,
            &scoped_source_rescan_route_key,
        );
        let runtime_state = self.runtime_control_state();
        let allow_unmanaged_local_source_rescan_proxy =
            source_rescan_proxy_allowed_for_runtime_state(&self.source, runtime_state);
        let allow_unmanaged_worker_source_roots_control_proxy =
            matches!(&*self.source, SourceFacade::Worker(_));
        let allow_unmanaged_worker_sink_roots_control_proxy =
            matches!(&*self.sink, SinkFacade::Worker(_));
        for task in endpoint_tasks.drain(..) {
            if task.is_finished() {
                eprintln!(
                    "fs_meta_runtime_app: pruning finished runtime endpoint route={} reason={}",
                    task.route_key(),
                    task.finish_reason()
                        .unwrap_or_else(|| "unclassified_finish".to_string())
                );
                continue;
            }
            if task.finish_reason().is_some() || task.is_shutdown_requested() {
                retained_tasks.push(task);
                continue;
            }
            if !task.belongs_to_boundary(&boundary) {
                eprintln!(
                    "fs_meta_runtime_app: retiring endpoint from stale runtime boundary route={}",
                    task.route_key()
                );
                task.request_shutdown_and_close();
                retained_tasks.push(task);
                continue;
            }
            let unmanaged_local_source_rescan_proxy = allow_unmanaged_local_source_rescan_proxy
                && task.route_key() == scoped_source_rescan_route_key;
            let unmanaged_worker_source_roots_control_proxy =
                allow_unmanaged_worker_source_roots_control_proxy
                    && task.route_key() == scoped_source_roots_control_route_key;
            let unmanaged_worker_sink_roots_control_proxy =
                allow_unmanaged_worker_sink_roots_control_proxy
                    && task.route_key() == scoped_sink_roots_control_route_key;
            if !runtime_endpoint_task_route_still_active(&self.facade_gate, &task)
                && !unmanaged_local_source_rescan_proxy
                && !unmanaged_worker_source_roots_control_proxy
                && !unmanaged_worker_sink_roots_control_proxy
            {
                eprintln!(
                    "fs_meta_runtime_app: retiring inactive runtime endpoint route={}",
                    task.route_key()
                );
                task.request_shutdown_and_close_on(&boundary);
                retained_tasks.push(task);
                continue;
            }
            if task.route_key() == scoped_source_rescan_route_key
                && task
                    .route_generation()
                    .is_some_and(|generation| generation != scoped_source_rescan_generation)
            {
                eprintln!(
                    "fs_meta_runtime_app: retiring generation-stale source rescan proxy endpoint route={} task_generation={:?} current_generation={}",
                    task.route_key(),
                    task.route_generation(),
                    scoped_source_rescan_generation
                );
                task.request_shutdown_and_close_on(&boundary);
                self.source_rescan_proxy_ready_generation
                    .store(0, Ordering::Release);
                retained_tasks.push(task);
                continue;
            }
            retained_tasks.push(task);
        }
        let mut tasks = self.runtime_endpoint_tasks.lock().await;
        *tasks = retained_tasks;
        let mut spawned_routes = self.runtime_endpoint_routes.lock().await;
        spawned_routes.clear();
        for task in tasks.iter() {
            let unmanaged_local_source_rescan_proxy = allow_unmanaged_local_source_rescan_proxy
                && task.route_key() == scoped_source_rescan_route_key;
            let unmanaged_worker_source_roots_control_proxy =
                allow_unmanaged_worker_source_roots_control_proxy
                    && task.route_key() == scoped_source_roots_control_route_key;
            let unmanaged_worker_sink_roots_control_proxy =
                allow_unmanaged_worker_sink_roots_control_proxy
                    && task.route_key() == scoped_sink_roots_control_route_key;
            if !task.is_finished()
                && task.finish_reason().is_none()
                && !task.is_shutdown_requested()
                && (runtime_endpoint_task_route_still_active(&self.facade_gate, task)
                    || unmanaged_local_source_rescan_proxy
                    || unmanaged_worker_source_roots_control_proxy
                    || unmanaged_worker_sink_roots_control_proxy)
            {
                if task.route_key() == scoped_source_rescan_route_key
                    && task
                        .route_generation()
                        .is_some_and(|generation| generation != scoped_source_rescan_generation)
                {
                    continue;
                }
                if task.route_key() == scoped_source_rescan_route_key {
                    mark_source_rescan_proxy_ready_for_current_generation_if_receive_armed(
                        &self.facade_gate,
                        task.route_key(),
                        &self.source_rescan_proxy_ready_generation,
                        task.is_receive_polling(),
                    );
                }
                spawned_routes.insert(task.route_key().to_string());
            }
        }
        let routes = default_route_bindings();
        let query_active = self
            .facade_gate
            .unit_state(execution_units::QUERY_RUNTIME_UNIT_ID)?
            .map(|(active, _)| active)
            .unwrap_or(false);
        let query_peer_active = self
            .facade_gate
            .unit_state(execution_units::QUERY_PEER_RUNTIME_UNIT_ID)?
            .map(|(active, _)| active)
            .unwrap_or(false);
        let internal_query_active = query_active || query_peer_active;
        let source_active = self
            .facade_gate
            .unit_state(execution_units::SOURCE_RUNTIME_UNIT_ID)?
            .map(|(active, _)| active)
            .unwrap_or(false);
        if matches!(&*self.source, SourceFacade::Worker(_)) {
            let mut source_roots_control_routes = Vec::new();
            if let Ok(route) =
                routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_ROOTS_CONTROL)
                && self
                    .facade_gate
                    .route_active(execution_units::SOURCE_RUNTIME_UNIT_ID, &route.0)
                    .unwrap_or(false)
            {
                source_roots_control_routes.push(route.0);
            }
            source_roots_control_routes.push(scoped_source_roots_control_route_key.clone());
            for route_key in source_roots_control_routes {
                if !spawned_routes.insert(route_key.clone()) {
                    continue;
                }
                eprintln!(
                    "fs_meta_runtime_app: spawning worker-backed source roots-control endpoint route={}",
                    route_key
                );
                let route_for_gate = route_key.clone();
                let scoped_route_for_gate = scoped_source_roots_control_route_key.clone();
                let facade_gate = self.facade_gate.clone();
                let source = self.source.clone();
                let generation_cell = self.source_logical_roots_control_generation.clone();
                let endpoint = ManagedEndpointTask::spawn_stream(
                    boundary.clone(),
                    RouteKey(route_key.clone()),
                    format!(
                        "app:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_ROOTS_CONTROL
                    ),
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    tokio_util::sync::CancellationToken::new(),
                    move || match facade_gate
                        .route_generation(execution_units::SOURCE_RUNTIME_UNIT_ID, &route_for_gate)
                    {
                        Ok(Some(generation)) => facade_gate
                            .accept_tick(
                                execution_units::SOURCE_RUNTIME_UNIT_ID,
                                &route_for_gate,
                                generation,
                            )
                            .unwrap_or(false),
                        Ok(None) => route_for_gate == scoped_route_for_gate,
                        Err(_) => false,
                    },
                    move |events| {
                        let source = source.clone();
                        let generation_cell = generation_cell.clone();
                        async move {
                            for event in events {
                                let payload = match decode_logical_roots_control_payload(
                                    event.payload_bytes(),
                                ) {
                                    Ok(payload) => payload,
                                    Err(err) => {
                                        log::warn!(
                                            "worker-backed source logical-roots control decode failed: {:?}",
                                            err
                                        );
                                        continue;
                                    }
                                };
                                if ManagementWriteRecoveryContext::source_roots_control_generation_is_stale(
                                    &generation_cell,
                                    payload.generation,
                                ) {
                                    log::warn!(
                                        "worker-backed source logical-roots control stale: payload_generation={}",
                                        payload.generation
                                    );
                                    continue;
                                }
                                let update_result = if payload.defer_scan_audit {
                                    source
                                        .update_logical_roots_defer_scan_audit_with_failure(
                                            payload.roots,
                                        )
                                        .await
                                } else {
                                    source
                                        .update_logical_roots_with_failure(payload.roots)
                                        .await
                                };
                                if let Err(err) = update_result {
                                    log::warn!(
                                        "worker-backed source logical-roots control apply failed: {:?}",
                                        err.as_error()
                                    );
                                    continue;
                                }
                                ManagementWriteRecoveryContext::mark_source_roots_control_generation(
                                    &generation_cell,
                                    payload.generation,
                                );
                            }
                        }
                    },
                );
                tasks.push(endpoint);
            }
        }
        if matches!(&*self.sink, SinkFacade::Worker(_)) {
            let mut sink_roots_control_routes = Vec::new();
            if let Ok(route) =
                routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_ROOTS_CONTROL)
                && self
                    .facade_gate
                    .route_active(execution_units::SINK_RUNTIME_UNIT_ID, &route.0)
                    .unwrap_or(false)
            {
                sink_roots_control_routes.push(route.0);
            }
            sink_roots_control_routes.push(scoped_sink_roots_control_route_key.clone());
            for route_key in sink_roots_control_routes {
                if !spawned_routes.insert(route_key.clone()) {
                    continue;
                }
                eprintln!(
                    "fs_meta_runtime_app: spawning worker-backed sink roots-control endpoint route={}",
                    route_key
                );
                let route_for_gate = route_key.clone();
                let scoped_route_for_gate = scoped_sink_roots_control_route_key.clone();
                let facade_gate = self.facade_gate.clone();
                let sink = self.sink.clone();
                let config_host_object_grants = self.config.source.host_object_grants.clone();
                let retained_sink_control_state = self.retained_sink_control_state.clone();
                let generation_cell = self.sink_logical_roots_control_generation.clone();
                let endpoint = ManagedEndpointTask::spawn_stream(
                    boundary.clone(),
                    RouteKey(route_key.clone()),
                    format!(
                        "app:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_ROOTS_CONTROL
                    ),
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    tokio_util::sync::CancellationToken::new(),
                    move || match facade_gate
                        .route_generation(execution_units::SINK_RUNTIME_UNIT_ID, &route_for_gate)
                    {
                        Ok(Some(generation)) => facade_gate
                            .accept_tick(
                                execution_units::SINK_RUNTIME_UNIT_ID,
                                &route_for_gate,
                                generation,
                            )
                            .unwrap_or(false),
                        Ok(None) => route_for_gate == scoped_route_for_gate,
                        Err(_) => false,
                    },
                    move |events| {
                        let sink = sink.clone();
                        let config_host_object_grants = config_host_object_grants.clone();
                        let retained_sink_control_state = retained_sink_control_state.clone();
                        let generation_cell = generation_cell.clone();
                        async move {
                            for event in events {
                                let payload = match decode_logical_roots_control_payload(
                                    event.payload_bytes(),
                                ) {
                                    Ok(payload) => payload,
                                    Err(err) => {
                                        log::warn!(
                                            "worker-backed sink logical-roots control decode failed: {:?}",
                                            err
                                        );
                                        continue;
                                    }
                                };
                                let replay_envelopes = payload.sink_replay_envelopes;
                                let replay_signals = match sink_control_signals_from_envelopes(
                                    &replay_envelopes,
                                ) {
                                    Ok(signals) => signals,
                                    Err(err) => {
                                        log::warn!(
                                            "worker-backed sink logical-roots replay decode failed: {:?}",
                                            err
                                        );
                                        continue;
                                    }
                                };
                                if ManagementWriteRecoveryContext::sink_roots_control_generation_is_stale(
                                    &generation_cell,
                                    payload.generation,
                                ) {
                                    if ManagementWriteRecoveryContext::sink_roots_control_generation_can_replay_same_declaration(
                                        &generation_cell,
                                        payload.generation,
                                    ) && !replay_signals.is_empty()
                                    {
                                        if let Err(err) = sink
                                            .apply_retained_orchestration_signals_with_total_timeout_with_failure(
                                                &replay_signals,
                                                SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT,
                                            )
                                            .await
                                        {
                                            log::warn!(
                                                "worker-backed sink logical-roots stale replay apply failed: {:?}",
                                                err.as_error()
                                            );
                                        }
                                    } else {
                                        log::warn!(
                                            "worker-backed sink logical-roots control stale: payload_generation={}",
                                            payload.generation
                                        );
                                    }
                                    continue;
                                }
                                let host_object_grants =
                                    ManagementWriteRecoveryContext::sink_roots_control_grants_from_replay_signals(
                                        &replay_signals,
                                    )
                                    .unwrap_or_else(|| config_host_object_grants.clone());
                                if let Err(err) = sink
                                    .update_logical_roots_with_failure(
                                        payload.roots,
                                        &host_object_grants,
                                    )
                                    .await
                                {
                                    log::warn!(
                                        "worker-backed sink logical-roots control apply failed: {:?}",
                                        err.as_error()
                                    );
                                    continue;
                                }
                                ManagementWriteRecoveryContext::mark_sink_roots_control_generation(
                                    &generation_cell,
                                    payload.generation,
                                );
                                if !replay_signals.is_empty() {
                                    {
                                        let mut retained = retained_sink_control_state.lock().await;
                                        Self::apply_sink_signals_to_state(
                                            &mut retained,
                                            &replay_signals,
                                        );
                                    }
                                    if let Err(err) = sink
                                        .apply_retained_orchestration_signals_with_total_timeout_with_failure(
                                            &replay_signals,
                                            SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT,
                                        )
                                        .await
                                    {
                                        log::warn!(
                                            "worker-backed sink logical-roots replay apply failed: {:?}",
                                            err.as_error()
                                        );
                                    }
                                }
                            }
                        }
                    },
                );
                tasks.push(endpoint);
            }
        }
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META, METHOD_QUERY) {
            let route_active = self
                .facade_gate
                .route_active(execution_units::QUERY_RUNTIME_UNIT_ID, &route.0)
                .unwrap_or(false);
            if !query_active || !route_active || !spawned_routes.insert(route.0.clone()) {
                // Not currently selected as query ingress owner, or already running.
            } else {
                let boundary_for_calls = self.runtime_boundary.clone().ok_or_else(|| {
                    CnxError::InvalidInput("fs-meta public query requires runtime boundary".into())
                })?;
                let caller_node = self.node_id.clone();
                eprintln!(
                    "fs_meta_runtime_app: spawning public query endpoint route={}",
                    route.0
                );
                let endpoint = ManagedEndpointTask::spawn_without_ready_wait(
                    boundary.clone(),
                    route,
                    format!("app:{}:{}", ROUTE_TOKEN_FS_META, METHOD_QUERY),
                    tokio_util::sync::CancellationToken::new(),
                    move |requests| {
                        let boundary_for_calls = boundary_for_calls.clone();
                        let caller_node = caller_node.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                    req.payload_bytes(),
                                ) else {
                                    continue;
                                };
                                eprintln!(
                                    "fs_meta_runtime_app: public query request selected_group={:?} recursive={} path={}",
                                    params.scope.selected_group,
                                    params.scope.recursive,
                                    String::from_utf8_lossy(&params.scope.path)
                                );
                                let adapter = crate::runtime::seam::exchange_host_adapter(
                                    boundary_for_calls.clone(),
                                    caller_node.clone(),
                                    crate::runtime::routes::default_route_bindings(),
                                );
                                let result: Result<Vec<Event>> = async {
                                    let payload =
                                        rmp_serde::to_vec_named(&params).map_err(|err| {
                                            CnxError::Internal(format!(
                                                "encode public query request failed: {err}"
                                            ))
                                        })?;
                                    capanix_host_adapter_fs::HostAdapter::call_collect(
                                        &adapter,
                                        ROUTE_TOKEN_FS_META_INTERNAL,
                                        crate::runtime::routes::METHOD_SINK_QUERY_PROXY,
                                        bytes::Bytes::from(payload),
                                        Duration::from_secs(30),
                                        Duration::from_secs(5),
                                    )
                                    .await
                                }
                                .await;
                                match result {
                                    Ok(mut events) => {
                                        eprintln!(
                                            "fs_meta_runtime_app: public query response events={}",
                                            events.len()
                                        );
                                        for event in &mut events {
                                            let mut meta = event.metadata().clone();
                                            meta.correlation_id = req.metadata().correlation_id;
                                            responses.push(Event::new(
                                                meta,
                                                bytes::Bytes::copy_from_slice(
                                                    event.payload_bytes(),
                                                ),
                                            ));
                                        }
                                    }
                                    Err(err) => {
                                        eprintln!(
                                            "fs_meta_runtime_app: public query failed err={}",
                                            err
                                        );
                                    }
                                }
                            }
                            responses
                        }
                    },
                );
                tasks.push(endpoint);
            }
        }
        let mut sink_status_endpoint_routes = Vec::new();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS) {
            let sink_route_active = self
                .facade_gate
                .route_active(execution_units::SINK_RUNTIME_UNIT_ID, &route.0)
                .unwrap_or(false);
            let query_route_active = self
                .facade_gate
                .route_active(execution_units::QUERY_RUNTIME_UNIT_ID, &route.0)
                .unwrap_or(false);
            let query_peer_route_active = self
                .facade_gate
                .route_active(execution_units::QUERY_PEER_RUNTIME_UNIT_ID, &route.0)
                .unwrap_or(false);
            let prefer_query_peer_first = self
                .mirrored_query_peer_routes
                .lock()
                .await
                .contains_key(&route.0);
            let query_endpoint_unit_ids = preferred_internal_query_endpoint_units(
                query_route_active,
                query_peer_route_active,
                prefer_query_peer_first,
            );
            let endpoint_unit_ids = if !query_endpoint_unit_ids.is_empty() {
                query_endpoint_unit_ids
            } else if sink_route_active {
                vec![execution_units::SINK_RUNTIME_UNIT_ID]
            } else {
                Vec::new()
            };
            if !endpoint_unit_ids.is_empty() {
                sink_status_endpoint_routes.push((route, endpoint_unit_ids));
            }
        }
        let node_scoped_sink_status_route = sink_status_request_route_for(&self.node_id.0);
        if self
            .facade_gate
            .route_active(
                execution_units::SINK_RUNTIME_UNIT_ID,
                &node_scoped_sink_status_route.0,
            )
            .unwrap_or(false)
        {
            sink_status_endpoint_routes.push((
                node_scoped_sink_status_route,
                vec![execution_units::SINK_RUNTIME_UNIT_ID],
            ));
        }
        for (route, endpoint_unit_ids) in sink_status_endpoint_routes {
            if endpoint_unit_ids.is_empty()
                || !route_still_active_for_units(&self.facade_gate, &route.0, &endpoint_unit_ids)
                || !spawned_routes.insert(route.0.clone())
            {
                // Not currently selected as sink-status owner, route inactive, or already running.
            } else {
                let active_unit_ids = endpoint_unit_ids.clone();
                let recv_unit_ids = endpoint_unit_ids.clone();
                let facade_gate = self.facade_gate.clone();
                let runtime_gate_state = self.runtime_gate_state.clone();
                let sink = self.sink.clone();
                let api_task = self.api_task.clone();
                let pending_facade = self.pending_facade.clone();
                let facade_service_state = self.facade_service_state.clone();
                let local_node_id = self.node_id.clone();
                let source = self.source.clone();
                let retained_sink_control_state = self.retained_sink_control_state.clone();
                let runtime_state_changed = self.runtime_state_changed.clone();
                let control_failure_uninitialized = self.control_failure_uninitialized.clone();
                let route_key = route.0.clone();
                eprintln!(
                    "fs_meta_runtime_app: spawning sink status endpoint route={}",
                    route.0
                );
                let endpoint =
                    ManagedEndpointTask::spawn_with_recv_units_and_task_units_wait_receive_poll(
                        boundary.clone(),
                        route,
                        format!(
                            "app:{}:{}",
                            ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS
                        ),
                        recv_unit_ids,
                        active_unit_ids.clone(),
                        tokio_util::sync::CancellationToken::new(),
                        move |requests| {
                            let facade_gate = facade_gate.clone();
                            let runtime_gate_state = runtime_gate_state.clone();
                            let sink = sink.clone();
                            let api_task = api_task.clone();
                            let pending_facade = pending_facade.clone();
                            let facade_service_state = facade_service_state.clone();
                            let local_node_id = local_node_id.clone();
                            let source = source.clone();
                            let retained_sink_control_state = retained_sink_control_state.clone();
                            let runtime_state_changed = runtime_state_changed.clone();
                            let control_failure_uninitialized =
                                control_failure_uninitialized.clone();
                            let route_key = route_key.clone();
                            let active_unit_ids = active_unit_ids.clone();
                            async move {
                                let mut responses = Vec::new();
                                for req in requests {
                                    #[cfg(test)]
                                    maybe_pause_runtime_proxy_request("sink_status").await;
                                    if !route_still_active_for_units(
                                        &facade_gate,
                                        &route_key,
                                        &active_unit_ids,
                                    ) {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink status endpoint unavailable reason=route_deactivated_after_pause route={}",
                                            route_key
                                        );
                                        match explicit_empty_sink_status_reply(
                                            &req.metadata().origin_id,
                                            req.metadata().correlation_id,
                                        ) {
                                            Ok(event) => responses.push(event),
                                            Err(err) => eprintln!(
                                                "fs_meta_runtime_app: sink status endpoint deactivated empty reply encode failed: {err}"
                                            ),
                                        }
                                        continue;
                                    }
                                    let mut runtime_state =
                                        RuntimeControlState::from_state_cell(&runtime_gate_state);
                                    let facade_owned_status_route =
                                        Self::internal_status_route_requires_facade_service_state(
                                            &active_unit_ids,
                                        );
                                    let sink_control_op_inflight = sink.control_op_inflight().await;
                                    let expected_uninitialized_sink_status_generation =
                                        if facade_owned_status_route
                                            && !runtime_state.control_initialized()
                                        {
                                            Self::retained_control_generation_for_sink_status(
                                                &source,
                                                &retained_sink_control_state,
                                            )
                                            .await
                                        } else {
                                            None
                                        };
                                    let mut uninitialized_sink_status_ready =
                                        facade_owned_status_route
                                            && !runtime_state.control_initialized()
                                            && Self::sink_status_ready_for_uninitialized_facade_route(
                                                &sink,
                                                expected_uninitialized_sink_status_generation,
                                            )
                                            .await;
                                    if facade_owned_status_route
                                        && !runtime_state.control_initialized()
                                        && control_failure_uninitialized.load(Ordering::Acquire)
                                        && !uninitialized_sink_status_ready
                                        && !sink_control_op_inflight
                                    {
                                        uninitialized_sink_status_ready = Self::wait_for_uninitialized_sink_status_ready_for_facade_route(
                                            &sink,
                                            &runtime_state_changed,
                                            expected_uninitialized_sink_status_generation,
                                        )
                                        .await;
                                        runtime_state = RuntimeControlState::from_state_cell(
                                            &runtime_gate_state,
                                        );
                                    }
                                    if facade_owned_status_route
                                        && !runtime_state.control_initialized()
                                        && !uninitialized_sink_status_ready
                                    {
                                        if sink_control_op_inflight {
                                            eprintln!(
                                                "fs_meta_runtime_app: sink status endpoint unavailable reason=runtime_uninitialized_sink_apply_inflight route={}",
                                                route_key
                                            );
                                            match explicit_empty_sink_status_reply(
                                                &req.metadata().origin_id,
                                                req.metadata().correlation_id,
                                            ) {
                                                Ok(event) => responses.push(event),
                                                Err(err) => eprintln!(
                                                    "fs_meta_runtime_app: sink status endpoint uninitialized apply-pending empty reply encode failed: {err}"
                                                ),
                                            }
                                            continue;
                                        }
                                        eprintln!(
                                            "fs_meta_runtime_app: sink status endpoint unavailable reason=runtime_uninitialized route={}",
                                            route_key
                                        );
                                        continue;
                                    }
                                    let replay_or_apply_pending = runtime_state
                                        .sink_state_replay_required()
                                        || sink_control_op_inflight;
                                    if replay_or_apply_pending && !uninitialized_sink_status_ready {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink status endpoint unavailable reason=runtime_replay_or_sink_apply_pending route={}",
                                            route_key
                                        );
                                        let request_origin_is_local =
                                            req.metadata().origin_id == local_node_id;
                                        if request_origin_is_local
                                            && !runtime_state.source_state_replay_required()
                                        {
                                            match explicit_empty_sink_status_reply(
                                                &req.metadata().origin_id,
                                                req.metadata().correlation_id,
                                            ) {
                                                Ok(event) => responses.push(event),
                                                Err(err) => eprintln!(
                                                    "fs_meta_runtime_app: sink status endpoint replay/apply pending empty reply encode failed: {err}"
                                                ),
                                            }
                                        }
                                        continue;
                                    }
                                    if !Self::internal_status_available_for_local_facade_owner(
                                        &active_unit_ids,
                                        &runtime_gate_state,
                                        &api_task,
                                        &pending_facade,
                                        &facade_service_state,
                                    )
                                    .await
                                    {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink status endpoint unavailable reason=local_facade_not_serving route={}",
                                            route_key
                                        );
                                        continue;
                                    }
                                    match sink
                                        .status_snapshot_nonblocking_for_status_route_key(
                                            &route_key,
                                        )
                                        .await
                                    {
                                        Ok((snapshot, _used_cached_fallback)) => {
                                            if debug_status_endpoint_response_enabled() {
                                                eprintln!(
                                                    "fs_meta_runtime_app: sink status endpoint response {}",
                                                    summarize_sink_status_endpoint(&snapshot)
                                                );
                                            }
                                            match sink_status_reply(
                                                &snapshot,
                                                &req.metadata().origin_id,
                                                req.metadata().correlation_id,
                                            ) {
                                                Ok(event) => responses.push(event),
                                                Err(err) => eprintln!(
                                                    "fs_meta_runtime_app: sink status endpoint reply encode failed: {err}"
                                                ),
                                            }
                                        }
                                        Err(err) => {
                                            let mut terminal_err = err;
                                            if !route_still_active_for_units(
                                                &facade_gate,
                                                &route_key,
                                                &active_unit_ids,
                                            ) {
                                                eprintln!(
                                                    "fs_meta_runtime_app: sink status endpoint fail-closed after route deactivate route={} err={}",
                                                    route_key, terminal_err
                                                );
                                                match explicit_empty_sink_status_reply(
                                                    &req.metadata().origin_id,
                                                    req.metadata().correlation_id,
                                                ) {
                                                    Ok(event) => responses.push(event),
                                                    Err(reply_err) => eprintln!(
                                                        "fs_meta_runtime_app: sink status endpoint deactivated empty reply encode failed: {reply_err}"
                                                    ),
                                                }
                                                continue;
                                            }
                                            let sink_control_inflight =
                                                sink.control_op_inflight().await;
                                            if matches!(terminal_err, CnxError::Timeout)
                                                && !sink_control_inflight
                                                && let Ok(cached_snapshot) =
                                                    sink.cached_status_snapshot_with_failure()
                                                && sink_status_snapshot_has_ready_scheduled_groups(
                                                    &cached_snapshot,
                                                )
                                            {
                                                eprintln!(
                                                    "fs_meta_runtime_app: sink status endpoint cached fallback err={} {}",
                                                    terminal_err,
                                                    summarize_sink_status_endpoint(
                                                        &cached_snapshot
                                                    )
                                                );
                                                match sink_status_reply(
                                                    &cached_snapshot,
                                                    &req.metadata().origin_id,
                                                    req.metadata().correlation_id,
                                                ) {
                                                    Ok(event) => responses.push(event),
                                                    Err(reply_err) => eprintln!(
                                                        "fs_meta_runtime_app: sink status endpoint cached fallback encode failed: {reply_err}"
                                                    ),
                                                }
                                                continue;
                                            }
                                            if matches!(terminal_err, CnxError::Timeout)
                                                && !sink_control_inflight
                                                && !sink.is_worker()
                                            {
                                                match tokio::time::timeout(
                                                    INTERNAL_SINK_STATUS_BLOCKING_FALLBACK_BUDGET,
                                                    sink.status_snapshot_with_failure(),
                                                )
                                                .await
                                                {
                                                    Ok(Ok(snapshot)) => {
                                                        if !route_still_active_for_units(
                                                            &facade_gate,
                                                            &route_key,
                                                            &active_unit_ids,
                                                        ) {
                                                            eprintln!(
                                                                "fs_meta_runtime_app: sink status endpoint blocking fallback deactivated route={}",
                                                                route_key
                                                            );
                                                            match explicit_empty_sink_status_reply(
                                                                &req.metadata().origin_id,
                                                                req.metadata().correlation_id,
                                                            ) {
                                                                Ok(event) => responses.push(event),
                                                                Err(reply_err) => eprintln!(
                                                                    "fs_meta_runtime_app: sink status endpoint blocking fallback deactivated empty reply encode failed: {reply_err}"
                                                                ),
                                                            }
                                                            continue;
                                                        }
                                                        eprintln!(
                                                            "fs_meta_runtime_app: sink status endpoint blocking fallback {}",
                                                            summarize_sink_status_endpoint(
                                                                &snapshot
                                                            )
                                                        );
                                                        match sink_status_reply(
                                                            &snapshot,
                                                            &req.metadata().origin_id,
                                                            req.metadata().correlation_id,
                                                        ) {
                                                            Ok(event) => responses.push(event),
                                                            Err(reply_err) => eprintln!(
                                                                "fs_meta_runtime_app: sink status endpoint blocking fallback encode failed: {reply_err}"
                                                            ),
                                                        }
                                                        continue;
                                                    }
                                                    Ok(Err(fallback_err)) => {
                                                        terminal_err = fallback_err.into_error();
                                                    }
                                                    Err(_) => {
                                                        terminal_err = CnxError::Timeout;
                                                    }
                                                }
                                            }
                                            if matches!(terminal_err, CnxError::Timeout)
                                                && RuntimeControlState::from_state_cell(
                                                    &runtime_gate_state,
                                                )
                                                .control_initialized()
                                            {
                                                let route_still_active =
                                                    route_still_active_for_units(
                                                        &facade_gate,
                                                        &route_key,
                                                        &active_unit_ids,
                                                    );
                                                if route_still_active {
                                                    eprintln!(
                                                        "fs_meta_runtime_app: sink status endpoint initialized timeout empty fallback route={}",
                                                        route_key
                                                    );
                                                    match explicit_empty_sink_status_reply(
                                                        &req.metadata().origin_id,
                                                        req.metadata().correlation_id,
                                                    ) {
                                                        Ok(event) => responses.push(event),
                                                        Err(reply_err) => eprintln!(
                                                            "fs_meta_runtime_app: sink status endpoint initialized timeout empty fallback encode failed: {reply_err}"
                                                        ),
                                                    }
                                                    continue;
                                                }
                                            }
                                            eprintln!(
                                                "fs_meta_runtime_app: sink status endpoint failed err={}",
                                                terminal_err
                                            );
                                        }
                                    }
                                }
                                responses
                            }
                        },
                    );
                tasks.push(endpoint);
            }
        }
        let mut source_status_endpoint_routes = Vec::new();
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS) {
            let source_route_active = self
                .facade_gate
                .route_active(execution_units::SOURCE_RUNTIME_UNIT_ID, &route.0)
                .unwrap_or(false);
            let endpoint_unit_ids = if source_route_active {
                vec![execution_units::SOURCE_RUNTIME_UNIT_ID]
            } else {
                preferred_internal_query_endpoint_units(query_active, query_peer_active, false)
            };
            source_status_endpoint_routes.push((route, endpoint_unit_ids));
        }
        let node_scoped_source_status_route = source_status_request_route_for(&self.node_id.0);
        if self
            .facade_gate
            .route_active(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                &node_scoped_source_status_route.0,
            )
            .unwrap_or(false)
        {
            source_status_endpoint_routes.push((
                node_scoped_source_status_route,
                vec![execution_units::SOURCE_RUNTIME_UNIT_ID],
            ));
        }
        for (route, endpoint_unit_ids) in source_status_endpoint_routes {
            if endpoint_unit_ids.is_empty()
                || !spawned_routes.insert(route.0.clone())
                || !route_still_active_for_units(&self.facade_gate, &route.0, &endpoint_unit_ids)
            {
                // Not currently selected as source-status owner, route inactive, or already running.
            } else {
                let active_unit_ids = endpoint_unit_ids.clone();
                let recv_unit_ids = endpoint_unit_ids.clone();
                let source = self.source.clone();
                let source_repair_recovery = self.source_repair_recovery();
                let source_repair_recovery_without_proxy_ready =
                    self.source_repair_recovery_without_proxy_ready();
                let api_task = self.api_task.clone();
                let pending_facade = self.pending_facade.clone();
                let facade_service_state = self.facade_service_state.clone();
                let node_id = self.node_id.clone();
                let boundary_for_source_status_rearm = boundary.clone();
                let facade_gate_for_source_status = self.facade_gate.clone();
                let runtime_gate_state_for_source_status = self.runtime_gate_state.clone();
                let runtime_endpoint_tasks_for_source_status = self.runtime_endpoint_tasks.clone();
                let source_rescan_proxy_ready_generation =
                    self.source_rescan_proxy_ready_generation.clone();
                let control_failure_uninitialized = self.control_failure_uninitialized.clone();
                let source_rescan_proxy_route_key =
                    source_rescan_request_route_for(&self.node_id.0).0;
                let route_key = route.0.clone();
                eprintln!(
                    "fs_meta_runtime_app: spawning source status endpoint route={}",
                    route.0
                );
                let endpoint =
                    ManagedEndpointTask::spawn_with_recv_units_and_task_units_wait_receive_poll(
                        boundary.clone(),
                        route,
                        format!(
                            "app:{}:{}",
                            ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS
                        ),
                        recv_unit_ids,
                        active_unit_ids.clone(),
                        tokio_util::sync::CancellationToken::new(),
                        move |requests| {
                            let active_unit_ids = active_unit_ids.clone();
                            let facade_gate_for_route = facade_gate_for_source_status.clone();
                            let source = source.clone();
                            let source_repair_recovery = source_repair_recovery.clone();
                            let source_repair_recovery_without_proxy_ready =
                                source_repair_recovery_without_proxy_ready.clone();
                            let api_task = api_task.clone();
                            let pending_facade = pending_facade.clone();
                            let facade_service_state = facade_service_state.clone();
                            let node_id = node_id.clone();
                            let boundary_for_source_status_rearm =
                                boundary_for_source_status_rearm.clone();
                            let facade_gate_for_source_status =
                                facade_gate_for_source_status.clone();
                            let runtime_gate_state_for_source_status =
                                runtime_gate_state_for_source_status.clone();
                            let runtime_endpoint_tasks_for_source_status =
                                runtime_endpoint_tasks_for_source_status.clone();
                            let source_rescan_proxy_ready_generation =
                                source_rescan_proxy_ready_generation.clone();
                            let control_failure_uninitialized =
                                control_failure_uninitialized.clone();
                            let source_rescan_proxy_route_key =
                                source_rescan_proxy_route_key.clone();
                            let route_key = route_key.clone();
                            async move {
                                let mut responses = Vec::new();
                                for req in requests {
                                    if !Self::internal_status_available_for_local_facade_owner(
                                        &active_unit_ids,
                                        &runtime_gate_state_for_source_status,
                                        &api_task,
                                        &pending_facade,
                                        &facade_service_state,
                                    )
                                    .await
                                    {
                                        eprintln!(
                                            "fs_meta_runtime_app: source status endpoint unavailable reason=local_facade_not_serving route={}",
                                            route_key
                                        );
                                        continue;
                                    }
                                    if !route_still_active_for_units(
                                        &facade_gate_for_route,
                                        &route_key,
                                        &active_unit_ids,
                                    ) {
                                        eprintln!(
                                            "fs_meta_runtime_app: source status endpoint unavailable reason=route_deactivated route={}",
                                            route_key
                                        );
                                        continue;
                                    }
                                    let trace_id = next_source_status_endpoint_trace_id();
                                    let route_name = format!(
                                        "{}:{}",
                                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS
                                    );
                                    let mut trace_guard = SourceStatusEndpointTraceGuard::new(
                                        route_name,
                                        req.metadata().correlation_id,
                                        trace_id,
                                        "before_source_snapshot_await",
                                    );
                                    if debug_source_status_lifecycle_enabled() {
                                        eprintln!(
                                            "fs_meta_runtime_app: source status endpoint begin correlation={:?} trace_id={}",
                                            req.metadata().correlation_id,
                                            trace_id
                                        );
                                    }
                                    #[cfg(test)]
                                    maybe_pause_runtime_proxy_request("source_status").await;
                                    if !route_still_active_for_units(
                                        &facade_gate_for_route,
                                        &route_key,
                                        &active_unit_ids,
                                    ) {
                                        eprintln!(
                                            "fs_meta_runtime_app: source status endpoint unavailable reason=route_deactivated_after_pause route={}",
                                            route_key
                                        );
                                        continue;
                                    }
                                    let manual_rescan_delivery_evidence =
                                    crate::query::api::source_status_request_requires_manual_rescan_delivery_evidence(
                                        req.payload_bytes(),
                                    );
                                    let current_owner_health_evidence =
                                    crate::query::api::source_status_request_requires_current_owner_health_evidence(
                                        req.payload_bytes(),
                                    );
                                    let scan_audit_admission_release =
                                    crate::query::api::source_status_request_requires_scan_audit_admission_release(
                                        req.payload_bytes(),
                                    );
                                    let source_status_live_probe_timeout =
                                        crate::query::api::source_status_request_live_probe_timeout(
                                            req.payload_bytes(),
                                        );
                                    let runtime_state = RuntimeControlState::from_state_cell(
                                        &runtime_gate_state_for_source_status,
                                    );
                                    let source_control_apply_inflight =
                                        runtime_state.source_control_apply_inflight();
                                    if source_control_apply_inflight {
                                        eprintln!(
                                            "fs_meta_runtime_app: source status endpoint pending reason=runtime_source_apply_pending route={}",
                                            route_key
                                        );
                                    }
                                    let source_state_pending_observation = !runtime_state
                                        .source_state_current()
                                        && !current_owner_health_evidence
                                        && !manual_rescan_delivery_evidence
                                        && !scan_audit_admission_release;
                                    if debug_source_status_lifecycle_enabled() {
                                        eprintln!(
                                            "fs_meta_runtime_app: source status endpoint request node={} payload_bytes={} manual_rescan_delivery={} release_scan_audit_admission={} live_probe_timeout_ms={:?} correlation={:?} trace_id={}",
                                            node_id.0,
                                            req.payload_bytes().len(),
                                            manual_rescan_delivery_evidence,
                                            scan_audit_admission_release,
                                            source_status_live_probe_timeout
                                                .map(|timeout| timeout.as_millis()),
                                            req.metadata().correlation_id,
                                            trace_id
                                        );
                                    }
                                    let manual_rescan_delivery_probe_budget =
                                        if manual_rescan_delivery_evidence {
                                            Some(source_status_live_probe_timeout.unwrap_or(
                                                MANUAL_RESCAN_SOURCE_STATUS_DEFAULT_PROBE_BUDGET,
                                            ))
                                        } else {
                                            None
                                        };
                                    let manual_rescan_delivery_deadline =
                                        manual_rescan_delivery_probe_budget.map(|budget| {
                                            tokio::time::Instant::now()
                                                .checked_add(budget)
                                                .unwrap_or_else(tokio::time::Instant::now)
                                        });
                                    let retained_source_replay_from_control_failure =
                                        manual_rescan_delivery_evidence
                                            && runtime_state.source_state_replay_required()
                                            && control_failure_uninitialized
                                                .load(Ordering::Acquire);
                                    let (snapshot, used_cached_fallback) =
                                        if source_control_apply_inflight
                                            || source_state_pending_observation
                                            || retained_source_replay_from_control_failure
                                        {
                                            source
                                            .source_state_pending_observability_snapshot_for_status_route()
                                            .await
                                        } else if scan_audit_admission_release {
                                            let release_budget = source_status_live_probe_timeout
                                                .unwrap_or(Duration::from_millis(500))
                                                .min(Duration::from_secs(2));
                                            match tokio::time::timeout(
                                                release_budget,
                                                source
                                                    .open_scan_audit_admission_and_trigger_rescan_when_ready_epoch_with_failure(),
                                            )
                                            .await
                                            {
                                                Ok(Ok(epoch)) => {
                                                    eprintln!(
                                                        "fs_meta_runtime_app: release_source_scan_audit_after_sink_scope_ready_if_needed source-status ok node={} route={} epoch={} correlation={:?} trace_id={}",
                                                        node_id.0,
                                                        route_key,
                                                        epoch,
                                                        req.metadata().correlation_id,
                                                        trace_id
                                                    );
                                                }
                                                Ok(Err(err)) => {
                                                    eprintln!(
                                                        "fs_meta_runtime_app: release_source_scan_audit_after_sink_scope_ready_if_needed source-status failed node={} route={} err={} correlation={:?} trace_id={}",
                                                        node_id.0,
                                                        route_key,
                                                        err.as_error(),
                                                        req.metadata().correlation_id,
                                                        trace_id
                                                    );
                                                }
                                                Err(_) => {
                                                    eprintln!(
                                                        "fs_meta_runtime_app: release_source_scan_audit_after_sink_scope_ready_if_needed source-status timed out node={} route={} budget_ms={} correlation={:?} trace_id={}",
                                                        node_id.0,
                                                        route_key,
                                                        release_budget.as_millis(),
                                                        req.metadata().correlation_id,
                                                        trace_id
                                                    );
                                                }
                                            }
                                            if !runtime_state.source_state_current() {
                                                source
                                                    .source_state_pending_observability_snapshot_for_status_route()
                                                    .await
                                            } else {
                                                source
                                                    .observability_snapshot_nonblocking_for_status_route_with_timeout(
                                                        source_status_live_probe_timeout,
                                                    )
                                                    .await
                                            }
                                        } else if current_owner_health_evidence {
                                            source
                                            .current_owner_health_observability_snapshot_for_status_route_with_timeout(
                                                source_status_live_probe_timeout,
                                            )
                                            .await
                                        } else if manual_rescan_delivery_evidence {
                                            if !runtime_state.source_state_current()
                                                && !source.is_worker()
                                            {
                                                source
                                                .source_state_pending_observability_snapshot_for_status_route()
                                                .await
                                            } else {
                                                let probe_budget = manual_rescan_delivery_probe_budget
                                                .unwrap_or(
                                                MANUAL_RESCAN_SOURCE_STATUS_DEFAULT_PROBE_BUDGET,
                                            );
                                                let probe_deadline =
                                                    manual_rescan_delivery_deadline
                                                        .unwrap_or_else(tokio::time::Instant::now);
                                                let rearm_budget = probe_deadline
                                                    .saturating_duration_since(
                                                        tokio::time::Instant::now(),
                                                    );
                                                let mut source_rescan_proxy_route_groups =
                                                    None::<(u64, BTreeSet<String>)>;
                                                let non_target_status_snapshot = if source
                                                    .is_worker()
                                                {
                                                    let route_targets_local_source =
                                                        source_rescan_route_targets_node(
                                                            &facade_gate_for_source_status,
                                                            &source_rescan_proxy_route_key,
                                                            &node_id,
                                                        );
                                                    let source_rescan_proxy_ready =
                                                        route_targets_local_source
                                                            &&
                                                    ensure_worker_source_rescan_proxy_ready_until(
                                                        boundary_for_source_status_rearm.clone(),
                                                        source.clone(),
                                                        node_id.clone(),
                                                        &facade_gate_for_source_status,
                                                        &source_rescan_proxy_route_key,
                                                        source_rescan_proxy_ready_generation
                                                            .clone(),
                                                        &runtime_endpoint_tasks_for_source_status,
                                                        source_repair_recovery_without_proxy_ready
                                                            .clone(),
                                                        probe_deadline,
                                                        true,
                                                    )
                                                    .await;
                                                    if !source_rescan_proxy_ready {
                                                        eprintln!(
                                                            "fs_meta_runtime_app: source status endpoint manual-rescan route not receive-armed node={} route={} correlation={:?} trace_id={}",
                                                            node_id.0,
                                                            source_rescan_proxy_route_key,
                                                            req.metadata().correlation_id,
                                                            trace_id
                                                        );
                                                    }
                                                    if source_rescan_proxy_ready {
                                                        source_rescan_proxy_route_groups =
                                                        source_rescan_route_groups_for_current_generation(
                                                            &facade_gate_for_source_status,
                                                            &source_rescan_proxy_route_key,
                                                        );
                                                    }
                                                    let route_group_ids =
                                                        source_rescan_proxy_route_groups
                                                            .as_ref()
                                                            .map(|(_, groups)| groups);
                                                    let app_route_snapshot =
                                                        if source_rescan_proxy_ready {
                                                            source
                                                        .manual_rescan_app_route_observability_snapshot_for_status_route(
                                                            route_group_ids,
                                                        )
                                                        .await
                                                        } else {
                                                            None
                                                        };
                                                    let app_route_snapshot_ready =
                                                        app_route_snapshot.is_some();
                                                    let (mut snapshot, used_cached_fallback) =
                                                        if let Some(snapshot) = app_route_snapshot {
                                                            (snapshot, true)
                                                        } else {
                                                            source
                                                            .source_state_pending_observability_snapshot_for_status_route()
                                                            .await
                                                        };
                                                    if source_rescan_proxy_ready
                                                        && app_route_snapshot_ready
                                                    {
                                                        if let Some((generation, groups)) =
                                                            source_rescan_proxy_route_groups
                                                                .as_ref()
                                                        {
                                                            annotate_manual_rescan_route_receivable_evidence_for_route_groups(
                                                            &mut snapshot,
                                                            &node_id,
                                                            &source_rescan_proxy_route_key,
                                                            Some(*generation),
                                                            groups,
                                                        );
                                                        } else {
                                                            let generation =
                                                                source_rescan_route_semantic_generation(
                                                                    &facade_gate_for_source_status,
                                                                    &source_rescan_proxy_route_key,
                                                                );
                                                            if generation == 0 {
                                                                annotate_manual_rescan_route_receivable_evidence(
                                                                    &mut snapshot,
                                                                    &node_id,
                                                                );
                                                            } else {
                                                                annotate_manual_rescan_route_receivable_evidence_for_current_groups_and_generation(
                                                                    &mut snapshot,
                                                                    &node_id,
                                                                    &source_rescan_proxy_route_key,
                                                                    generation,
                                                                );
                                                            }
                                                        }
                                                    }
                                                    Some((snapshot, used_cached_fallback))
                                                } else {
                                                    let rearm_result = if rearm_budget.is_zero() {
                                                        Err(SourceFailure::from(CnxError::Timeout))
                                                    } else {
                                                        match tokio::time::timeout(
                                                        rearm_budget,
                                                        source
                                                            .rearm_source_rescan_request_endpoints_with_failure(
                                                                boundary_for_source_status_rearm.clone(),
                                                            ),
                                                        )
                                                        .await
                                                    {
                                                        Ok(result) => result,
                                                        Err(_) => {
                                                        Err(SourceFailure::from(CnxError::Timeout))
                                                        }
                                                    }
                                                    };
                                                    if let Err(err) = rearm_result {
                                                        eprintln!(
                                                            "fs_meta_runtime_app: source status endpoint manual-rescan rearm failed node={} correlation={:?} trace_id={} budget_ms={} err={}",
                                                            node_id.0,
                                                            req.metadata().correlation_id,
                                                            trace_id,
                                                            probe_budget.as_millis(),
                                                            err.as_error()
                                                        );
                                                        continue;
                                                    }
                                                    if !source_rescan_route_active_for_current_generation(
                                                    &facade_gate_for_source_status,
                                                    &source_rescan_proxy_route_key,
                                                ) {
                                                    continue;
                                                }
                                                    let acceptance_budget = probe_deadline
                                                        .saturating_duration_since(
                                                            tokio::time::Instant::now(),
                                                        );
                                                    if acceptance_budget.is_zero() {
                                                        Some(
                                                        source
                                                            .source_state_pending_observability_snapshot_for_status_route()
                                                            .await,
                                                    )
                                                    } else {
                                                        let delivery_acceptance = match tokio::time::timeout(
                                                        acceptance_budget,
                                                        source
                                                            .targeted_rescan_delivery_acceptance_with_failure(),
                                                    )
                                                    .await
                                                    {
                                                        Ok(Ok(acceptance)) => acceptance,
                                                        Ok(Err(err)) => {
                                                            eprintln!(
                                                                "fs_meta_runtime_app: source status endpoint manual-rescan local delivery acceptance check failed node={} correlation={:?} trace_id={} budget_ms={} err={}",
                                                                node_id.0,
                                                                req.metadata().correlation_id,
                                                                trace_id,
                                                                probe_budget.as_millis(),
                                                                err.as_error()
                                                            );
                                                            SourceTargetedRescanDeliveryAcceptance::NotLocalScanRoot
                                                        }
                                                        Err(_) => {
                                                            eprintln!(
                                                                "fs_meta_runtime_app: source status endpoint manual-rescan local delivery acceptance check timed out node={} correlation={:?} trace_id={} budget_ms={}",
                                                                node_id.0,
                                                                req.metadata().correlation_id,
                                                                trace_id,
                                                                probe_budget.as_millis()
                                                            );
                                                            SourceTargetedRescanDeliveryAcceptance::NotLocalScanRoot
                                                        }
                                                    };
                                                        if !matches!(
                                                        delivery_acceptance,
                                                        SourceTargetedRescanDeliveryAcceptance::Accepted
                                                    ) {
                                                        Some(
                                                            source
                                                                .source_state_pending_observability_snapshot_for_status_route()
                                                                .await,
                                                        )
                                                    } else {
                                                let snapshot_budget = probe_deadline
                                                    .saturating_duration_since(
                                                        tokio::time::Instant::now(),
                                                    );
                                                let (
                                                    mut snapshot,
                                                    used_cached_fallback,
                                                ) = source
                                                    .observability_snapshot_nonblocking_for_status_route_with_timeout(
                                                        Some(snapshot_budget),
                                                    )
                                                    .await;
                                                annotate_manual_rescan_route_receivable_evidence(
                                                    &mut snapshot,
                                                    &node_id,
                                                );
                                                Some((snapshot, used_cached_fallback))
                                                    }
                                                    }
                                                };
                                                if let Some(snapshot) = non_target_status_snapshot {
                                                snapshot
                                            } else if let Some(snapshot) = source
                                            .cached_manual_rescan_delivery_observability_snapshot_for_status_route(
                                                source_rescan_proxy_route_groups
                                                    .as_ref()
                                                    .map(|(_, groups)| groups),
                                            )
                                        {
                                            (snapshot, true)
                                        } else {
                                            let repair_budget = probe_deadline
                                                .saturating_duration_since(tokio::time::Instant::now());
                                            if repair_budget.is_zero() {
                                                eprintln!(
                                                    "fs_meta_runtime_app: source status endpoint manual-rescan source repair failed node={} correlation={:?} trace_id={} budget_ms={} err=operation timed out",
                                                    node_id.0,
                                                    req.metadata().correlation_id,
                                                    trace_id,
                                                    probe_budget.as_millis()
                                                );
                                                continue;
                                            }
                                            match tokio::time::timeout(
                                                repair_budget,
                                                (source_repair_recovery)(),
                                            )
                                            .await
                                            {
                                                Ok(Ok(())) => {}
                                                Ok(Err(err)) => {
                                                    eprintln!(
                                                        "fs_meta_runtime_app: source status endpoint manual-rescan source repair failed node={} correlation={:?} trace_id={} budget_ms={} err={}",
                                                        node_id.0,
                                                        req.metadata().correlation_id,
                                                        trace_id,
                                                        probe_budget.as_millis(),
                                                        err
                                                    );
                                                    continue;
                                                }
                                                Err(_) => {
                                                    eprintln!(
                                                        "fs_meta_runtime_app: source status endpoint manual-rescan source repair failed node={} correlation={:?} trace_id={} budget_ms={} err=operation timed out",
                                                        node_id.0,
                                                        req.metadata().correlation_id,
                                                        trace_id,
                                                        probe_budget.as_millis()
                                                    );
                                                    continue;
                                                }
                                            }
                                        let snapshot_budget = probe_deadline
                                            .saturating_duration_since(tokio::time::Instant::now());
                                        source
                                            .observability_snapshot_nonblocking_for_status_route_with_timeout(
                                                Some(snapshot_budget),
                                            )
                                            .await
                                        }
                                            }
                                        } else {
                                            let remaining_live_probe_budget =
                                                manual_rescan_delivery_deadline.map(|deadline| {
                                                    deadline.saturating_duration_since(
                                                        tokio::time::Instant::now(),
                                                    )
                                                });
                                            source
                                            .observability_snapshot_nonblocking_for_status_route_with_timeout(
                                                remaining_live_probe_budget
                                                    .or(source_status_live_probe_timeout),
                                            )
                                            .await
                                        };
                                    if used_cached_fallback && source.is_worker() {
                                        eprintln!(
                                            "fs_meta_runtime_app: source status endpoint using cached/degraded source observability snapshot node={} correlation={:?} trace_id={}",
                                            node_id.0,
                                            req.metadata().correlation_id,
                                            trace_id
                                        );
                                    }
                                    trace_guard.phase("after_source_snapshot_await");
                                    if debug_status_endpoint_response_enabled() {
                                        eprintln!(
                                            "fs_meta_runtime_app: source status endpoint response node={} groups={} runners={} correlation={:?} trace_id={}",
                                            node_id.0,
                                            snapshot.source_primary_by_group.len(),
                                            snapshot.last_force_find_runner_by_group.len(),
                                            req.metadata().correlation_id,
                                            trace_id
                                        );
                                    }
                                    if debug_force_find_runner_capture_enabled() {
                                        eprintln!(
                                            "fs_meta_runtime_app: source status endpoint runner_capture node={} correlation={:?} trace_id={} last_runner={:?} inflight={:?}",
                                            node_id.0,
                                            req.metadata().correlation_id,
                                            trace_id,
                                            summarize_group_string_map(
                                                &snapshot.last_force_find_runner_by_group
                                            ),
                                            snapshot.force_find_inflight_groups
                                        );
                                    }
                                    if let Ok(payload) = rmp_serde::to_vec_named(&snapshot) {
                                        responses.push(Event::new(
                                            EventMetadata {
                                                origin_id: node_id.clone(),
                                                timestamp_us: now_us(),
                                                logical_ts: None,
                                                correlation_id: req.metadata().correlation_id,
                                                ingress_auth: None,
                                                trace: None,
                                            },
                                            bytes::Bytes::from(payload),
                                        ));
                                    }
                                    trace_guard.complete();
                                }
                                responses
                            }
                        },
                    );
                tasks.push(endpoint);
            }
        }
        if matches!(&*self.sink, SinkFacade::Worker(_)) {
            let mut sink_query_route_keys = BTreeSet::new();
            if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY) {
                sink_query_route_keys.insert(route.0);
            }
            let local_owner_sink_query_route =
                crate::runtime::routes::sink_query_request_route_for(&self.node_id.0).0;
            for unit_id in [
                execution_units::QUERY_RUNTIME_UNIT_ID,
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            ] {
                for route_key in self.facade_gate.active_route_keys(unit_id)? {
                    if route_key == format!("{}.req", ROUTE_KEY_SINK_QUERY_INTERNAL)
                        || route_key == local_owner_sink_query_route
                    {
                        sink_query_route_keys.insert(route_key);
                    }
                }
            }
            for route_key in sink_query_route_keys {
                let prefer_query_peer_first = self
                    .mirrored_query_peer_routes
                    .lock()
                    .await
                    .contains_key(&route_key);
                let preferred_endpoint_unit_ids = preferred_internal_query_endpoint_units(
                    query_active,
                    query_peer_active,
                    prefer_query_peer_first,
                );
                let sink_route_active =
                    sink_query_route_active_for_sink_unit(&self.facade_gate, &route_key);
                let active_unit_ids = if !preferred_endpoint_unit_ids.is_empty()
                    && route_still_active_for_units(
                        &self.facade_gate,
                        &route_key,
                        &preferred_endpoint_unit_ids,
                    ) {
                    preferred_endpoint_unit_ids.clone()
                } else if sink_route_active {
                    vec![execution_units::SINK_RUNTIME_UNIT_ID]
                } else {
                    Vec::new()
                };
                if active_unit_ids.is_empty() || !spawned_routes.insert(route_key.clone()) {
                    // Not currently selected as query/query-peer/sink sink-query owner, route inactive, or already running.
                    continue;
                }
                eprintln!(
                    "fs_meta_runtime_app: spawning worker-backed sink query endpoint route={}",
                    route_key
                );
                let mut recv_unit_ids = active_unit_ids.clone();
                if !recv_unit_ids.contains(&execution_units::SINK_RUNTIME_UNIT_ID) {
                    recv_unit_ids.push(execution_units::SINK_RUNTIME_UNIT_ID);
                }
                let facade_gate = self.facade_gate.clone();
                let sink = self.sink.clone();
                let endpoint = ManagedEndpointTask::spawn_with_units_without_ready_wait(
                    boundary.clone(),
                    RouteKey(route_key.clone()),
                    format!("app:{}:{}", ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY),
                    recv_unit_ids,
                    tokio_util::sync::CancellationToken::new(),
                    move |requests| {
                        let facade_gate = facade_gate.clone();
                        let route_key = route_key.clone();
                        let active_unit_ids = active_unit_ids.clone();
                        let sink = sink.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                    req.payload_bytes(),
                                ) else {
                                    continue;
                                };
                                let trace_sink_query_route = debug_sink_query_route_trace_enabled();
                                if trace_sink_query_route {
                                    eprintln!(
                                        "fs_meta_runtime_app: worker-backed sink query request route={} selected_group={:?} recursive={} path={}",
                                        route_key,
                                        params.scope.selected_group,
                                        params.scope.recursive,
                                        String::from_utf8_lossy(&params.scope.path)
                                    );
                                }
                                if !worker_sink_query_route_still_active_for_units(
                                    &facade_gate,
                                    &route_key,
                                    &active_unit_ids,
                                ) {
                                    eprintln!(
                                        "fs_meta_runtime_app: worker-backed sink query unavailable reason=route_deactivated_after_recv route={}",
                                        route_key
                                    );
                                    match selected_group_empty_materialized_reply(
                                        &params,
                                        req.metadata().correlation_id,
                                    ) {
                                        Ok(Some(event)) => responses.push(event),
                                        Ok(None) => {}
                                        Err(err) => eprintln!(
                                            "fs_meta_runtime_app: worker-backed sink query deactivated empty reply encode failed: {err}"
                                        ),
                                    }
                                    continue;
                                }
                                match sink
                                    .materialized_query_nonblocking_with_failure(&params)
                                    .await
                                {
                                    Ok(mut events) => {
                                        if trace_sink_query_route {
                                            eprintln!(
                                                "fs_meta_runtime_app: worker-backed sink query response route={} events={}",
                                                route_key,
                                                events.len()
                                            );
                                        }
                                        for event in &mut events {
                                            responses.push(
                                                sink_query_reply_event_from_materialized_event(
                                                    event,
                                                    req.metadata().correlation_id,
                                                ),
                                            );
                                        }
                                    }
                                    Err(err) => {
                                        let err = err.into_error();
                                        eprintln!(
                                            "fs_meta_runtime_app: worker-backed sink query failed route={} err={}",
                                            route_key, err
                                        );
                                        responses.push(sink_query_proxy_error_reply(
                                            &err,
                                            req.metadata().correlation_id,
                                        ));
                                    }
                                }
                            }
                            responses
                        }
                    },
                );
                tasks.push(endpoint);
            }
        }
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY) {
            if internal_query_active {
                let prefer_query_peer_first = self
                    .mirrored_query_peer_routes
                    .lock()
                    .await
                    .contains_key(&route.0);
                let endpoint_unit_ids = preferred_internal_query_endpoint_units(
                    query_active,
                    query_peer_active,
                    prefer_query_peer_first,
                );
                if endpoint_unit_ids.is_empty()
                    || !route_still_active_for_units(
                        &self.facade_gate,
                        &route.0,
                        &endpoint_unit_ids,
                    )
                    || !spawned_routes.insert(route.0.clone())
                {
                    // Not currently selected as query/query-peer proxy owner, route inactive, or already running.
                } else {
                    eprintln!(
                        "fs_meta_runtime_app: spawning sink query proxy endpoint route={}",
                        route.0
                    );
                    let route_key = route.0.clone();
                    let facade_gate = self.facade_gate.clone();
                    let boundary_for_calls = boundary.clone();
                    let sink = self.sink.clone();
                    let source = self.source.clone();
                    let node_id = self.node_id.clone();
                    let endpoint = ManagedEndpointTask::spawn_with_units_without_ready_wait(
                        boundary.clone(),
                        route,
                        format!(
                            "app:{}:{}",
                            ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY
                        ),
                        endpoint_unit_ids,
                        tokio_util::sync::CancellationToken::new(),
                        move |requests| {
                            let route_key = route_key.clone();
                            let facade_gate = facade_gate.clone();
                            let boundary_for_calls = boundary_for_calls.clone();
                            let sink = sink.clone();
                            let source = source.clone();
                            let node_id = node_id.clone();
                            async move {
                                let mut responses = Vec::new();
                                for req in requests {
                                    let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                        req.payload_bytes(),
                                    ) else {
                                        continue;
                                    };
                                    let trace_sink_query_route =
                                        debug_sink_query_route_trace_enabled();
                                    if trace_sink_query_route {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink query proxy request selected_group={:?} recursive={} path={}",
                                            params.scope.selected_group,
                                            params.scope.recursive,
                                            String::from_utf8_lossy(&params.scope.path)
                                        );
                                    }
                                    #[cfg(test)]
                                    maybe_pause_runtime_proxy_request("sink_query_proxy").await;
                                    if !internal_query_route_still_active(&facade_gate, &route_key)
                                    {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink query proxy unavailable reason=route_deactivated_after_pause route={}",
                                            route_key
                                        );
                                        if params.scope.selected_group.is_some() {
                                            // This request has already entered the selected-group
                                            // proxy route and then lost ownership during a later
                                            // deactivate. Fail closed immediately with an explicit
                                            // empty reply instead of spending the full query timeout
                                            // on a now-inactive route.
                                            match selected_group_empty_materialized_reply(
                                                &params,
                                                req.metadata().correlation_id,
                                            ) {
                                                Ok(Some(event)) => {
                                                    responses.push(event);
                                                }
                                                Ok(None) => {}
                                                Err(err) => {
                                                    eprintln!(
                                                        "fs_meta_runtime_app: sink query proxy deactivated empty reply encode failed: {err}"
                                                    );
                                                }
                                            }
                                        }
                                        continue;
                                    }
                                    let result = sink
                                        .materialized_query_nonblocking_with_failure(&params)
                                        .await;
                                    match result {
                                        Ok(mut events) => {
                                            if trace_sink_query_route {
                                                eprintln!(
                                                    "fs_meta_runtime_app: sink query proxy response events={}",
                                                    events.len()
                                                );
                                                for event in &events {
                                                    match rmp_serde::from_slice::<
                                                        MaterializedQueryPayload,
                                                    >(
                                                        event.payload_bytes()
                                                    ) {
                                                        Ok(MaterializedQueryPayload::Tree(
                                                            payload,
                                                        )) => {
                                                            eprintln!(
                                                                "fs_meta_runtime_app: sink query proxy payload group={} root_exists={} entries={} has_children={}",
                                                                event.metadata().origin_id.0,
                                                                payload.root.exists,
                                                                payload.entries.len(),
                                                                payload.root.has_children
                                                            );
                                                        }
                                                        Ok(MaterializedQueryPayload::Stats(_)) => {
                                                            eprintln!(
                                                                "fs_meta_runtime_app: sink query proxy payload group={} stats",
                                                                event.metadata().origin_id.0
                                                            );
                                                        }
                                                        Err(err) => {
                                                            eprintln!(
                                                                "fs_meta_runtime_app: sink query proxy payload decode failed group={} err={}",
                                                                event.metadata().origin_id.0,
                                                                err
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            let bridge_decision_without_status =
                                                selected_group_sink_query_bridge_decision_without_status(
                                                    &params,
                                                    &events,
                                                );
                                            let (
                                                bridge_sink_status_snapshot,
                                                local_selected_group_bridge_eligible,
                                                should_bridge,
                                            ) = if let Some(should_bridge) =
                                                bridge_decision_without_status
                                            {
                                                (None, true, should_bridge)
                                            } else {
                                                let live_local_sink_status_snapshot = sink
                                                    .status_snapshot_nonblocking_for_status_route()
                                                    .await
                                                    .ok()
                                                    .map(|(snapshot, _used_cached_fallback)| {
                                                        snapshot
                                                    });
                                                let cached_local_sink_status_snapshot =
                                                    sink.cached_status_snapshot_with_failure().ok();
                                                let bridge_sink_status_snapshot =
                                                    selected_group_sink_query_bridge_snapshot(
                                                        &params,
                                                        live_local_sink_status_snapshot.as_ref(),
                                                        cached_local_sink_status_snapshot.as_ref(),
                                                    )
                                                    .cloned();
                                                let local_selected_group_bridge_eligible =
                                                    bridge_sink_status_snapshot
                                                        .as_ref()
                                                        .map(|snapshot| {
                                                            selected_group_bridge_eligible_from_sink_status(
                                                                &params, snapshot,
                                                            )
                                                        })
                                                        .unwrap_or(true);
                                                let should_bridge =
                                                    should_bridge_selected_group_sink_query(
                                                        &params,
                                                        &events,
                                                        local_selected_group_bridge_eligible,
                                                    );
                                                (
                                                    bridge_sink_status_snapshot,
                                                    local_selected_group_bridge_eligible,
                                                    should_bridge,
                                                )
                                            };
                                            if trace_sink_query_route {
                                                eprintln!(
                                                    "fs_meta_runtime_app: sink query proxy bridge_decision correlation={:?} selected_group={:?} local_events={} status_probe_required={} eligible={} should_bridge={}",
                                                    req.metadata().correlation_id,
                                                    params.scope.selected_group,
                                                    events.len(),
                                                    bridge_decision_without_status.is_none(),
                                                    local_selected_group_bridge_eligible,
                                                    should_bridge
                                                );
                                            }
                                            let mut fail_closed_after_bridge_gap = false;
                                            if should_bridge {
                                                match rmp_serde::to_vec_named(&params) {
                                                    Ok(payload) => {
                                                        let bridge_adapter = crate::runtime::seam::exchange_host_adapter(
                                                        boundary_for_calls.clone(),
                                                        node_id.clone(),
                                                        selected_group_sink_query_bridge_bindings(
                                                            &params,
                                                            bridge_sink_status_snapshot.as_ref(),
                                                        ),
                                                    );
                                                        match capanix_host_adapter_fs::HostAdapter::call_collect(
                                                        &bridge_adapter,
                                                        ROUTE_TOKEN_FS_META_INTERNAL,
                                                        METHOD_SINK_QUERY,
                                                        bytes::Bytes::from(payload),
                                                        SINK_QUERY_PROXY_BRIDGE_TIMEOUT,
                                                        SINK_QUERY_PROXY_BRIDGE_IDLE_GRACE,
                                                    )
                                                    .await
                                                    {
	                                                            Ok(mut bridged) => {
	                                                                if trace_sink_query_route {
	                                                                    eprintln!(
	                                                                        "fs_meta_runtime_app: sink query proxy bridged internal sink query events={}",
	                                                                        bridged.len()
	                                                                    );
	                                                                }
	                                                                events.append(&mut bridged);
	                                                                discard_selected_group_empty_tree_payloads_shadowed_by_data(
	                                                                    &params,
	                                                                    &mut events,
	                                                                );
	                                                            }
                                                        Err(err) => {
                                                            eprintln!(
                                                                "fs_meta_runtime_app: sink query proxy bridge failed err={}",
                                                                err
                                                            );
                                                            match selected_group_empty_materialized_reply_after_bridge_failure(
                                                                &params,
                                                                &events,
                                                                local_selected_group_bridge_eligible,
                                                                &err,
                                                                req.metadata().correlation_id,
                                                            ) {
                                                                Ok(Some(event)) => {
                                                                    responses.push(event);
                                                                    continue;
                                                                }
                                                                Ok(None) => {}
                                                                Err(reply_err) => {
                                                                    eprintln!(
                                                                        "fs_meta_runtime_app: sink query proxy bridge failure empty reply encode failed: {reply_err}"
                                                                    );
                                                                }
                                                            }
                                                            if should_fail_closed_selected_group_empty_after_bridge_failure(
                                                                &params,
                                                                &events,
                                                                local_selected_group_bridge_eligible,
                                                                &err,
                                                            ) {
                                                                events.clear();
                                                                fail_closed_after_bridge_gap = true;
            }
        }
    }
                                                    }
                                                    Err(err) => {
                                                        eprintln!(
                                                            "fs_meta_runtime_app: sink query proxy bridge encode failed err={}",
                                                            err
                                                        );
                                                    }
                                                }
                                            }
                                            if events.is_empty() && !fail_closed_after_bridge_gap {
                                                let should_emit_empty = if params
                                                    .scope
                                                    .selected_group
                                                    .is_some()
                                                {
                                                    let (snapshot, _) = source
                                                    .observability_snapshot_nonblocking_for_status_route()
                                                    .await;
                                                    should_emit_selected_group_empty_materialized_reply(
                                                    &node_id,
                                                    &snapshot.source_primary_by_group,
                                                    &params,
                                                )
                                                } else {
                                                    false
                                                };
                                                if should_emit_empty {
                                                    match selected_group_empty_materialized_reply(
                                                        &params,
                                                        req.metadata().correlation_id,
                                                    ) {
                                                        Ok(Some(event)) => {
                                                            responses.push(event);
                                                            continue;
                                                        }
                                                        Ok(None) => {}
                                                        Err(err) => {
                                                            eprintln!(
                                                                "fs_meta_runtime_app: sink query proxy empty reply encode failed: {err}"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            for event in &mut events {
                                                responses.push(
                                                    sink_query_reply_event_from_materialized_event(
                                                        event,
                                                        req.metadata().correlation_id,
                                                    ),
                                                );
                                            }
                                        }
                                        Err(err) => {
                                            let err = err.into_error();
                                            eprintln!(
                                                "fs_meta_runtime_app: sink query proxy failed err={}",
                                                err
                                            );
                                            responses.push(sink_query_proxy_error_reply(
                                                &err,
                                                req.metadata().correlation_id,
                                            ));
                                        }
                                    }
                                }
                                responses
                            }
                        },
                    );
                    tasks.push(endpoint);
                }
            }
        }
        let scoped_source_rescan_route = source_rescan_request_route_for(&self.node_id.0);
        let mut source_rescan_routes = self
            .facade_gate
            .active_route_keys(execution_units::SOURCE_RUNTIME_UNIT_ID)
            .unwrap_or_default()
            .into_iter()
            .filter(|route_key| is_scoped_source_rescan_request_route(route_key))
            .collect::<std::collections::BTreeSet<_>>();
        if allow_unmanaged_local_source_rescan_proxy {
            source_rescan_routes.insert(scoped_source_rescan_route.0.clone());
        }
        for source_rescan_route_key in source_rescan_routes {
            if source_rescan_route_key == scoped_source_rescan_route.0 {
                if matches!(&*self.source, SourceFacade::Worker(_))
                    && spawned_routes.insert(source_rescan_route_key.clone())
                {
                    tasks.push(spawn_worker_source_rescan_proxy_endpoint(
                        boundary.clone(),
                        self.source.clone(),
                        self.node_id.clone(),
                        source_rescan_route_key.clone(),
                        self.facade_gate.clone(),
                        self.source_rescan_proxy_ready_generation.clone(),
                        self.source_repair_recovery_without_proxy_ready(),
                    ));
                }
                continue;
            }
            if !spawned_routes.insert(source_rescan_route_key.clone()) {
                continue;
            }
            let source_rescan_route = RouteKey(source_rescan_route_key.clone());
            let node_id = self.node_id.clone();
            eprintln!(
                "fs_meta_runtime_app: spawning source rescan non-target drain endpoint route={}",
                source_rescan_route_key
            );
            let endpoint = ManagedEndpointTask::spawn_with_unit_without_ready_wait(
                boundary.clone(),
                source_rescan_route,
                format!(
                    "app:{}:{}:non-target",
                    ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN
                ),
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                tokio_util::sync::CancellationToken::new(),
                move |requests| {
                    let node_id = node_id.clone();
                    async move {
                        requests
                            .into_iter()
                            .map(|req| {
                                Event::new(
                                    EventMetadata {
                                        origin_id: node_id.clone(),
                                        timestamp_us: now_us(),
                                        logical_ts: None,
                                        correlation_id: req.metadata().correlation_id,
                                        ingress_auth: None,
                                        trace: None,
                                    },
                                    bytes::Bytes::from_static(b"not-target"),
                                )
                            })
                            .collect()
                    }
                },
            );
            tasks.push(endpoint);
        }
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND) {
            if source_active {
                let endpoint_unit_ids = vec![execution_units::SOURCE_RUNTIME_UNIT_ID];
                if !route_still_active_for_units(&self.facade_gate, &route.0, &endpoint_unit_ids)
                    || !spawned_routes.insert(route.0.clone())
                {
                    // Not currently selected as source-find owner, route inactive, or already running.
                } else {
                    eprintln!(
                        "fs_meta_runtime_app: spawning source find proxy endpoint route={}",
                        route.0
                    );
                    let source = self.source.clone();
                    let node_id = self.node_id.clone();
                    let endpoint = ManagedEndpointTask::spawn_with_units_without_ready_wait(
                        boundary.clone(),
                        route,
                        format!(
                            "app:{}:{}",
                            ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND
                        ),
                        endpoint_unit_ids,
                        tokio_util::sync::CancellationToken::new(),
                        move |requests| {
                            let source = source.clone();
                            let node_id = node_id.clone();
                            async move {
                                let mut responses = Vec::new();
                                for req in requests {
                                    let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                        req.payload_bytes(),
                                    ) else {
                                        continue;
                                    };
                                    eprintln!(
                                        "fs_meta_runtime_app: source find proxy request selected_group={:?} recursive={} path={}",
                                        params.scope.selected_group,
                                        params.scope.recursive,
                                        String::from_utf8_lossy(&params.scope.path)
                                    );
                                    #[cfg(test)]
                                    maybe_pause_runtime_proxy_request("source_find").await;
                                    match source.force_find_with_failure(&params).await {
                                        Ok(mut events) => {
                                            eprintln!(
                                                "fs_meta_runtime_app: source find proxy response events={}",
                                                events.len()
                                            );
                                            if debug_force_find_runner_capture_enabled() {
                                                let last_runner = source
                                                .last_force_find_runner_by_group_snapshot_with_failure()
                                                .await
                                                .unwrap_or_default();
                                                let inflight = source
                                                .force_find_inflight_groups_snapshot_with_failure()
                                                .await
                                                .unwrap_or_default();
                                                let response_origins = events
                                                    .iter()
                                                    .map(|event| {
                                                        event.metadata().origin_id.0.clone()
                                                    })
                                                    .collect::<Vec<_>>();
                                                eprintln!(
                                                    "fs_meta_runtime_app: source find proxy runner_capture node={} selected_group={:?} path={} response_events={} response_origins={:?} last_runner={:?} inflight={:?}",
                                                    node_id.0,
                                                    params.scope.selected_group,
                                                    String::from_utf8_lossy(&params.scope.path),
                                                    events.len(),
                                                    response_origins,
                                                    summarize_group_string_map(&last_runner),
                                                    inflight
                                                );
                                            }
                                            for event in &mut events {
                                                let mut meta = event.metadata().clone();
                                                meta.correlation_id = req.metadata().correlation_id;
                                                responses.push(Event::new(
                                                    meta,
                                                    bytes::Bytes::copy_from_slice(
                                                        event.payload_bytes(),
                                                    ),
                                                ));
                                            }
                                        }
                                        Err(err) => {
                                            eprintln!(
                                                "fs_meta_runtime_app: source find proxy failed err={}",
                                                err.as_error()
                                            );
                                            if debug_force_find_runner_capture_enabled() {
                                                let last_runner = source
                                                .last_force_find_runner_by_group_snapshot_with_failure()
                                                .await
                                                .unwrap_or_default();
                                                let inflight = source
                                                .force_find_inflight_groups_snapshot_with_failure()
                                                .await
                                                .unwrap_or_default();
                                                eprintln!(
                                                    "fs_meta_runtime_app: source find proxy runner_capture_failed node={} selected_group={:?} path={} err={} last_runner={:?} inflight={:?}",
                                                    node_id.0,
                                                    params.scope.selected_group,
                                                    String::from_utf8_lossy(&params.scope.path),
                                                    err.as_error(),
                                                    summarize_group_string_map(&last_runner),
                                                    inflight
                                                );
                                            }
                                            responses.push(Event::new(
                                                EventMetadata {
                                                    origin_id: NodeId(
                                                        params
                                                            .scope
                                                            .selected_group
                                                            .clone()
                                                            .unwrap_or_else(|| {
                                                                "source-find-proxy".to_string()
                                                            }),
                                                    ),
                                                    timestamp_us: now_us(),
                                                    logical_ts: None,
                                                    correlation_id: req.metadata().correlation_id,
                                                    ingress_auth: None,
                                                    trace: None,
                                                },
                                                bytes::Bytes::from(err.into_error().to_string()),
                                            ));
                                        }
                                    }
                                }
                                responses
                            }
                        },
                    );
                    tasks.push(endpoint);
                }
            }
        }
        let scoped_source_find_routes = source_find_route_bindings_for(&self.node_id.0);
        if let Ok(route) =
            scoped_source_find_routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND)
        {
            if source_active {
                let endpoint_unit_ids = vec![execution_units::SOURCE_RUNTIME_UNIT_ID];
                if !route_still_active_for_units(&self.facade_gate, &route.0, &endpoint_unit_ids)
                    || !spawned_routes.insert(route.0.clone())
                {
                    // Not currently selected as scoped source-find owner, route inactive, or already running.
                } else {
                    eprintln!(
                        "fs_meta_runtime_app: spawning source find proxy endpoint route={}",
                        route.0
                    );
                    let source = self.source.clone();
                    let node_id = self.node_id.clone();
                    let endpoint = ManagedEndpointTask::spawn_with_units_without_ready_wait(
                        boundary.clone(),
                        route,
                        format!(
                            "app:{}:{}:scoped",
                            ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND
                        ),
                        endpoint_unit_ids,
                        tokio_util::sync::CancellationToken::new(),
                        move |requests| {
                            let source = source.clone();
                            let node_id = node_id.clone();
                            async move {
                                let mut responses = Vec::new();
                                for req in requests {
                                    let Ok(params) = rmp_serde::from_slice::<InternalQueryRequest>(
                                        req.payload_bytes(),
                                    ) else {
                                        continue;
                                    };
                                    eprintln!(
                                        "fs_meta_runtime_app: source find proxy request selected_group={:?} recursive={} path={}",
                                        params.scope.selected_group,
                                        params.scope.recursive,
                                        String::from_utf8_lossy(&params.scope.path)
                                    );
                                    #[cfg(test)]
                                    maybe_pause_runtime_proxy_request("source_find").await;
                                    match source.force_find_with_failure(&params).await {
                                        Ok(mut events) => {
                                            eprintln!(
                                                "fs_meta_runtime_app: source find proxy response events={}",
                                                events.len()
                                            );
                                            if debug_force_find_runner_capture_enabled() {
                                                let last_runner = source
                                                .last_force_find_runner_by_group_snapshot_with_failure()
                                                .await
                                                .unwrap_or_default();
                                                let inflight = source
                                                .force_find_inflight_groups_snapshot_with_failure()
                                                .await
                                                .unwrap_or_default();
                                                let response_origins = events
                                                    .iter()
                                                    .map(|event| {
                                                        event.metadata().origin_id.0.clone()
                                                    })
                                                    .collect::<Vec<_>>();
                                                eprintln!(
                                                    "fs_meta_runtime_app: source find proxy runner_capture node={} selected_group={:?} path={} response_events={} response_origins={:?} last_runner={:?} inflight={:?}",
                                                    node_id.0,
                                                    params.scope.selected_group,
                                                    String::from_utf8_lossy(&params.scope.path),
                                                    events.len(),
                                                    response_origins,
                                                    summarize_group_string_map(&last_runner),
                                                    inflight
                                                );
                                            }
                                            for event in &mut events {
                                                let mut meta = event.metadata().clone();
                                                meta.correlation_id = req.metadata().correlation_id;
                                                responses.push(Event::new(
                                                    meta,
                                                    bytes::Bytes::copy_from_slice(
                                                        event.payload_bytes(),
                                                    ),
                                                ));
                                            }
                                        }
                                        Err(err) => {
                                            eprintln!(
                                                "fs_meta_runtime_app: source find proxy failed err={}",
                                                err.as_error()
                                            );
                                            if debug_force_find_runner_capture_enabled() {
                                                let last_runner = source
                                                .last_force_find_runner_by_group_snapshot_with_failure()
                                                .await
                                                .unwrap_or_default();
                                                let inflight = source
                                                .force_find_inflight_groups_snapshot_with_failure()
                                                .await
                                                .unwrap_or_default();
                                                eprintln!(
                                                    "fs_meta_runtime_app: source find proxy runner_capture_failed node={} selected_group={:?} path={} err={} last_runner={:?} inflight={:?}",
                                                    node_id.0,
                                                    params.scope.selected_group,
                                                    String::from_utf8_lossy(&params.scope.path),
                                                    err.as_error(),
                                                    summarize_group_string_map(&last_runner),
                                                    inflight
                                                );
                                            }
                                            responses.push(Event::new(
                                                EventMetadata {
                                                    origin_id: NodeId(
                                                        params
                                                            .scope
                                                            .selected_group
                                                            .clone()
                                                            .unwrap_or_else(|| {
                                                                "source-find-proxy".to_string()
                                                            }),
                                                    ),
                                                    timestamp_us: now_us(),
                                                    logical_ts: None,
                                                    correlation_id: req.metadata().correlation_id,
                                                    ingress_auth: None,
                                                    trace: None,
                                                },
                                                bytes::Bytes::from(err.into_error().to_string()),
                                            ));
                                        }
                                    }
                                }
                                responses
                            }
                        },
                    );
                    tasks.push(endpoint);
                }
            }
        }
        Ok(())
    }

    fn facade_candidate_resource_ids(bound_scopes: &[RuntimeBoundScope]) -> Vec<String> {
        let mut ids = std::collections::BTreeSet::new();
        for scope in bound_scopes {
            for resource_id in &scope.resource_ids {
                let trimmed = resource_id.trim();
                if !trimmed.is_empty() {
                    ids.insert(trimmed.to_string());
                }
            }
        }
        ids.into_iter().collect()
    }

    async fn runtime_scoped_facade_group_ids(
        source: &SourceFacade,
        sink: &SinkFacade,
    ) -> std::result::Result<Vec<String>, RuntimeWorkerObservationFailure> {
        let mut source_groups = source
            .scheduled_source_group_ids_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)?
            .unwrap_or_default();
        let scan_groups = source
            .scheduled_scan_group_ids_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)?
            .unwrap_or_default();
        source_groups.extend(scan_groups);
        let sink_groups = sink
            .scheduled_group_ids_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)?
            .unwrap_or_default();
        if !source_groups.is_empty() && !sink_groups.is_empty() {
            return Ok(source_groups.intersection(&sink_groups).cloned().collect());
        }
        if !source_groups.is_empty() {
            return Ok(source_groups.into_iter().collect());
        }
        Ok(sink_groups.into_iter().collect())
    }

    async fn facade_candidate_group_ids(
        source: &SourceFacade,
        sink: &SinkFacade,
        bound_scopes: &[RuntimeBoundScope],
    ) -> std::result::Result<Vec<String>, RuntimeWorkerObservationFailure> {
        let logical_root_ids = source
            .logical_roots_snapshot_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)?
            .into_iter()
            .map(|root| root.id)
            .collect::<std::collections::BTreeSet<_>>();
        let mut ids = std::collections::BTreeSet::new();
        for scope in bound_scopes {
            let scope_id = scope.scope_id.trim();
            if !scope_id.is_empty() && logical_root_ids.contains(scope_id) {
                ids.insert(scope_id.to_string());
            }
            for resource_id in &scope.resource_ids {
                let trimmed = resource_id.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Some(group_id) = source
                    .resolve_group_id_for_object_ref_with_failure(trimmed)
                    .await
                    .map_err(RuntimeWorkerObservationFailure::from)?
                {
                    ids.insert(group_id);
                }
            }
        }
        if !ids.is_empty() {
            return Ok(ids.into_iter().collect());
        }
        Self::runtime_scoped_facade_group_ids(source, sink).await
    }

    async fn observation_candidate_group_ids(
        source: &SourceFacade,
        sink: &SinkFacade,
        pending: &PendingFacadeActivation,
    ) -> std::result::Result<std::collections::BTreeSet<String>, RuntimeWorkerObservationFailure>
    {
        if !pending.group_ids.is_empty() {
            return Ok(pending.group_ids.iter().cloned().collect());
        }
        Ok(
            Self::facade_candidate_group_ids(source, sink, &pending.bound_scopes)
                .await?
                .into_iter()
                .collect(),
        )
    }

    fn listener_only_facade_activation_evidence(
        source_status: &source::SourceStatusSnapshot,
        sink_status: &crate::sink::SinkStatusSnapshot,
        candidate_groups: &std::collections::BTreeSet<String>,
    ) -> ObservationEvidence {
        let sink_groups = sink_status
            .groups
            .iter()
            .map(|group| (group.group_id.as_str(), group))
            .collect::<BTreeMap<_, _>>();
        let mut degraded_groups = source_status
            .degraded_roots
            .iter()
            .filter(|(root_id, _)| candidate_groups.contains(root_id))
            .map(|(root_id, _)| root_id.clone())
            .collect::<BTreeSet<_>>();
        degraded_groups.extend(
            source_status
                .logical_roots
                .iter()
                .filter(|root| {
                    candidate_groups.contains(&root.root_id)
                        && (root.matched_grants == 0
                            || root.active_members == 0
                            || root.status.contains("failed")
                            || root.status.contains("overflow"))
                })
                .map(|root| root.root_id.clone()),
        );

        let mut initial_audit_groups = BTreeSet::new();
        let mut overflow_pending_groups = BTreeSet::new();
        for group_id in candidate_groups {
            let Some(group) = sink_groups.get(group_id.as_str()) else {
                initial_audit_groups.insert(group_id.clone());
                continue;
            };
            if !group.is_ready() {
                initial_audit_groups.insert(group_id.clone());
            }
            if group.overflow_pending_materialization {
                overflow_pending_groups.insert(group_id.clone());
            }
        }

        ObservationEvidence {
            candidate_groups: candidate_groups.clone(),
            initial_audit_groups,
            degraded_groups,
            overflow_pending_groups,
        }
    }

    async fn observation_eligible_for(
        source: &SourceFacade,
        sink: &SinkFacade,
        pending: &PendingFacadeActivation,
    ) -> std::result::Result<bool, RuntimeWorkerObservationFailure> {
        let configured_roots = source
            .logical_roots_snapshot_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)?;
        let source_status = source
            .status_snapshot_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)?;
        let sink_status = sink
            .status_snapshot_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)?;
        let candidate_groups = Self::observation_candidate_group_ids(source, sink, pending).await?;
        // Empty-roots is a valid deployed state for the management API surface.
        // Runtime grants may already project candidate groups before the operator
        // has selected business monitoring scope, but that must not block the
        // facade from coming up so `/runtime/grants -> roots preview/apply` can
        // complete.
        if configured_roots.is_empty() {
            return Ok(true);
        }
        let evidence = if pending.runtime_managed {
            candidate_group_observation_evidence(&source_status, &sink_status, &candidate_groups)
        } else {
            Self::listener_only_facade_activation_evidence(
                &source_status,
                &sink_status,
                &candidate_groups,
            )
        };
        let status =
            evaluate_observation_status(&evidence, ObservationTrustPolicy::candidate_generation());
        Ok(status.state == ObservationState::TrustedMaterialized)
    }

    async fn apply_pending_fixed_bind_spawn_success_followup(
        &self,
        pending: Option<PendingFacadeActivation>,
    ) {
        if let Some(pending) = pending {
            let registrant = PendingFixedBindHandoffRegistrant {
                instance_id: self.instance_id,
                api_task: self.api_task.clone(),
                pending_facade: self.pending_facade.clone(),
                pending_fixed_bind_claim_release_followup: self
                    .pending_fixed_bind_claim_release_followup
                    .clone(),
                pending_fixed_bind_has_suppressed_dependent_routes: self
                    .pending_fixed_bind_has_suppressed_dependent_routes
                    .clone(),
                retained_active_facade_continuity: self.retained_active_facade_continuity.clone(),
                facade_spawn_in_progress: self.facade_spawn_in_progress.clone(),
                facade_pending_status: self.facade_pending_status.clone(),
                facade_service_state: self.facade_service_state.clone(),
                rollout_status: self.rollout_status.clone(),
                api_request_tracker: self.api_request_tracker.clone(),
                api_control_gate: self.api_control_gate.clone(),
                control_failure_uninitialized: self.control_failure_uninitialized.clone(),
                runtime_gate_state: self.runtime_gate_state.clone(),
                runtime_state_changed: self.runtime_state_changed.clone(),
                node_id: self.node_id.clone(),
                runtime_boundary: self.runtime_boundary.clone(),
                source: self.source.clone(),
                sink: self.sink.clone(),
                query_sink: self.query_sink.clone(),
                query_runtime_boundary: self.runtime_boundary.clone(),
            };
            let fixed_bind = FSMetaApp::build_fixed_bind_snapshot_for_pending_publication(
                registrant.instance_id,
                registrant.api_task.clone(),
                Some(pending.clone()),
                registrant
                    .pending_fixed_bind_claim_release_followup
                    .load(Ordering::Acquire),
            )
            .await;
            let machine = FixedBindLifecycleMachine {
                observation: FacadeGateObservation::inert(),
                publication_ready: false,
                fixed_bind,
                release_handoff: None,
                shutdown_handoff: None,
            };
            let pending_fixed_bind_handoff_ready_bind_addr = machine
                .fixed_bind
                .pending_publication
                .as_ref()
                .filter(|pending| {
                    machine.fixed_bind.conflicting_process_claim.is_some()
                        && pending.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                        && !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
                })
                .map(|pending| pending.resolved.bind_addr.as_str());
            if let Some(bind_addr) = pending_fixed_bind_handoff_ready_bind_addr {
                mark_pending_fixed_bind_handoff_ready_with_registrant(
                    bind_addr,
                    PendingFixedBindHandoffRegistrant {
                        instance_id: self.instance_id,
                        api_task: self.api_task.clone(),
                        pending_facade: self.pending_facade.clone(),
                        pending_fixed_bind_claim_release_followup: self
                            .pending_fixed_bind_claim_release_followup
                            .clone(),
                        pending_fixed_bind_has_suppressed_dependent_routes: self
                            .pending_fixed_bind_has_suppressed_dependent_routes
                            .clone(),
                        retained_active_facade_continuity: self
                            .retained_active_facade_continuity
                            .clone(),
                        facade_spawn_in_progress: self.facade_spawn_in_progress.clone(),
                        facade_pending_status: self.facade_pending_status.clone(),
                        facade_service_state: self.facade_service_state.clone(),
                        rollout_status: self.rollout_status.clone(),
                        api_request_tracker: self.api_request_tracker.clone(),
                        api_control_gate: self.api_control_gate.clone(),
                        control_failure_uninitialized: self.control_failure_uninitialized.clone(),
                        runtime_gate_state: self.runtime_gate_state.clone(),
                        runtime_state_changed: self.runtime_state_changed.clone(),
                        node_id: self.node_id.clone(),
                        runtime_boundary: self.runtime_boundary.clone(),
                        source: self.source.clone(),
                        sink: self.sink.clone(),
                        query_sink: self.query_sink.clone(),
                        query_runtime_boundary: self.runtime_boundary.clone(),
                    },
                );
            }
            self.pending_fixed_bind_claim_release_followup.store(
                machine.fixed_bind.claim_release_followup_pending
                    && machine.fixed_bind.pending_publication.is_some()
                    && machine.fixed_bind.conflicting_process_claim.is_some(),
                Ordering::Release,
            );
            self.notify_runtime_state_changed();
        } else {
            self.pending_fixed_bind_claim_release_followup
                .store(false, Ordering::Release);
            self.notify_runtime_state_changed();
        }
        let bind_addr = {
            let api_task = self.api_task.lock().await;
            api_task.as_ref().and_then(|active| {
                (active.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL))
                    .then(|| {
                        self.config
                            .api
                            .resolve_for_candidate_ids(&active.resource_ids)
                    })
                    .flatten()
                    .map(|resolved| resolved.bind_addr)
                    .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
            })
        };
        if let Some(bind_addr) = bind_addr {
            mark_active_fixed_bind_facade_owner(
                &bind_addr,
                ActiveFixedBindFacadeRegistrant {
                    instance_id: self.instance_id,
                    api_task: self.api_task.clone(),
                    api_request_tracker: self.api_request_tracker.clone(),
                    api_control_gate: self.api_control_gate.clone(),
                    control_failure_uninitialized: self.control_failure_uninitialized.clone(),
                    retained_active_facade_continuity: self
                        .retained_active_facade_continuity
                        .clone(),
                },
            );
        }
    }

    async fn try_spawn_pending_facade(&self) -> Result<bool> {
        let spawn_result = PendingFixedBindHandoffRegistrant {
            instance_id: self.instance_id,
            api_task: self.api_task.clone(),
            pending_facade: self.pending_facade.clone(),
            pending_fixed_bind_claim_release_followup: self
                .pending_fixed_bind_claim_release_followup
                .clone(),
            pending_fixed_bind_has_suppressed_dependent_routes: self
                .pending_fixed_bind_has_suppressed_dependent_routes
                .clone(),
            retained_active_facade_continuity: self.retained_active_facade_continuity.clone(),
            facade_spawn_in_progress: self.facade_spawn_in_progress.clone(),
            facade_pending_status: self.facade_pending_status.clone(),
            facade_service_state: self.facade_service_state.clone(),
            rollout_status: self.rollout_status.clone(),
            api_request_tracker: self.api_request_tracker.clone(),
            api_control_gate: self.api_control_gate.clone(),
            control_failure_uninitialized: self.control_failure_uninitialized.clone(),
            runtime_gate_state: self.runtime_gate_state.clone(),
            runtime_state_changed: self.runtime_state_changed.clone(),
            node_id: self.node_id.clone(),
            runtime_boundary: self.runtime_boundary.clone(),
            source: self.source.clone(),
            sink: self.sink.clone(),
            query_sink: self.query_sink.clone(),
            query_runtime_boundary: self.runtime_boundary.clone(),
        }
        .try_spawn_pending_facade()
        .await;
        let pending = self.pending_facade.lock().await.clone();
        match (spawn_result, pending) {
            (Ok(spawned), pending) => {
                let pending_after_spawn = pending.clone();
                self.apply_pending_fixed_bind_spawn_success_followup(pending)
                    .await;
                if !spawned
                    && let Some(pending) = pending_after_spawn.as_ref()
                    && let Some(completed) = self
                        .release_pending_fixed_bind_handoff_blocker_if_needed(pending)
                        .await?
                {
                    return Ok(completed);
                }
                Ok(spawned)
            }
            (Err(err), Some(pending))
                if Self::facade_spawn_error_is_bind_addr_in_use(&err)
                    && pending.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                    && !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr) =>
            {
                Self::record_pending_facade_retry_error(
                    &self.facade_pending_status,
                    &pending,
                    &err,
                );
                mark_pending_fixed_bind_handoff_ready_with_registrant(
                    &pending.resolved.bind_addr,
                    PendingFixedBindHandoffRegistrant {
                        instance_id: self.instance_id,
                        api_task: self.api_task.clone(),
                        pending_facade: self.pending_facade.clone(),
                        pending_fixed_bind_claim_release_followup: self
                            .pending_fixed_bind_claim_release_followup
                            .clone(),
                        pending_fixed_bind_has_suppressed_dependent_routes: self
                            .pending_fixed_bind_has_suppressed_dependent_routes
                            .clone(),
                        retained_active_facade_continuity: self
                            .retained_active_facade_continuity
                            .clone(),
                        facade_spawn_in_progress: self.facade_spawn_in_progress.clone(),
                        facade_pending_status: self.facade_pending_status.clone(),
                        facade_service_state: self.facade_service_state.clone(),
                        rollout_status: self.rollout_status.clone(),
                        api_request_tracker: self.api_request_tracker.clone(),
                        api_control_gate: self.api_control_gate.clone(),
                        control_failure_uninitialized: self.control_failure_uninitialized.clone(),
                        runtime_gate_state: self.runtime_gate_state.clone(),
                        runtime_state_changed: self.runtime_state_changed.clone(),
                        node_id: self.node_id.clone(),
                        runtime_boundary: self.runtime_boundary.clone(),
                        source: self.source.clone(),
                        sink: self.sink.clone(),
                        query_sink: self.query_sink.clone(),
                        query_runtime_boundary: self.runtime_boundary.clone(),
                    },
                );
                self.pending_fixed_bind_claim_release_followup
                    .store(true, Ordering::Release);
                self.notify_runtime_state_changed();
                if let Some(completed) = self
                    .release_pending_fixed_bind_handoff_blocker_if_needed(&pending)
                    .await?
                {
                    return Ok(completed);
                }
                Ok(false)
            }
            (Err(err), _) => Err(err),
        }
    }

    #[cfg(test)]
    async fn try_spawn_pending_facade_from_parts_with_spawn<Spawn, SpawnFut>(
        instance_id: u64,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
        facade_spawn_in_progress: Arc<Mutex<Option<FacadeSpawnInProgress>>>,
        pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
        facade_pending_status: SharedFacadePendingStatusCell,
        facade_service_state: SharedFacadeServiceStateCell,
        api_request_tracker: Arc<ApiRequestTracker>,
        api_control_gate: Arc<ApiControlGate>,
        runtime_gate_state: Arc<StdMutex<RuntimeControlState>>,
        node_id: NodeId,
        runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        query_sink: Arc<SinkFacade>,
        query_runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
        spawn_facade: Spawn,
    ) -> Result<bool>
    where
        Spawn: FnOnce(
            api::config::ResolvedApiConfig,
            NodeId,
            Option<Arc<dyn ChannelIoSubset>>,
            Arc<SourceFacade>,
            Arc<SinkFacade>,
            Arc<SinkFacade>,
            Option<Arc<dyn ChannelIoSubset>>,
            SharedFacadePendingStatusCell,
            SharedFacadeServiceStateCell,
            Arc<ApiRequestTracker>,
            Arc<ApiControlGate>,
        ) -> SpawnFut,
        SpawnFut: std::future::Future<Output = Result<api::ApiServerHandle>>,
    {
        Self::try_spawn_pending_facade_from_registrant_with_spawn(
            PendingFixedBindHandoffRegistrant {
                instance_id,
                api_task,
                pending_facade,
                pending_fixed_bind_claim_release_followup: Arc::new(AtomicBool::new(false)),
                pending_fixed_bind_has_suppressed_dependent_routes,
                retained_active_facade_continuity: Arc::new(AtomicBool::new(false)),
                facade_spawn_in_progress,
                facade_pending_status,
                facade_service_state,
                rollout_status: shared_rollout_status_cell(),
                api_request_tracker,
                api_control_gate,
                control_failure_uninitialized: Arc::new(AtomicBool::new(false)),
                runtime_gate_state,
                runtime_state_changed: Arc::new(tokio::sync::Notify::new()),
                node_id,
                runtime_boundary,
                source,
                sink,
                query_sink,
                query_runtime_boundary,
            },
            PendingFacadeSpawnMode::Normal,
            move |resolved,
                  node_id,
                  runtime_boundary,
                  source,
                  sink,
                  query_sink,
                  query_runtime_boundary,
                  facade_pending_status,
                  facade_service_state,
                  _rollout_status,
                  api_request_tracker,
                  api_control_gate| {
                spawn_facade(
                    resolved,
                    node_id,
                    runtime_boundary,
                    source,
                    sink,
                    query_sink,
                    query_runtime_boundary,
                    facade_pending_status,
                    facade_service_state,
                    api_request_tracker,
                    api_control_gate,
                )
            },
        )
        .await
    }

    async fn try_spawn_pending_facade_from_registrant_with_spawn<Spawn, SpawnFut>(
        registrant: PendingFixedBindHandoffRegistrant,
        spawn_mode: PendingFacadeSpawnMode,
        spawn_facade: Spawn,
    ) -> Result<bool>
    where
        Spawn: FnOnce(
            api::config::ResolvedApiConfig,
            NodeId,
            Option<Arc<dyn ChannelIoSubset>>,
            Arc<SourceFacade>,
            Arc<SinkFacade>,
            Arc<SinkFacade>,
            Option<Arc<dyn ChannelIoSubset>>,
            SharedFacadePendingStatusCell,
            SharedFacadeServiceStateCell,
            SharedRolloutStatusCell,
            Arc<ApiRequestTracker>,
            Arc<ApiControlGate>,
        ) -> SpawnFut,
        SpawnFut: std::future::Future<Output = Result<api::ApiServerHandle>>,
    {
        let PendingFixedBindHandoffRegistrant {
            instance_id,
            api_task,
            pending_facade,
            pending_fixed_bind_claim_release_followup: _,
            pending_fixed_bind_has_suppressed_dependent_routes: _,
            retained_active_facade_continuity,
            facade_spawn_in_progress,
            facade_pending_status,
            facade_service_state,
            rollout_status,
            api_request_tracker,
            api_control_gate,
            control_failure_uninitialized: _,
            runtime_gate_state: _,
            runtime_state_changed: _,
            node_id,
            runtime_boundary,
            source,
            sink,
            query_sink,
            query_runtime_boundary,
        } = registrant.clone();
        let Some(pending) = pending_facade.lock().await.clone() else {
            return Ok(false);
        };
        if !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr) {
            let claim = ProcessFacadeClaim {
                owner_instance_id: instance_id,
                bind_addr: pending.resolved.bind_addr.clone(),
            };
            let mut guard = match process_facade_claim_cell().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(existing) = guard.get(&claim.bind_addr)
                && existing.owner_instance_id != instance_id
                && existing.bind_addr == pending.resolved.bind_addr
            {
                eprintln!(
                    "fs_meta_runtime_app: facade process claim already owned instance_id={} generation={} route_key={} resources={:?} bind_addr={}",
                    existing.owner_instance_id,
                    pending.generation,
                    pending.route_key,
                    pending.resource_ids,
                    pending.resolved.bind_addr
                );
                mark_pending_fixed_bind_handoff_ready_with_registrant(
                    &pending.resolved.bind_addr,
                    registrant.clone(),
                );
                registrant.notify_runtime_state_changed();
                return Ok(false);
            }
            guard.insert(claim.bind_addr.clone(), claim);
        }
        let stale_active = {
            let mut api_task_guard = api_task.lock().await;
            if api_task_guard
                .as_ref()
                .is_some_and(|active| !active.handle.is_running())
            {
                api_task_guard.take()
            } else {
                None
            }
        };
        if let Some(stale) = stale_active {
            eprintln!(
                "fs_meta_runtime_app: dropping stale inactive facade handle generation={} route_key={}",
                stale.generation, stale.route_key
            );
            stale.handle.shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT).await;
            clear_owned_process_facade_claim(instance_id);
            if !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr) {
                clear_process_facade_claim_for_retired_fixed_bind(&pending.resolved.bind_addr);
                clear_active_fixed_bind_facade_owner(&pending.resolved.bind_addr, instance_id);
            }
        }
        let (
            reuse_current_generation,
            promote_existing_same_resource_generation,
            replacing_existing,
        ) = {
            let api_task_guard = api_task.lock().await;
            let continuity = match api_task_guard.as_ref() {
                Some(active)
                    if active.route_key == pending.route_key
                        && active.resource_ids == pending.resource_ids
                        && active.generation == pending.generation =>
                {
                    (true, None)
                }
                Some(active)
                    if active.route_key == pending.route_key
                        && active.resource_ids == pending.resource_ids =>
                {
                    (false, Some(active.generation))
                }
                _ => (false, None),
            };
            (continuity.0, continuity.1, api_task_guard.is_some())
        };
        if reuse_current_generation {
            let active_matches = registrant
                .api_task
                .lock()
                .await
                .as_ref()
                .is_some_and(|active| {
                    active.route_key == pending.route_key
                        && active.resource_ids == pending.resource_ids
                        && active.generation == pending.generation
                });
            if active_matches {
                if !pending.runtime_exposure_confirmed
                    && !spawn_mode.permits_unconfirmed_fixed_bind_boundary(&pending)
                {
                    Self::set_pending_facade_status_waiting(
                        &facade_pending_status,
                        &pending,
                        FacadePendingReason::AwaitingRuntimeExposure,
                    );
                    return Ok(false);
                }
                let mut pending_guard = registrant.pending_facade.lock().await;
                if pending_guard.as_ref().is_some_and(|candidate| {
                    candidate.route_key == pending.route_key
                        && candidate.resource_ids == pending.resource_ids
                        && candidate.generation == pending.generation
                }) {
                    pending_guard.take();
                }
                drop(pending_guard);
                FSMetaApp::clear_pending_facade_status(&registrant.facade_pending_status);
                let ready_tail_decision = registrant
                    .runtime_control_state()
                    .facade_ready_tail_decision(true, false, false);
                registrant
                    .api_control_gate
                    .set_ready(ready_tail_decision.control_gate_ready);
                *registrant
                    .facade_service_state
                    .write()
                    .expect("write published facade service state") =
                    ready_tail_decision.published_state;
                registrant.notify_runtime_state_changed();
                return Ok(true);
            }
        }
        if let Some(active_generation) = promote_existing_same_resource_generation {
            let mut api_task_guard = registrant.api_task.lock().await;
            let active_matches = api_task_guard.as_ref().is_some_and(|active| {
                active.route_key == pending.route_key
                    && active.resource_ids == pending.resource_ids
                    && active.generation == active_generation
            });
            if active_matches {
                if !pending.runtime_exposure_confirmed
                    && !spawn_mode.permits_unconfirmed_fixed_bind_boundary(&pending)
                {
                    drop(api_task_guard);
                    Self::set_pending_facade_status_waiting(
                        &facade_pending_status,
                        &pending,
                        FacadePendingReason::AwaitingRuntimeExposure,
                    );
                    return Ok(false);
                }
                if let Some(active) = api_task_guard.as_mut() {
                    active.generation = pending.generation;
                }
                drop(api_task_guard);
                let mut pending_guard = registrant.pending_facade.lock().await;
                if pending_guard.as_ref().is_some_and(|candidate| {
                    candidate.route_key == pending.route_key
                        && candidate.resource_ids == pending.resource_ids
                        && candidate.generation == pending.generation
                }) {
                    pending_guard.take();
                }
                drop(pending_guard);
                FSMetaApp::clear_pending_facade_status(&registrant.facade_pending_status);
                let ready_tail_decision = registrant
                    .runtime_control_state()
                    .facade_ready_tail_decision(true, false, false);
                registrant
                    .api_control_gate
                    .set_ready(ready_tail_decision.control_gate_ready);
                *registrant
                    .facade_service_state
                    .write()
                    .expect("write published facade service state") =
                    ready_tail_decision.published_state;
                registrant.notify_runtime_state_changed();
                return Ok(true);
            }
        }
        let replacement_wait_reason =
            if spawn_mode.permits_unconfirmed_fixed_bind_boundary(&pending) {
                None
            } else {
                Self::facade_replacement_wait_reason(
                    source.as_ref(),
                    sink.as_ref(),
                    &pending,
                    replacing_existing,
                )
                .await?
            };
        if let Some(wait_reason) = replacement_wait_reason {
            Self::set_pending_facade_status_waiting(&facade_pending_status, &pending, wait_reason);
            return Ok(false);
        }
        {
            let mut inflight_guard = facade_spawn_in_progress.lock().await;
            if let Some(inflight) = inflight_guard.as_ref() {
                eprintln!(
                    "fs_meta_runtime_app: facade spawn already in progress generation={} route_key={} resources={:?} same_resource={}",
                    pending.generation,
                    pending.route_key,
                    pending.resource_ids,
                    pending.route_key == inflight.route_key
                        && pending.resource_ids == inflight.resource_ids
                );
                return Ok(false);
            }
            *inflight_guard = Some(FacadeSpawnInProgress {
                route_key: pending.route_key.clone(),
                resource_ids: pending.resource_ids.clone(),
            });
        }
        // Cold start has no prior facade to retain, so runtime confirmation is
        // sufficient to bring up the hosting boundary. Replacement also proceeds once
        // runtime confirms external exposure; materialized `/tree` and `/stats`
        // readiness is enforced at the query surface so `/on-demand-force-find`
        // can become available earlier.

        eprintln!(
            "fs_meta_runtime_app: spawning facade api server generation={} route_key={} resources={:?}",
            pending.generation, pending.route_key, pending.resource_ids
        );
        let spawn_result = spawn_facade(
            pending.resolved.clone(),
            node_id,
            runtime_boundary,
            source,
            sink,
            query_sink,
            query_runtime_boundary,
            facade_pending_status.clone(),
            facade_service_state.clone(),
            rollout_status.clone(),
            api_request_tracker.clone(),
            api_control_gate.clone(),
        )
        .await;
        {
            let mut inflight_guard = facade_spawn_in_progress.lock().await;
            if inflight_guard.as_ref().is_some_and(|inflight| {
                pending.route_key == inflight.route_key
                    && pending.resource_ids == inflight.resource_ids
            }) {
                inflight_guard.take();
            }
        }
        let handle = match spawn_result {
            Ok(handle) => handle,
            Err(err) => {
                let mut guard = match process_facade_claim_cell().lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                if guard.get(&pending.resolved.bind_addr).is_some_and(|claim| {
                    claim.owner_instance_id == instance_id
                        && claim.bind_addr == pending.resolved.bind_addr
                }) {
                    guard.remove(&pending.resolved.bind_addr);
                }
                return Err(err);
            }
        };
        eprintln!(
            "fs_meta_runtime_app: facade api::spawn returned generation={} route_key={}",
            pending.generation, pending.route_key
        );

        let adopted_generation = {
            let pending_guard = pending_facade.lock().await;
            pending_guard.as_ref().and_then(|candidate| {
                (candidate.route_key == pending.route_key
                    && candidate.resource_ids == pending.resource_ids)
                    .then_some(candidate.generation)
            })
        };
        let Some(adopted_generation) = adopted_generation else {
            eprintln!(
                "fs_meta_runtime_app: shutting down stale facade handle generation={} route_key={}",
                pending.generation, pending.route_key
            );
            handle.shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT).await;
            let mut guard = match process_facade_claim_cell().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if guard.get(&pending.resolved.bind_addr).is_some_and(|claim| {
                claim.owner_instance_id == instance_id
                    && claim.bind_addr == pending.resolved.bind_addr
            }) {
                guard.remove(&pending.resolved.bind_addr);
            }
            return Ok(false);
        };

        let previous = api_task.lock().await.replace(FacadeActivation {
            route_key: pending.route_key.clone(),
            generation: adopted_generation,
            resource_ids: pending.resource_ids.clone(),
            handle,
        });
        if pending.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
            && !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
        {
            mark_active_fixed_bind_facade_owner(
                &pending.resolved.bind_addr,
                ActiveFixedBindFacadeRegistrant {
                    instance_id,
                    api_task: api_task.clone(),
                    api_request_tracker: api_request_tracker.clone(),
                    api_control_gate: api_control_gate.clone(),
                    control_failure_uninitialized: registrant.control_failure_uninitialized.clone(),
                    retained_active_facade_continuity: retained_active_facade_continuity.clone(),
                },
            );
        }
        clear_pending_fixed_bind_handoff_ready(&pending.resolved.bind_addr);
        eprintln!(
            "fs_meta_runtime_app: facade handle active generation={} route_key={} resources={:?}",
            adopted_generation, pending.route_key, pending.resource_ids
        );
        let mut pending_guard = pending_facade.lock().await;
        if pending_guard.as_ref().is_some_and(|candidate| {
            candidate.route_key == pending.route_key
                && candidate.resource_ids == pending.resource_ids
        }) {
            pending_guard.take();
        }
        Self::clear_pending_facade_status(&facade_pending_status);
        let ready_tail_allows_facade_only_handoff =
            Self::facade_only_handoff_allowance_decision(FacadeOnlyHandoffAllowanceDecisionInput {
                pending_facade_present: true,
                pending_facade_is_control_route: pending.route_key
                    == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL),
                pending_fixed_bind_has_suppressed_dependent_routes: registrant
                    .pending_fixed_bind_has_suppressed_dependent_routes
                    .load(Ordering::Acquire),
                pending_bind_is_ephemeral: facade_bind_addr_is_ephemeral(
                    &pending.resolved.bind_addr,
                ),
                pending_bind_owned_by_instance: !facade_bind_addr_is_ephemeral(
                    &pending.resolved.bind_addr,
                ) && active_fixed_bind_facade_owned_by(
                    &pending.resolved.bind_addr,
                    instance_id,
                ),
            })
            .allows_handoff();
        let ready_tail_decision = registrant
            .runtime_control_state()
            .facade_ready_tail_decision(true, false, ready_tail_allows_facade_only_handoff);
        registrant
            .api_control_gate
            .set_ready(ready_tail_decision.control_gate_ready);
        *registrant
            .facade_service_state
            .write()
            .expect("write published facade service state") = ready_tail_decision.published_state;
        registrant.notify_runtime_state_changed();
        drop(pending_guard);
        if let Some(current) = previous {
            eprintln!(
                "fs_meta_runtime_app: shutting down previous active facade generation={}",
                current.generation
            );
            current
                .handle
                .shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT)
                .await;
        }
        Ok(true)
    }

    fn facade_retry_backoff(retry_attempts: u64) -> Duration {
        match retry_attempts {
            0 | 1 => Duration::ZERO,
            2 => Duration::from_secs(2),
            3 => Duration::from_secs(4),
            4 => Duration::from_secs(8),
            5 => Duration::from_secs(16),
            _ => Duration::from_secs(30),
        }
    }

    fn clear_pending_facade_status(status_cell: &SharedFacadePendingStatusCell) {
        if let Ok(mut guard) = status_cell.write() {
            *guard = None;
        }
    }

    fn set_pending_facade_status_waiting(
        status_cell: &SharedFacadePendingStatusCell,
        pending: &PendingFacadeActivation,
        reason: FacadePendingReason,
    ) {
        let pending_since_us = status_cell
            .read()
            .ok()
            .and_then(|guard| {
                guard.as_ref().and_then(|status| {
                    (status.route_key == pending.route_key
                        && status.resource_ids == pending.resource_ids
                        && status.generation == pending.generation)
                        .then_some(status.pending_since_us)
                })
            })
            .unwrap_or_else(now_us);
        if let Ok(mut guard) = status_cell.write() {
            *guard = Some(SharedFacadePendingStatus {
                route_key: pending.route_key.clone(),
                generation: pending.generation,
                resource_ids: pending.resource_ids.clone(),
                runtime_managed: pending.runtime_managed,
                runtime_exposure_confirmed: pending.runtime_exposure_confirmed,
                reason,
                retry_attempts: 0,
                pending_since_us,
                last_error: None,
                last_attempt_at_us: None,
                last_error_at_us: None,
                retry_backoff_ms: None,
                next_retry_at_us: None,
            });
        }
    }

    async fn facade_replacement_wait_reason(
        source: &SourceFacade,
        sink: &SinkFacade,
        pending: &PendingFacadeActivation,
        replacing_existing: bool,
    ) -> Result<Option<FacadePendingReason>> {
        if !replacing_existing {
            return Ok(None);
        }
        if !pending.runtime_exposure_confirmed {
            return Ok(Some(FacadePendingReason::AwaitingRuntimeExposure));
        }
        if pending.runtime_managed
            && (source.is_worker() || sink.is_worker())
            && !Self::observation_eligible_for(source, sink, pending)
                .await
                .map_err(RuntimeWorkerObservationFailure::into_error)?
        {
            return Ok(Some(FacadePendingReason::AwaitingObservationEligibility));
        }
        Ok(None)
    }

    fn record_pending_facade_retry_error(
        status_cell: &SharedFacadePendingStatusCell,
        pending: &PendingFacadeActivation,
        err: &CnxError,
    ) {
        let now = now_us();
        let (retry_attempts, pending_since_us) = status_cell
            .read()
            .ok()
            .and_then(|guard| {
                guard.as_ref().and_then(|status| {
                    (status.route_key == pending.route_key
                        && status.resource_ids == pending.resource_ids
                        && status.generation == pending.generation)
                        .then_some((
                            status.retry_attempts.saturating_add(1),
                            status.pending_since_us,
                        ))
                })
            })
            .unwrap_or((1, now));
        let backoff = Self::facade_retry_backoff(retry_attempts);
        let next_retry_at_us = now.saturating_add(backoff.as_micros() as u64);
        if let Ok(mut guard) = status_cell.write() {
            *guard = Some(SharedFacadePendingStatus {
                route_key: pending.route_key.clone(),
                generation: pending.generation,
                resource_ids: pending.resource_ids.clone(),
                runtime_managed: pending.runtime_managed,
                runtime_exposure_confirmed: pending.runtime_exposure_confirmed,
                reason: FacadePendingReason::RetryingAfterError,
                retry_attempts,
                pending_since_us,
                last_error: Some(err.to_string()),
                last_attempt_at_us: Some(now),
                last_error_at_us: Some(now),
                retry_backoff_ms: Some(backoff.as_millis() as u64),
                next_retry_at_us: Some(next_retry_at_us),
            });
        }
    }

    fn pending_facade_retry_due(
        status_cell: &SharedFacadePendingStatusCell,
        pending: &PendingFacadeActivation,
    ) -> bool {
        let now = now_us();
        status_cell
            .read()
            .ok()
            .and_then(|guard| {
                guard.as_ref().and_then(|status| {
                    (status.route_key == pending.route_key
                        && status.resource_ids == pending.resource_ids
                        && status.generation == pending.generation)
                        .then_some(
                            status
                                .next_retry_at_us
                                .map_or(true, |deadline| deadline <= now),
                        )
                })
            })
            .unwrap_or(true)
    }

    async fn pending_facade_snapshot_for(
        &self,
        route_key: &str,
        generation: u64,
    ) -> Option<PendingFacadeActivation> {
        self.pending_facade
            .lock()
            .await
            .as_ref()
            .and_then(|pending| {
                (pending.route_key == route_key && pending.generation == generation)
                    .then_some(pending.clone())
            })
    }

    async fn retry_pending_facade(
        &self,
        route_key: &str,
        generation: u64,
        from_tick: bool,
    ) -> Result<()> {
        let Some(pending) = self
            .pending_facade_snapshot_for(route_key, generation)
            .await
        else {
            return Ok(());
        };
        let has_active = self
            .api_task
            .lock()
            .await
            .as_ref()
            .is_some_and(|active| active.handle.is_running());
        let same_listener_generation_advance =
            self.api_task.lock().await.as_ref().is_some_and(|active| {
                active.handle.is_running()
                    && active.route_key == pending.route_key
                    && active.resource_ids == pending.resource_ids
            });
        let wait_reason = if same_listener_generation_advance && !pending.runtime_exposure_confirmed
        {
            Some(FacadePendingReason::AwaitingRuntimeExposure)
        } else if same_listener_generation_advance {
            None
        } else {
            Self::facade_replacement_wait_reason(
                self.source.as_ref(),
                self.sink.as_ref(),
                &pending,
                has_active,
            )
            .await?
        };
        if let Some(wait_reason) = wait_reason {
            Self::set_pending_facade_status_waiting(
                &self.facade_pending_status,
                &pending,
                wait_reason,
            );
            return Ok(());
        }
        let fixed_bind_handoff_completed = self
            .release_pending_fixed_bind_handoff_blocker_if_needed(&pending)
            .await?;
        if fixed_bind_handoff_completed == Some(false) {
            return Ok(());
        }
        if fixed_bind_handoff_completed.is_none()
            && from_tick
            && !Self::pending_facade_retry_due(&self.facade_pending_status, &pending)
        {
            return Ok(());
        }
        match self.try_spawn_pending_facade().await {
            Ok(_) => Ok(()),
            Err(err) => {
                Self::record_pending_facade_retry_error(
                    &self.facade_pending_status,
                    &pending,
                    &err,
                );
                log::warn!("fs-meta facade pending activation retry failed: {err}");
                Ok(())
            }
        }
    }

    async fn release_pending_fixed_bind_handoff_blocker_if_needed(
        &self,
        pending: &PendingFacadeActivation,
    ) -> Result<Option<bool>> {
        if pending.route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
            || facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
        {
            return Ok(None);
        }
        let registrant = PendingFixedBindHandoffRegistrant {
            instance_id: self.instance_id,
            api_task: self.api_task.clone(),
            pending_facade: self.pending_facade.clone(),
            pending_fixed_bind_claim_release_followup: self
                .pending_fixed_bind_claim_release_followup
                .clone(),
            pending_fixed_bind_has_suppressed_dependent_routes: self
                .pending_fixed_bind_has_suppressed_dependent_routes
                .clone(),
            retained_active_facade_continuity: self.retained_active_facade_continuity.clone(),
            facade_spawn_in_progress: self.facade_spawn_in_progress.clone(),
            facade_pending_status: self.facade_pending_status.clone(),
            facade_service_state: self.facade_service_state.clone(),
            rollout_status: self.rollout_status.clone(),
            api_request_tracker: self.api_request_tracker.clone(),
            api_control_gate: self.api_control_gate.clone(),
            control_failure_uninitialized: self.control_failure_uninitialized.clone(),
            runtime_gate_state: self.runtime_gate_state.clone(),
            runtime_state_changed: self.runtime_state_changed.clone(),
            node_id: self.node_id.clone(),
            runtime_boundary: self.runtime_boundary.clone(),
            source: self.source.clone(),
            sink: self.sink.clone(),
            query_sink: self.query_sink.clone(),
            query_runtime_boundary: self.runtime_boundary.clone(),
        };
        let Some(handoff) = registrant
            .release_handoff_blocker_for_publication(pending)
            .await
        else {
            return Ok(None);
        };
        self.pending_fixed_bind_claim_release_followup
            .store(true, Ordering::Release);
        self.notify_runtime_state_changed();
        Ok(Some(
            execute_fixed_bind_after_release_handoff(handoff).await?,
        ))
    }

    fn facade_signal_apply_priority(signal: &FacadeControlSignal) -> u8 {
        match signal {
            FacadeControlSignal::Activate {
                unit: FacadeRuntimeUnit::Facade,
                ..
            } => 0,
            _ => 1,
        }
    }

    fn facade_signal_updates_facade_claim(signal: &FacadeControlSignal) -> bool {
        matches!(
            signal,
            FacadeControlSignal::Activate {
                unit: FacadeRuntimeUnit::Facade,
                ..
            } | FacadeControlSignal::Deactivate {
                unit: FacadeRuntimeUnit::Facade,
                ..
            } | FacadeControlSignal::Tick {
                unit: FacadeRuntimeUnit::Facade,
                ..
            } | FacadeControlSignal::ExposureConfirmed {
                unit: FacadeRuntimeUnit::Facade,
                ..
            }
        )
    }

    fn facade_publication_signal_is_query_activate(signal: &FacadeControlSignal) -> bool {
        matches!(
            signal,
            FacadeControlSignal::Activate {
                unit: FacadeRuntimeUnit::Query,
                route_key,
                ..
            } if route_key == &format!("{}.req", ROUTE_KEY_QUERY)
        )
    }

    fn facade_publication_signal_is_sink_owned_query_peer_activate(
        signal: &FacadeControlSignal,
    ) -> bool {
        matches!(
            signal,
            FacadeControlSignal::Activate {
                unit: FacadeRuntimeUnit::QueryPeer,
                route_key,
                ..
            } if route_key == &format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)
                || is_sink_status_query_request_route(route_key)
                || is_per_peer_sink_query_request_route(route_key)
        )
    }

    fn bound_scopes_reference_any_resource(
        bound_scopes: &[RuntimeBoundScope],
        resource_ids: &[String],
    ) -> bool {
        if bound_scopes.is_empty() || resource_ids.is_empty() {
            return true;
        }
        let resource_ids = resource_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        bound_scopes.iter().any(|scope| {
            resource_ids.contains(scope.scope_id.as_str())
                || scope
                    .resource_ids
                    .iter()
                    .any(|resource_id| resource_ids.contains(resource_id.as_str()))
        })
    }

    fn facade_dependent_route_targets_blocked_pending_publication(
        followup_snapshot: &FixedBindClaimReleaseFollowupSnapshot,
        bound_scopes: &[RuntimeBoundScope],
    ) -> bool {
        followup_snapshot
            .pending_publication
            .as_ref()
            .is_none_or(|pending| {
                Self::bound_scopes_reference_any_resource(bound_scopes, &pending.resource_ids)
            })
    }

    fn suppressed_facade_business_read_activate_is_replayable(
        unit: FacadeRuntimeUnit,
        route_key: &str,
    ) -> bool {
        match unit {
            FacadeRuntimeUnit::Query => is_fixed_bind_facade_business_read_route(route_key),
            FacadeRuntimeUnit::QueryPeer => is_materialized_query_request_route(route_key),
            FacadeRuntimeUnit::Facade => false,
        }
    }

    async fn record_suppressed_public_query_activate(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
        bound_scopes: &[RuntimeBoundScope],
    ) {
        if !Self::suppressed_facade_business_read_activate_is_replayable(unit, route_key) {
            return;
        }
        self.retained_suppressed_public_query_activates
            .lock()
            .await
            .insert(
                (unit.unit_id().to_string(), route_key.to_string()),
                FacadeControlSignal::Activate {
                    unit,
                    route_key: route_key.to_string(),
                    generation,
                    bound_scopes: bound_scopes.to_vec(),
                },
            );
    }

    async fn replay_suppressed_public_query_activates_after_publication_with_session(
        &self,
        mut fixed_bind_session: FixedBindLifecycleSession,
        replay_public_query_signals: bool,
    ) -> Result<FixedBindLifecycleSession> {
        let facade_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
        let publication_complete = self.pending_facade.lock().await.is_none()
            && self
                .api_task
                .lock()
                .await
                .as_ref()
                .is_some_and(|active| active.route_key == facade_route_key);
        if !publication_complete {
            return Ok(fixed_bind_session);
        }
        fixed_bind_session = self
            .rebuild_fixed_bind_lifecycle_session(
                &fixed_bind_session,
                FixedBindLifecycleRebuildReason::ReplaySuppressedFacadeDependentRoutesAfterPublication,
            )
            .await;
        let suppressed_dependent_routes_pending = self
            .pending_fixed_bind_has_suppressed_dependent_routes
            .load(Ordering::Acquire);
        let signals = {
            let mut retained = self.retained_suppressed_public_query_activates.lock().await;
            let drained = std::mem::take(&mut *retained);
            drained.into_values().collect::<Vec<_>>()
        };
        if replay_public_query_signals {
            for signal in signals {
                fixed_bind_session = self
                    .apply_facade_signal_with_session(fixed_bind_session, signal)
                    .await?;
            }
        }
        if suppressed_dependent_routes_pending {
            let retained_sink_signals = self.retained_sink_facade_dependent_route_activates().await;
            if !retained_sink_signals.is_empty() {
                self.apply_sink_signals_with_recovery(
                    &fixed_bind_session,
                    &retained_sink_signals,
                    false,
                    false,
                    false,
                )
                .await
                .map_err(RuntimeWorkerControlApplyFailure::into_error)?;
            }
        }
        self.pending_fixed_bind_has_suppressed_dependent_routes
            .store(false, Ordering::Release);
        Ok(fixed_bind_session)
    }

    fn facade_spawn_error_is_bind_addr_in_use(err: &CnxError) -> bool {
        let message = err.to_string().to_ascii_lowercase();
        message.contains("fs-meta api bind failed") && message.contains("address already in use")
    }

    async fn build_fixed_bind_snapshot_for_pending_publication(
        instance_id: u64,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        pending: Option<PendingFacadeActivation>,
        claim_release_followup_pending: bool,
    ) -> FacadeFixedBindSnapshot {
        let Some(pending) = pending else {
            return FacadeFixedBindSnapshot {
                pending_publication: None,
                conflicting_process_claim: None,
                claim_release_followup_pending,
            };
        };
        if pending.route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
            || facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
        {
            return FacadeFixedBindSnapshot {
                pending_publication: None,
                conflicting_process_claim: None,
                claim_release_followup_pending,
            };
        }
        if api_task.lock().await.as_ref().is_some_and(|active| {
            active.route_key == pending.route_key && active.resource_ids == pending.resource_ids
        }) {
            return FacadeFixedBindSnapshot {
                pending_publication: None,
                conflicting_process_claim: None,
                claim_release_followup_pending,
            };
        }
        let conflicting_process_claim = {
            let guard = match process_facade_claim_cell().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.get(&pending.resolved.bind_addr).and_then(|claim| {
                (claim.owner_instance_id != instance_id
                    && claim.bind_addr == pending.resolved.bind_addr)
                    .then_some(claim.clone())
            })
        };
        FacadeFixedBindSnapshot {
            pending_publication: Some(pending.clone()),
            conflicting_process_claim,
            claim_release_followup_pending,
        }
    }

    fn sink_route_is_facade_dependent_query_request(route_key: &str) -> bool {
        route_key == format!("{}.req", ROUTE_KEY_QUERY)
            || route_key == format!("{}.req", ROUTE_KEY_FORCE_FIND)
            || route_key == format!("{}.req", ROUTE_KEY_SINK_QUERY_INTERNAL)
            || is_per_peer_sink_query_request_route(route_key)
    }

    async fn retained_sink_facade_dependent_route_activates(&self) -> Vec<SinkControlSignal> {
        let retained = self.retained_sink_control_state.lock().await.clone();
        Self::sink_signals_from_state(&retained)
            .into_iter()
            .filter(|signal| {
                matches!(
                    signal,
                    SinkControlSignal::Activate { route_key, .. }
                        if Self::sink_route_is_facade_dependent_query_request(route_key)
                )
            })
            .collect()
    }

    async fn fixed_bind_handoff_preparation_active(&self) -> bool {
        let Some(pending) = self.pending_facade.lock().await.clone() else {
            return false;
        };
        if pending.route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
            || facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
        {
            return false;
        }
        if self
            .pending_fixed_bind_claim_release_followup
            .load(Ordering::Acquire)
        {
            return true;
        }
        let active_matches_pending = self.api_task.lock().await.as_ref().is_some_and(|active| {
            active.route_key == pending.route_key
                && active.resource_ids == pending.resource_ids
                && active.generation == pending.generation
        });
        if !active_matches_pending {
            return true;
        }
        let guard = match process_facade_claim_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.get(&pending.resolved.bind_addr).is_some_and(|claim| {
            claim.bind_addr == pending.resolved.bind_addr
                && claim.owner_instance_id != self.instance_id
        })
    }

    async fn fixed_bind_publication_tail_pending(&self) -> bool {
        if !self
            .pending_fixed_bind_has_suppressed_dependent_routes
            .load(Ordering::Acquire)
        {
            return false;
        }
        if self.pending_facade.lock().await.is_some() {
            return false;
        }
        let bind_addr = {
            let api_task = self.api_task.lock().await;
            api_task.as_ref().and_then(|active| {
                (active.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL))
                    .then(|| {
                        self.config
                            .api
                            .resolve_for_candidate_ids(&active.resource_ids)
                    })
                    .flatten()
                    .map(|resolved| resolved.bind_addr)
                    .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
            })
        };
        bind_addr
            .as_deref()
            .is_some_and(|bind_addr| active_fixed_bind_facade_owned_by(bind_addr, self.instance_id))
    }

    async fn fixed_bind_publication_continuation_active(&self) -> bool {
        self.fixed_bind_handoff_preparation_active().await
            || self.fixed_bind_publication_tail_pending().await
    }

    async fn filter_sink_facade_dependent_route_activates_during_pending_fixed_bind_claim_with_session(
        &self,
        sink_signals: &[SinkControlSignal],
        session: &FixedBindLifecycleSession,
    ) -> Vec<SinkControlSignal> {
        let followup_snapshot = session.claim_release_followup_snapshot();
        let allow_routes = !followup_snapshot.blocks_facade_dependent_routes();
        let mut filtered = Vec::with_capacity(sink_signals.len());
        for signal in sink_signals {
            match signal {
                SinkControlSignal::Activate {
                    route_key,
                    generation,
                    ..
                } if Self::sink_route_is_facade_dependent_query_request(route_key)
                    && !allow_routes =>
                {
                    self.pending_fixed_bind_has_suppressed_dependent_routes
                        .store(true, Ordering::Release);
                    eprintln!(
                        "fs_meta_runtime_app: suppress sink-owned facade-dependent route route_key={} generation={} while pending fixed-bind facade publication is incomplete",
                        route_key, generation
                    );
                }
                _ => filtered.push(signal.clone()),
            }
        }
        filtered
    }

    #[cfg(test)]
    async fn apply_facade_activate(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
        bound_scopes: &[RuntimeBoundScope],
    ) -> Result<()> {
        let session = self.begin_fixed_bind_lifecycle_session().await;
        let _ = self
            .apply_facade_activate_with_session(session, unit, route_key, generation, bound_scopes)
            .await?;
        Ok(())
    }

    async fn apply_facade_activate_with_session(
        &self,
        mut session: FixedBindLifecycleSession,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
        bound_scopes: &[RuntimeBoundScope],
    ) -> Result<FixedBindLifecycleSession> {
        eprintln!(
            "fs_meta_runtime_app: apply_facade_activate unit={} route_key={} generation={} scopes={}",
            unit.unit_id(),
            route_key,
            generation,
            bound_scopes.len()
        );
        if !facade_route_key_matches(unit, route_key) {
            return Ok(session);
        }
        if matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        ) && is_fixed_bind_facade_business_read_route(route_key)
        {
            let (followup_snapshot, next_session) = self
                .settle_fixed_bind_claim_release_followup_with_session(session)
                .await?;
            session = next_session;
            let publication_still_incomplete =
                followup_snapshot.blocks_facade_dependent_routes_without_claim();
            let targets_blocked_pending_publication =
                Self::facade_dependent_route_targets_blocked_pending_publication(
                    &followup_snapshot,
                    bound_scopes,
                );
            if followup_snapshot.conflicting_process_claim.is_some()
                && targets_blocked_pending_publication
            {
                self.pending_fixed_bind_has_suppressed_dependent_routes
                    .store(true, Ordering::Release);
                self.record_suppressed_public_query_activate(
                    unit,
                    route_key,
                    generation,
                    bound_scopes,
                )
                .await;
                eprintln!(
                    "fs_meta_runtime_app: suppress facade-dependent route unit={} route_key={} generation={} while pending facade fixed-bind claim is still owned by another instance",
                    unit.unit_id(),
                    route_key,
                    generation
                );
                return Ok(session);
            }
            if publication_still_incomplete && targets_blocked_pending_publication {
                self.pending_fixed_bind_has_suppressed_dependent_routes
                    .store(true, Ordering::Release);
                self.record_suppressed_public_query_activate(
                    unit,
                    route_key,
                    generation,
                    bound_scopes,
                )
                .await;
                eprintln!(
                    "fs_meta_runtime_app: suppress facade-dependent route unit={} route_key={} generation={} while fixed-bind facade publication is still incomplete",
                    unit.unit_id(),
                    route_key,
                    generation
                );
                return Ok(session);
            }
        }
        // Facade activation is a runtime-owned generation/bind/run handoff carrier.
        // Trusted external observation remains subordinate to package-local
        // observation_eligible and projection catch-up; activation alone does
        // not imply current authoritative truth is already reflected outside.
        let unit_id = unit.unit_id();
        let accepted =
            self.facade_gate
                .apply_activate(unit_id, route_key, generation, bound_scopes)?;
        if !accepted {
            log::info!(
                "fs-meta facade: ignore stale activate unit={} generation={}",
                unit_id,
                generation
            );
            return Ok(session);
        }
        if matches!(unit, FacadeRuntimeUnit::Query) {
            self.mirrored_query_peer_routes
                .lock()
                .await
                .remove(route_key);
        } else if matches!(unit, FacadeRuntimeUnit::QueryPeer)
            && is_dual_lane_internal_query_route(route_key)
        {
            let query_active = self
                .facade_gate
                .route_active(execution_units::QUERY_RUNTIME_UNIT_ID, route_key)?;
            let mut mirrored = self.mirrored_query_peer_routes.lock().await;
            if mirrored.contains_key(route_key) || !query_active {
                self.facade_gate.apply_activate(
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    route_key,
                    generation,
                    bound_scopes,
                )?;
                mirrored.insert(route_key.to_string(), generation);
            }
        }
        let facade_control_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
        if matches!(unit, FacadeRuntimeUnit::Query) || route_key != facade_control_route_key {
            return Ok(session);
        }
        self.retained_active_facade_continuity
            .store(false, Ordering::Release);
        let candidate_resource_ids = Self::facade_candidate_resource_ids(bound_scopes);
        let runtime_managed = self.runtime_boundary.is_some();
        let resolved = self
            .config
            .api
            .resolve_for_candidate_ids(&candidate_resource_ids)
            .ok_or_else(|| {
                CnxError::InvalidInput(format!(
                    "fs-meta facade activation requires locally announced facade resource among {:?}",
                    candidate_resource_ids
                ))
            })?;
        let stale_active = {
            let mut api_task = self.api_task.lock().await;
            if api_task.as_ref().is_some_and(|active| {
                active.route_key == route_key
                    && active.resource_ids == candidate_resource_ids
                    && !active.handle.is_running()
            }) {
                api_task.take()
            } else {
                None
            }
        };
        if let Some(stale) = stale_active {
            eprintln!(
                "fs_meta_runtime_app: prune dead active facade handle before same-resource activate route_key={} generation={}",
                route_key, generation
            );
            stale.handle.shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT).await;
            clear_owned_process_facade_claim(self.instance_id);
            if let Some(bind_addr) = self
                .config
                .api
                .resolve_for_candidate_ids(&stale.resource_ids)
                .map(|resolved| resolved.bind_addr)
                .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
            {
                clear_process_facade_claim_for_retired_fixed_bind(&bind_addr);
                clear_active_fixed_bind_facade_owner(&bind_addr, self.instance_id);
            }
        }
        {
            let mut api_task = self.api_task.lock().await;
            if let Some(current) = api_task.as_mut()
                && current.route_key == route_key
                && current.resource_ids == candidate_resource_ids
            {
                current.generation = generation;
                drop(api_task);
                let mut pending = self.pending_facade.lock().await;
                if pending.as_ref().is_some_and(|candidate| {
                    candidate.route_key == route_key
                        && candidate.resource_ids == candidate_resource_ids
                }) {
                    pending.take();
                }
                Self::clear_pending_facade_status(&self.facade_pending_status);
                drop(pending);
                session = self
                    .rebuild_fixed_bind_lifecycle_session(
                        &session,
                        FixedBindLifecycleRebuildReason::PublishFacadeServiceStateAfterFacadeActivateGenerationUpdate,
                    )
                    .await;
                let _ = self
                    .publish_facade_service_state_with_session(&session)
                    .await;
                return Ok(session);
            }
        }
        let runtime_exposure_confirmed = !runtime_managed;
        let pending = PendingFacadeActivation {
            route_key: route_key.to_string(),
            generation,
            resource_ids: candidate_resource_ids,
            bound_scopes: bound_scopes.to_vec(),
            group_ids: Vec::new(),
            runtime_managed,
            runtime_exposure_confirmed,
            resolved,
        };
        *self.pending_facade.lock().await = Some(pending.clone());
        session = self
            .rebuild_fixed_bind_lifecycle_session(
                &session,
                FixedBindLifecycleRebuildReason::PublishFacadeServiceStateAfterPendingFacadeActivate,
            )
            .await;
        let _ = self
            .publish_facade_service_state_with_session(&session)
            .await;
        if !self.try_spawn_pending_facade().await? {
            eprintln!(
                "fs_meta_runtime_app: pending facade generation={} route_key={} runtime_exposure_confirmed={}",
                pending.generation, pending.route_key, pending.runtime_exposure_confirmed
            );
        }
        session = self
            .rebuild_fixed_bind_lifecycle_session(
                &session,
                FixedBindLifecycleRebuildReason::PublishFacadeServiceStateAfterPendingFacadeSpawnAttempt,
            )
            .await;
        Ok(session)
    }

    #[cfg(test)]
    async fn shutdown_active_facade(&self) {
        self.drive_fixed_bind_lifecycle_request(FixedBindLifecycleRequest::Shutdown)
            .await
            .expect("shutdown handoff should complete");
    }

    async fn withdraw_uninitialized_query_routes_with_policy(
        &self,
        recovery_lane_policy: ControlFailureRecoveryLanePolicy,
        preserve_core_business_read_routes: bool,
    ) {
        #[cfg(test)]
        maybe_pause_before_uninitialized_query_withdraw().await;
        let mut query_routes = Vec::new();
        if recovery_lane_policy == ControlFailureRecoveryLanePolicy::WithdrawInternalStatus {
            query_routes.push(format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL));
        }
        if matches!(
            recovery_lane_policy,
            ControlFailureRecoveryLanePolicy::WithdrawInternalStatus
                | ControlFailureRecoveryLanePolicy::PreserveSourceStatus
        ) {
            query_routes.push(format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY));
        }
        for route_key in &query_routes {
            if preserve_core_business_read_routes
                && is_serving_facade_business_read_route(route_key)
            {
                continue;
            }
            let _ = self
                .facade_gate
                .clear_route(execution_units::QUERY_RUNTIME_UNIT_ID, route_key);
            let _ = self
                .facade_gate
                .clear_route(execution_units::QUERY_PEER_RUNTIME_UNIT_ID, route_key);
            self.mirrored_query_peer_routes
                .lock()
                .await
                .remove(route_key);
        }

        let mut endpoint_tasks = std::mem::take(&mut *self.runtime_endpoint_tasks.lock().await);
        let mut retained = Vec::with_capacity(endpoint_tasks.len());
        for mut task in endpoint_tasks.drain(..) {
            let cleanup_route = if preserve_core_business_read_routes {
                is_uninitialized_cleanup_query_route_tail_with_policy(
                    task.route_key(),
                    recovery_lane_policy,
                )
            } else {
                is_uninitialized_cleanup_query_route_with_policy(
                    task.route_key(),
                    recovery_lane_policy,
                )
            };
            if cleanup_route {
                task.shutdown(Duration::from_secs(1)).await;
            } else {
                retained.push(task);
            }
        }
        *self.runtime_endpoint_tasks.lock().await = retained;

        let mut routes = self.runtime_endpoint_routes.lock().await;
        routes.retain(|route_key| {
            let cleanup_route = if preserve_core_business_read_routes {
                is_uninitialized_cleanup_query_route_tail_with_policy(
                    route_key,
                    recovery_lane_policy,
                )
            } else {
                is_uninitialized_cleanup_query_route_with_policy(route_key, recovery_lane_policy)
            };
            !cleanup_route
        });
    }

    async fn wait_for_shared_worker_control_handoff(&self) {
        self.source
            .wait_for_control_ops_to_drain_for_handoff()
            .await;
        self.sink.wait_for_control_ops_to_drain_for_handoff().await;
    }

    #[cfg(test)]
    async fn reinitialize_after_control_reset_with_deadline_and_session(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        self.reinitialize_after_control_reset_with_deadline_and_session_with_failure(
            fixed_bind_session,
            deadline,
        )
        .await
        .map_err(RuntimeInitializeFailure::into_error)
    }

    async fn reinitialize_after_control_reset_with_deadline_and_session_with_failure(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        deadline: tokio::time::Instant,
    ) -> std::result::Result<(), RuntimeInitializeFailure> {
        self.update_runtime_control_state(|state| {
            state.mark_uninitialized_with_full_replay();
        });
        self.api_control_gate.set_ready(false);
        let fixed_bind_session = self
            .rebuild_fixed_bind_lifecycle_session(
                fixed_bind_session,
                FixedBindLifecycleRebuildReason::PublishFacadeServiceStateAfterControlFailureUninitialized,
            )
            .await;
        let _ = self
            .publish_facade_service_state_with_session(&fixed_bind_session)
            .await;
        self.initialize_from_control_with_deadline_and_session(
            false,
            false,
            Some(deadline),
            &fixed_bind_session,
        )
        .await
    }

    #[cfg(test)]
    async fn reinitialize_after_control_reset_with_deadline(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        let fixed_bind_session = self.begin_fixed_bind_lifecycle_session().await;
        self.reinitialize_after_control_reset_with_deadline_and_session(
            &fixed_bind_session,
            deadline,
        )
        .await
    }

    async fn mark_control_uninitialized_after_failure_with_session(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
    ) {
        self.mark_control_uninitialized_after_failure_with_session_and_policy(
            fixed_bind_session,
            ControlFailureRecoveryLanePolicy::WithdrawInternalStatus,
        )
        .await;
    }

    async fn mark_control_uninitialized_after_deferred_source_replay_with_session(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        require_sink_replay: bool,
    ) {
        self.mark_control_uninitialized_after_deferred_source_replay_with_session_and_schedule(
            fixed_bind_session,
            require_sink_replay,
            true,
        )
        .await;
    }

    async fn mark_control_uninitialized_after_deferred_source_replay_without_scheduled_repair_with_session(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        require_sink_replay: bool,
    ) {
        self.mark_control_uninitialized_after_deferred_source_replay_with_session_and_schedule(
            fixed_bind_session,
            require_sink_replay,
            false,
        )
        .await;
    }

    async fn mark_control_uninitialized_after_deferred_source_replay_with_session_and_schedule(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        require_sink_replay: bool,
        schedule_repair: bool,
    ) {
        self.source_generation_cutover_replay_deferred
            .store(true, Ordering::Release);
        if require_sink_replay {
            self.sink_generation_cutover_replay_deferred
                .store(true, Ordering::Release);
        }
        self.mark_control_uninitialized_after_failure_with_session_and_policy_and_replay(
            fixed_bind_session,
            if require_sink_replay {
                ControlFailureRecoveryLanePolicy::PreserveSourceStatus
            } else {
                ControlFailureRecoveryLanePolicy::PreserveInternalStatus
            },
            if require_sink_replay {
                ControlFailureReplayRequirement::SourceAndSink
            } else {
                ControlFailureReplayRequirement::Source
            },
            true,
        )
        .await;
        if schedule_repair {
            self.schedule_deferred_source_repair_recovery("source-generation-cutover");
        }
    }

    async fn mark_control_uninitialized_after_deferred_sink_replay_with_session(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
    ) {
        self.mark_control_uninitialized_after_deferred_sink_replay_with_session_and_schedule(
            fixed_bind_session,
            true,
        )
        .await;
    }

    async fn mark_control_uninitialized_after_deferred_sink_replay_without_scheduled_repair_with_session(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
    ) {
        self.mark_control_uninitialized_after_deferred_sink_replay_with_session_and_schedule(
            fixed_bind_session,
            false,
        )
        .await;
    }

    async fn mark_control_uninitialized_after_deferred_sink_replay_with_session_and_schedule(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        schedule_repair: bool,
    ) {
        self.sink_generation_cutover_replay_deferred
            .store(true, Ordering::Release);
        self.mark_control_uninitialized_after_failure_with_session_and_policy_and_replay(
            fixed_bind_session,
            ControlFailureRecoveryLanePolicy::WithdrawInternalStatus,
            ControlFailureReplayRequirement::Sink,
            true,
        )
        .await;
        if schedule_repair {
            self.schedule_deferred_sink_repair_recovery("sink-generation-cutover");
        }
    }

    async fn mark_control_uninitialized_after_generation_cutover_cleanup_with_session(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        preserve_source_status: bool,
    ) {
        self.mark_control_uninitialized_after_failure_with_session_and_policy_and_replay(
            fixed_bind_session,
            if preserve_source_status {
                ControlFailureRecoveryLanePolicy::PreserveSourceStatus
            } else {
                ControlFailureRecoveryLanePolicy::PreserveInternalStatus
            },
            ControlFailureReplayRequirement::None,
            true,
        )
        .await;
    }

    async fn mark_control_uninitialized_after_failure_preserving_internal_status_with_session(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
    ) {
        self.mark_control_uninitialized_after_failure_with_session_and_policy(
            fixed_bind_session,
            ControlFailureRecoveryLanePolicy::PreserveInternalStatus,
        )
        .await;
    }

    async fn mark_control_uninitialized_after_failure_with_session_and_policy(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        recovery_lane_policy: ControlFailureRecoveryLanePolicy,
    ) {
        self.mark_control_uninitialized_after_failure_with_session_and_policy_and_replay(
            fixed_bind_session,
            recovery_lane_policy,
            ControlFailureReplayRequirement::Full,
            false,
        )
        .await;
    }

    async fn mark_control_uninitialized_after_failure_with_session_and_policy_and_replay(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        recovery_lane_policy: ControlFailureRecoveryLanePolicy,
        replay_requirement: ControlFailureReplayRequirement,
        preserve_core_business_read_routes: bool,
    ) {
        self.update_runtime_control_state(|state| match replay_requirement {
            ControlFailureReplayRequirement::None => state.mark_uninitialized_preserving_replay(),
            ControlFailureReplayRequirement::Full => state.mark_uninitialized_with_full_replay(),
            ControlFailureReplayRequirement::Source => {
                state.mark_uninitialized_preserving_replay();
                state.require_source_replay();
            }
            ControlFailureReplayRequirement::SourceAndSink => {
                state.mark_uninitialized_preserving_replay();
                state.require_source_replay();
                state.require_sink_replay();
            }
            ControlFailureReplayRequirement::Sink => {
                state.mark_uninitialized_preserving_replay();
                state.require_sink_replay();
            }
        });
        self.control_failure_uninitialized
            .store(true, Ordering::Release);
        self.api_control_gate.set_ready(false);
        let fixed_bind_session = self
            .rebuild_fixed_bind_lifecycle_session(
                fixed_bind_session,
                FixedBindLifecycleRebuildReason::PublishFacadeServiceStateAfterControlFailureUninitialized,
            )
            .await;
        let _ = self
            .publish_facade_service_state_with_session(&fixed_bind_session)
            .await;
        self.retained_active_facade_continuity
            .store(false, Ordering::Release);
        self.clear_shared_source_route_claims_for_instance().await;
        self.clear_shared_sink_route_claims_for_instance().await;
        let _ = self
            .release_failed_fixed_bind_owner_for_pending_successor(&fixed_bind_session)
            .await;
        match recovery_lane_policy {
            ControlFailureRecoveryLanePolicy::WithdrawInternalStatus => {
                self.withdraw_uninitialized_query_routes_with_policy(
                    ControlFailureRecoveryLanePolicy::WithdrawInternalStatus,
                    preserve_core_business_read_routes,
                )
                .await;
            }
            ControlFailureRecoveryLanePolicy::PreserveSourceStatus => {
                self.withdraw_uninitialized_query_routes_with_policy(
                    ControlFailureRecoveryLanePolicy::PreserveSourceStatus,
                    preserve_core_business_read_routes,
                )
                .await;
            }
            ControlFailureRecoveryLanePolicy::PreserveInternalStatus => {
                self.withdraw_uninitialized_query_routes_with_policy(
                    ControlFailureRecoveryLanePolicy::PreserveInternalStatus,
                    preserve_core_business_read_routes,
                )
                .await;
            }
        }
    }

    #[cfg(test)]
    async fn mark_control_uninitialized_after_failure(&self) {
        let fixed_bind_session = self.begin_fixed_bind_lifecycle_session().await;
        self.mark_control_uninitialized_after_failure_with_session(&fixed_bind_session)
            .await
    }

    async fn release_failed_fixed_bind_owner_for_pending_successor(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
    ) -> bool {
        if let Some(handoff) = self
            .fixed_bind_handoff_after_control_failure_uninitialized()
            .await
        {
            eprintln!(
                "fs_meta_runtime_app: release failed fixed-bind owner for pending successor bind_addr={}",
                handoff.bind_addr
            );
            if let Err(err) = self
                .drive_fixed_bind_shutdown_handoff(fixed_bind_session.clone(), Some(handoff))
                .await
            {
                eprintln!(
                    "fs_meta_runtime_app: release failed fixed-bind owner err={}",
                    err
                );
            }
            return true;
        }
        self.release_continuity_retained_fixed_bind_owner_for_failed_pending_successor()
            .await
    }

    async fn fixed_bind_handoff_after_control_failure_uninitialized(
        &self,
    ) -> Option<ActiveFixedBindShutdownContinuation> {
        if self.control_initialized() {
            return None;
        }
        if !self.control_failure_uninitialized.load(Ordering::Acquire)
            && self
                .retained_active_facade_continuity
                .load(Ordering::Acquire)
        {
            return None;
        }
        let bind_addr = {
            let api_task = self.api_task.lock().await;
            api_task.as_ref().and_then(|active| {
                (active.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL))
                    .then(|| {
                        self.config
                            .api
                            .resolve_for_candidate_ids(&active.resource_ids)
                    })
                    .flatten()
                    .map(|resolved| resolved.bind_addr)
                    .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
            })
        }?;
        let registrant = pending_fixed_bind_handoff_ready_for(&bind_addr)?;
        if registrant.instance_id == self.instance_id {
            return None;
        }
        let pending = registrant.pending_facade.lock().await.clone()?;
        if pending.route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL) {
            return None;
        }
        Some(ActiveFixedBindShutdownContinuation {
            bind_addr: bind_addr.clone(),
            pending_handoff: Some(PendingFixedBindHandoffContinuation {
                bind_addr,
                registrant,
            }),
            release_mode: FixedBindOwnerReleaseMode::FailedOwnerCutover,
        })
    }

    async fn release_continuity_retained_fixed_bind_owner_for_failed_pending_successor(
        &self,
    ) -> bool {
        let pending = self.pending_facade.lock().await.clone();
        let Some(pending) = pending else {
            return false;
        };
        if pending.route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
            || facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
        {
            return false;
        }
        let bind_addr = pending.resolved.bind_addr.clone();
        let Some(owner) = active_fixed_bind_facade_owner_for(&bind_addr, self.instance_id) else {
            return false;
        };
        if !owner
            .retained_active_facade_continuity
            .load(Ordering::Acquire)
        {
            return false;
        }

        let registrant = PendingFixedBindHandoffRegistrant {
            instance_id: self.instance_id,
            api_task: self.api_task.clone(),
            pending_facade: self.pending_facade.clone(),
            pending_fixed_bind_claim_release_followup: self
                .pending_fixed_bind_claim_release_followup
                .clone(),
            pending_fixed_bind_has_suppressed_dependent_routes: self
                .pending_fixed_bind_has_suppressed_dependent_routes
                .clone(),
            retained_active_facade_continuity: self.retained_active_facade_continuity.clone(),
            facade_spawn_in_progress: self.facade_spawn_in_progress.clone(),
            facade_pending_status: self.facade_pending_status.clone(),
            facade_service_state: self.facade_service_state.clone(),
            rollout_status: self.rollout_status.clone(),
            api_request_tracker: self.api_request_tracker.clone(),
            api_control_gate: self.api_control_gate.clone(),
            control_failure_uninitialized: self.control_failure_uninitialized.clone(),
            runtime_gate_state: self.runtime_gate_state.clone(),
            runtime_state_changed: self.runtime_state_changed.clone(),
            node_id: self.node_id.clone(),
            runtime_boundary: self.runtime_boundary.clone(),
            source: self.source.clone(),
            sink: self.sink.clone(),
            query_sink: self.query_sink.clone(),
            query_runtime_boundary: self.runtime_boundary.clone(),
        };
        mark_pending_fixed_bind_handoff_ready_with_registrant(&bind_addr, registrant.clone());
        eprintln!(
            "fs_meta_runtime_app: release continuity-retained fixed-bind owner after pending successor fail-closed bind_addr={}",
            bind_addr
        );
        owner
            .release_for_handoff(&bind_addr, FixedBindOwnerReleaseMode::FailedOwnerCutover)
            .await;
        self.pending_fixed_bind_claim_release_followup
            .store(true, Ordering::Release);
        self.notify_runtime_state_changed();
        match execute_fixed_bind_after_release_handoff(PendingFixedBindHandoffContinuation {
            bind_addr,
            registrant,
        })
        .await
        {
            Ok(completed) => completed,
            Err(err) => {
                eprintln!(
                    "fs_meta_runtime_app: release continuity-retained fixed-bind owner after pending successor fail-closed err={}",
                    err
                );
                false
            }
        }
    }

    async fn source_signals_with_replay(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> Vec<SourceControlSignal> {
        if !self.runtime_control_state().source_state_replay_required() {
            return source_signals.to_vec();
        }

        if Self::source_signals_are_host_grant_change_only(source_signals) {
            return source_signals.to_vec();
        }

        let mut replayed = self
            .source
            .control_signals_with_replay(source_signals)
            .await;
        replayed.extend(Self::source_transient_followup_signals(source_signals));
        replayed
    }

    async fn source_retained_replay_has_active_route_state(&self) -> bool {
        self.source
            .control_signals_with_replay(&[])
            .await
            .iter()
            .any(|signal| matches!(signal, SourceControlSignal::Activate { .. }))
    }

    fn source_signals_are_host_grant_change_only(source_signals: &[SourceControlSignal]) -> bool {
        !source_signals.is_empty()
            && source_signals
                .iter()
                .all(|signal| matches!(signal, SourceControlSignal::RuntimeHostGrantChange { .. }))
    }

    fn sink_signals_are_host_grant_change_only(sink_signals: &[SinkControlSignal]) -> bool {
        !sink_signals.is_empty()
            && sink_signals
                .iter()
                .all(|signal| matches!(signal, SinkControlSignal::RuntimeHostGrantChange { .. }))
    }

    fn facade_signals_are_host_grant_change_only(facade_signals: &[FacadeControlSignal]) -> bool {
        facade_signals
            .iter()
            .all(|signal| matches!(signal, FacadeControlSignal::RuntimeHostGrantChange { .. }))
    }

    fn control_frame_is_host_grant_change_only(
        source_signals: &[SourceControlSignal],
        sink_signals: &[SinkControlSignal],
        facade_signals: &[FacadeControlSignal],
    ) -> bool {
        (!source_signals.is_empty() || !sink_signals.is_empty() || !facade_signals.is_empty())
            && (source_signals.is_empty()
                || Self::source_signals_are_host_grant_change_only(source_signals))
            && (sink_signals.is_empty()
                || Self::sink_signals_are_host_grant_change_only(sink_signals))
            && Self::facade_signals_are_host_grant_change_only(facade_signals)
    }

    async fn apply_host_grant_change_fast_lane(
        &self,
        source_signals: &[SourceControlSignal],
        sink_signals: &[SinkControlSignal],
    ) -> std::result::Result<(), RuntimeControlFrameFailure> {
        eprintln!(
            "fs_meta_runtime_app: host-grant fast lane begin source_signals={} sink_signals={}",
            source_signals.len(),
            sink_signals.len()
        );
        let state_at_entry = self.runtime_control_state();
        if !state_at_entry.control_initialized()
            && !state_at_entry.source_state_replay_required()
            && !state_at_entry.sink_state_replay_required()
        {
            self.record_retained_source_control_state(source_signals)
                .await;
            self.record_retained_sink_control_state(sink_signals).await;
            eprintln!(
                "fs_meta_runtime_app: host-grant fast lane deferred while runtime uninitialized"
            );
            return Err(Self::not_ready_error().into());
        }
        let source_apply = async {
            if source_signals.is_empty() {
                return Ok(());
            }
            if self.runtime_control_state().source_state_replay_required() {
                eprintln!(
                    "fs_meta_runtime_app: host-grant fast lane deferred source worker apply role=source reason=retained_source_replay_pending"
                );
                return Ok(());
            }
            self.source
                .apply_orchestration_signals_with_total_timeout_with_failure(
                    source_signals,
                    HOST_GRANT_CONTROL_FAST_LANE_TIMEOUT,
                )
                .await
                .map_err(RuntimeWorkerControlApplyFailure::from)
        };
        let sink_apply = async {
            if sink_signals.is_empty() {
                return Ok(());
            }
            if self.runtime_control_state().sink_state_replay_required() {
                eprintln!(
                    "fs_meta_runtime_app: host-grant fast lane deferred sink worker apply role=sink reason=retained_sink_replay_pending"
                );
                return Ok(());
            }
            self.sink
                .apply_orchestration_signals_with_total_timeout_with_failure(
                    sink_signals,
                    HOST_GRANT_CONTROL_FAST_LANE_TIMEOUT,
                )
                .await
                .map_err(RuntimeWorkerControlApplyFailure::from)
        };
        let (source_result, sink_result) = tokio::join!(source_apply, sink_apply);
        self.accept_host_grant_worker_result("source", source_result, source_signals)
            .await?;
        self.accept_host_grant_worker_result("sink", sink_result, sink_signals)
            .await?;
        eprintln!("fs_meta_runtime_app: host-grant fast lane done");
        Ok(())
    }

    async fn accept_host_grant_worker_result<T>(
        &self,
        role: &str,
        result: std::result::Result<(), RuntimeWorkerControlApplyFailure>,
        signals: &[T],
    ) -> std::result::Result<(), RuntimeControlFrameFailure>
    where
        T: HostGrantFastLaneSignal,
    {
        match result {
            Ok(()) => {
                T::record_retained(self, signals).await;
                Ok(())
            }
            Err(err) if is_retryable_worker_control_reset(err.as_error()) => {
                eprintln!(
                    "fs_meta_runtime_app: host-grant fast lane accepted with deferred worker apply role={} err={}",
                    role,
                    err.as_error()
                );
                T::record_retained(self, signals).await;
                Ok(())
            }
            Err(err) => Err(RuntimeControlFrameFailure::ControlApply(err)),
        }
    }

    async fn record_retained_source_control_state(&self, source_signals: &[SourceControlSignal]) {
        self.source
            .record_retained_control_signals(source_signals)
            .await;
    }

    fn source_signals_are_transient_followup_only(source_signals: &[SourceControlSignal]) -> bool {
        !source_signals.is_empty()
            && source_signals.iter().all(|signal| {
                matches!(
                    signal,
                    SourceControlSignal::Tick { .. }
                        | SourceControlSignal::ManualRescan { .. }
                        | SourceControlSignal::Passthrough(_)
                )
            })
    }

    fn source_tick_route_generation(signal: &SourceControlSignal) -> Option<(String, String, u64)> {
        match signal {
            SourceControlSignal::Tick {
                unit,
                route_key,
                generation,
                ..
            } => Some((unit.unit_id().to_string(), route_key.clone(), *generation)),
            _ => None,
        }
    }

    fn source_active_route_generation(
        signal: &SourceControlSignal,
    ) -> Option<(String, String, u64)> {
        match signal {
            SourceControlSignal::Activate {
                unit,
                route_key,
                generation,
                ..
            } => Some((unit.unit_id().to_string(), route_key.clone(), *generation)),
            _ => None,
        }
    }

    fn source_route_state_key(signal: &SourceControlSignal) -> Option<(String, String)> {
        match signal {
            SourceControlSignal::Activate {
                unit, route_key, ..
            }
            | SourceControlSignal::Deactivate {
                unit, route_key, ..
            } => Some((unit.unit_id().to_string(), route_key.clone())),
            _ => None,
        }
    }

    fn source_route_state_signal_matches(
        incoming: &SourceControlSignal,
        retained: &SourceControlSignal,
        allow_forward_generation: bool,
    ) -> bool {
        match (incoming, retained) {
            (
                SourceControlSignal::Activate {
                    unit,
                    route_key,
                    generation,
                    bound_scopes,
                    ..
                },
                SourceControlSignal::Activate {
                    unit: retained_unit,
                    route_key: retained_route_key,
                    generation: retained_generation,
                    bound_scopes: retained_bound_scopes,
                    ..
                },
            ) => {
                unit.unit_id() == retained_unit.unit_id()
                    && route_key == retained_route_key
                    && (*generation == *retained_generation
                        || (allow_forward_generation && *generation >= *retained_generation))
                    && bound_scopes == retained_bound_scopes
            }
            (
                SourceControlSignal::Deactivate {
                    unit,
                    route_key,
                    generation,
                    ..
                },
                SourceControlSignal::Deactivate {
                    unit: retained_unit,
                    route_key: retained_route_key,
                    generation: retained_generation,
                    ..
                },
            ) => {
                unit.unit_id() == retained_unit.unit_id()
                    && route_key == retained_route_key
                    && (*generation == *retained_generation
                        || (allow_forward_generation && *generation >= *retained_generation))
            }
            _ => false,
        }
    }

    fn source_host_grant_change_signal_matches(
        incoming: &SourceControlSignal,
        retained: &SourceControlSignal,
    ) -> bool {
        match (incoming, retained) {
            (
                SourceControlSignal::RuntimeHostGrantChange { changed, .. },
                SourceControlSignal::RuntimeHostGrantChange {
                    changed: retained_changed,
                    ..
                },
            ) => {
                changed.version >= retained_changed.version
                    && changed.grants == retained_changed.grants
            }
            _ => false,
        }
    }

    fn source_host_grant_change_matches_config(
        changed: &RuntimeHostGrantChange,
        config: &crate::source::config::SourceConfig,
    ) -> bool {
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
        struct GrantFingerprint {
            object_ref: String,
            host_ref: String,
            host_ip: String,
            mount_point: String,
            fs_source: String,
            fs_type: String,
            mount_options: Vec<String>,
            interfaces: Vec<String>,
            active: bool,
        }

        let incoming = changed
            .grants
            .iter()
            .filter(|grant| matches!(grant.object_type, RuntimeHostObjectType::MountRoot))
            .map(|grant| GrantFingerprint {
                object_ref: grant.object_ref.clone(),
                host_ref: grant.host.host_ref.clone(),
                host_ip: grant.host.host_ip.clone(),
                mount_point: grant.object.mount_point.clone(),
                fs_source: grant.object.fs_source.clone(),
                fs_type: grant.object.fs_type.clone(),
                mount_options: grant.object.mount_options.clone(),
                interfaces: grant.interfaces.clone(),
                active: matches!(grant.grant_state, RuntimeHostGrantState::Active),
            })
            .collect::<BTreeSet<_>>();
        let expected = config
            .host_object_grants
            .iter()
            .map(|grant| GrantFingerprint {
                object_ref: grant.object_ref.clone(),
                host_ref: grant.host_ref.clone(),
                host_ip: grant.host_ip.clone(),
                mount_point: grant.mount_point.to_string_lossy().into_owned(),
                fs_source: grant.fs_source.clone(),
                fs_type: grant.fs_type.clone(),
                mount_options: grant.mount_options.clone(),
                interfaces: grant.interfaces.clone(),
                active: grant.active,
            })
            .collect::<BTreeSet<_>>();
        !incoming.is_empty() && incoming == expected
    }

    fn source_signal_is_route_semantics_candidate(signal: &SourceControlSignal) -> bool {
        matches!(
            signal,
            SourceControlSignal::Activate { .. }
                | SourceControlSignal::Deactivate { .. }
                | SourceControlSignal::RuntimeHostGrantChange { .. }
        )
    }

    async fn source_signals_match_retained_current_route_semantics(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> bool {
        self.source_signals_match_retained_current_route_semantics_with_generation(
            source_signals,
            false,
        )
        .await
    }

    async fn source_signals_match_retained_current_or_forward_route_semantics(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> bool {
        self.source_signals_match_retained_current_route_semantics_with_generation(
            source_signals,
            true,
        )
        .await
    }

    async fn source_signals_match_retained_current_route_semantics_with_generation(
        &self,
        source_signals: &[SourceControlSignal],
        allow_forward_generation: bool,
    ) -> bool {
        if source_signals.is_empty()
            || !source_signals
                .iter()
                .all(Self::source_signal_is_route_semantics_candidate)
        {
            return false;
        }
        let retained_signals = self.source.control_signals_with_replay(&[]).await;
        let retained_by_route = retained_signals
            .iter()
            .filter_map(|signal| Self::source_route_state_key(signal).map(|key| (key, signal)))
            .collect::<BTreeMap<_, _>>();
        let retained_host_grant_change = retained_signals
            .iter()
            .find(|signal| matches!(signal, SourceControlSignal::RuntimeHostGrantChange { .. }));
        let mut route_state_present = false;
        for signal in source_signals {
            if let Some(key) = Self::source_route_state_key(signal) {
                route_state_present = true;
                if !retained_by_route.get(&key).is_some_and(|retained| {
                    Self::source_route_state_signal_matches(
                        signal,
                        retained,
                        allow_forward_generation,
                    )
                }) {
                    return false;
                }
            } else if matches!(signal, SourceControlSignal::RuntimeHostGrantChange { .. })
                && !retained_host_grant_change.is_some_and(|retained| {
                    Self::source_host_grant_change_signal_matches(signal, retained)
                })
                && !matches!(
                    signal,
                    SourceControlSignal::RuntimeHostGrantChange { changed, .. }
                        if Self::source_host_grant_change_matches_config(changed, &self.config.source)
                )
            {
                return false;
            }
        }
        route_state_present
    }

    async fn source_signals_cover_retained_route_state(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> bool {
        let incoming_route_keys = source_signals
            .iter()
            .filter_map(Self::source_route_state_key)
            .collect::<BTreeSet<_>>();
        if incoming_route_keys.is_empty() {
            return false;
        }
        let retained_route_keys = self
            .source
            .control_signals_with_replay(&[])
            .await
            .iter()
            .filter_map(Self::source_route_state_key)
            .collect::<BTreeSet<_>>();
        !retained_route_keys.is_empty()
            && retained_route_keys
                .iter()
                .all(|route_key| incoming_route_keys.contains(route_key))
    }

    async fn source_signals_match_retained_generation_ticks(
        &self,
        source_signals: &[SourceControlSignal],
        allow_forward_ticks: bool,
    ) -> bool {
        if source_signals.is_empty()
            || !source_signals
                .iter()
                .all(|signal| matches!(signal, SourceControlSignal::Tick { .. }))
        {
            return false;
        }
        let active_by_route = self
            .source
            .control_signals_with_replay(&[])
            .await
            .iter()
            .filter_map(Self::source_active_route_generation)
            .map(|(unit_id, route_key, generation)| ((unit_id, route_key), generation))
            .collect::<std::collections::BTreeMap<_, _>>();
        !active_by_route.is_empty()
            && source_signals.iter().all(|signal| {
                Self::source_tick_route_generation(signal).is_some_and(
                    |(unit_id, route_key, generation)| {
                        active_by_route.get(&(unit_id, route_key)).is_some_and(
                            |active_generation| {
                                if allow_forward_ticks {
                                    generation >= *active_generation
                                } else {
                                    generation == *active_generation
                                }
                            },
                        )
                    },
                )
            })
    }

    async fn source_signals_are_current_retained_generation_ticks(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> bool {
        self.source_signals_match_retained_generation_ticks(source_signals, false)
            .await
    }

    async fn source_signals_are_retained_or_forward_generation_ticks(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> bool {
        self.source_signals_match_retained_generation_ticks(source_signals, true)
            .await
    }

    fn source_signal_is_retained_replay_wakeup(signal: &SourceControlSignal) -> bool {
        matches!(
            signal,
            SourceControlSignal::Tick { .. }
                | SourceControlSignal::ManualRescan { .. }
                | SourceControlSignal::Passthrough(_)
        ) || Self::source_signal_is_drained_retire_cleanup(signal)
    }

    fn source_signals_are_retained_replay_wakeup_only(
        source_signals: &[SourceControlSignal],
    ) -> bool {
        !source_signals.is_empty()
            && source_signals
                .iter()
                .all(Self::source_signal_is_retained_replay_wakeup)
    }

    fn source_signals_are_retained_route_cleanup_only(
        source_signals: &[SourceControlSignal],
    ) -> bool {
        !source_signals.is_empty()
            && source_signals
                .iter()
                .all(|signal| matches!(signal, SourceControlSignal::Deactivate { .. }))
    }

    fn source_transient_followup_signals(
        source_signals: &[SourceControlSignal],
    ) -> Vec<SourceControlSignal> {
        source_signals
            .iter()
            .filter(|signal| {
                matches!(
                    signal,
                    SourceControlSignal::Tick { .. }
                        | SourceControlSignal::ManualRescan { .. }
                        | SourceControlSignal::Passthrough(_)
                )
            })
            .cloned()
            .collect()
    }

    fn sink_signals_are_transient_followup_only(sink_signals: &[SinkControlSignal]) -> bool {
        !sink_signals.is_empty()
            && sink_signals.iter().all(|signal| {
                matches!(
                    signal,
                    SinkControlSignal::Tick { .. } | SinkControlSignal::Passthrough(_)
                )
            })
    }

    fn sink_transient_followup_signals(
        sink_signals: &[SinkControlSignal],
    ) -> Vec<SinkControlSignal> {
        sink_signals
            .iter()
            .filter(|signal| {
                matches!(
                    signal,
                    SinkControlSignal::Tick { .. } | SinkControlSignal::Passthrough(_)
                )
            })
            .cloned()
            .collect()
    }

    fn shared_route_claim_id(unit_id: &str, route_key: &str) -> SharedRouteClaimId {
        (unit_id.to_string(), route_key.to_string())
    }

    fn filter_shared_route_deactivates<T, RouteId>(
        signals: &[T],
        claims: &SharedRouteClaims,
        instance_id: u64,
        deactivate_route_id: RouteId,
    ) -> Vec<T>
    where
        T: Clone,
        RouteId: Fn(&T) -> Option<SharedRouteClaimId>,
    {
        signals
            .iter()
            .filter(|signal| {
                deactivate_route_id(signal).is_none_or(|route_id| {
                    claims
                        .get(&route_id)
                        .is_none_or(|owners| owners.iter().all(|owner| *owner == instance_id))
                })
            })
            .cloned()
            .collect()
    }

    fn apply_shared_route_claim_deltas(
        claims: &mut SharedRouteClaims,
        instance_id: u64,
        deltas: impl IntoIterator<Item = SharedRouteClaimDelta>,
    ) {
        for delta in deltas {
            match delta {
                SharedRouteClaimDelta::Claim(route_id) => {
                    claims.entry(route_id).or_default().insert(instance_id);
                }
                SharedRouteClaimDelta::Release(route_id) => {
                    if let Some(owners) = claims.get_mut(&route_id) {
                        owners.remove(&instance_id);
                        if owners.is_empty() {
                            claims.remove(&route_id);
                        }
                    }
                }
            }
        }
    }

    fn clear_shared_route_claims_for_instance(claims: &mut SharedRouteClaims, instance_id: u64) {
        claims.retain(|_, owners| {
            owners.remove(&instance_id);
            !owners.is_empty()
        });
    }

    fn shared_route_claim_delta_from_source_signal(
        signal: &SourceControlSignal,
    ) -> Option<SharedRouteClaimDelta> {
        match signal {
            SourceControlSignal::Activate {
                unit, route_key, ..
            } => Some(SharedRouteClaimDelta::Claim(Self::shared_route_claim_id(
                unit.unit_id(),
                route_key,
            ))),
            SourceControlSignal::Deactivate {
                unit, route_key, ..
            } => Some(SharedRouteClaimDelta::Release(Self::shared_route_claim_id(
                unit.unit_id(),
                route_key,
            ))),
            SourceControlSignal::Tick { .. }
            | SourceControlSignal::RuntimeHostGrantChange { .. }
            | SourceControlSignal::ManualRescan { .. }
            | SourceControlSignal::Passthrough(_) => None,
        }
    }

    fn shared_route_claim_delta_from_sink_signal(
        signal: &SinkControlSignal,
    ) -> Option<SharedRouteClaimDelta> {
        match signal {
            SinkControlSignal::Activate {
                unit, route_key, ..
            } => Some(SharedRouteClaimDelta::Claim(Self::shared_route_claim_id(
                unit.unit_id(),
                route_key,
            ))),
            SinkControlSignal::Deactivate {
                unit, route_key, ..
            } => Some(SharedRouteClaimDelta::Release(Self::shared_route_claim_id(
                unit.unit_id(),
                route_key,
            ))),
            SinkControlSignal::Tick { .. }
            | SinkControlSignal::RuntimeHostGrantChange { .. }
            | SinkControlSignal::Passthrough(_) => None,
        }
    }

    async fn filter_shared_source_route_deactivates(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> Vec<SourceControlSignal> {
        let claims = self.shared_source_route_claims.lock().await;
        Self::filter_shared_route_deactivates(source_signals, &claims, self.instance_id, |signal| {
            match signal {
                SourceControlSignal::Deactivate {
                    unit, route_key, ..
                } => Some(Self::shared_route_claim_id(unit.unit_id(), route_key)),
                _ => None,
            }
        })
    }

    async fn record_shared_source_route_claims(&self, source_signals: &[SourceControlSignal]) {
        let mut claims = self.shared_source_route_claims.lock().await;
        Self::apply_shared_route_claim_deltas(
            &mut claims,
            self.instance_id,
            source_signals
                .iter()
                .filter_map(Self::shared_route_claim_delta_from_source_signal),
        );
    }

    async fn clear_shared_source_route_claims_for_instance(&self) {
        let mut claims = self.shared_source_route_claims.lock().await;
        Self::clear_shared_route_claims_for_instance(&mut claims, self.instance_id);
    }

    fn apply_source_signal_to_runtime_endpoint_gate(
        &self,
        signal: &SourceControlSignal,
    ) -> Result<()> {
        match signal {
            SourceControlSignal::Activate {
                unit,
                route_key,
                generation,
                bound_scopes,
                ..
            } => {
                let accepted = self.facade_gate.apply_activate(
                    unit.unit_id(),
                    route_key,
                    *generation,
                    bound_scopes,
                )?;
                if accepted
                    && unit.unit_id() == execution_units::SOURCE_RUNTIME_UNIT_ID
                    && route_key == &source_rescan_request_route_for(&self.node_id.0).0
                {
                    let current_generation =
                        source_rescan_route_semantic_generation(&self.facade_gate, route_key);
                    if current_generation == 0
                        || self
                            .source_rescan_proxy_ready_generation
                            .load(Ordering::Acquire)
                            != current_generation
                    {
                        self.source_rescan_proxy_ready_generation
                            .store(0, Ordering::Release);
                    }
                }
            }
            SourceControlSignal::Deactivate {
                unit,
                route_key,
                generation,
                ..
            } => {
                let accepted =
                    self.facade_gate
                        .apply_deactivate(unit.unit_id(), route_key, *generation)?;
                if accepted
                    && unit.unit_id() == execution_units::SOURCE_RUNTIME_UNIT_ID
                    && route_key == &source_rescan_request_route_for(&self.node_id.0).0
                {
                    self.source_rescan_proxy_ready_generation
                        .store(0, Ordering::Release);
                }
            }
            SourceControlSignal::Tick {
                unit,
                route_key,
                generation,
                ..
            } => {
                self.facade_gate
                    .accept_tick(unit.unit_id(), route_key, *generation)?;
            }
            SourceControlSignal::RuntimeHostGrantChange { .. }
            | SourceControlSignal::ManualRescan { .. }
            | SourceControlSignal::Passthrough(_) => {}
        }
        Ok(())
    }

    fn apply_source_signals_to_runtime_endpoint_gate(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> Result<()> {
        for signal in source_signals {
            self.apply_source_signal_to_runtime_endpoint_gate(signal)?;
        }
        Ok(())
    }

    async fn source_generation_cutover_replay_should_defer_inline(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> bool {
        if !self.runtime_control_state().source_state_replay_required() {
            return false;
        }
        if self.control_initialized() {
            return false;
        }
        if self
            .source_signals_are_current_retained_generation_ticks(source_signals)
            .await
        {
            return false;
        }
        if !source_signals.is_empty() {
            return false;
        }
        self.source_signals_with_replay(&[])
            .await
            .iter()
            .any(Self::source_signal_is_route_state)
    }

    async fn source_generation_cutover_apply_should_defer_inline(
        &self,
        source_signals: &[SourceControlSignal],
        generation_cutover_disposition: SourceGenerationCutoverDisposition,
        allow_forward_route_semantics_without_worker: bool,
        sink_signals_present: bool,
        facade_signals_present: bool,
    ) -> bool {
        if !self.runtime_control_state().source_state_replay_required() {
            let retained_route_semantics = if allow_forward_route_semantics_without_worker {
                self.source_signals_match_retained_current_or_forward_route_semantics(
                    source_signals,
                )
                .await
            } else {
                self.source_signals_match_retained_current_route_semantics(source_signals)
                    .await
            };
            if retained_route_semantics {
                return false;
            }
        }
        if self.runtime_control_state().source_state_replay_required()
            && self
                .source_generation_cutover_replay_deferred
                .load(Ordering::Acquire)
            && Self::source_signals_are_post_initial_route_cutover(source_signals)
            && Self::source_signals_are_single_route_state_cutover(source_signals)
            && matches!(
                generation_cutover_disposition,
                SourceGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
                    | SourceGenerationCutoverDisposition::FailClosedColdSuccessorCandidateOnRetryableReset
            )
        {
            return true;
        }
        if !self.control_initialized()
            && !self.runtime_control_state().source_state_replay_required()
            && sink_signals_present
            && !facade_signals_present
            && Self::source_signals_are_post_initial_route_cutover(source_signals)
        {
            return false;
        }
        if allow_forward_route_semantics_without_worker
            && !self.runtime_control_state().source_state_replay_required()
            && !self.control_initialized()
            && self
                .source_signals_match_retained_current_or_forward_route_semantics(source_signals)
                .await
        {
            return false;
        }
        if matches!(
            generation_cutover_disposition,
            SourceGenerationCutoverDisposition::FailClosedRestartDeferredRetirePending
        ) {
            return !source_signals.is_empty()
                && source_signals
                    .iter()
                    .all(Self::source_signal_is_drained_retire_cleanup);
        }
        if matches!(
            generation_cutover_disposition,
            SourceGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
        ) && Self::source_signals_are_post_initial_route_cutover(source_signals)
            && ((self.runtime_control_state().source_state_replay_required()
                && !self
                    .source_generation_cutover_replay_deferred
                    .load(Ordering::Acquire)
                && (sink_signals_present || facade_signals_present))
                || (self
                    .source_signals_cover_retained_route_state(source_signals)
                    .await
                    && (sink_signals_present
                        || Self::source_signals_include_source_control_route(source_signals))
                    && !self.runtime_control_state().sink_state_replay_required()
                    && !facade_signals_present
                    && (!sink_signals_present
                        || (Self::source_signals_are_late_post_initial_route_cutover(
                            source_signals,
                        ) && Self::source_signals_are_single_route_state_cutover(
                            source_signals,
                        )))))
        {
            return true;
        }
        if self.runtime_control_state().source_state_replay_required()
            && Self::source_signals_are_retained_route_cleanup_only(source_signals)
        {
            return false;
        }
        self.source_generation_cutover_replay_should_defer_inline(source_signals)
            .await
    }

    async fn defer_source_generation_cutover_replay_inline(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> std::result::Result<(), RuntimeWorkerControlApplyFailure> {
        let filtered_source_signals = self
            .filter_shared_source_route_deactivates(source_signals)
            .await;
        self.source
            .record_retained_control_signals(&filtered_source_signals)
            .await;
        self.source.arm_retained_control_replay().await;
        self.record_shared_source_route_claims(&filtered_source_signals)
            .await;
        self.apply_source_signals_to_runtime_endpoint_gate(&filtered_source_signals)
            .map_err(RuntimeWorkerControlApplyFailure::from)?;
        self.refresh_source_rescan_proxy_ready_from_retained_endpoint()
            .await;
        Ok(())
    }

    async fn apply_restart_deferred_events_cleanup_locally(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        source_signals: &[SourceControlSignal],
        sink_signals: &[SinkControlSignal],
    ) -> std::result::Result<(), RuntimeWorkerControlApplyFailure> {
        let filtered_source_signals = self
            .filter_shared_source_route_deactivates(source_signals)
            .await;
        self.source
            .record_retained_control_signals(&filtered_source_signals)
            .await;
        self.record_shared_source_route_claims(&filtered_source_signals)
            .await;
        self.apply_source_signals_to_runtime_endpoint_gate(&filtered_source_signals)
            .map_err(RuntimeWorkerControlApplyFailure::from)?;
        self.refresh_source_rescan_proxy_ready_from_retained_endpoint()
            .await;

        let filtered_sink_signals = self
            .filter_sink_facade_dependent_route_activates_during_pending_fixed_bind_claim_with_session(
                &self
                    .filter_shared_sink_route_deactivates(
                        &self.filter_stale_sink_ticks(sink_signals).await,
                    )
                    .await,
                fixed_bind_session,
            )
            .await;
        self.record_retained_sink_control_state(&filtered_sink_signals)
            .await;
        self.record_shared_sink_route_claims(&filtered_sink_signals)
            .await;
        self.apply_sink_signals_to_runtime_endpoint_gate(&filtered_sink_signals)
            .map_err(RuntimeWorkerControlApplyFailure::from)?;
        Ok(())
    }

    async fn apply_current_source_route_semantics_without_worker(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> std::result::Result<bool, RuntimeWorkerControlApplyFailure> {
        let filtered_source_signals = self
            .filter_shared_source_route_deactivates(source_signals)
            .await;
        if self.runtime_control_state().source_state_replay_required() {
            return Ok(false);
        }
        let retained_current = self
            .source_signals_match_retained_current_route_semantics(&filtered_source_signals)
            .await;
        let retained_forward_after_repair = !retained_current
            && !self.control_initialized()
            && !self.runtime_control_state().sink_state_replay_required()
            && self
                .source_signals_match_retained_current_or_forward_route_semantics(
                    &filtered_source_signals,
                )
                .await;
        if !retained_current && !retained_forward_after_repair {
            return Ok(false);
        }
        self.record_retained_source_control_state(&filtered_source_signals)
            .await;
        self.record_shared_source_route_claims(&filtered_source_signals)
            .await;
        self.apply_source_signals_to_runtime_endpoint_gate(&filtered_source_signals)
            .map_err(RuntimeWorkerControlApplyFailure::from)?;
        self.refresh_source_rescan_proxy_ready_from_retained_endpoint()
            .await;
        Ok(true)
    }

    fn apply_source_signals_with_recovery<'a>(
        &'a self,
        fixed_bind_session: &'a FixedBindLifecycleSession,
        source_signals: &'a [SourceControlSignal],
        control_initialized_at_entry: bool,
        generation_cutover_disposition: SourceGenerationCutoverDisposition,
        fail_closed_generation_cutover_requires_sink_replay: bool,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = std::result::Result<(), RuntimeWorkerControlApplyFailure>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let filtered_source_signals = self
                .filter_shared_source_route_deactivates(source_signals)
                .await;
            if matches!(
                generation_cutover_disposition,
                SourceGenerationCutoverDisposition::None
            ) && !self.runtime_control_state().source_state_replay_required()
                && self
                    .source_signals_match_retained_current_route_semantics(&filtered_source_signals)
                    .await
            {
                self.record_retained_source_control_state(&filtered_source_signals)
                    .await;
                self.record_shared_source_route_claims(&filtered_source_signals)
                    .await;
                self.apply_source_signals_to_runtime_endpoint_gate(&filtered_source_signals)
                    .map_err(RuntimeWorkerControlApplyFailure::from)?;
                self.refresh_source_rescan_proxy_ready_from_retained_endpoint()
                    .await;
                return Ok(());
            }
            if control_initialized_at_entry
                && self.control_initialized()
                && !self.runtime_control_state().source_state_replay_required()
                && !filtered_source_signals.is_empty()
                && filtered_source_signals
                    .iter()
                    .all(|signal| matches!(signal, SourceControlSignal::Tick { .. }))
            {
                return Ok(());
            }
            if !control_initialized_at_entry
                && !self.control_initialized()
                && !self.runtime_control_state().source_state_replay_required()
                && self.runtime_control_state().sink_state_replay_required()
                && self
                    .source_signals_are_retained_or_forward_generation_ticks(
                        &filtered_source_signals,
                    )
                    .await
            {
                self.source
                    .record_retained_control_signals(&filtered_source_signals)
                    .await;
                self.record_shared_source_route_claims(&filtered_source_signals)
                    .await;
                self.apply_source_signals_to_runtime_endpoint_gate(&filtered_source_signals)
                    .map_err(RuntimeWorkerControlApplyFailure::from)?;
                return Ok(());
            }
            let fail_closed_restart_deferred_retire_pending = matches!(
                generation_cutover_disposition,
                SourceGenerationCutoverDisposition::FailClosedRestartDeferredRetirePending
            ) && !source_signals.is_empty()
                && source_signals
                    .iter()
                    .all(Self::source_signal_is_drained_retire_cleanup);
            if fail_closed_restart_deferred_retire_pending {
                let preserve_retained_source_replay =
                    (self.runtime_control_state().source_state_replay_required()
                        || fail_closed_generation_cutover_requires_sink_replay)
                        && self.source_retained_replay_has_active_route_state().await;
                if !preserve_retained_source_replay {
                    self.source
                        .record_retained_control_signals(source_signals)
                        .await;
                }
                self.record_shared_source_route_claims(source_signals).await;
                self.apply_source_signals_to_runtime_endpoint_gate(source_signals)
                    .map_err(RuntimeWorkerControlApplyFailure::from)?;
                if preserve_retained_source_replay {
                    self.mark_control_uninitialized_after_deferred_source_replay_without_scheduled_repair_with_session(
                        fixed_bind_session,
                        fail_closed_generation_cutover_requires_sink_replay,
                    )
                    .await;
                } else {
                    self.mark_control_uninitialized_after_generation_cutover_cleanup_with_session(
                        fixed_bind_session,
                        false,
                    )
                    .await;
                    self.update_runtime_control_state(|state| state.clear_source_replay());
                }
                eprintln!(
                    "fs_meta_runtime_app: source control deferred restart_deferred_retire_pending cleanup at app boundary"
                );
                return Ok(());
            }
            let source_replay_wakeup_is_current_retained_generation_tick = self
                .source_signals_are_current_retained_generation_ticks(&filtered_source_signals)
                .await;
            if !self.control_initialized()
                && self.runtime_control_state().source_state_replay_required()
                && self
                    .source_generation_cutover_replay_deferred
                    .load(Ordering::Acquire)
                && Self::source_signals_are_retained_replay_wakeup_only(&filtered_source_signals)
            {
                self.source
                    .record_retained_control_signals(&filtered_source_signals)
                    .await;
                self.record_shared_source_route_claims(&filtered_source_signals)
                    .await;
                self.apply_source_signals_to_runtime_endpoint_gate(&filtered_source_signals)
                    .map_err(RuntimeWorkerControlApplyFailure::from)?;
                eprintln!(
                    "fs_meta_runtime_app: source control kept deferred generation-cutover wakeup local signals={} reason=retained_source_replay_pending",
                    filtered_source_signals.len()
                );
                return Ok(());
            }
            if !self.control_initialized()
                && self.runtime_control_state().source_state_replay_required()
                && Self::source_signals_are_retained_replay_wakeup_only(&filtered_source_signals)
                && !source_replay_wakeup_is_current_retained_generation_tick
                && filtered_source_signals
                    .iter()
                    .all(|signal| matches!(signal, SourceControlSignal::Tick { .. }))
            {
                self.source
                    .record_retained_control_signals(&filtered_source_signals)
                    .await;
                self.record_shared_source_route_claims(&filtered_source_signals)
                    .await;
                self.apply_source_signals_to_runtime_endpoint_gate(&filtered_source_signals)
                    .map_err(RuntimeWorkerControlApplyFailure::from)?;
                eprintln!(
                    "fs_meta_runtime_app: source control deferred retained-replay tick wakeup while runtime uninitialized signals={}",
                    filtered_source_signals.len()
                );
                return Ok(());
            }
            if self.runtime_control_state().source_state_replay_required()
                && Self::source_signals_are_retained_replay_wakeup_only(&filtered_source_signals)
                && !source_replay_wakeup_is_current_retained_generation_tick
            {
                self.source
                    .record_retained_control_signals(&filtered_source_signals)
                    .await;
                self.record_shared_source_route_claims(&filtered_source_signals)
                    .await;
                self.apply_source_signals_to_runtime_endpoint_gate(&filtered_source_signals)
                    .map_err(RuntimeWorkerControlApplyFailure::from)?;
                eprintln!(
                    "fs_meta_runtime_app: source control deferred retained-replay wakeup while retained replay pending signals={} reason=retained_source_replay_pending",
                    filtered_source_signals.len()
                );
                return Ok(());
            }
            if self.runtime_control_state().source_state_replay_required()
                && Self::source_signals_are_retained_route_cleanup_only(&filtered_source_signals)
            {
                self.source
                    .record_retained_control_signals(&filtered_source_signals)
                    .await;
                self.record_shared_source_route_claims(&filtered_source_signals)
                    .await;
                self.apply_source_signals_to_runtime_endpoint_gate(&filtered_source_signals)
                    .map_err(RuntimeWorkerControlApplyFailure::from)?;
                eprintln!(
                    "fs_meta_runtime_app: source control deferred retained route cleanup while retained replay pending signals={} reason=retained_source_replay_pending",
                    filtered_source_signals.len()
                );
                return Ok(());
            }
            let defer_retained_state_until_success = fail_closed_restart_deferred_retire_pending;
            let fail_closed_retained_replay_on_retryable_reset = !defer_retained_state_until_success
            && !filtered_source_signals.is_empty()
            && (matches!(
                generation_cutover_disposition,
                SourceGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
                    | SourceGenerationCutoverDisposition::FailClosedColdSuccessorCandidateOnRetryableReset
            ) || (!control_initialized_at_entry
                && filtered_source_signals
                    .iter()
                    .any(Self::source_signal_is_post_initial_activate)));
            let fail_closed_generation_cutover_source_error = !defer_retained_state_until_success
            && !filtered_source_signals.is_empty()
            && matches!(
                generation_cutover_disposition,
                SourceGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
                    | SourceGenerationCutoverDisposition::FailClosedColdSuccessorCandidateOnRetryableReset
            );
            if !defer_retained_state_until_success {
                self.source
                    .record_retained_control_signals(&filtered_source_signals)
                    .await;
            }
            let replay_followup_signals = if self
                .runtime_control_state()
                .source_state_replay_required()
                && Self::source_signals_are_transient_followup_only(&filtered_source_signals)
            {
                let followups = Self::source_transient_followup_signals(&filtered_source_signals);
                (!followups.is_empty()).then_some(followups)
            } else {
                None
            };
            let replay_followup_pending = replay_followup_signals.clone();
            let deadline = tokio::time::Instant::now() + SOURCE_CONTROL_RECOVERY_TOTAL_TIMEOUT;
            let mut retryable_reset_recovery_attempts = 0usize;
            loop {
                let replaying_retained_state_only =
                    self.runtime_control_state().source_state_replay_required()
                        && replay_followup_pending.is_some();
                let effective_source_signals = if replaying_retained_state_only {
                    self.source_signals_with_replay(&[]).await
                } else if let Some(followups) = replay_followup_pending.clone() {
                    followups
                } else {
                    self.source_signals_with_replay(&filtered_source_signals)
                        .await
                };
                if effective_source_signals.is_empty() {
                    self.record_shared_source_route_claims(&effective_source_signals)
                        .await;
                    self.update_runtime_control_state(|state| state.clear_source_replay());
                    if replaying_retained_state_only && replay_followup_pending.is_some() {
                        continue;
                    }
                    return Ok(());
                }
                self.record_shared_source_route_claims(&effective_source_signals)
                    .await;
                self.apply_source_signals_to_runtime_endpoint_gate(&effective_source_signals)
                    .map_err(RuntimeWorkerControlApplyFailure::from)?;
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                let retained_generation_cutover_replay_wakeup = filtered_source_signals.is_empty()
                    || Self::source_signals_are_retained_replay_wakeup_only(
                        &filtered_source_signals,
                    );
                let retained_generation_cutover_replay_fail_closed =
                    self.runtime_control_state().source_state_replay_required()
                        && retained_generation_cutover_replay_wakeup
                        && effective_source_signals
                            .iter()
                            .any(Self::source_signal_is_route_state);
                #[cfg(test)]
                let source_apply_result = if let Some(err) = take_source_apply_error_queue_hook() {
                    Err(SourceFailure::from(err))
                } else {
                    self.source
                        .apply_orchestration_signals_with_total_timeout_with_failure(
                            &effective_source_signals,
                            remaining,
                        )
                        .await
                };
                #[cfg(not(test))]
                let source_apply_result = self
                    .source
                    .apply_orchestration_signals_with_total_timeout_with_failure(
                        &effective_source_signals,
                        remaining,
                    )
                    .await;
                match source_apply_result {
                Ok(()) => {
                    if defer_retained_state_until_success {
                        self.source
                            .record_retained_control_signals(&filtered_source_signals)
                            .await;
                    }
                    self.record_shared_source_route_claims(&effective_source_signals)
                        .await;
                    self.update_runtime_control_state(|state| state.clear_source_replay());
                    self.source_fail_closed_reinitialize_required
                        .store(false, Ordering::Release);
                    self.source_generation_cutover_replay_deferred
                        .store(false, Ordering::Release);
                    if replaying_retained_state_only {
                        continue;
                    }
                    return Ok(());
                }
                Err(err)
                    if fail_closed_restart_deferred_retire_pending
                        && !is_stale_drained_fenced_grant_attachment_retry(err.as_error()) =>
                {
                    eprintln!(
                        "fs_meta_runtime_app: source control fail-closed restart_deferred_retire_pending err={}",
                        err.as_error()
                    );
                    self.source
                        .record_retained_control_signals(&filtered_source_signals)
                        .await;
                    let _ = self
                        .source
                        .reconnect_shared_worker_client_with_failure()
                        .await;
                    self.mark_control_uninitialized_after_failure_preserving_internal_status_with_session(
                        fixed_bind_session,
                    )
                    .await;
                    return Ok(());
                }
                Err(err)
                    if (fail_closed_retained_replay_on_retryable_reset
                        || retained_generation_cutover_replay_fail_closed)
                        && is_retryable_worker_control_reset(err.as_error()) =>
                {
                    eprintln!(
                        "fs_meta_runtime_app: source control fail-closed retained release cutover err={}",
                        err.as_error()
                    );
                    let _ = self
                        .source
                        .reconnect_shared_worker_client_with_failure()
                        .await;
                    self.source_generation_cutover_replay_deferred
                        .store(true, Ordering::Release);
                    self.mark_control_uninitialized_after_deferred_source_replay_with_session(
                        fixed_bind_session,
                        fail_closed_generation_cutover_requires_sink_replay,
                    )
                        .await;
                    return Ok(());
                }
                Err(err)
                    if fail_closed_generation_cutover_source_error
                        && control_initialized_at_entry
                        && fail_closed_generation_cutover_requires_sink_replay
                        && matches!(
                            generation_cutover_disposition,
                            SourceGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
                        ) =>
                {
                    eprintln!(
                        "fs_meta_runtime_app: source control fail-closed generation cutover err={}",
                        err.as_error()
                    );
                    let _ = self
                        .source
                        .reconnect_shared_worker_client_with_failure()
                        .await;
                    self.mark_control_uninitialized_after_deferred_source_replay_with_session(
                        fixed_bind_session,
                        fail_closed_generation_cutover_requires_sink_replay,
                    )
                    .await;
                    return Ok(());
                }
                Err(err)
                    if control_initialized_at_entry
                        && is_source_post_ack_scheduled_groups_refresh_exhaustion(err.as_error())
                        && tokio::time::Instant::now() < deadline =>
                {
                    eprintln!(
                        "fs_meta_runtime_app: source control recovery after post-ack refresh exhaustion err={}",
                        err.as_error()
                    );
                    let _ = self
                        .source
                        .reconnect_shared_worker_client_with_failure()
                        .await;
                    let source_recovery_result = self
                        .reinitialize_after_control_reset_with_deadline_and_session_with_failure(
                            fixed_bind_session,
                            deadline,
                        )
                        .await;
                    match source_recovery_result {
                        Ok(()) => {}
                        Err(recovery_err)
                            if should_fail_closed_source_control_recovery_exhaustion(
                                recovery_err.as_error(),
                            ) =>
                        {
                            eprintln!(
                                "fs_meta_runtime_app: source control recovery failed after post-ack refresh exhaustion err={}",
                                recovery_err.as_error()
                            );
                            self.mark_control_uninitialized_after_failure_with_session(
                                fixed_bind_session,
                            )
                            .await;
                            return Err(RuntimeWorkerControlApplyFailure::from(recovery_err));
                        }
                        Err(recovery_err) => {
                            return Err(RuntimeWorkerControlApplyFailure::from(recovery_err));
                        }
                    }
                }
                Err(err)
                    if is_retryable_worker_control_reset(err.as_error())
                        && tokio::time::Instant::now() < deadline =>
                {
                    retryable_reset_recovery_attempts =
                        retryable_reset_recovery_attempts.saturating_add(1);
                    if retryable_reset_recovery_attempts
                        > SOURCE_CONTROL_RECOVERY_MAX_RETRYABLE_RESETS
                    {
                        eprintln!(
                            "fs_meta_runtime_app: source control recovery retry budget exhausted attempts={} err={}",
                            retryable_reset_recovery_attempts,
                            err.as_error()
                        );
                        self.mark_control_uninitialized_after_failure_with_session(
                            fixed_bind_session,
                        )
                        .await;
                        return Err(RuntimeWorkerControlApplyFailure::from(CnxError::Timeout));
                    }
                    eprintln!(
                        "fs_meta_runtime_app: source control replay after retryable reset err={}",
                        err.as_error()
                    );
                    let _ = self
                        .source
                        .reconnect_shared_worker_client_with_failure()
                        .await;
                    #[cfg(test)]
                    let source_recovery_result = if let Some(err) =
                        take_source_control_recovery_error_queue_hook()
                    {
                        Err(RuntimeInitializeFailure::from(err))
                    } else {
                        self.reinitialize_after_control_reset_with_deadline_and_session_with_failure(
                                fixed_bind_session,
                                deadline,
                            )
                            .await
                    };
                    #[cfg(not(test))]
                    let source_recovery_result = self
                        .reinitialize_after_control_reset_with_deadline_and_session_with_failure(
                            fixed_bind_session,
                            deadline,
                        )
                        .await;
                    match source_recovery_result {
                        Ok(()) => {}
                        Err(recovery_err)
                            if should_fail_closed_source_control_recovery_exhaustion(
                                recovery_err.as_error(),
                            ) =>
                        {
                            eprintln!(
                                "fs_meta_runtime_app: source control fail-closed after worker recovery exhausted err={}",
                                recovery_err.as_error()
                            );
                            self.mark_control_uninitialized_after_failure_with_session(
                                fixed_bind_session,
                            )
                            .await;
                            return Ok(());
                        }
                        Err(recovery_err) => {
                            return Err(RuntimeWorkerControlApplyFailure::from(recovery_err));
                        }
                    }
                }
                Err(err)
                    if should_fail_closed_source_control_recovery_exhaustion(err.as_error()) =>
                {
                    eprintln!(
                        "fs_meta_runtime_app: source control fail-closed after worker recovery exhausted err={}",
                        err.as_error()
                    );
                    let _ = self
                        .source
                        .reconnect_shared_worker_client_with_failure()
                        .await;
                    self.mark_control_uninitialized_after_failure_with_session(fixed_bind_session)
                        .await;
                    return Ok(());
                }
                Err(err) => {
                    let _ = self
                        .source
                        .reconnect_shared_worker_client_with_failure()
                        .await;
                    self.source.arm_retained_control_replay().await;
                    self.source_fail_closed_reinitialize_required
                        .store(true, Ordering::Release);
                    self.mark_control_uninitialized_after_failure_with_session(fixed_bind_session)
                        .await;
                    return Err(RuntimeWorkerControlApplyFailure::from(err));
                }
            }
            }
        })
    }

    async fn sink_signals_with_replay(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> Vec<SinkControlSignal> {
        let tick_only = !sink_signals.is_empty()
            && sink_signals
                .iter()
                .all(|signal| matches!(signal, SinkControlSignal::Tick { .. }));
        let replay_retained = self.runtime_control_state().sink_state_replay_required()
            || (tick_only && self.sink.retained_replay_required());
        if !replay_retained {
            return sink_signals.to_vec();
        }

        let mut desired = self.retained_sink_control_state.lock().await.clone();
        Self::apply_sink_signals_to_state(&mut desired, sink_signals);
        let replay_generation = sink_signals.iter().find_map(|signal| match signal {
            SinkControlSignal::Tick { generation, .. } => Some(*generation),
            _ => None,
        });
        let mut replayed = Self::sink_signals_from_state(&desired);
        if let Some(generation) = replay_generation {
            replayed = replayed
                .iter()
                .map(|signal| Self::rebase_sink_signal_generation(signal, generation))
                .collect();
        }
        replayed.extend(
            sink_signals
                .iter()
                .filter(|signal| matches!(signal, SinkControlSignal::Tick { .. }))
                .cloned(),
        );
        replayed
    }

    async fn record_retained_sink_control_state(&self, sink_signals: &[SinkControlSignal]) {
        let mut retained = self.retained_sink_control_state.lock().await;
        Self::apply_sink_signals_to_state(&mut retained, sink_signals);
    }

    async fn sink_generation_cutover_replay_should_defer_inline(&self) -> bool {
        if !self.runtime_control_state().sink_state_replay_required() {
            return false;
        }
        let retained = self.retained_sink_control_state.lock().await;
        retained.latest_host_grant_change.is_some() || !retained.active_by_route.is_empty()
    }

    fn apply_sink_signals_to_state(
        state: &mut RetainedSinkControlState,
        sink_signals: &[SinkControlSignal],
    ) {
        for signal in sink_signals {
            match signal {
                SinkControlSignal::Activate {
                    unit, route_key, ..
                } => {
                    state.active_by_route.insert(
                        (unit.unit_id().to_string(), route_key.clone()),
                        signal.clone(),
                    );
                }
                SinkControlSignal::Deactivate {
                    unit, route_key, ..
                } => {
                    state.active_by_route.insert(
                        (unit.unit_id().to_string(), route_key.clone()),
                        signal.clone(),
                    );
                }
                SinkControlSignal::RuntimeHostGrantChange { .. } => {
                    state.latest_host_grant_change = Some(signal.clone());
                }
                SinkControlSignal::Tick { .. } | SinkControlSignal::Passthrough(_) => {}
            }
        }
    }

    fn sink_signals_from_state(state: &RetainedSinkControlState) -> Vec<SinkControlSignal> {
        let mut merged = Vec::new();
        if let Some(changed) = state.latest_host_grant_change.clone() {
            merged.push(changed);
        }
        merged.extend(state.active_by_route.values().cloned());
        merged
    }

    fn rebase_sink_signal_generation(
        signal: &SinkControlSignal,
        generation: u64,
    ) -> SinkControlSignal {
        match signal {
            SinkControlSignal::Activate {
                unit,
                route_key,
                bound_scopes,
                ..
            } => SinkControlSignal::Activate {
                unit: *unit,
                route_key: route_key.clone(),
                generation,
                bound_scopes: bound_scopes.clone(),
                envelope: encode_runtime_exec_control(&RuntimeExecControl::Activate(
                    RuntimeExecActivate {
                        route_key: route_key.clone(),
                        unit_id: unit.unit_id().to_string(),
                        lease: None,
                        generation,
                        expires_at_ms: 1,
                        bound_scopes: bound_scopes.clone(),
                    },
                ))
                .expect("encode rebased sink activate"),
            },
            SinkControlSignal::Deactivate {
                unit,
                route_key,
                envelope,
                ..
            } => {
                let mut reason = "restart_deferred_retire_pending".to_string();
                let mut lease = None;
                if let Ok(Some(RuntimeExecControl::Deactivate(decoded))) =
                    decode_runtime_exec_control(envelope)
                {
                    reason = decoded.reason;
                    lease = decoded.lease;
                }
                SinkControlSignal::Deactivate {
                    unit: *unit,
                    route_key: route_key.clone(),
                    generation,
                    envelope: encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: route_key.clone(),
                            unit_id: unit.unit_id().to_string(),
                            lease,
                            generation,
                            reason,
                        },
                    ))
                    .expect("encode rebased sink deactivate"),
                }
            }
            SinkControlSignal::RuntimeHostGrantChange { .. }
            | SinkControlSignal::Tick { .. }
            | SinkControlSignal::Passthrough(_) => signal.clone(),
        }
    }

    fn sink_signal_is_cleanup_only_query_request_route(signal: &SinkControlSignal) -> bool {
        matches!(
            signal,
            SinkControlSignal::Deactivate { route_key, .. }
                if is_per_peer_sink_query_request_route(route_key)
                    || route_key == &format!("{}.req", ROUTE_KEY_QUERY)
                    || route_key == &format!("{}.req", ROUTE_KEY_SINK_QUERY_INTERNAL)
                    || route_key == &format!("{}.req", ROUTE_KEY_FORCE_FIND)
                    || route_key == &format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL)
                    || is_per_peer_sink_roots_control_stream_route(route_key)
        )
    }

    async fn filter_stale_sink_ticks(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> Vec<SinkControlSignal> {
        let retained = self.retained_sink_control_state.lock().await.clone();
        sink_signals
            .iter()
            .filter(|signal| match signal {
                SinkControlSignal::Tick {
                    unit,
                    route_key,
                    generation,
                    ..
                } => Self::sink_tick_matches_retained_active_generation(
                    &retained,
                    *unit,
                    route_key,
                    *generation,
                ),
                _ => true,
            })
            .cloned()
            .collect()
    }

    fn sink_tick_matches_retained_active_generation(
        retained: &RetainedSinkControlState,
        unit: crate::runtime::orchestration::SinkRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> bool {
        matches!(
            retained
                .active_by_route
                .get(&(unit.unit_id().to_string(), route_key.to_string())),
            Some(SinkControlSignal::Activate {
                generation: retained_generation,
                ..
            }) if generation >= *retained_generation
        )
    }

    async fn filter_shared_sink_route_deactivates(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> Vec<SinkControlSignal> {
        let claims = self.shared_sink_route_claims.lock().await;
        Self::filter_shared_route_deactivates(sink_signals, &claims, self.instance_id, |signal| {
            if Self::sink_signal_is_restart_deferred_retire_pending(signal) {
                return None;
            }
            match signal {
                SinkControlSignal::Deactivate {
                    unit, route_key, ..
                } => Some(Self::shared_route_claim_id(unit.unit_id(), route_key)),
                _ => None,
            }
        })
    }

    async fn record_shared_sink_route_claims(&self, sink_signals: &[SinkControlSignal]) {
        let mut claims = self.shared_sink_route_claims.lock().await;
        Self::apply_shared_route_claim_deltas(
            &mut claims,
            self.instance_id,
            sink_signals
                .iter()
                .filter_map(Self::shared_route_claim_delta_from_sink_signal),
        );
    }

    async fn clear_shared_sink_route_claims_for_instance(&self) {
        let mut claims = self.shared_sink_route_claims.lock().await;
        Self::clear_shared_route_claims_for_instance(&mut claims, self.instance_id);
    }

    fn apply_sink_signal_to_runtime_endpoint_gate(&self, signal: &SinkControlSignal) -> Result<()> {
        match signal {
            SinkControlSignal::Activate {
                unit,
                route_key,
                generation,
                bound_scopes,
                ..
            } => {
                self.facade_gate.apply_activate(
                    unit.unit_id(),
                    route_key,
                    *generation,
                    bound_scopes,
                )?;
            }
            SinkControlSignal::Deactivate {
                unit,
                route_key,
                generation,
                ..
            } => {
                self.facade_gate
                    .apply_deactivate(unit.unit_id(), route_key, *generation)?;
            }
            SinkControlSignal::Tick {
                unit,
                route_key,
                generation,
                ..
            } => {
                self.facade_gate
                    .accept_tick(unit.unit_id(), route_key, *generation)?;
            }
            SinkControlSignal::RuntimeHostGrantChange { .. }
            | SinkControlSignal::Passthrough(_) => {}
        }
        Ok(())
    }

    fn apply_sink_signals_to_runtime_endpoint_gate(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> Result<()> {
        for signal in sink_signals {
            self.apply_sink_signal_to_runtime_endpoint_gate(signal)?;
        }
        Ok(())
    }

    fn sink_signal_is_post_initial_route_state(signal: &SinkControlSignal) -> bool {
        matches!(
            signal,
            SinkControlSignal::Activate { generation, .. }
                | SinkControlSignal::Deactivate { generation, .. } if *generation > 1
        )
    }

    fn sink_signal_is_post_initial_activate(signal: &SinkControlSignal) -> bool {
        matches!(
            signal,
            SinkControlSignal::Activate { generation, .. } if *generation > 1
        )
    }

    fn sink_signals_are_single_route_state_cutover(sink_signals: &[SinkControlSignal]) -> bool {
        sink_signals
            .iter()
            .filter(|signal| {
                matches!(
                    signal,
                    SinkControlSignal::Activate { .. } | SinkControlSignal::Deactivate { .. }
                )
            })
            .count()
            == 1
    }

    async fn sink_generation_cutover_inline_action(
        &self,
        sink_signals: &[SinkControlSignal],
        generation_cutover_disposition: SinkGenerationCutoverDisposition,
    ) -> SinkGenerationCutoverInlineAction {
        let has_post_initial_single_route_activate = sink_signals
            .iter()
            .any(Self::sink_signal_is_post_initial_activate)
            && Self::sink_signals_are_single_route_state_cutover(sink_signals);
        let can_defer_replay = matches!(
            generation_cutover_disposition,
            SinkGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
        ) && has_post_initial_single_route_activate;
        let initial_materialization_catchup = can_defer_replay
            && !self.runtime_control_state().source_state_replay_required()
            && self
                .sink_status_and_source_audit_show_initial_materialization_catchup()
                .await;
        sink_generation_cutover_inline_action_from_evidence(
            generation_cutover_disposition,
            has_post_initial_single_route_activate,
            self.runtime_control_state().source_state_replay_required(),
            self.sink_generation_cutover_replay_deferred
                .load(Ordering::Acquire),
            initial_materialization_catchup,
        )
    }

    async fn sink_status_and_source_audit_show_initial_materialization_catchup(&self) -> bool {
        let Ok(expected_groups) = Self::runtime_scoped_facade_group_ids(&self.source, &self.sink)
            .await
            .map(|groups| {
                groups
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>()
            })
        else {
            return false;
        };
        if expected_groups.is_empty() {
            return false;
        }
        let sink_has_materialized_pending_progress = self
            .sink
            .cached_status_snapshot_with_failure()
            .ok()
            .is_some_and(|snapshot| {
                FSMetaApp::sink_status_snapshot_has_materialized_pending_progress_for_expected_groups(
                    &snapshot,
                    &expected_groups,
                )
            });
        if !sink_has_materialized_pending_progress {
            return false;
        }
        self.source
            .status_snapshot_with_failure()
            .await
            .ok()
            .is_some_and(|snapshot| {
                FSMetaApp::source_status_snapshot_has_inflight_initial_audit_for_expected_groups(
                    &snapshot,
                    &expected_groups,
                )
            })
    }

    async fn defer_sink_generation_cutover_replay_inline(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        sink_signals: &[SinkControlSignal],
    ) -> std::result::Result<(), RuntimeWorkerControlApplyFailure> {
        let filtered_sink_signals = self
            .filter_sink_facade_dependent_route_activates_during_pending_fixed_bind_claim_with_session(
                &self
                    .filter_shared_sink_route_deactivates(
                        &self.filter_stale_sink_ticks(sink_signals).await,
                    )
                    .await,
                fixed_bind_session,
            )
            .await;
        self.record_retained_sink_control_state(&filtered_sink_signals)
            .await;
        self.record_shared_sink_route_claims(&filtered_sink_signals)
            .await;
        self.apply_sink_signals_to_runtime_endpoint_gate(&filtered_sink_signals)
            .map_err(RuntimeWorkerControlApplyFailure::from)?;
        self.mark_control_uninitialized_after_deferred_sink_replay_with_session(fixed_bind_session)
            .await;
        eprintln!(
            "fs_meta_runtime_app: sink control deferred post-initial route cutover at app boundary"
        );
        Ok(())
    }

    async fn clear_shared_sink_query_route_claims_for_cleanup(&self) {
        let mut claims = self.shared_sink_route_claims.lock().await;
        claims.retain(|(unit_id, route_key), _| {
            !(unit_id == execution_units::SINK_RUNTIME_UNIT_ID
                && is_per_peer_sink_query_request_route(route_key))
        });
    }

    async fn sink_signals_are_in_shared_generation_cutover_lane(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> bool {
        if sink_signals.is_empty()
            || !sink_signals
                .iter()
                .all(Self::sink_signal_requires_fail_closed_retry_after_generation_cutover)
        {
            return false;
        }

        let claims = self.shared_sink_route_claims.lock().await;
        sink_signals.iter().any(|signal| {
            Self::sink_signal_is_restart_deferred_retire_pending(signal)
                || match signal {
                    SinkControlSignal::Activate {
                        unit, route_key, ..
                    }
                    | SinkControlSignal::Deactivate {
                        unit, route_key, ..
                    } => claims
                        .get(&(unit.unit_id().to_string(), route_key.clone()))
                        .is_some_and(|owners| {
                            owners.iter().any(|owner| *owner != self.instance_id)
                        }),
                    SinkControlSignal::Tick { .. }
                    | SinkControlSignal::RuntimeHostGrantChange { .. }
                    | SinkControlSignal::Passthrough(_) => false,
                }
        })
    }

    async fn apply_sink_signals_with_recovery(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        sink_signals: &[SinkControlSignal],
        replay_retained_state: bool,
        fail_closed_in_generation_cutover_lane: bool,
        fail_closed_on_retryable_reset: bool,
    ) -> std::result::Result<(), RuntimeWorkerControlApplyFailure> {
        let filtered_sink_signals = self
            .filter_sink_facade_dependent_route_activates_during_pending_fixed_bind_claim_with_session(
                &self
                    .filter_shared_sink_route_deactivates(
                        &self.filter_stale_sink_ticks(sink_signals).await,
                    )
                    .await,
                fixed_bind_session,
            )
            .await;
        if self.control_initialized()
            && !self.runtime_control_state().sink_state_replay_required()
            && self.sink.current_generation_tick_fast_path_eligible().await
            && !filtered_sink_signals.is_empty()
            && filtered_sink_signals
                .iter()
                .all(|signal| matches!(signal, SinkControlSignal::Tick { .. }))
        {
            return Ok(());
        }
        self.record_retained_sink_control_state(&filtered_sink_signals)
            .await;
        let replay_followup_signals = if replay_retained_state
            && self.runtime_control_state().sink_state_replay_required()
            && Self::sink_signals_are_transient_followup_only(&filtered_sink_signals)
        {
            let followups = Self::sink_transient_followup_signals(&filtered_sink_signals);
            (!followups.is_empty()).then_some(followups)
        } else {
            None
        };
        let replay_followup_pending = replay_followup_signals.clone();
        let deadline = tokio::time::Instant::now() + SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT;
        loop {
            let replaying_retained_state_only = replay_retained_state
                && self.runtime_control_state().sink_state_replay_required()
                && replay_followup_pending.is_some();
            let effective_sink_signals = if replaying_retained_state_only {
                let replayed_sink_signals = self.sink_signals_with_replay(&[]).await;
                self.filter_shared_sink_route_deactivates(&replayed_sink_signals)
                    .await
            } else if let Some(followups) = replay_followup_pending.clone() {
                if self.control_initialized()
                    && !self.runtime_control_state().sink_state_replay_required()
                    && self.sink.current_generation_tick_fast_path_eligible().await
                    && !followups.is_empty()
                    && followups
                        .iter()
                        .all(|signal| matches!(signal, SinkControlSignal::Tick { .. }))
                {
                    return Ok(());
                }
                followups
            } else if replay_retained_state {
                let replayed_sink_signals =
                    self.sink_signals_with_replay(&filtered_sink_signals).await;
                self.filter_shared_sink_route_deactivates(&replayed_sink_signals)
                    .await
            } else {
                filtered_sink_signals.clone()
            };
            if effective_sink_signals.is_empty() {
                self.record_shared_sink_route_claims(&effective_sink_signals)
                    .await;
                if replay_retained_state {
                    self.update_runtime_control_state(|state| state.clear_sink_replay());
                }
                if replaying_retained_state_only && replay_followup_pending.is_some() {
                    continue;
                }
                return Ok(());
            }
            self.record_shared_sink_route_claims(&effective_sink_signals)
                .await;
            self.apply_sink_signals_to_runtime_endpoint_gate(&effective_sink_signals)
                .map_err(RuntimeWorkerControlApplyFailure::from)?;
            let fail_closed_restart_deferred_retire_pending = fail_closed_in_generation_cutover_lane
                && effective_sink_signals
                    .iter()
                    .all(Self::sink_signal_is_restart_deferred_retire_pending);
            let fail_closed_cleanup_wakeup = fail_closed_in_generation_cutover_lane
                && !filtered_sink_signals.is_empty()
                && filtered_sink_signals
                    .iter()
                    .all(Self::sink_signal_is_cleanup_only);
            if debug_control_frame_trace_enabled() && effective_sink_signals.len() == 1 {
                eprintln!(
                    "fs_meta_runtime_app: apply_sink_signals_with_recovery effective_signals={:?} replay_retained={} fail_closed={}",
                    effective_sink_signals,
                    replay_retained_state,
                    fail_closed_in_generation_cutover_lane
                );
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            #[cfg(test)]
            let sink_apply_result = if let Some(err) = take_sink_apply_error_queue_hook() {
                Err(SinkFailure::from(err))
            } else {
                self.sink
                    .apply_orchestration_signals_with_total_timeout_with_failure(
                        &effective_sink_signals,
                        remaining,
                    )
                    .await
            };
            #[cfg(not(test))]
            let sink_apply_result = self
                .sink
                .apply_orchestration_signals_with_total_timeout_with_failure(
                    &effective_sink_signals,
                    remaining,
                )
                .await;
            match sink_apply_result {
                Ok(()) => {
                    if replay_retained_state {
                        self.record_retained_sink_control_state(&effective_sink_signals)
                            .await;
                    }
                    if replay_retained_state {
                        self.update_runtime_control_state(|state| state.clear_sink_replay());
                        self.sink_generation_cutover_replay_deferred
                            .store(false, Ordering::Release);
                    }
                    if replaying_retained_state_only {
                        continue;
                    }
                    return Ok(());
                }
                Err(err) if fail_closed_restart_deferred_retire_pending => {
                    eprintln!(
                        "fs_meta_runtime_app: sink control fail-closed restart_deferred_retire_pending err={}",
                        err.as_error()
                    );
                    self.mark_control_uninitialized_after_deferred_sink_replay_without_scheduled_repair_with_session(
                        fixed_bind_session,
                    )
                        .await;
                    return Ok(());
                }
                Err(err)
                    if fail_closed_cleanup_wakeup
                        && is_retryable_worker_control_reset(err.as_error()) =>
                {
                    eprintln!(
                        "fs_meta_runtime_app: sink control fail-closed cleanup wakeup err={}",
                        err.as_error()
                    );
                    self.mark_control_uninitialized_after_deferred_sink_replay_without_scheduled_repair_with_session(
                        fixed_bind_session,
                    )
                        .await;
                    return Ok(());
                }
                Err(err)
                    if is_retryable_worker_control_reset(err.as_error())
                        && tokio::time::Instant::now() < deadline =>
                {
                    if fail_closed_on_retryable_reset {
                        eprintln!(
                            "fs_meta_runtime_app: sink control fail-closed release cutover err={}",
                            err.as_error()
                        );
                        self.mark_control_uninitialized_after_deferred_sink_replay_with_session(
                            fixed_bind_session,
                        )
                        .await;
                        if !fail_closed_in_generation_cutover_lane {
                            return Err(RuntimeWorkerControlApplyFailure::from(err));
                        }
                        return Ok(());
                    }
                    if fail_closed_in_generation_cutover_lane
                        && is_retryable_worker_transport_close_reset(err.as_error())
                    {
                        eprintln!(
                            "fs_meta_runtime_app: sink control fail-closed during generation cutover err={}",
                            err.as_error()
                        );
                        self.mark_control_uninitialized_after_deferred_sink_replay_without_scheduled_repair_with_session(
                            fixed_bind_session,
                        )
                        .await;
                        if self.runtime_control_state().source_state_current()
                            && self.source_retained_replay_has_active_route_state().await
                        {
                            return Ok(());
                        }
                        return Err(RuntimeWorkerControlApplyFailure::from(err));
                    }
                    eprintln!(
                        "fs_meta_runtime_app: sink control replay after retryable reset err={}",
                        err.as_error()
                    );
                    #[cfg(test)]
                    let sink_recovery_result =
                        if let Some(err) = take_sink_control_recovery_error_queue_hook() {
                            Err(SinkFailure::from(err))
                        } else {
                            self.sink
                                .recover_control_lane_after_retryable_reset_until(deadline)
                                .await
                        };
                    #[cfg(not(test))]
                    let sink_recovery_result = self
                        .sink
                        .recover_control_lane_after_retryable_reset_until(deadline)
                        .await;
                    match sink_recovery_result {
                        Ok(()) => {}
                        Err(recovery_err)
                            if should_fail_closed_sink_control_recovery_exhaustion(
                                recovery_err.as_error(),
                            ) =>
                        {
                            eprintln!(
                                "fs_meta_runtime_app: sink control fail-closed after worker recovery exhausted err={}",
                                recovery_err.as_error()
                            );
                            self.mark_control_uninitialized_after_failure_with_session(
                                fixed_bind_session,
                            )
                            .await;
                            return Ok(());
                        }
                        Err(recovery_err) => {
                            return Err(RuntimeWorkerControlApplyFailure::from(recovery_err));
                        }
                    }
                }
                Err(err) if should_fail_closed_sink_control_recovery_exhaustion(err.as_error()) => {
                    eprintln!(
                        "fs_meta_runtime_app: sink control fail-closed after worker recovery exhausted err={}",
                        err.as_error()
                    );
                    self.mark_control_uninitialized_after_failure_with_session(fixed_bind_session)
                        .await;
                    return Ok(());
                }
                Err(err) => {
                    if fail_closed_in_generation_cutover_lane {
                        self.mark_control_uninitialized_after_deferred_sink_replay_without_scheduled_repair_with_session(
                            fixed_bind_session,
                        )
                        .await;
                    } else {
                        self.mark_control_uninitialized_after_failure_with_session(
                            fixed_bind_session,
                        )
                        .await;
                    }
                    return Err(RuntimeWorkerControlApplyFailure::from(err));
                }
            }
        }
    }

    #[cfg(test)]
    async fn apply_facade_deactivate(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
        restart_deferred_retire_pending: bool,
    ) -> Result<()> {
        let session = self.begin_fixed_bind_lifecycle_session().await;
        let _ = self
            .apply_facade_deactivate_with_session(
                session,
                unit,
                route_key,
                generation,
                restart_deferred_retire_pending,
                true,
            )
            .await?;
        Ok(())
    }

    async fn apply_facade_deactivate_with_session(
        &self,
        session: FixedBindLifecycleSession,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
        restart_deferred_retire_pending: bool,
        preserve_core_business_read_routes: bool,
    ) -> Result<FixedBindLifecycleSession> {
        let mut session = session;
        eprintln!(
            "fs_meta_runtime_app: apply_facade_deactivate unit={} route_key={} generation={}",
            unit.unit_id(),
            route_key,
            generation
        );
        if !facade_route_key_matches(unit, route_key) {
            return Ok(session);
        }
        let query_lane = matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        );
        let sink_query_proxy_route = format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY);
        let facade_control_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
        let sink_query_proxy_current_generation =
            if query_lane && route_key == sink_query_proxy_route {
                self.facade_gate
                    .route_generation(unit.unit_id(), route_key)?
                    .unwrap_or(0)
            } else {
                0
            };
        if matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        ) && is_internal_status_route(route_key)
            && self.control_initialized()
            && matches!(
                session.root_reason(),
                FixedBindLifecycleRootReason::PublicOnControlFrame
            )
        {
            self.api_control_gate.wait_for_facade_request_drain().await;
            self.api_control_gate
                .wait_for_status_remote_collection_drain()
                .await;
        }
        if query_lane
            && matches!(
                session.root_reason(),
                FixedBindLifecycleRootReason::PublicOnControlFrame
            )
        {
            self.wait_for_shared_worker_control_handoff().await;
        }
        if !self.control_initialized() && query_lane && route_key == sink_query_proxy_route {
            // Only clear per-peer sink-query shared claims as part of the uninitialized
            // stale cleanup chain. Clearing these claims during a live proxy deactivate
            // can disrupt in-flight internal proxy requests.
            if generation < sink_query_proxy_current_generation {
                self.clear_shared_sink_query_route_claims_for_cleanup()
                    .await;
            }
        }
        let unit_id = unit.unit_id();
        #[cfg(test)]
        maybe_pause_facade_deactivate(unit_id, route_key).await;
        let active_facade_controls_public_listener =
            self.api_task.lock().await.as_ref().is_some_and(|active| {
                active.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
            });
        if query_lane
            && !self.control_initialized()
            && preserve_core_business_read_routes
            && self.control_failure_uninitialized.load(Ordering::Acquire)
            && is_serving_facade_business_read_route(route_key)
        {
            eprintln!(
                "fs_meta_runtime_app: retain core business read route during control-failure cleanup query deactivate route_key={} generation={}",
                route_key, generation
            );
            return Ok(session);
        }
        if query_lane
            && !self.control_initialized()
            && preserve_core_business_read_routes
            && active_facade_controls_public_listener
            && is_serving_facade_business_read_route(route_key)
        {
            eprintln!(
                "fs_meta_runtime_app: retain serving facade business read route during cleanup-only query deactivate route_key={} generation={}",
                route_key, generation
            );
            return Ok(session);
        }
        if matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        ) && is_facade_dependent_query_route(route_key)
            && self
                .retained_active_facade_continuity
                .load(Ordering::Acquire)
            && active_facade_controls_public_listener
        {
            eprintln!(
                "fs_meta_runtime_app: retain facade-dependent query route during continuity-preserving deactivate route_key={} generation={}",
                route_key, generation
            );
            return Ok(session);
        }
        if matches!(unit, FacadeRuntimeUnit::Facade)
            && route_key == facade_control_route_key
            && !self.control_initialized()
            && self.control_failure_uninitialized.load(Ordering::Acquire)
        {
            eprintln!(
                "fs_meta_runtime_app: retain facade-control route during control-failure cleanup facade deactivate route_key={} generation={}",
                route_key, generation
            );
            return Ok(session);
        }
        if matches!(unit, FacadeRuntimeUnit::Facade) && route_key == facade_control_route_key {
            let pending_fixed_bind_successor_unready = self
                .pending_facade
                .lock()
                .await
                .as_ref()
                .is_some_and(|pending| {
                    pending.route_key == route_key
                        && !pending.runtime_exposure_confirmed
                        && pending.generation >= generation
                });
            let pending_external_fixed_bind_successor_unready =
                !pending_fixed_bind_successor_unready
                    && self
                        .pending_external_fixed_bind_successor_unready_for_active_facade(
                            route_key, generation,
                        )
                        .await;
            if (pending_fixed_bind_successor_unready
                || pending_external_fixed_bind_successor_unready)
                && active_facade_controls_public_listener
            {
                self.retained_active_facade_continuity
                    .store(true, Ordering::Release);
                eprintln!(
                    "fs_meta_runtime_app: retain active facade during pending successor cleanup deactivate route_key={} generation={}",
                    route_key, generation
                );
                return Ok(session);
            }
        }
        let accepted = self
            .facade_gate
            .apply_deactivate(unit_id, route_key, generation)?;
        if !accepted {
            log::info!(
                "fs-meta facade: ignore stale deactivate unit={} generation={}",
                unit_id,
                generation
            );
            return Ok(session);
        }
        if matches!(unit, FacadeRuntimeUnit::Query) {
            self.mirrored_query_peer_routes
                .lock()
                .await
                .remove(route_key);
        } else if matches!(unit, FacadeRuntimeUnit::QueryPeer)
            && is_dual_lane_internal_query_route(route_key)
        {
            let mut mirrored = self.mirrored_query_peer_routes.lock().await;
            if mirrored
                .get(route_key)
                .is_some_and(|mirrored_generation| generation >= *mirrored_generation)
            {
                self.facade_gate.apply_deactivate(
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    route_key,
                    generation,
                )?;
                mirrored.remove(route_key);
            }
        }
        if matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        ) {
            return Ok(session);
        }
        if !matches!(unit, FacadeRuntimeUnit::Facade) || route_key != facade_control_route_key {
            return Ok(session);
        }
        let retain_active_facade = {
            let active_guard = self.api_task.lock().await;
            active_guard.as_ref().is_some_and(|active| {
                active.route_key == route_key && generation >= active.generation
            })
        };
        let retain_pending_spawn = self.pending_spawn_in_flight_for_route_key(route_key).await;
        let retire_active_uninitialized_cleanup = !self.control_initialized()
            && !self.control_failure_uninitialized.load(Ordering::Acquire);
        session = self
            .drive_fixed_bind_lifecycle_request_with_session(
                session,
                FixedBindLifecycleRequest::Deactivate {
                    route_key: route_key.to_string(),
                    generation,
                    retain_active_facade,
                    retain_pending_spawn,
                    restart_deferred_retire_pending: restart_deferred_retire_pending
                        || retire_active_uninitialized_cleanup,
                },
            )
            .await?;
        Ok(session)
    }

    fn accept_facade_tick(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> Result<bool> {
        if !facade_route_key_matches(unit, route_key) {
            return Ok(false);
        }
        self.facade_gate
            .accept_tick(unit.unit_id(), route_key, generation)
    }

    async fn confirm_pending_facade_exposure(&self, route_key: &str, generation: u64) -> bool {
        let mut pending_guard = self.pending_facade.lock().await;
        let Some(pending) = pending_guard.as_mut() else {
            return false;
        };
        if pending.route_key != route_key || pending.generation != generation {
            return false;
        }
        pending.runtime_exposure_confirmed = true;
        true
    }

    fn apply_facade_signal_with_session<'a>(
        &'a self,
        session: FixedBindLifecycleSession,
        signal: FacadeControlSignal,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<FixedBindLifecycleSession>> + Send + 'a>,
    > {
        self.apply_facade_signal_with_session_and_policy(session, signal, true)
    }

    fn apply_facade_signal_with_session_and_policy<'a>(
        &'a self,
        session: FixedBindLifecycleSession,
        signal: FacadeControlSignal,
        preserve_core_business_read_routes: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<FixedBindLifecycleSession>> + Send + 'a>,
    > {
        Box::pin(async move {
            match signal {
                FacadeControlSignal::Activate {
                    unit,
                    route_key,
                    generation,
                    bound_scopes,
                } => {
                    return self
                        .apply_facade_activate_with_session(
                            session,
                            unit,
                            &route_key,
                            generation,
                            &bound_scopes,
                        )
                        .await;
                }
                FacadeControlSignal::Deactivate {
                    unit,
                    route_key,
                    generation,
                    restart_deferred_retire_pending,
                } => {
                    return self
                        .apply_facade_deactivate_with_session(
                            session,
                            unit,
                            &route_key,
                            generation,
                            restart_deferred_retire_pending,
                            preserve_core_business_read_routes,
                        )
                        .await;
                }
                FacadeControlSignal::Tick {
                    unit,
                    route_key,
                    generation,
                } => {
                    let accepted = self.accept_facade_tick(unit, &route_key, generation)?;
                    if !accepted {
                        log::info!(
                            "fs-meta facade: ignore stale/inactive tick unit={} generation={}",
                            unit.unit_id(),
                            generation
                        );
                    } else if matches!(unit, FacadeRuntimeUnit::Facade)
                        && route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                    {
                        self.retry_pending_facade(&route_key, generation, true)
                            .await?;
                    }
                }
                FacadeControlSignal::ExposureConfirmed {
                    unit,
                    route_key,
                    generation,
                    confirmed_at_us: _confirmed_at_us,
                } => {
                    let accepted = self.accept_facade_tick(unit, &route_key, generation)?;
                    if !accepted {
                        log::info!(
                            "fs-meta facade: ignore stale/inactive exposure_confirmed unit={} generation={}",
                            unit.unit_id(),
                            generation
                        );
                    } else if matches!(unit, FacadeRuntimeUnit::Facade)
                        && route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                        && self
                            .confirm_pending_facade_exposure(&route_key, generation)
                            .await
                    {
                        self.retry_pending_facade(&route_key, generation, false)
                            .await?;
                    }
                }
                FacadeControlSignal::RuntimeHostGrantChange { .. }
                | FacadeControlSignal::Passthrough => {}
            }
            Ok(session)
        })
    }

    #[cfg(test)]
    async fn fixed_bind_handoff_ready_for_release(&self) -> bool {
        let bind_addr = {
            let api_task = self.api_task.lock().await;
            api_task.as_ref().and_then(|active| {
                (active.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL))
                    .then(|| {
                        self.config
                            .api
                            .resolve_for_candidate_ids(&active.resource_ids)
                    })
                    .flatten()
                    .map(|resolved| resolved.bind_addr)
                    .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
            })
        };
        let Some(bind_addr) = bind_addr else {
            return false;
        };
        let ready = pending_fixed_bind_handoff_ready_for(&bind_addr);
        match ready.as_ref() {
            Some(ready) => ready
                .release_handoff_for_active_owner(&bind_addr, self.instance_id)
                .await
                .is_some(),
            None => false,
        }
    }

    async fn pending_external_fixed_bind_successor_unready_for_active_facade(
        &self,
        route_key: &str,
        generation: u64,
    ) -> bool {
        if route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL) {
            return false;
        }
        let bind_addr = {
            let api_task = self.api_task.lock().await;
            api_task.as_ref().and_then(|active| {
                (active.route_key == route_key)
                    .then(|| {
                        self.config
                            .api
                            .resolve_for_candidate_ids(&active.resource_ids)
                    })
                    .flatten()
                    .map(|resolved| resolved.bind_addr)
                    .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
            })
        };
        let Some(bind_addr) = bind_addr else {
            return false;
        };
        let Some(registrant) = pending_fixed_bind_handoff_ready_for(&bind_addr) else {
            return false;
        };
        if registrant.instance_id == self.instance_id {
            return false;
        }
        registrant
            .pending_facade
            .lock()
            .await
            .as_ref()
            .is_some_and(|pending| {
                pending.route_key == route_key
                    && pending.generation >= generation
                    && !pending.runtime_exposure_confirmed
            })
    }

    async fn pending_spawn_in_flight_for_route_key(&self, route_key: &str) -> bool {
        let pending = self.pending_facade.lock().await.clone();
        let Some(pending) = pending else {
            return false;
        };
        if pending.route_key != route_key {
            return false;
        }
        self.facade_spawn_in_progress
            .lock()
            .await
            .as_ref()
            .is_some_and(|inflight| {
                pending.route_key == inflight.route_key
                    && pending.resource_ids == inflight.resource_ids
            })
    }

    fn service_on_control_frame_with_failure_and_session<'a>(
        &'a self,
        envelopes: &'a [ControlEnvelope],
        fixed_bind_session: FixedBindLifecycleSession,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<(), RuntimeControlFrameFailure>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let (source_signals, sink_signals, mut facade_signals) =
                split_app_control_signals(envelopes)?;
            let runtime_unit_exposure_present = control_frame_has_runtime_unit_exposure(envelopes)?;
            facade_signals.sort_by_key(Self::facade_signal_apply_priority);
            if Self::control_frame_is_host_grant_change_only(
                &source_signals,
                &sink_signals,
                &facade_signals,
            ) {
                return self
                    .apply_host_grant_change_fast_lane(&source_signals, &sink_signals)
                    .await;
            }
            let uninitialized_cleanup_disposition =
                Self::classify_uninitialized_cleanup_disposition(
                    UninitializedCleanupDecisionInput {
                        control_initialized_now: self.control_initialized(),
                        source_signals: &source_signals,
                        sink_signals: &sink_signals,
                        facade_signals: &facade_signals,
                    },
                );
            let query_lane_deactivate_only = source_signals.is_empty()
                && sink_signals.is_empty()
                && !runtime_unit_exposure_present
                && Self::facade_signals_are_query_lane_deactivate_only(&facade_signals);
            let source_status_recovery_followup_present = source_signals.is_empty()
                && sink_signals.is_empty()
                && !runtime_unit_exposure_present
                && facade_signals
                    .iter()
                    .any(facade_publication_signal_is_source_status_activate);
            let facade_cleanup_only_while_uninitialized =
                uninitialized_cleanup_disposition.is_facade_only();
            let sink_query_cleanup_only_while_uninitialized =
                uninitialized_cleanup_disposition.is_sink_query_only();
            let requires_shared_serial = if sink_query_cleanup_only_while_uninitialized
                || facade_cleanup_only_while_uninitialized
            {
                false
            } else {
                let runtime_gate = self.runtime_control_state();
                !source_signals.is_empty()
                    || !sink_signals.is_empty()
                    || runtime_unit_exposure_present
                    || self.control_initialized()
                    || facade_signals
                        .iter()
                        .any(Self::facade_signal_requires_shared_serial_while_uninitialized)
                    || runtime_gate.replay_fully_cleared()
            };
            let _serial_guard = if query_lane_deactivate_only {
                None
            } else {
                Some(self.control_frame_serial.lock().await)
            };
            let _shared_serial_guard = if requires_shared_serial {
                Some(self.shared_control_frame_serial.lock().await)
            } else {
                None
            };
            let _lease_guard = match (
                requires_shared_serial,
                self.control_frame_lease_path.as_deref(),
            ) {
                (true, Some(path)) => {
                    let deadline = tokio::time::Instant::now() + CONTROL_FRAME_LEASE_ACQUIRE_BUDGET;
                    Some(
                    ControlFrameLeaseGuard::acquire(path, deadline, || {
                        self.closing.load(Ordering::Acquire)
                    })
                    .await
                    .map_err(|err| match err.kind() {
                        std::io::ErrorKind::TimedOut | std::io::ErrorKind::Interrupted => {
                            CnxError::NotReady(format!(
                                "fs-meta control-frame lease acquisition failed closed at {}: {err}",
                                path.display()
                            ))
                        }
                        _ => CnxError::Internal(format!(
                            "fs-meta control-frame lease acquisition failed at {}: {err}",
                            path.display()
                        )),
                    })?,
                )
                }
                _ => None,
            };
            let control_initialized_at_entry = self.control_initialized();
            let retained_sink_state_present_at_entry = !self
                .retained_sink_control_state
                .lock()
                .await
                .active_by_route
                .is_empty();
            #[cfg(test)]
            notify_runtime_control_frame_started();
            let should_initialize_from_control = Self::should_initialize_from_control(
                &source_signals,
                &sink_signals,
                &facade_signals,
            );
            let (source_replay_required_at_entry, retained_replay_pending_at_entry) = {
                let runtime_gate = self.runtime_control_state();
                (
                    runtime_gate.source_state_replay_required(),
                    runtime_gate.source_state_replay_required()
                        || runtime_gate.sink_state_replay_required(),
                )
            };
            let retained_source_route_state_present_at_entry =
                self.source_retained_replay_has_active_route_state().await;
            let should_run_normal_runtime_initialization = should_initialize_from_control
                && !control_initialized_at_entry
                && !retained_replay_pending_at_entry;
            let should_run_fail_closed_runtime_reinitialization = should_initialize_from_control
                && !control_initialized_at_entry
                && retained_replay_pending_at_entry
                && (self
                    .source_fail_closed_reinitialize_required
                    .load(Ordering::Acquire)
                    || (source_replay_required_at_entry
                        && source_status_recovery_followup_present));
            let source_empty_root_route_liveness_only =
                self.source_signals_are_empty_root_route_liveness_only(&source_signals);
            let initialize_wait_for_source_worker_handoff =
                !source_signals.is_empty() && !source_empty_root_route_liveness_only;
            let initialize_wait_for_sink_worker_handoff = !sink_signals.is_empty();
            let request_sensitive = !source_signals.is_empty()
                || !sink_signals.is_empty()
                || !facade_signals.is_empty();
            if request_sensitive && self.api_request_tracker.inflight() > 0 {
                let skip_request_drain_for_uninitialized_recovery = !self.control_initialized()
                    && (should_initialize_from_control || facade_cleanup_only_while_uninitialized);
                if !skip_request_drain_for_uninitialized_recovery {
                    self.api_request_tracker.wait_for_drain().await;
                }
            }
            let mut source_control_apply_inflight =
                (!source_signals.is_empty()).then(|| self.begin_source_control_apply());
            if request_sensitive {
                self.api_control_gate.set_ready(false);
            }
            let debug_control_frame = debug_control_frame_trace_enabled();
            if debug_control_frame {
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame begin source_signals={} sink_signals={} facade_signals={} initialized={}",
                    source_signals.len(),
                    sink_signals.len(),
                    facade_signals.len(),
                    self.control_initialized()
                );
            }
            if debug_control_frame
                && source_signals.is_empty()
                && facade_signals.is_empty()
                && sink_signals.len() == 1
            {
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame sink_only_lane signals={:?}",
                    sink_signals
                );
            }
            if debug_control_frame && source_signals.len() <= 2 && !source_signals.is_empty() {
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame source_lane signals={:?}",
                    source_signals
                );
            }
            let source_cleanup_only_while_uninitialized =
                uninitialized_cleanup_disposition.is_source_only();
            let sink_cleanup_only_while_uninitialized =
                uninitialized_cleanup_disposition.is_sink_only();
            let mut fixed_bind_session = fixed_bind_session;
            if should_initialize_from_control {
                if (should_run_normal_runtime_initialization
                    || should_run_fail_closed_runtime_reinitialization)
                    && !source_cleanup_only_while_uninitialized
                    && !sink_cleanup_only_while_uninitialized
                {
                    self.initialize_from_control_with_deadline_and_session(
                        initialize_wait_for_source_worker_handoff,
                        initialize_wait_for_sink_worker_handoff,
                        None,
                        &fixed_bind_session,
                    )
                    .await?;
                    fixed_bind_session = self
                    .rebuild_fixed_bind_lifecycle_session(
                        &fixed_bind_session,
                        FixedBindLifecycleRebuildReason::AfterOnControlFrameInitializeFromControl,
                    )
                    .await;
                    if should_run_fail_closed_runtime_reinitialization {
                        self.source_fail_closed_reinitialize_required
                            .store(false, Ordering::Release);
                    }
                }
            } else if !self.control_initialized()
                && !facade_cleanup_only_while_uninitialized
                && !source_cleanup_only_while_uninitialized
                && !sink_cleanup_only_while_uninitialized
            {
                if retained_replay_pending_at_entry {
                    if runtime_unit_exposure_present {
                        self.recover_retained_replay_readiness_from_runtime_unit_exposure(
                            &fixed_bind_session,
                        )
                        .await?;
                    } else {
                        if self
                            .source_generation_cutover_replay_should_defer_inline(&[])
                            .await
                        {
                            eprintln!(
                                "fs_meta_runtime_app: on_control_frame deferred empty retained source replay after fail-closed generation cutover"
                            );
                            return Ok(());
                        }
                        if self
                            .sink_generation_cutover_replay_should_defer_inline()
                            .await
                        {
                            self.mark_control_uninitialized_after_deferred_sink_replay_with_session(
                            &fixed_bind_session,
                        )
                        .await;
                            eprintln!(
                                "fs_meta_runtime_app: on_control_frame deferred empty retained sink replay after fail-closed generation cutover"
                            );
                            return Ok(());
                        }
                        self.recover_retained_replay_readiness_inside_control_frame(
                            &fixed_bind_session,
                        )
                        .await?;
                    }
                    if self.control_initialized() || runtime_unit_exposure_present {
                        return Ok(());
                    }
                }
                return Err(Self::not_ready_error().into());
            }
            if !facade_cleanup_only_while_uninitialized
                && !source_cleanup_only_while_uninitialized
                && !sink_cleanup_only_while_uninitialized
                && !retained_replay_pending_at_entry
            {
                self.ensure_runtime_endpoints_started().await?;
            }
            if source_cleanup_only_while_uninitialized {
                self.record_shared_source_route_claims(&source_signals)
                    .await;
            }
            if sink_query_cleanup_only_while_uninitialized {
                self.withdraw_uninitialized_query_routes_with_policy(
                    ControlFailureRecoveryLanePolicy::WithdrawInternalStatus,
                    true,
                )
                .await;
                self.clear_shared_sink_query_route_claims_for_cleanup()
                    .await;
            }
            if !sink_signals.is_empty() {
                let staged_sink_signals = if sink_cleanup_only_while_uninitialized {
                    sink_signals.clone()
                } else {
                    self.filter_shared_sink_route_deactivates(&sink_signals)
                        .await
                };
                if !sink_query_cleanup_only_while_uninitialized {
                    self.record_retained_sink_control_state(&staged_sink_signals)
                        .await;
                }
            }
            let facade_signals_for_generation_cutover = facade_signals.clone();
            let (facade_claim_signals, mut facade_publication_signals): (Vec<_>, Vec<_>) =
                facade_signals
                    .into_iter()
                    .partition(Self::facade_signal_updates_facade_claim);
            let sink_status_publication_present = facade_publication_signals
                .iter()
                .any(facade_publication_signal_is_sink_status_activate);
            let facade_claim_signals_present = !facade_claim_signals.is_empty();
            let mut pretriggered_source_to_sink_convergence_epoch = None::<u64>;
            let mut sink_recovery_tail_plan = SinkRecoveryTailPlan::new();
            for signal in facade_claim_signals {
                fixed_bind_session = self
                    .apply_facade_signal_with_session(fixed_bind_session, signal)
                    .await?;
            }
            let fixed_bind_publication_continuation_active =
                self.fixed_bind_publication_continuation_active().await;
            let source_generation_cutover_disposition =
                Self::classify_source_generation_cutover_disposition(
                    SourceGenerationCutoverDecisionInput {
                        control_initialized_at_entry,
                        source_state_replay_required_at_entry: source_replay_required_at_entry,
                        retained_source_route_state_present_at_entry,
                        fixed_bind_publication_continuation_active:
                            fixed_bind_publication_continuation_active,
                        source_signals: &source_signals,
                        sink_signals: &sink_signals,
                        facade_signals: &facade_signals_for_generation_cutover,
                    },
                );
            let sink_generation_cutover_disposition = if sink_cleanup_only_while_uninitialized {
                SinkGenerationCutoverDisposition::None
            } else {
                Self::classify_sink_generation_cutover_disposition(
                    SinkGenerationCutoverDecisionInput {
                        control_initialized_at_entry,
                        retained_replay_pending_at_entry,
                        retained_sink_route_state_present_at_entry:
                            retained_sink_state_present_at_entry,
                        fixed_bind_publication_continuation_active:
                            fixed_bind_publication_continuation_active,
                        source_signals: &source_signals,
                        sink_signals: &sink_signals,
                        facade_signals: &facade_signals_for_generation_cutover,
                        sink_signals_in_shared_generation_cutover_lane: self
                            .sink_signals_are_in_shared_generation_cutover_lane(&sink_signals)
                            .await,
                    },
                )
            };
            let sink_tick_fast_path_eligible =
                self.sink.current_generation_tick_fast_path_eligible().await;
            let runtime_gate = self.runtime_control_state();
            let control_wave_observation = ControlFrameWaveObservation {
                control_initialized_at_entry,
                control_initialized_now: self.control_initialized(),
                retained_sink_state_present_at_entry,
                source_state_replay_required: runtime_gate.source_state_replay_required(),
                sink_state_replay_required: runtime_gate.sink_state_replay_required(),
                sink_tick_fast_path_eligible,
                cleanup_disposition: uninitialized_cleanup_disposition,
                facade_claim_signals_present,
                facade_publication_signals_present: !facade_publication_signals.is_empty(),
                source_replay_tick_only_while_sink_replay_pending: !source_signals.is_empty()
                    && runtime_gate.source_state_replay_required()
                    && runtime_gate.sink_state_replay_required()
                    && self
                        .source_signals_are_retained_or_forward_generation_ticks(&source_signals)
                        .await,
                source_tick_gate_only_while_sink_replay_pending: !source_signals.is_empty()
                    && !control_initialized_at_entry
                    && !self.control_initialized()
                    && !runtime_gate.source_state_replay_required()
                    && runtime_gate.sink_state_replay_required()
                    && self
                        .source_signals_are_retained_or_forward_generation_ticks(&source_signals)
                        .await,
                sink_tick_gate_only_while_source_replay_pending: !sink_signals.is_empty()
                    && !control_initialized_at_entry
                    && !self.control_initialized()
                    && runtime_gate.source_state_replay_required()
                    && sink_signals
                        .iter()
                        .all(|signal| matches!(signal, SinkControlSignal::Tick { .. })),
                sink_tick_gate_only_while_sink_replay_pending: !sink_signals.is_empty()
                    && !control_initialized_at_entry
                    && !self.control_initialized()
                    && !runtime_gate.source_state_replay_required()
                    && runtime_gate.sink_state_replay_required()
                    && sink_signals
                        .iter()
                        .all(|signal| matches!(signal, SinkControlSignal::Tick { .. })),
                source_signals_present: !source_signals.is_empty(),
                sink_signals_present: !sink_signals.is_empty(),
            };
            let source_wave_disposition = if source_empty_root_route_liveness_only {
                SourceControlWaveDisposition::EmptyRootRouteLivenessOnly
            } else {
                Self::classify_source_control_wave_disposition(
                    control_wave_observation,
                    &source_signals,
                )
            };
            let sink_wave_disposition = Self::classify_sink_control_wave_disposition(
                control_wave_observation,
                source_wave_disposition,
                &sink_signals,
            );
            let mut sink_wave_applied_before_source = false;
            if facade_cleanup_only_while_uninitialized {
                for signal in facade_publication_signals.drain(..) {
                    fixed_bind_session = self
                        .apply_facade_signal_with_session_and_policy(
                            fixed_bind_session,
                            signal,
                            !query_lane_deactivate_only,
                        )
                        .await?;
                }
                let _ = self
                    .release_failed_fixed_bind_owner_for_pending_successor(&fixed_bind_session)
                    .await;
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame cleanup-only facade followup left runtime uninitialized"
                );
                return Ok(());
            }
            if matches!(
                sink_wave_disposition,
                SinkControlWaveDisposition::ApplyBeforeInitialSourceWave
            ) {
                if self.should_defer_sink_cleanup_wakeup_after_fail_closed_cutover(
                    sink_generation_cutover_disposition,
                    &sink_signals,
                ) {
                    eprintln!(
                        "fs_meta_runtime_app: on_control_frame deferred sink cleanup wakeup after fail-closed generation cutover"
                    );
                } else {
                    #[cfg(test)]
                    note_sink_apply_entry_for_tests(self.instance_id);
                    #[cfg(test)]
                    maybe_pause_before_sink_apply().await;
                    if debug_control_frame {
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals begin"
                        );
                    }
                    if let Err(err) = self
                    .apply_sink_signals_with_recovery(
                        &fixed_bind_session,
                        &sink_signals,
                        !sink_cleanup_only_while_uninitialized,
                        matches!(
                            sink_generation_cutover_disposition,
                            SinkGenerationCutoverDisposition::FailClosedSharedGenerationCutover
                                | SinkGenerationCutoverDisposition::FailClosedColdSuccessorCandidateOnRetryableReset
                        ),
                        matches!(
                            sink_generation_cutover_disposition,
                            SinkGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
                                | SinkGenerationCutoverDisposition::FailClosedColdSuccessorCandidateOnRetryableReset
                        ),
                    )
                    .await
                {
                    eprintln!(
                        "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals err={}",
                        err.as_error()
                    );
                    return Err(err.into());
                }
                    if debug_control_frame {
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals ok"
                        );
                    }
                    sink_wave_applied_before_source = true;
                }
            }
            match source_wave_disposition {
                SourceControlWaveDisposition::CleanupOnlyWhileUninitialized => {
                    let preserve_retained_source_replay =
                        self.runtime_control_state().source_state_replay_required()
                            && self.source_retained_replay_has_active_route_state().await;
                    if !preserve_retained_source_replay {
                        self.record_retained_source_control_state(&source_signals)
                            .await;
                    }
                    self.apply_source_signals_to_runtime_endpoint_gate(&source_signals)
                        .map_err(RuntimeWorkerControlApplyFailure::from)?;
                    if !self
                        .retained_sink_control_state
                        .lock()
                        .await
                        .active_by_route
                        .is_empty()
                    {
                        self.update_runtime_control_state(|state| state.require_sink_replay());
                    }
                    eprintln!(
                        "fs_meta_runtime_app: on_control_frame source cleanup-only followup left runtime uninitialized"
                    );
                }
                SourceControlWaveDisposition::SteadyTickNoop => {}
                SourceControlWaveDisposition::RetainedTickGateOnlyWhileSinkReplayPending => {
                    self.record_retained_source_control_state(&source_signals)
                        .await;
                    self.record_shared_source_route_claims(&source_signals)
                        .await;
                    self.apply_source_signals_to_runtime_endpoint_gate(&source_signals)
                        .map_err(RuntimeWorkerControlApplyFailure::from)?;
                }
                SourceControlWaveDisposition::EmptyRootRouteLivenessOnly => {
                    self.record_retained_source_control_state(&source_signals)
                        .await;
                    self.record_shared_source_route_claims(&source_signals)
                        .await;
                    self.apply_source_signals_to_runtime_endpoint_gate(&source_signals)
                        .map_err(RuntimeWorkerControlApplyFailure::from)?;
                    self.finish_source_control_apply_and_publish_source_repair(
                        &mut source_control_apply_inflight,
                        true,
                    )
                    .await?;
                }
                SourceControlWaveDisposition::ApplySignals => {
                    if (sink_signals.is_empty()
                        || Self::sink_signals_are_host_grant_change_only(&sink_signals))
                        && (facade_publication_signals.is_empty()
                            || Self::facade_signals_are_host_grant_change_only(
                                &facade_publication_signals,
                            ))
                        && !facade_claim_signals_present
                        && self
                            .apply_current_source_route_semantics_without_worker(&source_signals)
                            .await?
                    {
                        if matches!(
                        source_generation_cutover_disposition,
                        SourceGenerationCutoverDisposition::FailClosedRestartDeferredRetirePending
                    ) && source_signals
                            .iter()
                            .all(Self::source_signal_is_drained_retire_cleanup)
                        {
                            self.mark_control_uninitialized_after_generation_cutover_cleanup_with_session(
                            &fixed_bind_session,
                            false,
                        )
                        .await;
                            if !self.source_retained_replay_has_active_route_state().await {
                                self.update_runtime_control_state(|state| {
                                    state.clear_source_replay()
                                });
                            }
                            self.finish_source_control_apply_and_publish_source_repair(
                                &mut source_control_apply_inflight,
                                false,
                            )
                            .await?;
                            return Ok(());
                        }
                        self.finish_source_control_apply_and_publish_source_repair(
                            &mut source_control_apply_inflight,
                            true,
                        )
                        .await?;
                        return Ok(());
                    }
                    let source_forward_route_semantics_can_stay_app_local =
                        !Self::source_signals_are_post_initial_route_cutover(&source_signals)
                            && ((sink_signals.is_empty()
                                && facade_signals_for_generation_cutover.is_empty()
                                && !runtime_gate.sink_state_replay_required())
                                || (!self
                                    .source_generation_cutover_replay_deferred
                                    .load(Ordering::Acquire)
                                    && runtime_gate.sink_state_replay_required()
                                    && !sink_signals.is_empty()
                                    && facade_signals_for_generation_cutover.is_empty())
                                || (!self
                                    .source_generation_cutover_replay_deferred
                                    .load(Ordering::Acquire)
                                    && runtime_gate.sink_state_replay_required()
                                    && sink_signals.is_empty()
                                    && sink_status_publication_present
                                    && !facade_claim_signals_present
                                    && facade_publication_signals
                                        .iter()
                                        .all(facade_publication_signal_is_sink_status_activate)));
                    if self
                        .source_generation_cutover_apply_should_defer_inline(
                            &source_signals,
                            source_generation_cutover_disposition,
                            source_forward_route_semantics_can_stay_app_local,
                            !sink_signals.is_empty(),
                            !facade_signals_for_generation_cutover.is_empty(),
                        )
                        .await
                    {
                        if matches!(
                            source_generation_cutover_disposition,
                            SourceGenerationCutoverDisposition::FailClosedRestartDeferredRetirePending
                        ) && source_signals
                            .iter()
                            .all(Self::source_signal_is_drained_retire_cleanup)
                            && sink_signals.iter().all(Self::sink_signal_is_cleanup_only)
                        {
                            let preserve_retained_source_replay = self
                                .source_retained_replay_has_active_route_state()
                                .await
                                && ((self.runtime_control_state().source_state_replay_required()
                                    || self.peer_source_query_route_active_for_replay_preservation()
                                    || !sink_signals.is_empty())
                                    && Self::source_signals_include_source_control_route(
                                        &source_signals,
                                    ));
                            if preserve_retained_source_replay {
                                let filtered_source_signals = self
                                    .filter_shared_source_route_deactivates(&source_signals)
                                    .await;
                                self.record_shared_source_route_claims(&filtered_source_signals)
                                    .await;
                                self.apply_source_signals_to_runtime_endpoint_gate(
                                    &filtered_source_signals,
                                )
                                .map_err(RuntimeWorkerControlApplyFailure::from)?;
                                self.refresh_source_rescan_proxy_ready_from_retained_endpoint()
                                    .await;

                                let filtered_sink_signals = self
                                    .filter_sink_facade_dependent_route_activates_during_pending_fixed_bind_claim_with_session(
                                        &self
                                            .filter_shared_sink_route_deactivates(
                                                &self.filter_stale_sink_ticks(&sink_signals).await,
                                            )
                                            .await,
                                        &fixed_bind_session,
                                    )
                                    .await;
                                self.record_retained_sink_control_state(&filtered_sink_signals)
                                    .await;
                                self.record_shared_sink_route_claims(&filtered_sink_signals)
                                    .await;
                                self.apply_sink_signals_to_runtime_endpoint_gate(
                                    &filtered_sink_signals,
                                )
                                .map_err(RuntimeWorkerControlApplyFailure::from)?;
                                self.mark_control_uninitialized_after_deferred_source_replay_without_scheduled_repair_with_session(
                                    &fixed_bind_session,
                                    !sink_signals.is_empty(),
                                )
                                .await;
                            } else {
                                self.apply_restart_deferred_events_cleanup_locally(
                                    &fixed_bind_session,
                                    &source_signals,
                                    &sink_signals,
                                )
                                .await?;
                                self.mark_control_uninitialized_after_generation_cutover_cleanup_with_session(
                                    &fixed_bind_session,
                                    false,
                                )
                                .await;
                                self.update_runtime_control_state(|state| {
                                    state.clear_source_replay()
                                });
                            }
                            eprintln!(
                                "fs_meta_runtime_app: on_control_frame kept restart-deferred events cleanup local after fail-closed generation cutover"
                            );
                            return Ok(());
                        }
                        self.defer_source_generation_cutover_replay_inline(&source_signals)
                            .await?;
                        let require_sink_replay = retained_sink_state_present_at_entry
                            || !sink_signals.is_empty()
                            || matches!(
                                sink_wave_disposition,
                                SinkControlWaveDisposition::ReplayRetained
                                    | SinkControlWaveDisposition::ApplySignals { .. }
                                    | SinkControlWaveDisposition::ApplyBeforeInitialSourceWave
                            );
                        self.mark_control_uninitialized_after_deferred_source_replay_with_session(
                            &fixed_bind_session,
                            require_sink_replay,
                        )
                        .await;
                        if matches!(
                        source_generation_cutover_disposition,
                        SourceGenerationCutoverDisposition::FailClosedRestartDeferredRetirePending
                    ) && !self.source_retained_replay_has_active_route_state().await
                        {
                            self.update_runtime_control_state(|state| state.clear_source_replay());
                        }
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame deferred inline source replay after fail-closed generation cutover"
                        );
                        return Ok(());
                    }
                    if sink_signals.is_empty()
                        && facade_publication_signals.is_empty()
                        && !facade_claim_signals_present
                        && self.runtime_control_state().source_state_replay_required()
                        && Self::source_signals_are_retained_route_cleanup_only(&source_signals)
                    {
                        let filtered_source_signals = self
                            .filter_shared_source_route_deactivates(&source_signals)
                            .await;
                        self.source
                            .record_retained_control_signals(&filtered_source_signals)
                            .await;
                        self.record_shared_source_route_claims(&filtered_source_signals)
                            .await;
                        self.apply_source_signals_to_runtime_endpoint_gate(
                            &filtered_source_signals,
                        )
                        .map_err(RuntimeWorkerControlApplyFailure::from)?;
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame kept retained source route cleanup local while replay pending"
                        );
                        return Ok(());
                    }
                    #[cfg(test)]
                    note_source_apply_entry_for_tests(self.instance_id);
                    #[cfg(test)]
                    maybe_pause_before_source_apply().await;
                    if debug_control_frame {
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame source.apply_orchestration_signals begin"
                        );
                    }
                    if let Err(err) = self
                        .apply_source_signals_with_recovery(
                            &fixed_bind_session,
                            &source_signals,
                            control_initialized_at_entry,
                            source_generation_cutover_disposition,
                            !sink_wave_applied_before_source
                                && (retained_sink_state_present_at_entry
                                    || !sink_signals.is_empty()
                                    || matches!(
                                        sink_wave_disposition,
                                        SinkControlWaveDisposition::ReplayRetained
                                            | SinkControlWaveDisposition::ApplySignals { .. }
                                            | SinkControlWaveDisposition::ApplyBeforeInitialSourceWave
                                    )),
                        )
                        .await
                    {
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame source.apply_orchestration_signals err={}",
                            err.as_error()
                        );
                        return Err(err.into());
                    }
                    if debug_control_frame {
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame source.apply_orchestration_signals ok"
                        );
                    }
                    if matches!(
                        source_generation_cutover_disposition,
                        SourceGenerationCutoverDisposition::FailClosedRestartDeferredRetirePending
                    ) && source_signals
                        .iter()
                        .all(Self::source_signal_is_drained_retire_cleanup)
                        && sink_signals.is_empty()
                        && facade_publication_signals.is_empty()
                        && !facade_claim_signals_present
                    {
                        self.finish_source_control_apply_and_publish_source_repair(
                            &mut source_control_apply_inflight,
                            false,
                        )
                        .await?;
                        return Ok(());
                    }
                    self.finish_source_control_apply_and_publish_source_repair(
                        &mut source_control_apply_inflight,
                        true,
                    )
                    .await?;
                    if runtime_gate.initial_mixed_source_to_sink_pretrigger_eligible(
                        control_initialized_at_entry,
                        self.runtime_control_state(),
                        !sink_signals.is_empty(),
                    ) {
                        #[cfg(test)]
                        maybe_pause_before_initial_mixed_source_to_sink_pretrigger().await;
                        let initial_mixed_expected_groups =
                            Self::runtime_scoped_facade_group_ids(&self.source, &self.sink)
                                .await
                                .map_err(RuntimeWorkerObservationFailure::from)?
                                .into_iter()
                                .collect::<std::collections::BTreeSet<_>>();
                        if !initial_mixed_expected_groups.is_empty() {
                            // Initial mixed source/sink activation can leave the local sink
                            // with only scheduled-group truth unless we explicitly kick the
                            // first source->sink convergence pass after the source worker
                            // finishes applying its control wave. This must not depend on a
                            // facade sink-status publication lane because local sink readiness
                            // can be required before any facade publication exists.
                            pretriggered_source_to_sink_convergence_epoch = Some(
                                self.source
                                    .submit_rescan_when_ready_epoch_with_failure()
                                    .await
                                    .map_err(RuntimeWorkerObservationFailure::from)?,
                            );
                        }
                    }
                    if !control_initialized_at_entry && sink_signals.is_empty() {
                        let retained_sink_routes_present = !self
                            .retained_sink_control_state
                            .lock()
                            .await
                            .active_by_route
                            .is_empty();
                        if retained_sink_routes_present {
                            if sink_status_publication_present {
                                // Source-led uninitialized mixed recovery still owns the
                                // source wave only. The followup source->sink convergence
                                // belongs to the local sink republish machine; kicking it
                                // here races a second owner against the deferred sink
                                // recovery lane and can leave the sink probing against a
                                // partial pretriggered publication.
                                pretriggered_source_to_sink_convergence_epoch = None;
                            }
                            // A later source-only recovery can reopen the runtime before the sink
                            // worker has replayed its retained control state into the current
                            // generation. Keep the sink replay armed so peer status/query routes do
                            // not resume against a zero-state sink snapshot.
                            self.update_runtime_control_state(|state| state.require_sink_replay());
                        }
                    }
                    if !self.control_initialized()
                        && self.runtime_control_state().source_state_replay_required()
                        && (!sink_signals.is_empty()
                            || matches!(
                                sink_wave_disposition,
                                SinkControlWaveDisposition::ReplayRetained
                                    | SinkControlWaveDisposition::ApplySignals { .. }
                                    | SinkControlWaveDisposition::ApplyBeforeInitialSourceWave
                            ))
                    {
                        self.ensure_runtime_endpoints_started().await?;
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame deferred same-frame sink replay after source fail-closed generation cutover"
                        );
                        return Ok(());
                    }
                }
                SourceControlWaveDisposition::ReplayRetained => {
                    let source_status_recovery_followup_present = facade_publication_signals
                        .iter()
                        .any(facade_publication_signal_is_source_status_activate);
                    if !source_status_recovery_followup_present
                        && self
                            .source_generation_cutover_replay_should_defer_inline(&[])
                            .await
                    {
                        self.defer_source_generation_cutover_replay_inline(&[])
                            .await?;
                        let require_sink_replay = retained_sink_state_present_at_entry
                            || !sink_signals.is_empty()
                            || matches!(
                                sink_wave_disposition,
                                SinkControlWaveDisposition::ReplayRetained
                                    | SinkControlWaveDisposition::ApplySignals { .. }
                                    | SinkControlWaveDisposition::ApplyBeforeInitialSourceWave
                            );
                        self.mark_control_uninitialized_after_deferred_source_replay_without_scheduled_repair_with_session(
                            &fixed_bind_session,
                            require_sink_replay,
                        )
                        .await;
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame deferred inline retained source replay after fail-closed generation cutover"
                        );
                        return Ok(());
                    }
                    #[cfg(test)]
                    note_source_apply_entry_for_tests(self.instance_id);
                    #[cfg(test)]
                    maybe_pause_before_source_apply().await;
                    if debug_control_frame {
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame source.replay_retained begin"
                        );
                    }
                    self.apply_source_signals_with_recovery(
                        &fixed_bind_session,
                        &[],
                        control_initialized_at_entry,
                        SourceGenerationCutoverDisposition::None,
                        !sink_wave_applied_before_source
                            && (retained_sink_state_present_at_entry
                                || !sink_signals.is_empty()
                                || matches!(
                                    sink_wave_disposition,
                                    SinkControlWaveDisposition::ReplayRetained
                                        | SinkControlWaveDisposition::ApplySignals { .. }
                                        | SinkControlWaveDisposition::ApplyBeforeInitialSourceWave
                                )),
                    )
                    .await?;
                    if debug_control_frame {
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame source.replay_retained ok"
                        );
                    }
                    self.finish_source_control_apply_and_publish_source_repair(
                        &mut source_control_apply_inflight,
                        true,
                    )
                    .await?;
                    if !self.control_initialized()
                        && self.runtime_control_state().source_state_replay_required()
                        && (!sink_signals.is_empty()
                            || matches!(
                                sink_wave_disposition,
                                SinkControlWaveDisposition::ReplayRetained
                                    | SinkControlWaveDisposition::ApplySignals { .. }
                                    | SinkControlWaveDisposition::ApplyBeforeInitialSourceWave
                            ))
                    {
                        self.ensure_runtime_endpoints_started().await?;
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame deferred same-frame sink replay after source fail-closed retained replay"
                        );
                        return Ok(());
                    }
                }
                SourceControlWaveDisposition::Idle => {}
            }
            if !source_signals.is_empty() {
                self.ensure_runtime_endpoints_started().await?;
            }
            self.finish_source_control_apply_and_publish_source_repair(
                &mut source_control_apply_inflight,
                false,
            )
            .await?;
            let mut replayed_sink_state_after_uninitialized_source_recovery = false;
            let apply_full_sink_recovery_synchronously = !control_initialized_at_entry
                && retained_sink_state_present_at_entry
                && !source_signals.is_empty()
                && !sink_signals.is_empty();
            match sink_wave_disposition {
                SinkControlWaveDisposition::ApplyBeforeInitialSourceWave => {}
                SinkControlWaveDisposition::CleanupOnlyQueryWhileUninitialized => {
                    eprintln!(
                        "fs_meta_runtime_app: on_control_frame sink cleanup-only query followup left runtime uninitialized"
                    );
                }
                SinkControlWaveDisposition::CleanupOnlyWhileUninitialized => {
                    self.apply_restart_deferred_events_cleanup_locally(
                        &fixed_bind_session,
                        &[],
                        &sink_signals,
                    )
                    .await?;
                    eprintln!(
                        "fs_meta_runtime_app: on_control_frame sink cleanup-only followup left runtime uninitialized"
                    );
                }
                SinkControlWaveDisposition::RetainedTickGateOnlyWhileSourceReplayPending => {
                    let gate_sink_signals = self.filter_stale_sink_ticks(&sink_signals).await;
                    self.record_retained_sink_control_state(&gate_sink_signals)
                        .await;
                    self.record_shared_sink_route_claims(&gate_sink_signals)
                        .await;
                    self.apply_sink_signals_to_runtime_endpoint_gate(&gate_sink_signals)
                        .map_err(RuntimeWorkerControlApplyFailure::from)?;
                }
                SinkControlWaveDisposition::RetainedTickGateOnlyWhileSinkReplayPending => {
                    let gate_sink_signals = self.filter_stale_sink_ticks(&sink_signals).await;
                    self.record_retained_sink_control_state(&gate_sink_signals)
                        .await;
                    self.record_shared_sink_route_claims(&gate_sink_signals)
                        .await;
                    self.apply_sink_signals_to_runtime_endpoint_gate(&gate_sink_signals)
                        .map_err(RuntimeWorkerControlApplyFailure::from)?;
                }
                SinkControlWaveDisposition::SteadyTickNoop => {}
                SinkControlWaveDisposition::ApplySignals {
                    wait_for_status_republish_after_apply,
                } => {
                    if self.should_defer_sink_cleanup_wakeup_after_fail_closed_cutover(
                        sink_generation_cutover_disposition,
                        &sink_signals,
                    ) {
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame deferred sink cleanup wakeup after fail-closed generation cutover"
                        );
                    } else {
                        let mut sink_apply_as_initial_audit_catchup = false;
                        if !apply_full_sink_recovery_synchronously {
                            match self
                                .sink_generation_cutover_inline_action(
                                    &sink_signals,
                                    sink_generation_cutover_disposition,
                                )
                                .await
                            {
                                SinkGenerationCutoverInlineAction::DeferReplay => {
                                    let mut deferred_sink_tail_plan = SinkRecoveryTailPlan::new();
                                    let deferred_local_sink_replay_signals =
                                        if sink_status_publication_present {
                                            self.current_generation_retained_sink_replay_signals_for_local_republish()
                                                .await
                                        } else {
                                            Vec::new()
                                        };
                                    if sink_status_publication_present {
                                        let expected_groups =
                                            Self::runtime_scoped_facade_group_ids(
                                                &self.source,
                                                &self.sink,
                                            )
                                            .await
                                            .map_err(RuntimeWorkerObservationFailure::from)?
                                            .into_iter()
                                            .collect::<std::collections::BTreeSet<_>>();
                                        if !expected_groups.is_empty() {
                                            deferred_sink_tail_plan.gate_reopen_disposition =
                                            SinkRecoveryGateReopenDisposition::WaitForLocalSinkStatusRepublish {
                                                expected_groups,
                                            };
                                        }
                                    }
                                    self.defer_sink_generation_cutover_replay_inline(
                                        &fixed_bind_session,
                                        &sink_signals,
                                    )
                                    .await?;
                                    self.apply_sink_recovery_tail_plan(
                                        fixed_bind_session,
                                        request_sensitive,
                                        sink_cleanup_only_while_uninitialized,
                                        deferred_sink_tail_plan,
                                        deferred_local_sink_replay_signals,
                                        facade_publication_signals,
                                    )
                                    .await?;
                                    return Ok(());
                                }
                                SinkGenerationCutoverInlineAction::ApplyAsInitialAuditCatchup => {
                                    sink_apply_as_initial_audit_catchup = true;
                                    eprintln!(
                                        "fs_meta_runtime_app: sink control applying post-initial route cutover as initial audit materialization catch-up"
                                    );
                                }
                                SinkGenerationCutoverInlineAction::Apply => {}
                            }
                        }
                        #[cfg(test)]
                        note_sink_apply_entry_for_tests(self.instance_id);
                        #[cfg(test)]
                        maybe_pause_before_sink_apply().await;
                        if debug_control_frame {
                            eprintln!(
                                "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals begin"
                            );
                        }
                        if let Err(err) = self
                        .apply_sink_signals_with_recovery(
                            &fixed_bind_session,
                            &sink_signals,
                            !sink_cleanup_only_while_uninitialized,
                            matches!(
                                sink_generation_cutover_disposition,
                                SinkGenerationCutoverDisposition::FailClosedSharedGenerationCutover
                                    | SinkGenerationCutoverDisposition::FailClosedColdSuccessorCandidateOnRetryableReset
                            ) && !sink_apply_as_initial_audit_catchup,
                            matches!(
                                sink_generation_cutover_disposition,
                                SinkGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
                                    | SinkGenerationCutoverDisposition::FailClosedColdSuccessorCandidateOnRetryableReset
                            ) && !sink_apply_as_initial_audit_catchup,
                        )
                        .await
                    {
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals err={}",
                            err.as_error()
                        );
                        return Err(err.into());
                    }
                        if debug_control_frame {
                            eprintln!(
                                "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals ok"
                            );
                        }
                        if wait_for_status_republish_after_apply {
                            if let Some(expected_groups) = self
                                .wait_for_sink_status_republish_readiness_after_recovery(None)
                                .await?
                            {
                                self.wait_for_local_sink_status_republish_after_recovery(
                                    &expected_groups,
                                )
                                .await?;
                            }
                        }
                    }
                    if !control_initialized_at_entry
                        && retained_sink_state_present_at_entry
                        && !source_signals.is_empty()
                        && sink_status_publication_present
                    {
                        let expected_groups =
                            Self::runtime_scoped_facade_group_ids(&self.source, &self.sink)
                                .await
                                .map_err(RuntimeWorkerObservationFailure::from)?
                                .into_iter()
                                .collect::<std::collections::BTreeSet<_>>();
                        sink_recovery_tail_plan
                            .push_immediate_local_sink_status_republish_wait(expected_groups);
                    }
                }
                SinkControlWaveDisposition::ReplayRetained => {
                    let source_led_sink_status_republish_recovery = !control_initialized_at_entry
                        && !source_signals.is_empty()
                        && sink_signals.is_empty()
                        && sink_status_publication_present
                        && !self.runtime_control_state().source_state_replay_required();
                    let source_status_recovery_followup_present = facade_publication_signals
                        .iter()
                        .any(facade_publication_signal_is_source_status_activate);
                    let source_led_source_only_recovery = !source_signals.is_empty()
                        && sink_signals.is_empty()
                        && !sink_status_publication_present
                        && !self.runtime_control_state().source_state_replay_required()
                        && self.runtime_control_state().sink_state_replay_required();
                    if source_led_source_only_recovery {
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame retained sink replay left for local sink-status recovery after source-led source-only recovery"
                        );
                    } else if self
                        .sink_generation_cutover_replay_should_defer_inline()
                        .await
                        && !source_led_sink_status_republish_recovery
                        && !source_status_recovery_followup_present
                        && !runtime_unit_exposure_present
                    {
                        self.mark_control_uninitialized_after_deferred_sink_replay_with_session(
                            &fixed_bind_session,
                        )
                        .await;
                        eprintln!(
                            "fs_meta_runtime_app: on_control_frame deferred inline retained sink replay after fail-closed generation cutover"
                        );
                        return Ok(());
                    } else {
                        #[cfg(test)]
                        note_sink_apply_entry_for_tests(self.instance_id);
                        #[cfg(test)]
                        maybe_pause_before_sink_apply().await;
                        if debug_control_frame {
                            eprintln!(
                                "fs_meta_runtime_app: on_control_frame sink.replay_retained begin"
                            );
                        }
                        let replay_signals = current_generation_sink_replay_tick(
                            &self.node_id.0,
                            &source_signals,
                            &facade_publication_signals,
                        )
                        .into_iter()
                        .collect::<Vec<_>>();
                        self.apply_sink_signals_with_recovery(
                            &fixed_bind_session,
                            &replay_signals,
                            true,
                            false,
                            false,
                        )
                        .await?;
                        if debug_control_frame {
                            eprintln!(
                                "fs_meta_runtime_app: on_control_frame sink.replay_retained ok"
                            );
                        }
                        replayed_sink_state_after_uninitialized_source_recovery =
                            !control_initialized_at_entry;
                        if control_initialized_at_entry && sink_status_publication_present {
                            let expected_groups =
                                Self::runtime_scoped_facade_group_ids(&self.source, &self.sink)
                                    .await
                                    .map_err(RuntimeWorkerObservationFailure::from)?
                                    .into_iter()
                                    .collect::<std::collections::BTreeSet<_>>();
                            sink_recovery_tail_plan
                                .push_immediate_local_sink_status_republish_wait(expected_groups);
                        }
                    }
                }
                SinkControlWaveDisposition::Idle => {}
            }
            let source_led_uninitialized_sink_status_republish_recovery =
                !control_initialized_at_entry
                    && retained_sink_state_present_at_entry
                    && !source_signals.is_empty()
                    && sink_signals.is_empty()
                    && sink_status_publication_present
                    && self.runtime_control_state().sink_state_replay_required();
            let sink_status_recovery_after_uninitialized_source_recovery =
                replayed_sink_state_after_uninitialized_source_recovery
                    || source_led_uninitialized_sink_status_republish_recovery;
            let deferred_local_sink_replay_signals =
                if sink_status_recovery_after_uninitialized_source_recovery
                    && sink_status_publication_present
                {
                    self.current_generation_retained_sink_replay_signals_for_local_republish()
                        .await
                } else {
                    Vec::new()
                };
            sink_recovery_tail_plan.gate_reopen_disposition = self
                .determine_sink_recovery_gate_reopen_disposition(
                    sink_status_recovery_after_uninitialized_source_recovery,
                    facade_publication_signals.iter().any(|signal| {
                        matches!(
                            signal,
                            FacadeControlSignal::Activate {
                                unit: FacadeRuntimeUnit::Facade,
                                ..
                            }
                        ) || Self::facade_publication_signal_is_query_activate(signal)
                    }),
                    sink_status_publication_present,
                    &source_signals,
                    &sink_signals,
                    pretriggered_source_to_sink_convergence_epoch,
                    &deferred_local_sink_replay_signals,
                )
                .await?;
            let gate_reopen_deferred = sink_recovery_tail_plan
                .gate_reopen_disposition
                .defers_gate_reopen();
            if !source_cleanup_only_while_uninitialized {
                self.apply_sink_recovery_tail_plan(
                    fixed_bind_session,
                    request_sensitive,
                    sink_cleanup_only_while_uninitialized,
                    sink_recovery_tail_plan,
                    deferred_local_sink_replay_signals,
                    facade_publication_signals,
                )
                .await?;
            }
            if !source_cleanup_only_while_uninitialized
                && !sink_cleanup_only_while_uninitialized
                && !gate_reopen_deferred
                && self.runtime_control_state().replay_fully_cleared()
            {
                self.update_runtime_control_state(|state| state.mark_initialized());
                self.control_failure_uninitialized
                    .store(false, Ordering::Release);
                self.publish_recovered_facade_state_after_retained_replay()
                    .await;
            }
            if debug_control_frame {
                eprintln!("fs_meta_runtime_app: on_control_frame done");
            }
            Ok(())
        })
    }

    pub async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        let fixed_bind_session = self
            .begin_fixed_bind_lifecycle_root_session(
                FixedBindLifecycleRootReason::PublicOnControlFrame,
            )
            .await;
        self.service_on_control_frame_with_failure_and_session(envelopes, fixed_bind_session)
            .await
            .map_err(RuntimeControlFrameFailure::into_error)
    }

    async fn wait_for_conflicting_active_fixed_bind_facade_request_drain(&self) {
        let Some(bind_addr) = self
            .config
            .api
            .resolve_for_candidate_ids(&[self.config.api.facade_resource_id.clone()])
            .map(|resolved| resolved.bind_addr)
            .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
        else {
            return;
        };
        let Some(owner) = active_fixed_bind_facade_owner_for(&bind_addr, self.instance_id) else {
            return;
        };
        owner.api_control_gate.close_management_write_gate();
        owner.api_request_tracker.wait_for_drain().await;
        owner.api_control_gate.wait_for_facade_request_drain().await;
    }

    async fn service_close_with_failure_and_session(
        &self,
        fixed_bind_session: FixedBindLifecycleSession,
    ) -> std::result::Result<(), RuntimeCloseFailure> {
        self.api_control_gate.close_management_write_gate();
        self.api_request_tracker.wait_for_drain().await;
        self.wait_for_conflicting_active_fixed_bind_facade_request_drain()
            .await;
        self.retained_active_facade_continuity
            .store(false, Ordering::Release);
        self.pending_fixed_bind_has_suppressed_dependent_routes
            .store(false, Ordering::Release);
        self.clear_shared_source_route_claims_for_instance().await;
        self.clear_shared_sink_route_claims_for_instance().await;
        let mut fixed_bind_addrs = std::collections::BTreeSet::new();
        if let Some(active) = self
            .api_task
            .lock()
            .await
            .as_ref()
            .map(|active| active.resource_ids.clone())
            && let Some(bind_addr) = self
                .config
                .api
                .resolve_for_candidate_ids(&active)
                .map(|resolved| resolved.bind_addr)
                .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
        {
            fixed_bind_addrs.insert(bind_addr);
        }
        if let Some(pending_bind_addr) = self
            .pending_facade
            .lock()
            .await
            .as_ref()
            .map(|pending| pending.resolved.bind_addr.clone())
            .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
        {
            fixed_bind_addrs.insert(pending_bind_addr);
        }
        *self.pending_facade.lock().await = None;
        *self.facade_spawn_in_progress.lock().await = None;
        Self::clear_pending_facade_status(&self.facade_pending_status);
        let fixed_bind_session = self
            .drive_fixed_bind_lifecycle_request_with_session(
                fixed_bind_session,
                FixedBindLifecycleRequest::Shutdown,
            )
            .await
            .map_err(RuntimeCloseFailure::from)?;
        self.closing.store(true, Ordering::Release);
        self.update_runtime_control_state(|state| {
            state.mark_uninitialized_preserving_replay();
        });
        self.api_control_gate.set_ready(false);
        let fixed_bind_session = self
            .rebuild_fixed_bind_lifecycle_session(
                &fixed_bind_session,
                FixedBindLifecycleRebuildReason::PublishFacadeServiceStateAfterServiceCloseControlUninitialized,
            )
            .await;
        let _ = self
            .publish_facade_service_state_with_session(&fixed_bind_session)
            .await;
        clear_owned_process_facade_claim(self.instance_id);
        for bind_addr in fixed_bind_addrs {
            clear_active_fixed_bind_facade_owner(&bind_addr, self.instance_id);
            clear_pending_fixed_bind_handoff_ready(&bind_addr);
        }
        self.wait_for_shared_worker_control_handoff().await;
        let mut endpoint_tasks = std::mem::take(&mut *self.runtime_endpoint_tasks.lock().await);
        for task in &mut endpoint_tasks {
            task.shutdown(Duration::from_secs(2)).await;
        }
        self.runtime_endpoint_routes.lock().await.clear();
        self.source
            .close_with_failure()
            .await
            .map_err(RuntimeCloseFailure::from)?;
        self.sink
            .close_with_failure()
            .await
            .map_err(RuntimeCloseFailure::from)?;
        if let Some(handle) = self.pump_task.lock().await.take() {
            handle.abort();
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let fixed_bind_session = self
            .begin_fixed_bind_lifecycle_root_session(FixedBindLifecycleRootReason::PublicClose)
            .await;
        self.service_close_with_failure_and_session(fixed_bind_session)
            .await
            .map_err(RuntimeCloseFailure::into_error)
    }

    async fn query_tree_with_failure(
        &self,
        params: &InternalQueryRequest,
    ) -> std::result::Result<
        std::collections::BTreeMap<String, TreeGroupPayload>,
        RuntimeAppQueryFailure,
    > {
        let events = self.sink.materialized_query_with_failure(params).await?;
        let mut grouped = std::collections::BTreeMap::<String, TreeGroupPayload>::new();
        for event in &events {
            let payload = rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
                .map_err(|e| {
                    RuntimeAppQueryFailure::from_cause(CnxError::Internal(format!(
                        "decode tree response failed: {e}"
                    )))
                })?;
            let MaterializedQueryPayload::Tree(response) = payload else {
                return Err(RuntimeAppQueryFailure::from_cause(CnxError::Internal(
                    "unexpected stats payload for query_tree".into(),
                )));
            };
            grouped.insert(event.metadata().origin_id.0.clone(), response);
        }
        Ok(grouped)
    }

    pub async fn query_tree(
        &self,
        params: &InternalQueryRequest,
    ) -> Result<std::collections::BTreeMap<String, TreeGroupPayload>> {
        self.query_tree_with_failure(params)
            .await
            .map_err(RuntimeAppQueryFailure::into_error)
    }

    async fn query_stats_with_failure(
        &self,
        path: &[u8],
    ) -> std::result::Result<SubtreeStats, RuntimeAppQueryFailure> {
        let events = self.sink.subtree_stats_with_failure(path).await?;
        let mut agg = SubtreeStats::default();
        for event in &events {
            let stats =
                rmp_serde::from_slice::<SubtreeStats>(event.payload_bytes()).map_err(|e| {
                    RuntimeAppQueryFailure::from_cause(CnxError::Internal(format!(
                        "decode stats response failed: {e}"
                    )))
                })?;
            agg.total_nodes += stats.total_nodes;
            agg.total_files += stats.total_files;
            agg.total_dirs += stats.total_dirs;
            agg.total_size += stats.total_size;
            agg.attested_count += stats.attested_count;
            agg.blind_spot_count += stats.blind_spot_count;
        }
        Ok(agg)
    }

    pub async fn query_stats(&self, path: &[u8]) -> Result<SubtreeStats> {
        self.query_stats_with_failure(path)
            .await
            .map_err(RuntimeAppQueryFailure::into_error)
    }

    async fn source_status_snapshot_with_failure(
        &self,
    ) -> std::result::Result<crate::source::SourceStatusSnapshot, RuntimeWorkerObservationFailure>
    {
        self.source
            .status_snapshot_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)
    }

    pub async fn source_status_snapshot(&self) -> Result<crate::source::SourceStatusSnapshot> {
        self.source_status_snapshot_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::into_error)
    }

    async fn sink_status_snapshot_with_failure(
        &self,
    ) -> std::result::Result<crate::sink::SinkStatusSnapshot, RuntimeWorkerObservationFailure> {
        self.sink
            .status_snapshot_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)
    }

    pub async fn sink_status_snapshot(&self) -> Result<crate::sink::SinkStatusSnapshot> {
        self.sink_status_snapshot_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::into_error)
    }

    async fn trigger_rescan_when_ready_epoch_with_failure(
        &self,
    ) -> std::result::Result<u64, RuntimeWorkerObservationFailure> {
        self.source
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::from)
    }

    pub async fn trigger_rescan_when_ready_epoch(&self) -> Result<u64> {
        self.trigger_rescan_when_ready_epoch_with_failure()
            .await
            .map_err(RuntimeWorkerObservationFailure::into_error)
    }

    async fn query_node_with_failure(
        &self,
        path: &[u8],
    ) -> std::result::Result<Option<QueryNode>, RuntimeAppQueryFailure> {
        self.sink
            .query_node_with_failure(path)
            .await
            .map_err(RuntimeAppQueryFailure::from)
    }

    pub async fn query_node(&self, path: &[u8]) -> Result<Option<QueryNode>> {
        self.query_node_with_failure(path)
            .await
            .map_err(RuntimeAppQueryFailure::into_error)
    }
}

impl ManagedStateProfile for FSMetaApp {
    fn managed_state_declaration(&self) -> ManagedStateDeclaration {
        ManagedStateDeclaration::new(
            "statecell authoritative journal",
            "materialized sink/query projection tree",
            "authoritative_revision",
            "observed_projection_revision",
            "shared observation evaluator drives trusted-materialized and observation_eligible",
            "generation high-water plus statecell stale-writer fencing",
        )
    }
}

pub struct FSMetaRuntimeApp {
    runtime: RuntimeLoadedServiceApp,
}

impl FSMetaRuntimeApp {
    fn init_error_message(err: CnxError) -> String {
        match err {
            CnxError::InvalidInput(msg) => msg,
            other => other.to_string(),
        }
    }

    #[cfg(test)]
    fn runtime_local_host_ref(
        cfg: &std::collections::HashMap<String, ConfigValue>,
    ) -> Option<NodeId> {
        let runtime = match cfg.get("__cnx_runtime") {
            Some(ConfigValue::Map(map)) => map,
            _ => return None,
        };
        let local_host_ref = match runtime.get("local_host_ref") {
            Some(ConfigValue::String(v)) => v.trim(),
            _ => return None,
        };
        if local_host_ref.is_empty() {
            None
        } else {
            Some(NodeId(local_host_ref.to_string()))
        }
    }

    #[cfg(test)]
    fn required_runtime_local_host_ref(
        cfg: &std::collections::HashMap<String, ConfigValue>,
    ) -> Result<NodeId> {
        Self::runtime_local_host_ref(cfg).ok_or_else(|| {
            CnxError::InvalidInput(
                "__cnx_runtime.local_host_ref is required for fs-meta local execution identity"
                    .to_string(),
            )
        })
    }

    fn runtime_worker_bindings_from_bootstrap(
        bootstrap: &RuntimeBootstrapContext,
    ) -> Result<(RuntimeWorkerBinding, RuntimeWorkerBinding)> {
        runtime_worker_client_bindings(&bootstrap.worker_bindings()?)
    }

    fn build_from_runtime_boundaries(
        runtime_boundary: Arc<dyn RuntimeBoundary>,
        data_boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> Self {
        let format_error = |err: CnxError| {
            let msg = Self::init_error_message(err);
            log::error!("fs-meta runtime init failed: {msg}");
            msg
        };
        let runtime = RuntimeLoadedServiceApp::from_runtime_config(
            runtime_boundary,
            data_boundary,
            FSMetaConfig::from_runtime_manifest_config,
            move |bootstrap, cfg| {
                let boundary_handles = boundary_handles(&bootstrap);
                let ordinary_boundary = boundary_handles.ordinary_boundary();
                let (source_worker_binding, sink_worker_binding) =
                    Self::runtime_worker_bindings_from_bootstrap(&bootstrap)?;
                let app = Arc::new(FSMetaApp::with_boundaries_and_state(
                    cfg,
                    source_worker_binding,
                    sink_worker_binding,
                    bootstrap.local_host_ref().clone(),
                    boundary_handles.data_boundary(),
                    Some(ordinary_boundary),
                    boundary_handles.state_boundary(),
                )?);
                let close_app = app.clone();
                let built = AppBuilder::new()
                    .register_role("facade")
                    .request({
                        let app = app.clone();
                        move |_context, events| {
                            let app = app.clone();
                            async move {
                                app.service_send_with_failure(&events)
                                    .await
                                    .map_err(RuntimeServiceIoFailure::into_error)
                            }
                        }
                    })
                    .stream({
                        let app = app.clone();
                        move |_context, opts| {
                            let app = app.clone();
                            async move {
                                app.service_recv_with_failure(opts)
                                    .await
                                    .map_err(RuntimeServiceIoFailure::into_error)
                            }
                        }
                    })
                    .control({
                        let app = app.clone();
                        move |_context, envelopes| {
                            let app = app.clone();
                            async move { app.on_control_frame(&envelopes).await }
                        }
                    })
                    .close(move |_context| {
                        let app = close_app.clone();
                        async move { app.close().await }
                    })
                    .build(bootstrap.service_context());
                Ok(built)
            },
            format_error,
        );
        Self { runtime }
    }

    pub fn new_without_io(boundary: Arc<dyn RuntimeBoundary>) -> Self {
        Self::build_from_runtime_boundaries(boundary, None)
    }

    pub fn new(
        boundary: Arc<dyn RuntimeBoundary>,
        data_boundary: Arc<dyn ChannelIoSubset>,
    ) -> Self {
        Self::build_from_runtime_boundaries(boundary, Some(data_boundary))
    }
}

#[async_trait]
impl RuntimeBoundaryApp for FSMetaRuntimeApp {
    async fn start(&self) -> Result<()> {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.runtime.start().await
        } else {
            shared_tokio_runtime().block_on(self.runtime.start())
        }
    }

    async fn send(&self, events: &[Event], timeout: Duration) -> Result<()> {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.runtime.send(events, timeout).await
        } else {
            shared_tokio_runtime().block_on(self.runtime.send(events, timeout))
        }
    }

    async fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.runtime.recv(opts).await
        } else {
            shared_tokio_runtime().block_on(self.runtime.recv(opts))
        }
    }

    async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.runtime.on_control_frame(envelopes).await
        } else {
            shared_tokio_runtime().block_on(self.runtime.on_control_frame(envelopes))
        }
    }

    async fn close(&self) -> Result<()> {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.runtime.close().await.or(Ok(()))
        } else {
            shared_tokio_runtime()
                .block_on(self.runtime.close())
                .or(Ok(()))
        }
    }
}

#[cfg(test)]
mod tests;
