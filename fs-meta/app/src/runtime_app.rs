use std::collections::BTreeMap;
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
    ObservationTrustPolicy, candidate_group_observation_evidence, evaluate_observation_status,
};
use crate::query::reliability::GroupReliability;
use crate::query::tree::ObservationState;
use crate::query::tree::{TreePageRoot, TreeStability};
use crate::query::{InternalQueryRequest, MaterializedQueryPayload, QueryNode, SubtreeStats};
use crate::runtime::endpoint::ManagedEndpointTask;
use crate::runtime::execution_units;
use crate::runtime::orchestration::{
    FacadeControlSignal, FacadeRuntimeUnit, SinkControlSignal, SinkRuntimeUnit,
    SourceControlSignal, split_app_control_signals,
};
use crate::runtime::routes::{
    METHOD_QUERY, METHOD_SINK_QUERY, METHOD_SINK_QUERY_PROXY, METHOD_SINK_STATUS,
    METHOD_SOURCE_FIND, METHOD_SOURCE_STATUS, ROUTE_KEY_EVENTS, ROUTE_KEY_FACADE_CONTROL,
    ROUTE_KEY_FORCE_FIND, ROUTE_KEY_QUERY, ROUTE_KEY_SINK_QUERY_INTERNAL,
    ROUTE_KEY_SINK_QUERY_PROXY, ROUTE_KEY_SINK_ROOTS_CONTROL, ROUTE_KEY_SINK_STATUS_INTERNAL,
    ROUTE_KEY_SOURCE_FIND_INTERNAL, ROUTE_KEY_SOURCE_STATUS_INTERNAL, ROUTE_TOKEN_FS_META,
    ROUTE_TOKEN_FS_META_INTERNAL, default_route_bindings, sink_query_route_bindings_for,
    source_find_request_route_for, source_find_route_bindings_for,
};
use crate::runtime::unit_gate::RuntimeUnitGate;
use crate::workers::sink::{SinkFacade, SinkFailure, SinkWorkerClientHandle};
use crate::workers::source::{SourceFacade, SourceFailure, SourceWorkerClientHandle};
use crate::{FSMetaConfig, api, source};
use async_trait::async_trait;
#[cfg(test)]
use capanix_app_sdk::runtime::ConfigValue;
use capanix_app_sdk::runtime::{
    ControlEnvelope, EventMetadata, NodeId, RecvOpts, RuntimeWorkerBinding, RuntimeWorkerBindings,
    RuntimeWorkerLauncherKind, in_memory_state_boundary,
};
use capanix_app_sdk::worker::WorkerMode;
use capanix_app_sdk::{CnxError, Event, Result, RuntimeBoundary, RuntimeBoundaryApp};
use capanix_managed_state_sdk::{ManagedStateDeclaration, ManagedStateProfile};
use capanix_runtime_entry_sdk::advanced::boundary::{
    ChannelBoundary, ChannelIoSubset, StateBoundary, boundary_handles,
};
use capanix_runtime_entry_sdk::control::{
    RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeExecDeactivate,
    RuntimeUnitTick, decode_runtime_exec_control, encode_runtime_exec_control,
    encode_runtime_unit_tick,
};
use capanix_runtime_entry_sdk::worker_runtime::RuntimeWorkerClientFactory;
use capanix_runtime_entry_sdk::{RuntimeBootstrapContext, RuntimeLoadedServiceApp};
use capanix_service_sdk::AppBuilder;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::sink::SinkFileMeta;
#[cfg(test)]
use crate::sink::{SinkGroupStatusSnapshot, SinkStatusSnapshot};
#[cfg(test)]
use crate::source::config::SourceConfig;

// Top-level fs-meta runtime-entry/bootstrap glue lowers through
// `service-sdk -> runtime-entry-sdk -> app-sdk`; lower runtime mirror/control
// carriers stay behind the sanctioned helper layer.
const ACTIVE_FACADE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);
const SOURCE_CONTROL_RECOVERY_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_FRAME_LEASE_ACQUIRE_BUDGET: Duration = Duration::from_secs(1);
const CONTROL_FRAME_LEASE_RETRY_INTERVAL: Duration = Duration::from_millis(10);
const INTERNAL_SINK_STATUS_BLOCKING_FALLBACK_BUDGET: Duration = Duration::from_millis(250);
const HOST_GRANT_CONTROL_FAST_LANE_TIMEOUT: Duration = Duration::from_secs(5);

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
        FacadeControlSignal::Activate { unit, route_key, .. }
            if matches!(unit, FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer)
                && route_key == &format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InternalStatusAvailability {
    Available,
    UnavailableLocalFacadeNotServing,
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
}

impl RuntimeControlState {
    const fn bootstrapping(source_replay_required: bool, sink_replay_required: bool) -> Self {
        Self {
            lifecycle: RuntimeLifecyclePhase::Bootstrapping,
            source_replay_required,
            sink_replay_required,
        }
    }

    #[cfg(test)]
    const fn live(source_replay_required: bool, sink_replay_required: bool) -> Self {
        Self {
            lifecycle: RuntimeLifecyclePhase::Live,
            source_replay_required,
            sink_replay_required,
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

    const fn sink_state_replay_required(self) -> bool {
        self.sink_replay_required
    }

    const fn replay_fully_cleared(self) -> bool {
        !self.source_replay_required && !self.sink_replay_required
    }

    const fn control_gate_ready(self, allow_facade_only_handoff: bool) -> bool {
        self.replay_fully_cleared() && (self.control_initialized() || allow_facade_only_handoff)
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
}

#[derive(Clone)]
struct PendingFixedBindHandoffContinuation {
    bind_addr: String,
    registrant: PendingFixedBindHandoffRegistrant,
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
                FixedBindAfterReleaseAction::WaitForProgress(remaining)
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
                self.next_action_at(now, completion_ready)
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
        FixedBindAfterReleaseAction::WaitForProgress(Duration::from_millis(800)),
        "after-release handoff should derive wait duration from the machine-owned deadline instead of leaving the outer loop to re-budget progress waits",
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingFixedBindHandoffAttemptDisposition {
    NoAttempt,
    ReleaseActiveOwner,
    ReleaseConflictingProcessClaim { owner_instance_id: u64 },
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
    PublishFacadeServiceStateBeforeFixedBindReleaseHandoff,
    PublishFacadeServiceStateBeforeFixedBindShutdown,
    RetryPendingFacadeAfterClaimReleaseFollowup,
    PublishFacadeServiceStateAfterFacadeActivateGenerationUpdate,
    PublishFacadeServiceStateAfterPendingFacadeActivate,
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
                .try_spawn_pending_facade()
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
    RetainedTickGateOnlyWhileSinkReplayPending,
    SteadyTickNoop,
    ApplySignals,
    ReplayRetained,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkControlWaveDisposition {
    Idle,
    CleanupOnlyQueryWhileUninitialized,
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
    source_tick_gate_only_while_sink_replay_pending: bool,
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
}

#[derive(Clone, Copy, Debug)]
struct SourceGenerationCutoverDecisionInput<'a> {
    control_initialized_at_entry: bool,
    source_signals: &'a [SourceControlSignal],
    sink_signals: &'a [SinkControlSignal],
    facade_signals: &'a [FacadeControlSignal],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkGenerationCutoverDisposition {
    None,
    FailClosedSharedGenerationCutover,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlFailureRecoveryLanePolicy {
    WithdrawInternalStatus,
    PreserveInternalStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlFailureReplayRequirement {
    Full,
    Source,
    SourceAndSink,
    Sink,
}

#[derive(Clone, Copy, Debug)]
struct SinkGenerationCutoverDecisionInput<'a> {
    control_initialized_at_entry: bool,
    source_signals: &'a [SourceControlSignal],
    sink_signals: &'a [SinkControlSignal],
    facade_signals: &'a [FacadeControlSignal],
    sink_signals_in_shared_generation_cutover_lane: bool,
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
            Self::DeferGateReopenUntilSinkStatusReady { expected_groups }
        })
    }

    fn defers_gate_reopen(&self) -> bool {
        matches!(self, Self::DeferGateReopenUntilSinkStatusReady { .. })
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

#[derive(Clone, Copy, Debug)]
enum RuntimeScopeConvergenceExpectation<'a> {
    ExpectedGroups(&'a RuntimeScopeExpectedGroups),
}

impl RuntimeScopeConvergenceObservation {
    fn matches_expected(&self, expected_groups: &RuntimeScopeExpectedGroups) -> bool {
        self.source_groups == expected_groups.source_groups
            && self.scan_groups == expected_groups.scan_groups
            && self.sink_groups == expected_groups.sink_groups
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
    ) -> Option<RuntimeScopeExpectedGroups> {
        let mut sink_groups = self.sink_groups.clone();
        if sink_groups.is_empty() {
            sink_groups.extend(self.source_groups.iter().cloned());
            sink_groups.extend(self.scan_groups.iter().cloned());
        }
        if sink_groups.is_empty() {
            return None;
        }
        Some(RuntimeScopeExpectedGroups::from_logical_roots(
            logical_roots,
            &sink_groups,
        ))
    }

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
                    if root.watch {
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
                        SinkRecoveryRetriggerState::Idle { count: 0 }
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
        expected_scope_groups: Option<&RuntimeScopeExpectedGroups>,
        deadline: tokio::time::Instant,
    ) -> SinkRecoveryScopeConvergenceAction {
        if expected_scope_groups.is_some_and(|expected_groups| {
            observation.matches_expectation(RuntimeScopeConvergenceExpectation::ExpectedGroups(
                expected_groups,
            ))
        }) && !self.post_recovery_source_rescan_pending()
        {
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
                    expected_scope_groups.as_ref(),
                    scope_convergence_deadline,
                ) {
                    SinkRecoveryScopeConvergenceAction::Converged => {
                        break expected_scope_groups
                            .expect("converged scope observation must define expected groups")
                            .sink_groups;
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
                        if tokio::time::Instant::now() >= sink_readiness_deadline {
                            return Err(RuntimeWorkerObservationFailure::from_cause(
                                CnxError::Internal(format!(
                                    "sink status readiness not restored after retained replay once runtime scope converged: {}",
                                    summarize_sink_status_endpoint(&snapshot)
                                )),
                            ));
                        }
                    }
                    Ok(Err(err))
                        if matches!(err.as_error(), CnxError::Timeout)
                            && self
                                .maybe_schedule_post_recovery_sink_timeout_retry(
                                    source,
                                    tokio::time::Instant::now() < sink_readiness_deadline,
                                )
                                .await
                                .map_err(RuntimeWorkerObservationFailure::from_cause)? =>
                    {
                        break;
                    }
                    Ok(Err(_err)) if tokio::time::Instant::now() < sink_readiness_deadline => {}
                    Ok(Err(err)) if matches!(err.as_error(), CnxError::Timeout) => {
                        if sink
                            .cached_status_snapshot_with_failure()
                            .ok()
                            .is_some_and(|snapshot| {
                                FSMetaApp::sink_status_snapshot_should_defer_republish_after_retained_replay_timeout(&snapshot)
                            })
                        {
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
                        if sink
                            .cached_status_snapshot_with_failure()
                            .ok()
                            .is_some_and(|snapshot| {
                                FSMetaApp::sink_status_snapshot_should_defer_republish_after_retained_replay_timeout(&snapshot)
                            })
                        {
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
        let sink_materialized_for_expected_groups =
            self.local_sink_materialized_for_expected_groups(cached_sink_progress, expected_groups);
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
        if retrigger_pending {
            return SinkRecoveryStep::TriggerSourceToSinkConvergence;
        }
        if self.local_requires_probe_before_ready() {
            if !source_publication_satisfied || !sink_materialized_for_expected_groups {
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
        self.local_state_mut().retained_sink_replay_state =
            SinkRecoveryRetainedSinkReplayState::None;
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
            state.retained_sink_replay_state = if post_return_sink_replay_signals.is_empty() {
                SinkRecoveryRetainedSinkReplayState::None
            } else {
                SinkRecoveryRetainedSinkReplayState::Pending
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
            SinkRecoveryProbeOutcome::Ready => {
                match self.local_state().retained_sink_replay_state {
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
            let scope_converged = scope_observation.matches_expectation(
                RuntimeScopeConvergenceExpectation::ExpectedGroups(&expected_scope_groups),
            );
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
                                FSMetaApp::sink_status_snapshot_should_defer_local_republish_after_retained_replay(&snapshot)
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
            Some(&expected),
            tokio::time::Instant::now() + Duration::from_secs(1)
        ),
        SinkRecoveryScopeConvergenceAction::TriggerInitialAndWait,
        "post-recovery readiness waits should derive the initial convergence trigger lane from the wait state itself",
    );
    assert_eq!(
        pretriggered.post_recovery_scope_action(
            &observation,
            Some(&expected),
            tokio::time::Instant::now() + Duration::from_secs(1)
        ),
        SinkRecoveryScopeConvergenceAction::Wait,
        "once pretriggered, the wait state should stay in pure wait mode without reopening the initial trigger helper seam",
    );
    assert_eq!(
        pretriggered.post_recovery_scope_action(
            &observation,
            Some(&expected),
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
            Some(&expected),
            tokio::time::Instant::now() + Duration::from_secs(1)
        ),
        SinkRecoveryScopeConvergenceAction::Converged,
        "post-recovery readiness waits should treat scan-only roots as converged once scan and sink lanes match the expected groups",
    );
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
fn local_sink_status_republish_timeout_defers_scheduled_only_zero_state() {
    let snapshot = crate::sink::SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            String::from("node-a"),
            vec![String::from("g1"), String::from("g2")],
        )]),
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        FSMetaApp::sink_status_snapshot_should_defer_local_republish_after_retained_replay(
            &snapshot
        ),
        "scheduled-only local sink status after retained replay is catch-up evidence; it must not turn a steady sink tick into a terminal republish failure"
    );
    assert!(
        !FSMetaApp::sink_status_snapshot_should_defer_local_republish_after_retained_replay(
            &crate::sink::SinkStatusSnapshot::default()
        ),
        "an empty status without scheduled-group evidence must still fail closed"
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
    let route_key = format!("{}.stream", ROUTE_KEY_EVENTS);
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
            Some(&expected),
            tokio::time::Instant::now() + Duration::from_secs(1),
        ),
        SinkRecoveryScopeConvergenceAction::Wait,
        "post-recovery readiness must keep waiting while the owned pretriggered source-rescan request has not yet been observed, even if runtime scope convergence arrives early",
    );
}

fn sink_group_status_counts_as_ready(group: &crate::sink::SinkGroupStatusSnapshot) -> bool {
    matches!(
        group.normalized_readiness(),
        crate::sink::GroupReadinessState::Ready
    )
}

fn sink_status_snapshot_scheduled_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
) -> std::collections::BTreeSet<String> {
    snapshot.progress_snapshot().scheduled_groups
}

fn sink_status_snapshot_ready_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
) -> std::collections::BTreeSet<String> {
    snapshot.progress_snapshot().ready_groups
}

fn sink_status_snapshot_has_ready_scheduled_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
) -> bool {
    snapshot.progress_snapshot().has_ready_scheduled_groups()
}

fn sink_status_snapshot_ready_for_expected_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
    expected_groups: &std::collections::BTreeSet<String>,
) -> bool {
    snapshot
        .progress_snapshot()
        .ready_for_expected_groups(expected_groups)
}

fn current_generation_sink_replay_tick(
    source_signals: &[SourceControlSignal],
    facade_signals: &[FacadeControlSignal],
) -> Option<SinkControlSignal> {
    let generation = source_signals
        .iter()
        .map(source_signal_generation)
        .chain(facade_signals.iter().map(facade_signal_generation))
        .find(|generation| *generation > 0)?;
    let route_key = format!("{}.stream", ROUTE_KEY_EVENTS);
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
    pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
    control_frame_serial: Arc<Mutex<()>>,
    inflight: Arc<AtomicBool>,
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

    fn runtime_control_state(&self) -> RuntimeControlState {
        RuntimeControlState::from_state_cell(&self.runtime_gate_state)
    }

    fn update_runtime_control_state(&self, update: impl FnOnce(&mut RuntimeControlState)) {
        if let Ok(mut guard) = self.runtime_gate_state.lock() {
            update(&mut guard);
        }
        self.runtime_state_changed.notify_waiters();
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

    async fn run(&self) -> std::result::Result<(), CnxError> {
        let Some(_inflight) = self.try_begin() else {
            return Ok(());
        };
        let _serial = self.control_frame_serial.lock().await;
        let state_at_entry = self.runtime_control_state();
        if state_at_entry.control_gate_ready(false) {
            return Ok(());
        }

        if state_at_entry.source_state_replay_required() {
            self.source
                .observability_snapshot_with_failure()
                .await
                .map_err(SourceFailure::into_error)?;
            if !self.source.retained_replay_required().await {
                self.update_runtime_control_state(|state| state.clear_source_replay());
            }
        }

        if self.runtime_control_state().sink_state_replay_required() {
            self.sink
                .status_snapshot_with_failure()
                .await
                .map_err(SinkFailure::into_error)?;
            if !self.sink.retained_replay_required() {
                self.update_runtime_control_state(|state| state.clear_sink_replay());
            }
        }

        if self.runtime_control_state().replay_fully_cleared() {
            self.update_runtime_control_state(|state| state.mark_initialized());
            self.control_failure_uninitialized
                .store(false, Ordering::Release);
        }
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
                owner.release_for_handoff(&bind_addr).await;
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
        FSMetaApp::try_spawn_pending_facade_from_registrant_with_spawn(
            self.clone(),
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
        let suppressed_dependent_routes_remain = self
            .pending_fixed_bind_has_suppressed_dependent_routes
            .load(Ordering::Acquire);
        if pending.runtime_exposure_confirmed || !suppressed_dependent_routes_remain {
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
    async fn release_for_handoff(self, bind_addr: &str) {
        self.api_request_tracker.wait_for_drain().await;
        self.api_control_gate.wait_for_facade_request_drain().await;
        #[cfg(test)]
        maybe_pause_facade_shutdown_started().await;
        if let Some(current) = self.api_task.lock().await.take() {
            self.api_request_tracker.wait_for_drain().await;
            self.api_control_gate.wait_for_facade_request_drain().await;
            current
                .handle
                .shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT)
                .await;
        }
        clear_owned_process_facade_claim(self.instance_id);
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

fn node_id_from_object_ref(object_ref: &str) -> Option<NodeId> {
    object_ref
        .split_once("::")
        .map(|(node_id, _)| NodeId(node_id.to_string()))
}

fn scheduled_sink_owner_node_for_group(
    snapshot: &crate::sink::SinkStatusSnapshot,
    group_id: &str,
) -> Option<NodeId> {
    let mut scheduled_nodes = snapshot
        .scheduled_groups_by_node
        .iter()
        .filter(|(_, groups)| groups.iter().any(|group| group == group_id))
        .map(|(node_id, _)| NodeId(node_id.clone()))
        .collect::<Vec<_>>();
    scheduled_nodes.sort_by(|a, b| a.0.cmp(&b.0));
    scheduled_nodes.dedup_by(|a, b| a.0 == b.0);

    if let Some(primary_node) = snapshot
        .groups
        .iter()
        .find(|group| group.group_id == group_id)
        .and_then(|group| node_id_from_object_ref(&group.primary_object_ref))
    {
        if scheduled_nodes.is_empty() || scheduled_nodes.iter().any(|node| node.0 == primary_node.0)
        {
            return Some(primary_node);
        }
    }

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
    let owner_node = if readiness_rank > 0 {
        scheduled_sink_owner_node_for_group(snapshot, selected_group)
    } else {
        None
    };
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
    if !state.eligible() {
        return default_route_bindings();
    }
    if let Some(owner_node) = state.owner_node {
        return sink_query_route_bindings_for(&owner_node.0);
    }
    default_route_bindings()
}

fn selected_group_payload_has_materialized_data(
    request: &InternalQueryRequest,
    payload: &MaterializedQueryPayload,
) -> bool {
    let trusted_root_tree_request = request.op == crate::query::QueryOp::Tree
        && request.scope.path.as_slice() == b"/"
        && request.scope.recursive
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    match payload {
        MaterializedQueryPayload::Tree(payload) => {
            if trusted_root_tree_request {
                payload.root.exists && (payload.root.has_children || !payload.entries.is_empty())
            } else {
                payload.root.exists || payload.root.has_children || !payload.entries.is_empty()
            }
        }
        MaterializedQueryPayload::Stats(_) => false,
    }
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
    if has_selected_group_tree_payload && !trusted_tree_request {
        return false;
    }
    let has_materialized_tree_data = local_events.iter().any(|event| {
        matches!(
            rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()),
            Ok(payload) if selected_group_payload_has_materialized_data(request, &payload)
        )
    });
    if !local_selected_group_bridge_eligible {
        return trusted_tree_request
            && local_events.is_empty()
            && !selected_group_tree_has_materialized_data
            && !has_materialized_tree_data;
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
        return Some(false);
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
    let trusted_non_root_bounded_tree_request = request.op == crate::query::QueryOp::Tree
        && request.scope.path.as_slice() != b"/"
        && request.scope.recursive
        && request.scope.max_depth.is_some()
        && request.tree_options.as_ref().is_some_and(|options| {
            options.read_class == crate::query::ReadClass::TrustedMaterialized
        });
    if !trusted_non_root_bounded_tree_request
        || !should_fail_closed_selected_group_empty_after_bridge_failure(
            request,
            local_events,
            local_selected_group_bridge_eligible,
            err,
        )
    {
        return Ok(None);
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
    let sink_query_proxy_route = format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY);
    let sink_status_route = format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL);
    let source_status_route = format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
    let source_find_route = format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL);
    match unit {
        FacadeRuntimeUnit::Facade => route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL),
        FacadeRuntimeUnit::Query => {
            route_key == format!("{}.req", ROUTE_KEY_QUERY)
                || route_key == sink_query_proxy_route
                || route_key == sink_status_route
                || route_key == source_status_route
                || route_key == source_find_route
                || is_per_peer_source_find_request_route(route_key)
        }
        FacadeRuntimeUnit::QueryPeer => {
            route_key == sink_query_proxy_route
                || route_key == sink_status_route
                || route_key == source_status_route
                || route_key == source_find_route
                || is_per_peer_source_find_request_route(route_key)
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
    route_key == format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)
        || route_key == format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL)
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL)
        || is_per_peer_source_find_request_route(route_key)
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

fn runtime_endpoint_task_route_still_active(
    facade_gate: &RuntimeUnitGate,
    task: &ManagedEndpointTask,
) -> bool {
    let source_status_route = format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
    if task.route_key() == source_status_route {
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

fn is_internal_status_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL)
}

fn is_facade_dependent_query_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_QUERY)
        || route_key == format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)
        || route_key == format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL)
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL)
        || is_per_peer_source_find_request_route(route_key)
}

fn is_serving_facade_business_read_route(route_key: &str) -> bool {
    route_key == format!("{}.req", ROUTE_KEY_QUERY)
        || route_key == format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)
        || route_key == format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)
}

fn is_uninitialized_cleanup_query_route_with_policy(
    route_key: &str,
    recovery_lane_policy: ControlFailureRecoveryLanePolicy,
) -> bool {
    is_internal_status_route(route_key)
        && recovery_lane_policy == ControlFailureRecoveryLanePolicy::WithdrawInternalStatus
        || route_key == format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)
            && recovery_lane_policy == ControlFailureRecoveryLanePolicy::WithdrawInternalStatus
        || is_per_peer_sink_query_request_route(route_key)
            && recovery_lane_policy == ControlFailureRecoveryLanePolicy::WithdrawInternalStatus
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL)
        || is_per_peer_source_find_request_route(route_key)
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

fn is_per_peer_source_find_request_route(route_key: &str) -> bool {
    let Some(request_route) = route_key.strip_suffix(".req") else {
        return false;
    };
    let Some((stem, version)) = ROUTE_KEY_SOURCE_FIND_INTERNAL.rsplit_once(':') else {
        return request_route.starts_with(&format!("{ROUTE_KEY_SOURCE_FIND_INTERNAL}."));
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
struct SinkApplyPauseHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
struct SourceApplyErrorQueueHook {
    errs: std::collections::VecDeque<CnxError>,
}

#[cfg(test)]
fn source_apply_entry_count_hook_cell() -> &'static StdMutex<Option<Arc<AtomicUsize>>> {
    static CELL: OnceLock<StdMutex<Option<Arc<AtomicUsize>>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_source_apply_entry_count_hook(count: Arc<AtomicUsize>) {
    let mut guard = match source_apply_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(count);
}

#[cfg(test)]
fn clear_source_apply_entry_count_hook() {
    let mut guard = match source_apply_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn note_source_apply_entry_for_tests() {
    let hook = {
        let guard = match source_apply_entry_count_hook_cell().lock() {
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
fn sink_apply_entry_count_hook_cell() -> &'static StdMutex<Option<Arc<AtomicUsize>>> {
    static CELL: OnceLock<StdMutex<Option<Arc<AtomicUsize>>>> = OnceLock::new();
    CELL.get_or_init(|| StdMutex::new(None))
}

#[cfg(test)]
fn install_sink_apply_entry_count_hook(count: Arc<AtomicUsize>) {
    let mut guard = match sink_apply_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(count);
}

#[cfg(test)]
fn clear_sink_apply_entry_count_hook() {
    let mut guard = match sink_apply_entry_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn note_sink_apply_entry_for_tests() {
    let hook = {
        let guard = match sink_apply_entry_count_hook_cell().lock() {
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
    pump_task: Mutex<Option<JoinHandle<()>>>,
    runtime_endpoint_tasks: Mutex<Vec<ManagedEndpointTask>>,
    runtime_endpoint_routes: Mutex<std::collections::BTreeSet<String>>,
    api_task: Arc<Mutex<Option<FacadeActivation>>>,
    pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
    facade_spawn_in_progress: Arc<Mutex<Option<FacadeSpawnInProgress>>>,
    retained_active_facade_continuity: Arc<AtomicBool>,
    pending_fixed_bind_claim_release_followup: Arc<AtomicBool>,
    pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
    control_failure_uninitialized: Arc<AtomicBool>,
    management_write_recovery_inflight: Arc<AtomicBool>,
    api_request_tracker: Arc<ApiRequestTracker>,
    api_control_gate: Arc<ApiControlGate>,
    control_frame_serial: Arc<Mutex<()>>,
    shared_control_frame_serial: Arc<Mutex<()>>,
    facade_pending_status: SharedFacadePendingStatusCell,
    facade_service_state: SharedFacadeServiceStateCell,
    rollout_status: SharedRolloutStatusCell,
    facade_gate: RuntimeUnitGate,
    mirrored_query_peer_routes: Arc<Mutex<std::collections::BTreeMap<String, u64>>>,
    runtime_gate_state: Arc<StdMutex<RuntimeControlState>>,
    retained_sink_control_state: Mutex<RetainedSinkControlState>,
    runtime_state_changed: Arc<tokio::sync::Notify>,
    retained_suppressed_public_query_activates:
        Mutex<std::collections::BTreeMap<String, FacadeControlSignal>>,
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
    fn management_write_recovery(&self) -> ManagementWriteRecovery {
        let context = ManagementWriteRecoveryContext {
            instance_id: self.instance_id,
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
            pending_fixed_bind_has_suppressed_dependent_routes: self
                .pending_fixed_bind_has_suppressed_dependent_routes
                .clone(),
            control_frame_serial: self.control_frame_serial.clone(),
            inflight: self.management_write_recovery_inflight.clone(),
        };
        Arc::new(move || {
            let context = context.clone();
            Box::pin(async move { context.run().await })
        })
    }

    #[cfg(test)]
    fn management_write_recovery_for_tests(&self) -> ManagementWriteRecovery {
        self.management_write_recovery()
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

    async fn recover_retained_replay_readiness_inside_control_frame(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
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
            self.apply_source_signals_with_recovery(
                fixed_bind_session,
                &[],
                false,
                SourceGenerationCutoverDisposition::None,
            )
            .await?;
            if self.runtime_control_state().source_state_replay_required() {
                self.publish_recovered_facade_state_after_retained_replay()
                    .await;
                return Ok(());
            }
        }

        if self.runtime_control_state().sink_state_replay_required() {
            self.apply_sink_signals_with_recovery(fixed_bind_session, &[], true, false)
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

    fn sink_status_snapshot_should_defer_republish_after_retained_replay_timeout(
        snapshot: &crate::sink::SinkStatusSnapshot,
    ) -> bool {
        FSMetaApp::sink_status_snapshot_summary_is_empty(snapshot)
            || FSMetaApp::sink_status_snapshot_has_scheduled_only_republish_evidence(snapshot)
    }

    fn sink_status_snapshot_should_defer_local_republish_after_retained_replay(
        snapshot: &crate::sink::SinkStatusSnapshot,
    ) -> bool {
        FSMetaApp::sink_status_snapshot_has_scheduled_only_republish_evidence(snapshot)
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

    async fn execute_local_sink_status_republish_until_terminal(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        runtime_state_changed: Arc<tokio::sync::Notify>,
        expected_groups: &std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &[SinkControlSignal],
        mode: LocalSinkStatusRepublishWaitMode,
        pretrigger_request_epoch: Option<u64>,
    ) -> Result<()> {
        let mut machine = match pretrigger_request_epoch {
            Some(request_epoch) => SinkRecoveryMachine::new_local_pretriggered(mode, request_epoch),
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
    }

    async fn wait_for_local_sink_status_republish_after_recovery_from_parts(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        runtime_state_changed: Arc<tokio::sync::Notify>,
        expected_groups: &std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &[SinkControlSignal],
    ) -> Result<()> {
        Self::execute_local_sink_status_republish_until_terminal(
            source,
            sink,
            runtime_state_changed,
            expected_groups,
            post_return_sink_replay_signals,
            LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath,
            None,
        )
        .await
    }

    async fn wait_for_local_sink_status_republish_after_recovery_requiring_probe_from_parts_with_pretrigger_epoch(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        runtime_state_changed: Arc<tokio::sync::Notify>,
        expected_groups: &std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &[SinkControlSignal],
        pretrigger_request_epoch: u64,
    ) -> Result<()> {
        Self::execute_local_sink_status_republish_until_terminal(
            source,
            sink,
            runtime_state_changed,
            expected_groups,
            post_return_sink_replay_signals,
            LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady,
            Some(pretrigger_request_epoch),
        )
        .await
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
            SinkRecoveryProbeOutcome::Ready
        } else {
            SinkRecoveryProbeOutcome::NotReady
        }
    }

    async fn nonblocking_sink_status_probe_ready_for_expected_groups(
        sink: &Arc<SinkFacade>,
        expected_groups: &std::collections::BTreeSet<String>,
        remaining: Duration,
    ) -> bool {
        match tokio::time::timeout(
            remaining.min(Duration::from_millis(350)),
            sink.status_snapshot_nonblocking_with_failure(),
        )
        .await
        {
            Ok(Ok(snapshot)) => {
                sink_status_snapshot_ready_for_expected_groups(&snapshot, expected_groups)
            }
            Ok(Err(_)) | Err(_) => false,
        }
    }

    fn cached_sink_status_ready_for_expected_groups(
        sink: &Arc<SinkFacade>,
        expected_groups: &std::collections::BTreeSet<String>,
    ) -> bool {
        sink.cached_status_snapshot_with_failure()
            .ok()
            .is_some_and(|snapshot| {
                sink_status_snapshot_ready_for_expected_groups(&snapshot, expected_groups)
            })
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
            }
            Ok(Err(_)) | Err(_) => false,
        }
    }

    async fn wait_for_local_sink_status_republish_after_recovery_requiring_probe_from_parts(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        runtime_state_changed: Arc<tokio::sync::Notify>,
        expected_groups: &std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &[SinkControlSignal],
    ) -> Result<()> {
        Self::execute_local_sink_status_republish_until_terminal(
            source,
            sink,
            runtime_state_changed,
            expected_groups,
            post_return_sink_replay_signals,
            LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady,
            None,
        )
        .await
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
        let route_key = format!("{}.stream", ROUTE_KEY_EVENTS);
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
            SinkRecoveryGateReopenDisposition::WaitForLocalSinkStatusRepublish {
                expected_groups,
            } => {
                eprintln!(
                    "fs_meta_runtime_app: retaining local sink-status republish wait after source-led uninitialized mixed recovery despite cached ready snapshot groups={expected_groups:?}"
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
            .defers_gate_reopen()
        {
            facade_publication_signals
                .into_iter()
                .partition(Self::facade_publication_signal_is_sink_owned_query_peer_activate)
        } else {
            (Vec::new(), facade_publication_signals)
        };
        let query_publication_followup_present = facade_publication_signals
            .iter()
            .any(Self::facade_publication_signal_is_query_activate);
        if !sink_cleanup_only_while_uninitialized && !query_publication_followup_present {
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
                let pretrigger_request_epoch = self
                    .source
                    .submit_rescan_when_ready_epoch_with_failure()
                    .await
                    .map_err(RuntimeWorkerObservationFailure::from)
                    .map_err(RuntimeWorkerObservationFailure::into_error)?;
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
                    Some(pretrigger_request_epoch),
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
    ) {
        tokio::spawn(async move {
            let helper_result = match pretrigger_request_epoch {
                Some(request_epoch) => {
                    Self::wait_for_local_sink_status_republish_after_recovery_requiring_probe_from_parts_with_pretrigger_epoch(
                        source,
                        sink,
                        runtime_state_changed,
                        &expected_groups,
                        &post_return_sink_replay_signals,
                        request_epoch,
                    )
                    .await
                }
                None => {
                    Self::wait_for_local_sink_status_republish_after_recovery_from_parts(
                        source,
                        sink,
                        runtime_state_changed,
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
                ),
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
            runtime_endpoint_tasks: Mutex::new(Vec::new()),
            runtime_endpoint_routes: Mutex::new(std::collections::BTreeSet::new()),
            api_task: Arc::new(Mutex::new(None)),
            pending_facade: Arc::new(Mutex::new(None)),
            facade_spawn_in_progress: Arc::new(Mutex::new(None)),
            retained_active_facade_continuity: Arc::new(AtomicBool::new(false)),
            pending_fixed_bind_claim_release_followup: Arc::new(AtomicBool::new(false)),
            pending_fixed_bind_has_suppressed_dependent_routes: Arc::new(AtomicBool::new(false)),
            control_failure_uninitialized: Arc::new(AtomicBool::new(false)),
            management_write_recovery_inflight: Arc::new(AtomicBool::new(false)),
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
            mirrored_query_peer_routes: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            runtime_gate_state: Arc::new(StdMutex::new(RuntimeControlState::bootstrapping(
                false, false,
            ))),
            retained_sink_control_state: Mutex::new(RetainedSinkControlState::default()),
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

    fn internal_status_available_from_published_facade_state(
        published_facade_state: FacadeServiceState,
        local_api_task_present: bool,
    ) -> InternalStatusAvailability {
        if !local_api_task_present
            || matches!(
                published_facade_state,
                FacadeServiceState::Serving | FacadeServiceState::Degraded
            )
        {
            InternalStatusAvailability::Available
        } else {
            InternalStatusAvailability::UnavailableLocalFacadeNotServing
        }
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
                .all(Self::source_signal_is_restart_deferred_retire_pending)
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
                .all(Self::source_signal_is_restart_deferred_retire_pending)
            && (input.sink_signals.is_empty()
                || input
                    .sink_signals
                    .iter()
                    .all(Self::sink_signal_is_cleanup_only))
        {
            SourceGenerationCutoverDisposition::FailClosedRestartDeferredRetirePending
        } else if !input.source_signals.is_empty()
            && input
                .source_signals
                .iter()
                .any(Self::source_signal_is_post_initial_activate)
        {
            SourceGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
        } else {
            SourceGenerationCutoverDisposition::None
        }
    }

    fn classify_sink_generation_cutover_disposition(
        input: SinkGenerationCutoverDecisionInput<'_>,
    ) -> SinkGenerationCutoverDisposition {
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
                .all(Self::source_signal_is_restart_deferred_retire_pending)
            && !input.sink_signals.is_empty()
            && input
                .sink_signals
                .iter()
                .all(Self::sink_signal_is_cleanup_only)
        {
            SinkGenerationCutoverDisposition::FailClosedSharedGenerationCutover
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
        api_control_gate
            .set_ready_state(snapshot.control_gate_ready, snapshot.management_write_ready);
        *facade_service_state
            .write()
            .expect("write published facade service state") = snapshot.published_state;
        snapshot.published_state
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
                    owner.release_for_handoff(&bind_addr).await;
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
        self.api_request_tracker.wait_for_drain().await;
        self.api_control_gate.wait_for_facade_request_drain().await;
        #[cfg(test)]
        maybe_pause_facade_shutdown_started().await;
        eprintln!("fs_meta_runtime_app: shutdown_active_facade");
        if let Some(current) = self.api_task.lock().await.take() {
            eprintln!(
                "fs_meta_runtime_app: shutting down previous active facade generation={}",
                current.generation
            );
            current
                .handle
                .shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT)
                .await;
        }
        clear_owned_process_facade_claim(self.instance_id);
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
    async fn settle_fixed_bind_claim_release_followup(
        &self,
    ) -> Result<FixedBindClaimReleaseFollowupSnapshot> {
        let session = self.begin_fixed_bind_lifecycle_session().await;
        let (snapshot, _) = self
            .settle_fixed_bind_claim_release_followup_with_session(session)
            .await?;
        Ok(snapshot)
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

    fn source_signal_is_restart_deferred_retire_pending(signal: &SourceControlSignal) -> bool {
        let SourceControlSignal::Deactivate { envelope, .. } = signal else {
            return false;
        };
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

    fn sink_signal_is_restart_deferred_retire_pending(signal: &SinkControlSignal) -> bool {
        let SinkControlSignal::Deactivate {
            route_key,
            envelope,
            ..
        } = signal
        else {
            return false;
        };
        if route_key != &format!("{}.stream", ROUTE_KEY_EVENTS)
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
        matches!(
            signal,
            SinkControlSignal::Activate { route_key, .. }
                | SinkControlSignal::Deactivate { route_key, .. }
                if route_key == &format!("{}.stream", ROUTE_KEY_EVENTS)
        ) || Self::sink_signal_is_restart_deferred_retire_pending(signal)
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
            eprintln!("fs_meta_runtime_app: initialize_from_control source.start begin");
            let start = self
                .source
                .start_with_failure(self.sink.clone(), self.runtime_boundary.clone());
            *guard = if let Some(deadline) = deadline {
                let start = Self::map_runtime_initialize_timeout_result(
                    tokio::time::timeout(Self::remaining_initialize_budget(Some(deadline))?, start)
                        .await,
                )?;
                start.map_err(RuntimeInitializeFailure::from)?
            } else {
                start.await.map_err(RuntimeInitializeFailure::from)?
            };
            eprintln!("fs_meta_runtime_app: initialize_from_control source.start ok");
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
            if !runtime_endpoint_task_route_still_active(&self.facade_gate, &task) {
                eprintln!(
                    "fs_meta_runtime_app: retiring inactive runtime endpoint route={}",
                    task.route_key()
                );
                task.request_shutdown();
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
            if runtime_endpoint_task_route_still_active(&self.facade_gate, task) {
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
                let endpoint = ManagedEndpointTask::spawn(
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
                                    let payload = rmp_serde::to_vec(&params).map_err(|err| {
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
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS) {
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
                    // Not currently selected as query/query-peer sink-status owner, route inactive, or already running.
                } else {
                    let active_unit_ids = endpoint_unit_ids.clone();
                    let facade_service_state = self.facade_service_state.clone();
                    let facade_gate = self.facade_gate.clone();
                    let api_task = self.api_task.clone();
                    let runtime_gate_state = self.runtime_gate_state.clone();
                    let sink = self.sink.clone();
                    let route_key = route.0.clone();
                    eprintln!(
                        "fs_meta_runtime_app: spawning sink status endpoint route={}",
                        route.0
                    );
                    let endpoint = ManagedEndpointTask::spawn_with_units(
                        boundary.clone(),
                        route,
                        format!(
                            "app:{}:{}",
                            ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS
                        ),
                        endpoint_unit_ids,
                        tokio_util::sync::CancellationToken::new(),
                        move |requests| {
                            let facade_service_state = facade_service_state.clone();
                            let facade_gate = facade_gate.clone();
                            let api_task = api_task.clone();
                            let runtime_gate_state = runtime_gate_state.clone();
                            let sink = sink.clone();
                            let route_key = route_key.clone();
                            let active_unit_ids = active_unit_ids.clone();
                            async move {
                                let mut responses = Vec::new();
                                for req in requests {
                                    let published_facade_state = *facade_service_state
                                        .read()
                                        .expect("read published facade service state");
                                    let local_api_task_present = api_task.lock().await.is_some();
                                    if !matches!(
                                        Self::internal_status_available_from_published_facade_state(
                                            published_facade_state,
                                            local_api_task_present,
                                        ),
                                        InternalStatusAvailability::Available
                                    ) {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink status endpoint unavailable reason=facade_state_not_serving state={:?}",
                                            published_facade_state
                                        );
                                        continue;
                                    }
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
                                    let published_facade_state = *facade_service_state
                                        .read()
                                        .expect("read published facade service state after pause");
                                    let local_api_task_present = api_task.lock().await.is_some();
                                    if !matches!(
                                        Self::internal_status_available_from_published_facade_state(
                                            published_facade_state,
                                            local_api_task_present,
                                        ),
                                        InternalStatusAvailability::Available
                                    ) {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink status endpoint unavailable reason=facade_state_not_serving_after_pause state={:?}",
                                            published_facade_state
                                        );
                                        continue;
                                    }
                                    match sink.status_snapshot_nonblocking_for_status_route().await
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
                                                    internal_query_route_still_active(
                                                        &facade_gate,
                                                        &route_key,
                                                    );
                                                let published_facade_state =
                                                *facade_service_state
                                                    .read()
                                                    .expect(
                                                        "read published facade service state after sink timeout fallback",
                                                    );
                                                let local_api_task_present =
                                                    api_task.lock().await.is_some();
                                                if route_still_active
                                                && matches!(
                                                    Self::internal_status_available_from_published_facade_state(
                                                        published_facade_state,
                                                        local_api_task_present,
                                                    ),
                                                    InternalStatusAvailability::Available
                                                )
                                            {
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
        }
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
            if !source_active
                || endpoint_unit_ids.is_empty()
                || !spawned_routes.insert(route.0.clone())
            {
                // Not currently selected as source-status owner, or already running.
            } else {
                let facade_service_state = self.facade_service_state.clone();
                let api_task = self.api_task.clone();
                let source = self.source.clone();
                let node_id = self.node_id.clone();
                eprintln!(
                    "fs_meta_runtime_app: spawning source status endpoint route={}",
                    route.0
                );
                let endpoint = ManagedEndpointTask::spawn_with_units(
                    boundary.clone(),
                    route,
                    format!(
                        "app:{}:{}",
                        ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS
                    ),
                    endpoint_unit_ids,
                    tokio_util::sync::CancellationToken::new(),
                    move |requests| {
                        let facade_service_state = facade_service_state.clone();
                        let api_task = api_task.clone();
                        let source = source.clone();
                        let node_id = node_id.clone();
                        async move {
                            let mut responses = Vec::new();
                            for req in requests {
                                let published_facade_state = *facade_service_state
                                    .read()
                                    .expect("read published facade service state");
                                let local_api_task_present = api_task.lock().await.is_some();
                                if !matches!(
                                    Self::internal_status_available_from_published_facade_state(
                                        published_facade_state,
                                        local_api_task_present,
                                    ),
                                    InternalStatusAvailability::Available
                                ) {
                                    eprintln!(
                                        "fs_meta_runtime_app: source status endpoint unavailable reason=facade_state_not_serving state={:?}",
                                        published_facade_state
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
                                let published_facade_state = *facade_service_state
                                    .read()
                                    .expect("read published facade service state after pause");
                                let local_api_task_present = api_task.lock().await.is_some();
                                if !matches!(
                                    Self::internal_status_available_from_published_facade_state(
                                        published_facade_state,
                                        local_api_task_present,
                                    ),
                                    InternalStatusAvailability::Available
                                ) {
                                    eprintln!(
                                        "fs_meta_runtime_app: source status endpoint unavailable reason=facade_state_not_serving_after_pause state={:?}",
                                        published_facade_state
                                    );
                                    continue;
                                }
                                let (snapshot, used_cached_fallback) = source
                                    .observability_snapshot_nonblocking_for_status_route()
                                    .await;
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
                    let endpoint = ManagedEndpointTask::spawn_with_units(
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
                                                match rmp_serde::to_vec(&params) {
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
                                            let err = err.into_error();
                                            eprintln!(
                                                "fs_meta_runtime_app: sink query proxy failed err={}",
                                                err
                                            );
                                            responses.push(Event::new(
                                                EventMetadata {
                                                    origin_id: NodeId(
                                                        params
                                                            .scope
                                                            .selected_group
                                                            .clone()
                                                            .unwrap_or_else(|| {
                                                                "sink-query-proxy".to_string()
                                                            }),
                                                    ),
                                                    timestamp_us: now_us(),
                                                    logical_ts: None,
                                                    correlation_id: req.metadata().correlation_id,
                                                    ingress_auth: None,
                                                    trace: None,
                                                },
                                                bytes::Bytes::from(err.to_string()),
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
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND) {
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
                    // Not currently selected as query/query-peer source-find owner, route inactive, or already running.
                } else {
                    eprintln!(
                        "fs_meta_runtime_app: spawning source find proxy endpoint route={}",
                        route.0
                    );
                    let source = self.source.clone();
                    let node_id = self.node_id.clone();
                    let endpoint = ManagedEndpointTask::spawn_with_units(
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
                    // Not currently selected as query/query-peer scoped source-find owner, route inactive, or already running.
                } else {
                    eprintln!(
                        "fs_meta_runtime_app: spawning source find proxy endpoint route={}",
                        route.0
                    );
                    let source = self.source.clone();
                    let node_id = self.node_id.clone();
                    let endpoint = ManagedEndpointTask::spawn_with_units(
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
        let status = evaluate_observation_status(
            &candidate_group_observation_evidence(&source_status, &sink_status, &candidate_groups),
            ObservationTrustPolicy::candidate_generation(),
        );
        Ok(status.state == ObservationState::TrustedMaterialized)
    }

    async fn apply_pending_fixed_bind_spawn_success_followup(
        &self,
        pending: Option<PendingFacadeActivation>,
        reset_suppressed_dependent_routes: bool,
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
        if reset_suppressed_dependent_routes {
            self.pending_fixed_bind_has_suppressed_dependent_routes
                .store(false, Ordering::Release);
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
                let reset_suppressed_dependent_routes = spawned || pending.is_none();
                let pending_after_spawn = pending.clone();
                self.apply_pending_fixed_bind_spawn_success_followup(
                    pending,
                    reset_suppressed_dependent_routes,
                )
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
                if !pending.runtime_exposure_confirmed {
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
                if !pending.runtime_exposure_confirmed {
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
        let replacement_wait_reason = Self::facade_replacement_wait_reason(
            source.as_ref(),
            sink.as_ref(),
            &pending,
            replacing_existing,
        )
        .await?;
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
                active.route_key == pending.route_key && active.resource_ids == pending.resource_ids
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
            } if route_key == &format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)
                || route_key == &format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)
                || is_per_peer_sink_query_request_route(route_key)
        )
    }

    async fn record_suppressed_public_query_activate(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
        bound_scopes: &[RuntimeBoundScope],
    ) {
        if !matches!(unit, FacadeRuntimeUnit::Query)
            || route_key != format!("{}.req", ROUTE_KEY_QUERY)
        {
            return;
        }
        self.retained_suppressed_public_query_activates
            .lock()
            .await
            .insert(
                route_key.to_string(),
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
        let suppressed_dependent_routes_pending = self
            .pending_fixed_bind_has_suppressed_dependent_routes
            .load(Ordering::Acquire);
        let signals = {
            let mut retained = self.retained_suppressed_public_query_activates.lock().await;
            let drained = std::mem::take(&mut *retained);
            drained.into_values().collect::<Vec<_>>()
        };
        for signal in signals {
            fixed_bind_session = self
                .apply_facade_signal_with_session(fixed_bind_session, signal)
                .await?;
        }
        if suppressed_dependent_routes_pending {
            let retained_sink_signals = self.retained_sink_facade_dependent_route_activates().await;
            if !retained_sink_signals.is_empty() {
                self.apply_sink_signals_with_recovery(
                    &fixed_bind_session,
                    &retained_sink_signals,
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

    #[cfg(test)]
    async fn replay_suppressed_public_query_activates_after_publication(&self) -> Result<()> {
        let fixed_bind_session = self.begin_fixed_bind_lifecycle_session().await;
        let _ = self
            .replay_suppressed_public_query_activates_after_publication_with_session(
                fixed_bind_session,
            )
            .await?;
        Ok(())
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
        ) {
            let (followup_snapshot, next_session) = self
                .settle_fixed_bind_claim_release_followup_with_session(session)
                .await?;
            session = next_session;
            let publication_still_incomplete =
                followup_snapshot.blocks_facade_dependent_routes_without_claim();
            if followup_snapshot.conflicting_process_claim.is_some() {
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
            if publication_still_incomplete {
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
                .unit_state(execution_units::QUERY_RUNTIME_UNIT_ID)?
                .map(|(active, _)| active)
                .unwrap_or(false);
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
        Ok(session)
    }

    #[cfg(test)]
    async fn shutdown_active_facade(&self) {
        self.drive_fixed_bind_lifecycle_request(FixedBindLifecycleRequest::Shutdown)
            .await
            .expect("shutdown handoff should complete");
    }

    async fn withdraw_uninitialized_query_routes(&self) {
        self.withdraw_uninitialized_query_routes_with_policy(
            ControlFailureRecoveryLanePolicy::WithdrawInternalStatus,
        )
        .await;
    }

    async fn withdraw_uninitialized_query_routes_preserving_internal_status(&self) {
        self.withdraw_uninitialized_query_routes_with_policy(
            ControlFailureRecoveryLanePolicy::PreserveInternalStatus,
        )
        .await;
    }

    async fn withdraw_uninitialized_query_routes_with_policy(
        &self,
        recovery_lane_policy: ControlFailureRecoveryLanePolicy,
    ) {
        let mut query_routes = vec![
            format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
            source_find_request_route_for(&self.node_id.0).0,
        ];
        if recovery_lane_policy == ControlFailureRecoveryLanePolicy::WithdrawInternalStatus {
            query_routes.push(format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL));
            query_routes.push(format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY));
            query_routes.push(format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL));
        }
        for route_key in &query_routes {
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
            if is_uninitialized_cleanup_query_route_with_policy(
                task.route_key(),
                recovery_lane_policy,
            ) {
                task.shutdown(Duration::from_secs(1)).await;
            } else {
                retained.push(task);
            }
        }
        *self.runtime_endpoint_tasks.lock().await = retained;

        let mut routes = self.runtime_endpoint_routes.lock().await;
        routes.retain(|route_key| {
            !is_uninitialized_cleanup_query_route_with_policy(route_key, recovery_lane_policy)
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
        let _ = self
            .publish_facade_service_state_with_session(fixed_bind_session)
            .await;
        self.initialize_from_control_with_deadline_and_session(
            false,
            false,
            Some(deadline),
            fixed_bind_session,
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
        self.mark_control_uninitialized_after_failure_with_session_and_policy_and_replay(
            fixed_bind_session,
            ControlFailureRecoveryLanePolicy::PreserveInternalStatus,
            if require_sink_replay {
                ControlFailureReplayRequirement::SourceAndSink
            } else {
                ControlFailureReplayRequirement::Source
            },
        )
        .await;
    }

    async fn mark_control_uninitialized_after_deferred_sink_replay_with_session(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
    ) {
        self.mark_control_uninitialized_after_failure_with_session_and_policy_and_replay(
            fixed_bind_session,
            ControlFailureRecoveryLanePolicy::WithdrawInternalStatus,
            ControlFailureReplayRequirement::Sink,
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
        )
        .await;
    }

    async fn mark_control_uninitialized_after_failure_with_session_and_policy_and_replay(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        recovery_lane_policy: ControlFailureRecoveryLanePolicy,
        replay_requirement: ControlFailureReplayRequirement,
    ) {
        self.update_runtime_control_state(|state| match replay_requirement {
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
        let _ = self
            .publish_facade_service_state_with_session(fixed_bind_session)
            .await;
        self.retained_active_facade_continuity
            .store(false, Ordering::Release);
        self.clear_shared_source_route_claims_for_instance().await;
        self.clear_shared_sink_route_claims_for_instance().await;
        match recovery_lane_policy {
            ControlFailureRecoveryLanePolicy::WithdrawInternalStatus => {
                self.withdraw_uninitialized_query_routes().await;
            }
            ControlFailureRecoveryLanePolicy::PreserveInternalStatus => {
                self.withdraw_uninitialized_query_routes_preserving_internal_status()
                    .await;
            }
        }
        let released_failed_owner = if let Some(handoff) = self
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
            true
        } else {
            false
        };
        if !released_failed_owner {
            let _ = self
                .release_continuity_retained_fixed_bind_owner_for_failed_pending_successor()
                .await;
        }
    }

    #[cfg(test)]
    async fn mark_control_uninitialized_after_failure(&self) {
        let fixed_bind_session = self.begin_fixed_bind_lifecycle_session().await;
        self.mark_control_uninitialized_after_failure_with_session(&fixed_bind_session)
            .await
    }

    async fn fixed_bind_handoff_after_control_failure_uninitialized(
        &self,
    ) -> Option<ActiveFixedBindShutdownContinuation> {
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
        owner.release_for_handoff(&bind_addr).await;
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
        ) || Self::source_signal_is_restart_deferred_retire_pending(signal)
    }

    fn source_signals_are_retained_replay_wakeup_only(
        source_signals: &[SourceControlSignal],
    ) -> bool {
        !source_signals.is_empty()
            && source_signals
                .iter()
                .all(Self::source_signal_is_retained_replay_wakeup)
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
                self.facade_gate.apply_activate(
                    unit.unit_id(),
                    route_key,
                    *generation,
                    bound_scopes,
                )?;
            }
            SourceControlSignal::Deactivate {
                unit,
                route_key,
                generation,
                ..
            } => {
                self.facade_gate
                    .apply_deactivate(unit.unit_id(), route_key, *generation)?;
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
            .any(Self::source_signal_is_post_initial_route_state)
    }

    async fn source_generation_cutover_apply_should_defer_inline(
        &self,
        source_signals: &[SourceControlSignal],
        _generation_cutover_disposition: SourceGenerationCutoverDisposition,
    ) -> bool {
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
        self.record_shared_source_route_claims(&filtered_source_signals)
            .await;
        self.apply_source_signals_to_runtime_endpoint_gate(&filtered_source_signals)
            .map_err(RuntimeWorkerControlApplyFailure::from)
    }

    async fn apply_source_signals_with_recovery(
        &self,
        fixed_bind_session: &FixedBindLifecycleSession,
        source_signals: &[SourceControlSignal],
        control_initialized_at_entry: bool,
        generation_cutover_disposition: SourceGenerationCutoverDisposition,
    ) -> std::result::Result<(), RuntimeWorkerControlApplyFailure> {
        let filtered_source_signals = self
            .filter_shared_source_route_deactivates(source_signals)
            .await;
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
                .source_signals_are_retained_or_forward_generation_ticks(&filtered_source_signals)
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
        let current_retained_generation_ticks = self
            .source_signals_are_current_retained_generation_ticks(&filtered_source_signals)
            .await;
        if self.runtime_control_state().source_state_replay_required()
            && Self::source_signals_are_retained_replay_wakeup_only(&filtered_source_signals)
            && !current_retained_generation_ticks
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
        let fail_closed_restart_deferred_retire_pending = matches!(
            generation_cutover_disposition,
            SourceGenerationCutoverDisposition::FailClosedRestartDeferredRetirePending
        ) && !filtered_source_signals.is_empty()
            && filtered_source_signals
                .iter()
                .all(Self::source_signal_is_restart_deferred_retire_pending);
        let defer_retained_state_until_success = fail_closed_restart_deferred_retire_pending;
        let fail_closed_retained_replay_on_retryable_reset = matches!(
            generation_cutover_disposition,
            SourceGenerationCutoverDisposition::FailClosedRetainedReplayOnRetryableReset
        ) && !filtered_source_signals
            .is_empty()
            && !defer_retained_state_until_success;
        if !defer_retained_state_until_success {
            self.source
                .record_retained_control_signals(&filtered_source_signals)
                .await;
        }
        let replay_followup_signals = if self.runtime_control_state().source_state_replay_required()
            && Self::source_signals_are_transient_followup_only(&filtered_source_signals)
        {
            let followups = Self::source_transient_followup_signals(&filtered_source_signals);
            (!followups.is_empty()).then_some(followups)
        } else {
            None
        };
        let mut replay_followup_pending = replay_followup_signals.clone();
        let deadline = tokio::time::Instant::now() + SOURCE_CONTROL_RECOVERY_TOTAL_TIMEOUT;
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
            let retained_generation_cutover_replay_fail_closed =
                self.runtime_control_state().source_state_replay_required()
                    && filtered_source_signals.is_empty()
                    && effective_source_signals
                        .iter()
                        .any(Self::source_signal_is_post_initial_activate);
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
                    if replaying_retained_state_only {
                        continue;
                    }
                    replay_followup_pending = None;
                    return Ok(());
                }
                Err(err) if fail_closed_restart_deferred_retire_pending => {
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
                    self.mark_control_uninitialized_after_failure_preserving_internal_status_with_session(
                        fixed_bind_session,
                    )
                        .await;
                    return Ok(());
                }
                Err(err)
                    if is_retryable_worker_control_reset(err.as_error())
                        && tokio::time::Instant::now() < deadline =>
                {
                    eprintln!(
                        "fs_meta_runtime_app: source control replay after retryable reset err={}",
                        err.as_error()
                    );
                    let _ = self
                        .source
                        .reconnect_shared_worker_client_with_failure()
                        .await;
                    self.reinitialize_after_control_reset_with_deadline_and_session_with_failure(
                        fixed_bind_session,
                        deadline,
                    )
                    .await
                    .map_err(RuntimeWorkerControlApplyFailure::from)?;
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
                    self.mark_control_uninitialized_after_failure_with_session(fixed_bind_session)
                        .await;
                    return Err(RuntimeWorkerControlApplyFailure::from(err));
                }
            }
        }
    }

    async fn sink_signals_with_replay(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> Vec<SinkControlSignal> {
        let replay_retained = self.runtime_control_state().sink_state_replay_required()
            || (!sink_signals.is_empty()
                && sink_signals
                    .iter()
                    .all(|signal| matches!(signal, SinkControlSignal::Tick { .. })));
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
        if !self.runtime_control_state().source_state_replay_required() {
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
                    state
                        .active_by_route
                        .remove(&(unit.unit_id().to_string(), route_key.clone()));
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
        let mut replay_followup_pending = replay_followup_signals.clone();
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
                    replay_followup_pending = None;
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
            match self
                .sink
                .apply_orchestration_signals_with_total_timeout_with_failure(
                    &effective_sink_signals,
                    remaining,
                )
                .await
            {
                Ok(()) => {
                    if replay_retained_state {
                        self.record_retained_sink_control_state(&effective_sink_signals)
                            .await;
                    }
                    if replay_retained_state {
                        self.update_runtime_control_state(|state| state.clear_sink_replay());
                    }
                    if replaying_retained_state_only {
                        continue;
                    }
                    replay_followup_pending = None;
                    return Ok(());
                }
                Err(err) if fail_closed_restart_deferred_retire_pending => {
                    eprintln!(
                        "fs_meta_runtime_app: sink control fail-closed restart_deferred_retire_pending err={}",
                        err.as_error()
                    );
                    self.mark_control_uninitialized_after_failure_with_session(fixed_bind_session)
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
                    self.mark_control_uninitialized_after_failure_with_session(fixed_bind_session)
                        .await;
                    return Ok(());
                }
                Err(err)
                    if is_retryable_worker_control_reset(err.as_error())
                        && tokio::time::Instant::now() < deadline =>
                {
                    if fail_closed_in_generation_cutover_lane
                        && is_retryable_worker_transport_close_reset(err.as_error())
                    {
                        eprintln!(
                            "fs_meta_runtime_app: sink control fail-closed during generation cutover err={}",
                            err.as_error()
                        );
                        self.mark_control_uninitialized_after_failure_with_session(
                            fixed_bind_session,
                        )
                        .await;
                        return Ok(());
                    }
                    eprintln!(
                        "fs_meta_runtime_app: sink control replay after retryable reset err={}",
                        err.as_error()
                    );
                    if replay_retained_state {
                        self.reinitialize_after_control_reset_with_deadline_and_session_with_failure(
                            fixed_bind_session,
                            deadline,
                        )
                        .await
                        .map_err(RuntimeWorkerControlApplyFailure::from)?;
                    } else {
                        self.sink
                            .ensure_started_with_failure()
                            .await
                            .map_err(RuntimeWorkerControlApplyFailure::from)?;
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
                    self.mark_control_uninitialized_after_failure_with_session(fixed_bind_session)
                        .await;
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
        _restart_deferred_retire_pending: bool,
    ) -> Result<()> {
        let session = self.begin_fixed_bind_lifecycle_session().await;
        let _ = self
            .apply_facade_deactivate_with_session(session, unit, route_key, generation)
            .await?;
        Ok(())
    }

    async fn apply_facade_deactivate_with_session(
        &self,
        session: FixedBindLifecycleSession,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
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
        let sink_query_proxy_current_generation =
            if query_lane && route_key == sink_query_proxy_route {
                self.facade_gate
                    .route_generation(unit.unit_id(), route_key)?
                    .unwrap_or(0)
            } else {
                0
            };
        let stale_sink_query_proxy_deactivate = query_lane
            && route_key == sink_query_proxy_route
            && generation < sink_query_proxy_current_generation;
        if matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        ) && is_internal_status_route(route_key)
            && self.control_initialized()
        {
            self.api_control_gate.wait_for_facade_request_drain().await;
        }
        if query_lane && route_key == sink_query_proxy_route && !stale_sink_query_proxy_deactivate {
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
        let facade_control_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
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
        session = self
            .drive_fixed_bind_lifecycle_request_with_session(
                session,
                FixedBindLifecycleRequest::Deactivate {
                    route_key: route_key.to_string(),
                    generation,
                    retain_active_facade,
                    retain_pending_spawn,
                    restart_deferred_retire_pending: false,
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

    async fn apply_facade_signal_with_session(
        &self,
        session: FixedBindLifecycleSession,
        signal: FacadeControlSignal,
    ) -> Result<FixedBindLifecycleSession> {
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
                restart_deferred_retire_pending: _,
            } => {
                return self
                    .apply_facade_deactivate_with_session(session, unit, &route_key, generation)
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

    async fn service_on_control_frame_with_failure_and_session(
        &self,
        envelopes: &[ControlEnvelope],
        fixed_bind_session: FixedBindLifecycleSession,
    ) -> std::result::Result<(), RuntimeControlFrameFailure> {
        let (source_signals, sink_signals, mut facade_signals) =
            split_app_control_signals(envelopes)?;
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
            Self::classify_uninitialized_cleanup_disposition(UninitializedCleanupDecisionInput {
                control_initialized_now: self.control_initialized(),
                source_signals: &source_signals,
                sink_signals: &sink_signals,
                facade_signals: &facade_signals,
            });
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
                || self.control_initialized()
                || facade_signals
                    .iter()
                    .any(Self::facade_signal_requires_shared_serial_while_uninitialized)
                || runtime_gate.replay_fully_cleared()
        };
        let _serial_guard = self.control_frame_serial.lock().await;
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
        let should_initialize_from_control =
            Self::should_initialize_from_control(&source_signals, &sink_signals, &facade_signals);
        let retained_replay_pending_at_entry = {
            let runtime_gate = self.runtime_control_state();
            runtime_gate.source_state_replay_required() || runtime_gate.sink_state_replay_required()
        };
        let should_run_normal_runtime_initialization = should_initialize_from_control
            && !control_initialized_at_entry
            && !retained_replay_pending_at_entry;
        let initialize_wait_for_source_worker_handoff = !source_signals.is_empty();
        let initialize_wait_for_sink_worker_handoff = !sink_signals.is_empty();
        let request_sensitive =
            !source_signals.is_empty() || !sink_signals.is_empty() || !facade_signals.is_empty();
        if request_sensitive && self.api_request_tracker.inflight() > 0 {
            let skip_request_drain_for_uninitialized_recovery = !self.control_initialized()
                && (should_initialize_from_control || facade_cleanup_only_while_uninitialized);
            if !skip_request_drain_for_uninitialized_recovery {
                self.api_request_tracker.wait_for_drain().await;
            }
        }
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
        let source_generation_cutover_disposition =
            Self::classify_source_generation_cutover_disposition(
                SourceGenerationCutoverDecisionInput {
                    control_initialized_at_entry,
                    source_signals: &source_signals,
                    sink_signals: &sink_signals,
                    facade_signals: &facade_signals,
                },
            );
        let sink_generation_cutover_disposition =
            Self::classify_sink_generation_cutover_disposition(
                SinkGenerationCutoverDecisionInput {
                    control_initialized_at_entry,
                    source_signals: &source_signals,
                    sink_signals: &sink_signals,
                    facade_signals: &facade_signals,
                    sink_signals_in_shared_generation_cutover_lane: self
                        .sink_signals_are_in_shared_generation_cutover_lane(&sink_signals)
                        .await,
                },
            );
        let mut fixed_bind_session = fixed_bind_session;
        if should_initialize_from_control {
            if should_run_normal_runtime_initialization
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
            }
        } else if !self.control_initialized()
            && !facade_cleanup_only_while_uninitialized
            && !source_cleanup_only_while_uninitialized
            && !sink_cleanup_only_while_uninitialized
        {
            if retained_replay_pending_at_entry {
                self.recover_retained_replay_readiness_inside_control_frame(&fixed_bind_session)
                    .await?;
                if self.control_initialized() {
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
            self.withdraw_uninitialized_query_routes().await;
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
            source_tick_gate_only_while_sink_replay_pending: !source_signals.is_empty()
                && !control_initialized_at_entry
                && !self.control_initialized()
                && !runtime_gate.source_state_replay_required()
                && runtime_gate.sink_state_replay_required()
                && self
                    .source_signals_are_retained_or_forward_generation_ticks(&source_signals)
                    .await,
            source_signals_present: !source_signals.is_empty(),
            sink_signals_present: !sink_signals.is_empty(),
        };
        let source_wave_disposition = Self::classify_source_control_wave_disposition(
            control_wave_observation,
            &source_signals,
        );
        let sink_wave_disposition = Self::classify_sink_control_wave_disposition(
            control_wave_observation,
            source_wave_disposition,
            &sink_signals,
        );
        if facade_cleanup_only_while_uninitialized {
            for signal in facade_publication_signals.drain(..) {
                fixed_bind_session = self
                    .apply_facade_signal_with_session(fixed_bind_session, signal)
                    .await?;
            }
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
                note_sink_apply_entry_for_tests();
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
            }
        }
        match source_wave_disposition {
            SourceControlWaveDisposition::CleanupOnlyWhileUninitialized => {
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
            SourceControlWaveDisposition::ApplySignals => {
                if self
                    .source_generation_cutover_apply_should_defer_inline(
                        &source_signals,
                        source_generation_cutover_disposition,
                    )
                    .await
                {
                    self.defer_source_generation_cutover_replay_inline(&source_signals)
                        .await?;
                    let require_sink_replay = !sink_signals.is_empty()
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
                    eprintln!(
                        "fs_meta_runtime_app: on_control_frame deferred inline source replay after fail-closed generation cutover"
                    );
                    return Ok(());
                }
                #[cfg(test)]
                note_source_apply_entry_for_tests();
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
                if runtime_gate.initial_mixed_source_to_sink_pretrigger_eligible(
                    control_initialized_at_entry,
                    self.runtime_control_state(),
                    !sink_signals.is_empty(),
                ) {
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
                if self
                    .source_generation_cutover_replay_should_defer_inline(&[])
                    .await
                {
                    self.defer_source_generation_cutover_replay_inline(&[])
                        .await?;
                    let require_sink_replay = !sink_signals.is_empty()
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
                    eprintln!(
                        "fs_meta_runtime_app: on_control_frame deferred inline retained source replay after fail-closed generation cutover"
                    );
                    return Ok(());
                }
                #[cfg(test)]
                note_source_apply_entry_for_tests();
                #[cfg(test)]
                maybe_pause_before_source_apply().await;
                if debug_control_frame {
                    eprintln!("fs_meta_runtime_app: on_control_frame source.replay_retained begin");
                }
                self.apply_source_signals_with_recovery(
                    &fixed_bind_session,
                    &[],
                    control_initialized_at_entry,
                    SourceGenerationCutoverDisposition::None,
                )
                .await?;
                if debug_control_frame {
                    eprintln!("fs_meta_runtime_app: on_control_frame source.replay_retained ok");
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
                        "fs_meta_runtime_app: on_control_frame deferred same-frame sink replay after source fail-closed retained replay"
                    );
                    return Ok(());
                }
            }
            SourceControlWaveDisposition::Idle => {}
        }
        let mut replayed_sink_state_after_uninitialized_source_recovery = false;
        match sink_wave_disposition {
            SinkControlWaveDisposition::ApplyBeforeInitialSourceWave => {}
            SinkControlWaveDisposition::CleanupOnlyQueryWhileUninitialized => {
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame sink cleanup-only query followup left runtime uninitialized"
                );
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
                    #[cfg(test)]
                    note_sink_apply_entry_for_tests();
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
                if self
                    .sink_generation_cutover_replay_should_defer_inline()
                    .await
                {
                    self.mark_control_uninitialized_after_deferred_sink_replay_with_session(
                        &fixed_bind_session,
                    )
                    .await;
                    eprintln!(
                        "fs_meta_runtime_app: on_control_frame deferred inline retained sink replay after fail-closed generation cutover"
                    );
                    return Ok(());
                }
                #[cfg(test)]
                note_sink_apply_entry_for_tests();
                #[cfg(test)]
                maybe_pause_before_sink_apply().await;
                if debug_control_frame {
                    eprintln!("fs_meta_runtime_app: on_control_frame sink.replay_retained begin");
                }
                let replay_signals = current_generation_sink_replay_tick(
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
                )
                .await?;
                if debug_control_frame {
                    eprintln!("fs_meta_runtime_app: on_control_frame sink.replay_retained ok");
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
            SinkControlWaveDisposition::Idle => {}
        }
        let deferred_local_sink_replay_signals =
            if replayed_sink_state_after_uninitialized_source_recovery
                && sink_status_publication_present
            {
                self.current_generation_retained_sink_replay_signals_for_local_republish()
                    .await
            } else {
                Vec::new()
            };
        sink_recovery_tail_plan.gate_reopen_disposition = self
            .determine_sink_recovery_gate_reopen_disposition(
                replayed_sink_state_after_uninitialized_source_recovery,
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
        if !gate_reopen_deferred && self.runtime_control_state().replay_fully_cleared() {
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
        owner.api_request_tracker.wait_for_drain().await;
        owner.api_control_gate.wait_for_facade_request_drain().await;
    }

    async fn service_close_with_failure_and_session(
        &self,
        fixed_bind_session: FixedBindLifecycleSession,
    ) -> std::result::Result<(), RuntimeCloseFailure> {
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
