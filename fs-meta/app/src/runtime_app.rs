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
use crate::api::{ApiControlGate, ApiRequestTracker};
use crate::domain_state::FacadeServiceState;
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
};
use crate::runtime::unit_gate::RuntimeUnitGate;
use crate::workers::sink::{SinkFacade, SinkWorkerClientHandle};
use crate::workers::source::{SourceFacade, SourceWorkerClientHandle};
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
const FIXED_BIND_HANDOFF_LATE_REQUEST_GRACE: Duration = Duration::from_millis(25);
const SOURCE_CONTROL_RECOVERY_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_FRAME_LEASE_ACQUIRE_BUDGET: Duration = Duration::from_secs(1);
const CONTROL_FRAME_LEASE_RETRY_INTERVAL: Duration = Duration::from_millis(10);
const INTERNAL_SINK_STATUS_BLOCKING_FALLBACK_BUDGET: Duration = Duration::from_millis(250);
const RUNTIME_APP_POLL_INTERVAL: Duration = Duration::from_millis(50);

struct FacadeActivation {
    route_key: String,
    generation: u64,
    resource_ids: Vec<String>,
    handle: api::ApiServerHandle,
}

impl FacadeActivation {
    fn matches_route_resources(&self, route_key: &str, resource_ids: &[String]) -> bool {
        self.route_key == route_key && self.resource_ids == resource_ids
    }

    fn matches_pending(&self, pending: &PendingFacadeActivation) -> bool {
        self.matches_route_resources(&pending.route_key, &pending.resource_ids)
            && self.generation == pending.generation
    }
}

#[derive(Clone)]
struct FacadeSpawnInProgress {
    route_key: String,
    resource_ids: Vec<String>,
}

impl FacadeSpawnInProgress {
    fn from_pending(pending: &PendingFacadeActivation) -> Self {
        Self {
            route_key: pending.route_key.clone(),
            resource_ids: pending.resource_ids.clone(),
        }
    }

    fn matches_pending(&self, pending: &PendingFacadeActivation) -> bool {
        pending.matches_route_resources(&self.route_key, &self.resource_ids)
    }
}

#[derive(Clone)]
struct ProcessFacadeClaim {
    owner_instance_id: u64,
    #[cfg_attr(not(test), allow(dead_code))]
    route_key: String,
    #[cfg_attr(not(test), allow(dead_code))]
    resource_ids: Vec<String>,
    bind_addr: String,
}

impl ProcessFacadeClaim {
    fn from_pending(owner_instance_id: u64, pending: &PendingFacadeActivation) -> Option<Self> {
        if facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr) {
            return None;
        }
        Some(Self {
            owner_instance_id,
            route_key: pending.route_key.clone(),
            resource_ids: pending.resource_ids.clone(),
            bind_addr: pending.resolved.bind_addr.clone(),
        })
    }

    fn matches_pending(&self, pending: &PendingFacadeActivation) -> bool {
        self.bind_addr == pending.resolved.bind_addr
    }
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
struct RuntimeGateFacts {
    control_initialized: bool,
    source_state_replay_required: bool,
    sink_state_replay_required: bool,
}

impl RuntimeGateFacts {
    fn from_atomic_flags(
        control_initialized: &AtomicBool,
        source_state_replay_required: &AtomicBool,
        sink_state_replay_required: &AtomicBool,
    ) -> Self {
        Self {
            control_initialized: control_initialized.load(Ordering::Acquire),
            source_state_replay_required: source_state_replay_required.load(Ordering::Acquire),
            sink_state_replay_required: sink_state_replay_required.load(Ordering::Acquire),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FacadeServiceStateDecisionInput {
    control_gate_ready: bool,
    publication_ready: bool,
    pending_facade_present: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FacadeReadyTailDecisionInput {
    runtime: RuntimeGateFacts,
    allow_facade_only_handoff: bool,
    publication_ready: bool,
    pending_facade_present: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FacadePublicationReadinessDecisionInput {
    runtime: RuntimeGateFacts,
    allow_facade_only_handoff: bool,
    pending_facade_present: bool,
    pending_facade_is_control_route: bool,
    pending_fixed_bind_has_suppressed_dependent_routes: bool,
    active_control_stream_present: bool,
    active_pending_control_stream_present: bool,
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
    runtime: RuntimeGateFacts,
    current_pending: Option<PendingFacadeActivation>,
    pending_facade_present: bool,
    pending_facade_is_control_route: bool,
    pending_fixed_bind_has_suppressed_dependent_routes: bool,
    active_control_stream_present: bool,
    active_pending_control_stream_present: bool,
    allow_facade_only_handoff: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FixedBindHandoffReleaseDecisionInput {
    handoff_ready_entry_present: bool,
    handoff_ready_owned_by_current_instance: bool,
    pending_facade_present: bool,
    pending_facade_is_control_route: bool,
    pending_runtime_exposure_confirmed: bool,
    suppressed_dependent_routes_remain: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FixedBindHandoffReleaseDecision {
    Blocked,
    Ready,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingFixedBindHandoffAttemptDecisionInput {
    claim_conflict: bool,
    pending_runtime_exposure_confirmed: bool,
    active_owner_present: bool,
    conflicting_process_claim_owner_instance_id: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FacadeDeactivateContinuityDecisionInput {
    retain_active_facade: bool,
    pending_fixed_bind_conflict: bool,
    release_for_fixed_bind_handoff: bool,
    retain_pending_spawn: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FacadeDeactivateContinuityDisposition {
    RetainActiveContinuity,
    RetainActiveWhilePendingFixedBindClaimConflict,
    RetainPendingWhilePendingFixedBindClaimConflict,
    ReleaseActiveForFixedBindHandoff,
    RetainPendingForFixedBindHandoff,
    RetainPendingWhileSpawnInFlight,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingFacadeSpawnContinuityDisposition {
    ReuseCurrentGeneration,
    PromoteExistingSameResource { active_generation: u64 },
    Continue,
}

#[derive(Clone)]
struct PendingFixedBindPublicationState {
    pending: Option<PendingFacadeActivation>,
    conflicting_process_claim: Option<ProcessFacadeClaim>,
    claim_release_followup_pending: bool,
}

#[derive(Clone)]
struct FacadeDeactivateContinuityObservation {
    retain_active_facade: bool,
    retain_pending_spawn: bool,
    pending_fixed_bind_publication: PendingFixedBindPublicationState,
    active_fixed_bind_handoff: Option<FixedBindHandoffObservation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingFixedBindClaimReleaseFollowupDisposition {
    Inactive,
    WaitingForClaimRelease,
    PublicationStillIncomplete,
    PublicationCompleted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingFixedBindRouteSuppressionDisposition {
    Allow,
    SuppressClaimConflict,
    SuppressPublicationIncomplete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingFixedBindPublicationProjection {
    claim_release_followup_required: bool,
    claim_release_followup_disposition: PendingFixedBindClaimReleaseFollowupDisposition,
    route_suppression_disposition: PendingFixedBindRouteSuppressionDisposition,
    handoff_ready_bind_addr_present: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingFixedBindHandoffFollowup {
    bind_addr: String,
    claim_release_followup_required: bool,
}

enum PendingFacadeSpawnFollowupDisposition {
    Spawned {
        spawned: bool,
        pending: Option<PendingFacadeActivation>,
        reset_suppressed_dependent_routes: bool,
    },
    RetryAfterBindAddrInUse {
        pending: PendingFacadeActivation,
        err: CnxError,
    },
    PropagateError(CnxError),
}

#[derive(Clone)]
struct FixedBindHandoffObservation {
    handoff_ready_entry_present: bool,
    handoff_ready_owned_by_current_instance: bool,
    pending_facade_present: bool,
    pending_facade_is_control_route: bool,
    pending_runtime_exposure_confirmed: bool,
    suppressed_dependent_routes_remain: bool,
    claim_conflict: bool,
    active_owner: Option<ActiveFixedBindFacadeRegistrant>,
    conflicting_process_claim: Option<ProcessFacadeClaim>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingFixedBindHandoffAttemptDisposition {
    NoAttempt,
    ReleaseActiveOwner,
    ReleaseConflictingProcessClaim { owner_instance_id: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingFixedBindHandoffCompletionDecisionInput {
    pending_facade_present: bool,
    active_control_stream_present: bool,
    deadline_expired: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingFixedBindHandoffCompletionDisposition {
    ContinuePolling,
    ApplyReadyTail,
    Abort,
}

impl FacadeOnlyHandoffAllowanceDecision {
    fn allows_handoff(self) -> bool {
        !matches!(self, Self::Blocked)
    }
}

impl FacadeGateObservation {
    fn publication_readiness_input(&self) -> FacadePublicationReadinessDecisionInput {
        FacadePublicationReadinessDecisionInput {
            runtime: self.runtime,
            allow_facade_only_handoff: self.allow_facade_only_handoff,
            pending_facade_present: self.current_pending.is_some(),
            pending_facade_is_control_route: self.pending_facade_is_control_route,
            pending_fixed_bind_has_suppressed_dependent_routes: self
                .pending_fixed_bind_has_suppressed_dependent_routes,
            active_control_stream_present: self.active_control_stream_present,
            active_pending_control_stream_present: self.active_pending_control_stream_present,
        }
    }

    fn publication_readiness_decision(&self) -> FacadePublicationReadinessDecision {
        FSMetaApp::facade_publication_readiness_decision_from_runtime_facts(
            self.publication_readiness_input(),
        )
    }

    fn publication_ready(&self) -> bool {
        matches!(
            self.publication_readiness_decision(),
            FacadePublicationReadinessDecision::Ready
        )
    }

    fn ready_tail_decision(&self, publication_ready: bool) -> FacadeReadyTailDecision {
        FSMetaApp::facade_ready_tail_decision_from_runtime_facts(FacadeReadyTailDecisionInput {
            runtime: self.runtime,
            allow_facade_only_handoff: self.allow_facade_only_handoff,
            publication_ready,
            pending_facade_present: self.pending_facade_present,
        })
    }
}

impl FixedBindHandoffObservation {
    fn pending_facts(pending: Option<&PendingFacadeActivation>) -> (bool, bool, bool) {
        (
            pending.is_some(),
            pending.is_some_and(|pending| {
                pending.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
            }),
            pending.is_some_and(|pending| pending.runtime_exposure_confirmed),
        )
    }

    fn suppressed_dependent_routes_remain(
        ready: Option<&PendingFixedBindHandoffReady>,
        fallback: bool,
    ) -> bool {
        ready.map_or(fallback, |ready| {
            ready
                .registrant
                .pending_fixed_bind_has_suppressed_dependent_routes
                .load(Ordering::Acquire)
        })
    }

    fn from_parts(
        current_instance_id: u64,
        bind_addr: &str,
        pending: Option<PendingFacadeActivation>,
        ready: Option<PendingFixedBindHandoffReady>,
        suppressed_dependent_routes_fallback: bool,
        claim_conflict: bool,
        conflicting_process_claim: Option<ProcessFacadeClaim>,
    ) -> Self {
        let (
            pending_facade_present,
            pending_facade_is_control_route,
            pending_runtime_exposure_confirmed,
        ) = Self::pending_facts(pending.as_ref());
        let handoff_ready_owned_by_current_instance = ready
            .as_ref()
            .is_some_and(|ready| ready.registrant.instance_id == current_instance_id);

        Self {
            pending_facade_present,
            pending_facade_is_control_route,
            pending_runtime_exposure_confirmed,
            handoff_ready_entry_present: ready.is_some(),
            handoff_ready_owned_by_current_instance,
            suppressed_dependent_routes_remain: Self::suppressed_dependent_routes_remain(
                ready.as_ref(),
                suppressed_dependent_routes_fallback,
            ),
            claim_conflict,
            active_owner: active_fixed_bind_facade_owner_for(bind_addr, current_instance_id),
            conflicting_process_claim,
        }
    }

    fn release_decision_input(&self) -> FixedBindHandoffReleaseDecisionInput {
        FixedBindHandoffReleaseDecisionInput {
            handoff_ready_entry_present: self.handoff_ready_entry_present,
            handoff_ready_owned_by_current_instance: self.handoff_ready_owned_by_current_instance,
            pending_facade_present: self.pending_facade_present,
            pending_facade_is_control_route: self.pending_facade_is_control_route,
            pending_runtime_exposure_confirmed: self.pending_runtime_exposure_confirmed,
            suppressed_dependent_routes_remain: self.suppressed_dependent_routes_remain,
        }
    }

    fn release_decision(&self) -> FixedBindHandoffReleaseDecision {
        FSMetaApp::fixed_bind_handoff_release_decision(self.release_decision_input())
    }

    fn ready_for_release(&self) -> bool {
        matches!(
            self.release_decision(),
            FixedBindHandoffReleaseDecision::Ready
        )
    }

    fn pending_publication_attempt_input(&self) -> PendingFixedBindHandoffAttemptDecisionInput {
        PendingFixedBindHandoffAttemptDecisionInput {
            claim_conflict: self.claim_conflict,
            pending_runtime_exposure_confirmed: self.pending_runtime_exposure_confirmed,
            active_owner_present: self.active_owner.is_some(),
            conflicting_process_claim_owner_instance_id: self
                .conflicting_process_claim
                .as_ref()
                .map(|claim| claim.owner_instance_id),
        }
    }

    fn pending_publication_attempt_disposition(&self) -> PendingFixedBindHandoffAttemptDisposition {
        FSMetaApp::pending_fixed_bind_handoff_attempt_disposition(
            self.pending_publication_attempt_input(),
        )
    }

    async fn from_ready_entry(
        bind_addr: String,
        ready: Option<PendingFixedBindHandoffReady>,
        current_instance_id: u64,
    ) -> Self {
        let pending = match ready.as_ref() {
            Some(ready) => ready.registrant.pending_facade.lock().await.clone(),
            None => None,
        };
        Self::from_parts(
            current_instance_id,
            &bind_addr,
            pending,
            ready,
            false,
            false,
            None,
        )
    }

    fn from_pending_publication(
        bind_addr: String,
        pending: PendingFacadeActivation,
        ready: Option<PendingFixedBindHandoffReady>,
        publication_state: &PendingFixedBindPublicationState,
        suppressed_dependent_routes_fallback: bool,
        current_instance_id: u64,
    ) -> Self {
        Self::from_parts(
            current_instance_id,
            &bind_addr,
            Some(pending),
            ready,
            suppressed_dependent_routes_fallback,
            publication_state.claim_conflict(),
            publication_state.conflicting_process_claim_for_handoff(),
        )
    }
}

impl PendingFixedBindPublicationState {
    fn inactive(claim_release_followup_pending: bool) -> Self {
        Self {
            pending: None,
            conflicting_process_claim: None,
            claim_release_followup_pending,
        }
    }

    fn new(
        pending: Option<PendingFacadeActivation>,
        conflicting_process_claim: Option<ProcessFacadeClaim>,
        claim_release_followup_pending: bool,
    ) -> Self {
        Self {
            pending,
            conflicting_process_claim,
            claim_release_followup_pending,
        }
    }

    fn publication_incomplete(&self) -> Option<PendingFacadeActivation> {
        self.pending.clone()
    }

    fn claim_conflict(&self) -> bool {
        self.conflicting_process_claim.is_some()
    }

    fn conflicting_process_claim_for_handoff(&self) -> Option<ProcessFacadeClaim> {
        self.claim_release_followup_pending
            .then(|| self.conflicting_process_claim.clone())
            .flatten()
    }

    fn projection(&self) -> PendingFixedBindPublicationProjection {
        let claim_release_followup_required = self.claim_conflict();
        let claim_release_followup_disposition = if !self.claim_release_followup_pending {
            PendingFixedBindClaimReleaseFollowupDisposition::Inactive
        } else if self.publication_incomplete().is_none() {
            PendingFixedBindClaimReleaseFollowupDisposition::PublicationCompleted
        } else if claim_release_followup_required {
            PendingFixedBindClaimReleaseFollowupDisposition::WaitingForClaimRelease
        } else {
            PendingFixedBindClaimReleaseFollowupDisposition::PublicationStillIncomplete
        };
        let route_suppression_disposition = if claim_release_followup_required {
            PendingFixedBindRouteSuppressionDisposition::SuppressClaimConflict
        } else {
            match claim_release_followup_disposition {
                PendingFixedBindClaimReleaseFollowupDisposition::Inactive
                | PendingFixedBindClaimReleaseFollowupDisposition::PublicationCompleted => {
                    PendingFixedBindRouteSuppressionDisposition::Allow
                }
                PendingFixedBindClaimReleaseFollowupDisposition::WaitingForClaimRelease => {
                    PendingFixedBindRouteSuppressionDisposition::SuppressClaimConflict
                }
                PendingFixedBindClaimReleaseFollowupDisposition::PublicationStillIncomplete => {
                    PendingFixedBindRouteSuppressionDisposition::SuppressPublicationIncomplete
                }
            }
        };

        PendingFixedBindPublicationProjection {
            claim_release_followup_required,
            claim_release_followup_disposition,
            route_suppression_disposition,
            handoff_ready_bind_addr_present: self.pending.as_ref().is_some_and(|pending| {
                claim_release_followup_required
                    && pending.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                    && !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
            }),
        }
    }
}

impl PendingFixedBindPublicationProjection {
    fn handoff_followup_for_pending(
        self,
        pending: &PendingFacadeActivation,
    ) -> Option<PendingFixedBindHandoffFollowup> {
        self.handoff_ready_bind_addr_present
            .then(|| PendingFixedBindHandoffFollowup {
                bind_addr: pending.resolved.bind_addr.clone(),
                claim_release_followup_required: self.claim_release_followup_required,
            })
    }
}

impl FacadeDeactivateContinuityObservation {
    fn decision_input(&self) -> FacadeDeactivateContinuityDecisionInput {
        FacadeDeactivateContinuityDecisionInput {
            retain_active_facade: self.retain_active_facade,
            pending_fixed_bind_conflict: self.pending_fixed_bind_publication.claim_conflict(),
            release_for_fixed_bind_handoff: self
                .active_fixed_bind_handoff
                .as_ref()
                .is_some_and(|observation| observation.ready_for_release()),
            retain_pending_spawn: self.retain_pending_spawn,
        }
    }

    fn disposition(&self) -> FacadeDeactivateContinuityDisposition {
        FSMetaApp::facade_deactivate_continuity_disposition(self.decision_input())
    }
}

impl FacadeReadyTailDecisionInput {
    fn facade_service_state_input(
        self,
        control_gate_ready: bool,
    ) -> FacadeServiceStateDecisionInput {
        FacadeServiceStateDecisionInput {
            control_gate_ready,
            publication_ready: self.publication_ready,
            pending_facade_present: self.pending_facade_present,
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
    deferred_local_sink_replay_present: bool,
    cached_sink_status_ready_for_expected_groups: bool,
    source_led_expected_groups: std::collections::BTreeSet<String>,
    recovered_expected_groups: Option<std::collections::BTreeSet<String>>,
}

impl SinkRecoveryGateReopenDisposition {
    fn from_decision_input(input: SinkRecoveryGateReopenDecisionInput) -> Self {
        let SinkRecoveryGateReopenDecisionInput {
            source_led_uninitialized_mixed_recovery,
            deferred_local_sink_replay_present,
            cached_sink_status_ready_for_expected_groups,
            source_led_expected_groups,
            recovered_expected_groups,
        } = input;

        if source_led_uninitialized_mixed_recovery && !source_led_expected_groups.is_empty() {
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

#[derive(Clone, Copy, Debug)]
enum RuntimeScopeConvergenceExpectation<'a> {
    NonemptyFullyConverged,
    ExpectedGroups(&'a std::collections::BTreeSet<String>),
}

#[derive(Clone, Copy, Debug)]
struct RuntimeScopeConvergenceWaitDecisionInput<'a> {
    observation: &'a RuntimeScopeConvergenceObservation,
    expectation: RuntimeScopeConvergenceExpectation<'a>,
    deadline_expired: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeScopeConvergenceWaitDecision {
    Wait,
    Converged,
    ReturnTimeout,
}

impl RuntimeScopeConvergenceObservation {
    fn is_nonempty_fully_converged(&self) -> bool {
        !self.source_groups.is_empty()
            && self.source_groups == self.scan_groups
            && self.source_groups == self.sink_groups
    }

    fn matches_expected(&self, expected_groups: &std::collections::BTreeSet<String>) -> bool {
        self.source_groups == *expected_groups
            && self.scan_groups == *expected_groups
            && self.sink_groups == *expected_groups
    }

    fn timeout_context(&self) -> String {
        format!(
            "source={:?} scan={:?} sink={:?}",
            self.source_groups, self.scan_groups, self.sink_groups
        )
    }

    fn matches_expectation(
        self: &Self,
        expectation: RuntimeScopeConvergenceExpectation<'_>,
    ) -> bool {
        match expectation {
            RuntimeScopeConvergenceExpectation::NonemptyFullyConverged => {
                self.is_nonempty_fully_converged()
            }
            RuntimeScopeConvergenceExpectation::ExpectedGroups(expected_groups) => {
                self.matches_expected(expected_groups)
            }
        }
    }
}

impl RuntimeScopeConvergenceWaitDecisionInput<'_> {
    fn decide(self) -> RuntimeScopeConvergenceWaitDecision {
        if self.observation.matches_expectation(self.expectation) {
            RuntimeScopeConvergenceWaitDecision::Converged
        } else if self.deadline_expired {
            RuntimeScopeConvergenceWaitDecision::ReturnTimeout
        } else {
            RuntimeScopeConvergenceWaitDecision::Wait
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostRecoveryScopeConvergenceTriggerState {
    InitialNotTriggered,
    Triggered,
    RetryPending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostRecoverySinkStatusTimeoutRetryState {
    NotRetried,
    RetriedAfterManualRescan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PostRecoverySinkStatusReadinessWaitState {
    scope_trigger_state: PostRecoveryScopeConvergenceTriggerState,
    sink_timeout_retry_state: PostRecoverySinkStatusTimeoutRetryState,
}

#[derive(Debug)]
struct LocalSinkStatusRepublishWaitState {
    retrigger_state: LocalSinkStatusRepublishRetriggerState,
    manual_rescan_state: LocalSinkStatusRepublishManualRescanState,
    retained_sink_replay_state: LocalSinkStatusRepublishRetainedSinkReplayState,
    #[cfg(test)]
    first_sink_probe_pending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSinkStatusRepublishPendingStep {
    TriggerSourceToSinkConvergence,
    ReplayRetainedSinkWave,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSinkStatusRepublishRetriggerState {
    Pending { count: usize },
    Idle { count: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSinkStatusRepublishManualRescanState {
    NotPublished,
    Published,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSinkStatusRepublishRetainedSinkReplayState {
    None,
    Pending,
    RequireReadyProbe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalSinkStatusRepublishIterationDecisionInput {
    runtime_scope_converged: bool,
    cached_ready_fast_path_satisfied: bool,
    deadline_expired: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSinkStatusRepublishIterationDecision {
    WaitForRuntimeScopeConvergence,
    ReturnReady,
    DrivePendingStepsAndProbe,
    ReturnTimeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSinkStatusRepublishPostProbeDecision {
    ReturnReady,
    ReplayRetainedSinkWave,
    PublishManualRescanFallback,
    ReturnTimeout,
}

impl LocalSinkStatusRepublishIterationDecisionInput {
    fn decide(self) -> LocalSinkStatusRepublishIterationDecision {
        if !self.runtime_scope_converged {
            return LocalSinkStatusRepublishIterationDecision::WaitForRuntimeScopeConvergence;
        }
        if self.cached_ready_fast_path_satisfied {
            return LocalSinkStatusRepublishIterationDecision::ReturnReady;
        }
        if self.deadline_expired {
            LocalSinkStatusRepublishIterationDecision::ReturnTimeout
        } else {
            LocalSinkStatusRepublishIterationDecision::DrivePendingStepsAndProbe
        }
    }
}

impl ControlFrameWaveObservation {
    fn classify_source_control_wave_disposition(
        self,
        source_signals: &[SourceControlSignal],
    ) -> SourceControlWaveDisposition {
        if !source_signals.is_empty() {
            if self.cleanup_disposition.is_source_only() {
                SourceControlWaveDisposition::CleanupOnlyWhileUninitialized
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

impl LocalSinkStatusRepublishRetriggerState {
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

impl LocalSinkStatusRepublishRetainedSinkReplayState {
    fn is_pending(self) -> bool {
        matches!(self, Self::Pending)
    }
}

impl LocalSinkStatusRepublishWaitState {
    fn new(mode: LocalSinkStatusRepublishWaitMode) -> Self {
        Self {
            retrigger_state: match mode {
                LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath => {
                    LocalSinkStatusRepublishRetriggerState::Pending { count: 0 }
                }
                LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady => {
                    LocalSinkStatusRepublishRetriggerState::Idle { count: 0 }
                }
            },
            manual_rescan_state: LocalSinkStatusRepublishManualRescanState::NotPublished,
            retained_sink_replay_state: match mode {
                LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath => {
                    LocalSinkStatusRepublishRetainedSinkReplayState::None
                }
                LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady => {
                    LocalSinkStatusRepublishRetainedSinkReplayState::RequireReadyProbe
                }
            },
            #[cfg(test)]
            first_sink_probe_pending: true,
        }
    }

    async fn trigger_source_to_sink_convergence(
        &mut self,
        source: &Arc<SourceFacade>,
    ) -> Result<()> {
        source.trigger_rescan_when_ready().await?;
        #[cfg(test)]
        maybe_pause_after_local_sink_status_republish_retrigger().await;
        self.retrigger_state.mark_triggered();
        Ok(())
    }

    async fn apply_pending_steps(
        &mut self,
        source: &Arc<SourceFacade>,
        sink: &Arc<SinkFacade>,
        post_return_sink_replay_signals: &[SinkControlSignal],
        remaining: Duration,
    ) -> Result<()> {
        loop {
            match self.next_pending_step() {
                LocalSinkStatusRepublishPendingStep::TriggerSourceToSinkConvergence => {
                    self.trigger_source_to_sink_convergence(source).await?;
                }
                LocalSinkStatusRepublishPendingStep::ReplayRetainedSinkWave => {
                    sink.apply_orchestration_signals_with_total_timeout(
                        post_return_sink_replay_signals,
                        remaining,
                    )
                    .await?;
                    self.retained_sink_replay_state =
                        LocalSinkStatusRepublishRetainedSinkReplayState::None;
                }
                LocalSinkStatusRepublishPendingStep::None => return Ok(()),
            }
        }
    }

    async fn maybe_pause_before_probe(&mut self) {
        #[cfg(test)]
        if self.first_sink_probe_pending {
            maybe_pause_before_local_sink_status_republish_probe().await;
            self.first_sink_probe_pending = false;
        }
    }

    async fn schedule_manual_rescan_fallback(
        &mut self,
        source: &Arc<SourceFacade>,
        post_return_sink_replay_signals: &[SinkControlSignal],
    ) -> Result<()> {
        if matches!(
            self.manual_rescan_state,
            LocalSinkStatusRepublishManualRescanState::Published
        ) {
            return Ok(());
        }
        // Retained sink replay can leave the local sink status lane effectively fresh
        // again. Force one baseline replay pass, then allow one more direct
        // source->sink retrigger so the helper waits on a real rematerialization
        // instead of timing out against an empty-but-scheduled local summary.
        source.publish_manual_rescan_signal().await?;
        self.manual_rescan_state = LocalSinkStatusRepublishManualRescanState::Published;
        self.retained_sink_replay_state = if post_return_sink_replay_signals.is_empty() {
            LocalSinkStatusRepublishRetainedSinkReplayState::None
        } else {
            LocalSinkStatusRepublishRetainedSinkReplayState::Pending
        };
        if self.retrigger_state.count() < 2 {
            self.trigger_source_to_sink_convergence(source).await?;
        }
        Ok(())
    }

    fn post_probe_decision(
        &mut self,
        probe_outcome: LocalSinkStatusRepublishProbeOutcome,
        post_return_sink_replay_signals: &[SinkControlSignal],
        deadline_expired: bool,
    ) -> LocalSinkStatusRepublishPostProbeDecision {
        match probe_outcome {
            LocalSinkStatusRepublishProbeOutcome::Ready => match self.retained_sink_replay_state {
                LocalSinkStatusRepublishRetainedSinkReplayState::RequireReadyProbe => {
                    self.retained_sink_replay_state = if post_return_sink_replay_signals.is_empty()
                    {
                        LocalSinkStatusRepublishRetainedSinkReplayState::None
                    } else {
                        LocalSinkStatusRepublishRetainedSinkReplayState::Pending
                    };
                    if self.retained_sink_replay_state.is_pending() {
                        LocalSinkStatusRepublishPostProbeDecision::ReplayRetainedSinkWave
                    } else {
                        LocalSinkStatusRepublishPostProbeDecision::ReturnReady
                    }
                }
                _ => LocalSinkStatusRepublishPostProbeDecision::ReturnReady,
            },
            LocalSinkStatusRepublishProbeOutcome::NotReady if deadline_expired => {
                LocalSinkStatusRepublishPostProbeDecision::ReturnTimeout
            }
            LocalSinkStatusRepublishProbeOutcome::NotReady => {
                LocalSinkStatusRepublishPostProbeDecision::PublishManualRescanFallback
            }
        }
    }

    fn next_pending_step(&self) -> LocalSinkStatusRepublishPendingStep {
        if self.retrigger_state.is_pending() {
            return LocalSinkStatusRepublishPendingStep::TriggerSourceToSinkConvergence;
        }
        if self.retained_sink_replay_state.is_pending() {
            return LocalSinkStatusRepublishPendingStep::ReplayRetainedSinkWave;
        }
        LocalSinkStatusRepublishPendingStep::None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSinkStatusRepublishProbeOutcome {
    Ready,
    NotReady,
}

impl PostRecoverySinkStatusReadinessWaitState {
    fn new(source_to_sink_convergence_pretriggered: bool) -> Self {
        Self {
            scope_trigger_state: if source_to_sink_convergence_pretriggered {
                PostRecoveryScopeConvergenceTriggerState::Triggered
            } else {
                PostRecoveryScopeConvergenceTriggerState::InitialNotTriggered
            },
            sink_timeout_retry_state: PostRecoverySinkStatusTimeoutRetryState::NotRetried,
        }
    }

    async fn maybe_drive_pending_scope_convergence_retrigger(
        &mut self,
        source: &Arc<SourceFacade>,
    ) -> Result<()> {
        if matches!(
            self.scope_trigger_state,
            PostRecoveryScopeConvergenceTriggerState::RetryPending
        ) {
            source.trigger_rescan_when_ready().await?;
            self.scope_trigger_state = PostRecoveryScopeConvergenceTriggerState::Triggered;
        }
        Ok(())
    }

    async fn maybe_trigger_initial_scope_convergence(
        &mut self,
        source: &Arc<SourceFacade>,
    ) -> Result<()> {
        if matches!(
            self.scope_trigger_state,
            PostRecoveryScopeConvergenceTriggerState::InitialNotTriggered
        ) {
            source.trigger_rescan_when_ready().await?;
            self.scope_trigger_state = PostRecoveryScopeConvergenceTriggerState::Triggered;
        }
        Ok(())
    }

    async fn maybe_schedule_sink_timeout_retry(
        &mut self,
        source: &Arc<SourceFacade>,
        before_deadline: bool,
    ) -> Result<bool> {
        if !before_deadline
            || !matches!(
                self.sink_timeout_retry_state,
                PostRecoverySinkStatusTimeoutRetryState::NotRetried
            )
        {
            return Ok(false);
        }
        // A later source-only recovery can converge route scopes from primed
        // cached groups before the post-recovery rescan has actually
        // rematerialized baseline source publication. Request one explicit
        // manual rescan before the final source->sink retry so sink readiness
        // wait does not return on a scheduled-only zero-state sink.
        source.publish_manual_rescan_signal().await?;
        self.sink_timeout_retry_state =
            PostRecoverySinkStatusTimeoutRetryState::RetriedAfterManualRescan;
        self.scope_trigger_state = PostRecoveryScopeConvergenceTriggerState::RetryPending;
        Ok(true)
    }
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
    snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>()
}

fn sink_status_snapshot_ready_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
) -> std::collections::BTreeSet<String> {
    snapshot
        .groups
        .iter()
        .filter(|group| sink_group_status_counts_as_ready(group))
        .map(|group| group.group_id.clone())
        .collect::<std::collections::BTreeSet<_>>()
}

fn sink_status_snapshot_has_ready_scheduled_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
) -> bool {
    let scheduled_groups = sink_status_snapshot_scheduled_groups(snapshot);
    if scheduled_groups.is_empty() {
        return false;
    }
    let ready_groups = sink_status_snapshot_ready_groups(snapshot);
    scheduled_groups.is_subset(&ready_groups)
}

fn sink_status_snapshot_ready_for_expected_groups(
    snapshot: &crate::sink::SinkStatusSnapshot,
    expected_groups: &std::collections::BTreeSet<String>,
) -> bool {
    if expected_groups.is_empty() {
        return false;
    }
    let scheduled_groups = sink_status_snapshot_scheduled_groups(snapshot);
    let ready_groups = sink_status_snapshot_ready_groups(snapshot);
    expected_groups.is_subset(&scheduled_groups) && expected_groups.is_subset(&ready_groups)
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
    pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
    facade_spawn_in_progress: Arc<Mutex<Option<FacadeSpawnInProgress>>>,
    facade_pending_status: SharedFacadePendingStatusCell,
    facade_service_state: SharedFacadeServiceStateCell,
    api_request_tracker: Arc<ApiRequestTracker>,
    api_control_gate: Arc<ApiControlGate>,
    control_initialized: Arc<AtomicBool>,
    source_state_replay_required: Arc<AtomicBool>,
    sink_state_replay_required: Arc<AtomicBool>,
    node_id: NodeId,
    runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    source: Arc<SourceFacade>,
    sink: Arc<SinkFacade>,
    query_sink: Arc<SinkFacade>,
    query_runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
}

#[derive(Clone)]
struct ActiveFixedBindFacadeRegistrant {
    instance_id: u64,
    api_task: Arc<Mutex<Option<FacadeActivation>>>,
    api_request_tracker: Arc<ApiRequestTracker>,
    api_control_gate: Arc<ApiControlGate>,
}

impl PendingFixedBindHandoffRegistrant {
    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        instance_id: u64,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
        pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
        facade_spawn_in_progress: Arc<Mutex<Option<FacadeSpawnInProgress>>>,
        facade_pending_status: SharedFacadePendingStatusCell,
        facade_service_state: SharedFacadeServiceStateCell,
        api_request_tracker: Arc<ApiRequestTracker>,
        api_control_gate: Arc<ApiControlGate>,
        control_initialized: Arc<AtomicBool>,
        source_state_replay_required: Arc<AtomicBool>,
        sink_state_replay_required: Arc<AtomicBool>,
        node_id: NodeId,
        runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        query_sink: Arc<SinkFacade>,
        query_runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> Self {
        Self {
            instance_id,
            api_task,
            pending_facade,
            pending_fixed_bind_has_suppressed_dependent_routes,
            facade_spawn_in_progress,
            facade_pending_status,
            facade_service_state,
            api_request_tracker,
            api_control_gate,
            control_initialized,
            source_state_replay_required,
            sink_state_replay_required,
            node_id,
            runtime_boundary,
            source,
            sink,
            query_sink,
            query_runtime_boundary,
        }
    }

    fn runtime_gate_facts(&self) -> RuntimeGateFacts {
        RuntimeGateFacts::from_atomic_flags(
            self.control_initialized.as_ref(),
            self.source_state_replay_required.as_ref(),
            self.sink_state_replay_required.as_ref(),
        )
    }

    fn active_fixed_bind_facade_registrant(&self) -> ActiveFixedBindFacadeRegistrant {
        ActiveFixedBindFacadeRegistrant::from_parts(
            self.instance_id,
            self.api_task.clone(),
            self.api_request_tracker.clone(),
            self.api_control_gate.clone(),
        )
    }

    fn clear_pending_facade_and_publish_ready_tail_after_publication(&self) {
        FSMetaApp::clear_pending_facade_and_publish_ready_tail_from_runtime_facts(
            &self.facade_pending_status,
            &self.facade_service_state,
            &self.api_control_gate,
            FacadeReadyTailDecisionInput {
                runtime: self.runtime_gate_facts(),
                allow_facade_only_handoff: false,
                publication_ready: true,
                pending_facade_present: false,
            },
        );
    }

    async fn apply_forced_handoff_ready_tail(&self, bind_addr: &str) -> bool {
        FSMetaApp::apply_observed_facade_ready_tail_from_parts(
            self.instance_id,
            self.api_task.clone(),
            self.pending_facade.clone(),
            Some(&self.facade_pending_status),
            &self.facade_service_state,
            &self.api_control_gate,
            self.runtime_gate_facts(),
            self.pending_fixed_bind_has_suppressed_dependent_routes
                .load(Ordering::Acquire),
            FacadeOnlyHandoffObservationPolicy::ForceAllowed,
            Some((bind_addr, self.active_fixed_bind_facade_registrant())),
        )
        .await
    }

    async fn apply_pending_facade_spawn_continuity_disposition(
        &self,
        disposition: PendingFacadeSpawnContinuityDisposition,
        pending: &PendingFacadeActivation,
    ) -> Option<bool> {
        match disposition {
            PendingFacadeSpawnContinuityDisposition::ReuseCurrentGeneration => {
                let active_matches = self
                    .api_task
                    .lock()
                    .await
                    .as_ref()
                    .is_some_and(|active| pending.matches_active(active));
                if !active_matches {
                    return None;
                }
                let mut pending_guard = self.pending_facade.lock().await;
                if pending_guard.as_ref().is_some_and(|candidate| {
                    candidate.matches_route_resources_generation(
                        &pending.route_key,
                        &pending.resource_ids,
                        pending.generation,
                    )
                }) {
                    pending_guard.take();
                }
                drop(pending_guard);
                self.clear_pending_facade_and_publish_ready_tail_after_publication();
                Some(true)
            }
            PendingFacadeSpawnContinuityDisposition::PromoteExistingSameResource {
                active_generation,
            } => {
                let mut api_task_guard = self.api_task.lock().await;
                let active_matches = api_task_guard.as_ref().is_some_and(|active| {
                    pending.matches_active_route_resources(active)
                        && active.generation == active_generation
                });
                if !active_matches {
                    return None;
                }
                if let Some(active) = api_task_guard.as_mut() {
                    active.generation = pending.generation;
                }
                drop(api_task_guard);
                let mut pending_guard = self.pending_facade.lock().await;
                if pending_guard.as_ref().is_some_and(|candidate| {
                    candidate.matches_route_resources_generation(
                        &pending.route_key,
                        &pending.resource_ids,
                        pending.generation,
                    )
                }) {
                    pending_guard.take();
                }
                drop(pending_guard);
                self.clear_pending_facade_and_publish_ready_tail_after_publication();
                Some(true)
            }
            PendingFacadeSpawnContinuityDisposition::Continue => None,
        }
    }
}

impl ActiveFixedBindFacadeRegistrant {
    fn from_parts(
        instance_id: u64,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        api_request_tracker: Arc<ApiRequestTracker>,
        api_control_gate: Arc<ApiControlGate>,
    ) -> Self {
        Self {
            instance_id,
            api_task,
            api_request_tracker,
            api_control_gate,
        }
    }
}

#[derive(Clone)]
struct PendingFixedBindHandoffReady {
    registrant: PendingFixedBindHandoffRegistrant,
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

impl PendingFacadeActivation {
    fn matches_route_generation(&self, route_key: &str, generation: u64) -> bool {
        self.route_key == route_key && self.generation == generation
    }

    fn matches_route_resources(&self, route_key: &str, resource_ids: &[String]) -> bool {
        self.route_key == route_key && self.resource_ids == resource_ids
    }

    fn matches_route_resources_generation(
        &self,
        route_key: &str,
        resource_ids: &[String],
        generation: u64,
    ) -> bool {
        self.matches_route_resources(route_key, resource_ids) && self.generation == generation
    }

    fn matches_active(&self, active: &FacadeActivation) -> bool {
        active.matches_pending(self)
    }

    fn matches_active_route_resources(&self, active: &FacadeActivation) -> bool {
        active.matches_route_resources(&self.route_key, &self.resource_ids)
    }

    fn matches_status(&self, status: &SharedFacadePendingStatus) -> bool {
        self.matches_route_resources_generation(
            &status.route_key,
            &status.resource_ids,
            status.generation,
        )
    }
}

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
        }
        FacadeRuntimeUnit::QueryPeer => {
            route_key == sink_query_proxy_route
                || route_key == sink_status_route
                || route_key == source_status_route
                || route_key == source_find_route
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
}

fn is_uninitialized_cleanup_query_route(route_key: &str) -> bool {
    is_internal_status_route(route_key)
        || route_key == format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL)
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

fn is_retryable_worker_control_reset(err: &CnxError) -> bool {
    matches!(err, CnxError::TransportClosed(_) | CnxError::Timeout)
        || matches!(
            err,
            CnxError::PeerError(message) | CnxError::Internal(message)
                if message.contains("operation timed out")
        )
        || matches!(
            err,
            CnxError::PeerError(message)
                if message.contains("transport closed")
                    && (message.contains("Connection reset by peer")
                        || message.contains("early eof")
                        || message.contains("Broken pipe")
                        || message.contains("bridge stopped"))
        )
        || matches!(
            err,
            CnxError::AccessDenied(message) | CnxError::PeerError(message)
                if message.contains("drained/fenced")
                    && message.contains("grant attachments")
        )
}

fn is_retryable_worker_transport_close_reset(err: &CnxError) -> bool {
    matches!(err, CnxError::TransportClosed(_) | CnxError::Timeout)
        || matches!(
            err,
            CnxError::PeerError(message) | CnxError::Internal(message)
                if message.contains("transport closed")
                    && (message.contains("Connection reset by peer")
                        || message.contains("early eof")
                        || message.contains("Broken pipe")
                        || message.contains("bridge stopped"))
        )
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
-> &'static StdMutex<BTreeMap<String, PendingFixedBindHandoffReady>> {
    static CELL: OnceLock<StdMutex<BTreeMap<String, PendingFixedBindHandoffReady>>> =
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
    guard.insert(
        bind_addr.to_string(),
        PendingFixedBindHandoffReady { registrant },
    );
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

fn pending_fixed_bind_handoff_registrant_for(
    bind_addr: &str,
    released_by_instance_id: u64,
) -> Option<PendingFixedBindHandoffRegistrant> {
    let guard = match pending_fixed_bind_handoff_ready_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.get(bind_addr).and_then(|ready| {
        (ready.registrant.instance_id != released_by_instance_id)
            .then_some(ready.registrant.clone())
    })
}

fn pending_fixed_bind_handoff_ready_for(bind_addr: &str) -> Option<PendingFixedBindHandoffReady> {
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
fn notify_facade_shutdown_started() {
    let hook = {
        let guard = match facade_shutdown_start_hook_cell().lock() {
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
        let guard = match local_sink_status_republish_retrigger_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
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
    retained_active_facade_continuity: AtomicBool,
    pending_fixed_bind_claim_release_followup: AtomicBool,
    pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
    api_request_tracker: Arc<ApiRequestTracker>,
    api_control_gate: Arc<ApiControlGate>,
    control_frame_serial: Arc<Mutex<()>>,
    shared_control_frame_serial: Arc<Mutex<()>>,
    facade_pending_status: SharedFacadePendingStatusCell,
    facade_service_state: SharedFacadeServiceStateCell,
    facade_gate: RuntimeUnitGate,
    mirrored_query_peer_routes: Arc<Mutex<std::collections::BTreeMap<String, u64>>>,
    control_initialized: Arc<AtomicBool>,
    retained_source_control_state: Mutex<RetainedSourceControlState>,
    source_state_replay_required: Arc<AtomicBool>,
    retained_sink_control_state: Mutex<RetainedSinkControlState>,
    sink_state_replay_required: Arc<AtomicBool>,
    retained_suppressed_public_query_activates:
        Mutex<std::collections::BTreeMap<String, FacadeControlSignal>>,
    shared_source_route_claims: Arc<Mutex<SharedSourceRouteClaims>>,
    shared_sink_route_claims: Arc<Mutex<SharedSinkRouteClaims>>,
    control_frame_lease_path: Option<std::path::PathBuf>,
    control_init_lock: Mutex<()>,
    closing: AtomicBool,
}

#[derive(Default, Clone)]
struct RetainedSourceControlState {
    latest_host_grant_change: Option<SourceControlSignal>,
    active_by_route: std::collections::BTreeMap<(String, String), SourceControlSignal>,
}

#[derive(Default, Clone)]
struct RetainedSinkControlState {
    latest_host_grant_change: Option<SinkControlSignal>,
    active_by_route: std::collections::BTreeMap<(String, String), SinkControlSignal>,
}

impl FSMetaApp {
    fn sink_status_snapshot_summary_is_empty(snapshot: &crate::sink::SinkStatusSnapshot) -> bool {
        snapshot.groups.is_empty() && snapshot.scheduled_groups_by_node.is_empty()
    }

    fn sink_status_snapshot_has_convergence_evidence(
        snapshot: &crate::sink::SinkStatusSnapshot,
    ) -> bool {
        !snapshot.received_batches_by_node.is_empty()
            || !snapshot.received_events_by_node.is_empty()
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

    async fn observe_runtime_scope_convergence(
        source: &Arc<SourceFacade>,
        sink: &Arc<SinkFacade>,
    ) -> Result<RuntimeScopeConvergenceObservation> {
        Ok(RuntimeScopeConvergenceObservation {
            source_groups: source
                .scheduled_source_group_ids()
                .await?
                .unwrap_or_default(),
            scan_groups: source.scheduled_scan_group_ids().await?.unwrap_or_default(),
            sink_groups: sink.scheduled_group_ids().await?.unwrap_or_default(),
        })
    }

    async fn wait_for_sink_status_republish_readiness_after_recovery(
        &self,
        source_to_sink_convergence_pretriggered: bool,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        let summarize_cached_sink_status_after_scope_convergence_timeout = |app: &Self| {
            app.sink
                .cached_status_snapshot()
                .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                .unwrap_or_else(|err| format!("cached_sink_status_unavailable err={err}"))
        };
        let mut wait_state =
            PostRecoverySinkStatusReadinessWaitState::new(source_to_sink_convergence_pretriggered);
        loop {
            let scope_convergence_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let converged_groups = loop {
                wait_state
                    .maybe_drive_pending_scope_convergence_retrigger(&self.source)
                    .await?;
                let scope_observation =
                    Self::observe_runtime_scope_convergence(&self.source, &self.sink).await?;
                match (RuntimeScopeConvergenceWaitDecisionInput {
                    observation: &scope_observation,
                    expectation: RuntimeScopeConvergenceExpectation::NonemptyFullyConverged,
                    deadline_expired: tokio::time::Instant::now() >= scope_convergence_deadline,
                })
                .decide()
                {
                    RuntimeScopeConvergenceWaitDecision::Converged => {
                        break scope_observation.sink_groups;
                    }
                    RuntimeScopeConvergenceWaitDecision::ReturnTimeout => {
                        return Err(CnxError::Internal(format!(
                            "runtime scope convergence not observed after retained replay: {}",
                            scope_observation.timeout_context()
                        )));
                    }
                    RuntimeScopeConvergenceWaitDecision::Wait => {
                        wait_state
                            .maybe_trigger_initial_scope_convergence(&self.source)
                            .await?;
                        tokio::time::sleep(RUNTIME_APP_POLL_INTERVAL).await;
                    }
                }
            };

            let sink_readiness_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                match tokio::time::timeout(Duration::from_millis(250), self.sink.status_snapshot())
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
                            return Err(CnxError::Internal(format!(
                                "sink status readiness not restored after retained replay once runtime scope converged: {}",
                                summarize_sink_status_endpoint(&snapshot)
                            )));
                        }
                    }
                    Ok(Err(err))
                        if matches!(err, CnxError::Timeout)
                            && wait_state
                                .maybe_schedule_sink_timeout_retry(
                                    &self.source,
                                    tokio::time::Instant::now() < sink_readiness_deadline,
                                )
                                .await? =>
                    {
                        break;
                    }
                    Ok(Err(_err)) if tokio::time::Instant::now() < sink_readiness_deadline => {}
                    Ok(Err(err)) if matches!(err, CnxError::Timeout) => {
                        if self
                            .sink
                            .cached_status_snapshot()
                            .ok()
                            .is_some_and(|snapshot| {
                                Self::sink_status_snapshot_summary_is_empty(&snapshot)
                            })
                        {
                            return Ok(Some(converged_groups));
                        }
                        return Err(CnxError::Internal(format!(
                            "sink status readiness not restored after retained replay once runtime scope converged: groups={:?} raw_timeout=true {}",
                            converged_groups,
                            summarize_cached_sink_status_after_scope_convergence_timeout(self)
                        )));
                    }
                    Ok(Err(err)) => return Err(err),
                    Err(_)
                        if wait_state
                            .maybe_schedule_sink_timeout_retry(
                                &self.source,
                                tokio::time::Instant::now() < sink_readiness_deadline,
                            )
                            .await? =>
                    {
                        break;
                    }
                    Err(_) if tokio::time::Instant::now() < sink_readiness_deadline => {}
                    Err(_) => {
                        if self
                            .sink
                            .cached_status_snapshot()
                            .ok()
                            .is_some_and(|snapshot| {
                                Self::sink_status_snapshot_summary_is_empty(&snapshot)
                            })
                        {
                            return Ok(Some(converged_groups));
                        }
                        return Err(CnxError::Internal(format!(
                            "sink status readiness not restored after retained replay once runtime scope converged: groups={:?} raw_timeout=true {}",
                            converged_groups,
                            summarize_cached_sink_status_after_scope_convergence_timeout(self)
                        )));
                    }
                }
                tokio::time::sleep(RUNTIME_APP_POLL_INTERVAL).await;
            }
        }
    }

    async fn wait_for_local_sink_status_republish_after_recovery_from_parts_with_mode(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        expected_groups: &std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &[SinkControlSignal],
        mode: LocalSinkStatusRepublishWaitMode,
    ) -> Result<()> {
        #[cfg(test)]
        note_local_sink_status_republish_helper_entry_for_tests();
        let summarize_cached_sink_status = |sink: &Arc<SinkFacade>| {
            sink.cached_status_snapshot()
                .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                .unwrap_or_else(|err| format!("cached_sink_status_unavailable err={err}"))
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut wait_state = LocalSinkStatusRepublishWaitState::new(mode);
        loop {
            let scope_observation = Self::observe_runtime_scope_convergence(&source, &sink).await?;
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let runtime_scope_wait_decision = (RuntimeScopeConvergenceWaitDecisionInput {
                observation: &scope_observation,
                expectation: RuntimeScopeConvergenceExpectation::ExpectedGroups(expected_groups),
                deadline_expired: remaining.is_zero(),
            })
            .decide();
            let runtime_scope_converged = matches!(
                runtime_scope_wait_decision,
                RuntimeScopeConvergenceWaitDecision::Converged
            );
            let cached_ready_fast_path_satisfied = runtime_scope_converged
                && matches!(
                    mode,
                    LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath
                )
                && Self::cached_sink_status_ready_for_expected_groups(&sink, expected_groups);
            match (LocalSinkStatusRepublishIterationDecisionInput {
                runtime_scope_converged: matches!(
                    runtime_scope_wait_decision,
                    RuntimeScopeConvergenceWaitDecision::Converged
                ),
                cached_ready_fast_path_satisfied,
                deadline_expired: matches!(
                    runtime_scope_wait_decision,
                    RuntimeScopeConvergenceWaitDecision::ReturnTimeout
                ),
            })
            .decide()
            {
                LocalSinkStatusRepublishIterationDecision::WaitForRuntimeScopeConvergence => {
                    tokio::time::sleep(RUNTIME_APP_POLL_INTERVAL).await;
                    continue;
                }
                LocalSinkStatusRepublishIterationDecision::ReturnReady => return Ok(()),
                LocalSinkStatusRepublishIterationDecision::DrivePendingStepsAndProbe => {}
                LocalSinkStatusRepublishIterationDecision::ReturnTimeout => {
                    return Err(CnxError::Internal(format!(
                        "local sink status republish not restored after retained replay once runtime scope converged: expected_groups={expected_groups:?} {}",
                        summarize_cached_sink_status(&sink)
                    )));
                }
            }
            wait_state
                .apply_pending_steps(&source, &sink, post_return_sink_replay_signals, remaining)
                .await?;
            wait_state.maybe_pause_before_probe().await;
            let post_probe_decision = wait_state.post_probe_decision(
                Self::probe_local_sink_status_republish_readiness(&sink, expected_groups, deadline)
                    .await,
                post_return_sink_replay_signals,
                tokio::time::Instant::now() >= deadline,
            );
            match post_probe_decision {
                LocalSinkStatusRepublishPostProbeDecision::ReplayRetainedSinkWave => continue,
                LocalSinkStatusRepublishPostProbeDecision::ReturnReady => return Ok(()),
                LocalSinkStatusRepublishPostProbeDecision::PublishManualRescanFallback => {}
                LocalSinkStatusRepublishPostProbeDecision::ReturnTimeout => {
                    return Err(CnxError::Internal(format!(
                        "local sink status republish not restored after retained replay once runtime scope converged: expected_groups={expected_groups:?} {}",
                        summarize_cached_sink_status(&sink)
                    )));
                }
            }
            wait_state
                .schedule_manual_rescan_fallback(&source, post_return_sink_replay_signals)
                .await?;
            tokio::time::sleep(RUNTIME_APP_POLL_INTERVAL).await;
        }
    }

    async fn wait_for_local_sink_status_republish_after_recovery_from_parts(
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
        expected_groups: &std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &[SinkControlSignal],
    ) -> Result<()> {
        Self::wait_for_local_sink_status_republish_after_recovery_from_parts_with_mode(
            source,
            sink,
            expected_groups,
            post_return_sink_replay_signals,
            LocalSinkStatusRepublishWaitMode::AllowCachedReadyFastPath,
        )
        .await
    }

    async fn probe_local_sink_status_republish_readiness(
        sink: &Arc<SinkFacade>,
        expected_groups: &std::collections::BTreeSet<String>,
        deadline: tokio::time::Instant,
    ) -> LocalSinkStatusRepublishProbeOutcome {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if Self::nonblocking_sink_status_probe_ready_for_expected_groups(
            sink,
            expected_groups,
            remaining,
        )
        .await
        {
            return LocalSinkStatusRepublishProbeOutcome::Ready;
        }
        let remaining_after_nonblocking_probe =
            deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining_after_nonblocking_probe.is_zero() {
            return LocalSinkStatusRepublishProbeOutcome::NotReady;
        }
        if Self::blocking_sink_status_probe_ready_for_expected_groups(
            sink,
            expected_groups,
            remaining_after_nonblocking_probe,
        )
        .await
        {
            LocalSinkStatusRepublishProbeOutcome::Ready
        } else {
            LocalSinkStatusRepublishProbeOutcome::NotReady
        }
    }

    async fn nonblocking_sink_status_probe_ready_for_expected_groups(
        sink: &Arc<SinkFacade>,
        expected_groups: &std::collections::BTreeSet<String>,
        remaining: Duration,
    ) -> bool {
        match tokio::time::timeout(
            remaining.min(Duration::from_millis(350)),
            sink.status_snapshot_nonblocking(),
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
        sink.cached_status_snapshot().ok().is_some_and(|snapshot| {
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
            sink.status_snapshot(),
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
        expected_groups: &std::collections::BTreeSet<String>,
        post_return_sink_replay_signals: &[SinkControlSignal],
    ) -> Result<()> {
        Self::wait_for_local_sink_status_republish_after_recovery_from_parts_with_mode(
            source,
            sink,
            expected_groups,
            post_return_sink_replay_signals,
            LocalSinkStatusRepublishWaitMode::RequireProbeBeforeReady,
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
            expected_groups,
            &post_return_sink_replay_signals,
        )
        .await
    }

    async fn current_generation_retained_sink_replay_signals_for_local_republish(
        &self,
    ) -> Vec<SinkControlSignal> {
        let generation = self
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
        sink_status_publication_present: bool,
        source_signals: &[SourceControlSignal],
        sink_signals: &[SinkControlSignal],
        pretriggered_source_to_sink_convergence: bool,
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
                .await?
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        } else {
            std::collections::BTreeSet::new()
        };
        let cached_sink_status_ready_for_expected_groups = !source_led_expected_groups.is_empty()
            && self
                .sink
                .cached_status_snapshot()
                .ok()
                .is_some_and(|snapshot| {
                    sink_status_snapshot_ready_for_expected_groups(
                        &snapshot,
                        &source_led_expected_groups,
                    )
                });
        let recovered_expected_groups = self
            .wait_for_sink_status_republish_readiness_after_recovery(
                pretriggered_source_to_sink_convergence,
            )
            .await?;
        let disposition = SinkRecoveryGateReopenDisposition::from_decision_input(
            SinkRecoveryGateReopenDecisionInput {
                source_led_uninitialized_mixed_recovery,
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
        request_sensitive: bool,
        sink_cleanup_only_while_uninitialized: bool,
        sink_recovery_tail_plan: SinkRecoveryTailPlan,
        deferred_local_sink_replay_signals: Vec<SinkControlSignal>,
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
        if !sink_cleanup_only_while_uninitialized {
            self.retry_pending_fixed_bind_facade_after_claim_release_without_query_followup(
                query_publication_followup_present,
            )
            .await?;
        }
        for signal in facade_publication_signals {
            self.apply_facade_signal(signal).await?;
        }
        self.replay_suppressed_public_query_activates_after_publication()
            .await?;
        if !sink_cleanup_only_while_uninitialized {
            self.ensure_runtime_proxy_endpoints_started().await?;
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
                self.suppress_deferred_sink_owned_query_peer_publication_signals(
                    &deferred_sink_owned_query_peer_publication_signals,
                )
                .await?;
                let control_ready_after_republish = self.facade_publication_ready().await;
                self.api_control_gate
                    .set_ready(control_ready_after_republish);
                let _ = self.current_facade_service_state().await;
                Self::spawn_deferred_sink_owned_query_peer_publication_after_local_sink_status_ready(
                    self.instance_id,
                    self.source.clone(),
                    self.sink.clone(),
                    expected_groups,
                    deferred_local_sink_replay_signals,
                    deferred_sink_owned_query_peer_publication_signals,
                    self.facade_gate.clone(),
                    self.mirrored_query_peer_routes.clone(),
                    self.pending_facade.clone(),
                    self.facade_pending_status.clone(),
                    self.api_task.clone(),
                    self.api_control_gate.clone(),
                    self.facade_service_state.clone(),
                    self.control_initialized.clone(),
                    self.source_state_replay_required.clone(),
                    self.sink_state_replay_required.clone(),
                    self.pending_fixed_bind_has_suppressed_dependent_routes.clone(),
                );
            } else {
                self.api_control_gate
                    .set_ready(self.facade_publication_ready().await);
                let _ = self.current_facade_service_state().await;
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
        post_return_sink_replay_signals: Vec<SinkControlSignal>,
        deferred_signals: Vec<FacadeControlSignal>,
        facade_gate: RuntimeUnitGate,
        mirrored_query_peer_routes: Arc<Mutex<std::collections::BTreeMap<String, u64>>>,
        pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
        facade_pending_status: SharedFacadePendingStatusCell,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        api_control_gate: Arc<ApiControlGate>,
        facade_service_state: SharedFacadeServiceStateCell,
        control_initialized: Arc<AtomicBool>,
        source_state_replay_required: Arc<AtomicBool>,
        sink_state_replay_required: Arc<AtomicBool>,
        pending_fixed_bind_has_suppressed_dependent_routes: Arc<AtomicBool>,
    ) {
        tokio::spawn(async move {
            if let Err(err) = Self::wait_for_local_sink_status_republish_after_recovery_from_parts(
                source,
                sink,
                &expected_groups,
                &post_return_sink_replay_signals,
            )
            .await
            {
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
            let _ = Self::apply_observed_facade_ready_tail_from_parts(
                instance_id,
                api_task.clone(),
                pending_facade.clone(),
                Some(&facade_pending_status),
                &facade_service_state,
                &api_control_gate,
                RuntimeGateFacts::from_atomic_flags(
                    control_initialized.as_ref(),
                    source_state_replay_required.as_ref(),
                    sink_state_replay_required.as_ref(),
                ),
                pending_fixed_bind_has_suppressed_dependent_routes.load(Ordering::Acquire),
                FacadeOnlyHandoffObservationPolicy::ForceBlocked,
                None,
            )
            .await;
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
        let source_cfg = config.source.clone();
        let sink_source_cfg = config.source.clone();
        let api_request_tracker = shared_api_request_tracker_for_config(&config.api);
        let api_control_gate = Arc::new(ApiControlGate::new(false));
        let source = match source_worker_binding.mode {
            WorkerMode::Embedded => Arc::new(SourceFacade::local(Arc::new(
                source::FSMetaSource::with_boundaries_and_state(
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
                    return Err(CnxError::InvalidInput(
                        "source worker mode requires runtime-boundary injection".to_string(),
                    ));
                }
            },
        };
        let sink = match sink_worker_binding.mode {
            WorkerMode::Embedded => Arc::new(SinkFacade::local(Arc::new(
                SinkFileMeta::with_boundaries_and_state(
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
                    return Err(CnxError::InvalidInput(
                        "sink worker mode requires runtime-boundary injection".to_string(),
                    ));
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
        Ok(Self {
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
            retained_active_facade_continuity: AtomicBool::new(false),
            pending_fixed_bind_claim_release_followup: AtomicBool::new(false),
            pending_fixed_bind_has_suppressed_dependent_routes: Arc::new(AtomicBool::new(false)),
            api_request_tracker,
            api_control_gate,
            control_frame_serial: Arc::new(Mutex::new(())),
            shared_control_frame_serial,
            facade_pending_status: shared_facade_pending_status_cell(),
            facade_service_state: shared_facade_service_state_cell(),
            facade_gate: RuntimeUnitGate::new(
                "fs-meta",
                &[
                    execution_units::FACADE_RUNTIME_UNIT_ID,
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                ],
            ),
            mirrored_query_peer_routes: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            control_initialized: Arc::new(AtomicBool::new(false)),
            retained_source_control_state: Mutex::new(RetainedSourceControlState::default()),
            source_state_replay_required: Arc::new(AtomicBool::new(false)),
            retained_sink_control_state: Mutex::new(RetainedSinkControlState::default()),
            sink_state_replay_required: Arc::new(AtomicBool::new(false)),
            retained_suppressed_public_query_activates: Mutex::new(
                std::collections::BTreeMap::new(),
            ),
            shared_source_route_claims,
            shared_sink_route_claims,
            control_frame_lease_path,
            control_init_lock: Mutex::new(()),
            closing: AtomicBool::new(false),
        })
    }

    fn control_initialized(&self) -> bool {
        self.control_initialized.load(Ordering::Acquire)
    }

    fn runtime_gate_facts(&self) -> RuntimeGateFacts {
        RuntimeGateFacts {
            control_initialized: self.control_initialized(),
            source_state_replay_required: self.source_state_replay_required.load(Ordering::Acquire),
            sink_state_replay_required: self.sink_state_replay_required.load(Ordering::Acquire),
        }
    }

    fn control_gate_ready_from_runtime_facts(
        runtime: RuntimeGateFacts,
        allow_facade_only_handoff: bool,
    ) -> bool {
        !runtime.source_state_replay_required
            && !runtime.sink_state_replay_required
            && (runtime.control_initialized || allow_facade_only_handoff)
    }

    fn current_facade_service_state_from_runtime_facts(
        input: FacadeServiceStateDecisionInput,
    ) -> FacadeServiceState {
        if input.pending_facade_present {
            FacadeServiceState::Pending
        } else if input.control_gate_ready && input.publication_ready {
            FacadeServiceState::Serving
        } else {
            FacadeServiceState::Unavailable
        }
    }

    fn facade_ready_tail_decision_from_runtime_facts(
        input: FacadeReadyTailDecisionInput,
    ) -> FacadeReadyTailDecision {
        let control_gate_ready = input.publication_ready
            && Self::control_gate_ready_from_runtime_facts(
                input.runtime,
                input.allow_facade_only_handoff,
            );
        let published_state = Self::current_facade_service_state_from_runtime_facts(
            input.facade_service_state_input(control_gate_ready),
        );
        FacadeReadyTailDecision {
            control_gate_ready,
            published_state,
        }
    }

    fn apply_facade_ready_tail_decision(
        api_control_gate: &ApiControlGate,
        facade_service_state: &SharedFacadeServiceStateCell,
        ready_tail_decision: FacadeReadyTailDecision,
    ) {
        api_control_gate.set_ready(ready_tail_decision.control_gate_ready);
        *facade_service_state
            .write()
            .expect("write published facade service state") = ready_tail_decision.published_state;
    }

    fn clear_pending_facade_and_publish_ready_tail_from_runtime_facts(
        facade_pending_status: &SharedFacadePendingStatusCell,
        facade_service_state: &SharedFacadeServiceStateCell,
        api_control_gate: &ApiControlGate,
        input: FacadeReadyTailDecisionInput,
    ) {
        Self::clear_pending_facade_status(facade_pending_status);
        Self::apply_facade_ready_tail_decision(
            api_control_gate,
            facade_service_state,
            Self::facade_ready_tail_decision_from_runtime_facts(input),
        );
    }

    fn facade_publication_readiness_decision_from_runtime_facts(
        input: FacadePublicationReadinessDecisionInput,
    ) -> FacadePublicationReadinessDecision {
        let control_gate_ready = Self::control_gate_ready_from_runtime_facts(
            input.runtime,
            input.allow_facade_only_handoff,
        );
        if !control_gate_ready {
            return FacadePublicationReadinessDecision::BlockedControlGate;
        }
        if !input.pending_facade_present {
            return if input.active_control_stream_present {
                FacadePublicationReadinessDecision::Ready
            } else {
                FacadePublicationReadinessDecision::BlockedWithoutActiveControlStream
            };
        }
        if !input.pending_facade_is_control_route {
            return if input.active_control_stream_present {
                FacadePublicationReadinessDecision::Ready
            } else {
                FacadePublicationReadinessDecision::BlockedWithoutActiveControlStream
            };
        }
        if input.active_pending_control_stream_present {
            FacadePublicationReadinessDecision::Ready
        } else {
            FacadePublicationReadinessDecision::FixedBindHandoffPending
        }
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

    fn fixed_bind_handoff_release_decision(
        input: FixedBindHandoffReleaseDecisionInput,
    ) -> FixedBindHandoffReleaseDecision {
        if !input.handoff_ready_entry_present
            || input.handoff_ready_owned_by_current_instance
            || !input.pending_facade_present
            || !input.pending_facade_is_control_route
        {
            return FixedBindHandoffReleaseDecision::Blocked;
        }
        if input.pending_runtime_exposure_confirmed || !input.suppressed_dependent_routes_remain {
            FixedBindHandoffReleaseDecision::Ready
        } else {
            FixedBindHandoffReleaseDecision::Blocked
        }
    }

    fn facade_deactivate_continuity_disposition(
        input: FacadeDeactivateContinuityDecisionInput,
    ) -> FacadeDeactivateContinuityDisposition {
        if input.retain_active_facade
            && !input.pending_fixed_bind_conflict
            && !input.release_for_fixed_bind_handoff
        {
            return FacadeDeactivateContinuityDisposition::RetainActiveContinuity;
        }
        if input.pending_fixed_bind_conflict {
            return if input.retain_active_facade {
                FacadeDeactivateContinuityDisposition::RetainActiveWhilePendingFixedBindClaimConflict
            } else {
                FacadeDeactivateContinuityDisposition::RetainPendingWhilePendingFixedBindClaimConflict
            };
        }
        if input.release_for_fixed_bind_handoff {
            return if input.retain_active_facade {
                FacadeDeactivateContinuityDisposition::ReleaseActiveForFixedBindHandoff
            } else {
                FacadeDeactivateContinuityDisposition::RetainPendingForFixedBindHandoff
            };
        }
        if input.retain_pending_spawn {
            return FacadeDeactivateContinuityDisposition::RetainPendingWhileSpawnInFlight;
        }
        FacadeDeactivateContinuityDisposition::Shutdown
    }

    fn pending_fixed_bind_handoff_attempt_disposition(
        input: PendingFixedBindHandoffAttemptDecisionInput,
    ) -> PendingFixedBindHandoffAttemptDisposition {
        if !input.claim_conflict || !input.pending_runtime_exposure_confirmed {
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

    fn pending_fixed_bind_handoff_completion_disposition(
        input: PendingFixedBindHandoffCompletionDecisionInput,
    ) -> PendingFixedBindHandoffCompletionDisposition {
        if !input.pending_facade_present && input.active_control_stream_present {
            return PendingFixedBindHandoffCompletionDisposition::ApplyReadyTail;
        }
        if input.deadline_expired {
            return PendingFixedBindHandoffCompletionDisposition::Abort;
        }
        PendingFixedBindHandoffCompletionDisposition::ContinuePolling
    }

    fn pending_facade_spawn_continuity_disposition(
        active: Option<&FacadeActivation>,
        pending: &PendingFacadeActivation,
    ) -> PendingFacadeSpawnContinuityDisposition {
        let Some(active) = active else {
            return PendingFacadeSpawnContinuityDisposition::Continue;
        };
        if pending.matches_active(active) {
            return PendingFacadeSpawnContinuityDisposition::ReuseCurrentGeneration;
        }
        if pending.matches_active_route_resources(active) {
            return PendingFacadeSpawnContinuityDisposition::PromoteExistingSameResource {
                active_generation: active.generation,
            };
        }
        PendingFacadeSpawnContinuityDisposition::Continue
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
            && input.sink_signals.is_empty()
            && input.facade_signals.is_empty()
            && !input.source_signals.is_empty()
            && input
                .source_signals
                .iter()
                .all(Self::source_signal_is_restart_deferred_retire_pending)
        {
            SourceGenerationCutoverDisposition::FailClosedRestartDeferredRetirePending
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
        } else {
            SinkGenerationCutoverDisposition::None
        }
    }

    fn publish_facade_service_state_from_runtime_facts(
        facade_service_state: &SharedFacadeServiceStateCell,
        input: FacadeServiceStateDecisionInput,
    ) -> FacadeServiceState {
        let state = Self::current_facade_service_state_from_runtime_facts(input);
        *facade_service_state
            .write()
            .expect("write published facade service state") = state;
        state
    }

    async fn observe_facade_gate_from_parts(
        instance_id: u64,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
        facade_pending_status: Option<&SharedFacadePendingStatusCell>,
        runtime: RuntimeGateFacts,
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
                (Some(active), Some(pending)) => pending.matches_active(active),
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
            pending_fixed_bind_has_suppressed_dependent_routes,
            active_control_stream_present,
            active_pending_control_stream_present,
            allow_facade_only_handoff,
        }
    }

    async fn apply_observed_facade_ready_tail_from_parts(
        instance_id: u64,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
        facade_pending_status: Option<&SharedFacadePendingStatusCell>,
        facade_service_state: &SharedFacadeServiceStateCell,
        api_control_gate: &ApiControlGate,
        runtime: RuntimeGateFacts,
        pending_fixed_bind_has_suppressed_dependent_routes: bool,
        handoff_policy: FacadeOnlyHandoffObservationPolicy,
        active_fixed_bind_owner: Option<(&str, ActiveFixedBindFacadeRegistrant)>,
    ) -> bool {
        let observation = Self::observe_facade_gate_from_parts(
            instance_id,
            api_task,
            pending_facade,
            facade_pending_status,
            runtime,
            pending_fixed_bind_has_suppressed_dependent_routes,
            handoff_policy,
        )
        .await;
        let publication_ready = observation.publication_ready();
        if publication_ready && let Some((bind_addr, registrant)) = active_fixed_bind_owner {
            mark_active_fixed_bind_facade_owner(bind_addr, registrant);
        }
        Self::apply_facade_ready_tail_decision(
            api_control_gate,
            facade_service_state,
            observation.ready_tail_decision(publication_ready),
        );
        publication_ready
    }

    async fn observe_facade_gate(
        &self,
        handoff_policy: FacadeOnlyHandoffObservationPolicy,
    ) -> FacadeGateObservation {
        Self::observe_facade_gate_from_parts(
            self.instance_id,
            self.api_task.clone(),
            self.pending_facade.clone(),
            Some(&self.facade_pending_status),
            self.runtime_gate_facts(),
            self.pending_fixed_bind_has_suppressed_dependent_routes
                .load(Ordering::Acquire),
            handoff_policy,
        )
        .await
    }

    async fn current_facade_service_state(&self) -> FacadeServiceState {
        let pending_facade_present = self.pending_facade.lock().await.is_some()
            || self
                .facade_pending_status
                .read()
                .ok()
                .is_some_and(|status| status.is_some());
        let publication_ready = self.facade_publication_ready().await;
        Self::publish_facade_service_state_from_runtime_facts(
            &self.facade_service_state,
            FacadeServiceStateDecisionInput {
                control_gate_ready: self.api_control_gate.is_ready(),
                publication_ready,
                pending_facade_present,
            },
        )
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

    fn remaining_initialize_budget(deadline: Option<tokio::time::Instant>) -> Result<Duration> {
        match deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    Err(CnxError::Timeout)
                } else {
                    Ok(remaining)
                }
            }
            None => Ok(Duration::MAX),
        }
    }

    async fn initialize_from_control(
        &self,
        wait_for_source_worker_handoff: bool,
        wait_for_sink_worker_handoff: bool,
    ) -> Result<()> {
        self.initialize_from_control_with_deadline(
            wait_for_source_worker_handoff,
            wait_for_sink_worker_handoff,
            None,
        )
        .await
    }

    async fn initialize_from_control_with_deadline(
        &self,
        wait_for_source_worker_handoff: bool,
        wait_for_sink_worker_handoff: bool,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<()> {
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
            return Err(CnxError::InvalidInput(
                "api.enabled must be true; fs-meta management API boundary is mandatory".into(),
            ));
        }

        if self.sink.is_worker() {
            eprintln!("fs_meta_runtime_app: initialize_from_control sink.ensure_started begin");
            let ensure_started = self.sink.ensure_started();
            if let Some(deadline) = deadline {
                tokio::time::timeout(
                    Self::remaining_initialize_budget(Some(deadline))?,
                    ensure_started,
                )
                .await
                .map_err(|_| CnxError::Timeout)??;
            } else {
                ensure_started.await?;
            }
            eprintln!("fs_meta_runtime_app: initialize_from_control sink.ensure_started ok");
        }
        let mut guard = self.pump_task.lock().await;
        if guard.is_none() {
            if wait_for_source_worker_handoff && wait_for_sink_worker_handoff {
                if let Some(deadline) = deadline {
                    tokio::time::timeout(
                        Self::remaining_initialize_budget(Some(deadline))?,
                        self.wait_for_shared_worker_control_handoff(),
                    )
                    .await
                    .map_err(|_| CnxError::Timeout)?;
                } else {
                    self.wait_for_shared_worker_control_handoff().await;
                }
            } else if wait_for_source_worker_handoff {
                if let Some(deadline) = deadline {
                    tokio::time::timeout(
                        Self::remaining_initialize_budget(Some(deadline))?,
                        self.source.wait_for_control_ops_to_drain_for_handoff(),
                    )
                    .await
                    .map_err(|_| CnxError::Timeout)?;
                } else {
                    self.source
                        .wait_for_control_ops_to_drain_for_handoff()
                        .await;
                }
            } else if wait_for_sink_worker_handoff {
                if let Some(deadline) = deadline {
                    tokio::time::timeout(
                        Self::remaining_initialize_budget(Some(deadline))?,
                        self.sink.wait_for_control_ops_to_drain_for_handoff(),
                    )
                    .await
                    .map_err(|_| CnxError::Timeout)?;
                } else {
                    self.sink.wait_for_control_ops_to_drain_for_handoff().await;
                }
            }
            eprintln!("fs_meta_runtime_app: initialize_from_control source.start begin");
            let start = self
                .source
                .start(self.sink.clone(), self.runtime_boundary.clone());
            *guard = if let Some(deadline) = deadline {
                tokio::time::timeout(Self::remaining_initialize_budget(Some(deadline))?, start)
                    .await
                    .map_err(|_| CnxError::Timeout)??
            } else {
                start.await?
            };
            eprintln!("fs_meta_runtime_app: initialize_from_control source.start ok");
        }
        drop(guard);

        eprintln!("fs_meta_runtime_app: initialize_from_control endpoints begin");
        let ensure_endpoints = self.ensure_runtime_proxy_endpoints_started();
        if let Some(deadline) = deadline {
            tokio::time::timeout(
                Self::remaining_initialize_budget(Some(deadline))?,
                ensure_endpoints,
            )
            .await
            .map_err(|_| CnxError::Timeout)??;
        } else {
            ensure_endpoints.await?;
        }
        eprintln!("fs_meta_runtime_app: initialize_from_control endpoints ok");
        self.control_initialized.store(true, Ordering::Release);
        self.api_control_gate
            .set_ready(self.facade_publication_ready().await);
        let _ = self.current_facade_service_state().await;
        eprintln!("fs_meta_runtime_app: initialize_from_control done");

        Ok(())
    }

    pub async fn start(&self) -> Result<()> {
        self.initialize_from_control(true, true).await
    }

    pub async fn send(&self, events: &[Event]) -> Result<()> {
        self.service_send(events).await
    }

    pub async fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        self.service_recv(opts).await
    }

    async fn service_send(&self, events: &[Event]) -> Result<()> {
        if !self.control_initialized() {
            return Err(Self::not_ready_error());
        }
        self.sink.send(events).await
    }

    async fn service_recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        if !self.control_initialized() {
            return Err(Self::not_ready_error());
        }
        self.sink.recv(opts).await
    }

    async fn ensure_runtime_proxy_endpoints_started(&self) -> Result<()> {
        let Some(boundary) = self.runtime_boundary.clone() else {
            return Ok(());
        };
        let mut tasks = self.runtime_endpoint_tasks.lock().await;
        let mut spawned_routes = self.runtime_endpoint_routes.lock().await;
        tasks.retain(|task| {
            if !task.is_finished() {
                return true;
            }
            eprintln!(
                "fs_meta_runtime_app: pruning finished runtime endpoint route={} reason={}",
                task.route_key(),
                task.finish_reason()
                    .unwrap_or_else(|| "unclassified_finish".to_string())
            );
            false
        });
        spawned_routes.clear();
        for task in tasks.iter() {
            spawned_routes.insert(task.route_key().to_string());
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
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META, METHOD_QUERY) {
            if !query_active || !spawned_routes.insert(route.0.clone()) {
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
            if !internal_query_active || !spawned_routes.insert(route.0.clone()) {
                // Not currently selected as query/query-peer sink-status owner, or already running.
            } else {
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
                assert!(
                    !endpoint_unit_ids.is_empty(),
                    "internal query endpoint unit must exist when route is active"
                );
                let facade_service_state = self.facade_service_state.clone();
                let facade_gate = self.facade_gate.clone();
                let api_task = self.api_task.clone();
                let control_initialized = self.control_initialized.clone();
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
                        let control_initialized = control_initialized.clone();
                        let sink = sink.clone();
                        let route_key = route_key.clone();
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
                                if !internal_query_route_still_active(&facade_gate, &route_key) {
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
                                match sink.status_snapshot_nonblocking().await {
                                    Ok(snapshot) => {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink status endpoint response {}",
                                            summarize_sink_status_endpoint(&snapshot)
                                        );
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
                                        if !internal_query_route_still_active(
                                            &facade_gate,
                                            &route_key,
                                        ) {
                                            eprintln!(
                                                "fs_meta_runtime_app: sink status endpoint fail-closed after route deactivate route={} err={}",
                                                route_key, err
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
                                        let mut terminal_err = err;
                                        let sink_control_inflight =
                                            sink.control_op_inflight().await;
                                        if matches!(terminal_err, CnxError::Timeout)
                                            && !sink_control_inflight
                                            && let Ok(cached_snapshot) =
                                                sink.cached_status_snapshot()
                                            && sink_status_snapshot_has_ready_scheduled_groups(
                                                &cached_snapshot,
                                            )
                                        {
                                            eprintln!(
                                                "fs_meta_runtime_app: sink status endpoint cached fallback err={} {}",
                                                terminal_err,
                                                summarize_sink_status_endpoint(&cached_snapshot)
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
                                                sink.status_snapshot(),
                                            )
                                            .await
                                            {
                                                Ok(Ok(snapshot)) => {
                                                    if !internal_query_route_still_active(
                                                        &facade_gate,
                                                        &route_key,
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
                                                        summarize_sink_status_endpoint(&snapshot)
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
                                                    terminal_err = fallback_err;
                                                }
                                                Err(_) => {
                                                    terminal_err = CnxError::Timeout;
                                                }
                                            }
                                        }
                                        if matches!(terminal_err, CnxError::Timeout)
                                            && control_initialized.load(Ordering::Acquire)
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
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS) {
            if !internal_query_active || !spawned_routes.insert(route.0.clone()) {
                // Not currently selected as query/query-peer source-status owner, or already running.
            } else {
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
                assert!(
                    !endpoint_unit_ids.is_empty(),
                    "internal query endpoint unit must exist when route is active"
                );
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
                                let snapshot = source.observability_snapshot_nonblocking().await;
                                trace_guard.phase("after_source_snapshot_await");
                                eprintln!(
                                    "fs_meta_runtime_app: source status endpoint response node={} groups={} runners={} correlation={:?} trace_id={}",
                                    node_id.0,
                                    snapshot.source_primary_by_group.len(),
                                    snapshot.last_force_find_runner_by_group.len(),
                                    req.metadata().correlation_id,
                                    trace_id
                                );
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
                                            origin_id: req.metadata().origin_id.clone(),
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
            if !internal_query_active || !spawned_routes.insert(route.0.clone()) {
                // Not currently selected as query/query-peer proxy owner, or already running.
            } else {
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
                assert!(
                    !endpoint_unit_ids.is_empty(),
                    "internal query endpoint unit must exist when route is active"
                );
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
                                eprintln!(
                                    "fs_meta_runtime_app: sink query proxy request selected_group={:?} recursive={} path={}",
                                    params.scope.selected_group,
                                    params.scope.recursive,
                                    String::from_utf8_lossy(&params.scope.path)
                                );
                                #[cfg(test)]
                                maybe_pause_runtime_proxy_request("sink_query_proxy").await;
                                if !internal_query_route_still_active(&facade_gate, &route_key) {
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
                                let result = sink.materialized_query_nonblocking(&params).await;
                                match result {
                                    Ok(mut events) => {
                                        eprintln!(
                                            "fs_meta_runtime_app: sink query proxy response events={}",
                                            events.len()
                                        );
                                        for event in &events {
                                            match rmp_serde::from_slice::<MaterializedQueryPayload>(
                                                event.payload_bytes(),
                                            ) {
                                                Ok(MaterializedQueryPayload::Tree(payload)) => {
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
                                        let live_local_sink_status_snapshot =
                                            sink.status_snapshot_nonblocking().await.ok();
                                        let cached_local_sink_status_snapshot =
                                            sink.cached_status_snapshot().ok();
                                        let bridge_sink_status_snapshot =
                                            selected_group_sink_query_bridge_snapshot(
                                                &params,
                                                live_local_sink_status_snapshot.as_ref(),
                                                cached_local_sink_status_snapshot.as_ref(),
                                            );
                                        let local_selected_group_bridge_eligible =
                                            bridge_sink_status_snapshot
                                                .map(|snapshot| {
                                                    selected_group_bridge_eligible_from_sink_status(
                                                        &params, snapshot,
                                                    )
                                                })
                                                .unwrap_or(true);
                                        let should_bridge = should_bridge_selected_group_sink_query(
                                            &params,
                                            &events,
                                            local_selected_group_bridge_eligible,
                                        );
                                        if debug_sink_query_route_trace_enabled() {
                                            eprintln!(
                                                "fs_meta_runtime_app: sink query proxy bridge_decision correlation={:?} selected_group={:?} local_events={} eligible={} should_bridge={}",
                                                req.metadata().correlation_id,
                                                params.scope.selected_group,
                                                events.len(),
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
                                                            bridge_sink_status_snapshot,
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
                                                            eprintln!(
                                                                "fs_meta_runtime_app: sink query proxy bridged internal sink query events={}",
                                                                bridged.len()
                                                            );
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
                                                let snapshot = source
                                                    .observability_snapshot_nonblocking()
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
        if let Ok(route) = routes.resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_FIND) {
            if !internal_query_active || !spawned_routes.insert(route.0.clone()) {
                // Not currently selected as query/query-peer source-find owner, or already running.
            } else {
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
                assert!(
                    !endpoint_unit_ids.is_empty(),
                    "internal query endpoint unit must exist when route is active"
                );
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
                                match source.force_find(&params).await {
                                    Ok(mut events) => {
                                        eprintln!(
                                            "fs_meta_runtime_app: source find proxy response events={}",
                                            events.len()
                                        );
                                        if debug_force_find_runner_capture_enabled() {
                                            let last_runner = source
                                                .last_force_find_runner_by_group_snapshot()
                                                .await
                                                .unwrap_or_default();
                                            let inflight = source
                                                .force_find_inflight_groups_snapshot()
                                                .await
                                                .unwrap_or_default();
                                            let response_origins = events
                                                .iter()
                                                .map(|event| event.metadata().origin_id.0.clone())
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
                                            err
                                        );
                                        if debug_force_find_runner_capture_enabled() {
                                            let last_runner = source
                                                .last_force_find_runner_by_group_snapshot()
                                                .await
                                                .unwrap_or_default();
                                            let inflight = source
                                                .force_find_inflight_groups_snapshot()
                                                .await
                                                .unwrap_or_default();
                                            eprintln!(
                                                "fs_meta_runtime_app: source find proxy runner_capture_failed node={} selected_group={:?} path={} err={} last_runner={:?} inflight={:?}",
                                                node_id.0,
                                                params.scope.selected_group,
                                                String::from_utf8_lossy(&params.scope.path),
                                                err,
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
    ) -> Result<Vec<String>> {
        let mut source_groups = source
            .scheduled_source_group_ids()
            .await?
            .unwrap_or_default();
        let scan_groups = source.scheduled_scan_group_ids().await?.unwrap_or_default();
        source_groups.extend(scan_groups);
        let sink_groups = sink.scheduled_group_ids().await?.unwrap_or_default();
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
    ) -> Result<Vec<String>> {
        let logical_root_ids = source
            .logical_roots_snapshot()
            .await?
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
                if let Some(group_id) = source.resolve_group_id_for_object_ref(trimmed).await? {
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
    ) -> Result<std::collections::BTreeSet<String>> {
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
    ) -> Result<bool> {
        let source_status = source.status_snapshot().await?;
        let sink_status = sink.status_snapshot().await?;
        let candidate_groups = Self::observation_candidate_group_ids(source, sink, pending).await?;
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
            let state = self
                .observe_pending_fixed_bind_publication_state_for_pending(&pending)
                .await;
            let _ = self.apply_pending_fixed_bind_handoff_followup(
                state.projection().handoff_followup_for_pending(&pending),
            );
        } else {
            let _ = self.apply_pending_fixed_bind_handoff_followup(None);
        }
        if reset_suppressed_dependent_routes {
            self.pending_fixed_bind_has_suppressed_dependent_routes
                .store(false, Ordering::Release);
        }
        self.refresh_active_fixed_bind_facade_owner().await;
    }

    fn apply_pending_fixed_bind_handoff_followup(
        &self,
        followup: Option<PendingFixedBindHandoffFollowup>,
    ) -> bool {
        if let Some(followup) = &followup {
            mark_pending_fixed_bind_handoff_ready_with_registrant(
                &followup.bind_addr,
                self.pending_fixed_bind_handoff_registrant(),
            );
        }
        let claim_release_followup = followup
            .as_ref()
            .is_some_and(|followup| followup.claim_release_followup_required);
        self.pending_fixed_bind_claim_release_followup
            .store(claim_release_followup, Ordering::Release);
        claim_release_followup
    }

    fn apply_pending_fixed_bind_bind_addr_in_use_followup(&self, bind_addr: &str) {
        let _ =
            self.apply_pending_fixed_bind_handoff_followup(Some(PendingFixedBindHandoffFollowup {
                bind_addr: bind_addr.to_string(),
                claim_release_followup_required: true,
            }));
    }

    fn pending_facade_spawn_followup_disposition(
        spawn_result: Result<bool>,
        pending: Option<PendingFacadeActivation>,
    ) -> PendingFacadeSpawnFollowupDisposition {
        match (spawn_result, pending) {
            (Ok(spawned), pending) => PendingFacadeSpawnFollowupDisposition::Spawned {
                spawned,
                reset_suppressed_dependent_routes: spawned || pending.is_none(),
                pending,
            },
            (Err(err), Some(pending))
                if Self::facade_spawn_error_is_bind_addr_in_use(&err)
                    && pending.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                    && !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr) =>
            {
                PendingFacadeSpawnFollowupDisposition::RetryAfterBindAddrInUse { pending, err }
            }
            (Err(err), _) => PendingFacadeSpawnFollowupDisposition::PropagateError(err),
        }
    }

    async fn apply_pending_facade_spawn_followup(
        &self,
        disposition: PendingFacadeSpawnFollowupDisposition,
    ) -> Result<bool> {
        match disposition {
            PendingFacadeSpawnFollowupDisposition::Spawned {
                spawned,
                pending,
                reset_suppressed_dependent_routes,
            } => {
                self.apply_pending_fixed_bind_spawn_success_followup(
                    pending,
                    reset_suppressed_dependent_routes,
                )
                .await;
                Ok(spawned)
            }
            PendingFacadeSpawnFollowupDisposition::RetryAfterBindAddrInUse { pending, err } => {
                Self::record_pending_facade_retry_error(
                    &self.facade_pending_status,
                    &pending,
                    &err,
                );
                self.apply_pending_fixed_bind_bind_addr_in_use_followup(
                    &pending.resolved.bind_addr,
                );
                Ok(false)
            }
            PendingFacadeSpawnFollowupDisposition::PropagateError(err) => Err(err),
        }
    }

    async fn try_spawn_pending_facade(&self) -> Result<bool> {
        let spawn_result = Self::try_spawn_pending_facade_from_registrant(
            self.pending_fixed_bind_handoff_registrant(),
        )
        .await;
        let pending = self.pending_facade.lock().await.clone();
        self.apply_pending_facade_spawn_followup(Self::pending_facade_spawn_followup_disposition(
            spawn_result,
            pending,
        ))
        .await
    }

    async fn try_spawn_pending_facade_from_registrant(
        registrant: PendingFixedBindHandoffRegistrant,
    ) -> Result<bool> {
        Self::try_spawn_pending_facade_from_registrant_with_spawn(
            registrant,
            |resolved,
             node_id,
             runtime_boundary,
             source,
             sink,
             query_sink,
             query_runtime_boundary,
             facade_pending_status,
             facade_service_state,
             api_request_tracker,
             api_control_gate| async move {
                api::spawn(
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
                .await
            },
        )
        .await
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
        control_initialized: Arc<AtomicBool>,
        source_state_replay_required: Arc<AtomicBool>,
        sink_state_replay_required: Arc<AtomicBool>,
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
            PendingFixedBindHandoffRegistrant::from_parts(
                instance_id,
                api_task,
                pending_facade,
                pending_fixed_bind_has_suppressed_dependent_routes,
                facade_spawn_in_progress,
                facade_pending_status,
                facade_service_state,
                api_request_tracker,
                api_control_gate,
                control_initialized,
                source_state_replay_required,
                sink_state_replay_required,
                node_id,
                runtime_boundary,
                source,
                sink,
                query_sink,
                query_runtime_boundary,
            ),
            spawn_facade,
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
            Arc<ApiRequestTracker>,
            Arc<ApiControlGate>,
        ) -> SpawnFut,
        SpawnFut: std::future::Future<Output = Result<api::ApiServerHandle>>,
    {
        let PendingFixedBindHandoffRegistrant {
            instance_id,
            api_task,
            pending_facade,
            pending_fixed_bind_has_suppressed_dependent_routes: _,
            facade_spawn_in_progress,
            facade_pending_status,
            facade_service_state,
            api_request_tracker,
            api_control_gate,
            control_initialized: _,
            source_state_replay_required: _,
            sink_state_replay_required: _,
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
        if let Some(claim) = ProcessFacadeClaim::from_pending(instance_id, &pending) {
            let mut guard = match process_facade_claim_cell().lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(existing) = guard.get(&claim.bind_addr)
                && existing.owner_instance_id != instance_id
                && existing.matches_pending(&pending)
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
        let (continuity_disposition, replacing_existing) = {
            let api_task_guard = api_task.lock().await;
            (
                Self::pending_facade_spawn_continuity_disposition(
                    api_task_guard.as_ref(),
                    &pending,
                ),
                api_task_guard.is_some(),
            )
        };
        if let Some(result) = registrant
            .apply_pending_facade_spawn_continuity_disposition(continuity_disposition, &pending)
            .await
        {
            return Ok(result);
        }
        if let Some(wait_reason) = Self::facade_replacement_wait_reason(
            source.as_ref(),
            sink.as_ref(),
            &pending,
            replacing_existing,
        )
        .await?
        {
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
                    inflight.matches_pending(&pending)
                );
                return Ok(false);
            }
            *inflight_guard = Some(FacadeSpawnInProgress::from_pending(&pending));
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
            api_request_tracker.clone(),
            api_control_gate.clone(),
        )
        .await;
        {
            let mut inflight_guard = facade_spawn_in_progress.lock().await;
            if inflight_guard
                .as_ref()
                .is_some_and(|inflight| inflight.matches_pending(&pending))
            {
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
                    claim.owner_instance_id == instance_id && claim.matches_pending(&pending)
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
                candidate
                    .matches_route_resources(&pending.route_key, &pending.resource_ids)
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
                claim.owner_instance_id == instance_id && claim.matches_pending(&pending)
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
        clear_pending_fixed_bind_handoff_ready(&pending.resolved.bind_addr);
        eprintln!(
            "fs_meta_runtime_app: facade handle active generation={} route_key={} resources={:?}",
            adopted_generation, pending.route_key, pending.resource_ids
        );
        let mut pending_guard = pending_facade.lock().await;
        if pending_guard.as_ref().is_some_and(|candidate| {
            candidate.matches_route_resources(&pending.route_key, &pending.resource_ids)
        }) {
            pending_guard.take();
        }
        Self::clear_pending_facade_status(&facade_pending_status);
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

    fn pending_status_matches(
        status: &SharedFacadePendingStatus,
        pending: &PendingFacadeActivation,
    ) -> bool {
        pending.matches_status(status)
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
                    Self::pending_status_matches(status, pending).then_some(status.pending_since_us)
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
        if pending.runtime_managed && !Self::observation_eligible_for(source, sink, pending).await?
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
                    Self::pending_status_matches(status, pending).then_some((
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
                    Self::pending_status_matches(status, pending).then_some(
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
                pending
                    .matches_route_generation(route_key, generation)
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
        let has_active = self.api_task.lock().await.is_some();
        if let Some(wait_reason) = Self::facade_replacement_wait_reason(
            self.source.as_ref(),
            self.sink.as_ref(),
            &pending,
            has_active,
        )
        .await?
        {
            Self::set_pending_facade_status_waiting(
                &self.facade_pending_status,
                &pending,
                wait_reason,
            );
            return Ok(());
        }
        if from_tick && !Self::pending_facade_retry_due(&self.facade_pending_status, &pending) {
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

    async fn replay_suppressed_public_query_activates_after_publication(&self) -> Result<()> {
        let facade_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
        let publication_complete = self.pending_facade.lock().await.is_none()
            && self
                .api_task
                .lock()
                .await
                .as_ref()
                .is_some_and(|active| active.route_key == facade_route_key);
        if !publication_complete {
            return Ok(());
        }
        self.pending_fixed_bind_has_suppressed_dependent_routes
            .store(false, Ordering::Release);
        let signals = {
            let mut retained = self.retained_suppressed_public_query_activates.lock().await;
            let drained = std::mem::take(&mut *retained);
            drained.into_values().collect::<Vec<_>>()
        };
        for signal in signals {
            self.apply_facade_signal(signal).await?;
        }
        Ok(())
    }

    fn facade_spawn_error_is_bind_addr_in_use(err: &CnxError) -> bool {
        let message = err.to_string().to_ascii_lowercase();
        message.contains("fs-meta api bind failed") && message.contains("address already in use")
    }

    fn conflicting_process_facade_claim_for_pending(
        instance_id: u64,
        pending: &PendingFacadeActivation,
    ) -> Option<ProcessFacadeClaim> {
        if pending.route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
            || facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
        {
            return None;
        }
        let guard = match process_facade_claim_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.get(&pending.resolved.bind_addr).and_then(|claim| {
            (claim.owner_instance_id != instance_id && claim.matches_pending(pending))
                .then_some(claim.clone())
        })
    }

    async fn observe_pending_fixed_bind_publication_for_pending(
        instance_id: u64,
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        pending: Option<PendingFacadeActivation>,
        claim_release_followup_pending: bool,
    ) -> PendingFixedBindPublicationState {
        let Some(pending) = pending else {
            return PendingFixedBindPublicationState::inactive(claim_release_followup_pending);
        };
        if pending.route_key != format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
            || facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr)
        {
            return PendingFixedBindPublicationState::inactive(claim_release_followup_pending);
        }
        if api_task
            .lock()
            .await
            .as_ref()
            .is_some_and(|active| pending.matches_active_route_resources(active))
        {
            return PendingFixedBindPublicationState::inactive(claim_release_followup_pending);
        }
        PendingFixedBindPublicationState::new(
            Some(pending.clone()),
            Self::conflicting_process_facade_claim_for_pending(instance_id, &pending),
            claim_release_followup_pending,
        )
    }

    async fn observe_pending_fixed_bind_publication_state(
        &self,
    ) -> PendingFixedBindPublicationState {
        let pending = self.pending_facade.lock().await.clone();
        Self::observe_pending_fixed_bind_publication_for_pending(
            self.instance_id,
            self.api_task.clone(),
            pending,
            self.pending_fixed_bind_claim_release_followup
                .load(Ordering::Acquire),
        )
        .await
    }

    async fn observe_pending_fixed_bind_publication_state_for_pending(
        &self,
        pending: &PendingFacadeActivation,
    ) -> PendingFixedBindPublicationState {
        Self::observe_pending_fixed_bind_publication_for_pending(
            self.instance_id,
            self.api_task.clone(),
            Some(pending.clone()),
            self.pending_fixed_bind_claim_release_followup
                .load(Ordering::Acquire),
        )
        .await
    }

    fn clear_pending_fixed_bind_claim_release_followup_if_completed(
        &self,
        disposition: PendingFixedBindClaimReleaseFollowupDisposition,
    ) {
        if matches!(
            disposition,
            PendingFixedBindClaimReleaseFollowupDisposition::PublicationCompleted
        ) {
            self.pending_fixed_bind_claim_release_followup
                .store(false, Ordering::Release);
        }
    }

    async fn settle_pending_fixed_bind_publication_state_after_optional_retry(
        &self,
        state: PendingFixedBindPublicationState,
        retry_pending_facade: bool,
    ) -> Result<PendingFixedBindPublicationState> {
        if !matches!(
            state.projection().claim_release_followup_disposition,
            PendingFixedBindClaimReleaseFollowupDisposition::PublicationStillIncomplete
        ) {
            return Ok(state);
        }

        let pending = state
            .publication_incomplete()
            .expect("publication incomplete disposition requires pending facade");
        if retry_pending_facade {
            self.retry_pending_facade(&pending.route_key, pending.generation, false)
                .await?;
        }
        Ok(self.observe_pending_fixed_bind_publication_state().await)
    }

    async fn pending_fixed_bind_claim_release_followup_disposition(
        &self,
        retry_pending_facade: bool,
    ) -> Result<PendingFixedBindClaimReleaseFollowupDisposition> {
        Ok(self
            .settle_pending_fixed_bind_publication_projection_after_optional_retry(
                retry_pending_facade,
            )
            .await?
            .claim_release_followup_disposition)
    }

    async fn retry_pending_fixed_bind_facade_after_claim_release_without_query_followup(
        &self,
        query_followup_present: bool,
    ) -> Result<()> {
        if query_followup_present {
            return Ok(());
        }
        let _ = self
            .pending_fixed_bind_claim_release_followup_disposition(true)
            .await?;
        Ok(())
    }

    async fn settle_pending_fixed_bind_publication_projection_after_optional_retry(
        &self,
        retry_pending_facade: bool,
    ) -> Result<PendingFixedBindPublicationProjection> {
        let settled = self
            .settle_pending_fixed_bind_publication_state_after_optional_retry(
                self.observe_pending_fixed_bind_publication_state().await,
                retry_pending_facade,
            )
            .await?;
        let projection = settled.projection();
        self.clear_pending_fixed_bind_claim_release_followup_if_completed(
            projection.claim_release_followup_disposition,
        );
        Ok(projection)
    }

    async fn pending_fixed_bind_route_suppression_disposition(
        &self,
        retry_pending_facade: bool,
    ) -> Result<PendingFixedBindRouteSuppressionDisposition> {
        Ok(self
            .settle_pending_fixed_bind_publication_projection_after_optional_retry(
                retry_pending_facade,
            )
            .await?
            .route_suppression_disposition)
    }

    async fn retained_active_facade_continuity_keeps_internal_status_routes(&self) -> bool {
        if !self
            .retained_active_facade_continuity
            .load(Ordering::Acquire)
        {
            return false;
        }
        let facade_control_route_key = format!("{}.stream", ROUTE_KEY_FACADE_CONTROL);
        self.api_task
            .lock()
            .await
            .as_ref()
            .is_some_and(|active| active.route_key == facade_control_route_key)
    }

    fn sink_route_is_facade_dependent_query_request(route_key: &str) -> bool {
        route_key == format!("{}.req", ROUTE_KEY_QUERY)
            || route_key == format!("{}.req", ROUTE_KEY_FORCE_FIND)
            || is_per_peer_sink_query_request_route(route_key)
    }

    async fn filter_sink_facade_dependent_route_activates_during_pending_fixed_bind_claim(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> Vec<SinkControlSignal> {
        let publication_state = self.observe_pending_fixed_bind_publication_state().await;
        let suppression = publication_state.projection().route_suppression_disposition;
        let mut filtered = Vec::with_capacity(sink_signals.len());
        for signal in sink_signals {
            match signal {
                SinkControlSignal::Activate {
                    route_key,
                    generation,
                    ..
                } if Self::sink_route_is_facade_dependent_query_request(route_key)
                    && !matches!(
                        suppression,
                        PendingFixedBindRouteSuppressionDisposition::Allow
                    ) =>
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

    async fn apply_facade_activate(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
        bound_scopes: &[RuntimeBoundScope],
    ) -> Result<()> {
        eprintln!(
            "fs_meta_runtime_app: apply_facade_activate unit={} route_key={} generation={} scopes={}",
            unit.unit_id(),
            route_key,
            generation,
            bound_scopes.len()
        );
        if !facade_route_key_matches(unit, route_key) {
            return Ok(());
        }
        if matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        ) {
            match self
                .pending_fixed_bind_route_suppression_disposition(true)
                .await?
            {
                PendingFixedBindRouteSuppressionDisposition::Allow => {}
                PendingFixedBindRouteSuppressionDisposition::SuppressClaimConflict => {
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
                    return Ok(());
                }
                PendingFixedBindRouteSuppressionDisposition::SuppressPublicationIncomplete => {
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
                        "fs_meta_runtime_app: suppress facade-dependent route unit={} route_key={} generation={} while pending fixed-bind facade publication is still incomplete after predecessor claim release",
                        unit.unit_id(),
                        route_key,
                        generation
                    );
                    return Ok(());
                }
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
            return Ok(());
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
            return Ok(());
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
                active.matches_route_resources(route_key, &candidate_resource_ids)
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
                && current.matches_route_resources(route_key, &candidate_resource_ids)
            {
                current.generation = generation;
                drop(api_task);
                let mut pending = self.pending_facade.lock().await;
                if pending.as_ref().is_some_and(|candidate| {
                    candidate.matches_route_resources(route_key, &candidate_resource_ids)
                }) {
                    pending.take();
                }
                Self::clear_pending_facade_status(&self.facade_pending_status);
                let _ = self.current_facade_service_state().await;
                return Ok(());
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
        let _ = self.current_facade_service_state().await;
        if !self.try_spawn_pending_facade().await? {
            eprintln!(
                "fs_meta_runtime_app: pending facade generation={} route_key={} runtime_exposure_confirmed={}",
                pending.generation, pending.route_key, pending.runtime_exposure_confirmed
            );
        }
        Ok(())
    }

    async fn shutdown_active_facade(&self) {
        self.api_request_tracker.wait_for_drain().await;
        self.api_control_gate.wait_for_facade_request_drain().await;
        #[cfg(test)]
        notify_facade_shutdown_started();
        eprintln!("fs_meta_runtime_app: shutdown_active_facade");
        let released_bind_addr = {
            let api_task = self.api_task.lock().await;
            api_task
                .as_ref()
                .and_then(|current| self.active_fixed_bind_bind_addr_for(current))
        };
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
        if let Some(bind_addr) = released_bind_addr {
            clear_active_fixed_bind_facade_owner(&bind_addr, self.instance_id);
            if let Some(registrant) =
                pending_fixed_bind_handoff_registrant_for(&bind_addr, self.instance_id)
            {
                self.complete_pending_fixed_bind_handoff_after_release_with_registrant(
                    &bind_addr, registrant,
                )
                .await;
            }
        }
        if let Some(pending) = self.pending_facade.lock().await.clone() {
            clear_pending_fixed_bind_handoff_ready(&pending.resolved.bind_addr);
        }
        let _ = self.current_facade_service_state().await;
    }

    async fn withdraw_uninitialized_query_routes(&self) {
        let query_routes = [
            format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
        ];
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
            if is_uninitialized_cleanup_query_route(task.route_key()) {
                task.shutdown(Duration::from_secs(1)).await;
            } else {
                retained.push(task);
            }
        }
        *self.runtime_endpoint_tasks.lock().await = retained;

        let mut routes = self.runtime_endpoint_routes.lock().await;
        routes.retain(|route_key| !is_uninitialized_cleanup_query_route(route_key));
    }

    async fn wait_for_shared_worker_control_handoff(&self) {
        self.source
            .wait_for_control_ops_to_drain_for_handoff()
            .await;
        self.sink.wait_for_control_ops_to_drain_for_handoff().await;
    }

    async fn reinitialize_after_control_reset(&self) -> Result<()> {
        self.control_initialized.store(false, Ordering::Release);
        self.api_control_gate.set_ready(false);
        let _ = self.current_facade_service_state().await;
        self.source_state_replay_required
            .store(true, Ordering::Release);
        self.sink_state_replay_required
            .store(true, Ordering::Release);
        self.initialize_from_control(true, true).await
    }

    async fn reinitialize_after_control_reset_with_deadline(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        self.control_initialized.store(false, Ordering::Release);
        self.api_control_gate.set_ready(false);
        let _ = self.current_facade_service_state().await;
        self.source_state_replay_required
            .store(true, Ordering::Release);
        self.sink_state_replay_required
            .store(true, Ordering::Release);
        self.initialize_from_control_with_deadline(true, true, Some(deadline))
            .await
    }

    async fn mark_control_uninitialized_after_failure(&self) {
        self.control_initialized.store(false, Ordering::Release);
        self.api_control_gate.set_ready(false);
        let _ = self.current_facade_service_state().await;
        self.retained_active_facade_continuity
            .store(false, Ordering::Release);
        self.source_state_replay_required
            .store(true, Ordering::Release);
        self.sink_state_replay_required
            .store(true, Ordering::Release);
        self.clear_shared_source_route_claims_for_instance().await;
        self.clear_shared_sink_route_claims_for_instance().await;
        self.withdraw_uninitialized_query_routes().await;
    }

    async fn source_signals_with_replay(
        &self,
        source_signals: &[SourceControlSignal],
    ) -> Vec<SourceControlSignal> {
        if !self.source_state_replay_required.load(Ordering::Acquire) {
            return source_signals.to_vec();
        }

        if Self::source_signals_are_host_grant_change_only(source_signals) {
            return source_signals.to_vec();
        }

        let mut desired = self.retained_source_control_state.lock().await.clone();
        Self::apply_source_signals_to_state(&mut desired, source_signals);
        let mut replayed = Self::source_signals_from_state(&desired);
        replayed.extend(Self::source_transient_followup_signals(source_signals));
        replayed
    }

    async fn record_retained_source_control_state(&self, source_signals: &[SourceControlSignal]) {
        let mut retained = self.retained_source_control_state.lock().await;
        Self::apply_source_signals_to_state(&mut retained, source_signals);
    }

    fn apply_source_signals_to_state(
        state: &mut RetainedSourceControlState,
        source_signals: &[SourceControlSignal],
    ) {
        for signal in source_signals {
            match signal {
                SourceControlSignal::Activate {
                    unit, route_key, ..
                }
                | SourceControlSignal::Deactivate {
                    unit, route_key, ..
                } => {
                    state.active_by_route.insert(
                        (unit.unit_id().to_string(), route_key.clone()),
                        signal.clone(),
                    );
                }
                SourceControlSignal::RuntimeHostGrantChange { .. } => {
                    state.latest_host_grant_change = Some(signal.clone());
                }
                SourceControlSignal::Tick { .. }
                | SourceControlSignal::ManualRescan { .. }
                | SourceControlSignal::Passthrough(_) => {}
            }
        }
    }

    fn source_signals_from_state(state: &RetainedSourceControlState) -> Vec<SourceControlSignal> {
        let mut merged = Vec::new();
        if let Some(changed) = state.latest_host_grant_change.clone() {
            merged.push(changed);
        }
        merged.extend(state.active_by_route.values().cloned());
        merged
    }

    fn source_signals_are_host_grant_change_only(source_signals: &[SourceControlSignal]) -> bool {
        !source_signals.is_empty()
            && source_signals
                .iter()
                .all(|signal| matches!(signal, SourceControlSignal::RuntimeHostGrantChange { .. }))
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

    async fn apply_source_signals_with_recovery(
        &self,
        source_signals: &[SourceControlSignal],
        control_initialized_at_entry: bool,
        fail_closed_in_generation_cutover_lane: bool,
    ) -> Result<()> {
        let filtered_source_signals = self
            .filter_shared_source_route_deactivates(source_signals)
            .await;
        if control_initialized_at_entry
            && self.control_initialized.load(Ordering::Acquire)
            && !self.source_state_replay_required.load(Ordering::Acquire)
            && !filtered_source_signals.is_empty()
            && filtered_source_signals
                .iter()
                .all(|signal| matches!(signal, SourceControlSignal::Tick { .. }))
        {
            return Ok(());
        }
        let fail_closed_restart_deferred_retire_pending = fail_closed_in_generation_cutover_lane
            && !filtered_source_signals.is_empty()
            && filtered_source_signals
                .iter()
                .all(Self::source_signal_is_restart_deferred_retire_pending);
        let defer_retained_state_until_success = fail_closed_restart_deferred_retire_pending;
        if !defer_retained_state_until_success {
            self.record_retained_source_control_state(&filtered_source_signals)
                .await;
        }
        let replay_followup_signals = if self.source_state_replay_required.load(Ordering::Acquire)
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
                self.source_state_replay_required.load(Ordering::Acquire)
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
                self.source_state_replay_required
                    .store(false, Ordering::Release);
                if replaying_retained_state_only && replay_followup_pending.is_some() {
                    continue;
                }
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            #[cfg(test)]
            let source_apply_result = if let Some(err) = take_source_apply_error_queue_hook() {
                Err(err)
            } else {
                self.source
                    .apply_orchestration_signals_with_total_timeout(
                        &effective_source_signals,
                        remaining,
                    )
                    .await
            };
            #[cfg(not(test))]
            let source_apply_result = self
                .source
                .apply_orchestration_signals_with_total_timeout(
                    &effective_source_signals,
                    remaining,
                )
                .await;
            match source_apply_result {
                Ok(()) => {
                    if defer_retained_state_until_success {
                        self.record_retained_source_control_state(&filtered_source_signals)
                            .await;
                    }
                    self.record_shared_source_route_claims(&effective_source_signals)
                        .await;
                    self.source_state_replay_required
                        .store(false, Ordering::Release);
                    if replaying_retained_state_only {
                        continue;
                    }
                    replay_followup_pending = None;
                    return Ok(());
                }
                Err(err) if fail_closed_restart_deferred_retire_pending => {
                    eprintln!(
                        "fs_meta_runtime_app: source control fail-closed restart_deferred_retire_pending err={}",
                        err
                    );
                    let _ = self
                        .source
                        .reconnect_after_fail_closed_control_error()
                        .await;
                    self.mark_control_uninitialized_after_failure().await;
                    return Ok(());
                }
                Err(err)
                    if is_retryable_worker_control_reset(&err)
                        && tokio::time::Instant::now() < deadline =>
                {
                    eprintln!(
                        "fs_meta_runtime_app: source control replay after retryable reset err={}",
                        err
                    );
                    let _ = self.source.reconnect_after_retryable_control_reset().await;
                    self.reinitialize_after_control_reset_with_deadline(deadline)
                        .await?;
                }
                Err(err) => {
                    self.mark_control_uninitialized_after_failure().await;
                    return Err(err);
                }
            }
        }
    }

    async fn sink_signals_with_replay(
        &self,
        sink_signals: &[SinkControlSignal],
    ) -> Vec<SinkControlSignal> {
        let replay_retained = self.sink_state_replay_required.load(Ordering::Acquire)
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
        sink_signals: &[SinkControlSignal],
        replay_retained_state: bool,
        fail_closed_in_generation_cutover_lane: bool,
    ) -> Result<()> {
        let filtered_sink_signals = self
            .filter_sink_facade_dependent_route_activates_during_pending_fixed_bind_claim(
                &self
                    .filter_shared_sink_route_deactivates(
                        &self.filter_stale_sink_ticks(sink_signals).await,
                    )
                    .await,
            )
            .await;
        if self.control_initialized.load(Ordering::Acquire)
            && !self.sink_state_replay_required.load(Ordering::Acquire)
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
        let deadline = tokio::time::Instant::now() + SINK_CONTROL_RECOVERY_TOTAL_TIMEOUT;
        loop {
            let effective_sink_signals = if replay_retained_state {
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
                    self.sink_state_replay_required
                        .store(false, Ordering::Release);
                }
                return Ok(());
            }
            self.record_shared_sink_route_claims(&effective_sink_signals)
                .await;
            let fail_closed_restart_deferred_retire_pending = fail_closed_in_generation_cutover_lane
                && effective_sink_signals
                    .iter()
                    .all(Self::sink_signal_is_restart_deferred_retire_pending);
            if effective_sink_signals.len() == 1 {
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
                .apply_orchestration_signals_with_total_timeout(&effective_sink_signals, remaining)
                .await
            {
                Ok(()) => {
                    if replay_retained_state {
                        self.record_retained_sink_control_state(&effective_sink_signals)
                            .await;
                    }
                    if replay_retained_state && self.control_initialized.load(Ordering::Acquire) {
                        self.sink_state_replay_required
                            .store(false, Ordering::Release);
                    }
                    return Ok(());
                }
                Err(err) if fail_closed_restart_deferred_retire_pending => {
                    eprintln!(
                        "fs_meta_runtime_app: sink control fail-closed restart_deferred_retire_pending err={}",
                        err
                    );
                    self.mark_control_uninitialized_after_failure().await;
                    return Ok(());
                }
                Err(err)
                    if is_retryable_worker_control_reset(&err)
                        && tokio::time::Instant::now() < deadline =>
                {
                    if fail_closed_in_generation_cutover_lane
                        && is_retryable_worker_transport_close_reset(&err)
                    {
                        self.mark_control_uninitialized_after_failure().await;
                        return Err(err);
                    }
                    eprintln!(
                        "fs_meta_runtime_app: sink control replay after retryable reset err={}",
                        err
                    );
                    if replay_retained_state {
                        self.reinitialize_after_control_reset_with_deadline(deadline)
                            .await?;
                    } else {
                        self.sink.ensure_started().await?;
                    }
                }
                Err(err) => {
                    self.mark_control_uninitialized_after_failure().await;
                    return Err(err);
                }
            }
        }
    }

    async fn apply_facade_deactivate(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> Result<()> {
        eprintln!(
            "fs_meta_runtime_app: apply_facade_deactivate unit={} route_key={} generation={}",
            unit.unit_id(),
            route_key,
            generation
        );
        if !facade_route_key_matches(unit, route_key) {
            return Ok(());
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
            self.sink.wait_for_control_ops_to_drain_for_handoff().await;
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
        if matches!(
            unit,
            FacadeRuntimeUnit::Query | FacadeRuntimeUnit::QueryPeer
        ) && is_facade_dependent_query_route(route_key)
            && self
                .retained_active_facade_continuity_keeps_internal_status_routes()
                .await
        {
            eprintln!(
                "fs_meta_runtime_app: retain facade-dependent query route during continuity-preserving deactivate route_key={} generation={}",
                route_key, generation
            );
            return Ok(());
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
            return Ok(());
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
            return Ok(());
        }
        if !matches!(unit, FacadeRuntimeUnit::Facade) || route_key != facade_control_route_key {
            return Ok(());
        }
        let continuity_observation = self
            .observe_facade_deactivate_continuity(route_key, generation)
            .await;
        let disposition = continuity_observation.disposition();
        match disposition {
            FacadeDeactivateContinuityDisposition::RetainActiveContinuity => {
                self.retained_active_facade_continuity
                    .store(true, Ordering::Release);
                eprintln!(
                    "fs_meta_runtime_app: retain active facade during continuity-preserving deactivate route_key={} generation={}",
                    route_key, generation
                );
                return Ok(());
            }
            FacadeDeactivateContinuityDisposition::RetainActiveWhilePendingFixedBindClaimConflict => {
                self.retained_active_facade_continuity
                    .store(true, Ordering::Release);
                eprintln!(
                    "fs_meta_runtime_app: retain active facade while pending fixed-bind claim remains owned elsewhere route_key={} generation={}",
                    route_key, generation
                );
                return Ok(());
            }
            FacadeDeactivateContinuityDisposition::RetainPendingWhilePendingFixedBindClaimConflict => {
                eprintln!(
                    "fs_meta_runtime_app: retain pending facade during fixed-bind continuity-preserving deactivate route_key={} generation={}",
                    route_key, generation
                );
                return Ok(());
            }
            FacadeDeactivateContinuityDisposition::ReleaseActiveForFixedBindHandoff => {
                eprintln!(
                    "fs_meta_runtime_app: release active facade for fixed-bind handoff route_key={} generation={}",
                    route_key, generation
                );
            }
            FacadeDeactivateContinuityDisposition::RetainPendingForFixedBindHandoff => {
                eprintln!(
                    "fs_meta_runtime_app: retain pending facade during fixed-bind continuity-preserving deactivate route_key={} generation={}",
                    route_key, generation
                );
                return Ok(());
            }
            FacadeDeactivateContinuityDisposition::RetainPendingWhileSpawnInFlight => {
                eprintln!(
                    "fs_meta_runtime_app: retain pending facade during in-flight spawn route_key={} generation={}",
                    route_key, generation
                );
                return Ok(());
            }
            FacadeDeactivateContinuityDisposition::Shutdown => {}
        }
        self.retained_active_facade_continuity
            .store(false, Ordering::Release);
        *self.pending_facade.lock().await = None;
        Self::clear_pending_facade_status(&self.facade_pending_status);
        let _ = self.current_facade_service_state().await;
        let _ = self.current_facade_service_state().await;
        self.wait_for_shared_worker_control_handoff().await;
        self.shutdown_active_facade().await;
        Ok(())
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

    async fn apply_facade_signal(&self, signal: FacadeControlSignal) -> Result<()> {
        match signal {
            FacadeControlSignal::Activate {
                unit,
                route_key,
                generation,
                bound_scopes,
            } => {
                self.apply_facade_activate(unit, &route_key, generation, &bound_scopes)
                    .await?;
            }
            FacadeControlSignal::Deactivate {
                unit,
                route_key,
                generation,
            } => {
                self.apply_facade_deactivate(unit, &route_key, generation)
                    .await?;
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
        Ok(())
    }

    async fn active_fixed_bind_bind_addr(&self) -> Option<String> {
        let api_task = self.api_task.lock().await;
        api_task
            .as_ref()
            .and_then(|active| self.active_fixed_bind_bind_addr_for(active))
    }

    async fn observe_active_fixed_bind_handoff(&self) -> Option<FixedBindHandoffObservation> {
        let bind_addr = self.active_fixed_bind_bind_addr().await?;
        let ready = pending_fixed_bind_handoff_ready_for(&bind_addr);
        Some(
            FixedBindHandoffObservation::from_ready_entry(bind_addr, ready, self.instance_id).await,
        )
    }

    #[cfg(test)]
    async fn fixed_bind_handoff_ready_for_release(&self, generation: u64) -> bool {
        let _ = generation;
        self.observe_active_fixed_bind_handoff()
            .await
            .is_some_and(|observation| observation.ready_for_release())
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
            .is_some_and(|inflight| inflight.matches_pending(&pending))
    }

    async fn observe_facade_deactivate_continuity(
        &self,
        route_key: &str,
        generation: u64,
    ) -> FacadeDeactivateContinuityObservation {
        let retain_active_facade = {
            let active_guard = self.api_task.lock().await;
            active_guard.as_ref().is_some_and(|active| {
                active.route_key == route_key && generation >= active.generation
            })
        };

        FacadeDeactivateContinuityObservation {
            retain_active_facade,
            retain_pending_spawn: self.pending_spawn_in_flight_for_route_key(route_key).await,
            pending_fixed_bind_publication: self
                .observe_pending_fixed_bind_publication_state()
                .await,
            active_fixed_bind_handoff: self.observe_active_fixed_bind_handoff().await,
        }
    }

    fn pending_fixed_bind_handoff_registrant(&self) -> PendingFixedBindHandoffRegistrant {
        PendingFixedBindHandoffRegistrant::from_parts(
            self.instance_id,
            self.api_task.clone(),
            self.pending_facade.clone(),
            self.pending_fixed_bind_has_suppressed_dependent_routes
                .clone(),
            self.facade_spawn_in_progress.clone(),
            self.facade_pending_status.clone(),
            self.facade_service_state.clone(),
            self.api_request_tracker.clone(),
            self.api_control_gate.clone(),
            self.control_initialized.clone(),
            self.source_state_replay_required.clone(),
            self.sink_state_replay_required.clone(),
            self.node_id.clone(),
            self.runtime_boundary.clone(),
            self.source.clone(),
            self.sink.clone(),
            self.query_sink.clone(),
            self.runtime_boundary.clone(),
        )
    }

    fn active_fixed_bind_facade_registrant(&self) -> ActiveFixedBindFacadeRegistrant {
        ActiveFixedBindFacadeRegistrant::from_parts(
            self.instance_id,
            self.api_task.clone(),
            self.api_request_tracker.clone(),
            self.api_control_gate.clone(),
        )
    }

    fn active_fixed_bind_bind_addr_for(&self, active: &FacadeActivation) -> Option<String> {
        (active.route_key == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL))
            .then(|| {
                self.config
                    .api
                    .resolve_for_candidate_ids(&active.resource_ids)
            })
            .flatten()
            .map(|resolved| resolved.bind_addr)
            .filter(|bind_addr| !facade_bind_addr_is_ephemeral(bind_addr))
    }

    async fn refresh_active_fixed_bind_facade_owner(&self) {
        let Some(bind_addr) = self.active_fixed_bind_bind_addr().await else {
            return;
        };
        mark_active_fixed_bind_facade_owner(&bind_addr, self.active_fixed_bind_facade_registrant());
    }

    async fn release_active_fixed_bind_facade_owner_for_handoff(
        bind_addr: &str,
        owner: ActiveFixedBindFacadeRegistrant,
    ) {
        owner.api_request_tracker.wait_for_drain().await;
        owner.api_control_gate.wait_for_facade_request_drain().await;
        #[cfg(test)]
        notify_facade_shutdown_started();
        if let Some(current) = owner.api_task.lock().await.take() {
            tokio::time::sleep(FIXED_BIND_HANDOFF_LATE_REQUEST_GRACE).await;
            owner.api_request_tracker.wait_for_drain().await;
            owner.api_control_gate.wait_for_facade_request_drain().await;
            current
                .handle
                .shutdown(ACTIVE_FACADE_SHUTDOWN_TIMEOUT)
                .await;
        }
        clear_owned_process_facade_claim(owner.instance_id);
        clear_active_fixed_bind_facade_owner(bind_addr, owner.instance_id);
    }

    async fn complete_pending_fixed_bind_handoff_after_release_with_registrant(
        &self,
        bind_addr: &str,
        registrant: PendingFixedBindHandoffRegistrant,
    ) {
        let api_task = registrant.api_task.clone();
        let pending_facade = registrant.pending_facade.clone();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            match Self::try_spawn_pending_facade_from_registrant(registrant.clone()).await {
                Ok(_) => {
                    let completion_disposition =
                        Self::pending_fixed_bind_handoff_completion_disposition(
                            PendingFixedBindHandoffCompletionDecisionInput {
                                pending_facade_present: pending_facade.lock().await.is_some(),
                                active_control_stream_present: api_task
                                    .lock()
                                    .await
                                    .as_ref()
                                    .is_some_and(|active| {
                                        active.route_key
                                            == format!("{}.stream", ROUTE_KEY_FACADE_CONTROL)
                                    }),
                                deadline_expired: tokio::time::Instant::now() >= deadline,
                            },
                        );
                    match completion_disposition {
                        PendingFixedBindHandoffCompletionDisposition::ApplyReadyTail => {
                            #[cfg(test)]
                            maybe_pause_before_pending_fixed_bind_handoff_completion_gate_reopen()
                                .await;
                            let _ = registrant.apply_forced_handoff_ready_tail(bind_addr).await;
                            #[cfg(test)]
                            notify_pending_fixed_bind_handoff_completion_completion();
                            break;
                        }
                        PendingFixedBindHandoffCompletionDisposition::Abort => break,
                        PendingFixedBindHandoffCompletionDisposition::ContinuePolling => {}
                    }
                }
                Err(err) => {
                    if tokio::time::Instant::now() >= deadline {
                        eprintln!(
                            "fs_meta_runtime_app: fixed-bind handoff completion retry failed bind_addr={} err={}",
                            bind_addr, err
                        );
                        break;
                    }
                }
            }
            tokio::time::sleep(RUNTIME_APP_POLL_INTERVAL).await;
        }
    }

    #[cfg(test)]
    async fn complete_pending_fixed_bind_handoff_after_release(
        &self,
        bind_addr: &str,
        released_by_instance_id: u64,
    ) {
        let Some(registrant) =
            pending_fixed_bind_handoff_registrant_for(bind_addr, released_by_instance_id)
        else {
            return;
        };
        self.complete_pending_fixed_bind_handoff_after_release_with_registrant(
            bind_addr, registrant,
        )
        .await;
    }

    async fn observe_pending_fixed_bind_handoff_for_publication(
        &self,
        pending: &PendingFacadeActivation,
    ) -> FixedBindHandoffObservation {
        let bind_addr = pending.resolved.bind_addr.clone();
        mark_pending_fixed_bind_handoff_ready_with_registrant(
            &bind_addr,
            self.pending_fixed_bind_handoff_registrant(),
        );
        let ready = pending_fixed_bind_handoff_ready_for(&bind_addr);
        let state = self
            .observe_pending_fixed_bind_publication_state_for_pending(pending)
            .await;
        FixedBindHandoffObservation::from_pending_publication(
            bind_addr,
            pending.clone(),
            ready,
            &state,
            self.pending_fixed_bind_has_suppressed_dependent_routes
                .load(Ordering::Acquire),
            self.instance_id,
        )
    }

    async fn release_pending_fixed_bind_handoff_blocker(
        &self,
        bind_addr: &str,
        disposition: PendingFixedBindHandoffAttemptDisposition,
    ) -> Option<PendingFixedBindHandoffRegistrant> {
        match disposition {
            PendingFixedBindHandoffAttemptDisposition::NoAttempt => None,
            PendingFixedBindHandoffAttemptDisposition::ReleaseActiveOwner => {
                let owner = active_fixed_bind_facade_owner_for(bind_addr, self.instance_id)?;
                let registrant =
                    pending_fixed_bind_handoff_registrant_for(bind_addr, owner.instance_id)?;
                Self::release_active_fixed_bind_facade_owner_for_handoff(bind_addr, owner).await;
                Some(registrant)
            }
            PendingFixedBindHandoffAttemptDisposition::ReleaseConflictingProcessClaim {
                owner_instance_id,
            } => {
                let registrant =
                    pending_fixed_bind_handoff_registrant_for(bind_addr, owner_instance_id)?;
                clear_process_facade_claim_for_bind_addr(bind_addr, owner_instance_id);
                Some(registrant)
            }
        }
    }

    async fn apply_pending_fixed_bind_handoff_attempt_disposition(
        &self,
        bind_addr: &str,
        disposition: PendingFixedBindHandoffAttemptDisposition,
    ) -> bool {
        let Some(registrant) = self
            .release_pending_fixed_bind_handoff_blocker(bind_addr, disposition)
            .await
        else {
            return false;
        };
        self.complete_pending_fixed_bind_handoff_after_release_with_registrant(
            bind_addr, registrant,
        )
        .await;
        true
    }

    async fn complete_pending_fixed_bind_handoff_for_publication(
        &self,
        pending: &PendingFacadeActivation,
    ) -> bool {
        let bind_addr = pending.resolved.bind_addr.clone();
        let handoff = self
            .observe_pending_fixed_bind_handoff_for_publication(pending)
            .await;
        if !self
            .apply_pending_fixed_bind_handoff_attempt_disposition(
                &bind_addr,
                handoff.pending_publication_attempt_disposition(),
            )
            .await
        {
            return false;
        }
        if self
            .api_task
            .lock()
            .await
            .as_ref()
            .is_some_and(|active| pending.matches_active(active))
        {
            clear_pending_fixed_bind_handoff_ready(&bind_addr);
            return true;
        }
        false
    }

    async fn facade_publication_ready(&self) -> bool {
        let observation = self
            .observe_facade_gate(FacadeOnlyHandoffObservationPolicy::DeriveFromPendingBind)
            .await;
        match observation.publication_readiness_decision() {
            FacadePublicationReadinessDecision::BlockedControlGate
            | FacadePublicationReadinessDecision::BlockedWithoutActiveControlStream => {
                return false;
            }
            FacadePublicationReadinessDecision::Ready => {
                if let Some(pending) = observation.current_pending.as_ref()
                    && observation.pending_facade_is_control_route
                    && observation.active_pending_control_stream_present
                {
                    clear_pending_fixed_bind_handoff_ready(&pending.resolved.bind_addr);
                }
                return true;
            }
            FacadePublicationReadinessDecision::FixedBindHandoffPending => {}
        }
        let Some(pending) = observation.current_pending else {
            return false;
        };
        if !facade_bind_addr_is_ephemeral(&pending.resolved.bind_addr) {
            if self
                .complete_pending_fixed_bind_handoff_for_publication(&pending)
                .await
            {
                return true;
            }
        }
        false
    }

    async fn service_on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        let (source_signals, sink_signals, mut facade_signals) =
            split_app_control_signals(envelopes)?;
        facade_signals.sort_by_key(Self::facade_signal_apply_priority);
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
            !source_signals.is_empty()
                || !sink_signals.is_empty()
                || self.control_initialized()
                || facade_signals
                    .iter()
                    .any(Self::facade_signal_requires_shared_serial_while_uninitialized)
                || (!self.source_state_replay_required.load(Ordering::Acquire)
                    && !self.sink_state_replay_required.load(Ordering::Acquire))
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
        eprintln!(
            "fs_meta_runtime_app: on_control_frame begin source_signals={} sink_signals={} facade_signals={} initialized={}",
            source_signals.len(),
            sink_signals.len(),
            facade_signals.len(),
            self.control_initialized()
        );
        if source_signals.is_empty() && facade_signals.is_empty() && sink_signals.len() == 1 {
            eprintln!(
                "fs_meta_runtime_app: on_control_frame sink_only_lane signals={:?}",
                sink_signals
            );
        }
        if std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
            && source_signals.len() <= 2
            && !source_signals.is_empty()
        {
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
        if should_initialize_from_control {
            if !source_cleanup_only_while_uninitialized && !sink_cleanup_only_while_uninitialized {
                self.initialize_from_control(
                    initialize_wait_for_source_worker_handoff,
                    initialize_wait_for_sink_worker_handoff,
                )
                .await?;
            }
        } else if !self.control_initialized()
            && !facade_cleanup_only_while_uninitialized
            && !source_cleanup_only_while_uninitialized
            && !sink_cleanup_only_while_uninitialized
        {
            return Err(Self::not_ready_error());
        }
        if !facade_cleanup_only_while_uninitialized
            && !source_cleanup_only_while_uninitialized
            && !sink_cleanup_only_while_uninitialized
        {
            self.ensure_runtime_proxy_endpoints_started().await?;
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
        let mut pretriggered_source_to_sink_convergence = false;
        let mut sink_recovery_tail_plan = SinkRecoveryTailPlan::new();
        for signal in facade_claim_signals {
            self.apply_facade_signal(signal).await?;
        }
        let sink_tick_fast_path_eligible =
            self.sink.current_generation_tick_fast_path_eligible().await;
        let control_wave_observation = ControlFrameWaveObservation {
            control_initialized_at_entry,
            control_initialized_now: self.control_initialized(),
            retained_sink_state_present_at_entry,
            source_state_replay_required: self.source_state_replay_required.load(Ordering::Acquire),
            sink_state_replay_required: self.sink_state_replay_required.load(Ordering::Acquire),
            sink_tick_fast_path_eligible,
            cleanup_disposition: uninitialized_cleanup_disposition,
            facade_claim_signals_present,
            facade_publication_signals_present: !facade_publication_signals.is_empty(),
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
                self.apply_facade_signal(signal).await?;
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
            #[cfg(test)]
            note_sink_apply_entry_for_tests();
            #[cfg(test)]
            maybe_pause_before_sink_apply().await;
            eprintln!(
                "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals begin"
            );
            if let Err(err) = self
                .apply_sink_signals_with_recovery(
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
                    err
                );
                return Err(err);
            }
            eprintln!("fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals ok");
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
                    self.sink_state_replay_required
                        .store(true, Ordering::Release);
                }
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame source cleanup-only followup left runtime uninitialized"
                );
            }
            SourceControlWaveDisposition::SteadyTickNoop => {}
            SourceControlWaveDisposition::ApplySignals => {
                #[cfg(test)]
                note_source_apply_entry_for_tests();
                #[cfg(test)]
                maybe_pause_before_source_apply().await;
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame source.apply_orchestration_signals begin"
                );
                if let Err(err) = self
                    .apply_source_signals_with_recovery(
                        &source_signals,
                        control_initialized_at_entry,
                        matches!(
                            source_generation_cutover_disposition,
                            SourceGenerationCutoverDisposition::FailClosedRestartDeferredRetirePending
                        ),
                    )
                    .await
                {
                    eprintln!(
                        "fs_meta_runtime_app: on_control_frame source.apply_orchestration_signals err={}",
                        err
                    );
                    return Err(err);
                }
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame source.apply_orchestration_signals ok"
                );
                if !control_initialized_at_entry && sink_signals.is_empty() {
                    let retained_sink_routes_present = !self
                        .retained_sink_control_state
                        .lock()
                        .await
                        .active_by_route
                        .is_empty();
                    if retained_sink_routes_present {
                        if sink_status_publication_present {
                            self.source.trigger_rescan_when_ready().await?;
                            pretriggered_source_to_sink_convergence = true;
                        }
                        // A later source-only recovery can reopen the runtime before the sink
                        // worker has replayed its retained control state into the current
                        // generation. Keep the sink replay armed so peer status/query routes do
                        // not resume against a zero-state sink snapshot.
                        self.sink_state_replay_required
                            .store(true, Ordering::Release);
                    }
                }
            }
            SourceControlWaveDisposition::ReplayRetained => {
                #[cfg(test)]
                note_source_apply_entry_for_tests();
                #[cfg(test)]
                maybe_pause_before_source_apply().await;
                eprintln!("fs_meta_runtime_app: on_control_frame source.replay_retained begin");
                self.apply_source_signals_with_recovery(&[], control_initialized_at_entry, false)
                    .await?;
                eprintln!("fs_meta_runtime_app: on_control_frame source.replay_retained ok");
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
                #[cfg(test)]
                note_sink_apply_entry_for_tests();
                #[cfg(test)]
                maybe_pause_before_sink_apply().await;
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals begin"
                );
                if let Err(err) = self
                    .apply_sink_signals_with_recovery(
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
                        err
                    );
                    return Err(err);
                }
                eprintln!(
                    "fs_meta_runtime_app: on_control_frame sink.apply_orchestration_signals ok"
                );
                if wait_for_status_republish_after_apply {
                    if let Some(expected_groups) = self
                        .wait_for_sink_status_republish_readiness_after_recovery(false)
                        .await?
                    {
                        self.wait_for_local_sink_status_republish_after_recovery(&expected_groups)
                            .await?;
                    }
                }
                if !control_initialized_at_entry
                    && retained_sink_state_present_at_entry
                    && !source_signals.is_empty()
                {
                    let expected_groups =
                        Self::runtime_scoped_facade_group_ids(&self.source, &self.sink)
                            .await?
                            .into_iter()
                            .collect::<std::collections::BTreeSet<_>>();
                    sink_recovery_tail_plan
                        .push_immediate_local_sink_status_republish_wait(expected_groups);
                }
            }
            SinkControlWaveDisposition::ReplayRetained => {
                #[cfg(test)]
                note_sink_apply_entry_for_tests();
                #[cfg(test)]
                maybe_pause_before_sink_apply().await;
                eprintln!("fs_meta_runtime_app: on_control_frame sink.replay_retained begin");
                let replay_signals = current_generation_sink_replay_tick(
                    &source_signals,
                    &facade_publication_signals,
                )
                .into_iter()
                .collect::<Vec<_>>();
                self.apply_sink_signals_with_recovery(&replay_signals, true, false)
                    .await?;
                eprintln!("fs_meta_runtime_app: on_control_frame sink.replay_retained ok");
                replayed_sink_state_after_uninitialized_source_recovery =
                    !control_initialized_at_entry;
                if control_initialized_at_entry && sink_status_publication_present {
                    let expected_groups =
                        Self::runtime_scoped_facade_group_ids(&self.source, &self.sink)
                            .await?
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
                sink_status_publication_present,
                &source_signals,
                &sink_signals,
                pretriggered_source_to_sink_convergence,
                &deferred_local_sink_replay_signals,
            )
            .await?;
        if !source_cleanup_only_while_uninitialized {
            self.apply_sink_recovery_tail_plan(
                request_sensitive,
                sink_cleanup_only_while_uninitialized,
                sink_recovery_tail_plan,
                deferred_local_sink_replay_signals,
                facade_publication_signals,
            )
            .await?;
        }
        eprintln!("fs_meta_runtime_app: on_control_frame done");
        Ok(())
    }

    pub async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        self.service_on_control_frame(envelopes).await
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

    async fn service_close(&self) -> Result<()> {
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
        self.shutdown_active_facade().await;
        self.closing.store(true, Ordering::Release);
        self.control_initialized.store(false, Ordering::Release);
        self.api_control_gate.set_ready(false);
        let _ = self.current_facade_service_state().await;
        clear_owned_process_facade_claim(self.instance_id);
        for bind_addr in fixed_bind_addrs {
            clear_active_fixed_bind_facade_owner(&bind_addr, self.instance_id);
            clear_pending_fixed_bind_handoff_ready(&bind_addr);
        }
        self.wait_for_shared_worker_control_handoff().await;
        self.source.close().await?;
        self.sink.close().await?;
        let mut endpoint_tasks = std::mem::take(&mut *self.runtime_endpoint_tasks.lock().await);
        for task in &mut endpoint_tasks {
            task.shutdown(Duration::from_secs(2)).await;
        }
        self.runtime_endpoint_routes.lock().await.clear();
        if let Some(handle) = self.pump_task.lock().await.take() {
            handle.abort();
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.service_close().await
    }

    pub async fn query_tree(
        &self,
        params: &InternalQueryRequest,
    ) -> Result<std::collections::BTreeMap<String, TreeGroupPayload>> {
        let events = self.sink.materialized_query(params).await?;
        let mut grouped = std::collections::BTreeMap::<String, TreeGroupPayload>::new();
        for event in &events {
            let payload = rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
                .map_err(|e| CnxError::Internal(format!("decode tree response failed: {e}")))?;
            let MaterializedQueryPayload::Tree(response) = payload else {
                return Err(CnxError::Internal(
                    "unexpected stats payload for query_tree".into(),
                ));
            };
            grouped.insert(event.metadata().origin_id.0.clone(), response);
        }
        Ok(grouped)
    }

    pub async fn query_stats(&self, path: &[u8]) -> Result<SubtreeStats> {
        let events = self.sink.subtree_stats(path).await?;
        let mut agg = SubtreeStats::default();
        for event in &events {
            let stats = rmp_serde::from_slice::<SubtreeStats>(event.payload_bytes())
                .map_err(|e| CnxError::Internal(format!("decode stats response failed: {e}")))?;
            agg.total_nodes += stats.total_nodes;
            agg.total_files += stats.total_files;
            agg.total_dirs += stats.total_dirs;
            agg.total_size += stats.total_size;
            agg.attested_count += stats.attested_count;
            agg.blind_spot_count += stats.blind_spot_count;
        }
        Ok(agg)
    }

    pub async fn source_status_snapshot(&self) -> Result<crate::source::SourceStatusSnapshot> {
        self.source.status_snapshot().await
    }

    pub async fn sink_status_snapshot(&self) -> Result<crate::sink::SinkStatusSnapshot> {
        self.sink.status_snapshot().await
    }

    pub async fn trigger_rescan_when_ready(&self) -> Result<()> {
        self.source.trigger_rescan_when_ready().await
    }

    pub async fn query_node(&self, path: &[u8]) -> Result<Option<QueryNode>> {
        self.sink.query_node(path).await
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
                            async move { app.service_send(&events).await }
                        }
                    })
                    .stream({
                        let app = app.clone();
                        move |_context, opts| {
                            let app = app.clone();
                            async move { app.service_recv(opts).await }
                        }
                    })
                    .control({
                        let app = app.clone();
                        move |_context, envelopes| {
                            let app = app.clone();
                            async move { app.service_on_control_frame(&envelopes).await }
                        }
                    })
                    .close(move |_context| {
                        let app = close_app.clone();
                        async move { app.service_close().await }
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
