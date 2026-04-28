use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use capanix_app_sdk::runtime::{ControlEnvelope, NodeId, RuntimeWorkerBinding};
use capanix_app_sdk::{CnxError, Event, Result};
use capanix_runtime_entry_sdk::advanced::boundary::{
    BoundaryContext, ChannelIoSubset, ChannelKey, ChannelSendRequest, StateBoundary,
};
use capanix_runtime_entry_sdk::control::{RuntimeBoundScope, RuntimeHostGrantState};
use capanix_runtime_entry_sdk::worker_runtime::{
    RuntimeWorkerClientFactory, TypedRuntimeWorkerClient, TypedWorkerClient, TypedWorkerInit,
};
use futures_util::StreamExt;
use tokio::task::JoinHandle;

use crate::query::request::InternalQueryRequest;
use crate::runtime::orchestration::{
    SourceControlSignal, SourceRuntimeUnit, source_control_signals_from_envelopes,
};
use crate::runtime::routes::ROUTE_KEY_EVENTS;
use crate::source::config::{GrantedMountRoot, RootSpec, SourceConfig};
use crate::source::{FSMetaSource, SourceStatusSnapshot};
use crate::state::cell::SignalCell;
use crate::workers::sink::SinkFacade;
use crate::workers::source_ipc::{
    SourceWorkerRequest, SourceWorkerResponse, decode_request, decode_response, encode_request,
    encode_response,
};

const SOURCE_WORKER_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(15);
const SOURCE_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_WORKER_START_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const SOURCE_WORKER_FORCE_FIND_TIMEOUT: Duration = Duration::from_secs(60);
const SOURCE_WORKER_FORCE_FIND_RETRY_BACKOFF: Duration = Duration::from_millis(25);
const SOURCE_WORKER_UPDATE_ROOTS_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT: Duration = Duration::from_secs(90);
const SOURCE_WORKER_CLOSE_DRAIN_TIMEOUT: Duration = SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT;
const SOURCE_WORKER_SCHEDULE_REFRESH_TOTAL_TIMEOUT: Duration = Duration::from_secs(4);
const SOURCE_WORKER_SCHEDULE_REFRESH_RPC_TIMEOUT: Duration = Duration::from_secs(1);
const SOURCE_WORKER_OBSERVABILITY_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const SOURCE_WORKER_NONBLOCKING_OBSERVABILITY_CACHE_TTL: Duration = Duration::from_secs(1);
const SOURCE_WORKER_DEGRADED_STATE: &str = "degraded_worker_unreachable";
const SOURCE_WORKER_DEGRADED_ROOT_KEY: &str = "source-worker";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceProgressReason {
    WorkerClientReplaced,
    ControlSignalsRetained,
    HostObjectGrantsUpdated,
    ScheduledGroupsUpdated,
    ObservabilityUpdated,
    Started,
    LogicalRootsUpdated,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SourceProgressState {
    worker_client_replaced_epoch: u64,
    control_signals_retained_epoch: u64,
    host_object_grants_updated_epoch: u64,
    scheduled_groups_updated_epoch: u64,
    observability_updated_epoch: u64,
    started_epoch: u64,
    logical_roots_updated_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceWaitFor {
    ControlFlowRetry,
    ScheduledGroupsRefresh,
    Observability,
    Started,
    LogicalRoots,
}

impl SourceWaitFor {
    fn satisfied_by(self, current: SourceProgressState, after: SourceProgressState) -> bool {
        match self {
            Self::ControlFlowRetry => {
                current.worker_client_replaced_epoch > after.worker_client_replaced_epoch
                    || current.control_signals_retained_epoch > after.control_signals_retained_epoch
            }
            Self::ScheduledGroupsRefresh => {
                current.worker_client_replaced_epoch > after.worker_client_replaced_epoch
                    || current.host_object_grants_updated_epoch
                        > after.host_object_grants_updated_epoch
                    || current.scheduled_groups_updated_epoch > after.scheduled_groups_updated_epoch
            }
            Self::Observability => {
                current.worker_client_replaced_epoch > after.worker_client_replaced_epoch
                    || current.observability_updated_epoch > after.observability_updated_epoch
            }
            Self::Started => {
                current.worker_client_replaced_epoch > after.worker_client_replaced_epoch
                    || current.started_epoch > after.started_epoch
            }
            Self::LogicalRoots => {
                current.worker_client_replaced_epoch > after.worker_client_replaced_epoch
                    || current.logical_roots_updated_epoch > after.logical_roots_updated_epoch
            }
        }
    }
}

impl SourceProgressState {
    fn bump(&mut self, reason: SourceProgressReason) {
        match reason {
            SourceProgressReason::WorkerClientReplaced => {
                self.worker_client_replaced_epoch += 1;
            }
            SourceProgressReason::ControlSignalsRetained => {
                self.control_signals_retained_epoch += 1;
            }
            SourceProgressReason::HostObjectGrantsUpdated => {
                self.host_object_grants_updated_epoch += 1;
            }
            SourceProgressReason::ScheduledGroupsUpdated => {
                self.scheduled_groups_updated_epoch += 1;
            }
            SourceProgressReason::ObservabilityUpdated => {
                self.observability_updated_epoch += 1;
            }
            SourceProgressReason::Started => {
                self.started_epoch += 1;
            }
            SourceProgressReason::LogicalRootsUpdated => {
                self.logical_roots_updated_epoch += 1;
            }
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug, PartialEq, Eq)]
enum SourceControlFrameLaneInspect {
    RetainedTickFastPath,
    RetainedTickReplay,
    Attempt,
    Reconnect,
    FailClosedReconnect,
    Wait {
        after: SourceProgressState,
        deadline: std::time::Instant,
    },
    ApplyPostAckCacheAndRetainSignals {
        cache_priming: SourceControlFramePostAckCachePriming,
    },
    ArmReplay,
    RefreshScheduledGroups {
        scheduled_groups_expectation: SourceScheduledGroupsExpectation,
        deadline: std::time::Instant,
    },
}

#[async_trait]
trait SourceControlFrameLane: Send {
    #[cfg_attr(not(test), allow(dead_code))]
    fn inspect(&self) -> SourceControlFrameLaneInspect;

    async fn execute(
        self: Box<Self>,
        handle: &SourceWorkerClientHandle,
    ) -> std::result::Result<SourceControlFrameEvent, SourceFailure>;
}

type SourceControlFrameRun = Box<dyn SourceControlFrameLane>;

struct RetainedTickFastPathLane {
    signals: Vec<SourceControlSignal>,
}

#[async_trait]
impl SourceControlFrameLane for RetainedTickFastPathLane {
    fn inspect(&self) -> SourceControlFrameLaneInspect {
        SourceControlFrameLaneInspect::RetainedTickFastPath
    }

    async fn execute(
        self: Box<Self>,
        handle: &SourceWorkerClientHandle,
    ) -> std::result::Result<SourceControlFrameEvent, SourceFailure> {
        handle.apply_control_frame_retained_tick_fast_path(&self.signals);
        Ok(SourceControlFrameEvent::RetainedTickFastPathApplied)
    }
}

struct RetainedTickReplayLane {
    deadline: std::time::Instant,
}

#[async_trait]
impl SourceControlFrameLane for RetainedTickReplayLane {
    fn inspect(&self) -> SourceControlFrameLaneInspect {
        SourceControlFrameLaneInspect::RetainedTickReplay
    }

    async fn execute(
        self: Box<Self>,
        handle: &SourceWorkerClientHandle,
    ) -> std::result::Result<SourceControlFrameEvent, SourceFailure> {
        handle
            .replay_control_frame_retained_tick(self.deadline)
            .await?;
        Ok(SourceControlFrameEvent::RetainedTickReplayCompleted)
    }
}

struct AttemptLane {
    envelopes: Arc<[ControlEnvelope]>,
    timeout: Duration,
}

#[async_trait]
impl SourceControlFrameLane for AttemptLane {
    fn inspect(&self) -> SourceControlFrameLaneInspect {
        SourceControlFrameLaneInspect::Attempt
    }

    async fn execute(
        self: Box<Self>,
        handle: &SourceWorkerClientHandle,
    ) -> std::result::Result<SourceControlFrameEvent, SourceFailure> {
        let (rpc_result, after) = handle
            .perform_control_frame_attempt(&self.envelopes, self.timeout)
            .await;
        Ok(SourceControlFrameEvent::RpcCompleted { rpc_result, after })
    }
}

struct ReconnectLane;

#[async_trait]
impl SourceControlFrameLane for ReconnectLane {
    fn inspect(&self) -> SourceControlFrameLaneInspect {
        SourceControlFrameLaneInspect::Reconnect
    }

    async fn execute(
        self: Box<Self>,
        handle: &SourceWorkerClientHandle,
    ) -> std::result::Result<SourceControlFrameEvent, SourceFailure> {
        handle.reconnect_for_control_frame().await?;
        Ok(SourceControlFrameEvent::ReconnectCompleted)
    }
}

struct FailClosedReconnectLane;

#[async_trait]
impl SourceControlFrameLane for FailClosedReconnectLane {
    fn inspect(&self) -> SourceControlFrameLaneInspect {
        SourceControlFrameLaneInspect::FailClosedReconnect
    }

    async fn execute(
        self: Box<Self>,
        handle: &SourceWorkerClientHandle,
    ) -> std::result::Result<SourceControlFrameEvent, SourceFailure> {
        handle
            .replace_worker_client_for_fail_closed_control_frame()
            .await?;
        Ok(SourceControlFrameEvent::ReconnectCompleted)
    }
}

struct WaitLane {
    after: SourceProgressState,
    deadline: std::time::Instant,
}

#[async_trait]
impl SourceControlFrameLane for WaitLane {
    fn inspect(&self) -> SourceControlFrameLaneInspect {
        SourceControlFrameLaneInspect::Wait {
            after: self.after,
            deadline: self.deadline,
        }
    }

    async fn execute(
        self: Box<Self>,
        handle: &SourceWorkerClientHandle,
    ) -> std::result::Result<SourceControlFrameEvent, SourceFailure> {
        handle
            .wait_control_frame_retry_after(self.after, self.deadline)
            .await;
        Ok(SourceControlFrameEvent::WaitCompleted)
    }
}

struct ApplyPostAckCacheAndRetainSignalsLane {
    signals: Vec<SourceControlSignal>,
    cache_priming: SourceControlFramePostAckCachePriming,
}

#[async_trait]
impl SourceControlFrameLane for ApplyPostAckCacheAndRetainSignalsLane {
    fn inspect(&self) -> SourceControlFrameLaneInspect {
        SourceControlFrameLaneInspect::ApplyPostAckCacheAndRetainSignals {
            cache_priming: self.cache_priming,
        }
    }

    async fn execute(
        self: Box<Self>,
        handle: &SourceWorkerClientHandle,
    ) -> std::result::Result<SourceControlFrameEvent, SourceFailure> {
        handle
            .apply_post_ack_cache_and_retain_signals(&self.signals, self.cache_priming)
            .await;
        Ok(SourceControlFrameEvent::PostAckCacheAndSignalsRetained)
    }
}

struct ArmReplayLane;

#[async_trait]
impl SourceControlFrameLane for ArmReplayLane {
    fn inspect(&self) -> SourceControlFrameLaneInspect {
        SourceControlFrameLaneInspect::ArmReplay
    }

    async fn execute(
        self: Box<Self>,
        handle: &SourceWorkerClientHandle,
    ) -> std::result::Result<SourceControlFrameEvent, SourceFailure> {
        handle.arm_control_frame_replay().await;
        Ok(SourceControlFrameEvent::ArmReplayCompleted)
    }
}

struct RefreshScheduledGroupsLane {
    scheduled_groups_expectation: SourceScheduledGroupsExpectation,
    deadline: std::time::Instant,
}

#[async_trait]
impl SourceControlFrameLane for RefreshScheduledGroupsLane {
    fn inspect(&self) -> SourceControlFrameLaneInspect {
        SourceControlFrameLaneInspect::RefreshScheduledGroups {
            scheduled_groups_expectation: self.scheduled_groups_expectation,
            deadline: self.deadline,
        }
    }

    async fn execute(
        self: Box<Self>,
        handle: &SourceWorkerClientHandle,
    ) -> std::result::Result<SourceControlFrameEvent, SourceFailure> {
        Ok(SourceControlFrameEvent::RefreshScheduledGroupsCompleted {
            refresh_result: handle
                .refresh_scheduled_groups_for_control_frame(
                    self.deadline,
                    self.scheduled_groups_expectation,
                )
                .await,
        })
    }
}

enum SourceControlFrameTerminal {
    Complete,
    Fail(SourceFailure),
}

enum SourceControlFrameStep {
    Run(SourceControlFrameRun),
    Terminal(SourceControlFrameTerminal),
}

impl SourceControlFrameStep {
    fn run<L>(lane: L) -> Self
    where
        L: SourceControlFrameLane + 'static,
    {
        Self::Run(Box::new(lane))
    }

    const fn complete() -> Self {
        Self::Terminal(SourceControlFrameTerminal::Complete)
    }

    fn fail(failure: SourceFailure) -> Self {
        Self::Terminal(SourceControlFrameTerminal::Fail(failure))
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn inspect(&self) -> Option<SourceControlFrameLaneInspect> {
        match self {
            Self::Run(lane) => Some(lane.inspect()),
            Self::Terminal(_) => None,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn terminal(&self) -> Option<&SourceControlFrameTerminal> {
        match self {
            Self::Run(_) => None,
            Self::Terminal(terminal) => Some(terminal),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn is_complete(&self) -> bool {
        matches!(self.terminal(), Some(SourceControlFrameTerminal::Complete))
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn terminal_error(&self) -> Option<&CnxError> {
        match self.terminal() {
            Some(SourceControlFrameTerminal::Fail(failure)) => Some(failure.as_error()),
            _ => None,
        }
    }
}

enum SourceControlFrameInitialStep {
    Run(SourceControlFrameRun),
}

impl SourceControlFrameInitialStep {
    fn run<L>(lane: L) -> Self
    where
        L: SourceControlFrameLane + 'static,
    {
        Self::Run(Box::new(lane))
    }
}

#[derive(Clone, Debug)]
enum SourceControlFrameTickOnlyDisposition {
    RetainedTickFastPath { signals: Vec<SourceControlSignal> },
    RetainedTickReplay,
}

#[derive(Clone, Debug)]
enum SourceControlFrameDecodedSignals {
    Decoded(Vec<SourceControlSignal>),
    DecodeFailed,
}

struct SourceControlFrameFacts {
    envelopes: Arc<[ControlEnvelope]>,
    decoded_signals: SourceControlFrameDecodedSignals,
    primed_local_schedule: Option<PrimedLocalScheduleSummary>,
    post_ack_refresh_requirement: SourceControlFramePostAckRefreshRequirement,
    initial_step: Option<SourceControlFrameInitialStep>,
    operation_deadline: std::time::Instant,
    rpc_timeout: Duration,
    existing_client_present: bool,
    bridge_reset_policy: SourceControlFrameBridgeResetPolicy,
    timeout_reset_policy: SourceControlFrameTimeoutResetPolicy,
    attempt_timeout_policy: SourceControlFrameAttemptTimeoutPolicy,
}

enum SourceControlFrameEvent {
    RetainedTickFastPathApplied,
    RetainedTickReplayCompleted,
    RpcCompleted {
        rpc_result: std::result::Result<SourceWorkerResponse, CnxError>,
        after: SourceProgressState,
    },
    ReconnectCompleted,
    WaitCompleted,
    PostAckCacheAndSignalsRetained,
    ArmReplayCompleted,
    RefreshScheduledGroupsCompleted {
        refresh_result: std::result::Result<(), SourceFailure>,
    },
}

struct SourceControlFrameExecutor<'a> {
    handle: &'a SourceWorkerClientHandle,
}

#[derive(Clone, Copy, Debug)]
enum SourceControlFrameAttemptTimeoutPolicy {
    Standard,
    PreferShortIfExistingClient,
}

impl SourceControlFrameAttemptTimeoutPolicy {
    const fn timeout_cap(self, existing_client_present: bool, rpc_timeout: Duration) -> Duration {
        match self {
            Self::Standard => rpc_timeout,
            Self::PreferShortIfExistingClient if existing_client_present => {
                SOURCE_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT
            }
            Self::PreferShortIfExistingClient => rpc_timeout,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SourceControlFrameBridgeResetPolicy {
    RetryAfterReconnect,
    ReconnectThenFail,
    FailImmediately,
}

#[derive(Clone, Copy, Debug)]
enum SourceControlFrameTimeoutResetPolicy {
    Standard,
    GenerationOneActivateWave,
}

#[derive(Clone, Copy, Debug)]
enum SourceControlFrameTimeoutResetObservation {
    Clear,
    Seen,
}

#[derive(Clone, Copy, Debug)]
enum SourceControlFrameRequestAttemptKind {
    ControlFrame,
    RetainedReplay,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceControlFramePostAckCachePriming {
    Standard,
    ReplayRecovery,
}

#[derive(Clone, Copy, Debug)]
enum SourceControlFramePostAckRefreshRequirement {
    NotRequired,
    Required,
}

impl SourceControlFramePostAckRefreshRequirement {
    fn replay_required_refresh_expectation(
        self,
        primed_local_schedule: Option<PrimedLocalScheduleSummary>,
    ) -> Option<SourceScheduledGroupsExpectation> {
        match primed_local_schedule {
            None => matches!(self, Self::Required)
                .then_some(SourceScheduledGroupsExpectation::DoNotExpectLocalRunnableGroups),
            Some(primed_local_schedule) => {
                let scheduled_groups_expectation = if primed_local_schedule
                    .saw_activate_with_bound_scopes
                    && primed_local_schedule.has_local_runnable_groups
                {
                    SourceScheduledGroupsExpectation::ExpectLocalRunnableGroups
                } else {
                    SourceScheduledGroupsExpectation::DoNotExpectLocalRunnableGroups
                };
                let should_refresh_cached_schedule = matches!(self, Self::Required)
                    && !(primed_local_schedule.saw_activate_with_bound_scopes
                        && !primed_local_schedule.has_local_runnable_groups);
                should_refresh_cached_schedule.then_some(scheduled_groups_expectation)
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct SourceControlFrameSignalPolicies {
    timeout_reset_policy: SourceControlFrameTimeoutResetPolicy,
    attempt_timeout_policy: SourceControlFrameAttemptTimeoutPolicy,
    post_ack_refresh_requirement: SourceControlFramePostAckRefreshRequirement,
}

impl SourceControlFrameSignalPolicies {
    const fn standard() -> Self {
        Self {
            timeout_reset_policy: SourceControlFrameTimeoutResetPolicy::Standard,
            attempt_timeout_policy: SourceControlFrameAttemptTimeoutPolicy::Standard,
            post_ack_refresh_requirement: SourceControlFramePostAckRefreshRequirement::NotRequired,
        }
    }

    fn from_signals(signals: &[SourceControlSignal]) -> Self {
        let mut saw_generation_one_activate = false;
        let mut generation_one_activate_only = true;
        let mut prefer_short_existing_client_attempt = false;
        let mut post_ack_refresh_requirement =
            SourceControlFramePostAckRefreshRequirement::NotRequired;

        for signal in signals {
            match signal {
                SourceControlSignal::Activate {
                    generation,
                    bound_scopes,
                    ..
                } => {
                    prefer_short_existing_client_attempt = true;
                    if !bound_scopes.is_empty() {
                        post_ack_refresh_requirement =
                            SourceControlFramePostAckRefreshRequirement::Required;
                    }
                    if *generation == 1 {
                        saw_generation_one_activate = true;
                    } else {
                        generation_one_activate_only = false;
                    }
                }
                SourceControlSignal::Deactivate { .. }
                | SourceControlSignal::Tick { .. }
                | SourceControlSignal::ManualRescan { .. }
                | SourceControlSignal::Passthrough(_) => {
                    prefer_short_existing_client_attempt = true;
                    generation_one_activate_only = false;
                }
                _ => {
                    generation_one_activate_only = false;
                }
            }
        }

        if !saw_generation_one_activate {
            generation_one_activate_only = false;
        }

        Self {
            timeout_reset_policy: if generation_one_activate_only {
                SourceControlFrameTimeoutResetPolicy::GenerationOneActivateWave
            } else {
                SourceControlFrameTimeoutResetPolicy::Standard
            },
            attempt_timeout_policy: if prefer_short_existing_client_attempt {
                SourceControlFrameAttemptTimeoutPolicy::PreferShortIfExistingClient
            } else {
                SourceControlFrameAttemptTimeoutPolicy::Standard
            },
            post_ack_refresh_requirement,
        }
    }
}

impl SourceControlFramePostAckCachePriming {
    fn apply_to_cache(
        self,
        cache: &mut SourceWorkerSnapshotCache,
        node_id: &NodeId,
        signals: &[SourceControlSignal],
        fallback_roots: &[RootSpec],
        fallback_grants: &[GrantedMountRoot],
    ) {
        let _ = prime_cached_schedule_from_control_signals(
            cache,
            node_id,
            signals,
            fallback_roots,
            fallback_grants,
        );
        prime_cached_control_summary_from_control_signals(cache, node_id, signals, fallback_grants);
        cache.observability_control_summary_override_by_node = None;
        if matches!(self, Self::ReplayRecovery) {
            prime_cached_schedule_from_control_signals_for_replay_recovery(
                cache,
                node_id,
                signals,
                fallback_roots,
                fallback_grants,
            );
        }
    }
}

#[derive(Debug)]
enum SourceControlFrameReconnectResume {
    Attempt,
    Wait(SourceProgressState),
    RetainedTickReplay,
    Fail(SourceFailure),
}

#[derive(Debug)]
enum SourceControlFramePendingRefreshNextStep {
    ArmReplay,
    RefreshScheduledGroups,
}

#[derive(Clone, Copy, Debug)]
enum SourceScheduledGroupsRefreshDisposition {
    OrdinaryScheduleRefresh,
    ReplayRequiredRecovery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceScheduledGroupsExpectation {
    DoNotExpectLocalRunnableGroups,
    ExpectLocalRunnableGroups,
}

impl SourceScheduledGroupsExpectation {
    const fn expect_local_runnable_groups(self) -> bool {
        matches!(self, Self::ExpectLocalRunnableGroups)
    }
}

impl SourceScheduledGroupsRefreshDisposition {
    const fn requires_replay_recovery(self) -> bool {
        matches!(self, Self::ReplayRequiredRecovery)
    }
}

#[derive(Debug)]
struct SourceControlFramePendingRefresh {
    scheduled_groups_expectation: SourceScheduledGroupsExpectation,
    next_step: SourceControlFramePendingRefreshNextStep,
}

impl SourceControlFramePendingRefresh {
    fn step(&self, deadline: std::time::Instant) -> SourceControlFrameStep {
        match self.next_step {
            SourceControlFramePendingRefreshNextStep::ArmReplay => {
                SourceControlFrameStep::run(ArmReplayLane)
            }
            SourceControlFramePendingRefreshNextStep::RefreshScheduledGroups => {
                SourceControlFrameStep::run(RefreshScheduledGroupsLane {
                    scheduled_groups_expectation: self.scheduled_groups_expectation,
                    deadline,
                })
            }
        }
    }
}

#[derive(Debug)]
struct SourceControlFrameMachine {
    envelopes: Arc<[ControlEnvelope]>,
    operation_deadline: std::time::Instant,
    rpc_timeout: Duration,
    decoded_signals: SourceControlFrameDecodedSignals,
    primed_local_schedule: Option<PrimedLocalScheduleSummary>,
    post_ack_refresh_requirement: SourceControlFramePostAckRefreshRequirement,
    existing_client_present: bool,
    bridge_reset_policy: SourceControlFrameBridgeResetPolicy,
    timeout_reset_policy: SourceControlFrameTimeoutResetPolicy,
    attempt_timeout_policy: SourceControlFrameAttemptTimeoutPolicy,
    timeout_reset_observation: SourceControlFrameTimeoutResetObservation,
    pending_reconnect_resume: Option<SourceControlFrameReconnectResume>,
    pending_refresh: Option<SourceControlFramePendingRefresh>,
}

impl SourceControlFrameMachine {
    fn start(
        facts: SourceControlFrameFacts,
    ) -> std::result::Result<(Self, SourceControlFrameStep), SourceFailure> {
        let SourceControlFrameFacts {
            envelopes,
            decoded_signals,
            primed_local_schedule,
            post_ack_refresh_requirement,
            initial_step,
            operation_deadline,
            rpc_timeout,
            existing_client_present,
            bridge_reset_policy,
            timeout_reset_policy,
            attempt_timeout_policy,
        } = facts;
        let initial_step = match initial_step {
            Some(SourceControlFrameInitialStep::Run(lane)) => Some(lane),
            None => None,
        };
        let machine = Self {
            envelopes,
            operation_deadline,
            rpc_timeout,
            decoded_signals,
            primed_local_schedule,
            post_ack_refresh_requirement,
            existing_client_present,
            bridge_reset_policy,
            timeout_reset_policy,
            attempt_timeout_policy,
            timeout_reset_observation: SourceControlFrameTimeoutResetObservation::Clear,
            pending_reconnect_resume: None,
            pending_refresh: None,
        };
        let initial_effect = if let Some(initial_step) = initial_step {
            SourceControlFrameStep::Run(initial_step)
        } else if machine.pending_reconnect_resume.is_some() {
            SourceControlFrameStep::run(ReconnectLane)
        } else {
            machine.rpc_attempt_plan()?
        };
        Ok((machine, initial_effect))
    }

    fn rpc_attempt_plan(&self) -> std::result::Result<SourceControlFrameStep, SourceFailure> {
        let timeout_cap = self
            .attempt_timeout_policy
            .timeout_cap(self.existing_client_present, self.rpc_timeout);
        Ok(SourceControlFrameStep::run(AttemptLane {
            envelopes: self.envelopes.clone(),
            timeout: source_operation_attempt_timeout(self.operation_deadline, timeout_cap)?,
        }))
    }

    fn queue_pending_refresh(
        &mut self,
        scheduled_groups_expectation: SourceScheduledGroupsExpectation,
        next_step: SourceControlFramePendingRefreshNextStep,
    ) {
        self.pending_refresh = Some(SourceControlFramePendingRefresh {
            scheduled_groups_expectation,
            next_step,
        });
    }

    fn bridge_reset_policy_after_timeout_reset(&self) -> SourceControlFrameBridgeResetPolicy {
        match (self.timeout_reset_policy, self.timeout_reset_observation) {
            (
                SourceControlFrameTimeoutResetPolicy::GenerationOneActivateWave,
                SourceControlFrameTimeoutResetObservation::Seen,
            ) => SourceControlFrameBridgeResetPolicy::ReconnectThenFail,
            _ => self.bridge_reset_policy,
        }
    }

    fn pending_refresh_effect(&self) -> std::result::Result<SourceControlFrameStep, SourceFailure> {
        Ok(self
            .pending_refresh
            .as_ref()
            .ok_or_else(|| {
                missing_control_frame_pending_refresh_state("control-frame refresh followup")
            })?
            .step(self.operation_deadline))
    }

    fn schedule_post_ack_cache_and_signal_retention(
        &mut self,
        signals: Vec<SourceControlSignal>,
        replay_required_refresh_expectation: Option<SourceScheduledGroupsExpectation>,
    ) -> SourceControlFrameStep {
        let cache_priming = match replay_required_refresh_expectation {
            None => {
                self.pending_refresh = None;
                SourceControlFramePostAckCachePriming::Standard
            }
            Some(scheduled_groups_expectation) => {
                self.queue_pending_refresh(
                    scheduled_groups_expectation,
                    SourceControlFramePendingRefreshNextStep::ArmReplay,
                );
                SourceControlFramePostAckCachePriming::ReplayRecovery
            }
        };
        SourceControlFrameStep::run(ApplyPostAckCacheAndRetainSignalsLane {
            signals,
            cache_priming,
        })
    }

    fn replay_required_refresh_expectation_after_ack(
        &self,
    ) -> Option<SourceScheduledGroupsExpectation> {
        self.post_ack_refresh_requirement
            .replay_required_refresh_expectation(self.primed_local_schedule)
    }

    fn step_after_ack_success(
        &mut self,
    ) -> std::result::Result<SourceControlFrameStep, SourceFailure> {
        match &self.decoded_signals {
            SourceControlFrameDecodedSignals::DecodeFailed => {
                self.queue_pending_refresh(
                    SourceScheduledGroupsExpectation::DoNotExpectLocalRunnableGroups,
                    SourceControlFramePendingRefreshNextStep::RefreshScheduledGroups,
                );
                self.pending_refresh_effect()
            }
            SourceControlFrameDecodedSignals::Decoded(signals) => Ok(self
                .schedule_post_ack_cache_and_signal_retention(
                    signals.clone(),
                    self.replay_required_refresh_expectation_after_ack(),
                )),
        }
    }

    fn step_after_reconnect_completion(
        &mut self,
    ) -> std::result::Result<SourceControlFrameStep, SourceFailure> {
        self.existing_client_present = true;
        match self.pending_reconnect_resume.take() {
            Some(SourceControlFrameReconnectResume::Attempt) => self.rpc_attempt_plan(),
            Some(SourceControlFrameReconnectResume::Wait(after)) => {
                Ok(SourceControlFrameStep::run(WaitLane {
                    after,
                    deadline: self.operation_deadline,
                }))
            }
            Some(SourceControlFrameReconnectResume::RetainedTickReplay) => {
                Ok(SourceControlFrameStep::run(RetainedTickReplayLane {
                    deadline: self.operation_deadline,
                }))
            }
            Some(SourceControlFrameReconnectResume::Fail(failure)) => {
                Ok(SourceControlFrameStep::fail(failure))
            }
            None => Err(missing_control_frame_pending_reconnect_resume()),
        }
    }

    fn step_after_post_ack_completion(
        &self,
    ) -> std::result::Result<SourceControlFrameStep, SourceFailure> {
        if self.pending_refresh.is_some() {
            self.pending_refresh_effect()
        } else {
            Ok(SourceControlFrameStep::complete())
        }
    }

    fn step_after_arm_replay_completion(
        &mut self,
    ) -> std::result::Result<SourceControlFrameStep, SourceFailure> {
        let pending_refresh = self
            .pending_refresh
            .as_mut()
            .ok_or_else(|| missing_control_frame_pending_refresh_state("replay arm completion"))?;
        pending_refresh.next_step =
            SourceControlFramePendingRefreshNextStep::RefreshScheduledGroups;
        self.pending_refresh_effect()
    }

    fn step_after_refresh_completion(
        &mut self,
        refresh_result: std::result::Result<(), SourceFailure>,
    ) -> std::result::Result<SourceControlFrameStep, SourceFailure> {
        let _pending_refresh = self.pending_refresh.take().ok_or_else(|| {
            missing_control_frame_pending_refresh_state("scheduled-groups refresh completion")
        })?;
        Ok(match refresh_result {
            Ok(()) => SourceControlFrameStep::complete(),
            Err(refresh_failure) => SourceControlFrameStep::fail(refresh_failure),
        })
    }

    fn action_after_error(
        &mut self,
        err: CnxError,
        after: SourceProgressState,
    ) -> SourceControlFrameStep {
        let retryable_control_error_kind = classify_source_worker_control_reset(&err);
        let failure = match retryable_control_error_kind {
            Some(kind) => SourceFailure {
                cause: err,
                reason: SourceFailureReason::ControlReset(kind),
            },
            None => SourceFailure::non_retryable(err),
        };
        let bridge_reset_policy = self.bridge_reset_policy_after_timeout_reset();
        if retryable_control_error_kind
            .is_some_and(|kind| kind.should_follow_fail_closed_bridge_policy(bridge_reset_policy))
        {
            match bridge_reset_policy {
                SourceControlFrameBridgeResetPolicy::ReconnectThenFail => {
                    self.pending_reconnect_resume =
                        Some(SourceControlFrameReconnectResume::Fail(failure));
                    return SourceControlFrameStep::run(FailClosedReconnectLane);
                }
                SourceControlFrameBridgeResetPolicy::FailImmediately => {
                    self.pending_reconnect_resume = None;
                    return SourceControlFrameStep::fail(failure);
                }
                SourceControlFrameBridgeResetPolicy::RetryAfterReconnect => {}
            }
        }
        if retryable_control_error_kind.is_none() {
            self.pending_reconnect_resume = None;
            return SourceControlFrameStep::fail(failure);
        }
        if matches!(
            self.timeout_reset_policy,
            SourceControlFrameTimeoutResetPolicy::GenerationOneActivateWave
        ) && retryable_control_error_kind.is_some_and(|kind| kind.is_timeout_like_reset())
        {
            self.timeout_reset_observation = SourceControlFrameTimeoutResetObservation::Seen;
            self.pending_reconnect_resume = Some(SourceControlFrameReconnectResume::Attempt);
            return SourceControlFrameStep::run(ReconnectLane);
        }
        if matches!(
            classify_source_retry_budget(self.operation_deadline),
            SourceRetryBudgetDisposition::Exhausted
        ) {
            self.pending_reconnect_resume = None;
            return SourceControlFrameStep::fail(SourceFailure::retry_budget_exhausted(
                SourceRetryBudgetExhaustionKind::ControlFrameRetry,
            ));
        }
        self.pending_reconnect_resume = Some(SourceControlFrameReconnectResume::Wait(after));
        SourceControlFrameStep::run(ReconnectLane)
    }

    fn advance(
        &mut self,
        event: SourceControlFrameEvent,
    ) -> std::result::Result<SourceControlFrameStep, SourceFailure> {
        match event {
            SourceControlFrameEvent::RetainedTickFastPathApplied => {
                Ok(SourceControlFrameStep::complete())
            }
            SourceControlFrameEvent::RetainedTickReplayCompleted => {
                Ok(SourceControlFrameStep::complete())
            }
            SourceControlFrameEvent::RpcCompleted { rpc_result, after } => Ok(match rpc_result {
                Ok(response) => match expect_source_worker_ack("for on_control_frame", response) {
                    Ok(()) => self.step_after_ack_success()?,
                    Err(failure) => SourceControlFrameStep::fail(failure),
                },
                Err(err) => self.action_after_error(err, after),
            }),
            SourceControlFrameEvent::ReconnectCompleted => self.step_after_reconnect_completion(),
            SourceControlFrameEvent::WaitCompleted => self.rpc_attempt_plan(),
            SourceControlFrameEvent::PostAckCacheAndSignalsRetained => {
                self.step_after_post_ack_completion()
            }
            SourceControlFrameEvent::ArmReplayCompleted => self.step_after_arm_replay_completion(),
            SourceControlFrameEvent::RefreshScheduledGroupsCompleted { refresh_result } => {
                self.step_after_refresh_completion(refresh_result)
            }
        }
    }
}

fn clip_retry_deadline(deadline: std::time::Instant, budget: Duration) -> std::time::Instant {
    std::cmp::min(deadline, std::time::Instant::now() + budget)
}

fn map_source_timeout_result_with_failure<T>(
    result: std::result::Result<Result<T>, tokio::time::error::Elapsed>,
) -> std::result::Result<T, SourceFailure> {
    match result {
        Ok(result) => result.map_err(SourceFailure::from),
        Err(_) => Err(SourceFailure::timeout_reset()),
    }
}

fn map_source_failure_timeout_result<T>(
    result: std::result::Result<std::result::Result<T, SourceFailure>, tokio::time::error::Elapsed>,
) -> std::result::Result<T, SourceFailure> {
    match result {
        Ok(result) => result,
        Err(_) => Err(SourceFailure::timeout_reset()),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceRetryBudgetDisposition {
    Exhausted,
    Remaining(Duration),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceRetryBudgetExhaustionKind {
    ControlFrameRetry,
    OperationAttempt,
    OperationWait,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceScheduledGroupsRefreshExhaustionReason {
    Timeout,
    TransportClosed,
    MissingRouteState,
    RetryableReset,
    NoLiveWorkerRecovery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceProtocolViolationKind {
    UnexpectedWorkerResponse {
        context: &'static str,
        response: String,
    },
    MissingReconnectWait {
        reconnect_context: &'static str,
    },
    MissingControlFramePendingReconnectResume,
    MissingControlFramePendingRefreshState {
        context: &'static str,
    },
}

impl SourceProtocolViolationKind {
    fn into_error(self) -> CnxError {
        match self {
            Self::UnexpectedWorkerResponse { context, response } => CnxError::ProtocolViolation(
                format!("unexpected source worker response {context}: {response}"),
            ),
            Self::MissingReconnectWait { reconnect_context } => CnxError::ProtocolViolation(
                format!("unexpected {reconnect_context} reconnect completion without pending wait"),
            ),
            Self::MissingControlFramePendingReconnectResume => CnxError::ProtocolViolation(
                "unexpected reconnect completion without pending control-frame resume".to_string(),
            ),
            Self::MissingControlFramePendingRefreshState { context } => {
                CnxError::ProtocolViolation(format!(
                    "unexpected {context} without pending control-frame refresh state"
                ))
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceFailureReason {
    ProtocolViolation(SourceProtocolViolationKind),
    RetryBudgetExhausted(SourceRetryBudgetExhaustionKind),
    ControlReset(SourceWorkerControlResetKind),
    RefreshExhausted(SourceScheduledGroupsRefreshExhaustionReason),
    NonRetryable,
}

#[derive(Debug)]
pub(crate) struct SourceFailure {
    cause: CnxError,
    reason: SourceFailureReason,
}

impl SourceFailure {
    fn timeout_reset() -> Self {
        Self {
            cause: CnxError::Timeout,
            reason: SourceFailureReason::ControlReset(SourceWorkerControlResetKind::Timeout),
        }
    }

    fn non_retryable(cause: CnxError) -> Self {
        Self {
            cause,
            reason: SourceFailureReason::NonRetryable,
        }
    }

    fn from_cause(cause: CnxError) -> Self {
        match cause {
            CnxError::Timeout => Self::timeout_reset(),
            _ => match classify_source_worker_control_reset(&cause) {
                Some(kind) => Self {
                    cause,
                    reason: SourceFailureReason::ControlReset(kind),
                },
                None => Self::non_retryable(cause),
            },
        }
    }

    fn protocol_violation(kind: SourceProtocolViolationKind) -> Self {
        Self {
            cause: kind.clone().into_error(),
            reason: SourceFailureReason::ProtocolViolation(kind),
        }
    }

    fn retry_budget_exhausted(kind: SourceRetryBudgetExhaustionKind) -> Self {
        Self {
            cause: shape_source_retry_budget_exhaustion(kind),
            reason: SourceFailureReason::RetryBudgetExhausted(kind),
        }
    }

    fn refresh_exhausted(kind: SourceScheduledGroupsRefreshExhaustionReason) -> Self {
        Self {
            cause: shape_source_scheduled_groups_refresh_exhaustion(kind),
            reason: SourceFailureReason::RefreshExhausted(kind),
        }
    }

    pub(crate) fn as_error(&self) -> &CnxError {
        &self.cause
    }

    pub(crate) fn into_error(self) -> CnxError {
        self.cause
    }
}

impl From<CnxError> for SourceFailure {
    fn from(cause: CnxError) -> Self {
        Self::from_cause(cause)
    }
}

fn classify_source_retry_budget(deadline: std::time::Instant) -> SourceRetryBudgetDisposition {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        SourceRetryBudgetDisposition::Exhausted
    } else {
        SourceRetryBudgetDisposition::Remaining(remaining)
    }
}

fn shape_source_retry_budget_exhaustion(kind: SourceRetryBudgetExhaustionKind) -> CnxError {
    let _ = kind;
    CnxError::Timeout
}

fn source_operation_attempt_timeout(
    deadline: std::time::Instant,
    attempt_timeout_cap: Duration,
) -> std::result::Result<Duration, SourceFailure> {
    match classify_source_retry_budget(deadline) {
        SourceRetryBudgetDisposition::Exhausted => Err(SourceFailure::retry_budget_exhausted(
            SourceRetryBudgetExhaustionKind::OperationAttempt,
        )),
        SourceRetryBudgetDisposition::Remaining(remaining) => {
            Ok(remaining.min(attempt_timeout_cap))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceWorkerControlResetKind {
    WorkerNotInitialized,
    TransportClosed,
    ChannelClosed,
    Timeout,
    TimeoutLikePeerOrInternal,
    BridgePeerReset,
    UnexpectedCorrelationReply,
    GrantAttachmentsDrainedOrFenced,
    InvalidGrantAttachmentToken,
    MissingChannelBufferRouteState,
}

impl SourceWorkerControlResetKind {
    const fn supports_cached_grant_derived_snapshot(self) -> bool {
        matches!(
            self,
            Self::WorkerNotInitialized
                | Self::TransportClosed
                | Self::Timeout
                | Self::GrantAttachmentsDrainedOrFenced
        )
    }

    const fn is_bridge_reset_like(self) -> bool {
        matches!(
            self,
            Self::TransportClosed | Self::ChannelClosed | Self::BridgePeerReset
        )
    }

    const fn is_timeout_like_reset(self) -> bool {
        matches!(self, Self::Timeout | Self::TimeoutLikePeerOrInternal)
    }

    const fn should_follow_fail_closed_bridge_policy(
        self,
        policy: SourceControlFrameBridgeResetPolicy,
    ) -> bool {
        self.is_bridge_reset_like()
            || (self.is_timeout_like_reset()
                && !matches!(
                    policy,
                    SourceControlFrameBridgeResetPolicy::RetryAfterReconnect
                ))
    }
}

fn classify_source_worker_control_reset(err: &CnxError) -> Option<SourceWorkerControlResetKind> {
    match err {
        CnxError::PeerError(message) if message == "worker not initialized" => {
            Some(SourceWorkerControlResetKind::WorkerNotInitialized)
        }
        CnxError::TransportClosed(_) => Some(SourceWorkerControlResetKind::TransportClosed),
        CnxError::ChannelClosed => Some(SourceWorkerControlResetKind::ChannelClosed),
        CnxError::Timeout => Some(SourceWorkerControlResetKind::Timeout),
        CnxError::PeerError(message) | CnxError::Internal(message)
            if message.contains("operation timed out") =>
        {
            Some(SourceWorkerControlResetKind::TimeoutLikePeerOrInternal)
        }
        CnxError::PeerError(message)
            if message.contains("transport closed")
                && (message.contains("Connection reset by peer")
                    || message.contains("early eof")
                    || message.contains("Broken pipe")
                    || message.contains("bridge stopped")) =>
        {
            Some(SourceWorkerControlResetKind::BridgePeerReset)
        }
        CnxError::ProtocolViolation(message) | CnxError::PeerError(message)
            if message.contains("unexpected correlation_id in reply batch") =>
        {
            Some(SourceWorkerControlResetKind::UnexpectedCorrelationReply)
        }
        CnxError::AccessDenied(message) | CnxError::PeerError(message)
            if message.contains("drained/fenced") && message.contains("grant attachments") =>
        {
            Some(SourceWorkerControlResetKind::GrantAttachmentsDrainedOrFenced)
        }
        CnxError::AccessDenied(message)
        | CnxError::PeerError(message)
        | CnxError::Internal(message)
            if message.contains("invalid or revoked grant attachment token") =>
        {
            Some(SourceWorkerControlResetKind::InvalidGrantAttachmentToken)
        }
        CnxError::AccessDenied(message)
        | CnxError::PeerError(message)
        | CnxError::Internal(message)
            if message.contains("missing route state for channel_buffer") =>
        {
            Some(SourceWorkerControlResetKind::MissingChannelBufferRouteState)
        }
        _ => None,
    }
}

fn classify_source_scheduled_groups_refresh_deadline_exhaustion(
    refresh_disposition: SourceScheduledGroupsRefreshDisposition,
    recovery_observation: SourceScheduledGroupsRefreshRecoveryObservation,
) -> SourceScheduledGroupsRefreshExhaustionReason {
    if matches!(
        (refresh_disposition, recovery_observation),
        (
            SourceScheduledGroupsRefreshDisposition::ReplayRequiredRecovery,
            SourceScheduledGroupsRefreshRecoveryObservation::NoLiveWorkerRecovery,
        )
    ) {
        SourceScheduledGroupsRefreshExhaustionReason::NoLiveWorkerRecovery
    } else {
        SourceScheduledGroupsRefreshExhaustionReason::Timeout
    }
}

fn classify_source_scheduled_groups_refresh_exhaustion(
    err: &CnxError,
    refresh_disposition: SourceScheduledGroupsRefreshDisposition,
    recovery_observation: SourceScheduledGroupsRefreshRecoveryObservation,
) -> Option<SourceScheduledGroupsRefreshExhaustionReason> {
    if matches!(err, CnxError::Timeout) {
        return Some(
            classify_source_scheduled_groups_refresh_deadline_exhaustion(
                refresh_disposition,
                recovery_observation,
            ),
        );
    }

    Some(match classify_source_worker_control_reset(err)? {
        SourceWorkerControlResetKind::Timeout
        | SourceWorkerControlResetKind::TimeoutLikePeerOrInternal => {
            SourceScheduledGroupsRefreshExhaustionReason::Timeout
        }
        SourceWorkerControlResetKind::TransportClosed
        | SourceWorkerControlResetKind::ChannelClosed => {
            SourceScheduledGroupsRefreshExhaustionReason::TransportClosed
        }
        SourceWorkerControlResetKind::MissingChannelBufferRouteState => {
            SourceScheduledGroupsRefreshExhaustionReason::MissingRouteState
        }
        SourceWorkerControlResetKind::WorkerNotInitialized
        | SourceWorkerControlResetKind::BridgePeerReset
        | SourceWorkerControlResetKind::UnexpectedCorrelationReply
        | SourceWorkerControlResetKind::GrantAttachmentsDrainedOrFenced
        | SourceWorkerControlResetKind::InvalidGrantAttachmentToken => {
            SourceScheduledGroupsRefreshExhaustionReason::RetryableReset
        }
    })
}

fn source_scheduled_groups_refresh_failure_from_error(
    err: CnxError,
    refresh_disposition: SourceScheduledGroupsRefreshDisposition,
    recovery_observation: SourceScheduledGroupsRefreshRecoveryObservation,
) -> SourceFailure {
    match classify_source_scheduled_groups_refresh_exhaustion(
        &err,
        refresh_disposition,
        recovery_observation,
    ) {
        Some(reason) => SourceFailure::refresh_exhausted(reason),
        None => SourceFailure::from_cause(err),
    }
}

fn source_scheduled_groups_refresh_deadline_failure(
    refresh_disposition: SourceScheduledGroupsRefreshDisposition,
    recovery_observation: SourceScheduledGroupsRefreshRecoveryObservation,
) -> SourceFailure {
    SourceFailure::refresh_exhausted(
        classify_source_scheduled_groups_refresh_deadline_exhaustion(
            refresh_disposition,
            recovery_observation,
        ),
    )
}

fn shape_source_scheduled_groups_refresh_exhaustion(
    reason: SourceScheduledGroupsRefreshExhaustionReason,
) -> CnxError {
    match reason {
        SourceScheduledGroupsRefreshExhaustionReason::NoLiveWorkerRecovery => {
            CnxError::Internal(
                "source worker replay-required scheduled-groups refresh exhausted before a live worker recovered"
                    .to_string(),
            )
        }
        reason => {
            let reason = match reason {
                SourceScheduledGroupsRefreshExhaustionReason::Timeout => "timeout",
                SourceScheduledGroupsRefreshExhaustionReason::TransportClosed => {
                    "transport_closed"
                }
                SourceScheduledGroupsRefreshExhaustionReason::MissingRouteState => {
                    "missing_route_state"
                }
                SourceScheduledGroupsRefreshExhaustionReason::RetryableReset => {
                    "retryable_reset"
                }
                SourceScheduledGroupsRefreshExhaustionReason::NoLiveWorkerRecovery => {
                    unreachable!("handled above")
                }
            };
            CnxError::Internal(format!(
                "source worker post-ack scheduled-groups refresh exhausted before scheduled groups converged ({reason})"
            ))
        }
    }
}

fn can_use_cached_grant_derived_snapshot(err: &CnxError) -> bool {
    classify_source_worker_control_reset(err)
        .is_some_and(|kind| kind.supports_cached_grant_derived_snapshot())
}

fn can_retry_update_logical_roots(err: &CnxError) -> bool {
    classify_source_worker_control_reset(err).is_some()
}

fn can_retry_force_find(err: &CnxError) -> bool {
    matches!(
        classify_source_worker_control_reset(err),
        Some(SourceWorkerControlResetKind::UnexpectedCorrelationReply)
    )
}

#[derive(Debug)]
enum SourceReconnectRetryDisposition {
    ReconnectAfter(SourceProgressState),
    Fail(SourceFailure),
}

#[derive(Debug)]
enum SourceWaitRetryDisposition {
    WaitAfter(SourceProgressState),
    Fail(SourceFailure),
}

#[derive(Debug)]
enum SourceScheduledGroupsRefreshRetryDisposition {
    ReconnectAfter(SourceProgressState),
    Fail(SourceFailure),
}

#[derive(Clone, Copy, Debug)]
struct SourceRetryMachine {
    deadline: std::time::Instant,
}

impl SourceRetryMachine {
    fn new(deadline: std::time::Instant) -> Self {
        Self { deadline }
    }

    fn budget(&self) -> SourceRetryBudgetDisposition {
        classify_source_retry_budget(self.deadline)
    }

    fn reconnect_after_update_roots_gap(
        &self,
        err: CnxError,
        after: SourceProgressState,
    ) -> SourceReconnectRetryDisposition {
        if !can_retry_update_logical_roots(&err) {
            return SourceReconnectRetryDisposition::Fail(SourceFailure::from_cause(err));
        }
        if matches!(self.budget(), SourceRetryBudgetDisposition::Exhausted) {
            return SourceReconnectRetryDisposition::Fail(SourceFailure::retry_budget_exhausted(
                SourceRetryBudgetExhaustionKind::OperationWait,
            ));
        }
        SourceReconnectRetryDisposition::ReconnectAfter(after)
    }

    fn wait_after_retryable_gap(
        &self,
        err: CnxError,
        after: SourceProgressState,
        can_retry: impl FnOnce(&CnxError) -> bool,
    ) -> SourceWaitRetryDisposition {
        if !can_retry(&err) {
            return SourceWaitRetryDisposition::Fail(SourceFailure::from_cause(err));
        }
        if matches!(self.budget(), SourceRetryBudgetDisposition::Exhausted) {
            return SourceWaitRetryDisposition::Fail(SourceFailure::retry_budget_exhausted(
                SourceRetryBudgetExhaustionKind::OperationWait,
            ));
        }
        SourceWaitRetryDisposition::WaitAfter(after)
    }

    fn scheduled_groups_refresh_after_error(
        &self,
        err: CnxError,
        after: SourceProgressState,
        refresh_disposition: SourceScheduledGroupsRefreshDisposition,
        recovery_observation: SourceScheduledGroupsRefreshRecoveryObservation,
    ) -> SourceScheduledGroupsRefreshRetryDisposition {
        if !can_retry_update_logical_roots(&err) {
            return SourceScheduledGroupsRefreshRetryDisposition::Fail(
                source_scheduled_groups_refresh_failure_from_error(
                    err,
                    refresh_disposition,
                    recovery_observation,
                ),
            );
        }
        if matches!(self.budget(), SourceRetryBudgetDisposition::Exhausted) {
            return SourceScheduledGroupsRefreshRetryDisposition::Fail(
                source_scheduled_groups_refresh_deadline_failure(
                    refresh_disposition,
                    recovery_observation,
                ),
            );
        }
        SourceScheduledGroupsRefreshRetryDisposition::ReconnectAfter(after)
    }

    fn scheduled_groups_refresh_after_failure(
        &self,
        failure: SourceFailure,
        after: SourceProgressState,
        refresh_disposition: SourceScheduledGroupsRefreshDisposition,
        recovery_observation: SourceScheduledGroupsRefreshRecoveryObservation,
    ) -> SourceScheduledGroupsRefreshRetryDisposition {
        match failure.reason {
            SourceFailureReason::RetryBudgetExhausted(_) => {
                SourceScheduledGroupsRefreshRetryDisposition::Fail(
                    source_scheduled_groups_refresh_deadline_failure(
                        refresh_disposition,
                        recovery_observation,
                    ),
                )
            }
            SourceFailureReason::RefreshExhausted(_)
            | SourceFailureReason::ProtocolViolation(_)
            | SourceFailureReason::NonRetryable => {
                SourceScheduledGroupsRefreshRetryDisposition::Fail(failure)
            }
            SourceFailureReason::ControlReset(_) => self.scheduled_groups_refresh_after_error(
                failure.into_error(),
                after,
                refresh_disposition,
                recovery_observation,
            ),
        }
    }
}

fn take_pending_reconnect_wait_after(
    pending_reconnect_wait_after: &mut Option<SourceProgressState>,
    reconnect_context: &'static str,
) -> std::result::Result<SourceProgressState, SourceFailure> {
    pending_reconnect_wait_after
        .take()
        .ok_or_else(|| missing_source_reconnect_wait_after(reconnect_context))
}

fn missing_source_reconnect_wait_after(reconnect_context: &'static str) -> SourceFailure {
    SourceFailure::protocol_violation(SourceProtocolViolationKind::MissingReconnectWait {
        reconnect_context,
    })
}

fn missing_control_frame_pending_reconnect_resume() -> SourceFailure {
    SourceFailure::protocol_violation(
        SourceProtocolViolationKind::MissingControlFramePendingReconnectResume,
    )
}

fn missing_control_frame_pending_refresh_state(context: &'static str) -> SourceFailure {
    SourceFailure::protocol_violation(
        SourceProtocolViolationKind::MissingControlFramePendingRefreshState { context },
    )
}

fn expect_source_worker_ack(
    context: &'static str,
    response: SourceWorkerResponse,
) -> std::result::Result<(), SourceFailure> {
    match response {
        SourceWorkerResponse::Ack => Ok(()),
        other => Err(SourceFailure::protocol_violation(
            SourceProtocolViolationKind::UnexpectedWorkerResponse {
                context,
                response: format!("{:?}", other),
            },
        )),
    }
}

fn unexpected_source_worker_response_failure(
    context: &'static str,
    response: SourceWorkerResponse,
) -> SourceFailure {
    SourceFailure::protocol_violation(SourceProtocolViolationKind::UnexpectedWorkerResponse {
        context,
        response: format!("{:?}", response),
    })
}

fn unexpected_source_worker_response_result<T>(
    context: &'static str,
    response: SourceWorkerResponse,
) -> std::result::Result<T, SourceFailure> {
    Err(unexpected_source_worker_response_failure(context, response))
}

struct SourceForceFindMachine {
    deadline: std::time::Instant,
}

struct SourceScheduledGroupIdsMachine {
    deadline: std::time::Instant,
    pending_reconnect_wait_after: Option<SourceProgressState>,
}

struct SourceUpdateLogicalRootsMachine {
    deadline: std::time::Instant,
}

struct SourceObservabilitySnapshotMachine {
    deadline: std::time::Instant,
    timeout_cap: Duration,
}

struct SourceStartMachine {
    deadline: std::time::Instant,
    pending_reconnect_wait_after: Option<SourceProgressState>,
}

struct SourceReplayRetainedControlStateMachine {
    deadline: std::time::Instant,
    pending_reconnect_wait_after: Option<SourceProgressState>,
}

struct SourceScheduledGroupsRefreshMachine {
    deadline: std::time::Instant,
    scheduled_groups_expectation: SourceScheduledGroupsExpectation,
    refresh_disposition: SourceScheduledGroupsRefreshDisposition,
    recovery_observation: SourceScheduledGroupsRefreshRecoveryObservation,
    pending_reconnect_wait_after: Option<SourceProgressState>,
}

enum SourceScheduledGroupIdsEffect {
    Attempt { timeout: Duration },
    Reconnect,
    Wait { after: SourceProgressState },
    Complete(SourceWorkerResponse),
    Fail(SourceFailure),
}

enum SourceReplayRetainedControlStateEffect {
    Attempt { timeout: Duration },
    Reconnect,
    Wait { after: SourceProgressState },
    Complete,
    Fail(SourceFailure),
}

enum SourceScheduledGroupsRefreshEffect {
    EnsureStarted,
    ReplayRetainedControlState,
    AcquireClient,
    RefreshGrants {
        client: TypedWorkerClient<SourceWorkerRpc>,
    },
    FetchScheduledGroups {
        client: TypedWorkerClient<SourceWorkerRpc>,
        stable_host_ref: String,
    },
    Reconnect,
    Wait {
        after: SourceProgressState,
    },
    CommitGroups {
        scheduled_source: std::collections::BTreeMap<String, Vec<String>>,
        scheduled_scan: std::collections::BTreeMap<String, Vec<String>>,
    },
    Complete,
    Fail(SourceFailure),
}

enum SourceStartEffect {
    Attempt { timeout: Duration },
    Reconnect,
    Wait { after: SourceProgressState },
    Complete,
    Fail(SourceFailure),
}

enum SourceObservabilitySnapshotEffect {
    Attempt { timeout: Duration },
    Reconnect,
    Complete(SourceWorkerResponse),
    Fail(SourceFailure),
}

enum SourceUpdateLogicalRootsEffect {
    Attempt { timeout: Duration },
    Wait { after: SourceProgressState },
    Complete,
    Fail(SourceFailure),
}

enum SourceScheduledGroupIdsEvent {
    AttemptCompleted {
        rpc_result: std::result::Result<SourceWorkerResponse, CnxError>,
        after: SourceProgressState,
    },
    ReconnectCompleted,
    WaitCompleted,
}

enum SourceUpdateLogicalRootsEvent {
    AttemptCompleted {
        rpc_result: std::result::Result<SourceWorkerResponse, CnxError>,
        after: SourceProgressState,
    },
    WaitCompleted,
}

enum SourceObservabilitySnapshotEvent {
    AttemptCompleted {
        rpc_result: std::result::Result<SourceWorkerResponse, CnxError>,
        after: SourceProgressState,
    },
    ReconnectCompleted,
}

enum SourceStartEvent {
    AttemptCompleted {
        start_result: std::result::Result<(), CnxError>,
        after: SourceProgressState,
    },
    ReconnectCompleted,
    WaitCompleted,
}

enum SourceReplayRetainedControlStateEvent {
    AttemptCompleted {
        rpc_result: std::result::Result<SourceWorkerResponse, CnxError>,
        after: SourceProgressState,
    },
    ReconnectCompleted,
    WaitCompleted,
}

struct SourceScheduledGroupsRefreshGrantedClient {
    client: TypedWorkerClient<SourceWorkerRpc>,
    stable_host_ref: String,
}

struct SourceScheduledGroupsRefreshFetchedGroups {
    scheduled_source: std::collections::BTreeMap<String, Vec<String>>,
    scheduled_scan: std::collections::BTreeMap<String, Vec<String>>,
    cache_empty: bool,
}

enum SourceScheduledGroupsRefreshEvent {
    EnsureStartedCompleted {
        result: std::result::Result<(), SourceFailure>,
        after: SourceProgressState,
    },
    ReplayRetainedControlStateCompleted(std::result::Result<(), SourceFailure>),
    ClientAcquired {
        result: std::result::Result<TypedWorkerClient<SourceWorkerRpc>, SourceFailure>,
        after: SourceProgressState,
    },
    GrantsRefreshed {
        result: std::result::Result<SourceScheduledGroupsRefreshGrantedClient, SourceFailure>,
        after: SourceProgressState,
    },
    FetchScheduledGroupsCompleted {
        result: std::result::Result<SourceScheduledGroupsRefreshFetchedGroups, SourceFailure>,
        after: SourceProgressState,
    },
    ReconnectCompleted,
    WaitCompleted,
    CommitGroupsCompleted,
}

enum SourceForceFindEffect {
    Attempt { timeout: Duration },
    RetryBackoff { delay: Duration },
    Complete(Vec<Event>),
    Fail(SourceFailure),
}

enum SourceForceFindEvent {
    AttemptCompleted {
        rpc_result: std::result::Result<Vec<Event>, CnxError>,
    },
    WaitCompleted,
}

enum SourceOperationLoopStep<Event, Output, Error> {
    Event(Event),
    Complete(Output),
    Fail(Error),
}

async fn drive_source_machine_loop<Effect, Event, Output, Error, Execute, ExecuteFuture, Advance>(
    mut effect: Effect,
    mut execute: Execute,
    mut advance: Advance,
) -> std::result::Result<Output, Error>
where
    Execute: FnMut(Effect) -> ExecuteFuture,
    ExecuteFuture: std::future::Future<Output = SourceOperationLoopStep<Event, Output, Error>>,
    Advance: FnMut(Event) -> std::result::Result<Effect, Error>,
{
    loop {
        match execute(effect).await {
            SourceOperationLoopStep::Event(event) => {
                effect = advance(event)?;
            }
            SourceOperationLoopStep::Complete(output) => return Ok(output),
            SourceOperationLoopStep::Fail(err) => return Err(err),
        }
    }
}

impl SourceForceFindMachine {
    fn new(deadline: std::time::Instant) -> Self {
        Self { deadline }
    }

    fn start(&self) -> std::result::Result<SourceForceFindEffect, SourceFailure> {
        Ok(SourceForceFindEffect::Attempt {
            timeout: source_operation_attempt_timeout(
                self.deadline,
                SOURCE_WORKER_FORCE_FIND_TIMEOUT,
            )?,
        })
    }

    fn advance(
        &self,
        event: SourceForceFindEvent,
    ) -> std::result::Result<SourceForceFindEffect, SourceFailure> {
        match event {
            SourceForceFindEvent::AttemptCompleted {
                rpc_result: Ok(events),
                ..
            } => Ok(SourceForceFindEffect::Complete(events)),
            SourceForceFindEvent::AttemptCompleted {
                rpc_result: Err(err),
                ..
            } => {
                if !can_retry_force_find(&err) {
                    return Ok(SourceForceFindEffect::Fail(SourceFailure::from_cause(err)));
                }
                if matches!(
                    SourceRetryMachine::new(self.deadline).budget(),
                    SourceRetryBudgetDisposition::Exhausted
                ) {
                    return Ok(SourceForceFindEffect::Fail(
                        SourceFailure::retry_budget_exhausted(
                            SourceRetryBudgetExhaustionKind::OperationWait,
                        ),
                    ));
                }
                Ok(SourceForceFindEffect::RetryBackoff {
                    delay: SOURCE_WORKER_FORCE_FIND_RETRY_BACKOFF,
                })
            }
            SourceForceFindEvent::WaitCompleted => self.start(),
        }
    }
}

impl SourceScheduledGroupIdsMachine {
    fn new(deadline: std::time::Instant) -> Self {
        Self {
            deadline,
            pending_reconnect_wait_after: None,
        }
    }

    fn start(&self) -> std::result::Result<SourceScheduledGroupIdsEffect, SourceFailure> {
        Ok(SourceScheduledGroupIdsEffect::Attempt {
            timeout: source_operation_attempt_timeout(
                self.deadline,
                SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
            )?,
        })
    }

    fn advance(
        &mut self,
        event: SourceScheduledGroupIdsEvent,
    ) -> std::result::Result<SourceScheduledGroupIdsEffect, SourceFailure> {
        match event {
            SourceScheduledGroupIdsEvent::AttemptCompleted {
                rpc_result: Ok(response),
                ..
            } => Ok(SourceScheduledGroupIdsEffect::Complete(response)),
            SourceScheduledGroupIdsEvent::AttemptCompleted {
                rpc_result: Err(err),
                after,
            } => Ok(
                match SourceRetryMachine::new(self.deadline)
                    .reconnect_after_update_roots_gap(err, after)
                {
                    SourceReconnectRetryDisposition::ReconnectAfter(after) => {
                        self.pending_reconnect_wait_after = Some(after);
                        SourceScheduledGroupIdsEffect::Reconnect
                    }
                    SourceReconnectRetryDisposition::Fail(failure) => {
                        SourceScheduledGroupIdsEffect::Fail(failure)
                    }
                },
            ),
            SourceScheduledGroupIdsEvent::ReconnectCompleted => {
                Ok(SourceScheduledGroupIdsEffect::Wait {
                    after: take_pending_reconnect_wait_after(
                        &mut self.pending_reconnect_wait_after,
                        "scheduled-group-ids",
                    )?,
                })
            }
            SourceScheduledGroupIdsEvent::WaitCompleted => self.start(),
        }
    }
}

impl SourceUpdateLogicalRootsMachine {
    fn new(deadline: std::time::Instant) -> Self {
        Self { deadline }
    }

    fn start(&self) -> std::result::Result<SourceUpdateLogicalRootsEffect, SourceFailure> {
        Ok(SourceUpdateLogicalRootsEffect::Attempt {
            timeout: source_operation_attempt_timeout(
                self.deadline,
                SOURCE_WORKER_UPDATE_ROOTS_RPC_TIMEOUT,
            )?,
        })
    }

    fn advance(
        &self,
        event: SourceUpdateLogicalRootsEvent,
    ) -> std::result::Result<SourceUpdateLogicalRootsEffect, SourceFailure> {
        match event {
            SourceUpdateLogicalRootsEvent::AttemptCompleted {
                rpc_result: Err(CnxError::InvalidInput(message)),
                ..
            } => Ok(SourceUpdateLogicalRootsEffect::Fail(
                SourceFailure::non_retryable(CnxError::InvalidInput(message)),
            )),
            SourceUpdateLogicalRootsEvent::AttemptCompleted {
                rpc_result: Ok(response),
                ..
            } => Ok(
                match expect_source_worker_ack("for update roots", response) {
                    Ok(()) => SourceUpdateLogicalRootsEffect::Complete,
                    Err(failure) => SourceUpdateLogicalRootsEffect::Fail(failure),
                },
            ),
            SourceUpdateLogicalRootsEvent::AttemptCompleted {
                rpc_result: Err(err),
                after,
            } => Ok(
                match SourceRetryMachine::new(self.deadline).wait_after_retryable_gap(
                    err,
                    after,
                    can_retry_update_logical_roots,
                ) {
                    SourceWaitRetryDisposition::WaitAfter(after) => {
                        SourceUpdateLogicalRootsEffect::Wait { after }
                    }
                    SourceWaitRetryDisposition::Fail(failure) => {
                        SourceUpdateLogicalRootsEffect::Fail(failure)
                    }
                },
            ),
            SourceUpdateLogicalRootsEvent::WaitCompleted => self.start(),
        }
    }
}

impl SourceObservabilitySnapshotMachine {
    fn new(deadline: std::time::Instant, timeout_cap: Duration) -> Self {
        Self {
            deadline,
            timeout_cap,
        }
    }

    fn start(&self) -> std::result::Result<SourceObservabilitySnapshotEffect, SourceFailure> {
        Ok(SourceObservabilitySnapshotEffect::Attempt {
            timeout: source_operation_attempt_timeout(self.deadline, self.timeout_cap)?,
        })
    }

    fn advance(
        &self,
        event: SourceObservabilitySnapshotEvent,
    ) -> std::result::Result<SourceObservabilitySnapshotEffect, SourceFailure> {
        match event {
            SourceObservabilitySnapshotEvent::AttemptCompleted {
                rpc_result: Ok(response),
                ..
            } => Ok(SourceObservabilitySnapshotEffect::Complete(response)),
            SourceObservabilitySnapshotEvent::AttemptCompleted {
                rpc_result: Err(err),
                after,
            } => Ok(
                match SourceRetryMachine::new(self.deadline)
                    .reconnect_after_update_roots_gap(err, after)
                {
                    SourceReconnectRetryDisposition::ReconnectAfter(_) => {
                        SourceObservabilitySnapshotEffect::Reconnect
                    }
                    SourceReconnectRetryDisposition::Fail(failure) => {
                        SourceObservabilitySnapshotEffect::Fail(failure)
                    }
                },
            ),
            SourceObservabilitySnapshotEvent::ReconnectCompleted => self.start(),
        }
    }
}

impl SourceStartMachine {
    fn new(deadline: std::time::Instant) -> Self {
        Self {
            deadline,
            pending_reconnect_wait_after: None,
        }
    }

    fn start(&self) -> std::result::Result<SourceStartEffect, SourceFailure> {
        Ok(SourceStartEffect::Attempt {
            timeout: source_operation_attempt_timeout(
                self.deadline,
                SOURCE_WORKER_START_ATTEMPT_TIMEOUT,
            )?,
        })
    }

    fn advance(
        &mut self,
        event: SourceStartEvent,
    ) -> std::result::Result<SourceStartEffect, SourceFailure> {
        match event {
            SourceStartEvent::AttemptCompleted {
                start_result: Ok(()),
                ..
            } => Ok(SourceStartEffect::Complete),
            SourceStartEvent::AttemptCompleted {
                start_result: Err(err),
                after,
            } => Ok(
                match SourceRetryMachine::new(self.deadline)
                    .reconnect_after_update_roots_gap(err, after)
                {
                    SourceReconnectRetryDisposition::ReconnectAfter(after) => {
                        self.pending_reconnect_wait_after = Some(after);
                        SourceStartEffect::Reconnect
                    }
                    SourceReconnectRetryDisposition::Fail(failure) => {
                        SourceStartEffect::Fail(failure)
                    }
                },
            ),
            SourceStartEvent::ReconnectCompleted => Ok(SourceStartEffect::Wait {
                after: take_pending_reconnect_wait_after(
                    &mut self.pending_reconnect_wait_after,
                    "start",
                )?,
            }),
            SourceStartEvent::WaitCompleted => self.start(),
        }
    }
}

impl SourceReplayRetainedControlStateMachine {
    fn new(deadline: std::time::Instant) -> Self {
        Self {
            deadline,
            pending_reconnect_wait_after: None,
        }
    }

    fn start(&self) -> SourceReplayRetainedControlStateEffect {
        match source_operation_attempt_timeout(
            self.deadline,
            SOURCE_WORKER_SCHEDULE_REFRESH_RPC_TIMEOUT,
        ) {
            Ok(timeout) => SourceReplayRetainedControlStateEffect::Attempt { timeout },
            Err(failure) => SourceReplayRetainedControlStateEffect::Fail(failure),
        }
    }

    fn advance(
        &mut self,
        event: SourceReplayRetainedControlStateEvent,
    ) -> SourceReplayRetainedControlStateEffect {
        match event {
            SourceReplayRetainedControlStateEvent::AttemptCompleted {
                rpc_result: Ok(response),
                ..
            } => match expect_source_worker_ack("while replaying retained control state", response)
            {
                Ok(()) => SourceReplayRetainedControlStateEffect::Complete,
                Err(failure) => SourceReplayRetainedControlStateEffect::Fail(failure),
            },
            SourceReplayRetainedControlStateEvent::AttemptCompleted {
                rpc_result: Err(err),
                after,
            } => match SourceRetryMachine::new(self.deadline)
                .reconnect_after_update_roots_gap(err, after)
            {
                SourceReconnectRetryDisposition::ReconnectAfter(after) => {
                    self.pending_reconnect_wait_after = Some(after);
                    SourceReplayRetainedControlStateEffect::Reconnect
                }
                SourceReconnectRetryDisposition::Fail(failure) => {
                    SourceReplayRetainedControlStateEffect::Fail(failure)
                }
            },
            SourceReplayRetainedControlStateEvent::ReconnectCompleted => {
                match take_pending_reconnect_wait_after(
                    &mut self.pending_reconnect_wait_after,
                    "retained-control replay",
                ) {
                    Ok(after) => SourceReplayRetainedControlStateEffect::Wait { after },
                    Err(failure) => SourceReplayRetainedControlStateEffect::Fail(failure),
                }
            }
            SourceReplayRetainedControlStateEvent::WaitCompleted => self.start(),
        }
    }
}

impl SourceScheduledGroupsRefreshMachine {
    fn new(
        deadline: std::time::Instant,
        scheduled_groups_expectation: SourceScheduledGroupsExpectation,
        refresh_disposition: SourceScheduledGroupsRefreshDisposition,
    ) -> Self {
        Self {
            deadline,
            scheduled_groups_expectation,
            refresh_disposition,
            recovery_observation:
                SourceScheduledGroupsRefreshRecoveryObservation::NoLiveWorkerRecovery,
            pending_reconnect_wait_after: None,
        }
    }

    fn start(&self) -> SourceScheduledGroupsRefreshEffect {
        SourceScheduledGroupsRefreshEffect::EnsureStarted
    }

    fn fail(&self, err: CnxError) -> SourceScheduledGroupsRefreshEffect {
        let failure = source_scheduled_groups_refresh_failure_from_error(
            err,
            self.refresh_disposition,
            self.recovery_observation,
        );
        SourceScheduledGroupsRefreshEffect::Fail(failure)
    }

    fn retry_or_fail(
        &mut self,
        failure: SourceFailure,
        after: SourceProgressState,
    ) -> SourceScheduledGroupsRefreshEffect {
        match SourceRetryMachine::new(self.deadline).scheduled_groups_refresh_after_failure(
            failure,
            after,
            self.refresh_disposition,
            self.recovery_observation,
        ) {
            SourceScheduledGroupsRefreshRetryDisposition::ReconnectAfter(after) => {
                self.pending_reconnect_wait_after = Some(after);
                SourceScheduledGroupsRefreshEffect::Reconnect
            }
            SourceScheduledGroupsRefreshRetryDisposition::Fail(failure) => {
                SourceScheduledGroupsRefreshEffect::Fail(failure)
            }
        }
    }

    fn advance(
        &mut self,
        event: SourceScheduledGroupsRefreshEvent,
    ) -> SourceScheduledGroupsRefreshEffect {
        match event {
            SourceScheduledGroupsRefreshEvent::EnsureStartedCompleted {
                result: Ok(()), ..
            } => SourceScheduledGroupsRefreshEffect::ReplayRetainedControlState,
            SourceScheduledGroupsRefreshEvent::EnsureStartedCompleted {
                result: Err(err),
                after,
            } => self.retry_or_fail(err, after),
            SourceScheduledGroupsRefreshEvent::ReplayRetainedControlStateCompleted(Ok(())) => {
                if self.refresh_disposition.requires_replay_recovery() {
                    self.recovery_observation =
                        SourceScheduledGroupsRefreshRecoveryObservation::RecoveredLiveWorker;
                }
                SourceScheduledGroupsRefreshEffect::AcquireClient
            }
            SourceScheduledGroupsRefreshEvent::ReplayRetainedControlStateCompleted(Err(err)) => {
                SourceScheduledGroupsRefreshEffect::Fail(err)
            }
            SourceScheduledGroupsRefreshEvent::ClientAcquired {
                result: Ok(client), ..
            } => SourceScheduledGroupsRefreshEffect::RefreshGrants { client },
            SourceScheduledGroupsRefreshEvent::ClientAcquired {
                result: Err(err),
                after,
            } => self.retry_or_fail(err, after),
            SourceScheduledGroupsRefreshEvent::GrantsRefreshed {
                result:
                    Ok(SourceScheduledGroupsRefreshGrantedClient {
                        client,
                        stable_host_ref,
                    }),
                ..
            } => SourceScheduledGroupsRefreshEffect::FetchScheduledGroups {
                client,
                stable_host_ref,
            },
            SourceScheduledGroupsRefreshEvent::GrantsRefreshed {
                result: Err(err),
                after,
            } => self.retry_or_fail(err, after),
            SourceScheduledGroupsRefreshEvent::FetchScheduledGroupsCompleted {
                result:
                    Ok(SourceScheduledGroupsRefreshFetchedGroups {
                        scheduled_source,
                        scheduled_scan,
                        cache_empty,
                    }),
                ..
            } => {
                let groups_empty = scheduled_source.values().all(|groups| groups.is_empty())
                    && scheduled_scan.values().all(|groups| groups.is_empty());
                if groups_empty
                    && (cache_empty
                        || self
                            .scheduled_groups_expectation
                            .expect_local_runnable_groups())
                {
                    if matches!(
                        classify_source_retry_budget(self.deadline),
                        SourceRetryBudgetDisposition::Exhausted
                    ) {
                        self.fail(CnxError::Timeout)
                    } else {
                        self.start()
                    }
                } else {
                    SourceScheduledGroupsRefreshEffect::CommitGroups {
                        scheduled_source,
                        scheduled_scan,
                    }
                }
            }
            SourceScheduledGroupsRefreshEvent::FetchScheduledGroupsCompleted {
                result: Err(err),
                after,
            } => self.retry_or_fail(err, after),
            SourceScheduledGroupsRefreshEvent::ReconnectCompleted => {
                match take_pending_reconnect_wait_after(
                    &mut self.pending_reconnect_wait_after,
                    "scheduled-groups refresh",
                ) {
                    Ok(after) => SourceScheduledGroupsRefreshEffect::Wait { after },
                    Err(failure) => SourceScheduledGroupsRefreshEffect::Fail(failure),
                }
            }
            SourceScheduledGroupsRefreshEvent::WaitCompleted => self.start(),
            SourceScheduledGroupsRefreshEvent::CommitGroupsCompleted => {
                SourceScheduledGroupsRefreshEffect::Complete
            }
        }
    }
}

fn is_restart_deferred_retire_pending_deactivate_batch(envelopes: &[ControlEnvelope]) -> bool {
    let Ok(signals) = source_control_signals_from_envelopes(envelopes) else {
        return false;
    };
    !signals.is_empty()
        && signals.iter().all(|signal| {
            matches!(
                signal,
                SourceControlSignal::Deactivate { envelope, .. }
                    if matches!(
                        capanix_runtime_entry_sdk::control::decode_runtime_exec_control(envelope),
                        Ok(Some(
                            capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                                deactivate
                            )
                        )) if deactivate.reason == "restart_deferred_retire_pending"
                    )
            )
        })
}

fn debug_control_scope_capture_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
}

fn debug_force_find_route_capture_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_FORCE_FIND_ROUTE_CAPTURE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn summarize_event_counts_by_origin(events: &[Event]) -> Vec<String> {
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for event in events {
        *counts
            .entry(event.metadata().origin_id.0.clone())
            .or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(origin, count)| format!("{origin}={count}"))
        .collect()
}

fn host_ref_matches_node_id(host_ref: &str, node_id: &NodeId) -> bool {
    host_ref == node_id.0
        || node_id
            .0
            .strip_prefix(host_ref)
            .is_some_and(|suffix| suffix.starts_with('-'))
        || node_id.0.strip_prefix("cluster-").is_some_and(|scoped| {
            scoped == host_ref
                || scoped
                    .strip_prefix(host_ref)
                    .is_some_and(|suffix| suffix.starts_with('-'))
        })
}

fn stable_host_ref_for_node_id(node_id: &NodeId, grants: &[GrantedMountRoot]) -> String {
    let host_refs = grants
        .iter()
        .filter(|grant| host_ref_matches_node_id(&grant.host_ref, node_id))
        .map(|grant| grant.host_ref.clone())
        .collect::<std::collections::BTreeSet<_>>();
    match host_refs.len() {
        1 => host_refs
            .into_iter()
            .next()
            .unwrap_or_else(|| node_id.0.clone()),
        _ => node_id.0.clone(),
    }
}

fn host_ref_from_resource_id(resource_id: &str) -> Option<&str> {
    resource_id.split_once("::").map(|(host_ref, _)| host_ref)
}

fn runtime_scope_resource_matches_logical_root(resource_id: &str, logical_root_id: &str) -> bool {
    resource_id == logical_root_id
        || resource_id
            .rsplit_once("::")
            .is_some_and(|(_, tail)| tail == logical_root_id)
}

fn bound_scope_matches_logical_root(
    bound_scope: &RuntimeBoundScope,
    logical_root_id: &str,
) -> bool {
    bound_scope.scope_id == logical_root_id
        || bound_scope.resource_ids.iter().any(|resource_id| {
            runtime_scope_resource_matches_logical_root(resource_id, logical_root_id)
        })
}

fn bound_scope_has_explicit_local_resource_id(
    bound_scope: &RuntimeBoundScope,
    logical_root_id: &str,
    node_id: &NodeId,
) -> bool {
    bound_scope.resource_ids.iter().any(|resource_id| {
        runtime_scope_resource_matches_logical_root(resource_id, logical_root_id)
            && host_ref_from_resource_id(resource_id)
                .is_some_and(|host_ref| host_ref_matches_node_id(host_ref, node_id))
    })
}

fn bound_scope_has_bare_logical_root_id(
    bound_scope: &RuntimeBoundScope,
    logical_root_id: &str,
) -> bool {
    bound_scope.scope_id == logical_root_id
        || bound_scope
            .resource_ids
            .iter()
            .any(|resource_id| resource_id == logical_root_id)
}

fn root_has_any_matching_grant(root: &RootSpec, grants: &[GrantedMountRoot]) -> bool {
    grants.iter().any(|grant| root.selector.matches(grant))
}

fn root_has_local_matching_grant(
    root: &RootSpec,
    node_id: &NodeId,
    grants: &[GrantedMountRoot],
) -> bool {
    grants.iter().any(|grant| {
        host_ref_matches_node_id(&grant.host_ref, node_id) && root.selector.matches(grant)
    })
}

fn bound_scope_applies_locally(
    bound_scope: &RuntimeBoundScope,
    root: &RootSpec,
    node_id: &NodeId,
    grants: &[GrantedMountRoot],
) -> bool {
    if !bound_scope_matches_logical_root(bound_scope, &root.id) {
        return false;
    }
    if root_has_local_matching_grant(root, node_id, grants) {
        return true;
    }
    if bound_scope_has_explicit_local_resource_id(bound_scope, &root.id, node_id) {
        return true;
    }
    bound_scope_has_bare_logical_root_id(bound_scope, &root.id)
        && !root_has_any_matching_grant(root, grants)
}

fn normalize_node_groups_key(
    groups_by_node: &mut std::collections::BTreeMap<String, Vec<String>>,
    from_node_id: &str,
    stable_host_ref: &str,
) {
    if from_node_id == stable_host_ref {
        return;
    }
    let Some(groups) = groups_by_node.remove(from_node_id) else {
        return;
    };
    let entry = groups_by_node
        .entry(stable_host_ref.to_string())
        .or_default();
    if groups.is_empty() || entry.is_empty() {
        entry.clear();
        return;
    }
    entry.extend(groups);
    entry.sort();
    entry.dedup();
}

fn normalize_observability_snapshot_scheduled_group_keys(
    snapshot: &mut SourceObservabilitySnapshot,
    node_id: &NodeId,
    cached_grants: Option<&Vec<GrantedMountRoot>>,
    cached_schedule_stable_host_ref: Option<&str>,
) {
    let mut stable_host_ref = stable_host_ref_for_node_id(node_id, &snapshot.grants);
    if stable_host_ref == node_id.0
        && let Some(cached_grants) = cached_grants
    {
        let cached_host_ref = stable_host_ref_for_node_id(node_id, cached_grants);
        if cached_host_ref != node_id.0 {
            stable_host_ref = cached_host_ref;
        }
    }
    if stable_host_ref == node_id.0
        && let Some(cached_schedule_stable_host_ref) = cached_schedule_stable_host_ref
    {
        stable_host_ref = cached_schedule_stable_host_ref.to_string();
    }
    normalize_node_groups_key(
        &mut snapshot.scheduled_source_groups_by_node,
        &node_id.0,
        &stable_host_ref,
    );
    normalize_node_groups_key(
        &mut snapshot.scheduled_scan_groups_by_node,
        &node_id.0,
        &stable_host_ref,
    );
}

fn merge_non_empty_cached_groups(
    current: &mut std::collections::BTreeMap<String, Vec<String>>,
    cached: &Option<std::collections::BTreeMap<String, Vec<String>>>,
) {
    let Some(cached) = cached else {
        return;
    };
    if current.is_empty() {
        if !cached.is_empty() {
            *current = cached.clone();
        }
        return;
    }
    for (node_id, groups) in cached {
        match current.get_mut(node_id) {
            Some(existing) if existing.is_empty() => {
                let _ = groups;
            }
            None => {
                current.insert(node_id.clone(), groups.clone());
            }
            _ => {}
        }
    }
}

fn union_cached_groups(
    current: &mut std::collections::BTreeMap<String, Vec<String>>,
    cached: &Option<std::collections::BTreeMap<String, Vec<String>>>,
) {
    let Some(cached) = cached else {
        return;
    };
    for (node_id, groups) in cached {
        if groups.is_empty() {
            continue;
        }
        match current.get_mut(node_id) {
            Some(existing) if existing.is_empty() => {}
            Some(existing) => {
                existing.extend(groups.iter().cloned());
                existing.sort();
                existing.dedup();
            }
            None => {
                current.insert(node_id.clone(), groups.clone());
            }
        }
    }
}

fn update_cached_scheduled_groups_from_refresh(
    cache: &mut SourceWorkerSnapshotCache,
    mut scheduled_source: std::collections::BTreeMap<String, Vec<String>>,
    mut scheduled_scan: std::collections::BTreeMap<String, Vec<String>>,
) {
    union_cached_groups(
        &mut scheduled_source,
        &cache.scheduled_source_groups_by_node,
    );
    union_cached_groups(&mut scheduled_scan, &cache.scheduled_scan_groups_by_node);
    cache.scheduled_source_groups_by_node = Some(scheduled_source);
    cache.scheduled_scan_groups_by_node = Some(scheduled_scan);
}

fn stable_host_ref_from_cached_scheduled_groups(
    node_id: &NodeId,
    cache: &SourceWorkerSnapshotCache,
) -> Option<String> {
    let host_refs = cache
        .scheduled_source_groups_by_node
        .iter()
        .chain(cache.scheduled_scan_groups_by_node.iter())
        .flat_map(|groups| groups.keys())
        .filter(|host_ref| host_ref_matches_node_id(host_ref, node_id))
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    match host_refs.len() {
        1 => host_refs.into_iter().next(),
        _ => None,
    }
}

fn cached_local_scheduled_groups(
    node_id: &NodeId,
    groups_by_node: &Option<std::collections::BTreeMap<String, Vec<String>>>,
) -> Option<std::collections::BTreeSet<String>> {
    let groups = groups_by_node
        .iter()
        .flat_map(|groups_by_node| groups_by_node.iter())
        .filter(|(host_ref, _)| host_ref_matches_node_id(host_ref, node_id))
        .flat_map(|(_, groups)| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    if groups.is_empty() {
        None
    } else {
        Some(groups)
    }
}

fn merge_cached_local_scheduled_groups(
    node_id: &NodeId,
    live_groups: Option<std::collections::BTreeSet<String>>,
    groups_by_node: &Option<std::collections::BTreeMap<String, Vec<String>>>,
    replay_recovery_groups_by_node: &Option<std::collections::BTreeMap<String, Vec<String>>>,
) -> Option<std::collections::BTreeSet<String>> {
    if live_groups.as_ref().is_some_and(|groups| groups.is_empty()) {
        if let Some(recovery_groups) =
            cached_local_scheduled_groups(node_id, replay_recovery_groups_by_node)
        {
            return Some(recovery_groups);
        }
        return Some(std::collections::BTreeSet::new());
    }
    let mut groups = live_groups.unwrap_or_default();
    if let Some(cached) = cached_local_scheduled_groups(node_id, groups_by_node) {
        groups.extend(cached);
    }
    if let Some(recovery) = cached_local_scheduled_groups(node_id, replay_recovery_groups_by_node) {
        groups.extend(recovery);
    }
    if groups.is_empty() {
        None
    } else {
        Some(groups)
    }
}

#[derive(Default, Clone, Copy, Debug)]
struct PrimedLocalScheduleSummary {
    saw_activate_with_bound_scopes: bool,
    has_local_runnable_groups: bool,
}

fn prime_cached_schedule_from_control_signals(
    cache: &mut SourceWorkerSnapshotCache,
    node_id: &NodeId,
    signals: &[SourceControlSignal],
    fallback_roots: &[RootSpec],
    fallback_grants: &[GrantedMountRoot],
) -> PrimedLocalScheduleSummary {
    let changed_grants = signals.iter().rev().find_map(|signal| match signal {
        SourceControlSignal::RuntimeHostGrantChange { changed, .. } => Some((
            changed.version,
            changed
                .grants
                .iter()
                .filter(|row| std::path::Path::new(&row.object.mount_point).is_absolute())
                .map(|row| GrantedMountRoot {
                    object_ref: row.object_ref.clone(),
                    host_ref: row.host.host_ref.clone(),
                    host_ip: row.host.host_ip.clone(),
                    host_name: row.host.host_name.clone(),
                    site: row.host.site.clone(),
                    zone: row.host.zone.clone(),
                    host_labels: row.host.host_labels.clone(),
                    mount_point: row.object.mount_point.clone().into(),
                    fs_source: row.object.fs_source.clone(),
                    fs_type: row.object.fs_type.clone(),
                    mount_options: row.object.mount_options.clone(),
                    interfaces: row.interfaces.clone(),
                    active: matches!(row.grant_state, RuntimeHostGrantState::Active),
                })
                .collect::<Vec<_>>(),
        )),
        _ => None,
    });
    if let Some((version, grants)) = changed_grants {
        cache.last_live_observability_snapshot_at = None;
        cache.host_object_grants_version = Some(version);
        cache.grants = Some(grants);
    }
    let grants = cache
        .grants
        .as_ref()
        .cloned()
        .unwrap_or_else(|| fallback_grants.to_vec());
    let roots = cache
        .logical_roots
        .as_ref()
        .cloned()
        .unwrap_or_else(|| fallback_roots.to_vec());
    let stable_host_ref = {
        let stable = stable_host_ref_for_node_id(node_id, &grants);
        if stable != node_id.0 {
            stable
        } else {
            let local_scope_hosts = signals
                .iter()
                .filter_map(|signal| match signal {
                    SourceControlSignal::Activate { bound_scopes, .. } => Some(bound_scopes),
                    _ => None,
                })
                .flat_map(|bound_scopes| bound_scopes.iter())
                .flat_map(|scope| scope.resource_ids.iter())
                .filter_map(|resource_id| host_ref_from_resource_id(resource_id))
                .filter(|host_ref| host_ref_matches_node_id(host_ref, node_id))
                .map(str::to_string)
                .collect::<std::collections::BTreeSet<_>>();
            match local_scope_hosts.len() {
                1 => local_scope_hosts
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| node_id.0.clone()),
                _ => stable,
            }
        }
    };
    let mut scheduled_source = std::collections::BTreeSet::new();
    let mut scheduled_scan = std::collections::BTreeSet::new();
    let mut saw_source_activate = false;
    let mut saw_scan_activate = false;
    for signal in signals {
        let (unit, bound_scopes) = match signal {
            SourceControlSignal::Activate {
                unit, bound_scopes, ..
            } => (Some(*unit), Some(bound_scopes.as_slice())),
            _ => (None, None),
        };
        let Some(unit) = unit else { continue };
        let Some(bound_scopes) = bound_scopes else {
            continue;
        };
        match unit {
            SourceRuntimeUnit::Source => saw_source_activate = true,
            SourceRuntimeUnit::Scan => saw_scan_activate = true,
        }
        for scope in bound_scopes {
            let applies_locally = roots
                .iter()
                .filter(|root| match unit {
                    SourceRuntimeUnit::Source => root.watch,
                    SourceRuntimeUnit::Scan => root.scan,
                })
                .any(|root| bound_scope_applies_locally(scope, root, node_id, &grants));
            if !applies_locally {
                continue;
            }
            match unit {
                SourceRuntimeUnit::Source => {
                    scheduled_source.insert(scope.scope_id.clone());
                }
                SourceRuntimeUnit::Scan => {
                    scheduled_scan.insert(scope.scope_id.clone());
                }
            }
        }
    }
    let has_local_runnable_groups = !(scheduled_source.is_empty() && scheduled_scan.is_empty());
    if saw_source_activate && scheduled_source.is_empty() {
        cache.scheduled_source_groups_by_node = None;
    } else if !scheduled_source.is_empty() {
        cache.scheduled_source_groups_by_node = Some(std::collections::BTreeMap::from([(
            stable_host_ref.clone(),
            scheduled_source.into_iter().collect::<Vec<_>>(),
        )]));
    }
    if saw_scan_activate && scheduled_scan.is_empty() {
        cache.scheduled_scan_groups_by_node = None;
    } else if !scheduled_scan.is_empty() {
        cache.scheduled_scan_groups_by_node = Some(std::collections::BTreeMap::from([(
            stable_host_ref,
            scheduled_scan.into_iter().collect::<Vec<_>>(),
        )]));
    }
    PrimedLocalScheduleSummary {
        saw_activate_with_bound_scopes: saw_source_activate || saw_scan_activate,
        has_local_runnable_groups,
    }
}

fn bound_scope_matches_local_grant(
    bound_scope: &RuntimeBoundScope,
    node_id: &NodeId,
    grants: &[GrantedMountRoot],
) -> bool {
    bound_scope.resource_ids.iter().any(|resource_id| {
        host_ref_from_resource_id(resource_id)
            .is_some_and(|host_ref| host_ref_matches_node_id(host_ref, node_id))
    }) || grants.iter().any(|grant| {
        host_ref_matches_node_id(&grant.host_ref, node_id)
            && (runtime_scope_resource_matches_logical_root(
                &grant.object_ref,
                &bound_scope.scope_id,
            ) || runtime_scope_resource_matches_logical_root(
                &grant.fs_source,
                &bound_scope.scope_id,
            ))
    })
}

fn prime_cached_schedule_from_control_signals_for_replay_recovery(
    cache: &mut SourceWorkerSnapshotCache,
    node_id: &NodeId,
    signals: &[SourceControlSignal],
    fallback_roots: &[RootSpec],
    fallback_grants: &[GrantedMountRoot],
) {
    let roots = cache
        .logical_roots
        .as_ref()
        .cloned()
        .unwrap_or_else(|| fallback_roots.to_vec());
    let grants = cache
        .grants
        .as_ref()
        .cloned()
        .unwrap_or_else(|| fallback_grants.to_vec());
    let stable_host_ref = {
        let stable = stable_host_ref_for_node_id(node_id, &grants);
        if stable != node_id.0 {
            stable
        } else {
            stable_host_ref_from_cached_scheduled_groups(node_id, cache).unwrap_or(stable)
        }
    };
    let mut desired_source = std::collections::BTreeSet::new();
    let mut desired_scan = std::collections::BTreeSet::new();
    for signal in signals {
        let SourceControlSignal::Activate {
            unit, bound_scopes, ..
        } = signal
        else {
            continue;
        };
        for scope in bound_scopes {
            let applies_locally = roots
                .iter()
                .filter(|root| match unit {
                    SourceRuntimeUnit::Source => root.watch,
                    SourceRuntimeUnit::Scan => root.scan,
                })
                .any(|root| bound_scope_applies_locally(scope, root, node_id, &grants));
            if !applies_locally {
                continue;
            }
            match unit {
                SourceRuntimeUnit::Source => {
                    desired_source.insert(scope.scope_id.clone());
                }
                SourceRuntimeUnit::Scan => {
                    desired_scan.insert(scope.scope_id.clone());
                }
            }
        }
    }

    let replace_groups = |slot: &mut Option<std::collections::BTreeMap<String, Vec<String>>>,
                          groups: std::collections::BTreeSet<String>| {
        if let Some(groups_by_node) = slot.as_mut() {
            groups_by_node.retain(|host_ref, _| !host_ref_matches_node_id(host_ref, node_id));
            if groups_by_node.is_empty() {
                *slot = None;
            }
        }
        if groups.is_empty() {
            return;
        }
        let groups_by_node = slot.get_or_insert_with(std::collections::BTreeMap::new);
        groups_by_node.insert(
            stable_host_ref.clone(),
            groups.into_iter().collect::<Vec<_>>(),
        );
    };

    replace_groups(
        &mut cache.replay_recovery_scheduled_source_groups_by_node,
        desired_source,
    );
    replace_groups(
        &mut cache.replay_recovery_scheduled_scan_groups_by_node,
        desired_scan,
    );
}

#[test]
fn source_retry_machine_removes_legacy_retry_classifier_helpers() {
    let source = include_str!("source.rs");
    assert!(
        source.contains(concat!("struct ", "SourceRetryMachine")),
        "source retry ownership should stay explicit in SourceRetryMachine",
    );
    assert!(
        !source.contains(concat!("fn ", "classify_source_reconnect_retry")),
        "source retry hard cut should not leave standalone reconnect classifier helpers",
    );
    assert!(
        !source.contains(concat!("fn ", "classify_source_wait_retry")),
        "source retry hard cut should not leave standalone wait classifier helpers",
    );
    assert!(
        !source.contains(concat!(
            "fn ",
            "classify_source_scheduled_groups_refresh_retry_failure"
        )),
        "source retry hard cut should not leave standalone scheduled-groups classifier helpers",
    );
}

#[test]
fn replay_recovery_schedule_priming_respects_scan_only_roots() {
    let mut cache = SourceWorkerSnapshotCache::default();
    let node_id = NodeId("node-a".to_string());
    let root = {
        let mut root = RootSpec::new("nfs1", "/tmp");
        root.watch = false;
        root.scan = true;
        root
    };
    let bound_scopes = vec![RuntimeBoundScope {
        scope_id: "nfs1".to_string(),
        resource_ids: vec!["node-a::nfs1".to_string()],
    }];
    let signals = [
        SourceControlSignal::Activate {
            unit: SourceRuntimeUnit::Source,
            route_key: "source-roots-control:v1.stream".to_string(),
            generation: 2,
            bound_scopes: bound_scopes.clone(),
            envelope: capanix_runtime_entry_sdk::control::encode_runtime_exec_control(
                &capanix_runtime_entry_sdk::control::RuntimeExecControl::Activate(
                    capanix_runtime_entry_sdk::control::RuntimeExecActivate {
                        route_key: "source-roots-control:v1.stream".to_string(),
                        unit_id: SourceRuntimeUnit::Source.unit_id().to_string(),
                        lease: None,
                        generation: 2,
                        expires_at_ms: 1,
                        bound_scopes: bound_scopes.clone(),
                    },
                ),
            )
            .expect("encode source activate"),
        },
        SourceControlSignal::Activate {
            unit: SourceRuntimeUnit::Scan,
            route_key: "source-rescan-control:v1.stream".to_string(),
            generation: 2,
            bound_scopes: bound_scopes.clone(),
            envelope: capanix_runtime_entry_sdk::control::encode_runtime_exec_control(
                &capanix_runtime_entry_sdk::control::RuntimeExecControl::Activate(
                    capanix_runtime_entry_sdk::control::RuntimeExecActivate {
                        route_key: "source-rescan-control:v1.stream".to_string(),
                        unit_id: SourceRuntimeUnit::Scan.unit_id().to_string(),
                        lease: None,
                        generation: 2,
                        expires_at_ms: 1,
                        bound_scopes,
                    },
                ),
            )
            .expect("encode scan activate"),
        },
    ];
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.11".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: Default::default(),
        mount_point: "/tmp".into(),
        fs_source: "/tmp".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];

    prime_cached_schedule_from_control_signals_for_replay_recovery(
        &mut cache,
        &node_id,
        &signals,
        &[root],
        &grants,
    );

    assert!(
        cache
            .replay_recovery_scheduled_source_groups_by_node
            .as_ref()
            .is_none_or(|groups| groups.is_empty()),
        "scan-only roots must not prime replay-recovery source groups: {:?}",
        cache.replay_recovery_scheduled_source_groups_by_node
    );
    assert_eq!(
        cache
            .replay_recovery_scheduled_scan_groups_by_node
            .as_ref()
            .and_then(|groups| groups.get("node-a")),
        Some(&vec!["nfs1".to_string()]),
        "scan-only roots must still prime replay-recovery scan groups",
    );
}

fn prime_cached_control_summary_from_control_signals(
    cache: &mut SourceWorkerSnapshotCache,
    node_id: &NodeId,
    signals: &[SourceControlSignal],
    fallback_grants: &[GrantedMountRoot],
) {
    let summary = summarize_source_control_signals(signals);
    if summary.is_empty() {
        return;
    }
    let grants = cache
        .grants
        .as_ref()
        .cloned()
        .unwrap_or_else(|| fallback_grants.to_vec());
    let stable_host_ref = stable_host_ref_for_node_id(node_id, &grants);
    cache.last_control_frame_signals_by_node = Some(std::collections::BTreeMap::from([(
        stable_host_ref,
        summary,
    )]));
}

fn summarize_bound_scopes(
    bound_scopes: &[capanix_runtime_entry_sdk::control::RuntimeBoundScope],
) -> Vec<String> {
    bound_scopes
        .iter()
        .map(|scope| format!("{}=>{}", scope.scope_id, scope.resource_ids.join("|")))
        .collect()
}

fn summarize_source_control_signals(signals: &[SourceControlSignal]) -> Vec<String> {
    signals
        .iter()
        .map(|signal| match signal {
            SourceControlSignal::Activate {
                unit,
                route_key,
                generation,
                bound_scopes,
                ..
            } => format!(
                "activate unit={} route={} generation={} scopes={:?}",
                unit.unit_id(),
                route_key,
                generation,
                summarize_bound_scopes(bound_scopes)
            ),
            SourceControlSignal::Deactivate {
                unit,
                route_key,
                generation,
                ..
            } => format!(
                "deactivate unit={} route={} generation={}",
                unit.unit_id(),
                route_key,
                generation
            ),
            SourceControlSignal::Tick {
                unit,
                route_key,
                generation,
                ..
            } => format!(
                "tick unit={} route={} generation={}",
                unit.unit_id(),
                route_key,
                generation
            ),
            SourceControlSignal::RuntimeHostGrantChange { .. } => "host_grant_change".into(),
            SourceControlSignal::ManualRescan { .. } => "manual_rescan".into(),
            SourceControlSignal::Passthrough(_) => "passthrough".into(),
        })
        .collect()
}

fn summarize_groups_by_node(
    groups: &std::collections::BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    groups
        .iter()
        .map(|(node_id, groups)| format!("{node_id}={}", groups.join("|")))
        .collect()
}

fn summarize_source_observability_snapshot(snapshot: &SourceObservabilitySnapshot) -> String {
    format!(
        "lifecycle={} primaries={} runners={} source={:?} scan={:?} control={:?} published_batches={:?} published_events={:?} published_origins={:?} published_origin_counts={:?} published_path_target={:?} enqueued_path_counts={:?} pending_path_counts={:?} yielded_path_counts={:?} summarized_path_counts={:?} published_path_counts={:?} degraded={:?}",
        snapshot.lifecycle_state,
        snapshot.source_primary_by_group.len(),
        snapshot.last_force_find_runner_by_group.len(),
        summarize_groups_by_node(&snapshot.scheduled_source_groups_by_node),
        summarize_groups_by_node(&snapshot.scheduled_scan_groups_by_node),
        summarize_groups_by_node(&snapshot.last_control_frame_signals_by_node),
        snapshot.published_batches_by_node,
        snapshot.published_events_by_node,
        summarize_groups_by_node(&snapshot.last_published_origins_by_node),
        summarize_groups_by_node(&snapshot.published_origin_counts_by_node),
        snapshot.published_path_capture_target,
        summarize_groups_by_node(&snapshot.enqueued_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.pending_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.yielded_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.summarized_path_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.published_path_origin_counts_by_node),
        snapshot.status.degraded_roots
    )
}

fn source_observability_snapshot_debug_maps_absent(snapshot: &SourceObservabilitySnapshot) -> bool {
    snapshot.last_force_find_runner_by_group.is_empty()
        && snapshot.force_find_inflight_groups.is_empty()
        && snapshot.scheduled_source_groups_by_node.is_empty()
        && snapshot.scheduled_scan_groups_by_node.is_empty()
        && snapshot.last_control_frame_signals_by_node.is_empty()
        && snapshot
            .published_batches_by_node
            .values()
            .all(|count| *count == 0)
        && snapshot
            .published_events_by_node
            .values()
            .all(|count| *count == 0)
        && snapshot
            .published_control_events_by_node
            .values()
            .all(|count| *count == 0)
        && snapshot
            .published_data_events_by_node
            .values()
            .all(|count| *count == 0)
        && snapshot.last_published_at_us_by_node.is_empty()
        && snapshot.last_published_origins_by_node.is_empty()
        && snapshot.published_origin_counts_by_node.is_empty()
        && snapshot.published_path_capture_target.is_none()
        && snapshot.enqueued_path_origin_counts_by_node.is_empty()
        && snapshot.pending_path_origin_counts_by_node.is_empty()
        && snapshot.yielded_path_origin_counts_by_node.is_empty()
        && snapshot.summarized_path_origin_counts_by_node.is_empty()
        && snapshot.published_path_origin_counts_by_node.is_empty()
}

fn source_logical_root_counts_as_status_readiness(
    root: &crate::source::SourceLogicalRootHealthSnapshot,
) -> bool {
    root.matched_grants > 0
        && root.active_members > 0
        && !root.status.starts_with("waiting_for_root:")
}

fn source_concrete_root_counts_as_status_readiness(
    root: &crate::source::SourceConcreteRootHealthSnapshot,
) -> bool {
    root.active
        && root.scan_enabled
        && root.last_error.is_none()
        && !root.status.starts_with("waiting_for_root:")
}

fn source_status_readiness_groups(
    status: &SourceStatusSnapshot,
) -> std::collections::BTreeSet<String> {
    let degraded_groups = status
        .degraded_roots
        .iter()
        .map(|(group, _)| group.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut groups = status
        .logical_roots
        .iter()
        .filter(|root| source_logical_root_counts_as_status_readiness(root))
        .map(|root| root.root_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    groups.extend(
        status
            .concrete_roots
            .iter()
            .filter(|root| source_concrete_root_counts_as_status_readiness(root))
            .map(|root| root.logical_root_id.clone()),
    );
    for group in degraded_groups {
        groups.remove(&group);
    }
    groups
}

fn source_observability_snapshot_has_configured_status_root_health(
    snapshot: &SourceObservabilitySnapshot,
) -> bool {
    let configured_groups = snapshot
        .logical_roots
        .iter()
        .filter(|root| root.watch || root.scan)
        .map(|root| root.id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    if configured_groups.is_empty() {
        return false;
    }
    configured_groups.is_subset(&source_status_readiness_groups(&snapshot.status))
}

pub(crate) fn source_status_has_active_observability_truth(status: &SourceStatusSnapshot) -> bool {
    status.current_stream_generation.is_some()
        || status
            .logical_roots
            .iter()
            .any(|entry| entry.active_members > 0 || entry.status.eq_ignore_ascii_case("ready"))
        || status.concrete_roots.iter().any(|entry| {
            entry.active
                || entry.current_stream_generation.is_some()
                || entry.emitted_batch_count > 0
                || entry.forwarded_batch_count > 0
                || entry.emitted_event_count > 0
                || entry.forwarded_event_count > 0
        })
}

pub(crate) fn scheduled_groups_by_node_have_recovered_members(
    groups_by_node: &std::collections::BTreeMap<String, Vec<String>>,
) -> bool {
    groups_by_node.values().any(|groups| !groups.is_empty())
}

pub(crate) fn source_observability_has_recovered_schedule_members(
    scheduled_source_groups_by_node: &std::collections::BTreeMap<String, Vec<String>>,
    scheduled_scan_groups_by_node: &std::collections::BTreeMap<String, Vec<String>>,
) -> bool {
    scheduled_groups_by_node_have_recovered_members(scheduled_source_groups_by_node)
        || scheduled_groups_by_node_have_recovered_members(scheduled_scan_groups_by_node)
}

pub(crate) fn source_observability_has_recovered_active_state(
    status: &SourceStatusSnapshot,
    last_force_find_runner_by_group: &std::collections::BTreeMap<String, String>,
    force_find_inflight_groups: &[String],
    scheduled_source_groups_by_node: &std::collections::BTreeMap<String, Vec<String>>,
    scheduled_scan_groups_by_node: &std::collections::BTreeMap<String, Vec<String>>,
) -> bool {
    source_status_has_active_observability_truth(status)
        || !last_force_find_runner_by_group.is_empty()
        || !force_find_inflight_groups.is_empty()
        || source_observability_has_recovered_schedule_members(
            scheduled_source_groups_by_node,
            scheduled_scan_groups_by_node,
        )
}

pub(crate) fn source_observability_recovered_control_summary_allowed(
    has_recovered_active_state: bool,
    scheduled_source_groups_by_node: &std::collections::BTreeMap<String, Vec<String>>,
    scheduled_scan_groups_by_node: &std::collections::BTreeMap<String, Vec<String>>,
) -> bool {
    let has_recovered_schedule_members = source_observability_has_recovered_schedule_members(
        scheduled_source_groups_by_node,
        scheduled_scan_groups_by_node,
    );
    let has_explicit_empty_recovered_schedule = (!scheduled_source_groups_by_node.is_empty()
        || !scheduled_scan_groups_by_node.is_empty())
        && !has_recovered_schedule_members;
    has_recovered_active_state && !has_explicit_empty_recovered_schedule
}

#[derive(Clone, Debug)]
pub(crate) struct SourceObservabilityRecoveryProjection {
    pub stable_host_ref: String,
    pub scheduled_source_groups_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub scheduled_scan_groups_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub allow_recovered_control_summary: bool,
}

impl SourceObservabilityRecoveryProjection {
    pub(crate) fn allow_control_summary(&self, has_local_source_grants: bool) -> bool {
        self.allow_recovered_control_summary || has_local_source_grants
    }
}

fn resolved_source_observability_schedule_groups_by_node(
    stable_host_ref: &str,
    status: &SourceStatusSnapshot,
    explicit_groups_by_node: std::collections::BTreeMap<String, Vec<String>>,
    select_group: impl Fn(&crate::source::SourceConcreteRootHealthSnapshot) -> bool,
) -> std::collections::BTreeMap<String, Vec<String>> {
    if explicit_groups_by_node.is_empty() {
        recovered_scheduled_groups_by_node_from_active_status(stable_host_ref, status, select_group)
    } else {
        explicit_groups_by_node
    }
}

pub(crate) fn source_observability_recovery_projection(
    stable_host_ref: impl Into<String>,
    status: &SourceStatusSnapshot,
    last_force_find_runner_by_group: &std::collections::BTreeMap<String, String>,
    force_find_inflight_groups: &[String],
    explicit_scheduled_source_groups_by_node: std::collections::BTreeMap<String, Vec<String>>,
    explicit_scheduled_scan_groups_by_node: std::collections::BTreeMap<String, Vec<String>>,
) -> SourceObservabilityRecoveryProjection {
    let stable_host_ref = stable_host_ref.into();
    let scheduled_source_groups_by_node = resolved_source_observability_schedule_groups_by_node(
        &stable_host_ref,
        status,
        explicit_scheduled_source_groups_by_node,
        |entry| entry.watch_enabled,
    );
    let scheduled_scan_groups_by_node = resolved_source_observability_schedule_groups_by_node(
        &stable_host_ref,
        status,
        explicit_scheduled_scan_groups_by_node,
        |entry| entry.scan_enabled,
    );
    let has_recovered_active_state = source_observability_has_recovered_active_state(
        status,
        last_force_find_runner_by_group,
        force_find_inflight_groups,
        &scheduled_source_groups_by_node,
        &scheduled_scan_groups_by_node,
    );
    let allow_recovered_control_summary = source_observability_recovered_control_summary_allowed(
        has_recovered_active_state,
        &scheduled_source_groups_by_node,
        &scheduled_scan_groups_by_node,
    );
    SourceObservabilityRecoveryProjection {
        stable_host_ref,
        scheduled_source_groups_by_node,
        scheduled_scan_groups_by_node,
        allow_recovered_control_summary,
    }
}

pub(crate) fn recovered_control_signals_by_node_from_active_status(
    stable_host_ref: &str,
    status: &SourceStatusSnapshot,
    scheduled_source_groups_by_node: &std::collections::BTreeMap<String, Vec<String>>,
    scheduled_scan_groups_by_node: &std::collections::BTreeMap<String, Vec<String>>,
) -> std::collections::BTreeMap<String, Vec<String>> {
    let projection = SourceObservabilityRecoveryProjection {
        stable_host_ref: stable_host_ref.to_string(),
        scheduled_source_groups_by_node: scheduled_source_groups_by_node.clone(),
        scheduled_scan_groups_by_node: scheduled_scan_groups_by_node.clone(),
        allow_recovered_control_summary: source_status_has_active_observability_truth(status),
    };
    recovered_control_signals_by_node_from_projection(&projection, status)
}

fn source_observability_snapshot_has_active_state(snapshot: &SourceObservabilitySnapshot) -> bool {
    source_observability_has_recovered_active_state(
        &snapshot.status,
        &snapshot.last_force_find_runner_by_group,
        &snapshot.force_find_inflight_groups,
        &snapshot.scheduled_source_groups_by_node,
        &snapshot.scheduled_scan_groups_by_node,
    ) || !snapshot.scheduled_source_groups_by_node.is_empty()
        || !snapshot.scheduled_scan_groups_by_node.is_empty()
        || !snapshot.last_control_frame_signals_by_node.is_empty()
}

fn source_status_publication_marker(status: &SourceStatusSnapshot) -> (u64, u64) {
    let published_batches = status
        .concrete_roots
        .iter()
        .map(|entry| entry.forwarded_batch_count)
        .sum::<u64>();
    let last_published_at_us = status
        .concrete_roots
        .iter()
        .filter_map(|entry| entry.last_forwarded_at_us.or(entry.last_emitted_at_us))
        .max()
        .unwrap_or_default();
    (published_batches, last_published_at_us)
}

fn source_status_rescan_completion_marker(status: &SourceStatusSnapshot) -> u64 {
    status
        .concrete_roots
        .iter()
        .filter_map(|entry| entry.last_audit_completed_at_us)
        .max()
        .unwrap_or_default()
}

fn source_observability_publication_marker(snapshot: &SourceObservabilitySnapshot) -> (u64, u64) {
    let (status_published_batches, status_last_published_at_us) =
        source_status_publication_marker(&snapshot.status);
    let snapshot_published_batches = snapshot
        .published_batches_by_node
        .values()
        .copied()
        .sum::<u64>();
    let snapshot_last_published_at_us = snapshot
        .last_published_at_us_by_node
        .values()
        .copied()
        .max()
        .unwrap_or_default();
    (
        std::cmp::max(status_published_batches, snapshot_published_batches),
        std::cmp::max(status_last_published_at_us, snapshot_last_published_at_us),
    )
}

fn cached_source_publication_marker(cache: &SourceWorkerSnapshotCache) -> (u64, u64) {
    let (status_published_batches, status_last_published_at_us) = cache
        .status
        .as_ref()
        .map(source_status_publication_marker)
        .unwrap_or_default();
    let cached_published_batches = cache
        .published_batches_by_node
        .as_ref()
        .map(|counts| counts.values().copied().sum::<u64>())
        .unwrap_or_default();
    let cached_last_published_at_us = cache
        .last_published_at_us_by_node
        .as_ref()
        .and_then(|counts| counts.values().copied().max())
        .unwrap_or_default();
    (
        std::cmp::max(status_published_batches, cached_published_batches),
        std::cmp::max(status_last_published_at_us, cached_last_published_at_us),
    )
}

fn source_observability_snapshot_can_preserve_cached_observability(
    snapshot: &SourceObservabilitySnapshot,
) -> bool {
    source_observability_snapshot_has_active_state(snapshot)
        && !source_observability_snapshot_debug_maps_absent(snapshot)
}

fn should_preserve_cached_observability_map<K: Ord, V>(
    can_preserve: bool,
    current: &std::collections::BTreeMap<K, V>,
    cached: Option<&std::collections::BTreeMap<K, V>>,
) -> bool {
    can_preserve && current.is_empty() && cached.is_some_and(|entries| !entries.is_empty())
}

fn should_preserve_cached_observability_option<T>(
    can_preserve: bool,
    current: &Option<T>,
    cached: &Option<T>,
) -> bool {
    can_preserve && current.is_none() && cached.is_some()
}

fn explicit_zero_published_counter_nodes(
    snapshot: &SourceObservabilitySnapshot,
) -> std::collections::BTreeSet<String> {
    let mut nodes = std::collections::BTreeSet::new();
    for node_id in snapshot.published_batches_by_node.keys() {
        nodes.insert(node_id.clone());
    }
    for node_id in snapshot.published_events_by_node.keys() {
        nodes.insert(node_id.clone());
    }
    for node_id in snapshot.published_control_events_by_node.keys() {
        nodes.insert(node_id.clone());
    }
    for node_id in snapshot.published_data_events_by_node.keys() {
        nodes.insert(node_id.clone());
    }
    nodes.retain(|node_id| {
        let mut saw_counter = false;
        for counter in [
            snapshot.published_batches_by_node.get(node_id),
            snapshot.published_events_by_node.get(node_id),
            snapshot.published_control_events_by_node.get(node_id),
            snapshot.published_data_events_by_node.get(node_id),
        ]
        .into_iter()
        .flatten()
        {
            saw_counter = true;
            if *counter != 0 {
                return false;
            }
        }
        saw_counter
    });
    nodes
}

fn merge_cached_observability_node_map<V: Clone>(
    can_preserve: bool,
    current: &std::collections::BTreeMap<String, V>,
    cached: Option<&std::collections::BTreeMap<String, V>>,
    explicit_clear_nodes: &std::collections::BTreeSet<String>,
) -> std::collections::BTreeMap<String, V> {
    let mut merged = if should_preserve_cached_observability_map(can_preserve, current, cached) {
        cached.cloned().unwrap_or_default()
    } else {
        current.clone()
    };
    for node_id in explicit_clear_nodes {
        merged.remove(node_id);
    }
    merged
}

fn merge_cached_observability_option<T: Clone>(
    can_preserve: bool,
    current: &Option<T>,
    cached: &Option<T>,
    explicit_clear: bool,
) -> Option<T> {
    if explicit_clear {
        None
    } else if should_preserve_cached_observability_option(can_preserve, current, cached) {
        cached.clone()
    } else {
        current.clone()
    }
}

fn apply_observability_snapshot_to_cache(
    cache: &mut SourceWorkerSnapshotCache,
    snapshot: &SourceObservabilitySnapshot,
) {
    let can_preserve_omitted_observability =
        source_observability_snapshot_can_preserve_cached_observability(snapshot);
    let explicit_zero_published_nodes = explicit_zero_published_counter_nodes(snapshot);
    let preserve_last_scheduled_source_groups = should_preserve_cached_observability_map(
        can_preserve_omitted_observability,
        &snapshot.scheduled_source_groups_by_node,
        cache.scheduled_source_groups_by_node.as_ref(),
    );
    let preserve_last_scheduled_scan_groups = should_preserve_cached_observability_map(
        can_preserve_omitted_observability,
        &snapshot.scheduled_scan_groups_by_node,
        cache.scheduled_scan_groups_by_node.as_ref(),
    );
    let preserve_last_control_summary = should_preserve_cached_observability_map(
        can_preserve_omitted_observability,
        &snapshot.last_control_frame_signals_by_node,
        cache.last_control_frame_signals_by_node.as_ref(),
    );
    let preserve_last_published_batches = should_preserve_cached_observability_map(
        can_preserve_omitted_observability,
        &snapshot.published_batches_by_node,
        cache.published_batches_by_node.as_ref(),
    );
    let preserve_last_published_events = should_preserve_cached_observability_map(
        can_preserve_omitted_observability,
        &snapshot.published_events_by_node,
        cache.published_events_by_node.as_ref(),
    );
    let preserve_last_published_control_events = should_preserve_cached_observability_map(
        can_preserve_omitted_observability,
        &snapshot.published_control_events_by_node,
        cache.published_control_events_by_node.as_ref(),
    );
    let preserve_last_published_data_events = should_preserve_cached_observability_map(
        can_preserve_omitted_observability,
        &snapshot.published_data_events_by_node,
        cache.published_data_events_by_node.as_ref(),
    );
    cache.lifecycle_state = Some(snapshot.lifecycle_state.clone());
    cache.last_live_observability_snapshot_at = Some(Instant::now());
    cache.host_object_grants_version = Some(snapshot.host_object_grants_version);
    cache.grants = Some(snapshot.grants.clone());
    cache.logical_roots = Some(snapshot.logical_roots.clone());
    cache.status = Some(snapshot.status.clone());
    cache.source_primary_by_group = Some(snapshot.source_primary_by_group.clone());
    cache.last_force_find_runner_by_group = Some(snapshot.last_force_find_runner_by_group.clone());
    cache.force_find_inflight_groups = Some(snapshot.force_find_inflight_groups.clone());
    if !preserve_last_scheduled_source_groups {
        cache.scheduled_source_groups_by_node =
            Some(snapshot.scheduled_source_groups_by_node.clone());
    }
    if !preserve_last_scheduled_scan_groups {
        cache.scheduled_scan_groups_by_node = Some(snapshot.scheduled_scan_groups_by_node.clone());
    }
    if !preserve_last_control_summary {
        cache.last_control_frame_signals_by_node =
            Some(snapshot.last_control_frame_signals_by_node.clone());
    }
    if !preserve_last_published_batches {
        cache.published_batches_by_node = Some(snapshot.published_batches_by_node.clone());
    }
    if !preserve_last_published_events {
        cache.published_events_by_node = Some(snapshot.published_events_by_node.clone());
    }
    if !preserve_last_published_control_events {
        cache.published_control_events_by_node =
            Some(snapshot.published_control_events_by_node.clone());
    }
    if !preserve_last_published_data_events {
        cache.published_data_events_by_node = Some(snapshot.published_data_events_by_node.clone());
    }
    cache.last_published_at_us_by_node = Some(merge_cached_observability_node_map(
        can_preserve_omitted_observability,
        &snapshot.last_published_at_us_by_node,
        cache.last_published_at_us_by_node.as_ref(),
        &explicit_zero_published_nodes,
    ));
    cache.last_published_origins_by_node = Some(merge_cached_observability_node_map(
        can_preserve_omitted_observability,
        &snapshot.last_published_origins_by_node,
        cache.last_published_origins_by_node.as_ref(),
        &explicit_zero_published_nodes,
    ));
    cache.published_origin_counts_by_node = Some(merge_cached_observability_node_map(
        can_preserve_omitted_observability,
        &snapshot.published_origin_counts_by_node,
        cache.published_origin_counts_by_node.as_ref(),
        &explicit_zero_published_nodes,
    ));
    cache.published_path_capture_target = merge_cached_observability_option(
        can_preserve_omitted_observability,
        &snapshot.published_path_capture_target,
        &cache.published_path_capture_target,
        !explicit_zero_published_nodes.is_empty(),
    );
    cache.enqueued_path_origin_counts_by_node = Some(merge_cached_observability_node_map(
        can_preserve_omitted_observability,
        &snapshot.enqueued_path_origin_counts_by_node,
        cache.enqueued_path_origin_counts_by_node.as_ref(),
        &explicit_zero_published_nodes,
    ));
    cache.pending_path_origin_counts_by_node = Some(merge_cached_observability_node_map(
        can_preserve_omitted_observability,
        &snapshot.pending_path_origin_counts_by_node,
        cache.pending_path_origin_counts_by_node.as_ref(),
        &explicit_zero_published_nodes,
    ));
    cache.yielded_path_origin_counts_by_node = Some(merge_cached_observability_node_map(
        can_preserve_omitted_observability,
        &snapshot.yielded_path_origin_counts_by_node,
        cache.yielded_path_origin_counts_by_node.as_ref(),
        &explicit_zero_published_nodes,
    ));
    cache.summarized_path_origin_counts_by_node = Some(merge_cached_observability_node_map(
        can_preserve_omitted_observability,
        &snapshot.summarized_path_origin_counts_by_node,
        cache.summarized_path_origin_counts_by_node.as_ref(),
        &explicit_zero_published_nodes,
    ));
    cache.published_path_origin_counts_by_node = Some(merge_cached_observability_node_map(
        can_preserve_omitted_observability,
        &snapshot.published_path_origin_counts_by_node,
        cache.published_path_origin_counts_by_node.as_ref(),
        &explicit_zero_published_nodes,
    ));
}

fn merge_live_observability_snapshot_with_recent_cache(
    cache: &SourceWorkerSnapshotCache,
    snapshot: &SourceObservabilitySnapshot,
) -> SourceObservabilitySnapshot {
    let mut merged_cache = cache.clone();
    apply_observability_snapshot_to_cache(&mut merged_cache, snapshot);
    build_cached_worker_observability_snapshot(&merged_cache).unwrap_or_else(|| snapshot.clone())
}

fn recent_cached_source_observability_snapshot_is_incomplete(
    snapshot: &SourceObservabilitySnapshot,
) -> bool {
    source_observability_snapshot_has_active_state(snapshot)
        && !source_observability_snapshot_can_preserve_cached_observability(snapshot)
}

fn fail_close_incomplete_active_source_observability_snapshot(
    snapshot: &mut SourceObservabilitySnapshot,
) {
    if !recent_cached_source_observability_snapshot_is_incomplete(snapshot) {
        return;
    }
    snapshot.status.current_stream_generation = None;
    snapshot.status.logical_roots.clear();
    snapshot.status.concrete_roots.clear();
}

pub(crate) fn control_signals_by_node(
    node_key: &str,
    signals: &[String],
) -> std::collections::BTreeMap<String, Vec<String>> {
    if signals.is_empty() {
        return std::collections::BTreeMap::new();
    }
    std::collections::BTreeMap::from([(node_key.to_string(), signals.to_vec())])
}

fn debug_source_status_lifecycle_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FSMETA_DEBUG_SOURCE_STATUS_LIFECYCLE")
            .ok()
            .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
    })
}

fn next_source_status_trace_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

struct SourceStatusTraceGuard {
    node_id: String,
    trace_id: u64,
    phase: &'static str,
    completed: bool,
}

impl SourceStatusTraceGuard {
    fn new(node_id: String, trace_id: u64, phase: &'static str) -> Self {
        Self {
            node_id,
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

impl Drop for SourceStatusTraceGuard {
    fn drop(&mut self) {
        if debug_source_status_lifecycle_enabled() && !self.completed {
            eprintln!(
                "fs_meta_source_worker_client: observability_snapshot dropped node={} trace_id={} phase={}",
                self.node_id, self.trace_id, self.phase
            );
        }
    }
}

#[derive(Debug, Clone, Default)]
struct SourceWorkerSnapshotCache {
    lifecycle_state: Option<String>,
    last_live_observability_snapshot_at: Option<Instant>,
    host_object_grants_version: Option<u64>,
    grants: Option<Vec<GrantedMountRoot>>,
    logical_roots: Option<Vec<RootSpec>>,
    status: Option<SourceStatusSnapshot>,
    source_primary_by_group: Option<std::collections::BTreeMap<String, String>>,
    last_force_find_runner_by_group: Option<std::collections::BTreeMap<String, String>>,
    force_find_inflight_groups: Option<Vec<String>>,
    scheduled_source_groups_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    scheduled_scan_groups_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    replay_recovery_scheduled_source_groups_by_node:
        Option<std::collections::BTreeMap<String, Vec<String>>>,
    replay_recovery_scheduled_scan_groups_by_node:
        Option<std::collections::BTreeMap<String, Vec<String>>>,
    last_control_frame_signals_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    observability_control_summary_override_by_node:
        Option<std::collections::BTreeMap<String, Vec<String>>>,
    published_batches_by_node: Option<std::collections::BTreeMap<String, u64>>,
    published_events_by_node: Option<std::collections::BTreeMap<String, u64>>,
    published_control_events_by_node: Option<std::collections::BTreeMap<String, u64>>,
    published_data_events_by_node: Option<std::collections::BTreeMap<String, u64>>,
    last_published_at_us_by_node: Option<std::collections::BTreeMap<String, u64>>,
    last_published_origins_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    published_origin_counts_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    published_path_capture_target: Option<String>,
    enqueued_path_origin_counts_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    pending_path_origin_counts_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    yielded_path_origin_counts_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    summarized_path_origin_counts_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    published_path_origin_counts_by_node: Option<std::collections::BTreeMap<String, Vec<String>>>,
    rescan_request_epoch: u64,
    rescan_request_published_batches: u64,
    rescan_request_last_published_at_us: u64,
    rescan_request_last_audit_completed_at_us: u64,
    rescan_observed_epoch: u64,
}

#[derive(Clone)]
pub struct SourceWorkerClientHandle {
    node_id: NodeId,
    config: SourceConfig,
    worker_factory: RuntimeWorkerClientFactory,
    worker_binding: RuntimeWorkerBinding,
    _shared: Arc<SharedSourceWorkerHandleState>,
    manual_rescan_signal: SignalCell,
    cache: Arc<Mutex<SourceWorkerSnapshotCache>>,
    control_state: Arc<tokio::sync::Mutex<SourceControlState>>,
    start_serial: Arc<tokio::sync::Mutex<()>>,
    control_ops_inflight: Arc<AtomicUsize>,
    control_ops_state_tx: tokio::sync::watch::Sender<usize>,
    control_ops_state: tokio::sync::watch::Receiver<usize>,
    source_progress_state_current: Arc<Mutex<SourceProgressState>>,
    source_progress_state_tx: tokio::sync::watch::Sender<SourceProgressState>,
    source_progress_state: tokio::sync::watch::Receiver<SourceProgressState>,
    control_ops_serial: Arc<tokio::sync::Mutex<()>>,
    observability_read_serial: Arc<tokio::sync::Mutex<()>>,
}

struct InflightControlOpGuard {
    counter: Arc<AtomicUsize>,
    state: tokio::sync::watch::Sender<usize>,
}

impl Drop for InflightControlOpGuard {
    fn drop(&mut self) {
        let next = self
            .counter
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        let _ = self.state.send_replace(next);
    }
}

struct SharedSourceWorkerHandleState {
    worker: tokio::sync::Mutex<SharedSourceWorkerClient>,
    cache: Arc<Mutex<SourceWorkerSnapshotCache>>,
    control_state: Arc<tokio::sync::Mutex<SourceControlState>>,
    start_serial: Arc<tokio::sync::Mutex<()>>,
    control_ops_inflight: Arc<AtomicUsize>,
    control_ops_state: tokio::sync::watch::Sender<usize>,
    source_progress_state_current: Arc<Mutex<SourceProgressState>>,
    source_progress_state: tokio::sync::watch::Sender<SourceProgressState>,
    control_ops_serial: Arc<tokio::sync::Mutex<()>>,
    observability_read_serial: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceScheduledGroupsRefreshRecoveryObservation {
    NoLiveWorkerRecovery,
    RecoveredLiveWorker,
}

impl SourceScheduledGroupsRefreshRecoveryObservation {
    const fn as_atomic_u8(self) -> u8 {
        match self {
            Self::NoLiveWorkerRecovery => 0,
            Self::RecoveredLiveWorker => 1,
        }
    }

    const fn from_atomic_u8(raw: u8) -> Self {
        match raw {
            1 => Self::RecoveredLiveWorker,
            _ => Self::NoLiveWorkerRecovery,
        }
    }
}

struct SharedSourceWorkerClient {
    instance_id: u64,
    client: Arc<TypedRuntimeWorkerClient<SourceWorkerRpc, SourceConfig>>,
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceControlReplayState {
    #[default]
    Clear,
    Required,
}

impl SourceControlReplayState {
    const fn refresh_disposition(self) -> SourceScheduledGroupsRefreshDisposition {
        match self {
            Self::Clear => SourceScheduledGroupsRefreshDisposition::OrdinaryScheduleRefresh,
            Self::Required => SourceScheduledGroupsRefreshDisposition::ReplayRequiredRecovery,
        }
    }
}

#[derive(Default, Clone)]
pub(crate) struct SourceControlState {
    pub(crate) replay_state: SourceControlReplayState,
    pub(crate) latest_host_grant_change: Option<SourceControlSignal>,
    pub(crate) active_by_route: std::collections::BTreeMap<(String, String), SourceControlSignal>,
}

impl SourceControlState {
    fn replay_state(&self) -> SourceControlReplayState {
        self.replay_state
    }

    fn arm_replay(&mut self) {
        self.replay_state = SourceControlReplayState::Required;
    }

    fn take_replay_state(&mut self) -> SourceControlReplayState {
        let replay_state = self.replay_state;
        if matches!(replay_state, SourceControlReplayState::Required) {
            self.replay_state = SourceControlReplayState::Clear;
        }
        replay_state
    }

    pub(crate) fn retain_signals(&mut self, signals: &[SourceControlSignal]) {
        for signal in signals {
            match signal {
                SourceControlSignal::RuntimeHostGrantChange { .. } => {
                    self.latest_host_grant_change = Some(signal.clone());
                }
                SourceControlSignal::Activate {
                    unit, route_key, ..
                }
                | SourceControlSignal::Deactivate {
                    unit, route_key, ..
                } => {
                    self.active_by_route.insert(
                        (unit.unit_id().to_string(), route_key.clone()),
                        signal.clone(),
                    );
                }
                SourceControlSignal::Tick { .. }
                | SourceControlSignal::ManualRescan { .. }
                | SourceControlSignal::Passthrough(_) => {}
            }
        }
    }

    fn matches_generation_one_activate_wave(&self, signals: &[SourceControlSignal]) -> bool {
        if !matches!(
            SourceControlFrameSignalPolicies::from_signals(signals).timeout_reset_policy,
            SourceControlFrameTimeoutResetPolicy::GenerationOneActivateWave
        ) {
            return false;
        }
        !self.active_by_route.is_empty()
            && signals.iter().all(|signal| match signal {
                SourceControlSignal::Activate {
                    unit,
                    route_key,
                    bound_scopes,
                    ..
                } => self
                    .active_by_route
                    .get(&(unit.unit_id().to_string(), route_key.clone()))
                    .is_some_and(|retained_signal| match retained_signal {
                        SourceControlSignal::Activate {
                            generation,
                            bound_scopes: retained_bound_scopes,
                            ..
                        } => *generation == 1 && retained_bound_scopes == bound_scopes,
                        _ => false,
                    }),
                _ => false,
            })
    }

    fn classify_tick_only_wave(
        &self,
        signals: &[SourceControlSignal],
    ) -> Option<SourceControlFrameTickOnlyDisposition> {
        if signals.is_empty()
            || !signals
                .iter()
                .all(|signal| matches!(signal, SourceControlSignal::Tick { .. }))
        {
            return None;
        }
        if self.active_by_route.is_empty() {
            return None;
        }
        let mut current_or_forward_ticks = Vec::new();
        for signal in signals {
            let SourceControlSignal::Tick {
                unit,
                route_key,
                generation,
                ..
            } = signal
            else {
                return None;
            };
            let Some(retained_signal) = self
                .active_by_route
                .get(&(unit.unit_id().to_string(), route_key.clone()))
            else {
                return None;
            };
            let SourceControlSignal::Activate {
                generation: retained_generation,
                ..
            } = retained_signal
            else {
                return None;
            };
            if generation >= retained_generation {
                current_or_forward_ticks.push(signal.clone());
            }
        }
        if current_or_forward_ticks.is_empty() {
            return Some(
                SourceControlFrameTickOnlyDisposition::RetainedTickFastPath {
                    signals: Vec::new(),
                },
            );
        }
        if matches!(self.replay_state(), SourceControlReplayState::Required) {
            Some(SourceControlFrameTickOnlyDisposition::RetainedTickReplay)
        } else {
            Some(
                SourceControlFrameTickOnlyDisposition::RetainedTickFastPath {
                    signals: current_or_forward_ticks,
                },
            )
        }
    }

    fn replay_envelopes(&self) -> Vec<ControlEnvelope> {
        let mut envelopes = Vec::new();
        if let Some(host_grant_change) = self.latest_host_grant_change.as_ref() {
            envelopes.push(host_grant_change.envelope());
        }
        envelopes.extend(
            self.active_by_route
                .values()
                .map(SourceControlSignal::envelope),
        );
        envelopes
    }

    fn merge_replay_envelopes_for_next_wave(
        &self,
        incoming: &[ControlEnvelope],
    ) -> Vec<ControlEnvelope> {
        if !matches!(self.replay_state, SourceControlReplayState::Required) {
            return incoming.to_vec();
        }
        let mut envelopes = self.replay_envelopes();
        envelopes.extend(incoming.iter().cloned());
        envelopes
    }

    pub(crate) fn replay_signals(&self) -> Vec<SourceControlSignal> {
        let mut signals = Vec::new();
        if let Some(host_grant_change) = self.latest_host_grant_change.as_ref() {
            signals.push(host_grant_change.clone());
        }
        signals.extend(self.active_by_route.values().cloned());
        signals
    }
}

fn next_shared_source_worker_instance_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn source_worker_handle_registry_key(
    node_id: &NodeId,
    worker_binding: &RuntimeWorkerBinding,
    worker_factory: &RuntimeWorkerClientFactory,
) -> String {
    let runtime_boundary_id = {
        let io_boundary = worker_factory.io_boundary();
        Arc::as_ptr(&io_boundary) as *const () as usize
    };
    format!(
        "{}|{}|{:?}|{:?}|{}",
        node_id.0,
        worker_binding.role_id,
        worker_binding.mode,
        worker_binding.launcher_kind,
        runtime_boundary_id
    )
}

fn source_worker_handle_registry()
-> &'static Mutex<std::collections::BTreeMap<String, Weak<SharedSourceWorkerHandleState>>> {
    static REGISTRY: std::sync::OnceLock<
        Mutex<std::collections::BTreeMap<String, Weak<SharedSourceWorkerHandleState>>>,
    > = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(std::collections::BTreeMap::new()))
}

fn lock_source_worker_handle_registry() -> std::sync::MutexGuard<
    'static,
    std::collections::BTreeMap<String, Weak<SharedSourceWorkerHandleState>>,
> {
    match source_worker_handle_registry().lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            log::warn!(
                "source worker handle registry lock poisoned; recovering shared handle state"
            );
            poisoned.into_inner()
        }
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerCloseHook {
    pub entered: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerUpdateRootsHook {
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerUpdateRootsErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerLogicalRootsSnapshotHook {
    pub roots: Option<Vec<RootSpec>>,
    pub entered: Option<Arc<tokio::sync::Notify>>,
    pub release: Option<Arc<tokio::sync::Notify>>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerControlFrameErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
pub(crate) struct SourceWorkerControlFrameErrorQueueHook {
    pub errs: std::collections::VecDeque<CnxError>,
    pub sticky_worker_instance_id: Option<u64>,
    pub sticky_peer_err: Option<String>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerStartErrorQueueHook {
    pub errs: std::collections::VecDeque<CnxError>,
    pub sticky_worker_instance_id: Option<u64>,
    pub sticky_peer_err: Option<String>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerStatusErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
pub(crate) struct SourceWorkerObservabilityErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
pub(crate) struct SourceWorkerObservabilityDelayHook {
    pub delay: Duration,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerObservabilityCallCountHook {
    pub count: Arc<AtomicUsize>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerTriggerRescanWhenReadyCallCountHook {
    pub count: Arc<AtomicUsize>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerScheduledGroupsErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerControlFrameHook {
    pub entered: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerControlFramePauseHook {
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SourceWorkerStartPauseHook {
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerStartDelayHook {
    pub delays: std::collections::VecDeque<Duration>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerScheduledGroupsRefreshQueueHook {
    pub replies: std::collections::VecDeque<(
        Option<std::collections::BTreeSet<String>>,
        Option<std::collections::BTreeSet<String>>,
    )>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerScheduledGroupsRefreshErrorQueueHook {
    pub errs: std::collections::VecDeque<CnxError>,
    pub sticky_worker_instance_id: Option<u64>,
    pub sticky_peer_err: Option<String>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerScheduledGroupsRefreshDelayHook {
    pub delays: std::collections::VecDeque<Duration>,
    pub sticky_worker_instance_id: Option<u64>,
}

#[cfg(test)]
pub(crate) struct SourceWorkerForceFindErrorQueueHook {
    pub errs: std::collections::VecDeque<CnxError>,
    pub sticky_worker_instance_id: Option<u64>,
    pub sticky_peer_err: Option<String>,
}

#[cfg(test)]
fn source_worker_close_hook_cell() -> &'static Mutex<Option<SourceWorkerCloseHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerCloseHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_update_roots_hook_cell() -> &'static Mutex<Option<SourceWorkerUpdateRootsHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerUpdateRootsHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_control_frame_hook_cell() -> &'static Mutex<Option<SourceWorkerControlFrameHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerControlFrameHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_control_frame_pause_hook_cell()
-> &'static Mutex<Option<SourceWorkerControlFramePauseHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerControlFramePauseHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_update_roots_error_hook_cell()
-> &'static Mutex<Option<SourceWorkerUpdateRootsErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerUpdateRootsErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_logical_roots_snapshot_hook_cell()
-> &'static Mutex<Option<SourceWorkerLogicalRootsSnapshotHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerLogicalRootsSnapshotHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_control_frame_error_hook_cell()
-> &'static Mutex<Option<SourceWorkerControlFrameErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerControlFrameErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_control_frame_error_queue_hook_cell()
-> &'static Mutex<Option<SourceWorkerControlFrameErrorQueueHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerControlFrameErrorQueueHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_status_error_hook_cell() -> &'static Mutex<Option<SourceWorkerStatusErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerStatusErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_observability_error_hook_cell()
-> &'static Mutex<Option<SourceWorkerObservabilityErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerObservabilityErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_observability_delay_hook_cell()
-> &'static Mutex<Option<SourceWorkerObservabilityDelayHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerObservabilityDelayHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_observability_call_count_hook_cell()
-> &'static Mutex<Option<SourceWorkerObservabilityCallCountHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerObservabilityCallCountHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_trigger_rescan_when_ready_call_count_hook_cell()
-> &'static Mutex<Option<SourceWorkerTriggerRescanWhenReadyCallCountHook>> {
    static CELL: std::sync::OnceLock<
        Mutex<Option<SourceWorkerTriggerRescanWhenReadyCallCountHook>>,
    > = std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_scheduled_groups_error_hook_cell()
-> &'static Mutex<Option<SourceWorkerScheduledGroupsErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerScheduledGroupsErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_start_error_queue_hook_cell()
-> &'static Mutex<Option<SourceWorkerStartErrorQueueHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerStartErrorQueueHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_start_pause_hook_cell() -> &'static Mutex<Option<SourceWorkerStartPauseHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerStartPauseHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_start_delay_hook_cell() -> &'static Mutex<Option<SourceWorkerStartDelayHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerStartDelayHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_scheduled_groups_refresh_queue_hook_cell()
-> &'static Mutex<Option<SourceWorkerScheduledGroupsRefreshQueueHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerScheduledGroupsRefreshQueueHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_scheduled_groups_refresh_error_queue_hook_cell()
-> &'static Mutex<Option<SourceWorkerScheduledGroupsRefreshErrorQueueHook>> {
    static CELL: std::sync::OnceLock<
        Mutex<Option<SourceWorkerScheduledGroupsRefreshErrorQueueHook>>,
    > = std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_scheduled_groups_refresh_delay_hook_cell()
-> &'static Mutex<Option<SourceWorkerScheduledGroupsRefreshDelayHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerScheduledGroupsRefreshDelayHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn source_worker_force_find_error_queue_hook_cell()
-> &'static Mutex<Option<SourceWorkerForceFindErrorQueueHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SourceWorkerForceFindErrorQueueHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub(crate) fn install_source_worker_close_hook(hook: SourceWorkerCloseHook) {
    let mut guard = match source_worker_close_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_control_frame_hook(hook: SourceWorkerControlFrameHook) {
    let mut guard = match source_worker_control_frame_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_control_frame_pause_hook(
    hook: SourceWorkerControlFramePauseHook,
) {
    let mut guard = match source_worker_control_frame_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_start_pause_hook(hook: SourceWorkerStartPauseHook) {
    let mut guard = match source_worker_start_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_start_delay_hook(hook: SourceWorkerStartDelayHook) {
    let mut guard = match source_worker_start_delay_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_scheduled_groups_refresh_queue_hook(
    hook: SourceWorkerScheduledGroupsRefreshQueueHook,
) {
    let mut guard = match source_worker_scheduled_groups_refresh_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_scheduled_groups_refresh_error_queue_hook(
    hook: SourceWorkerScheduledGroupsRefreshErrorQueueHook,
) {
    let mut guard = match source_worker_scheduled_groups_refresh_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_scheduled_groups_refresh_delay_hook(
    hook: SourceWorkerScheduledGroupsRefreshDelayHook,
) {
    let mut guard = match source_worker_scheduled_groups_refresh_delay_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_force_find_error_queue_hook(
    hook: SourceWorkerForceFindErrorQueueHook,
) {
    let mut guard = match source_worker_force_find_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn clear_source_worker_control_frame_hook() {
    let mut guard = match source_worker_control_frame_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_control_frame_pause_hook() {
    let mut guard = match source_worker_control_frame_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_control_frame_error_queue_hook() {
    let mut guard = match source_worker_control_frame_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_start_pause_hook() {
    let mut guard = match source_worker_start_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_start_delay_hook() {
    let mut guard = match source_worker_start_delay_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_scheduled_groups_refresh_queue_hook() {
    let mut guard = match source_worker_scheduled_groups_refresh_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_scheduled_groups_refresh_error_queue_hook() {
    let mut guard = match source_worker_scheduled_groups_refresh_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_scheduled_groups_refresh_delay_hook() {
    let mut guard = match source_worker_scheduled_groups_refresh_delay_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_force_find_error_queue_hook() {
    let mut guard = match source_worker_force_find_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_close_hook() {
    let mut guard = match source_worker_close_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn install_source_worker_update_roots_hook(hook: SourceWorkerUpdateRootsHook) {
    let mut guard = match source_worker_update_roots_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn clear_source_worker_update_roots_hook() {
    let mut guard = match source_worker_update_roots_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn install_source_worker_update_roots_error_hook(
    hook: SourceWorkerUpdateRootsErrorHook,
) {
    let mut guard = match source_worker_update_roots_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_logical_roots_snapshot_hook(
    hook: SourceWorkerLogicalRootsSnapshotHook,
) {
    let mut guard = match source_worker_logical_roots_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_control_frame_error_hook(
    hook: SourceWorkerControlFrameErrorHook,
) {
    let mut guard = match source_worker_control_frame_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_control_frame_error_queue_hook(
    hook: SourceWorkerControlFrameErrorQueueHook,
) {
    let mut guard = match source_worker_control_frame_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_status_error_hook(hook: SourceWorkerStatusErrorHook) {
    let mut guard = match source_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_observability_error_hook(
    hook: SourceWorkerObservabilityErrorHook,
) {
    let mut guard = match source_worker_observability_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_observability_call_count_hook(
    hook: SourceWorkerObservabilityCallCountHook,
) {
    let mut guard = match source_worker_observability_call_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_observability_delay_hook(
    hook: SourceWorkerObservabilityDelayHook,
) {
    let mut guard = match source_worker_observability_delay_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_trigger_rescan_when_ready_call_count_hook(
    hook: SourceWorkerTriggerRescanWhenReadyCallCountHook,
) {
    let mut guard = match source_worker_trigger_rescan_when_ready_call_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_scheduled_groups_error_hook(
    hook: SourceWorkerScheduledGroupsErrorHook,
) {
    let mut guard = match source_worker_scheduled_groups_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_source_worker_start_error_queue_hook(hook: SourceWorkerStartErrorQueueHook) {
    let mut guard = match source_worker_start_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn clear_source_worker_update_roots_error_hook() {
    let mut guard = match source_worker_update_roots_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_logical_roots_snapshot_hook() {
    let mut guard = match source_worker_logical_roots_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_control_frame_error_hook() {
    let mut guard = match source_worker_control_frame_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
    drop(guard);
    let mut queued = match source_worker_control_frame_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *queued = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_status_error_hook() {
    let mut guard = match source_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_observability_error_hook() {
    let mut guard = match source_worker_observability_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_observability_call_count_hook() {
    let mut guard = match source_worker_observability_call_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_observability_delay_hook() {
    let mut guard = match source_worker_observability_delay_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_trigger_rescan_when_ready_call_count_hook() {
    let mut guard = match source_worker_trigger_rescan_when_ready_call_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_scheduled_groups_error_hook() {
    let mut guard = match source_worker_scheduled_groups_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_source_worker_start_error_queue_hook() {
    let mut guard = match source_worker_start_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn take_source_worker_status_error_hook() -> Option<CnxError> {
    let mut guard = match source_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
fn source_worker_logical_roots_snapshot_hook() -> Option<SourceWorkerLogicalRootsSnapshotHook> {
    let guard = match source_worker_logical_roots_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.as_ref().cloned()
}

#[cfg(test)]
async fn maybe_pause_before_logical_roots_snapshot_rpc() -> Option<Vec<RootSpec>> {
    let hook = source_worker_logical_roots_snapshot_hook()?;
    if let Some(entered) = hook.entered.as_ref() {
        entered.notify_waiters();
    }
    if let Some(release) = hook.release.as_ref() {
        release.notified().await;
    }
    hook.roots
}

#[cfg(test)]
fn take_source_worker_observability_error_hook() -> Option<CnxError> {
    let mut guard = match source_worker_observability_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
fn source_worker_observability_delay_hook() -> Option<Duration> {
    let guard = match source_worker_observability_delay_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.as_ref().map(|hook| hook.delay)
}

#[cfg(test)]
fn record_source_worker_observability_rpc_attempt() {
    let guard = match source_worker_observability_call_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(hook) = guard.as_ref() {
        hook.count.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
fn record_source_worker_trigger_rescan_when_ready_attempt() {
    let guard = match source_worker_trigger_rescan_when_ready_call_count_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(hook) = guard.as_ref() {
        hook.count.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
fn take_source_worker_scheduled_groups_error_hook() -> Option<CnxError> {
    let mut guard = match source_worker_scheduled_groups_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
fn take_source_worker_start_error_queue_hook(current_worker_instance_id: u64) -> Option<CnxError> {
    let mut guard = match source_worker_start_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    if hook.sticky_worker_instance_id == Some(current_worker_instance_id)
        && let Some(err) = hook.sticky_peer_err.clone()
    {
        return Some(CnxError::PeerError(err));
    }
    let err = hook.errs.pop_front();
    if hook.errs.is_empty() && hook.sticky_peer_err.is_none() {
        *guard = None;
    }
    err
}

#[cfg(test)]
fn take_source_worker_start_delay_hook() -> Option<Duration> {
    let mut guard = match source_worker_start_delay_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    let delay = hook.delays.pop_front();
    if hook.delays.is_empty() {
        *guard = None;
    }
    delay
}

#[cfg(test)]
fn take_source_worker_scheduled_groups_refresh_queue_hook() -> Option<(
    Option<std::collections::BTreeSet<String>>,
    Option<std::collections::BTreeSet<String>>,
)> {
    let mut guard = match source_worker_scheduled_groups_refresh_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    let reply = hook.replies.pop_front();
    if hook.replies.is_empty() {
        *guard = None;
    }
    reply
}

#[cfg(test)]
fn take_source_worker_scheduled_groups_refresh_error_queue_hook(
    current_worker_instance_id: u64,
) -> Option<CnxError> {
    let mut guard = match source_worker_scheduled_groups_refresh_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    if let Some(sticky_worker_instance_id) = hook.sticky_worker_instance_id {
        if sticky_worker_instance_id != current_worker_instance_id {
            return None;
        }
        if let Some(err) = hook.sticky_peer_err.clone() {
            return Some(CnxError::PeerError(err));
        }
    }
    let err = hook.errs.pop_front();
    if hook.errs.is_empty() && hook.sticky_peer_err.is_none() {
        *guard = None;
    }
    err
}

#[cfg(test)]
fn take_source_worker_scheduled_groups_refresh_delay_hook(
    current_worker_instance_id: u64,
) -> Option<Duration> {
    let mut guard = match source_worker_scheduled_groups_refresh_delay_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    if let Some(sticky_worker_instance_id) = hook.sticky_worker_instance_id
        && sticky_worker_instance_id != current_worker_instance_id
    {
        return None;
    }
    let delay = hook.delays.pop_front();
    if hook.delays.is_empty() {
        *guard = None;
    }
    delay
}

#[cfg(test)]
fn take_source_worker_force_find_error_queue_hook(
    current_worker_instance_id: u64,
) -> Option<CnxError> {
    let mut guard = match source_worker_force_find_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_mut()?;
    if let Some(sticky_worker_instance_id) = hook.sticky_worker_instance_id {
        if sticky_worker_instance_id != current_worker_instance_id {
            return None;
        }
        if let Some(err) = hook.sticky_peer_err.clone() {
            return Some(CnxError::PeerError(err));
        }
    }
    let err = hook.errs.pop_front();
    if hook.errs.is_empty() && hook.sticky_peer_err.is_none() {
        *guard = None;
    }
    err
}

#[cfg(test)]
fn notify_source_worker_control_frame_started() {
    let hook = {
        let guard = match source_worker_control_frame_hook_cell().lock() {
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
async fn maybe_pause_before_on_control_frame_rpc() {
    let hook = {
        let mut guard = match source_worker_control_frame_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.take()
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
async fn maybe_pause_before_ensure_started() {
    let hook = {
        let mut guard = match source_worker_start_pause_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.take()
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
fn notify_source_worker_close_started() {
    let hook = {
        let guard = match source_worker_close_hook_cell().lock() {
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
async fn maybe_pause_before_update_logical_roots_rpc() {
    let hook = {
        let mut guard = match source_worker_update_roots_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.take()
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
fn take_update_logical_roots_error_hook() -> Option<CnxError> {
    let mut guard = match source_worker_update_roots_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
fn take_on_control_frame_error_hook(current_worker_instance_id: u64) -> Option<CnxError> {
    {
        let mut guard = match source_worker_control_frame_error_queue_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(hook) = guard.as_mut() {
            if hook.sticky_worker_instance_id == Some(current_worker_instance_id)
                && let Some(err) = hook.sticky_peer_err.clone()
            {
                return Some(CnxError::PeerError(err));
            }
            if let Some(err) = hook.errs.pop_front() {
                if hook.errs.is_empty() && hook.sticky_peer_err.is_none() {
                    *guard = None;
                }
                return Some(err);
            }
            if hook.errs.is_empty() && hook.sticky_peer_err.is_none() {
                *guard = None;
            }
        }
    }
    let mut guard = match source_worker_control_frame_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

impl SourceWorkerClientHandle {
    pub(crate) fn new(
        node_id: NodeId,
        config: SourceConfig,
        worker_binding: RuntimeWorkerBinding,
        worker_factory: RuntimeWorkerClientFactory,
    ) -> Result<Self> {
        let key = source_worker_handle_registry_key(&node_id, &worker_binding, &worker_factory);
        let shared = {
            let mut registry = lock_source_worker_handle_registry();
            if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
                existing
            } else {
                let (control_ops_state, _control_ops_state_rx) = tokio::sync::watch::channel(0);
                let initial_progress_state = SourceProgressState::default();
                let (source_progress_state, _source_progress_state_rx) =
                    tokio::sync::watch::channel(initial_progress_state);
                let shared = Arc::new(SharedSourceWorkerHandleState {
                    worker: tokio::sync::Mutex::new(SharedSourceWorkerClient {
                        instance_id: next_shared_source_worker_instance_id(),
                        client: Arc::new(worker_factory.connect(
                            node_id.clone(),
                            config.clone(),
                            worker_binding.clone(),
                        )?),
                    }),
                    cache: Arc::new(Mutex::new(SourceWorkerSnapshotCache {
                        grants: Some(config.host_object_grants.clone()),
                        logical_roots: Some(config.roots.clone()),
                        ..SourceWorkerSnapshotCache::default()
                    })),
                    control_state: Arc::new(tokio::sync::Mutex::new(SourceControlState::default())),
                    start_serial: Arc::new(tokio::sync::Mutex::new(())),
                    control_ops_inflight: Arc::new(AtomicUsize::new(0)),
                    control_ops_state,
                    source_progress_state_current: Arc::new(Mutex::new(initial_progress_state)),
                    source_progress_state,
                    control_ops_serial: Arc::new(tokio::sync::Mutex::new(())),
                    observability_read_serial: Arc::new(tokio::sync::Mutex::new(())),
                });
                registry.insert(key, Arc::downgrade(&shared));
                shared
            }
        };
        let manual_rescan_signal = SignalCell::from_state_boundary(
            crate::runtime::execution_units::SOURCE_RUNTIME_UNIT_ID,
            "manual_rescan",
            worker_factory.state_boundary(),
        )
        .map_err(|err| {
            CnxError::InvalidInput(format!("source manual-rescan signal init failed: {err}"))
        })?;
        Ok(Self {
            _shared: shared.clone(),
            node_id,
            worker_factory,
            worker_binding,
            manual_rescan_signal,
            cache: shared.cache.clone(),
            control_state: shared.control_state.clone(),
            start_serial: shared.start_serial.clone(),
            control_ops_inflight: shared.control_ops_inflight.clone(),
            control_ops_state_tx: shared.control_ops_state.clone(),
            control_ops_state: shared.control_ops_state.subscribe(),
            source_progress_state_current: shared.source_progress_state_current.clone(),
            source_progress_state_tx: shared.source_progress_state.clone(),
            source_progress_state: shared.source_progress_state.subscribe(),
            control_ops_serial: shared.control_ops_serial.clone(),
            observability_read_serial: shared.observability_read_serial.clone(),
            config,
        })
    }

    async fn worker_client(&self) -> Arc<TypedRuntimeWorkerClient<SourceWorkerRpc, SourceConfig>> {
        self.shared_worker().await.1
    }

    async fn shared_worker(
        &self,
    ) -> (
        u64,
        Arc<TypedRuntimeWorkerClient<SourceWorkerRpc, SourceConfig>>,
    ) {
        let guard = self._shared.worker.lock().await;
        (guard.instance_id, guard.client.clone())
    }

    #[cfg(test)]
    pub(crate) async fn worker_instance_id_for_tests(&self) -> u64 {
        self.shared_worker().await.0
    }

    async fn replace_shared_worker_client(
        &self,
    ) -> Result<Arc<TypedRuntimeWorkerClient<SourceWorkerRpc, SourceConfig>>> {
        let replacement = SharedSourceWorkerClient {
            instance_id: next_shared_source_worker_instance_id(),
            client: Arc::new(self.worker_factory.connect(
                self.node_id.clone(),
                self.config.clone(),
                self.worker_binding.clone(),
            )?),
        };
        let stale_client = {
            let mut guard = self._shared.worker.lock().await;
            let stale = guard.client.clone();
            *guard = replacement;
            stale
        };
        self.control_state.lock().await.arm_replay();
        self.bump_source_progress(SourceProgressReason::WorkerClientReplaced);
        Ok(stale_client)
    }

    async fn reconnect_shared_worker_client_with_failure(
        &self,
    ) -> std::result::Result<(), SourceFailure> {
        let stale_client = self.replace_shared_worker_client().await?;
        let _ = stale_client.shutdown(Duration::from_millis(250)).await;
        Ok(())
    }

    async fn replace_worker_client_for_fail_closed_control_frame(
        &self,
    ) -> std::result::Result<(), SourceFailure> {
        let stale_client = self.replace_shared_worker_client().await?;
        tokio::spawn(async move {
            let _ = stale_client.shutdown(Duration::from_millis(250)).await;
        });
        Ok(())
    }

    async fn retain_control_signals(&self, signals: &[SourceControlSignal]) {
        self.control_state.lock().await.retain_signals(signals);
        self.bump_source_progress(SourceProgressReason::ControlSignalsRetained);
    }

    fn apply_control_frame_retained_tick_fast_path(&self, signals: &[SourceControlSignal]) {
        self.with_cache_mut(|cache| {
            prime_cached_control_summary_from_control_signals(
                cache,
                &self.node_id,
                signals,
                &self.config.host_object_grants,
            );
            cache.observability_control_summary_override_by_node =
                cache.last_control_frame_signals_by_node.clone();
        });
    }

    async fn replay_control_frame_retained_tick(
        &self,
        deadline: std::time::Instant,
    ) -> std::result::Result<(), SourceFailure> {
        self.with_cache_mut(|cache| {
            cache.observability_control_summary_override_by_node = None;
        });
        self.replay_retained_control_state_if_needed_for_refresh_until(deadline)
            .await
    }

    async fn apply_post_ack_cache_and_retain_signals(
        &self,
        signals: &[SourceControlSignal],
        cache_priming: SourceControlFramePostAckCachePriming,
    ) {
        self.with_cache_mut(|cache| {
            cache_priming.apply_to_cache(
                cache,
                &self.node_id,
                signals,
                &self.config.roots,
                &self.config.host_object_grants,
            );
        });
        self.retain_control_signals(signals).await;
    }

    async fn perform_control_frame_attempt(
        &self,
        envelopes: &[ControlEnvelope],
        timeout: Duration,
    ) -> (
        std::result::Result<SourceWorkerResponse, CnxError>,
        SourceProgressState,
    ) {
        let rpc_result = self
            .execute_on_control_frame_request_attempt(
                envelopes,
                timeout,
                SourceControlFrameRequestAttemptKind::ControlFrame,
            )
            .await;
        (rpc_result, self.current_source_progress_state())
    }

    async fn reconnect_for_control_frame(&self) -> std::result::Result<(), SourceFailure> {
        self.reconnect_shared_worker_client_with_failure().await
    }

    async fn wait_control_frame_retry_after(
        &self,
        after: SourceProgressState,
        deadline: std::time::Instant,
    ) {
        self.wait_for_source_progress_after(after, SourceWaitFor::ControlFlowRetry, deadline)
            .await;
    }

    async fn arm_control_frame_replay(&self) {
        self.control_state.lock().await.arm_replay();
    }

    async fn refresh_scheduled_groups_for_control_frame(
        &self,
        deadline: std::time::Instant,
        scheduled_groups_expectation: SourceScheduledGroupsExpectation,
    ) -> std::result::Result<(), SourceFailure> {
        self.refresh_cached_scheduled_groups_from_live_worker_until(
            deadline,
            scheduled_groups_expectation,
        )
        .await
    }

    async fn replay_retained_control_state_if_needed_for_refresh_until(
        &self,
        deadline: std::time::Instant,
    ) -> std::result::Result<(), SourceFailure> {
        let envelopes = {
            let mut control_state = self.control_state.lock().await;
            if !matches!(
                control_state.take_replay_state(),
                SourceControlReplayState::Required
            ) {
                return Ok(());
            }
            control_state.replay_envelopes()
        };
        if envelopes.is_empty() {
            return Ok(());
        }

        let deadline = clip_retry_deadline(deadline, SOURCE_WORKER_SCHEDULE_REFRESH_TOTAL_TIMEOUT);
        let mut machine = SourceReplayRetainedControlStateMachine::new(deadline);
        let replay_result = drive_source_machine_loop(
            machine.start(),
            |effect| async {
                match effect {
                    SourceReplayRetainedControlStateEffect::Attempt { timeout } => {
                        SourceOperationLoopStep::Event(
                            SourceReplayRetainedControlStateEvent::AttemptCompleted {
                                rpc_result: self
                                    .execute_on_control_frame_request_attempt(
                                        &envelopes,
                                        timeout,
                                        SourceControlFrameRequestAttemptKind::RetainedReplay,
                                    )
                                    .await,
                                after: self.current_source_progress_state(),
                            },
                        )
                    }
                    SourceReplayRetainedControlStateEffect::Reconnect => {
                        match self.reconnect_shared_worker_client_with_failure().await {
                            Ok(()) => SourceOperationLoopStep::Event(
                                SourceReplayRetainedControlStateEvent::ReconnectCompleted,
                            ),
                            Err(err) => SourceOperationLoopStep::Fail(err),
                        }
                    }
                    SourceReplayRetainedControlStateEffect::Wait { after } => {
                        self.wait_for_source_progress_after(
                            after,
                            SourceWaitFor::ControlFlowRetry,
                            deadline,
                        )
                        .await;
                        SourceOperationLoopStep::Event(
                            SourceReplayRetainedControlStateEvent::WaitCompleted,
                        )
                    }
                    SourceReplayRetainedControlStateEffect::Complete => {
                        SourceOperationLoopStep::Complete(())
                    }
                    SourceReplayRetainedControlStateEffect::Fail(failure) => {
                        SourceOperationLoopStep::Fail(failure)
                    }
                }
            },
            |event| Ok(machine.advance(event)),
        )
        .await;
        if replay_result.is_err() {
            self.control_state.lock().await.arm_replay();
        }
        replay_result
    }

    async fn replay_retained_control_state_before_observability_read_until(
        &self,
        deadline: std::time::Instant,
    ) -> std::result::Result<(), SourceFailure> {
        let replay_required = matches!(
            self.control_state.lock().await.replay_state(),
            SourceControlReplayState::Required
        );
        if !replay_required {
            return Ok(());
        }

        let _inflight = self.begin_control_op();
        let _serial = self.control_ops_serial.lock().await;
        eprintln!(
            "fs_meta_source_worker_client: observability retained replay begin node={}",
            self.node_id.0
        );
        let result = self
            .replay_retained_control_state_if_needed_for_refresh_until(deadline)
            .await;
        eprintln!(
            "fs_meta_source_worker_client: observability retained replay done node={} ok={}",
            self.node_id.0,
            result.is_ok()
        );
        result
    }

    fn begin_control_op(&self) -> InflightControlOpGuard {
        let next = self.control_ops_inflight.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.control_ops_state_tx.send_replace(next);
        InflightControlOpGuard {
            counter: self.control_ops_inflight.clone(),
            state: self.control_ops_state_tx.clone(),
        }
    }

    fn control_op_inflight(&self) -> bool {
        self.control_ops_inflight.load(Ordering::Relaxed) > 0
    }

    pub(crate) async fn wait_for_control_ops_to_drain(&self, timeout: Duration) {
        if !self.control_op_inflight() {
            return;
        }
        let mut state = self.control_ops_state.clone();
        let wait_for_drain = async {
            while self.control_op_inflight() {
                if state.changed().await.is_err() {
                    break;
                }
            }
        };
        let _ = tokio::time::timeout(timeout, wait_for_drain).await;
    }

    fn current_source_progress_state(&self) -> SourceProgressState {
        let guard = match self.source_progress_state_current.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                log::warn!("source progress state lock poisoned; recovering progress wait state");
                poisoned.into_inner()
            }
        };
        *guard
    }

    fn bump_source_progress(&self, reason: SourceProgressReason) {
        let next = {
            let mut guard = match self.source_progress_state_current.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    log::warn!(
                        "source progress state lock poisoned; recovering progress publish state"
                    );
                    poisoned.into_inner()
                }
            };
            guard.bump(reason);
            *guard
        };
        let _ = self.source_progress_state_tx.send_replace(next);
    }

    fn cached_rescan_observed_epoch(&self) -> u64 {
        self.with_cache_mut(|cache| cache.rescan_observed_epoch)
    }

    fn cached_materialized_read_cache_epoch(&self) -> u64 {
        self.with_cache_mut(|cache| cache.rescan_request_epoch.max(cache.rescan_observed_epoch))
    }

    fn bump_cached_materialized_read_cache_epoch(&self) {
        self.with_cache_mut(|cache| {
            let next = cache
                .rescan_request_epoch
                .max(cache.rescan_observed_epoch)
                .saturating_add(1);
            cache.rescan_request_epoch = cache.rescan_request_epoch.max(next);
            cache.last_live_observability_snapshot_at = None;
        });
    }

    async fn wait_for_source_progress_after(
        &self,
        after: SourceProgressState,
        wait_for: SourceWaitFor,
        deadline: std::time::Instant,
    ) {
        if self
            .source_progress_state_current
            .lock()
            .map(|guard| wait_for.satisfied_by(*guard, after))
            .unwrap_or_else(|poisoned| wait_for.satisfied_by(*poisoned.into_inner(), after))
        {
            return;
        }
        let mut state = self.source_progress_state.clone();
        if wait_for.satisfied_by(*state.borrow(), after) {
            return;
        }
        let wait_for_progress = async {
            while !wait_for.satisfied_by(*state.borrow(), after) {
                if state.changed().await.is_err() {
                    break;
                }
            }
        };
        let _ =
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), wait_for_progress)
                .await;
    }

    fn with_cache_mut<T>(&self, f: impl FnOnce(&mut SourceWorkerSnapshotCache) -> T) -> T {
        let mut guard = match self.cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                log::warn!("source worker cache lock poisoned; recovering cached snapshot state");
                poisoned.into_inner()
            }
        };
        f(&mut guard)
    }

    fn degraded_observability_snapshot_from_cache(
        &self,
        reason: impl Into<String>,
    ) -> SourceObservabilitySnapshot {
        let guard = match self.cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                log::warn!("source worker cache lock poisoned; recovering cached snapshot state");
                poisoned.into_inner()
            }
        };
        build_degraded_worker_observability_snapshot(&guard, reason)
    }

    fn update_cached_observability_snapshot(&self, snapshot: &SourceObservabilitySnapshot) {
        self.with_cache_mut(|cache| apply_observability_snapshot_to_cache(cache, snapshot));
        self.bump_source_progress(SourceProgressReason::ObservabilityUpdated);
    }

    async fn refresh_cached_scheduled_groups_from_live_worker_until(
        &self,
        deadline: std::time::Instant,
        scheduled_groups_expectation: SourceScheduledGroupsExpectation,
    ) -> std::result::Result<(), SourceFailure> {
        eprintln!(
            "fs_meta_source_worker_client: refresh_cached_scheduled_groups begin node={}",
            self.node_id.0
        );
        let deadline = clip_retry_deadline(deadline, SOURCE_WORKER_SCHEDULE_REFRESH_TOTAL_TIMEOUT);
        let refresh_disposition = self
            .control_state
            .lock()
            .await
            .replay_state()
            .refresh_disposition();
        let mut machine = SourceScheduledGroupsRefreshMachine::new(
            deadline,
            scheduled_groups_expectation,
            refresh_disposition,
        );
        let recovery_observation = Arc::new(std::sync::atomic::AtomicU8::new(
            machine.recovery_observation.as_atomic_u8(),
        ));
        let recovery_observation_for_deadline = recovery_observation.clone();
        let drive_refresh = drive_source_machine_loop(
            machine.start(),
            |effect| async {
                match effect {
                    SourceScheduledGroupsRefreshEffect::EnsureStarted => {
                        SourceOperationLoopStep::Event(
                            SourceScheduledGroupsRefreshEvent::EnsureStartedCompleted {
                                result: self
                                    .execute_scheduled_groups_refresh_ensure_started(deadline)
                                    .await,
                                after: self.current_source_progress_state(),
                            },
                        )
                    }
                    SourceScheduledGroupsRefreshEffect::ReplayRetainedControlState => {
                        SourceOperationLoopStep::Event(
                            SourceScheduledGroupsRefreshEvent::ReplayRetainedControlStateCompleted(
                                self.replay_retained_control_state_if_needed_for_refresh_until(
                                    deadline,
                                )
                                .await,
                            ),
                        )
                    }
                    SourceScheduledGroupsRefreshEffect::AcquireClient => {
                        SourceOperationLoopStep::Event(
                            SourceScheduledGroupsRefreshEvent::ClientAcquired {
                                result: self
                                    .execute_scheduled_groups_refresh_acquire_client()
                                    .await,
                                after: self.current_source_progress_state(),
                            },
                        )
                    }
                    SourceScheduledGroupsRefreshEffect::RefreshGrants { client } => {
                        SourceOperationLoopStep::Event(
                            SourceScheduledGroupsRefreshEvent::GrantsRefreshed {
                                result: self
                                    .execute_scheduled_groups_refresh_grants(&client, deadline)
                                    .await
                                    .map(|stable_host_ref| {
                                        SourceScheduledGroupsRefreshGrantedClient {
                                            client,
                                            stable_host_ref,
                                        }
                                    }),
                                after: self.current_source_progress_state(),
                            },
                        )
                    }
                    SourceScheduledGroupsRefreshEffect::FetchScheduledGroups {
                        client,
                        stable_host_ref,
                    } => SourceOperationLoopStep::Event(
                        SourceScheduledGroupsRefreshEvent::FetchScheduledGroupsCompleted {
                            result: self
                                .execute_scheduled_groups_refresh_fetch(
                                    &client,
                                    &stable_host_ref,
                                    deadline,
                                )
                                .await,
                            after: self.current_source_progress_state(),
                        },
                    ),
                    SourceScheduledGroupsRefreshEffect::Reconnect => {
                        match self.reconnect_shared_worker_client_with_failure().await {
                            Ok(()) => SourceOperationLoopStep::Event(
                                SourceScheduledGroupsRefreshEvent::ReconnectCompleted,
                            ),
                            Err(err) => SourceOperationLoopStep::Fail(
                                source_scheduled_groups_refresh_failure_from_error(
                                    err.into_error(),
                                    refresh_disposition,
                                    SourceScheduledGroupsRefreshRecoveryObservation::from_atomic_u8(
                                        recovery_observation.load(Ordering::Relaxed),
                                    ),
                                ),
                            ),
                        }
                    }
                    SourceScheduledGroupsRefreshEffect::Wait { after } => {
                        self.wait_for_source_progress_after(
                            after,
                            SourceWaitFor::ScheduledGroupsRefresh,
                            deadline,
                        )
                        .await;
                        SourceOperationLoopStep::Event(
                            SourceScheduledGroupsRefreshEvent::WaitCompleted,
                        )
                    }
                    SourceScheduledGroupsRefreshEffect::CommitGroups {
                        scheduled_source,
                        scheduled_scan,
                    } => {
                        self.execute_scheduled_groups_refresh_commit_groups(
                            scheduled_source,
                            scheduled_scan,
                        );
                        SourceOperationLoopStep::Event(
                            SourceScheduledGroupsRefreshEvent::CommitGroupsCompleted,
                        )
                    }
                    SourceScheduledGroupsRefreshEffect::Complete => {
                        SourceOperationLoopStep::Complete(())
                    }
                    SourceScheduledGroupsRefreshEffect::Fail(failure) => {
                        SourceOperationLoopStep::Fail(failure)
                    }
                }
            },
            |event| {
                let effect = machine.advance(event);
                recovery_observation.store(
                    machine.recovery_observation.as_atomic_u8(),
                    Ordering::Relaxed,
                );
                Ok(effect)
            },
        );
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), drive_refresh).await
        {
            Ok(result) => result,
            Err(_) => Err(source_scheduled_groups_refresh_deadline_failure(
                refresh_disposition,
                SourceScheduledGroupsRefreshRecoveryObservation::from_atomic_u8(
                    recovery_observation_for_deadline.load(Ordering::Relaxed),
                ),
            )),
        }
    }

    async fn execute_scheduled_groups_refresh_ensure_started(
        &self,
        deadline: std::time::Instant,
    ) -> std::result::Result<(), SourceFailure> {
        let worker = self.worker_client().await;
        let ensure_started_timeout =
            source_operation_attempt_timeout(deadline, SOURCE_WORKER_SCHEDULE_REFRESH_RPC_TIMEOUT)?;
        map_source_timeout_result_with_failure(
            tokio::time::timeout(ensure_started_timeout, worker.ensure_started()).await,
        )
    }

    async fn execute_scheduled_groups_refresh_acquire_client(
        &self,
    ) -> std::result::Result<TypedWorkerClient<SourceWorkerRpc>, SourceFailure> {
        self.client_with_failure().await
    }

    fn scheduled_groups_refresh_stable_host_ref(&self, grants: &[GrantedMountRoot]) -> String {
        let stable = stable_host_ref_for_node_id(&self.node_id, grants);
        if stable != self.node_id.0 {
            stable
        } else {
            self.with_cache_mut(|cache| {
                stable_host_ref_from_cached_scheduled_groups(&self.node_id, cache).unwrap_or(stable)
            })
        }
    }

    async fn execute_scheduled_groups_refresh_grants_snapshot(
        &self,
        client: &TypedWorkerClient<SourceWorkerRpc>,
        deadline: std::time::Instant,
    ) -> std::result::Result<SourceWorkerResponse, SourceFailure> {
        Self::call_worker_with_failure(
            client,
            SourceWorkerRequest::HostObjectGrantsSnapshot,
            source_operation_attempt_timeout(deadline, SOURCE_WORKER_SCHEDULE_REFRESH_RPC_TIMEOUT)?,
        )
        .await
    }

    async fn execute_scheduled_groups_refresh_grants(
        &self,
        client: &TypedWorkerClient<SourceWorkerRpc>,
        deadline: std::time::Instant,
    ) -> std::result::Result<String, SourceFailure> {
        match self
            .execute_scheduled_groups_refresh_grants_snapshot(client, deadline)
            .await
        {
            Ok(SourceWorkerResponse::HostObjectGrants(grants)) => {
                self.with_cache_mut(|cache| {
                    cache.grants = Some(grants.clone());
                });
                self.bump_source_progress(SourceProgressReason::HostObjectGrantsUpdated);
                Ok(self.scheduled_groups_refresh_stable_host_ref(&grants))
            }
            Ok(other) => unexpected_source_worker_response_result(
                "for scheduled groups host grants refresh",
                other,
            ),
            Err(err) if can_use_cached_grant_derived_snapshot(err.as_error()) => Ok(self
                .cached_host_object_grants_snapshot_with_failure()
                .map(|grants| self.scheduled_groups_refresh_stable_host_ref(&grants))?),
            Err(err) => Err(err),
        }
    }

    async fn execute_scheduled_groups_refresh_fetch(
        &self,
        client: &TypedWorkerClient<SourceWorkerRpc>,
        stable_host_ref: &str,
        deadline: std::time::Instant,
    ) -> std::result::Result<SourceScheduledGroupsRefreshFetchedGroups, SourceFailure> {
        #[cfg(test)]
        let refresh_attempt = {
            let worker_instance_id = self.worker_instance_id_for_tests().await;
            if let Some(delay) =
                take_source_worker_scheduled_groups_refresh_delay_hook(worker_instance_id)
            {
                tokio::time::sleep(delay).await;
            }
            let injected =
                take_source_worker_scheduled_groups_refresh_error_queue_hook(worker_instance_id);
            if let Some(err) = injected {
                Err(err.into())
            } else {
                self.fetch_scheduled_groups_refresh_attempt(client, stable_host_ref, deadline)
                    .await
            }
        };
        #[cfg(not(test))]
        let refresh_attempt = self
            .fetch_scheduled_groups_refresh_attempt(client, stable_host_ref, deadline)
            .await;
        match refresh_attempt {
            Ok((scheduled_source, scheduled_scan)) => {
                Ok(SourceScheduledGroupsRefreshFetchedGroups {
                    cache_empty: self.with_cache_mut(|cache| {
                        cache
                            .scheduled_source_groups_by_node
                            .as_ref()
                            .is_none_or(|groups| groups.is_empty())
                            && cache
                                .scheduled_scan_groups_by_node
                                .as_ref()
                                .is_none_or(|groups| groups.is_empty())
                    }),
                    scheduled_source,
                    scheduled_scan,
                })
            }
            Err(err) => Err(err),
        }
    }

    fn execute_scheduled_groups_refresh_commit_groups(
        &self,
        scheduled_source: std::collections::BTreeMap<String, Vec<String>>,
        scheduled_scan: std::collections::BTreeMap<String, Vec<String>>,
    ) {
        self.with_cache_mut(|cache| {
            update_cached_scheduled_groups_from_refresh(cache, scheduled_source, scheduled_scan);
        });
        self.bump_source_progress(SourceProgressReason::ScheduledGroupsUpdated);
        eprintln!(
            "fs_meta_source_worker_client: refresh_cached_scheduled_groups ok node={}",
            self.node_id.0
        );
    }

    async fn fetch_scheduled_groups_refresh_attempt(
        &self,
        client: &TypedWorkerClient<SourceWorkerRpc>,
        stable_host_ref: &str,
        deadline: std::time::Instant,
    ) -> std::result::Result<
        (
            std::collections::BTreeMap<String, Vec<String>>,
            std::collections::BTreeMap<String, Vec<String>>,
        ),
        SourceFailure,
    > {
        #[cfg(test)]
        if let Some((source_groups, scan_groups)) =
            take_source_worker_scheduled_groups_refresh_queue_hook()
        {
            let to_map = |groups: Option<std::collections::BTreeSet<String>>| {
                groups
                    .map(|groups| {
                        std::collections::BTreeMap::from([(
                            stable_host_ref.to_string(),
                            groups.into_iter().collect::<Vec<_>>(),
                        )])
                    })
                    .unwrap_or_default()
            };
            return Ok((to_map(source_groups), to_map(scan_groups)));
        }

        let scheduled_source = match Self::call_worker_with_failure(
            client,
            SourceWorkerRequest::ScheduledSourceGroupIds,
            source_operation_attempt_timeout(deadline, SOURCE_WORKER_SCHEDULE_REFRESH_RPC_TIMEOUT)?,
        )
        .await?
        {
            SourceWorkerResponse::ScheduledGroupIds(groups) => groups
                .map(|groups| {
                    std::collections::BTreeMap::from([(
                        stable_host_ref.to_string(),
                        groups.into_iter().collect::<Vec<_>>(),
                    )])
                })
                .unwrap_or_default(),
            other => {
                return unexpected_source_worker_response_result(
                    "for scheduled source groups cache refresh",
                    other,
                );
            }
        };
        let scheduled_scan = match Self::call_worker_with_failure(
            client,
            SourceWorkerRequest::ScheduledScanGroupIds,
            source_operation_attempt_timeout(deadline, SOURCE_WORKER_SCHEDULE_REFRESH_RPC_TIMEOUT)?,
        )
        .await?
        {
            SourceWorkerResponse::ScheduledGroupIds(groups) => groups
                .map(|groups| {
                    std::collections::BTreeMap::from([(
                        stable_host_ref.to_string(),
                        groups.into_iter().collect::<Vec<_>>(),
                    )])
                })
                .unwrap_or_default(),
            other => {
                return unexpected_source_worker_response_result(
                    "for scheduled scan groups cache refresh",
                    other,
                );
            }
        };
        Ok((scheduled_source, scheduled_scan))
    }

    #[cfg(test)]
    async fn call_worker(
        client: &TypedWorkerClient<SourceWorkerRpc>,
        request: SourceWorkerRequest,
        timeout: Duration,
    ) -> Result<SourceWorkerResponse> {
        client.call_with_timeout(request, timeout).await
    }

    async fn call_worker_with_failure(
        client: &TypedWorkerClient<SourceWorkerRpc>,
        request: SourceWorkerRequest,
        timeout: Duration,
    ) -> std::result::Result<SourceWorkerResponse, SourceFailure> {
        client
            .call_with_timeout(request, timeout)
            .await
            .map_err(SourceFailure::from)
    }

    async fn with_started_retry_with_failure<T, F, Fut>(
        &self,
        op: F,
    ) -> std::result::Result<T, SourceFailure>
    where
        F: Fn(TypedWorkerClient<SourceWorkerRpc>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, SourceFailure>>,
    {
        self.worker_client()
            .await
            .with_started_retry_mapped(op, SourceFailure::into_error)
            .await
    }

    async fn with_started_once_with_failure<T, F, Fut>(
        &self,
        op: F,
    ) -> std::result::Result<T, SourceFailure>
    where
        F: FnOnce(TypedWorkerClient<SourceWorkerRpc>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, SourceFailure>>,
    {
        self.worker_client()
            .await
            .with_started_once_mapped(op, SourceFailure::into_error)
            .await
    }

    #[cfg(test)]
    async fn client(&self) -> Result<TypedWorkerClient<SourceWorkerRpc>> {
        self.worker_client().await.client().await
    }

    async fn client_with_failure(
        &self,
    ) -> std::result::Result<TypedWorkerClient<SourceWorkerRpc>, SourceFailure> {
        self.worker_client()
            .await
            .client()
            .await
            .map_err(SourceFailure::from)
    }

    #[cfg(test)]
    async fn existing_client(&self) -> Result<Option<TypedWorkerClient<SourceWorkerRpc>>> {
        self.worker_client().await.existing_client().await
    }

    async fn existing_client_with_failure(
        &self,
    ) -> std::result::Result<Option<TypedWorkerClient<SourceWorkerRpc>>, SourceFailure> {
        self.worker_client()
            .await
            .existing_client()
            .await
            .map_err(SourceFailure::from)
    }

    async fn shared_existing_client_with_failure(
        &self,
    ) -> std::result::Result<(u64, Option<TypedWorkerClient<SourceWorkerRpc>>), SourceFailure> {
        let (worker_instance_id, worker) = self.shared_worker().await;
        let client = worker
            .existing_client()
            .await
            .map_err(SourceFailure::from)?;
        Ok((worker_instance_id, client))
    }

    async fn observability_snapshot_with_timeout_with_failure(
        &self,
        timeout: Duration,
    ) -> std::result::Result<SourceObservabilitySnapshot, SourceFailure> {
        let deadline = clip_retry_deadline(std::time::Instant::now() + timeout, timeout);
        self.replay_retained_control_state_before_observability_read_until(deadline)
            .await?;
        let machine = SourceObservabilitySnapshotMachine::new(deadline, timeout);
        let response = drive_source_machine_loop(
            machine.start()?,
            |effect| async {
                match effect {
                    SourceObservabilitySnapshotEffect::Attempt { timeout } => {
                        SourceOperationLoopStep::Event(
                            SourceObservabilitySnapshotEvent::AttemptCompleted {
                                rpc_result: self
                                    .execute_observability_snapshot_attempt(timeout)
                                    .await,
                                after: self.current_source_progress_state(),
                            },
                        )
                    }
                    SourceObservabilitySnapshotEffect::Reconnect => {
                        match self.reconnect_shared_worker_client_with_failure().await {
                            Ok(()) => SourceOperationLoopStep::Event(
                                SourceObservabilitySnapshotEvent::ReconnectCompleted,
                            ),
                            Err(err) => SourceOperationLoopStep::Fail(err),
                        }
                    }
                    SourceObservabilitySnapshotEffect::Complete(response) => {
                        SourceOperationLoopStep::Complete(response)
                    }
                    SourceObservabilitySnapshotEffect::Fail(failure) => {
                        SourceOperationLoopStep::Fail(failure)
                    }
                }
            },
            |event| machine.advance(event),
        )
        .await?;
        match response {
            SourceWorkerResponse::ObservabilitySnapshot(mut snapshot) => {
                snapshot = self.with_cache_mut(|cache| {
                    let cached_schedule_stable_host_ref =
                        stable_host_ref_from_cached_scheduled_groups(&self.node_id, cache);
                    normalize_observability_snapshot_scheduled_group_keys(
                        &mut snapshot,
                        &self.node_id,
                        cache.grants.as_ref(),
                        cached_schedule_stable_host_ref.as_deref(),
                    );
                    merge_non_empty_cached_groups(
                        &mut snapshot.scheduled_source_groups_by_node,
                        &cache.scheduled_source_groups_by_node,
                    );
                    merge_non_empty_cached_groups(
                        &mut snapshot.scheduled_scan_groups_by_node,
                        &cache.scheduled_scan_groups_by_node,
                    );
                    let mut merged =
                        merge_live_observability_snapshot_with_recent_cache(cache, &snapshot);
                    if let Some(override_summary) = cache
                        .observability_control_summary_override_by_node
                        .as_ref()
                    {
                        merged.last_control_frame_signals_by_node = override_summary.clone();
                    }
                    merged
                });
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_source_worker_client: observability_snapshot reply node={} {}",
                        self.node_id.0,
                        summarize_source_observability_snapshot(&snapshot)
                    );
                }
                self.update_cached_observability_snapshot(&snapshot);
                Ok(snapshot)
            }
            other => unexpected_source_worker_response_result("for observability snapshot", other),
        }
    }

    #[cfg(test)]
    async fn observability_snapshot_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<SourceObservabilitySnapshot> {
        self.observability_snapshot_with_timeout_with_failure(timeout)
            .await
            .map_err(SourceFailure::into_error)
    }

    async fn wait_for_rescan_observed_epoch_with_failure(
        &self,
        request_epoch: u64,
        total_timeout: Duration,
    ) -> std::result::Result<(), SourceFailure> {
        let deadline = Instant::now() + total_timeout;
        loop {
            if self.cached_rescan_observed_epoch() >= request_epoch {
                return Ok(());
            }
            let remaining = match classify_source_retry_budget(deadline) {
                SourceRetryBudgetDisposition::Exhausted => {
                    return Err(SourceFailure::retry_budget_exhausted(
                        SourceRetryBudgetExhaustionKind::OperationWait,
                    ));
                }
                SourceRetryBudgetDisposition::Remaining(remaining) => remaining,
            };
            let snapshot_timeout = remaining.min(SOURCE_WORKER_OBSERVABILITY_RPC_TIMEOUT);
            let snapshot = self
                .progress_snapshot_with_timeout_with_failure(snapshot_timeout)
                .await?;
            if snapshot.rescan_observed_epoch >= request_epoch {
                return Ok(());
            }
        }
    }

    async fn start_with_failure(&self) -> std::result::Result<(), SourceFailure> {
        let _start_serial = self.start_serial.lock().await;
        let deadline = clip_retry_deadline(
            std::time::Instant::now() + SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
            SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
        );
        let mut machine = SourceStartMachine::new(deadline);
        drive_source_machine_loop(
            machine.start()?,
            |effect| async {
                match effect {
                    SourceStartEffect::Attempt { timeout } => {
                        eprintln!(
                            "fs_meta_source_worker_client: ensure_started begin node={}",
                            self.node_id.0
                        );
                        SourceOperationLoopStep::Event(SourceStartEvent::AttemptCompleted {
                            start_result: self.execute_start_attempt(timeout).await,
                            after: self.current_source_progress_state(),
                        })
                    }
                    SourceStartEffect::Reconnect => {
                        match self.reconnect_shared_worker_client_with_failure().await {
                            Ok(()) => {
                                SourceOperationLoopStep::Event(SourceStartEvent::ReconnectCompleted)
                            }
                            Err(err) => SourceOperationLoopStep::Fail(err),
                        }
                    }
                    SourceStartEffect::Wait { after } => {
                        self.wait_for_source_progress_after(
                            after,
                            SourceWaitFor::Started,
                            deadline,
                        )
                        .await;
                        SourceOperationLoopStep::Event(SourceStartEvent::WaitCompleted)
                    }
                    SourceStartEffect::Complete => {
                        eprintln!(
                            "fs_meta_source_worker_client: ensure_started ok node={}",
                            self.node_id.0
                        );
                        self.bump_source_progress(SourceProgressReason::Started);
                        SourceOperationLoopStep::Complete(())
                    }
                    SourceStartEffect::Fail(failure) => SourceOperationLoopStep::Fail(failure),
                }
            },
            |event| machine.advance(event),
        )
        .await
    }

    #[cfg(test)]
    pub async fn start(&self) -> Result<()> {
        self.start_with_failure()
            .await
            .map_err(SourceFailure::into_error)
    }

    pub(crate) async fn update_logical_roots_with_failure(
        &self,
        roots: Vec<RootSpec>,
    ) -> std::result::Result<(), SourceFailure> {
        let _inflight = self.begin_control_op();
        let _serial = self.control_ops_serial.lock().await;
        eprintln!(
            "fs_meta_source_worker_client: update_logical_roots begin node={} roots={}",
            self.node_id.0,
            roots.len()
        );
        let deadline = clip_retry_deadline(
            std::time::Instant::now() + SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT,
            SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT,
        );
        let machine = SourceUpdateLogicalRootsMachine::new(deadline);
        drive_source_machine_loop(
            machine.start()?,
            |effect| async {
                match effect {
                    SourceUpdateLogicalRootsEffect::Attempt { timeout } => {
                        SourceOperationLoopStep::Event(
                            SourceUpdateLogicalRootsEvent::AttemptCompleted {
                                rpc_result: self
                                    .execute_update_logical_roots_attempt(&roots, timeout)
                                    .await,
                                after: self.current_source_progress_state(),
                            },
                        )
                    }
                    SourceUpdateLogicalRootsEffect::Wait { after } => {
                        self.execute_update_logical_roots_wait(after, deadline).await;
                        SourceOperationLoopStep::Event(
                            SourceUpdateLogicalRootsEvent::WaitCompleted,
                        )
                    }
                    SourceUpdateLogicalRootsEffect::Complete => {
                        self.with_cache_mut(|cache| {
                            cache.logical_roots = Some(roots.clone());
                        });
                        self.bump_source_progress(SourceProgressReason::LogicalRootsUpdated);
                        eprintln!(
                            "fs_meta_source_worker_client: update_logical_roots ok node={} roots={}",
                            self.node_id.0,
                            roots.len()
                        );
                        SourceOperationLoopStep::Complete(())
                    }
                    SourceUpdateLogicalRootsEffect::Fail(failure) => {
                        SourceOperationLoopStep::Fail(failure)
                    }
                }
            },
            |event| machine.advance(event),
        )
        .await
    }

    fn cached_logical_roots_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<RootSpec>, SourceFailure> {
        self.with_cache_mut(|cache| {
            Ok::<Vec<RootSpec>, CnxError>(
                cache
                    .logical_roots
                    .clone()
                    .unwrap_or_else(|| self.config.roots.clone()),
            )
        })
        .map_err(SourceFailure::from)
    }

    async fn logical_roots_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<RootSpec>, SourceFailure> {
        let (worker_instance_id, client) = self.shared_existing_client_with_failure().await?;
        let Some(client) = client else {
            return self.cached_logical_roots_snapshot_with_failure();
        };
        #[cfg(test)]
        if let Some(roots) = maybe_pause_before_logical_roots_snapshot_rpc().await {
            return Ok(roots);
        }
        let result = Self::call_worker_with_failure(
            &client,
            SourceWorkerRequest::LogicalRootsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await;
        if self.shared_worker().await.0 != worker_instance_id {
            return Err(SourceFailure::from_cause(CnxError::TransportClosed(
                "stale shared source worker client detached during logical_roots_snapshot".into(),
            )));
        }
        match result {
            Ok(SourceWorkerResponse::LogicalRoots(roots)) => {
                self.with_cache_mut(|cache| {
                    cache.logical_roots = Some(roots.clone());
                });
                Ok(roots)
            }
            Ok(other) => unexpected_source_worker_response_result("for logical roots", other),
            Err(err) if can_use_cached_grant_derived_snapshot(err.as_error()) => {
                self.cached_logical_roots_snapshot_with_failure()
            }
            Err(err) => Err(err),
        }
    }

    fn cached_host_object_grants_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<GrantedMountRoot>, SourceFailure> {
        self.with_cache_mut(|cache| {
            Ok::<Vec<GrantedMountRoot>, CnxError>(
                cache
                    .grants
                    .clone()
                    .unwrap_or_else(|| self.config.host_object_grants.clone()),
            )
        })
        .map_err(SourceFailure::from)
    }

    async fn host_object_grants_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<GrantedMountRoot>, SourceFailure> {
        let Some(client) = self.existing_client_with_failure().await? else {
            return self.cached_host_object_grants_snapshot_with_failure();
        };
        let result = Self::call_worker_with_failure(
            &client,
            SourceWorkerRequest::HostObjectGrantsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await;
        match result {
            Ok(SourceWorkerResponse::HostObjectGrants(grants)) => {
                self.with_cache_mut(|cache| {
                    cache.grants = Some(grants.clone());
                });
                Ok(grants)
            }
            Ok(other) => unexpected_source_worker_response_result("for host object grants", other),
            Err(err) if can_use_cached_grant_derived_snapshot(err.as_error()) => {
                self.cached_host_object_grants_snapshot_with_failure()
            }
            Err(err) => Err(err),
        }
    }

    #[cfg(test)]
    async fn host_object_grants_version_snapshot_with_failure(
        &self,
    ) -> std::result::Result<u64, SourceFailure> {
        match Self::call_worker_with_failure(
            &self.client_with_failure().await?,
            SourceWorkerRequest::HostObjectGrantsVersionSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        {
            Ok(SourceWorkerResponse::HostObjectGrantsVersion(version)) => {
                self.with_cache_mut(|cache| {
                    cache.host_object_grants_version = Some(version);
                });
                Ok(version)
            }
            Ok(other) => {
                unexpected_source_worker_response_result("for host object grants version", other)
            }
            Err(err) => Err(err),
        }
    }

    async fn status_snapshot_with_failure(
        &self,
    ) -> std::result::Result<SourceStatusSnapshot, SourceFailure> {
        let response = self
            .with_started_retry_with_failure(|client| async move {
                #[cfg(test)]
                if let Some(err) = take_source_worker_status_error_hook() {
                    return Err(SourceFailure::from(err));
                }
                Self::call_worker_with_failure(
                    &client,
                    SourceWorkerRequest::StatusSnapshot,
                    SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
                )
                .await
            })
            .await?;
        match response {
            SourceWorkerResponse::StatusSnapshot(snapshot) => {
                self.with_cache_mut(|cache| {
                    cache.status = Some(snapshot.clone());
                });
                Ok(snapshot)
            }
            other => unexpected_source_worker_response_result("for status snapshot", other),
        }
    }

    #[cfg(test)]
    async fn lifecycle_state_label_with_failure(
        &self,
    ) -> std::result::Result<String, SourceFailure> {
        match Self::call_worker_with_failure(
            &self.client_with_failure().await?,
            SourceWorkerRequest::LifecycleState,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        {
            Ok(SourceWorkerResponse::LifecycleState(state)) => {
                self.with_cache_mut(|cache| {
                    cache.lifecycle_state = Some(state.clone());
                });
                Ok(state)
            }
            Ok(other) => unexpected_source_worker_response_result("for lifecycle state", other),
            Err(err) => Err(err),
        }
    }

    #[cfg(test)]
    async fn on_control_frame_with_timeouts_for_tests(
        &self,
        envelopes: Vec<ControlEnvelope>,
        total_timeout: Duration,
        rpc_timeout: Duration,
    ) -> Result<()> {
        self.on_control_frame_with_timeouts(envelopes, total_timeout, rpc_timeout)
            .await
            .map_err(SourceFailure::into_error)
    }

    async fn scheduled_source_group_ids_with_failure(
        &self,
    ) -> std::result::Result<Option<std::collections::BTreeSet<String>>, SourceFailure> {
        match self
            .scheduled_group_ids_with_timeout(SourceWorkerRequest::ScheduledSourceGroupIds)
            .await?
        {
            SourceWorkerResponse::ScheduledGroupIds(groups) => Ok(self.with_cache_mut(|cache| {
                merge_cached_local_scheduled_groups(
                    &self.node_id,
                    groups.map(|groups| groups.into_iter().collect()),
                    &cache.scheduled_source_groups_by_node,
                    &cache.replay_recovery_scheduled_source_groups_by_node,
                )
            })),
            other => unexpected_source_worker_response_result("for scheduled source groups", other),
        }
    }

    #[cfg(test)]
    pub async fn scheduled_source_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.scheduled_source_group_ids_with_failure()
            .await
            .map_err(SourceFailure::into_error)
    }

    async fn scheduled_scan_group_ids_with_failure(
        &self,
    ) -> std::result::Result<Option<std::collections::BTreeSet<String>>, SourceFailure> {
        match self
            .scheduled_group_ids_with_timeout(SourceWorkerRequest::ScheduledScanGroupIds)
            .await?
        {
            SourceWorkerResponse::ScheduledGroupIds(groups) => Ok(self.with_cache_mut(|cache| {
                merge_cached_local_scheduled_groups(
                    &self.node_id,
                    groups.map(|groups| groups.into_iter().collect()),
                    &cache.scheduled_scan_groups_by_node,
                    &cache.replay_recovery_scheduled_scan_groups_by_node,
                )
            })),
            other => unexpected_source_worker_response_result("for scheduled scan groups", other),
        }
    }

    #[cfg(test)]
    pub async fn scheduled_scan_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.scheduled_scan_group_ids_with_failure()
            .await
            .map_err(SourceFailure::into_error)
    }

    async fn scheduled_group_ids_with_timeout(
        &self,
        request: SourceWorkerRequest,
    ) -> std::result::Result<SourceWorkerResponse, SourceFailure> {
        let deadline = clip_retry_deadline(
            std::time::Instant::now() + SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
            SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
        );
        let mut machine = SourceScheduledGroupIdsMachine::new(deadline);
        drive_source_machine_loop(
            machine.start()?,
            |effect| async {
                match effect {
                    SourceScheduledGroupIdsEffect::Attempt { timeout } => {
                        SourceOperationLoopStep::Event(
                            SourceScheduledGroupIdsEvent::AttemptCompleted {
                                rpc_result: self
                                    .execute_scheduled_group_ids_attempt(&request, timeout)
                                    .await,
                                after: self.current_source_progress_state(),
                            },
                        )
                    }
                    SourceScheduledGroupIdsEffect::Reconnect => {
                        match self.reconnect_shared_worker_client_with_failure().await {
                            Ok(()) => SourceOperationLoopStep::Event(
                                SourceScheduledGroupIdsEvent::ReconnectCompleted,
                            ),
                            Err(err) => SourceOperationLoopStep::Fail(err),
                        }
                    }
                    SourceScheduledGroupIdsEffect::Wait { after } => {
                        self.wait_for_source_progress_after(
                            after,
                            SourceWaitFor::ControlFlowRetry,
                            deadline,
                        )
                        .await;
                        SourceOperationLoopStep::Event(SourceScheduledGroupIdsEvent::WaitCompleted)
                    }
                    SourceScheduledGroupIdsEffect::Complete(response) => {
                        SourceOperationLoopStep::Complete(response)
                    }
                    SourceScheduledGroupIdsEffect::Fail(failure) => {
                        SourceOperationLoopStep::Fail(failure)
                    }
                }
            },
            |event| machine.advance(event),
        )
        .await
    }

    async fn source_primary_by_group_snapshot_with_failure(
        &self,
    ) -> std::result::Result<std::collections::BTreeMap<String, String>, SourceFailure> {
        match Self::call_worker_with_failure(
            &self.client_with_failure().await?,
            SourceWorkerRequest::SourcePrimaryByGroupSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::SourcePrimaryByGroup(groups) => {
                self.with_cache_mut(|cache| {
                    cache.source_primary_by_group = Some(groups.clone());
                });
                Ok(groups)
            }
            other => unexpected_source_worker_response_result("for primary groups", other),
        }
    }

    async fn last_force_find_runner_by_group_snapshot_with_failure(
        &self,
    ) -> std::result::Result<std::collections::BTreeMap<String, String>, SourceFailure> {
        match Self::call_worker_with_failure(
            &self.client_with_failure().await?,
            SourceWorkerRequest::LastForceFindRunnerByGroupSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::LastForceFindRunnerByGroup(groups) => {
                self.with_cache_mut(|cache| {
                    cache.last_force_find_runner_by_group = Some(groups.clone());
                });
                Ok(groups)
            }
            other => unexpected_source_worker_response_result("for last force-find runner", other),
        }
    }

    async fn force_find_inflight_groups_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<String>, SourceFailure> {
        match Self::call_worker_with_failure(
            &self.client_with_failure().await?,
            SourceWorkerRequest::ForceFindInflightGroupsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::ForceFindInflightGroups(groups) => {
                self.with_cache_mut(|cache| {
                    cache.force_find_inflight_groups = Some(groups.clone());
                });
                Ok(groups)
            }
            other => {
                unexpected_source_worker_response_result("for force-find inflight groups", other)
            }
        }
    }

    async fn resolve_group_id_for_object_ref_with_failure(
        &self,
        object_ref: &str,
    ) -> std::result::Result<Option<String>, SourceFailure> {
        match Self::call_worker_with_failure(
            &self.client_with_failure().await?,
            SourceWorkerRequest::ResolveGroupIdForObjectRef {
                object_ref: object_ref.to_string(),
            },
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await?
        {
            SourceWorkerResponse::ResolveGroupIdForObjectRef(group) => Ok(group),
            other => unexpected_source_worker_response_result("for resolve group", other),
        }
    }

    async fn execute_force_find_attempt(
        &self,
        params: &InternalQueryRequest,
        target_node: &NodeId,
        timeout: Duration,
    ) -> std::result::Result<Vec<Event>, CnxError> {
        #[cfg(test)]
        let worker_instance_id = self.worker_instance_id_for_tests().await;
        self.with_started_retry_with_failure(|client| {
            let params = params.clone();
            let target_node = target_node.clone();
            async move {
                if debug_force_find_route_capture_enabled() {
                    eprintln!(
                        "fs_meta_source_worker_client: force_find rpc begin target_node={} selected_group={:?} recursive={} max_depth={:?} path={}",
                        target_node.0,
                        params.scope.selected_group,
                        params.scope.recursive,
                        params.scope.max_depth,
                        String::from_utf8_lossy(&params.scope.path)
                    );
                }
                #[cfg(test)]
                if let Some(err) = take_source_worker_force_find_error_queue_hook(worker_instance_id)
                {
                    return Err(SourceFailure::from(err));
                }
                let response = Self::call_worker_with_failure(
                    &client,
                    SourceWorkerRequest::ForceFind {
                        request: params.clone(),
                    },
                    timeout,
                )
                .await;
                match response {
                    Err(err) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_source_worker_client: force_find rpc failed target_node={} err={}",
                                target_node.0,
                                err.as_error()
                            );
                        }
                        Err(err)
                    }
                    Ok(SourceWorkerResponse::Events(events)) => {
                        if debug_force_find_route_capture_enabled() {
                            eprintln!(
                                "fs_meta_source_worker_client: force_find rpc done target_node={} events={} origins={:?}",
                                target_node.0,
                                events.len(),
                                summarize_event_counts_by_origin(&events)
                            );
                        }
                        Ok(events)
                    }
                    Ok(other) => Err(unexpected_source_worker_response_failure(
                        "for force-find",
                        other,
                    )),
                }
            }
        })
        .await
        .map_err(SourceFailure::into_error)
    }

    async fn execute_scheduled_group_ids_attempt(
        &self,
        request: &SourceWorkerRequest,
        timeout: Duration,
    ) -> std::result::Result<SourceWorkerResponse, CnxError> {
        self.with_started_retry_with_failure(|client| {
            let request = request.clone();
            async move {
                #[cfg(test)]
                if let Some(err) = take_source_worker_scheduled_groups_error_hook() {
                    return Err(SourceFailure::from(err));
                }
                Self::call_worker_with_failure(&client, request, timeout).await
            }
        })
        .await
        .map_err(SourceFailure::into_error)
    }

    async fn execute_observability_snapshot_attempt(
        &self,
        timeout: Duration,
    ) -> std::result::Result<SourceWorkerResponse, CnxError> {
        self.with_started_retry_with_failure(|client| async move {
            #[cfg(test)]
            record_source_worker_observability_rpc_attempt();
            #[cfg(test)]
            if let Some(delay) = source_worker_observability_delay_hook() {
                tokio::time::sleep(delay).await;
            }
            #[cfg(test)]
            if let Some(err) = take_source_worker_observability_error_hook() {
                return Err(SourceFailure::from(err));
            }
            Self::call_worker_with_failure(
                &client,
                SourceWorkerRequest::ObservabilitySnapshot,
                timeout,
            )
            .await
        })
        .await
        .map_err(SourceFailure::into_error)
    }

    async fn execute_start_attempt(&self, timeout: Duration) -> std::result::Result<(), CnxError> {
        #[cfg(test)]
        maybe_pause_before_ensure_started().await;
        #[cfg(test)]
        let injected =
            take_source_worker_start_error_queue_hook(self.worker_instance_id_for_tests().await);
        #[cfg(not(test))]
        let injected = None::<CnxError>;
        #[cfg(test)]
        let injected_delay = take_source_worker_start_delay_hook();
        let worker = self.worker_client().await;
        let start_result = match injected {
            Some(err) => Err(SourceFailure::from(err)),
            None => map_source_failure_timeout_result(
                tokio::time::timeout(timeout, async {
                    #[cfg(test)]
                    if let Some(delay) = injected_delay {
                        tokio::time::sleep(delay).await;
                    }
                    worker.ensure_started().await.map_err(SourceFailure::from)
                })
                .await,
            ),
        };
        if let Err(err) = &start_result {
            eprintln!(
                "fs_meta_source_worker_client: on_control_frame retry node={} err={}",
                self.node_id.0,
                err.as_error()
            );
        }
        start_result.map_err(SourceFailure::into_error)
    }

    async fn execute_on_control_frame_request_attempt(
        &self,
        envelopes: &[ControlEnvelope],
        timeout: Duration,
        attempt_kind: SourceControlFrameRequestAttemptKind,
    ) -> std::result::Result<SourceWorkerResponse, CnxError> {
        map_source_failure_timeout_result(
            tokio::time::timeout(
                timeout,
                self.with_started_retry_with_failure(|client| {
                    let envelopes = envelopes.to_vec();
                    async move {
                        if matches!(
                            attempt_kind,
                            SourceControlFrameRequestAttemptKind::ControlFrame
                        ) {
                            #[cfg(test)]
                            {
                                maybe_pause_before_on_control_frame_rpc().await;
                                if let Some(err) = take_on_control_frame_error_hook(
                                    self.worker_instance_id_for_tests().await,
                                ) {
                                    return Err(SourceFailure::from(err));
                                }
                            }
                        }
                        Self::call_worker_with_failure(
                            &client,
                            SourceWorkerRequest::OnControlFrame { envelopes },
                            timeout,
                        )
                        .await
                    }
                }),
            )
            .await,
        )
        .map_err(SourceFailure::into_error)
    }

    async fn execute_update_logical_roots_attempt(
        &self,
        roots: &[RootSpec],
        timeout: Duration,
    ) -> std::result::Result<SourceWorkerResponse, CnxError> {
        self.with_started_retry_with_failure(|client| {
            let roots = roots.to_vec();
            async move {
                #[cfg(test)]
                maybe_pause_before_update_logical_roots_rpc().await;
                #[cfg(test)]
                if let Some(err) = take_update_logical_roots_error_hook() {
                    return Err(SourceFailure::from(err));
                }
                Self::call_worker_with_failure(
                    &client,
                    SourceWorkerRequest::UpdateLogicalRoots { roots },
                    timeout,
                )
                .await
            }
        })
        .await
        .map_err(SourceFailure::into_error)
    }

    async fn execute_update_logical_roots_wait(
        &self,
        after: SourceProgressState,
        deadline: std::time::Instant,
    ) {
        self.wait_for_source_progress_after(after, SourceWaitFor::LogicalRoots, deadline)
            .await;
    }

    async fn force_find_with_failure(
        &self,
        params: InternalQueryRequest,
    ) -> std::result::Result<Vec<Event>, SourceFailure> {
        let target_node = self.node_id.clone();
        let deadline = clip_retry_deadline(
            std::time::Instant::now() + SOURCE_WORKER_FORCE_FIND_TIMEOUT,
            SOURCE_WORKER_FORCE_FIND_TIMEOUT,
        );
        let machine = SourceForceFindMachine::new(deadline);
        drive_source_machine_loop(
            machine.start()?,
            |effect| async {
                match effect {
                    SourceForceFindEffect::Attempt { timeout } => {
                        SourceOperationLoopStep::Event(SourceForceFindEvent::AttemptCompleted {
                            rpc_result: self
                                .execute_force_find_attempt(&params, &target_node, timeout)
                                .await,
                        })
                    }
                    SourceForceFindEffect::RetryBackoff { delay } => {
                        tokio::time::sleep(std::cmp::min(
                            delay,
                            deadline.saturating_duration_since(std::time::Instant::now()),
                        ))
                        .await;
                        SourceOperationLoopStep::Event(SourceForceFindEvent::WaitCompleted)
                    }
                    SourceForceFindEffect::Complete(events) => {
                        let _ = self
                            .last_force_find_runner_by_group_snapshot_with_failure()
                            .await;
                        SourceOperationLoopStep::Complete(events)
                    }
                    SourceForceFindEffect::Fail(failure) => SourceOperationLoopStep::Fail(failure),
                }
            },
            |event| machine.advance(event),
        )
        .await
    }

    pub(crate) async fn publish_manual_rescan_signal_with_failure(
        &self,
    ) -> std::result::Result<(), SourceFailure> {
        self.manual_rescan_signal
            .emit(&self.node_id.0)
            .await
            .map(|_| {
                self.bump_cached_materialized_read_cache_epoch();
            })
            .map_err(|err| {
                SourceFailure::from(CnxError::Internal(format!(
                    "publish manual rescan signal failed: {err}"
                )))
            })
    }

    #[cfg(test)]
    pub async fn on_control_frame(&self, envelopes: Vec<ControlEnvelope>) -> Result<()> {
        self.on_control_frame_with_timeouts(
            envelopes,
            SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        .map_err(SourceFailure::into_error)
    }

    async fn observe_control_frame(
        &self,
        envelopes: &[ControlEnvelope],
        total_timeout: Duration,
        rpc_timeout: Duration,
    ) -> Result<SourceControlFrameFacts> {
        let control_envelopes = Arc::<[ControlEnvelope]>::from({
            let control_state = self.control_state.lock().await;
            control_state.merge_replay_envelopes_for_next_wave(envelopes)
        });
        let operation_deadline =
            clip_retry_deadline(std::time::Instant::now() + total_timeout, total_timeout);
        let decoded_signals = source_control_signals_from_envelopes(&control_envelopes)
            .map(SourceControlFrameDecodedSignals::Decoded)
            .unwrap_or(SourceControlFrameDecodedSignals::DecodeFailed);
        let (fail_fast_generation_one_activate_replay, initial_step) = match &decoded_signals {
            SourceControlFrameDecodedSignals::Decoded(signals) => {
                let control_state = self.control_state.lock().await;
                let initial_step = match control_state.classify_tick_only_wave(signals) {
                    Some(SourceControlFrameTickOnlyDisposition::RetainedTickFastPath {
                        signals,
                    }) => Some(SourceControlFrameInitialStep::run(
                        RetainedTickFastPathLane { signals },
                    )),
                    Some(SourceControlFrameTickOnlyDisposition::RetainedTickReplay) => {
                        Some(SourceControlFrameInitialStep::run(RetainedTickReplayLane {
                            deadline: operation_deadline,
                        }))
                    }
                    None => None,
                };
                (
                    control_state.matches_generation_one_activate_wave(signals),
                    initial_step,
                )
            }
            SourceControlFrameDecodedSignals::DecodeFailed => (false, None),
        };
        let signal_policies = match &decoded_signals {
            SourceControlFrameDecodedSignals::Decoded(signals) => {
                SourceControlFrameSignalPolicies::from_signals(signals)
            }
            SourceControlFrameDecodedSignals::DecodeFailed => {
                SourceControlFrameSignalPolicies::standard()
            }
        };
        let timeout_reset_policy = signal_policies.timeout_reset_policy;
        let attempt_timeout_policy = signal_policies.attempt_timeout_policy;
        let primed_local_schedule = match &decoded_signals {
            SourceControlFrameDecodedSignals::Decoded(signals) => {
                Some(self.with_cache_mut(|cache| {
                    let mut observed_cache = cache.clone();
                    prime_cached_schedule_from_control_signals(
                        &mut observed_cache,
                        &self.node_id,
                        signals,
                        &self.config.roots,
                        &self.config.host_object_grants,
                    )
                }))
            }
            SourceControlFrameDecodedSignals::DecodeFailed => None,
        };
        let post_ack_refresh_requirement = signal_policies.post_ack_refresh_requirement;
        let existing_client_present = if initial_step.is_none() {
            self.existing_client_with_failure()
                .await
                .map_err(SourceFailure::into_error)?
                .is_some()
        } else {
            false
        };
        let bridge_reset_policy = if fail_fast_generation_one_activate_replay
            || is_restart_deferred_retire_pending_deactivate_batch(&control_envelopes)
        {
            SourceControlFrameBridgeResetPolicy::ReconnectThenFail
        } else if total_timeout <= SOURCE_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT {
            SourceControlFrameBridgeResetPolicy::FailImmediately
        } else {
            SourceControlFrameBridgeResetPolicy::RetryAfterReconnect
        };
        Ok(SourceControlFrameFacts {
            envelopes: control_envelopes,
            decoded_signals,
            primed_local_schedule,
            post_ack_refresh_requirement,
            initial_step,
            operation_deadline,
            rpc_timeout,
            existing_client_present,
            bridge_reset_policy,
            timeout_reset_policy,
            attempt_timeout_policy,
        })
    }

    async fn on_control_frame_with_timeouts(
        &self,
        envelopes: Vec<ControlEnvelope>,
        total_timeout: Duration,
        rpc_timeout: Duration,
    ) -> std::result::Result<(), SourceFailure> {
        let _inflight = self.begin_control_op();
        let _serial = self.control_ops_serial.lock().await;
        #[cfg(test)]
        notify_source_worker_control_frame_started();
        let facts = self
            .observe_control_frame(&envelopes, total_timeout, rpc_timeout)
            .await?;
        let decoded_signals = facts.decoded_signals.clone();
        let (control_machine, initial_effect) = SourceControlFrameMachine::start(facts)?;
        eprintln!(
            "fs_meta_source_worker_client: on_control_frame begin node={} envelopes={}",
            self.node_id.0,
            envelopes.len()
        );
        if debug_control_scope_capture_enabled() {
            match &decoded_signals {
                SourceControlFrameDecodedSignals::Decoded(signals) => eprintln!(
                    "fs_meta_source_worker_client: on_control_frame summary node={} signals={:?}",
                    self.node_id.0,
                    summarize_source_control_signals(&signals)
                ),
                SourceControlFrameDecodedSignals::DecodeFailed => eprintln!(
                    "fs_meta_source_worker_client: on_control_frame summary node={} decode_err={}",
                    self.node_id.0, "decode failed"
                ),
            }
        }
        SourceControlFrameExecutor { handle: self }
            .execute_control_frame(control_machine, initial_effect)
            .await
    }

    async fn trigger_rescan_when_ready_epoch_with_failure(
        &self,
    ) -> std::result::Result<u64, SourceFailure> {
        let response = self
            .with_started_retry_with_failure(|client| async move {
                Self::call_worker_with_failure(
                    &client,
                    SourceWorkerRequest::TriggerRescanWhenReadyEpoch,
                    SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
                )
                .await
            })
            .await?;
        let epoch = match response {
            SourceWorkerResponse::RescanRequestEpoch(epoch) => epoch,
            other => {
                return Err(unexpected_source_worker_response_failure(
                    "for trigger_rescan_when_ready_epoch",
                    other,
                ));
            }
        };
        self.with_cache_mut(|cache| {
            let last_audit_completed_at_us = cache
                .status
                .as_ref()
                .map(source_status_rescan_completion_marker)
                .unwrap_or_default();
            let (published_batches, last_published_at_us) = cached_source_publication_marker(cache);
            cache.rescan_request_published_batches = published_batches;
            cache.rescan_request_last_published_at_us = last_published_at_us;
            cache.rescan_request_last_audit_completed_at_us = last_audit_completed_at_us;
            cache.rescan_request_epoch = cache.rescan_request_epoch.max(epoch);
        });
        self.wait_for_rescan_observed_epoch_with_failure(
            epoch,
            SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
        )
        .await?;
        Ok(epoch)
    }

    async fn submit_rescan_when_ready_epoch_with_failure(
        &self,
    ) -> std::result::Result<u64, SourceFailure> {
        let response = self
            .with_started_once_with_failure(|client| async move {
                Self::call_worker_with_failure(
                    &client,
                    SourceWorkerRequest::TriggerRescanWhenReadyEpoch,
                    SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
                )
                .await
            })
            .await?;
        let epoch = match response {
            SourceWorkerResponse::RescanRequestEpoch(epoch) => epoch,
            other => {
                return Err(unexpected_source_worker_response_failure(
                    "for submit_rescan_when_ready_epoch",
                    other,
                ));
            }
        };
        self.with_cache_mut(|cache| {
            let last_audit_completed_at_us = cache
                .status
                .as_ref()
                .map(source_status_rescan_completion_marker)
                .unwrap_or_default();
            let (published_batches, last_published_at_us) = cached_source_publication_marker(cache);
            cache.rescan_request_published_batches = published_batches;
            cache.rescan_request_last_published_at_us = last_published_at_us;
            cache.rescan_request_last_audit_completed_at_us = last_audit_completed_at_us;
            cache.rescan_request_epoch = cache.rescan_request_epoch.max(epoch);
        });
        Ok(epoch)
    }

    async fn progress_snapshot_with_timeout_with_failure(
        &self,
        timeout: Duration,
    ) -> std::result::Result<crate::source::SourceProgressSnapshot, SourceFailure> {
        let response = self
            .with_started_retry_with_failure(|client| async move {
                Self::call_worker_with_failure(
                    &client,
                    SourceWorkerRequest::ProgressSnapshot,
                    timeout,
                )
                .await
            })
            .await?;
        match response {
            SourceWorkerResponse::ProgressSnapshot(snapshot) => {
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_source_worker_client: progress_snapshot reply node={} rescan_observed_epoch={} published_groups={:?}",
                        self.node_id.0,
                        snapshot.rescan_observed_epoch,
                        snapshot.published_group_epoch
                    );
                }
                self.with_cache_mut(|cache| {
                    let previous_rescan_observed_epoch = cache.rescan_observed_epoch;
                    cache.rescan_observed_epoch = cache
                        .rescan_observed_epoch
                        .max(snapshot.rescan_observed_epoch);
                    if cache.rescan_observed_epoch > previous_rescan_observed_epoch {
                        cache.last_live_observability_snapshot_at = None;
                    }
                });
                Ok(snapshot)
            }
            other => unexpected_source_worker_response_result("for progress_snapshot", other),
        }
    }

    async fn close_with_failure(&self) -> std::result::Result<(), SourceFailure> {
        #[cfg(test)]
        notify_source_worker_close_started();
        self.wait_for_control_ops_to_drain(SOURCE_WORKER_CLOSE_DRAIN_TIMEOUT)
            .await;
        if Arc::strong_count(&self._shared) > 1 {
            return Ok(());
        }
        self.worker_client()
            .await
            .shutdown(Duration::from_secs(2))
            .await
            .map_err(SourceFailure::from)?;
        self.with_cache_mut(|cache| {
            cache.lifecycle_state = Some("closed".to_string());
        });
        Ok(())
    }

    #[cfg(test)]
    pub async fn close(&self) -> Result<()> {
        self.close_with_failure()
            .await
            .map_err(SourceFailure::into_error)
    }

    #[cfg(test)]
    pub(crate) async fn shutdown_shared_worker_for_tests(&self) -> Result<()> {
        self.worker_client()
            .await
            .shutdown(Duration::from_secs(2))
            .await?;
        self.with_cache_mut(|cache| {
            cache.lifecycle_state = Some("closed".to_string());
        });
        self.control_state.lock().await.arm_replay();
        self.bump_source_progress(SourceProgressReason::WorkerClientReplaced);
        Ok(())
    }

    async fn observability_snapshot_nonblocking_with_access_path(
        &self,
    ) -> std::result::Result<(SourceObservabilitySnapshot, bool), SourceFailure> {
        if self.control_op_inflight() {
            let snapshot =
                self.degraded_observability_snapshot_from_cache("source worker control in flight");
            if debug_control_scope_capture_enabled() {
                eprintln!(
                    "fs_meta_source_worker_client: observability_snapshot cache_fallback node={} reason=control_inflight {}",
                    self.node_id.0,
                    summarize_source_observability_snapshot(&snapshot)
                );
            }
            return Ok((snapshot, true));
        }
        let read_deadline = std::time::Instant::now() + SOURCE_WORKER_OBSERVABILITY_RPC_TIMEOUT;
        self.replay_retained_control_state_before_observability_read_until(read_deadline)
            .await?;
        let Some(_client) = self.existing_client_with_failure().await? else {
            let snapshot =
                self.degraded_observability_snapshot_from_cache("source worker status not started");
            if debug_control_scope_capture_enabled() {
                eprintln!(
                    "fs_meta_source_worker_client: observability_snapshot cache_fallback node={} reason=not_started {}",
                    self.node_id.0,
                    summarize_source_observability_snapshot(&snapshot)
                );
            }
            return Ok((snapshot, true));
        };
        if let Some(snapshot) = self.recent_nonblocking_observability_cache_snapshot() {
            self.log_observability_cache_fallback("recent_live_cache", &snapshot);
            return Ok((snapshot, true));
        }
        if let Some(snapshot) = self.stable_status_root_health_observability_cache_snapshot() {
            self.log_observability_cache_fallback("stable_status_root_health_cache", &snapshot);
            return Ok((snapshot, true));
        }
        let _read_serial = self.observability_read_serial.lock().await;
        if self.control_op_inflight() {
            let snapshot =
                self.degraded_observability_snapshot_from_cache("source worker control in flight");
            if debug_control_scope_capture_enabled() {
                eprintln!(
                    "fs_meta_source_worker_client: observability_snapshot cache_fallback node={} reason=control_inflight {}",
                    self.node_id.0,
                    summarize_source_observability_snapshot(&snapshot)
                );
            }
            return Ok((snapshot, true));
        }
        if let Some(snapshot) = self.recent_nonblocking_observability_cache_snapshot() {
            self.log_observability_cache_fallback("recent_live_cache", &snapshot);
            return Ok((snapshot, true));
        }
        if let Some(snapshot) = self.stable_status_root_health_observability_cache_snapshot() {
            self.log_observability_cache_fallback("stable_status_root_health_cache", &snapshot);
            return Ok((snapshot, true));
        }
        let trace_id = next_source_status_trace_id();
        let mut trace_guard =
            SourceStatusTraceGuard::new(self.node_id.0.clone(), trace_id, "before_rpc_await");
        eprintln!(
            "fs_meta_source_worker_client: observability_snapshot begin node={} timeout_ms={} trace_id={}",
            self.node_id.0,
            SOURCE_WORKER_OBSERVABILITY_RPC_TIMEOUT.as_millis(),
            trace_id
        );
        let snapshot_timeout = match classify_source_retry_budget(read_deadline) {
            SourceRetryBudgetDisposition::Exhausted => {
                return Err(SourceFailure::retry_budget_exhausted(
                    SourceRetryBudgetExhaustionKind::OperationAttempt,
                ));
            }
            SourceRetryBudgetDisposition::Remaining(remaining) => {
                remaining.min(SOURCE_WORKER_OBSERVABILITY_RPC_TIMEOUT)
            }
        };
        let result = self
            .observability_snapshot_with_timeout_with_failure(snapshot_timeout)
            .await;
        trace_guard.phase("after_rpc_await");
        eprintln!(
            "fs_meta_source_worker_client: observability_snapshot done node={} ok={} trace_id={}",
            self.node_id.0,
            result.is_ok(),
            trace_id
        );
        trace_guard.complete();
        result.map(|snapshot| (snapshot, false))
    }

    fn recent_nonblocking_observability_cache_snapshot(
        &self,
    ) -> Option<SourceObservabilitySnapshot> {
        let snapshot = self.with_cache_mut(|cache| {
            cache
                .last_live_observability_snapshot_at
                .filter(|last| last.elapsed() < SOURCE_WORKER_NONBLOCKING_OBSERVABILITY_CACHE_TTL)
                .and_then(|_| build_cached_worker_observability_snapshot(cache))
        })?;
        (!recent_cached_source_observability_snapshot_is_incomplete(&snapshot)).then_some(snapshot)
    }

    fn stable_status_root_health_observability_cache_snapshot(
        &self,
    ) -> Option<SourceObservabilitySnapshot> {
        let snapshot = self.with_cache_mut(|cache| {
            let last_live = cache.last_live_observability_snapshot_at?;
            if last_live.elapsed() < SOURCE_WORKER_NONBLOCKING_OBSERVABILITY_CACHE_TTL {
                return None;
            }
            build_cached_worker_observability_snapshot(cache)
        })?;
        source_observability_snapshot_has_configured_status_root_health(&snapshot)
            .then_some(snapshot)
    }

    fn log_observability_cache_fallback(
        &self,
        reason: &str,
        snapshot: &SourceObservabilitySnapshot,
    ) {
        if debug_control_scope_capture_enabled() {
            eprintln!(
                "fs_meta_source_worker_client: observability_snapshot cache_fallback node={} reason={} {}",
                self.node_id.0,
                reason,
                summarize_source_observability_snapshot(snapshot)
            );
        }
    }

    async fn observability_snapshot_nonblocking_for_status_route(
        &self,
    ) -> (SourceObservabilitySnapshot, bool) {
        match self
            .observability_snapshot_nonblocking_with_access_path()
            .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                let snapshot = self.degraded_observability_snapshot_from_cache(format!(
                    "source worker unavailable: {}",
                    err.as_error()
                ));
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_source_worker_client: observability_snapshot cache_fallback node={} reason=worker_unavailable err={} {}",
                        self.node_id.0,
                        err.as_error(),
                        summarize_source_observability_snapshot(&snapshot)
                    );
                }
                (snapshot, true)
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceObservabilitySnapshot {
    pub lifecycle_state: String,
    pub host_object_grants_version: u64,
    pub grants: Vec<GrantedMountRoot>,
    pub logical_roots: Vec<RootSpec>,
    pub status: SourceStatusSnapshot,
    pub source_primary_by_group: std::collections::BTreeMap<String, String>,
    pub last_force_find_runner_by_group: std::collections::BTreeMap<String, String>,
    pub force_find_inflight_groups: Vec<String>,
    pub scheduled_source_groups_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub scheduled_scan_groups_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub last_control_frame_signals_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_batches_by_node: std::collections::BTreeMap<String, u64>,
    pub published_events_by_node: std::collections::BTreeMap<String, u64>,
    pub published_control_events_by_node: std::collections::BTreeMap<String, u64>,
    pub published_data_events_by_node: std::collections::BTreeMap<String, u64>,
    pub last_published_at_us_by_node: std::collections::BTreeMap<String, u64>,
    pub last_published_origins_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_path_capture_target: Option<String>,
    pub enqueued_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub pending_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub yielded_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub summarized_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
}

pub(crate) struct SourceObservabilitySnapshotParts {
    pub lifecycle_state: String,
    pub host_object_grants_version: u64,
    pub grants: Vec<GrantedMountRoot>,
    pub logical_roots: Vec<RootSpec>,
    pub status: SourceStatusSnapshot,
    pub source_primary_by_group: std::collections::BTreeMap<String, String>,
    pub last_force_find_runner_by_group: std::collections::BTreeMap<String, String>,
    pub force_find_inflight_groups: Vec<String>,
    pub recovery: SourceObservabilityRecoveryProjection,
    pub last_control_frame_signals_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_batches_by_node: std::collections::BTreeMap<String, u64>,
    pub published_events_by_node: std::collections::BTreeMap<String, u64>,
    pub published_control_events_by_node: std::collections::BTreeMap<String, u64>,
    pub published_data_events_by_node: std::collections::BTreeMap<String, u64>,
    pub last_published_at_us_by_node: std::collections::BTreeMap<String, u64>,
    pub last_published_origins_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_path_capture_target: Option<String>,
    pub enqueued_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub pending_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub yielded_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub summarized_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
}

pub(crate) struct SourceObservabilityLiveSourceContext {
    pub lifecycle_state: String,
    pub host_object_grants_version: u64,
    pub grants: Vec<GrantedMountRoot>,
    pub logical_roots: Vec<RootSpec>,
    pub status: SourceStatusSnapshot,
    pub source_primary_by_group: std::collections::BTreeMap<String, String>,
    pub last_force_find_runner_by_group: std::collections::BTreeMap<String, String>,
    pub force_find_inflight_groups: Vec<String>,
    pub recovery: SourceObservabilityRecoveryProjection,
    pub last_control_frame_signals_snapshot: Vec<String>,
}

pub(crate) struct SourceObservabilityLiveAugment {
    pub last_control_frame_signals_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_batches_by_node: std::collections::BTreeMap<String, u64>,
    pub published_events_by_node: std::collections::BTreeMap<String, u64>,
    pub published_control_events_by_node: std::collections::BTreeMap<String, u64>,
    pub published_data_events_by_node: std::collections::BTreeMap<String, u64>,
    pub last_published_at_us_by_node: std::collections::BTreeMap<String, u64>,
    pub last_published_origins_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_path_capture_target: Option<String>,
    pub enqueued_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub pending_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub yielded_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub summarized_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
    pub published_path_origin_counts_by_node: std::collections::BTreeMap<String, Vec<String>>,
}

#[derive(Default)]
pub(crate) struct SourceObservabilityPublicationView {
    pub last_control_frame_signals: Vec<String>,
    pub keep_zero_publication_counters: bool,
    pub published_batch_count: u64,
    pub published_event_count: u64,
    pub published_control_event_count: u64,
    pub published_data_event_count: u64,
    pub last_published_at_us: Option<u64>,
    pub last_published_origins: Vec<String>,
    pub published_origin_counts: std::collections::BTreeMap<String, u64>,
    pub published_path_capture_target: Option<String>,
    pub enqueued_path_origin_counts: std::collections::BTreeMap<String, u64>,
    pub pending_path_origin_counts: std::collections::BTreeMap<String, u64>,
    pub yielded_path_origin_counts: std::collections::BTreeMap<String, u64>,
    pub summarized_path_origin_counts: std::collections::BTreeMap<String, u64>,
    pub published_path_origin_counts: std::collections::BTreeMap<String, u64>,
}

pub(crate) fn build_source_observability_snapshot_from_parts(
    parts: SourceObservabilitySnapshotParts,
) -> SourceObservabilitySnapshot {
    SourceObservabilitySnapshot {
        lifecycle_state: parts.lifecycle_state,
        host_object_grants_version: parts.host_object_grants_version,
        grants: parts.grants,
        logical_roots: parts.logical_roots,
        status: parts.status,
        source_primary_by_group: parts.source_primary_by_group,
        last_force_find_runner_by_group: parts.last_force_find_runner_by_group,
        force_find_inflight_groups: parts.force_find_inflight_groups,
        scheduled_source_groups_by_node: parts.recovery.scheduled_source_groups_by_node,
        scheduled_scan_groups_by_node: parts.recovery.scheduled_scan_groups_by_node,
        last_control_frame_signals_by_node: parts.last_control_frame_signals_by_node,
        published_batches_by_node: parts.published_batches_by_node,
        published_events_by_node: parts.published_events_by_node,
        published_control_events_by_node: parts.published_control_events_by_node,
        published_data_events_by_node: parts.published_data_events_by_node,
        last_published_at_us_by_node: parts.last_published_at_us_by_node,
        last_published_origins_by_node: parts.last_published_origins_by_node,
        published_origin_counts_by_node: parts.published_origin_counts_by_node,
        published_path_capture_target: parts.published_path_capture_target,
        enqueued_path_origin_counts_by_node: parts.enqueued_path_origin_counts_by_node,
        pending_path_origin_counts_by_node: parts.pending_path_origin_counts_by_node,
        yielded_path_origin_counts_by_node: parts.yielded_path_origin_counts_by_node,
        summarized_path_origin_counts_by_node: parts.summarized_path_origin_counts_by_node,
        published_path_origin_counts_by_node: parts.published_path_origin_counts_by_node,
    }
}

pub(crate) fn build_source_observability_live_source_context(
    source: &FSMetaSource,
) -> SourceObservabilityLiveSourceContext {
    let node_id = source.node_id();
    let grants = source.snapshot_host_object_grants();
    let stable_host_ref = stable_host_ref_for_node_id(&node_id, &grants);
    let status = source.build_status_snapshot();
    let last_force_find_runner_by_group = source.snapshot_last_force_find_runner_by_group();
    let force_find_inflight_groups = source.snapshot_force_find_inflight_groups();
    let recovery = source_observability_recovery_projection(
        stable_host_ref,
        &status,
        &last_force_find_runner_by_group,
        &force_find_inflight_groups,
        scheduled_groups_by_node(
            &node_id,
            &grants,
            source.snapshot_scheduled_source_group_ids(),
        ),
        scheduled_groups_by_node(
            &node_id,
            &grants,
            source.snapshot_scheduled_scan_group_ids(),
        ),
    );
    SourceObservabilityLiveSourceContext {
        lifecycle_state: source.snapshot_lifecycle_state_label(),
        host_object_grants_version: source.snapshot_host_object_grants_version(),
        grants,
        logical_roots: source.snapshot_logical_roots(),
        status,
        source_primary_by_group: source.snapshot_source_primary_by_group(),
        last_force_find_runner_by_group,
        force_find_inflight_groups,
        recovery,
        last_control_frame_signals_snapshot: source.snapshot_last_control_frame_signals(),
    }
}

pub(crate) fn build_source_observability_snapshot_from_live_context(
    context: SourceObservabilityLiveSourceContext,
    augment: SourceObservabilityLiveAugment,
) -> SourceObservabilitySnapshot {
    build_source_observability_snapshot_from_parts(SourceObservabilitySnapshotParts {
        lifecycle_state: context.lifecycle_state,
        host_object_grants_version: context.host_object_grants_version,
        grants: context.grants,
        logical_roots: context.logical_roots,
        status: context.status,
        source_primary_by_group: context.source_primary_by_group,
        last_force_find_runner_by_group: context.last_force_find_runner_by_group,
        force_find_inflight_groups: context.force_find_inflight_groups,
        recovery: context.recovery,
        last_control_frame_signals_by_node: augment.last_control_frame_signals_by_node,
        published_batches_by_node: augment.published_batches_by_node,
        published_events_by_node: augment.published_events_by_node,
        published_control_events_by_node: augment.published_control_events_by_node,
        published_data_events_by_node: augment.published_data_events_by_node,
        last_published_at_us_by_node: augment.last_published_at_us_by_node,
        last_published_origins_by_node: augment.last_published_origins_by_node,
        published_origin_counts_by_node: augment.published_origin_counts_by_node,
        published_path_capture_target: augment.published_path_capture_target,
        enqueued_path_origin_counts_by_node: augment.enqueued_path_origin_counts_by_node,
        pending_path_origin_counts_by_node: augment.pending_path_origin_counts_by_node,
        yielded_path_origin_counts_by_node: augment.yielded_path_origin_counts_by_node,
        summarized_path_origin_counts_by_node: augment.summarized_path_origin_counts_by_node,
        published_path_origin_counts_by_node: augment.published_path_origin_counts_by_node,
    })
}

fn source_observability_counter_by_node(
    stable_host_ref: &str,
    count: u64,
    keep_zero_count: bool,
) -> std::collections::BTreeMap<String, u64> {
    (count > 0 || keep_zero_count)
        .then(|| std::collections::BTreeMap::from([(stable_host_ref.to_string(), count)]))
        .unwrap_or_default()
}

fn source_observability_count_strings_by_node(
    stable_host_ref: &str,
    counts: &std::collections::BTreeMap<String, u64>,
) -> std::collections::BTreeMap<String, Vec<String>> {
    (!counts.is_empty())
        .then(|| {
            std::collections::BTreeMap::from([(
                stable_host_ref.to_string(),
                counts
                    .iter()
                    .map(|(origin, count)| format!("{origin}={count}"))
                    .collect::<Vec<_>>(),
            )])
        })
        .unwrap_or_default()
}

fn source_observability_control_signals_by_node(
    context: &SourceObservabilityLiveSourceContext,
    last_control_frame_signals: &[String],
) -> std::collections::BTreeMap<String, Vec<String>> {
    let stable_host_ref = &context.recovery.stable_host_ref;
    let has_local_source_grants = context
        .grants
        .iter()
        .any(|grant| grant.host_ref.eq_ignore_ascii_case(stable_host_ref));
    let allow_control_summary = context
        .recovery
        .allow_control_summary(has_local_source_grants);
    let control_summary = if !last_control_frame_signals.is_empty() {
        if allow_control_summary {
            last_control_frame_signals.to_vec()
        } else {
            Vec::new()
        }
    } else if allow_control_summary {
        context.last_control_frame_signals_snapshot.clone()
    } else {
        Vec::new()
    };
    let explicit = control_signals_by_node(stable_host_ref, &control_summary);
    if explicit.is_empty() {
        recovered_control_signals_by_node_from_projection(&context.recovery, &context.status)
    } else {
        explicit
    }
}

pub(crate) fn build_live_source_observability_snapshot(
    source: &FSMetaSource,
    publication: SourceObservabilityPublicationView,
) -> SourceObservabilitySnapshot {
    let context = build_source_observability_live_source_context(source);
    let stable_host_ref = context.recovery.stable_host_ref.clone();
    let last_control_frame_signals_by_node = source_observability_control_signals_by_node(
        &context,
        &publication.last_control_frame_signals,
    );
    let has_explicit_zero_publication_counters = publication.published_batch_count == 0
        && publication.published_event_count == 0
        && publication.published_control_event_count == 0
        && publication.published_data_event_count == 0;
    build_source_observability_snapshot_from_live_context(
        context,
        SourceObservabilityLiveAugment {
            last_control_frame_signals_by_node,
            published_batches_by_node: source_observability_counter_by_node(
                &stable_host_ref,
                publication.published_batch_count,
                publication.keep_zero_publication_counters,
            ),
            published_events_by_node: source_observability_counter_by_node(
                &stable_host_ref,
                publication.published_event_count,
                publication.keep_zero_publication_counters,
            ),
            published_control_events_by_node: source_observability_counter_by_node(
                &stable_host_ref,
                publication.published_control_event_count,
                publication.keep_zero_publication_counters,
            ),
            published_data_events_by_node: source_observability_counter_by_node(
                &stable_host_ref,
                publication.published_data_event_count,
                publication.keep_zero_publication_counters,
            ),
            last_published_at_us_by_node: (!has_explicit_zero_publication_counters)
                .then_some(publication.last_published_at_us)
                .flatten()
                .map(|ts| std::collections::BTreeMap::from([(stable_host_ref.clone(), ts)]))
                .unwrap_or_default(),
            last_published_origins_by_node: (!has_explicit_zero_publication_counters
                && !publication.last_published_origins.is_empty())
            .then_some(std::collections::BTreeMap::from([(
                stable_host_ref.clone(),
                publication.last_published_origins,
            )]))
            .unwrap_or_default(),
            published_origin_counts_by_node: (!has_explicit_zero_publication_counters)
                .then(|| {
                    source_observability_count_strings_by_node(
                        &stable_host_ref,
                        &publication.published_origin_counts,
                    )
                })
                .unwrap_or_default(),
            published_path_capture_target: publication.published_path_capture_target,
            enqueued_path_origin_counts_by_node: source_observability_count_strings_by_node(
                &stable_host_ref,
                &publication.enqueued_path_origin_counts,
            ),
            pending_path_origin_counts_by_node: source_observability_count_strings_by_node(
                &stable_host_ref,
                &publication.pending_path_origin_counts,
            ),
            yielded_path_origin_counts_by_node: source_observability_count_strings_by_node(
                &stable_host_ref,
                &publication.yielded_path_origin_counts,
            ),
            summarized_path_origin_counts_by_node: (!has_explicit_zero_publication_counters)
                .then(|| {
                    source_observability_count_strings_by_node(
                        &stable_host_ref,
                        &publication.summarized_path_origin_counts,
                    )
                })
                .unwrap_or_default(),
            published_path_origin_counts_by_node: (!has_explicit_zero_publication_counters)
                .then(|| {
                    source_observability_count_strings_by_node(
                        &stable_host_ref,
                        &publication.published_path_origin_counts,
                    )
                })
                .unwrap_or_default(),
        },
    )
}

fn scheduled_groups_by_node(
    node_id: &NodeId,
    grants: &[GrantedMountRoot],
    groups: Result<Option<std::collections::BTreeSet<String>>>,
) -> std::collections::BTreeMap<String, Vec<String>> {
    let Ok(Some(groups)) = groups else {
        return std::collections::BTreeMap::new();
    };
    let stable_host_ref = stable_host_ref_for_node_id(node_id, grants);
    std::collections::BTreeMap::from([(stable_host_ref, groups.into_iter().collect::<Vec<_>>())])
}

pub(crate) fn source_status_entry_looks_active_for_local_observability(
    entry: &crate::source::SourceConcreteRootHealthSnapshot,
) -> bool {
    entry.active
        || entry.current_stream_generation.is_some()
        || entry.emitted_batch_count > 0
        || entry.forwarded_batch_count > 0
        || entry.emitted_event_count > 0
        || entry.forwarded_event_count > 0
}

pub(crate) fn recovered_scheduled_groups_by_node_from_active_status(
    stable_host_ref: &str,
    status: &SourceStatusSnapshot,
    select_group: impl Fn(&crate::source::SourceConcreteRootHealthSnapshot) -> bool,
) -> std::collections::BTreeMap<String, Vec<String>> {
    let groups = status
        .concrete_roots
        .iter()
        .filter(|entry| source_status_entry_looks_active_for_local_observability(entry))
        .filter(|entry| select_group(entry))
        .map(|entry| entry.logical_root_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    if groups.is_empty() {
        return std::collections::BTreeMap::new();
    }
    std::collections::BTreeMap::from([(
        stable_host_ref.to_string(),
        groups.into_iter().collect::<Vec<_>>(),
    )])
}

pub(crate) fn recovered_control_signals_by_node_from_projection(
    projection: &SourceObservabilityRecoveryProjection,
    status: &SourceStatusSnapshot,
) -> std::collections::BTreeMap<String, Vec<String>> {
    if !projection.allow_recovered_control_summary {
        return std::collections::BTreeMap::new();
    }
    let recovered_generation = status.current_stream_generation.unwrap_or_else(|| {
        status
            .concrete_roots
            .iter()
            .filter_map(|entry| entry.current_stream_generation)
            .max()
            .unwrap_or_default()
    });
    let mut signals = Vec::<String>::new();
    if let Some(groups) = projection
        .scheduled_source_groups_by_node
        .get(&projection.stable_host_ref)
    {
        if !groups.is_empty() {
            signals.push(format!(
                "recovered_active_state unit=runtime.exec.source generation={} groups={:?}",
                recovered_generation, groups
            ));
        }
    }
    if let Some(groups) = projection
        .scheduled_scan_groups_by_node
        .get(&projection.stable_host_ref)
    {
        if !groups.is_empty() {
            signals.push(format!(
                "recovered_active_state unit=runtime.exec.scan generation={} groups={:?}",
                recovered_generation, groups
            ));
        }
    }
    if signals.is_empty() {
        std::collections::BTreeMap::new()
    } else {
        std::collections::BTreeMap::from([(projection.stable_host_ref.clone(), signals)])
    }
}

fn recovered_published_observability_from_active_status(
    _stable_host_ref: &str,
    status: &SourceStatusSnapshot,
) -> SourceObservabilityPublicationView {
    let mut published_batch_count = 0u64;
    let mut published_event_count = 0u64;
    let mut published_control_event_count = 0u64;
    let mut published_data_event_count = 0u64;
    let mut last_published_at_us = None::<u64>;
    let mut last_published_origins = std::collections::BTreeSet::<String>::new();
    let mut published_origin_counts = std::collections::BTreeMap::<String, u64>::new();
    let mut path_origin_counts = std::collections::BTreeMap::<String, u64>::new();
    let mut published_path_capture_target = None::<String>;

    for entry in status
        .concrete_roots
        .iter()
        .filter(|entry| source_status_entry_looks_active_for_local_observability(entry))
    {
        published_batch_count = published_batch_count.saturating_add(entry.forwarded_batch_count);
        published_event_count = published_event_count.saturating_add(entry.forwarded_event_count);
        published_control_event_count =
            published_control_event_count.saturating_add(entry.emitted_control_event_count);
        published_data_event_count =
            published_data_event_count.saturating_add(entry.emitted_data_event_count);
        last_published_at_us =
            last_published_at_us.max(entry.last_forwarded_at_us.or(entry.last_emitted_at_us));
        for origin in entry
            .last_forwarded_origins
            .iter()
            .chain(entry.last_emitted_origins.iter())
        {
            last_published_origins.insert(origin.clone());
        }
        if entry.forwarded_event_count > 0 {
            *published_origin_counts
                .entry(entry.object_ref.clone())
                .or_default() += entry.forwarded_event_count;
        }
        if entry.forwarded_path_event_count > 0 {
            *path_origin_counts
                .entry(entry.object_ref.clone())
                .or_default() += entry.forwarded_path_event_count;
        }
        if published_path_capture_target.is_none() {
            published_path_capture_target = entry.emitted_path_capture_target.clone();
        }
    }

    SourceObservabilityPublicationView {
        published_batch_count,
        published_event_count,
        published_control_event_count,
        published_data_event_count,
        last_published_at_us,
        last_published_origins: last_published_origins.into_iter().collect::<Vec<_>>(),
        published_origin_counts,
        published_path_capture_target,
        summarized_path_origin_counts: path_origin_counts.clone(),
        published_path_origin_counts: path_origin_counts,
        ..Default::default()
    }
}

fn build_local_source_observability_snapshot(source: &FSMetaSource) -> SourceObservabilitySnapshot {
    let context = build_source_observability_live_source_context(source);
    build_live_source_observability_snapshot(
        source,
        recovered_published_observability_from_active_status(
            &context.recovery.stable_host_ref,
            &context.status,
        ),
    )
}

fn build_cached_worker_observability_snapshot(
    cache: &SourceWorkerSnapshotCache,
) -> Option<SourceObservabilitySnapshot> {
    Some(build_source_observability_snapshot_from_cache(
        cache,
        cache.lifecycle_state.clone()?,
        cache.status.clone().unwrap_or_default(),
    ))
}

fn build_degraded_worker_observability_snapshot(
    cache: &SourceWorkerSnapshotCache,
    reason: impl Into<String>,
) -> SourceObservabilitySnapshot {
    let reason = reason.into();
    let mut status = cache.status.clone().unwrap_or_default();
    status
        .degraded_roots
        .push((SOURCE_WORKER_DEGRADED_ROOT_KEY.to_string(), reason));
    let mut snapshot = build_source_observability_snapshot_from_cache(
        cache,
        SOURCE_WORKER_DEGRADED_STATE.to_string(),
        status,
    );
    fail_close_incomplete_active_source_observability_snapshot(&mut snapshot);
    snapshot
}

fn build_source_observability_snapshot_from_cache(
    cache: &SourceWorkerSnapshotCache,
    lifecycle_state: String,
    status: SourceStatusSnapshot,
) -> SourceObservabilitySnapshot {
    SourceObservabilitySnapshot {
        lifecycle_state,
        host_object_grants_version: cache.host_object_grants_version.unwrap_or_default(),
        grants: cache.grants.clone().unwrap_or_default(),
        logical_roots: cache.logical_roots.clone().unwrap_or_default(),
        status,
        source_primary_by_group: cache.source_primary_by_group.clone().unwrap_or_default(),
        last_force_find_runner_by_group: cache
            .last_force_find_runner_by_group
            .clone()
            .unwrap_or_default(),
        force_find_inflight_groups: cache.force_find_inflight_groups.clone().unwrap_or_default(),
        scheduled_source_groups_by_node: cache
            .scheduled_source_groups_by_node
            .clone()
            .unwrap_or_default(),
        scheduled_scan_groups_by_node: cache
            .scheduled_scan_groups_by_node
            .clone()
            .unwrap_or_default(),
        last_control_frame_signals_by_node: cache
            .last_control_frame_signals_by_node
            .clone()
            .unwrap_or_default(),
        published_batches_by_node: cache.published_batches_by_node.clone().unwrap_or_default(),
        published_events_by_node: cache.published_events_by_node.clone().unwrap_or_default(),
        published_control_events_by_node: cache
            .published_control_events_by_node
            .clone()
            .unwrap_or_default(),
        published_data_events_by_node: cache
            .published_data_events_by_node
            .clone()
            .unwrap_or_default(),
        last_published_at_us_by_node: cache
            .last_published_at_us_by_node
            .clone()
            .unwrap_or_default(),
        last_published_origins_by_node: cache
            .last_published_origins_by_node
            .clone()
            .unwrap_or_default(),
        published_origin_counts_by_node: cache
            .published_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        published_path_capture_target: cache.published_path_capture_target.clone(),
        enqueued_path_origin_counts_by_node: cache
            .enqueued_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        pending_path_origin_counts_by_node: cache
            .pending_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        yielded_path_origin_counts_by_node: cache
            .yielded_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        summarized_path_origin_counts_by_node: cache
            .summarized_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
        published_path_origin_counts_by_node: cache
            .published_path_origin_counts_by_node
            .clone()
            .unwrap_or_default(),
    }
}

impl<'a> SourceControlFrameExecutor<'a> {
    fn log_control_frame_terminal_outcome(&self, terminal: std::result::Result<(), &CnxError>) {
        match terminal {
            Ok(()) => eprintln!(
                "fs_meta_source_worker_client: on_control_frame done node={} ok=true",
                self.handle.node_id.0
            ),
            Err(err) => eprintln!(
                "fs_meta_source_worker_client: on_control_frame done node={} ok=false err={}",
                self.handle.node_id.0, err
            ),
        }
    }

    async fn execute_control_frame(
        &self,
        control_machine: SourceControlFrameMachine,
        initial_effect: SourceControlFrameStep,
    ) -> std::result::Result<(), SourceFailure> {
        let mut control_machine = control_machine;
        let mut action = initial_effect;
        loop {
            match action {
                SourceControlFrameStep::Terminal(SourceControlFrameTerminal::Complete) => {
                    self.log_control_frame_terminal_outcome(Ok(()));
                    return Ok(());
                }
                SourceControlFrameStep::Terminal(SourceControlFrameTerminal::Fail(failure)) => {
                    self.log_control_frame_terminal_outcome(Err(failure.as_error()));
                    return Err(failure);
                }
                SourceControlFrameStep::Run(lane) => {
                    let event = lane.execute(self.handle).await?;
                    action = control_machine.advance(event)?;
                }
            }
        }
    }
}

#[derive(Clone)]
pub enum SourceFacade {
    Local(Arc<FSMetaSource>),
    Worker(Arc<SourceWorkerClientHandle>),
}

impl FSMetaSource {
    pub(crate) fn with_boundaries_and_state_with_failure(
        config: SourceConfig,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        state_boundary: Arc<dyn StateBoundary>,
    ) -> std::result::Result<Self, SourceFailure> {
        Self::with_boundaries_and_state_internal(config, node_id, boundary, state_boundary, false)
            .map_err(SourceFailure::from)
    }

    pub(crate) async fn pub_stream_with_failure(
        &self,
    ) -> std::result::Result<
        std::pin::Pin<Box<dyn futures_core::Stream<Item = Vec<Event>> + Send>>,
        SourceFailure,
    > {
        self.build_pub_stream().await.map_err(SourceFailure::from)
    }

    pub(crate) async fn start_runtime_endpoints_with_failure(
        &self,
        boundary: Arc<dyn ChannelIoSubset>,
    ) -> std::result::Result<(), SourceFailure> {
        self.start_runtime_endpoints_on_boundary(boundary)
            .await
            .map_err(SourceFailure::from)
    }

    pub(crate) async fn apply_orchestration_signals_with_failure(
        &self,
        signals: &[SourceControlSignal],
    ) -> std::result::Result<(), SourceFailure> {
        self.perform_apply_orchestration_signals(signals)
            .await
            .map_err(SourceFailure::from)
    }

    pub(crate) async fn update_logical_roots_with_failure(
        &self,
        roots: Vec<RootSpec>,
    ) -> std::result::Result<(), SourceFailure> {
        self.apply_logical_roots_update(roots)
            .await
            .map_err(SourceFailure::from)
    }

    pub(crate) fn logical_roots_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<RootSpec>, SourceFailure> {
        Ok(self.snapshot_logical_roots())
    }

    pub(crate) fn cached_logical_roots_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<RootSpec>, SourceFailure> {
        Ok(self.snapshot_cached_logical_roots())
    }

    pub(crate) fn host_object_grants_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<GrantedMountRoot>, SourceFailure> {
        Ok(self.snapshot_host_object_grants())
    }

    pub(crate) fn cached_host_object_grants_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<GrantedMountRoot>, SourceFailure> {
        Ok(self.snapshot_cached_host_object_grants())
    }

    pub(crate) fn host_object_grants_version_snapshot_with_failure(
        &self,
    ) -> std::result::Result<u64, SourceFailure> {
        Ok(self.snapshot_host_object_grants_version())
    }

    pub(crate) fn status_snapshot_with_failure(
        &self,
    ) -> std::result::Result<SourceStatusSnapshot, SourceFailure> {
        Ok(self.build_status_snapshot())
    }

    pub(crate) fn progress_snapshot_with_failure(
        &self,
    ) -> std::result::Result<crate::source::SourceProgressSnapshot, SourceFailure> {
        Ok(self.build_progress_snapshot())
    }

    pub(crate) fn materialized_read_cache_epoch_with_failure(
        &self,
    ) -> std::result::Result<u64, SourceFailure> {
        Ok(self.materialized_read_cache_epoch())
    }

    fn local_observability_snapshot(&self) -> SourceObservabilitySnapshot {
        let snapshot = build_local_source_observability_snapshot(self);
        let last_audit_completed_at_us = source_status_rescan_completion_marker(&snapshot.status);
        let (published_batches, last_published_at_us) =
            source_observability_publication_marker(&snapshot);
        self.maybe_mark_rescan_observed_if_publication_advanced(
            published_batches,
            last_published_at_us,
            last_audit_completed_at_us,
        );
        snapshot
    }

    pub(crate) fn observability_snapshot_with_failure(
        &self,
    ) -> std::result::Result<SourceObservabilitySnapshot, SourceFailure> {
        Ok(self.local_observability_snapshot())
    }

    pub(crate) fn observability_snapshot_nonblocking_for_status_route(
        &self,
    ) -> (SourceObservabilitySnapshot, bool) {
        (self.local_observability_snapshot(), false)
    }

    pub(crate) fn lifecycle_state_label_with_failure(
        &self,
    ) -> std::result::Result<String, SourceFailure> {
        Ok(self.snapshot_lifecycle_state_label())
    }

    pub(crate) fn scheduled_source_group_ids_with_failure(
        &self,
    ) -> std::result::Result<Option<std::collections::BTreeSet<String>>, SourceFailure> {
        self.snapshot_scheduled_source_group_ids()
            .map_err(SourceFailure::from)
    }

    pub(crate) fn scheduled_scan_group_ids_with_failure(
        &self,
    ) -> std::result::Result<Option<std::collections::BTreeSet<String>>, SourceFailure> {
        self.snapshot_scheduled_scan_group_ids()
            .map_err(SourceFailure::from)
    }

    pub(crate) fn force_find_with_failure(
        &self,
        params: &InternalQueryRequest,
    ) -> std::result::Result<Vec<Event>, SourceFailure> {
        self.perform_force_find(params).map_err(SourceFailure::from)
    }

    pub(crate) fn source_primary_by_group_snapshot_with_failure(
        &self,
    ) -> std::result::Result<std::collections::BTreeMap<String, String>, SourceFailure> {
        Ok(self.snapshot_source_primary_by_group())
    }

    pub(crate) fn last_force_find_runner_by_group_snapshot_with_failure(
        &self,
    ) -> std::result::Result<std::collections::BTreeMap<String, String>, SourceFailure> {
        Ok(self.snapshot_last_force_find_runner_by_group())
    }

    pub(crate) fn force_find_inflight_groups_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<String>, SourceFailure> {
        Ok(self.snapshot_force_find_inflight_groups())
    }

    pub(crate) fn resolve_group_id_for_object_ref_with_failure(
        &self,
        object_ref: &str,
    ) -> std::result::Result<Option<String>, SourceFailure> {
        Ok(self.snapshot_group_id_for_object_ref(object_ref))
    }

    pub(crate) async fn publish_manual_rescan_signal_with_failure(
        &self,
    ) -> std::result::Result<(), SourceFailure> {
        self.emit_manual_rescan_signal()
            .await
            .map_err(SourceFailure::from)
    }

    pub(crate) async fn trigger_rescan_when_ready_epoch_with_failure(
        &self,
    ) -> std::result::Result<u64, SourceFailure> {
        Ok(self.perform_trigger_rescan_when_ready_epoch().await)
    }

    pub(crate) async fn on_control_frame_with_failure(
        &self,
        envelopes: Vec<ControlEnvelope>,
    ) -> std::result::Result<(), SourceFailure> {
        let signals =
            source_control_signals_from_envelopes(&envelopes).map_err(SourceFailure::from)?;
        self.apply_control_frame_signals(&signals)
            .await
            .map_err(SourceFailure::from)
    }

    pub(crate) async fn close_with_failure(&self) -> std::result::Result<(), SourceFailure> {
        self.perform_close().await.map_err(SourceFailure::from)
    }
}

impl SourceFacade {
    pub fn local(source: Arc<FSMetaSource>) -> Self {
        Self::Local(source)
    }

    pub fn worker(client: Arc<SourceWorkerClientHandle>) -> Self {
        Self::Worker(client)
    }

    pub fn is_worker(&self) -> bool {
        matches!(self, Self::Worker(_))
    }

    pub(crate) async fn start_with_failure(
        &self,
        sink: Arc<SinkFacade>,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> std::result::Result<Option<JoinHandle<()>>, SourceFailure> {
        match self {
            Self::Local(source) => {
                let stream = source.pub_stream_with_failure().await?;
                Ok(Some(spawn_local_source_pump(stream, sink, boundary)))
            }
            Self::Worker(client) => {
                client.start_with_failure().await?;
                Ok(None)
            }
        }
    }

    #[cfg(test)]
    pub async fn start(
        &self,
        sink: Arc<SinkFacade>,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> Result<Option<JoinHandle<()>>> {
        self.start_with_failure(sink, boundary)
            .await
            .map_err(SourceFailure::into_error)
    }

    pub(crate) async fn apply_orchestration_signals_with_total_timeout_with_failure(
        &self,
        signals: &[SourceControlSignal],
        total_timeout: Duration,
    ) -> std::result::Result<(), SourceFailure> {
        if total_timeout.is_zero() {
            return Err(SourceFailure::retry_budget_exhausted(
                SourceRetryBudgetExhaustionKind::OperationAttempt,
            ));
        }
        // runtime_app owns the outer recovery loop for local source recovery.
        // Keep each nested source-client control attempt short so retryable resets
        // return to the caller instead of burning the full nested client budget.
        let single_attempt_total_timeout = std::cmp::min(
            total_timeout,
            SOURCE_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT,
        );
        match self {
            Self::Local(source) => map_source_failure_timeout_result(
                tokio::time::timeout(
                    single_attempt_total_timeout,
                    source.apply_orchestration_signals_with_failure(signals),
                )
                .await,
            ),
            Self::Worker(client) => {
                let envelopes = signals
                    .iter()
                    .map(SourceControlSignal::envelope)
                    .collect::<Vec<_>>();
                let rpc_timeout = std::cmp::min(
                    SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
                    single_attempt_total_timeout,
                );
                client
                    .on_control_frame_with_timeouts(
                        envelopes,
                        single_attempt_total_timeout,
                        rpc_timeout,
                    )
                    .await
            }
        }
    }

    pub(crate) async fn control_signals_with_replay(
        &self,
        signals: &[SourceControlSignal],
    ) -> Vec<SourceControlSignal> {
        match self {
            Self::Local(source) => source.control_signals_with_replay(signals).await,
            Self::Worker(client) => {
                let mut desired = client.control_state.lock().await.clone();
                desired.retain_signals(signals);
                desired.replay_signals()
            }
        }
    }

    pub(crate) async fn record_retained_control_signals(&self, signals: &[SourceControlSignal]) {
        match self {
            Self::Local(source) => source.record_retained_control_signals(signals).await,
            Self::Worker(client) => client.retain_control_signals(signals).await,
        }
    }

    #[cfg(test)]
    pub(crate) async fn retained_control_state_for_tests(&self) -> SourceControlState {
        match self {
            Self::Local(source) => source.retained_control_state_for_tests().await,
            Self::Worker(client) => client.control_state.lock().await.clone(),
        }
    }

    pub(crate) async fn retained_replay_required(&self) -> bool {
        match self {
            Self::Local(_) => false,
            Self::Worker(client) => matches!(
                client.control_state.lock().await.replay_state(),
                SourceControlReplayState::Required
            ),
        }
    }

    pub(crate) async fn update_logical_roots_with_failure(
        &self,
        roots: Vec<RootSpec>,
    ) -> std::result::Result<(), SourceFailure> {
        match self {
            Self::Local(source) => source.update_logical_roots_with_failure(roots).await,
            Self::Worker(client) => client.update_logical_roots_with_failure(roots).await,
        }
    }

    pub(crate) fn cached_logical_roots_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<RootSpec>, SourceFailure> {
        match self {
            Self::Local(source) => source.cached_logical_roots_snapshot_with_failure(),
            Self::Worker(client) => client.cached_logical_roots_snapshot_with_failure(),
        }
    }

    pub(crate) async fn logical_roots_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<RootSpec>, SourceFailure> {
        match self {
            Self::Local(source) => source.logical_roots_snapshot_with_failure(),
            Self::Worker(client) => client.logical_roots_snapshot_with_failure().await,
        }
    }

    pub(crate) fn cached_host_object_grants_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<GrantedMountRoot>, SourceFailure> {
        match self {
            Self::Local(source) => source.cached_host_object_grants_snapshot_with_failure(),
            Self::Worker(client) => client.cached_host_object_grants_snapshot_with_failure(),
        }
    }

    pub(crate) async fn host_object_grants_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<GrantedMountRoot>, SourceFailure> {
        match self {
            Self::Local(source) => source.host_object_grants_snapshot_with_failure(),
            Self::Worker(client) => client.host_object_grants_snapshot_with_failure().await,
        }
    }

    #[cfg(test)]
    pub(crate) async fn host_object_grants_version_snapshot_with_failure(
        &self,
    ) -> std::result::Result<u64, SourceFailure> {
        match self {
            Self::Local(source) => source.host_object_grants_version_snapshot_with_failure(),
            Self::Worker(client) => {
                client
                    .host_object_grants_version_snapshot_with_failure()
                    .await
            }
        }
    }

    pub(crate) async fn status_snapshot_with_failure(
        &self,
    ) -> std::result::Result<SourceStatusSnapshot, SourceFailure> {
        match self {
            Self::Local(source) => {
                let _ = source
                    .sync_logical_roots_from_authoritative_cell_if_changed()
                    .await;
                source.status_snapshot_with_failure()
            }
            Self::Worker(client) => client.status_snapshot_with_failure().await,
        }
    }

    #[cfg(test)]
    pub(crate) async fn lifecycle_state_label_with_failure(
        &self,
    ) -> std::result::Result<String, SourceFailure> {
        match self {
            Self::Local(source) => source.lifecycle_state_label_with_failure(),
            Self::Worker(client) => client.lifecycle_state_label_with_failure().await,
        }
    }

    pub(crate) async fn scheduled_source_group_ids_with_failure(
        &self,
    ) -> std::result::Result<Option<std::collections::BTreeSet<String>>, SourceFailure> {
        match self {
            Self::Local(source) => source.scheduled_source_group_ids_with_failure(),
            Self::Worker(client) => client.scheduled_source_group_ids_with_failure().await,
        }
    }

    #[cfg(test)]
    pub async fn scheduled_source_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.scheduled_source_group_ids_with_failure()
            .await
            .map_err(SourceFailure::into_error)
    }

    pub(crate) async fn scheduled_scan_group_ids_with_failure(
        &self,
    ) -> std::result::Result<Option<std::collections::BTreeSet<String>>, SourceFailure> {
        match self {
            Self::Local(source) => source.scheduled_scan_group_ids_with_failure(),
            Self::Worker(client) => client.scheduled_scan_group_ids_with_failure().await,
        }
    }

    #[cfg(test)]
    pub async fn scheduled_scan_group_ids(
        &self,
    ) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.scheduled_scan_group_ids_with_failure()
            .await
            .map_err(SourceFailure::into_error)
    }

    pub(crate) async fn source_primary_by_group_snapshot_with_failure(
        &self,
    ) -> std::result::Result<std::collections::BTreeMap<String, String>, SourceFailure> {
        match self {
            Self::Local(source) => source.source_primary_by_group_snapshot_with_failure(),
            Self::Worker(client) => client.source_primary_by_group_snapshot_with_failure().await,
        }
    }

    pub(crate) async fn last_force_find_runner_by_group_snapshot_with_failure(
        &self,
    ) -> std::result::Result<std::collections::BTreeMap<String, String>, SourceFailure> {
        match self {
            Self::Local(source) => source.last_force_find_runner_by_group_snapshot_with_failure(),
            Self::Worker(client) => {
                client
                    .last_force_find_runner_by_group_snapshot_with_failure()
                    .await
            }
        }
    }

    pub(crate) async fn force_find_inflight_groups_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<String>, SourceFailure> {
        match self {
            Self::Local(source) => source.force_find_inflight_groups_snapshot_with_failure(),
            Self::Worker(client) => {
                client
                    .force_find_inflight_groups_snapshot_with_failure()
                    .await
            }
        }
    }

    pub(crate) async fn resolve_group_id_for_object_ref_with_failure(
        &self,
        object_ref: &str,
    ) -> std::result::Result<Option<String>, SourceFailure> {
        match self {
            Self::Local(source) => source.resolve_group_id_for_object_ref_with_failure(object_ref),
            Self::Worker(client) => {
                client
                    .resolve_group_id_for_object_ref_with_failure(object_ref)
                    .await
            }
        }
    }

    pub(crate) async fn force_find_with_failure(
        &self,
        params: &InternalQueryRequest,
    ) -> std::result::Result<Vec<Event>, SourceFailure> {
        match self {
            Self::Local(source) => {
                let _ = source
                    .sync_logical_roots_from_authoritative_cell_if_changed()
                    .await;
                source.force_find_with_failure(params)
            }
            Self::Worker(client) => client.force_find_with_failure(params.clone()).await,
        }
    }

    #[cfg(test)]
    pub(crate) async fn force_find_via_node_with_failure(
        &self,
        node_id: &NodeId,
        params: &InternalQueryRequest,
    ) -> std::result::Result<Vec<Event>, SourceFailure> {
        match self {
            Self::Local(source) => source.force_find_with_failure(params),
            Self::Worker(client) => {
                if client.node_id == *node_id {
                    client.force_find_with_failure(params.clone()).await
                } else {
                    let remote = SourceWorkerClientHandle::new(
                        node_id.clone(),
                        client.config.clone(),
                        client.worker_binding.clone(),
                        client.worker_factory.clone(),
                    )?;
                    remote.force_find_with_failure(params.clone()).await
                }
            }
        }
    }

    pub(crate) async fn publish_manual_rescan_signal_with_failure(
        &self,
    ) -> std::result::Result<(), SourceFailure> {
        match self {
            Self::Local(source) => source.publish_manual_rescan_signal_with_failure().await,
            Self::Worker(client) => client.publish_manual_rescan_signal_with_failure().await,
        }
    }

    pub(crate) async fn trigger_rescan_when_ready_epoch_with_failure(
        &self,
    ) -> std::result::Result<u64, SourceFailure> {
        #[cfg(test)]
        record_source_worker_trigger_rescan_when_ready_attempt();
        match self {
            Self::Local(source) => source.trigger_rescan_when_ready_epoch_with_failure().await,
            Self::Worker(client) => client.trigger_rescan_when_ready_epoch_with_failure().await,
        }
    }

    pub(crate) async fn submit_rescan_when_ready_epoch_with_failure(
        &self,
    ) -> std::result::Result<u64, SourceFailure> {
        #[cfg(test)]
        record_source_worker_trigger_rescan_when_ready_attempt();
        match self {
            Self::Local(source) => source.trigger_rescan_when_ready_epoch_with_failure().await,
            Self::Worker(client) => client.submit_rescan_when_ready_epoch_with_failure().await,
        }
    }

    pub(crate) async fn reconnect_shared_worker_client_with_failure(
        &self,
    ) -> std::result::Result<(), SourceFailure> {
        match self {
            Self::Local(_) => Ok(()),
            Self::Worker(client) => client.reconnect_shared_worker_client_with_failure().await,
        }
    }

    pub(crate) async fn close_with_failure(&self) -> std::result::Result<(), SourceFailure> {
        match self {
            Self::Local(source) => source.close_with_failure().await,
            Self::Worker(client) => client.close_with_failure().await,
        }
    }

    #[cfg(test)]
    pub async fn close(&self) -> Result<()> {
        self.close_with_failure()
            .await
            .map_err(SourceFailure::into_error)
    }

    pub(crate) async fn wait_for_control_ops_to_drain_for_handoff(&self) {
        if let Self::Worker(client) = self {
            client
                .wait_for_control_ops_to_drain(SOURCE_WORKER_CLOSE_DRAIN_TIMEOUT)
                .await;
        }
    }

    pub(crate) async fn observability_snapshot_with_failure(
        &self,
    ) -> std::result::Result<SourceObservabilitySnapshot, SourceFailure> {
        match self {
            Self::Local(source) => {
                let _ = source
                    .sync_logical_roots_from_authoritative_cell_if_changed()
                    .await;
                source.observability_snapshot_with_failure()
            }
            Self::Worker(client) => {
                client
                    .observability_snapshot_with_timeout_with_failure(
                        SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
                    )
                    .await
            }
        }
    }

    pub(crate) async fn observability_snapshot_nonblocking_for_status_route(
        &self,
    ) -> (SourceObservabilitySnapshot, bool) {
        match self {
            Self::Local(source) => {
                let _ = source
                    .sync_logical_roots_from_authoritative_cell_if_changed()
                    .await;
                source.observability_snapshot_nonblocking_for_status_route()
            }
            Self::Worker(client) => {
                client
                    .observability_snapshot_nonblocking_for_status_route()
                    .await
            }
        }
    }

    pub(crate) async fn progress_snapshot_with_failure(
        &self,
    ) -> std::result::Result<crate::source::SourceProgressSnapshot, SourceFailure> {
        match self {
            Self::Local(source) => source.progress_snapshot_with_failure(),
            Self::Worker(client) => {
                client
                    .progress_snapshot_with_timeout_with_failure(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                    .await
            }
        }
    }

    pub(crate) fn materialized_read_cache_epoch_with_failure(
        &self,
    ) -> std::result::Result<u64, SourceFailure> {
        match self {
            Self::Local(source) => source.materialized_read_cache_epoch_with_failure(),
            Self::Worker(client) => Ok(client.cached_materialized_read_cache_epoch()),
        }
    }
}

fn spawn_local_source_pump(
    stream: std::pin::Pin<Box<dyn futures_core::Stream<Item = Vec<Event>> + Send>>,
    sink: Arc<SinkFacade>,
    boundary: Option<Arc<dyn ChannelIoSubset>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Some(boundary) = boundary {
            tokio::pin!(stream);
            while let Some(batch) = stream.next().await {
                let origin = batch
                    .first()
                    .map(|event| event.metadata().origin_id.0.clone())
                    .unwrap_or_else(|| "__empty__".to_string());
                if let Err(err) = boundary
                    .channel_send(
                        BoundaryContext::default(),
                        ChannelSendRequest {
                            channel_key: ChannelKey(format!("{}.stream", ROUTE_KEY_EVENTS)),
                            events: batch,
                            timeout_ms: Some(Duration::from_secs(5).as_millis() as u64),
                        },
                    )
                    .await
                {
                    log::error!(
                        "fs-meta app pump failed to publish source batch on stream route origin={}: {:?}",
                        origin,
                        err
                    );
                    break;
                }
            }
        } else {
            tokio::pin!(stream);
            while let Some(batch) = stream.next().await {
                if let Err(err) = sink.send_with_failure(&batch).await {
                    log::error!(
                        "fs-meta app pump failed to apply batch: {:?}",
                        err.as_error()
                    );
                }
            }
        }
    })
}

capanix_runtime_entry_sdk::worker_runtime::define_typed_worker_rpc! {
    pub struct SourceWorkerRpc {
        request: SourceWorkerRequest,
        response: SourceWorkerResponse,
        encode_request: encode_request,
        decode_request: decode_request,
        encode_response: encode_response,
        decode_response: decode_response,
        invalid_input: SourceWorkerResponse::InvalidInput,
        error: SourceWorkerResponse::Error,
        unavailable: "source worker unavailable",
    }
}

impl TypedWorkerInit<SourceConfig> for SourceWorkerRpc {
    type InitPayload = SourceConfig;

    fn init_payload(_node_id: &NodeId, config: &SourceConfig) -> Result<Self::InitPayload> {
        Ok(config.clone())
    }
}

#[cfg(test)]
mod tests;
