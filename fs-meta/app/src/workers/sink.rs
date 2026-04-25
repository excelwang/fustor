use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

#[cfg(test)]
use bytes::Bytes;
use capanix_app_sdk::runtime::{ControlEnvelope, NodeId, RecvOpts, RuntimeWorkerBinding};
use capanix_app_sdk::{CnxError, Event, Result};
#[cfg(test)]
use capanix_host_adapter_fs::{HostAdapter, exchange_host_adapter_from_channel_boundary};
use capanix_runtime_entry_sdk::advanced::boundary::{ChannelIoSubset, StateBoundary};
use capanix_runtime_entry_sdk::worker_runtime::{
    RuntimeWorkerClientFactory, TypedRuntimeWorkerClient, TypedWorkerClient, TypedWorkerInit,
};

use crate::query::models::QueryNode;
use crate::query::path::root_file_name_bytes;
use crate::query::request::{InternalQueryRequest, MaterializedQueryPayload, QueryOp, QueryScope};
use crate::runtime::orchestration::{SinkControlSignal, sink_control_signals_from_envelopes};
#[cfg(test)]
use crate::runtime::routes::{METHOD_FIND, ROUTE_TOKEN_FS_META, default_route_bindings};
#[cfg(test)]
use crate::sink::SinkStatusSnapshotIssue;
use crate::sink::VisibilityLagSample;
use crate::sink::{
    SinkFileMeta, SinkStatusConcern, SinkStatusConcernProjection, SinkStatusSnapshot,
    SinkStatusSnapshotReadinessSummary, sink_status_origin_entry_group_id,
};
use crate::source::config::{GrantedMountRoot, SourceConfig};
use crate::workers::sink_ipc::{
    SinkWorkerInitConfig, SinkWorkerRequest, SinkWorkerResponse, decode_request, decode_response,
    encode_request, encode_response,
};

const SINK_WORKER_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(15);
const SINK_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(2);
const SINK_WORKER_CONTROL_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const SINK_WORKER_UPDATE_ROOTS_RPC_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const SINK_WORKER_FORCE_FIND_TIMEOUT: Duration = Duration::from_secs(60);
#[cfg(test)]
const SINK_WORKER_FORCE_FIND_REPLY_IDLE_GRACE: Duration = Duration::from_secs(5);
const SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT: Duration = Duration::from_secs(60);
const SINK_WORKER_CLOSE_DRAIN_TIMEOUT: Duration = SINK_WORKER_UPDATE_ROOTS_RPC_TIMEOUT;
const SINK_WORKER_STATUS_NONBLOCKING_PROBE_BUDGET: Duration = Duration::from_millis(350);
const SINK_WORKER_STATUS_NONBLOCKING_SETTLE_SLACK: Duration = Duration::from_millis(50);
const SINK_WORKER_STATUS_RETRY_RESET_FINAL_PROBE_BUDGET: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkControlResetKind {
    WorkerNotInitialized,
    TransportClosed,
    ChannelClosed,
    BridgePeerReset,
    GrantAttachmentsDrainedOrFenced,
    InvalidGrantAttachmentToken,
    MissingChannelBufferRouteState,
}

impl SinkControlResetKind {
    const fn is_bridge_reset_like(self) -> bool {
        matches!(
            self,
            Self::TransportClosed | Self::ChannelClosed | Self::BridgePeerReset
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkRetryBudgetExhaustionKind {
    StatusProbeAttempt,
    ControlFrameAttempt,
    ControlFrameRetry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkReplayHandlingKind {
    BestEffortAfterRetryReset,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SinkProtocolViolationKind {
    UnexpectedWorkerResponse {
        context: &'static str,
        response: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SinkFailureReason {
    RetryBudgetExhausted(SinkRetryBudgetExhaustionKind),
    ControlReset(SinkControlResetKind),
    ProtocolViolation(SinkProtocolViolationKind),
    ReplayHandling(SinkReplayHandlingKind),
    TimeoutLike,
    NonRetryable,
}

#[derive(Debug)]
pub(crate) struct SinkFailure {
    cause: CnxError,
    reason: SinkFailureReason,
}

impl SinkFailure {
    fn timeout_like() -> Self {
        Self {
            cause: CnxError::Timeout,
            reason: SinkFailureReason::TimeoutLike,
        }
    }

    fn from_cause(cause: CnxError) -> Self {
        match cause {
            CnxError::Timeout => Self::timeout_like(),
            _ => match classify_sink_control_reset(&cause) {
                Some(kind) => Self {
                    cause,
                    reason: SinkFailureReason::ControlReset(kind),
                },
                None => Self {
                    cause,
                    reason: SinkFailureReason::NonRetryable,
                },
            },
        }
    }

    fn allows_retry(&self) -> bool {
        matches!(
            self.reason,
            SinkFailureReason::TimeoutLike | SinkFailureReason::ControlReset(_)
        )
    }

    fn supports_best_effort_replay(&self) -> bool {
        self.allows_retry()
    }

    fn retry_budget_exhausted(kind: SinkRetryBudgetExhaustionKind) -> Self {
        Self {
            cause: shape_sink_retry_budget_exhaustion(kind),
            reason: SinkFailureReason::RetryBudgetExhausted(kind),
        }
    }

    fn replay_best_effort(cause: CnxError) -> Self {
        Self {
            cause,
            reason: SinkFailureReason::ReplayHandling(
                SinkReplayHandlingKind::BestEffortAfterRetryReset,
            ),
        }
    }

    fn protocol_violation(kind: SinkProtocolViolationKind) -> Self {
        Self {
            cause: kind.clone().into_error(),
            reason: SinkFailureReason::ProtocolViolation(kind),
        }
    }

    pub(crate) fn into_error(self) -> CnxError {
        self.cause
    }

    pub(crate) fn as_error(&self) -> &CnxError {
        &self.cause
    }
}

impl From<CnxError> for SinkFailure {
    fn from(cause: CnxError) -> Self {
        Self::from_cause(cause)
    }
}

fn shape_sink_retry_budget_exhaustion(kind: SinkRetryBudgetExhaustionKind) -> CnxError {
    let _ = kind;
    CnxError::Timeout
}

impl SinkProtocolViolationKind {
    fn into_error(self) -> CnxError {
        match self {
            Self::UnexpectedWorkerResponse { context, response } => CnxError::ProtocolViolation(
                format!("unexpected sink worker response {context}: {response}"),
            ),
        }
    }
}

fn unexpected_sink_worker_response_failure(
    context: &'static str,
    response: SinkWorkerResponse,
) -> SinkFailure {
    SinkFailure::protocol_violation(SinkProtocolViolationKind::UnexpectedWorkerResponse {
        context,
        response: format!("{:?}", response),
    })
}

fn unexpected_sink_worker_response_result<T>(
    context: &'static str,
    response: SinkWorkerResponse,
) -> std::result::Result<T, SinkFailure> {
    Err(unexpected_sink_worker_response_failure(context, response))
}

fn map_sink_failure_timeout_result<T>(
    result: std::result::Result<std::result::Result<T, SinkFailure>, tokio::time::error::Elapsed>,
    exhaustion_kind: SinkRetryBudgetExhaustionKind,
) -> std::result::Result<T, SinkFailure> {
    match result {
        Ok(result) => result,
        Err(_) => Err(SinkFailure::retry_budget_exhausted(exhaustion_kind)),
    }
}

fn classify_sink_control_reset(err: &CnxError) -> Option<SinkControlResetKind> {
    match err {
        CnxError::PeerError(message) if message == "worker not initialized" => {
            Some(SinkControlResetKind::WorkerNotInitialized)
        }
        CnxError::TransportClosed(_) => Some(SinkControlResetKind::TransportClosed),
        CnxError::ChannelClosed => Some(SinkControlResetKind::ChannelClosed),
        CnxError::PeerError(message) | CnxError::Internal(message)
            if is_retryable_worker_bridge_transport_error_message(message) =>
        {
            Some(SinkControlResetKind::BridgePeerReset)
        }
        CnxError::AccessDenied(message) | CnxError::PeerError(message)
            if message.contains("drained/fenced") && message.contains("grant attachments") =>
        {
            Some(SinkControlResetKind::GrantAttachmentsDrainedOrFenced)
        }
        CnxError::AccessDenied(message)
        | CnxError::PeerError(message)
        | CnxError::Internal(message)
            if message.contains("invalid or revoked grant attachment token") =>
        {
            Some(SinkControlResetKind::InvalidGrantAttachmentToken)
        }
        CnxError::AccessDenied(message)
        | CnxError::PeerError(message)
        | CnxError::Internal(message)
            if message.contains("missing route state for channel_buffer") =>
        {
            Some(SinkControlResetKind::MissingChannelBufferRouteState)
        }
        _ => None,
    }
}

#[derive(Debug)]
enum SinkRetryDisposition {
    Retry,
    Fail(SinkFailure),
}

#[cfg(test)]
fn classify_sink_retry_disposition(
    deadline: std::time::Instant,
    err: CnxError,
    exhaustion_kind: SinkRetryBudgetExhaustionKind,
) -> SinkRetryDisposition {
    if std::time::Instant::now() >= deadline {
        SinkRetryDisposition::Fail(SinkFailure::retry_budget_exhausted(exhaustion_kind))
    } else {
        let failure = SinkFailure::from_cause(err);
        if failure.allows_retry() {
            SinkRetryDisposition::Retry
        } else {
            SinkRetryDisposition::Fail(failure)
        }
    }
}

fn classify_sink_retry_failure_disposition(
    deadline: std::time::Instant,
    failure: SinkFailure,
    exhaustion_kind: SinkRetryBudgetExhaustionKind,
) -> SinkRetryDisposition {
    if std::time::Instant::now() >= deadline {
        SinkRetryDisposition::Fail(SinkFailure::retry_budget_exhausted(exhaustion_kind))
    } else if failure.allows_retry() {
        SinkRetryDisposition::Retry
    } else {
        SinkRetryDisposition::Fail(failure)
    }
}

fn is_restart_deferred_retire_pending_deactivate_batch(envelopes: &[ControlEnvelope]) -> bool {
    let Ok(signals) = sink_control_signals_from_envelopes(envelopes) else {
        return false;
    };
    !signals.is_empty()
        && signals.iter().all(|signal| {
            matches!(
                signal,
                SinkControlSignal::Deactivate { envelope, .. }
                    if matches!(
                        capanix_runtime_entry_sdk::control::decode_runtime_exec_control(envelope),
                        Ok(Some(capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(deactivate)))
                            if deactivate.reason == "restart_deferred_retire_pending"
                    )
            )
        })
}

fn is_retryable_worker_bridge_transport_error_message(message: &str) -> bool {
    message.contains("transport closed")
        && (message.contains("Connection reset by peer")
            || message.contains("early eof")
            || message.contains("Broken pipe")
            || message.contains("bridge stopped"))
}

fn debug_control_scope_capture_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE").is_some()
}

fn debug_sink_worker_pre_dispatch_enabled() -> bool {
    std::env::var_os("FSMETA_DEBUG_SINK_WORKER_PRE_DISPATCH").is_some()
}

fn status_snapshot_nonblocking_live_probe_budget() -> Duration {
    SINK_WORKER_STATUS_NONBLOCKING_PROBE_BUDGET
        .saturating_sub(SINK_WORKER_STATUS_NONBLOCKING_SETTLE_SLACK)
}

fn summarize_bound_scopes(
    bound_scopes: &[capanix_runtime_entry_sdk::control::RuntimeBoundScope],
) -> Vec<String> {
    bound_scopes
        .iter()
        .map(|scope| format!("{}=>{}", scope.scope_id, scope.resource_ids.join("|")))
        .collect()
}

fn summarize_sink_control_signals(signals: &[SinkControlSignal]) -> Vec<String> {
    signals
        .iter()
        .map(|signal| match signal {
            SinkControlSignal::Activate {
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
            SinkControlSignal::Deactivate {
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
            SinkControlSignal::Tick {
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
            SinkControlSignal::RuntimeHostGrantChange { .. } => "host_grant_change".into(),
            SinkControlSignal::Passthrough(_) => "passthrough".into(),
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

fn summarize_sink_status_snapshot(snapshot: &SinkStatusSnapshot) -> String {
    format!(
        "groups={} scheduled={:?} control={:?} received_batches={:?} received_events={:?} received_origins={:?} received_origin_counts={:?} stream_received_batches={:?} stream_received_events={:?} stream_received_origin_counts={:?} stream_ready_origin_counts={:?} stream_deferred_origin_counts={:?} stream_dropped_origin_counts={:?} stream_applied_batches={:?} stream_applied_events={:?} stream_applied_control_events={:?} stream_applied_data_events={:?} stream_applied_origin_counts={:?} stream_last_applied_at_us={:?}",
        snapshot.groups.len(),
        summarize_groups_by_node(&snapshot.scheduled_groups_by_node),
        summarize_groups_by_node(&snapshot.last_control_frame_signals_by_node),
        snapshot.received_batches_by_node,
        snapshot.received_events_by_node,
        summarize_groups_by_node(&snapshot.last_received_origins_by_node),
        summarize_groups_by_node(&snapshot.received_origin_counts_by_node),
        snapshot.stream_received_batches_by_node,
        snapshot.stream_received_events_by_node,
        summarize_groups_by_node(&snapshot.stream_received_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_ready_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_deferred_origin_counts_by_node),
        summarize_groups_by_node(&snapshot.stream_dropped_origin_counts_by_node),
        snapshot.stream_applied_batches_by_node,
        snapshot.stream_applied_events_by_node,
        snapshot.stream_applied_control_events_by_node,
        snapshot.stream_applied_data_events_by_node,
        summarize_groups_by_node(&snapshot.stream_applied_origin_counts_by_node),
        snapshot.stream_last_applied_at_us_by_node
    )
}

fn readiness_summary_ready_groups_cover_expected(
    summary: &SinkStatusSnapshotReadinessSummary,
    expected_groups: &std::collections::BTreeSet<String>,
) -> bool {
    !expected_groups.is_empty() && expected_groups.is_subset(&summary.ready_groups)
}

#[cfg(test)]
fn classify_sink_status_snapshot_issue(
    snapshot: &SinkStatusSnapshot,
) -> Option<crate::sink::SinkStatusSnapshotIssue> {
    snapshot.issue()
}

fn snapshot_looks_stale_empty(snapshot: &SinkStatusSnapshot) -> bool {
    snapshot.looks_stale_empty()
}

fn snapshot_has_scheduled_zero_uninitialized_groups(snapshot: &SinkStatusSnapshot) -> bool {
    !snapshot.scheduled_groups_by_node.is_empty()
        && !snapshot.groups.is_empty()
        && snapshot
            .groups
            .iter()
            .all(|group| group.live_nodes == 0 && group.total_nodes == 0)
}

fn snapshot_has_delivery_evidence(snapshot: &SinkStatusSnapshot) -> bool {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusAccessPath {
    Blocking,
    ControlInflight,
    Steady,
    SteadyAfterRetryReset,
    WorkerUnavailable,
    ControlInflightNoClient,
    NotStarted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusNonblockingMode {
    ControlInflight,
    Steady,
}

#[derive(Clone, Debug)]
struct SinkStatusNonblockingProbePlan {
    mode: SinkStatusNonblockingMode,
    cached_snapshot: SinkStatusSnapshot,
    inflight_control_summaries: Vec<Vec<String>>,
}

#[derive(Clone, Debug)]
enum SinkStatusNonblockingEntryAction {
    ReturnCached {
        snapshot: SinkStatusSnapshot,
        access_path: SinkStatusAccessPath,
    },
    ProbeLive(SinkStatusNonblockingProbePlan),
}

impl SinkStatusNonblockingProbePlan {
    fn live_access_path(&self, recovered_after_retry_reset: bool) -> SinkStatusAccessPath {
        match (self.mode, recovered_after_retry_reset) {
            (SinkStatusNonblockingMode::ControlInflight, _) => {
                SinkStatusAccessPath::ControlInflight
            }
            (SinkStatusNonblockingMode::Steady, true) => {
                SinkStatusAccessPath::SteadyAfterRetryReset
            }
            (SinkStatusNonblockingMode::Steady, false) => SinkStatusAccessPath::Steady,
        }
    }

    fn err_access_path(&self) -> SinkStatusAccessPath {
        match self.mode {
            SinkStatusNonblockingMode::ControlInflight => {
                SinkStatusAccessPath::ControlInflightNoClient
            }
            SinkStatusNonblockingMode::Steady => SinkStatusAccessPath::WorkerUnavailable,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusLiveScenario {
    Blocking,
    ControlInflight,
    Steady,
    SteadyAfterRetryReset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusCachedScenario {
    WorkerUnavailable,
    ControlInflightNoClient,
    NotStarted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusScenario {
    Live(SinkStatusLiveScenario),
    Cached(SinkStatusCachedScenario),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusConcernLane {
    Clear,
    CoverageGap,
    ReplayPending,
    WaitingForMaterializedRoot,
    MixedReadiness,
    StaleEmpty,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SinkStatusScenarioFacts {
    concern: Option<SinkStatusConcern>,
    concern_lane: SinkStatusConcernLane,
    cached_concern: Option<SinkStatusConcern>,
    cached_ready_truth_covers_issue: bool,
    cached_preactivate_unscheduled: bool,
    worker_unavailable_delivery_backed_zero_uninitialized: bool,
}

impl SinkStatusAccessPath {
    fn scenario(self) -> SinkStatusScenario {
        match self {
            Self::Blocking => SinkStatusScenario::Live(SinkStatusLiveScenario::Blocking),
            Self::ControlInflight => {
                SinkStatusScenario::Live(SinkStatusLiveScenario::ControlInflight)
            }
            Self::Steady => SinkStatusScenario::Live(SinkStatusLiveScenario::Steady),
            Self::SteadyAfterRetryReset => {
                SinkStatusScenario::Live(SinkStatusLiveScenario::SteadyAfterRetryReset)
            }
            Self::WorkerUnavailable => {
                SinkStatusScenario::Cached(SinkStatusCachedScenario::WorkerUnavailable)
            }
            Self::ControlInflightNoClient => {
                SinkStatusScenario::Cached(SinkStatusCachedScenario::ControlInflightNoClient)
            }
            Self::NotStarted => SinkStatusScenario::Cached(SinkStatusCachedScenario::NotStarted),
        }
    }
}

impl SinkStatusConcernLane {
    fn from_concern(concern: Option<SinkStatusConcern>) -> Self {
        match concern {
            None => Self::Clear,
            Some(SinkStatusConcern::CoverageGap) => Self::CoverageGap,
            Some(SinkStatusConcern::ReplayPending) => Self::ReplayPending,
            Some(SinkStatusConcern::WaitingForMaterializedRoot) => Self::WaitingForMaterializedRoot,
            Some(SinkStatusConcern::MixedReadiness) => Self::MixedReadiness,
            Some(SinkStatusConcern::StaleEmpty) => Self::StaleEmpty,
        }
    }
}

impl SinkStatusScenarioFacts {
    fn new(
        concern: Option<SinkStatusConcern>,
        cached_concern: Option<SinkStatusConcern>,
        cached_ready_truth_covers_issue: bool,
    ) -> Self {
        Self {
            concern,
            concern_lane: SinkStatusConcernLane::from_concern(concern),
            cached_concern,
            cached_ready_truth_covers_issue,
            cached_preactivate_unscheduled: false,
            worker_unavailable_delivery_backed_zero_uninitialized: false,
        }
    }

    fn with_cached_preactivate_unscheduled(mut self, cached_preactivate_unscheduled: bool) -> Self {
        self.cached_preactivate_unscheduled = cached_preactivate_unscheduled;
        self
    }

    fn with_worker_unavailable_delivery_backed_zero_uninitialized(
        mut self,
        worker_unavailable_delivery_backed_zero_uninitialized: bool,
    ) -> Self {
        self.worker_unavailable_delivery_backed_zero_uninitialized =
            worker_unavailable_delivery_backed_zero_uninitialized;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusOutcomeKind {
    ReturnLive,
    ReturnCached,
    FailClosed,
    PropagateError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SinkStatusSnapshotOutcome {
    kind: SinkStatusOutcomeKind,
    concern: Option<SinkStatusConcern>,
    should_mark_replay_required: bool,
    should_republish_zero_row_summary: bool,
}

#[derive(Clone, Debug)]
struct SinkStatusSnapshotProbeOutcome {
    snapshot: SinkStatusSnapshot,
    recovered_after_retry_reset: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkStatusProbePhase {
    Initial,
    ProbeAfterRetryResetWithoutReplay,
    ReplayAfterRetryReset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SinkStatusProbeAttemptPlan {
    replay_timeout: Duration,
    attempt_timeout: Duration,
    skip_replay: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SinkStatusProbeMachine {
    deadline: std::time::Instant,
    phase: SinkStatusProbePhase,
}

#[derive(Debug)]
enum SinkStatusProbeReplayDisposition {
    BestEffort(SinkFailure),
    Fail(SinkFailure),
}

impl SinkStatusProbeMachine {
    fn new(deadline: std::time::Instant) -> Self {
        Self {
            deadline,
            phase: SinkStatusProbePhase::Initial,
        }
    }

    fn recovered_after_retry_reset(self) -> bool {
        !matches!(self.phase, SinkStatusProbePhase::Initial)
    }

    fn attempt_plan(
        self,
        replay_required_for_attempt: bool,
    ) -> std::result::Result<SinkStatusProbeAttemptPlan, SinkFailure> {
        let now = std::time::Instant::now();
        let remaining_before_replay = self.deadline.saturating_duration_since(now);
        if remaining_before_replay.is_zero() {
            return Err(SinkFailure::retry_budget_exhausted(
                SinkRetryBudgetExhaustionKind::StatusProbeAttempt,
            ));
        }
        let skip_replay = replay_required_for_attempt
            && matches!(
                self.phase,
                SinkStatusProbePhase::ProbeAfterRetryResetWithoutReplay
            );
        let replay_timeout = if replay_required_for_attempt
            && matches!(self.phase, SinkStatusProbePhase::ReplayAfterRetryReset)
        {
            let capped = remaining_before_replay
                .saturating_sub(SINK_WORKER_STATUS_RETRY_RESET_FINAL_PROBE_BUDGET);
            if capped.is_zero() {
                remaining_before_replay
            } else {
                capped
            }
        } else {
            remaining_before_replay
        };
        let now = std::time::Instant::now();
        let attempt_timeout = self.deadline.saturating_duration_since(now);
        if attempt_timeout.is_zero() {
            return Err(SinkFailure::retry_budget_exhausted(
                SinkRetryBudgetExhaustionKind::StatusProbeAttempt,
            ));
        }
        Ok(SinkStatusProbeAttemptPlan {
            replay_timeout,
            attempt_timeout,
            skip_replay,
        })
    }

    fn classify_replay_failure_disposition(
        self,
        failure: SinkFailure,
        replay_required_for_attempt: bool,
    ) -> SinkStatusProbeReplayDisposition {
        let best_effort = replay_required_for_attempt
            && matches!(self.phase, SinkStatusProbePhase::ReplayAfterRetryReset)
            && failure.supports_best_effort_replay()
            && std::time::Instant::now() < self.deadline;
        if best_effort {
            SinkStatusProbeReplayDisposition::BestEffort(SinkFailure::replay_best_effort(
                failure.cause,
            ))
        } else {
            SinkStatusProbeReplayDisposition::Fail(if std::time::Instant::now() >= self.deadline {
                SinkFailure::retry_budget_exhausted(
                    SinkRetryBudgetExhaustionKind::StatusProbeAttempt,
                )
            } else {
                failure
            })
        }
    }

    fn phase_after_retry_reset(mut self) -> Self {
        self.phase = match self.phase {
            SinkStatusProbePhase::Initial => {
                SinkStatusProbePhase::ProbeAfterRetryResetWithoutReplay
            }
            SinkStatusProbePhase::ProbeAfterRetryResetWithoutReplay
            | SinkStatusProbePhase::ReplayAfterRetryReset => {
                SinkStatusProbePhase::ReplayAfterRetryReset
            }
        };
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkControlFramePhase {
    Initial,
    AfterMissingChannelBufferRouteState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SinkControlFrameMachine {
    deadline: std::time::Instant,
    phase: SinkControlFramePhase,
    restart_deferred_retire_pending_deactivate: bool,
}

#[derive(Debug)]
enum SinkControlFrameFollowup {
    RestartAndRetry(SinkControlFrameMachine),
    RestartAndFailFast(SinkFailure),
    Failed(SinkFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SinkControlFrameFailureDisposition {
    RetrySamePhase,
    RetryAfterMissingRouteState,
    RestartAndFailFast,
    Fail,
}

impl SinkControlFrameMachine {
    fn new(deadline: std::time::Instant, envelopes: &[ControlEnvelope]) -> Self {
        Self {
            deadline,
            phase: SinkControlFramePhase::Initial,
            restart_deferred_retire_pending_deactivate:
                is_restart_deferred_retire_pending_deactivate_batch(envelopes),
        }
    }

    fn attempt_timeout(self, rpc_timeout: Duration) -> std::result::Result<Duration, SinkFailure> {
        let attempt_timeout = std::cmp::min(
            rpc_timeout,
            self.deadline
                .saturating_duration_since(std::time::Instant::now()),
        );
        if attempt_timeout.is_zero() {
            Err(SinkFailure::retry_budget_exhausted(
                SinkRetryBudgetExhaustionKind::ControlFrameAttempt,
            ))
        } else {
            Ok(attempt_timeout)
        }
    }

    fn classify_failure_disposition(
        self,
        failure: &SinkFailure,
    ) -> SinkControlFrameFailureDisposition {
        match failure.reason {
            SinkFailureReason::RetryBudgetExhausted(_)
            | SinkFailureReason::ProtocolViolation(_)
            | SinkFailureReason::ReplayHandling(_)
            | SinkFailureReason::NonRetryable => SinkControlFrameFailureDisposition::Fail,
            SinkFailureReason::TimeoutLike => SinkControlFrameFailureDisposition::RetrySamePhase,
            SinkFailureReason::ControlReset(reset_kind) => {
                if self.restart_deferred_retire_pending_deactivate {
                    SinkControlFrameFailureDisposition::RestartAndFailFast
                } else if matches!(
                    reset_kind,
                    SinkControlResetKind::MissingChannelBufferRouteState
                ) {
                    SinkControlFrameFailureDisposition::RetryAfterMissingRouteState
                } else if matches!(
                    self.phase,
                    SinkControlFramePhase::AfterMissingChannelBufferRouteState
                ) && reset_kind.is_bridge_reset_like()
                {
                    SinkControlFrameFailureDisposition::RestartAndFailFast
                } else {
                    SinkControlFrameFailureDisposition::RetrySamePhase
                }
            }
        }
    }

    fn followup_after_failure(self, failure: SinkFailure) -> SinkControlFrameFollowup {
        match self.classify_failure_disposition(&failure) {
            SinkControlFrameFailureDisposition::RetrySamePhase => {
                SinkControlFrameFollowup::RestartAndRetry(self)
            }
            SinkControlFrameFailureDisposition::RetryAfterMissingRouteState => {
                SinkControlFrameFollowup::RestartAndRetry(Self {
                    phase: SinkControlFramePhase::AfterMissingChannelBufferRouteState,
                    ..self
                })
            }
            SinkControlFrameFailureDisposition::RestartAndFailFast => {
                SinkControlFrameFollowup::RestartAndFailFast(failure)
            }
            SinkControlFrameFailureDisposition::Fail => SinkControlFrameFollowup::Failed(failure),
        }
    }

    #[cfg(test)]
    fn followup_after_error(self, err: CnxError) -> SinkControlFrameFollowup {
        let failure = if std::time::Instant::now() >= self.deadline {
            SinkFailure::retry_budget_exhausted(SinkRetryBudgetExhaustionKind::ControlFrameRetry)
        } else {
            SinkFailure::from_cause(err)
        };
        self.followup_after_failure(failure)
    }
}

fn project_sink_status_snapshot_concern(
    snapshot: &SinkStatusSnapshot,
) -> SinkStatusConcernProjection {
    snapshot.concern_projection()
}

fn sink_status_snapshot_concern(snapshot: &SinkStatusSnapshot) -> Option<SinkStatusConcern> {
    project_sink_status_snapshot_concern(snapshot).concern
}

fn cached_ready_truth_covers_live_concern(
    live_concern: SinkStatusConcern,
    live_projection: &SinkStatusConcernProjection,
    cached_projection: &SinkStatusConcernProjection,
) -> bool {
    match live_concern {
        SinkStatusConcern::CoverageGap
        | SinkStatusConcern::ReplayPending
        | SinkStatusConcern::WaitingForMaterializedRoot
        | SinkStatusConcern::MixedReadiness => readiness_summary_ready_groups_cover_expected(
            &cached_projection.summary,
            &live_projection.summary.scheduled_groups,
        ),
        SinkStatusConcern::StaleEmpty => false,
    }
}

impl SinkStatusLiveScenario {
    fn stale_empty_outcome_with_facts(
        self,
        facts: SinkStatusScenarioFacts,
    ) -> SinkStatusOutcomeKind {
        match self {
            Self::Blocking | Self::ControlInflight => SinkStatusOutcomeKind::ReturnLive,
            Self::Steady | Self::SteadyAfterRetryReset => {
                if facts.cached_preactivate_unscheduled {
                    SinkStatusOutcomeKind::ReturnLive
                } else if matches!(facts.cached_concern, Some(SinkStatusConcern::CoverageGap)) {
                    SinkStatusOutcomeKind::ReturnCached
                } else {
                    SinkStatusOutcomeKind::FailClosed
                }
            }
        }
    }

    fn cached_truth_driven_outcome(
        self,
        cached_ready_truth_covers_issue: bool,
    ) -> SinkStatusOutcomeKind {
        if cached_ready_truth_covers_issue {
            SinkStatusOutcomeKind::ReturnCached
        } else {
            SinkStatusOutcomeKind::FailClosed
        }
    }

    fn replay_pending_outcome(
        self,
        cached_ready_truth_covers_issue: bool,
    ) -> SinkStatusOutcomeKind {
        match self {
            Self::Blocking => SinkStatusOutcomeKind::FailClosed,
            Self::ControlInflight => {
                if cached_ready_truth_covers_issue {
                    SinkStatusOutcomeKind::ReturnCached
                } else {
                    SinkStatusOutcomeKind::ReturnLive
                }
            }
            Self::Steady | Self::SteadyAfterRetryReset => {
                self.cached_truth_driven_outcome(cached_ready_truth_covers_issue)
            }
        }
    }

    fn replay_pending_outcome_with_facts(
        self,
        facts: SinkStatusScenarioFacts,
    ) -> SinkStatusOutcomeKind {
        if facts.cached_ready_truth_covers_issue {
            SinkStatusOutcomeKind::ReturnCached
        } else if matches!(facts.cached_concern, Some(SinkStatusConcern::StaleEmpty)) {
            SinkStatusOutcomeKind::ReturnLive
        } else if facts.cached_preactivate_unscheduled {
            SinkStatusOutcomeKind::ReturnLive
        } else {
            SinkStatusOutcomeKind::FailClosed
        }
    }

    fn waiting_for_materialized_root_outcome(
        self,
        cached_ready_truth_covers_issue: bool,
    ) -> SinkStatusOutcomeKind {
        match self {
            Self::Blocking => SinkStatusOutcomeKind::FailClosed,
            Self::ControlInflight | Self::Steady => {
                self.cached_truth_driven_outcome(cached_ready_truth_covers_issue)
            }
            Self::SteadyAfterRetryReset => SinkStatusOutcomeKind::ReturnLive,
        }
    }

    fn outcome_kind(self, facts: SinkStatusScenarioFacts) -> SinkStatusOutcomeKind {
        match facts.concern_lane {
            SinkStatusConcernLane::Clear => SinkStatusOutcomeKind::ReturnLive,
            SinkStatusConcernLane::StaleEmpty => self.stale_empty_outcome_with_facts(facts),
            SinkStatusConcernLane::ReplayPending => match self {
                Self::Steady | Self::SteadyAfterRetryReset => {
                    self.replay_pending_outcome_with_facts(facts)
                }
                _ => self.replay_pending_outcome(facts.cached_ready_truth_covers_issue),
            },
            SinkStatusConcernLane::CoverageGap => match self {
                Self::Blocking => SinkStatusOutcomeKind::FailClosed,
                Self::ControlInflight | Self::Steady | Self::SteadyAfterRetryReset => {
                    self.cached_truth_driven_outcome(facts.cached_ready_truth_covers_issue)
                }
            },
            SinkStatusConcernLane::WaitingForMaterializedRoot => {
                self.waiting_for_materialized_root_outcome(facts.cached_ready_truth_covers_issue)
            }
            SinkStatusConcernLane::MixedReadiness => match self {
                Self::Blocking => SinkStatusOutcomeKind::ReturnLive,
                Self::ControlInflight | Self::Steady | Self::SteadyAfterRetryReset => {
                    self.cached_truth_driven_outcome(facts.cached_ready_truth_covers_issue)
                }
            },
        }
    }
}

impl SinkStatusCachedScenario {
    fn outcome_kind(self, facts: SinkStatusScenarioFacts) -> SinkStatusOutcomeKind {
        if matches!(self, Self::WorkerUnavailable)
            && facts.worker_unavailable_delivery_backed_zero_uninitialized
        {
            return SinkStatusOutcomeKind::PropagateError;
        }
        if matches!(self, Self::WorkerUnavailable)
            && matches!(facts.concern_lane, SinkStatusConcernLane::StaleEmpty)
            && facts.cached_preactivate_unscheduled
        {
            return SinkStatusOutcomeKind::ReturnCached;
        }
        match (self, facts.concern_lane) {
            (_, SinkStatusConcernLane::Clear)
            | (_, SinkStatusConcernLane::WaitingForMaterializedRoot) => {
                SinkStatusOutcomeKind::ReturnCached
            }
            (_, SinkStatusConcernLane::ReplayPending)
            | (Self::WorkerUnavailable | Self::NotStarted, SinkStatusConcernLane::CoverageGap) => {
                SinkStatusOutcomeKind::ReturnCached
            }
            (
                Self::WorkerUnavailable | Self::ControlInflightNoClient,
                SinkStatusConcernLane::StaleEmpty,
            )
            | (Self::WorkerUnavailable, SinkStatusConcernLane::MixedReadiness) => {
                SinkStatusOutcomeKind::PropagateError
            }
            (Self::NotStarted, SinkStatusConcernLane::StaleEmpty)
            | (
                Self::ControlInflightNoClient | Self::NotStarted,
                SinkStatusConcernLane::CoverageGap | SinkStatusConcernLane::MixedReadiness,
            ) => SinkStatusOutcomeKind::FailClosed,
        }
    }
}

impl SinkStatusScenario {
    fn outcome_kind(self, facts: SinkStatusScenarioFacts) -> SinkStatusOutcomeKind {
        match self {
            Self::Live(live_scenario) => live_scenario.outcome_kind(facts),
            Self::Cached(cached_scenario) => cached_scenario.outcome_kind(facts),
        }
    }

    fn should_mark_replay_required(self, concern_lane: SinkStatusConcernLane) -> bool {
        matches!(
            (self, concern_lane),
            (
                Self::Live(
                    SinkStatusLiveScenario::Blocking
                        | SinkStatusLiveScenario::ControlInflight
                        | SinkStatusLiveScenario::Steady
                ),
                SinkStatusConcernLane::ReplayPending
            )
        )
    }

    fn should_republish_zero_row_summary(self, concern_lane: SinkStatusConcernLane) -> bool {
        matches!(concern_lane, SinkStatusConcernLane::StaleEmpty) && matches!(self, Self::Cached(_))
    }
}

fn reduce_sink_status_scenario_outcome(
    scenario: SinkStatusScenario,
    concern: Option<SinkStatusConcern>,
    cached_concern: Option<SinkStatusConcern>,
    cached_ready_truth_covers_issue: bool,
) -> SinkStatusSnapshotOutcome {
    let facts =
        SinkStatusScenarioFacts::new(concern, cached_concern, cached_ready_truth_covers_issue);
    reduce_sink_status_scenario_outcome_with_facts(scenario, facts)
}

fn reduce_sink_status_scenario_outcome_with_facts(
    scenario: SinkStatusScenario,
    facts: SinkStatusScenarioFacts,
) -> SinkStatusSnapshotOutcome {
    let kind = scenario.outcome_kind(facts);
    let should_mark_replay_required = scenario.should_mark_replay_required(facts.concern_lane);
    let should_republish_zero_row_summary =
        scenario.should_republish_zero_row_summary(facts.concern_lane);
    SinkStatusSnapshotOutcome {
        kind,
        concern: facts.concern,
        should_mark_replay_required,
        should_republish_zero_row_summary,
    }
}

fn evaluate_live_sink_status_snapshot(
    live_snapshot: &SinkStatusSnapshot,
    cached_snapshot: &SinkStatusSnapshot,
    cached_concern: Option<SinkStatusConcern>,
    access_path: SinkStatusAccessPath,
) -> SinkStatusSnapshotOutcome {
    let SinkStatusScenario::Live(scenario) = access_path.scenario() else {
        unreachable!("cached-only access path passed to live sink status evaluation");
    };
    let live_projection = project_sink_status_snapshot_concern(live_snapshot);
    let cached_projection = project_sink_status_snapshot_concern(cached_snapshot);
    let live_concern = live_projection.concern;
    let cached_ready_truth_covers_issue = live_concern.is_some_and(|concern| {
        cached_ready_truth_covers_live_concern(concern, &live_projection, &cached_projection)
    });
    let cached_preactivate_unscheduled = !snapshot_has_delivery_evidence(cached_snapshot)
        && (cached_snapshot.scheduled_groups_by_node.is_empty()
            || snapshot_has_scheduled_zero_uninitialized_groups(cached_snapshot));
    reduce_sink_status_scenario_outcome_with_facts(
        SinkStatusScenario::Live(scenario),
        SinkStatusScenarioFacts::new(
            live_concern,
            cached_concern,
            cached_ready_truth_covers_issue,
        )
        .with_cached_preactivate_unscheduled(cached_preactivate_unscheduled),
    )
}

fn evaluate_cached_sink_status_snapshot(
    cached_snapshot: &SinkStatusSnapshot,
    access_path: SinkStatusAccessPath,
) -> SinkStatusSnapshotOutcome {
    let SinkStatusScenario::Cached(scenario) = access_path.scenario() else {
        unreachable!("live access path passed to cached sink status evaluation");
    };
    let cached_projection = project_sink_status_snapshot_concern(cached_snapshot);
    let delivery_backed_zero_uninitialized =
        matches!(scenario, SinkStatusCachedScenario::WorkerUnavailable)
            && snapshot_has_scheduled_zero_uninitialized_groups(cached_snapshot)
            && !matches!(
                cached_projection.concern,
                Some(SinkStatusConcern::WaitingForMaterializedRoot)
            )
            && snapshot_has_delivery_evidence(cached_snapshot);
    let cached_preactivate_unscheduled = !snapshot_has_delivery_evidence(cached_snapshot)
        && (cached_snapshot.scheduled_groups_by_node.is_empty()
            || snapshot_has_scheduled_zero_uninitialized_groups(cached_snapshot));
    reduce_sink_status_scenario_outcome_with_facts(
        SinkStatusScenario::Cached(scenario),
        SinkStatusScenarioFacts::new(cached_projection.concern, None, false)
            .with_cached_preactivate_unscheduled(cached_preactivate_unscheduled)
            .with_worker_unavailable_delivery_backed_zero_uninitialized(
                delivery_backed_zero_uninitialized,
            ),
    )
}

fn apply_live_sink_status_snapshot_outcome_side_effects(
    sink: &SinkWorkerClientHandle,
    snapshot: &SinkStatusSnapshot,
    outcome: &SinkStatusSnapshotOutcome,
) -> Result<()> {
    if outcome.should_mark_replay_required {
        sink.control_state_replay_required
            .store(1, Ordering::Release);
    }
    if matches!(outcome.kind, SinkStatusOutcomeKind::ReturnLive) {
        sink.update_cached_status_snapshot(snapshot.clone())?;
    }
    Ok(())
}

fn republish_scheduled_groups_into_zero_row_summary(
    snapshot: &mut SinkStatusSnapshot,
    node_id: &NodeId,
    groups: &std::collections::BTreeSet<String>,
) {
    if groups.is_empty() || !snapshot.scheduled_groups_by_node.is_empty() {
        return;
    }
    let zero_rows_only = !snapshot.groups.is_empty()
        && snapshot
            .groups
            .iter()
            .all(|group| group.live_nodes == 0 && group.total_nodes == 0);
    let rows_cover_cached_schedule = !snapshot.groups.is_empty()
        && groups.iter().all(|group_id| {
            snapshot
                .groups
                .iter()
                .any(|group| group.group_id == *group_id)
        });
    if !snapshot.groups.is_empty() && !zero_rows_only && !rows_cover_cached_schedule {
        return;
    }
    snapshot.scheduled_groups_by_node =
        std::collections::BTreeMap::from([(node_id.0.clone(), groups.iter().cloned().collect())]);
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

pub(crate) fn stable_host_ref_for_node_id(node_id: &NodeId, grants: &[GrantedMountRoot]) -> String {
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
    entry.extend(groups);
    entry.sort();
    entry.dedup();
}

fn normalize_sink_status_snapshot_node_keys(
    snapshot: &mut SinkStatusSnapshot,
    node_id: &NodeId,
    grants: &[GrantedMountRoot],
) {
    let stable_host_ref = stable_host_ref_for_node_id(node_id, grants);
    normalize_node_groups_key(
        &mut snapshot.scheduled_groups_by_node,
        &node_id.0,
        &stable_host_ref,
    );
    normalize_node_groups_key(
        &mut snapshot.last_control_frame_signals_by_node,
        &node_id.0,
        &stable_host_ref,
    );
}

fn decode_exact_query_node(events: Vec<Event>, path: &[u8]) -> Result<Option<QueryNode>> {
    let mut selected = None::<QueryNode>;
    for event in &events {
        let payload = rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
            .map_err(|e| CnxError::Internal(format!("decode query_node response failed: {e}")))?;
        let MaterializedQueryPayload::Tree(response) = payload else {
            return Err(CnxError::Internal(
                "unexpected stats payload for query_node".into(),
            ));
        };
        let mut consider = |node: QueryNode| {
            if node.path != path {
                return;
            }
            match selected.as_mut() {
                Some(existing) if existing.modified_time_us > node.modified_time_us => {}
                Some(existing) => *existing = node,
                None => selected = Some(node),
            }
        };
        if response.root.exists {
            consider(QueryNode {
                path: response.root.path.clone(),
                file_name: root_file_name_bytes(&response.root.path),
                size: response.root.size,
                modified_time_us: response.root.modified_time_us,
                is_dir: response.root.is_dir,
                monitoring_attested: response.reliability.reliable,
                is_suspect: false,
                is_blind_spot: false,
            });
        }
        for entry in response.entries {
            consider(QueryNode {
                file_name: root_file_name_bytes(&entry.path),
                path: entry.path,
                size: entry.size,
                modified_time_us: entry.modified_time_us,
                is_dir: entry.is_dir,
                monitoring_attested: response.reliability.reliable,
                is_suspect: false,
                is_blind_spot: false,
            });
        }
    }
    Ok(selected)
}

#[derive(Clone)]
pub struct SinkWorkerClientHandle {
    _shared: Arc<SharedSinkWorkerHandleState>,
    node_id: NodeId,
    worker_factory: RuntimeWorkerClientFactory,
    worker_binding: RuntimeWorkerBinding,
    worker: Arc<tokio::sync::Mutex<SharedSinkWorkerClient>>,
    config: Arc<Mutex<SourceConfig>>,
    logical_roots_cache: Arc<Mutex<Vec<crate::source::config::RootSpec>>>,
    status_cache: Arc<Mutex<SinkStatusSnapshot>>,
    scheduled_groups_cache: Arc<Mutex<Option<std::collections::BTreeSet<String>>>>,
    retained_control_state: Arc<tokio::sync::Mutex<RetainedSinkWorkerControlState>>,
    control_state_replay_required: Arc<AtomicUsize>,
    control_ops_inflight: Arc<AtomicUsize>,
    control_ops_state_tx: tokio::sync::watch::Sender<usize>,
    control_ops_state: tokio::sync::watch::Receiver<usize>,
    inflight_control_frame_summaries: Arc<Mutex<Vec<Vec<String>>>>,
}

struct SharedSinkWorkerHandleState {
    worker: Arc<tokio::sync::Mutex<SharedSinkWorkerClient>>,
    config: Arc<Mutex<SourceConfig>>,
    logical_roots_cache: Arc<Mutex<Vec<crate::source::config::RootSpec>>>,
    status_cache: Arc<Mutex<SinkStatusSnapshot>>,
    scheduled_groups_cache: Arc<Mutex<Option<std::collections::BTreeSet<String>>>>,
    retained_control_state: Arc<tokio::sync::Mutex<RetainedSinkWorkerControlState>>,
    control_state_replay_required: Arc<AtomicUsize>,
    control_ops_inflight: Arc<AtomicUsize>,
    control_ops_state: tokio::sync::watch::Sender<usize>,
    inflight_control_frame_summaries: Arc<Mutex<Vec<Vec<String>>>>,
}

struct SharedSinkWorkerClient {
    instance_id: u64,
    client: Arc<TypedRuntimeWorkerClient<SinkWorkerRpc, SourceConfig>>,
}

#[derive(Default, Clone)]
struct RetainedSinkWorkerControlState {
    latest_host_grant_change: Option<SinkControlSignal>,
    active_by_route: std::collections::BTreeMap<(String, String), SinkControlSignal>,
}

fn retained_scheduled_group_ids(
    retained: &RetainedSinkWorkerControlState,
) -> Option<std::collections::BTreeSet<String>> {
    let groups = retained
        .active_by_route
        .values()
        .filter_map(|signal| match signal {
            SinkControlSignal::Activate { bound_scopes, .. } => Some(bound_scopes.as_slice()),
            _ => None,
        })
        .flat_map(|bound_scopes| bound_scopes.iter())
        .map(|scope| scope.scope_id.trim())
        .filter(|scope_id| !scope_id.is_empty())
        .map(|scope_id| scope_id.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    (!groups.is_empty()).then_some(groups)
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

struct InflightControlFrameSummaryGuard {
    summaries: Arc<Mutex<Vec<Vec<String>>>>,
    summary: Option<Vec<String>>,
}

impl Drop for InflightControlFrameSummaryGuard {
    fn drop(&mut self) {
        let Some(summary) = self.summary.take() else {
            return;
        };
        let mut guard = match self.summaries.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(index) = guard.iter().position(|candidate| *candidate == summary) {
            guard.remove(index);
        }
    }
}

fn snapshot_reflects_inflight_control_frame_summaries(
    snapshot: &SinkStatusSnapshot,
    node_id: &NodeId,
    inflight_summaries: &[Vec<String>],
) -> bool {
    if inflight_summaries.is_empty() {
        return true;
    }
    let Some(applied) = snapshot.last_control_frame_signals_by_node.get(&node_id.0) else {
        return false;
    };
    inflight_summaries
        .iter()
        .all(|summary| summary.iter().all(|signal| applied.contains(signal)))
}

fn next_shared_sink_worker_instance_id() -> u64 {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn sink_worker_handle_registry_key(
    node_id: &NodeId,
    worker_binding: &RuntimeWorkerBinding,
    worker_factory: &RuntimeWorkerClientFactory,
) -> String {
    let runtime_boundary_id = {
        let io_boundary = worker_factory.io_boundary();
        Arc::as_ptr(&io_boundary) as *const () as usize
    };
    format!(
        "{}|{}|{:?}|{:?}|{}|{}|{}",
        node_id.0,
        worker_binding.role_id,
        worker_binding.mode,
        worker_binding.launcher_kind,
        runtime_boundary_id,
        worker_binding
            .module_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
        worker_binding
            .socket_dir
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default()
    )
}

fn sink_worker_handle_registry()
-> &'static Mutex<std::collections::BTreeMap<String, Weak<SharedSinkWorkerHandleState>>> {
    static REGISTRY: std::sync::OnceLock<
        Mutex<std::collections::BTreeMap<String, Weak<SharedSinkWorkerHandleState>>>,
    > = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(std::collections::BTreeMap::new()))
}

fn lock_sink_worker_handle_registry() -> std::sync::MutexGuard<
    'static,
    std::collections::BTreeMap<String, Weak<SharedSinkWorkerHandleState>>,
> {
    match sink_worker_handle_registry().lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            log::warn!("sink worker handle registry lock poisoned; recovering shared handle state");
            poisoned.into_inner()
        }
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SinkWorkerCloseHook {
    pub entered: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SinkWorkerUpdateRootsHook {
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
pub(crate) struct SinkWorkerControlFrameErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
pub(crate) struct SinkWorkerControlFrameErrorQueueHook {
    pub errs: std::collections::VecDeque<CnxError>,
    pub sticky_worker_instance_id: Option<u64>,
    pub sticky_peer_err: Option<String>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SinkWorkerControlFramePauseHook {
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
pub(crate) struct SinkWorkerStatusErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
struct SinkWorkerStatusErrorHookSlot {
    target_worker_instance_id: Option<u64>,
    err: CnxError,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SinkWorkerStatusTimeoutObserveHook {
    pub observed_timeouts: Arc<Mutex<Vec<Duration>>>,
}

#[cfg(test)]
pub(crate) struct SinkWorkerStatusSnapshotHook {
    pub snapshot: SinkStatusSnapshot,
}

#[cfg(test)]
struct SinkWorkerStatusSnapshotHookSlot {
    target_worker_instance_id: Option<u64>,
    snapshot: SinkStatusSnapshot,
}

#[cfg(test)]
pub(crate) struct SinkWorkerStatusResponseQueueHook {
    pub replies: std::collections::VecDeque<Result<SinkWorkerResponse>>,
}

#[cfg(test)]
pub(crate) struct SinkWorkerScheduledGroupsErrorHook {
    pub err: CnxError,
}

#[cfg(test)]
pub(crate) struct SinkWorkerStatusNonblockingCacheFallbackHook;

#[cfg(test)]
struct SinkWorkerStatusNonblockingCacheFallbackHookSlot {
    target_worker_instance_id: Option<u64>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SinkWorkerRetryResetHook {
    pub reset_count: Arc<AtomicUsize>,
}

#[cfg(test)]
fn sink_worker_close_hook_cell() -> &'static Mutex<Option<SinkWorkerCloseHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerCloseHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_update_roots_hook_cell() -> &'static Mutex<Option<SinkWorkerUpdateRootsHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerUpdateRootsHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_control_frame_error_hook_cell()
-> &'static Mutex<Option<SinkWorkerControlFrameErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerControlFrameErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_control_frame_error_queue_hook_cell()
-> &'static Mutex<Option<SinkWorkerControlFrameErrorQueueHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerControlFrameErrorQueueHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_control_frame_pause_hook_cell()
-> &'static Mutex<Option<SinkWorkerControlFramePauseHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerControlFramePauseHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_status_error_hook_cell() -> &'static Mutex<Option<SinkWorkerStatusErrorHookSlot>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerStatusErrorHookSlot>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_status_snapshot_hook_cell()
-> &'static Mutex<Option<SinkWorkerStatusSnapshotHookSlot>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerStatusSnapshotHookSlot>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_status_timeout_observe_hook_cell()
-> &'static Mutex<Option<SinkWorkerStatusTimeoutObserveHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerStatusTimeoutObserveHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_status_response_queue_hook_cell()
-> &'static Mutex<Option<SinkWorkerStatusResponseQueueHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerStatusResponseQueueHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_scheduled_groups_error_hook_cell()
-> &'static Mutex<Option<SinkWorkerScheduledGroupsErrorHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerScheduledGroupsErrorHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_status_nonblocking_cache_fallback_hook_cell()
-> &'static Mutex<Option<SinkWorkerStatusNonblockingCacheFallbackHookSlot>> {
    static CELL: std::sync::OnceLock<
        Mutex<Option<SinkWorkerStatusNonblockingCacheFallbackHookSlot>>,
    > = std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn sink_worker_retry_reset_hook_cell() -> &'static Mutex<Option<SinkWorkerRetryResetHook>> {
    static CELL: std::sync::OnceLock<Mutex<Option<SinkWorkerRetryResetHook>>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub(crate) fn install_sink_worker_close_hook(hook: SinkWorkerCloseHook) {
    let mut guard = match sink_worker_close_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_update_roots_hook(hook: SinkWorkerUpdateRootsHook) {
    let mut guard = match sink_worker_update_roots_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_close_hook() {
    let mut guard = match sink_worker_close_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_update_roots_hook() {
    let mut guard = match sink_worker_update_roots_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn install_sink_worker_control_frame_error_hook(hook: SinkWorkerControlFrameErrorHook) {
    let mut guard = match sink_worker_control_frame_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_control_frame_error_queue_hook(
    hook: SinkWorkerControlFrameErrorQueueHook,
) {
    let mut guard = match sink_worker_control_frame_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_control_frame_pause_hook(hook: SinkWorkerControlFramePauseHook) {
    let mut guard = match sink_worker_control_frame_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_error_hook(hook: SinkWorkerStatusErrorHook) {
    let mut guard = match sink_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(SinkWorkerStatusErrorHookSlot {
        target_worker_instance_id: None,
        err: hook.err,
    });
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_error_hook_for_worker_instance(
    worker_instance_id: u64,
    hook: SinkWorkerStatusErrorHook,
) {
    let mut guard = match sink_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(SinkWorkerStatusErrorHookSlot {
        target_worker_instance_id: Some(worker_instance_id),
        err: hook.err,
    });
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_snapshot_hook(hook: SinkWorkerStatusSnapshotHook) {
    let mut guard = match sink_worker_status_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(SinkWorkerStatusSnapshotHookSlot {
        target_worker_instance_id: None,
        snapshot: hook.snapshot,
    });
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_snapshot_hook_for_worker_instance(
    worker_instance_id: u64,
    hook: SinkWorkerStatusSnapshotHook,
) {
    let mut guard = match sink_worker_status_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(SinkWorkerStatusSnapshotHookSlot {
        target_worker_instance_id: Some(worker_instance_id),
        snapshot: hook.snapshot,
    });
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_timeout_observe_hook(
    hook: SinkWorkerStatusTimeoutObserveHook,
) {
    let mut guard = match sink_worker_status_timeout_observe_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_response_queue_hook(
    hook: SinkWorkerStatusResponseQueueHook,
) {
    let mut guard = match sink_worker_status_response_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_scheduled_groups_error_hook(
    hook: SinkWorkerScheduledGroupsErrorHook,
) {
    let mut guard = match sink_worker_scheduled_groups_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_nonblocking_cache_fallback_hook(
    hook: SinkWorkerStatusNonblockingCacheFallbackHook,
) {
    install_sink_worker_status_nonblocking_cache_fallback_hook_for_worker_instance_inner(
        None, hook,
    );
}

#[cfg(test)]
fn install_sink_worker_status_nonblocking_cache_fallback_hook_for_worker_instance_inner(
    worker_instance_id: Option<u64>,
    _hook: SinkWorkerStatusNonblockingCacheFallbackHook,
) {
    let mut guard = match sink_worker_status_nonblocking_cache_fallback_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(SinkWorkerStatusNonblockingCacheFallbackHookSlot {
        target_worker_instance_id: worker_instance_id,
    });
}

#[cfg(test)]
pub(crate) fn install_sink_worker_status_nonblocking_cache_fallback_hook_for_worker_instance(
    worker_instance_id: u64,
    hook: SinkWorkerStatusNonblockingCacheFallbackHook,
) {
    install_sink_worker_status_nonblocking_cache_fallback_hook_for_worker_instance_inner(
        Some(worker_instance_id),
        hook,
    );
}

#[cfg(test)]
pub(crate) fn install_sink_worker_retry_reset_hook(hook: SinkWorkerRetryResetHook) {
    let mut guard = match sink_worker_retry_reset_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(hook);
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_control_frame_error_hook() {
    let mut guard = match sink_worker_control_frame_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
    drop(guard);
    let mut queued = match sink_worker_control_frame_error_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *queued = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_scheduled_groups_error_hook() {
    let mut guard = match sink_worker_scheduled_groups_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_status_nonblocking_cache_fallback_hook() {
    let mut guard = match sink_worker_status_nonblocking_cache_fallback_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_retry_reset_hook() {
    let mut guard = match sink_worker_retry_reset_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_control_frame_pause_hook() {
    let mut guard = match sink_worker_control_frame_pause_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_status_error_hook() {
    let mut guard = match sink_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_status_snapshot_hook() {
    let mut guard = match sink_worker_status_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_status_timeout_observe_hook() {
    let mut guard = match sink_worker_status_timeout_observe_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
pub(crate) fn clear_sink_worker_status_response_queue_hook() {
    let mut guard = match sink_worker_status_response_queue_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = None;
}

#[cfg(test)]
fn take_sink_worker_status_error_hook(current_worker_instance_id: u64) -> Option<CnxError> {
    let mut guard = match sink_worker_status_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_ref()?;
    if let Some(target_worker_instance_id) = hook.target_worker_instance_id
        && target_worker_instance_id != current_worker_instance_id
    {
        return None;
    }
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
fn take_sink_worker_status_snapshot_hook(
    current_worker_instance_id: u64,
) -> Option<SinkStatusSnapshot> {
    let mut guard = match sink_worker_status_snapshot_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let hook = guard.as_ref()?;
    if let Some(target_worker_instance_id) = hook.target_worker_instance_id
        && target_worker_instance_id != current_worker_instance_id
    {
        return None;
    }
    guard.take().map(|hook| hook.snapshot)
}

#[cfg(test)]
fn record_sink_worker_status_timeout_probe(timeout: Duration) {
    let hook = {
        let guard = match sink_worker_status_timeout_observe_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        let mut observed = match hook.observed_timeouts.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        observed.push(timeout);
    }
}

#[cfg(test)]
fn take_sink_worker_status_response_queue_hook() -> Option<Result<SinkWorkerResponse>> {
    let mut guard = match sink_worker_status_response_queue_hook_cell().lock() {
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
fn take_sink_worker_scheduled_groups_error_hook() -> Option<CnxError> {
    let mut guard = match sink_worker_scheduled_groups_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
fn take_sink_worker_status_nonblocking_cache_fallback_hook(
    current_worker_instance_id: u64,
) -> bool {
    let mut guard = match sink_worker_status_nonblocking_cache_fallback_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let Some(hook) = guard.take() else {
        return false;
    };
    if let Some(target_worker_instance_id) = hook.target_worker_instance_id
        && target_worker_instance_id != current_worker_instance_id
    {
        *guard = Some(hook);
        return false;
    }
    true
}

#[cfg(test)]
fn take_sink_worker_control_frame_error_hook(current_worker_instance_id: u64) -> Option<CnxError> {
    {
        let mut guard = match sink_worker_control_frame_error_queue_hook_cell().lock() {
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
    let mut guard = match sink_worker_control_frame_error_hook_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.take().map(|hook| hook.err)
}

#[cfg(test)]
async fn maybe_pause_before_on_control_frame_rpc() {
    let hook = {
        let mut guard = match sink_worker_control_frame_pause_hook_cell().lock() {
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
fn notify_sink_worker_close_started() {
    let hook = {
        let guard = match sink_worker_close_hook_cell().lock() {
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
fn notify_sink_worker_retry_reset() {
    let hook = {
        let guard = match sink_worker_retry_reset_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    };
    if let Some(hook) = hook {
        hook.reset_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
async fn maybe_pause_before_update_logical_roots_rpc() {
    let hook = {
        let mut guard = match sink_worker_update_roots_hook_cell().lock() {
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

impl SinkWorkerClientHandle {
    pub(crate) fn new(
        node_id: NodeId,
        config: SourceConfig,
        worker_binding: RuntimeWorkerBinding,
        worker_factory: RuntimeWorkerClientFactory,
    ) -> Result<Self> {
        let key = sink_worker_handle_registry_key(&node_id, &worker_binding, &worker_factory);
        let shared = {
            let mut registry = lock_sink_worker_handle_registry();
            if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
                existing
            } else {
                let (control_ops_state, _control_ops_state_rx) = tokio::sync::watch::channel(0);
                let shared = Arc::new(SharedSinkWorkerHandleState {
                    worker: Arc::new(tokio::sync::Mutex::new(SharedSinkWorkerClient {
                        instance_id: next_shared_sink_worker_instance_id(),
                        client: Arc::new(worker_factory.connect(
                            node_id.clone(),
                            config.clone(),
                            worker_binding.clone(),
                        )?),
                    })),
                    config: Arc::new(Mutex::new(config.clone())),
                    logical_roots_cache: Arc::new(Mutex::new(config.roots.clone())),
                    status_cache: Arc::new(Mutex::new(SinkStatusSnapshot::default())),
                    scheduled_groups_cache: Arc::new(Mutex::new(None)),
                    retained_control_state: Arc::new(tokio::sync::Mutex::new(
                        RetainedSinkWorkerControlState::default(),
                    )),
                    control_state_replay_required: Arc::new(AtomicUsize::new(0)),
                    control_ops_inflight: Arc::new(AtomicUsize::new(0)),
                    control_ops_state,
                    inflight_control_frame_summaries: Arc::new(Mutex::new(Vec::new())),
                });
                registry.insert(key, Arc::downgrade(&shared));
                shared
            }
        };
        Ok(Self {
            _shared: shared.clone(),
            node_id,
            worker_factory,
            worker_binding,
            worker: shared.worker.clone(),
            config: shared.config.clone(),
            logical_roots_cache: shared.logical_roots_cache.clone(),
            status_cache: shared.status_cache.clone(),
            scheduled_groups_cache: shared.scheduled_groups_cache.clone(),
            retained_control_state: shared.retained_control_state.clone(),
            control_state_replay_required: shared.control_state_replay_required.clone(),
            control_ops_inflight: shared.control_ops_inflight.clone(),
            control_ops_state_tx: shared.control_ops_state.clone(),
            control_ops_state: shared.control_ops_state.subscribe(),
            inflight_control_frame_summaries: shared.inflight_control_frame_summaries.clone(),
        })
    }

    fn begin_control_op(&self) -> InflightControlOpGuard {
        let next = self.control_ops_inflight.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.control_ops_state_tx.send_replace(next);
        InflightControlOpGuard {
            counter: self.control_ops_inflight.clone(),
            state: self.control_ops_state_tx.clone(),
        }
    }

    fn begin_control_frame_summary_tracking(
        &self,
        summary: Option<Vec<String>>,
    ) -> InflightControlFrameSummaryGuard {
        if let Some(summary) = summary.as_ref().filter(|summary| !summary.is_empty()) {
            let mut guard = match self.inflight_control_frame_summaries.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.push(summary.clone());
        }
        InflightControlFrameSummaryGuard {
            summaries: self.inflight_control_frame_summaries.clone(),
            summary,
        }
    }

    fn inflight_control_frame_summaries(&self) -> Vec<Vec<String>> {
        let guard = match self.inflight_control_frame_summaries.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
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

    pub(crate) fn control_op_inflight_for_internal_status(&self) -> bool {
        self.control_op_inflight()
    }

    async fn shared_worker(
        &self,
    ) -> (
        u64,
        Arc<TypedRuntimeWorkerClient<SinkWorkerRpc, SourceConfig>>,
    ) {
        let guard = self.worker.lock().await;
        (guard.instance_id, guard.client.clone())
    }

    async fn worker_client(&self) -> Arc<TypedRuntimeWorkerClient<SinkWorkerRpc, SourceConfig>> {
        self.shared_worker().await.1
    }

    async fn current_generation_tick_fast_path_eligible(&self) -> bool {
        self.control_state_replay_required.load(Ordering::Acquire) == 0
            && self
                .existing_client_with_failure()
                .await
                .ok()
                .flatten()
                .is_some()
    }

    fn current_config(&self) -> Result<SourceConfig> {
        self.config
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| CnxError::Internal("sink worker config lock poisoned".into()))
    }

    fn update_cached_runtime_config(
        &self,
        roots: &[crate::source::config::RootSpec],
        host_object_grants: &[GrantedMountRoot],
    ) -> Result<()> {
        let mut guard = self
            .config
            .lock()
            .map_err(|_| CnxError::Internal("sink worker config lock poisoned".into()))?;
        guard.roots = roots.to_vec();
        guard.host_object_grants = host_object_grants.to_vec();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn worker_instance_id_for_tests(&self) -> u64 {
        self.shared_worker().await.0
    }

    #[cfg(test)]
    async fn shared_worker_identity_for_tests(&self) -> usize {
        Arc::as_ptr(&self.worker_client().await) as *const () as usize
    }

    #[cfg(test)]
    pub(crate) async fn shared_worker_existing_client_for_tests(
        &self,
    ) -> Result<Option<TypedWorkerClient<SinkWorkerRpc>>> {
        self.worker_client().await.existing_client().await
    }

    #[cfg(test)]
    async fn shutdown_shared_worker_for_tests(&self, timeout: Duration) -> Result<()> {
        self.worker_client().await.shutdown(timeout).await
    }

    fn update_cached_logical_roots(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
    ) -> Result<()> {
        let mut guard = self.logical_roots_cache.lock().map_err(|_| {
            CnxError::Internal("sink worker logical roots cache lock poisoned".into())
        })?;
        *guard = roots;
        Ok(())
    }

    fn cached_status_snapshot_with_failure(
        &self,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        self.status_cache
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| {
                SinkFailure::from_cause(CnxError::Internal(
                    "sink worker status cache lock poisoned".into(),
                ))
            })
    }

    fn update_cached_status_snapshot(&self, snapshot: SinkStatusSnapshot) -> Result<()> {
        let mut guard = self
            .status_cache
            .lock()
            .map_err(|_| CnxError::Internal("sink worker status cache lock poisoned".into()))?;
        *guard = snapshot;
        Ok(())
    }

    fn cached_scheduled_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.scheduled_groups_cache
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| {
                CnxError::Internal("sink worker scheduled groups cache lock poisoned".into())
            })
    }

    fn cached_scheduled_group_ids_with_failure(
        &self,
    ) -> std::result::Result<Option<std::collections::BTreeSet<String>>, SinkFailure> {
        self.scheduled_groups_cache
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| {
                SinkFailure::from_cause(CnxError::Internal(
                    "sink worker scheduled groups cache lock poisoned".into(),
                ))
            })
    }

    fn replace_cached_scheduled_group_ids(
        &self,
        groups: Option<std::collections::BTreeSet<String>>,
    ) -> Result<()> {
        let mut guard = self.scheduled_groups_cache.lock().map_err(|_| {
            CnxError::Internal("sink worker scheduled groups cache lock poisoned".into())
        })?;
        *guard = groups;
        Ok(())
    }

    fn update_cached_scheduled_group_ids(
        &self,
        groups: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        self.replace_cached_scheduled_group_ids(Some(groups.clone()))
    }

    fn republish_cached_scheduled_groups_into_empty_status_summary(&self) -> Result<()> {
        let Some(groups) = self.cached_scheduled_group_ids()? else {
            return Ok(());
        };
        let mut guard = self
            .status_cache
            .lock()
            .map_err(|_| CnxError::Internal("sink worker status cache lock poisoned".into()))?;
        republish_scheduled_groups_into_zero_row_summary(&mut guard, &self.node_id, &groups);
        Ok(())
    }

    fn retain_cached_status_for_surviving_roots(
        &self,
        roots: &[crate::source::config::RootSpec],
    ) -> Result<()> {
        let surviving_groups = roots
            .iter()
            .map(|root| root.id.clone())
            .collect::<std::collections::BTreeSet<_>>();

        {
            let mut guard = self.scheduled_groups_cache.lock().map_err(|_| {
                CnxError::Internal("sink worker scheduled groups cache lock poisoned".into())
            })?;
            *guard = (!surviving_groups.is_empty()).then_some(surviving_groups.clone());
        }

        let filter_group_entries =
            |entries_by_node: &mut std::collections::BTreeMap<String, Vec<String>>| {
                entries_by_node.retain(|_, entries| {
                    entries.retain(|entry| {
                        surviving_groups.contains(sink_status_origin_entry_group_id(entry))
                    });
                    !entries.is_empty()
                });
            };

        let mut guard = self
            .status_cache
            .lock()
            .map_err(|_| CnxError::Internal("sink worker status cache lock poisoned".into()))?;
        guard
            .groups
            .retain(|group| surviving_groups.contains(&group.group_id));
        guard.scheduled_groups_by_node.retain(|_, groups| {
            groups.retain(|group_id| surviving_groups.contains(group_id));
            !groups.is_empty()
        });
        filter_group_entries(&mut guard.last_received_origins_by_node);
        filter_group_entries(&mut guard.received_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_received_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_received_path_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_ready_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_ready_path_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_deferred_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_dropped_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_applied_origin_counts_by_node);
        filter_group_entries(&mut guard.stream_applied_path_origin_counts_by_node);
        guard.live_nodes = guard.groups.iter().map(|group| group.live_nodes).sum();
        guard.tombstoned_count = guard
            .groups
            .iter()
            .map(|group| group.tombstoned_count)
            .sum();
        guard.attested_count = guard.groups.iter().map(|group| group.attested_count).sum();
        guard.suspect_count = guard.groups.iter().map(|group| group.suspect_count).sum();
        guard.blind_spot_count = guard
            .groups
            .iter()
            .map(|group| group.blind_spot_count)
            .sum();
        guard.shadow_time_us = guard
            .groups
            .iter()
            .map(|group| group.shadow_time_us)
            .max()
            .unwrap_or(0);
        guard.estimated_heap_bytes = guard
            .groups
            .iter()
            .map(|group| group.estimated_heap_bytes)
            .sum();
        Ok(())
    }

    async fn replace_shared_worker_client(
        &self,
    ) -> Result<Arc<TypedRuntimeWorkerClient<SinkWorkerRpc, SourceConfig>>> {
        #[cfg(test)]
        notify_sink_worker_retry_reset();
        let replacement = SharedSinkWorkerClient {
            instance_id: next_shared_sink_worker_instance_id(),
            client: Arc::new(self.worker_factory.connect(
                self.node_id.clone(),
                self.current_config()?,
                self.worker_binding.clone(),
            )?),
        };
        let stale_client = {
            let mut guard = self.worker.lock().await;
            let stale = guard.client.clone();
            *guard = replacement;
            stale
        };
        self.control_state_replay_required
            .store(1, Ordering::Release);
        Ok(stale_client)
    }

    async fn restart_shared_worker_client_for_retry_until(
        &self,
        deadline: std::time::Instant,
        exhaustion_kind: SinkRetryBudgetExhaustionKind,
    ) -> std::result::Result<(), SinkFailure> {
        let stale_client = self
            .replace_shared_worker_client()
            .await
            .map_err(SinkFailure::from)?;
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(SinkFailure::retry_budget_exhausted(exhaustion_kind));
        }
        let stale_shutdown_timeout = remaining.min(Duration::from_millis(250));
        if !stale_shutdown_timeout.is_zero() {
            let _ = stale_client.shutdown(stale_shutdown_timeout).await;
        }
        match tokio::time::timeout(remaining, self.ensure_started_with_failure()).await {
            Ok(result) => result,
            Err(_) => Err(SinkFailure::retry_budget_exhausted(exhaustion_kind)),
        }
    }

    fn scheduled_group_ids_retry_action(
        deadline: std::time::Instant,
    ) -> std::result::Result<std::time::Instant, SinkFailure> {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            Err(SinkFailure::retry_budget_exhausted(
                SinkRetryBudgetExhaustionKind::ControlFrameRetry,
            ))
        } else {
            Ok(deadline)
        }
    }

    async fn retain_control_signals(&self, signals: &[SinkControlSignal]) -> Result<()> {
        let scheduled_groups = {
            let mut retained = self.retained_control_state.lock().await;
            for signal in signals {
                match signal {
                    SinkControlSignal::RuntimeHostGrantChange { .. } => {
                        retained.latest_host_grant_change = Some(signal.clone());
                    }
                    SinkControlSignal::Activate {
                        unit, route_key, ..
                    } => {
                        retained.active_by_route.insert(
                            (unit.unit_id().to_string(), route_key.clone()),
                            signal.clone(),
                        );
                    }
                    SinkControlSignal::Deactivate {
                        unit, route_key, ..
                    } => {
                        retained
                            .active_by_route
                            .remove(&(unit.unit_id().to_string(), route_key.clone()));
                    }
                    SinkControlSignal::Tick { .. } | SinkControlSignal::Passthrough(_) => {}
                }
            }
            retained_scheduled_group_ids(&retained)
        };
        self.replace_cached_scheduled_group_ids(scheduled_groups)
    }

    async fn replay_retained_control_state_if_needed_with_timeouts(
        &self,
        total_timeout: Duration,
        rpc_timeout: Duration,
    ) -> std::result::Result<(), SinkFailure> {
        if self
            .control_state_replay_required
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let envelopes = {
            let retained = self.retained_control_state.lock().await;
            let mut envelopes = Vec::new();
            if let Some(host_grant_change) = retained.latest_host_grant_change.as_ref() {
                envelopes.push(host_grant_change.envelope());
            }
            envelopes.extend(
                retained
                    .active_by_route
                    .values()
                    .map(SinkControlSignal::envelope),
            );
            envelopes
        };
        if envelopes.is_empty() {
            return Ok(());
        }
        if let Err(failure) = self
            .on_control_frame_with_timeouts_with_failure(envelopes, total_timeout, rpc_timeout)
            .await
        {
            self.control_state_replay_required
                .store(1, Ordering::Release);
            return Err(failure);
        }
        Ok(())
    }

    async fn replay_retained_control_state_if_needed(
        &self,
    ) -> std::result::Result<(), SinkFailure> {
        self.replay_retained_control_state_if_needed_with_timeouts(
            SINK_WORKER_CONTROL_TOTAL_TIMEOUT,
            SINK_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
    }

    #[cfg(test)]
    async fn client(&self) -> Result<TypedWorkerClient<SinkWorkerRpc>> {
        self.worker_client().await.client().await
    }

    #[cfg(test)]
    async fn call_worker(
        client: &TypedWorkerClient<SinkWorkerRpc>,
        request: SinkWorkerRequest,
        timeout: Duration,
    ) -> Result<SinkWorkerResponse> {
        client.call_with_timeout(request, timeout).await
    }

    async fn call_worker_with_failure(
        client: &TypedWorkerClient<SinkWorkerRpc>,
        request: SinkWorkerRequest,
        timeout: Duration,
    ) -> std::result::Result<SinkWorkerResponse, SinkFailure> {
        client
            .call_with_timeout(request, timeout)
            .await
            .map_err(SinkFailure::from)
    }

    async fn with_started_retry_with_failure<T, F, Fut>(
        &self,
        op: F,
    ) -> std::result::Result<T, SinkFailure>
    where
        F: Fn(TypedWorkerClient<SinkWorkerRpc>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, SinkFailure>>,
    {
        let closure_entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker = self.worker_client().await;
        let existing_client = worker
            .existing_client()
            .await
            .map_err(SinkFailure::from)?
            .is_some();
        if debug_sink_worker_pre_dispatch_enabled() {
            eprintln!(
                "fs_meta_sink_worker_client: pre_dispatch with_started_retry begin node={} existing_client={}",
                self.node_id.0, existing_client
            );
        }
        let result = worker
            .with_started_retry_mapped(
                |client| {
                    closure_entered.store(true, std::sync::atomic::Ordering::Relaxed);
                    op(client)
                },
                SinkFailure::into_error,
            )
            .await;
        let closure_entered = closure_entered.load(std::sync::atomic::Ordering::Relaxed);
        if debug_sink_worker_pre_dispatch_enabled() {
            match &result {
                Ok(_) => eprintln!(
                    "fs_meta_sink_worker_client: pre_dispatch with_started_retry done node={} ok=true closure_entered={}",
                    self.node_id.0, closure_entered
                ),
                Err(failure) => eprintln!(
                    "fs_meta_sink_worker_client: pre_dispatch with_started_retry done node={} ok=false closure_entered={} err={}",
                    self.node_id.0,
                    closure_entered,
                    failure.as_error()
                ),
            }
        }
        result
    }

    #[cfg(test)]
    async fn client_with_failure(
        &self,
    ) -> std::result::Result<TypedWorkerClient<SinkWorkerRpc>, SinkFailure> {
        self.client().await.map_err(SinkFailure::from)
    }

    async fn existing_client_with_failure(
        &self,
    ) -> std::result::Result<Option<TypedWorkerClient<SinkWorkerRpc>>, SinkFailure> {
        self.worker_client()
            .await
            .existing_client()
            .await
            .map_err(SinkFailure::from)
    }

    #[cfg(test)]
    pub async fn ensure_started(&self) -> Result<()> {
        self.ensure_started_with_failure()
            .await
            .map_err(SinkFailure::into_error)
    }

    pub(crate) async fn ensure_started_with_failure(&self) -> std::result::Result<(), SinkFailure> {
        eprintln!(
            "fs_meta_sink_worker_client: ensure_started begin node={}",
            self.node_id.0
        );
        self.worker_client()
            .await
            .ensure_started()
            .await
            .map(|_| {
                eprintln!(
                    "fs_meta_sink_worker_client: ensure_started ok node={}",
                    self.node_id.0
                );
            })
            .map_err(SinkFailure::from)
    }

    async fn update_logical_roots_with_failure(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
        host_object_grants: Vec<GrantedMountRoot>,
    ) -> std::result::Result<(), SinkFailure> {
        let _inflight = self.begin_control_op();
        eprintln!(
            "fs_meta_sink_worker_client: update_logical_roots begin node={} roots={} grants={}",
            self.node_id.0,
            roots.len(),
            host_object_grants.len()
        );
        let response = self
            .with_started_retry_with_failure(|client| {
                let roots = roots.clone();
                let host_object_grants = host_object_grants.clone();
                async move {
                    #[cfg(test)]
                    maybe_pause_before_update_logical_roots_rpc().await;
                    Self::call_worker_with_failure(
                        &client,
                        SinkWorkerRequest::UpdateLogicalRoots {
                            roots: roots.clone(),
                            host_object_grants: host_object_grants.clone(),
                        },
                        SINK_WORKER_UPDATE_ROOTS_RPC_TIMEOUT,
                    )
                    .await
                }
            })
            .await?;
        match response {
            SinkWorkerResponse::Ack => {
                eprintln!(
                    "fs_meta_sink_worker_client: update_logical_roots ok node={} roots={} grants={}",
                    self.node_id.0,
                    roots.len(),
                    host_object_grants.len()
                );
            }
            other => {
                return Err(unexpected_sink_worker_response_failure(
                    "for update roots",
                    other,
                ));
            }
        }
        self.update_cached_logical_roots(roots.clone())
            .map_err(SinkFailure::from)?;
        self.update_cached_runtime_config(&roots, &host_object_grants)
            .map_err(SinkFailure::from)?;
        self.retain_cached_status_for_surviving_roots(&roots)
            .map_err(SinkFailure::from)
    }

    fn cached_logical_roots_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<crate::source::config::RootSpec>, SinkFailure> {
        self.logical_roots_cache
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| {
                SinkFailure::from_cause(CnxError::Internal(
                    "sink worker logical roots cache lock poisoned".into(),
                ))
            })
    }

    #[cfg(test)]
    async fn logical_roots_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<crate::source::config::RootSpec>, SinkFailure> {
        let response = Self::call_worker_with_failure(
            &self.client_with_failure().await?,
            SinkWorkerRequest::LogicalRootsSnapshot,
            Duration::from_secs(5),
        )
        .await?;
        let roots = match response {
            SinkWorkerResponse::LogicalRoots(roots) => roots,
            other => return unexpected_sink_worker_response_result("for logical roots", other),
        };
        self.update_cached_logical_roots(roots.clone())
            .map_err(SinkFailure::from)?;
        Ok(roots)
    }

    fn prepare_status_snapshot_for_evaluation(
        &self,
        mut snapshot: SinkStatusSnapshot,
    ) -> Result<SinkStatusSnapshot> {
        let grants = self
            .config
            .lock()
            .map_err(|_| CnxError::Internal("sink worker config lock poisoned".into()))?
            .host_object_grants
            .clone();
        normalize_sink_status_snapshot_node_keys(&mut snapshot, &self.node_id, &grants);
        let scheduled_groups = snapshot.readiness_summary().scheduled_groups;
        if !scheduled_groups.is_empty() {
            self.update_cached_scheduled_group_ids(&scheduled_groups)?;
        }
        if let Some(groups) = self.cached_scheduled_group_ids()? {
            republish_scheduled_groups_into_zero_row_summary(&mut snapshot, &self.node_id, &groups);
        }
        if debug_control_scope_capture_enabled() {
            eprintln!(
                "fs_meta_sink_worker_client: status_snapshot reply node={} {}",
                self.node_id.0,
                summarize_sink_status_snapshot(&snapshot)
            );
        }
        Ok(snapshot)
    }

    fn prepare_status_snapshot_for_evaluation_with_failure(
        &self,
        snapshot: SinkStatusSnapshot,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        self.prepare_status_snapshot_for_evaluation(snapshot)
            .map_err(SinkFailure::from)
    }

    fn nonblocking_cached_status_snapshot_reason(
        access_path: SinkStatusAccessPath,
        outcome: &SinkStatusSnapshotOutcome,
        err: Option<&CnxError>,
    ) -> &'static str {
        match access_path {
            SinkStatusAccessPath::ControlInflightNoClient => match outcome.kind {
                SinkStatusOutcomeKind::ReturnCached => {
                    if err.is_some() {
                        "control_inflight_cached_after_err"
                    } else {
                        "control_inflight"
                    }
                }
                SinkStatusOutcomeKind::FailClosed => match outcome.concern {
                    Some(SinkStatusConcern::WaitingForMaterializedRoot) => {
                        "control_inflight_scheduled_waiting_for_materialized_root_cached_snapshot"
                    }
                    Some(SinkStatusConcern::CoverageGap) => {
                        "control_inflight_missing_scheduled_group_rows_cached_snapshot"
                    }
                    Some(SinkStatusConcern::MixedReadiness) => {
                        "control_inflight_scheduled_mixed_ready_and_unready_cached_snapshot"
                    }
                    Some(SinkStatusConcern::ReplayPending) => {
                        "control_inflight_pending_materialization_without_stream_receipts_cached_snapshot"
                    }
                    Some(SinkStatusConcern::StaleEmpty) | None => {
                        "control_inflight_fail_closed_cached_snapshot"
                    }
                },
                SinkStatusOutcomeKind::PropagateError => {
                    "control_inflight_stale_empty_cached_snapshot"
                }
                SinkStatusOutcomeKind::ReturnLive => {
                    unreachable!("control-inflight cached evaluation never returns live")
                }
            },
            SinkStatusAccessPath::WorkerUnavailable => match outcome.concern {
                Some(SinkStatusConcern::CoverageGap) => {
                    "worker_unavailable_missing_scheduled_group_rows_after_stream_evidence"
                }
                Some(SinkStatusConcern::ReplayPending) => {
                    "worker_unavailable_pending_materialization_without_stream_receipts"
                }
                Some(SinkStatusConcern::WaitingForMaterializedRoot) => {
                    "worker_unavailable_scheduled_waiting_for_materialized_root"
                }
                Some(SinkStatusConcern::StaleEmpty) => {
                    "worker_unavailable_stale_empty_cached_snapshot"
                }
                Some(SinkStatusConcern::MixedReadiness) => {
                    "worker_unavailable_scheduled_mixed_ready_and_unready"
                }
                None => "worker_unavailable",
            },
            SinkStatusAccessPath::NotStarted => match outcome.kind {
                SinkStatusOutcomeKind::ReturnCached => "not_started",
                SinkStatusOutcomeKind::FailClosed => match outcome.concern {
                    Some(SinkStatusConcern::WaitingForMaterializedRoot) => {
                        "not_started_scheduled_waiting_for_materialized_root_cached_snapshot"
                    }
                    Some(SinkStatusConcern::CoverageGap) => {
                        "not_started_missing_scheduled_group_rows_cached_snapshot"
                    }
                    Some(SinkStatusConcern::MixedReadiness) => {
                        "not_started_scheduled_mixed_ready_and_unready_cached_snapshot"
                    }
                    Some(SinkStatusConcern::ReplayPending) => {
                        "not_started_pending_materialization_without_stream_receipts_cached_snapshot"
                    }
                    Some(SinkStatusConcern::StaleEmpty) | None => {
                        "not_started_fail_closed_cached_snapshot"
                    }
                },
                SinkStatusOutcomeKind::PropagateError => {
                    unreachable!("not-started cached evaluation never propagates error")
                }
                SinkStatusOutcomeKind::ReturnLive => {
                    unreachable!("not-started cached evaluation never returns live")
                }
            },
            SinkStatusAccessPath::Blocking
            | SinkStatusAccessPath::ControlInflight
            | SinkStatusAccessPath::Steady
            | SinkStatusAccessPath::SteadyAfterRetryReset => {
                unreachable!("live access path passed to cached reason formatter")
            }
        }
    }

    async fn nonblocking_status_entry_action(
        &self,
    ) -> std::result::Result<SinkStatusNonblockingEntryAction, SinkFailure> {
        if self.control_op_inflight() {
            let snapshot = self.cached_status_snapshot_with_failure()?;
            if self.existing_client_with_failure().await?.is_some() {
                return Ok(SinkStatusNonblockingEntryAction::ProbeLive(
                    SinkStatusNonblockingProbePlan {
                        mode: SinkStatusNonblockingMode::ControlInflight,
                        cached_snapshot: snapshot,
                        inflight_control_summaries: self.inflight_control_frame_summaries(),
                    },
                ));
            }
            return Ok(SinkStatusNonblockingEntryAction::ReturnCached {
                snapshot,
                access_path: SinkStatusAccessPath::ControlInflightNoClient,
            });
        }

        let mut snapshot = self.cached_status_snapshot_with_failure()?;
        if self.existing_client_with_failure().await?.is_some() {
            if snapshot_looks_stale_empty(&snapshot) {
                self.republish_cached_scheduled_groups_into_empty_status_summary()
                    .map_err(SinkFailure::from)?;
                snapshot = self.cached_status_snapshot_with_failure()?;
            }
            return Ok(SinkStatusNonblockingEntryAction::ProbeLive(
                SinkStatusNonblockingProbePlan {
                    mode: SinkStatusNonblockingMode::Steady,
                    cached_snapshot: snapshot,
                    inflight_control_summaries: Vec::new(),
                },
            ));
        }

        Ok(SinkStatusNonblockingEntryAction::ReturnCached {
            snapshot,
            access_path: SinkStatusAccessPath::NotStarted,
        })
    }

    fn nonblocking_live_status_reason(
        mode: SinkStatusNonblockingMode,
        concern: Option<SinkStatusConcern>,
        recovered_after_retry_reset: bool,
    ) -> &'static str {
        match mode {
            SinkStatusNonblockingMode::ControlInflight => match concern {
                Some(SinkStatusConcern::CoverageGap) => {
                    "control_inflight_missing_scheduled_group_rows_after_stream_evidence"
                }
                Some(SinkStatusConcern::WaitingForMaterializedRoot) => {
                    "control_inflight_scheduled_waiting_for_materialized_root"
                }
                Some(SinkStatusConcern::MixedReadiness) => {
                    "control_inflight_scheduled_mixed_ready_and_unready"
                }
                Some(SinkStatusConcern::ReplayPending) => {
                    "control_inflight_pending_materialization_without_stream_receipts"
                }
                Some(SinkStatusConcern::StaleEmpty) | None => {
                    if recovered_after_retry_reset {
                        "control_inflight_recovered_after_retry_reset"
                    } else {
                        "control_inflight"
                    }
                }
            },
            SinkStatusNonblockingMode::Steady => match concern {
                Some(SinkStatusConcern::StaleEmpty) => "stale_empty_after_retry_reset",
                Some(SinkStatusConcern::CoverageGap) => {
                    "missing_scheduled_group_rows_after_stream_evidence"
                }
                Some(SinkStatusConcern::ReplayPending) => {
                    "replay_required_not_ready_without_stream_evidence"
                }
                Some(SinkStatusConcern::WaitingForMaterializedRoot) => {
                    "scheduled_waiting_for_materialized_root"
                }
                Some(SinkStatusConcern::MixedReadiness) => "scheduled_mixed_ready_and_unready",
                None => "steady",
            },
        }
    }

    async fn finalize_nonblocking_live_probe(
        &self,
        plan: SinkStatusNonblockingProbePlan,
        probe_outcome: SinkStatusSnapshotProbeOutcome,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        let live_snapshot =
            self.prepare_status_snapshot_for_evaluation_with_failure(probe_outcome.snapshot)?;
        if matches!(plan.mode, SinkStatusNonblockingMode::ControlInflight)
            && !snapshot_reflects_inflight_control_frame_summaries(
                &live_snapshot,
                &self.node_id,
                &plan.inflight_control_summaries,
            )
        {
            if debug_control_scope_capture_enabled() {
                eprintln!(
                    "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason=control_inflight_snapshot_missing_inflight_control_summary expected={:?} {}",
                    self.node_id.0,
                    plan.inflight_control_summaries,
                    summarize_sink_status_snapshot(&live_snapshot)
                );
            }
            return Err(SinkFailure::timeout_like());
        }

        let cached_snapshot = self.cached_status_snapshot_with_failure()?;
        let cached_concern = sink_status_snapshot_concern(&cached_snapshot);
        let outcome = evaluate_live_sink_status_snapshot(
            &live_snapshot,
            &cached_snapshot,
            cached_concern,
            plan.live_access_path(probe_outcome.recovered_after_retry_reset),
        );
        apply_live_sink_status_snapshot_outcome_side_effects(self, &live_snapshot, &outcome)
            .map_err(SinkFailure::from)?;
        match outcome.kind {
            SinkStatusOutcomeKind::ReturnLive => {
                if debug_control_scope_capture_enabled() {
                    match plan.mode {
                        SinkStatusNonblockingMode::ControlInflight => eprintln!(
                            "fs_meta_sink_worker_client: status_snapshot short_probe node={} reason={} {}",
                            self.node_id.0,
                            Self::nonblocking_live_status_reason(
                                plan.mode,
                                outcome.concern,
                                probe_outcome.recovered_after_retry_reset,
                            ),
                            summarize_sink_status_snapshot(&live_snapshot)
                        ),
                        SinkStatusNonblockingMode::Steady
                            if probe_outcome.recovered_after_retry_reset
                                && matches!(
                                    outcome.concern,
                                    Some(SinkStatusConcern::WaitingForMaterializedRoot)
                                ) =>
                        {
                            eprintln!(
                                "fs_meta_sink_worker_client: status_snapshot return_live node={} reason=recovered_after_retry_reset_waiting_for_materialized_root {}",
                                self.node_id.0,
                                summarize_sink_status_snapshot(&live_snapshot)
                            );
                        }
                        SinkStatusNonblockingMode::Steady => {}
                    }
                }
                Ok(live_snapshot)
            }
            SinkStatusOutcomeKind::ReturnCached => {
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason={} live={} cached={}",
                        self.node_id.0,
                        Self::nonblocking_live_status_reason(
                            plan.mode,
                            outcome.concern,
                            probe_outcome.recovered_after_retry_reset,
                        ),
                        summarize_sink_status_snapshot(&live_snapshot),
                        summarize_sink_status_snapshot(&cached_snapshot)
                    );
                }
                Ok(cached_snapshot)
            }
            SinkStatusOutcomeKind::FailClosed => {
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason={} {}",
                        self.node_id.0,
                        Self::nonblocking_live_status_reason(
                            plan.mode,
                            outcome.concern,
                            probe_outcome.recovered_after_retry_reset,
                        ),
                        summarize_sink_status_snapshot(&live_snapshot)
                    );
                }
                Err(SinkFailure::timeout_like())
            }
            SinkStatusOutcomeKind::PropagateError => {
                unreachable!("live sink status evaluation never propagates error")
            }
        }
    }

    fn finalize_nonblocking_cached_status_snapshot(
        &self,
        snapshot: SinkStatusSnapshot,
        access_path: SinkStatusAccessPath,
        err: Option<SinkFailure>,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        let outcome = evaluate_cached_sink_status_snapshot(&snapshot, access_path);
        if outcome.should_republish_zero_row_summary {
            self.republish_cached_scheduled_groups_into_empty_status_summary()
                .map_err(SinkFailure::from)?;
        }
        let err_cause = err.as_ref().map(SinkFailure::as_error);
        let reason =
            Self::nonblocking_cached_status_snapshot_reason(access_path, &outcome, err_cause);
        match outcome.kind {
            SinkStatusOutcomeKind::ReturnCached => {
                if debug_control_scope_capture_enabled() {
                    match err.as_ref() {
                        Some(err) => eprintln!(
                            "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason={} err={} {}",
                            self.node_id.0,
                            reason,
                            err.as_error(),
                            summarize_sink_status_snapshot(&snapshot)
                        ),
                        None => eprintln!(
                            "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason={} {}",
                            self.node_id.0,
                            reason,
                            summarize_sink_status_snapshot(&snapshot)
                        ),
                    }
                }
                Ok(snapshot)
            }
            SinkStatusOutcomeKind::FailClosed => {
                if debug_control_scope_capture_enabled() {
                    match err.as_ref() {
                        Some(err) => eprintln!(
                            "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason={} err={} {}",
                            self.node_id.0,
                            reason,
                            err.as_error(),
                            summarize_sink_status_snapshot(&snapshot)
                        ),
                        None => eprintln!(
                            "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason={} {}",
                            self.node_id.0,
                            reason,
                            summarize_sink_status_snapshot(&snapshot)
                        ),
                    }
                }
                Err(SinkFailure::timeout_like())
            }
            SinkStatusOutcomeKind::PropagateError => Err(err.expect(
                "cached sink status propagation requires the original worker-unavailable error",
            )),
            SinkStatusOutcomeKind::ReturnLive => {
                unreachable!("cached sink status evaluation never returns live")
            }
        }
    }

    fn finalize_blocking_status_snapshot(
        &self,
        snapshot: SinkStatusSnapshot,
        replay_required: bool,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        let snapshot = self.prepare_status_snapshot_for_evaluation_with_failure(snapshot)?;
        let cached_snapshot = SinkStatusSnapshot::default();
        let outcome = evaluate_live_sink_status_snapshot(
            &snapshot,
            &cached_snapshot,
            None,
            SinkStatusAccessPath::Blocking,
        );
        apply_live_sink_status_snapshot_outcome_side_effects(self, &snapshot, &outcome)
            .map_err(SinkFailure::from)?;
        match outcome.kind {
            SinkStatusOutcomeKind::ReturnLive => Ok(snapshot),
            SinkStatusOutcomeKind::FailClosed => {
                if debug_control_scope_capture_enabled() {
                    let reason = match outcome.concern {
                        Some(SinkStatusConcern::CoverageGap) => {
                            "blocking_missing_scheduled_group_rows_after_stream_evidence"
                        }
                        Some(SinkStatusConcern::ReplayPending) => {
                            "blocking_scheduled_pending_materialization_without_stream_receipts"
                        }
                        Some(SinkStatusConcern::WaitingForMaterializedRoot) => {
                            "blocking_scheduled_waiting_for_materialized_root"
                        }
                        Some(SinkStatusConcern::MixedReadiness) => {
                            "blocking_scheduled_mixed_ready_and_unready"
                        }
                        Some(SinkStatusConcern::StaleEmpty) | None => "blocking_fail_closed",
                    };
                    eprintln!(
                        "fs_meta_sink_worker_client: status_snapshot fail_closed node={} reason={} replay_required={} {}",
                        self.node_id.0,
                        reason,
                        replay_required,
                        summarize_sink_status_snapshot(&snapshot)
                    );
                }
                Err(SinkFailure::timeout_like())
            }
            SinkStatusOutcomeKind::ReturnCached => {
                unreachable!("blocking live sink status evaluation never returns cached")
            }
            SinkStatusOutcomeKind::PropagateError => {
                unreachable!("blocking live sink status evaluation never propagates error")
            }
        }
    }

    #[cfg(test)]
    fn finalize_test_hooked_blocking_status_snapshot(
        &self,
        snapshot: SinkStatusSnapshot,
    ) -> Result<SinkStatusSnapshot> {
        let snapshot = self.prepare_status_snapshot_for_evaluation(snapshot)?;
        self.update_cached_status_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }

    pub(crate) async fn status_snapshot_with_failure(
        &self,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        let replay_required = self.control_state_replay_required.load(Ordering::Acquire) > 0;
        #[cfg(test)]
        if let Some(snapshot) = take_sink_worker_status_snapshot_hook(self.shared_worker().await.0)
        {
            return self
                .finalize_test_hooked_blocking_status_snapshot(snapshot)
                .map_err(SinkFailure::from);
        }
        self.replay_retained_control_state_if_needed().await?;
        let snapshot = self
            .status_snapshot_with_timeout_with_failure(Duration::from_secs(5))
            .await?;
        self.finalize_blocking_status_snapshot(snapshot, replay_required)
            .map_err(SinkFailure::from)
    }

    async fn status_snapshot_nonblocking_with_access_path(
        &self,
    ) -> std::result::Result<(SinkStatusSnapshot, bool), SinkFailure> {
        #[cfg(test)]
        {
            let current_worker_instance_id = self.shared_worker().await.0;
            if take_sink_worker_status_nonblocking_cache_fallback_hook(current_worker_instance_id) {
                let snapshot = self.cached_status_snapshot_with_failure()?;
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason=test_hook {}",
                        self.node_id.0,
                        summarize_sink_status_snapshot(&snapshot)
                    );
                }
                return Ok((snapshot, true));
            }
        }
        match self.nonblocking_status_entry_action().await? {
            SinkStatusNonblockingEntryAction::ReturnCached {
                snapshot,
                access_path,
            } => self
                .finalize_nonblocking_cached_status_snapshot(snapshot, access_path, None)
                .map(|snapshot| (snapshot, true)),
            SinkStatusNonblockingEntryAction::ProbeLive(plan) => {
                match self
                    .status_snapshot_with_timeout_outcome(
                        status_snapshot_nonblocking_live_probe_budget(),
                    )
                    .await
                {
                    Ok(probe_outcome) => self
                        .finalize_nonblocking_live_probe(plan, probe_outcome)
                        .await
                        .map(|snapshot| (snapshot, false)),
                    Err(err) => {
                        let access_path = plan.err_access_path();
                        self.finalize_nonblocking_cached_status_snapshot(
                            plan.cached_snapshot,
                            access_path,
                            Some(err),
                        )
                        .map(|snapshot| (snapshot, true))
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub async fn status_snapshot_nonblocking(&self) -> Result<SinkStatusSnapshot> {
        self.status_snapshot_nonblocking_with_access_path()
            .await
            .map(|(snapshot, _)| snapshot)
            .map_err(SinkFailure::into_error)
    }

    pub(crate) async fn status_snapshot_nonblocking_with_failure(
        &self,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        self.status_snapshot_nonblocking_with_access_path()
            .await
            .map(|(snapshot, _)| snapshot)
    }

    pub(crate) async fn status_snapshot_nonblocking_for_status_route(
        &self,
    ) -> (SinkStatusSnapshot, bool) {
        match self.status_snapshot_nonblocking_with_access_path().await {
            Ok(outcome) => outcome,
            Err(err) => {
                let snapshot = self
                    .cached_status_snapshot_with_failure()
                    .unwrap_or_else(|_| SinkStatusSnapshot::default());
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_sink_worker_client: status_snapshot cache_fallback node={} reason=worker_unavailable err={} {}",
                        self.node_id.0,
                        err.as_error(),
                        summarize_sink_status_snapshot(&snapshot)
                    );
                }
                (snapshot, true)
            }
        }
    }

    #[cfg(test)]
    async fn status_snapshot_probe_once_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<SinkStatusSnapshot> {
        self.status_snapshot_probe_once_with_timeout_with_failure(timeout)
            .await
            .map_err(SinkFailure::into_error)
    }

    #[cfg(test)]
    async fn status_snapshot_probe_once_with_timeout_with_failure(
        &self,
        timeout: Duration,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        self.replay_retained_control_state_if_needed().await?;
        #[cfg(test)]
        let current_worker_instance_id = self.shared_worker().await.0;
        let client = self.client_with_failure().await?;
        #[cfg(test)]
        if let Some(reply) = take_sink_worker_status_response_queue_hook() {
            return match reply? {
                SinkWorkerResponse::StatusSnapshot(snapshot) => Ok(snapshot),
                other => Err(unexpected_sink_worker_response_failure(
                    "for status snapshot",
                    other,
                )),
            };
        }
        #[cfg(test)]
        if let Some(snapshot) = take_sink_worker_status_snapshot_hook(current_worker_instance_id) {
            return Ok(snapshot);
        }
        #[cfg(test)]
        if let Some(err) = take_sink_worker_status_error_hook(current_worker_instance_id) {
            return Err(err.into());
        }
        match Self::call_worker_with_failure(&client, SinkWorkerRequest::StatusSnapshot, timeout)
            .await?
        {
            SinkWorkerResponse::StatusSnapshot(snapshot) => Ok(snapshot),
            other => Err(unexpected_sink_worker_response_failure(
                "for status snapshot",
                other,
            )),
        }
    }

    async fn status_snapshot_with_timeout_outcome(
        &self,
        timeout: Duration,
    ) -> std::result::Result<SinkStatusSnapshotProbeOutcome, SinkFailure> {
        let mut machine = SinkStatusProbeMachine::new(std::time::Instant::now() + timeout);
        let response = loop {
            let replay_required_for_attempt =
                self.control_state_replay_required.load(Ordering::Acquire) > 0;
            let attempt_plan = machine.attempt_plan(replay_required_for_attempt)?;
            if attempt_plan.skip_replay {
                if debug_control_scope_capture_enabled() {
                    eprintln!(
                        "fs_meta_sink_worker_client: status_snapshot replay_deferred node={} reason=retry_reset_first_probe",
                        self.node_id.0
                    );
                }
            }
            if !attempt_plan.skip_replay {
                if let Err(err) = self
                    .replay_retained_control_state_if_needed_with_timeouts(
                        attempt_plan.replay_timeout,
                        attempt_plan
                            .replay_timeout
                            .min(SINK_WORKER_CONTROL_RPC_TIMEOUT),
                    )
                    .await
                {
                    match machine
                        .classify_replay_failure_disposition(err, replay_required_for_attempt)
                    {
                        SinkStatusProbeReplayDisposition::BestEffort(failure) => {
                            if debug_control_scope_capture_enabled() {
                                eprintln!(
                                    "fs_meta_sink_worker_client: status_snapshot replay_best_effort node={} err={} remaining_ms={}",
                                    self.node_id.0,
                                    failure.as_error(),
                                    machine
                                        .deadline
                                        .saturating_duration_since(std::time::Instant::now())
                                        .as_millis()
                                );
                            }
                        }
                        SinkStatusProbeReplayDisposition::Fail(failure) => {
                            return Err(failure);
                        }
                    }
                }
            }
            #[cfg(test)]
            record_sink_worker_status_timeout_probe(attempt_plan.attempt_timeout);
            #[cfg(test)]
            let current_worker_instance_id = self.shared_worker().await.0;
            let rpc_result = self
                .with_started_retry_with_failure(|client| async move {
                    #[cfg(test)]
                    if let Some(reply) = take_sink_worker_status_response_queue_hook() {
                        return reply.map_err(SinkFailure::from);
                    }
                    #[cfg(test)]
                    if let Some(snapshot) =
                        take_sink_worker_status_snapshot_hook(current_worker_instance_id)
                    {
                        return Ok(SinkWorkerResponse::StatusSnapshot(snapshot));
                    }
                    #[cfg(test)]
                    if let Some(err) =
                        take_sink_worker_status_error_hook(current_worker_instance_id)
                    {
                        return Err(SinkFailure::from(err));
                    }
                    Self::call_worker_with_failure(
                        &client,
                        SinkWorkerRequest::StatusSnapshot,
                        attempt_plan.attempt_timeout,
                    )
                    .await
                })
                .await;
            match rpc_result {
                Ok(SinkWorkerResponse::Ack) if replay_required_for_attempt => {
                    machine = machine.phase_after_retry_reset();
                    self.control_state_replay_required
                        .store(1, Ordering::Release);
                    self.restart_shared_worker_client_for_retry_until(
                        machine.deadline,
                        SinkRetryBudgetExhaustionKind::StatusProbeAttempt,
                    )
                    .await?;
                }
                Ok(response) => break response,
                Err(failure) => match classify_sink_retry_failure_disposition(
                    machine.deadline,
                    failure,
                    SinkRetryBudgetExhaustionKind::StatusProbeAttempt,
                ) {
                    SinkRetryDisposition::Retry => {
                        machine = machine.phase_after_retry_reset();
                        self.control_state_replay_required
                            .store(1, Ordering::Release);
                        self.restart_shared_worker_client_for_retry_until(
                            machine.deadline,
                            SinkRetryBudgetExhaustionKind::StatusProbeAttempt,
                        )
                        .await?;
                    }
                    SinkRetryDisposition::Fail(failure) => {
                        return Err(failure);
                    }
                },
            }
        };
        match response {
            SinkWorkerResponse::StatusSnapshot(snapshot) => Ok(SinkStatusSnapshotProbeOutcome {
                snapshot,
                recovered_after_retry_reset: machine.recovered_after_retry_reset(),
            }),
            other => Err(SinkFailure::protocol_violation(
                SinkProtocolViolationKind::UnexpectedWorkerResponse {
                    context: "for status snapshot",
                    response: format!("{:?}", other),
                },
            )),
        }
    }

    async fn status_snapshot_with_timeout_with_failure(
        &self,
        timeout: Duration,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        self.status_snapshot_with_timeout_outcome(timeout)
            .await
            .map(|outcome| outcome.snapshot)
    }

    async fn scheduled_group_ids_result_with_failure(
        &self,
    ) -> std::result::Result<Option<std::collections::BTreeSet<String>>, SinkFailure> {
        match self.scheduled_group_ids_with_timeout().await? {
            SinkWorkerResponse::ScheduledGroupIds(groups) => {
                let groups = groups.map(|groups| {
                    groups
                        .into_iter()
                        .collect::<std::collections::BTreeSet<_>>()
                });
                if let Some(groups) = groups.as_ref().filter(|groups| !groups.is_empty()) {
                    self.update_cached_scheduled_group_ids(groups)
                        .map_err(SinkFailure::from)?;
                    return Ok(Some(groups.clone()));
                }
                self.cached_scheduled_group_ids_with_failure()
            }
            other => unexpected_sink_worker_response_result("for scheduled groups", other),
        }
    }

    pub(crate) async fn scheduled_group_ids_with_failure(
        &self,
    ) -> std::result::Result<Option<std::collections::BTreeSet<String>>, SinkFailure> {
        self.replay_retained_control_state_if_needed().await?;
        self.scheduled_group_ids_result_with_failure().await
    }

    #[cfg(test)]
    pub async fn scheduled_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.scheduled_group_ids_with_failure()
            .await
            .map_err(SinkFailure::into_error)
    }

    async fn scheduled_group_ids_with_timeout(
        &self,
    ) -> std::result::Result<SinkWorkerResponse, SinkFailure> {
        let deadline = std::time::Instant::now() + SINK_WORKER_CONTROL_TOTAL_TIMEOUT;
        loop {
            let now = std::time::Instant::now();
            let attempt_timeout =
                Duration::from_secs(5).min(deadline.saturating_duration_since(now));
            if attempt_timeout.is_zero() {
                return Err(SinkFailure::retry_budget_exhausted(
                    SinkRetryBudgetExhaustionKind::ControlFrameAttempt,
                ));
            }
            let rpc_result = self
                .with_started_retry_with_failure(|client| async move {
                    #[cfg(test)]
                    if let Some(err) = take_sink_worker_scheduled_groups_error_hook() {
                        return Err(SinkFailure::from(err));
                    }
                    Self::call_worker_with_failure(
                        &client,
                        SinkWorkerRequest::ScheduledGroupIds,
                        attempt_timeout,
                    )
                    .await
                })
                .await;
            match rpc_result {
                Ok(response) => return Ok(response),
                Err(failure) => match classify_sink_retry_failure_disposition(
                    deadline,
                    failure,
                    SinkRetryBudgetExhaustionKind::ControlFrameRetry,
                ) {
                    SinkRetryDisposition::Retry => {
                        self.restart_shared_worker_client_for_retry_until(
                            Self::scheduled_group_ids_retry_action(deadline)?,
                            SinkRetryBudgetExhaustionKind::ControlFrameRetry,
                        )
                        .await?;
                    }
                    SinkRetryDisposition::Fail(failure) => {
                        return Err(failure);
                    }
                },
            }
        }
    }

    #[cfg(test)]
    async fn visibility_lag_samples_since_with_failure(
        &self,
        since_us: u64,
    ) -> std::result::Result<Vec<VisibilityLagSample>, SinkFailure> {
        match Self::call_worker_with_failure(
            &self.client_with_failure().await?,
            SinkWorkerRequest::VisibilityLagSamplesSince { since_us },
            Duration::from_secs(5),
        )
        .await?
        {
            SinkWorkerResponse::VisibilityLagSamples(samples) => Ok(samples),
            other => unexpected_sink_worker_response_result("for visibility lag samples", other),
        }
    }

    pub(crate) async fn send_with_failure(
        &self,
        events: Vec<Event>,
    ) -> std::result::Result<(), SinkFailure> {
        let response = self
            .with_started_retry_with_failure(|client| {
                let events = events.clone();
                async move {
                    Self::call_worker_with_failure(
                        &client,
                        SinkWorkerRequest::Send {
                            events: events.clone(),
                        },
                        Duration::from_secs(5),
                    )
                    .await
                }
            })
            .await?;
        match response {
            SinkWorkerResponse::Ack => Ok(()),
            other => unexpected_sink_worker_response_result("for send", other),
        }
    }

    #[cfg(test)]
    pub(crate) async fn send(&self, events: Vec<Event>) -> Result<()> {
        self.send_with_failure(events)
            .await
            .map_err(SinkFailure::into_error)
    }

    async fn query_node_with_failure(
        &self,
        path: Vec<u8>,
    ) -> std::result::Result<Option<QueryNode>, SinkFailure> {
        let response = self
            .with_started_retry_with_failure(|client| {
                let path = path.clone();
                async move {
                    let request = InternalQueryRequest::materialized(
                        QueryOp::Tree,
                        QueryScope {
                            path: path.clone(),
                            recursive: false,
                            max_depth: Some(0),
                            selected_group: None,
                        },
                        None,
                    );
                    Self::call_worker_with_failure(
                        &client,
                        SinkWorkerRequest::MaterializedQuery { request },
                        SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
                    )
                    .await
                }
            })
            .await?;
        let events = match response {
            SinkWorkerResponse::Events(events) => events,
            other => {
                return Err(unexpected_sink_worker_response_failure(
                    "for materialized query",
                    other,
                ));
            }
        };
        decode_exact_query_node(events, &path).map_err(SinkFailure::from)
    }

    pub(crate) async fn materialized_query_with_failure(
        &self,
        request: InternalQueryRequest,
    ) -> std::result::Result<Vec<Event>, SinkFailure> {
        self.replay_retained_control_state_if_needed().await?;
        let response = self
            .with_started_retry_with_failure(|client| {
                let request = request.clone();
                async move {
                    Self::call_worker_with_failure(
                        &client,
                        SinkWorkerRequest::MaterializedQuery {
                            request: request.clone(),
                        },
                        SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
                    )
                    .await
                }
            })
            .await?;
        match response {
            SinkWorkerResponse::Events(events) => Ok(events),
            other => Err(unexpected_sink_worker_response_failure(
                "for materialized query",
                other,
            )),
        }
    }

    async fn materialized_query_nonblocking_with_failure(
        &self,
        request: InternalQueryRequest,
    ) -> std::result::Result<Vec<Event>, SinkFailure> {
        if self.control_op_inflight() {
            return Ok(Vec::new());
        }
        match self.existing_client_with_failure().await? {
            Some(client) => {
                self.replay_retained_control_state_if_needed().await?;
                match Self::call_worker_with_failure(
                    &client,
                    SinkWorkerRequest::MaterializedQuery { request },
                    SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
                )
                .await?
                {
                    SinkWorkerResponse::Events(events) => Ok(events),
                    other => Err(unexpected_sink_worker_response_failure(
                        "for materialized query",
                        other,
                    )),
                }
            }
            None => Ok(Vec::new()),
        }
    }

    async fn subtree_stats_with_failure(
        &self,
        path: Vec<u8>,
    ) -> std::result::Result<Vec<Event>, SinkFailure> {
        let response = self
            .with_started_retry_with_failure(|client| {
                let path = path.clone();
                async move {
                    Self::call_worker_with_failure(
                        &client,
                        SinkWorkerRequest::MaterializedQuery {
                            request: InternalQueryRequest::materialized(
                                QueryOp::Stats,
                                QueryScope {
                                    path: path.clone(),
                                    recursive: true,
                                    max_depth: None,
                                    selected_group: None,
                                },
                                None,
                            ),
                        },
                        SINK_WORKER_MATERIALIZED_QUERY_TIMEOUT,
                    )
                    .await
                }
            })
            .await?;
        match response {
            SinkWorkerResponse::Events(events) => Ok(events),
            other => Err(unexpected_sink_worker_response_failure(
                "for materialized query",
                other,
            )),
        }
    }

    async fn recv_with_failure(
        &self,
        opts: RecvOpts,
    ) -> std::result::Result<Vec<Event>, SinkFailure> {
        let timeout_ms = opts.timeout.map(|d| d.as_millis() as u64);
        let limit = opts.limit;
        let response = self
            .with_started_retry_with_failure(|client| async move {
                Self::call_worker_with_failure(
                    &client,
                    SinkWorkerRequest::Recv { timeout_ms, limit },
                    Duration::from_secs(5),
                )
                .await
            })
            .await?;
        match response {
            SinkWorkerResponse::Events(events) => Ok(events),
            other => unexpected_sink_worker_response_result("for recv", other),
        }
    }

    #[cfg(test)]
    pub async fn on_control_frame(&self, envelopes: Vec<ControlEnvelope>) -> Result<()> {
        self.on_control_frame_with_timeouts_with_failure(
            envelopes,
            SINK_WORKER_CONTROL_TOTAL_TIMEOUT,
            SINK_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        .map_err(SinkFailure::into_error)
    }

    async fn on_control_frame_with_timeouts_with_failure(
        &self,
        envelopes: Vec<ControlEnvelope>,
        total_timeout: Duration,
        rpc_timeout: Duration,
    ) -> std::result::Result<(), SinkFailure> {
        let _inflight = self.begin_control_op();
        let decoded_signals = sink_control_signals_from_envelopes(&envelopes).ok();
        let inflight_summary = decoded_signals
            .as_ref()
            .map(|signals| summarize_sink_control_signals(signals));
        let _inflight_summary_guard = self.begin_control_frame_summary_tracking(inflight_summary);
        eprintln!(
            "fs_meta_sink_worker_client: on_control_frame begin node={} envelopes={}",
            self.node_id.0,
            envelopes.len()
        );
        if debug_control_scope_capture_enabled() {
            match decoded_signals.as_ref() {
                Some(signals) => eprintln!(
                    "fs_meta_sink_worker_client: on_control_frame summary node={} signals={:?}",
                    self.node_id.0,
                    summarize_sink_control_signals(signals)
                ),
                None => eprintln!(
                    "fs_meta_sink_worker_client: on_control_frame summary node={} decode_err={}",
                    self.node_id.0, "decode failed"
                ),
            }
        }
        let mut machine =
            SinkControlFrameMachine::new(std::time::Instant::now() + total_timeout, &envelopes);
        loop {
            let attempt_timeout = machine.attempt_timeout(rpc_timeout)?;
            #[cfg(test)]
            let current_worker_instance_id = self.worker_instance_id_for_tests().await;
            let rpc_result = map_sink_failure_timeout_result(
                tokio::time::timeout(
                    attempt_timeout,
                    self.with_started_retry_with_failure(|client| {
                        let envelopes = envelopes.clone();
                        async move {
                            #[cfg(test)]
                            maybe_pause_before_on_control_frame_rpc().await;
                            #[cfg(test)]
                            if let Some(err) = take_sink_worker_control_frame_error_hook(
                                current_worker_instance_id,
                            ) {
                                return Err(SinkFailure::from(err));
                            }
                            Self::call_worker_with_failure(
                                &client,
                                SinkWorkerRequest::OnControlFrame {
                                    envelopes: envelopes.clone(),
                                },
                                attempt_timeout,
                            )
                            .await
                        }
                    }),
                )
                .await,
                SinkRetryBudgetExhaustionKind::ControlFrameAttempt,
            );
            match rpc_result {
                Ok(SinkWorkerResponse::Ack) => {
                    if let Some(signals) = decoded_signals.as_ref() {
                        self.retain_control_signals(signals).await?;
                    }
                    eprintln!(
                        "fs_meta_sink_worker_client: on_control_frame done node={} ok=true",
                        self.node_id.0
                    );
                    return Ok(());
                }
                Ok(other) => {
                    eprintln!(
                        "fs_meta_sink_worker_client: on_control_frame done node={} ok=false err=unexpected_response:{:?}",
                        self.node_id.0, other
                    );
                    return Err(unexpected_sink_worker_response_failure(
                        "for on_control_frame",
                        other,
                    ));
                }
                Err(err) => match machine.followup_after_failure(err) {
                    SinkControlFrameFollowup::RestartAndRetry(next_machine) => {
                        self.restart_shared_worker_client_for_retry_until(
                            machine.deadline,
                            SinkRetryBudgetExhaustionKind::ControlFrameRetry,
                        )
                        .await?;
                        machine = next_machine;
                    }
                    SinkControlFrameFollowup::RestartAndFailFast(failure) => {
                        self.restart_shared_worker_client_for_retry_until(
                            machine.deadline,
                            SinkRetryBudgetExhaustionKind::ControlFrameRetry,
                        )
                        .await?;
                        let lane = if machine.restart_deferred_retire_pending_deactivate {
                            "restart_deferred_retire_pending_events_deactivate"
                        } else {
                            "missing_channel_buffer_route_state_then_bridge_reset"
                        };
                        eprintln!(
                            "fs_meta_sink_worker_client: on_control_frame fail-fast node={} err={} lane={}",
                            self.node_id.0,
                            failure.as_error(),
                            lane
                        );
                        return Err(failure);
                    }
                    SinkControlFrameFollowup::Failed(failure) => {
                        eprintln!(
                            "fs_meta_sink_worker_client: on_control_frame done node={} ok=false err={}",
                            self.node_id.0,
                            failure.as_error()
                        );
                        return Err(failure);
                    }
                },
            }
        }
    }

    #[cfg(test)]
    async fn on_control_frame_with_timeouts_for_tests(
        &self,
        envelopes: Vec<ControlEnvelope>,
        total_timeout: Duration,
        rpc_timeout: Duration,
    ) -> Result<()> {
        self.on_control_frame_with_timeouts_with_failure(envelopes, total_timeout, rpc_timeout)
            .await
            .map_err(SinkFailure::into_error)
    }

    async fn close_with_failure(&self) -> std::result::Result<(), SinkFailure> {
        #[cfg(test)]
        notify_sink_worker_close_started();
        self.wait_for_control_ops_to_drain(SINK_WORKER_CLOSE_DRAIN_TIMEOUT)
            .await;
        if Arc::strong_count(&self._shared) > 1 {
            return Ok(());
        }
        self.worker_client()
            .await
            .shutdown(Duration::from_secs(2))
            .await
            .map_err(SinkFailure::from)
    }

    #[cfg(test)]
    pub async fn close(&self) -> Result<()> {
        self.close_with_failure()
            .await
            .map_err(SinkFailure::into_error)
    }
}

#[derive(Clone)]
pub enum SinkFacade {
    Local(Arc<SinkFileMeta>),
    Worker(Arc<SinkWorkerClientHandle>),
}

impl SinkFileMeta {
    pub(crate) fn with_boundaries_and_state_with_failure(
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        state_boundary: Arc<dyn StateBoundary>,
        source_cfg: SourceConfig,
    ) -> std::result::Result<Self, SinkFailure> {
        Self::with_boundaries_and_state_internal(
            node_id,
            boundary,
            state_boundary,
            source_cfg,
            false,
        )
        .map_err(SinkFailure::from)
    }

    pub(crate) fn with_boundaries_and_state_deferred_authority_with_failure(
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        state_boundary: Arc<dyn StateBoundary>,
        source_cfg: SourceConfig,
    ) -> std::result::Result<Self, SinkFailure> {
        Self::with_boundaries_and_state_internal(
            node_id,
            boundary,
            state_boundary,
            source_cfg,
            true,
        )
        .map_err(SinkFailure::from)
    }

    pub(crate) fn start_runtime_endpoints_with_failure(
        &self,
        io_boundary: Arc<dyn ChannelIoSubset>,
        node_id: NodeId,
    ) -> std::result::Result<(), SinkFailure> {
        self.start_runtime_endpoints_on_boundary(io_boundary, node_id)
            .map_err(SinkFailure::from)
    }

    pub(crate) fn update_logical_roots_with_failure(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
        host_object_grants: &[GrantedMountRoot],
    ) -> std::result::Result<(), SinkFailure> {
        self.perform_update_logical_roots(roots, host_object_grants)
            .map_err(SinkFailure::from)
    }

    pub(crate) fn logical_roots_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<crate::source::config::RootSpec>, SinkFailure> {
        self.snapshot_logical_roots().map_err(SinkFailure::from)
    }

    pub(crate) fn cached_logical_roots_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<crate::source::config::RootSpec>, SinkFailure> {
        self.snapshot_cached_logical_roots()
            .map_err(SinkFailure::from)
    }

    pub(crate) fn status_snapshot_with_failure(
        &self,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        self.build_status_snapshot().map_err(SinkFailure::from)
    }

    pub(crate) fn cached_status_snapshot_with_failure(
        &self,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        self.build_cached_status_snapshot()
            .map_err(SinkFailure::from)
    }

    pub(crate) fn status_snapshot_nonblocking_with_failure(
        &self,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        self.build_status_snapshot().map_err(SinkFailure::from)
    }

    pub(crate) fn scheduled_group_ids_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Option<std::collections::BTreeSet<String>>, SinkFailure> {
        self.snapshot_scheduled_group_ids()
            .map_err(SinkFailure::from)
    }

    pub(crate) fn health_with_failure(
        &self,
    ) -> std::result::Result<crate::query::models::HealthStats, SinkFailure> {
        self.build_health_snapshot().map_err(SinkFailure::from)
    }

    pub(crate) fn visibility_lag_samples_since_with_failure(
        &self,
        since_us: u64,
    ) -> std::result::Result<Vec<VisibilityLagSample>, SinkFailure> {
        Ok(self.snapshot_visibility_lag_samples_since(since_us))
    }

    fn query_node_with_failure(
        &self,
        path: &[u8],
    ) -> std::result::Result<Option<QueryNode>, SinkFailure> {
        let request = InternalQueryRequest::materialized(
            QueryOp::Tree,
            QueryScope {
                path: path.to_vec(),
                recursive: false,
                max_depth: Some(0),
                selected_group: None,
            },
            None,
        );
        decode_exact_query_node(self.perform_materialized_query(&request)?, path)
            .map_err(SinkFailure::from)
    }

    pub(crate) fn materialized_query_with_failure(
        &self,
        request: &InternalQueryRequest,
    ) -> std::result::Result<Vec<Event>, SinkFailure> {
        self.perform_materialized_query(request)
            .map_err(SinkFailure::from)
    }

    fn subtree_stats_with_failure(
        &self,
        path: &[u8],
    ) -> std::result::Result<Vec<Event>, SinkFailure> {
        self.perform_materialized_query(&InternalQueryRequest::materialized(
            QueryOp::Stats,
            QueryScope {
                path: path.to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: None,
            },
            None,
        ))
        .map_err(SinkFailure::from)
    }

    pub(crate) async fn send_with_failure(
        &self,
        events: &[Event],
    ) -> std::result::Result<(), SinkFailure> {
        self.apply_events(events).map_err(SinkFailure::from)
    }

    pub(crate) async fn recv_with_failure(
        &self,
        opts: RecvOpts,
    ) -> std::result::Result<Vec<Event>, SinkFailure> {
        let _ = opts;
        self.perform_materialized_query(&InternalQueryRequest::default())
            .map_err(SinkFailure::from)
    }

    pub(crate) async fn apply_orchestration_signals_with_failure(
        &self,
        signals: &[SinkControlSignal],
    ) -> std::result::Result<(), SinkFailure> {
        self.perform_apply_orchestration_signals(signals)
            .await
            .map(|_| ())
            .map_err(SinkFailure::from)
    }

    pub(crate) async fn on_control_frame_with_failure(
        &self,
        envelopes: &[ControlEnvelope],
    ) -> std::result::Result<(), SinkFailure> {
        let signals = sink_control_signals_from_envelopes(envelopes).map_err(SinkFailure::from)?;
        self.apply_orchestration_signals_with_failure(&signals)
            .await
    }

    pub(crate) async fn close_with_failure(&self) -> std::result::Result<(), SinkFailure> {
        self.perform_close().await.map_err(SinkFailure::from)
    }
}

impl SinkFacade {
    pub fn local(sink: Arc<SinkFileMeta>) -> Self {
        Self::Local(sink)
    }

    pub fn worker(client: Arc<SinkWorkerClientHandle>) -> Self {
        Self::Worker(client)
    }

    pub fn is_worker(&self) -> bool {
        matches!(self, Self::Worker(_))
    }

    pub(crate) async fn ensure_started_with_failure(&self) -> std::result::Result<(), SinkFailure> {
        match self {
            Self::Local(_) => Ok(()),
            Self::Worker(client) => client.ensure_started_with_failure().await,
        }
    }

    #[cfg(test)]
    pub async fn ensure_started(&self) -> Result<()> {
        self.ensure_started_with_failure()
            .await
            .map_err(SinkFailure::into_error)
    }

    pub(crate) async fn update_logical_roots_with_failure(
        &self,
        roots: Vec<crate::source::config::RootSpec>,
        host_object_grants: &[GrantedMountRoot],
    ) -> std::result::Result<(), SinkFailure> {
        match self {
            Self::Local(sink) => sink.update_logical_roots_with_failure(roots, host_object_grants),
            Self::Worker(client) => {
                client
                    .update_logical_roots_with_failure(roots, host_object_grants.to_vec())
                    .await
            }
        }
    }

    pub(crate) fn cached_logical_roots_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<crate::source::config::RootSpec>, SinkFailure> {
        match self {
            Self::Local(sink) => sink.cached_logical_roots_snapshot_with_failure(),
            Self::Worker(client) => client.cached_logical_roots_snapshot_with_failure(),
        }
    }

    #[cfg(test)]
    pub(crate) async fn logical_roots_snapshot_with_failure(
        &self,
    ) -> std::result::Result<Vec<crate::source::config::RootSpec>, SinkFailure> {
        match self {
            Self::Local(sink) => sink.logical_roots_snapshot_with_failure(),
            Self::Worker(client) => client.logical_roots_snapshot_with_failure().await,
        }
    }

    pub(crate) async fn status_snapshot_with_failure(
        &self,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        match self {
            Self::Local(sink) => sink.status_snapshot_with_failure(),
            Self::Worker(client) => client.status_snapshot_with_failure().await,
        }
    }

    pub(crate) async fn status_snapshot_nonblocking_with_failure(
        &self,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        match self {
            Self::Local(sink) => sink.status_snapshot_nonblocking_with_failure(),
            Self::Worker(client) => client.status_snapshot_nonblocking_with_failure().await,
        }
    }

    #[cfg(test)]
    pub async fn status_snapshot_nonblocking(&self) -> Result<SinkStatusSnapshot> {
        self.status_snapshot_nonblocking_with_failure()
            .await
            .map_err(SinkFailure::into_error)
    }

    pub(crate) async fn status_snapshot_nonblocking_for_status_route(
        &self,
    ) -> Result<(SinkStatusSnapshot, bool)> {
        match self {
            Self::Local(sink) => sink
                .status_snapshot_nonblocking_with_failure()
                .map(|snapshot| (snapshot, false))
                .map_err(SinkFailure::into_error),
            Self::Worker(client) => Ok(client.status_snapshot_nonblocking_for_status_route().await),
        }
    }

    pub(crate) fn cached_status_snapshot_with_failure(
        &self,
    ) -> std::result::Result<SinkStatusSnapshot, SinkFailure> {
        match self {
            Self::Local(sink) => sink.cached_status_snapshot_with_failure(),
            Self::Worker(client) => client.cached_status_snapshot_with_failure(),
        }
    }

    pub(crate) fn cached_progress_snapshot_with_failure(
        &self,
    ) -> std::result::Result<crate::sink::SinkProgressSnapshot, SinkFailure> {
        self.cached_status_snapshot_with_failure()
            .map(|snapshot| snapshot.progress_snapshot())
    }

    #[cfg(test)]
    pub(crate) async fn worker_instance_id_for_tests(&self) -> Option<u64> {
        match self {
            Self::Local(_) => None,
            Self::Worker(client) => Some(client.worker_instance_id_for_tests().await),
        }
    }

    pub(crate) async fn scheduled_group_ids_with_failure(
        &self,
    ) -> std::result::Result<Option<std::collections::BTreeSet<String>>, SinkFailure> {
        match self {
            Self::Local(sink) => sink.scheduled_group_ids_snapshot_with_failure(),
            Self::Worker(client) => client.scheduled_group_ids_with_failure().await,
        }
    }

    #[cfg(test)]
    pub async fn scheduled_group_ids(&self) -> Result<Option<std::collections::BTreeSet<String>>> {
        self.scheduled_group_ids_with_failure()
            .await
            .map_err(SinkFailure::into_error)
    }

    pub(crate) async fn control_op_inflight(&self) -> bool {
        match self {
            Self::Local(_) => false,
            Self::Worker(client) => client.control_op_inflight_for_internal_status(),
        }
    }

    pub(crate) async fn query_node_with_failure(
        &self,
        path: &[u8],
    ) -> std::result::Result<Option<QueryNode>, SinkFailure> {
        match self {
            Self::Local(sink) => sink.query_node_with_failure(path),
            Self::Worker(client) => client.query_node_with_failure(path.to_vec()).await,
        }
    }

    pub(crate) async fn materialized_query_with_failure(
        &self,
        request: &InternalQueryRequest,
    ) -> std::result::Result<Vec<Event>, SinkFailure> {
        match self {
            Self::Local(sink) => sink.materialized_query_with_failure(request),
            Self::Worker(client) => {
                client
                    .materialized_query_with_failure(request.clone())
                    .await
            }
        }
    }

    pub(crate) async fn materialized_query_nonblocking_with_failure(
        &self,
        request: &InternalQueryRequest,
    ) -> std::result::Result<Vec<Event>, SinkFailure> {
        match self {
            Self::Local(sink) => sink.materialized_query_with_failure(request),
            Self::Worker(client) => {
                client
                    .materialized_query_nonblocking_with_failure(request.clone())
                    .await
            }
        }
    }

    pub(crate) async fn subtree_stats_with_failure(
        &self,
        path: &[u8],
    ) -> std::result::Result<Vec<Event>, SinkFailure> {
        match self {
            Self::Local(sink) => sink.subtree_stats_with_failure(path),
            Self::Worker(client) => client.subtree_stats_with_failure(path.to_vec()).await,
        }
    }

    pub(crate) async fn send_with_failure(
        &self,
        events: &[Event],
    ) -> std::result::Result<(), SinkFailure> {
        match self {
            Self::Local(sink) => sink.send_with_failure(events).await,
            Self::Worker(client) => client.send_with_failure(events.to_vec()).await,
        }
    }

    #[cfg(test)]
    pub(crate) async fn send(&self, events: &[Event]) -> Result<()> {
        self.send_with_failure(events)
            .await
            .map_err(SinkFailure::into_error)
    }

    pub(crate) async fn recv_with_failure(
        &self,
        opts: RecvOpts,
    ) -> std::result::Result<Vec<Event>, SinkFailure> {
        match self {
            Self::Local(sink) => sink.recv_with_failure(opts).await,
            Self::Worker(client) => client.recv_with_failure(opts).await,
        }
    }

    #[cfg(test)]
    pub async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        let signals = sink_control_signals_from_envelopes(envelopes)?;
        self.apply_orchestration_signals_with_total_timeout_with_failure(
            &signals,
            SINK_WORKER_CONTROL_TOTAL_TIMEOUT,
        )
        .await
        .map_err(SinkFailure::into_error)
    }

    pub(crate) async fn current_generation_tick_fast_path_eligible(&self) -> bool {
        match self {
            Self::Local(_) => true,
            Self::Worker(client) => client.current_generation_tick_fast_path_eligible().await,
        }
    }

    pub(crate) async fn apply_orchestration_signals_with_total_timeout_with_failure(
        &self,
        signals: &[SinkControlSignal],
        total_timeout: Duration,
    ) -> std::result::Result<(), SinkFailure> {
        if total_timeout.is_zero() {
            return Err(SinkFailure::retry_budget_exhausted(
                SinkRetryBudgetExhaustionKind::ControlFrameAttempt,
            ));
        }
        // runtime_app owns the outer recovery loop for mixed source/sink recovery.
        // Keep each nested sink-client control attempt short so retryable resets
        // return to the caller instead of burning the full nested client budget.
        let single_attempt_total_timeout = std::cmp::min(
            total_timeout,
            SINK_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT,
        );
        match self {
            Self::Local(sink) => map_sink_failure_timeout_result(
                tokio::time::timeout(
                    single_attempt_total_timeout,
                    sink.apply_orchestration_signals_with_failure(signals),
                )
                .await,
                SinkRetryBudgetExhaustionKind::ControlFrameAttempt,
            ),
            Self::Worker(client) => {
                let envelopes = signals
                    .iter()
                    .map(SinkControlSignal::envelope)
                    .collect::<Vec<_>>();
                let rpc_timeout = std::cmp::min(
                    SINK_WORKER_CONTROL_RPC_TIMEOUT,
                    single_attempt_total_timeout,
                );
                client
                    .on_control_frame_with_timeouts_with_failure(
                        envelopes,
                        single_attempt_total_timeout,
                        rpc_timeout,
                    )
                    .await
            }
        }
    }

    pub(crate) async fn close_with_failure(&self) -> std::result::Result<(), SinkFailure> {
        match self {
            Self::Local(sink) => sink.close_with_failure().await,
            Self::Worker(client) => client.close_with_failure().await,
        }
    }

    #[cfg(test)]
    pub async fn close(&self) -> Result<()> {
        self.close_with_failure()
            .await
            .map_err(SinkFailure::into_error)
    }

    pub(crate) async fn wait_for_control_ops_to_drain_for_handoff(&self) {
        if let Self::Worker(client) = self {
            client
                .wait_for_control_ops_to_drain(SINK_WORKER_CLOSE_DRAIN_TIMEOUT)
                .await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn shutdown_shared_worker_for_tests(&self, timeout: Duration) -> Result<()> {
        match self {
            Self::Local(_) => Err(CnxError::InvalidInput(
                "shutdown_shared_worker_for_tests requires worker-backed sink facade".into(),
            )),
            Self::Worker(client) => client.shutdown_shared_worker_for_tests(timeout).await,
        }
    }
}

capanix_runtime_entry_sdk::worker_runtime::define_typed_worker_rpc! {
    pub struct SinkWorkerRpc {
        request: SinkWorkerRequest,
        response: SinkWorkerResponse,
        encode_request: encode_request,
        decode_request: decode_request,
        encode_response: encode_response,
        decode_response: decode_response,
        invalid_input: SinkWorkerResponse::InvalidInput,
        error: SinkWorkerResponse::Error,
        unavailable: "sink worker unavailable",
    }
}

impl TypedWorkerInit<SourceConfig> for SinkWorkerRpc {
    type InitPayload = SinkWorkerInitConfig;

    fn init_payload(_node_id: &NodeId, config: &SourceConfig) -> Result<Self::InitPayload> {
        Ok(SinkWorkerInitConfig {
            roots: config.roots.clone(),
            host_object_grants: config.host_object_grants.clone(),
            sink_tombstone_ttl_ms: config.sink_tombstone_ttl.as_millis() as u64,
            sink_tombstone_tolerance_us: config.sink_tombstone_tolerance_us,
        })
    }
}

#[cfg(test)]
mod tests;
